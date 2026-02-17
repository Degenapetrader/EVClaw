#!/usr/bin/env python3
"""Opt3: Exit decider + executor using OpenClaw agent (stateless).

We do NOT call provider APIs directly.

Input stream (plans):
- decay_worker in --notify-only records a CLOSE plan decision with source=decay_worker_notify and reason=DECAY_EXIT.
- position_review_worker records CLOSE/HOLD plan decisions with source=position_review_worker and reasons like HOURLY_REVIEW_*.

This daemon:
- polls ai_trader.db for new plan decisions
- asks an OpenClaw agent to decide CLOSE vs HOLD
- if CLOSE: executes via `python3 cli.py positions --close ...`
- records an audit decision row in decay_decisions
  (legacy `decided_by=hl-exit-decider` tag retained for DB compatibility)
- appends JSONL journal to configured docs path (default: <EVCLAW_DOCS_DIR>/llm_decisions.jsonl)

Execution-gate contract:
- this module is the only executor for producer-plan exits
- producer workers never execute close orders directly

Failure policy:
- if agent call fails/invalid -> HOLD (safe)
"""

from __future__ import annotations

import argparse
import asyncio
import datetime
import fcntl
import hashlib
import json
import logging
import os
import sqlite3
import sys
import time
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Dict, List, Optional, Tuple

import yaml
from dotenv import load_dotenv

# Load .env from skill directory
load_dotenv(Path(__file__).resolve().parent / ".env")

from ai_trader_db import AITraderDB
from config_env import apply_env_overrides
from env_utils import env_float as _shared_env_float
from env_utils import env_int as _shared_env_int
from env_utils import env_str as _shared_env_str
from env_utils import EVCLAW_DOCS_DIR, EVCLAW_MEMORY_DIR, EVCLAW_RUNTIME_DIR, EVCLAW_DB_PATH
from jsonl_io import append_jsonl
from openclaw_agent_client import openclaw_agent_turn, safe_json_loads

try:
    from learning_dossier_aggregator import get_dossier_snippet as _get_dossier_snippet
except Exception:
    _get_dossier_snippet = None

SKILL_DIR = Path(__file__).parent
DEFAULT_DB = EVCLAW_DB_PATH
LOG = logging.getLogger(__name__)
_WAL_INITIALIZED_DB_PATHS: set[str] = set()

EXIT_DECIDER_AGENT_ID = "evclaw-exit-decider"
EXIT_DECIDER_TIMEOUT_SEC = 120.0
EXIT_DECIDER_CLOSE_TIMEOUT_SEC = 120.0
EXIT_DECIDER_STATE_PATH = Path(EVCLAW_MEMORY_DIR) / "exit_decider_state.json"
EXIT_DECIDER_LOCK_PATH = Path(EVCLAW_MEMORY_DIR) / "exit_decider.lock"
EXIT_DECIDER_DECISIONS_JSONL = str(Path(EVCLAW_DOCS_DIR) / "llm_decisions.jsonl")
EXIT_DECIDER_DIARY_PATH = str(Path(EVCLAW_DOCS_DIR) / "openclawdiary.md")
EXIT_DECIDER_MODEL_DEFAULT: Optional[str] = None
EXIT_DECIDER_THINKING_DEFAULT = "medium"
EXIT_DYNAMIC_BACKLOG_TWO = 10
EXIT_DYNAMIC_BACKLOG_THREE = 25
EXIT_ENABLE_DOSSIER = True
EXIT_DOSSIER_MAX_CHARS = 480
EXIT_EXPOSURE_MAX_AGE_SEC = 900.0
EXIT_LOG_RAW_TEXT = False


def _truthy(raw: Optional[str]) -> bool:
    return str(raw or "").strip().lower() in {"1", "true", "yes", "y", "on"}


def _normalize_agent_id(value: Optional[str]) -> Optional[str]:
    raw = str(value or "").strip()
    if not raw:
        return None
    if raw.lower() in {"default", "openclaw-default"}:
        return "default"
    return raw


def _utc_now() -> str:
    return datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def _env_int(name: str, default: int) -> int:
    return _shared_env_int(name, default)


def _env_float(name: str, default: float) -> float:
    return _shared_env_float(name, default)


def _env_csv(name: str) -> Tuple[str, ...]:
    raw = _shared_env_str(name)
    if raw is None:
        return tuple()
    parts = [str(x or "").strip() for x in str(raw).split(",")]
    return tuple(x for x in parts if x)


def _coerce_csv_tuple(value: Any) -> Tuple[str, ...]:
    if value is None:
        return tuple()
    if isinstance(value, str):
        parts = [str(x or "").strip() for x in value.split(",")]
        return tuple(x for x in parts if x)
    if isinstance(value, (list, tuple, set)):
        out: List[str] = []
        for item in value:
            text = str(item or "").strip()
            if text:
                out.append(text)
        return tuple(out)
    text = str(value).strip()
    return (text,) if text else tuple()


def _load_skill_config() -> Dict[str, Any]:
    cfg_file = SKILL_DIR / "skill.yaml"
    if not cfg_file.exists():
        return {}
    try:
        with cfg_file.open("r", encoding="utf-8") as f:
            raw = yaml.safe_load(f) or {}
        if not isinstance(raw, dict):
            return {}
        return apply_env_overrides(raw)
    except Exception:
        return {}


def _skill_exit_decider_cfg() -> Dict[str, Any]:
    cfg = _load_skill_config()
    ex = ((cfg.get("config") or {}).get("exit_decider") or {})
    return ex if isinstance(ex, dict) else {}


def _skill_exposure_cfg() -> Dict[str, Any]:
    cfg = _load_skill_config()
    ex = ((cfg.get("config") or {}).get("exposure") or {})
    return ex if isinstance(ex, dict) else {}


def _skill_float(cfg: Dict[str, Any], key: str, default: float) -> float:
    try:
        raw = cfg.get(key, default)
        return float(default if raw is None else raw)
    except Exception:
        return float(default)


EXIT_MAX_NET_EXPOSURE_MULT = _skill_float(_skill_exposure_cfg(), "max_net_exposure_mult", 2.0)


def _agent_audit_payload(agent_id: str, session_id: str, assistant_text: Optional[str]) -> Dict[str, Any]:
    raw = str(assistant_text or "")
    payload: Dict[str, Any] = {
        "agent_id": str(agent_id or ""),
        "session_id": str(session_id or ""),
        "raw_text_len": len(raw),
        "raw_text_sha256": hashlib.sha256(raw.encode("utf-8")).hexdigest() if raw else None,
    }
    if EXIT_LOG_RAW_TEXT and raw:
        payload["raw_text"] = raw
    return payload


def _load_latest_context_compact(runtime_dir: str = EVCLAW_RUNTIME_DIR) -> Tuple[str, Optional[Dict[str, Any]]]:
    """Return (global_context_compact, optional symbol blob map for quick lookup)."""
    try:
        rdir = Path(runtime_dir)
        if not rdir.exists():
            return "", None

        # Preferred: direct latest pointer maintained by writer-side tooling.
        latest_pointer = rdir / "evclaw_context_latest.json"
        candidates: List[Path] = []
        if latest_pointer.exists():
            candidates.append(latest_pointer)

        # Fallback: single-pass scan (avoid sorting/stat calls across all files).
        newest_path: Optional[Path] = None
        newest_mtime = float("-inf")
        with os.scandir(rdir) as entries:
            for ent in entries:
                name = str(ent.name or "")
                if not (name.startswith("evclaw_context_") and name.endswith(".json")):
                    continue
                try:
                    st = ent.stat()
                except Exception:
                    continue
                if st.st_mtime > newest_mtime:
                    newest_mtime = float(st.st_mtime)
                    newest_path = Path(ent.path)
        if newest_path is not None:
            candidates.append(newest_path)

        payload = None
        for path in candidates:
            try:
                payload = json.loads(path.read_text())
                break
            except Exception:
                payload = None
        if payload is None:
            return "", None
        if not isinstance(payload, dict):
            return "", None
        compact = str(payload.get("global_context_compact") or "")
        symbols = payload.get("symbols")
        if isinstance(symbols, dict):
            return compact, symbols
        return compact, None
    except Exception:
        return "", None


def _load_cursor_state(state_path: Path) -> int:
    try:
        if state_path.exists():
            return int(json.loads(state_path.read_text()).get("last_id") or 0)
    except Exception:
        return 0
    return 0


def _save_cursor_state(state_path: Path, last_id: int) -> None:
    try:
        state_path.write_text(json.dumps({"last_id": int(last_id), "ts": time.time()}, separators=(",", ":")))
    except Exception as exc:
        LOG.warning("failed to persist exit decider cursor state: %s", exc)


def _acquire_single_instance_lock(lock_path: Path) -> Optional[Any]:
    """Best-effort non-blocking advisory lock for single-process execution."""
    try:
        lock_path.parent.mkdir(parents=True, exist_ok=True)
        fh = lock_path.open("a+", encoding="utf-8")
    except Exception as exc:
        LOG.warning("failed to open lock file %s: %s", lock_path, exc)
        return None
    try:
        fcntl.flock(fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        try:
            fh.close()
        except Exception:
            pass
        return None
    try:
        fh.seek(0)
        fh.truncate(0)
        fh.write(f"{os.getpid()}\n")
        fh.flush()
    except Exception:
        pass
    return fh


def _release_single_instance_lock(lock_fh: Optional[Any]) -> None:
    if lock_fh is None:
        return
    try:
        fcntl.flock(lock_fh.fileno(), fcntl.LOCK_UN)
    except Exception:
        pass
    try:
        lock_fh.close()
    except Exception:
        pass


@contextmanager
def _db_connect(db_path: str):
    conn = sqlite3.connect(db_path, timeout=30.0)
    try:
        db_key = os.path.abspath(str(db_path))
        if db_key not in _WAL_INITIALIZED_DB_PATHS:
            conn.execute("PRAGMA journal_mode=WAL")
            _WAL_INITIALIZED_DB_PATHS.add(db_key)
        conn.execute("PRAGMA busy_timeout=30000")
        yield conn
    finally:
        conn.close()


def _load_actionable_plans(
    *,
    db_path: str,
    db: AITraderDB,
    after_id: int,
    hold_backoff_sec: float,
    close_failed_backoff_sec: float,
    close_failed_streak_threshold: int,
    close_failed_streak_backoff_sec: float,
    symbol_prefixes: Tuple[str, ...] = tuple(),
    allowed_venues: Tuple[str, ...] = tuple(),
    conn: Optional[sqlite3.Connection] = None,
) -> Tuple[List["PlanRow"], List["PlanRow"], set[int], set[int], Dict[int, Any]]:
    """Load pending plans and filter to actionable rows with backoff/validity checks."""
    if conn is None:
        with _db_connect(db_path) as local_conn:
            return _load_actionable_plans(
                db_path=db_path,
                db=db,
                after_id=after_id,
                hold_backoff_sec=hold_backoff_sec,
                close_failed_backoff_sec=close_failed_backoff_sec,
                close_failed_streak_threshold=close_failed_streak_threshold,
                close_failed_streak_backoff_sec=close_failed_streak_backoff_sec,
                symbol_prefixes=symbol_prefixes,
                allowed_venues=allowed_venues,
                conn=local_conn,
            )

    resolved_plan_ids: set[int] = set()
    deferred_plan_ids: set[int] = set()
    trade_cache: Dict[int, Any] = {}
    now_ts = time.time()

    plans = _fetch_pending_plans(
        conn,
        after_id=after_id,
        limit=50,
        symbol_prefixes=symbol_prefixes,
        allowed_venues=allowed_venues,
    )
    if not plans:
        return plans, [], resolved_plan_ids, deferred_plan_ids, trade_cache

    plan_ids = [int(plan.id) for plan in plans]
    processed_plan_ids = _already_processed_batch(conn, plan_ids)
    if processed_plan_ids:
        resolved_plan_ids.update(processed_plan_ids)

    candidate_trade_ids = sorted(
        {
            int(plan.trade_id)
            for plan in plans
            if int(plan.id) not in processed_plan_ids and int(plan.trade_id) > 0
        }
    )
    backoff_map = _decision_backoff_remaining_batch(
        conn,
        trade_ids=candidate_trade_ids,
        now_ts=now_ts,
        hold_backoff_sec=hold_backoff_sec,
        close_failed_backoff_sec=close_failed_backoff_sec,
        close_failed_streak_threshold=close_failed_streak_threshold,
        close_failed_streak_backoff_sec=close_failed_streak_backoff_sec,
    )
    trade_cache = _load_trade_cache_for_ids(conn, candidate_trade_ids)

    actionable: List[PlanRow] = []
    for plan in plans:
        plan_id = int(plan.id)
        trade_id = int(plan.trade_id)
        if plan_id in processed_plan_ids:
            continue
        try:
            remaining_backoff_sec, backoff_mode = backoff_map.get(trade_id, (0.0, ""))
            if remaining_backoff_sec > 0:
                LOG.info(
                    "exit plan deferred by backoff plan_id=%s trade_id=%s symbol=%s venue=%s mode=%s remaining_sec=%.1f",
                    plan.id,
                    plan.trade_id,
                    plan.symbol,
                    plan.venue,
                    backoff_mode,
                    remaining_backoff_sec,
                )
                deferred_plan_ids.add(plan_id)
                continue
        except Exception as exc:
            LOG.warning("exit-decider gate check failed for plan_id=%s: %s", plan.id, exc)
            continue

        trade = trade_cache.get(trade_id)
        if not trade:
            # Legacy fallback path for partial/older schemas.
            trade = db.get_trade(trade_id)
            if trade:
                trade_cache[trade_id] = trade
        if not trade:
            resolved_plan_ids.add(plan_id)
            continue
        if getattr(trade, "exit_time", None):
            resolved_plan_ids.add(plan_id)
            continue
        if str(getattr(trade, "state", "") or "").upper() in {"DUST", "EXITING"}:
            resolved_plan_ids.add(plan_id)
            continue
        actionable.append(plan)

    return plans, actionable, resolved_plan_ids, deferred_plan_ids, trade_cache


@dataclass
class PlanRow:
    id: int
    ts: float
    symbol: str
    venue: str
    trade_id: int
    reason: str
    detail: str
    source: str


def _fetch_pending_plans(
    conn: sqlite3.Connection,
    *,
    after_id: int,
    limit: int,
    symbol_prefixes: Tuple[str, ...] = tuple(),
    allowed_venues: Tuple[str, ...] = tuple(),
) -> List[PlanRow]:
    """Fetch producer plan rows to consider.

    Producers may persist plan rows as either HOLD or CLOSE (plan-only intent).
    Decider ownership is still enforced by source/reason filters below.
    """
    rows = conn.execute(
        """
        SELECT id, ts, symbol, venue, trade_id, COALESCE(reason,'') reason, COALESCE(detail,'') detail, COALESCE(source,'') source
        FROM decay_decisions
        WHERE id > ?
          AND action IN ('HOLD', 'CLOSE')
          AND trade_id IS NOT NULL
          AND (
                (source = 'decay_worker_notify' AND reason = 'DECAY_EXIT')
             OR (source = 'position_review_worker' AND reason IN (
                    'HOURLY_REVIEW_NO_PROGRESS',
                    'HOURLY_REVIEW_DEAD_FLAT',
                    'HOURLY_REVIEW_DEADLY_LOSER',
                    'HOURLY_REVIEW_EXPOSURE_REDUCE'
                ))
          )
        ORDER BY id ASC
        LIMIT ?
        """,
        (int(after_id), int(limit)),
    ).fetchall()

    out: List[PlanRow] = []
    prefixes = tuple(str(p or "").upper() for p in (symbol_prefixes or tuple()) if str(p or "").strip())
    venues = {str(v or "").lower() for v in (allowed_venues or tuple()) if str(v or "").strip()}
    for r in rows:
        symbol = str(r[2] or "").upper()
        venue = str(r[3] or "").lower()
        if prefixes and not any(symbol.startswith(pfx) for pfx in prefixes):
            continue
        if venues and venue not in venues:
            continue
        out.append(
            PlanRow(
                id=int(r[0]),
                ts=float(r[1]),
                symbol=symbol,
                venue=venue,
                trade_id=int(r[4]),
                reason=str(r[5] or "").upper(),
                detail=str(r[6] or ""),
                source=str(r[7] or ""),
            )
        )
    return out


def _already_processed(conn: sqlite3.Connection, plan_id: int) -> bool:
    """Return True if an exit-decider decision already exists for this plan id."""
    pid = int(plan_id)
    try:
        row = conn.execute(
            """
            SELECT id FROM decay_decisions
            WHERE source IN ('hl_exit_decider', 'hl_exit_decider_fail')
              AND source_plan_id = ?
            ORDER BY ts DESC
            LIMIT 1
            """,
            (pid,),
        ).fetchone()
        if row:
            return True
    except Exception:
        return False
    return False


def _already_processed_batch(conn: sqlite3.Connection, plan_ids: List[int]) -> set[int]:
    """Batch version of _already_processed for plan id lists."""
    if not plan_ids:
        return set()
    ids = sorted({int(pid) for pid in plan_ids})
    placeholders = ",".join("?" for _ in ids)
    try:
        rows = conn.execute(
            f"""
            SELECT DISTINCT source_plan_id
            FROM decay_decisions
            WHERE source IN ('hl_exit_decider', 'hl_exit_decider_fail')
              AND source_plan_id IN ({placeholders})
            """,
            ids,
        ).fetchall()
    except Exception:
        return set()
    out: set[int] = set()
    for row in rows:
        try:
            out.add(int(row[0]))
        except Exception:
            continue
    return out


def _decision_backoff_remaining_batch(
    conn: sqlite3.Connection,
    *,
    trade_ids: List[int],
    now_ts: float,
    hold_backoff_sec: float,
    close_failed_backoff_sec: float,
    close_failed_streak_threshold: int,
    close_failed_streak_backoff_sec: float,
    sample_limit: int = 12,
) -> Dict[int, Tuple[float, str]]:
    """Batch backoff evaluation for many trades using one decisions query."""
    if not trade_ids:
        return {}
    ids = sorted({int(tid) for tid in trade_ids})
    placeholders = ",".join("?" for _ in ids)
    try:
        rows = conn.execute(
            f"""
            SELECT trade_id, ts, action, source
            FROM decay_decisions
            WHERE trade_id IN ({placeholders})
              AND source IN ('hl_exit_decider', 'hl_exit_decider_fail')
            ORDER BY trade_id ASC, ts DESC
            """,
            ids,
        ).fetchall()
    except Exception:
        return {}

    grouped: Dict[int, List[Tuple[float, str, str]]] = {}
    for row in rows:
        try:
            tid = int(row[0])
            ts = float(row[1] or 0.0)
            action = str(row[2] or "").upper()
            source = str(row[3] or "").lower()
        except Exception:
            continue
        grouped.setdefault(tid, []).append((ts, action, source))

    out: Dict[int, Tuple[float, str]] = {}
    for tid, events in grouped.items():
        if not events:
            out[tid] = (0.0, "")
            continue
        decision_ts, action, source = events[0]
        elapsed = max(0.0, float(now_ts) - float(decision_ts))

        if action == "HOLD" and source == "hl_exit_decider":
            window = max(0.0, float(hold_backoff_sec or 0.0))
            remaining = window - elapsed
            out[tid] = ((remaining if remaining > 0 else 0.0), "hold")
            continue

        if action == "CLOSE_FAILED" and source == "hl_exit_decider_fail":
            window = max(0.0, float(close_failed_backoff_sec or 0.0))
            if int(close_failed_streak_threshold or 0) > 0 and float(close_failed_streak_backoff_sec or 0.0) > 0:
                streak = 0
                for _ts, act, src in events[: max(1, int(sample_limit))]:
                    if act == "CLOSE_FAILED" and src == "hl_exit_decider_fail":
                        streak += 1
                        continue
                    break
                if streak >= int(close_failed_streak_threshold):
                    window = max(window, float(close_failed_streak_backoff_sec))
            remaining = window - elapsed
            out[tid] = ((remaining if remaining > 0 else 0.0), "close_failed")
            continue

        out[tid] = (0.0, "")

    for tid in ids:
        out.setdefault(int(tid), (0.0, ""))
    return out


def _load_trade_cache_for_ids(conn: sqlite3.Connection, trade_ids: List[int]) -> Dict[int, Any]:
    """Load open-trade fields for plan payload generation in one query."""
    if not trade_ids:
        return {}
    ids = sorted({int(tid) for tid in trade_ids})
    placeholders = ",".join("?" for _ in ids)
    rows = conn.execute(
        f"""
        SELECT
            id, symbol, venue, direction, entry_price, notional_usd, entry_time,
            exit_time, state, sl_price, tp_price, sl_order_id, tp_order_id,
            signals_snapshot, signals_agreed
        FROM trades
        WHERE id IN ({placeholders})
        """,
        ids,
    ).fetchall()
    out: Dict[int, Any] = {}
    for row in rows:
        try:
            trade_id = int(row[0])
            out[trade_id] = SimpleNamespace(
                id=trade_id,
                symbol=str(row[1] or ""),
                venue=str(row[2] or ""),
                direction=str(row[3] or ""),
                entry_price=float(row[4] or 0.0),
                notional_usd=float(row[5] or 0.0),
                entry_time=float(row[6] or 0.0),
                exit_time=(float(row[7]) if row[7] is not None else None),
                state=str(row[8] or ""),
                sl_price=(float(row[9]) if row[9] is not None else None),
                tp_price=(float(row[10]) if row[10] is not None else None),
                sl_order_id=row[11],
                tp_order_id=row[12],
                signals_snapshot=row[13],
                signals_agreed=row[14],
            )
        except Exception:
            continue
    return out


def _compute_next_cursor(
    *,
    last_id: int,
    plans: List[PlanRow],
    resolved_ids: set[int],
) -> int:
    """Advance cursor without skipping unresolved/deferred plans."""
    if not plans:
        return int(last_id)
    plan_ids = [int(p.id) for p in plans]
    unresolved = sorted(pid for pid in plan_ids if pid not in resolved_ids)
    if unresolved:
        # Keep cursor just before earliest unresolved id so it can be revisited.
        return max(int(last_id), int(unresolved[0]) - 1)
    return max(int(last_id), max(plan_ids))


@dataclass
class ExitRuntimeConfig:
    agent_id: Optional[str]
    thinking: str
    model: Optional[str]
    timeout_sec: float
    close_timeout_sec: float
    poll_sec: float
    hold_backoff_sec: float
    close_failed_backoff_sec: float
    close_failed_streak_threshold: int
    close_failed_streak_backoff_sec: float
    max_per_loop: int
    dynamic_max_actions_enabled: bool
    dynamic_max_actions_cap: int
    dynamic_backlog_two: int
    dynamic_backlog_three: int
    exit_enable_dossier: bool
    exit_dossier_max_chars: int
    exposure_max_age_sec: float
    state_path: Path
    runtime_dir: str
    openclaw_cmd: str
    decisions_jsonl_path: str
    diary_path: str
    lock_path: Path
    symbol_prefixes: Tuple[str, ...] = tuple()
    allowed_venues: Tuple[str, ...] = tuple()


def _load_runtime_config(
    exit_cfg: Dict[str, Any],
    *,
    runtime_overrides: Optional[Dict[str, Any]] = None,
) -> ExitRuntimeConfig:
    runtime_overrides = runtime_overrides or {}

    def _cfg_int(name: str, default: int) -> int:
        raw = exit_cfg.get(name, default)
        try:
            return int(float(raw))
        except Exception:
            return int(default)

    def _cfg_float(name: str, default: float) -> float:
        raw = exit_cfg.get(name, default)
        try:
            return float(raw)
        except Exception:
            return float(default)

    agent_id = _normalize_agent_id(
        runtime_overrides.get("agent_id")
        or _shared_env_str("EVCLAW_EXIT_DECIDER_AGENT_ID")
        or EXIT_DECIDER_AGENT_ID
    )

    thinking_raw = str(
        runtime_overrides.get("thinking")
        or _shared_env_str("EVCLAW_EXIT_DECIDER_THINKING")
        or EXIT_DECIDER_THINKING_DEFAULT
    ).strip().lower()
    thinking = (
        thinking_raw
        if thinking_raw in {"off", "minimal", "low", "medium", "high"}
        else EXIT_DECIDER_THINKING_DEFAULT
    )
    # Model is controlled by OpenClaw agent config (single source of truth).
    model: Optional[str] = EXIT_DECIDER_MODEL_DEFAULT
    timeout_sec = EXIT_DECIDER_TIMEOUT_SEC
    close_timeout_sec = max(1.0, EXIT_DECIDER_CLOSE_TIMEOUT_SEC)
    poll_sec = _env_float("EVCLAW_EXIT_DECIDER_POLL_SEC", float(exit_cfg.get("poll_sec") or 10.0))
    hold_backoff_sec = max(
        0.0,
        _env_float(
            "EVCLAW_EXIT_DECIDER_HOLD_BACKOFF_SEC",
            float(exit_cfg.get("backoff_after_hold_sec") or 900.0),
        ),
    )
    close_failed_backoff_sec = max(
        0.0,
        _env_float(
            "EVCLAW_EXIT_DECIDER_CLOSE_FAILED_BACKOFF_SEC",
            float(exit_cfg.get("backoff_after_close_failed_sec") or 1800.0),
        ),
    )
    close_failed_streak_threshold = max(
        0,
        _env_int(
            "EVCLAW_EXIT_DECIDER_CLOSE_FAILED_STREAK_THRESHOLD",
            int(exit_cfg.get("close_failed_streak_threshold") or 3),
        ),
    )
    close_failed_streak_backoff_sec = max(
        0.0,
        _env_float(
            "EVCLAW_EXIT_DECIDER_CLOSE_FAILED_STREAK_BACKOFF_SEC",
            float(exit_cfg.get("backoff_after_close_failed_streak_sec") or 3600.0),
        ),
    )
    max_per_loop = max(
        0,
        _env_int("EVCLAW_EXIT_DECIDER_MAX_ACTIONS_PER_LOOP", int(exit_cfg.get("max_actions_per_loop") or 1)),
    )
    raw_dynamic_enabled = exit_cfg.get("dynamic_max_actions_enabled")
    if raw_dynamic_enabled is None:
        raw_dynamic_enabled = "1"
    dynamic_max_actions_enabled = _truthy(str(raw_dynamic_enabled))
    dynamic_max_actions_cap = max(
        1,
        _cfg_int("dynamic_max_actions_cap", 3),
    )
    dynamic_backlog_two = max(1, EXIT_DYNAMIC_BACKLOG_TWO)
    dynamic_backlog_three = max(int(dynamic_backlog_two) + 1, EXIT_DYNAMIC_BACKLOG_THREE)
    exit_enable_dossier = EXIT_ENABLE_DOSSIER
    exit_dossier_max_chars = max(128, EXIT_DOSSIER_MAX_CHARS)
    exposure_max_age_sec = max(0.0, _cfg_float("exposure_max_age_sec", EXIT_EXPOSURE_MAX_AGE_SEC))

    state_path = Path(str(runtime_overrides.get("state_path") or EXIT_DECIDER_STATE_PATH))
    runtime_dir = EVCLAW_RUNTIME_DIR
    openclaw_cmd = (os.getenv("OPENCLAW_CMD") or "openclaw").strip()
    decisions_jsonl_path = str(
        runtime_overrides.get("decisions_jsonl_path") or EXIT_DECIDER_DECISIONS_JSONL
    ).strip() or EXIT_DECIDER_DECISIONS_JSONL
    diary_path = str(
        runtime_overrides.get("diary_path") or EXIT_DECIDER_DIARY_PATH
    ).strip() or EXIT_DECIDER_DIARY_PATH
    symbol_prefixes = _coerce_csv_tuple(runtime_overrides.get("symbol_prefixes"))
    if not symbol_prefixes:
        symbol_prefixes = _env_csv("EVCLAW_EXIT_DECIDER_SYMBOL_PREFIXES")
    allowed_venues = _coerce_csv_tuple(runtime_overrides.get("allowed_venues"))
    if not allowed_venues:
        allowed_venues = _env_csv("EVCLAW_EXIT_DECIDER_VENUES")
    lock_path = Path(str(runtime_overrides.get("lock_path") or EXIT_DECIDER_LOCK_PATH))
    return ExitRuntimeConfig(
        agent_id=agent_id,
        thinking=thinking,
        model=model,
        timeout_sec=timeout_sec,
        close_timeout_sec=close_timeout_sec,
        poll_sec=poll_sec,
        hold_backoff_sec=hold_backoff_sec,
        close_failed_backoff_sec=close_failed_backoff_sec,
        close_failed_streak_threshold=close_failed_streak_threshold,
        close_failed_streak_backoff_sec=close_failed_streak_backoff_sec,
        max_per_loop=max_per_loop,
        dynamic_max_actions_enabled=dynamic_max_actions_enabled,
        dynamic_max_actions_cap=dynamic_max_actions_cap,
        dynamic_backlog_two=dynamic_backlog_two,
        dynamic_backlog_three=dynamic_backlog_three,
        exit_enable_dossier=exit_enable_dossier,
        exit_dossier_max_chars=exit_dossier_max_chars,
        exposure_max_age_sec=exposure_max_age_sec,
        state_path=state_path,
        runtime_dir=runtime_dir,
        openclaw_cmd=openclaw_cmd,
        decisions_jsonl_path=decisions_jsonl_path,
        diary_path=diary_path,
        lock_path=lock_path,
        symbol_prefixes=symbol_prefixes,
        allowed_venues=allowed_venues,
    )


def _effective_max_actions(
    *,
    base: int,
    backlog: int,
    enabled: bool,
    cap: int,
    backlog_to_two: int,
    backlog_to_three: int,
) -> int:
    """Compute safe per-loop close cap under backlog pressure."""
    base_safe = max(0, int(base))
    cap_safe = max(1, int(cap))
    if not enabled:
        return min(base_safe, cap_safe)

    backlog_safe = max(0, int(backlog))
    target = base_safe
    if backlog_safe >= int(backlog_to_three):
        target = max(target, 3)
    elif backlog_safe >= int(backlog_to_two):
        target = max(target, 2)
    return min(target, cap_safe)


# ---------------------------------------------------------------------------
# Enrichment: 9 contextual fields for better CLOSE/HOLD decisions
# ---------------------------------------------------------------------------

SECTOR_MAP = {
    "BTC": "BTC", "ETH": "L1", "SOL": "L1", "AVAX": "L1", "SUI": "L1",
    "SEI": "L1", "TIA": "L1", "NEAR": "L1", "ADA": "L1",
    "LINK": "Oracle", "PYTH": "Oracle",
    "UNI": "DeFi", "AAVE": "DeFi", "CRV": "DeFi", "PENDLE": "DeFi",
    "LDO": "DeFi", "ENA": "DeFi", "RESOLV": "DeFi",
    "DOGE": "Meme", "WIF": "Meme", "FARTCOIN": "Meme", "SPX": "Meme",
    "MON": "Meme", "TRUMP": "Meme",
    "JUP": "Solana-DeFi",
    "HYPE": "Exchange", "ASTER": "Exchange", "DYDX": "Exchange", "BNB": "Exchange",
    "XRP": "Payment", "LTC": "Payment", "BCH": "Payment",
    "ZEC": "Privacy",
    "ZK": "L2", "POL": "L2", "MNT": "L2", "ZRO": "L2",
    "ICP": "Infra", "LIT": "Infra", "XPL": "Infra",
    "SKR": "AI", "AVNT": "AI",
    "ZORA": "NFT",
}

_context_cache: Dict[str, Dict[str, Any]] = {}
_context_cache_ts: Dict[str, float] = {}


def _get_context_symbols(runtime_dir: str) -> Dict[str, Any]:
    """Load latest context JSON symbols (cached 60s)."""
    global _context_cache, _context_cache_ts
    runtime_key = str(runtime_dir or "").strip() or EVCLAW_RUNTIME_DIR
    now = time.time()
    last_ts = float(_context_cache_ts.get(runtime_key, 0.0) or 0.0)
    cached = _context_cache.get(runtime_key) or {}
    if cached and (now - last_ts) < 60:
        return cached
    try:
        rdir = Path(runtime_key)
        newest = None
        newest_mt = 0.0
        for f in rdir.glob("evclaw_context_*.json"):
            mt = f.stat().st_mtime
            if mt > newest_mt:
                newest_mt = mt
                newest = f
        if newest:
            data = json.loads(newest.read_text())
            _context_cache[runtime_key] = data.get("symbols") or {}
            _context_cache_ts[runtime_key] = now
    except Exception:
        pass
    return _context_cache.get(runtime_key) or {}


def _enrich_trade_blob(
    trade_blob: Dict[str, Any],
    trade: Any,
    db: AITraderDB,
    *,
    db_path: str,
    runtime_dir: str,
    enable_dossier: bool = True,
    dossier_max_chars: int = 480,
    db_conn: Optional[sqlite3.Connection] = None,
) -> None:
    """Add 8 contextual enrichments to trade_blob (best-effort, never raises)."""
    symbol = str(trade_blob.get("symbol") or "")
    direction = str(trade_blob.get("direction") or "").upper()
    entry = float(trade_blob.get("entry_price") or 0)
    mark = float(trade_blob.get("mark_price") or 0)
    sl = float(trade_blob.get("sl_price") or 0)
    tp = float(trade_blob.get("tp_price") or 0)
    age_h = float(trade_blob.get("age_hours") or 0)
    trade_id = int(trade_blob.get("trade_id") or 0)
    unrl_pct = trade_blob.get("unrealized_pnl_pct")

    resolved_db_path = str(db_path or DEFAULT_DB)

    # Signal context (including HIP3_MAIN) for richer exit decisions.
    try:
        raw_sig_snap = getattr(trade, "signals_snapshot", None)
        if isinstance(raw_sig_snap, str):
            sig_snap = json.loads(raw_sig_snap) if raw_sig_snap.strip() else {}
        elif isinstance(raw_sig_snap, dict):
            sig_snap = raw_sig_snap
        else:
            sig_snap = {}
        if not isinstance(sig_snap, dict):
            sig_snap = {}

        raw_sig_agreed = getattr(trade, "signals_agreed", None)
        if isinstance(raw_sig_agreed, str):
            sig_agreed = json.loads(raw_sig_agreed) if raw_sig_agreed.strip() else []
        elif isinstance(raw_sig_agreed, list):
            sig_agreed = raw_sig_agreed
        else:
            sig_agreed = []
        if not isinstance(sig_agreed, list):
            sig_agreed = []
        sig_agreed_norm = [str(s) for s in sig_agreed if str(s or "").strip()]

        signal_context: Dict[str, Any] = {}
        if sig_agreed_norm:
            signal_context["signals_agreed"] = sig_agreed_norm[:12]
        if sig_snap:
            signal_context["signal_keys"] = sorted(str(k) for k in sig_snap.keys())[:12]

        hip3 = sig_snap.get("hip3_main") if isinstance(sig_snap, dict) else None
        if isinstance(hip3, dict) and hip3:
            hip3_ctx: Dict[str, Any] = {}
            hip3_dir = str(hip3.get("direction") or "").upper()
            if hip3_dir in {"LONG", "SHORT"}:
                hip3_ctx["direction"] = hip3_dir
            try:
                hip3_ctx["z_score"] = float(hip3.get("z_score"))
            except Exception:
                pass
            if hip3_ctx:
                signal_context["hip3_main"] = hip3_ctx
                signal_context["has_hip3_main"] = True

        if signal_context:
            trade_blob["signal_context"] = signal_context
    except Exception:
        pass

    # Symbol learning dossier / conclusion (best-effort, optional).
    if enable_dossier and _get_dossier_snippet:
        try:
            candidates: List[str] = []
            sym = str(symbol or "").strip().upper()
            if sym:
                candidates.append(sym)
                if ":" in sym:
                    base_sym = sym.split(":")[-1].upper()
                    if base_sym and base_sym not in candidates:
                        candidates.append(base_sym)
            for cand in candidates:
                snippet = str(
                    _get_dossier_snippet(
                        resolved_db_path,
                        cand,
                        max_chars=max(128, int(dossier_max_chars)),
                    )
                    or ""
                ).strip()
                if snippet:
                    trade_blob["symbol_learning_dossier"] = snippet
                    break
        except Exception:
            pass

    # 1. Symbol history (net profit focus — win_rate can mislead)
    try:
        if db_conn is not None:
            row = db_conn.execute(
                "SELECT COUNT(*), COALESCE(SUM(realized_pnl), 0), COALESCE(AVG(realized_pnl_pct), 0) "
                "FROM trades WHERE symbol = ? AND exit_time IS NOT NULL AND realized_pnl IS NOT NULL",
                (symbol,),
            ).fetchone()
        else:
            with sqlite3.connect(resolved_db_path) as local_conn:
                row = local_conn.execute(
                    "SELECT COUNT(*), COALESCE(SUM(realized_pnl), 0), COALESCE(AVG(realized_pnl_pct), 0) "
                    "FROM trades WHERE symbol = ? AND exit_time IS NOT NULL AND realized_pnl IS NOT NULL",
                    (symbol,),
                ).fetchone()
        if row and row[0]:
            trade_blob["symbol_history"] = {
                "total_trades": int(row[0]),
                "net_pnl_usd": round(float(row[1]), 2),
                "avg_pnl_pct": round(float(row[2]), 3),
            }
    except Exception:
        pass

    # 2. SL/TP progress
    try:
        if entry and mark and sl and tp:
            if direction == "LONG":
                sl_range = entry - sl
                tp_range = tp - entry
                sl_prog = (entry - mark) / sl_range if sl_range > 0 else 0
                tp_prog = (mark - entry) / tp_range if tp_range > 0 else 0
            else:  # SHORT
                sl_range = sl - entry
                tp_range = entry - tp
                sl_prog = (mark - entry) / sl_range if sl_range > 0 else 0
                tp_prog = (entry - mark) / tp_range if tp_range > 0 else 0
            sl_prog = max(0.0, min(1.0, sl_prog))
            tp_prog = max(0.0, min(1.0, tp_prog))
            trade_blob["sl_tp_progress"] = {
                "sl_progress": round(sl_prog, 3),
                "tp_progress": round(tp_prog, 3),
                "trending_toward": "SL" if sl_prog > tp_prog else "TP",
            }
    except Exception:
        pass

    # 3. Consecutive HOLDs
    try:
        if db_conn is not None:
            rows = db_conn.execute(
                "SELECT action FROM decay_decisions WHERE trade_id = ? AND source = 'hl_exit_decider' "
                "ORDER BY rowid DESC LIMIT 50",
                (trade_id,),
            ).fetchall()
        else:
            with sqlite3.connect(resolved_db_path) as local_conn:
                rows = local_conn.execute(
                    "SELECT action FROM decay_decisions WHERE trade_id = ? AND source = 'hl_exit_decider' "
                    "ORDER BY rowid DESC LIMIT 50",
                    (trade_id,),
                ).fetchall()
        count = 0
        for r in rows:
            if str(r[0]).upper() == "HOLD":
                count += 1
            else:
                break
        trade_blob["consecutive_holds"] = count
    except Exception:
        pass

    # 4. Max hold status
    try:
        max_hold = 24.0
        overdue = max(0, age_h - max_hold)
        if age_h < 18:
            urgency = "OK"
        elif age_h < 24:
            urgency = "WARNING"
        elif age_h < 36:
            urgency = "OVERDUE"
        else:
            urgency = "CRITICAL"
        trade_blob["max_hold_status"] = {
            "max_hold_hours": max_hold,
            "overdue_hours": round(overdue, 1),
            "urgency": urgency,
        }
    except Exception:
        pass

    # 5. Sector exposure
    try:
        base_sym = symbol.split(":")[-1].upper() if ":" in symbol else symbol.upper()
        sector = SECTOR_MAP.get(base_sym)
        if sector:
            if db_conn is not None:
                rows = db_conn.execute(
                    "SELECT symbol, direction FROM trades WHERE exit_time IS NULL"
                ).fetchall()
            else:
                with sqlite3.connect(resolved_db_path) as local_conn:
                    rows = local_conn.execute(
                        "SELECT symbol, direction FROM trades WHERE exit_time IS NULL"
                    ).fetchall()
            same_dir = 0
            total_sect = 0
            for r in rows:
                s = str(r[0] or "")
                bs = s.split(":")[-1].upper() if ":" in s else s.upper()
                if SECTOR_MAP.get(bs) == sector:
                    total_sect += 1
                    if str(r[1] or "").upper() == direction:
                        same_dir += 1
            trade_blob["sector_exposure"] = {
                "sector": sector,
                "same_dir_count": same_dir,
                "total_sector_positions": total_sect,
            }
    except Exception:
        pass

    # 6. Funding rate
    try:
        ctx_symbols = _get_context_symbols(runtime_dir=runtime_dir)
        sym_data = ctx_symbols.get(symbol.upper()) or ctx_symbols.get(symbol) or {}
        funding = sym_data.get("funding") or sym_data.get("funding_rate")
        if funding is not None:
            rate = float(funding)
            ann_pct = round(rate * 365 * 24 * 100, 2)  # hourly rate to annualized %
            if direction == "LONG":
                cost = "paying" if rate > 0 else "earning" if rate < 0 else "neutral"
            else:
                cost = "paying" if rate < 0 else "earning" if rate > 0 else "neutral"
            trade_blob["funding_rate"] = {
                "current_rate": rate,
                "annualized_pct": ann_pct,
                "direction_cost": cost,
            }
    except Exception:
        pass

    # 7. Volume/liquidity
    try:
        ctx_symbols = _get_context_symbols(runtime_dir=runtime_dir)
        sym_data = ctx_symbols.get(symbol.upper()) or ctx_symbols.get(symbol) or {}
        vol = sym_data.get("volume_24h") or sym_data.get("dayNtlVlm") or sym_data.get("volume")
        if vol is not None:
            vol_f = float(vol)
            if vol_f > 100_000_000:
                tier = "HIGH"
            elif vol_f > 10_000_000:
                tier = "MEDIUM"
            elif vol_f > 1_000_000:
                tier = "LOW"
            else:
                tier = "VERY_LOW"
            trade_blob["volume_liquidity"] = {"volume_24h_usd": round(vol_f, 0), "liquidity_tier": tier}
    except Exception:
        pass

    # 8. Recent price action
    try:
        if entry and mark:
            pct = round(((mark - entry) / entry) * 100, 3) if direction == "LONG" else round(((entry - mark) / entry) * 100, 3)
            trending_sl = bool(unrl_pct is not None and float(unrl_pct) < 0)
            trade_blob["recent_price_action"] = {
                "pct_from_entry": pct,
                "trending_toward_sl": trending_sl,
            }
    except Exception:
        pass


def _enrich_portfolio_drag(plan_payloads: List[Dict[str, Any]]) -> None:
    """Add portfolio_drag field comparing each trade's PnL against all in batch."""
    try:
        pnls = []
        for p in plan_payloads:
            t = p.get("trade") or {}
            pnl = t.get("unrealized_pnl_usd")
            pnls.append(float(pnl) if pnl is not None else 0.0)

        if not pnls:
            return

        total_loss = sum(x for x in pnls if x < 0) or -0.01  # avoid div by zero
        sorted_pnls = sorted(pnls)

        for i, p in enumerate(plan_payloads):
            t = p.get("trade") or {}
            pnl = pnls[i]
            contribution = round((pnl / total_loss) * 100, 1) if pnl < 0 else 0.0
            rank = sorted_pnls.index(pnl) + 1  # 1 = biggest loser
            t["portfolio_drag"] = {
                "pnl_contribution_pct": contribution,
                "is_biggest_loser": rank == 1 and pnl < 0,
                "rank": rank,
            }
    except Exception:
        pass


def _build_plan_payloads(
    *,
    batch: List[PlanRow],
    db: AITraderDB,
    db_path: str,
    runtime_dir: str,
    enable_dossier: bool,
    dossier_max_chars: int,
    trade_cache: Dict[int, Any],
    resolved_plan_ids: set[int],
    db_conn: Optional[sqlite3.Connection] = None,
) -> Tuple[List[Dict[str, Any]], Dict[int, PlanRow]]:
    plan_payloads: List[Dict[str, Any]] = []
    plan_by_id: Dict[int, PlanRow] = {}
    for plan in batch:
        trade = trade_cache.get(int(plan.trade_id))
        if not trade:
            trade = db.get_trade(int(plan.trade_id))
            if trade:
                trade_cache[int(plan.trade_id)] = trade
        if not trade:
            resolved_plan_ids.add(int(plan.id))
            continue
        detail_obj = _safe_load_detail(plan.detail)
        if not isinstance(detail_obj, dict):
            detail_obj = {}
        try:
            mark_price = float(detail_obj.get("mark")) if detail_obj.get("mark") is not None else None
        except Exception:
            mark_price = None
        try:
            unrealized_pnl_usd = float(detail_obj.get("unrl")) if detail_obj.get("unrl") is not None else None
        except Exception:
            unrealized_pnl_usd = None
        try:
            unrealized_pnl_pct = float(detail_obj.get("pnl_pct")) if detail_obj.get("pnl_pct") is not None else None
        except Exception:
            unrealized_pnl_pct = None
        trade_blob = {
            "trade_id": int(trade.id),
            "symbol": str(trade.symbol),
            "venue": str(trade.venue),
            "direction": str(trade.direction),
            "entry_price": float(trade.entry_price or 0.0),
            "notional_usd": float(trade.notional_usd or 0.0),
            "age_hours": round((time.time() - float(trade.entry_time or time.time())) / 3600.0, 3),
            "unrealized_pnl_usd": unrealized_pnl_usd,
            "unrealized_pnl_pct": unrealized_pnl_pct,
            "mark_price": mark_price,
            "sl_price": float(trade.sl_price) if getattr(trade, "sl_price", None) is not None else None,
            "tp_price": float(trade.tp_price) if getattr(trade, "tp_price", None) is not None else None,
            "sl_order_id": getattr(trade, "sl_order_id", None),
            "tp_order_id": getattr(trade, "tp_order_id", None),
        }
        # Enrich trade blob with contextual fields (best-effort)
        _enrich_trade_blob(
            trade_blob,
            trade,
            db,
            db_path=db_path,
            runtime_dir=runtime_dir,
            enable_dossier=enable_dossier,
            dossier_max_chars=dossier_max_chars,
            db_conn=db_conn,
        )
        _drop_none_fields(
            trade_blob,
            (
                "unrealized_pnl_usd",
                "unrealized_pnl_pct",
                "mark_price",
                "sl_price",
                "tp_price",
                "sl_order_id",
                "tp_order_id",
            ),
        )

        plan_payloads.append(
            {
                "plan_id": int(plan.id),
                "ts": float(plan.ts),
                "source": str(plan.source or ""),
                "reason": str(plan.reason or ""),
                "detail": detail_obj,
                "trade": trade_blob,
            }
        )
        plan_by_id[int(plan.id)] = plan

    # Portfolio drag requires all payloads built first
    _enrich_portfolio_drag(plan_payloads)

    return plan_payloads, plan_by_id


def _record_invalid_reply_holds(
    *,
    db: AITraderDB,
    plan_by_id: Dict[int, PlanRow],
    resolved_plan_ids: set[int],
    decisions_jsonl_path: str,
    global_compact: str,
    exposure_ctx: Dict[str, Any],
    agent_id: Optional[str],
    session_id: str,
    assistant_text: Optional[str],
    plan_payloads: List[Dict[str, Any]],
) -> None:
    for pid, plan in plan_by_id.items():
        marker = f"plan_id={pid} action=HOLD agent_reason=invalid_or_empty_agent_reply plan_reason={plan.reason}"
        try:
            db.record_decay_decision(
                symbol=plan.symbol,
                venue=plan.venue,
                trade_id=plan.trade_id,
                source_plan_id=int(pid),
                action="HOLD",
                reason=plan.reason,
                detail=marker,
                decided_by="hl-exit-decider",
                source="hl_exit_decider",
                dedupe_seconds=0.0,
            )
            resolved_plan_ids.add(int(pid))
        except Exception as exc:
            LOG.warning("failed to record HOLD decision for plan_id=%s: %s", pid, exc)

    append_jsonl(
        decisions_jsonl_path,
        {
            "ts_utc": _utc_now(),
            "kind": "EXIT_GATE_V2",
            "global_context": global_compact,
            "exposure": exposure_ctx,
            "agent": _agent_audit_payload(agent_id or "", session_id, assistant_text),
            "error": "invalid_agent_reply",
            "plans": [{"plan_id": p["plan_id"], "trade_id": p["trade"]["trade_id"], "symbol": p["trade"]["symbol"], "venue": p["trade"]["venue"]} for p in plan_payloads],
        },
    )


async def _apply_agent_decision(
    *,
    db: AITraderDB,
    plan_by_id: Dict[int, PlanRow],
    close_map: Dict[int, Tuple[str, str]],
    hold_map: Dict[int, Tuple[str, str]],
    resolved_plan_ids: set[int],
    max_per_loop: int,
    close_timeout_sec: float,
    decisions_jsonl_path: str,
    diary_path: str,
    global_compact: str,
    exposure_ctx: Dict[str, Any],
    agent_id: Optional[str],
    session_id: str,
    assistant_text: Optional[str],
) -> int:
    acted = 0
    executed_rows: List[Dict[str, Any]] = []
    for pid, (agent_reason, urgency) in close_map.items():
        plan = plan_by_id.get(pid)
        if not plan:
            continue
        detail_marker = f"plan_id={pid} action=CLOSE urgency={urgency} agent_reason={agent_reason} plan_reason={plan.reason} plan_detail={plan.detail}".strip()
        ok, msg = await _run_cli_close(
            symbol=plan.symbol,
            venue=plan.venue,
            reason=plan.reason,
            detail=(detail_marker[:900] if detail_marker else "plan_close"),
            timeout_sec=close_timeout_sec,
        )
        executed_rows.append({"plan_id": pid, "ok": bool(ok), "msg": (msg[-800:] if isinstance(msg, str) else None)})
        try:
            db.record_decay_decision(
                symbol=plan.symbol,
                venue=plan.venue,
                trade_id=plan.trade_id,
                source_plan_id=int(pid),
                action=("CLOSE" if ok else "CLOSE_FAILED"),
                reason=plan.reason,
                detail=detail_marker,
                decided_by="hl-exit-decider",
                source=("hl_exit_decider" if ok else "hl_exit_decider_fail"),
                dedupe_seconds=0.0,
            )
            resolved_plan_ids.add(int(pid))
        except Exception as exc:
            LOG.warning("failed to record CLOSE decision for plan_id=%s trade_id=%s: %s", pid, plan.trade_id, exc)
        try:
            diary = Path(diary_path)
            diary.parent.mkdir(parents=True, exist_ok=True)
            with diary.open("a", encoding="utf-8") as f:
                f.write(f"[{_utc_now()}] EXIT_GATE plan_id={pid} trade_id={plan.trade_id} {plan.symbol} {plan.venue} action=CLOSE ok={bool(ok)} reason={plan.reason}\n")
        except Exception:
            pass
        acted += 1
        if acted >= max_per_loop:
            break

    for pid, (agent_reason, urgency) in hold_map.items():
        plan = plan_by_id.get(pid)
        if not plan:
            continue
        detail_marker = f"plan_id={pid} action=HOLD urgency={urgency} agent_reason={agent_reason} plan_reason={plan.reason} plan_detail={plan.detail}".strip()
        try:
            db.record_decay_decision(
                symbol=plan.symbol,
                venue=plan.venue,
                trade_id=plan.trade_id,
                source_plan_id=int(pid),
                action="HOLD",
                reason=plan.reason,
                detail=detail_marker,
                decided_by="hl-exit-decider",
                source="hl_exit_decider",
                dedupe_seconds=0.0,
            )
            resolved_plan_ids.add(int(pid))
        except Exception as exc:
            LOG.warning("failed to record HOLD decision for plan_id=%s: %s", pid, exc)

    append_jsonl(
        decisions_jsonl_path,
        {
            "ts_utc": _utc_now(),
            "kind": "EXIT_GATE_V2",
            "global_context": global_compact,
            "exposure": exposure_ctx,
            "agent": _agent_audit_payload(agent_id or "", session_id, assistant_text),
            "decision": {"closes": list(close_map.keys()), "holds": list(hold_map.keys())},
            "execution": executed_rows,
        },
    )
    return acted


async def _run_loop_once(
    *,
    db_path: str,
    db: AITraderDB,
    cfg: ExitRuntimeConfig,
    last_id: int,
) -> int:
    global_compact, _symbols = _load_latest_context_compact(runtime_dir=cfg.runtime_dir)
    with _db_connect(db_path) as loop_conn:
        plans, actionable, resolved_plan_ids, deferred_plan_ids, trade_cache = _load_actionable_plans(
            db_path=db_path,
            db=db,
            after_id=last_id,
            hold_backoff_sec=cfg.hold_backoff_sec,
            close_failed_backoff_sec=cfg.close_failed_backoff_sec,
            close_failed_streak_threshold=cfg.close_failed_streak_threshold,
            close_failed_streak_backoff_sec=cfg.close_failed_streak_backoff_sec,
            symbol_prefixes=cfg.symbol_prefixes,
            allowed_venues=cfg.allowed_venues,
            conn=loop_conn,
        )

        exposure_ctx = _load_exposure_context(
            db_path,
            max_age_sec=cfg.exposure_max_age_sec,
            conn=loop_conn,
        )

        if not actionable:
            next_id = _compute_next_cursor(
                last_id=last_id,
                plans=plans,
                resolved_ids=resolved_plan_ids,
            )
            # Persist cursor only after decider write paths (CLOSE/HOLD/CLOSE_FAILED).
            await asyncio.sleep(max(1.0, float(cfg.poll_sec)))
            return next_id

        batch = actionable[: min(10, len(actionable))]
        effective_max_close = _effective_max_actions(
            base=cfg.max_per_loop,
            backlog=len(actionable),
            enabled=cfg.dynamic_max_actions_enabled,
            cap=cfg.dynamic_max_actions_cap,
            backlog_to_two=cfg.dynamic_backlog_two,
            backlog_to_three=cfg.dynamic_backlog_three,
        )
        plan_payloads, plan_by_id = _build_plan_payloads(
            batch=batch,
            db=db,
            db_path=db_path,
            runtime_dir=cfg.runtime_dir,
            enable_dossier=cfg.exit_enable_dossier,
            dossier_max_chars=cfg.exit_dossier_max_chars,
            trade_cache=trade_cache,
            resolved_plan_ids=resolved_plan_ids,
            db_conn=loop_conn,
        )

        if not plan_payloads:
            next_id = _compute_next_cursor(
                last_id=last_id,
                plans=plans,
                resolved_ids=resolved_plan_ids,
            )
            await asyncio.sleep(max(1.0, float(cfg.poll_sec)))
            return next_id

    prompt = _build_batch_prompt(
        plans=plan_payloads,
        global_compact=global_compact,
        exposure=exposure_ctx,
        max_close=effective_max_close,
    )
    session_id = f"hl_exit_gate_{plan_payloads[0]['plan_id']}_{int(time.time())}"

    _raw_json, assistant_text = await openclaw_agent_turn(
        message=prompt,
        session_id=session_id,
        agent_id=cfg.agent_id,
        model=cfg.model,
        thinking=cfg.thinking,
        timeout_sec=cfg.timeout_sec,
        openclaw_cmd=cfg.openclaw_cmd,
    )

    decision = safe_json_loads(assistant_text or "") if assistant_text else None

    ok_schema, close_map, hold_map = _normalize_batch_decision(
        decision,
        input_ids=set(plan_by_id.keys()),
        max_close=effective_max_close,
    )

    if not ok_schema:
        _record_invalid_reply_holds(
            db=db,
            plan_by_id=plan_by_id,
            resolved_plan_ids=resolved_plan_ids,
            decisions_jsonl_path=cfg.decisions_jsonl_path,
            global_compact=global_compact,
            exposure_ctx=exposure_ctx,
            agent_id=cfg.agent_id,
            session_id=session_id,
            assistant_text=assistant_text,
            plan_payloads=plan_payloads,
        )
    else:
        await _apply_agent_decision(
            db=db,
            plan_by_id=plan_by_id,
            close_map=close_map,
            hold_map=hold_map,
            resolved_plan_ids=resolved_plan_ids,
            max_per_loop=effective_max_close,
            close_timeout_sec=cfg.close_timeout_sec,
            decisions_jsonl_path=cfg.decisions_jsonl_path,
            diary_path=cfg.diary_path,
            global_compact=global_compact,
            exposure_ctx=exposure_ctx,
            agent_id=cfg.agent_id,
            session_id=session_id,
            assistant_text=assistant_text,
        )

    if deferred_plan_ids:
        resolved_plan_ids.difference_update(deferred_plan_ids)
    next_id = _compute_next_cursor(
        last_id=last_id,
        plans=plans,
        resolved_ids=resolved_plan_ids,
    )
    _save_cursor_state(cfg.state_path, next_id)
    await asyncio.sleep(max(1.0, float(cfg.poll_sec)))
    return next_id


def _close_failed_streak(
    conn: sqlite3.Connection,
    *,
    trade_id: int,
    sample_limit: int = 12,
) -> int:
    """Count most-recent consecutive CLOSE_FAILED decisions for a trade."""
    rows = conn.execute(
        """
        SELECT action, source
        FROM decay_decisions
        WHERE trade_id = ?
          AND source IN ('hl_exit_decider', 'hl_exit_decider_fail')
        ORDER BY ts DESC
        LIMIT ?
        """,
        (int(trade_id), max(1, int(sample_limit))),
    ).fetchall()
    streak = 0
    for action, source in rows:
        act = str(action or "").upper()
        src = str(source or "").lower()
        if act == "CLOSE_FAILED" and src == "hl_exit_decider_fail":
            streak += 1
            continue
        break
    return int(streak)


def _decision_backoff_remaining(
    conn: sqlite3.Connection,
    *,
    trade_id: int,
    now_ts: float,
    hold_backoff_sec: float,
    close_failed_backoff_sec: float,
    close_failed_streak_threshold: int,
    close_failed_streak_backoff_sec: float,
) -> Tuple[float, str]:
    """Return remaining cooldown seconds for recent decider actions on a trade.

    Backoff policy:
    - HOLD (source=hl_exit_decider) -> hold_backoff_sec
    - CLOSE_FAILED (source=hl_exit_decider_fail) -> close_failed_backoff_sec
      and optional escalation when consecutive CLOSE_FAILED streak is high.
    """
    row = conn.execute(
        """
        SELECT ts, action, source
        FROM decay_decisions
        WHERE trade_id = ?
          AND source IN ('hl_exit_decider', 'hl_exit_decider_fail')
        ORDER BY ts DESC
        LIMIT 1
        """,
        (int(trade_id),),
    ).fetchone()
    if not row:
        return 0.0, ""

    try:
        decision_ts = float(row[0] or 0.0)
    except Exception:
        decision_ts = 0.0
    action = str(row[1] or "").upper()
    source = str(row[2] or "").lower()
    elapsed = max(0.0, float(now_ts) - float(decision_ts))

    if action == "HOLD" and source == "hl_exit_decider":
        window = max(0.0, float(hold_backoff_sec or 0.0))
        remaining = window - elapsed
        return (remaining if remaining > 0 else 0.0), "hold"

    if action == "CLOSE_FAILED" and source == "hl_exit_decider_fail":
        window = max(0.0, float(close_failed_backoff_sec or 0.0))
        if int(close_failed_streak_threshold or 0) > 0 and float(close_failed_streak_backoff_sec or 0.0) > 0:
            streak = _close_failed_streak(
                conn,
                trade_id=int(trade_id),
                sample_limit=max(12, int(close_failed_streak_threshold) + 6),
            )
            if streak >= int(close_failed_streak_threshold):
                window = max(window, float(close_failed_streak_backoff_sec))
        remaining = window - elapsed
        return (remaining if remaining > 0 else 0.0), "close_failed"

    return 0.0, ""


def _safe_load_detail(raw: str) -> Any:
    """Best-effort parse plan.detail.

    Workers increasingly emit JSON strings; for older rows this will just be text.
    """
    try:
        s = str(raw or "").strip()
        if s.startswith("{") and s.endswith("}"):
            return json.loads(s)
    except Exception:
        pass
    return str(raw or "")


def _drop_none_fields(payload: Dict[str, Any], keys: Tuple[str, ...]) -> None:
    """Remove null-heavy fields from prompt payloads to reduce token noise."""
    for key in keys:
        if payload.get(key) is None:
            payload.pop(key, None)


def _load_exposure_context(
    db_path: str,
    *,
    max_age_sec: float = EXIT_EXPOSURE_MAX_AGE_SEC,
    conn: Optional[sqlite3.Connection] = None,
) -> Dict[str, Any]:
    """Return HL equity + net exposure + cap (best-effort).

    Uses latest snapshot; if missing tries previous snapshot; if still missing returns {}.
    """
    ctx: Dict[str, Any] = {}
    max_mult = float(EXIT_MAX_NET_EXPOSURE_MULT if EXIT_MAX_NET_EXPOSURE_MULT > 0 else 2.0)
    max_age_sec = max(0.0, float(max_age_sec))
    now_ts = time.time()

    try:
        if conn is not None:
            rows = conn.execute(
                """
                SELECT ts, ts_iso, hl_equity, hl_net_notional
                FROM monitor_snapshots
                ORDER BY ts DESC
                LIMIT 2
                """
            ).fetchall()
        else:
            with _db_connect(db_path) as local_conn:
                rows = local_conn.execute(
                    """
                    SELECT ts, ts_iso, hl_equity, hl_net_notional
                    FROM monitor_snapshots
                    ORDER BY ts DESC
                    LIMIT 2
                    """
                ).fetchall()
        for r in rows:
            try:
                snap_ts = float((r["ts"] if isinstance(r, sqlite3.Row) else r[0]) or 0.0)
                hl_eq = float((r["hl_equity"] if isinstance(r, sqlite3.Row) else r[2]) or 0.0)
                hl_net = float((r["hl_net_notional"] if isinstance(r, sqlite3.Row) else r[3]) or 0.0)
            except Exception:
                continue
            if max_age_sec > 0 and snap_ts > 0 and (now_ts - snap_ts) > max_age_sec:
                continue
            if hl_eq > 0:
                cap_abs = hl_eq * max_mult if max_mult > 0 else 0.0
                ctx = {
                    "ts_iso": (r["ts_iso"] if isinstance(r, sqlite3.Row) else r[1]),
                    "age_sec": (now_ts - snap_ts) if snap_ts > 0 else None,
                    "hl_equity_usd": hl_eq,
                    "hl_net_exposure_usd": hl_net,
                    "hl_net_cap_abs_usd": cap_abs,
                    "max_net_exposure_mult": max_mult,
                }
                break
    except Exception:
        ctx = {}

    return ctx


def _build_batch_prompt(
    *,
    plans: List[Dict[str, Any]],
    global_compact: str,
    exposure: Dict[str, Any],
    max_close: int,
) -> str:
    """Exit gate prompt (Opt2-style pick/reject).

    Output schema:
      {"closes":[{plan_id,reason,urgency?}],"holds":[{plan_id,reason}]}

    Rules:
    - Return ONLY valid JSON.
    - Each input plan_id must appear exactly once in closes or holds.
    - Every close/hold must include a non-empty reason.
    - At most max_close closes.
    """
    policy = {
        "task": "exit_gate_pick_mode",
        "rules": [
            "Return ONLY valid JSON.",
            f"Pick at most {max_close} plans to CLOSE.",
            "Every plan you do NOT close must be returned in holds.",
            "Every close and hold MUST include a non-empty reason.",
            "Every plan_id must appear EXACTLY ONCE in closes or holds.",
            "If uncertain, choose HOLD.",
            "Consider sl_tp_progress: trades near SL (>0.7) with negative momentum should be closed.",
            "Trades overdue on max_hold (OVERDUE/CRITICAL) should have strong close bias.",
            "High consecutive_holds (>10) indicates signal fatigue — lean toward closing.",
            "Consider funding_rate cost and portfolio_drag rank when breaking ties.",
            "symbol_history is informational; do not over-weight it as a standalone close trigger.",
            "If symbol_learning_dossier indicates repeated failure patterns, use it as a close-bias tie-breaker.",
        ],
    }

    payload = {
        "policy": policy,
        "global_context_compact": str(global_compact or ""),
        "exposure": exposure or {},
        "plans": plans,
        "max_close": int(max_close),
    }
    return "Return ONLY valid JSON.\n" + json.dumps(payload, separators=(",", ":"), ensure_ascii=False)


def _normalize_batch_decision(
    decision: Any,
    *,
    input_ids: set[int],
    max_close: int,
) -> Tuple[bool, Dict[int, Tuple[str, str]], Dict[int, Tuple[str, str]]]:
    """Validate/normalize agent batch decision output.

    Returns:
      (ok_schema, close_map, hold_map)
    """
    closes: List[Dict[str, Any]] = []
    holds: List[Dict[str, Any]] = []
    if isinstance(decision, dict):
        closes = decision.get("closes") or []
        holds = decision.get("holds") or []
    if not isinstance(closes, list) or not isinstance(holds, list):
        return False, {}, {}

    def _norm_item(it: Dict[str, Any]) -> Optional[Tuple[int, str, str]]:
        if not isinstance(it, dict):
            return None
        try:
            pid = int(it.get("plan_id"))
        except Exception:
            return None
        rsn = str(it.get("reason") or "").strip()
        urg = str(it.get("urgency") or "").strip().lower()
        if not rsn:
            return None
        return pid, rsn, urg

    close_map: Dict[int, Tuple[str, str]] = {}
    hold_map: Dict[int, Tuple[str, str]] = {}
    for it in closes:
        norm = _norm_item(it)
        if not norm:
            return False, {}, {}
        pid, rsn, urg = norm
        # Duplicate/overlap plan_ids are invalid: each plan_id must appear exactly once.
        if pid in close_map or pid in hold_map:
            return False, {}, {}
        close_map[pid] = (rsn, urg)
    for it in holds:
        norm = _norm_item(it)
        if not norm:
            return False, {}, {}
        pid, rsn, urg = norm
        if pid in hold_map or pid in close_map:
            return False, {}, {}
        hold_map[pid] = (rsn, urg)

    decided_ids = set(close_map.keys()) | set(hold_map.keys())
    if decided_ids != input_ids:
        return False, {}, {}

    max_close_safe = max(0, int(max_close))
    if len(close_map) > max_close_safe:
        return False, {}, {}

    return True, close_map, hold_map


async def _run_cli_close(
    *,
    symbol: str,
    venue: str,
    reason: str,
    detail: str,
    timeout_sec: float = 120.0,
) -> Tuple[bool, str]:
    cmd = [
        sys.executable,
        str(SKILL_DIR / "cli.py"),
        "positions",
        "--close",
        symbol,
        "--venue",
        venue,
        "--reason",
        reason,
        "--detail",
        detail,
    ]
    try:
        proc = await asyncio.create_subprocess_exec(*cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        try:
            out_b, err_b = await asyncio.wait_for(proc.communicate(), timeout=max(1.0, float(timeout_sec)))
        except asyncio.TimeoutError:
            try:
                proc.kill()
            except Exception:
                pass
            try:
                await asyncio.wait_for(proc.wait(), timeout=3.0)
            except Exception:
                pass
            return False, f"close_timeout_after_{float(timeout_sec):.1f}s"
        out = (out_b or b"").decode("utf-8", errors="replace")
        err = (err_b or b"").decode("utf-8", errors="replace")
        ok = proc.returncode == 0
        msg = (out + "\n" + err).strip()
        return ok, msg[-4000:]
    except Exception as e:
        return False, str(e)


async def _run_loop_impl(
    *,
    db_path: str,
    runtime_overrides: Optional[Dict[str, Any]] = None,
) -> int:
    cfg = _load_runtime_config(
        _skill_exit_decider_cfg(),
        runtime_overrides=runtime_overrides,
    )
    cfg.state_path.parent.mkdir(parents=True, exist_ok=True)
    lock_fh = _acquire_single_instance_lock(cfg.lock_path)
    if lock_fh is None:
        LOG.error("exit decider already running (lock busy): %s", cfg.lock_path)
        return 1
    last_id = _load_cursor_state(cfg.state_path)

    db = AITraderDB(db_path)

    try:
        while True:
            last_id = await _run_loop_once(
                db_path=db_path,
                db=db,
                cfg=cfg,
                last_id=last_id,
            )
    finally:
        _release_single_instance_lock(lock_fh)


async def run_loop(
    *,
    db_path: str,
    runtime_overrides: Optional[Dict[str, Any]] = None,
) -> int:
    """Thin wrapper for decider loop entrypoint."""
    return await _run_loop_impl(db_path=db_path, runtime_overrides=runtime_overrides)


def main() -> int:
    parser = argparse.ArgumentParser(description="Exit decider via OpenClaw agent")
    parser.add_argument("--db-path", default=DEFAULT_DB)
    args = parser.parse_args()
    try:
        return asyncio.run(run_loop(db_path=str(args.db_path)))
    except KeyboardInterrupt:
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
