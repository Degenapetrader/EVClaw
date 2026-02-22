#!/usr/bin/env python3
"""Learning reflector worker.

Goal
----
Generate:
1) Per-trade reflections (reflections_v2)
2) Per-symbol rolling conclusions (symbol_conclusions_v1) that evolve A→B over time

This is intentionally OUTSIDE entry/exit AGI flow. It processes a DB queue
(reflection_tasks_v1) and calls an OpenClaw agent (hl-learning-reflector).

Design constraints
------------------
- Best-effort: never blocks trading.
- Idempotent: tasks are keyed by trade_id.
- Stable reflection ids: reflections_v2 is updated in-place (no DELETE/REPLACE).
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import re
import sqlite3
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from ai_trader_db import AITraderDB
from logging_utils import get_logger
from openclaw_agent_client import openclaw_agent_turn, safe_json_loads
from env_utils import EVCLAW_DB_PATH, env_float as _compat_env_float, env_str as _compat_env_str

LOG = get_logger("learning_reflector")


def _env_float(name: str, default: float) -> float:
    return _compat_env_float(name, default)


def _normalize_agent_id(value: Optional[str]) -> Optional[str]:
    raw = str(value or "").strip()
    if not raw:
        return None
    if raw.lower() in {"default", "openclaw-default"}:
        return "default"
    return raw


DEFAULT_AGENT_ID = (
    _normalize_agent_id(
        _compat_env_str("EVCLAW_LEARNING_REFLECTOR_AGENT_ID", "evclaw-learning-reflector")
        or "evclaw-learning-reflector"
    )
)
DEFAULT_THINKING = (
    _compat_env_str("EVCLAW_LEARNING_REFLECTOR_THINKING", "medium")
    or "medium"
).strip() or "medium"
DEFAULT_DB = EVCLAW_DB_PATH
DEFAULT_STALE_RUNNING_SEC = _env_float("EVCLAW_REFLECTION_STALE_RUNNING_SEC", 600.0)
DEFAULT_RETRY_BACKOFF_SEC = _env_float("EVCLAW_REFLECTION_RETRY_BACKOFF_SEC", 30.0)
_ABSOLUTE_PATTERN = re.compile(r"\b(permanent(?:ly)?|never|always|ban(?:ned)?|absolute)\b", re.IGNORECASE)


def _as_float(x: Any, default: Optional[float] = None) -> Optional[float]:
    try:
        if x is None:
            return default
        return float(x)
    except Exception:
        return default


def _cap(s: str, n: int) -> str:
    txt = str(s or "")
    if len(txt) <= n:
        return txt
    return txt[: max(0, n - 1)] + "…"


def _utc(ts: Optional[float]) -> Optional[str]:
    if ts is None:
        return None
    try:
        return datetime.fromtimestamp(float(ts), tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return None


def _safe_json(x: Any, default: Any) -> Any:
    if x is None:
        return default


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return int(default)
    try:
        return int(str(raw).strip())
    except Exception:
        return int(default)


def _conclusion_min_trades() -> int:
    return max(1, _env_int("EVCLAW_CONCLUSION_MIN_TRADES", 20))


def _conclusion_max_adjust() -> float:
    return max(0.0, min(1.0, _env_float("EVCLAW_CONCLUSION_MAX_DIRECTIONAL_ADJUST", 0.30)))


def _conclusion_max_conditions() -> int:
    return max(0, _env_int("EVCLAW_CONCLUSION_MAX_CONDITIONAL_RULES", 2))


def _sanitize_conclusion_text(text: Any, *, max_chars: int = 1200) -> str:
    t = str(text or "").strip()
    if not t:
        return ""
    t = _ABSOLUTE_PATTERN.sub("cautious", t)
    t = " ".join(t.split())
    return _cap(t, max_chars)


def _normalize_conditions(raw: Any, *, max_conditions: int) -> List[str]:
    out: List[str] = []
    if not isinstance(raw, list):
        return out
    seen = set()
    for item in raw:
        txt = str(item or "").strip()
        if not txt:
            continue
        txt = _sanitize_conclusion_text(txt, max_chars=140)
        key = txt.lower()
        if not txt or key in seen:
            continue
        seen.add(key)
        out.append(txt)
        if len(out) >= max_conditions:
            break
    return out


def _fetch_symbol_closed_trades(db_path: str, symbol: str) -> int:
    sym = str(symbol or "").strip().upper()
    if not sym:
        return 0
    conn = sqlite3.connect(db_path, timeout=30.0)
    conn.row_factory = sqlite3.Row
    try:
        try:
            row = conn.execute(
                "SELECT n_closed FROM symbol_learning_state WHERE symbol = ? LIMIT 1",
                (sym,),
            ).fetchone()
            if row is not None:
                try:
                    n_closed = int(row["n_closed"] or 0)
                    if n_closed > 0:
                        return n_closed
                except Exception:
                    pass
        except Exception:
            pass
        row2 = conn.execute(
            "SELECT COUNT(*) AS n FROM trades WHERE symbol = ? AND exit_time IS NOT NULL",
            (sym,),
        ).fetchone()
        return int(row2["n"] or 0) if row2 else 0
    finally:
        conn.close()


def _normalize_symbol_conclusion(
    *,
    symbol: str,
    raw: Dict[str, Any],
    sample_size_seen: int,
    min_trades: int,
    max_adjust: float,
    max_conditions: int,
) -> Tuple[str, float, str]:
    data = dict(raw or {})
    conf = _as_float(data.get("confidence"), default=0.0)
    if conf is None:
        conf = 0.0
    conf = max(0.0, min(1.0, float(conf)))

    j = data.get("conclusion_json")
    j_obj = dict(j) if isinstance(j, dict) else {}

    long_adj = _as_float(j_obj.get("long_adjustment"), default=0.0)
    short_adj = _as_float(j_obj.get("short_adjustment"), default=0.0)
    long_adj = 0.0 if long_adj is None else float(long_adj)
    short_adj = 0.0 if short_adj is None else float(short_adj)
    long_adj = max(-max_adjust, min(max_adjust, long_adj))
    short_adj = max(-max_adjust, min(max_adjust, short_adj))

    if int(sample_size_seen) < int(min_trades):
        soft_cap = min(0.05, max_adjust)
        long_adj = max(-soft_cap, min(soft_cap, long_adj))
        short_adj = max(-soft_cap, min(soft_cap, short_adj))
        conf = min(conf, 0.35)

    conditions = _normalize_conditions(j_obj.get("conditions"), max_conditions=max_conditions)
    notes = _normalize_conditions(j_obj.get("notes"), max_conditions=3)
    bias = _sanitize_conclusion_text(j_obj.get("bias"), max_chars=100)

    safe_text = _sanitize_conclusion_text(data.get("conclusion_text"), max_chars=320)
    if not safe_text:
        safe_text = (
            f"{str(symbol or '').upper()} advisory prior: long_adj={long_adj:+.2f}, "
            f"short_adj={short_adj:+.2f}, n={int(sample_size_seen)}."
        )
    if int(sample_size_seen) < int(min_trades):
        safe_text += f" Low sample (n<{int(min_trades)}); keep near-neutral."
    safe_text = _cap(" ".join(safe_text.split()), 1200)

    normalized_json = {
        "version": "symbol_conclusion_v2",
        "sample_size_seen": int(sample_size_seen),
        "min_trades_required": int(min_trades),
        "confidence": float(conf),
        "long_adjustment": float(round(long_adj, 4)),
        "short_adjustment": float(round(short_adj, 4)),
        "max_directional_adjust": float(round(max_adjust, 4)),
        "conditions": conditions,
        "bias": bias,
        "notes": notes,
    }
    return safe_text, float(conf), json.dumps(normalized_json, separators=(",", ":"), ensure_ascii=False)
    if isinstance(x, (dict, list)):
        return x
    try:
        return json.loads(str(x))
    except Exception:
        return default


def fetch_trade_context(db_path: str, trade_id: int) -> Optional[Dict[str, Any]]:
    """Return enriched trade context dict for prompting."""
    tid = int(trade_id)
    conn = sqlite3.connect(db_path, timeout=30.0)
    conn.row_factory = sqlite3.Row
    try:
        row = conn.execute("SELECT * FROM trades WHERE id = ? LIMIT 1", (tid,)).fetchone()
        if not row:
            return None
        t = dict(row)

        entry_ts = _as_float(t.get("entry_time"))
        exit_ts = _as_float(t.get("exit_time"))
        hold_min = None
        if entry_ts and exit_ts and exit_ts > entry_ts:
            hold_min = (exit_ts - entry_ts) / 60.0

        # Parse snapshots (best-effort)
        t["signals_snapshot"] = _safe_json(t.get("signals_snapshot"), {})
        t["context_snapshot"] = _safe_json(t.get("context_snapshot"), {})
        t["protection_snapshot"] = _safe_json(t.get("protection_snapshot"), {})

        # Attach human-friendly timestamps
        t["entry_utc"] = _utc(entry_ts)
        t["exit_utc"] = _utc(exit_ts)
        if hold_min is not None:
            t["holding_minutes"] = round(float(hold_min), 2)

        # Add latest monitor snapshot (coarse global context)
        try:
            snap = conn.execute(
                "SELECT ts_iso, hl_net_notional, hl_equity, hl_unrealized, hip3_equity, hip3_unrealized "
                "FROM monitor_snapshots ORDER BY ts DESC LIMIT 1"
            ).fetchone()
            if snap:
                t["monitor_snapshot"] = dict(snap)
        except Exception:
            pass

        return t
    finally:
        conn.close()


def build_reflection_prompt(trade: Dict[str, Any]) -> str:
    """Build strict JSON-in/JSON-out prompt for per-trade reflection."""
    # Keep payload compact but information-rich.
    payload = {
        "task": "trade_reflection_v2",
        "rules": {
            "output_format": "json_only",
            "no_markdown": True,
            "lesson_text_max_chars": 220,
            "confidence_range": [0.0, 1.0],
        },
        "trade": {
            "trade_id": trade.get("id"),
            "symbol": trade.get("symbol"),
            "venue": trade.get("venue"),
            "direction": trade.get("direction"),
            "entry_utc": trade.get("entry_utc"),
            "exit_utc": trade.get("exit_utc"),
            "holding_minutes": trade.get("holding_minutes"),
            "entry_price": trade.get("entry_price"),
            "exit_price": trade.get("exit_price"),
            "exit_reason": trade.get("exit_reason"),
            "realized_pnl_usd": trade.get("realized_pnl"),
            "realized_pnl_pct": trade.get("realized_pnl_pct"),
            "total_fees": trade.get("total_fees"),
            "notional_usd": trade.get("notional_usd"),
            "risk_pct_used": trade.get("risk_pct_used"),
            "equity_at_entry": trade.get("equity_at_entry"),
            "mae_pct": trade.get("mae_pct"),
            "mfe_pct": trade.get("mfe_pct"),
            "sl_price": trade.get("sl_price"),
            "tp_price": trade.get("tp_price"),
            "confidence_at_entry": trade.get("confidence"),
            "strategy": trade.get("strategy"),
        },
        "signals_snapshot": trade.get("signals_snapshot") or {},
        "context_snapshot": trade.get("context_snapshot") or {},
        "protection_snapshot": trade.get("protection_snapshot") or {},
        "monitor_snapshot": trade.get("monitor_snapshot") or {},
    }

    schema = {
        "lesson_text": "string (1-2 sentences; actionable; no fluff)",
        "confidence": "float 0..1",
        "reflection_json": {
            "tags": ["string"],
            "what_worked": "string",
            "what_failed": "string",
            "next_time": ["string"],
            "sizing_note": "string",
        },
    }

    return (
        "You are the EVClaw learning reflector.\n"
        "Write ONE concise lesson for this closed trade.\n"
        "Return ONLY valid JSON matching this schema (no markdown, no commentary):\n"
        + json.dumps(schema, separators=(",", ":"), ensure_ascii=False)
        + "\nINPUT:\n"
        + json.dumps(payload, separators=(",", ":"), ensure_ascii=False)
    )


def build_symbol_conclusion_prompt(
    *,
    symbol: str,
    previous: Optional[Dict[str, Any]],
    new_lessons: List[Dict[str, Any]],
) -> str:
    prev_payload = previous or {}
    payload = {
        "task": "symbol_conclusion_update_v1",
        "symbol": symbol,
        "previous": {
            "conclusion_text": prev_payload.get("conclusion_text"),
            "conclusion_json": _safe_json(prev_payload.get("conclusion_json"), {}),
            "confidence": prev_payload.get("confidence"),
            "last_reflection_id_seen": prev_payload.get("last_reflection_id_seen", 0),
        },
        "new_lessons": new_lessons,
        "rules": {
            "output_format": "json_only",
            "no_markdown": True,
            "conclusion_text_max_chars": 300,
            "confidence_range": [0.0, 1.0],
            "no_absolute_language": True,
            "max_directional_adjust_abs": _conclusion_max_adjust(),
            "max_conditions": _conclusion_max_conditions(),
            "advisory_only": True,
        },
    }

    schema = {
        "conclusion_text": "string (compact; probabilistic; advisory-only; avoid 'never/always/permanent/ban')",
        "confidence": "float 0..1",
        "conclusion_json": {
            "long_adjustment": "float in [-0.30, +0.30]",
            "short_adjustment": "float in [-0.30, +0.30]",
            "sample_size_seen": "int",
            "conditions": ["string (broad condition only; max 2)"],
            "bias": "string (advisory bias; optional)",
            "notes": ["string"],
        },
    }

    return (
        "You maintain a rolling per-symbol trading conclusion that evolves as new lessons arrive.\n"
        "Update the conclusion for this symbol using ONLY the new lessons (plus previous conclusion).\n"
        "Output advisory priors only; do not output hard bans or absolute prohibitions.\n"
        "Use probabilistic language and bounded directional adjustments, not rules like NEVER/PERMANENTLY.\n"
        "Return ONLY valid JSON matching this schema (no markdown, no commentary):\n"
        + json.dumps(schema, separators=(",", ":"), ensure_ascii=False)
        + "\nINPUT:\n"
        + json.dumps(payload, separators=(",", ":"), ensure_ascii=False)
    )


def _fetch_new_symbol_lessons(
    db_path: str,
    *,
    symbol: str,
    last_seen_id: int,
    limit: int = 50,
) -> List[Dict[str, Any]]:
    sym = str(symbol or "").strip().upper()
    if not sym:
        return []
    conn = sqlite3.connect(db_path, timeout=30.0)
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            """
            SELECT
                r.id AS reflection_id,
                r.trade_id,
                r.lesson_text,
                r.confidence,
                r.created_at,
                t.realized_pnl_pct,
                t.exit_reason,
                t.direction
            FROM reflections_v2 r
            JOIN trades t ON t.id = r.trade_id
            WHERE t.symbol = ?
              AND r.id > ?
            ORDER BY r.id ASC
            LIMIT ?
            """,
            (sym, int(last_seen_id or 0), int(limit)),
        ).fetchall()

        out: List[Dict[str, Any]] = []
        for row in rows:
            d = dict(row)
            d["lesson_text"] = _cap(str(d.get("lesson_text") or ""), 400)
            out.append(d)
        return out
    finally:
        conn.close()


async def _call_agent_json(
    *,
    agent_id: Optional[str],
    thinking: str,
    session_id: str,
    message: str,
    timeout_sec: float,
) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    meta, txt = await openclaw_agent_turn(
        agent_id=agent_id,
        model=None,
        thinking=thinking,
        message=message,
        session_id=str(session_id),
        timeout_sec=timeout_sec,
        openclaw_cmd=os.getenv("OPENCLAW_CMD") or "openclaw",
    )
    parsed = safe_json_loads(txt)
    if not isinstance(parsed, dict):
        raise ValueError("agent did not return JSON object")
    return meta or {}, parsed


def _reflector_timeout_sec() -> float:
    """Timeout used for LLM reflector calls (reflections + conclusions)."""
    raw = _compat_env_str("EVCLAW_LEARNING_REFLECTOR_TIMEOUT_SEC")
    try:
        value = float(raw) if raw is not None else 120.0
    except Exception:
        value = 120.0
    return max(1.0, value)


async def process_one_task(
    *,
    db: AITraderDB,
    db_path: str,
    worker_id: str,
    agent_id: Optional[str],
    thinking: str,
    max_attempts: int,
    stale_running_sec: float,
    retry_backoff_sec: float,
) -> bool:
    trade_id = db.claim_reflection_task(
        worker_id=worker_id,
        max_attempts=max_attempts,
        include_error=True,
        stale_running_sec=stale_running_sec,
        error_backoff_sec=retry_backoff_sec,
    )
    if not trade_id:
        return False

    try:
        trade = fetch_trade_context(db_path, int(trade_id))
        if not trade:
            raise RuntimeError(f"trade_id {trade_id} not found")

        prompt = build_reflection_prompt(trade)
        _meta, out = await _call_agent_json(
            agent_id=agent_id,
            thinking=thinking,
            # OpenClaw session IDs must avoid ':' characters.
            session_id=f"reflect_{trade_id}_{int(time.time()*1000)}",
            message=prompt,
            timeout_sec=_reflector_timeout_sec(),
        )

        lesson_text = str(out.get("lesson_text") or "").strip()
        if not lesson_text:
            lesson_text = f"{trade.get('symbol')} {trade.get('direction')} closed: add a lesson here.".strip()
        lesson_text = _cap(lesson_text, 800)

        conf = _as_float(out.get("confidence"), default=None)
        if conf is not None:
            conf = max(0.0, min(1.0, float(conf)))

        reflection_json_obj = out.get("reflection_json")
        reflection_json = None
        if isinstance(reflection_json_obj, (dict, list)):
            reflection_json = json.dumps(reflection_json_obj, separators=(",", ":"), ensure_ascii=False)
        elif reflection_json_obj is not None:
            reflection_json = _cap(str(reflection_json_obj), 4000)

        db.upsert_reflection_v2(
            trade_id=int(trade_id),
            lesson_text=lesson_text,
            reflection_json=reflection_json,
            confidence=conf,
            created_at=time.time(),
        )

        # Merge new reflection snippets into symbol_learning_state.notes_summary
        try:
            from learning_dossier_aggregator import update_from_new_reflections

            sym = str(trade.get("symbol") or "").strip().upper()
            if sym:
                update_from_new_reflections(db_path, sym)
        except Exception as exc:
            LOG.warning("notes_summary merge failed for trade %s: %s", trade_id, exc)

        # Update per-symbol conclusion incrementally
        sym = str(trade.get("symbol") or "").strip().upper()
        if sym:
            prev = db.get_symbol_conclusion_row(sym) or {}
            last_seen = int(prev.get("last_reflection_id_seen") or 0)
            new_lessons = _fetch_new_symbol_lessons(db_path, symbol=sym, last_seen_id=last_seen)
            if new_lessons:
                concl_prompt = build_symbol_conclusion_prompt(symbol=sym, previous=prev, new_lessons=new_lessons)
                _m2, concl = await _call_agent_json(
                    agent_id=agent_id,
                    thinking=thinking,
                    # OpenClaw session IDs must avoid ':' characters.
                    session_id=f"concl_{sym}_{int(time.time()*1000)}",
                    message=concl_prompt,
                    timeout_sec=_reflector_timeout_sec(),
                )

                sample_size_seen = _fetch_symbol_closed_trades(db_path, sym)
                conclusion_text, c_conf, concl_json = _normalize_symbol_conclusion(
                    symbol=sym,
                    raw=concl,
                    sample_size_seen=sample_size_seen,
                    min_trades=_conclusion_min_trades(),
                    max_adjust=_conclusion_max_adjust(),
                    max_conditions=_conclusion_max_conditions(),
                )

                last_id = int(new_lessons[-1].get("reflection_id") or last_seen)
                db.upsert_symbol_conclusion(
                    symbol=sym,
                    conclusion_text=conclusion_text,
                    conclusion_json=concl_json,
                    confidence=c_conf,
                    last_reflection_id_seen=last_id,
                )

        db.mark_reflection_task_done(trade_id=int(trade_id))
        LOG.info("reflection done trade_id=%s symbol=%s", trade_id, sym)
        return True

    except Exception as exc:
        LOG.warning("reflection task failed trade_id=%s: %s", trade_id, exc)
        try:
            db.mark_reflection_task_error(trade_id=int(trade_id), error=str(exc))
        except Exception:
            pass
        return True


async def main_async() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db-path", default=DEFAULT_DB or str(os.path.join(os.path.dirname(__file__), "ai_trader.db")))
    ap.add_argument("--agent-id", default=DEFAULT_AGENT_ID)
    ap.add_argument("--thinking", default=DEFAULT_THINKING)
    ap.add_argument("--sleep", type=float, default=2.0)
    ap.add_argument("--max-attempts", type=int, default=5)
    ap.add_argument("--stale-running-sec", type=float, default=DEFAULT_STALE_RUNNING_SEC)
    ap.add_argument("--retry-backoff-sec", type=float, default=DEFAULT_RETRY_BACKOFF_SEC)
    ap.add_argument("--once", action="store_true")
    args = ap.parse_args()

    db_path = str(args.db_path)
    agent_id = _normalize_agent_id(args.agent_id)
    thinking = str(args.thinking or "").strip() or "medium"
    worker_id = f"learning_reflector:{os.uname().nodename}:{os.getpid()}"

    db = AITraderDB(db_path)

    LOG.info("starting worker db=%s agent_id=%s thinking=%s", db_path, agent_id or "default", thinking)

    while True:
        did = await process_one_task(
            db=db,
            db_path=db_path,
            worker_id=worker_id,
            agent_id=agent_id,
            thinking=thinking,
            max_attempts=int(args.max_attempts),
            stale_running_sec=max(0.0, float(args.stale_running_sec)),
            retry_backoff_sec=max(0.0, float(args.retry_backoff_sec)),
        )
        if args.once:
            return 0
        if not did:
            await asyncio.sleep(max(0.1, float(args.sleep)))


def main() -> int:
    import asyncio

    try:
        return asyncio.run(main_async())
    except KeyboardInterrupt:
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
