#!/usr/bin/env python3
"""Evaluate EXIT GATE decision outcomes/regret on a delayed horizon."""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional

import yaml
from dotenv import load_dotenv

from ai_trader_db import AITraderDB
from logging_utils import get_logger


SKILL_DIR = Path(__file__).parent
DEFAULT_DB = str(SKILL_DIR / "ai_trader.db")
LOG = get_logger("exit_outcome_worker")

# Keep environment handling consistent with other workers.
load_dotenv(SKILL_DIR / ".env")


def _env_int(name: str, default: int) -> int:
    raw = str((os.environ.get(name) or "")).strip()
    if not raw:
        return int(default)
    try:
        return int(float(raw))
    except Exception:
        return int(default)


def _env_float(name: str, default: float) -> float:
    raw = str((os.environ.get(name) or "")).strip()
    if not raw:
        return float(default)
    try:
        return float(raw)
    except Exception:
        return float(default)


def _safe_int(value: Any, default: int) -> int:
    try:
        return int(float(value))
    except Exception:
        return int(default)


def _safe_float(value: Any, default: float) -> float:
    try:
        return float(value)
    except Exception:
        return float(default)


def _load_skill_exit_outcome_cfg() -> Dict[str, Any]:
    cfg_file = SKILL_DIR / "skill.yaml"
    if not cfg_file.exists():
        return {}
    try:
        raw = yaml.safe_load(cfg_file.read_text(encoding="utf-8")) or {}
        if not isinstance(raw, dict):
            return {}
        cfg = (raw.get("config") or {}) if isinstance(raw, dict) else {}
        ex = (cfg.get("exit_outcome") or {}) if isinstance(cfg, dict) else {}
        return ex if isinstance(ex, dict) else {}
    except Exception:
        return {}


@dataclass(frozen=True)
class OutcomeConfig:
    interval_sec: float
    horizon_sec: int
    batch_size: int
    mark_window_sec: float
    close_regret_move_pct: float


def _estimate_mark(
    *,
    entry_price: float,
    size: float,
    direction: str,
    unrealized_pnl: float,
) -> Optional[float]:
    if entry_price <= 0 or size <= 0:
        return None
    d = str(direction or "").upper()
    if d == "LONG":
        return float(entry_price) + (float(unrealized_pnl) / float(size))
    if d == "SHORT":
        return float(entry_price) - (float(unrealized_pnl) / float(size))
    return None


def _extract_plan_mark(detail: str) -> Optional[float]:
    raw = str(detail or "")
    if not raw:
        return None
    marker = "plan_detail="
    idx = raw.find(marker)
    if idx < 0:
        return None
    payload = raw[idx + len(marker) :].strip()
    if not payload:
        return None
    try:
        obj = json.loads(payload)
    except Exception:
        return None
    if not isinstance(obj, dict):
        return None
    try:
        val = obj.get("mark")
        return float(val) if val is not None else None
    except Exception:
        return None


def _lookup_mark_near(
    *,
    db_path: str,
    symbol: str,
    venue: str,
    ts: float,
    window_sec: float,
    fallback_direction: str,
) -> Optional[float]:
    sym = str(symbol or "").upper()
    ven = str(venue or "").lower()
    if not sym:
        return None
    start = float(ts) - max(1.0, float(window_sec))
    end = float(ts) + max(1.0, float(window_sec))
    q = """
        SELECT
            ms.ts,
            mp.entry_price,
            mp.unrealized_pnl,
            mp.size,
            mp.direction
        FROM monitor_positions mp
        JOIN monitor_snapshots ms ON ms.id = mp.snapshot_id
        WHERE UPPER(COALESCE(mp.symbol,'')) = ?
          AND ms.ts BETWEEN ? AND ?
          AND (
            LOWER(COALESCE(mp.venue,'')) = ?
            OR LOWER(COALESCE(mp.source,'')) = ?
          )
        ORDER BY ABS(ms.ts - ?) ASC
        LIMIT 1
    """
    fallback_q = """
        SELECT
            ms.ts,
            mp.entry_price,
            mp.unrealized_pnl,
            mp.size,
            mp.direction
        FROM monitor_positions mp
        JOIN monitor_snapshots ms ON ms.id = mp.snapshot_id
        WHERE UPPER(COALESCE(mp.symbol,'')) = ?
          AND ms.ts BETWEEN ? AND ?
        ORDER BY ABS(ms.ts - ?) ASC
        LIMIT 1
    """
    try:
        with sqlite3.connect(db_path, timeout=30.0) as conn:
            row = conn.execute(q, (sym, start, end, ven, ven, float(ts))).fetchone()
            if not row:
                row = conn.execute(fallback_q, (sym, start, end, float(ts))).fetchone()
            if not row:
                return None
            entry = float(row[1] or 0.0)
            unrl = float(row[2] or 0.0)
            size = float(row[3] or 0.0)
            direction = str(row[4] or fallback_direction or "").upper()
            return _estimate_mark(
                entry_price=entry,
                size=abs(size),
                direction=direction,
                unrealized_pnl=unrl,
            )
    except Exception:
        return None


def _classify_outcome(
    *,
    action: str,
    trade_exit_time: Optional[float],
    trade_exit_reason: Optional[str],
    horizon_ts: float,
    mark_move_pct: Optional[float],
    close_regret_move_pct: float,
) -> str:
    act = str(action or "").upper()
    rsn = str(trade_exit_reason or "").upper()

    if act == "HOLD":
        if trade_exit_time is not None and float(trade_exit_time) <= float(horizon_ts):
            if "SL" in rsn:
                return "HOLD_THEN_SL"
            if "TP" in rsn:
                return "HOLD_THEN_TP"
            if rsn:
                return "HOLD_THEN_EXIT"
        return "HOLD_NO_EVENT"

    if act == "CLOSE":
        if mark_move_pct is None:
            return "CLOSE_NO_MARK_DATA"
        thr = abs(float(close_regret_move_pct))
        if mark_move_pct >= thr:
            return "CLOSE_THEN_FAVORABLE_MOVE"
        if mark_move_pct <= -thr:
            return "CLOSE_THEN_ADVERSE_MOVE"
        return "CLOSE_THEN_FLAT"

    return "UNKNOWN"


def evaluate_once(*, db: AITraderDB, db_path: str, cfg: OutcomeConfig) -> Dict[str, int]:
    now_ts = time.time()
    rows = db.fetch_unevaluated_exit_decisions(
        horizon_sec=int(cfg.horizon_sec),
        as_of_ts=float(now_ts),
        limit=int(cfg.batch_size),
    )
    counts = {"candidates": len(rows), "recorded": 0, "errors": 0}
    if not rows:
        return counts

    for row in rows:
        try:
            decision_ts = float(row.get("decision_ts") or 0.0)
            horizon_ts = float(decision_ts) + float(cfg.horizon_sec)
            symbol = str(row.get("symbol") or "").upper()
            venue = str(row.get("venue") or "").lower()
            action = str(row.get("action") or "").upper()
            direction = str(row.get("trade_direction") or "").upper()

            decision_mark = _extract_plan_mark(str(row.get("detail") or ""))
            if decision_mark is None:
                decision_mark = _lookup_mark_near(
                    db_path=db_path,
                    symbol=symbol,
                    venue=venue,
                    ts=decision_ts,
                    window_sec=cfg.mark_window_sec,
                    fallback_direction=direction,
                )

            horizon_mark = _lookup_mark_near(
                db_path=db_path,
                symbol=symbol,
                venue=venue,
                ts=horizon_ts,
                window_sec=cfg.mark_window_sec,
                fallback_direction=direction,
            )

            mark_move_pct = None
            if decision_mark and decision_mark > 0 and horizon_mark is not None:
                if direction == "SHORT":
                    mark_move_pct = ((float(decision_mark) - float(horizon_mark)) / float(decision_mark)) * 100.0
                else:
                    mark_move_pct = ((float(horizon_mark) - float(decision_mark)) / float(decision_mark)) * 100.0

            outcome_kind = _classify_outcome(
                action=action,
                trade_exit_time=(float(row.get("trade_exit_time")) if row.get("trade_exit_time") is not None else None),
                trade_exit_reason=(str(row.get("trade_exit_reason") or "") or None),
                horizon_ts=horizon_ts,
                mark_move_pct=mark_move_pct,
                close_regret_move_pct=cfg.close_regret_move_pct,
            )

            meta = {
                "source": str(row.get("source") or ""),
                "reason": str(row.get("reason") or ""),
                "mark_window_sec": float(cfg.mark_window_sec),
                "horizon_ts": horizon_ts,
                "trade_exit_price": row.get("trade_exit_price"),
            }
            db.record_exit_decision_outcome(
                decision_rowid=int(row["decision_rowid"]),
                source_plan_id=(int(row["source_plan_id"]) if row.get("source_plan_id") is not None else None),
                trade_id=int(row["trade_id"]),
                symbol=symbol,
                venue=venue,
                action=action,
                outcome_kind=outcome_kind,
                horizon_sec=int(cfg.horizon_sec),
                decision_ts=decision_ts,
                evaluated_ts=now_ts,
                decision_mark=decision_mark,
                horizon_mark=horizon_mark,
                mark_move_pct=mark_move_pct,
                meta_json=json.dumps(meta, separators=(",", ":"), sort_keys=True),
            )
            counts["recorded"] += 1
        except Exception as exc:
            counts["errors"] += 1
            LOG.warning("Outcome evaluation failed for row=%s: %s", row, exc)
    return counts


def build_parser() -> argparse.ArgumentParser:
    exit_outcome_cfg = _load_skill_exit_outcome_cfg()
    p = argparse.ArgumentParser(description="Evaluate delayed EXIT GATE outcomes")
    p.add_argument("--db-path", default=DEFAULT_DB)
    p.add_argument("--once", action="store_true")
    p.add_argument(
        "--interval",
        type=float,
        default=float(_env_float("EVCLAW_EXIT_OUTCOME_INTERVAL_SEC", 60.0)),
    )
    p.add_argument(
        "--horizon-sec",
        type=int,
        default=_safe_int(exit_outcome_cfg.get("regret_horizon_sec"), 7200),
    )
    p.add_argument(
        "--batch-size",
        type=int,
        default=int(_env_int("EVCLAW_EXIT_OUTCOME_BATCH_SIZE", 200)),
    )
    p.add_argument(
        "--mark-window-sec",
        type=float,
        default=float(_env_float("EVCLAW_EXIT_MARK_LOOKUP_WINDOW_SEC", 1800.0)),
    )
    p.add_argument(
        "--close-regret-move-pct",
        type=float,
        default=_safe_float(exit_outcome_cfg.get("close_regret_move_pct"), 0.8),
    )
    return p


def main() -> int:
    args = build_parser().parse_args()
    cfg = OutcomeConfig(
        interval_sec=max(1.0, float(args.interval)),
        horizon_sec=max(60, int(args.horizon_sec)),
        batch_size=max(1, int(args.batch_size)),
        mark_window_sec=max(60.0, float(args.mark_window_sec)),
        close_regret_move_pct=abs(float(args.close_regret_move_pct)),
    )
    db_path = str(args.db_path or DEFAULT_DB)
    db = AITraderDB(db_path)

    while True:
        stats = evaluate_once(db=db, db_path=db_path, cfg=cfg)
        LOG.info(
            "exit outcome eval: candidates=%s recorded=%s errors=%s horizon=%ss",
            stats["candidates"],
            stats["recorded"],
            stats["errors"],
            cfg.horizon_sec,
        )
        if args.once:
            break
        time.sleep(cfg.interval_sec)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
