#!/usr/bin/env python3
"""
Decay worker for EVClaw.

Producer-only worker:
- evaluates open positions and emits DECAY_EXIT plans
- never executes close orders directly
- execution gate is centralized in llm_exit_decider.py
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from threading import Lock
from typing import Dict, Optional, Tuple, Any

from ai_trader_db import AITraderDB, TradeRecord
from cli import build_execution_config, load_config
from executor import Executor
from risk_manager import RiskConfig
from context_runtime import load_latest_context_path
from env_utils import EVCLAW_DB_PATH, EVCLAW_RUNTIME_DIR

logger = logging.getLogger(__name__)


SKILL_DIR = Path(__file__).parent
DEFAULT_DB = EVCLAW_DB_PATH


def _safe_float(value: Any, default: float) -> float:
    try:
        out = float(value)
    except Exception:
        return float(default)
    if out != out:
        return float(default)
    return out


DEFAULT_INTERVAL_SECONDS = 60.0
DEFAULT_CONTEXT_DIR = Path(EVCLAW_RUNTIME_DIR)
TREND_FLIP_SCORE_SHORT = -40.0
TREND_FLIP_SCORE_LONG = 40.0
CONTEXT_MISS_ALERT_COUNT = 3
CONTEXT_MISS_ALERT_COOLDOWN_SEC = 300.0
DEFAULT_NOTIFY_COOLDOWN_SECONDS = 3600.0

_CONTEXT_MISS_COUNT = 0
_CONTEXT_LAST_ALERT_TS = 0.0
_CONTEXT_MISS_LOCK = Lock()


@dataclass
class ContextSnapshot:
    path: Optional[str]
    generated_at: Optional[float]
    age_seconds: Optional[float]
    symbol_directions: Dict[str, str]
    symbol_trends: Dict[str, Dict[str, Any]] = None  # {symbol: {score, direction, regime}}
    
    def __post_init__(self):
        if self.symbol_trends is None:
            self.symbol_trends = {}


def _parse_iso_ts(value: Optional[str]) -> Optional[float]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value).timestamp()
    except Exception:
        return None


def load_latest_context_json(context_dir: Path = DEFAULT_CONTEXT_DIR) -> Optional[Path]:
    return load_latest_context_path(context_dir)


def parse_context(path: Optional[Path]) -> ContextSnapshot:
    if not path or not path.exists():
        return ContextSnapshot(path=None, generated_at=None, age_seconds=None, symbol_directions={})

    try:
        payload = json.loads(path.read_text())
    except Exception as e:
        logger.warning(f"Failed to parse context file {path}: {e}")
        return ContextSnapshot(path=str(path), generated_at=None, age_seconds=None, symbol_directions={})

    generated_at = _parse_iso_ts(payload.get("generated_at"))
    now = time.time()
    if generated_at is None:
        generated_at = path.stat().st_mtime
    age_seconds = max(0.0, now - generated_at)

    directions: Dict[str, str] = {}

    # Preferred: explicit symbol_directions map (covers all open positions).
    symbol_map = payload.get("symbol_directions")
    if isinstance(symbol_map, dict) and symbol_map:
        for k, v in symbol_map.items():
            sym = str(k or "").upper()
            if not sym:
                continue
            dir_v = str(v or "").upper()
            if dir_v:
                directions[sym] = dir_v
    else:
        # Fallback: only directions for top opportunities (legacy behavior).
        opps = payload.get("top_opportunities")
        if not isinstance(opps, list) or not opps:
            opps = payload.get("selected_opportunities")
        opps = opps or []

        for opp in opps:
            symbol = str(opp.get("symbol") or "").upper()
            if not symbol:
                continue
            direction = str(opp.get("direction") or "").upper()
            if direction:
                directions[symbol] = direction

    # Extract symbol_trends for trend flip detection
    symbol_trends: Dict[str, Dict[str, Any]] = {}
    trends_map = payload.get("symbol_trends")
    if isinstance(trends_map, dict):
        for sym, trend_data in trends_map.items():
            if isinstance(trend_data, dict):
                symbol_trends[sym.upper()] = trend_data
    
    return ContextSnapshot(
        path=str(path),
        generated_at=generated_at,
        age_seconds=age_seconds,
        symbol_directions=directions,
        symbol_trends=symbol_trends,
    )


def build_risk_config(config: Dict) -> RiskConfig:
    risk_cfg = RiskConfig()
    risk = (config.get("config") or {}).get("risk", {}) if isinstance(config, dict) else {}
    for field in risk_cfg.__dataclass_fields__:
        if field in risk:
            try:
                setattr(risk_cfg, field, type(getattr(risk_cfg, field))(risk[field]))
            except Exception:
                setattr(risk_cfg, field, risk[field])
    return risk_cfg


def should_close_trade(
    trade: TradeRecord,
    risk_cfg: RiskConfig,
    context: ContextSnapshot,
    now_ts: float,
    *,
    current_direction: Optional[str] = None,
    signal_flip_only: bool = False,
) -> Tuple[bool, str, str]:
    state = (trade.state or "").upper()
    if state in {"DUST", "EXITING"}:
        return False, "SKIP_STATE", state

    hold_hours = (now_ts - float(trade.entry_time)) / 3600.0
    if hold_hours < risk_cfg.min_hold_hours:
        return False, "MIN_HOLD", f"{hold_hours:.2f}h"

    ctx_dir = None
    if context:
        ctx_dir = context.symbol_directions.get(trade.symbol.upper())

    effective_dir = (current_direction or trade.direction or "").upper()

    # User-requested mode: only close on explicit signal flips (includes trend flips).
    if signal_flip_only:
        # Check perp signal flip
        if ctx_dir in {"LONG", "SHORT"} and ctx_dir != effective_dir:
            db_dir = (trade.direction or "").upper()
            suffix = f" (db={db_dir})" if db_dir and db_dir != effective_dir else ""
            return True, "DECAY_EXIT", f"SIGNAL_FLIP {effective_dir}->{ctx_dir}{suffix}"
        
        # Check trend flip (trend strongly against position)
        if context and context.symbol_trends:
            trend_data = context.symbol_trends.get(trade.symbol.upper(), {})
            raw_score = trend_data.get('score')
            try:
                trend_score = float(raw_score) if raw_score is not None else None
            except Exception:
                trend_score = None
            trend_dir = str(trend_data.get('direction', '') or '').upper()

            if trend_score is not None:
                # LONG but trend strongly bearish
                if effective_dir == "LONG" and trend_score <= TREND_FLIP_SCORE_SHORT:
                    return True, "DECAY_EXIT", f"TREND_FLIP LONG but trend={trend_score:.0f} ({trend_dir})"
                # SHORT but trend strongly bullish
                if effective_dir == "SHORT" and trend_score >= TREND_FLIP_SCORE_LONG:
                    return True, "DECAY_EXIT", f"TREND_FLIP SHORT but trend=+{trend_score:.0f} ({trend_dir})"
        
        return False, "NO_DECAY", ""

    # Max hold time safety exit (<=0 disables).
    try:
        max_hold = float(getattr(risk_cfg, "max_hold_hours", 0.0) or 0.0)
    except Exception:
        max_hold = 0.0
    if max_hold > 0:
        try:
            hold_hours = (float(now_ts) - float(trade.entry_time or 0.0)) / 3600.0
        except Exception:
            hold_hours = 0.0
        if hold_hours > max_hold:
            return True, "MAX_HOLD", f"max_hold {hold_hours:.1f}h"

    if context and context.age_seconds is not None:
        age_minutes = context.age_seconds / 60.0
        if age_minutes > risk_cfg.stale_signal_max_minutes:
            return True, "DECAY_EXIT", f"stale_context {age_minutes:.1f}m"

        if ctx_dir in {"LONG", "SHORT"} and effective_dir and ctx_dir != effective_dir:
            db_dir = (trade.direction or "").upper()
            suffix = f" (db={db_dir})" if db_dir and db_dir != effective_dir else ""
            return True, "DECAY_EXIT", f"signal_flip {effective_dir}->{ctx_dir}{suffix}"

    return False, "NO_DECAY", ""


async def run_decay_once(
    executor: Executor,
    db: AITraderDB,
    risk_cfg: RiskConfig,
    dry_run: bool,
    context_path: Optional[Path],
    *,
    signal_flip_only: bool = False,
    notify_only: bool = False,
    notify_cooldown_seconds: float = 3600.0,
    notify_state: Optional[Dict[str, Dict[str, Any]]] = None,
) -> Dict[str, int]:
    # Producer-only invariant: this worker never executes closes directly.
    notify_only = True
    dry_run = True

    context = parse_context(context_path)
    now_ts = time.time()
    global _CONTEXT_MISS_COUNT, _CONTEXT_LAST_ALERT_TS

    context_missing = (
        context.age_seconds is None
        and not context.symbol_directions
        and not context.symbol_trends
    )
    if context_missing:
        should_alert = False
        miss_count = 0
        with _CONTEXT_MISS_LOCK:
            _CONTEXT_MISS_COUNT += 1
            miss_count = int(_CONTEXT_MISS_COUNT)
            if (
                miss_count >= max(1, CONTEXT_MISS_ALERT_COUNT)
                and (now_ts - _CONTEXT_LAST_ALERT_TS) >= float(CONTEXT_MISS_ALERT_COOLDOWN_SEC)
            ):
                _CONTEXT_LAST_ALERT_TS = now_ts
                should_alert = True
        if should_alert:
            logger.warning(
                f"Context missing for {miss_count} consecutive cycle(s); "
                f"path={context.path or 'none'}"
            )
    else:
        with _CONTEXT_MISS_LOCK:
            _CONTEXT_MISS_COUNT = 0

    open_trades = db.get_open_trades()
    counts = {"checked": 0, "closed": 0, "skipped": 0}

    def _notify_dedup_skip(*, symbol: str, venue: str, reason: str) -> bool:
        if notify_state is None:
            return False
        state_key = f"{venue}:{symbol}".upper()
        rec = notify_state.get(state_key) or {}
        last_ts = float(rec.get("ts", 0.0) or 0.0)
        last_reason = str(rec.get("reason", "") or "")
        now_ts2 = time.time()
        if last_reason == reason and (now_ts2 - last_ts) < float(notify_cooldown_seconds or 0.0):
            return True
        notify_state[state_key] = {"ts": now_ts2, "reason": reason}
        return False

    def _safe_json(payload: Dict[str, Any]) -> str:
        try:
            return json.dumps(payload, separators=(",", ":"), sort_keys=True)
        except Exception:
            return str(payload)

    for trade in open_trades:
        counts["checked"] += 1
        symbol = str(trade.symbol or "").upper()
        venue = str(trade.venue or "").lower()
        db_dir = str((trade.direction or "")).upper()
        ctx_dir = context.symbol_directions.get(symbol) if context else None

        try:
            truth = await executor.get_position_truth(symbol, venue=venue)
        except Exception as exc:
            logger.warning(f"get_position_truth failed for {symbol} venue={venue}: {exc}")
            counts["skipped"] += 1
            continue

        tracked_dir = str(truth.get("tracked_direction") or "").upper()
        tracked_size = float(truth.get("tracked_size") or 0.0)
        tracked_live = tracked_dir in {"LONG", "SHORT"} and tracked_size > 0.0

        exchange_confirmed = bool(truth.get("exchange_confirmed"))
        exchange_dir = str(truth.get("exchange_direction") or "").upper()
        exchange_size = float(truth.get("exchange_size") or 0.0)
        exchange_live = exchange_dir in {"LONG", "SHORT"} and exchange_size > 0.0
        confirm_error = str(truth.get("confirm_error") or "").strip() or None

        eval_dir = tracked_dir if tracked_live else db_dir
        should_close, reason, detail_why = should_close_trade(
            trade,
            risk_cfg,
            context,
            now_ts,
            current_direction=eval_dir,
            signal_flip_only=signal_flip_only,
        )

        suspect_mismatch = (
            (not tracked_live)
            or (db_dir in {"LONG", "SHORT"} and tracked_dir in {"LONG", "SHORT"} and db_dir != tracked_dir)
        )
        mismatch_confirmed = False
        direction_source = "tracked"
        confirmation_status = "not_checked"
        reconcile_reason = None

        if suspect_mismatch:
            if exchange_confirmed:
                if not exchange_live:
                    confirmation_status = "flat_on_exchange"
                    reconcile_reason = "exchange_flat_or_missing_for_open_trade"
                else:
                    eval_dir = exchange_dir
                    direction_source = "exchange"
                    confirmation_status = "confirmed"
                    mismatch_confirmed = (
                        db_dir in {"LONG", "SHORT"}
                        and exchange_dir in {"LONG", "SHORT"}
                        and db_dir != exchange_dir
                    )

                    if (
                        db_dir in {"LONG", "SHORT"}
                        and tracked_dir in {"LONG", "SHORT"}
                        and db_dir != tracked_dir
                        and not mismatch_confirmed
                    ):
                        reconcile_reason = "cache_mismatch_not_confirmed_on_exchange"
                    else:
                        should_close, reason, detail_why = should_close_trade(
                            trade,
                            risk_cfg,
                            context,
                            now_ts,
                            current_direction=eval_dir,
                            signal_flip_only=signal_flip_only,
                        )
            else:
                confirmation_status = "unconfirmed_fetch_failed"
                direction_source = "tracked_unconfirmed" if tracked_live else "db_unconfirmed"

        live_dir = eval_dir if eval_dir in {"LONG", "SHORT"} else ""
        structured_detail: Dict[str, Any] = {
            "type": "DECAY_EXIT_PLAN",
            "reason": str(reason or ""),
            "why": str(detail_why or ""),
            "symbol": symbol,
            "venue": venue,
            "trade_id": int(trade.id),
            "live_direction": live_dir or None,
            "db_direction": db_dir or None,
            "tracked_direction": tracked_dir or None,
            "tracked_size": tracked_size,
            "exchange_direction": exchange_dir or None,
            "exchange_size": exchange_size,
            "direction_source": direction_source,
            "confirmation_status": confirmation_status,
            "confirm_error": confirm_error,
            "mismatch_confirmed": bool(mismatch_confirmed),
            "context_direction": str(ctx_dir or "") if ctx_dir else None,
            "context_path": context.path,
            "context_age_seconds": context.age_seconds,
        }

        if reconcile_reason:
            structured_detail["type"] = "RECONCILE_NEEDED_PLAN"
            structured_detail["reason"] = "RECONCILE_NEEDED"
            structured_detail["why"] = reconcile_reason
            detail = _safe_json(structured_detail)

            try:
                db.record_decay_flag(
                    symbol=symbol,
                    venue=venue,
                    trade_id=trade.id,
                    db_direction=db_dir or None,
                    live_direction=live_dir or None,
                    reason="RECONCILE_NEEDED",
                    detail=detail,
                    context_path=context.path,
                    context_age_seconds=context.age_seconds,
                    context_generated_at=context.generated_at,
                    context_direction=ctx_dir,
                    signal_flip_only=signal_flip_only,
                    notify_only=True,
                )
            except Exception as exc:
                logger.warning(
                    "record_decay_flag(RECONCILE_NEEDED) failed for %s/%s trade_id=%s: %s",
                    venue,
                    symbol,
                    trade.id,
                    exc,
                )

            if _notify_dedup_skip(symbol=symbol, venue=venue, reason="RECONCILE_NEEDED"):
                counts["skipped"] += 1
                continue

            logger.warning(
                "[RECONCILE_NEEDED] %s %s venue=%s detail=%s",
                symbol,
                db_dir or "?",
                venue,
                detail,
            )
            try:
                db.record_decay_decision(
                    symbol=symbol,
                    venue=venue,
                    trade_id=trade.id,
                    action="HOLD",
                    reason="RECONCILE_NEEDED",
                    detail=detail,
                    decided_by=None,
                    source="decay_worker_notify",
                    dedupe_seconds=float(notify_cooldown_seconds or 0.0),
                )
            except Exception as exc:
                logger.warning(
                    "record_decay_decision(RECONCILE_NEEDED) failed for %s/%s trade_id=%s: %s",
                    venue,
                    symbol,
                    trade.id,
                    exc,
                )
            counts["skipped"] += 1
            continue

        detail = _safe_json(structured_detail)
        if not should_close:
            counts["skipped"] += 1
            continue

        dir_tag = live_dir if not db_dir or db_dir == live_dir else f"{live_dir} (db={db_dir})"
        msg = (
            f"Decay check: {symbol} {dir_tag} venue={venue} "
            f"reason={reason} detail={detail}"
        )

        # Always persist the flag (even in notify-only) so we can audit/learn.
        try:
            db.record_decay_flag(
                symbol=symbol,
                venue=venue,
                trade_id=trade.id,
                db_direction=db_dir or None,
                live_direction=live_dir or None,
                reason=reason,
                detail=detail,
                context_path=context.path,
                context_age_seconds=context.age_seconds,
                context_generated_at=context.generated_at,
                context_direction=ctx_dir,
                signal_flip_only=signal_flip_only,
                notify_only=bool(notify_only or dry_run),
            )
        except Exception as exc:
            logger.warning(
                "record_decay_flag failed for %s/%s trade_id=%s: %s",
                venue,
                symbol,
                trade.id,
                exc,
            )

        if _notify_dedup_skip(symbol=symbol, venue=venue, reason=reason):
            counts["skipped"] += 1
            continue

        logger.info(f"[NOTIFY_ONLY] {msg}")

        try:
            db.record_decay_decision(
                symbol=symbol,
                venue=venue,
                trade_id=trade.id,
                action="CLOSE",
                reason=reason,
                detail=detail,
                decided_by=None,
                source="decay_worker_notify",
                dedupe_seconds=float(notify_cooldown_seconds or 0.0),
            )
        except Exception as exc:
            logger.warning(
                "record_decay_decision failed for %s/%s trade_id=%s: %s",
                venue,
                symbol,
                trade.id,
                exc,
            )

        counts["skipped"] += 1
        continue

    return counts


async def run_decay_loop(args) -> int:
    config = load_config()
    cfg_root = (config.get("config") or {}) if isinstance(config, dict) else {}
    decay_cfg = (cfg_root.get("decay") or {}) if isinstance(cfg_root, dict) else {}
    global TREND_FLIP_SCORE_SHORT, TREND_FLIP_SCORE_LONG
    TREND_FLIP_SCORE_SHORT = _safe_float(decay_cfg.get("trend_flip_score_short"), -40.0)
    TREND_FLIP_SCORE_LONG = _safe_float(decay_cfg.get("trend_flip_score_long"), 40.0)

    exec_config = build_execution_config(config, dry_run=args.dry_run)
    risk_cfg = build_risk_config(config)
    risk_cfg.min_hold_hours = args.min_hold_hours if args.min_hold_hours is not None else risk_cfg.min_hold_hours
    # max_hold_hours removed by design

    db_path = args.db_path or str(exec_config.db_path or DEFAULT_DB)
    db = AITraderDB(db_path)

    executor = Executor(config=exec_config)
    await executor.initialize()

    notify_state: Dict[str, Dict[str, Any]] = {}

    try:
        while True:
            context_path = Path(args.context_json) if args.context_json else load_latest_context_json()
            # Producer-only mode: always notify/plan (never direct close).
            should_notify_only = True
            no_close = True
            counts = await run_decay_once(
                executor=executor,
                db=db,
                risk_cfg=risk_cfg,
                dry_run=no_close,
                context_path=context_path,
                signal_flip_only=args.signal_flip_only,
                notify_only=should_notify_only,
                notify_cooldown_seconds=args.notify_cooldown_seconds,
                notify_state=notify_state,
            )
            logger.info(
                f"Decay summary: checked={counts['checked']} closed={counts['closed']} skipped={counts['skipped']}"
            )
            if args.once:
                break
            await asyncio.sleep(args.interval)
    finally:
        await executor.close()

    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Decay worker (signal decay / max hold)")
    parser.add_argument("--db-path", default=None, help="Override DB path")
    parser.add_argument("--context-json", default=None, help="Explicit context JSON file")
    parser.add_argument("--interval", type=float, default=DEFAULT_INTERVAL_SECONDS, help="Loop sleep seconds")
    parser.add_argument("--once", action="store_true", help="Run a single decay pass")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Deprecated compatibility flag. Worker is producer-only and never executes closes.",
    )
    parser.add_argument(
        "--notify-only",
        action="store_true",
        help="Deprecated compatibility flag. Worker is producer-only and always emits plan notifications.",
    )
    parser.add_argument(
        "--notify-cooldown-seconds",
        type=float,
        default=DEFAULT_NOTIFY_COOLDOWN_SECONDS,
        help="In --notify-only mode, suppress repeat alerts for the same venue:symbol+detail until this cooldown expires (default 3600s)",
    )
    parser.add_argument("--min-hold-hours", type=float, default=None)
    # --max-hold-hours removed (no forced time-based exits)
    parser.add_argument(
        "--signal-flip-only",
        action="store_true",
        default=True,
        help="(default) Only close positions on explicit signal flips (includes TREND_FLIP; ignores stale-context exits)",
    )
    parser.add_argument(
        "--allow-stale-exits",
        action="store_false",
        dest="signal_flip_only",
        help="Allow non-flip decay exits (e.g., stale context / max-hold checks).",
    )
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    try:
        return asyncio.run(run_decay_loop(args))
    except KeyboardInterrupt:
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
