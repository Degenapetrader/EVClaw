#!/usr/bin/env python3
"""Hourly position review worker (inventory + exposure hygiene).

Goal
----
We open positions frequently, but decay exits can lag. This worker provides an
hourly portfolio hygiene pass:

- Only evaluates positions older than `--min-age-hours` (default 6h)
- Only evaluates positions that have NOT been recently flagged by decay
- Prioritized actions (your spec):
  1) free inventory slots (close dead/flat)
  2) cut deadly wrong loser (near stop / strong negative drift)
  3) reduce net exposure if abs(net_notional) > equity * `--max-net-exposure-mult`

Producer-only worker:
- records plan/audit decisions in `decay_decisions`
- does not emit system events (llm_exit_decider polls DB)
- never executes close orders directly
- execution gate is centralized in llm_exit_decider.py

This worker is intentionally heuristic + deterministic. Learning/lessons are
handled via the fill reconciler (which emits reflection-needed events
on trade close) and recorded in reflections_v2 by the main agent.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from venues import normalize_venue
from context_runtime import load_latest_context_payload
from env_utils import EVCLAW_RUNTIME_DIR

RUNTIME_DIR = Path(EVCLAW_RUNTIME_DIR)


def _load_latest_cycle_context() -> Dict[str, Any]:
    """Load the latest cycle context JSON produced by cycle_trigger.

    Best-effort: used only as *priority context* (SR + signals) for hourly review.
    Falls back to {} if missing/unreadable.

    Files live at <runtime_dir>/evclaw_context_<seq>.json
    """
    return load_latest_context_payload(RUNTIME_DIR)


def _context_symbol_map(context_payload: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    """Return {SYMBOL: {key_metrics, perp_signals, worker_signals_summary}} from context payload."""
    out: Dict[str, Dict[str, Any]] = {}
    symbols = context_payload.get("symbols")
    if not isinstance(symbols, dict):
        # Some context builders only include top_opportunities; we can still fall back.
        symbols = {}

    for sym, blob in (symbols or {}).items():
        if not isinstance(blob, dict):
            continue
        s = str(sym or "").upper()
        if not s:
            continue
        out[s] = blob

    # Fallback: opportunites list
    for opp in (context_payload.get("top_opportunities") or context_payload.get("selected_opportunities") or []):
        if not isinstance(opp, dict):
            continue
        s = str(opp.get("symbol") or "").upper()
        if not s or s in out:
            continue
        out[s] = {
            "key_metrics": opp.get("key_metrics"),
            "perp_signals": opp.get("perp_signals") or opp.get("signals"),
            "worker_signals_summary": opp.get("worker_signals_summary"),
        }

    return out


def _sr_risk_bias(key_metrics: Dict[str, Any], direction: str) -> Tuple[float, str]:
    """SR-based soft bias for CLOSE decisions.

    Returns (score, note) where higher score => more incentive to close.

    Boss directive: SR must always be considered (even for whale/deadcap), but it's
    not a hard rule.
    """
    try:
        km = key_metrics or {}
        sr = km.get("sr_levels") or {}
        nearest = (sr.get("nearest") or {}) if isinstance(sr, dict) else {}
        price = float(sr.get("price") or km.get("price") or 0.0)
        atr_pct = float(km.get("atr_pct") or 0.0)
        if price <= 0 or not isinstance(nearest, dict) or atr_pct <= 0:
            return 0.0, ""
        sup = nearest.get("support") or {}
        res = nearest.get("resistance") or {}
        sup_px = float(sup.get("price") or 0.0) if isinstance(sup, dict) else 0.0
        res_px = float(res.get("price") or 0.0) if isinstance(res, dict) else 0.0

        dist_sup_pct = abs(price - sup_px) / price * 100.0 if sup_px > 0 else None
        dist_res_pct = abs(res_px - price) / price * 100.0 if res_px > 0 else None
        sup_atr = (dist_sup_pct / atr_pct) if (dist_sup_pct is not None and atr_pct > 0) else None
        res_atr = (dist_res_pct / atr_pct) if (dist_res_pct is not None and atr_pct > 0) else None

        d = str(direction or "").upper()
        # Close bias when LONG is near resistance, or SHORT is near support.
        if d == "LONG" and res_atr is not None:
            if res_atr < 0.5:
                return 1.0, f"SR close: near resistance (~{res_atr:.2f} ATR)"
            if res_atr < 1.0:
                return 0.5, f"SR close: close to resistance (~{res_atr:.2f} ATR)"
        if d == "SHORT" and sup_atr is not None:
            if sup_atr < 0.5:
                return 1.0, f"SR close: near support (~{sup_atr:.2f} ATR)"
            if sup_atr < 1.0:
                return 0.5, f"SR close: close to support (~{sup_atr:.2f} ATR)"
        return 0.0, ""
    except Exception:
        return 0.0, ""

from ai_trader_db import AITraderDB, TradeRecord
from cli import build_execution_config, load_config
from executor import Executor

logger = logging.getLogger(__name__)
SKILL_DIR = Path(__file__).parent
DEFAULT_DB = str(SKILL_DIR / "ai_trader.db")


@dataclass
class ReviewCandidate:
    trade: TradeRecord
    pos_key: str
    pos_direction: str
    notional_usd: float
    age_hours: float
    unrealized_pnl_usd: float
    pnl_pct: float
    est_price: float
    sl_price: Optional[float]
    tp_price: Optional[float]
    sl_buffer_ratio: Optional[float]
    tp_progress_ratio: Optional[float]
    vp_dir: Optional[str]
    vp_z: Optional[float]
    sr_close_bias: float = 0.0
    sr_note: Optional[str] = None
    banner_note: Optional[str] = None
    position_source: str = "tracked"
    confirmation_status: str = "not_checked"
    exchange_direction: Optional[str] = None
    exchange_size: Optional[float] = None
    confirm_error: Optional[str] = None


def _now() -> float:
    return time.time()


def _safe_float(x: Any, default: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return float(default)


def _load_volume_profile() -> Dict[str, Any]:
    path = SKILL_DIR / "signals" / "volume_profile.json"
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text())
        return payload.get("symbols", {}) if isinstance(payload, dict) else {}
    except Exception:
        return {}


def _vp_signal(vp_symbols: Dict[str, Any], symbol: str) -> Tuple[Optional[str], Optional[float]]:
    sym = symbol.upper()
    blob = vp_symbols.get(sym)
    if not isinstance(blob, dict):
        return None, None
    tfs = blob.get("timeframes") or {}
    # Use strongest signal magnitude across common timeframes.
    best_dir, best_z = None, None
    for tf in ("session_8h", "daily_24h", "weekly_7d"):
        tfb = tfs.get(tf) or {}
        sig = (tfb.get("signal") or {}) if isinstance(tfb, dict) else {}
        d = sig.get("direction")
        z = sig.get("z_score")
        if d is None or z is None:
            continue
        try:
            zf = float(z)
        except Exception:
            continue
        if best_z is None or abs(zf) > abs(best_z):
            best_z = zf
            best_dir = str(d).upper()
    return best_dir, best_z


def _estimate_mark_price(entry_price: float, size: float, direction: str, unrl: float) -> float:
    # Derive an approximate current price from PnL.
    if size <= 0 or entry_price <= 0:
        return entry_price
    d = str(direction or "").upper()
    if d == "LONG":
        return entry_price + (unrl / size)
    if d == "SHORT":
        return entry_price - (unrl / size)
    return entry_price


def _sl_buffer_ratio(direction: str, entry: float, sl: float, mark: float) -> Optional[float]:
    # Ratio in [0..1+] measuring how close we are to SL. Lower = closer to SL.
    try:
        d = str(direction or "").upper()
        if d == "LONG":
            denom = (entry - sl)
            if denom <= 0:
                return None
            return (mark - sl) / denom
        if d == "SHORT":
            denom = (sl - entry)
            if denom <= 0:
                return None
            return (sl - mark) / denom
        return None
    except Exception:
        return None


def _tp_progress_ratio(direction: str, entry: float, tp: float, mark: float) -> Optional[float]:
    """Progress toward TP, roughly in [0..1+] (higher = closer/past TP)."""
    try:
        d = str(direction or "").upper()
        if d == "LONG":
            denom = (tp - entry)
            if denom <= 0:
                return None
            return (mark - entry) / denom
        if d == "SHORT":
            denom = (entry - tp)
            if denom <= 0:
                return None
            return (entry - mark) / denom
        return None
    except Exception:
        return None


def _has_recent_decay_flag(db: AITraderDB, trade_id: int, since_ts: float) -> bool:
    try:
        return bool(db.has_recent_decay_flag(trade_id=int(trade_id), since_ts=float(since_ts)))
    except Exception:
        return False


def _load_portfolio_snapshot(db: AITraderDB) -> Tuple[Optional[float], Optional[float], Optional[Dict[str, float]], Optional[Dict[str, float]]]:
    """Load latest portfolio snapshot and normalize per-venue metrics."""
    equity = None
    net = None
    equity_by_venue = None
    net_by_venue = None
    try:
        snap = db.get_latest_monitor_snapshot() or {}
        if snap:
            equity = _safe_float(snap.get("hl_equity"), 0.0)
            net = _safe_float(snap.get("hl_net_notional"), 0.0)
            wallet_equity = _safe_float(snap.get("hl_wallet_equity"), 0.0)
            wallet_net = _safe_float(snap.get("hl_wallet_net_notional"), 0.0)
            equity_by_venue = {
                "hyperliquid": equity,
                "hip3": wallet_equity,
            }
            net_by_venue = {
                "hyperliquid": net,
                "hip3": wallet_net,
            }
    except Exception:
        pass
    return equity, net, equity_by_venue, net_by_venue


def _load_open_review_trades(db: AITraderDB) -> List[Any]:
    try:
        return [
            t
            for t in db.get_open_trades()
            if normalize_venue(t.venue or "") in ("hyperliquid", "hip3")
        ]
    except Exception as exc:
        logger.error(f"Failed to load open trades from DB: {exc}")
        return []


def _safe_json(payload: Dict[str, Any]) -> str:
    try:
        return json.dumps(payload, separators=(",", ":"), sort_keys=True)
    except Exception:
        return str(payload)


def _review_position_key(venue: str, symbol: str) -> str:
    return f"{normalize_venue(venue or '')}:{str(symbol or '').upper()}"


def _build_effective_review_positions(
    *,
    open_trades: List[Any],
    tracked_positions: Dict[str, Any],
    exchange_positions: Dict[str, Any],
    venue_errors: Dict[str, str],
) -> Tuple[Dict[str, Any], Dict[str, Dict[str, Any]], List[Tuple[Any, Dict[str, Any]]]]:
    """Build effective position map with confirmation metadata.

    Returns:
        (effective_positions, position_meta_by_key, reconcile_needed_rows)
    """
    effective_positions: Dict[str, Any] = {}
    position_meta: Dict[str, Dict[str, Any]] = {}
    reconcile_needed: List[Tuple[Any, Dict[str, Any]]] = []

    for trade in open_trades:
        key = _review_position_key(trade.venue, trade.symbol)
        venue = normalize_venue(trade.venue or "")
        tracked = tracked_positions.get(key)
        exch = exchange_positions.get(key)
        err = venue_errors.get(venue)

        if err:
            if tracked and (tracked.direction or "").upper() in {"LONG", "SHORT"} and float(tracked.size or 0.0) > 0:
                effective_positions[key] = tracked
            position_meta[key] = {
                "position_source": "tracked",
                "confirmation_status": "unconfirmed_fetch_failed",
                "exchange_direction": None,
                "exchange_size": None,
                "confirm_error": str(err),
            }
            continue

        if exch and (exch.direction or "").upper() in {"LONG", "SHORT"} and float(exch.size or 0.0) > 0:
            effective_positions[key] = exch
            position_meta[key] = {
                "position_source": "exchange",
                "confirmation_status": "confirmed",
                "exchange_direction": str(exch.direction or "").upper(),
                "exchange_size": float(exch.size or 0.0),
                "confirm_error": None,
            }
            continue

        meta = {
            "position_source": "exchange",
            "confirmation_status": "flat_on_exchange",
            "exchange_direction": str(getattr(exch, "direction", "") or "").upper() or None,
            "exchange_size": float(getattr(exch, "size", 0.0) or 0.0),
            "confirm_error": None,
        }
        position_meta[key] = meta
        reconcile_needed.append((trade, meta))

    return effective_positions, position_meta, reconcile_needed


def _build_eligible_candidates(
    *,
    db: AITraderDB,
    positions: Dict[str, Any],
    position_meta: Optional[Dict[str, Dict[str, Any]]],
    open_trades: List[Any],
    vp_symbols: Dict[str, Any],
    ctx_by_symbol: Dict[str, Any],
    now_ts: float,
    min_age_hours: float,
    min_hold_hours: float,
    no_flag_hours: float,
) -> List[ReviewCandidate]:
    eligible: List[ReviewCandidate] = []
    for trade in open_trades:
        age_hours = (now_ts - float(trade.entry_time)) / 3600.0
        if age_hours < max(float(min_age_hours), float(min_hold_hours)):
            continue

        key = _review_position_key(trade.venue, trade.symbol)
        pos = positions.get(key)
        if not pos or (pos.direction or "").upper() == "FLAT" or (pos.size or 0.0) <= 0:
            continue

        since_ts = now_ts - float(no_flag_hours) * 3600.0
        if _has_recent_decay_flag(db, trade.id, since_ts):
            continue

        unrl = _safe_float(pos.unrealized_pnl, 0.0)
        notional = abs(_safe_float(trade.notional_usd, 0.0))
        if notional <= 0:
            notional = abs(_safe_float(pos.size, 0.0) * _safe_float(pos.entry_price, 0.0))
        pnl_pct = (unrl / notional) * 100.0 if notional > 0 else 0.0

        mark = _estimate_mark_price(_safe_float(pos.entry_price, 0.0), _safe_float(pos.size, 0.0), pos.direction, unrl)
        sl_price = _safe_float(trade.sl_price, 0.0) if getattr(trade, "sl_price", None) is not None else None
        sl_val = sl_price if sl_price and sl_price > 0 else None
        sl_ratio = _sl_buffer_ratio(pos.direction, _safe_float(pos.entry_price, 0.0), sl_val, mark) if sl_val else None
        tp_price = _safe_float(trade.tp_price, 0.0) if getattr(trade, "tp_price", None) is not None else None
        tp_val = tp_price if tp_price and tp_price > 0 else None
        tp_prog = _tp_progress_ratio(pos.direction, _safe_float(pos.entry_price, 0.0), tp_val, mark) if tp_val else None
        vp_dir, vp_z = _vp_signal(vp_symbols, trade.symbol)

        sym_ctx = ctx_by_symbol.get(trade.symbol.upper(), {})
        km = sym_ctx.get("key_metrics") or {}
        perp = sym_ctx.get("perp_signals") or {}
        sr_bias, sr_note = _sr_risk_bias(km if isinstance(km, dict) else {}, (pos.direction or "").upper())
        banner_note = None
        try:
            if isinstance(perp, dict):
                for bn in ("whale", "dead_capital"):
                    b = perp.get(bn)
                    if isinstance(b, dict) and bool(b.get("banner_trigger")):
                        banner_note = f"banner:{bn}"
                        break
        except Exception:
            banner_note = None

        meta = (position_meta or {}).get(key) or {}

        eligible.append(
            ReviewCandidate(
                trade=trade,
                pos_key=key,
                pos_direction=(pos.direction or "").upper(),
                notional_usd=float(notional),
                age_hours=float(age_hours),
                unrealized_pnl_usd=float(unrl),
                pnl_pct=float(pnl_pct),
                est_price=float(mark),
                sl_price=float(sl_val) if sl_val else None,
                tp_price=float(tp_val) if tp_val else None,
                sl_buffer_ratio=float(sl_ratio) if sl_ratio is not None else None,
                tp_progress_ratio=float(tp_prog) if tp_prog is not None else None,
                vp_dir=vp_dir,
                vp_z=float(vp_z) if vp_z is not None else None,
                sr_close_bias=float(sr_bias),
                sr_note=str(sr_note) if sr_note else None,
                banner_note=banner_note,
                position_source=str(meta.get("position_source") or "tracked"),
                confirmation_status=str(meta.get("confirmation_status") or "not_checked"),
                exchange_direction=(
                    str(meta.get("exchange_direction") or "").upper() or None
                ),
                exchange_size=(
                    float(meta.get("exchange_size")) if meta.get("exchange_size") is not None else None
                ),
                confirm_error=str(meta.get("confirm_error") or "") or None,
            )
        )
    return eligible


def _bucket_review_candidates(
    *,
    eligible: List[ReviewCandidate],
    no_progress_hours: float,
    no_progress_max_win_pct: float,
    no_progress_tp_progress_max: float,
    max_net_exposure_mult: float,
    equity: Optional[float],
    net: Optional[float],
    equity_by_venue: Optional[Dict[str, float]],
    net_by_venue: Optional[Dict[str, float]],
) -> Tuple[List[Tuple[ReviewCandidate, str]], List[ReviewCandidate], List[ReviewCandidate]]:
    dead_flat: List[Tuple[ReviewCandidate, str]] = []
    deadly_loser: List[ReviewCandidate] = []
    exposure_reduce: List[ReviewCandidate] = []

    for c in eligible:
        flat_usd = max(5.0, 0.001 * c.notional_usd)
        is_flat = abs(c.unrealized_pnl_usd) <= flat_usd

        is_small_win = c.unrealized_pnl_usd > 0 and c.pnl_pct <= float(no_progress_max_win_pct)
        tp_stalled = c.tp_progress_ratio is not None and c.tp_progress_ratio < float(no_progress_tp_progress_max)
        is_no_progress = c.age_hours >= float(no_progress_hours) and is_small_win and tp_stalled

        loss_usd = max(25.0, 0.01 * c.notional_usd)
        near_sl = c.sl_buffer_ratio is not None and c.sl_buffer_ratio <= 0.25
        vp_against = (
            c.vp_dir in {"LONG", "SHORT"}
            and c.pos_direction in {"LONG", "SHORT"}
            and c.vp_dir != c.pos_direction
            and c.vp_z is not None
            and abs(c.vp_z) >= 2.0
        )
        is_deadly = (c.unrealized_pnl_usd <= -loss_usd) or near_sl or vp_against

        if is_no_progress:
            dead_flat.append((c, "HOURLY_REVIEW_NO_PROGRESS"))
        elif is_flat:
            dead_flat.append((c, "HOURLY_REVIEW_DEAD_FLAT"))
        elif is_deadly:
            deadly_loser.append(c)

        venue_key = str(c.trade.venue or "").lower()
        try:
            eq_for_venue = equity_by_venue.get(venue_key) if isinstance(equity_by_venue, dict) else equity
            net_for_venue = net_by_venue.get(venue_key) if isinstance(net_by_venue, dict) else net
        except Exception:
            eq_for_venue = equity
            net_for_venue = net

        if eq_for_venue and net_for_venue is not None and eq_for_venue > 0 and abs(net_for_venue) > float(max_net_exposure_mult) * eq_for_venue:
            net_dir = "SHORT" if net_for_venue < 0 else "LONG"
            if c.pos_direction == net_dir:
                exposure_reduce.append(c)

    dead_flat.sort(key=lambda pair: (pair[0].age_hours, -abs(pair[0].notional_usd)), reverse=True)
    deadly_loser.sort(
        key=lambda x: (
            x.unrealized_pnl_usd,
            -float(x.sr_close_bias or 0.0),
            (x.sl_buffer_ratio if x.sl_buffer_ratio is not None else 9e9),
        ),
        reverse=False,
    )
    exposure_reduce.sort(key=lambda x: (-float(x.sr_close_bias or 0.0), abs(x.unrealized_pnl_usd), x.notional_usd))
    return dead_flat, deadly_loser, exposure_reduce


def _build_close_plan(
    *,
    dead_flat: List[Tuple[ReviewCandidate, str]],
    deadly_loser: List[ReviewCandidate],
    exposure_reduce: List[ReviewCandidate],
    max_closes: int,
) -> List[Tuple[ReviewCandidate, str]]:
    to_close: List[Tuple[ReviewCandidate, str]] = []
    for c, reason in dead_flat:
        if len(to_close) >= max_closes:
            break
        to_close.append((c, reason))
    for c in deadly_loser:
        if len(to_close) >= max_closes:
            break
        if any(cc.trade.id == c.trade.id for cc, _ in to_close):
            continue
        to_close.append((c, "HOURLY_REVIEW_DEADLY_LOSER"))
    for c in exposure_reduce:
        if len(to_close) >= max_closes:
            break
        if any(cc.trade.id == c.trade.id for cc, _ in to_close):
            continue
        to_close.append((c, "HOURLY_REVIEW_EXPOSURE_REDUCE"))
    return to_close


async def _review_once_impl(
    *,
    db: AITraderDB,
    executor: Executor,
    min_age_hours: float,
    min_hold_hours: float,
    no_flag_hours: float,
    max_closes: int,
    max_net_exposure_mult: float,
    record_holds: bool,
    max_hold_records: int,
    no_progress_hours: float,
    no_progress_max_win_pct: float,
    no_progress_tp_progress_max: float,
) -> Dict[str, Any]:
    now_ts = _now()

    # Load portfolio snapshot for exposure cap.
    equity, net, equity_by_venue, net_by_venue = _load_portfolio_snapshot(db)

    open_trades = _load_open_review_trades(db)

    try:
        tracked_positions = await executor.get_all_positions()
    except Exception as exc:
        logger.error(f"Failed to load executor positions: {exc}")
        tracked_positions = {}

    venues = sorted({normalize_venue(t.venue or "") for t in open_trades if normalize_venue(t.venue or "")})
    exchange_positions: Dict[str, Any] = {}
    venue_errors: Dict[str, str] = {}
    for venue in venues:
        try:
            exchange_positions.update(await executor.get_live_positions_by_venue(venue))
        except Exception as exc:
            venue_errors[venue] = str(exc)
            logger.warning(f"Live position snapshot failed for venue={venue}: {exc}")

    positions, position_meta, reconcile_needed = _build_effective_review_positions(
        open_trades=open_trades,
        tracked_positions=tracked_positions,
        exchange_positions=exchange_positions,
        venue_errors=venue_errors,
    )

    vp_symbols = _load_volume_profile()

    # Best-input context: latest cycle context JSON (SR + signals) as a priority hint.
    ctx_payload = _load_latest_cycle_context()
    ctx_by_symbol = _context_symbol_map(ctx_payload)

    eligible = _build_eligible_candidates(
        db=db,
        positions=positions,
        position_meta=position_meta,
        open_trades=open_trades,
        vp_symbols=vp_symbols,
        ctx_by_symbol=ctx_by_symbol,
        now_ts=now_ts,
        min_age_hours=min_age_hours,
        min_hold_hours=min_hold_hours,
        no_flag_hours=no_flag_hours,
    )

    dead_flat, deadly_loser, exposure_reduce = _bucket_review_candidates(
        eligible=eligible,
        no_progress_hours=no_progress_hours,
        no_progress_max_win_pct=no_progress_max_win_pct,
        no_progress_tp_progress_max=no_progress_tp_progress_max,
        max_net_exposure_mult=max_net_exposure_mult,
        equity=equity,
        net=net,
        equity_by_venue=equity_by_venue,
        net_by_venue=net_by_venue,
    )
    to_close = _build_close_plan(
        dead_flat=dead_flat,
        deadly_loser=deadly_loser,
        exposure_reduce=exposure_reduce,
        max_closes=max_closes,
    )

    reconcile_records = 0
    for trade, meta in reconcile_needed:
        detail_obj = {
            "type": "POSITION_REVIEW_RECONCILE_NEEDED",
            "symbol": str(trade.symbol or "").upper(),
            "venue": str(trade.venue or "").lower(),
            "trade_id": int(trade.id),
            "db_direction": str(trade.direction or "").upper() or None,
            "position_source": str(meta.get("position_source") or ""),
            "confirmation_status": str(meta.get("confirmation_status") or ""),
            "exchange_direction": meta.get("exchange_direction"),
            "exchange_size": meta.get("exchange_size"),
            "confirm_error": meta.get("confirm_error"),
            "why": "db_open_but_exchange_flat_or_missing",
        }
        detail = _safe_json(detail_obj)
        try:
            db.record_decay_decision(
                symbol=trade.symbol,
                venue=trade.venue,
                trade_id=trade.id,
                action="HOLD",
                reason="RECONCILE_NEEDED",
                detail=detail,
                decided_by=None,
                source="position_review_worker",
                dedupe_seconds=900.0,
            )
            reconcile_records += 1
        except Exception as exc:
            logger.warning(
                "record_decay_decision RECONCILE_NEEDED failed for %s/%s trade_id=%s: %s",
                trade.venue,
                trade.symbol,
                trade.id,
                exc,
            )

    # Record HOLD decisions (deduped hourly) for the worst candidates we did NOT close.
    hold_records = 0
    if record_holds:
        # Score: prioritize dead_flat, then deadly, then exposure.
        scored: List[Tuple[float, ReviewCandidate]] = []
        for c in eligible:
            score = 0.0
            flat_usd = max(5.0, 0.001 * c.notional_usd)
            if abs(c.unrealized_pnl_usd) <= flat_usd:
                score += 1000.0
            if c.unrealized_pnl_usd < 0:
                score += min(500.0, abs(c.unrealized_pnl_usd))
            if c.sl_buffer_ratio is not None:
                score += max(0.0, 200.0 * (0.5 - min(0.5, c.sl_buffer_ratio)))
            scored.append((score, c))
        scored.sort(key=lambda x: x[0], reverse=True)

        for score, c in scored:
            if hold_records >= max_hold_records:
                break
            if any(cc.trade.id == c.trade.id for cc, _ in to_close):
                continue
            detail_obj = {
                "type": "POSITION_REVIEW_PLAN_HOLD",
                "score": round(float(score), 3),
                "age_h": round(float(c.age_hours), 3),
                "unrl": round(float(c.unrealized_pnl_usd), 4),
                "pnl_pct": round(float(c.pnl_pct), 4),
                "notional": round(float(c.notional_usd), 4),
                "mark": float(c.est_price),
                "sl_ratio": c.sl_buffer_ratio,
                "vp": f"{c.vp_dir}:{c.vp_z}" if c.vp_dir is not None and c.vp_z is not None else None,
                "sr_close_bias": float(c.sr_close_bias or 0.0),
                "sr_note": c.sr_note,
                "banner": c.banner_note,
                "position_source": c.position_source,
                "confirmation_status": c.confirmation_status,
                "exchange_direction": c.exchange_direction,
                "exchange_size": c.exchange_size,
                "confirm_error": c.confirm_error,
            }
            detail = _safe_json(detail_obj)
            try:
                db.record_decay_decision(
                    symbol=c.trade.symbol,
                    venue=c.trade.venue,
                    trade_id=c.trade.id,
                    action="HOLD",
                    reason="HOURLY_REVIEW_HOLD",
                    detail=detail,
                    decided_by=None,
                    source="position_review_worker",
                    dedupe_seconds=3600.0,
                )
                hold_records += 1
            except Exception as exc:
                logger.warning(
                    "record_decay_decision HOLD failed for %s/%s trade_id=%s: %s",
                    c.trade.venue,
                    c.trade.symbol,
                    c.trade.id,
                    exc,
                )

    closed = 0
    attempted = 0

    for c, reason in to_close:
        attempted += 1
        detail_obj = {
            "type": "POSITION_REVIEW_PLAN_CLOSE",
            "age_h": round(float(c.age_hours), 3),
            "unrl": round(float(c.unrealized_pnl_usd), 4),
            "pnl_pct": round(float(c.pnl_pct), 4),
            "notional": round(float(c.notional_usd), 4),
            "mark": float(c.est_price),
            "sl_ratio": c.sl_buffer_ratio,
            "vp": f"{c.vp_dir}:{c.vp_z}" if c.vp_dir is not None and c.vp_z is not None else None,
            "sr_close_bias": float(c.sr_close_bias or 0.0),
            "sr_note": c.sr_note,
            "banner": c.banner_note,
            "equity": float(equity or 0.0),
            "net_notional": float(net or 0.0),
            "position_source": c.position_source,
            "confirmation_status": c.confirmation_status,
            "exchange_direction": c.exchange_direction,
            "exchange_size": c.exchange_size,
            "confirm_error": c.confirm_error,
        }
        detail = _safe_json(detail_obj)

        # Record decision first (audit trail even if close fails).
        try:
            db.record_decay_decision(
                symbol=c.trade.symbol,
                venue=c.trade.venue,
                trade_id=c.trade.id,
                action="CLOSE",
                reason=reason,
                detail=detail,
                decided_by=None,
                source="position_review_worker",
                dedupe_seconds=60.0,
            )
        except Exception as exc:
            logger.warning(
                "record_decay_decision CLOSE failed for %s/%s trade_id=%s: %s",
                c.trade.venue,
                c.trade.symbol,
                c.trade.id,
                exc,
            )

        logger.info(f"[PLAN_ONLY] would close {c.trade.symbol} {c.pos_direction} ({reason}) {detail}")

    return {
        "now": now_ts,
        "equity": equity,
        "net_notional": net,
        "open_trades": len(open_trades),
        "eligible": len(eligible),
        "planned_closes": len(to_close),
        "attempted": attempted,
        "closed": closed,
        "holds_recorded": hold_records,
        "reconcile_records": reconcile_records,
        "exchange_snapshot_errors": len(venue_errors),
    }


async def review_once(
    *,
    db: AITraderDB,
    executor: Executor,
    min_age_hours: float,
    min_hold_hours: float,
    no_flag_hours: float,
    max_closes: int,
    max_net_exposure_mult: float,
    record_holds: bool,
    max_hold_records: int,
    no_progress_hours: float,
    no_progress_max_win_pct: float,
    no_progress_tp_progress_max: float,
) -> Dict[str, Any]:
    """Thin wrapper around review implementation."""
    return await _review_once_impl(
        db=db,
        executor=executor,
        min_age_hours=min_age_hours,
        min_hold_hours=min_hold_hours,
        no_flag_hours=no_flag_hours,
        max_closes=max_closes,
        max_net_exposure_mult=max_net_exposure_mult,
        record_holds=record_holds,
        max_hold_records=max_hold_records,
        no_progress_hours=no_progress_hours,
        no_progress_max_win_pct=no_progress_max_win_pct,
        no_progress_tp_progress_max=no_progress_tp_progress_max,
    )


async def main_async() -> int:
    parser = argparse.ArgumentParser(description="Hourly position review worker")
    parser.add_argument("--db-path", default=None)
    parser.add_argument("--interval", type=float, default=3600.0)
    parser.add_argument("--once", action="store_true")

    parser.add_argument("--min-age-hours", type=float, default=None)
    parser.add_argument("--no-flag-hours", type=float, default=None)
    parser.add_argument("--max-closes", type=int, default=None)
    parser.add_argument("--max-net-exposure-mult", type=float, default=None)

    # Time-based small-win/no-progress exit rule (inventory hygiene).
    parser.add_argument("--no-progress-hours", type=float, default=None)
    parser.add_argument("--no-progress-max-win-pct", type=float, default=None)
    parser.add_argument("--no-progress-tp-progress-max", type=float, default=None)

    parser.add_argument("--record-holds", action="store_true", default=True)
    parser.add_argument("--max-hold-records", type=int, default=None)

    args = parser.parse_args()

    config = load_config()
    config_root = (config.get("config") or {}) if isinstance(config, dict) else {}
    review_cfg = (config_root.get("review") or {}) if isinstance(config_root, dict) else {}
    risk_cfg = (config_root.get("risk") or {}) if isinstance(config_root, dict) else {}
    exposure_cfg = (config_root.get("exposure") or {}) if isinstance(config_root, dict) else {}

    def _cfg_float(cfg: Dict[str, Any], key: str, default: float) -> float:
        try:
            raw = cfg.get(key, default)
            return float(default if raw is None else raw)
        except Exception:
            return float(default)

    def _cfg_int(cfg: Dict[str, Any], key: str, default: int) -> int:
        try:
            raw = cfg.get(key, default)
            return int(float(default if raw is None else raw))
        except Exception:
            return int(default)

    min_age_hours = float(args.min_age_hours) if args.min_age_hours is not None else _cfg_float(review_cfg, "min_age_hours", 6.0)
    no_flag_hours = float(args.no_flag_hours) if args.no_flag_hours is not None else _cfg_float(review_cfg, "no_flag_hours", 2.0)
    max_closes = int(args.max_closes) if args.max_closes is not None else _cfg_int(review_cfg, "max_closes", 3)
    max_net_exposure_mult = (
        float(args.max_net_exposure_mult)
        if args.max_net_exposure_mult is not None
        else _cfg_float(exposure_cfg, "max_net_exposure_mult", 2.0)
    )
    no_progress_hours = (
        float(args.no_progress_hours)
        if args.no_progress_hours is not None
        else _cfg_float(review_cfg, "no_progress_hours", 12.0)
    )
    no_progress_max_win_pct = (
        float(args.no_progress_max_win_pct)
        if args.no_progress_max_win_pct is not None
        else _cfg_float(review_cfg, "no_progress_max_win_pct", 0.75)
    )
    no_progress_tp_progress_max = (
        float(args.no_progress_tp_progress_max)
        if args.no_progress_tp_progress_max is not None
        else _cfg_float(review_cfg, "no_progress_tp_progress_max", 0.25)
    )
    max_hold_records = (
        int(args.max_hold_records)
        if args.max_hold_records is not None
        else _cfg_int(review_cfg, "max_hold_records", 20)
    )
    min_hold_hours = _cfg_float(risk_cfg, "min_hold_hours", 2.0)

    # IMPORTANT: This worker needs *live* exchange positions to score. Do not
    # use exec_config.dry_run for plan-only mode.
    exec_config = build_execution_config(config, dry_run=False)

    db_path = str(args.db_path or exec_config.db_path or DEFAULT_DB)
    db = AITraderDB(db_path)

    executor = Executor(config=exec_config)
    await executor.initialize()
    logger.info("Position review worker mode: PLAN_ONLY")

    try:
        while True:
            result = await review_once(
                db=db,
                executor=executor,
                min_age_hours=float(min_age_hours),
                min_hold_hours=float(min_hold_hours),
                no_flag_hours=float(no_flag_hours),
                max_closes=int(max_closes),
                max_net_exposure_mult=float(max_net_exposure_mult),
                record_holds=bool(args.record_holds),
                max_hold_records=int(max_hold_records),
                no_progress_hours=float(no_progress_hours),
                no_progress_max_win_pct=float(no_progress_max_win_pct),
                no_progress_tp_progress_max=float(no_progress_tp_progress_max),
            )
            logger.info(
                "Position review: open=%s eligible=%s planned=%s closed=%s net=%s eq=%s",
                result.get("open_trades"),
                result.get("eligible"),
                result.get("planned_closes"),
                result.get("closed"),
                result.get("net_notional"),
                result.get("equity"),
            )
            if args.once:
                break
            await asyncio.sleep(float(args.interval))
    finally:
        await executor.close()

    return 0


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(levelname)s: %(message)s")
    try:
        return asyncio.run(main_async())
    except KeyboardInterrupt:
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
