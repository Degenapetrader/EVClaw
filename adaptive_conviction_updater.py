#!/usr/bin/env python3
"""Adaptive conviction threshold updater.

This module updates conviction routing thresholds using closed-trade features.
It does not change AGI flow. It only writes config snapshots and run metadata.
"""

from __future__ import annotations

import hashlib
import json
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from ai_trader_db import AITraderDB


@dataclass
class AdaptationResult:
    status: str
    run_id: Optional[int]
    fingerprint: str
    reason: str
    updated: bool
    old_config: Dict[str, float]
    new_config: Dict[str, float]
    sample_count: int


def _safe_float(value: Any, default: float) -> float:
    try:
        out = float(value)
    except Exception:
        return float(default)
    if out != out:
        return float(default)
    return out


def _clamp(value: float, lo: float, hi: float) -> float:
    return max(float(lo), min(float(hi), float(value)))


def _fingerprint(window_start: float, window_end: float, rows: List[Dict[str, Any]]) -> str:
    compact = {
        "window_start": round(float(window_start), 3),
        "window_end": round(float(window_end), 3),
        "rows": [
            {
                "trade_id": int(r.get("trade_id") or 0),
                "closed_at": round(float(r.get("closed_at") or 0.0), 3),
                "conviction": round(_safe_float(r.get("conviction"), 0.0), 5),
                "pnl_r": round(_safe_float(r.get("pnl_r"), 0.0), 6),
                "pnl_usd": round(_safe_float(r.get("pnl_usd"), 0.0), 6),
            }
            for r in rows
        ],
    }
    raw = json.dumps(compact, separators=(",", ":"), sort_keys=True)
    return "adapt:" + hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _expectancy(rows: List[Dict[str, Any]]) -> float:
    vals: List[float] = []
    for r in rows:
        pnl_r = r.get("pnl_r")
        if pnl_r is not None:
            vals.append(_safe_float(pnl_r, 0.0))
            continue
        pnl_usd = r.get("pnl_usd")
        if pnl_usd is None:
            continue
        vals.append(_safe_float(pnl_usd, 0.0))
    if not vals:
        return 0.0
    return float(sum(vals) / len(vals))


def _split_buckets(
    rows: List[Dict[str, Any]],
    *,
    limit_min: float,
    chase_threshold: float,
) -> Dict[str, List[Dict[str, Any]]]:
    low: List[Dict[str, Any]] = []
    limit: List[Dict[str, Any]] = []
    chase: List[Dict[str, Any]] = []
    for row in rows:
        conviction = _safe_float(row.get("conviction"), 0.0)
        if conviction <= limit_min:
            low.append(row)
        elif conviction >= chase_threshold:
            chase.append(row)
        else:
            limit.append(row)
    return {"low": low, "limit": limit, "chase": chase}


def run_adaptive_conviction_update(
    db: AITraderDB,
    *,
    now_ts: Optional[float] = None,
    lookback_seconds: float = 24 * 3600,
    min_closed_trades: int = 25,
    min_interval_seconds: float = 30 * 60,
    force: bool = False,
) -> AdaptationResult:
    """Run one adaptive update pass and persist outcomes in DB."""
    now = float(now_ts if now_ts is not None else time.time())
    window_end = now
    window_start = now - float(max(60.0, lookback_seconds))

    rows = db.get_trade_features_window(
        window_start=window_start,
        window_end=window_end,
        limit=10000,
    )
    fp = _fingerprint(window_start, window_end, rows)

    active = db.get_active_conviction_config() or {}
    params = active.get("params") if isinstance(active, dict) else {}
    if not isinstance(params, dict):
        params = {}

    old_limit_min = _safe_float(params.get("limit_min"), 0.2)
    old_chase = _safe_float(params.get("chase_threshold"), 0.7)
    old_degen = _safe_float(params.get("degen_threshold"), 0.75)
    old_mult = _safe_float(params.get("degen_multiplier"), 1.5)
    old_config = {
        "limit_min": old_limit_min,
        "chase_threshold": old_chase,
        "degen_threshold": old_degen,
        "degen_multiplier": old_mult,
    }

    if not force:
        latest_fp = db.get_latest_adaptation_fingerprint()
        if latest_fp == fp:
            return AdaptationResult(
                status="duplicate",
                run_id=None,
                fingerprint=fp,
                reason="fingerprint_already_processed",
                updated=False,
                old_config=old_config,
                new_config=old_config,
                sample_count=len(rows),
            )

        last_started = db.get_latest_adaptation_started_at() or 0.0
        if (now - last_started) < float(min_interval_seconds):
            return AdaptationResult(
                status="cooldown",
                run_id=None,
                fingerprint=fp,
                reason="min_interval_not_elapsed",
                updated=False,
                old_config=old_config,
                new_config=old_config,
                sample_count=len(rows),
            )

    run_id = db.create_adaptation_run(
        window_start=window_start,
        window_end=window_end,
        fingerprint=fp,
        status="running",
    )
    if run_id is None:
        return AdaptationResult(
            status="duplicate",
            run_id=None,
            fingerprint=fp,
            reason="run_fingerprint_exists",
            updated=False,
            old_config=old_config,
            new_config=old_config,
            sample_count=len(rows),
        )

    if len(rows) < int(min_closed_trades):
        result = {
            "reason": "insufficient_data",
            "samples": len(rows),
            "min_required": int(min_closed_trades),
        }
        db.finish_adaptation_run(run_id, status="noop", result=result)
        return AdaptationResult(
            status="noop",
            run_id=run_id,
            fingerprint=fp,
            reason="insufficient_data",
            updated=False,
            old_config=old_config,
            new_config=old_config,
            sample_count=len(rows),
        )

    buckets = _split_buckets(rows, limit_min=old_limit_min, chase_threshold=old_chase)
    chase_exp = _expectancy(buckets["chase"])
    limit_exp = _expectancy(buckets["limit"])
    low_exp = _expectancy(buckets["low"])

    new_limit_min = old_limit_min
    new_chase = old_chase

    if len(buckets["chase"]) >= 5 and len(buckets["limit"]) >= 5:
        # Step size tuning: smaller increments reduce oscillation.
        # Degen directive (2026-02-12): 0.02 -> 0.01.
        if chase_exp < (limit_exp - 0.05):
            new_chase = old_chase + 0.01
        elif chase_exp > (limit_exp + 0.05):
            new_chase = old_chase - 0.01

    if len(buckets["low"]) >= 5:
        if low_exp < -0.05:
            new_limit_min = old_limit_min + 0.01
        elif low_exp > 0.05:
            new_limit_min = old_limit_min - 0.01

    new_limit_min = _clamp(new_limit_min, 0.15, 0.30)
    new_chase = _clamp(new_chase, 0.65, 0.80)
    if new_limit_min >= new_chase:
        new_limit_min = max(0.15, new_chase - 0.05)

    new_config = {
        "limit_min": round(new_limit_min, 4),
        "chase_threshold": round(new_chase, 4),
        "degen_threshold": old_degen,
        "degen_multiplier": old_mult,
    }

    changed = (
        abs(new_config["limit_min"] - old_config["limit_min"]) > 1e-9
        or abs(new_config["chase_threshold"] - old_config["chase_threshold"]) > 1e-9
    )

    run_result = {
        "samples": len(rows),
        "buckets": {k: len(v) for k, v in buckets.items()},
        "expectancy": {
            "chase": chase_exp,
            "limit": limit_exp,
            "low": low_exp,
        },
        "old": old_config,
        "new": new_config,
        "changed": changed,
    }

    if changed:
        db.insert_conviction_config_snapshot(
            params=new_config,
            source="adaptive_conviction_updater",
            notes=(
                f"auto-update samples={len(rows)} "
                f"chase_exp={chase_exp:.5f} limit_exp={limit_exp:.5f} low_exp={low_exp:.5f}"
            ),
            effective_from=now,
            activate=True,
        )
        db.finish_adaptation_run(run_id, status="success", result=run_result)
        return AdaptationResult(
            status="success",
            run_id=run_id,
            fingerprint=fp,
            reason="config_updated",
            updated=True,
            old_config=old_config,
            new_config=new_config,
            sample_count=len(rows),
        )

    db.finish_adaptation_run(run_id, status="noop", result=run_result)
    return AdaptationResult(
        status="noop",
        run_id=run_id,
        fingerprint=fp,
        reason="no_change",
        updated=False,
        old_config=old_config,
        new_config=new_config,
        sample_count=len(rows),
    )


__all__ = ["AdaptationResult", "run_adaptive_conviction_update"]
