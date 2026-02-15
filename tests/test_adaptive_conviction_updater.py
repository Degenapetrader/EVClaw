#!/usr/bin/env python3
"""Tests for adaptive conviction updater."""

import tempfile
import time
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from ai_trader_db import AITraderDB
from adaptive_conviction_updater import run_adaptive_conviction_update


def _new_db() -> AITraderDB:
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        path = f.name
    return AITraderDB(path)


def _seed_feature(db: AITraderDB, trade_id: int, conviction: float, pnl_r: float, now: float) -> None:
    db.insert_trade_features(
        trade_id=trade_id,
        symbol="ETH",
        direction="LONG",
        venue="hyperliquid",
        closed_at=now - 10,
        conviction=conviction,
        order_type="chase_limit" if conviction >= 0.7 else "limit",
        pnl_r=pnl_r,
        pnl_usd=pnl_r * 100.0,
        exit_reason="TP",
    )


def test_updater_noop_on_insufficient_data() -> None:
    db = _new_db()
    now = time.time()
    _seed_feature(db, 1, 0.8, 0.2, now)

    result = run_adaptive_conviction_update(
        db,
        now_ts=now,
        min_closed_trades=5,
        min_interval_seconds=0,
    )
    assert result.status == "noop"
    assert result.reason == "insufficient_data"
    assert result.updated is False


def test_updater_adjusts_chase_threshold_when_chase_underperforms() -> None:
    db = _new_db()
    now = time.time()
    db.insert_conviction_config_snapshot(
        params={"limit_min": 0.2, "chase_threshold": 0.7, "degen_threshold": 0.75, "degen_multiplier": 1.5},
        source="test",
        activate=True,
    )

    trade_id = 1
    for _ in range(10):
        _seed_feature(db, trade_id, 0.8, -0.3, now)
        trade_id += 1
    for _ in range(10):
        _seed_feature(db, trade_id, 0.5, 0.2, now)
        trade_id += 1
    for _ in range(5):
        _seed_feature(db, trade_id, 0.1, -0.1, now)
        trade_id += 1

    result = run_adaptive_conviction_update(
        db,
        now_ts=now,
        min_closed_trades=20,
        min_interval_seconds=0,
    )
    assert result.status == "success"
    assert result.updated is True
    assert result.new_config["chase_threshold"] > 0.7


def test_updater_dedupes_same_fingerprint() -> None:
    db = _new_db()
    now = time.time()
    for i in range(30):
        _seed_feature(db, i + 1, 0.72, 0.05, now)

    first = run_adaptive_conviction_update(
        db,
        now_ts=now,
        min_closed_trades=10,
        min_interval_seconds=0,
    )
    second = run_adaptive_conviction_update(
        db,
        now_ts=now,
        min_closed_trades=10,
        min_interval_seconds=0,
    )

    assert first.status in {"success", "noop"}
    assert second.status == "duplicate"
    assert second.reason == "fingerprint_already_processed"
