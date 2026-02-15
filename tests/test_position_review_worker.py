#!/usr/bin/env python3
"""Tests for position_review_worker exchange confirmation behavior."""

import json
import sqlite3
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from ai_trader_db import AITraderDB
from exchanges import Position
from position_review_worker import _review_once_impl


class _ReviewExecutor:
    def __init__(self, *, tracked_positions, live_positions_by_venue=None, venue_errors=None):
        self._tracked = dict(tracked_positions or {})
        self._live = dict(live_positions_by_venue or {})
        self._errors = dict(venue_errors or {})

    async def get_all_positions(self):
        return dict(self._tracked)

    async def get_live_positions_by_venue(self, venue: str):
        if venue in self._errors:
            raise RuntimeError(str(self._errors[venue]))
        return dict(self._live.get(venue, {}))


def _seed_open_trade(db: AITraderDB, *, symbol: str = "SOL", venue: str = "hyperliquid", direction: str = "LONG"):
    return db.log_trade_entry(
        symbol=symbol,
        direction=direction,
        entry_price=100.0,
        size=1.0,
        venue=venue,
    )


def _pos(symbol: str, direction: str = "LONG", size: float = 1.0, unrl: float = 0.0, venue: str = "hyperliquid"):
    return Position(
        symbol=symbol,
        direction=direction,
        size=size,
        entry_price=100.0,
        unrealized_pnl=unrl,
        venue=venue,
    )


def _decision_rows(db_path: str):
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        return conn.execute(
            "SELECT action, reason, detail FROM decay_decisions WHERE source = 'position_review_worker' ORDER BY id ASC"
        ).fetchall()


async def test_review_emits_reconcile_needed_when_exchange_flat():
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    db = AITraderDB(db_path)
    _seed_open_trade(db, symbol="SOL", venue="hyperliquid", direction="LONG")
    tracked = {"hyperliquid:SOL": _pos("SOL", direction="LONG", unrl=0.0)}
    executor = _ReviewExecutor(
        tracked_positions=tracked,
        live_positions_by_venue={"hyperliquid": {}},  # flat / missing on exchange
    )

    result = await _review_once_impl(
        db=db,
        executor=executor,
        min_age_hours=0.0,
        min_hold_hours=0.0,
        no_flag_hours=0.0,
        max_closes=1,
        max_net_exposure_mult=10.0,
        record_holds=False,
        max_hold_records=0,
        no_progress_hours=12.0,
        no_progress_max_win_pct=0.75,
        no_progress_tp_progress_max=0.25,
    )

    rows = _decision_rows(db_path)
    assert result["planned_closes"] == 0
    assert any(r["reason"] == "RECONCILE_NEEDED" and r["action"] == "HOLD" for r in rows)


async def test_review_fetch_failure_uses_tracked_and_tags_close_unconfirmed():
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    db = AITraderDB(db_path)
    _seed_open_trade(db, symbol="SOL", venue="hyperliquid", direction="LONG")
    tracked = {"hyperliquid:SOL": _pos("SOL", direction="LONG", unrl=0.0)}
    executor = _ReviewExecutor(
        tracked_positions=tracked,
        venue_errors={"hyperliquid": "exchange timeout"},
    )

    await _review_once_impl(
        db=db,
        executor=executor,
        min_age_hours=0.0,
        min_hold_hours=0.0,
        no_flag_hours=0.0,
        max_closes=1,
        max_net_exposure_mult=10.0,
        record_holds=False,
        max_hold_records=0,
        no_progress_hours=12.0,
        no_progress_max_win_pct=0.75,
        no_progress_tp_progress_max=0.25,
    )

    rows = _decision_rows(db_path)
    close_rows = [r for r in rows if r["action"] == "CLOSE"]
    assert len(close_rows) == 1
    assert close_rows[0]["reason"] in {"HOURLY_REVIEW_DEAD_FLAT", "HOURLY_REVIEW_NO_PROGRESS"}
    payload = json.loads(close_rows[0]["detail"])
    assert payload["confirmation_status"] == "unconfirmed_fetch_failed"
    assert payload["position_source"] == "tracked"
    assert payload["confirm_error"] == "exchange timeout"
