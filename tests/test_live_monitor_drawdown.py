#!/usr/bin/env python3
"""live_monitor safety-state sync regressions."""

import sqlite3
import tempfile
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from ai_trader_db import AITraderDB
from live_monitor import _sync_safety_state_equity, load_tracking_positions


def test_sync_safety_state_preserves_max_drawdown() -> None:
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE safety_state (
                id INTEGER PRIMARY KEY,
                peak_equity REAL,
                current_equity REAL,
                max_drawdown_pct REAL,
                updated_at REAL
            )
            """
        )
        conn.execute(
            "INSERT INTO safety_state (id, peak_equity, current_equity, max_drawdown_pct, updated_at) VALUES (1, 100.0, 90.0, 10.0, 0)"
        )
        conn.commit()

        _sync_safety_state_equity(conn, total_equity=95.0)

        row = conn.execute(
            "SELECT peak_equity, current_equity, max_drawdown_pct FROM safety_state WHERE id = 1"
        ).fetchone()

    assert row is not None
    assert float(row[0]) == 100.0
    assert float(row[1]) == 95.0
    assert float(row[2]) == 10.0


def test_load_tracking_positions_keeps_same_symbol_across_venues() -> None:
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    db = AITraderDB(db_path)
    db.log_trade_entry(
        symbol="ETH",
        direction="LONG",
        entry_price=2000.0,
        size=0.1,
        venue="hyperliquid",
        state="SLTP_FAILED",
    )
    db.log_trade_entry(
        symbol="ETH",
        direction="SHORT",
        entry_price=2010.0,
        size=0.2,
        venue="hip3",
        state="PLACING_SLTP",
    )

    positions = load_tracking_positions(Path(db_path))
    assert len(positions) == 2

    venues = {str(v.get("venue")) for v in positions.values()}
    symbols = {str(v.get("symbol")) for v in positions.values()}
    states = {str(v.get("state")) for v in positions.values()}
    assert venues == {"hyperliquid", "hip3"}
    assert symbols == {"ETH"}
    assert states == {"SLTP_FAILED", "PLACING_SLTP"}
