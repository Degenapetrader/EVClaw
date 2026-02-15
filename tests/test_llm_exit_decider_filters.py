#!/usr/bin/env python3
"""Tests for llm_exit_decider plan filtering knobs."""

import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from llm_exit_decider import _fetch_pending_plans


def _seed(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE decay_decisions (
            id INTEGER PRIMARY KEY,
            ts REAL,
            symbol TEXT,
            venue TEXT,
            trade_id INTEGER,
            reason TEXT,
            detail TEXT,
            source TEXT,
            action TEXT
        )
        """
    )
    rows = [
        (1, 1.0, "XYZ:NVDA", "hip3", 11, "DECAY_EXIT", "{}", "decay_worker_notify", "HOLD"),
        (2, 2.0, "ETH", "hyperliquid", 12, "DECAY_EXIT", "{}", "decay_worker_notify", "HOLD"),
        (3, 3.0, "XYZ:AMD", "hyperliquid", 13, "DECAY_EXIT", "{}", "decay_worker_notify", "HOLD"),
    ]
    conn.executemany(
        """
        INSERT INTO decay_decisions (id, ts, symbol, venue, trade_id, reason, detail, source, action)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )
    conn.commit()


def test_fetch_pending_plans_filters_symbol_prefix_and_venue() -> None:
    conn = sqlite3.connect(":memory:")
    try:
        _seed(conn)
        rows = _fetch_pending_plans(
            conn,
            after_id=0,
            limit=20,
            symbol_prefixes=("XYZ:",),
            allowed_venues=("hip3",),
        )
        assert len(rows) == 1
        assert rows[0].symbol == "XYZ:NVDA"
        assert rows[0].venue == "hip3"
    finally:
        conn.close()

