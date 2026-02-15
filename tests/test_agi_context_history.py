#!/usr/bin/env python3
"""Regression tests for AGI context pct_24h history bootstrap."""

import json
import sqlite3
import tempfile
import time
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import agi_context


def test_load_pct_24h_history_skips_malformed_rows() -> None:
    with tempfile.TemporaryDirectory() as td:
        db_path = Path(td) / "test.db"
        now = time.time()
        good_ctx = json.dumps({"key_metrics": {"pct_24h": 1.25}})
        bad_ctx = "{bad-json"

        with sqlite3.connect(db_path) as conn:
            conn.execute(
                """
                CREATE TABLE trades (
                    symbol TEXT,
                    entry_time REAL,
                    context_snapshot TEXT
                )
                """
            )
            conn.executemany(
                "INSERT INTO trades (symbol, entry_time, context_snapshot) VALUES (?, ?, ?)",
                [
                    ("BTC", now, good_ctx),
                    ("ETH", now, bad_ctx),
                ],
            )
            conn.commit()

        agi_context._pct_24h_history.clear()
        agi_context.load_pct_24h_history_from_trades(str(db_path), days=30)

        assert agi_context._pct_24h_history.get("BTC") == [1.25]
        assert "ETH" not in agi_context._pct_24h_history


def test_calculate_zscore_uses_sample_std() -> None:
    values = [1.0, 2.0, 3.0, 4.0, 5.0]
    z = agi_context._calculate_zscore(values, 5.0)
    # mean=3, sample_std=sqrt(2.5)=1.5811 => z≈1.2649
    assert abs(z - 1.2649110640673518) < 1e-6


def test_pct_24h_history_prunes_stale_symbols() -> None:
    agi_context._pct_24h_history.clear()
    agi_context._pct_24h_last_seen.clear()

    agi_context._pct_24h_history["OLD"] = [1.0]
    agi_context._pct_24h_last_seen["OLD"] = 1.0
    agi_context._pct_24h_history["NEW"] = [2.0]
    agi_context._pct_24h_last_seen["NEW"] = 1_000_000.0

    now_ts = 1_000_000.0 + agi_context._HISTORY_STALE_SECONDS + 10.0
    agi_context._pct_24h_last_seen["NEW"] = now_ts - 5.0
    agi_context._prune_pct_24h_history(now_ts=now_ts)

    assert "OLD" not in agi_context._pct_24h_history
    assert "NEW" in agi_context._pct_24h_history
