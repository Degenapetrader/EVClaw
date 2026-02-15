#!/usr/bin/env python3
"""Pytest coverage for enhanced_learning_engine core behavior."""

import sqlite3
import sys
import tempfile
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from enhanced_learning_engine import EnhancedLearningEngine


def _create_schema(db_path: Path) -> None:
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE trades (
                id INTEGER PRIMARY KEY,
                symbol TEXT,
                direction TEXT,
                entry_time REAL,
                entry_price REAL,
                exit_time REAL,
                exit_price REAL,
                exit_reason TEXT,
                realized_pnl REAL,
                realized_pnl_pct REAL,
                signals_agreed TEXT,
                signals_snapshot TEXT,
                context_snapshot TEXT,
                ai_reasoning TEXT,
                confidence TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE signal_combos (
                id INTEGER PRIMARY KEY,
                combo_key TEXT UNIQUE,
                trades INTEGER DEFAULT 0,
                wins INTEGER DEFAULT 0,
                total_pnl REAL DEFAULT 0,
                avg_pnl REAL DEFAULT 0,
                win_rate REAL DEFAULT 0,
                last_trade_at REAL,
                updated_at REAL
            )
            """
        )
        conn.commit()


def test_combo_stats_update_and_accumulate() -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.db"
        _create_schema(db_path)

        with sqlite3.connect(db_path) as conn:
            conn.execute(
                """
                INSERT INTO trades (id, symbol, direction, entry_time, entry_price,
                    exit_time, exit_price, exit_reason, realized_pnl, realized_pnl_pct,
                    signals_agreed, ai_reasoning, confidence)
                VALUES (1, 'ETH', 'LONG', ?, 2500, ?, 2575, 'TP', 37.5, 3.0,
                    '["cvd", "fade", "whale"]', 'Strong CVD + FADE alignment', 'HIGH')
                """,
                (time.time() - 7200, time.time() - 3600),
            )
            conn.execute(
                """
                INSERT INTO trades (id, symbol, direction, entry_time, entry_price,
                    exit_time, exit_price, exit_reason, realized_pnl, realized_pnl_pct,
                    signals_agreed, ai_reasoning, confidence)
                VALUES (2, 'BTC', 'SHORT', ?, 42000, ?, 42500, 'SL', -50.0, -1.2,
                    '["cvd", "liq_pnl"]', 'CVD showing weakness', 'MEDIUM')
                """,
                (time.time() - 3600, time.time() - 1800),
            )
            conn.commit()

        engine = EnhancedLearningEngine(db_path=str(db_path))
        try:
            assert engine.update_combo_stats(1) == "cvd+fade+whale"
            assert engine.update_combo_stats(2) == "cvd+liq_pnl"
            stats = engine.get_combo_stats(min_trades=1)
            assert len(stats) == 2

            with sqlite3.connect(db_path) as conn:
                conn.execute(
                    """
                    INSERT INTO trades (id, symbol, direction, entry_time, entry_price,
                        exit_time, exit_price, exit_reason, realized_pnl, realized_pnl_pct,
                        signals_agreed)
                    VALUES (3, 'SOL', 'LONG', ?, 100, ?, 103, 'TP', 15.0, 3.0,
                        '["cvd", "fade", "whale"]')
                    """,
                    (time.time() - 1800, time.time() - 900),
                )
                conn.commit()

            engine.update_combo_stats(3)
            stats2 = engine.get_combo_stats(min_trades=1)
            combo = next((s for s in stats2 if s.combo_key == "cvd+fade+whale"), None)
            assert combo is not None
            assert combo.trades == 2
            assert combo.win_rate == 1.0
        finally:
            engine.close()
