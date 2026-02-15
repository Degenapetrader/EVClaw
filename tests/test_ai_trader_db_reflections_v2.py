#!/usr/bin/env python3
"""reflections_v2 bootstrap/index regressions."""

import sqlite3
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from ai_trader_db import AITraderDB


def test_reflections_v2_bootstrap_dedupes_before_unique_index() -> None:
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE reflections_v2 (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                trade_id INTEGER NOT NULL,
                reflection_json TEXT,
                lesson_text TEXT,
                confidence TEXT,
                created_at REAL
            )
            """
        )
        conn.execute(
            "INSERT INTO reflections_v2 (trade_id, reflection_json, lesson_text, confidence, created_at) VALUES (1, '{}', 'a', 'low', 1)"
        )
        conn.execute(
            "INSERT INTO reflections_v2 (trade_id, reflection_json, lesson_text, confidence, created_at) VALUES (1, '{}', 'b', 'high', 2)"
        )
        conn.commit()

    AITraderDB(db_path)

    with sqlite3.connect(db_path) as conn:
        count = int(conn.execute("SELECT COUNT(*) FROM reflections_v2 WHERE trade_id = 1").fetchone()[0] or 0)
        idx = conn.execute(
            "SELECT sql FROM sqlite_master WHERE type='index' AND name='idx_reflections_v2_trade'"
        ).fetchone()
    assert count == 1
    assert idx is not None
    assert "UNIQUE INDEX" in str(idx[0]).upper()


def test_reflections_v2_reinit_keeps_rows_stable() -> None:
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    db = AITraderDB(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "INSERT OR REPLACE INTO reflections_v2 (trade_id, reflection_json, lesson_text, confidence) VALUES (?, ?, ?, ?)",
            (42, "{}", "hello", "mid"),
        )
        conn.commit()
        before = int(conn.execute("SELECT COUNT(*) FROM reflections_v2").fetchone()[0] or 0)

    AITraderDB(db_path)
    with sqlite3.connect(db_path) as conn:
        after = int(conn.execute("SELECT COUNT(*) FROM reflections_v2").fetchone()[0] or 0)

    assert before == after == 1

