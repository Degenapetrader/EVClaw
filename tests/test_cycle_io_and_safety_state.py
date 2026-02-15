#!/usr/bin/env python3
"""Cycle I/O and safety-state regressions."""

import sqlite3
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from ai_trader_db import AITraderDB
from cycle_io import load_cycle_file


def test_load_cycle_file_invalid_or_missing_returns_empty_dict(tmp_path) -> None:
    bad = tmp_path / "bad.json"
    bad.write_text("{invalid")
    assert load_cycle_file(str(bad)) == {}
    assert load_cycle_file(str(tmp_path / "missing.json")) == {}


def test_get_safety_state_recovers_missing_singleton_row() -> None:
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    db = AITraderDB(db_path, starting_equity=12_345.0)

    with sqlite3.connect(db_path) as conn:
        conn.execute("DELETE FROM safety_state WHERE id = 1")
        conn.commit()

    state = db.get_safety_state()
    assert state.current_tier == 1
    assert float(state.peak_equity) == 12_345.0
    assert float(state.current_equity) == 12_345.0

    with sqlite3.connect(db_path) as conn:
        row = conn.execute("SELECT COUNT(*) FROM safety_state WHERE id = 1").fetchone()
    assert int(row[0] or 0) == 1


def test_open_connection_enables_wal_and_foreign_keys() -> None:
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    db = AITraderDB(db_path)
    conn = db._get_connection()
    fk = int(conn.execute("PRAGMA foreign_keys").fetchone()[0] or 0)
    journal_mode = str(conn.execute("PRAGMA journal_mode").fetchone()[0] or "").lower()
    assert fk == 1
    assert journal_mode == "wal"

