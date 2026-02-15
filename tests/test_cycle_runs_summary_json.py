#!/usr/bin/env python3

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from ai_trader_db import AITraderDB


def _fetch_cycle_run(db: AITraderDB, seq: int):
    with db._get_connection() as conn:
        row = conn.execute("SELECT * FROM cycle_runs WHERE seq = ?", (int(seq),)).fetchone()
    return dict(row) if row else None


def test_mark_cycle_processed_wraps_non_json_summary(tmp_path):
    db_path = tmp_path / "test.db"
    db = AITraderDB(str(db_path))

    db.insert_cycle_run(seq=1, cycle_file="/tmp/cycle.json")
    ok = db.mark_cycle_processed(seq=1, status="PROPOSED", summary="hello world")
    assert ok

    row = _fetch_cycle_run(db, 1)
    assert row is not None
    summary = row.get("processed_summary")
    assert isinstance(summary, str)

    parsed = json.loads(summary)
    assert parsed.get("text") == "hello world"


def test_db_init_sanitizes_legacy_cycle_summary(tmp_path):
    db_path = tmp_path / "test2.db"

    db = AITraderDB(str(db_path))
    db.insert_cycle_run(seq=2, cycle_file="/tmp/cycle2.json")
    db.mark_cycle_processed(seq=2, status="PROPOSED", summary=json.dumps({"ok": True}))

    # Manually corrupt processed_summary to emulate legacy/manual writes.
    with db._get_connection() as conn:
        conn.execute("update cycle_runs set processed_summary=? where seq=2", ("not json",))
        conn.commit()

    # Re-open DB (runs sanitization in _init_db).
    db2 = AITraderDB(str(db_path))
    row = _fetch_cycle_run(db2, 2)
    assert row is not None
    summary = row.get("processed_summary")
    parsed = json.loads(summary)
    assert parsed.get("text") == "not json"
