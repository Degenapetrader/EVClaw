#!/usr/bin/env python3
"""Tests for proposal tracking schema and helpers."""

import sqlite3
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from ai_trader_db import AITraderDB


def test_proposal_tables_created() -> None:
    """Fresh DB includes proposal and monitoring tables."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    AITraderDB(db_path)
    conn = sqlite3.connect(db_path)

    tables = {
        row[0]
        for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
    }
    assert "cycle_runs" in tables
    assert "trade_proposals" in tables
    assert "proposal_executions" in tables
    assert "execution_attempts_v1" in tables
    assert "sltp_incidents_v1" in tables
    assert "monitor_snapshots" in tables
    assert "monitor_positions" in tables

    conn.close()


def test_insert_and_fetch_proposal() -> None:
    """Insert proposal and fetch by cycle seq."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    db = AITraderDB(db_path)
    proposal_id = db.insert_proposal(
        cycle_seq=123,
        symbol="ETH",
        venue="lighter",
        direction="LONG",
        size_usd=25.0,
        sl=1900.0,
        tp=2200.0,
        conviction=0.7,
        reason_short="test",
        signals=["cvd", "fade"],
    )
    assert proposal_id > 0

    proposals = db.get_proposals_for_cycle(123)
    assert len(proposals) == 1
    proposal = proposals[0]
    assert proposal["symbol"] == "ETH"
    assert proposal["venue"] == "lighter"
    assert proposal["direction"] == "LONG"
    assert proposal["size_usd"] == 25.0
    assert proposal["sl"] == 1900.0
    assert proposal["tp"] == 2200.0
    assert proposal["status"] == "PROPOSED"
    assert "cvd" in proposal["signals"]


def test_insert_execution_result_writes_execution_attempt() -> None:
    """Legacy proposal execution insert also writes normalized attempt telemetry."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    db = AITraderDB(db_path)
    proposal_id = db.insert_proposal(
        cycle_seq=999,
        symbol="ETH",
        venue="lighter",
        direction="LONG",
        size_usd=50.0,
        sl=None,
        tp=None,
        conviction=0.6,
        reason_short="test exec result",
        signals=["cvd"],
    )
    assert proposal_id > 0

    exec_id = db.insert_execution_result(
        proposal_id=proposal_id,
        started_at=1000.0,
        finished_at=1001.5,
        success=False,
        error="dry_run",
        exchange="lighter",
        entry_price=2000.0,
        size=0.02,
        sl_price=None,
        tp_price=None,
    )
    assert exec_id > 0

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    row = conn.execute(
        """
        SELECT proposal_id, symbol, venue, action, success, error_code, error_text, elapsed_ms,
               is_builder_symbol, builder_error_category
        FROM execution_attempts_v1
        ORDER BY id DESC
        LIMIT 1
        """
    ).fetchone()
    conn.close()

    assert row is not None
    assert int(row["proposal_id"]) == int(proposal_id)
    assert row["symbol"] == "ETH"
    assert row["venue"] == "lighter"
    assert row["action"] == "entry"
    assert int(row["success"]) == 0
    assert row["error_code"] == "dry_run"
    assert row["error_text"] == "dry_run"
    assert float(row["elapsed_ms"]) == 1500.0
    assert int(row["is_builder_symbol"]) == 0
    assert row["builder_error_category"] is None


def test_builder_execution_attempt_has_category() -> None:
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    db = AITraderDB(db_path)
    proposal_id = db.insert_proposal(
        cycle_seq=1001,
        symbol="XYZ:NVDA",
        venue="hip3",
        direction="LONG",
        size_usd=120.0,
        sl=None,
        tp=None,
        conviction=0.7,
        reason_short="builder diagnostic test",
        signals=["hip3_main"],
    )
    assert proposal_id > 0

    db.insert_execution_result(
        proposal_id=proposal_id,
        started_at=2000.0,
        finished_at=2002.0,
        success=False,
        error="position verification failed (phantom trade)",
        exchange="hip3",
        entry_price=50.0,
        size=2.4,
        sl_price=None,
        tp_price=None,
    )

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    row = conn.execute(
        """
        SELECT symbol, is_builder_symbol, builder_error_category
        FROM execution_attempts_v1
        ORDER BY id DESC
        LIMIT 1
        """
    ).fetchone()
    conn.close()

    assert row is not None
    assert row["symbol"] == "XYZ:NVDA"
    assert int(row["is_builder_symbol"]) == 1
    assert row["builder_error_category"] == "verification"


def test_sltp_incident_open_and_resolve() -> None:
    """SL/TP incidents can be opened, refreshed, and resolved."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    db = AITraderDB(db_path)
    trade_id = db.log_trade_entry(
        symbol="ETH",
        direction="LONG",
        entry_price=2000.0,
        size=0.01,
        venue="lighter",
    )
    assert trade_id > 0

    incident_id = db.open_sltp_incident(
        trade_id=trade_id,
        symbol="ETH",
        venue="lighter",
        leg="SL",
        attempts=3,
        last_error="sl_placement_failed_after_retries",
    )
    assert incident_id > 0

    # Refresh same open incident should not create a new row.
    same_id = db.open_sltp_incident(
        trade_id=trade_id,
        symbol="ETH",
        venue="lighter",
        leg="SL",
        attempts=4,
        last_error="sl_retry_failed_again",
    )
    assert same_id == incident_id

    resolved = db.resolve_sltp_incident(
        trade_id=trade_id,
        leg="SL",
        resolution_note="manual_fix_applied",
    )
    assert resolved == 1

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    row = conn.execute(
        """
        SELECT state, attempts, last_error, resolution_note, resolved_at
        FROM sltp_incidents_v1
        WHERE id = ?
        """,
        (incident_id,),
    ).fetchone()
    conn.close()

    assert row is not None
    assert row["state"] == "RESOLVED"
    assert int(row["attempts"]) == 4
    assert row["last_error"] == "sl_retry_failed_again"
    assert row["resolution_note"] == "manual_fix_applied"
    assert row["resolved_at"] is not None
