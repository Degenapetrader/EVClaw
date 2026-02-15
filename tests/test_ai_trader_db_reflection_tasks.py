#!/usr/bin/env python3
"""Regression tests for reflection task queue claim/retry semantics."""

import sqlite3
import sys
import tempfile
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from ai_trader_db import AITraderDB


def _task_row(db_path: str, trade_id: int) -> sqlite3.Row:
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT * FROM reflection_tasks_v1 WHERE trade_id = ?",
            (int(trade_id),),
        ).fetchone()
    assert row is not None
    return row


def test_reflection_task_claim_reclaims_stale_running_and_clears_lock_fields() -> None:
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    db = AITraderDB(db_path)

    assert db.enqueue_reflection_task(trade_id=101, symbol="eth") is True
    assert (
        db.claim_reflection_task(
            worker_id="worker-a",
            include_error=True,
            max_attempts=5,
            stale_running_sec=600,
            error_backoff_sec=0,
        )
        == 101
    )
    assert (
        db.claim_reflection_task(
            worker_id="worker-b",
            include_error=True,
            max_attempts=5,
            stale_running_sec=600,
            error_backoff_sec=0,
        )
        is None
    )

    stale_ts = time.time() - 1200
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "UPDATE reflection_tasks_v1 SET locked_at = ?, updated_at = ? WHERE trade_id = ?",
            (stale_ts, stale_ts, 101),
        )
        conn.commit()

    assert (
        db.claim_reflection_task(
            worker_id="worker-c",
            include_error=True,
            max_attempts=5,
            stale_running_sec=60,
            error_backoff_sec=0,
        )
        == 101
    )
    assert db.mark_reflection_task_error(trade_id=101, error="boom") is True

    row = _task_row(db_path, 101)
    assert row["status"] == "ERROR"
    assert row["locked_by"] is None
    assert row["locked_at"] is None

    assert (
        db.claim_reflection_task(
            worker_id="worker-d",
            include_error=True,
            max_attempts=5,
            stale_running_sec=60,
            error_backoff_sec=0,
        )
        == 101
    )
    assert db.mark_reflection_task_done(trade_id=101) is True

    row = _task_row(db_path, 101)
    assert row["status"] == "DONE"
    assert row["last_error"] is None
    assert row["locked_by"] is None
    assert row["locked_at"] is None


def test_reflection_task_claim_prioritizes_pending_and_respects_error_backoff() -> None:
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    db = AITraderDB(db_path)

    assert db.enqueue_reflection_task(trade_id=1, symbol="btc") is True
    assert db.mark_reflection_task_error(trade_id=1, error="transient failure") is True
    assert db.enqueue_reflection_task(trade_id=2, symbol="eth") is True

    # Pending tasks should be preferred ahead of retryable ERROR tasks.
    claimed = db.claim_reflection_task(
        worker_id="worker-a",
        include_error=True,
        max_attempts=5,
        stale_running_sec=600,
        error_backoff_sec=60,
    )
    assert claimed == 2
    assert db.mark_reflection_task_done(trade_id=2) is True

    # Recent ERROR should not be retried until backoff expires.
    assert (
        db.claim_reflection_task(
            worker_id="worker-b",
            include_error=True,
            max_attempts=5,
            stale_running_sec=600,
            error_backoff_sec=60,
        )
        is None
    )
    assert (
        db.claim_reflection_task(
            worker_id="worker-b",
            include_error=False,
            max_attempts=5,
            stale_running_sec=600,
            error_backoff_sec=0,
        )
        is None
    )

    old_ts = time.time() - 120
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "UPDATE reflection_tasks_v1 SET updated_at = ? WHERE trade_id = ?",
            (old_ts, 1),
        )
        conn.commit()

    claimed = db.claim_reflection_task(
        worker_id="worker-c",
        include_error=True,
        max_attempts=5,
        stale_running_sec=600,
        error_backoff_sec=30,
    )
    assert claimed == 1
