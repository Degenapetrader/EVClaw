#!/usr/bin/env python3
"""Targeted tests for learning_reflector_worker queue processing."""

import asyncio
import sqlite3
import sys
import tempfile
from pathlib import Path
from unittest.mock import AsyncMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from ai_trader_db import AITraderDB
import learning_reflector_worker as worker


def _seed_closed_trade(db: AITraderDB, symbol: str = "ETH") -> int:
    trade_id = db.log_trade_entry(
        symbol=symbol,
        direction="LONG",
        entry_price=100.0,
        size=10.0,
        venue="hyperliquid",
        confidence="0.70",
        risk_pct_used=0.015,
        equity_at_entry=14000.0,
    )
    db.log_trade_exit(trade_id=trade_id, exit_price=102.0, exit_reason="TP", total_fees=0.2)
    return int(trade_id)


def test_learning_reflector_worker_processes_task_success_path() -> None:
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    db = AITraderDB(db_path)
    trade_id = _seed_closed_trade(db, symbol="ETH")
    assert db.enqueue_reflection_task(trade_id=trade_id, symbol="ETH") is True

    call_agent = AsyncMock(
        side_effect=[
            (
                {},
                {
                    "lesson_text": "Wait for pullback confirmation before adding size.",
                    "confidence": 0.82,
                    "reflection_json": {
                        "tags": ["entry_timing"],
                        "what_worked": "Trend alignment",
                        "what_failed": "Early add",
                        "next_time": ["Add only after reclaim"],
                        "sizing_note": "Reduce size on first breakout candle",
                    },
                },
            ),
            (
                {},
                {
                    "conclusion_text": "Prefer ETH longs on pullback reclaim; avoid immediate breakout adds.",
                    "confidence": 0.71,
                    "conclusion_json": {
                        "bias": "prefer longs",
                        "avoid": ["immediate breakout add"],
                        "prefer": ["pullback reclaim entries"],
                        "sizing": "half size on first impulse",
                        "notes": ["re-test confirmation improved outcomes"],
                    },
                },
            ),
        ]
    )

    with patch.object(worker, "_call_agent_json", call_agent), patch(
        "learning_dossier_aggregator.update_from_new_reflections",
        return_value=None,
    ):
        did = asyncio.run(
            worker.process_one_task(
                db=db,
                db_path=db_path,
                worker_id="test-worker",
                agent_id="hl-learning-reflector",
                thinking="high",
                max_attempts=5,
                stale_running_sec=600.0,
                retry_backoff_sec=30.0,
            )
        )
    assert did is True
    assert call_agent.await_count == 2

    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        reflection = conn.execute(
            "SELECT lesson_text, confidence FROM reflections_v2 WHERE trade_id = ?",
            (trade_id,),
        ).fetchone()
        conclusion = conn.execute(
            "SELECT conclusion_text, last_reflection_id_seen FROM symbol_conclusions_v1 WHERE symbol = ?",
            ("ETH",),
        ).fetchone()
        task = conn.execute(
            "SELECT status FROM reflection_tasks_v1 WHERE trade_id = ?",
            (trade_id,),
        ).fetchone()

    assert reflection is not None
    assert "pullback" in str(reflection["lesson_text"]).lower()
    assert float(reflection["confidence"]) > 0.8
    assert conclusion is not None
    assert int(conclusion["last_reflection_id_seen"] or 0) > 0
    assert task is not None
    assert task["status"] == "DONE"


def test_learning_reflector_worker_marks_error_and_defers_retry_by_backoff() -> None:
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    db = AITraderDB(db_path)
    trade_id = _seed_closed_trade(db, symbol="SOL")
    assert db.enqueue_reflection_task(trade_id=trade_id, symbol="SOL") is True

    with patch.object(worker, "_call_agent_json", AsyncMock(side_effect=RuntimeError("agent unavailable"))):
        did = asyncio.run(
            worker.process_one_task(
                db=db,
                db_path=db_path,
                worker_id="test-worker",
                agent_id="hl-learning-reflector",
                thinking="high",
                max_attempts=5,
                stale_running_sec=600.0,
                retry_backoff_sec=30.0,
            )
        )
    assert did is True

    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        task = conn.execute(
            "SELECT status, last_error FROM reflection_tasks_v1 WHERE trade_id = ?",
            (trade_id,),
        ).fetchone()

    assert task is not None
    assert task["status"] == "ERROR"
    assert "agent unavailable" in str(task["last_error"])

    # Backoff blocks immediate retry of the same ERROR task.
    assert (
        db.claim_reflection_task(
            worker_id="retry-worker",
            include_error=True,
            max_attempts=5,
            stale_running_sec=600.0,
            error_backoff_sec=30.0,
        )
        is None
    )
    assert (
        db.claim_reflection_task(
            worker_id="retry-worker",
            include_error=True,
            max_attempts=5,
            stale_running_sec=600.0,
            error_backoff_sec=0.0,
        )
        == trade_id
    )
