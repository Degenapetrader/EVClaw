#!/usr/bin/env python3
"""Targeted reliability tests for run_fill_reconciler orchestration."""

import asyncio
import logging
import os
import sqlite3
import time
import tempfile
from pathlib import Path
from unittest.mock import patch
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from ai_trader_db import AITraderDB
from run_fill_reconciler import build_on_trade_close, run_loop


class _FailingReconciler:
    venue = "hyperliquid"

    def __init__(self) -> None:
        self.stopped = False

    async def start(self) -> None:
        await asyncio.sleep(0.01)
        raise RuntimeError("boom")

    def stop(self) -> None:
        self.stopped = True


class _IdleStreamer:
    venue = "hyperliquid"

    def __init__(self) -> None:
        self.stopped = False

    async def start(self) -> None:
        while True:
            await asyncio.sleep(1.0)

    def stop(self) -> None:
        self.stopped = True


def test_on_trade_close_reflection_enqueue_is_non_blocking() -> None:
    """Reflection enqueue must not block close callback path."""
    cb = build_on_trade_close(
        db_path=":memory:",
        learning_engine=None,
        log=logging.getLogger("test.run_fill_reconciler"),
    )

    async def _run_once() -> float:
        # Simulate a slow enqueue operation; callback should still return quickly.
        with patch.object(AITraderDB, "enqueue_reflection_task", side_effect=lambda *a, **k: time.sleep(0.2)):
            start = time.perf_counter()
            cb({"trade_id": 123, "symbol": "ETH", "venue": "hyperliquid", "direction": "LONG"})
            elapsed = time.perf_counter() - start
            await asyncio.sleep(0.3)
            return elapsed

    elapsed = asyncio.run(_run_once())
    assert elapsed < 0.1


def test_on_trade_close_reflection_enqueue_best_effort_on_error() -> None:
    """Reflection enqueue errors must not break the close callback path."""
    cb = build_on_trade_close(
        db_path=":memory:",
        learning_engine=None,
        log=logging.getLogger("test.run_fill_reconciler"),
    )

    async def _run_once() -> None:
        with patch.object(AITraderDB, "enqueue_reflection_task", side_effect=RuntimeError("boom")):
            cb({"trade_id": 456, "symbol": "SOL", "venue": "hyperliquid", "direction": "SHORT"})
            await asyncio.sleep(0.1)

    asyncio.run(_run_once())
def test_on_trade_close_persists_trade_features() -> None:
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    db = AITraderDB(db_path)
    trade_id = db.log_trade_entry(
        symbol="ETH",
        direction="LONG",
        entry_price=3000.0,
        size=1.0,
        venue="hyperliquid",
        confidence="0.74",
        risk_pct_used=0.01,
        equity_at_entry=10000.0,
    )
    db.log_trade_exit(trade_id, exit_price=3030.0, exit_reason="TP", total_fees=1.0)

    cb = build_on_trade_close(
        db_path=db_path,
        learning_engine=None,
        log=logging.getLogger("test.run_fill_reconciler"),
    )

    async def _run_once() -> None:
        with patch("subprocess.run", return_value=None):
            cb({"trade_id": trade_id, "symbol": "ETH", "venue": "hyperliquid", "direction": "LONG"})
            await asyncio.sleep(0.2)

    asyncio.run(_run_once())
    rows = db.get_trade_features_window(window_start=time.time() - 600, window_end=time.time() + 5, limit=10)
    assert rows
    assert rows[0]["trade_id"] == trade_id
    assert rows[0]["conviction"] == 0.74


def test_on_trade_close_triggers_adaptive_updater() -> None:
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    db = AITraderDB(db_path)
    trade_id = db.log_trade_entry(
        symbol="ETH",
        direction="LONG",
        entry_price=3000.0,
        size=1.0,
        venue="hyperliquid",
        confidence="0.74",
    )
    db.log_trade_exit(trade_id, exit_price=3030.0, exit_reason="TP", total_fees=1.0)

    cb = build_on_trade_close(
        db_path=db_path,
        learning_engine=None,
        log=logging.getLogger("test.run_fill_reconciler"),
    )

    calls = {"n": 0}

    class _R:
        status = "noop"
        updated = False
        reason = "test"
        sample_count = 1

    def _fake_update(_db):
        calls["n"] += 1
        return _R()

    async def _run_once() -> None:
        with patch("subprocess.run", return_value=None), patch(
            "run_fill_reconciler.run_adaptive_conviction_update",
            side_effect=_fake_update,
        ):
            cb({"trade_id": trade_id, "symbol": "ETH", "venue": "hyperliquid", "direction": "LONG"})
            await asyncio.sleep(0.2)

    asyncio.run(_run_once())
    assert calls["n"] >= 1


def test_on_trade_close_adaptive_waits_for_feature_capture() -> None:
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    db = AITraderDB(db_path)
    trade_id = db.log_trade_entry(
        symbol="ETH",
        direction="LONG",
        entry_price=3000.0,
        size=1.0,
        venue="hyperliquid",
        confidence="0.74",
        risk_pct_used=0.01,
        equity_at_entry=10000.0,
    )
    db.log_trade_exit(trade_id, exit_price=3030.0, exit_reason="TP", total_fees=1.0)

    cb = build_on_trade_close(
        db_path=db_path,
        learning_engine=None,
        log=logging.getLogger("test.run_fill_reconciler"),
    )

    observed = {"feature_rows_seen": 0}

    class _R:
        status = "noop"
        updated = False
        reason = "test"
        sample_count = 1

    orig_insert = AITraderDB.insert_trade_features

    def _slow_insert(self, *args, **kwargs):
        time.sleep(0.1)
        return orig_insert(self, *args, **kwargs)

    def _fake_update(update_db):
        rows = update_db.get_trade_features_window(
            window_start=time.time() - 600,
            window_end=time.time() + 5,
            limit=10,
        )
        observed["feature_rows_seen"] = len(rows)
        return _R()

    async def _run_once() -> None:
        with patch("subprocess.run", return_value=None), patch.object(
            AITraderDB,
            "insert_trade_features",
            _slow_insert,
        ), patch(
            "run_fill_reconciler.run_adaptive_conviction_update",
            side_effect=_fake_update,
        ):
            cb({"trade_id": trade_id, "symbol": "ETH", "venue": "hyperliquid", "direction": "LONG"})
            await asyncio.sleep(0.4)

    asyncio.run(_run_once())
    assert observed["feature_rows_seen"] >= 1


def test_on_trade_close_triggers_symbol_rr_refresh() -> None:
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    db = AITraderDB(db_path)
    trade_id = db.log_trade_entry(
        symbol="ETH",
        direction="LONG",
        entry_price=3000.0,
        size=1.0,
        venue="hyperliquid",
        confidence="0.74",
    )
    db.log_trade_exit(trade_id, exit_price=3030.0, exit_reason="TP", total_fees=1.0)

    cb = build_on_trade_close(
        db_path=db_path,
        learning_engine=None,
        log=logging.getLogger("test.run_fill_reconciler"),
    )

    calls = {"rr": 0}

    async def _fake_mae(*_args, **_kwargs):
        return None

    def _fake_rr(*_args, **_kwargs):
        calls["rr"] += 1
        return False

    async def _run_once() -> None:
        with patch("subprocess.run", return_value=None), patch(
            "mae_mfe.compute_and_store_mae_mfe",
            side_effect=_fake_mae,
        ), patch(
            "run_fill_reconciler.update_symbol_policy_for_symbol",
            side_effect=_fake_rr,
        ):
            cb({"trade_id": trade_id, "symbol": "ETH", "venue": "hyperliquid", "direction": "LONG"})
            await asyncio.sleep(0.25)

    asyncio.run(_run_once())
    assert calls["rr"] >= 1


def test_on_trade_close_finalizes_entry_gate_outcome() -> None:
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    db = AITraderDB(db_path)
    gate_decision_id = db.insert_gate_decision(
        cycle_seq=9001,
        symbol="ETH",
        direction="LONG",
        decision="PICK",
        reason="gate pick",
        candidate_rank=1,
        conviction=0.7,
    )
    trade_id = db.log_trade_entry(
        symbol="ETH",
        direction="LONG",
        entry_price=3000.0,
        size=1.0,
        venue="hyperliquid",
        context_snapshot={
            "entry_gate": {
                "gate_decision_id": gate_decision_id,
                "proposal_id": 777,
                "venue": "hyperliquid",
            }
        },
    )
    db.log_trade_exit(trade_id, exit_price=3030.0, exit_reason="TP", total_fees=0.0)

    cb = build_on_trade_close(
        db_path=db_path,
        learning_engine=None,
        log=logging.getLogger("test.run_fill_reconciler"),
    )

    async def _run_once() -> None:
        with patch("subprocess.run", return_value=None):
            cb({"trade_id": trade_id, "symbol": "ETH", "venue": "hyperliquid", "direction": "LONG"})
            await asyncio.sleep(0.25)

    asyncio.run(_run_once())

    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            """
            SELECT trade_id, proposal_id, venue, outcome_status, outcome_pnl, outcome_pnl_pct
            FROM gate_decisions_v1
            WHERE id = ?
            """,
            (int(gate_decision_id),),
        ).fetchone()
    assert row is not None
    assert int(row["trade_id"] or 0) == int(trade_id)
    assert int(row["proposal_id"] or 0) == 777
    assert str(row["venue"] or "").lower() == "hyperliquid"
    assert str(row["outcome_status"] or "") == "WIN"
    assert float(row["outcome_pnl"] or 0.0) > 0.0


def test_run_loop_stops_when_worker_crashes() -> None:
    """run_loop should stop when a worker task exits unexpectedly."""
    reconciler = _FailingReconciler()
    streamer = _IdleStreamer()

    asyncio.run(
        asyncio.wait_for(
            run_loop([reconciler], [streamer], logging.getLogger("test.run_fill_reconciler")),
            timeout=1.5,
        )
    )

    assert reconciler.stopped is True
    assert streamer.stopped is True
