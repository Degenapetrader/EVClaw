#!/usr/bin/env python3
"""AITraderDB lock/stat update regressions."""

import sys
import tempfile
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import ai_trader_db as ai_db_module
from ai_trader_db import AITraderDB


def test_acquire_symbol_lock_reentrant_and_conflict() -> None:
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    db = AITraderDB(db_path)
    assert db.acquire_symbol_lock("BTC", "hyperliquid", "workerA", ttl_seconds=30.0) is True
    assert db.acquire_symbol_lock("BTC", "hyperliquid", "workerB", ttl_seconds=30.0) is False
    assert db.acquire_symbol_lock("BTC", "hyperliquid", "workerA", ttl_seconds=30.0) is True
    assert db.release_symbol_lock("BTC", "hyperliquid", owner="workerA") is True


def test_update_dead_capital_imbalance_stats_updates_n_and_z() -> None:
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    db = AITraderDB(db_path)
    now = time.time()
    first = db.update_dead_capital_imbalance_stats(
        symbol="BTC",
        venue="hyperliquid",
        ts=now,
        imbalance=1.0,
        min_samples=1,
    )
    second = db.update_dead_capital_imbalance_stats(
        symbol="BTC",
        venue="hyperliquid",
        ts=now + 60.0,
        imbalance=2.0,
        min_samples=1,
    )
    assert int(first["n"]) == 1
    assert int(second["n"]) == 2
    assert "z" in second


def test_get_connection_resets_handles_after_pid_change(monkeypatch) -> None:
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    db = AITraderDB(db_path)
    conn1 = db._get_connection()
    pid0 = db._conn_pid

    monkeypatch.setattr(ai_db_module.os, "getpid", lambda: int(pid0) + 1000)
    conn2 = db._get_connection()

    assert conn2 is not conn1
    assert int(db._conn_pid) == int(pid0) + 1000

