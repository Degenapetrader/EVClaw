#!/usr/bin/env python3
"""Regression tests for symbol_rr_learning safety guards."""

import sys
import tempfile
import time
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import symbol_rr_learning as srl
from symbol_rr_learning import SymbolRRLearner


def test_get_connection_sets_busy_timeout() -> None:
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    learner = SymbolRRLearner(db_path)
    conn = learner._get_connection()
    try:
        busy = conn.execute("PRAGMA busy_timeout").fetchone()[0]
        assert int(busy) == 30000
    finally:
        conn.close()


def test_fetch_oi_data_retry_cooldown_returns_fast_without_network(monkeypatch: pytest.MonkeyPatch) -> None:
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    learner = SymbolRRLearner(db_path)

    monkeypatch.setattr(srl, "_oi_cache", {})
    monkeypatch.setattr(srl, "_oi_cache_time", 0.0)
    monkeypatch.setattr(srl, "_oi_next_retry_ts", time.time() + 120.0)

    class FailRequests:
        @staticmethod
        def post(*_args, **_kwargs):
            raise AssertionError("network post should not be called during retry cooldown")

        @staticmethod
        def get(*_args, **_kwargs):
            raise AssertionError("network get should not be called during retry cooldown")

    monkeypatch.setitem(sys.modules, "requests", FailRequests)

    assert learner._fetch_oi_data() == {}


def test_fetch_oi_data_keeps_stale_cache_and_sets_retry_on_refresh_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    learner = SymbolRRLearner(db_path)

    stale = {"ETH": 123456.0}
    now = time.time()
    monkeypatch.setattr(srl, "_oi_cache", dict(stale))
    monkeypatch.setattr(srl, "_oi_cache_time", now - float(srl.OI_CACHE_TTL) - 10.0)
    monkeypatch.setattr(srl, "_oi_next_retry_ts", 0.0)
    monkeypatch.setattr(srl, "OI_FETCH_TOTAL_BUDGET_SEC", 0.5)
    monkeypatch.setattr(srl, "OI_FETCH_RETRY_COOLDOWN_SEC", 60.0)

    class BrokenRequests:
        @staticmethod
        def post(*_args, **_kwargs):
            raise RuntimeError("hl down")

        @staticmethod
        def get(*_args, **_kwargs):
            raise RuntimeError("binance down")

    monkeypatch.setitem(sys.modules, "requests", BrokenRequests)

    out = learner._fetch_oi_data()
    assert out == stale
    assert float(srl._oi_next_retry_ts) > now
