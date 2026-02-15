#!/usr/bin/env python3
"""Regression tests for /reconcile command behavior."""

import asyncio
import sys
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import cli


class _DummyPos:
    def __init__(self, size=1.0, entry=100.0, direction="LONG", venue="hyperliquid"):
        self.size = size
        self.entry_price = entry
        self.direction = direction
        self.venue = venue


class _DummyExecutor:
    last_instance = None

    def __init__(self, *args, **kwargs):
        self._positions = {}
        self.reconcile_called = False
        _DummyExecutor.last_instance = self

    async def initialize(self):
        self._positions = {"hyperliquid:ETH": _DummyPos()}
        return True

    async def _reconcile_positions_from_exchange(self):
        self.reconcile_called = True
        self._positions["hyperliquid:BTC"] = _DummyPos(size=2.0, entry=200.0)

    async def close(self):
        return None


def test_cmd_reconcile_runs_explicit_reconcile_pass(monkeypatch):
    monkeypatch.setattr(cli, "load_config", lambda: {"config": {"executor": {"use_db_positions": False}}})
    monkeypatch.setattr(cli, "build_execution_config", lambda _cfg, dry_run=False: object())
    monkeypatch.setattr(cli, "Executor", _DummyExecutor)

    class _DummyATR:
        async def close(self):
            return None

    monkeypatch.setattr(cli, "get_atr_service", lambda: _DummyATR())

    rc = asyncio.run(cli.cmd_reconcile(SimpleNamespace(dry_run=False)))
    assert rc == 0
    assert _DummyExecutor.last_instance is not None
    assert _DummyExecutor.last_instance.reconcile_called is True
