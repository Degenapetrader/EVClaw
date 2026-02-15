#!/usr/bin/env python3
"""Regression tests for safe CLI close behavior."""

import asyncio
import sys
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import cli


class _DummyPos:
    venue = "lighter"


class _DummyExecutor:
    last_instance = None

    def __init__(self, *args, **kwargs):
        self.close_position_called = False
        _DummyExecutor.last_instance = self

    async def initialize(self):
        return True

    async def get_position(self, symbol, venue=None):
        return _DummyPos()

    async def close_position(self, **kwargs):
        self.close_position_called = True
        return True

    async def close(self):
        return None


def test_cmd_positions_refuses_close_without_live_prices(monkeypatch):
    monkeypatch.setattr(cli, "load_config", lambda: {"config": {"executor": {"use_db_positions": False}}})
    monkeypatch.setattr(cli, "get_db", lambda _cfg: None)
    monkeypatch.setattr(cli, "load_memory_file", lambda _name: {})
    monkeypatch.setattr(cli, "build_execution_config", lambda _cfg, dry_run=False: object())
    monkeypatch.setattr(cli, "Executor", _DummyExecutor)

    async def _no_prices(_executor, _symbol, _venue):
        return 0.0, 0.0

    monkeypatch.setattr(cli, "_fetch_current_prices", _no_prices)

    args = SimpleNamespace(
        close="ETH",
        dry_run=False,
        venue=None,
        reason=None,
        detail="",
    )
    rc = asyncio.run(cli.cmd_positions(args))
    assert rc == 1
    assert _DummyExecutor.last_instance is not None
    assert _DummyExecutor.last_instance.close_position_called is False
