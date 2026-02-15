#!/usr/bin/env python3
"""Regression tests for close_position PnL sizing on fill-size mismatch."""

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from executor import ExecutionConfig, Executor
from exchanges import Position, VENUE_LIGHTER


class _CloseSizeMismatchAdapter:
    def __init__(self) -> None:
        self._pos_calls = 0

    async def cancel_all_orders(self, symbol: str) -> bool:
        return True

    async def get_position(self, symbol: str):
        self._pos_calls += 1
        if self._pos_calls == 1:
            return Position(
                symbol=symbol,
                direction="LONG",
                size=1.0,
                entry_price=100.0,
                unrealized_pnl=0.0,
            )
        return Position(
            symbol=symbol,
            direction="FLAT",
            size=0.0,
            entry_price=0.0,
            unrealized_pnl=0.0,
        )


def test_close_position_uses_requested_size_when_exchange_confirms_flat() -> None:
    config = ExecutionConfig(
        dry_run=True,
        lighter_enabled=True,
        hl_enabled=False,
        write_positions_yaml=False,
        enable_trade_journal=True,
        enable_trade_tracker=False,
    )
    executor = Executor(config=config)

    adapter = _CloseSizeMismatchAdapter()
    executor.lighter = adapter
    executor.router.lighter = adapter

    key = executor._position_key("ETH", VENUE_LIGHTER)
    executor._positions[key] = Position(
        symbol="ETH",
        direction="LONG",
        size=1.0,
        entry_price=100.0,
        state="ACTIVE",
        venue=VENUE_LIGHTER,
    )

    async def _fake_chase(*args, **kwargs):
        # Simulate under-reported fill size while exchange verifies FLAT.
        return True, 0.5, 110.0

    executor._chase_limit_fill = _fake_chase
    events = []
    executor.persistence.append_trade = lambda payload: events.append(payload) or True
    executor.persistence.save_positions_atomic = lambda _positions: True

    ok = asyncio.run(
        executor.close_position(
            symbol="ETH",
            reason="manual",
            best_bid=109.0,
            best_ask=110.0,
            venue=VENUE_LIGHTER,
        )
    )

    assert ok is True
    assert key not in executor._positions
    assert events, "Expected exit journal payload"
    exit_event = events[-1]
    assert abs(float(exit_event["size"]) - 1.0) < 1e-9
    assert abs(float(exit_event["reported_fill_size"]) - 0.5) < 1e-9
    assert abs(float(exit_event["pnl"]) - 10.0) < 1e-9
