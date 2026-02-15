#!/usr/bin/env python3
"""Regression for Chase Limit cancel-fail fill accounting."""

import asyncio
import tempfile
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from executor import ExecutionConfig, Executor


class _CancelFailFilledAdapter:
    def __init__(self) -> None:
        self._mid_calls = 0
        self._status_calls = 0

    async def get_tick_size(self, symbol: str) -> float:
        return 1.0

    async def get_mid_price(self, symbol: str) -> float:
        self._mid_calls += 1
        if self._mid_calls == 1:
            return 100.0  # initial placement
        return 120.0  # forces retrigger

    async def place_limit_order(self, **kwargs):
        return True, "oid_1"

    async def check_order_status(self, symbol: str, order_id: str):
        self._status_calls += 1
        if self._status_calls == 1:
            # First poll: partial fill on current order.
            return {"status": "open", "filled_size": 4.0, "remaining_size": 6.0, "avg_price": 98.0}
        # Cancel failed then status says fully filled.
        return {"status": "filled", "filled_size": 6.0, "remaining_size": 0.0, "avg_price": 99.0}

    async def cancel_order(self, symbol: str, order_id: str) -> bool:
        return False


class _MidUnavailableUsesBestBidAskAdapter:
    def __init__(self) -> None:
        self.placed_prices = []

    async def get_tick_size(self, symbol: str) -> float:
        return 1.0

    async def get_mid_price(self, symbol: str) -> float:
        return 0.0

    async def get_best_bid_ask(self, symbol: str):
        return 200.0, 202.0

    async def place_limit_order(self, **kwargs):
        self.placed_prices.append(float(kwargs.get("price", 0.0) or 0.0))
        return True, "oid_fallback"

    async def check_order_status(self, symbol: str, order_id: str):
        return {"status": "filled", "filled_size": 1.0, "remaining_size": 0.0, "avg_price": 201.0}

    async def cancel_order(self, symbol: str, order_id: str) -> bool:
        return True


def test_chase_limit_cancel_fail_filled_does_not_overcount() -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        config = ExecutionConfig(
            dry_run=True,
            memory_dir=Path(tmpdir),
            chase_tick_offset=1,
            chase_retrigger_ticks=1,
            chase_poll_interval=0.01,
            chase_timeout=1.0,
            chase_entry_tif="Gtc",
        )
        ex = Executor(config=config)
        adapter = _CancelFailFilledAdapter()

        ok, filled_size, avg_price = asyncio.run(
            ex._chase_limit_fill(
                adapter=adapter,
                symbol="ETH",
                side="buy",
                size=10.0,
                reduce_only=False,
                fallback_bid=99.0,
                fallback_ask=101.0,
            )
        )

        assert ok is True
        # Expected total fill:
        # - first poll: 4.0
        # - cancel-fail/final status: cumulative 6.0 => delta 2.0
        # Total should be 6.0 (not 10.0).
        assert abs(filled_size - 6.0) < 1e-9
        assert avg_price > 0.0


def test_chase_limit_refreshes_fallback_quotes_when_mid_unavailable() -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        config = ExecutionConfig(
            dry_run=True,
            memory_dir=Path(tmpdir),
            chase_tick_offset=1,
            chase_retrigger_ticks=1,
            chase_poll_interval=0.01,
            chase_timeout=1.0,
            chase_entry_tif="Gtc",
        )
        ex = Executor(config=config)
        adapter = _MidUnavailableUsesBestBidAskAdapter()

        ok, filled_size, avg_price = asyncio.run(
            ex._chase_limit_fill(
                adapter=adapter,
                symbol="ETH",
                side="buy",
                size=1.0,
                reduce_only=False,
                fallback_bid=0.0,
                fallback_ask=0.0,
            )
        )

        assert ok is True
        assert abs(filled_size - 1.0) < 1e-9
        assert avg_price > 0.0
        assert adapter.placed_prices
        # Buy side with 1 tick offset from refreshed mid 201.0.
        assert abs(adapter.placed_prices[0] - 200.0) < 1e-9
