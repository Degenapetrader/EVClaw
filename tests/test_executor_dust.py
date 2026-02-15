#!/usr/bin/env python3
"""Executor dust guard test for Lighter SL/TP placement."""

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from executor import ExecutionConfig, ExecutionDecision, Executor
from exchanges import Position, VENUE_LIGHTER
from exchanges.lighter_adapter import LighterAdapter, LighterMarket


class DummyLighterAdapter(LighterAdapter):
    def __init__(self, log, position_size: float, min_base_amount: float) -> None:
        super().__init__(log, dry_run=False)
        self._initialized = True
        self._position_size = position_size
        self.stop_calls = 0

        market = LighterMarket(
            market_id=1,
            symbol="ETH",
            size_decimals=3,
            price_decimals=2,
            min_base_amount=min_base_amount,
            last_trade_price=100.0,
        )
        self._markets[market.symbol] = market
        self._markets[f"{market.symbol}-PERP"] = market
        self._market_ids[market.market_id] = market

    async def initialize(self) -> bool:
        return True

    async def get_mid_price(self, symbol: str) -> float:
        return 100.0

    async def get_position(self, symbol: str):
        return Position(
            symbol=symbol,
            direction="LONG",
            size=self._position_size,
            entry_price=100.0,
            unrealized_pnl=0.0,
        )

    async def place_stop_order(self, *args, **kwargs):
        self.stop_calls += 1
        return True, "stop_1"


def test_lighter_dust_skips_sltp() -> None:
    config = ExecutionConfig(
        dry_run=False,
        lighter_enabled=True,
        hl_enabled=False,
        write_positions_yaml=False,
        enable_trade_journal=False,
        enable_trade_tracker=False,
    )
    executor = Executor(config=config)

    adapter = DummyLighterAdapter(executor.log, position_size=0.01, min_base_amount=0.05)
    executor.lighter = adapter
    executor.router.lighter = adapter

    async def fake_chase_limit_fill(*args, **kwargs):
        return True, 1.0, 100.0

    executor._chase_limit_fill = fake_chase_limit_fill

    decision = ExecutionDecision(symbol="ETH", direction="LONG")
    decision.size_usd = 1000.0

    result = asyncio.run(
        executor.execute(
            decision=decision,
            best_bid=99.0,
            best_ask=101.0,
            atr=1.0,
            venue=VENUE_LIGHTER,
        )
    )

    assert result.success is False
    assert "ENTRY_UNDERFILLED" in (result.error or "")
    assert adapter.stop_calls == 0

    key = executor._position_key("ETH", VENUE_LIGHTER)
    position = executor._positions.get(key)
    assert position is not None
    assert position.state == "DUST"


def test_total_exposure_ignores_dust_notional() -> None:
    config = ExecutionConfig(dry_run=True, lighter_enabled=False, hl_enabled=False, dust_notional_usd=50.0)
    executor = Executor(config=config)
    # Inject two positions: one dust, one normal
    executor._positions = {
        "lighter:AAA": Position(
            symbol="AAA",
            direction="LONG",
            size=0.1,
            entry_price=100.0,
            state="ACTIVE",
            venue="lighter",
        ),
        "lighter:BBB": Position(
            symbol="BBB",
            direction="LONG",
            size=0.2,
            entry_price=400.0,
            state="ACTIVE",
            venue="lighter",
        ),
    }

    # AAA notional = 10 < dust threshold; BBB notional = 80
    exposure = executor.get_total_exposure()
    assert exposure == 80.0


if __name__ == "__main__":
    test_lighter_dust_skips_sltp()
    print("[PASS] executor dust guard")
