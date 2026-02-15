#!/usr/bin/env python3
"""Tests for LighterAdapter market order fill verification."""

import asyncio
import logging
import sys
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from exchanges.lighter_adapter import LighterAdapter, LighterMarket


class DummyResp:
    def __init__(self, code: int = 200, message: str = "ok") -> None:
        self.code = code
        self.message = message


class DummyClient:
    async def create_market_order(self, **kwargs):
        return None, DummyResp(200), None

    def create_auth_token_with_expiry(self):
        return "token", None


class DummyOrderAPI:
    async def trades(self, **kwargs):
        return SimpleNamespace(trades=[])


def _build_adapter(min_base: float) -> LighterAdapter:
    adapter = LighterAdapter(log=logging.getLogger("test"), dry_run=False)
    market = LighterMarket(
        market_id=1,
        symbol="ETH",
        size_decimals=2,
        price_decimals=2,
        min_base_amount=min_base,
        last_trade_price=100.0,
    )
    adapter._markets[market.symbol] = market
    adapter._market_ids[market.market_id] = market
    adapter._initialized = True
    adapter._client = DummyClient()
    adapter._order_api = DummyOrderAPI()
    adapter._account_index = 1
    return adapter


def test_place_market_order_rejects_below_min() -> None:
    adapter = _build_adapter(min_base=0.05)
    ok, order_id, filled_size, filled_price = asyncio.run(
        adapter.place_market_order(
            symbol="ETH",
            side="buy",
            size=0.01,
            reduce_only=False,
            best_bid=99.0,
            best_ask=101.0,
        )
    )
    assert ok is False
    assert order_id is None
    assert filled_size == 0.0
    assert filled_price == 0.0


def test_place_market_order_no_fills_returns_false() -> None:
    adapter = _build_adapter(min_base=0.05)
    ok, order_id, filled_size, filled_price = asyncio.run(
        adapter.place_market_order(
            symbol="ETH",
            side="buy",
            size=0.10,
            reduce_only=False,
            best_bid=99.0,
            best_ask=101.0,
        )
    )
    assert ok is False
    assert order_id is not None
    assert filled_size == 0.0
    assert filled_price == 0.0


if __name__ == "__main__":
    test_place_market_order_rejects_below_min()
    test_place_market_order_no_fills_returns_false()
    print("[PASS] lighter market order verification")
