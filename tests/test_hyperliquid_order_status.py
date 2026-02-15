#!/usr/bin/env python3
"""Hyperliquid order status edge-case tests."""

import asyncio
import logging
import sys
from pathlib import Path
from typing import Any, Dict

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from exchanges.hyperliquid_adapter import HyperliquidAdapter  # noqa: E402


class DummyHLAdapter(HyperliquidAdapter):
    """Minimal adapter stub to test check_order_status logic."""

    def __init__(self) -> None:
        super().__init__(log=logging.getLogger("dummy_hl"), dry_run=False)
        self._initialized = True
        self._address = "0xdeadbeef"

    async def _post_public(self, payload: Dict[str, Any]) -> Any:
        kind = payload.get("type")
        if kind == "frontendOpenOrders":
            return []
        if kind == "orderStatus":
            return {
                "status": "order",
                "order": {
                    "status": "filledorresting",
                    "order": {"origSz": 10, "sz": 5, "limitPx": 100},
                },
            }
        return {}


class DummyCancelAllAdapter(HyperliquidAdapter):
    def __init__(self) -> None:
        super().__init__(log=logging.getLogger("dummy_hl_cancel"), dry_run=False)
        self._initialized = True
        self._address = "0xwallet"
        self._hip3_address = "0xhip3"
        self.cancel_calls = []

    async def _post_public(self, payload: Dict[str, Any]) -> Any:
        if payload.get("type") == "frontendOpenOrders":
            return [{"coin": "xyz:MSTR", "oid": 123}]
        return []

    async def cancel_order(self, symbol: str, order_id: str) -> bool:
        self.cancel_calls.append((symbol, order_id))
        return True


class DummyMarketAdapter(HyperliquidAdapter):
    def __init__(self) -> None:
        super().__init__(log=logging.getLogger("dummy_hl_market"), dry_run=False)
        self._initialized = True
        self._address = "0xwallet"
        self.wallet = object()

    async def _post_exchange(self, payload: dict) -> dict:
        return {"ok": True}

    def _build_order_payload(self, *args, **kwargs):
        return {}

    def _parse_order_response(self, result: dict, ref_price: float):
        return True, "oid_1", 0.0, 0.0, None


def test_filledorresting_not_terminal() -> None:
    """filledorresting with remaining size should be treated as partial, not filled."""
    adapter = DummyHLAdapter()
    status = asyncio.run(adapter.check_order_status("ETH", "123"))
    assert status["status"] == "partially_filled"
    assert status["filled_size"] == 5.0
    assert status["remaining_size"] == 5.0


def test_cancel_all_orders_matches_hip3_prefix() -> None:
    adapter = DummyCancelAllAdapter()
    attempts = asyncio.run(adapter.cancel_all_orders("XYZ:MSTR"))
    assert attempts == 1
    assert adapter.cancel_calls == [("XYZ:MSTR", "123")]


def test_market_order_does_not_fabricate_fill_size() -> None:
    adapter = DummyMarketAdapter()
    ok, oid, filled_size, filled_price = asyncio.run(
        adapter.place_market_order("ETH", "buy", 1.0, best_bid=100.0, best_ask=101.0)
    )
    assert ok is True
    assert oid == "oid_1"
    assert filled_size == 0.0
    assert filled_price == 0.0


if __name__ == "__main__":
    test_filledorresting_not_terminal()
    print("[PASS] filledorresting not terminal")
