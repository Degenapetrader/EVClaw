#!/usr/bin/env python3

import asyncio
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from exchanges.base import Position
from exchanges.hyperliquid_adapter import HyperliquidAdapter


class _FakeExchange:
    def __init__(self, *args, **kwargs):
        pass

    def order(self, **kwargs):
        raise KeyError("xyz:AMZN")


def test_place_limit_preserves_primary_error_when_sdk_fallback_throws(monkeypatch) -> None:
    a = HyperliquidAdapter(log=logging.getLogger("test"), dry_run=False)
    a._initialized = True
    a.wallet = object()

    async def _post(_payload):
        return {"status": "err"}

    def _parse(_result, _px=0):
        return False, "", 0.0, 0.0, "Order has invalid size."

    a._build_order_payload = lambda *args, **kwargs: {"ok": True}  # type: ignore[method-assign]
    a._post_exchange = _post  # type: ignore[method-assign]
    a._parse_order_response = _parse  # type: ignore[method-assign]

    from exchanges import hyperliquid_adapter as mod

    monkeypatch.setattr(mod, "Exchange", _FakeExchange)

    ok, oid = asyncio.run(
        a.place_limit_order(
            symbol="XYZ:AMZN",
            side="buy",
            size=1.0,
            price=100.0,
            reduce_only=True,
            tif="Gtc",
        )
    )

    assert ok is False
    assert oid is None
    assert a.last_order_error is not None
    assert "invalid size" in a.last_order_error.lower()
    assert "sdk_fallback_exception" in a.last_order_error


def test_reduce_only_invalid_size_retries_with_clamped_live_size() -> None:
    a = HyperliquidAdapter(log=logging.getLogger("test"), dry_run=False)
    a._initialized = True
    a.wallet = object()

    sent_sizes = []

    def _build(symbol, is_buy, size, price, order_type, reduce_only=False):
        sent_sizes.append(float(size))
        return {"size": float(size)}

    async def _post(payload):
        return payload

    # first attempt invalid-size, second attempt success
    parse_calls = {"n": 0}

    def _parse(result, _px=0):
        parse_calls["n"] += 1
        if parse_calls["n"] == 1:
            return False, "", 0.0, 0.0, "Order has invalid size."
        return True, "oid-ok", 0.0, 0.0, ""

    async def _pos(_symbol):
        return Position(symbol="XYZ:NVDA", direction="SHORT", size=37.722, entry_price=100.0, unrealized_pnl=0.0)

    a._build_order_payload = _build  # type: ignore[method-assign]
    a._post_exchange = _post  # type: ignore[method-assign]
    a._parse_order_response = _parse  # type: ignore[method-assign]
    a.get_position = _pos  # type: ignore[method-assign]
    a._sz_decimals = {"XYZ:NVDA": 3}
    a._asset_map = {"XYZ:NVDA": 120002}

    ok, oid = asyncio.run(
        a.place_limit_order(
            symbol="XYZ:NVDA",
            side="buy",
            size=37.722,
            price=100.0,
            reduce_only=True,
            tif="Gtc",
        )
    )

    assert ok is True
    assert oid == "oid-ok"
    assert len(sent_sizes) >= 2
    assert sent_sizes[0] == 37.722
    assert sent_sizes[1] < sent_sizes[0]


def test_wallet_mode_unknown_api_wallet_retries_with_fallback_signer() -> None:
    a = HyperliquidAdapter(log=logging.getLogger("test"), dry_run=False, account_mode="wallet")
    a._initialized = True

    primary_wallet = object()
    fallback_wallet = object()
    a.wallet = primary_wallet
    a._hip3_wallet = primary_wallet
    a._wallet_fallback = fallback_wallet

    seen_wallets = []

    def _build(symbol, is_buy, size, price, order_type, reduce_only=False):
        seen_wallets.append(a.wallet)
        return {"size": float(size)}

    async def _post(payload):
        return payload

    parse_calls = {"n": 0}

    def _parse(_result, _px=0):
        parse_calls["n"] += 1
        if parse_calls["n"] == 1:
            return False, "", 0.0, 0.0, "User or API Wallet 0xabc does not exist."
        return True, "oid-fallback", 0.0, 0.0, ""

    a._build_order_payload = _build  # type: ignore[method-assign]
    a._post_exchange = _post  # type: ignore[method-assign]
    a._parse_order_response = _parse  # type: ignore[method-assign]

    ok, oid = asyncio.run(
        a.place_limit_order(
            symbol="XYZ:AMZN",
            side="buy",
            size=1.0,
            price=100.0,
            reduce_only=True,
            tif="Gtc",
        )
    )

    assert ok is False
    assert oid is None
    assert len(seen_wallets) >= 1
    assert seen_wallets[0] is primary_wallet
    assert a.last_order_error is not None
    assert "does not exist" in a.last_order_error.lower()
    assert "sdk_fallback_exception" in a.last_order_error
