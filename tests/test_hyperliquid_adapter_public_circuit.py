#!/usr/bin/env python3
"""Public API circuit breaker regression tests for Hyperliquid adapter."""

import logging
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import exchanges.hyperliquid_adapter as hl_mod  # noqa: E402
from exchanges.hyperliquid_adapter import HyperliquidAdapter  # noqa: E402


class _Resp:
    def __init__(self, status: int, payload=None, text: str = "") -> None:
        self.status = status
        self._payload = payload if payload is not None else {}
        self._text = text

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def json(self):
        return self._payload

    async def text(self):
        return self._text


class _Always429Session:
    def __init__(self) -> None:
        self.closed = False
        self.calls = 0

    def post(self, *args, **kwargs):  # noqa: ANN002, ANN003
        self.calls += 1
        return _Resp(status=429, payload={"error": "rate_limited"}, text="rate_limited")


class _RouteSession:
    def __init__(self, routes) -> None:  # noqa: ANN001
        self.closed = False
        self._routes = dict(routes)
        self.calls = []

    def post(self, url, *args, **kwargs):  # noqa: ANN001, ANN002, ANN003
        self.calls.append((url, kwargs.get("json")))
        queue = self._routes.get(url) or []
        if queue:
            return queue.pop(0)
        return _Resp(status=599, payload={"error": "missing_route"}, text="missing_route")


@pytest.mark.asyncio
async def test_post_public_circuit_opens_and_short_circuits(monkeypatch) -> None:
    monkeypatch.setattr(hl_mod, "PUBLIC_CIRCUIT_BREAKER_ENABLED", True)
    monkeypatch.setattr(hl_mod, "PUBLIC_CIRCUIT_BREAKER_FAILS", 1)
    monkeypatch.setattr(hl_mod, "PUBLIC_CIRCUIT_BREAKER_COOLDOWN_SEC", 30)
    monkeypatch.setattr(hl_mod, "MAX_PUBLIC_RETRIES", 3)

    adapter = HyperliquidAdapter(log=logging.getLogger("hl-public-circuit"), dry_run=False)
    adapter._session = _Always429Session()

    with pytest.raises(Exception):
        await adapter._post_public({"type": "meta"})

    first_call_count = adapter._session.calls
    assert first_call_count >= 1

    with pytest.raises(Exception, match="circuit open"):
        await adapter._post_public({"type": "meta"})

    # Second call should fail fast without issuing another HTTP request.
    assert adapter._session.calls == first_call_count


@pytest.mark.asyncio
async def test_post_public_private_info_route_success() -> None:
    adapter = HyperliquidAdapter(log=logging.getLogger("hl-private-success"), dry_run=False)
    adapter._private_info_base_url = "https://node2.evplus"
    private_url = "https://node2.evplus/info"
    public_url = "https://api.hyperliquid.xyz/info"
    adapter._session = _RouteSession(
        {
            private_url: [_Resp(status=200, payload={"ok": "private"})],
            public_url: [_Resp(status=200, payload={"ok": "public"})],
        }
    )

    result = await adapter._post_public({"type": "clearinghouseState", "user": "0xabc"})
    assert result == {"ok": "private"}
    assert adapter._session.calls == [(private_url, {"type": "clearinghouseState", "user": "0xabc"})]


@pytest.mark.asyncio
async def test_post_public_private_info_fallback_to_public_on_unsupported_status() -> None:
    adapter = HyperliquidAdapter(log=logging.getLogger("hl-private-fallback"), dry_run=False)
    adapter._private_info_base_url = "https://node2.evplus"
    private_url = "https://node2.evplus/info"
    public_url = "https://api.hyperliquid.xyz/info"
    adapter._session = _RouteSession(
        {
            private_url: [_Resp(status=422, payload={"error": "unsupported"}, text="unsupported")],
            public_url: [_Resp(status=200, payload={"ok": "public"})],
        }
    )

    result = await adapter._post_public({"type": "clearinghouseState", "user": "0xabc"})
    assert result == {"ok": "public"}
    assert adapter._session.calls == [
        (private_url, {"type": "clearinghouseState", "user": "0xabc"}),
        (public_url, {"type": "clearinghouseState", "user": "0xabc"}),
    ]


@pytest.mark.asyncio
async def test_post_public_unsupported_type_skips_private_route() -> None:
    adapter = HyperliquidAdapter(log=logging.getLogger("hl-private-skip"), dry_run=False)
    adapter._private_info_base_url = "https://node2.evplus"
    public_url = "https://api.hyperliquid.xyz/info"
    adapter._session = _RouteSession(
        {
            public_url: [_Resp(status=200, payload={"ok": "public"})],
        }
    )

    result = await adapter._post_public({"type": "allMids"})
    assert result == {"ok": "public"}
    assert adapter._session.calls == [(public_url, {"type": "allMids"})]
