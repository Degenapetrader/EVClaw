#!/usr/bin/env python3
"""SSE sequence-gap resync behavior."""

import asyncio
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from sse_consumer import TrackerSSEClient, SSEMessage


class DummyResponse:
    def __init__(self) -> None:
        self.closed = False

    def close(self) -> None:
        self.closed = True


def test_tracker_gap_forces_resync() -> None:
    client = TrackerSSEClient()
    client._last_sequence = 10
    client._last_hip3_sequence = 5
    client._symbols = {"BTC": {"perp_signals": {}}}
    resp = DummyResponse()
    client._response = resp

    msg = SSEMessage(event_type="tracker-data", msg_type="delta", sequence=12, data={"ETH": {}}, removed=[])
    asyncio.run(client._handle_tracker_data(msg))

    assert resp.closed is True
    assert client._symbols == {}
    assert client._last_sequence == -1
    assert client._last_hip3_sequence == -1


def test_tracker_on_data_async_callback_supported() -> None:
    seen = {}

    async def _on_data(symbols):
        await asyncio.sleep(0)
        seen.update(symbols)

    client = TrackerSSEClient(on_data=_on_data)
    msg = SSEMessage(
        event_type="tracker-data",
        msg_type="snapshot",
        sequence=1,
        data={"ETH": {"perp_signals": {}}},
        removed=[],
    )

    asyncio.run(client._handle_tracker_data(msg))

    assert "ETH" in seen


def test_tracker_on_signal_async_callback_supported() -> None:
    called = []

    async def _on_signal(symbol, signals):
        await asyncio.sleep(0)
        called.append((symbol, signals))

    client = TrackerSSEClient(on_signal=_on_signal)
    msg = SSEMessage(
        event_type="tracker-data",
        msg_type="delta",
        sequence=1,
        data={"BTC": {"perp_signals": {"fade": {"signal": "LONG"}}}},
        removed=[],
    )

    asyncio.run(client._handle_tracker_data(msg))

    assert len(called) == 1
    assert called[0][0] == "BTC"


def test_sse_url_appends_profile_when_not_full() -> None:
    client = TrackerSSEClient(
        wallet_address="0x0000000000000000000000000000000000000001",
        host="tracker.evplus.ai",
        port=8443,
        endpoint="/sse/tracker",
        sse_profile="evclaw-lite",
    )
    assert client.url == (
        "https://tracker.evplus.ai:8443/sse/tracker"
        "?key=0x0000000000000000000000000000000000000001&profile=evclaw-lite"
    )


def test_sse_url_omits_profile_when_full() -> None:
    client = TrackerSSEClient(
        wallet_address="0x0000000000000000000000000000000000000001",
        host="tracker.evplus.ai",
        port=8443,
        endpoint="/sse/tracker?foo=bar",
        sse_profile="full",
    )
    assert client.url == (
        "https://tracker.evplus.ai:8443/sse/tracker"
        "?foo=bar&key=0x0000000000000000000000000000000000000001"
    )
