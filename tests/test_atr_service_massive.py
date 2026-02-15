#!/usr/bin/env python3

import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from atr_service import ATRService

import pytest


def test_atr_from_massive_aggs_basic() -> None:
    payload = {
        "results": [
            {"t": 1, "h": 10, "l": 8, "c": 9},
            {"t": 2, "h": 11, "l": 9, "c": 10},
            {"t": 3, "h": 12, "l": 10, "c": 11},
            {"t": 4, "h": 13, "l": 11, "c": 12},
            {"t": 5, "h": 14, "l": 12, "c": 13},
        ]
    }
    atr, close = ATRService._atr_from_massive_aggs(payload, period=3)
    assert atr is not None
    assert abs(atr - 2.0) < 1e-9
    assert close == 13.0


def test_binance_symbol_candidates_k_prefix_and_k_tickers() -> None:
    # HL "k" prefix should prefer 1000x contracts.
    assert ATRService._binance_symbol_candidates("kPEPE") == ["1000PEPEUSDT", "PEPEUSDT"]
    assert ATRService._binance_symbol_candidates("kSHIB") == ["1000SHIBUSDT", "SHIBUSDT"]

    # Normal symbols (including real K-tickers) should not be mis-mapped.
    assert ATRService._binance_symbol_candidates("BTC") == ["BTCUSDT"]
    assert ATRService._binance_symbol_candidates("KAITO") == ["KAITOUSDT"]


def test_filter_massive_results_rth_overlap() -> None:
    # 2026-02-11 is a Wednesday.
    from datetime import datetime, timezone

    zi = getattr(ATRService._filter_massive_results_rth, "__globals__", {}).get("ZoneInfo")
    if zi is None:
        pytest.skip("zoneinfo not available")

    results = [
        # 08:00 ET (13:00 UTC) -> premarket, should be excluded.
        {"t": int(datetime(2026, 2, 11, 13, 0, tzinfo=timezone.utc).timestamp() * 1000), "h": 1, "l": 1, "c": 1},
        # 09:00 ET (14:00 UTC) -> bar overlaps RTH open (09:30), should be included.
        {"t": int(datetime(2026, 2, 11, 14, 0, tzinfo=timezone.utc).timestamp() * 1000), "h": 2, "l": 2, "c": 2},
        # 16:00 ET (21:00 UTC) -> starts exactly at close, should be excluded.
        {"t": int(datetime(2026, 2, 11, 21, 0, tzinfo=timezone.utc).timestamp() * 1000), "h": 3, "l": 3, "c": 3},
    ]

    filtered = ATRService._filter_massive_results_rth(results, multiplier=1, timespan="hour")
    assert len(filtered) == 1
    assert filtered[0]["c"] == 2


@pytest.mark.asyncio
async def test_try_binance_falls_back_from_invalid_symbol_to_alternate_candidate() -> None:
    class DummyResp:
        def __init__(self, status: int, body_text: str = "", json_data=None):
            self.status = status
            self._body_text = body_text
            self._json_data = json_data

        async def text(self):
            return self._body_text

        async def json(self):
            return self._json_data

    class DummyCM:
        def __init__(self, resp: DummyResp):
            self.resp = resp

        async def __aenter__(self):
            return self.resp

        async def __aexit__(self, exc_type, exc, tb):
            return False

    class DummySession:
        def __init__(self, responses):
            self._responses = list(responses)
            self.calls = []

        def get(self, url, params=None, proxy=None):
            self.calls.append((url, dict(params or {})))
            resp = self._responses.pop(0)
            return DummyCM(resp)

    svc = ATRService(db_path=None)
    svc._session = DummySession(
        responses=[
            # First candidate invalid.
            DummyResp(400, body_text='{"code":-1121,"msg":"Invalid symbol."}'),
            # Second candidate ok.
            DummyResp(
                200,
                json_data=[
                    # [open_time, open, high, low, close, volume, ...]
                    [0, "1", "11", "9", "10", "0"],
                    [1, "1", "12", "10", "11", "0"],
                    [2, "1", "13", "11", "12", "0"],
                    [3, "1", "14", "12", "13", "0"],
                    [4, "1", "15", "13", "14", "0"],
                ],
            ),
        ]
    )

    # Disable proxy rotation.
    svc._proxy = type("P", (), {"next": lambda self: None})()

    res = await svc._try_binance("kPEPE", interval="1h", period=3, price=None)
    assert res is not None
    assert res.source == "binance"

    # First call should be 1000x contract (preferred for k-prefixed symbols).
    assert svc._session.calls[0][1]["symbol"] == "1000PEPEUSDT"
    assert svc._session.calls[1][1]["symbol"] == "PEPEUSDT"
