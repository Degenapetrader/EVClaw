#!/usr/bin/env python3

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from candle_fetcher import _normalize_massive_aggs


def test_normalize_massive_aggs_basic() -> None:
    payload = {
        "results": [
            {"t": 2, "o": 1, "h": 3, "l": 0.5, "c": 2, "v": 10},
            {"t": 1, "o": 2, "h": 4, "l": 1.0, "c": 3, "v": 20},
        ]
    }
    candles = _normalize_massive_aggs(payload)
    assert len(candles) == 2
    assert candles[0]["t"] == 1
    assert candles[1]["t"] == 2
    assert candles[0]["h"] == 4.0
    assert candles[0]["l"] == 1.0
