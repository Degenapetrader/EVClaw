#!/usr/bin/env python3
"""Tests for cycle-file price derivation."""

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from cli import derive_prices_from_symbol_data


def _approx(a: float, b: float, tol: float = 1e-6) -> bool:
    return abs(a - b) <= tol


def test_price_derivation() -> None:
    fixture = Path(__file__).resolve().parent / "fixtures" / "cycle_sample.json"
    data = json.loads(fixture.read_text())
    symbols = data["symbols"]

    price, bid, ask = derive_prices_from_symbol_data(symbols["ETH"])
    assert _approx(price, 3000.5)
    assert _approx(bid, 3000.0)
    assert _approx(ask, 3001.0)

    price, bid, ask = derive_prices_from_symbol_data(symbols["BTC"])
    assert _approx(price, 40005.0)
    assert _approx(bid, 40000.0)
    assert _approx(ask, 40010.0)

    price, bid, ask = derive_prices_from_symbol_data(symbols["SOL"])
    assert _approx(price, 100.0)
    assert _approx(bid, 99.9)
    assert _approx(ask, 100.1)

    price, bid, ask = derive_prices_from_symbol_data(symbols["BAD"])
    assert _approx(price, 100.0)
    assert _approx(bid, 99.9)
    assert _approx(ask, 100.1)


if __name__ == "__main__":
    test_price_derivation()
    print("[PASS] cycle price derivation")
