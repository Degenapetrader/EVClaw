#!/usr/bin/env python3
"""Tests for risk-based sizing helpers."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from live_agent_utils import clamp_risk_pct, compute_risk_size_usd


def test_compute_risk_size_usd_basic() -> None:
    # equity=10000, risk_pct=1%, atr=10, sl_mult=2, price=200
    # risk_usd=100, stop_distance=20, qty=5, size_usd=1000
    size_usd = compute_risk_size_usd(
        equity=10000.0,
        risk_pct=1.0,
        atr=10.0,
        sl_multiplier=2.0,
        price=200.0,
    )
    assert abs(size_usd - 1000.0) < 1e-6


def test_compute_risk_size_usd_invalid_inputs() -> None:
    assert compute_risk_size_usd(0.0, 1.0, 10.0, 2.0, 200.0) == 0.0
    assert compute_risk_size_usd(10000.0, 0.0, 10.0, 2.0, 200.0) == 0.0
    assert compute_risk_size_usd(10000.0, 1.0, None, 2.0, 200.0) == 0.0
    assert compute_risk_size_usd(10000.0, 1.0, 0.0, 2.0, 200.0) == 0.0
    assert compute_risk_size_usd(10000.0, 1.0, 10.0, 0.0, 200.0) == 0.0


def test_clamp_risk_pct() -> None:
    val, clamped = clamp_risk_pct(0.1)
    assert clamped is True
    assert abs(val - 0.5) < 1e-6

    val2, clamped2 = clamp_risk_pct(5.0)
    assert clamped2 is True
    assert abs(val2 - 2.5) < 1e-6

    val3, clamped3 = clamp_risk_pct(1.2)
    assert clamped3 is False
    assert abs(val3 - 1.2) < 1e-6


if __name__ == "__main__":
    test_compute_risk_size_usd_basic()
    test_compute_risk_size_usd_invalid_inputs()
    test_clamp_risk_pct()
    print("[PASS] risk sizing")
