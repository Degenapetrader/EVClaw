#!/usr/bin/env python3
"""Regression tests for dead_capital banner/locked magnitude.

Rule: locked magnitude must follow signal direction:
- LONG signal -> use locked_long_pct
- SHORT signal -> use locked_short_pct

This ensures dead_capital strength works for both long and short regimes.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from signal_parser import extract_z_score


def test_dead_capital_locked_mag_directional_long() -> None:
    data = {
        "signal": "LONG",
        "banner_trigger": False,
        "locked_long_pct": 5.0,
        "locked_short_pct": 20.0,
        "threshold": 10.0,
        "banner_threshold": 30.0,
    }
    z, _, _ = extract_z_score(data, "dead_capital")
    # LONG should be driven by trapped shorts (locked_short_pct).
    assert z > 2.0


def test_dead_capital_locked_mag_directional_short() -> None:
    data = {
        "signal": "SHORT",
        "banner_trigger": False,
        "locked_long_pct": 20.0,
        "locked_short_pct": 5.0,
        "threshold": 10.0,
        "banner_threshold": 30.0,
    }
    z, _, _ = extract_z_score(data, "dead_capital")
    # SHORT should be driven by trapped longs (locked_long_pct).
    assert z > 2.0


if __name__ == "__main__":
    test_dead_capital_locked_mag_directional_long()
    test_dead_capital_locked_mag_directional_short()
    print("[PASS] dead_capital banner directional locked magnitude")
