#!/usr/bin/env python3
"""Smoke tests for hourly review context loading helpers."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from position_review_worker import _context_symbol_map, _sr_risk_bias


def test_context_symbol_map_supports_top_opportunities() -> None:
    payload = {
        "top_opportunities": [
            {
                "symbol": "AAA",
                "key_metrics": {"atr_pct": 2.0},
                "perp_signals": {"whale": {"banner_trigger": True, "signal": "LONG"}},
                "worker_signals_summary": {},
            }
        ]
    }
    m = _context_symbol_map(payload)
    assert "AAA" in m


def test_sr_risk_bias_long_near_resistance() -> None:
    km = {
        "atr_pct": 2.0,
        "sr_levels": {
            "price": 100.0,
            "nearest": {"resistance": {"price": 101.0}, "support": {"price": 90.0}},
        },
    }
    score, note = _sr_risk_bias(km, "LONG")
    assert score > 0
    assert "resistance" in note


if __name__ == "__main__":
    test_context_symbol_map_supports_top_opportunities()
    test_sr_risk_bias_long_near_resistance()
    print("[PASS] position review context load")
