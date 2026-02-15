#!/usr/bin/env python3
"""Tests for mandatory banner constraints (boss directive 2026-02-01).

Rules:
- Whale + DeadCap banners => must_trade_symbols
- Other signals are context-only for constraints layer.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from constraints import compile_constraints


def test_must_trade_includes_whale_and_deadcap_banners() -> None:
    opps = [
        {
            "symbol": "AAA",
            "perp_signals": {"whale": {"direction": "LONG", "banner_trigger": True, "strength": 0.6}},
        },
        {
            "symbol": "BBB",
            "perp_signals": {"dead_capital": {"direction": "SHORT", "banner_trigger": True, "strength": 1.5}},
        },
        {
            "symbol": "CCC",
            "perp_signals": {"cvd": {"direction": "LONG", "z_score": 5.0, "banner_trigger": True}},
        },
    ]
    c = compile_constraints(opps, max_candidates=10)
    must = set(c.get("must_trade_symbols") or [])
    assert "AAA" in must
    assert "BBB" in must
    assert "CCC" not in must


if __name__ == "__main__":
    test_must_trade_includes_whale_and_deadcap_banners()
    print("[PASS] constraints mandatory banners")
