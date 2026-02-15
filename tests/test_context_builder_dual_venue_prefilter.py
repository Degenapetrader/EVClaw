#!/usr/bin/env python3
"""Tests for dual-venue prefiltering in context builder selection.

These tests must be hermetic (not depend on live VPS env vars).
"""

import os
import sys
from contextlib import contextmanager
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import context_builder_v2
from context_builder_v2 import OpportunityScorer, ScoredOpportunity


@contextmanager
def _hermetic_env():
    old_loader = context_builder_v2.load_dual_venue_symbols_cached
    old_hl_only = os.environ.get("EVCLAW_HL_ONLY")
    old_enabled_new = os.environ.get("EVCLAW_ENABLED_VENUES")
    try:
        # Disable Gate1 allowlist + make sure dual-venue prefilter path is enabled.
        context_builder_v2.load_dual_venue_symbols_cached = lambda *a, **k: []
        os.environ.pop("EVCLAW_HL_ONLY", None)
        os.environ["EVCLAW_ENABLED_VENUES"] = "lighter,hyperliquid"
        yield
    finally:
        context_builder_v2.load_dual_venue_symbols_cached = old_loader
        if old_hl_only is None:
            os.environ.pop("EVCLAW_HL_ONLY", None)
        else:
            os.environ["EVCLAW_HL_ONLY"] = old_hl_only
        if old_enabled_new is None:
            os.environ.pop("EVCLAW_ENABLED_VENUES", None)
        else:
            os.environ["EVCLAW_ENABLED_VENUES"] = old_enabled_new


def test_prefilter_non_dual_venue_before_top_n() -> None:
    with _hermetic_env():
        scorer = OpportunityScorer()

        blocked = ScoredOpportunity(
            symbol="BLOCKED",
            score=999.0,
            direction="LONG",
            signals={},
            key_metrics={},
            raw_data={"hl_mid": 123.0, "lighter_mid": 0.0},
        )

        allowed = [
            ScoredOpportunity(
                symbol=f"SYM{i}",
                score=100.0 - i,
                direction="LONG",
                signals={},
                key_metrics={},
                raw_data={"hip3_info": {"is_hip3": False}, "hl_mid": 100.0, "lighter_mid": 100.0},
            )
            for i in range(15)
        ]

        ranked = [blocked] + allowed

        # Force deterministic ranked list so we can verify prefiltering behavior.
        scorer.rank_opportunities = lambda _data: ranked

        selected = scorer.select_top_n({}, n=15, min_score=0)
        symbols = [opp.symbol for opp in selected]

        assert "BLOCKED" not in symbols
        assert len(symbols) == 15
        assert symbols == [f"SYM{i}" for i in range(15)]


def test_symbol_on_both_venues_rejects_invalid_hl_or_one_sided_quotes() -> None:
    scorer = OpportunityScorer()

    # Explicit HL quote present but invalid.
    assert scorer._symbol_on_both_venues("ETH", {"hl_mid": 0.0, "lighter_mid": 100.0}) is False

    # One-sided quote visibility is ambiguous; should not pass dual-venue gate.
    assert scorer._symbol_on_both_venues("ETH", {"lighter_mid": 100.0}) is False
    assert scorer._symbol_on_both_venues("ETH", {"hl_mid": 100.0}) is False


if __name__ == "__main__":
    test_prefilter_non_dual_venue_before_top_n()
    print("[PASS] context builder dual-venue prefilter")
