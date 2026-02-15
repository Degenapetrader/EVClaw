#!/usr/bin/env python3
"""Tests for mandatory inclusion of strong-signal symbols."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import context_builder_v2
from context_builder_v2 import OpportunityScorer, ScoredOpportunity


async def _empty_dual_symbols(*_args, **_kwargs):
    return []


# Force Gate 1 allowlist to be effectively disabled for this unit test.
context_builder_v2.load_dual_venue_symbols = _empty_dual_symbols


def _make_opportunity(symbol: str, score: float, signals, strong_signals=None) -> ScoredOpportunity:
    return ScoredOpportunity(
        symbol=symbol,
        score=score,
        direction="LONG",
        signals=signals,
        key_metrics={},
        strong_signals=list(strong_signals or []),
        raw_data={"hip3_info": {"is_hip3": False}},
    )


def test_trade_signal_mandatory_inclusion() -> None:
    scorer = OpportunityScorer()

    high_score = _make_opportunity(
        "HIGH",
        90.0,
        {"fade": {"direction": "LONG", "z_score": 1.0}},
    )
    mid_score = _make_opportunity(
        "MID",
        50.0,
        {"cvd": {"direction": "LONG", "z_score": 1.0}},
    )
    trade_signal = _make_opportunity(
        "TRADE",
        10.0,
        {"whale": {"direction": "LONG", "z_score": 2.5, "banner_trigger": True}},
        strong_signals=["whale"],
    )

    ranked = [high_score, mid_score, trade_signal]
    scorer.rank_opportunities = lambda _data: ranked

    selected = scorer.select_top_n({}, n=2, min_score=40.0)
    symbols = [opp.symbol for opp in selected]

    assert "TRADE" in symbols
    assert "HIGH" in symbols
    assert len(symbols) == 2


if __name__ == "__main__":
    test_trade_signal_mandatory_inclusion()
    print("[PASS] context builder mandatory inclusion")
