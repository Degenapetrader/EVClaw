#!/usr/bin/env python3
"""Tests for perp signal weighting adjustments."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from context_builder_v2 import (
    CB_COMPOSITE_NORMALIZE,
    CB_COMPOSITE_WEIGHTS_STANDARD,
    CB_DEAD_STRENGTH_Z_MULT,
    CB_WEIGHT_MULT_CVD,
    CB_WEIGHT_MULT_DEAD,
    OpportunityScorer,
)


def test_dead_whale_weighting_boosts_composite() -> None:
    scorer = OpportunityScorer()

    base = scorer._score_signals({
        "fade": {"signal": "LONG", "z_score": 1.2}
    })["composite"]

    whale = scorer._score_signals({
        "whale": {"signal": "LONG", "strength": 0.6}
    })["composite"]

    dead = scorer._score_signals({
        "dead_capital": {"signal": "LONG", "strength": 0.6}
    })["composite"]

    assert whale > base
    assert dead > whale


def test_dead_capital_bonus_applies() -> None:
    scorer = OpportunityScorer()

    no_bonus = scorer._score_signals({
        "dead_capital": {"signal": "NEUTRAL", "strength": 1.0}
    })["composite"]

    with_bonus = scorer._score_signals({
        "dead_capital": {"signal": "LONG", "strength": 1.0}
    })["composite"]

    assert with_bonus > no_bonus


def test_composite_uses_signal_identity_weights_not_magnitude_sort() -> None:
    scorer = OpportunityScorer()

    # CVD has larger magnitude than dead_capital here.
    out = scorer._score_signals(
        {
            "cvd": {"signal": "LONG", "z_smart": 5.0, "z_dumb": 0.0},
            "dead_capital": {"signal": "LONG", "strength": 0.4},
        }
    )

    weights = list(CB_COMPOSITE_WEIGHTS_STANDARD) if isinstance(CB_COMPOSITE_WEIGHTS_STANDARD, list) else [0.32, 0.28, 0.20, 0.12, 0.08]
    w_dead = float(weights[0]) if len(weights) > 0 else 0.32
    w_cvd = float(weights[2]) if len(weights) > 2 else 0.20
    norm = max(1e-9, w_dead + w_cvd)
    dead_weighted = (0.4 * CB_DEAD_STRENGTH_Z_MULT) * CB_WEIGHT_MULT_DEAD
    cvd_weighted = 5.0 * CB_WEIGHT_MULT_CVD
    expected = ((dead_weighted * w_dead) + (cvd_weighted * w_cvd)) / norm * CB_COMPOSITE_NORMALIZE

    # No dead-cap bonus in this setup (strength=0.4 < bonus threshold).
    assert abs(out["composite"] - expected) < 1e-6


def test_deprecated_liq_pnl_does_not_inflate_max_z() -> None:
    scorer = OpportunityScorer()
    out = scorer._score_signals(
        {
            "fade": {"signal": "SHORT", "z_score": 1.2},
            "liq_pnl": {"signal": "LONG", "z_score": 9.9},
        }
    )
    assert abs(float(out["max_z"]) - 1.2) < 1e-9


def test_determine_direction_ignores_deprecated_liq_pnl() -> None:
    scorer = OpportunityScorer()
    direction = scorer._determine_direction(
        {
            "fade": {"signal": "SHORT", "z_score": 1.0},
            "ofm": {"signal": "SHORT", "z_score": 1.1},
            "whale": {"signal": "SHORT", "strength": 0.7},
            "liq_pnl": {"signal": "LONG", "z_score": 5.0, "_deprecated": True},
        },
        {},
    )
    assert direction == "SHORT"


def test_score_symbol_enforces_volume_gate_with_1m_floor() -> None:
    scorer = OpportunityScorer()
    # Even if runtime tuning lowers this, hard floor must stay at $1M.
    scorer.min_volume_24h = 100.0

    low_vol_data = {
        "perp_signals": {
            "cvd": {"signal": "LONG", "z_smart": 2.4, "z_dumb": 0.0},
        },
        "price_change": {"volume_24h": 900_000, "pct_24h": 0.0},
        "atr": {"atr_pct": 1.8},
    }
    assert scorer.score_symbol("ETH", low_vol_data, skip_z_filter=False) is None

    # Open-position monitoring path bypasses discovery gates.
    assert scorer.score_symbol("ETH", low_vol_data, skip_z_filter=True) is not None

    high_vol_data = {
        **low_vol_data,
        "price_change": {"volume_24h": 1_100_000, "pct_24h": 0.0},
    }
    assert scorer.score_symbol("ETH", high_vol_data, skip_z_filter=False) is not None


def test_scorer_handles_string_and_nan_metrics_without_type_error() -> None:
    scorer = OpportunityScorer()

    data = {
        "perp_signals": {
            "cvd": {"signal": "LONG", "z_smart": "2.4", "z_dumb": "0.2"},
        },
        "price_change": {"volume_24h": "2000000", "pct_24h": "1.2"},
        "smart_dumb_cvd": {"divergence_z": "nan"},
        "cohort_delta": {"signal": "SMART_ACCUMULATING", "smart_vs_dumb_delta": "3.0"},
        "summary": {"net_bias_pct": "55", "hot_zone_pct": "12.5"},
        "fragile_wallets": {"count": "20", "total_notional": "2000000"},
        "liquidation_heatmap": {"long_total_pnl": "-1000", "short_total_pnl": "2500"},
        "atr": {"atr_pct": "1.2"},
    }

    flow = scorer._score_flow_alignment(data)
    smart = scorer._score_smart_money(data)
    liq = scorer._score_liquidity_edge(data)
    vol = scorer._score_volatility(data)
    opp = scorer.score_symbol("ETH", data, skip_z_filter=False)

    assert 0.0 <= float(flow) <= 100.0
    assert 0.0 <= float(smart) <= 100.0
    assert 0.0 <= float(liq) <= 100.0
    assert 0.0 <= float(vol) <= 100.0
    assert opp is not None


if __name__ == "__main__":
    test_dead_whale_weighting_boosts_composite()
    test_dead_capital_bonus_applies()
    print("[PASS] context builder signal weighting")
