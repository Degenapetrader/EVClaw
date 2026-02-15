#!/usr/bin/env python3
"""Regression tests for explicit mode-controller slider mappings."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from mode_controller import (
    BALANCED_PRESET,
    SLIDER_AGGR,
    SLIDER_RISK,
    _choose_slider,
    _slider_factor,
    compute_param,
)


def test_choose_slider_respects_explicit_rr_mapping() -> None:
    slider, inverse = _choose_slider("symbol_rr_learning", "default_sl_mult")
    assert slider == SLIDER_AGGR
    assert inverse is False


def test_choose_slider_respects_explicit_degen_mapping() -> None:
    slider, inverse = _choose_slider("brain", "degen_multiplier")
    assert slider == SLIDER_RISK
    assert inverse is False


def test_compute_param_uses_explicit_rr_slider_not_learning_slider() -> None:
    base = float(BALANCED_PRESET["symbol_rr_learning"]["default_sl_mult"])
    out = float(
        compute_param(
            "symbol_rr_learning",
            "default_sl_mult",
            mode="balanced",
            risk_appetite=50,
            trade_frequency=50,
            aggression=100,
            signal_weighting=50,
            learning_speed=0,
            allow_legacy_env=False,
        )
    )
    expected = base * _slider_factor(100)  # mapped to aggression, not learning_speed
    assert abs(out - expected) < 1e-9
