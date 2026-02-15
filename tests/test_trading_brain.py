#!/usr/bin/env python3
"""
Minimal TradingBrain decision test.
"""

import os
import subprocess
import sys
from pathlib import Path
from unittest.mock import Mock

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import trading_brain as tb
from context_builder_v2 import ScoredOpportunity
from trading_brain import BrainConfig, TradingBrain


def test_trading_brain_decision() -> None:
    opp = ScoredOpportunity(
        symbol="ETH",
        score=80.0,
        direction="LONG",
        signals={
            "cvd": {"direction": "LONG", "z_score": 3.0, "z_smart": 2.8, "z_dumb": 1.5},
            "fade": {"direction": "LONG", "z_score": 2.5},
            "liq_pnl": {"direction": "LONG", "z_score": 2.2},
            "whale": {"direction": "LONG", "z_score": 2.0, "strength": 1.0},
            "dead_capital": {"direction": "LONG", "z_score": 2.0, "strength": 1.0},
            "ofm": {"direction": "LONG", "z_score": 2.1},
        },
        key_metrics={
            "atr_pct": 1.2,
            "smart_cvd": 100,
            "dumb_cvd": -50,
            "divergence_z": 2.5,
            "cohort_signal": "SMART_ACCUMULATING",
            "smart_vs_dumb_delta": 10,
            "fragile_count": 40,
            "fragile_notional": 3_000_000,
            "hot_zone_pct": 15,
            "long_pain": 1,
            "short_pain": 5,
            "liq_suggested_direction": "LONG",
        },
        raw_data={},
    )

    brain = TradingBrain()
    decision = brain.evaluate_opportunity(opp)

    assert decision.symbol == "ETH"
    assert decision.direction == "LONG"
    assert decision.should_trade
    assert decision.conviction >= brain.config.base_confidence_threshold


def test_calculate_conviction_uses_cvd_z_smart_when_z_score_missing() -> None:
    brain = TradingBrain()
    brain.config.conviction_weights = {"cvd": 1.0}
    conviction = brain._calculate_conviction(
        {
            "cvd": {"direction": "LONG", "z_smart": 2.4, "z_dumb": 0.2},
        },
        {},
        "LONG",
    )
    assert conviction > 0.0


def test_calculate_conviction_uses_whale_strength_when_z_score_missing() -> None:
    brain = TradingBrain()
    brain.config.conviction_weights = {"whale": 1.0}
    conviction = brain._calculate_conviction(
        {
            "whale": {"direction": "LONG", "strength": 0.8},
        },
        {},
        "LONG",
    )
    assert conviction > 0.0


def test_calculate_conviction_does_not_flip_negative_with_agree_bonus(monkeypatch) -> None:
    monkeypatch.setattr(tb, "BRAIN_AGREE_BONUS_COUNT_1", 1)
    monkeypatch.setattr(tb, "BRAIN_AGREE_BONUS_1", 0.6)
    monkeypatch.setattr(tb, "BRAIN_AGREE_BONUS_COUNT_2", 2)
    monkeypatch.setattr(tb, "BRAIN_AGREE_BONUS_2", 0.6)

    brain = TradingBrain()
    brain.config.conviction_weights = {
        "opp": 1.0,
        "a1": 1.0,
        "a2": 1.0,
        "a3": 1.0,
        "a4": 1.0,
    }
    conviction = brain._calculate_conviction(
        {
            "opp": {"direction": "SHORT", "z_score": 8.0},
            "a1": {"direction": "LONG", "z_score": 0.1},
            "a2": {"direction": "LONG", "z_score": 0.1},
            "a3": {"direction": "LONG", "z_score": 0.1},
            "a4": {"direction": "LONG", "z_score": 0.1},
        },
        {},
        "LONG",
    )

    assert conviction == 0.0


def test_whale_string_zscore_does_not_crash() -> None:
    brain = TradingBrain()
    vetoed, reason = brain._check_veto(
        {"whale": {"direction": "SHORT", "z_score": "2.2"}},
        "LONG",
    )
    assert vetoed is True
    assert "WHALE opposite" in reason


def test_has_primary_signal_trigger_accepts_string_zscore() -> None:
    brain = TradingBrain()
    assert brain._has_primary_signal_trigger(
        {"whale": {"direction": "LONG", "z_score": "2.4"}},
        "LONG",
    ) is True


def test_cvd_veto_uses_z_score_when_smart_dumb_missing() -> None:
    brain = TradingBrain()
    vetoed, reason = brain._check_veto(
        {"cvd": {"direction": "SHORT", "z_score": 9.0}},
        "LONG",
    )
    assert vetoed is True
    assert "CVD opposite" in reason


def test_lowercase_direction_is_normalized_not_vetoed_as_opposite() -> None:
    brain = TradingBrain()
    opp = ScoredOpportunity(
        symbol="ETH",
        score=70.0,
        direction="LONG",
        signals={"whale": {"direction": "long", "z_score": 3.0}},
        key_metrics={"atr_pct": 2.0},
        raw_data={},
    )
    decision = brain.evaluate_opportunity(opp)
    assert decision.vetoed is False


def test_import_trading_brain_has_no_startup_config_banner() -> None:
    repo = str(Path(__file__).resolve().parents[1])
    env = os.environ.copy()
    env.pop("_EVCLAW_CONFIG_LOGGED", None)
    pythonpath = env.get("PYTHONPATH")
    env["PYTHONPATH"] = repo if not pythonpath else f"{repo}:{pythonpath}"
    out = subprocess.run(
        [sys.executable, "-c", "import trading_brain; print('import_ok')"],
        cwd=repo,
        env=env,
        capture_output=True,
        text=True,
        check=True,
    )
    assert "Config: enabled_venues=" not in out.stdout
    assert out.stdout.strip() == "import_ok"


def test_safe_float_rejects_nan_and_inf() -> None:
    assert tb._safe_float("nan", 0.0) == 0.0
    assert tb._safe_float(float("inf"), 1.5) == 1.5
    assert tb._safe_float("-inf", -2.0) == -2.0


def test_evaluate_opportunity_handles_string_metrics_without_type_error() -> None:
    opp = ScoredOpportunity(
        symbol="ETH",
        score=65.0,
        direction="LONG",
        signals={
            "whale": {"direction": "LONG", "z_score": "2.1"},
            "cvd": {"direction": "LONG", "z_score": "1.8"},
        },
        key_metrics={
            "atr_pct": "1.2",
            "smart_cvd": "10",
            "dumb_cvd": "-5",
            "divergence_z": "2.0",
            "fragile_count": "20",
            "fragile_notional": "2500000",
            "hot_zone_pct": "12.5",
            "long_pain": "100",
            "short_pain": "500",
        },
        raw_data={},
    )

    brain = TradingBrain()
    decision = brain.evaluate_opportunity(opp)
    assert decision.symbol == "ETH"
    assert 0.0 <= float(decision.conviction) <= 1.0


def test_learning_composite_floor_prevents_conviction_collapse() -> None:
    cfg = BrainConfig(strong_z_threshold=99.0)
    opp = ScoredOpportunity(
        symbol="SOL",
        score=70.0,
        direction="LONG",
        signals={
            "dead_capital": {"direction": "LONG", "z_score": 2.5},
            "whale": {"direction": "LONG", "z_score": 2.1},
        },
        key_metrics={"atr_pct": 1.2},
        raw_data={},
    )

    brain_plain = TradingBrain(config=cfg)
    plain = brain_plain.evaluate_opportunity(opp)
    assert plain.conviction > 0

    learning = Mock()
    learning.get_symbol_adjustment.return_value = 0.35  # Above history hard-block
    learning.should_avoid_pattern.return_value = True   # 0.7 penalty
    learning.get_signal_adjustment.return_value = 0.2   # heavy signal penalty

    brain_learning = TradingBrain(config=cfg, learning_engine=learning)
    learned = brain_learning.evaluate_opportunity(opp)

    expected_floor = plain.conviction * tb.BRAIN_LEARNING_COMPOSITE_FLOOR
    assert learned.conviction == pytest.approx(expected_floor, rel=1e-6)


def test_strong_dead_floor_is_disabled_by_default_but_configurable() -> None:
    opp = ScoredOpportunity(
        symbol="ETH",
        score=70.0,
        direction="LONG",
        signals={
            "dead_capital": {"direction": "LONG", "z_score": 2.0, "strength": 1.0},
        },
        key_metrics={"atr_pct": 1.0},
        raw_data={},
    )

    brain_default = TradingBrain(config=BrainConfig(dead_floor_enabled=False))
    decision_default = brain_default.evaluate_opportunity(opp)
    assert decision_default.conviction < tb.BRAIN_STRONG_DEAD_MIN_CONVICTION

    brain_floor_on = TradingBrain(config=BrainConfig(dead_floor_enabled=True))
    decision_floor_on = brain_floor_on.evaluate_opportunity(opp)
    assert decision_floor_on.conviction >= tb.BRAIN_STRONG_DEAD_MIN_CONVICTION


def test_degen_multiplier_activates_at_or_above_threshold() -> None:
    cfg = BrainConfig(
        degen_mode=True,
        degen_multiplier=1.5,
        degen_multiplier_min=0.75,
    )
    brain = TradingBrain(config=cfg)

    at_threshold = brain._calculate_size_multiplier(
        conviction=0.75,
        smart_money=0.0,
        liq_edge=0.0,
        vol_fit="SWING",
    )
    above_threshold = brain._calculate_size_multiplier(
        conviction=0.751,
        smart_money=0.0,
        liq_edge=0.0,
        vol_fit="SWING",
    )

    assert at_threshold == 1.5
    assert above_threshold == 1.5


if __name__ == "__main__":
    test_trading_brain_decision()
    print("[PASS] trading brain decision")
