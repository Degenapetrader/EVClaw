#!/usr/bin/env python3
"""Risk manager limit normalization regressions."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import risk_manager
from risk_manager import DynamicRiskManager, RiskConfig


class _StubSafety:
    def __init__(self, cap: float) -> None:
        self._cap = cap

    def get_size_cap(self) -> float:
        return self._cap


def test_max_position_pct_accepts_fraction_or_percent() -> None:
    cfg_fraction = RiskConfig(max_position_pct=0.25)
    mgr_fraction = DynamicRiskManager(config=cfg_fraction, equity=10_000.0)
    size_fraction = mgr_fraction.position_size_usd(risk_pct=10.0, atr_pct=1.0, sl_mult=1.0)

    cfg_percent = RiskConfig(max_position_pct=25.0)
    mgr_percent = DynamicRiskManager(config=cfg_percent, equity=10_000.0)
    size_percent = mgr_percent.position_size_usd(risk_pct=10.0, atr_pct=1.0, sl_mult=1.0)

    assert abs(size_fraction - 2500.0) < 1e-9
    assert abs(size_percent - 2500.0) < 1e-9


def test_adjust_for_conditions_normalizes_misordered_thresholds(monkeypatch) -> None:
    # Intentionally misordered: thresh_1 < thresh_2 > thresh_3
    monkeypatch.setattr(risk_manager, "RISK_DAILY_PNL_THRESH_1", 0.02)
    monkeypatch.setattr(risk_manager, "RISK_DAILY_PNL_THRESH_2", 0.04)
    monkeypatch.setattr(risk_manager, "RISK_DAILY_PNL_THRESH_3", 0.03)
    monkeypatch.setattr(risk_manager, "RISK_DAILY_PNL_MULT_1", 0.50)
    monkeypatch.setattr(risk_manager, "RISK_DAILY_PNL_MULT_2", 0.70)
    monkeypatch.setattr(risk_manager, "RISK_DAILY_PNL_MULT_3", 0.85)

    mgr = DynamicRiskManager(config=RiskConfig(), equity=10_000.0)
    mgr.daily_pnl = -350.0  # 3.5% drawdown
    adjusted = mgr._adjust_for_conditions(1.0)

    # Correct ordering should apply threshold 0.03 (middle) -> mult_2.
    assert abs(adjusted - 0.70) < 1e-9


def test_get_sector_strips_only_terminal_suffix(monkeypatch) -> None:
    monkeypatch.setitem(risk_manager.SECTOR_MAP, "USDC", "STABLE")
    assert risk_manager.get_sector("USDCUSD") == "STABLE"


def test_get_sector_supports_skill_map_override(monkeypatch) -> None:
    monkeypatch.setitem(risk_manager._SKILL_RISK_CFG, "sector_map", {"USDC": "STABLE", "NEWTKN": "AI"})
    assert risk_manager.get_sector("USDCUSDT") == "STABLE"
    assert risk_manager.get_sector("NEWTKN-PERP") == "AI"


def test_get_sector_supports_skill_map_override_normalization(monkeypatch) -> None:
    monkeypatch.setitem(risk_manager._SKILL_RISK_CFG, "sector_map", {"xyz": "meme"})
    assert risk_manager.get_sector("XYZUSDT") == "MEME"


def test_safety_cap_zero_can_force_zero_risk() -> None:
    cfg = RiskConfig(min_risk_pct=0.5, max_risk_pct=2.5, base_risk_pct=1.0)
    mgr = DynamicRiskManager(config=cfg, equity=10_000.0, safety_manager=_StubSafety(0.0))
    risk = mgr.calculate_risk_budget(0.5, "BTC")
    assert risk == 0.0


def test_safety_cap_allows_risk_below_min_floor() -> None:
    cfg = RiskConfig(min_risk_pct=0.5, max_risk_pct=2.5, base_risk_pct=1.0)
    mgr = DynamicRiskManager(config=cfg, equity=10_000.0, safety_manager=_StubSafety(0.5))
    risk = mgr.calculate_risk_budget(0.5, "BTC")
    assert 0.0 < risk < cfg.min_risk_pct
