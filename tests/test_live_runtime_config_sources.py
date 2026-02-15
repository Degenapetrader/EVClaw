#!/usr/bin/env python3
"""Runtime source ownership tests for live execution config paths."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import cli
import live_agent_process
import mode_controller
from risk_manager import RiskConfig


def test_build_execution_config_uses_skill_executor_value_not_mode_preset(monkeypatch) -> None:
    monkeypatch.setitem(
        mode_controller.BALANCED_PRESET["executor"],
        "default_base_position_size_usd",
        99999.0,
    )
    config = {"config": {"executor": {"base_position_size_usd": 321.0}}}
    out = cli.build_execution_config(config, dry_run=False)
    assert out.base_position_size_usd == 321.0


def test_build_live_risk_config_uses_skill_risk_values() -> None:
    config = {
        "config": {
            "risk": {
                "daily_drawdown_limit_pct": 12.0,
                "max_sector_concentration": 42,
                "max_position_pct": 0.65,
                "emergency_loss_pct": 17.0,
                "emergency_portfolio_loss_pct": 24.0,
                "stale_signal_max_minutes": 14.0,
            }
        }
    }
    out = live_agent_process.build_live_risk_config(config, risk_config_cls=RiskConfig)
    assert out.daily_drawdown_limit_pct == 12.0
    assert out.max_sector_concentration == 42
    assert out.max_position_pct == 0.65
    assert out.emergency_loss_pct == 17.0
    assert out.emergency_portfolio_loss_pct == 24.0
    assert out.stale_signal_max_minutes == 14.0


def test_build_live_risk_config_default_fallbacks_are_yaml_live_defaults() -> None:
    out = live_agent_process.build_live_risk_config({"config": {}}, risk_config_cls=RiskConfig)
    assert out.daily_drawdown_limit_pct == 10.0
    assert out.max_sector_concentration == 30
    assert out.max_position_pct == 0.5
    assert out.emergency_loss_pct == 15.0
    assert out.emergency_portfolio_loss_pct == 20.0
    assert out.stale_signal_max_minutes == 10.0
