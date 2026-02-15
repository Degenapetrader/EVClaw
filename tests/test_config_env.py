#!/usr/bin/env python3
"""config_env YAML-first guard regressions."""

import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from config_env import apply_env_overrides


def _set_env(updates: dict[str, str | None]) -> dict[str, str | None]:
    prev: dict[str, str | None] = {}
    for key, value in updates.items():
        prev[key] = os.environ.get(key)
        if value is None:
            os.environ.pop(key, None)
        else:
            os.environ[key] = value
    return prev


def _restore_env(prev: dict[str, str | None]) -> None:
    for key, value in prev.items():
        if value is None:
            os.environ.pop(key, None)
        else:
            os.environ[key] = value


def test_symbols_env_is_ignored_by_default_yaml_first_mode() -> None:
    cfg = {"config": {"symbols": ["BTC", "ETH"]}}
    prev = _set_env(
        {
            "EVCLAW_ALLOW_LEGACY_PARAM_ENV": None,
            "EVCLAW_SYMBOLS": "SOL,AVAX",
        }
    )
    try:
        out = apply_env_overrides(cfg)
    finally:
        _restore_env(prev)

    assert out["config"]["symbols"] == ["BTC", "ETH"]


def test_symbols_env_is_ignored_even_if_legacy_toggle_is_set() -> None:
    cfg = {"config": {"symbols": ["BTC"]}}
    prev = _set_env(
        {
            "EVCLAW_ALLOW_LEGACY_PARAM_ENV": "1",
            "EVCLAW_SYMBOLS": "SOL,AVAX",
        }
    )
    try:
        out = apply_env_overrides(cfg)
    finally:
        _restore_env(prev)

    assert out["config"]["symbols"] == ["BTC"]


def test_legacy_enabled_venues_env_no_longer_applies() -> None:
    cfg = {
        "config": {
            "executor": {
                "lighter_enabled": False,
                "hl_enabled": True,
                "hl_wallet_enabled": False,
            }
        }
    }
    prev = _set_env(
        {
            "EVCLAW_ENABLED_VENUES": "",
            "ENABLED_VENUES": "lighter",
            "HL_PRIVATE_KEY": None,
        }
    )
    try:
        out = apply_env_overrides(cfg)
    finally:
        _restore_env(prev)

    executor_cfg = out["config"]["executor"]
    assert executor_cfg["lighter_enabled"] is False
    assert executor_cfg["hl_enabled"] is True
