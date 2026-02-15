#!/usr/bin/env python3
"""Regression tests for balanced default alignment in CLI config builder."""

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import cli


@pytest.mark.parametrize(
    ("field", "expected"),
    [
        ("enable_sltp_backstop", True),
        ("use_fill_reconciler", True),
        ("write_positions_yaml", False),
        ("min_position_notional_usd", 100.0),
        ("min_entry_fill_ratio", 0.05),
        ("chase_entry_tif", "Alo"),
        ("chase_exit_tif", "Alo"),
        ("chase_entry_fallback_after_seconds", 180.0),
        ("chase_exit_fallback_after_seconds", 60.0),
        ("sr_limit_enabled", True),
        ("sr_limit_max_pending", 20),
        ("sr_limit_max_notional_pct", 200.0),
        ("sr_limit_timeout_minutes", 60),
        ("sr_limit_chase_conviction_threshold", 0.5),
        ("sr_limit_fallback_chase_min_conviction", 0.3),
    ],
)
def test_build_execution_config_balanced_fallbacks(monkeypatch, field, expected) -> None:
    monkeypatch.delenv("EVCLAW_EXECUTOR_BASE_POSITION_PCT", raising=False)
    out = cli.build_execution_config({"config": {}}, dry_run=False)
    assert getattr(out, field) == expected
