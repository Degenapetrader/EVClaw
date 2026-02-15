#!/usr/bin/env python3
"""Safety manager persistence regressions."""

import tempfile
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from safety_manager import SafetyManager, SafetyTier


def test_consecutive_wins_persisted_across_restart() -> None:
    with tempfile.TemporaryDirectory() as td:
        db_path = str(Path(td) / "safety.db")

        mgr1 = SafetyManager(db_path=db_path, starting_equity=10_000.0)
        mgr1.record_trade_result(25.0)
        mgr1.record_trade_result(10.0)

        mgr2 = SafetyManager(db_path=db_path, starting_equity=10_000.0)
        state = mgr2.get_state()
        assert state.consecutive_wins >= 2


def test_set_tier_updates_state_and_validates_range() -> None:
    with tempfile.TemporaryDirectory() as td:
        db_path = str(Path(td) / "safety.db")
        mgr = SafetyManager(db_path=db_path, starting_equity=10_000.0)

        state = mgr.set_tier(3)
        assert state.current_tier == SafetyTier.DEFENSIVE

        with pytest.raises(ValueError):
            mgr.set_tier(0)
        with pytest.raises(ValueError):
            mgr.set_tier(6)


def test_daily_loss_pct_uses_start_of_day_equity_not_depleted_equity() -> None:
    with tempfile.TemporaryDirectory() as td:
        db_path = str(Path(td) / "safety.db")
        mgr = SafetyManager(db_path=db_path, starting_equity=10_000.0)
        mgr.TIER_2_LOSS_PCT = 40.0
        mgr.TIER_3_LOSS_PCT = 60.0
        mgr.TIER_4_LOSS_PCT = 90.0

        state = mgr.get_state()
        state.current_tier = SafetyTier.NORMAL
        state.current_equity = 100.0
        state.daily_pnl = -50.0
        state.max_drawdown_pct = 0.0
        state.consecutive_losses = 0

        # start_of_day_equity = 150 -> daily_loss_pct=33.3%, below tier-2 threshold (40%).
        assert mgr._evaluate_tier(state) == SafetyTier.NORMAL
