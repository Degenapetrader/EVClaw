#!/usr/bin/env python3
"""Tests for strong-signal integration.

These tests must be hermetic (not depend on the VPS env vars used in live trading).
"""

import os
import sys
from contextlib import contextmanager
from pathlib import Path
from tempfile import TemporaryDirectory

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import context_builder_v2
from context_builder_v2 import ContextBuilderV2, ScoredOpportunity
from trading_brain import TradingBrain


@contextmanager
def _hermetic_context_builder_env():
    """Disable dual-universe allowlist + HL-only mode for unit tests."""
    old_loader = context_builder_v2.load_dual_venue_symbols_cached
    old_hl_only = os.environ.get("EVCLAW_HL_ONLY")
    try:
        context_builder_v2.load_dual_venue_symbols_cached = lambda *a, **k: []
        os.environ.pop("EVCLAW_HL_ONLY", None)
        yield
    finally:
        context_builder_v2.load_dual_venue_symbols_cached = old_loader
        if old_hl_only is None:
            os.environ.pop("EVCLAW_HL_ONLY", None)
        else:
            os.environ["EVCLAW_HL_ONLY"] = old_hl_only


def test_strong_signal_forced_into_top_list() -> None:
    with _hermetic_context_builder_env():
        with TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            memory_dir = tmp_path / "memory"
            signals_dir = tmp_path / "signals"
            memory_dir.mkdir()
            signals_dir.mkdir()

            builder = ContextBuilderV2(
                db_path="",
                memory_dir=memory_dir,
                signals_dir=signals_dir,
                max_opportunities=10,
                min_score=90.0,
            )

            sse_data = {
                "AAA": {
                    "symbol": "AAA",
                    "price": 100.0,
                    "hl_mid": 100.0,
                    "lighter_mid": 100.0,
                    "perp_signals": {
                        # Strong signals are whale/dead_capital with banner_trigger.
                        "whale": {"signal": "LONG", "strength": 1.0, "banner_trigger": True},
                    },
                    "atr": {"atr_pct": 1.0},
                    "price_change": {"volume_24h": 1_000_000, "pct_24h": 0.0},
                }
            }

            top = builder.get_opportunities(sse_data=sse_data)
            assert len(top) == 1
            assert top[0].symbol == "AAA"
            assert top[0].score < 90.0
            assert "whale" in (top[0].strong_signals or [])


def test_strong_signals_expand_top_k() -> None:
    with _hermetic_context_builder_env():
        with TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            memory_dir = tmp_path / "memory"
            signals_dir = tmp_path / "signals"
            memory_dir.mkdir()
            signals_dir.mkdir()

            builder = ContextBuilderV2(
                db_path="",
                memory_dir=memory_dir,
                signals_dir=signals_dir,
                max_opportunities=3,
                min_score=0.0,
            )

            sse_data = {}
            for idx in range(5):
                symbol = f"SYM{idx}"
                sse_data[symbol] = {
                    "symbol": symbol,
                    "price": 100.0,
                    "hl_mid": 100.0,
                    "lighter_mid": 100.0,
                    "perp_signals": {
                        "whale": {"signal": "LONG", "strength": 1.0, "banner_trigger": True},
                    },
                    "atr": {"atr_pct": 1.0},
                    "price_change": {"volume_24h": 1_000_000, "pct_24h": 0.0},
                }

            top = builder.get_opportunities(sse_data=sse_data)
            assert len(top) == 5


def test_dead_capital_strong_forces_trade() -> None:
    opp = ScoredOpportunity(
        symbol="ETH",
        score=10.0,
        direction="MIXED",
        signals={
            "dead_capital": {"direction": "LONG", "strength": 1.0, "z_score": 2.0},
            "cvd": {"direction": "SHORT", "z_score": 0.2, "z_smart": 0.2, "z_dumb": 0.1},
        },
        key_metrics={"atr_pct": 1.0},
        strong_signals=["dead_capital"],
    )

    brain = TradingBrain()
    decision = brain.evaluate_opportunity(opp)

    assert decision.direction == "LONG"
    assert decision.should_trade is True
    assert decision.vetoed is False


if __name__ == "__main__":
    test_strong_signal_forced_into_top_list()
    test_dead_capital_strong_forces_trade()
    print("[PASS] strong signal integration")
