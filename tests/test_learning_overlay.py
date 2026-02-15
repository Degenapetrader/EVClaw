#!/usr/bin/env python3
"""
Tests for apply_learning_overlay function.

Tests conviction penalty calculation:
- Pattern avoidance: 0.7 multiplier
- Signal adjustments: product of individual multipliers
- Symbol adjustment: symbol-specific multiplier
"""

import sys
from pathlib import Path
from unittest.mock import Mock

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from live_agent_utils import apply_learning_overlay


def test_apply_learning_overlay_all_penalties():
    """Test full penalty: pattern (0.7) * signal (0.9) * symbol (0.8)"""
    print("Test 1: Full penalty calculation")
    print("-" * 60)

    candidate = {
        "symbol": "ETH",
        "direction": "LONG",
        "conviction": 0.66,
        "signals": ["whale", "cvd"],
        "signals_snapshot": {
            "whale": {"direction": "LONG", "z_score": 2.5},
            "cvd": {"direction": "LONG", "z_score": 3.0},
        },
    }

    # Setup mock learning engine
    learning_engine = Mock()
    learning_engine.should_avoid_pattern.return_value = True  # -> 0.7 penalty
    learning_engine.get_signal_adjustment.side_effect = lambda sig: {
        "cvd": 0.9,
        "whale": 1.0,  # No penalty for whale
    }.get(sig, 1.0)
    learning_engine.get_symbol_adjustment.return_value = 0.8

    result = apply_learning_overlay(candidate, learning_engine)

    # 0.66 * 0.7 * 0.9 * 0.8 = 0.33264
    expected = 0.66 * 0.7 * 0.9 * 0.8
    print(f"  - Original conviction: 0.66")
    print(f"  - Penalties: pattern=0.7, cvd=0.9, symbol=0.8")
    print(f"  - Expected: {expected:.5f}")
    print(f"  - Actual: {result['conviction']:.5f}")

    assert abs(result["conviction"] - expected) < 0.0001, \
        f"Expected {expected}, got {result['conviction']}"
    assert "learning_note" in result, "Should have learning_note"
    assert "pattern_penalty=0.7" in result["learning_note"]
    assert "cvd_adj=0.90" in result["learning_note"]
    assert "symbol_adj=0.80" in result["learning_note"]

    print("PASS\n")


def test_apply_learning_overlay_bullish_direction():
    """Test BULLISH is normalized to LONG for signal matching."""
    print("Test 2: BULLISH direction normalization")
    print("-" * 60)

    candidate = {
        "symbol": "ETH",
        "direction": "LONG",
        "conviction": 0.66,
        "signals_snapshot": {
            "cvd": {"signal": "BULLISH", "z_smart": 2.8},  # Uses 'signal' key with BULLISH
        },
    }

    learning_engine = Mock()
    learning_engine.should_avoid_pattern.return_value = False
    learning_engine.get_signal_adjustment.return_value = 0.9
    learning_engine.get_symbol_adjustment.return_value = 1.0

    result = apply_learning_overlay(candidate, learning_engine)

    # Signal should match: BULLISH -> LONG, so cvd adjustment applies
    expected = 0.66 * 0.9
    print(f"  - Signal 'cvd' has direction 'BULLISH' (normalized to LONG)")
    print(f"  - Candidate direction: LONG")
    print(f"  - Expected: {expected:.3f} (0.66 * 0.9)")
    print(f"  - Actual: {result['conviction']:.3f}")

    assert abs(result["conviction"] - expected) < 0.001, \
        f"BULLISH should be normalized to LONG, expected {expected}, got {result['conviction']}"

    print("PASS\n")


def test_apply_learning_overlay_no_learning_engine():
    """Test graceful handling when learning_engine is None."""
    print("Test 3: No learning engine")
    print("-" * 60)

    candidate = {"symbol": "ETH", "direction": "LONG", "conviction": 0.5}

    result = apply_learning_overlay(candidate, None)

    assert result["conviction"] == 0.5, "Should return unchanged conviction"
    assert result is candidate, "Should return same object"
    print("  - Returned unchanged candidate")

    print("PASS\n")


def test_apply_learning_overlay_no_signals():
    """Test graceful handling when signals_snapshot is missing/empty."""
    print("Test 4: No signals snapshot")
    print("-" * 60)

    candidate = {"symbol": "ETH", "direction": "LONG", "conviction": 0.5}

    learning_engine = Mock()
    learning_engine.get_symbol_adjustment.return_value = 0.8

    result = apply_learning_overlay(candidate, learning_engine)

    # No signals -> no pattern or signal penalty, but symbol adjustment still applies
    expected = 0.5 * 0.8
    print(f"  - No signals_snapshot")
    print(f"  - Symbol adjustment still applies: 0.8")
    print(f"  - Expected: {expected} (0.5 * 0.8)")
    print(f"  - Actual: {result['conviction']}")

    assert abs(result["conviction"] - expected) < 0.001, \
        f"Symbol adjustment should still apply, expected {expected}, got {result['conviction']}"

    # Verify pattern check was NOT called (no signals_for)
    learning_engine.should_avoid_pattern.assert_not_called()

    print("PASS\n")


def test_apply_learning_overlay_no_penalty():
    """Test no penalty when pattern not avoided and adjustments are 1.0."""
    print("Test 5: No penalties applied")
    print("-" * 60)

    candidate = {
        "symbol": "ETH",
        "direction": "LONG",
        "conviction": 0.66,
        "signals_snapshot": {"cvd": {"direction": "LONG", "z_score": 2.0}},
    }

    learning_engine = Mock()
    learning_engine.should_avoid_pattern.return_value = False
    learning_engine.get_signal_adjustment.return_value = 1.0
    learning_engine.get_symbol_adjustment.return_value = 1.0

    result = apply_learning_overlay(candidate, learning_engine)

    assert result["conviction"] == 0.66, "Conviction should be unchanged"
    assert "learning_note" not in result, "No note when no penalty"
    print("  - No penalties -> conviction unchanged, no learning_note")

    print("PASS\n")


def test_apply_learning_overlay_exception_handling():
    """Test that exceptions don't crash the cycle."""
    print("Test 6: Exception handling")
    print("-" * 60)

    candidate = {
        "symbol": "ETH",
        "direction": "LONG",
        "conviction": 0.5,
        "signals_snapshot": {"cvd": {"direction": "LONG"}},
    }

    learning_engine = Mock()
    learning_engine.should_avoid_pattern.side_effect = Exception("DB error")

    result = apply_learning_overlay(candidate, learning_engine)

    assert result["conviction"] == 0.5, "Should return unchanged on exception"
    print("  - Exception caught, returned unchanged candidate")

    print("PASS\n")


def test_apply_learning_overlay_direction_filtering():
    """Test that only signals matching direction are penalized."""
    print("Test 7: Direction filtering")
    print("-" * 60)

    candidate = {
        "symbol": "ETH",
        "direction": "LONG",
        "conviction": 0.66,
        "signals_snapshot": {
            "cvd": {"direction": "LONG", "z_score": 2.0},    # Should be used
            "fade": {"direction": "SHORT", "z_score": 2.0},  # Should be ignored
        },
    }

    learning_engine = Mock()
    learning_engine.should_avoid_pattern.return_value = False
    learning_engine.get_signal_adjustment.side_effect = lambda sig: 0.9 if sig == "cvd" else 0.5
    learning_engine.get_symbol_adjustment.return_value = 1.0

    result = apply_learning_overlay(candidate, learning_engine)

    # Only cvd (0.9) should be applied, not fade (0.5)
    expected = 0.66 * 0.9
    print(f"  - cvd is LONG (matches), fade is SHORT (filtered out)")
    print(f"  - Only cvd adjustment (0.9) applied")
    print(f"  - Expected: {expected:.3f}")
    print(f"  - Actual: {result['conviction']:.3f}")

    assert abs(result["conviction"] - expected) < 0.001, \
        f"Only matching direction signals should apply, expected {expected}, got {result['conviction']}"

    print("PASS\n")


def test_apply_learning_overlay_conviction_clamping():
    """Test that conviction is clamped to [0, 1]."""
    print("Test 8: Conviction clamping")
    print("-" * 60)

    # Edge case: already at 0
    candidate = {
        "symbol": "ETH",
        "direction": "LONG",
        "conviction": 0.0,
        "signals_snapshot": {"cvd": {"direction": "LONG"}},
    }

    learning_engine = Mock()
    learning_engine.should_avoid_pattern.return_value = True
    learning_engine.get_signal_adjustment.return_value = 0.5
    learning_engine.get_symbol_adjustment.return_value = 0.5

    result = apply_learning_overlay(candidate, learning_engine)

    assert result["conviction"] == 0.0, "Should stay at 0"
    print("  - Conviction 0.0 stays at 0.0")

    # Edge case: at 1.0 with penalties
    candidate2 = {
        "symbol": "ETH",
        "direction": "LONG",
        "conviction": 1.0,
        "signals_snapshot": {"cvd": {"direction": "LONG"}},
    }

    result2 = apply_learning_overlay(candidate2, learning_engine)
    # 1.0 * 0.7 * 0.5 * 0.5 = 0.175
    expected2 = 1.0 * 0.7 * 0.5 * 0.5
    assert abs(result2["conviction"] - expected2) < 0.001, f"Expected {expected2}"
    print(f"  - Conviction 1.0 with penalties -> {result2['conviction']:.3f}")
    assert 0 <= result2["conviction"] <= 1, "Must be clamped to [0, 1]"

    print("PASS\n")


if __name__ == "__main__":
    print("=" * 60)
    print("Learning Overlay Tests")
    print("=" * 60)
    print()

    test_apply_learning_overlay_all_penalties()
    test_apply_learning_overlay_bullish_direction()
    test_apply_learning_overlay_no_learning_engine()
    test_apply_learning_overlay_no_signals()
    test_apply_learning_overlay_no_penalty()
    test_apply_learning_overlay_exception_handling()
    test_apply_learning_overlay_direction_filtering()
    test_apply_learning_overlay_conviction_clamping()

    print("=" * 60)
    print("All tests passed!")
    print("=" * 60)
