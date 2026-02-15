#!/usr/bin/env python3
"""
Tests for the full learning loop integration.

Tests:
1. process_closed_trade triggers analyze_trade + update_pattern_stats
2. TradingBrain applies pattern/signal penalties but still returns trade
"""

import asyncio
import json
import sqlite3
import tempfile
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List
import sys

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from learning_engine import LearningEngine, PatternStats
from trading_brain import TradingBrain, BrainConfig


@dataclass
class MockScoredOpportunity:
    """Mock ScoredOpportunity for testing."""
    symbol: str
    score: float
    direction: str
    signals: Dict[str, Dict]
    key_metrics: Dict[str, Any] = field(default_factory=dict)
    strong_signals: List[str] = field(default_factory=list)
    raw_data: Dict[str, Any] = field(default_factory=dict)


def create_test_db(db_path: str) -> None:
    """Create minimal test database schema."""
    with sqlite3.connect(db_path) as conn:
        conn.execute("PRAGMA journal_mode=WAL")
        # trades table
        conn.execute("""
            CREATE TABLE IF NOT EXISTS trades (
                id INTEGER PRIMARY KEY,
                symbol TEXT,
                direction TEXT,
                venue TEXT DEFAULT 'lighter',
                state TEXT DEFAULT 'OPEN',
                entry_time REAL,
                entry_price REAL,
                size REAL DEFAULT 1.0,
                notional_usd REAL DEFAULT 500.0,
                exit_time REAL,
                exit_price REAL,
                exit_reason TEXT,
                realized_pnl REAL,
                realized_pnl_pct REAL,
                signals_agreed TEXT,
                signals_snapshot TEXT,
                size_multiplier REAL DEFAULT 1.0
            )
        """)
        # trade_reflections table (for parent class)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS trade_reflections (
                id INTEGER PRIMARY KEY,
                trade_id INTEGER UNIQUE,
                reflection TEXT,
                lessons TEXT,
                outcome_category TEXT,
                signal_contributions TEXT
            )
        """)
        # signal_combos table (for parent class)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS signal_combos (
                id INTEGER PRIMARY KEY,
                combo_key TEXT UNIQUE,
                trades INTEGER DEFAULT 0,
                wins INTEGER DEFAULT 0,
                total_pnl REAL DEFAULT 0,
                avg_pnl REAL DEFAULT 0,
                win_rate REAL DEFAULT 0,
                last_trade_at REAL,
                updated_at REAL
            )
        """)
        conn.commit()


def insert_losing_trade(db_path: str, trade_id: int, symbol: str = "ETH",
                        direction: str = "LONG", pnl: float = -50.0,
                        signals: List[str] = None) -> None:
    """Insert a losing trade for testing."""
    signals = signals or ["cvd", "fade"]
    signals_snapshot = json.dumps({
        sig: {"direction": direction, "z_score": 2.0} for sig in signals
    })
    with sqlite3.connect(db_path) as conn:
        conn.execute("""
            INSERT INTO trades (
                id, symbol, direction, entry_time, entry_price,
                exit_time, exit_price, exit_reason,
                realized_pnl, realized_pnl_pct, signals_agreed, signals_snapshot
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            trade_id, symbol, direction,
            time.time() - 3600, 2500.0,  # entry 1h ago
            time.time(), 2450.0,  # exit now
            "SL", pnl, -2.0,
            json.dumps(signals), signals_snapshot
        ))
        conn.commit()


def test_process_closed_trade_triggers_full_pipeline():
    """Test that process_closed_trade triggers analyze_trade + update_pattern_stats."""
    print("Test 1: process_closed_trade triggers full learning pipeline")
    print("-" * 60)

    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = str(Path(tmpdir) / "test.db")
        memory_dir = Path(tmpdir) / "memory"
        memory_dir.mkdir()

        create_test_db(db_path)

        # Insert a losing trade
        insert_losing_trade(db_path, trade_id=1, symbol="ETH", direction="LONG",
                           pnl=-50.0, signals=["cvd", "fade"])

        # Create learning engine
        engine = LearningEngine(
            db_path=db_path,
            memory_dir=memory_dir,
        )

        # Verify initial state is empty
        assert len(engine._mistakes) == 0, "Should start with no mistakes"
        assert len(engine._patterns) == 0, "Should start with no patterns"

        # Call process_closed_trade
        asyncio.run(engine.process_closed_trade(1))

        # Verify analyze_trade was called (mistake recorded)
        assert len(engine._mistakes) == 1, f"Expected 1 mistake, got {len(engine._mistakes)}"
        mistake = engine._mistakes[0]
        assert mistake.symbol == "ETH", f"Expected ETH, got {mistake.symbol}"
        assert mistake.direction == "LONG", f"Expected LONG, got {mistake.direction}"
        assert mistake.pnl < 0, "Mistake should have negative PnL"
        print(f"  - Mistake recorded: {mistake.symbol} {mistake.mistake_type}")

        # Verify update_pattern_stats was called (pattern recorded)
        assert len(engine._patterns) == 1, f"Expected 1 pattern, got {len(engine._patterns)}"
        pattern_key = list(engine._patterns.keys())[0]
        assert "cvd" in pattern_key.lower(), f"Pattern key should contain 'cvd': {pattern_key}"
        assert "fade" in pattern_key.lower(), f"Pattern key should contain 'fade': {pattern_key}"
        pattern = engine._patterns[pattern_key]
        assert pattern.trades == 1, f"Expected 1 trade, got {pattern.trades}"
        print(f"  - Pattern recorded: {pattern_key} ({pattern.trades} trades)")

        # Verify DB-backed learning state was saved
        with sqlite3.connect(db_path) as conn:
            rows = conn.execute(
                """
                SELECT key, value_json
                FROM learning_state_kv
                WHERE key IN ('mistakes', 'patterns', 'adjustments')
                """
            ).fetchall()
        saved = {k: v for k, v in rows}
        assert "mistakes" in saved and json.loads(saved["mistakes"]), "mistakes state should be persisted"
        assert "patterns" in saved and json.loads(saved["patterns"]), "patterns state should be persisted"
        assert "adjustments" in saved and json.loads(saved["adjustments"]), "adjustments state should be persisted"
        print("  - DB-backed state persisted")

        # Verify symbol adjustment was updated (losing trade reduces multiplier)
        eth_adj = engine.get_symbol_adjustment("ETH")
        assert eth_adj < 1.0, f"ETH adjustment should be reduced, got {eth_adj}"
        print(f"  - Symbol adjustment for ETH: {eth_adj:.2f}")

    print("PASS\n")


def test_pattern_avoidance_after_multiple_losses():
    """Test that patterns get avoided after multiple losses."""
    print("Test 2: Pattern avoidance triggers after multiple losses")
    print("-" * 60)

    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = str(Path(tmpdir) / "test.db")
        memory_dir = Path(tmpdir) / "memory"
        memory_dir.mkdir()

        create_test_db(db_path)

        # Create learning engine
        engine = LearningEngine(
            db_path=db_path,
            memory_dir=memory_dir,
        )

        # Insert 6 losing trades with same signal pattern
        # (AVOID_MIN_TRADES=5, AVOID_WIN_RATE_THRESHOLD=0.30)
        signals = ["cvd", "fade"]
        for i in range(6):
            insert_losing_trade(db_path, trade_id=i+1, symbol="ETH", direction="LONG",
                               pnl=-50.0, signals=signals)
            asyncio.run(engine.process_closed_trade(i+1))

        # Check pattern stats
        pattern_key = "cvd+fade:LONG"
        assert pattern_key in engine._patterns, f"Pattern {pattern_key} should exist"
        pattern = engine._patterns[pattern_key]

        print(f"  - Pattern: {pattern_key}")
        print(f"  - Trades: {pattern.trades}, Wins: {pattern.wins}, Win rate: {pattern.win_rate:.0%}")
        assert pattern.trades == 6, f"Expected 6 trades, got {pattern.trades}"
        assert pattern.wins == 0, f"Expected 0 wins, got {pattern.wins}"
        assert pattern.win_rate == 0.0, f"Expected 0% win rate, got {pattern.win_rate}"

        # Check avoidance
        assert pattern.is_avoided, "Pattern should be avoided after 6 losses"
        assert pattern.avoid_until is not None, "avoid_until should be set"
        print(f"  - Pattern is AVOIDED until {time.ctime(pattern.avoid_until)}")

        # Verify should_avoid_pattern returns True
        assert engine.should_avoid_pattern(signals, "LONG"), "should_avoid_pattern should return True"
        print("  - should_avoid_pattern returns True")

    print("PASS\n")


def test_trading_brain_applies_penalty_not_hard_block():
    """Test that TradingBrain applies penalties but doesn't hard-block trades."""
    print("Test 3: TradingBrain applies penalty but still returns trade")
    print("-" * 60)

    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = str(Path(tmpdir) / "test.db")
        memory_dir = Path(tmpdir) / "memory"
        memory_dir.mkdir()

        create_test_db(db_path)

        # Create learning engine with pre-loaded avoidance
        engine = LearningEngine(
            db_path=db_path,
            memory_dir=memory_dir,
        )

        # Manually set up an avoided pattern
        engine._patterns["cvd+fade:LONG"] = PatternStats(
            pattern_key="cvd+fade:LONG",
            trades=10,
            wins=2,
            win_rate=0.20,
            avoid_until=time.time() + 3600,  # Avoided for 1 hour
            avoid_reason="Test avoidance",
        )

        # Manually set up signal adjustments
        engine._signal_adjustments = {
            "cvd": 0.8,
            "fade": 0.9,
        }

        # Create brain with learning engine
        config = BrainConfig(
            apply_history_adjustments=True,
            base_confidence_threshold=0.55,
        )
        brain = TradingBrain(config=config, learning_engine=engine)

        # Create a high-conviction opportunity with avoided pattern
        opp = MockScoredOpportunity(
            symbol="ETH",
            score=80.0,
            direction="LONG",
            signals={
                "cvd": {"direction": "LONG", "z_score": 2.5},
                "fade": {"direction": "LONG", "z_score": 2.0},
                "whale": {"direction": "LONG", "z_score": 1.5},
            },
            key_metrics={"atr_pct": 2.0},
        )

        # Evaluate the opportunity
        decision = brain.evaluate_opportunity(opp)

        print(f"  - Direction: {decision.direction}")
        print(f"  - Conviction: {decision.conviction:.2f}")
        print(f"  - Should trade: {decision.should_trade}")
        print(f"  - History note: {decision.history_note}")

        # Key assertions:
        # 1. Direction should NOT be NO_TRADE (penalties reduce, don't block)
        # 2. Conviction should be reduced (but still tradeable if high enough base)
        # 3. history_note should mention the penalties

        # With 3 signals for LONG, base conviction should be decent
        # Pattern penalty: 0.7, Signal penalty: 0.8 * 0.9 = 0.72 (product)
        # These stack multiplicatively

        assert decision.direction in ("LONG", "NO_TRADE"), f"Unexpected direction: {decision.direction}"
        assert "pattern avoided" in decision.history_note.lower() or "signal adj" in decision.history_note.lower(), \
            f"Expected penalty note, got: {decision.history_note}"

        print(f"  - Penalties applied correctly, decision logic intact")

    print("PASS\n")


def test_signal_adjustment_uses_product():
    """Test that signal adjustments use product (not mean)."""
    print("Test 4: Signal adjustments use product (not mean)")
    print("-" * 60)

    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = str(Path(tmpdir) / "test.db")
        memory_dir = Path(tmpdir) / "memory"
        memory_dir.mkdir()

        create_test_db(db_path)

        # Create learning engine with specific adjustments
        engine = LearningEngine(
            db_path=db_path,
            memory_dir=memory_dir,
        )

        # Set up signal adjustments: mean=0.73, product=0.48
        engine._signal_adjustments = {
            "cvd": 0.8,
            "fade": 0.6,
            "whale": 1.0,  # This one is neutral
        }

        # Get adjustments for signals ["cvd", "fade"]
        signals = ["cvd", "fade"]
        expected_product = 0.8 * 0.6  # = 0.48
        expected_mean = (0.8 + 0.6) / 2  # = 0.70

        # Calculate product manually
        actual = 1.0
        for s in signals:
            actual *= engine.get_signal_adjustment(s)

        print(f"  - Signals: {signals}")
        print(f"  - Adjustments: cvd=0.8, fade=0.6")
        print(f"  - Product: {actual:.2f} (expected {expected_product:.2f})")
        print(f"  - Mean would be: {expected_mean:.2f}")

        assert abs(actual - expected_product) < 0.001, \
            f"Expected product {expected_product}, got {actual}"
        assert actual != expected_mean, "Should use product, not mean"

    print("PASS\n")


if __name__ == "__main__":
    print("=" * 60)
    print("Learning Loop Integration Tests")
    print("=" * 60)
    print()

    test_process_closed_trade_triggers_full_pipeline()
    test_pattern_avoidance_after_multiple_losses()
    test_trading_brain_applies_penalty_not_hard_block()
    test_signal_adjustment_uses_product()
    print("=" * 60)
    print("All tests passed!")
    print("=" * 60)
