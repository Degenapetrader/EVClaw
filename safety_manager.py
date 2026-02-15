#!/usr/bin/env python3
"""
5-Tier Safety System for Captain EVP.

Tiers:
1. NORMAL - Full operation
2. CAUTION - Reduced size, extra logging
3. DEFENSIVE - Minimal trading, high-conviction only
4. HALT - No new trades, monitor only
5. EMERGENCY - Close all positions, full lockout

Features:
- Auto-escalation based on losses and drawdown
- Manual override controls
- Daily reset logic
- Cooldown support
"""

from logging_utils import get_logger
import sqlite3
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import IntEnum
from typing import Optional, Tuple

from mode_controller import get_param



# =============================================================================
# Enums and Configs
# =============================================================================

class SafetyTier(IntEnum):
    """Safety tier levels."""
    NORMAL = 1
    CAUTION = 2
    DEFENSIVE = 3
    HALT = 4
    EMERGENCY = 5


@dataclass
class TierConfig:
    """Configuration per tier."""
    max_size_multiplier: float
    min_z_score: float
    min_signals_agreeing: int
    alert_on_trade: bool
    allow_new_trades: bool


def _tier_cfg(prefix: str, *, max_size: float, min_z: float, min_signals: int, alert: bool, allow: bool) -> TierConfig:
    tier_digits = "".join(ch for ch in prefix if ch.isdigit())
    if tier_digits:
        tier_key = f"tier{tier_digits}"
        return TierConfig(
            max_size_multiplier=get_param("safety", f"{tier_key}_max_size_multiplier"),
            min_z_score=get_param("safety", f"{tier_key}_min_z_score"),
            min_signals_agreeing=get_param("safety", f"{tier_key}_min_signals_agreeing"),
            alert_on_trade=get_param("safety", f"{tier_key}_alert_on_trade"),
            allow_new_trades=get_param("safety", f"{tier_key}_allow_new_trades"),
        )
    return TierConfig(
        max_size_multiplier=max_size,
        min_z_score=min_z,
        min_signals_agreeing=min_signals,
        alert_on_trade=alert,
        allow_new_trades=allow,
    )


TIER_CONFIGS = {
    SafetyTier.NORMAL: _tier_cfg(
        "EVCLAW_SAFETY_TIER1",
        max_size=1.0,
        min_z=2.0,
        min_signals=3,
        alert=False,
        allow=True,
    ),
    SafetyTier.CAUTION: _tier_cfg(
        "EVCLAW_SAFETY_TIER2",
        max_size=0.5,
        min_z=2.0,
        min_signals=4,
        alert=True,
        allow=True,
    ),
    SafetyTier.DEFENSIVE: _tier_cfg(
        "EVCLAW_SAFETY_TIER3",
        max_size=0.25,
        min_z=2.5,
        min_signals=5,
        alert=True,
        allow=True,
    ),
    SafetyTier.HALT: _tier_cfg(
        "EVCLAW_SAFETY_TIER4",
        max_size=0.0,
        min_z=999.0,
        min_signals=999,
        alert=True,
        allow=False,
    ),
    SafetyTier.EMERGENCY: _tier_cfg(
        "EVCLAW_SAFETY_TIER5",
        max_size=0.0,
        min_z=999.0,
        min_signals=999,
        alert=True,
        allow=False,
    ),
}


@dataclass
class SafetyState:
    """Current safety state."""
    current_tier: SafetyTier
    daily_pnl: float
    daily_pnl_reset_at: str
    consecutive_losses: int
    consecutive_wins: int
    cooldown_until: Optional[str]
    max_drawdown_pct: float
    peak_equity: float
    current_equity: float
    paused: bool

    @property
    def config(self) -> TierConfig:
        """Get tier configuration."""
        return TIER_CONFIGS[self.current_tier]



# =============================================================================
# Safety Manager
# =============================================================================

class SafetyManager:
    """
    Manages safety tiers and circuit breakers.

    Tier Triggers (auto):
    - Tier 2: Daily loss > 2% OR consecutive losses >= 2
    - Tier 3: Daily loss > 4% OR consecutive losses >= 3
    - Tier 4: Daily loss > 5% OR drawdown > 15%
    - Tier 5: Manual trigger only (emergency)

    Recovery:
    - Tier 2->1: New day OR 2 consecutive wins
    - Tier 3->2: New day OR 1 win
    - Tier 4->1: Manual reset required
    """

    # Tier thresholds
    TIER_2_LOSS_PCT = get_param("safety", "tier_2_loss_pct")
    TIER_3_LOSS_PCT = get_param("safety", "tier_3_loss_pct")
    TIER_4_LOSS_PCT = get_param("safety", "tier_4_loss_pct")
    MAX_DRAWDOWN_PCT = get_param("safety", "max_drawdown_pct")

    # Recovery thresholds
    RECOVERY_WINS_TIER_2 = get_param("safety", "recovery_wins_tier_2")
    RECOVERY_WINS_TIER_3 = get_param("safety", "recovery_wins_tier_3")
    DB_RETRY_MAX = 3
    DB_RETRY_BASE_SECONDS = 0.2
    DB_RETRY_MAX_SECONDS = 2.0

    def __init__(self, db_path: str, starting_equity: float = 10000.0):
        """
        Initialize safety manager.

        Args:
            db_path: Path to ai_trader.db
            starting_equity: Starting equity for drawdown calculations
        """
        self.db_path = db_path
        self.starting_equity = starting_equity
        self.log = get_logger("safety_manager")

        self._ensure_table()

    def _connect(self) -> sqlite3.Connection:
        delay = float(self.DB_RETRY_BASE_SECONDS or 0.0)
        max_delay = float(self.DB_RETRY_MAX_SECONDS or 0.0)
        retries = int(self.DB_RETRY_MAX or 1)
        for attempt in range(1, retries + 1):
            try:
                return sqlite3.connect(self.db_path, timeout=30.0)
            except sqlite3.OperationalError as e:
                if attempt >= retries:
                    raise
                self.log.warning(
                    f"Safety DB busy (attempt {attempt}/{retries}): {e}"
                )
                sleep_for = min(max_delay or delay, delay if delay > 0 else 0.05)
                time.sleep(sleep_for)
                delay = min(max_delay or delay, max(delay * 2, 0.05))

    def _ensure_table(self) -> None:
        """Ensure safety_state table exists."""
        with self._connect() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS safety_state (
                    id INTEGER PRIMARY KEY CHECK (id = 1),
                    current_tier INTEGER DEFAULT 1,
                    daily_pnl REAL DEFAULT 0.0,
                    daily_pnl_reset_at TEXT,
                    consecutive_losses INTEGER DEFAULT 0,
                    consecutive_wins INTEGER DEFAULT 0,
                    cooldown_until TEXT,
                    max_drawdown_pct REAL DEFAULT 0.0,
                    peak_equity REAL DEFAULT 10000.0,
                    current_equity REAL DEFAULT 10000.0,
                    paused INTEGER DEFAULT 0,
                    updated_at REAL DEFAULT (strftime('%s', 'now'))
                )
            """)
            conn.execute(
                "INSERT OR IGNORE INTO safety_state (id, peak_equity, current_equity) VALUES (1, ?, ?)",
                (self.starting_equity, self.starting_equity)
            )
            cols = {
                str(r[1]).lower()
                for r in conn.execute("PRAGMA table_info(safety_state)").fetchall()
            }
            if "consecutive_wins" not in cols:
                conn.execute("ALTER TABLE safety_state ADD COLUMN consecutive_wins INTEGER DEFAULT 0")
            conn.commit()

    def get_state(self) -> SafetyState:
        """Load current safety state."""
        with self._connect() as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute("SELECT * FROM safety_state WHERE id = 1").fetchone()

            return SafetyState(
                current_tier=SafetyTier(row['current_tier']),
                daily_pnl=row['daily_pnl'],
                daily_pnl_reset_at=row['daily_pnl_reset_at'] or '',
                consecutive_losses=row['consecutive_losses'],
                consecutive_wins=int(row['consecutive_wins'] or 0),
                cooldown_until=row['cooldown_until'],
                max_drawdown_pct=row['max_drawdown_pct'],
                peak_equity=row['peak_equity'],
                current_equity=row['current_equity'],
                paused=bool(row['paused']),
            )

    def record_trade_result(self, pnl: float) -> SafetyTier:
        """
        Record trade result and update tier.

        Args:
            pnl: Trade P&L (positive or negative)

        Returns:
            New tier after evaluation
        """
        state = self.get_state()

        # Reset daily if new day (UTC)
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        if state.daily_pnl_reset_at != today:
            self.log.info(f"New day detected, resetting daily P&L (was ${state.daily_pnl:.2f})")
            state.daily_pnl = 0.0
            state.daily_pnl_reset_at = today
            # Clear cooldown on new day
            state.cooldown_until = None
            # Consider resetting tier on new day if tier < 4
            if state.current_tier < SafetyTier.HALT:
                old_tier = state.current_tier
                state.current_tier = SafetyTier.NORMAL
                state.consecutive_losses = 0
                state.consecutive_wins = 0
                if old_tier != SafetyTier.NORMAL:
                    self.log.info(f"New day: Reset tier {old_tier} -> {SafetyTier.NORMAL}")

        # Update P&L
        state.daily_pnl += pnl
        state.current_equity += pnl

        # Update peak equity (high water mark)
        if state.current_equity > state.peak_equity:
            state.peak_equity = state.current_equity

        # Calculate drawdown
        if state.peak_equity > 0:
            state.max_drawdown_pct = ((state.peak_equity - state.current_equity) / state.peak_equity) * 100

        # Update consecutive losses/wins
        if pnl < 0:
            state.consecutive_losses += 1
            state.consecutive_wins = 0
        else:
            state.consecutive_wins += 1
            # Check for recovery
            if state.current_tier == SafetyTier.CAUTION and state.consecutive_wins >= self.RECOVERY_WINS_TIER_2:
                self.log.info(f"Recovery: {self.RECOVERY_WINS_TIER_2} wins in a row, downgrading tier 2->1")
                state.current_tier = SafetyTier.NORMAL
                state.consecutive_losses = 0
            elif state.current_tier == SafetyTier.DEFENSIVE and state.consecutive_wins >= self.RECOVERY_WINS_TIER_3:
                self.log.info(f"Recovery: {self.RECOVERY_WINS_TIER_3} win(s), downgrading tier 3->2")
                state.current_tier = SafetyTier.CAUTION
                state.consecutive_losses = max(0, state.consecutive_losses - 1)

            if pnl > 0:
                state.consecutive_losses = 0

        # Evaluate tier (only escalate, not de-escalate except via recovery above)
        new_tier = self._evaluate_tier(state)
        if new_tier > state.current_tier:
            self.log.warning(f"Tier escalation: {state.current_tier.name} -> {new_tier.name}")
            state.current_tier = new_tier

        # Save
        self._save_state(state)

        return state.current_tier

    def _evaluate_tier(self, state: SafetyState) -> SafetyTier:
        """Determine appropriate tier based on state."""
        # Emergency tier (5) only from manual trigger - never auto
        if state.current_tier == SafetyTier.EMERGENCY:
            return SafetyTier.EMERGENCY

        # Halt tier (4) - requires manual reset
        if state.current_tier == SafetyTier.HALT:
            return SafetyTier.HALT

        # Calculate daily loss %
        # Use start-of-day equity estimate, not depleted current equity, so the
        # denominator does not shrink after losses.
        equity_base = float(state.current_equity or 0.0) - float(state.daily_pnl or 0.0)
        if equity_base <= 0:
            equity_base = self.starting_equity
        if equity_base <= 0:
            equity_base = 1.0
        daily_loss_pct = abs(min(0, state.daily_pnl)) / equity_base * 100

        # Check tier 4 triggers
        if daily_loss_pct >= self.TIER_4_LOSS_PCT or state.max_drawdown_pct >= self.MAX_DRAWDOWN_PCT:
            return SafetyTier.HALT

        # Check tier 3 triggers
        if daily_loss_pct >= self.TIER_3_LOSS_PCT or state.consecutive_losses >= 3:
            return SafetyTier.DEFENSIVE

        # Check tier 2 triggers
        if daily_loss_pct >= self.TIER_2_LOSS_PCT or state.consecutive_losses >= 2:
            return SafetyTier.CAUTION

        return state.current_tier  # Don't auto-downgrade

    def _save_state(self, state: SafetyState) -> None:
        """Save state to database."""
        with self._connect() as conn:
            conn.execute("""
                UPDATE safety_state SET
                    current_tier = ?,
                    daily_pnl = ?,
                    daily_pnl_reset_at = ?,
                    consecutive_losses = ?,
                    consecutive_wins = ?,
                    cooldown_until = ?,
                    max_drawdown_pct = ?,
                    peak_equity = ?,
                    current_equity = ?,
                    paused = ?,
                    updated_at = strftime('%s', 'now')
                WHERE id = 1
            """, (
                state.current_tier.value,
                state.daily_pnl,
                state.daily_pnl_reset_at,
                state.consecutive_losses,
                state.consecutive_wins,
                state.cooldown_until,
                state.max_drawdown_pct,
                state.peak_equity,
                state.current_equity,
                int(state.paused),
            ))
            conn.commit()

    # =========================================================================
    # Query Methods
    # =========================================================================

    def allows_trading(self) -> Tuple[bool, str]:
        """
        Check if trading is allowed.

        Returns:
            (allowed, reason) tuple
        """
        state = self.get_state()

        if state.paused:
            return False, "Trading paused by human"

        if state.current_tier >= SafetyTier.HALT:
            return False, f"Tier {state.current_tier.name} active - trading halted"

        if state.cooldown_until:
            try:
                cooldown_end = datetime.fromisoformat(state.cooldown_until.replace('Z', '+00:00'))
                if datetime.now(timezone.utc) < cooldown_end:
                    remaining = (cooldown_end - datetime.now(timezone.utc)).total_seconds()
                    return False, f"Cooldown active ({remaining:.0f}s remaining)"
            except ValueError:
                pass

        return True, ""

    def get_size_cap(self) -> float:
        """Get maximum size multiplier for current tier."""
        state = self.get_state()
        return state.config.max_size_multiplier



    # =========================================================================
    # Manual Controls
    # =========================================================================

    def pause(self) -> None:
        """Pause trading (human override)."""
        with self._connect() as conn:
            conn.execute("UPDATE safety_state SET paused = 1, updated_at = strftime('%s', 'now') WHERE id = 1")
            conn.commit()
        self.log.warning("Trading PAUSED by human override")

    def resume(self) -> None:
        """Resume trading (human override)."""
        with self._connect() as conn:
            conn.execute("UPDATE safety_state SET paused = 0, updated_at = strftime('%s', 'now') WHERE id = 1")
            conn.commit()
        self.log.info("Trading RESUMED by human override")

    def emergency_stop(self) -> None:
        """Trigger emergency tier (human override)."""
        with self._connect() as conn:
            conn.execute(
                "UPDATE safety_state SET current_tier = ?, updated_at = strftime('%s', 'now') WHERE id = 1",
                (SafetyTier.EMERGENCY.value,)
            )
            conn.commit()
        self.log.critical("EMERGENCY STOP triggered - all trading halted")

    def set_tier(self, tier: int) -> SafetyState:
        """Set safety tier explicitly (human override)."""
        tier_int = int(tier)
        if tier_int < SafetyTier.NORMAL.value or tier_int > SafetyTier.EMERGENCY.value:
            raise ValueError(f"Tier must be {SafetyTier.NORMAL.value}-{SafetyTier.EMERGENCY.value}")
        with self._connect() as conn:
            conn.execute(
                "UPDATE safety_state SET current_tier = ?, updated_at = strftime('%s', 'now') WHERE id = 1",
                (tier_int,),
            )
            conn.commit()
        self.log.warning(f"Safety tier manually set to {tier_int}")
        return self.get_state()

    def reset_to_normal(self) -> None:
        """Reset to tier 1 (human override for tier 4+)."""
        with self._connect() as conn:
            conn.execute("""
                UPDATE safety_state SET
                    current_tier = 1,
                    consecutive_losses = 0,
                    consecutive_wins = 0,
                    paused = 0,
                    updated_at = strftime('%s', 'now')
                WHERE id = 1
            """)
            conn.commit()
        self.log.info("Safety state RESET to NORMAL by human override")



# =============================================================================
# Test
# =============================================================================

if __name__ == "__main__":
    import tempfile
    from pathlib import Path

    print("Safety Manager Test")
    print("=" * 60)

    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.db"

        # Initialize with $10,000 starting equity
        manager = SafetyManager(db_path=str(db_path), starting_equity=10000.0)

        # Test 1: Initial state
        print("\nTest 1: Initial state")
        print("-" * 40)
        state = manager.get_state()
        print(f"Tier: {state.current_tier.name}")
        print(f"Daily P&L: ${state.daily_pnl:.2f}")
        print(f"Equity: ${state.current_equity:.2f}")
        assert state.current_tier == SafetyTier.NORMAL
        print("PASS")

        # Test 2: Trading allowed
        print("\nTest 2: Trading allowed check")
        print("-" * 40)
        allowed, reason = manager.allows_trading()
        print(f"Allowed: {allowed}, Reason: {reason}")
        assert allowed == True
        print("PASS")

        # Test 3: Record winning trade
        print("\nTest 3: Winning trade")
        print("-" * 40)
        tier = manager.record_trade_result(pnl=50.0)
        state = manager.get_state()
        print(f"P&L: +$50, Tier: {tier.name}, Equity: ${state.current_equity:.2f}")
        assert tier == SafetyTier.NORMAL
        assert state.current_equity == 10050.0
        print("PASS")

        # Test 4: Record losing trades to escalate to tier 2
        print("\nTest 4: Two losses -> Tier 2")
        print("-" * 40)
        manager.record_trade_result(pnl=-30.0)
        tier = manager.record_trade_result(pnl=-40.0)
        state = manager.get_state()
        print(f"Consecutive losses: {state.consecutive_losses}")
        print(f"Tier: {tier.name}")
        assert tier == SafetyTier.CAUTION
        assert state.consecutive_losses == 2
        print("PASS")

        # Test 5: Size cap at tier 2
        print("\nTest 5: Size cap at tier 2")
        print("-" * 40)
        size_cap = manager.get_size_cap()
        print(f"Size cap: {size_cap}")
        assert size_cap == 0.5
        print("PASS")

        # Test 6: Another loss -> Tier 3
        print("\nTest 6: Third loss -> Tier 3")
        print("-" * 40)
        tier = manager.record_trade_result(pnl=-25.0)
        state = manager.get_state()
        print(f"Consecutive losses: {state.consecutive_losses}")
        print(f"Tier: {tier.name}")
        assert tier == SafetyTier.DEFENSIVE
        print("PASS")

        # Test 7: Recovery with win
        print("\nTest 7: Win recovery -> Tier 2")
        print("-" * 40)
        tier = manager.record_trade_result(pnl=20.0)
        state = manager.get_state()
        print(f"Tier after win: {tier.name}")
        assert tier == SafetyTier.CAUTION
        print("PASS")

        # Test 8: Manual pause
        print("\nTest 8: Manual pause")
        print("-" * 40)
        manager.pause()
        allowed, reason = manager.allows_trading()
        print(f"Allowed: {allowed}, Reason: {reason}")
        assert allowed == False
        assert "paused" in reason.lower()
        print("PASS")

        # Test 9: Resume
        print("\nTest 9: Resume")
        print("-" * 40)
        manager.resume()
        allowed, reason = manager.allows_trading()
        print(f"Allowed: {allowed}")
        assert allowed == True
        print("PASS")

        # Test 10: Heavy loss -> Tier 4
        print("\nTest 10: Heavy loss -> Tier 4 (5% daily loss)")
        print("-" * 40)
        # Need to lose 5% of $10,000 = $500
        tier = manager.record_trade_result(pnl=-500.0)
        state = manager.get_state()
        allowed, reason = manager.allows_trading()
        print(f"Tier: {tier.name}")
        print(f"Daily P&L: ${state.daily_pnl:.2f}")
        print(f"Allowed: {allowed}, Reason: {reason}")
        assert tier == SafetyTier.HALT
        assert allowed == False
        print("PASS")

        # Test 11: Manual reset
        print("\nTest 11: Manual reset from Tier 4")
        print("-" * 40)
        manager.reset_to_normal()
        state = manager.get_state()
        allowed, reason = manager.allows_trading()
        print(f"Tier after reset: {state.current_tier.name}")
        print(f"Allowed: {allowed}")
        assert state.current_tier == SafetyTier.NORMAL
        assert allowed == True
        print("PASS")

        # Test 12: Emergency stop
        print("\nTest 12: Emergency stop")
        print("-" * 40)
        manager.emergency_stop()
        state = manager.get_state()
        allowed, reason = manager.allows_trading()
        print(f"Tier: {state.current_tier.name}")
        print(f"Allowed: {allowed}, Reason: {reason}")
        assert state.current_tier == SafetyTier.EMERGENCY
        assert allowed == False
        print("PASS")

        print()
        print("=" * 60)
        print("All tests passed!")
