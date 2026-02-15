#!/usr/bin/env python3
"""
Captain EVP Dynamic Risk Manager - DEGEN Profile

Dual-layer exit strategy:
  1. ATR-based hard SL/TP on-exchange (safety net — always present)
  2. Signal-decay / brain-driven early exits (can close before SL/TP hit)

WRAPS SafetyManager (not parallel) for tier-based constraints.

Key Features:
- Dynamic 0.5-2.5% risk per trade based on conviction
- ATR-based hard SL/TP as safety net (SL=2×ATR, TP=3×ATR)
- Signal decay exits for early close before hard stops
- Max hold time even for known triggers (24h default)
- Emergency exit threshold for catastrophic moves (10% loss)
- Correlation limits to prevent sector concentration
"""

import threading
from logging_utils import get_logger
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import yaml

from mode_controller import get_param

# =============================================================================
# Risk math constants (hard-coded; not user-configured)
# =============================================================================

RISK_CONVICTION_BASE = 0.55
RISK_CONVICTION_RANGE = 0.45
RISK_LOSS_STREAK_START = 3
RISK_LOSS_STREAK_REDUCTION_PER = 0.2
RISK_LOSS_STREAK_MIN_MULT = 0.5
RISK_WIN_STREAK_START = get_param("risk", "win_streak_start")
RISK_WIN_STREAK_BOOST_PER = 0.05
RISK_WIN_STREAK_MAX_MULT = get_param("risk", "win_streak_max_mult")
RISK_DAILY_PNL_THRESH_1 = 0.04
RISK_DAILY_PNL_THRESH_2 = 0.03
RISK_DAILY_PNL_THRESH_3 = 0.02
RISK_DAILY_PNL_MULT_1 = 0.25
RISK_DAILY_PNL_MULT_2 = 0.5
RISK_DAILY_PNL_MULT_3 = 0.75
RISK_SECTOR_REDUCTION_2 = 0.8
RISK_SECTOR_REDUCTION_3 = 0.7
RISK_FALLBACK_ATR_PCT = 2.0
RISK_MAX_POSITION_PCT = get_param("risk", "max_position_pct")
RISK_POSITION_SL_MULT = 2.0


def _as_float(value: Any, default: float) -> float:
    try:
        return float(value)
    except Exception:
        return float(default)


def _normalize_pct(value: Any, *, default_pct: float) -> float:
    raw = _as_float(value, default_pct)
    if raw <= 0:
        raw = float(default_pct)
    if 0 < raw <= 1.0:
        raw = raw * 100.0
    return max(0.0, min(100.0, raw))


def _ordered_daily_pnl_thresholds() -> Tuple[float, float, float]:
    """Return daily loss thresholds sorted from most severe to least severe."""
    vals = [
        max(0.0, _as_float(RISK_DAILY_PNL_THRESH_1, 0.04)),
        max(0.0, _as_float(RISK_DAILY_PNL_THRESH_2, 0.03)),
        max(0.0, _as_float(RISK_DAILY_PNL_THRESH_3, 0.02)),
    ]
    vals.sort(reverse=True)
    return vals[0], vals[1], vals[2]

# =============================================================================
# Configuration
# =============================================================================

@dataclass
class RiskConfig:
    """Configuration for dynamic risk management.

    Dual-layer exit strategy:
      - ATR-based hard SL/TP on-exchange (safety net)
      - Signal-decay early exit (brain can close before hard stops)
    """

    # Risk per trade (percentage of equity)
    min_risk_pct: float = get_param("risk", "min_risk_pct")   # Minimum risk: 0.5%
    max_risk_pct: float = get_param("risk", "max_risk_pct")   # Maximum risk: 2.5%
    base_risk_pct: float = get_param("risk", "base_risk_pct")  # Normal conditions: 1.0%

    # Equity protection
    equity_floor_pct: float = get_param("risk", "equity_floor_pct")  # Stop trading at 80% of peak
    daily_drawdown_limit_pct: float = get_param("risk", "daily_drawdown_limit_pct")  # Max daily loss

    # Position limits
    max_concurrent_positions: int = get_param("risk", "max_concurrent_positions")
    max_sector_concentration: int = get_param("risk", "max_sector_concentration")  # Max positions in same sector
    max_position_pct: float = get_param("risk", "max_position_pct")  # Max % of equity per position

    # If true, do not require on-exchange hard SL/TP (executor may still place them).
    # Kept for config compatibility with skill.yaml and analysis engine.
    no_hard_stops: bool = get_param("risk", "no_hard_stops")

    # Exit strategy: dual-layer (hard SL/TP + signal decay)
    max_hold_hours: float = 24.0
    min_hold_hours: float = get_param("risk", "min_hold_hours")   # Min hold before decay check

    # Emergency exit (circuit breaker, always active)
    emergency_loss_pct: float = get_param("risk", "emergency_loss_pct")  # Force exit if single position loses 10%
    emergency_portfolio_loss_pct: float = get_param("risk", "emergency_portfolio_loss_pct")  # Force all exits at 15% daily loss

    # Soft stop zone (for monitoring, not auto-exit)
    soft_stop_atr_mult: float = get_param("risk", "soft_stop_atr_mult")
    soft_stop_alert: bool = get_param("risk", "soft_stop_alert")  # Alert when breached

    # Stale signal fallback
    stale_signal_max_minutes: float = get_param("risk", "stale_signal_max_minutes")  # Exit if signals stale > 5 min


# =============================================================================
# Sector Mapping
# =============================================================================

SECTOR_MAP = {
    # Layer 1s
    'BTC': 'L1_MAJOR',
    'ETH': 'L1_MAJOR',
    'SOL': 'L1_ALT',
    'AVAX': 'L1_ALT',
    'NEAR': 'L1_ALT',
    'APT': 'L1_ALT',
    'SUI': 'L1_ALT',
    'SEI': 'L1_ALT',

    # Layer 2s
    'OP': 'L2',
    'ARB': 'L2',
    'MATIC': 'L2',
    'STRK': 'L2',

    # DeFi
    'UNI': 'DEFI',
    'AAVE': 'DEFI',
    'CRV': 'DEFI',
    'MKR': 'DEFI',
    'COMP': 'DEFI',
    'SNX': 'DEFI',
    'DYDX': 'DEFI',

    # Memes
    'DOGE': 'MEME',
    'SHIB': 'MEME',
    'PEPE': 'MEME',
    'FLOKI': 'MEME',
    'BONK': 'MEME',
    'WIF': 'MEME',

    # AI/Infrastructure
    'LINK': 'INFRA',
    'FET': 'AI',
    'RNDR': 'AI',
    'TAO': 'AI',
}

DEFAULT_SECTOR = 'OTHER'
_SKILL_FILE = Path(__file__).with_name("skill.yaml")


def _load_skill_risk_cfg() -> Dict[str, Any]:
    try:
        with _SKILL_FILE.open("r", encoding="utf-8") as f:
            raw = yaml.safe_load(f) or {}
        if not isinstance(raw, dict):
            return {}
        cfg = raw.get("config") or {}
        if not isinstance(cfg, dict):
            return {}
        risk_cfg = cfg.get("risk") or {}
        return risk_cfg if isinstance(risk_cfg, dict) else {}
    except Exception:
        return {}


_SKILL_RISK_CFG = _load_skill_risk_cfg()


def _load_sector_overrides() -> Dict[str, str]:
    """Load optional sector map overrides from skill.yaml config.risk.sector_map."""
    merged: Dict[str, str] = {}

    payload = _SKILL_RISK_CFG.get("sector_map")
    if isinstance(payload, dict):
        for key, value in payload.items():
            k = str(key or "").upper().strip()
            v = str(value or "").upper().strip()
            if k and v:
                merged[k] = v

    return merged


def get_sector(symbol: str) -> str:
    """Get sector for a symbol.

    Notes:
      - HIP3 symbols use the `xyz:` prefix (case-insensitive) and should not be
        bucketed into crypto sectors.
    """
    sym = (symbol or "").strip()

    # HIP3 (stocks/forex/commodities on HL) — treat as its own sector.
    # IMPORTANT: keep this case-insensitive because DB symbols are often stored uppercased.
    if ":" in sym:
        pfx = sym.split(":", 1)[0].strip().lower()
        if pfx == "xyz":
            return "HIP3"

    # Remove common crypto suffixes
    base = sym.upper()
    if base.endswith("-PERP"):
        base = base[:-5]
    if base.endswith("USDT"):
        base = base[:-4]
    elif base.endswith("USD"):
        base = base[:-3]
    sector_map = SECTOR_MAP
    overrides = _load_sector_overrides()
    if overrides:
        sector_map = dict(SECTOR_MAP)
        sector_map.update(overrides)
    return sector_map.get(base, DEFAULT_SECTOR)


# =============================================================================
# Tracked Position for Risk Monitoring
# =============================================================================

@dataclass
class TrackedRisk:
    """Risk tracking for an open position."""

    symbol: str
    direction: str
    entry_price: float
    entry_time: float  # Unix timestamp
    size: float
    notional_usd: float
    risk_pct: float  # Risk allocated (0.5-2.5%)

    # Signal info for decay
    trigger_signal: str
    entry_signals: Dict[str, str] = field(default_factory=dict)  # signal -> direction

    # Tracking
    current_price: float = 0.0
    unrealized_pnl: float = 0.0
    unrealized_pnl_pct: float = 0.0
    max_favorable: float = 0.0  # Best unrealized P&L seen
    max_adverse: float = 0.0    # Worst unrealized P&L seen

    # Alerts triggered
    soft_stop_breached: bool = False
    soft_stop_breach_time: Optional[float] = None

    def update_pnl(self, current_price: float):
        """Update P&L based on current price."""
        self.current_price = current_price

        if self.direction == 'LONG':
            self.unrealized_pnl = (current_price - self.entry_price) * self.size
        else:
            self.unrealized_pnl = (self.entry_price - current_price) * self.size

        if self.notional_usd > 0:
            self.unrealized_pnl_pct = (self.unrealized_pnl / self.notional_usd) * 100

        self.max_favorable = max(self.max_favorable, self.unrealized_pnl)
        self.max_adverse = min(self.max_adverse, self.unrealized_pnl)

    @property
    def hold_hours(self) -> float:
        """How long position has been held."""
        return (time.time() - self.entry_time) / 3600

    @property
    def sector(self) -> str:
        """Get sector for this symbol."""
        return get_sector(self.symbol)


# =============================================================================
# Dynamic Risk Manager
# =============================================================================

class DynamicRiskManager:
    """
    Risk management with dual-layer exits - DEGEN style.

    Layer 1: ATR-based hard SL/TP on-exchange (safety net, always present)
    Layer 2: Signal decay / brain-driven early exits (can close before SL/TP)

    This WRAPS SafetyManager - respects tier constraints while adding:
    - Dynamic position sizing based on conviction
    - Signal decay early exits (before hard SL/TP)
    - Correlation/sector limits
    - Emergency exits for catastrophic moves
    """

    def __init__(
        self,
        config: Optional[RiskConfig] = None,
        equity: float = 10000.0,
        safety_manager: Optional[Any] = None,  # SafetyManager
        adaptive_sizing: Optional[Any] = None,  # AdaptiveSizingManager for learned risk adjustments
    ):
        self.config = config or RiskConfig()
        self.equity = equity
        self.peak_equity = equity
        self.safety = safety_manager
        self.adaptive_sizing = adaptive_sizing  # For adaptive risk adjustments
        self.log = get_logger("risk_mgr")
        self._max_position_pct = _normalize_pct(self.config.max_position_pct, default_pct=25.0)

        # Tracked positions
        self.positions: Dict[str, TrackedRisk] = {}
        self._positions_lock = threading.Lock()

        # Daily tracking
        self.daily_pnl = 0.0
        self.daily_reset_date: Optional[str] = None
        self._check_daily_reset()

        # Streak tracking for adjustments
        self.win_streak = 0
        self.loss_streak = 0

    def _check_daily_reset(self):
        """Reset daily counters if new UTC day."""
        today = datetime.now(timezone.utc).strftime('%Y-%m-%d')
        if self.daily_reset_date != today:
            self.daily_pnl = 0.0
            self.daily_reset_date = today
            self.log.info(f"Daily reset for {today}")

    # =========================================================================
    # Risk Budget Calculation
    # =========================================================================

    def calculate_risk_budget(
        self,
        conviction: float,
        symbol: Optional[str] = None,
        direction: Optional[str] = None,
        context_snapshot: Optional[Dict] = None,
        learning_engine: Optional[Any] = None,
        return_breakdown: bool = False,
    ) -> Any:
        """
        Calculate risk percentage for a trade based on conviction.

        Args:
            conviction: 0.0 - 1.0 conviction score from TradingBrain
            symbol: Trading symbol (optional, for adaptive adjustments)
            direction: Trade direction LONG/SHORT (for context adjustment)
            context_snapshot: Context data (for context learning adjustment)
            learning_engine: LearningEngine instance (for context adjustment)
            return_breakdown: If True, returns (risk, breakdown_dict). If False, returns just risk.

        Returns:
            If return_breakdown=True: Tuple of (risk as percentage of equity, adjustment_breakdown dict)
            If return_breakdown=False: Just risk as percentage of equity (backward compatible)
        """
        self._check_daily_reset()
        
        adjustment_breakdown = {}

        # Start with base risk
        risk = self.config.base_risk_pct

        # Scale by conviction (higher conviction = more risk)
        # conviction base -> 0.5%, conviction 1.0 -> 2.5%
        conviction_scale = (conviction - RISK_CONVICTION_BASE) / RISK_CONVICTION_RANGE  # 0 to 1
        conviction_scale = max(0, min(1, conviction_scale))
        risk = self.config.min_risk_pct + (self.config.max_risk_pct - self.config.min_risk_pct) * conviction_scale
        adjustment_breakdown['base_risk'] = risk

        # Adjust for streaks
        risk = self._adjust_for_streak(risk)
        adjustment_breakdown['after_streak'] = risk

        # Adjust for volatility/market conditions
        risk = self._adjust_for_conditions(risk)
        adjustment_breakdown['after_conditions'] = risk

        # Adjust for correlation with existing positions
        risk = self._adjust_for_correlation(risk)
        adjustment_breakdown['after_correlation'] = risk

        # Apply adaptive sizing adjustment (learned from outcomes)
        if self.adaptive_sizing and symbol:
            try:
                risk_mult = self.adaptive_sizing.get_risk_adjustment(symbol, conviction)
                risk *= risk_mult
                self.log.debug(f"Adaptive sizing for {symbol}: mult={risk_mult:.3f}")
                adjustment_breakdown['adaptive_sizing_mult'] = risk_mult
            except Exception as e:
                self.log.warning(f"Adaptive sizing query failed for {symbol}: {e}")
                # Continue with unadjusted risk

        # Apply context learning adjustment.
        if learning_engine and context_snapshot and direction:
            try:
                context_engine = getattr(learning_engine, "_context_learning", None)
                if context_engine is None:
                    context_engine = getattr(learning_engine, "context_learning", None)
                if context_engine is None or not hasattr(context_engine, "get_context_adjustment"):
                    raise RuntimeError("context learning engine unavailable")
                ctx_mult, ctx_breakdown = context_engine.get_context_adjustment(context_snapshot, direction)
                risk *= ctx_mult
                self.log.debug(f"Context adjustment for {symbol}: mult={ctx_mult:.3f}")
                adjustment_breakdown['context_mult'] = ctx_mult
                adjustment_breakdown['context_breakdown'] = ctx_breakdown
            except Exception as e:
                self.log.warning(f"Context adjustment failed for {symbol}: {e}")

        # Respect safety tier if SafetyManager available.
        # Safety caps must be able to push risk below min_risk_pct (including zero),
        # so we apply the normal min clamp before safety and relax the floor after.
        risk = max(self.config.min_risk_pct, min(self.config.max_risk_pct, risk))
        adjustment_breakdown['pre_safety_clamped_risk'] = risk

        if self.safety:
            tier_cap = self.safety.get_size_cap()
            try:
                tier_cap = float(tier_cap)
            except Exception:
                tier_cap = 1.0
            tier_cap = max(0.0, min(1.0, tier_cap))
            risk *= tier_cap
            adjustment_breakdown['safety_tier_cap'] = tier_cap

        # Final clamp
        floor = 0.0 if self.safety else self.config.min_risk_pct
        risk = max(floor, min(self.config.max_risk_pct, risk))
        adjustment_breakdown['final_risk'] = risk

        if return_breakdown:
            return risk, adjustment_breakdown
        return risk

    def _adjust_for_streak(self, risk: float) -> float:
        """Adjust risk based on win/loss streak."""
        if self.loss_streak >= RISK_LOSS_STREAK_START:
            # Reduce risk after consecutive losses
            reduction = RISK_LOSS_STREAK_REDUCTION_PER * (self.loss_streak - (RISK_LOSS_STREAK_START - 1))
            risk *= max(RISK_LOSS_STREAK_MIN_MULT, 1 - reduction)
        elif self.win_streak >= RISK_WIN_STREAK_START:
            # Slight increase after wins (capped)
            boost = RISK_WIN_STREAK_BOOST_PER * (self.win_streak - (RISK_WIN_STREAK_START - 1))
            risk *= min(RISK_WIN_STREAK_MAX_MULT, 1 + boost)
        return risk

    def _adjust_for_conditions(self, risk: float) -> float:
        """Adjust risk based on market conditions."""
        # If we've already lost significantly today, reduce risk.
        # NOTE: Order matters: check the most negative threshold first.
        thresh_1, thresh_2, thresh_3 = _ordered_daily_pnl_thresholds()
        if self.daily_pnl < -self.equity * thresh_1:
            risk *= RISK_DAILY_PNL_MULT_1
        elif self.daily_pnl < -self.equity * thresh_2:
            risk *= RISK_DAILY_PNL_MULT_2
        elif self.daily_pnl < -self.equity * thresh_3:
            risk *= RISK_DAILY_PNL_MULT_3

        return risk

    def _adjust_for_correlation(self, risk: float) -> float:
        """Reduce risk if correlated positions exist."""
        with self._positions_lock:
            if not self.positions:
                return risk
            positions = list(self.positions.values())

        # Count positions by sector
        sector_counts: Dict[str, int] = {}
        for pos in positions:
            sector = pos.sector
            sector_counts[sector] = sector_counts.get(sector, 0) + 1

        # If any sector has 2+ positions, reduce risk
        max_sector_count = max(sector_counts.values()) if sector_counts else 0
        if max_sector_count >= 3:
            risk *= RISK_SECTOR_REDUCTION_3
        elif max_sector_count >= 2:
            risk *= RISK_SECTOR_REDUCTION_2

        return risk

    # =========================================================================
    # Position Sizing
    # =========================================================================

    def position_size_usd(
        self,
        risk_pct: float,
        atr_pct: float,
        sl_mult: float = RISK_POSITION_SL_MULT
    ) -> float:
        """
        Calculate position size in USD based on risk budget.

        Even with NO hard stops, we size as if we had an ATR-based stop
        to keep risk consistent.

        Args:
            risk_pct: Risk as % of equity (from calculate_risk_budget)
            atr_pct: ATR as % of price
            sl_mult: Multiplier for implied stop distance (default 2.0)

        Returns:
            Position size in USD
        """
        if atr_pct <= 0:
            atr_pct = RISK_FALLBACK_ATR_PCT  # Fallback: assume ATR

        # Risk amount in USD
        risk_usd = self.equity * (risk_pct / 100)

        # Implied stop distance
        stop_distance_pct = atr_pct * sl_mult

        # Position size = risk / stop_distance
        if stop_distance_pct <= 0:
            return 0.0

        size_usd = risk_usd / (stop_distance_pct / 100)

        # Cap to prevent oversizing (accepts both 0.25 and 25 for 25%).
        max_position = self.equity * (self._max_position_pct / 100.0)
        size_usd = min(size_usd, max_position)

        return size_usd

# =============================================================================
# Module exports
# =============================================================================

__all__ = [
    'RiskConfig',
    'TrackedRisk',
    'DynamicRiskManager',
    'get_sector',
    'SECTOR_MAP',
]
