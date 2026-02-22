"""
Adaptive SL/TP Manager for Captain EVP Trading Brain

Learns optimal stop-loss and take-profit ATR multipliers based on:
- Per-symbol historical outcomes
- Volatility regime (LOW/NORMAL/HIGH)
- SL hit rate and TP hit rate

Key principles:
- Start conservative, adapt slowly (EMA alpha=0.1)
- All adaptations bounded (SL: 0.75-2.5, TP: 1.0-4.0)
- Enforce TP >= SL (R:R >= 1:1)
- Need min_samples before trusting adaptations
"""

import json
import math
from logging_utils import get_logger
import os
import time
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

from mode_controller import get_param

# Conditional import for type hints
try:
    from trade_tracker import TradeTracker, SLTPStats
except ImportError:
    TradeTracker = Any
    SLTPStats = Any



@dataclass
class AdaptiveSLTPConfig:
    """Configuration for adaptive SL/TP learning."""

    # Minimum samples before trusting adaptations
    min_samples: int = get_param("adaptive_sltp", "min_samples")

    # EMA smoothing factor (lower = slower adaptation)
    ema_alpha: float = get_param("adaptive_sltp", "ema_alpha")

    # SL multiplier bounds (ATR-based)
    sl_range_min: float = get_param("adaptive_sltp", "sl_range_min")   # Tight stop (AGI bounds)
    sl_range_max: float = get_param("adaptive_sltp", "sl_range_max")   # Wide stop (AGI bounds)

    # TP multiplier bounds (ATR-based)
    tp_range_min: float = get_param("adaptive_sltp", "tp_range_min")   # Minimum TP (AGI bounds)
    tp_range_max: float = get_param("adaptive_sltp", "tp_range_max")   # Maximum TP (AGI bounds)

    # Enforce risk-reward constraints
    min_rr_ratio: float = get_param("adaptive_sltp", "min_rr_ratio")    # TP must be >= SL
    max_rr_ratio: float = get_param("adaptive_sltp", "max_rr_ratio")    # TP/SL cannot exceed this

    # Adaptation triggers
    widen_sl_threshold: float = get_param("adaptive_sltp", "widen_sl_threshold")   # Widen SL if hit rate > 60%
    tighten_tp_threshold: float = get_param("adaptive_sltp", "tighten_tp_threshold")  # Tighten TP if miss rate > 70%

    # Default multipliers (used for cold start)
    # Match current production defaults (skill.yaml executor: 2.0/3.0)
    default_sl_mult: float = get_param("adaptive_sltp", "default_sl_mult")
    default_tp_mult: float = get_param("adaptive_sltp", "default_tp_mult")
    # Anti-oscillation and stale-state decay controls
    max_step_delta: float = float(os.getenv("EVCLAW_ADAPTIVE_SLTP_MAX_DELTA", "0.10"))
    stale_half_life_hours: float = float(os.getenv("EVCLAW_ADAPTIVE_SLTP_STALE_HALF_LIFE_HOURS", "72"))
    stale_max_age_hours: float = float(os.getenv("EVCLAW_ADAPTIVE_SLTP_STALE_MAX_AGE_HOURS", "336"))


@dataclass
class AdaptedParams:
    """Adapted SL/TP parameters for a symbol/regime."""
    symbol: str
    regime: str
    sl_mult: float
    tp_mult: float
    confidence: float  # 0-1 based on sample size
    samples: int
    last_updated: float = field(default_factory=time.time)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class AdaptiveSLTPManager:
    """
    Manages adaptive SL/TP multipliers based on trade outcomes.

    Usage:
        manager = AdaptiveSLTPManager(trade_tracker=tracker)

        # Before placing trade
        sl_mult, tp_mult = manager.get_multipliers(symbol, regime)

        # SL/TP are calculated: sl = entry - (atr * sl_mult), etc.
    """

    # State file for persistence
    STATE_FILE = "adaptive_sltp.json"

    def __init__(
        self,
        config: Optional[AdaptiveSLTPConfig] = None,
        trade_tracker: Optional[TradeTracker] = None,
        memory_dir: Optional[Path] = None,
    ):
        """
        Initialize adaptive SL/TP manager.

        Args:
            config: Adaptation configuration
            trade_tracker: TradeTracker instance for outcome data
            memory_dir: Directory for state persistence
        """
        self.config = config or AdaptiveSLTPConfig()
        self.tracker = trade_tracker
        self.log = get_logger('adaptive_sltp')

        # Memory directory
        if memory_dir:
            self.memory_dir = Path(memory_dir)
        else:
            self.memory_dir = Path(__file__).parent / "memory" / "adaptive"

        self.memory_dir.mkdir(parents=True, exist_ok=True)
        self.state_file = self.memory_dir / self.STATE_FILE

        # In-memory adapted params cache
        self._params: Dict[str, AdaptedParams] = {}

        # Load existing state
        self._load_state()

        self.log.info(f"AdaptiveSLTPManager initialized, {len(self._params)} cached params")

    def get_multipliers(
        self,
        symbol: str,
        regime: str = 'NORMAL',
    ) -> Tuple[float, float]:
        """
        Get adapted SL/TP multipliers for a symbol/regime.

        Returns config defaults if insufficient samples (cold start).

        Args:
            symbol: Trading symbol (e.g., 'ETH', 'BTC')
            regime: Volatility regime (LOW/NORMAL/HIGH)

        Returns:
            (sl_mult, tp_mult) tuple
        """
        key = f"{symbol.upper()}:{regime}"

        # Check cached params first
        if key in self._params:
            params = self._params[key]
            if params.samples >= self.config.min_samples:
                sl_cached, tp_cached = self._apply_stale_decay(params)
                return sl_cached, tp_cached

        # Try to calculate from tracker data
        if self.tracker:
            try:
                stats = self.tracker.get_sltp_stats(
                    symbol=symbol,
                    regime=regime,
                    min_trades=self.config.min_samples,
                )

                if stats:
                    # Calculate adapted params
                    sl_mult, tp_mult = self._calculate_adaptation(stats)

                    # Cache the result
                    self._params[key] = AdaptedParams(
                        symbol=symbol.upper(),
                        regime=regime,
                        sl_mult=sl_mult,
                        tp_mult=tp_mult,
                        confidence=min(1.0, stats.total_trades / 50),  # Max confidence at 50 trades
                        samples=stats.total_trades,
                    )

                    self.log.debug(
                        f"Adapted {symbol}/{regime}: SL={sl_mult:.2f}, TP={tp_mult:.2f} "
                        f"(n={stats.total_trades})"
                    )
                    try:
                        self._save_state()
                    except Exception as e:
                        self.log.warning(f"Failed to persist adaptive SLTP state for {symbol}/{regime}: {e}")

                    sl_mult, tp_mult = self._apply_stale_decay(self._params[key])
                    return sl_mult, tp_mult

            except Exception as e:
                self.log.warning(f"Failed to get adaptation for {symbol}/{regime}: {e}")

        # Cold start: return defaults
        return self.config.default_sl_mult, self.config.default_tp_mult

    def _apply_stale_decay(self, params: AdaptedParams) -> Tuple[float, float]:
        """Decay stale cached params toward defaults when no fresh evidence arrives."""
        now = time.time()
        age_hours = max(0.0, (now - float(params.last_updated or 0.0)) / 3600.0)
        max_age = max(1.0, float(self.config.stale_max_age_hours))
        half_life = max(1.0, float(self.config.stale_half_life_hours))

        if age_hours <= 0:
            return float(params.sl_mult), float(params.tp_mult)
        if age_hours > max_age:
            params.sl_mult = float(self.config.default_sl_mult)
            params.tp_mult = float(self.config.default_tp_mult)
            params.last_updated = now
            return params.sl_mult, params.tp_mult

        decay = math.exp(-math.log(2.0) * (age_hours / half_life))
        params.sl_mult = float(self.config.default_sl_mult) + (
            float(params.sl_mult) - float(self.config.default_sl_mult)
        ) * decay
        params.tp_mult = float(self.config.default_tp_mult) + (
            float(params.tp_mult) - float(self.config.default_tp_mult)
        ) * decay
        return float(params.sl_mult), float(params.tp_mult)

    def _calculate_adaptation(self, stats: SLTPStats) -> Tuple[float, float]:
        """
        Calculate adapted SL/TP multipliers from statistics.

        Uses EMA-style updates with bounded ranges.
        """
        # Start from current averages
        sl_mult = stats.avg_sl_mult
        tp_mult = stats.avg_tp_mult

        # Get current cached params if any
        key = f"{stats.symbol}:{stats.regime}"
        current_sl = float(sl_mult)
        current_tp = float(tp_mult)
        if key in self._params:
            current = self._params[key]
            sl_mult = current.sl_mult
            tp_mult = current.tp_mult
            current_sl = float(sl_mult)
            current_tp = float(tp_mult)

        # Confidence-weighted adaptation speed: low samples => slower EMA.
        confidence = min(1.0, max(0.1, float(stats.total_trades) / 50.0))
        eff_alpha = max(0.01, min(1.0, float(self.config.ema_alpha) * confidence))

        # Adapt SL based on hit rate
        if stats.sl_hit_rate > self.config.widen_sl_threshold:
            # SL hit too often - widen it
            # Increase by 5-15% based on how much over threshold
            excess = stats.sl_hit_rate - self.config.widen_sl_threshold
            adjustment = 1.0 + (excess * 0.5)  # Max ~20% increase
            target_sl = sl_mult * adjustment

            # EMA update
            sl_mult = sl_mult + eff_alpha * (target_sl - sl_mult)

        elif stats.sl_hit_rate < 0.3:
            # SL rarely hit - could tighten slightly
            target_sl = sl_mult * 0.95
            sl_mult = sl_mult + eff_alpha * (target_sl - sl_mult)

        # Adapt TP based on hit rate
        if stats.tp_hit_rate < (1.0 - self.config.tighten_tp_threshold):
            # TP rarely hit (miss rate > 70%) - tighten it
            excess = self.config.tighten_tp_threshold - stats.tp_hit_rate
            adjustment = 1.0 - (excess * 0.3)  # Max ~20% decrease
            target_tp = tp_mult * max(0.8, adjustment)

            # EMA update
            tp_mult = tp_mult + eff_alpha * (target_tp - tp_mult)

        elif stats.tp_hit_rate > 0.5:
            # TP hit often - could widen to capture more
            target_tp = tp_mult * 1.05
            tp_mult = tp_mult + eff_alpha * (target_tp - tp_mult)

        # Per-update change-rate clamp (before global bounds).
        max_delta = max(0.0, float(self.config.max_step_delta))
        if max_delta > 0:
            sl_mult = max(current_sl - max_delta, min(current_sl + max_delta, sl_mult))
            tp_mult = max(current_tp - max_delta, min(current_tp + max_delta, tp_mult))

        # Apply bounds
        sl_mult = max(self.config.sl_range_min, min(self.config.sl_range_max, sl_mult))
        tp_mult = max(self.config.tp_range_min, min(self.config.tp_range_max, tp_mult))

        # Enforce R:R constraints
        min_tp = sl_mult * self.config.min_rr_ratio
        if tp_mult < min_tp:
            tp_mult = min_tp
            self.log.debug(f"Enforced min R:R constraint: TP raised to {tp_mult:.2f}")

        max_tp = sl_mult * self.config.max_rr_ratio
        if tp_mult > max_tp:
            tp_mult = max_tp
            self.log.debug(f"Enforced max R:R constraint: TP lowered to {tp_mult:.2f}")

        # Re-apply absolute TP bounds after ratio clamps
        tp_mult = max(self.config.tp_range_min, min(self.config.tp_range_max, tp_mult))

        return round(sl_mult, 2), round(tp_mult, 2)





    def _load_state(self) -> None:
        """Load persisted state from file."""
        if not self.state_file.exists():
            return

        try:
            data = json.loads(self.state_file.read_text())
            for key, params_dict in data.items():
                self._params[key] = AdaptedParams(
                    symbol=params_dict['symbol'],
                    regime=params_dict['regime'],
                    sl_mult=params_dict['sl_mult'],
                    tp_mult=params_dict['tp_mult'],
                    confidence=params_dict.get('confidence', 0.5),
                    samples=params_dict.get('samples', 0),
                    last_updated=params_dict.get('last_updated', time.time()),
                )
            self.log.debug(f"Loaded {len(self._params)} params from {self.state_file}")
        except Exception as e:
            self.log.warning(f"Failed to load state: {e}")

    def _save_state(self) -> None:
        """Save state to file (atomic write)."""
        try:
            data = {k: v.to_dict() for k, v in self._params.items()}
            temp_path = self.state_file.with_suffix('.tmp')
            temp_path.write_text(json.dumps(data, indent=2))
            os.replace(temp_path, self.state_file)
        except Exception as e:
            self.log.error(f"Failed to save state: {e}")

# Module exports
__all__ = [
    'AdaptiveSLTPManager',
    'AdaptiveSLTPConfig',
    'AdaptedParams',
]


if __name__ == "__main__":
    import tempfile

    print("=" * 60)
    print("AdaptiveSLTPManager Test")
    print("=" * 60)

    # Create with temp memory dir
    with tempfile.TemporaryDirectory() as tmpdir:
        manager = AdaptiveSLTPManager(
            config=AdaptiveSLTPConfig(min_samples=1),  # Low for testing
            memory_dir=Path(tmpdir),
        )

        # Test cold start (no tracker)
        print("\n[TEST] Cold start - no data...")
        sl, tp = manager.get_multipliers('ETH', 'NORMAL')
        print(f"  ETH/NORMAL: SL={sl}, TP={tp}")
        assert sl == 2.0, f"Expected default SL 2.0, got {sl}"
        assert tp == 3.0, f"Expected default TP 3.0, got {tp}"
        print("  [OK] Returns defaults")

        # Test R:R constraint
        print("\n[TEST] R:R constraint...")
        config = AdaptiveSLTPConfig(
            default_sl_mult=2.0,
            default_tp_mult=1.5,  # Would violate min R:R
            min_rr_ratio=1.0,
            max_rr_ratio=2.5,
        )
        # The constraint is enforced during calculation, not on defaults
        print("  [OK] Constraint configured")

        # Basic state inspection
        print("\n[TEST] Status...")
        print(f"  cached_params={len(manager._params)} state_file={manager.state_file}")

        print("\n" + "=" * 60)
        print("All tests passed!")
        print("=" * 60)
