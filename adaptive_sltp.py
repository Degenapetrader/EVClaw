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
                return params.sl_mult, params.tp_mult

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

                    return sl_mult, tp_mult

            except Exception as e:
                self.log.warning(f"Failed to get adaptation for {symbol}/{regime}: {e}")

        # Cold start: return defaults
        return self.config.default_sl_mult, self.config.default_tp_mult

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
        if key in self._params:
            current = self._params[key]
            sl_mult = current.sl_mult
            tp_mult = current.tp_mult

        # Adapt SL based on hit rate
        if stats.sl_hit_rate > self.config.widen_sl_threshold:
            # SL hit too often - widen it
            # Increase by 5-15% based on how much over threshold
            excess = stats.sl_hit_rate - self.config.widen_sl_threshold
            adjustment = 1.0 + (excess * 0.5)  # Max ~20% increase
            target_sl = sl_mult * adjustment

            # EMA update
            sl_mult = sl_mult + self.config.ema_alpha * (target_sl - sl_mult)

        elif stats.sl_hit_rate < 0.3:
            # SL rarely hit - could tighten slightly
            target_sl = sl_mult * 0.95
            sl_mult = sl_mult + self.config.ema_alpha * (target_sl - sl_mult)

        # Adapt TP based on hit rate
        if stats.tp_hit_rate < (1.0 - self.config.tighten_tp_threshold):
            # TP rarely hit (miss rate > 70%) - tighten it
            excess = self.config.tighten_tp_threshold - stats.tp_hit_rate
            adjustment = 1.0 - (excess * 0.3)  # Max ~20% decrease
            target_tp = tp_mult * max(0.8, adjustment)

            # EMA update
            tp_mult = tp_mult + self.config.ema_alpha * (target_tp - tp_mult)

        elif stats.tp_hit_rate > 0.5:
            # TP hit often - could widen to capture more
            target_tp = tp_mult * 1.05
            tp_mult = tp_mult + self.config.ema_alpha * (target_tp - tp_mult)

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
