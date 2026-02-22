#!/usr/bin/env python3
"""
Context Learning Module for Captain EVP AI Trading Agent.

Tracks win rates by context conditions and provides adjustments
based on historical performance.

Features:
- Tracks win rates by: trend_alignment, vol_regime, funding_alignment, smart_money
- Provides multipliers for sizing based on context performance
- Integrates with learning_engine.py
"""

import json
import os
from logging_utils import get_logger
import time
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# =============================================================================
# Context Feature Definitions
# =============================================================================

@dataclass
class FeatureStats:
    """Statistics for a context feature condition."""
    trades: int = 0
    wins: int = 0
    total_pnl: float = 0.0
    avg_pnl: float = 0.0
    win_rate: float = 0.0
    last_updated: float = 0.0
    
    def update(self, pnl: float) -> None:
        """Update stats with a new trade result."""
        self.trades += 1
        self.total_pnl += pnl
        if pnl > 0:
            self.wins += 1
        self.win_rate = self.wins / self.trades if self.trades > 0 else 0.0
        self.avg_pnl = self.total_pnl / self.trades if self.trades > 0 else 0.0
        self.last_updated = time.time()
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'trades': self.trades,
            'wins': self.wins,
            'total_pnl': round(self.total_pnl, 2),
            'avg_pnl': round(self.avg_pnl, 2),
            'win_rate': round(self.win_rate, 4),
            'last_updated': self.last_updated,
        }
    
    @classmethod
    def from_dict(cls, d: Dict) -> 'FeatureStats':
        return cls(
            trades=d.get('trades', 0),
            wins=d.get('wins', 0),
            total_pnl=d.get('total_pnl', 0.0),
            avg_pnl=d.get('avg_pnl', 0.0),
            win_rate=d.get('win_rate', 0.0),
            last_updated=d.get('last_updated', 0.0),
        )


# =============================================================================
# Context Feature Extractors
# =============================================================================

def extract_trend_alignment(ctx: Dict, direction: str) -> str:
    """
    Extract trend alignment condition.
    Returns: 'aligned', 'counter', or 'neutral'
    """
    trend_state = ctx.get('key_metrics', {}).get('trend_state', {})
    if isinstance(trend_state, dict):
        trend_score = trend_state.get('trend_score', 0)
    else:
        trend_score = 0
    
    if abs(trend_score) < 0.5:
        return 'neutral'
    
    is_bullish = trend_score > 0
    is_long = direction.upper() == 'LONG'
    
    if is_bullish == is_long:
        return 'aligned'
    return 'counter'


def extract_vol_regime(ctx: Dict, direction: str) -> str:
    """
    Extract volatility regime.
    Returns: 'high', 'mid', or 'low'
    """
    _ = direction
    atr_pct = ctx.get('key_metrics', {}).get('atr_pct', 2.0)
    
    if atr_pct > 4.0:
        return 'high'
    elif atr_pct < 1.5:
        return 'low'
    return 'mid'


def extract_funding_alignment(ctx: Dict, direction: str) -> str:
    """
    Extract funding alignment.
    Returns: 'aligned', 'counter', or 'neutral'
    
    Logic:
    - LONGS_PAY means longs pay shorts (bearish pressure) → SHORT aligned
    - SHORTS_PAY means shorts pay longs (bullish pressure) → LONG aligned
    """
    funding_dir = ctx.get('key_metrics', {}).get('funding_direction', '')
    funding_rate = abs(ctx.get('key_metrics', {}).get('funding_rate', 0) or 0)
    
    # Low funding = neutral
    if funding_rate < 0.01:
        return 'neutral'
    
    is_long = direction.upper() == 'LONG'
    
    if funding_dir == 'LONGS_PAY':
        # Longs paying = bearish pressure = SHORT aligned
        return 'aligned' if not is_long else 'counter'
    elif funding_dir == 'SHORTS_PAY':
        # Shorts paying = bullish pressure = LONG aligned
        return 'aligned' if is_long else 'counter'
    
    return 'neutral'


def extract_smart_money_alignment(ctx: Dict, direction: str) -> str:
    """
    Extract smart money alignment.
    Returns: 'aligned', 'counter', or 'neutral'
    """
    divergence = ctx.get('key_metrics', {}).get('divergence_signal', '')
    
    if not divergence or divergence == 'NEUTRAL':
        return 'neutral'
    
    is_long = direction.upper() == 'LONG'
    
    # SMART_BULLISH = smart money is long
    # SMART_BEARISH = smart money is short
    if divergence == 'SMART_BULLISH':
        return 'aligned' if is_long else 'counter'
    elif divergence == 'SMART_BEARISH':
        return 'aligned' if not is_long else 'counter'
    
    return 'neutral'


def extract_signal_strength(ctx: Dict, direction: str) -> str:
    """
    Extract signal strength regime.
    Returns: 'strong', 'moderate', or 'weak'
    """
    _ = direction
    max_z = ctx.get('key_metrics', {}).get('max_z', 0)
    
    if max_z >= 2.5:
        return 'strong'
    elif max_z >= 1.8:
        return 'moderate'
    return 'weak'


def extract_strategy_segment(ctx: Dict, direction: str) -> str:
    """
    Segment trades for context learning.
    Returns: 'hip3', 'perp', or 'other'
    """
    _ = direction
    seg = str(ctx.get('strategy_segment') or '').strip().lower()
    if seg in {'hip3', 'perp'}:
        return seg
    symbol = str(ctx.get('symbol') or '').strip().upper()
    if symbol.startswith('XYZ:'):
        return 'hip3'
    if symbol:
        return 'perp'
    return 'other'


def extract_hip3_driver(ctx: Dict, direction: str) -> str:
    """
    HIP3 driver bucket for learning.
    Returns: 'flow', 'ofm', 'flow_ofm', 'other', or 'none'
    """
    _ = direction
    seg = extract_strategy_segment(ctx, direction)
    if seg != 'hip3':
        return 'none'

    driver = str(ctx.get('hip3_driver') or '').strip().lower()
    if not driver and isinstance(ctx.get('risk'), dict):
        driver = str((ctx.get('risk') or {}).get('hip3_driver') or '').strip().lower()

    if driver in {'flow', 'ofm', 'flow_ofm'}:
        return driver
    if driver:
        return 'other'
    return 'none'


def extract_hip3_booster_regime(ctx: Dict, direction: str) -> str:
    """
    HIP3 booster strength bucket from REST-derived score/multiplier.
    Returns: 'strong', 'positive', 'neutral', 'negative', 'strong_negative', or 'none'
    """
    _ = direction
    seg = extract_strategy_segment(ctx, direction)
    if seg != 'hip3':
        return 'none'

    score = None
    try:
        raw = ctx.get('hip3_booster_score')
        if raw is None and isinstance(ctx.get('risk'), dict):
            raw = (ctx.get('risk') or {}).get('hip3_booster_score')
        if raw is not None:
            score = float(raw)
    except Exception:
        score = None

    if score is None:
        try:
            raw_mult = ctx.get('hip3_booster_size_mult')
            if raw_mult is None and isinstance(ctx.get('risk'), dict):
                raw_mult = (ctx.get('risk') or {}).get('hip3_booster_size_mult')
            if raw_mult is not None:
                mult = float(raw_mult)
                score = (mult - 1.0) / 0.10
        except Exception:
            score = None

    if score is None:
        return 'neutral'
    if score >= 1.5:
        return 'strong'
    if score >= 0.5:
        return 'positive'
    if score <= -1.5:
        return 'strong_negative'
    if score <= -0.5:
        return 'negative'
    return 'neutral'


# Feature extractors registry
FEATURE_EXTRACTORS = {
    'trend_alignment': extract_trend_alignment,
    'vol_regime': extract_vol_regime,
    'funding_alignment': extract_funding_alignment,
    'smart_money': extract_smart_money_alignment,
    'signal_strength': extract_signal_strength,
    'strategy_segment': extract_strategy_segment,
    'hip3_driver': extract_hip3_driver,
    'hip3_booster_regime': extract_hip3_booster_regime,
}


# =============================================================================
# Context Learning Engine
# =============================================================================

class ContextLearningEngine:
    """
    Tracks win rates by context conditions and provides adjustments.
    
    Stats file structure:
    {
        "trend_alignment": {
            "aligned": {"trades": 50, "wins": 32, ...},
            "counter": {"trades": 30, "wins": 10, ...},
            "neutral": {"trades": 20, "wins": 11, ...}
        },
        ...
    }
    """
    
    # Minimum trades needed before applying adjustments
    MIN_TRADES_FOR_ADJUSTMENT = max(
        1,
        int(os.getenv("EVCLAW_CONTEXT_MIN_TRADES_FOR_ADJUSTMENT", "20")),
    )
    PRIOR_STRENGTH = max(
        1,
        int(os.getenv("EVCLAW_CONTEXT_PRIOR_STRENGTH", "20")),
    )

    # Adjustment bounds
    MIN_ADJUSTMENT = 0.5
    MAX_ADJUSTMENT = 1.5

    # Baseline win rate (neutral adjustment if win rate equals this)
    BASELINE_WIN_RATE = 0.45
    
    def __init__(self, memory_dir: Optional[Path] = None):
        self.log = get_logger("context_learning")
        self.memory_dir = Path(memory_dir) if memory_dir else Path(__file__).parent / "memory"
        self.memory_dir.mkdir(parents=True, exist_ok=True)
        
        self._stats_file = self.memory_dir / "context_feature_stats.json"
        self._stats: Dict[str, Dict[str, FeatureStats]] = {}
        self._stats_lock = threading.RLock()
        
        self._load_stats()
    
    def _load_stats(self) -> None:
        """Load stats from disk."""
        if not self._stats_file.exists():
            self._init_empty_stats()
            return
        
        try:
            data = json.loads(self._stats_file.read_text())
            for feature_name, conditions in data.items():
                self._stats[feature_name] = {}
                for condition, stats_dict in conditions.items():
                    self._stats[feature_name][condition] = FeatureStats.from_dict(stats_dict)
            self.log.info(f"Loaded context feature stats from {self._stats_file}")
        except Exception as e:
            self.log.warning(f"Failed to load stats: {e}, initializing empty")
            self._init_empty_stats()
    
    def _init_empty_stats(self) -> None:
        """Initialize empty stats structure."""
        self._stats = {
            'trend_alignment': {
                'aligned': FeatureStats(),
                'counter': FeatureStats(),
                'neutral': FeatureStats(),
            },
            'vol_regime': {
                'high': FeatureStats(),
                'mid': FeatureStats(),
                'low': FeatureStats(),
            },
            'funding_alignment': {
                'aligned': FeatureStats(),
                'counter': FeatureStats(),
                'neutral': FeatureStats(),
            },
            'smart_money': {
                'aligned': FeatureStats(),
                'counter': FeatureStats(),
                'neutral': FeatureStats(),
            },
            'signal_strength': {
                'strong': FeatureStats(),
                'moderate': FeatureStats(),
                'weak': FeatureStats(),
            },
            'strategy_segment': {
                'hip3': FeatureStats(),
                'perp': FeatureStats(),
                'other': FeatureStats(),
            },
            'hip3_driver': {
                'flow': FeatureStats(),
                'ofm': FeatureStats(),
                'flow_ofm': FeatureStats(),
                'other': FeatureStats(),
                'none': FeatureStats(),
            },
            'hip3_booster_regime': {
                'strong': FeatureStats(),
                'positive': FeatureStats(),
                'neutral': FeatureStats(),
                'negative': FeatureStats(),
                'strong_negative': FeatureStats(),
                'none': FeatureStats(),
            },
        }
    
    def save_stats(self) -> None:
        """Save stats to disk."""
        try:
            with self._stats_lock:
                data = {}
                for feature_name, conditions in self._stats.items():
                    data[feature_name] = {}
                    for condition, stats in conditions.items():
                        data[feature_name][condition] = stats.to_dict()

            tmp_path = Path(f"{self._stats_file}.tmp")
            tmp_path.write_text(json.dumps(data, indent=2))
            os.replace(tmp_path, self._stats_file)
            self.log.debug(f"Saved context feature stats to {self._stats_file}")
        except Exception as e:
            self.log.error(f"Failed to save stats: {e}")
    
    def extract_conditions(self, context_snapshot: Dict, direction: str) -> Dict[str, str]:
        """
        Extract all context conditions from a trade's context snapshot.
        
        Returns dict like:
        {
            'trend_alignment': 'aligned',
            'vol_regime': 'high',
            'funding_alignment': 'neutral',
            'smart_money': 'counter',
            'signal_strength': 'moderate',
        }
        """
        conditions = {}
        for feature_name, extractor in FEATURE_EXTRACTORS.items():
            try:
                conditions[feature_name] = extractor(context_snapshot, direction)
            except Exception as e:
                self.log.warning(f"Failed to extract {feature_name}: {e}")
                conditions[feature_name] = 'neutral'
        return conditions
    
    def update_stats(self, context_snapshot: Dict, direction: str, pnl: float) -> Dict[str, str]:
        """
        Update stats with a closed trade result.
        
        Args:
            context_snapshot: The trade's context_snapshot from DB
            direction: Trade direction (LONG/SHORT)
            pnl: Realized PnL
            
        Returns:
            Dict of extracted conditions
        """
        conditions = self.extract_conditions(context_snapshot, direction)
        
        with self._stats_lock:
            for feature_name, condition in conditions.items():
                if feature_name not in self._stats:
                    continue
                if condition not in self._stats[feature_name]:
                    self._stats[feature_name][condition] = FeatureStats()
                
                self._stats[feature_name][condition].update(pnl)
        
        self.save_stats()
        
        self.log.info(
            f"Context stats updated: {conditions} | PnL: ${pnl:.2f}"
        )
        
        return conditions
    
    def get_condition_adjustment(self, feature_name: str, condition: str) -> float:
        """
        Get adjustment multiplier for a specific condition.
        
        Returns 1.0 if not enough data, otherwise scales based on win rate vs baseline.
        """
        with self._stats_lock:
            if feature_name not in self._stats:
                return 1.0
            if condition not in self._stats[feature_name]:
                return 1.0
            
            stats = self._stats[feature_name][condition]
            
            # Bayesian shrinkage toward baseline for low-sample buckets.
            n = max(0, int(stats.trades or 0))
            w = float(n) / float(n + int(self.PRIOR_STRENGTH))
            shrunk_win_rate = (w * float(stats.win_rate)) + ((1.0 - w) * float(self.BASELINE_WIN_RATE))

            # Keep neutral behavior until enough evidence.
            if n < self.MIN_TRADES_FOR_ADJUSTMENT:
                return 1.0

            # Scale adjustment based on shrunk win-rate deviation from baseline.
            deviation = shrunk_win_rate - self.BASELINE_WIN_RATE
            # Scale: ±0.20 win rate deviation = ±0.25 adjustment
            adjustment = 1.0 + (deviation * 1.25)
            
            return max(self.MIN_ADJUSTMENT, min(self.MAX_ADJUSTMENT, adjustment))
    
    def get_context_adjustment(self, context_snapshot: Dict, direction: str) -> Tuple[float, Dict[str, Any]]:
        """
        Get combined context adjustment multiplier.
        
        Args:
            context_snapshot: Current context
            direction: Proposed trade direction
            
        Returns:
            (multiplier, breakdown_dict)
            
        The multiplier is the product of individual feature adjustments,
        clamped to [0.5, 1.5].
        """
        conditions = self.extract_conditions(context_snapshot, direction)
        
        combined = 1.0
        n_adjustments = 0
        breakdown = {}
        
        for feature_name, condition in conditions.items():
            adj = self.get_condition_adjustment(feature_name, condition)
            with self._stats_lock:
                stats = self._stats.get(feature_name, {}).get(condition)
            
            breakdown[feature_name] = {
                'condition': condition,
                'adjustment': round(adj, 3),
                'win_rate': round(stats.win_rate, 3) if stats else None,
                'trades': stats.trades if stats else 0,
            }
            
            combined *= adj
            n_adjustments += 1

        # Prevent multiplicative collapse as feature count grows.
        if n_adjustments > 1 and combined > 0:
            combined = combined ** (1.0 / float(n_adjustments))

        # Composite floor: avoid over-penalizing stacked weak features.
        combined = max(0.4, combined)

        # Clamp final result
        final = max(self.MIN_ADJUSTMENT, min(self.MAX_ADJUSTMENT, combined))
        
        return final, breakdown
    
    def get_stats_summary(self) -> str:
        """Get formatted summary of all stats."""
        lines = ["## Context Feature Stats", ""]
        
        for feature_name, conditions in self._stats.items():
            lines.append(f"### {feature_name}")
            for condition, stats in sorted(conditions.items()):
                if stats.trades > 0:
                    lines.append(
                        f"  - {condition}: {stats.win_rate:.0%} WR "
                        f"({stats.trades} trades, avg ${stats.avg_pnl:.2f})"
                    )
            lines.append("")
        
        return "\n".join(lines)
    
    def get_actionable_insights(self) -> List[str]:
        """Get list of actionable insights from stats."""
        insights = []
        
        for feature_name, conditions in self._stats.items():
            for condition, stats in conditions.items():
                if stats.trades < self.MIN_TRADES_FOR_ADJUSTMENT:
                    continue
                
                adj = self.get_condition_adjustment(feature_name, condition)
                
                if adj < 0.8:
                    insights.append(
                        f"⚠️ AVOID {feature_name}={condition}: "
                        f"{stats.win_rate:.0%} WR ({stats.trades} trades)"
                    )
                elif adj > 1.2:
                    insights.append(
                        f"✅ PREFER {feature_name}={condition}: "
                        f"{stats.win_rate:.0%} WR ({stats.trades} trades)"
                    )
        
        return insights


# =============================================================================
# Module Exports
# =============================================================================

__all__ = [
    'ContextLearningEngine',
    'FeatureStats',
    'FEATURE_EXTRACTORS',
    'extract_trend_alignment',
    'extract_vol_regime',
    'extract_funding_alignment',
    'extract_smart_money_alignment',
    'extract_signal_strength',
    'extract_strategy_segment',
    'extract_hip3_driver',
    'extract_hip3_booster_regime',
]
