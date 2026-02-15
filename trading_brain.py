#!/usr/bin/env python3
"""
Captain EVP Trading Brain - DEGEN Profile

The decision-making core with personality. This REPLACES the legacy
consensus-based approach with conviction-based decisions.

Philosophy:
- NOT a bot - has opinions, evolving strategy
- Embrace risk (0.5-2.5% per trade)
- Microstructure focus - smart vs dumb money
- Symbol-adaptive - different approach per asset
- Self-improving - track mistakes, learn from losses
"""

from logging_utils import get_logger
from dataclasses import dataclass, field
from functools import lru_cache
import math
from typing import Any, Dict, List, Optional, Tuple

from env_utils import env_int

# Type imports for integration
try:
    from context_builder_v2 import ScoredOpportunity
except ImportError:
    ScoredOpportunity = Any



# =============================================================================
# Env-tunable constants
# =============================================================================

DEFAULT_BRAIN_BASE_CONFIDENCE_THRESHOLD = 0.55
DEFAULT_BRAIN_HIGH_CONVICTION_THRESHOLD = 0.75
DEFAULT_BRAIN_BLEND_PIPELINE = 0.20
DEFAULT_BRAIN_CONVICTION_THRESHOLD = 0.70
DEFAULT_BRAIN_CONVICTION_WEIGHTS = {
    "cvd": 0.10,
    "dead_capital": 0.35,
    "fade": 0.07,
    "ofm": 0.22,
    "hip3_main": 0.65,
    "whale": 0.26,
}
DEFAULT_BRAIN_PRIMARY_SIGNALS = ["dead_capital", "whale", "hip3_main"]
DEFAULT_BRAIN_SECONDARY_SIGNALS = ["cvd", "ofm", "fade"]
DEFAULT_BRAIN_DEGEN_MODE = True
DEFAULT_BRAIN_DEGEN_MULTIPLIER = 1.5
DEFAULT_BRAIN_DEGEN_MULTIPLIER_MIN = 0.85
DEFAULT_BRAIN_DEAD_FLOOR_ENABLED = False
DEFAULT_BRAIN_RESPECT_MOMENTUM = True
DEFAULT_BRAIN_HUNT_LIQUIDATIONS = True
DEFAULT_BRAIN_FADE_EXTREMES = True
DEFAULT_BRAIN_ENABLE_VETO = True
DEFAULT_BRAIN_WHALE_VETO_THRESHOLD = 1.0
DEFAULT_BRAIN_CVD_VETO_Z_THRESHOLD = 2.5
DEFAULT_BRAIN_STRONG_Z_THRESHOLD = 1.5
DEFAULT_BRAIN_APPLY_HISTORY_ADJUSTMENTS = True
DEFAULT_BRAIN_MAX_ADJUSTMENT_FACTOR = 0.5

# Runtime constants (safe fallbacks that do not import mode_controller).
BRAIN_PRIMARY_MIN_Z = 2.0
BRAIN_CONVICTION_Z_DENOM = 3.0
BRAIN_STRENGTH_Z_MULT = 2.0
BRAIN_AGREE_BONUS_COUNT_1 = 4
BRAIN_AGREE_BONUS_1 = 0.1
BRAIN_AGREE_BONUS_COUNT_2 = 5
BRAIN_AGREE_BONUS_2 = 0.05

BRAIN_SMART_ADJUST_MULT = 0.1
BRAIN_LIQ_EDGE_THRESHOLD = 0.5
BRAIN_LIQ_EDGE_BONUS = 0.05
BRAIN_VOL_AVOID_PENALTY = 0.15
BRAIN_STRONG_WHALE_BONUS = 0.2
BRAIN_STRONG_OTHER_BONUS = 0.1
BRAIN_STRONG_DEAD_MIN_CONVICTION = 0.9

BRAIN_HISTORY_BLOCK_THRESHOLD = 0.3
BRAIN_PATTERN_PENALTY = 0.7
BRAIN_LEARNING_COMPOSITE_FLOOR = 0.4

BRAIN_SMART_DIV_Z_DENOM = 3.0
BRAIN_DECISION_HISTORY_MAX = 1000
BRAIN_SMART_DIV_Z_MULT = 0.5
BRAIN_COHORT_ALIGN_BONUS = 0.3
BRAIN_COHORT_COUNTER_PENALTY = 0.2

BRAIN_LIQ_FRAGILE_COUNT_1 = 10
BRAIN_LIQ_FRAGILE_NOTIONAL_1 = 500000.0
BRAIN_LIQ_FRAGILE_BONUS_1 = 0.3
BRAIN_LIQ_FRAGILE_COUNT_2 = 30
BRAIN_LIQ_FRAGILE_NOTIONAL_2 = 2000000.0
BRAIN_LIQ_FRAGILE_BONUS_2 = 0.2
BRAIN_LIQ_HOT_PCT_THRESHOLD = 10.0
BRAIN_LIQ_HOT_PCT_BONUS = 0.2
BRAIN_LIQ_PAIN_RATIO = 1.5
BRAIN_LIQ_PAIN_BONUS = 0.2
BRAIN_LIQ_DIR_BONUS = 0.1

BRAIN_ATR_AVOID_MAX = 0.5
BRAIN_ATR_SCALP_MAX = 1.5
BRAIN_ATR_SWING_MAX = 3.0
BRAIN_ATR_VOLATILE_MAX = 5.0

BRAIN_SIZE_T1 = 0.55
BRAIN_SIZE_T2 = 0.65
BRAIN_SIZE_T3 = 0.75
BRAIN_SIZE_T4 = 0.85
BRAIN_SIZE_M1 = 0.5
BRAIN_SIZE_M2 = 0.6
BRAIN_SIZE_M3 = 0.8
BRAIN_SIZE_M4 = 1.0
BRAIN_SIZE_M5 = 1.2
BRAIN_SIZE_SMART_POS_THRESHOLD = 0.5
BRAIN_SIZE_SMART_NEG_THRESHOLD = -0.3
BRAIN_SIZE_SMART_POS_MULT = 1.1
BRAIN_SIZE_SMART_NEG_MULT = 0.9
BRAIN_SIZE_LIQ_EDGE_THRESHOLD = 0.5
BRAIN_SIZE_LIQ_EDGE_MULT = 1.1
BRAIN_SIZE_VOL_AVOID_MULT = 0.5
BRAIN_SIZE_VOL_SCALP_MULT = 0.9

_BRAIN_RUNTIME_REFRESHED = False


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        out = float(value)
    except (TypeError, ValueError):
        return float(default)
    if not math.isfinite(out):
        return float(default)
    return out


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return int(default)


def _normalize_direction(value: Any) -> str:
    d = str(value or "").upper()
    if d in ("LONG", "SHORT", "NEUTRAL"):
        return d
    return "NEUTRAL"


def _clamp01(value: Any) -> float:
    return max(0.0, min(1.0, _safe_float(value, 0.0)))


def compute_blended_conviction(
    pipeline_conv: float,
    brain_conv: float,
    blend: float = DEFAULT_BRAIN_BLEND_PIPELINE,
) -> float:
    """Blend pipeline and brain conviction where blend is pipeline weight."""
    blend_clamped = _clamp01(blend)
    pipeline = _clamp01(pipeline_conv)
    brain = _clamp01(brain_conv)
    return _clamp01((blend_clamped * pipeline) + ((1.0 - blend_clamped) * brain))


@lru_cache(maxsize=1)
def _get_mode_get_param():
    try:
        from mode_controller import get_param as mode_get_param

        return mode_get_param
    except Exception:
        return None


def _runtime_brain_param(name: str, default: Any) -> Any:
    getter = _get_mode_get_param()
    if getter is None:
        return default
    try:
        value = getter("brain", name)
    except Exception:
        return default
    return default if value is None else value


def _refresh_brain_runtime_constants() -> None:
    """Best-effort runtime refresh from mode_controller (lazy, monkeypatch-safe)."""
    global _BRAIN_RUNTIME_REFRESHED
    global BRAIN_PRIMARY_MIN_Z, BRAIN_CONVICTION_Z_DENOM, BRAIN_STRENGTH_Z_MULT
    global BRAIN_AGREE_BONUS_COUNT_1, BRAIN_AGREE_BONUS_1, BRAIN_AGREE_BONUS_COUNT_2, BRAIN_AGREE_BONUS_2
    global BRAIN_SMART_ADJUST_MULT, BRAIN_LIQ_EDGE_THRESHOLD, BRAIN_LIQ_EDGE_BONUS, BRAIN_VOL_AVOID_PENALTY
    global BRAIN_STRONG_WHALE_BONUS, BRAIN_STRONG_OTHER_BONUS, BRAIN_STRONG_DEAD_MIN_CONVICTION
    global BRAIN_HISTORY_BLOCK_THRESHOLD, BRAIN_PATTERN_PENALTY, BRAIN_LEARNING_COMPOSITE_FLOOR
    global BRAIN_SMART_DIV_Z_DENOM, BRAIN_SMART_DIV_Z_MULT, BRAIN_COHORT_ALIGN_BONUS, BRAIN_COHORT_COUNTER_PENALTY
    global BRAIN_LIQ_FRAGILE_COUNT_1, BRAIN_LIQ_FRAGILE_NOTIONAL_1, BRAIN_LIQ_FRAGILE_BONUS_1
    global BRAIN_LIQ_FRAGILE_COUNT_2, BRAIN_LIQ_FRAGILE_NOTIONAL_2, BRAIN_LIQ_FRAGILE_BONUS_2
    global BRAIN_LIQ_HOT_PCT_THRESHOLD, BRAIN_LIQ_HOT_PCT_BONUS, BRAIN_LIQ_PAIN_RATIO, BRAIN_LIQ_PAIN_BONUS
    global BRAIN_LIQ_DIR_BONUS, BRAIN_ATR_AVOID_MAX, BRAIN_ATR_SCALP_MAX, BRAIN_ATR_SWING_MAX, BRAIN_ATR_VOLATILE_MAX
    global BRAIN_SIZE_T1, BRAIN_SIZE_T2, BRAIN_SIZE_T3, BRAIN_SIZE_T4
    global BRAIN_SIZE_M1, BRAIN_SIZE_M2, BRAIN_SIZE_M3, BRAIN_SIZE_M4, BRAIN_SIZE_M5
    global BRAIN_SIZE_SMART_POS_THRESHOLD, BRAIN_SIZE_SMART_NEG_THRESHOLD, BRAIN_SIZE_SMART_POS_MULT
    global BRAIN_SIZE_SMART_NEG_MULT, BRAIN_SIZE_LIQ_EDGE_THRESHOLD, BRAIN_SIZE_LIQ_EDGE_MULT
    global BRAIN_SIZE_VOL_AVOID_MULT, BRAIN_SIZE_VOL_SCALP_MULT
    if _BRAIN_RUNTIME_REFRESHED:
        return

    if BRAIN_PRIMARY_MIN_Z == 2.0:
        BRAIN_PRIMARY_MIN_Z = _safe_float(_runtime_brain_param("primary_min_z", BRAIN_PRIMARY_MIN_Z), BRAIN_PRIMARY_MIN_Z)
    if BRAIN_CONVICTION_Z_DENOM == 3.0:
        BRAIN_CONVICTION_Z_DENOM = _safe_float(_runtime_brain_param("conviction_z_denom", BRAIN_CONVICTION_Z_DENOM), BRAIN_CONVICTION_Z_DENOM)
    if BRAIN_STRENGTH_Z_MULT == 2.0:
        BRAIN_STRENGTH_Z_MULT = _safe_float(_runtime_brain_param("strength_z_mult", BRAIN_STRENGTH_Z_MULT), BRAIN_STRENGTH_Z_MULT)
    if BRAIN_AGREE_BONUS_COUNT_1 == 4:
        BRAIN_AGREE_BONUS_COUNT_1 = _safe_int(_runtime_brain_param("agree_bonus_count_1", BRAIN_AGREE_BONUS_COUNT_1), BRAIN_AGREE_BONUS_COUNT_1)
    if BRAIN_AGREE_BONUS_1 == 0.1:
        BRAIN_AGREE_BONUS_1 = _safe_float(_runtime_brain_param("agree_bonus_1", BRAIN_AGREE_BONUS_1), BRAIN_AGREE_BONUS_1)
    if BRAIN_AGREE_BONUS_COUNT_2 == 5:
        BRAIN_AGREE_BONUS_COUNT_2 = _safe_int(_runtime_brain_param("agree_bonus_count_2", BRAIN_AGREE_BONUS_COUNT_2), BRAIN_AGREE_BONUS_COUNT_2)
    if BRAIN_AGREE_BONUS_2 == 0.05:
        BRAIN_AGREE_BONUS_2 = _safe_float(_runtime_brain_param("agree_bonus_2", BRAIN_AGREE_BONUS_2), BRAIN_AGREE_BONUS_2)

    if BRAIN_SMART_ADJUST_MULT == 0.1:
        BRAIN_SMART_ADJUST_MULT = _safe_float(_runtime_brain_param("smart_adjust_mult", BRAIN_SMART_ADJUST_MULT), BRAIN_SMART_ADJUST_MULT)
    if BRAIN_LIQ_EDGE_THRESHOLD == 0.5:
        BRAIN_LIQ_EDGE_THRESHOLD = _safe_float(_runtime_brain_param("liq_edge_threshold", BRAIN_LIQ_EDGE_THRESHOLD), BRAIN_LIQ_EDGE_THRESHOLD)
    if BRAIN_LIQ_EDGE_BONUS == 0.05:
        BRAIN_LIQ_EDGE_BONUS = _safe_float(_runtime_brain_param("liq_edge_bonus", BRAIN_LIQ_EDGE_BONUS), BRAIN_LIQ_EDGE_BONUS)
    if BRAIN_VOL_AVOID_PENALTY == 0.15:
        BRAIN_VOL_AVOID_PENALTY = _safe_float(_runtime_brain_param("vol_avoid_penalty", BRAIN_VOL_AVOID_PENALTY), BRAIN_VOL_AVOID_PENALTY)
    if BRAIN_STRONG_WHALE_BONUS == 0.2:
        BRAIN_STRONG_WHALE_BONUS = _safe_float(_runtime_brain_param("strong_whale_bonus", BRAIN_STRONG_WHALE_BONUS), BRAIN_STRONG_WHALE_BONUS)
    if BRAIN_STRONG_OTHER_BONUS == 0.1:
        BRAIN_STRONG_OTHER_BONUS = _safe_float(_runtime_brain_param("strong_other_bonus", BRAIN_STRONG_OTHER_BONUS), BRAIN_STRONG_OTHER_BONUS)
    if BRAIN_STRONG_DEAD_MIN_CONVICTION == 0.9:
        BRAIN_STRONG_DEAD_MIN_CONVICTION = _safe_float(
            _runtime_brain_param("strong_dead_min_conviction", BRAIN_STRONG_DEAD_MIN_CONVICTION),
            BRAIN_STRONG_DEAD_MIN_CONVICTION,
        )

    if BRAIN_HISTORY_BLOCK_THRESHOLD == 0.3:
        BRAIN_HISTORY_BLOCK_THRESHOLD = _safe_float(_runtime_brain_param("history_block_threshold", BRAIN_HISTORY_BLOCK_THRESHOLD), BRAIN_HISTORY_BLOCK_THRESHOLD)
    if BRAIN_PATTERN_PENALTY == 0.7:
        BRAIN_PATTERN_PENALTY = _safe_float(_runtime_brain_param("pattern_penalty", BRAIN_PATTERN_PENALTY), BRAIN_PATTERN_PENALTY)
    if BRAIN_LEARNING_COMPOSITE_FLOOR == 0.4:
        BRAIN_LEARNING_COMPOSITE_FLOOR = _safe_float(
            _runtime_brain_param("learning_composite_floor", BRAIN_LEARNING_COMPOSITE_FLOOR),
            BRAIN_LEARNING_COMPOSITE_FLOOR,
        )
    if BRAIN_SMART_DIV_Z_DENOM == 3.0:
        BRAIN_SMART_DIV_Z_DENOM = _safe_float(_runtime_brain_param("smart_div_z_denom", BRAIN_SMART_DIV_Z_DENOM), BRAIN_SMART_DIV_Z_DENOM)
    if BRAIN_SMART_DIV_Z_MULT == 0.5:
        BRAIN_SMART_DIV_Z_MULT = _safe_float(_runtime_brain_param("smart_div_z_mult", BRAIN_SMART_DIV_Z_MULT), BRAIN_SMART_DIV_Z_MULT)
    if BRAIN_COHORT_ALIGN_BONUS == 0.3:
        BRAIN_COHORT_ALIGN_BONUS = _safe_float(_runtime_brain_param("cohort_align_bonus", BRAIN_COHORT_ALIGN_BONUS), BRAIN_COHORT_ALIGN_BONUS)
    if BRAIN_COHORT_COUNTER_PENALTY == 0.2:
        BRAIN_COHORT_COUNTER_PENALTY = _safe_float(_runtime_brain_param("cohort_counter_penalty", BRAIN_COHORT_COUNTER_PENALTY), BRAIN_COHORT_COUNTER_PENALTY)

    if BRAIN_LIQ_FRAGILE_COUNT_1 == 10:
        BRAIN_LIQ_FRAGILE_COUNT_1 = _safe_int(_runtime_brain_param("liq_fragile_count_1", BRAIN_LIQ_FRAGILE_COUNT_1), BRAIN_LIQ_FRAGILE_COUNT_1)
    if BRAIN_LIQ_FRAGILE_NOTIONAL_1 == 500000.0:
        BRAIN_LIQ_FRAGILE_NOTIONAL_1 = _safe_float(_runtime_brain_param("liq_fragile_notional_1", BRAIN_LIQ_FRAGILE_NOTIONAL_1), BRAIN_LIQ_FRAGILE_NOTIONAL_1)
    if BRAIN_LIQ_FRAGILE_BONUS_1 == 0.3:
        BRAIN_LIQ_FRAGILE_BONUS_1 = _safe_float(_runtime_brain_param("liq_fragile_bonus_1", BRAIN_LIQ_FRAGILE_BONUS_1), BRAIN_LIQ_FRAGILE_BONUS_1)
    if BRAIN_LIQ_FRAGILE_COUNT_2 == 30:
        BRAIN_LIQ_FRAGILE_COUNT_2 = _safe_int(_runtime_brain_param("liq_fragile_count_2", BRAIN_LIQ_FRAGILE_COUNT_2), BRAIN_LIQ_FRAGILE_COUNT_2)
    if BRAIN_LIQ_FRAGILE_NOTIONAL_2 == 2000000.0:
        BRAIN_LIQ_FRAGILE_NOTIONAL_2 = _safe_float(_runtime_brain_param("liq_fragile_notional_2", BRAIN_LIQ_FRAGILE_NOTIONAL_2), BRAIN_LIQ_FRAGILE_NOTIONAL_2)
    if BRAIN_LIQ_FRAGILE_BONUS_2 == 0.2:
        BRAIN_LIQ_FRAGILE_BONUS_2 = _safe_float(_runtime_brain_param("liq_fragile_bonus_2", BRAIN_LIQ_FRAGILE_BONUS_2), BRAIN_LIQ_FRAGILE_BONUS_2)
    if BRAIN_LIQ_HOT_PCT_THRESHOLD == 10.0:
        BRAIN_LIQ_HOT_PCT_THRESHOLD = _safe_float(_runtime_brain_param("liq_hot_pct_threshold", BRAIN_LIQ_HOT_PCT_THRESHOLD), BRAIN_LIQ_HOT_PCT_THRESHOLD)
    if BRAIN_LIQ_HOT_PCT_BONUS == 0.2:
        BRAIN_LIQ_HOT_PCT_BONUS = _safe_float(_runtime_brain_param("liq_hot_pct_bonus", BRAIN_LIQ_HOT_PCT_BONUS), BRAIN_LIQ_HOT_PCT_BONUS)
    if BRAIN_LIQ_PAIN_RATIO == 1.5:
        BRAIN_LIQ_PAIN_RATIO = _safe_float(_runtime_brain_param("liq_pain_ratio", BRAIN_LIQ_PAIN_RATIO), BRAIN_LIQ_PAIN_RATIO)
    if BRAIN_LIQ_PAIN_BONUS == 0.2:
        BRAIN_LIQ_PAIN_BONUS = _safe_float(_runtime_brain_param("liq_pain_bonus", BRAIN_LIQ_PAIN_BONUS), BRAIN_LIQ_PAIN_BONUS)
    if BRAIN_LIQ_DIR_BONUS == 0.1:
        BRAIN_LIQ_DIR_BONUS = _safe_float(_runtime_brain_param("liq_dir_bonus", BRAIN_LIQ_DIR_BONUS), BRAIN_LIQ_DIR_BONUS)

    if BRAIN_ATR_AVOID_MAX == 0.5:
        BRAIN_ATR_AVOID_MAX = _safe_float(_runtime_brain_param("atr_avoid_max", BRAIN_ATR_AVOID_MAX), BRAIN_ATR_AVOID_MAX)
    if BRAIN_ATR_SCALP_MAX == 1.5:
        BRAIN_ATR_SCALP_MAX = _safe_float(_runtime_brain_param("atr_scalp_max", BRAIN_ATR_SCALP_MAX), BRAIN_ATR_SCALP_MAX)
    if BRAIN_ATR_SWING_MAX == 3.0:
        BRAIN_ATR_SWING_MAX = _safe_float(_runtime_brain_param("atr_swing_max", BRAIN_ATR_SWING_MAX), BRAIN_ATR_SWING_MAX)
    if BRAIN_ATR_VOLATILE_MAX == 5.0:
        BRAIN_ATR_VOLATILE_MAX = _safe_float(_runtime_brain_param("atr_volatile_max", BRAIN_ATR_VOLATILE_MAX), BRAIN_ATR_VOLATILE_MAX)

    if BRAIN_SIZE_T1 == 0.55:
        BRAIN_SIZE_T1 = _safe_float(_runtime_brain_param("size_t1", BRAIN_SIZE_T1), BRAIN_SIZE_T1)
    if BRAIN_SIZE_T2 == 0.65:
        BRAIN_SIZE_T2 = _safe_float(_runtime_brain_param("size_t2", BRAIN_SIZE_T2), BRAIN_SIZE_T2)
    if BRAIN_SIZE_T3 == 0.75:
        BRAIN_SIZE_T3 = _safe_float(_runtime_brain_param("size_t3", BRAIN_SIZE_T3), BRAIN_SIZE_T3)
    if BRAIN_SIZE_T4 == 0.85:
        BRAIN_SIZE_T4 = _safe_float(_runtime_brain_param("size_t4", BRAIN_SIZE_T4), BRAIN_SIZE_T4)
    if BRAIN_SIZE_M1 == 0.5:
        BRAIN_SIZE_M1 = _safe_float(_runtime_brain_param("size_m1", BRAIN_SIZE_M1), BRAIN_SIZE_M1)
    if BRAIN_SIZE_M2 == 0.6:
        BRAIN_SIZE_M2 = _safe_float(_runtime_brain_param("size_m2", BRAIN_SIZE_M2), BRAIN_SIZE_M2)
    if BRAIN_SIZE_M3 == 0.8:
        BRAIN_SIZE_M3 = _safe_float(_runtime_brain_param("size_m3", BRAIN_SIZE_M3), BRAIN_SIZE_M3)
    if BRAIN_SIZE_M4 == 1.0:
        BRAIN_SIZE_M4 = _safe_float(_runtime_brain_param("size_m4", BRAIN_SIZE_M4), BRAIN_SIZE_M4)
    if BRAIN_SIZE_M5 == 1.2:
        BRAIN_SIZE_M5 = _safe_float(_runtime_brain_param("size_m5", BRAIN_SIZE_M5), BRAIN_SIZE_M5)
    if BRAIN_SIZE_SMART_POS_THRESHOLD == 0.5:
        BRAIN_SIZE_SMART_POS_THRESHOLD = _safe_float(_runtime_brain_param("size_smart_pos_threshold", BRAIN_SIZE_SMART_POS_THRESHOLD), BRAIN_SIZE_SMART_POS_THRESHOLD)
    if BRAIN_SIZE_SMART_NEG_THRESHOLD == -0.3:
        BRAIN_SIZE_SMART_NEG_THRESHOLD = _safe_float(_runtime_brain_param("size_smart_neg_threshold", BRAIN_SIZE_SMART_NEG_THRESHOLD), BRAIN_SIZE_SMART_NEG_THRESHOLD)
    if BRAIN_SIZE_SMART_POS_MULT == 1.1:
        BRAIN_SIZE_SMART_POS_MULT = _safe_float(_runtime_brain_param("size_smart_pos_mult", BRAIN_SIZE_SMART_POS_MULT), BRAIN_SIZE_SMART_POS_MULT)
    if BRAIN_SIZE_SMART_NEG_MULT == 0.9:
        BRAIN_SIZE_SMART_NEG_MULT = _safe_float(_runtime_brain_param("size_smart_neg_mult", BRAIN_SIZE_SMART_NEG_MULT), BRAIN_SIZE_SMART_NEG_MULT)
    if BRAIN_SIZE_LIQ_EDGE_THRESHOLD == 0.5:
        BRAIN_SIZE_LIQ_EDGE_THRESHOLD = _safe_float(_runtime_brain_param("size_liq_edge_threshold", BRAIN_SIZE_LIQ_EDGE_THRESHOLD), BRAIN_SIZE_LIQ_EDGE_THRESHOLD)
    if BRAIN_SIZE_LIQ_EDGE_MULT == 1.1:
        BRAIN_SIZE_LIQ_EDGE_MULT = _safe_float(_runtime_brain_param("size_liq_edge_mult", BRAIN_SIZE_LIQ_EDGE_MULT), BRAIN_SIZE_LIQ_EDGE_MULT)
    if BRAIN_SIZE_VOL_AVOID_MULT == 0.5:
        BRAIN_SIZE_VOL_AVOID_MULT = _safe_float(_runtime_brain_param("size_vol_avoid_mult", BRAIN_SIZE_VOL_AVOID_MULT), BRAIN_SIZE_VOL_AVOID_MULT)
    if BRAIN_SIZE_VOL_SCALP_MULT == 0.9:
        BRAIN_SIZE_VOL_SCALP_MULT = _safe_float(_runtime_brain_param("size_vol_scalp_mult", BRAIN_SIZE_VOL_SCALP_MULT), BRAIN_SIZE_VOL_SCALP_MULT)

    _BRAIN_RUNTIME_REFRESHED = True

# =============================================================================
# Configuration
# =============================================================================

@dataclass
class BrainConfig:
    """Configuration for Captain EVP's trading brain."""

    # Conviction thresholds
    base_confidence_threshold: float = DEFAULT_BRAIN_BASE_CONFIDENCE_THRESHOLD  # Min conviction to trade
    high_conviction_threshold: float = DEFAULT_BRAIN_HIGH_CONVICTION_THRESHOLD  # Size up threshold
    blend_pipeline: float = DEFAULT_BRAIN_BLEND_PIPELINE  # Pipeline weight in blended conviction
    conviction_threshold: float = DEFAULT_BRAIN_CONVICTION_THRESHOLD  # Entry threshold for blended conviction

    # Per-signal conviction weights (AGI Trader policy - Jan 2026)
    # Primary signals: DEAD_CAPITAL, WHALE (can trigger independently)
    # Secondary signals: CVD, OFM, FADE (confirmation only)
    # DEPRECATED: LIQ_PNL (low performance, kept in pipeline for monitoring)
    # HIP3 Primary: hip3_main (for xyz: symbols, can trigger independently)
    conviction_weights: Dict[str, float] = field(default_factory=lambda: dict(DEFAULT_BRAIN_CONVICTION_WEIGHTS))

    # Primary signals that can trigger trades independently
    primary_signals: List[str] = field(default_factory=lambda: list(DEFAULT_BRAIN_PRIMARY_SIGNALS))

    # Secondary signals (confirmation only, require primary co-trigger)
    secondary_signals: List[str] = field(default_factory=lambda: list(DEFAULT_BRAIN_SECONDARY_SIGNALS))

    # DEGEN mode settings
    degen_mode: bool = DEFAULT_BRAIN_DEGEN_MODE  # Enable aggressive sizing
    degen_multiplier: float = DEFAULT_BRAIN_DEGEN_MULTIPLIER  # Size boost for high conviction
    degen_multiplier_min: float = DEFAULT_BRAIN_DEGEN_MULTIPLIER_MIN  # Min conviction for degen size boost
    dead_floor_enabled: bool = DEFAULT_BRAIN_DEAD_FLOOR_ENABLED  # Force strong-dead floor

    # Microstructure settings
    respect_momentum: bool = DEFAULT_BRAIN_RESPECT_MOMENTUM  # Follow smart money
    hunt_liquidations: bool = DEFAULT_BRAIN_HUNT_LIQUIDATIONS  # Target fragile positions
    fade_extremes: bool = DEFAULT_BRAIN_FADE_EXTREMES  # Counter extreme positioning

    # Veto settings (WHALE and CVD can still veto)
    enable_veto: bool = DEFAULT_BRAIN_ENABLE_VETO
    whale_veto_threshold: float = DEFAULT_BRAIN_WHALE_VETO_THRESHOLD  # Strength threshold
    cvd_veto_z_threshold: float = DEFAULT_BRAIN_CVD_VETO_Z_THRESHOLD  # Z-score threshold

    # Strong signal threshold (abs z-score)
    strong_z_threshold: float = DEFAULT_BRAIN_STRONG_Z_THRESHOLD

    # Learning integration
    apply_history_adjustments: bool = DEFAULT_BRAIN_APPLY_HISTORY_ADJUSTMENTS
    max_adjustment_factor: float = DEFAULT_BRAIN_MAX_ADJUSTMENT_FACTOR  # Max -50% from history

    @classmethod
    def from_runtime(cls) -> "BrainConfig":
        """Build config from mode_controller lazily (no import-time side effects)."""
        return cls(
            base_confidence_threshold=_safe_float(
                _runtime_brain_param("base_confidence_threshold", DEFAULT_BRAIN_BASE_CONFIDENCE_THRESHOLD),
                DEFAULT_BRAIN_BASE_CONFIDENCE_THRESHOLD,
            ),
            high_conviction_threshold=_safe_float(
                _runtime_brain_param("high_conviction_threshold", DEFAULT_BRAIN_HIGH_CONVICTION_THRESHOLD),
                DEFAULT_BRAIN_HIGH_CONVICTION_THRESHOLD,
            ),
            blend_pipeline=_safe_float(
                _runtime_brain_param("blend_pipeline", DEFAULT_BRAIN_BLEND_PIPELINE),
                DEFAULT_BRAIN_BLEND_PIPELINE,
            ),
            conviction_threshold=_safe_float(
                _runtime_brain_param("conviction_threshold", DEFAULT_BRAIN_CONVICTION_THRESHOLD),
                DEFAULT_BRAIN_CONVICTION_THRESHOLD,
            ),
            conviction_weights=dict(
                _runtime_brain_param("conviction_weights", DEFAULT_BRAIN_CONVICTION_WEIGHTS)
                or DEFAULT_BRAIN_CONVICTION_WEIGHTS
            ),
            primary_signals=list(
                _runtime_brain_param("primary_signals", DEFAULT_BRAIN_PRIMARY_SIGNALS)
                or DEFAULT_BRAIN_PRIMARY_SIGNALS
            ),
            secondary_signals=list(
                _runtime_brain_param("secondary_signals", DEFAULT_BRAIN_SECONDARY_SIGNALS)
                or DEFAULT_BRAIN_SECONDARY_SIGNALS
            ),
            degen_mode=bool(_runtime_brain_param("degen_mode", DEFAULT_BRAIN_DEGEN_MODE)),
            degen_multiplier=_safe_float(
                _runtime_brain_param("degen_multiplier", DEFAULT_BRAIN_DEGEN_MULTIPLIER),
                DEFAULT_BRAIN_DEGEN_MULTIPLIER,
            ),
            degen_multiplier_min=_safe_float(
                _runtime_brain_param("degen_multiplier_min", DEFAULT_BRAIN_DEGEN_MULTIPLIER_MIN),
                DEFAULT_BRAIN_DEGEN_MULTIPLIER_MIN,
            ),
            dead_floor_enabled=bool(
                _runtime_brain_param("dead_floor_enabled", DEFAULT_BRAIN_DEAD_FLOOR_ENABLED)
            ),
            respect_momentum=bool(_runtime_brain_param("respect_momentum", DEFAULT_BRAIN_RESPECT_MOMENTUM)),
            hunt_liquidations=bool(
                _runtime_brain_param("hunt_liquidations", DEFAULT_BRAIN_HUNT_LIQUIDATIONS)
            ),
            fade_extremes=bool(_runtime_brain_param("fade_extremes", DEFAULT_BRAIN_FADE_EXTREMES)),
            enable_veto=bool(_runtime_brain_param("enable_veto", DEFAULT_BRAIN_ENABLE_VETO)),
            whale_veto_threshold=_safe_float(
                _runtime_brain_param("whale_veto_threshold", DEFAULT_BRAIN_WHALE_VETO_THRESHOLD),
                DEFAULT_BRAIN_WHALE_VETO_THRESHOLD,
            ),
            cvd_veto_z_threshold=_safe_float(
                _runtime_brain_param("cvd_veto_z_threshold", DEFAULT_BRAIN_CVD_VETO_Z_THRESHOLD),
                DEFAULT_BRAIN_CVD_VETO_Z_THRESHOLD,
            ),
            strong_z_threshold=_safe_float(
                _runtime_brain_param("strong_z_threshold", DEFAULT_BRAIN_STRONG_Z_THRESHOLD),
                DEFAULT_BRAIN_STRONG_Z_THRESHOLD,
            ),
            apply_history_adjustments=bool(
                _runtime_brain_param("apply_history_adjustments", DEFAULT_BRAIN_APPLY_HISTORY_ADJUSTMENTS)
            ),
            max_adjustment_factor=_safe_float(
                _runtime_brain_param("max_adjustment_factor", DEFAULT_BRAIN_MAX_ADJUSTMENT_FACTOR),
                DEFAULT_BRAIN_MAX_ADJUSTMENT_FACTOR,
            ),
        )


@dataclass
class BrainDecision:
    """Output of brain evaluation - what to do and why."""

    symbol: str
    direction: str  # 'LONG', 'SHORT', 'NO_TRADE'
    conviction: float  # 0.0 - 1.0
    size_multiplier: float  # 0.5 - 2.0

    # Signal breakdown
    signals_for: List[str]  # Signals supporting direction
    signals_against: List[str]  # Signals opposing
    signals_neutral: List[str]  # Neutral signals

    # Microstructure factors
    smart_money_score: float  # -1 to +1
    liquidation_edge: float  # 0 to 1
    volatility_fit: str  # 'SCALP', 'SWING', 'AVOID'

    # Veto/Block reasons
    vetoed: bool = False
    veto_reason: str = ""
    blocked_by_history: bool = False
    history_note: str = ""
    min_confidence: float = 0.0

    # Reasoning
    reasoning: str = ""

    @property
    def should_trade(self) -> bool:
        """Whether this decision results in a trade."""
        return (
            self.direction in ('LONG', 'SHORT')
            and self.conviction >= self.min_confidence
            and not self.vetoed
            and not self.blocked_by_history
        )


def get_conviction_config(config: Optional[BrainConfig] = None) -> Dict[str, Any]:
    """Export active conviction config for logs and diagnostics."""
    cfg = config or BrainConfig.from_runtime()
    return {
        "weights": dict(cfg.conviction_weights),
        "blend_pipeline": float(_clamp01(cfg.blend_pipeline)),
        "conviction_threshold": float(_clamp01(cfg.conviction_threshold)),
        "degen_mult_min": float(_safe_float(cfg.degen_multiplier_min, DEFAULT_BRAIN_DEGEN_MULTIPLIER_MIN)),
        "degen_mult_max": float(_safe_float(cfg.degen_multiplier, DEFAULT_BRAIN_DEGEN_MULTIPLIER)),
        "dead_floor_enabled": bool(cfg.dead_floor_enabled),
    }


# =============================================================================
# Trading Brain
# =============================================================================

class TradingBrain:
    """
    Captain EVP's decision-making brain - DEGEN profile.

    Conviction score (0-1) drives sizing and trade/no-trade decisions.

    Usage:
        brain = TradingBrain(config)
        for opp in opportunities:
            decision = brain.evaluate_opportunity(opp)
            if decision.should_trade:
                execute(decision)
    """

    def __init__(
        self,
        config: Optional[BrainConfig] = None,
        symbol_profiler: Optional[Any] = None,  # SymbolProfiler
        learning_engine: Optional[Any] = None,  # LearningEngine
        adaptive_entry: Optional[Any] = None,  # AdaptiveEntryManager for learned conviction adjustments
    ):
        _refresh_brain_runtime_constants()
        self.config = config or BrainConfig.from_runtime()
        self.profiler = symbol_profiler
        self.learning = learning_engine
        self.adaptive_entry = adaptive_entry  # For adaptive conviction adjustments
        self.log = get_logger("brain")

        # Decision history for this session
        self._decisions: List[BrainDecision] = []

    def evaluate_opportunity(self, opp: ScoredOpportunity) -> BrainDecision:
        """
        Evaluate a scored opportunity and decide whether to trade.

        Args:
            opp: ScoredOpportunity from ContextBuilderV2

        Returns:
            BrainDecision with conviction, size, and reasoning
        """
        symbol = opp.symbol
        metrics = opp.key_metrics
        signals = opp.signals  # Dict[signal_name, {direction, z_score, etc}]

        # 1. Detect strong signals
        strong_signals = self._strong_signals(opp, signals)
        strong_dead = "dead_capital" in strong_signals
        strong_whale = "whale" in strong_signals
        strong_other = any(s not in ("dead_capital", "whale") for s in strong_signals)

        # 2. Determine direction override for strong dead_capital
        # (Dead capital can override direction basis when it is a strong signal.)
        direction_override = None
        if strong_dead:
            dead_dir = str(signals.get("dead_capital", {}).get("direction") or "").upper()
            if dead_dir in ("LONG", "SHORT"):
                direction_override = dead_dir

        direction_basis = _normalize_direction(direction_override or opp.direction)

        # 3. Count signals by direction
        signals_for, signals_against, signals_neutral = self._categorize_signals(
            signals, direction_basis
        )

        # 4. Check veto conditions first (skip veto for strong dead_capital)
        vetoed, veto_reason = False, ""
        if not strong_dead:
            vetoed, veto_reason = self._check_veto(signals, direction_basis)
            if vetoed:
                return BrainDecision(
                    symbol=symbol,
                    direction='NO_TRADE',
                    conviction=0.0,
                    size_multiplier=0.0,
                    signals_for=signals_for,
                    signals_against=signals_against,
                    signals_neutral=signals_neutral,
                    smart_money_score=0.0,
                    liquidation_edge=0.0,
                    volatility_fit='AVOID',
                    vetoed=True,
                    veto_reason=veto_reason,
                    min_confidence=self.config.base_confidence_threshold,
                    reasoning=f"VETOED: {veto_reason}"
                )

        # 4b. Check for primary signal requirement (secondary signals need co-trigger)
        #     DEAD_CAPITAL and FADE cannot trigger trades alone - need CVD/OFM/WHALE
        has_primary_trigger = self._has_primary_signal_trigger(signals, direction_basis)
        if not has_primary_trigger and not strong_dead:
            # Only secondary signals are firing - block entry
            secondary_only = [s for s in signals_for if s in self.config.secondary_signals]
            if secondary_only and not any(s in self.config.primary_signals for s in signals_for):
                return BrainDecision(
                    symbol=symbol,
                    direction='NO_TRADE',
                    conviction=0.0,
                    size_multiplier=0.0,
                    signals_for=signals_for,
                    signals_against=signals_against,
                    signals_neutral=signals_neutral,
                    smart_money_score=0.0,
                    liquidation_edge=0.0,
                    volatility_fit='AVOID',
                    vetoed=True,
                    veto_reason=f"Secondary-only ({', '.join(secondary_only)}) - requires primary co-trigger",
                    min_confidence=self.config.base_confidence_threshold,
                    reasoning=f"BLOCKED: Secondary signals only, no primary trigger"
                )

        # 5. Calculate conviction score
        conviction = self._calculate_conviction(signals, metrics, direction_basis)

        # 6. Calculate microstructure factors
        smart_money = self._smart_money_alignment(metrics, direction_basis)
        liq_edge = self._liquidation_edge(metrics, direction_basis)
        vol_fit = self._volatility_opportunity(metrics.get('atr_pct', 0))

        # 7. Adjust conviction based on factors
        adjusted_conviction = conviction

        # Smart money alignment boost/penalty
        if self.config.respect_momentum:
            adjusted_conviction += smart_money * BRAIN_SMART_ADJUST_MULT  # +/- adjustment

        # Liquidation hunting boost
        if self.config.hunt_liquidations and liq_edge > BRAIN_LIQ_EDGE_THRESHOLD:
            adjusted_conviction += BRAIN_LIQ_EDGE_BONUS

        # Volatility fit penalty
        if vol_fit == 'AVOID':
            adjusted_conviction -= BRAIN_VOL_AVOID_PENALTY

        # Clamp conviction
        adjusted_conviction = max(0.0, min(1.0, adjusted_conviction))

        # 8. Check history/learning adjustments
        blocked_by_history = False
        history_note = ""

        if self.learning and self.config.apply_history_adjustments and not strong_dead:
            history_mult = self.learning.get_symbol_adjustment(symbol)
            combined_learning_mult = 1.0
            if history_mult < BRAIN_HISTORY_BLOCK_THRESHOLD:  # Heavy penalty = avoid
                blocked_by_history = True
                history_note = f"Symbol has poor recent history (mult={history_mult:.2f})"
            else:
                combined_learning_mult *= history_mult
                if history_mult < 1.0:
                    history_note = f"Reduced by history (mult={history_mult:.2f})"

            # Pattern avoidance penalty (NOT hard block - just reduce conviction)
            if signals_for and self.learning.should_avoid_pattern(signals_for, direction_basis):
                pattern_penalty = BRAIN_PATTERN_PENALTY  # reduction for avoided patterns
                combined_learning_mult *= pattern_penalty
                if not history_note:
                    history_note = f"Pattern avoided (penalty={pattern_penalty:.2f})"
                else:
                    history_note += f", pattern avoided"

            # Signal adjustment multiplier (product, not mean - to properly compound penalties)
            if signals_for:
                signal_mult = 1.0
                for s in signals_for:
                    signal_mult *= self.learning.get_signal_adjustment(s)
                if signal_mult < 1.0:
                    combined_learning_mult *= signal_mult
                    if not history_note:
                        history_note = f"Signal adj ({signal_mult:.2f})"
                    else:
                        history_note += f", signal adj ({signal_mult:.2f})"

            if not blocked_by_history:
                floored_mult = max(BRAIN_LEARNING_COMPOSITE_FLOOR, combined_learning_mult)
                if floored_mult > combined_learning_mult:
                    if not history_note:
                        history_note = f"Learning floor applied ({floored_mult:.2f})"
                    else:
                        history_note += f", floor ({floored_mult:.2f})"
                adjusted_conviction *= floored_mult

        # 6b. Apply adaptive entry adjustment (learned from outcomes)
        if self.adaptive_entry and not strong_dead:
            try:
                # Find the strongest signal to use as trigger type
                trigger_signal = None
                best_z = 0.0
                for sig_name, sig_data in signals.items():
                    z_score = abs(_safe_float(sig_data.get('z_score', 0.0), 0.0))
                    if z_score <= 0 and sig_name == "cvd":
                        z_smart = abs(_safe_float(sig_data.get('z_smart', 0.0), 0.0))
                        z_dumb = abs(_safe_float(sig_data.get('z_dumb', 0.0), 0.0))
                        z_score = max(z_smart, z_dumb)
                    if z_score <= 0 and sig_name in ("whale", "dead_capital"):
                        strength = _safe_float(sig_data.get("strength", 0.0), 0.0)
                        z_score = abs(strength * _safe_float(BRAIN_STRENGTH_Z_MULT, 2.0))
                    if z_score > best_z:
                        best_z = z_score
                        trigger_signal = sig_name

                if trigger_signal:
                    conv_mult = self.adaptive_entry.get_conviction_adjustment(trigger_signal, symbol)
                    adjusted_conviction *= conv_mult
                    self.log.debug(f"Adaptive entry for {trigger_signal}/{symbol}: mult={conv_mult:.3f}")
            except Exception as e:
                self.log.warning(f"Adaptive entry query failed for {symbol}: {e}")
                # Continue with unadjusted conviction

        # Apply strong-signal boosts
        if strong_whale:
            adjusted_conviction += BRAIN_STRONG_WHALE_BONUS
        elif strong_other:
            adjusted_conviction += BRAIN_STRONG_OTHER_BONUS

        if strong_dead:
            if self.config.dead_floor_enabled:
                adjusted_conviction = max(
                    adjusted_conviction,
                    self.config.high_conviction_threshold,
                    BRAIN_STRONG_DEAD_MIN_CONVICTION,
                )

        # Re-clamp conviction after adaptive adjustment + strong boosts
        adjusted_conviction = max(0.0, min(1.0, adjusted_conviction))

        # 9. Determine direction
        if strong_dead:
            direction = direction_basis if direction_basis in ('LONG', 'SHORT') else 'NO_TRADE'
        elif adjusted_conviction < self.config.base_confidence_threshold:
            direction = 'NO_TRADE'
        else:
            direction = direction_basis

        # 10. Calculate size multiplier
        size_mult = self._calculate_size_multiplier(
            adjusted_conviction, smart_money, liq_edge, vol_fit
        )

        # 11. Apply symbol profile adjustment
        if self.profiler:
            profile = self.profiler.get_cached_profile(symbol)
            if profile:
                size_mult *= profile.risk_multiplier

        # 12. Build reasoning
        reasoning = self._build_reasoning(
            symbol, direction, adjusted_conviction, smart_money, liq_edge,
            signals_for, signals_against, vol_fit
        )
        if strong_signals:
            reasoning = f"Strong signals: {', '.join(strong_signals)}. {reasoning}"

        decision = BrainDecision(
            symbol=symbol,
            direction=direction,
            conviction=adjusted_conviction,
            size_multiplier=size_mult,
            signals_for=signals_for,
            signals_against=signals_against,
            signals_neutral=signals_neutral,
            smart_money_score=smart_money,
            liquidation_edge=liq_edge,
            volatility_fit=vol_fit,
            vetoed=False,
            blocked_by_history=False if strong_dead else blocked_by_history,
            history_note="" if strong_dead else history_note,
            # Strong dead-capital should be allowed to trade even if conviction is below the
            # global min-confidence threshold (direction is already overridden above).
            min_confidence=0.0 if strong_dead else self.config.base_confidence_threshold,
            reasoning=reasoning
        )

        # Track decision
        self._decisions.append(decision)
        if BRAIN_DECISION_HISTORY_MAX > 0 and len(self._decisions) > BRAIN_DECISION_HISTORY_MAX:
            self._decisions = self._decisions[-BRAIN_DECISION_HISTORY_MAX:]

        return decision

    def _categorize_signals(
        self,
        signals: Dict[str, Dict],
        direction: str
    ) -> Tuple[List[str], List[str], List[str]]:
        """Categorize signals as supporting, opposing, or neutral."""
        signals_for = []
        signals_against = []
        signals_neutral = []
        base_dir = _normalize_direction(direction)

        for sig_name, sig_data in signals.items():
            sig_dir = _normalize_direction(sig_data.get('direction', 'NEUTRAL'))

            if sig_dir == 'NEUTRAL':
                signals_neutral.append(sig_name)
            elif sig_dir == base_dir:
                signals_for.append(sig_name)
            else:
                signals_against.append(sig_name)

        return signals_for, signals_against, signals_neutral

    def _strong_signals(
        self,
        opp: ScoredOpportunity,
        signals: Dict[str, Dict],
    ) -> List[str]:
        """Return strong signal names based on z-score threshold."""
        strong = set(getattr(opp, "strong_signals", []) or [])
        if strong:
            return sorted(strong)

        for name, payload in (signals or {}).items():
            if not isinstance(payload, dict):
                continue
            direction = _normalize_direction(payload.get("direction"))
            if direction not in ("LONG", "SHORT"):
                continue
            z_score = payload.get("z_score")
            if z_score is None and name == "cvd":
                z_smart = payload.get("z_smart", 0) or 0
                z_dumb = payload.get("z_dumb", 0) or 0
                z_score = max(abs(z_smart), abs(z_dumb))
            if z_score is None and name in ("whale", "dead_capital"):
                strength = payload.get("strength", 0) or 0
                z_score = strength * BRAIN_STRENGTH_Z_MULT
            try:
                z_val = abs(float(z_score or 0))
            except (TypeError, ValueError):
                z_val = 0.0
            if z_val >= self.config.strong_z_threshold:
                strong.add(name)
        return sorted(strong)

    def _check_veto(
        self,
        signals: Dict[str, Dict],
        direction: str
    ) -> Tuple[bool, str]:
        """Check if any signal vetoes the trade."""
        if not self.config.enable_veto:
            return False, ""

        # WHALE veto: opposite direction
        whale = signals.get('whale', {})
        base_dir = _normalize_direction(direction)
        whale_dir = _normalize_direction(whale.get('direction', 'NEUTRAL'))
        whale_strength = _safe_float(whale.get('strength', 0), 0.0)
        if whale_strength <= 0:
            whale_strength = _safe_float(whale.get('z_score', 0), 0.0) / max(1e-9, _safe_float(BRAIN_STRENGTH_Z_MULT, 2.0))

        if whale_dir != 'NEUTRAL' and whale_dir != base_dir:
            if whale_strength >= self.config.whale_veto_threshold:
                return True, f"WHALE opposite ({whale_dir}, strength={whale_strength:.1f})"

        # CVD veto: opposite direction with strong z-score
        cvd = signals.get('cvd', {})
        cvd_dir = _normalize_direction(cvd.get('direction', 'NEUTRAL'))
        cvd_z = abs(_safe_float(cvd.get('z_score', 0), 0.0))
        if cvd_z <= 0:
            cvd_z = max(
                abs(_safe_float(cvd.get('z_smart', 0), 0.0)),
                abs(_safe_float(cvd.get('z_dumb', 0), 0.0)),
            )

        if cvd_dir != 'NEUTRAL' and cvd_dir != base_dir:
            if cvd_z >= self.config.cvd_veto_z_threshold:
                return True, f"CVD opposite ({cvd_dir}, z={cvd_z:.1f})"

        return False, ""

    def _has_primary_signal_trigger(
        self,
        signals: Dict[str, Dict],
        direction: str,
        min_z: Optional[float] = None,
    ) -> bool:
        """
        Check if at least one primary signal supports the direction at z >= min_z.

        Primary signals: DEAD_CAPITAL, WHALE (can trigger independently)
        Secondary signals: CVD, OFM, FADE (require primary co-trigger)
        """
        if min_z is None:
            min_z = _safe_float(BRAIN_PRIMARY_MIN_Z, 2.0)
        base_dir = _normalize_direction(direction)

        for sig_name in self.config.primary_signals:
            sig_data = signals.get(sig_name, {})
            sig_dir = _normalize_direction(sig_data.get('direction', 'NEUTRAL'))

            if sig_dir != base_dir:
                continue

            # Get z-score
            z_score = _safe_float(sig_data.get('z_score', 0), 0.0)
            if sig_name == 'cvd':
                z_smart = abs(_safe_float(sig_data.get('z_smart', 0), 0.0))
                z_dumb = abs(_safe_float(sig_data.get('z_dumb', 0), 0.0))
                z_score = max(abs(z_score), z_smart, z_dumb)
            elif sig_name in ('whale', 'dead_capital'):
                # WHALE/DEAD_CAPITAL may come from legacy strength or new watcher z_score.
                if abs(z_score) <= 0:
                    strength = _safe_float(sig_data.get('strength', 0), 0.0)
                    z_score = strength * _safe_float(BRAIN_STRENGTH_Z_MULT, 2.0) if strength else 0.0

            if abs(z_score) >= min_z:
                return True

        return False

    def _calculate_conviction(
        self,
        signals: Dict[str, Dict],
        metrics: Dict,
        direction: str
    ) -> float:
        """
        Calculate conviction score from signals.

        Conviction = weighted sum of aligned signal strengths.
        Only considers weights for signals that are actually present.
        """
        total_weight = 0.0
        weighted_strength = 0.0
        base_dir = _normalize_direction(direction)

        for sig_name, weight in self.config.conviction_weights.items():
            sig_data = signals.get(sig_name, {})

            # Only count weight for signals that are present in opportunity
            if not sig_data:
                continue

            sig_dir = _normalize_direction(sig_data.get('direction', 'NEUTRAL'))

            # Get signal strength (z-score normalized to 0-1)
            z = sig_data.get('z_score')
            if z is None and sig_name == 'cvd':
                z_smart = abs(_safe_float(sig_data.get('z_smart', 0), 0.0))
                z_dumb = abs(_safe_float(sig_data.get('z_dumb', 0), 0.0))
                z = max(abs(_safe_float(z, 0.0)), z_smart, z_dumb)
            if (z is None or not z) and sig_name in ('whale', 'dead_capital'):
                strength = _safe_float(sig_data.get('strength', 0), 0.0)
                z = strength * _safe_float(BRAIN_STRENGTH_Z_MULT, 2.0) if strength else 0.0
            try:
                z_val = float(z or 0.0)
            except Exception:
                z_val = 0.0
            weight_f = _safe_float(weight, 0.0)
            strength = min(1.0, abs(z_val) / max(1e-9, _safe_float(BRAIN_CONVICTION_Z_DENOM, 3.0)))  # z=denom -> strength=1.0

            # Only add weight for present signals to avoid under-normalization
            total_weight += weight_f

            if sig_dir == base_dir:
                weighted_strength += weight_f * strength
            elif sig_dir != 'NEUTRAL':
                weighted_strength -= weight_f * strength * 0.5  # Penalty for opposite

        if total_weight == 0:
            return 0.0

        # Normalize by present signal weights only
        conviction = weighted_strength / total_weight

        # Boost if many signals agree, but never flip a net-negative base score
        # into positive conviction purely via count bonus.
        if conviction > 0:
            agree_count = sum(1 for s in signals.values() if _normalize_direction(s.get('direction')) == base_dir)
            if agree_count >= _safe_int(BRAIN_AGREE_BONUS_COUNT_1, 4):
                conviction += BRAIN_AGREE_BONUS_1
            if agree_count >= _safe_int(BRAIN_AGREE_BONUS_COUNT_2, 5):
                conviction += BRAIN_AGREE_BONUS_2

        return max(0.0, min(1.0, conviction))

    def _smart_money_alignment(self, metrics: Dict, direction: str) -> float:
        """
        Calculate smart money alignment score (-1 to +1).

        Positive = smart money agrees with direction.
        Negative = smart money disagrees.
        """
        score = 0.0

        # Smart CVD vs Dumb CVD divergence
        smart_cvd = _safe_float(metrics.get('smart_cvd', 0), 0.0)
        dumb_cvd = _safe_float(metrics.get('dumb_cvd', 0), 0.0)
        div_z = _safe_float(metrics.get('divergence_z', 0), 0.0)

        # If smart is buying and we're going long, positive alignment
        if direction == 'LONG':
            if smart_cvd > 0 and div_z > 0:
                score += min(1.0, div_z / BRAIN_SMART_DIV_Z_DENOM) * BRAIN_SMART_DIV_Z_MULT
            elif smart_cvd < 0:
                score -= min(1.0, abs(div_z) / BRAIN_SMART_DIV_Z_DENOM) * BRAIN_SMART_DIV_Z_MULT
        else:  # SHORT
            if smart_cvd < 0 and div_z < 0:
                score += min(1.0, abs(div_z) / BRAIN_SMART_DIV_Z_DENOM) * BRAIN_SMART_DIV_Z_MULT
            elif smart_cvd > 0:
                score -= min(1.0, div_z / BRAIN_SMART_DIV_Z_DENOM) * BRAIN_SMART_DIV_Z_MULT

        # Cohort delta
        cohort_signal = metrics.get('cohort_signal', 'N/A')
        smart_vs_dumb = _safe_float(metrics.get('smart_vs_dumb_delta', 0), 0.0)

        if direction == 'LONG' and cohort_signal == 'SMART_ACCUMULATING':
            score += BRAIN_COHORT_ALIGN_BONUS
        elif direction == 'SHORT' and cohort_signal == 'SMART_DISTRIBUTING':
            score += BRAIN_COHORT_ALIGN_BONUS
        elif cohort_signal in ('SMART_ACCUMULATING', 'SMART_DISTRIBUTING'):
            score -= BRAIN_COHORT_COUNTER_PENALTY  # Smart money going other way

        return max(-1.0, min(1.0, score))

    def _liquidation_edge(self, metrics: Dict, direction: str) -> float:
        """
        Calculate liquidation hunting edge (0 to 1).

        Higher = more liquidation fodder in our direction.
        """
        edge = 0.0

        # Fragile wallets count
        fragile_count = _safe_int(metrics.get('fragile_count', 0), 0)
        fragile_notional = _safe_float(metrics.get('fragile_notional', 0), 0.0)

        if fragile_count > BRAIN_LIQ_FRAGILE_COUNT_1 and fragile_notional > BRAIN_LIQ_FRAGILE_NOTIONAL_1:
            edge += BRAIN_LIQ_FRAGILE_BONUS_1
        if fragile_count > BRAIN_LIQ_FRAGILE_COUNT_2 and fragile_notional > BRAIN_LIQ_FRAGILE_NOTIONAL_2:
            edge += BRAIN_LIQ_FRAGILE_BONUS_2

        # Hot zone (positions near liquidation)
        hot_pct = _safe_float(metrics.get('hot_zone_pct', 0), 0.0)
        if hot_pct > BRAIN_LIQ_HOT_PCT_THRESHOLD:
            edge += BRAIN_LIQ_HOT_PCT_BONUS

        # Asymmetric pain (one side hurting more)
        long_pain = abs(_safe_float(metrics.get('long_pain', 0), 0.0))
        short_pain = abs(_safe_float(metrics.get('short_pain', 0), 0.0))

        if direction == 'LONG' and short_pain > long_pain * BRAIN_LIQ_PAIN_RATIO:
            edge += BRAIN_LIQ_PAIN_BONUS  # Shorts are suffering, squeeze potential
        elif direction == 'SHORT' and long_pain > short_pain * BRAIN_LIQ_PAIN_RATIO:
            edge += BRAIN_LIQ_PAIN_BONUS  # Longs are suffering, dump potential

        # Suggested direction from liquidation analysis
        liq_dir = metrics.get('liq_suggested_direction', 'N/A')
        if liq_dir == direction:
            edge += BRAIN_LIQ_DIR_BONUS

        return min(1.0, edge)

    def _volatility_opportunity(self, atr_pct: float) -> str:
        """
        Determine volatility suitability.

        Returns: 'SCALP', 'SWING', or 'AVOID'
        """
        atr_pct = _safe_float(atr_pct, 0.0)
        if atr_pct <= 0:
            return 'AVOID'

        if atr_pct < BRAIN_ATR_AVOID_MAX:
            return 'AVOID'  # Too quiet
        elif atr_pct < BRAIN_ATR_SCALP_MAX:
            return 'SCALP'  # Good for quick trades
        elif atr_pct < BRAIN_ATR_SWING_MAX:
            return 'SWING'  # Ideal for holding
        elif atr_pct < BRAIN_ATR_VOLATILE_MAX:
            return 'SCALP'  # Volatile, quick in/out
        else:
            return 'AVOID'  # Too wild

    def _calculate_size_multiplier(
        self,
        conviction: float,
        smart_money: float,
        liq_edge: float,
        vol_fit: str
    ) -> float:
        """
        Calculate position size multiplier (0.5 to 2.0).

        Higher conviction + favorable factors = larger size.
        """
        # Base from conviction
        if conviction < BRAIN_SIZE_T1:
            return BRAIN_SIZE_M1
        elif conviction < BRAIN_SIZE_T2:
            mult = BRAIN_SIZE_M2
        elif conviction < BRAIN_SIZE_T3:
            mult = BRAIN_SIZE_M3
        elif conviction < BRAIN_SIZE_T4:
            mult = BRAIN_SIZE_M4
        else:
            mult = BRAIN_SIZE_M5

        # DEGEN mode boost for high conviction
        if self.config.degen_mode and conviction >= self.config.degen_multiplier_min:
            mult *= self.config.degen_multiplier

        # Smart money alignment boost
        if smart_money > BRAIN_SIZE_SMART_POS_THRESHOLD:
            mult *= BRAIN_SIZE_SMART_POS_MULT
        elif smart_money < BRAIN_SIZE_SMART_NEG_THRESHOLD:
            mult *= BRAIN_SIZE_SMART_NEG_MULT

        # Liquidation edge boost
        if liq_edge > BRAIN_SIZE_LIQ_EDGE_THRESHOLD:
            mult *= BRAIN_SIZE_LIQ_EDGE_MULT

        # Volatility adjustment
        if vol_fit == 'AVOID':
            mult *= BRAIN_SIZE_VOL_AVOID_MULT
        elif vol_fit == 'SCALP':
            mult *= BRAIN_SIZE_VOL_SCALP_MULT  # Slightly smaller for quick trades

        # Clamp to reasonable range
        return max(0.5, min(2.0, mult))

    def _build_reasoning(
        self,
        symbol: str,
        direction: str,
        conviction: float,
        smart_money: float,
        liq_edge: float,
        signals_for: List[str],
        signals_against: List[str],
        vol_fit: str
    ) -> str:
        """Build human-readable reasoning for the decision."""
        parts = []

        if direction == 'NO_TRADE':
            parts.append(f"{symbol}: PASS (conviction={conviction:.2f})")
            if signals_against:
                parts.append(f"Opposing: {', '.join(signals_against)}")
            return " | ".join(parts)

        # Direction and conviction
        parts.append(f"{symbol} {direction} (conviction={conviction:.2f})")

        # Signal support
        parts.append(f"Support: {', '.join(signals_for) or 'none'}")

        # Smart money
        if smart_money > 0.3:
            parts.append("Smart money aligned")
        elif smart_money < -0.3:
            parts.append("Smart money divergent")

        # Liquidation edge
        if liq_edge > 0.5:
            parts.append(f"Liq edge={liq_edge:.2f}")

        # Volatility
        parts.append(f"Vol: {vol_fit}")

        return " | ".join(parts)

# =============================================================================
# Module exports
# =============================================================================

__all__ = [
    'BrainConfig',
    'BrainDecision',
    'TradingBrain',
]
