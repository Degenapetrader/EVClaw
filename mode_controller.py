#!/usr/bin/env python3
"""
Unified mode controller for HL-Trader.

Provides mode presets, slider-based tuning, and legacy env overrides.

Modes:
- conservative: tighter risk and higher thresholds (fewer trades).
- balanced: current production defaults (no behavior change).
- aggressive: looser filters and larger risk (more trades).
"""

from __future__ import annotations

from copy import deepcopy
from typing import Any, Dict, Optional, Tuple
from pathlib import Path

import yaml

from config_env import apply_env_overrides

from env_utils import (
    env_bool,
    env_float,
    env_int,
    env_json,
    env_list,
    env_str,
    env_present,
)
from constants import (
    BASE_CONFIDENCE_THRESHOLD,
    DEFAULT_ADAPTIVE_ENTRY_BOOST_THRESHOLD,
    DEFAULT_ADAPTIVE_SIZING_INCREASE_THRESHOLD,
    DEFAULT_BRAIN_SIZE_T1,
    DEFAULT_RISK_CONVICTION_BASE,
)




# ------------------------------
# skill.yaml config bridge (YAML-first for specific params)
# ------------------------------

_SKILL_CFG: Optional[Dict[str, Any]] = None


def _load_skill_cfg() -> Dict[str, Any]:
    global _SKILL_CFG
    if _SKILL_CFG is not None:
        return _SKILL_CFG
    skill_dir = Path(__file__).parent
    cfg: Dict[str, Any] = {}
    try:
        path = skill_dir / "skill.yaml"
        if path.exists():
            cfg = yaml.safe_load(path.read_text()) or {}
    except Exception:
        cfg = {}
    try:
        cfg = apply_env_overrides(cfg)
    except Exception:
        # If config_env fails for any reason, continue with raw cfg.
        pass
    _SKILL_CFG = cfg
    return cfg


def _skill_get(path: Tuple[str, ...]) -> Any:
    cur: Any = _load_skill_cfg()
    for key in path:
        if not isinstance(cur, dict) or key not in cur:
            return None
        cur = cur[key]
    return cur

def _cfg_int(path, default: int) -> int:
    v = _skill_get(path)
    try:
        return int(v) if v is not None else int(default)
    except Exception:
        return int(default)

def _cfg_str(path, default: str) -> str:
    v = _skill_get(path)
    return str(v) if v is not None else str(default)

# Modes: conservative (=Safe), balanced, aggressive
# YAML-first; tuning mode is sourced from skill.yaml.
_yaml_tuning_mode = _cfg_str(('config','mode_controller','mode'), '').strip().lower()
EVCLAW_MODE = (_yaml_tuning_mode or 'balanced').strip().lower()
ALLOW_LEGACY_PARAM_ENV = False
EVCLAW_RISK_APPETITE = _cfg_int(('config','mode_controller','sliders','risk_appetite'), 50)
EVCLAW_TRADE_FREQUENCY = _cfg_int(('config','mode_controller','sliders','trade_frequency'), 50)
EVCLAW_AGGRESSION = _cfg_int(
    ('config','mode_controller','sliders','aggression'),
    env_int('EVCLAW_AGGRESSION', 50),
)
EVCLAW_SIGNAL_WEIGHTING = _cfg_int(('config','mode_controller','sliders','signal_weighting'), 50)
EVCLAW_LEARNING_SPEED = _cfg_int(('config','mode_controller','sliders','learning_speed'), 50)


# ------------------------------
# Presets (balanced = legacy defaults)
# ------------------------------

BALANCED_PRESET: Dict[str, Dict[str, Any]] = {
    'adaptive_entry': {
        'boost_threshold': DEFAULT_ADAPTIVE_ENTRY_BOOST_THRESHOLD,
        'conviction_mult_max': 1.5,
        'conviction_mult_min': 0.5,
        'delay_max': 300,
        'delay_min': 0,
        'ema_alpha': 0.1,
        'min_samples': 10,
        'reduce_threshold': 0.4,
    },
    'adaptive_sizing': {
        'conviction_buckets': {
            'HIGH': (0.7, 0.85),
            'LOW': (0.0, 0.5),
            'MEDIUM': (0.5, 0.7),
            'VERY_HIGH': (0.85, 1.0),
        },
        'decrease_threshold': 0.4,
        'ema_alpha': 0.1,
        'increase_threshold': DEFAULT_ADAPTIVE_SIZING_INCREASE_THRESHOLD,
        'kelly_fraction': 0.25,
        'max_kelly_bet': 2.5,
        'min_samples': 10,
        'risk_mult_max': 1.5,
        'risk_mult_min': 0.5,
        'use_kelly': True,
    },
    'adaptive_sltp': {
        'default_sl_mult': 2.0,
        'default_tp_mult': 3.0,
        'ema_alpha': 0.1,
        'max_rr_ratio': 2.5,
        'min_rr_ratio': 1.0,
        'min_samples': 10,
        'sl_range_max': 3.0,
        'sl_range_min': 1.0,
        'tighten_tp_threshold': 0.7,
        'tp_range_max': 5.0,
        'tp_range_min': 1.5,
        'widen_sl_threshold': 0.6,
    },
    'brain': {
        'agree_bonus_1': 0.1,
        'agree_bonus_2': 0.05,
        'agree_bonus_count_1': 4,
        'agree_bonus_count_2': 5,
        'apply_history_adjustments': True,
        'atr_avoid_max': 0.5,
        'atr_scalp_max': 1.5,
        'atr_swing_max': 3.0,
        'atr_volatile_max': 5.0,
        'base_confidence_threshold': BASE_CONFIDENCE_THRESHOLD,
        'blend_pipeline': 0.20,
        'cohort_align_bonus': 0.3,
        'cohort_counter_penalty': 0.2,
        'conviction_threshold': 0.70,
        'conviction_weights': {
            'cvd': 0.10,
            'dead_capital': 0.35,
            'fade': 0.07,
            'ofm': 0.22,
            'hip3_main': 0.65,
            'whale': 0.26,
        },
        'conviction_z_denom': 3.0,
        'cvd_veto_z_threshold': 2.5,
        'dead_floor_enabled': False,
        'degen_mode': True,
        'degen_multiplier': 1.5,
        'degen_multiplier_min': 0.75,
        'enable_veto': True,
        'fade_extremes': True,
        'high_conviction_threshold': 0.75,
        'history_block_threshold': 0.3,
        'hunt_liquidations': True,
        'liq_dir_bonus': 0.1,
        'liq_edge_bonus': 0.05,
        'liq_edge_threshold': 0.5,
        'liq_fragile_bonus_1': 0.3,
        'liq_fragile_bonus_2': 0.2,
        'liq_fragile_count_1': 10,
        'liq_fragile_count_2': 30,
        'liq_fragile_notional_1': 500000,
        'liq_fragile_notional_2': 2000000,
        'liq_hot_pct_bonus': 0.2,
        'liq_hot_pct_threshold': 10.0,
        'liq_pain_bonus': 0.2,
        'liq_pain_ratio': 1.5,
        'max_adjustment_factor': 0.5,
        'pattern_penalty': 0.7,
        'primary_min_z': 2.0,
        'primary_signals': ['dead_capital', 'whale', 'hip3_main'],
        'respect_momentum': True,
        'secondary_signals': ['cvd', 'ofm', 'fade'],
        'size_liq_edge_mult': 1.1,
        'size_liq_edge_threshold': 0.5,
        'size_m1': 0.5,
        'size_m2': 0.6,
        'size_m3': 0.8,
        'size_m4': 1.0,
        'size_m5': 1.2,
        'size_smart_neg_mult': 0.9,
        'size_smart_neg_threshold': -0.3,
        'size_smart_pos_mult': 1.1,
        'size_smart_pos_threshold': 0.5,
        'size_t1': DEFAULT_BRAIN_SIZE_T1,
        'size_t2': 0.65,
        'size_t3': 0.75,
        'size_t4': 0.85,
        'size_vol_avoid_mult': 0.5,
        'size_vol_scalp_mult': 0.9,
        'smart_adjust_mult': 0.1,
        'smart_div_z_denom': 3.0,
        'smart_div_z_mult': 0.5,
        'strength_z_mult': 2.0,
        'strong_dead_min_conviction': 0.9,
        'strong_other_bonus': 0.1,
        'strong_whale_bonus': 0.2,
        'strong_z_threshold': 1.5,
        'vol_avoid_penalty': 0.15,
        'whale_veto_threshold': 1.0,
    },
    'executor': {
        'chase_poll_interval': 5.0,
        'chase_retrigger_ticks': 2,
        'chase_tick_offset': 2,
        'chase_timeout': 300.0,
        'default_base_position_size_usd': 500.0,
        'default_dust_notional_usd': 10.0,
        'default_max_position_per_symbol_usd': 0.0,
        'default_max_total_exposure_usd': 100000000.0,
        'default_rate_limit_tokens': 40,
        'default_rate_limit_window_seconds': 60,
        'default_sl_atr_multiplier': 2.0,
        'default_sl_fallback_pct': 2.0,
        'default_sltp_delay_seconds': 1.5,
        'default_tp_atr_multiplier': 3.0,
        'dust_mismatch_pct': 100.0,
        'sltp_max_retries': 2,
        'sltp_retry_base_delay_seconds': 1.0,
        'sltp_retry_max_delay_seconds': 10.0,
    },
    'learning_engine': {
        'adjustment_decay_days': 7,
        'avoid_duration_hours': 24,
        'avoid_min_trades': 5,
        'avoid_win_rate_threshold': 0.3,
        'min_trades_for_adjustment': 3,
    },
    'risk': {
        'base_risk_pct': 1.0,
        'conviction_base': DEFAULT_RISK_CONVICTION_BASE,
        'conviction_range': 0.45,
        'daily_drawdown_limit_pct': 5.0,
        'daily_pnl_mult_1': 0.25,
        'daily_pnl_mult_2': 0.5,
        'daily_pnl_mult_3': 0.75,
        'daily_pnl_thresh_1': 0.04,
        'daily_pnl_thresh_2': 0.03,
        'daily_pnl_thresh_3': 0.02,
        'emergency_loss_pct': 10.0,
        'emergency_portfolio_loss_pct': 15.0,
        'equity_floor_pct': 80.0,
        'fallback_atr_pct': 2.0,
        'loss_streak_min_mult': 0.5,
        'loss_streak_reduction_per': 0.2,
        'loss_streak_start': 3,
        'max_concurrent_positions': 100,
        'max_position_pct': 0.25,
        'max_risk_pct': 2.5,
        'max_sector_concentration': 3,
        'min_hold_hours': 2.0,
        'min_risk_pct': 0.5,
        'no_hard_stops': False,
        'position_sl_mult': 2.0,
        'sector_reduction_2': 0.8,
        'sector_reduction_3': 0.7,
        'soft_stop_alert': True,
        'soft_stop_atr_mult': 2.0,
        'stale_signal_max_minutes': 5.0,
        'win_streak_boost_per': 0.05,
        'win_streak_max_mult': 1.2,
        'win_streak_start': 3,
    },
    'safety': {
        'max_drawdown_pct': 25.0,
        'recovery_wins_tier_2': 2,
        'recovery_wins_tier_3': 1,
        'tier1_alert_on_trade': False,
        'tier1_allow_new_trades': True,
        'tier1_max_size_multiplier': 1.0,
        'tier1_min_signals_agreeing': 3,
        'tier1_min_z_score': 2.0,
        'tier2_alert_on_trade': True,
        'tier2_allow_new_trades': True,
        'tier2_max_size_multiplier': 0.5,
        'tier2_min_signals_agreeing': 4,
        'tier2_min_z_score': 2.0,
        'tier3_alert_on_trade': True,
        'tier3_allow_new_trades': True,
        'tier3_max_size_multiplier': 0.25,
        'tier3_min_signals_agreeing': 5,
        'tier3_min_z_score': 2.5,
        'tier4_alert_on_trade': True,
        'tier4_allow_new_trades': False,
        'tier4_max_size_multiplier': 0.0,
        'tier4_min_signals_agreeing': 999,
        'tier4_min_z_score': 999.0,
        'tier5_alert_on_trade': True,
        'tier5_allow_new_trades': False,
        'tier5_max_size_multiplier': 0.0,
        'tier5_min_signals_agreeing': 999,
        'tier5_min_z_score': 999.0,
        'tier_2_loss_pct': 5.0,
        'tier_3_loss_pct': 10.0,
        'tier_4_loss_pct': 20.0,
    },
    'symbol_profiler': {
        'default_base_position_size_usd': 500.0,
        'default_cache_ttl_seconds': 900,
        'known_large_caps': ['SOL', 'XRP', 'BNB', 'AVAX', 'LINK', 'DOT', 'MATIC', 'UNI', 'ATOM'],
        'known_majors': ['BTC', 'ETH'],
        'oi_large_cap': 100000000,
        'oi_major': 500000000,
        'oi_mid_cap': 20000000,
        'oi_small_cap': 5000000,
        'risk_multipliers': {
            'DEGEN': 0.3,
            'LARGE_CAP': 0.9,
            'MAJOR': 1.0,
            'MID_CAP': 0.7,
            'SMALL_CAP': 0.5,
        },
        'vol_high_max': 3.0,
        'vol_low_max': 0.8,
        'vol_medium_max': 1.5,
        'vol_stable_max': 0.5,
        'vol_tier_1': 2000000000,
        'vol_tier_2': 500000000,
        'vol_tier_3': 100000000,
        'vol_tier_4': 20000000,
        'vol_tier_5': 5000000,
    },
    'symbol_rr_learning': {
        'binance_oi_weight': 0.75,
        'category_defaults': {
            'LARGE_CAP': {'sl': 1.75, 'tp': 2.75},
            'MAJOR': {'sl': 1.5, 'tp': 2.5},
            'MICRO_CAP': {'sl': 2.5, 'tp': 4.0},
            'MID_CAP': {'sl': 2.0, 'tp': 3.0},
            'SMALL_CAP': {'sl': 2.25, 'tp': 3.5},
        },
        'default_sl_mult': 2.0,
        'default_tp_mult': 3.0,
        'hl_oi_weight': 0.25,
        'lookback_days': 60,
        'min_samples_for_confidence': 50,
        'min_samples_for_symbol_policy': 15,
        'oi_cache_ttl': 3600,
        'oi_large_cap': 100000000,
        'oi_major': 500000000,
        'oi_mid_cap': 20000000,
        'oi_small_cap': 5000000,
        'prior_strength': 10,
        'sl_max': 4.0,
        'sl_min': 1.2,
        'tp_max': 6.0,
        'tp_min': 1.5,
    },
}


LEGACY_ENV_MAP: Dict[str, Dict[str, Tuple[Tuple[str, str], ...]]] = {}

_WARNED_IGNORED_LEGACY_ENVS = False


def _warn_ignored_legacy_envs_once() -> None:
    global _WARNED_IGNORED_LEGACY_ENVS
    if _WARNED_IGNORED_LEGACY_ENVS or ALLOW_LEGACY_PARAM_ENV:
        return

    present = sorted(
        {
            env_name
            for module_map in LEGACY_ENV_MAP.values()
            for entries in module_map.values()
            for env_name, _parser in entries
            if env_present(env_name)
        }
    )
    if not present:
        return

    preview = ", ".join(present[:12])
    extra = len(present) - 12
    if extra > 0:
        preview = f"{preview}, +{extra} more"
    print(
        "Config warning: ignoring non-YAML parameter env overrides "
        "(YAML-first mode). "
        f"Ignored keys: {preview}"
    )
    _WARNED_IGNORED_LEGACY_ENVS = True


_warn_ignored_legacy_envs_once()


PARSERS = {
    'env_bool': env_bool,
    'env_float': env_float,
    'env_int': env_int,
    'env_json': env_json,
    'env_list': env_list,
    'env_str': env_str,
}

SLIDER_RISK = 'risk_appetite'
SLIDER_FREQ = 'trade_frequency'
SLIDER_AGGR = 'aggression'
SLIDER_WEIGHT = 'signal_weighting'
SLIDER_LEARN = 'learning_speed'

# Map params to sliders (inverse=True for thresholds where higher slider lowers value)
PARAM_SLIDERS: Dict[str, Dict[str, Tuple[str, bool]]] = {
    'brain': {
        'primary_min_z': (SLIDER_FREQ, True),
        'base_confidence_threshold': (SLIDER_FREQ, True),
        'strong_z_threshold': (SLIDER_FREQ, True),
        'size_t1': (SLIDER_RISK, False),
        'size_t2': (SLIDER_RISK, False),
        'size_t3': (SLIDER_RISK, False),
        'size_t4': (SLIDER_RISK, False),
        'size_m1': (SLIDER_RISK, False),
        'size_m2': (SLIDER_RISK, False),
        'size_m3': (SLIDER_RISK, False),
        'size_m4': (SLIDER_RISK, False),
        'size_m5': (SLIDER_RISK, False),
        'size_smart_pos_mult': (SLIDER_RISK, False),
        'size_smart_neg_mult': (SLIDER_RISK, False),
        'size_liq_edge_mult': (SLIDER_RISK, False),
        'size_vol_avoid_mult': (SLIDER_RISK, False),
        'size_vol_scalp_mult': (SLIDER_RISK, False),
        'degen_multiplier': (SLIDER_RISK, False),
    },
    'risk': {
        'min_risk_pct': (SLIDER_RISK, False),
        'max_risk_pct': (SLIDER_RISK, False),
        'base_risk_pct': (SLIDER_RISK, False),
        'max_position_pct': (SLIDER_RISK, False),
        'max_concurrent_positions': (SLIDER_RISK, False),
        'position_sl_mult': (SLIDER_AGGR, False),
        'soft_stop_atr_mult': (SLIDER_AGGR, False),
        'min_hold_hours': (SLIDER_AGGR, False),
    },
    'executor': {
        'default_base_position_size_usd': (SLIDER_RISK, False),
        'default_max_position_per_symbol_usd': (SLIDER_RISK, False),
        'default_max_total_exposure_usd': (SLIDER_RISK, False),
        'default_sl_atr_multiplier': (SLIDER_AGGR, False),
        'default_tp_atr_multiplier': (SLIDER_AGGR, False),
        'default_sl_fallback_pct': (SLIDER_AGGR, False),
    },
    'adaptive_sltp': {
        'default_sl_mult': (SLIDER_AGGR, False),
        'default_tp_mult': (SLIDER_AGGR, False),
        'sl_range_min': (SLIDER_AGGR, False),
        'sl_range_max': (SLIDER_AGGR, False),
        'tp_range_min': (SLIDER_AGGR, False),
        'tp_range_max': (SLIDER_AGGR, False),
        'max_rr_ratio': (SLIDER_AGGR, False),
    },
    'adaptive_sizing': {
        'risk_mult_min': (SLIDER_RISK, False),
        'risk_mult_max': (SLIDER_RISK, False),
        'kelly_fraction': (SLIDER_RISK, False),
        'max_kelly_bet': (SLIDER_RISK, False),
    },
    'safety': {
        'tier1_min_z_score': (SLIDER_FREQ, True),
        'tier2_min_z_score': (SLIDER_FREQ, True),
        'tier3_min_z_score': (SLIDER_FREQ, True),
        'tier1_min_signals_agreeing': (SLIDER_FREQ, True),
        'tier2_min_signals_agreeing': (SLIDER_FREQ, True),
        'tier3_min_signals_agreeing': (SLIDER_FREQ, True),
        'tier1_max_size_multiplier': (SLIDER_RISK, False),
        'tier2_max_size_multiplier': (SLIDER_RISK, False),
        'tier3_max_size_multiplier': (SLIDER_RISK, False),
    },
    'symbol_rr_learning': {
        'default_sl_mult': (SLIDER_AGGR, False),
        'default_tp_mult': (SLIDER_AGGR, False),
        'sl_min': (SLIDER_AGGR, False),
        'sl_max': (SLIDER_AGGR, False),
        'tp_min': (SLIDER_AGGR, False),
        'tp_max': (SLIDER_AGGR, False),
    },
    'symbol_profiler': {
        'default_base_position_size_usd': (SLIDER_RISK, False),
    },
}

MODE_MULTIPLIERS = {
    'conservative': 0.5,
    'balanced': 1.0,
    'aggressive': 2.0,
}

# Params that should remain fixed across modes/sliders (unless explicitly overridden via env).
NO_SCALE_PARAMS = {
    ("executor", "default_sl_atr_multiplier"),
    ("executor", "default_tp_atr_multiplier"),
}

CONVICTION_CLAMP_PARAMS = {
    ("brain", "base_confidence_threshold"),
    ("brain", "conviction_threshold"),
    ("brain", "high_conviction_threshold"),
    ("brain", "strong_dead_min_conviction"),
    ("brain", "size_t1"),
    ("brain", "size_t2"),
    ("brain", "size_t3"),
    ("brain", "size_t4"),
}

CONVICTION_AGGRESSIVE_CAP_PARAMS = {
    ("brain", "base_confidence_threshold"),
    ("brain", "conviction_threshold"),
    ("brain", "high_conviction_threshold"),
}


def _normalize_mode(mode: str) -> str:
    if not mode:
        return 'balanced'
    mode = mode.strip().lower()
    return mode if mode in ('conservative', 'balanced', 'aggressive') else 'balanced'


def _clamp_slider(value: int) -> int:
    return max(0, min(100, int(value)))


def _slider_factor(value: int) -> float:
    return 0.5 + (_clamp_slider(value) / 100.0)


def _scale_value(value: Any, factor: float, inverse: bool = False) -> Any:
    if isinstance(value, bool):
        return value
    if isinstance(value, dict):
        return {k: _scale_value(v, factor, inverse) for k, v in value.items()}
    if isinstance(value, list):
        return [_scale_value(v, factor, inverse) for v in value]
    if isinstance(value, tuple):
        return tuple(_scale_value(v, factor, inverse) for v in value)
    if not isinstance(value, (int, float)):
        return value
    if inverse and factor:
        factor = 1.0 / factor
    new_val = float(value) * factor
    if isinstance(value, int) and not isinstance(value, bool):
        return max(0, int(round(new_val)))
    return float(new_val)


def _choose_slider(module: str, param: str) -> Tuple[str, bool]:
    explicit = PARAM_SLIDERS.get(module, {}).get(param)
    if explicit:
        return explicit

    name = f"{module}.{param}".lower()
    learn_modules = {
        "learning_engine",
        "symbol_rr_learning",
        "adaptive_entry",
        "adaptive_sizing",
        "adaptive_sltp",
    }
    if module in learn_modules:
        return SLIDER_LEARN, False
    if any(k in name for k in ("learning", "learn", "decay", "lookback", "sample", "ema", "alpha", "kelly", "prior")):
        return SLIDER_LEARN, False
    if any(k in name for k in ("risk", "position", "exposure", "size", "notional", "equity", "drawdown", "loss")):
        return SLIDER_RISK, False
    if any(k in name for k in ("sl", "tp", "atr", "hold", "sltp", "stop")):
        return SLIDER_AGGR, False
    if any(k in name for k in ("weight", "mult", "bonus", "score", "bias", "normalize", "boost", "cohort", "delta")):
        return SLIDER_WEIGHT, False
    if any(k in name for k in ("min", "max", "threshold", "z", "conviction", "signal", "ratio", "pct", "percent", "window", "limit", "cap")):
        return SLIDER_FREQ, True
    return SLIDER_WEIGHT, False


def _apply_mode_multiplier(base: Dict[str, Dict[str, Any]], factor: float) -> Dict[str, Dict[str, Any]]:
    data = deepcopy(base)
    for module, params in data.items():
        for param, value in params.items():
            params[param] = _scale_value(value, factor, False)
    return data


MODE_PRESETS: Dict[str, Dict[str, Dict[str, Any]]] = {
    'balanced': BALANCED_PRESET,
    'conservative': _apply_mode_multiplier(BALANCED_PRESET, MODE_MULTIPLIERS['conservative']),
    'aggressive': _apply_mode_multiplier(BALANCED_PRESET, MODE_MULTIPLIERS['aggressive']),
}

if EVCLAW_MODE not in MODE_PRESETS:
    EVCLAW_MODE = 'balanced'


def _maybe_copy(value: Any) -> Any:
    if isinstance(value, (dict, list)):
        return deepcopy(value)
    return value


def _parse_legacy(env_name: str, parser_name: str, default: Any) -> Any:
    parser = PARSERS[parser_name]
    if parser_name == 'env_list':
        return parser(env_name, list(default) if isinstance(default, (list, tuple)) else [])
    if parser_name == 'env_json':
        return parser(env_name, default)
    return parser(env_name, default)


def compute_param(
    module: str,
    param: str,
    *,
    mode: Optional[str] = None,
    risk_appetite: Optional[int] = None,
    trade_frequency: Optional[int] = None,
    aggression: Optional[int] = None,
    signal_weighting: Optional[int] = None,
    learning_speed: Optional[int] = None,
    allow_legacy_env: bool = True,
) -> Any:
    use_mode = _normalize_mode(mode or EVCLAW_MODE)
    if module not in MODE_PRESETS[use_mode]:
        raise KeyError(f"Unknown module '{module}'")
    if param not in MODE_PRESETS[use_mode][module]:
        raise KeyError(f"Unknown param '{module}.{param}'")

    base = _maybe_copy(MODE_PRESETS[use_mode][module][param])

    # YAML-first overrides for specific params that are now canonical in skill.yaml.
    yaml_first_params = {
        ("executor", "chase_timeout"),
        ("executor", "chase_poll_interval"),
        ("executor", "chase_tick_offset"),
        ("executor", "chase_retrigger_ticks"),
    }
    if (module, param) in yaml_first_params:
        yaml_val = _skill_get(("config", "executor", param))
        if yaml_val is not None:
            return yaml_val

    yaml_mode_override = _skill_get(("config", "mode_controller", "overrides", module, param))
    if yaml_mode_override is not None:
        base = _maybe_copy(yaml_mode_override)

    value: Any = None
    if allow_legacy_env:
        legacy = LEGACY_ENV_MAP.get(module, {}).get(param)
        if legacy:
            for env_name, parser_name in legacy:
                if env_present(env_name):
                    value = _maybe_copy(_parse_legacy(env_name, parser_name, base))
                    break

    if value is None:
        if (module, param) in NO_SCALE_PARAMS:
            value = _maybe_copy(BALANCED_PRESET[module][param])
        else:
            slider_name, inverse = _choose_slider(module, param)
            slider_values = {
                SLIDER_RISK: EVCLAW_RISK_APPETITE,
                SLIDER_FREQ: EVCLAW_TRADE_FREQUENCY,
                SLIDER_AGGR: EVCLAW_AGGRESSION,
                SLIDER_WEIGHT: EVCLAW_SIGNAL_WEIGHTING,
                SLIDER_LEARN: EVCLAW_LEARNING_SPEED,
            }
            if risk_appetite is not None:
                slider_values[SLIDER_RISK] = risk_appetite
            if trade_frequency is not None:
                slider_values[SLIDER_FREQ] = trade_frequency
            if aggression is not None:
                slider_values[SLIDER_AGGR] = aggression
            if signal_weighting is not None:
                slider_values[SLIDER_WEIGHT] = signal_weighting
            if learning_speed is not None:
                slider_values[SLIDER_LEARN] = learning_speed

            factor = _slider_factor(slider_values[slider_name])
            value = _scale_value(base, factor, inverse)

    if (module, param) in CONVICTION_CLAMP_PARAMS:
        cap = 1.0
        if use_mode == "aggressive" and (module, param) in CONVICTION_AGGRESSIVE_CAP_PARAMS:
            cap = 0.8
        try:
            value = max(0.0, min(cap, float(value)))
        except Exception:
            pass

    return value


def get_param(module: str, param: str) -> Any:
    """Get param with mode + slider adjustments and optional legacy env fallback."""
    return compute_param(module, param, allow_legacy_env=ALLOW_LEGACY_PARAM_ENV)
