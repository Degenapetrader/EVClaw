#!/usr/bin/env python3
"""
Context Builder V2 for Captain EVP AI Trading Agent.

Designed to maximize information density within ~100k token budget:
- Scores and selects top 10 opportunities from 210 symbols
- Includes ALL relevant data per symbol (not just perp_signals)
- Compresses data to ~3k tokens per symbol
- Maintains rolling memory across 15-minute cycles

Architecture:
1. OpportunityScorer - Ranks symbols by trading potential
2. CompactFormatter - Compresses symbol data efficiently  
3. CycleMemory - Manages rolling context between sessions
4. ContextBuilderV2 - Orchestrates full context generation
"""

import json
from logging_utils import get_logger
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple
import os
import math
import urllib.error
import urllib.request

from env_utils import (
    EVCLAW_DB_PATH,
    EVCLAW_SIGNALS_DIR,
    EVCLAW_TRACKER_HIP3_PREDATOR_URL,
    env_float,
    env_int,
    env_str,
)
from venues import parse_enabled_venues
from hip3_main import compute_hip3_main
# SQLite is the SINGLE source of truth for tracking. Do not remove DB usage.
from ai_trader_db import AITraderDB
from live_agent_utils import dual_venue_mids_ok
from universe_cache import load_dual_venue_symbols_cached

# Global context (IV term structure, exposure tracking)
try:
    from global_context import GlobalContextProvider
    _global_context_provider = GlobalContextProvider()
except ImportError:
    _global_context_provider = None



# =============================================================================
# Env-tunable constants
# =============================================================================

def _skill_slider(slider_key: str, default: int = 50) -> int:
    try:
        cfg_file = Path(__file__).with_name("skill.yaml")
        if not cfg_file.exists():
            return int(default)
        with cfg_file.open("r", encoding="utf-8") as f:
            import yaml as _yaml
            raw = _yaml.safe_load(f) or {}
        if not isinstance(raw, dict):
            return int(default)
        cfg = raw.get("config") or {}
        mode = cfg.get("mode_controller") or {}
        sliders = mode.get("sliders") or {}
        return int(sliders.get(slider_key, default))
    except Exception:
        return int(default)


def _skill_executor_max_total_exposure(default: float = 100000000.0) -> float:
    try:
        cfg_file = Path(__file__).with_name("skill.yaml")
        if not cfg_file.exists():
            return float(default)
        with cfg_file.open("r", encoding="utf-8") as f:
            import yaml as _yaml
            raw = _yaml.safe_load(f) or {}
        if not isinstance(raw, dict):
            return float(default)
        cfg = raw.get("config") or {}
        exec_cfg = cfg.get("executor") or {}
        return float(exec_cfg.get("max_total_exposure_usd", default))
    except Exception:
        return float(default)


def _skill_mode(default: str = "balanced") -> str:
    try:
        cfg_file = Path(__file__).with_name("skill.yaml")
        if not cfg_file.exists():
            return str(default)
        with cfg_file.open("r", encoding="utf-8") as f:
            import yaml as _yaml
            raw = _yaml.safe_load(f) or {}
        if not isinstance(raw, dict):
            return str(default)
        cfg = raw.get("config") or {}
        mode_cfg = cfg.get("mode_controller") or {}
        mode = str(mode_cfg.get("mode", default)).strip().lower()
        if mode in ("conservative", "balanced", "aggressive"):
            return mode
    except Exception:
        pass
    return str(default)


CB_MODE = _skill_mode("balanced")

CB_SLIDER_RISK = _skill_slider("risk_appetite", 50)
CB_SLIDER_FREQ = _skill_slider("trade_frequency", 50)
CB_SLIDER_AGGR = env_int("EVCLAW_AGGRESSION", 50)
CB_SLIDER_WEIGHT = _skill_slider("signal_weighting", 50)
CB_SLIDER_LEARN = _skill_slider("learning_speed", 50)
CB_EXECUTOR_MAX_TOTAL_EXPOSURE_USD = _skill_executor_max_total_exposure(100000000.0)

# Mode presets:
# - conservative: 0.5x balanced values.
# - balanced: current production defaults (legacy values).
# - aggressive: 2x balanced values.
CB_BALANCED_PRESET: Dict[str, Any] = {
        "CB_MIN_Z_SCORE": 1.5,
        "CB_MIN_VOLUME_24H": 1_000_000,
        "CB_STRONG_Z_SCORE": 1.5,
        "CB_SCORE_WEIGHT_SIGNAL": 0.30,
        "CB_SCORE_WEIGHT_FLOW": 0.20,
        "CB_SCORE_WEIGHT_SMART": 0.20,
        "CB_SCORE_WEIGHT_LIQ": 0.15,
        "CB_SCORE_WEIGHT_VOL": 0.15,
        "CB_WEIGHT_MULT_CVD": 0.8,
        "CB_WEIGHT_MULT_OFM": 0.6,
        "CB_WEIGHT_MULT_WHALE": 1.4,
        "CB_WEIGHT_MULT_DEAD": 1.6,
        "CB_WEIGHT_MULT_FADE": 0.4,
        "CB_WEIGHT_MULT_HIP3_MAIN": 1.4,
        "CB_BANNER_BOOST": 1.15,
        "CB_WHALE_STRENGTH_Z_MULT": 2.0,
        "CB_DEAD_STRENGTH_Z_MULT": 2.0,
        "CB_DEAD_BANNER_Z_MIN": 3.0,
        "CB_DEAD_OVERRIDE_Z_MIN": 3.5,
        "CB_DEAD_BASE_Z_CAP": 2.8,
        "CB_COMPOSITE_WEIGHTS_STANDARD": [0.32, 0.28, 0.20, 0.12, 0.08],
        "CB_COMPOSITE_WEIGHTS_HIP3": [0.7, 0.15, 0.10, 0.05, 0.0],
        "CB_COMPOSITE_NORMALIZE": 33.33,
        "CB_DEAD_BONUS_MIN_STRENGTH": 1.0,
        "CB_DEAD_BONUS_POINTS": 10.0,
        "CB_FLOW_DIV_Z_STRONG": 2.5,
        "CB_FLOW_DIV_Z_MID": 2.0,
        "CB_FLOW_DIV_Z_WEAK": 1.5,
        "CB_FLOW_SCORE_STRONG": 30.0,
        "CB_FLOW_SCORE_MID": 20.0,
        "CB_FLOW_SCORE_WEAK": 10.0,
        "CB_FLOW_COHORT_BONUS": 15.0,
        "CB_FLOW_DELTA_MULT": 30.0,
        "CB_FLOW_DELTA_CAP": 15.0,
        "CB_SMART_DELTA_MULT": 10.0,
        "CB_SMART_DELTA_CAP": 30.0,
        "CB_SMART_COHORT_BONUS": 10.0,
        "CB_SMART_DIV_Z_THRESHOLD": 2.0,
        "CB_SMART_DIV_Z_BONUS": 10.0,
        "CB_LIQ_BIAS_MULT": 0.5,
        "CB_LIQ_BIAS_CAP": 20.0,
        "CB_LIQ_HOT_PCT_1": 5.0,
        "CB_LIQ_HOT_BONUS_1": 10.0,
        "CB_LIQ_HOT_PCT_2": 10.0,
        "CB_LIQ_HOT_BONUS_2": 10.0,
        "CB_LIQ_FRAGILE_COUNT": 10,
        "CB_LIQ_FRAGILE_NOTIONAL": 1_000_000,
        "CB_LIQ_FRAGILE_BONUS": 15.0,
        "CB_LIQ_ASYMMETRY_RATIO": 2.0,
        "CB_LIQ_ASYMMETRY_BONUS": 10.0,
        "CB_VOL_ATR_1_MIN": 0.5,
        "CB_VOL_ATR_1_MAX": 1.0,
        "CB_VOL_SCORE_1": 70.0,
        "CB_VOL_ATR_2_MAX": 2.0,
        "CB_VOL_SCORE_2": 90.0,
        "CB_VOL_ATR_3_MAX": 3.0,
        "CB_VOL_SCORE_3": 80.0,
        "CB_VOL_ATR_4_MAX": 5.0,
        "CB_VOL_SCORE_4": 60.0,
        "CB_VOL_ATR_5_MIN": 5.0,
        "CB_VOL_SCORE_5": 30.0,
        "CB_VOL_SCORE_FALLBACK": 40.0,
        "CB_DIRECTION_MARGIN": 1,
}


def _cb_slider_factor(value: int) -> float:
    return 0.5 + (max(0, min(100, int(value))) / 100.0)


def _cb_scale_value(value: Any, factor: float, inverse: bool = False) -> Any:
    if isinstance(value, bool):
        return value
    if isinstance(value, dict):
        return {k: _cb_scale_value(v, factor, inverse) for k, v in value.items()}
    if isinstance(value, list):
        return [_cb_scale_value(v, factor, inverse) for v in value]
    if isinstance(value, tuple):
        return tuple(_cb_scale_value(v, factor, inverse) for v in value)
    if not isinstance(value, (int, float)):
        return value
    if abs(float(factor)) < 1e-9:
        factor = 1.0
    if inverse:
        factor = 1.0 / factor
    new_val = float(value) * factor
    if isinstance(value, int) and not isinstance(value, bool):
        return max(0, int(round(new_val)))
    return float(new_val)


def _cb_mode_preset(base: Dict[str, Any], factor: float) -> Dict[str, Any]:
    return {k: _cb_scale_value(v, factor, False) for k, v in base.items()}


def _cb_safe_float(value: Any, default: float = 0.0) -> float:
    try:
        out = float(value)
    except Exception:
        return float(default)
    if not math.isfinite(out):
        return float(default)
    return out


def _cb_slider_for_key(key: str) -> Tuple[str, bool]:
    name = key.lower()
    if any(k in name for k in ("weight", "mult", "bonus", "score", "bias", "boost", "cohort", "delta", "normalize")):
        return "weight", False
    if any(k in name for k in ("vol_atr", "vol_score", "atr")):
        return "aggr", False
    if any(k in name for k in ("min", "max", "threshold", "z", "pct", "ratio", "volume", "notional", "count", "cap", "margin")):
        return "freq", True
    return "weight", False


CB_MODE_PRESETS: Dict[str, Dict[str, Any]] = {
    "balanced": CB_BALANCED_PRESET,
    "conservative": _cb_mode_preset(CB_BALANCED_PRESET, 0.5),
    "aggressive": _cb_mode_preset(CB_BALANCED_PRESET, 2.0),
}

if CB_MODE not in CB_MODE_PRESETS:
    CB_MODE = "balanced"

CB_CRITICAL_OVERRIDE_KEYS: Set[str] = set()
CB_ENV_META: Dict[str, Tuple[str, Any]] = {}

def _load_cb_params() -> Dict[str, Any]:
    params = dict(CB_MODE_PRESETS[CB_MODE])
    slider_values = {
        "risk": CB_SLIDER_RISK,
        "freq": CB_SLIDER_FREQ,
        "aggr": CB_SLIDER_AGGR,
        "weight": CB_SLIDER_WEIGHT,
        "learn": CB_SLIDER_LEARN,
    }
    for key, value in list(params.items()):
        slider_name, inverse = _cb_slider_for_key(key)
        factor = _cb_slider_factor(slider_values[slider_name])
        params[key] = _cb_scale_value(value, factor, inverse)
    return params

_CB_PARAMS = _load_cb_params()

CB_MIN_Z_SCORE = _CB_PARAMS["CB_MIN_Z_SCORE"]
CB_MIN_VOLUME_24H = _CB_PARAMS["CB_MIN_VOLUME_24H"]
CB_STRONG_Z_SCORE = _CB_PARAMS["CB_STRONG_Z_SCORE"]

CB_SCORE_WEIGHT_SIGNAL = _CB_PARAMS["CB_SCORE_WEIGHT_SIGNAL"]
CB_SCORE_WEIGHT_FLOW = _CB_PARAMS["CB_SCORE_WEIGHT_FLOW"]
CB_SCORE_WEIGHT_SMART = _CB_PARAMS["CB_SCORE_WEIGHT_SMART"]
CB_SCORE_WEIGHT_LIQ = _CB_PARAMS["CB_SCORE_WEIGHT_LIQ"]
CB_SCORE_WEIGHT_VOL = _CB_PARAMS["CB_SCORE_WEIGHT_VOL"]

CB_WEIGHT_MULT_CVD = _CB_PARAMS["CB_WEIGHT_MULT_CVD"]
CB_WEIGHT_MULT_OFM = _CB_PARAMS["CB_WEIGHT_MULT_OFM"]
CB_WEIGHT_MULT_WHALE = _CB_PARAMS["CB_WEIGHT_MULT_WHALE"]
CB_WEIGHT_MULT_DEAD = _CB_PARAMS["CB_WEIGHT_MULT_DEAD"]
CB_WEIGHT_MULT_FADE = _CB_PARAMS["CB_WEIGHT_MULT_FADE"]
CB_WEIGHT_MULT_HIP3_MAIN = _CB_PARAMS["CB_WEIGHT_MULT_HIP3_MAIN"]
CB_BANNER_BOOST = _CB_PARAMS["CB_BANNER_BOOST"]

CB_WHALE_STRENGTH_Z_MULT = _CB_PARAMS["CB_WHALE_STRENGTH_Z_MULT"]
CB_DEAD_STRENGTH_Z_MULT = _CB_PARAMS["CB_DEAD_STRENGTH_Z_MULT"]
CB_DEAD_BANNER_Z_MIN = _CB_PARAMS["CB_DEAD_BANNER_Z_MIN"]
CB_DEAD_OVERRIDE_Z_MIN = _CB_PARAMS["CB_DEAD_OVERRIDE_Z_MIN"]
CB_DEAD_BASE_Z_CAP = _CB_PARAMS["CB_DEAD_BASE_Z_CAP"]

CB_COMPOSITE_WEIGHTS_STANDARD = _CB_PARAMS["CB_COMPOSITE_WEIGHTS_STANDARD"]
CB_COMPOSITE_WEIGHTS_HIP3 = _CB_PARAMS["CB_COMPOSITE_WEIGHTS_HIP3"]
CB_COMPOSITE_NORMALIZE = _CB_PARAMS["CB_COMPOSITE_NORMALIZE"]
CB_DEAD_BONUS_MIN_STRENGTH = _CB_PARAMS["CB_DEAD_BONUS_MIN_STRENGTH"]
CB_DEAD_BONUS_POINTS = _CB_PARAMS["CB_DEAD_BONUS_POINTS"]

CB_FLOW_DIV_Z_STRONG = _CB_PARAMS["CB_FLOW_DIV_Z_STRONG"]
CB_FLOW_DIV_Z_MID = _CB_PARAMS["CB_FLOW_DIV_Z_MID"]
CB_FLOW_DIV_Z_WEAK = _CB_PARAMS["CB_FLOW_DIV_Z_WEAK"]
CB_FLOW_SCORE_STRONG = _CB_PARAMS["CB_FLOW_SCORE_STRONG"]
CB_FLOW_SCORE_MID = _CB_PARAMS["CB_FLOW_SCORE_MID"]
CB_FLOW_SCORE_WEAK = _CB_PARAMS["CB_FLOW_SCORE_WEAK"]
CB_FLOW_COHORT_BONUS = _CB_PARAMS["CB_FLOW_COHORT_BONUS"]
CB_FLOW_DELTA_MULT = _CB_PARAMS["CB_FLOW_DELTA_MULT"]
CB_FLOW_DELTA_CAP = _CB_PARAMS["CB_FLOW_DELTA_CAP"]

CB_SMART_DELTA_MULT = _CB_PARAMS["CB_SMART_DELTA_MULT"]
CB_SMART_DELTA_CAP = _CB_PARAMS["CB_SMART_DELTA_CAP"]
CB_SMART_COHORT_BONUS = _CB_PARAMS["CB_SMART_COHORT_BONUS"]
CB_SMART_DIV_Z_THRESHOLD = _CB_PARAMS["CB_SMART_DIV_Z_THRESHOLD"]
CB_SMART_DIV_Z_BONUS = _CB_PARAMS["CB_SMART_DIV_Z_BONUS"]

CB_LIQ_BIAS_MULT = _CB_PARAMS["CB_LIQ_BIAS_MULT"]
CB_LIQ_BIAS_CAP = _CB_PARAMS["CB_LIQ_BIAS_CAP"]
CB_LIQ_HOT_PCT_1 = _CB_PARAMS["CB_LIQ_HOT_PCT_1"]
CB_LIQ_HOT_BONUS_1 = _CB_PARAMS["CB_LIQ_HOT_BONUS_1"]
CB_LIQ_HOT_PCT_2 = _CB_PARAMS["CB_LIQ_HOT_PCT_2"]
CB_LIQ_HOT_BONUS_2 = _CB_PARAMS["CB_LIQ_HOT_BONUS_2"]
CB_LIQ_FRAGILE_COUNT = _CB_PARAMS["CB_LIQ_FRAGILE_COUNT"]
CB_LIQ_FRAGILE_NOTIONAL = _CB_PARAMS["CB_LIQ_FRAGILE_NOTIONAL"]
CB_LIQ_FRAGILE_BONUS = _CB_PARAMS["CB_LIQ_FRAGILE_BONUS"]
CB_LIQ_ASYMMETRY_RATIO = _CB_PARAMS["CB_LIQ_ASYMMETRY_RATIO"]
CB_LIQ_ASYMMETRY_BONUS = _CB_PARAMS["CB_LIQ_ASYMMETRY_BONUS"]

CB_VOL_ATR_1_MIN = _CB_PARAMS["CB_VOL_ATR_1_MIN"]
CB_VOL_ATR_1_MAX = _CB_PARAMS["CB_VOL_ATR_1_MAX"]
CB_VOL_SCORE_1 = _CB_PARAMS["CB_VOL_SCORE_1"]
CB_VOL_ATR_2_MAX = _CB_PARAMS["CB_VOL_ATR_2_MAX"]
CB_VOL_SCORE_2 = _CB_PARAMS["CB_VOL_SCORE_2"]
CB_VOL_ATR_3_MAX = _CB_PARAMS["CB_VOL_ATR_3_MAX"]
CB_VOL_SCORE_3 = _CB_PARAMS["CB_VOL_SCORE_3"]
CB_VOL_ATR_4_MAX = _CB_PARAMS["CB_VOL_ATR_4_MAX"]
CB_VOL_SCORE_4 = _CB_PARAMS["CB_VOL_SCORE_4"]
CB_VOL_ATR_5_MIN = _CB_PARAMS["CB_VOL_ATR_5_MIN"]
CB_VOL_SCORE_5 = _CB_PARAMS["CB_VOL_SCORE_5"]
CB_VOL_SCORE_FALLBACK = _CB_PARAMS["CB_VOL_SCORE_FALLBACK"]

CB_DIRECTION_MARGIN = _CB_PARAMS["CB_DIRECTION_MARGIN"]

# =============================================================================
# Data Classes
# =============================================================================

@dataclass
class ScoredOpportunity:
    """A scored trading opportunity."""
    symbol: str
    score: float
    direction: str  # 'LONG', 'SHORT', 'MIXED'
    signals: Dict[str, Dict]  # signal_name -> {direction, z_score, etc}
    key_metrics: Dict[str, Any]  # Extracted key values
    strong_signals: List[str] = field(default_factory=list)
    raw_data: Dict[str, Any] = field(default_factory=dict)
    
    def __lt__(self, other):
        return self.score < other.score


# =============================================================================
# Opportunity Scorer
# =============================================================================

class OpportunityScorer:
    """
    Scores and ranks trading opportunities across all symbols.
    
    Scoring weights:
    - Signal strength (30%): Combined z-scores from perp_signals
    - Flow alignment (20%): EVScore + EVFlow agreement
    - Smart money bias (20%): Cohort delta smart vs dumb
    - Liquidity edge (15%): Position imbalances, fragile wallets
    - Volatility fit (15%): ATR in tradeable range
    """
    
    def __init__(self):
        self.log = get_logger("scorer")
        
        # Minimum thresholds
        self.min_z_score = CB_MIN_Z_SCORE
        self.min_volume_24h = CB_MIN_VOLUME_24H  # $1M
        self.strong_z_score = CB_STRONG_Z_SCORE
        self._hl_mid_keys = ("hl_mid", "hyperliquid_mid")
        self._lighter_mid_keys = ("lighter_mid", "lt_mid")

    def _extract_mid(self, data: Dict[str, Any], keys: Tuple[str, ...]) -> Tuple[Optional[Any], bool]:
        for key in keys:
            if key in data:
                return data.get(key), True
        return None, False

    def _symbol_on_both_venues(self, symbol: str, data: Dict[str, Any]) -> bool:
        """Best-effort dual-venue availability check from snapshot data."""
        if not isinstance(data, dict):
            return False

        hl_mid, hl_present = self._extract_mid(data, self._hl_mid_keys)
        lt_mid, lt_present = self._extract_mid(data, self._lighter_mid_keys)

        def _mid_ok(val: Any) -> bool:
            try:
                return float(val or 0.0) > 0
            except (TypeError, ValueError):
                return False

        # If both mids are present in snapshot data, use the same gate as live-agent.
        if hl_present and lt_present:
            return dual_venue_mids_ok(hl_mid, lt_mid)

        # If a side is explicitly present and invalid, drop early.
        if hl_present and not _mid_ok(hl_mid):
            return False
        if lt_present:
            if not _mid_ok(lt_mid):
                return False

        # Fallback: HIP3 symbols are HL-only and fail the dual-venue gate.
        hip3_info = data.get("hip3_info") or {}
        if hip3_info.get("is_hip3") is True or str(symbol).lower().startswith("xyz:"):
            return False

        # One-sided quote visibility is ambiguous for dual-venue readiness.
        if hl_present or lt_present:
            return False

        return True

    def _with_hip3_main(self, symbol: str, perp: Optional[Dict[str, Any]], data: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        """Inject HIP3_MAIN into perp_signals for xyz:* symbols."""
        if not str(symbol).lower().startswith("xyz:"):
            return perp if isinstance(perp, dict) else {}
        if not isinstance(data, dict):
            return perp if isinstance(perp, dict) else {}
        hip3_payload = data.get("hip3_predator")
        if not isinstance(hip3_payload, dict):
            return perp if isinstance(perp, dict) else {}
        base = perp if isinstance(perp, dict) else {}
        dead_capital = base.get("dead_capital") if isinstance(base, dict) else None
        hip3_main, _meta = compute_hip3_main(hip3_payload, dead_capital=dead_capital)
        if not hip3_main:
            return base
        merged = dict(base)
        merged["hip3_main"] = hip3_main
        return merged
        
    def score_symbol(self, symbol: str, data: Dict, skip_z_filter: bool = False) -> Optional[ScoredOpportunity]:
        """
        Score a single symbol for trading opportunity.
        
        Args:
            symbol: Symbol name
            data: Symbol data dict
            skip_z_filter: If True, skip min z-score filter (for open positions)
        
        Returns None if symbol doesn't meet minimum criteria.
        """
        if not data:
            return None

        # Liquidity gate (minimum notional volume in the last 24h).
        # Keep a hard floor at $1M for non-open-position discovery.
        price_change = data.get('price_change', {}) if isinstance(data.get('price_change', {}), dict) else {}
        volume_raw = price_change.get('volume_24h', data.get('volume_24h', 0))
        try:
            volume_24h = float(volume_raw or 0.0)
        except (TypeError, ValueError):
            volume_24h = 0.0
        min_volume_gate = max(1_000_000.0, float(self.min_volume_24h or 0.0))
        if not skip_z_filter and volume_24h < min_volume_gate:
            return None
            
        # Extract perp_signals
        perp = data.get('perp_signals', {})
        perp = self._with_hip3_main(symbol, perp, data)
        if not perp:
            return None
            
        # Calculate signal strength
        signal_scores = self._score_signals(perp, data, symbol)
        signal_strength = signal_scores.get('composite', 0)
        strong_signals = self._detect_strong_signals(signal_scores.get('signals', {}))
        
        # Check minimum signal threshold (skip for open positions)
        max_z = signal_scores.get('max_z', 0)
        if not skip_z_filter and max_z < self.min_z_score:
            return None
        
        # Score flow alignment
        flow_score = self._score_flow_alignment(data)
        
        # Score smart money
        smart_score = self._score_smart_money(data)
        
        # Score liquidity edge
        liq_score = self._score_liquidity_edge(data)
        
        # Score volatility fit
        vol_score = self._score_volatility(data)
        
        # Composite score (0-100)
        total_score = (
            signal_strength * CB_SCORE_WEIGHT_SIGNAL +
            flow_score * CB_SCORE_WEIGHT_FLOW +
            smart_score * CB_SCORE_WEIGHT_SMART +
            liq_score * CB_SCORE_WEIGHT_LIQ +
            vol_score * CB_SCORE_WEIGHT_VOL
        )
        
        # Determine direction
        direction = self._determine_direction(perp, data)
        
        # Extract key metrics for context
        key_metrics = self._extract_key_metrics(data, signal_scores)
        
        return ScoredOpportunity(
            symbol=symbol,
            score=total_score,
            direction=direction,
            signals=signal_scores.get('signals', {}),
            key_metrics=key_metrics,
            strong_signals=strong_signals,
            raw_data=data
        )
    
    def _score_signals(self, perp: Dict, data: Optional[Dict] = None, symbol: str = "") -> Dict:
        """Score perp_signals and return composite + individual scores.

        Signal hierarchy (AGI Trader policy - Jan 2026):
        - Primary: DEAD_CAPITAL (0.32), WHALE (0.28) - can trigger independently
        - Secondary: CVD (0.20), OFM (0.12), FADE (0.08) - confirmation only
        - Deprecated: LIQ_PNL - kept for monitoring, excluded from weighting
        - HIP3 Primary: hip3_main (FLOW gate + OFM confirm) - for xyz: symbols only
        """
        _ = data
        is_hip3_symbol = symbol.lower().startswith("xyz:")
        signals = {}
        z_scores = []
        weighted_scores: Dict[str, float] = {}

        def _add_signal(name: str, payload: Dict[str, Any], z_score: float, weight_mult: float = 1.0, include_in_scoring: bool = True) -> None:
            if include_in_scoring:
                z_scores.append(z_score)
                weighted_scores[name] = float(z_score * weight_mult)
            signals[name] = payload

        # CVD - SECONDARY (weight 0.20)
        cvd = perp.get('cvd', {})
        if cvd:
            z_smart = abs(_cb_safe_float(cvd.get('z_smart', 0), 0.0))
            z_dumb = abs(_cb_safe_float(cvd.get('z_dumb', 0), 0.0))
            z = max(z_smart, z_dumb)
            banner = bool(cvd.get('banner_trigger', False))
            z_eff = z * (CB_BANNER_BOOST if banner else 1.0)
            _add_signal('cvd', {
                'direction': self._normalize_direction(cvd.get('signal')),
                'z_score': z,
                'z_smart': cvd.get('z_smart', 0),
                'z_dumb': cvd.get('z_dumb', 0),
                'confidence': cvd.get('confidence', 'LOW'),
                'banner_trigger': banner,
            }, z_eff, weight_mult=CB_WEIGHT_MULT_CVD)  # Reduced weight for secondary signal

        # OFM - SECONDARY (weight 0.12)
        ofm = perp.get('ofm', {})
        if ofm:
            z = abs(_cb_safe_float(ofm.get('z_score', 0), 0.0))
            banner = bool(ofm.get('banner_trigger', False))
            z_eff = z * (CB_BANNER_BOOST if banner else 1.0)
            _add_signal('ofm', {
                'direction': self._normalize_direction(ofm.get('signal')),
                'z_score': z,
                'divergence': ofm.get('divergence', 0),
                'banner_trigger': banner,
            }, z_eff, weight_mult=CB_WEIGHT_MULT_OFM)  # Reduced weight for secondary signal

        # WHALE - PRIMARY (weight 0.28)
        whale = perp.get('whale', {})
        if whale:
            strength = _cb_safe_float(whale.get('strength', 0), 0.0)
            z_equiv = strength * CB_WHALE_STRENGTH_Z_MULT  # Convert strength to z-score scale
            banner = bool(whale.get('banner_trigger', False))
            z_eff = z_equiv * (CB_BANNER_BOOST if banner else 1.0)
            _add_signal('whale', {
                'direction': self._normalize_direction(whale.get('signal')),
                'strength': strength,
                'z_score': z_equiv,
                'banner_trigger': banner,
            }, z_eff, weight_mult=CB_WEIGHT_MULT_WHALE)

        # DEAD_CAPITAL - PRIMARY (weight 0.32)
        dead = perp.get('dead_capital', {})
        if dead:
            dead_dir = self._normalize_direction(dead.get('signal'))

            # New watcher format (2026-01): banner_trigger is the trigger, and
            # locked_*_pct are the magnitudes. Keep backwards compat with legacy
            # 'strength' field.
            banner_present = 'banner_trigger' in dead
            banner_trigger = bool(dead.get('banner_trigger', False))
            try:
                banner_threshold = float(dead.get('banner_threshold', 0) or 0)
            except (TypeError, ValueError):
                banner_threshold = 0.0
            try:
                threshold = float(dead.get('threshold', 0) or 0)
            except (TypeError, ValueError):
                threshold = 0.0
            try:
                locked_long = float(dead.get('locked_long_pct', 0) or 0)
            except (TypeError, ValueError):
                locked_long = 0.0
            try:
                locked_short = float(dead.get('locked_short_pct', 0) or 0)
            except (TypeError, ValueError):
                locked_short = 0.0

            # Prefer legacy strength if present, otherwise derive from locked_*.
            strength = dead.get('strength', 0) or 0
            try:
                strength = float(strength)
            except (TypeError, ValueError):
                strength = 0.0

            # Updated policy (boss directive): banner_trigger is a STRONG boost,
            # not the only gate. Dead-capital can still contribute without banner,
            # but we cap its z-equivalent below "strong" unless banner/override.

            # Interpret locked magnitude: for LONG, trapped shorts dominate; for SHORT, trapped longs dominate.
            locked_mag = 0.0
            if dead_dir == 'LONG':
                locked_mag = locked_short
            elif dead_dir == 'SHORT':
                locked_mag = locked_long

            # z_equiv is the numeric used for scoring/thresholding.
            # For dead_capital we support two modes:
            # 1) banner_trigger (rare, strong): always actionable
            # 2) internal learned z_imbalance override (>=3.5 for 2 consecutive snapshots)
            z_equiv = 0.0
            override_trigger = bool(dead.get('override_trigger', False))
            try:
                z_imbalance = float(dead.get('z_imbalance', 0) or 0)
            except (TypeError, ValueError):
                z_imbalance = 0.0
            z_abs_imbalance = abs(z_imbalance)

            # Direction from raw locked imbalance sign (more reliable than z sign).
            try:
                imb = float(dead.get('imbalance', locked_short - locked_long) or 0)
            except (TypeError, ValueError):
                imb = 0.0
            override_dir = 'LONG' if imb > 0 else 'SHORT' if imb < 0 else dead_dir

            # Backwards compat: if banner_trigger field is absent, fall back to legacy strength.
            if not banner_present:
                if strength > 0:
                    z_equiv = strength * CB_DEAD_STRENGTH_Z_MULT
                triggered = bool(z_equiv > 0 and dead_dir in ('LONG', 'SHORT'))
            else:
                triggered = False

                # Base z-equivalent from locked magnitudes / thresholds.
                # Used when banner_trigger is false (we still want dead_capital to
                # contribute, but not as a "strong" single-signal trigger).
                base_z = 0.0
                try:
                    if threshold > 0 and banner_threshold > threshold:
                        if locked_mag <= threshold:
                            base_z = (locked_mag / threshold) * 2.0
                        else:
                            slope = 1.0 / (banner_threshold - threshold)
                            base_z = 2.0 + (locked_mag - threshold) * slope
                    elif threshold > 0:
                        base_z = (locked_mag / threshold) * 2.0
                    else:
                        base_z = float(locked_mag or 0.0)
                except Exception:
                    base_z = 0.0

                if banner_trigger:
                    # Banner is a strong signal.
                    triggered = True
                    if z_abs_imbalance > 0:
                        z_equiv = max(CB_DEAD_BANNER_Z_MIN, z_abs_imbalance)
                    elif strength > 0:
                        z_equiv = max(CB_DEAD_BANNER_Z_MIN, strength * CB_DEAD_STRENGTH_Z_MULT)
                    else:
                        z_equiv = max(base_z, CB_DEAD_BANNER_Z_MIN)
                    strength = max(strength, z_equiv / CB_DEAD_STRENGTH_Z_MULT)

                elif override_trigger:
                    # Learned override trigger: treat as actionable even without banner.
                    # Safety: require direction consistency if watcher provides a direction.
                    if dead_dir in ('LONG', 'SHORT') and override_dir in ('LONG', 'SHORT') and dead_dir != override_dir:
                        triggered = False
                        z_equiv = 0.0
                    else:
                        triggered = True
                        z_equiv = max(CB_DEAD_OVERRIDE_Z_MIN, z_abs_imbalance)
                        strength = max(strength, z_equiv / CB_DEAD_STRENGTH_Z_MULT)
                        dead_dir = override_dir

                else:
                    # No banner/override: allow dead_capital to contribute but cap below strong.
                    z_equiv = min(CB_DEAD_BASE_Z_CAP, max(0.0, float(strength * CB_DEAD_STRENGTH_Z_MULT) if strength > 0 else base_z))
                    strength = max(strength, z_equiv / CB_DEAD_STRENGTH_Z_MULT) if z_equiv > 0 else 0.0
                    triggered = False

            # Banner-trigger boost (applied to z used for scoring only)
            # IMPORTANT: if dead_capital is NEUTRAL, do not let it dominate the composite score.
            include_scoring = bool(dead_dir in ('LONG', 'SHORT') and z_equiv > 0)
            z_eff = (z_equiv * (CB_BANNER_BOOST if banner_trigger else 1.0)) if include_scoring else 0.0
            _add_signal('dead_capital', {
                'direction': dead_dir if include_scoring else 'NEUTRAL',
                'strength': strength,
                'z_score': z_equiv,
                'locked_long_pct': locked_long,
                'locked_short_pct': locked_short,
                'imbalance': dead.get('imbalance', locked_short - locked_long),
                'z_imbalance': dead.get('z_imbalance', 0.0),
                'z_abs_imbalance': dead.get('z_abs_imbalance', 0.0),
                'override_streak': dead.get('override_streak', 0),
                'override_trigger': bool(dead.get('override_trigger', False)),
                'samples': dead.get('samples', 0),
                'threshold': threshold,
                'banner_threshold': banner_threshold,
                'banner_trigger': banner_trigger,
                'triggered': triggered,
            }, z_eff, weight_mult=CB_WEIGHT_MULT_DEAD, include_in_scoring=include_scoring)  # Boosted weight for primary signal

        # FADE - SECONDARY (weight 0.08)
        fade = perp.get('fade', {})
        if fade:
            z = abs(_cb_safe_float(fade.get('z_score', 0), 0.0))
            banner = bool(fade.get('banner_trigger', False))
            z_eff = z * (CB_BANNER_BOOST if banner else 1.0)
            _add_signal('fade', {
                'direction': self._normalize_direction(fade.get('signal')),
                'z_score': z,
                'divergence': fade.get('divergence', 0),
                'banner_trigger': banner,
            }, z_eff, weight_mult=CB_WEIGHT_MULT_FADE)  # Reduced weight, secondary signal

        # HIP3_MAIN (PRIMARY for xyz: symbols ONLY)
        # Derived from HIP3 predator payload; already gated/confirmed by compute_hip3_main().
        if is_hip3_symbol:
            hip3_main = perp.get('hip3_main', {})
            if isinstance(hip3_main, dict) and hip3_main:
                z = abs(_cb_safe_float(hip3_main.get('z_score', 0), 0.0))
                _add_signal('hip3_main', hip3_main, z, weight_mult=CB_WEIGHT_MULT_HIP3_MAIN)

        # LIQ_PNL - DEPRECATED (kept for monitoring only, excluded from scoring)
        liq = perp.get('liq_pnl', {})
        if liq:
            z = abs(_cb_safe_float(liq.get('z_score', 0), 0.0))
            _add_signal('liq_pnl', {
                'direction': self._normalize_direction(liq.get('signal')),
                'z_score': z,
                'pain_delta': liq.get('pain_delta', 0),
                '_deprecated': True  # Marker for monitoring tools
            }, z, include_in_scoring=False)  # NOT included in composite score

        # Composite signal score (weighted average normalized to 0-100)
        # Use different weight profiles for HIP3 vs non-HIP3 symbols
        composite = 0
        if weighted_scores:
            if is_hip3_symbol:
                # HIP3: hip3_main dominates; other signals optional
                weights_src = CB_COMPOSITE_WEIGHTS_HIP3 if isinstance(CB_COMPOSITE_WEIGHTS_HIP3, list) else [0.7, 0.15, 0.10, 0.05, 0.0]
                signal_order = ["hip3_main", "dead_capital", "whale", "cvd", "ofm", "fade"]
            else:
                # Standard: dead_capital/whale primary; others secondary
                weights_src = CB_COMPOSITE_WEIGHTS_STANDARD if isinstance(CB_COMPOSITE_WEIGHTS_STANDARD, list) else [0.32, 0.28, 0.20, 0.12, 0.08]
                signal_order = ["dead_capital", "whale", "cvd", "ofm", "fade", "hip3_main"]

            weighted_pairs: List[Tuple[float, float]] = []
            for idx, sig_name in enumerate(signal_order):
                if sig_name not in weighted_scores:
                    continue
                try:
                    w = float(weights_src[idx]) if idx < len(weights_src) else 0.0
                except Exception:
                    w = 0.0
                if w <= 0:
                    continue
                weighted_pairs.append((float(weighted_scores[sig_name]), w))

            if weighted_pairs:
                total_w = sum(w for _, w in weighted_pairs)
                if total_w > 0:
                    weighted_sum = sum(v * (w / total_w) for v, w in weighted_pairs)
                else:
                    weighted_sum = sum(v for v, _ in weighted_pairs) / max(1, len(weighted_pairs))
            else:
                # Fallback for any unknown signal names.
                vals = [float(v) for v in weighted_scores.values()]
                weighted_sum = sum(vals) / max(1, len(vals))
            # Normalize: z=3 should be ~100 points
            composite = weighted_sum * CB_COMPOSITE_NORMALIZE

        # Bonus for high-conviction dead capital signals (only if strong)
        dead_sig = signals.get('dead_capital', {})
        dead_dir = dead_sig.get('direction')
        dead_strength = _cb_safe_float(dead_sig.get('strength', 0), 0.0)
        if dead_dir in ('LONG', 'SHORT') and dead_strength >= CB_DEAD_BONUS_MIN_STRENGTH:
            composite += CB_DEAD_BONUS_POINTS

        composite = min(100, composite)
            
        return {
            'signals': signals,
            'composite': composite,
            'max_z': max(z_scores) if z_scores else 0
        }

    def _detect_strong_signals(self, signals: Dict[str, Dict[str, Any]]) -> List[str]:
        """Return signal names that should be treated as *strong* (mandatory include).

        Boss directive (2026-02-01):
        - Whale + DeadCap banners are mandatory-trade triggers.
        - Other signals are context-only.

        So we only mark strong when:
        - name in {whale, dead_capital} AND banner_trigger == True.
        """
        strong: List[str] = []
        for name, payload in (signals or {}).items():
            if not isinstance(payload, dict):
                continue
            n = str(name or "").lower()
            if n not in ("whale", "dead_capital"):
                continue
            direction = str(payload.get('direction') or '').upper()
            if direction not in ('LONG', 'SHORT'):
                continue
            if bool(payload.get('banner_trigger', False)):
                strong.append(n)
        return strong
    
    def _score_flow_alignment(self, data: Dict) -> float:
        """Score based on smart/dumb CVD alignment and cohort signals."""
        score = 50  # Start neutral
        
        # Use smart_dumb_cvd for flow alignment (still available)
        smart_dumb = data.get('smart_dumb_cvd', {})
        if smart_dumb:
            div_z = abs(_cb_safe_float(smart_dumb.get('divergence_z', 0), 0.0))
            # Strong divergence = tradeable signal
            if div_z >= CB_FLOW_DIV_Z_STRONG:
                score += CB_FLOW_SCORE_STRONG
            elif div_z >= CB_FLOW_DIV_Z_MID:
                score += CB_FLOW_SCORE_MID
            elif div_z >= CB_FLOW_DIV_Z_WEAK:
                score += CB_FLOW_SCORE_WEAK
        
        # Cohort delta adds confidence
        cohort = data.get('cohort_delta', {})
        if cohort:
            signal = cohort.get('signal', '')
            if signal in ('SMART_ACCUMULATING', 'SMART_DISTRIBUTING'):
                score += CB_FLOW_COHORT_BONUS
            delta = abs(_cb_safe_float(cohort.get('smart_vs_dumb_delta', 0), 0.0))
            score += min(CB_FLOW_DELTA_CAP, delta * CB_FLOW_DELTA_MULT)  # Up to cap points
            
        return min(100, max(0, score))
    
    def _score_smart_money(self, data: Dict) -> float:
        """Score based on smart money vs dumb money positioning."""
        score = 50  # Start neutral
        
        cohort = data.get('cohort_delta', {})
        smart_cvd = data.get('smart_dumb_cvd', {})
        
        if cohort:
            # Smart vs dumb delta
            delta = _cb_safe_float(cohort.get('smart_vs_dumb_delta', 0), 0.0)
            # Normalize to score: positive = smart buying
            score += min(CB_SMART_DELTA_CAP, max(-CB_SMART_DELTA_CAP, delta * CB_SMART_DELTA_MULT))
            
            if cohort.get('signal') in ('SMART_ACCUMULATING', 'SMART_DISTRIBUTING'):
                score += CB_SMART_COHORT_BONUS
                
        if smart_cvd:
            div_z = _cb_safe_float(smart_cvd.get('divergence_z', 0), 0.0)
            if abs(div_z) >= CB_SMART_DIV_Z_THRESHOLD:
                score += CB_SMART_DIV_Z_BONUS  # Strong divergence is tradeable
                
        return min(100, max(0, score))
    
    def _score_liquidity_edge(self, data: Dict) -> float:
        """Score based on position imbalances and fragile wallets."""
        score = 50  # Start neutral
        
        summary = data.get('summary', {})
        fragile = data.get('fragile_wallets', {})
        liq_heat = data.get('liquidation_heatmap', {})
        
        if summary:
            # Net bias extremity
            bias_pct = _cb_safe_float(summary.get('net_bias_pct', 50), 50.0)
            extremity = abs(bias_pct - 50)
            score += min(CB_LIQ_BIAS_CAP, extremity * CB_LIQ_BIAS_MULT)  # Up to cap points for imbalance
            
            # Hot zone indicates stressed positions
            hot_pct = _cb_safe_float(summary.get('hot_zone_pct', 0), 0.0)
            if hot_pct > CB_LIQ_HOT_PCT_1:
                score += CB_LIQ_HOT_BONUS_1
            if hot_pct > CB_LIQ_HOT_PCT_2:
                score += CB_LIQ_HOT_BONUS_2
                
        if fragile:
            # Fragile wallets = liquidation fodder
            count = int(_cb_safe_float(fragile.get('count', 0), 0.0))
            notional = _cb_safe_float(fragile.get('total_notional', 0), 0.0)
            if count > CB_LIQ_FRAGILE_COUNT and notional > CB_LIQ_FRAGILE_NOTIONAL:
                score += CB_LIQ_FRAGILE_BONUS
                
        if liq_heat:
            # Asymmetric pain is tradeable
            long_pain = abs(_cb_safe_float(liq_heat.get('long_total_pnl', 0), 0.0))
            short_pain = abs(_cb_safe_float(liq_heat.get('short_total_pnl', 0), 0.0))
            if long_pain > 0 and short_pain > 0:
                asymmetry = max(long_pain, short_pain) / min(long_pain, short_pain) if min(long_pain, short_pain) > 0 else 1
                if asymmetry > CB_LIQ_ASYMMETRY_RATIO:
                    score += CB_LIQ_ASYMMETRY_BONUS
                    
        return min(100, max(0, score))
    
    def _score_volatility(self, data: Dict) -> float:
        """Score based on ATR - prefer moderate volatility."""
        atr_data = data.get('atr', {})
        
        if not atr_data:
            return 50  # Neutral
            
        atr_pct = _cb_safe_float(atr_data.get('atr_pct', 0), 0.0)
        
        # Sweet spot: 1-3% ATR
        if CB_VOL_ATR_1_MIN <= atr_pct <= CB_VOL_ATR_1_MAX:
            return CB_VOL_SCORE_1  # Good for scalps
        elif CB_VOL_ATR_1_MAX < atr_pct <= CB_VOL_ATR_2_MAX:
            return CB_VOL_SCORE_2  # Ideal
        elif CB_VOL_ATR_2_MAX < atr_pct <= CB_VOL_ATR_3_MAX:
            return CB_VOL_SCORE_3  # Good but volatile
        elif CB_VOL_ATR_3_MAX < atr_pct <= CB_VOL_ATR_4_MAX:
            return CB_VOL_SCORE_4  # Risky
        elif atr_pct > CB_VOL_ATR_5_MIN:
            return CB_VOL_SCORE_5  # Too volatile
        else:
            return CB_VOL_SCORE_FALLBACK  # Too quiet
    
    def _normalize_direction(self, raw: Optional[str]) -> str:
        """Normalize direction to LONG/SHORT/NEUTRAL."""
        if not raw:
            return 'NEUTRAL'
        raw = raw.upper()
        if raw in ('LONG', 'BULLISH'):
            return 'LONG'
        elif raw in ('SHORT', 'BEARISH'):
            return 'SHORT'
        return 'NEUTRAL'
    
    def _determine_direction(self, perp: Dict, data: Dict) -> str:
        """Determine overall direction from signals."""
        long_count = 0
        short_count = 0
        
        for sig_name, sig_data in perp.items():
            if not isinstance(sig_data, dict):
                continue

            # Deprecated signals are monitored only and should not steer direction.
            if sig_name == "liq_pnl" or bool(sig_data.get("_deprecated", False)):
                continue

            # DEAD_CAPITAL direction consensus gating:
            # - banner_trigger / override_trigger => always count direction.
            # - if banner_trigger is false (including cooldown suppression), still count
            #   direction when locked magnitude is meaningfully above the base threshold.
            if sig_name == 'dead_capital':
                banner_present = ('banner_trigger' in sig_data) or ('override_trigger' in sig_data)
                banner_trigger = bool(sig_data.get('banner_trigger', False))
                override_trigger = bool(sig_data.get('override_trigger', False))
                if banner_present and not (banner_trigger or override_trigger):
                    try:
                        locked_long = float(sig_data.get('locked_long_pct', 0) or 0)
                    except (TypeError, ValueError):
                        locked_long = 0.0
                    try:
                        locked_short = float(sig_data.get('locked_short_pct', 0) or 0)
                    except (TypeError, ValueError):
                        locked_short = 0.0
                    try:
                        threshold = float(sig_data.get('threshold', 0) or 0)
                    except (TypeError, ValueError):
                        threshold = 0.0

                    # If below base threshold, treat as neutral for consensus.
                    if threshold > 0 and max(locked_long, locked_short) < threshold:
                        continue

            raw_dir = sig_data.get('signal')
            if raw_dir is None:
                raw_dir = sig_data.get('direction')
            direction = self._normalize_direction(raw_dir)
            if direction == 'LONG':
                long_count += 1
            elif direction == 'SHORT':
                short_count += 1
                
        # Add smart/dumb divergence signal
        smart_dumb = data.get('smart_dumb_cvd', {})
        div_sig = smart_dumb.get('divergence_signal', '')
        if 'BULLISH' in str(div_sig).upper():
            long_count += 1
        elif 'BEARISH' in str(div_sig).upper():
            short_count += 1
            
        if long_count > short_count + CB_DIRECTION_MARGIN:
            return 'LONG'
        elif short_count > long_count + CB_DIRECTION_MARGIN:
            return 'SHORT'
        return 'MIXED'
    
    def _extract_key_metrics(self, data: Dict, signal_scores: Dict) -> Dict:
        """Extract key metrics for context display."""
        metrics = {}
        
        # Price and volume
        metrics['price'] = data.get('price', 0)
        
        price_change = data.get('price_change', {})
        metrics['volume_24h'] = price_change.get('volume_24h', 0)
        metrics['pct_24h'] = price_change.get('pct_24h', 0)
        
        # ATR
        atr = data.get('atr', {})
        metrics['atr_pct'] = atr.get('atr_pct', 0)
        
        # Note: evscore, evflow, sentiment fields removed from tracker SSE
        
        # Summary
        summary = data.get('summary', {})
        metrics['net_bias'] = summary.get('net_bias', 'N/A')
        metrics['net_bias_pct'] = summary.get('net_bias_pct', 50)
        metrics['hot_zone_pct'] = summary.get('hot_zone_pct', 0)
        metrics['avg_leverage'] = summary.get('avg_leverage', 0)
        metrics['long_count'] = summary.get('long_count', 0)
        metrics['short_count'] = summary.get('short_count', 0)
        metrics['total_long_exposure'] = summary.get('total_long_exposure', 0)
        metrics['total_short_exposure'] = summary.get('total_short_exposure', 0)
        metrics['in_danger_threshold'] = summary.get('in_danger_threshold', 0)
        metrics['market_oi'] = summary.get('market_oi', 0)
        
        # Funding
        funding = data.get('funding', {})
        metrics['funding_rate'] = funding.get('rate_bps', 0)
        metrics['funding_direction'] = funding.get('direction', 'N/A')
        metrics['funding_annualized'] = funding.get('annualized_pct', 0)
        
        # Smart/dumb CVD
        smart_dumb = data.get('smart_dumb_cvd', {})
        metrics['smart_cvd'] = smart_dumb.get('smart_cvd', 0)
        metrics['dumb_cvd'] = smart_dumb.get('dumb_cvd', 0)
        metrics['divergence_z'] = smart_dumb.get('divergence_z', 0)
        metrics['divergence_signal'] = smart_dumb.get('divergence_signal', 'N/A')
        metrics['smart_volume'] = smart_dumb.get('smart_volume', 0)
        metrics['dumb_volume'] = smart_dumb.get('dumb_volume', 0)
        
        # Cohort delta
        cohort = data.get('cohort_delta', {})
        metrics['cohort_signal'] = cohort.get('signal', 'N/A')
        metrics['smart_vs_dumb_delta'] = cohort.get('smart_vs_dumb_delta', 0)
        metrics['whale_vs_retail_delta'] = cohort.get('whale_vs_retail_delta', 0)
        
        # Note: flow_score field removed from tracker SSE
        
        # Fragile wallets
        fragile = data.get('fragile_wallets', {})
        metrics['fragile_count'] = fragile.get('count', 0)
        metrics['fragile_notional'] = fragile.get('total_notional', 0)
        
        # Position alerts
        alerts = data.get('position_alerts', {})
        metrics['alert_signal'] = alerts.get('signal', 'N/A')
        metrics['alert_count'] = alerts.get('alert_count', 0)
        metrics['smart_long_alerts'] = alerts.get('smart_long', 0)
        metrics['smart_short_alerts'] = alerts.get('smart_short', 0)
        
        # Liquidation heatmap
        liq = data.get('liquidation_heatmap', {})
        metrics['nearest_long_liq'] = liq.get('nearest_long_liq')
        metrics['nearest_short_liq'] = liq.get('nearest_short_liq')
        metrics['long_pain'] = liq.get('long_total_pnl', 0)
        metrics['short_pain'] = liq.get('short_total_pnl', 0)
        metrics['smart_long_pnl'] = liq.get('smart_long_pnl', 0)
        metrics['smart_short_pnl'] = liq.get('smart_short_pnl', 0)
        metrics['liq_suggested_direction'] = liq.get('suggested_direction', 'N/A')
        
        # Spread
        spread = data.get('spread', {})
        metrics['spread_bps'] = spread.get('spread_bps', 0)
        metrics['spread_zscore'] = spread.get('zscore', 0)
        metrics['spread_signal'] = spread.get('signal', 'N/A')

        # Worker outputs (trend/VP/SR)
        trend_state = data.get('trend_state', {})
        if isinstance(trend_state, dict):
            metrics['trend_state'] = trend_state

        volume_profile = data.get('volume_profile', {})
        if isinstance(volume_profile, dict):
            metrics['volume_profile'] = volume_profile

        sr_levels = data.get('sr_levels', {})
        if isinstance(sr_levels, dict):
            metrics['sr_levels'] = sr_levels
        
        # Note: top-level ofm, cvd, signals fields removed from tracker SSE
        # (perp_signals.ofm and perp_signals.cvd still available)
        
        # Max z-score
        metrics['max_z'] = signal_scores.get('max_z', 0)
        
        return metrics

    def rank_opportunities(self, all_data: Dict[str, Dict], must_include_symbols: Optional[set] = None) -> List[ScoredOpportunity]:
        """
        Score all symbols and return ranked list of opportunities.
        
        Args:
            all_data: Full SSE data (all 210 symbols)
            must_include_symbols: Symbols to include even if below z-score threshold
            
        Returns:
            List of ScoredOpportunity sorted by score (highest first)
        """
        must_include_symbols = must_include_symbols or set()
        opportunities = []
        
        for symbol, data in all_data.items():
            try:
                # Skip z-score filter for open positions
                skip_z = symbol in must_include_symbols
                opp = self.score_symbol(symbol, data, skip_z_filter=skip_z)
                if opp:
                    opportunities.append(opp)
            except Exception as e:
                self.log.warning(f"Error scoring {symbol}: {e}")
                
        # Sort by score descending
        opportunities.sort(key=lambda x: x.score, reverse=True)
        
        return opportunities
    
    def select_top_n(
        self, 
        all_data: Dict[str, Dict], 
        n: int = 10,
        min_score: float = 40.0,
        must_include_symbols: Optional[set] = None,
    ) -> List[ScoredOpportunity]:
        """
        Select top N opportunities meeting minimum score.
        
        Args:
            all_data: Full SSE data
            n: Maximum number to return
            min_score: Minimum score threshold (0-100)
            must_include_symbols: Symbols to always include (e.g., open positions)
            
        Returns:
            List of top N ScoredOpportunity
        """
        must_include_symbols = must_include_symbols or set()
        # Backwards-compatible call: unit tests often monkeypatch rank_opportunities
        # with a lambda that only accepts (all_data).
        try:
            ranked = self.rank_opportunities(all_data, must_include_symbols=must_include_symbols)
        except TypeError:
            ranked = self.rank_opportunities(all_data)

        # Gate 1 (hard allowlist): only consider symbols in the cached dual-venue universe.
        # Falls back gracefully if the cache cannot be loaded.
        # NOTE: HIP3 symbols (xyz: prefix) bypass this gate - they are HL-only and valid.
        # NOTE: avoid asyncio.run() here (select_top_n is called from both sync + async
        # paths; asyncio.run() will crash if an event loop is already running).
        try:
            dual_symbols = set(load_dual_venue_symbols_cached(ttl_days=7))
        except Exception:
            dual_symbols = set()
        if dual_symbols:
            filtered_ranked = [
                opp for opp in ranked
                if opp.symbol in dual_symbols or str(opp.symbol).lower().startswith("xyz:")
            ]
            if filtered_ranked:
                ranked = filtered_ranked

        # Prefilter symbols that are not tradable on both venues (best-effort from snapshot mids).
        # NOTE: HIP3 symbols (xyz: prefix) bypass this gate - they are HL-only and valid.
        # HL-only mode: skip dual-venue gate.
        hl_only_env = str(
            env_str("EVCLAW_HL_ONLY", "0")
        ).strip().lower()
        enabled_venues_new = str(
            env_str("EVCLAW_ENABLED_VENUES", "hyperliquid,hip3")
        ).strip()
        hl_only = hl_only_env in ("1", "true", "yes")
        if enabled_venues_new:
            enabled_list = parse_enabled_venues(enabled_venues_new)
            if enabled_list and "lighter" not in enabled_list:
                hl_only = True

        if not hl_only:
            ranked = [
                opp for opp in ranked
                if str(opp.symbol).lower().startswith("xyz:") or self._symbol_on_both_venues(opp.symbol, opp.raw_data)
            ]
        
        # Mandatory include: symbols with strong perp signals for this cycle
        mandatory = [opp for opp in ranked if opp.strong_signals]
        mandatory_symbols = {opp.symbol for opp in mandatory}

        # Also include symbols we have open positions in (always monitor)
        position_opps = [opp for opp in ranked if opp.symbol in must_include_symbols and opp.symbol not in mandatory_symbols]
        position_symbols = {opp.symbol for opp in position_opps}
        
        # Fill remaining slots by score threshold
        filtered = [
            opp for opp in ranked
            if opp.symbol not in mandatory_symbols 
            and opp.symbol not in position_symbols
            and opp.score >= min_score
        ]

        selected = mandatory + position_opps + filtered
        limit = max(n, len(mandatory) + len(position_opps))
        return selected[:limit]


# =============================================================================
# Compact Formatter
# =============================================================================

class CompactFormatter:
    """
    Formats symbol data into compact, information-dense text.
    
    Target: ~3,000 tokens per symbol (vs 103k raw)
    """
    
    def __init__(self):
        self.log = get_logger("formatter")
    
    def format_number(self, n: float, decimals: int = 1) -> str:
        """Format number with K/M/B suffixes."""
        if n is None:
            return "N/A"
        if abs(n) >= 1_000_000_000:
            return f"${n/1_000_000_000:.{decimals}f}B"
        if abs(n) >= 1_000_000:
            return f"${n/1_000_000:.{decimals}f}M"
        if abs(n) >= 1_000:
            return f"${n/1_000:.{decimals}f}K"
        return f"${n:.{decimals}f}"
    
    def format_pct(self, p: float) -> str:
        """Format percentage."""
        if p is None:
            return "N/A"
        return f"{p:+.1f}%" if p != 0 else "0%"
    
    def format_opportunity(self, opp: ScoredOpportunity) -> str:
        """
        Format a scored opportunity into compact text.
        
        Target: ~3,000 tokens per symbol with rich data
        """
        m = opp.key_metrics
        lines = []
        
        # Header
        price = m.get('price', 0)
        atr = m.get('atr_pct', 0)
        vol = m.get('volume_24h', 0)
        pct_24h = m.get('pct_24h', 0)
        
        dir_emoji = "🟢" if opp.direction == 'LONG' else "🔴" if opp.direction == 'SHORT' else "⚪"
        lines.append(f"### {opp.symbol} — Score: {opp.score:.0f}/100 {dir_emoji} {opp.direction}")
        lines.append(f"Price: ${price:,.2f} | ATR: {atr:.1f}% | Vol: {self.format_number(vol)}/24h | Chg: {self.format_pct(pct_24h)}")
        lines.append("")
        
        # Signals section - compact but complete
        lines.append("**Perp Signals:**")
        sig_parts = []
        for sig_name, sig_data in opp.signals.items():
            d = sig_data.get('direction', 'N')[0]  # L/S/N
            z = sig_data.get('z_score', 0)
            if sig_name == 'cvd':
                zs = sig_data.get('z_smart', 0)
                zd = sig_data.get('z_dumb', 0)
                sig_parts.append(f"CVD={d}(zS:{zs:.1f}/zD:{zd:.1f})")
            elif sig_name in ('whale', 'dead_capital'):
                strength = sig_data.get('strength', 0)
                name = "WHALE" if sig_name == 'whale' else "DEAD"
                sig_parts.append(f"{name}={d}(str:{strength:.1f})")
            else:
                sig_parts.append(f"{sig_name.upper()}={d}(z:{z:.1f})")
        lines.append(" | ".join(sig_parts))
        
        # Consensus
        long_count = sum(1 for s in opp.signals.values() if s.get('direction') == 'LONG')
        short_count = sum(1 for s in opp.signals.values() if s.get('direction') == 'SHORT')
        neutral_count = len(opp.signals) - long_count - short_count
        lines.append(f"Consensus: {long_count}L/{short_count}S/{neutral_count}N")

        # Worker signals (trend/VP/SR)
        def _fmt_num(val: Any, decimals: int = 2) -> str:
            try:
                return f"{float(val):.{decimals}f}"
            except (TypeError, ValueError):
                return "N/A"

        def _fmt_price(val: Any) -> str:
            try:
                return f"{float(val):.4f}"
            except (TypeError, ValueError):
                return "N/A"

        worker_parts: List[str] = []
        trend_state = m.get("trend_state") or {}
        if isinstance(trend_state, dict) and trend_state:
            t_score = _fmt_num(trend_state.get("trend_score"), 1)
            t_dir = trend_state.get("direction", "N/A")
            t_regime = trend_state.get("regime", "N/A")
            t_sig = trend_state.get("signal", {}) or {}
            t_sig_dir = t_sig.get("direction", "N/A")
            t_sig_z = _fmt_num(t_sig.get("z_score"), 2)
            worker_parts.append(f"TREND: {t_dir}/{t_regime} score={t_score} z={t_sig_dir}:{t_sig_z}")

        volume_profile = m.get("volume_profile") or {}
        if isinstance(volume_profile, dict):
            tfs_raw = volume_profile.get("timeframes", {})
            tfs = tfs_raw if isinstance(tfs_raw, dict) else {}
            chosen = None
            for tf_name in ("session_8h", "daily_24h", "weekly_7d"):
                if tf_name in tfs:
                    chosen = (tf_name, tfs.get(tf_name, {}))
                    break
            if chosen:
                tf_name, tf = chosen
                if isinstance(tf, dict):
                    sig = tf.get("signal", {}) or {}
                    vp_dir = sig.get("direction", "N/A")
                    vp_z = _fmt_num(sig.get("z_score"), 2)
                    poc = _fmt_price(tf.get("poc"))
                    vah = _fmt_price(tf.get("vah"))
                    val = _fmt_price(tf.get("val"))
                    dist = _fmt_num(tf.get("distance_to_poc"), 3)
                    worker_parts.append(
                        f"VP: {tf_name} {vp_dir} z={vp_z} POC={poc} VAH={vah} VAL={val} dist={dist}"
                    )

        sr_levels = m.get("sr_levels") or {}
        if isinstance(sr_levels, dict) and sr_levels:
            nearest = sr_levels.get("nearest", {}) or {}
            support = nearest.get("support") if isinstance(nearest, dict) else None
            resistance = nearest.get("resistance") if isinstance(nearest, dict) else None
            s_price = _fmt_price(support.get("price")) if isinstance(support, dict) else "N/A"
            s_z = _fmt_num(support.get("strength_z"), 2) if isinstance(support, dict) else "N/A"
            r_price = _fmt_price(resistance.get("price")) if isinstance(resistance, dict) else "N/A"
            r_z = _fmt_num(resistance.get("strength_z"), 2) if isinstance(resistance, dict) else "N/A"
            sr_sig = sr_levels.get("signal", {}) or {}
            sr_dir = sr_sig.get("direction", "N/A")
            sr_z = _fmt_num(sr_sig.get("z_score"), 2)
            worker_parts.append(f"SR: S@{s_price}(z={s_z}) R@{r_price}(z={r_z}) signal={sr_dir} z={sr_z}")

        if worker_parts:
            lines.append("**Worker Signals:**")
            lines.append(" | ".join(worker_parts))

        lines.append("")
        
        # Position Structure
        net_bias = m.get('net_bias', 'N/A')
        net_bias_pct = m.get('net_bias_pct', 50)
        hot_pct = m.get('hot_zone_pct', 0)
        avg_lev = m.get('avg_leverage', 0)
        longs = m.get('long_count', 0)
        shorts = m.get('short_count', 0)
        total_long_exp = m.get('total_long_exposure', 0)
        total_short_exp = m.get('total_short_exposure', 0)
        market_oi = m.get('market_oi', 0)
        
        lines.append("**Position Structure:**")
        lines.append(f"Bias: {net_bias} ({net_bias_pct:.0f}%) | Positions: {longs:,}L/{shorts:,}S")
        lines.append(f"Exposure: {self.format_number(total_long_exp)}L / {self.format_number(total_short_exp)}S | OI: {self.format_number(market_oi)}")
        lines.append(f"Hot zone: {hot_pct:.1f}% | Avg leverage: {avg_lev:.1f}x | Danger: {m.get('in_danger_threshold', 0):.1f}%")
        
        # Fragile wallets
        frag_count = m.get('fragile_count', 0)
        frag_notional = m.get('fragile_notional', 0)
        if frag_count > 0:
            lines.append(f"Fragile wallets: {frag_count} ({self.format_number(frag_notional)})")
        lines.append("")
        
        # Smart vs Dumb Money (cohort + CVD)
        smart_cvd = m.get('smart_cvd', 0)
        dumb_cvd = m.get('dumb_cvd', 0)
        div_z = m.get('divergence_z', 0)
        div_sig = m.get('divergence_signal', 'N/A')
        cohort_sig = m.get('cohort_signal', 'N/A')
        smart_vs_dumb = m.get('smart_vs_dumb_delta', 0)
        whale_vs_retail = m.get('whale_vs_retail_delta', 0)
        
        lines.append("**Smart vs Dumb Money:**")
        lines.append(f"Smart CVD: {self.format_number(smart_cvd)} | Dumb CVD: {self.format_number(dumb_cvd)}")
        lines.append(f"Divergence: z={div_z:.1f} ({div_sig}) | Cohort: {cohort_sig}")
        lines.append(f"Smart/Dumb Δ: {smart_vs_dumb:+.2f} | Whale/Retail Δ: {whale_vs_retail:+.2f}")
        lines.append("")
        
        # Note: Order Flow Metrics (new_longs, cvd_5m, tfi) removed from tracker SSE
        
        # Funding
        funding_rate = m.get('funding_rate', 0)
        funding_dir = m.get('funding_direction', 'N/A')
        funding_ann = m.get('funding_annualized', 0)
        spread_bps = m.get('spread_bps', 0)
        spread_z = m.get('spread_zscore', 0)
        
        lines.append("**Funding & Spread:**")
        lines.append(f"Funding: {funding_rate:.2f}bps ({funding_dir}) | Ann: {funding_ann:.1f}%")
        lines.append(f"Spread: {spread_bps:.2f}bps (z={spread_z:.1f})")
        lines.append("")
        
        # Liquidation Edge
        nearest_long = m.get('nearest_long_liq')
        nearest_short = m.get('nearest_short_liq')
        long_pain = m.get('long_pain', 0)
        short_pain = m.get('short_pain', 0)
        smart_long_pnl = m.get('smart_long_pnl', 0)
        smart_short_pnl = m.get('smart_short_pnl', 0)
        liq_dir = m.get('liq_suggested_direction', 'N/A')
        
        lines.append("**Liquidation Edge:**")
        if nearest_long and nearest_short and price > 0:
            long_dist = ((nearest_long - price) / price) * 100 if nearest_long else 0
            short_dist = ((nearest_short - price) / price) * 100 if nearest_short else 0
            lines.append(f"Nearest liq: {long_dist:+.1f}% (longs) / {short_dist:+.1f}% (shorts)")
        lines.append(f"Pain: {self.format_number(long_pain)}L / {self.format_number(short_pain)}S | Suggested: {liq_dir}")
        lines.append(f"Smart P&L: {self.format_number(smart_long_pnl)}L / {self.format_number(smart_short_pnl)}S")
        lines.append("")
        
        # Position Alerts
        alert_count = m.get('alert_count', 0)
        alert_sig = m.get('alert_signal', 'N/A')
        smart_long_alerts = m.get('smart_long_alerts', 0)
        smart_short_alerts = m.get('smart_short_alerts', 0)
        
        if alert_count > 0:
            lines.append(f"**Alerts:** {alert_count} ({alert_sig}) | Smart: {smart_long_alerts}L/{smart_short_alerts}S")
            lines.append("")
        
        return "\n".join(lines)
    
    def format_market_overview(self, all_data: Dict[str, Dict]) -> str:
        """Format market overview from BTC/ETH/SOL."""
        lines = ["## Market Overview"]
        
        for symbol in ['BTC', 'ETH', 'SOL']:
            data = all_data.get(symbol, {})
            if not data:
                continue
                
            price = data.get('price', 0)
            atr = data.get('atr', {}).get('atr_pct', 0)
            funding = data.get('funding', {})
            summary = data.get('summary', {})
            
            fund_bps = funding.get('rate_bps', 0)
            fund_dir = funding.get('direction', 'NEUTRAL')
            net_bias = summary.get('net_bias', 'N/A')
            
            lines.append(f"**{symbol}:** ${price:,.0f} | ATR: {atr:.1f}% | Bias: {net_bias} | Funding: {fund_bps:.2f}bps ({fund_dir})")
            
        return "\n".join(lines)
    
    def format_safety_state(self, safety_state: Dict) -> str:
        """Format safety/risk state."""
        tier = safety_state.get('current_tier', 1)
        tier_names = {1: "NORMAL", 2: "CAUTION", 3: "DEFENSIVE", 4: "HALT", 5: "EMERGENCY"}
        tier_name = tier_names.get(tier, f"TIER_{tier}")
        
        daily_pnl = safety_state.get('daily_pnl', 0)
        cons_losses = safety_state.get('consecutive_losses', safety_state.get('loss_streak', 0))
        exposure = safety_state.get('current_exposure', safety_state.get('total_exposure', 0))
        max_exposure = safety_state.get('max_exposure', 10000)
        
        lines = [
            "## Safety Status",
            f"Tier: {tier}/5 ({tier_name}) | Daily P&L: ${daily_pnl:+.2f} | Streak: {cons_losses} losses",
            f"Exposure: ${exposure:,.0f}/${max_exposure:,.0f}"
        ]
        
        return "\n".join(lines)
    
    def format_active_positions(self, positions: List[Dict]) -> str:
        """Format active positions."""
        if not positions:
            return "## Active Positions\nNone"
            
        lines = ["## Active Positions"]
        
        for pos in positions:
            symbol = pos.get('symbol', '?')
            direction = pos.get('direction', '?')
            entry = pos.get('entry_price', 0)
            size = pos.get('notional_usd', 0)
            pnl = pos.get('unrealized_pnl', 0)
            age = pos.get('age_hours', 0)
            
            pnl_pct = (pnl / size * 100) if size > 0 else 0
            emoji = "📈" if pnl > 0 else "📉"
            
            lines.append(f"{emoji} **{symbol} {direction}**: Entry ${entry:,.2f} | Size: ${size:,.0f} | P&L: ${pnl:+.2f} ({pnl_pct:+.1f}%) | Age: {age:.1f}h")
            
            # Entry context
            signals = pos.get('entry_signals', [])
            if signals:
                lines.append(f"   Entry signals: {', '.join(signals)}")
                
        return "\n".join(lines)


# =============================================================================
# Cycle Memory
# =============================================================================

class CycleMemory:
    """
    Manages rolling memory between decision cycles.
    
    Stores:
    - Recent cycle records (opportunities, decisions)
    - Active positions with context
    - Signal performance stats
    - AI reflections
    """
    
    def __init__(self, memory_dir: Path):
        self.memory_dir = Path(memory_dir)
        self.memory_dir.mkdir(parents=True, exist_ok=True)
        self.log = get_logger("memory")
        
        # File paths
        self.cycle_state_path = self.memory_dir / "cycle_state.json"
        self.rolling_context_path = self.memory_dir / "rolling_context.jsonl"
        self.positions_path = self.memory_dir / "active_positions.json"
        self.performance_path = self.memory_dir / "signal_performance.json"
        self.reflections_path = self.memory_dir / "reflections.md"
        
        # Keep last N cycles
        self.max_cycles = 10  # ~2.5 hours at 15min/cycle
    
    def load_context(self) -> Dict:
        """
        Load context from previous cycles.
        
        Returns dict with:
        - recent_cycles: List of recent cycle dicts
        - active_positions: List of position dicts
        - signal_performance: Performance stats
        - reflections: Recent AI reflections
        """
        context = {
            'recent_cycles': [],
            'active_positions': [],
            'signal_performance': {},
            'reflections': []
        }
        
        # Load rolling context
        if self.rolling_context_path.exists():
            try:
                lines = self.rolling_context_path.read_text().strip().split('\n')
                for line in lines[-self.max_cycles:]:
                    if line:
                        context['recent_cycles'].append(json.loads(line))
            except Exception as e:
                self.log.warning(f"Error loading rolling context: {e}")
        
        # Load active positions
        if self.positions_path.exists():
            try:
                context['active_positions'] = json.loads(self.positions_path.read_text())
            except Exception as e:
                self.log.warning(f"Error loading positions: {e}")
        
        # Load performance stats
        if self.performance_path.exists():
            try:
                context['signal_performance'] = json.loads(self.performance_path.read_text())
            except Exception as e:
                self.log.warning(f"Error loading performance: {e}")
        
        # Load reflections
        if self.reflections_path.exists():
            try:
                text = self.reflections_path.read_text()
                # Parse reflections (format: - YYYY-MM-DD: text)
                for line in text.split('\n'):
                    if line.startswith('- '):
                        context['reflections'].append(line[2:])
                # Keep last 10
                context['reflections'] = context['reflections'][-10:]
            except Exception as e:
                self.log.warning(f"Error loading reflections: {e}")
        
        return context
    
    def format_for_context(self, memory_context: Dict) -> str:
        """Format loaded memory context for inclusion in prompt."""
        lines = []
        
        # Recent cycles
        cycles = memory_context.get('recent_cycles', [])
        if cycles:
            lines.append("## Previous Cycles")
            for c in cycles[-5:]:  # Last 5 cycles
                cycle_id = c.get('cycle_id', '?')
                top = c.get('top_opportunities', [])[:3]
                decisions = c.get('decisions', [])
                market = c.get('market_state', '?')
                
                dec_str = ", ".join(d.get('action', '?') for d in decisions) if decisions else "no action"
                lines.append(f"[{cycle_id}] Top: {', '.join(top)} | {dec_str} | Market: {market}")
            lines.append("")
        
        # Signal performance
        perf = memory_context.get('signal_performance', {})
        if perf:
            lines.append("## Signal Performance (7d)")
            lines.append("| Combo | Trades | Win% | Avg P&L |")
            lines.append("|-------|--------|------|---------|")
            for combo, stats in list(perf.items())[:10]:
                trades = stats.get('trades', 0)
                win_rate = stats.get('win_rate', 0) * 100
                avg_pnl = stats.get('avg_pnl', 0)
                lines.append(f"| {combo} | {trades} | {win_rate:.0f}% | ${avg_pnl:.2f} |")
            lines.append("")
        
        # Reflections
        reflections = memory_context.get('reflections', [])
        if reflections:
            lines.append("## AI Reflections")
            for r in reflections[-5:]:
                lines.append(f"- {r}")
            lines.append("")
        
        return "\n".join(lines)


# =============================================================================
# Context Builder V2
# =============================================================================

class ContextBuilderV2:
    """
    Builds full trading context for Captain EVP AI.
    
    Combines:
    - Top N opportunities (scored and compressed)
    - Market overview (BTC/ETH/SOL)
    - Safety state
    - Active positions
    - Rolling memory from previous cycles
    - Signal performance stats
    
    Target: ~100k tokens (50% of 200k context)
    """
    
    def __init__(
        self,
        db_path: str,
        memory_dir: Optional[Path] = None,
        signals_dir: Optional[Path] = None,
        max_opportunities: int = 10,
        min_score: float = 35.0
    ):
        """
        Initialize context builder.
        
        Args:
            db_path: Path to ai_trader.db
            memory_dir: Path to memory directory (default: ./memory)
            max_opportunities: Maximum opportunities to include
            min_score: Minimum score threshold
        """
        self.db_path = db_path
        self.memory_dir = Path(memory_dir) if memory_dir else Path(__file__).parent / "memory"
        default_signals = EVCLAW_SIGNALS_DIR
        self.signals_dir = Path(signals_dir) if signals_dir else Path(default_signals)
        self.hip3_state_url = EVCLAW_TRACKER_HIP3_PREDATOR_URL or ""
        self.max_opportunities = max_opportunities
        self.min_score = min_score
        
        self.log = get_logger("context_builder_v2")

        # Initialize components
        self.scorer = OpportunityScorer()
        self.formatter = CompactFormatter()
        self.memory = CycleMemory(self.memory_dir)
        self.db = AITraderDB(str(self.db_path)) if self.db_path else None
        
        # Path to positions file
        self.positions_file = self.memory_dir / "positions.yaml"
    
    def load_active_positions(self, sse_data: Optional[Dict[str, Dict]] = None) -> List[Dict]:
        """
        Load active positions from SQLite (source of truth); fallback to positions.yaml.
        
        Returns:
            List of position dicts for formatting
        """
        if self.db is None:
            self.log.warning(
                "SQLite not configured; falling back to positions.yaml (debug-only)"
            )
            return self._load_positions_from_yaml()

        try:
            open_trades = self.db.get_open_trades()
            active_positions = []
            for trade in open_trades:
                mid_price = self._derive_mid_price(trade.symbol, sse_data)
                unrealized = 0.0
                if mid_price > 0 and trade.entry_price > 0:
                    if trade.direction.upper() == "LONG":
                        unrealized = (mid_price - trade.entry_price) * trade.size
                    else:
                        unrealized = (trade.entry_price - mid_price) * trade.size

                active_positions.append({
                    'symbol': trade.symbol,
                    'direction': trade.direction,
                    'entry_price': trade.entry_price,
                    'notional_usd': trade.size * trade.entry_price,
                    'unrealized_pnl': unrealized,
                    'age_hours': self._calculate_age_hours(trade.entry_time),
                    'entry_signals': trade.get_signals_agreed_list(),
                    'venue': getattr(trade, "venue", ""),
                })

            self.log.info(f"Loaded {len(active_positions)} active positions from SQLite")
            return active_positions
        except Exception as e:
            self.log.error(f"Error loading positions from SQLite: {e}")
            self.log.warning("Falling back to positions.yaml after SQLite error")
            return self._load_positions_from_yaml()

    def _load_positions_from_yaml(self) -> List[Dict]:
        if not self.positions_file.exists():
            self.log.debug(f"No positions file found at {self.positions_file}")
            return []

        try:
            import yaml
            with open(self.positions_file, 'r') as f:
                positions_dict = yaml.safe_load(f) or {}

            active_positions = []
            for symbol, pos_data in positions_dict.items():
                if pos_data.get('state') != 'ACTIVE':
                    continue

                active_positions.append({
                    'symbol': symbol,
                    'direction': pos_data.get('direction', '?'),
                    'entry_price': pos_data.get('entry_price', 0),
                    'notional_usd': pos_data.get('size', 0) * pos_data.get('entry_price', 0),
                    'unrealized_pnl': pos_data.get('unrealized_pnl', 0),
                    'age_hours': self._calculate_age_hours(pos_data.get('opened_at', '')),
                    'entry_signals': pos_data.get('signals_agreeing', []),
                    'venue': pos_data.get('venue', ''),
                })

            self.log.info(f"Loaded {len(active_positions)} active positions from {self.positions_file}")
            return active_positions
        except Exception as e:
            self.log.error(f"Error loading positions from {self.positions_file}: {e}")
            return []
    
    def _calculate_age_hours(self, opened_at: Any) -> float:
        """Calculate position age in hours from ISO timestamp or epoch seconds."""
        if not opened_at:
            return 0.0
        
        try:
            if isinstance(opened_at, (int, float)):
                opened = datetime.fromtimestamp(float(opened_at), tz=timezone.utc)
            else:
                opened = datetime.fromisoformat(str(opened_at).replace('Z', '+00:00'))
            now = datetime.now(timezone.utc)
            delta = now - opened
            return delta.total_seconds() / 3600
        except Exception:
            return 0.0

    def _sse_symbol_variants(self, symbol: Any) -> List[str]:
        """Return possible SSE keys for a symbol (handles HIP3 xyz: prefix case)."""
        sym = str(symbol or "")
        if not sym:
            return []
        variants: List[str] = [sym]
        sym_u = sym.upper()
        if sym_u not in variants:
            variants.append(sym_u)
        if sym_u.startswith("XYZ:"):
            alt = f"xyz:{sym_u.split(':', 1)[1]}"
            if alt not in variants:
                variants.append(alt)
        return variants

    def _lookup_symbol_data(self, sse_data: Optional[Dict[str, Dict]], symbol: Any) -> Dict[str, Any]:
        """Find symbol data in SSE snapshot using best-effort key matching."""
        if not sse_data:
            return {}
        for key in self._sse_symbol_variants(symbol):
            data = sse_data.get(key)
            if isinstance(data, dict):
                return data
        return {}
    
    def _derive_mid_price(self, symbol: str, sse_data: Optional[Dict[str, Dict]]) -> float:
        """Best-effort mid price from SSE snapshot."""
        if not sse_data:
            return 0.0
        data = self._lookup_symbol_data(sse_data, symbol)
        try:
            price = float(data.get('price') or 0.0)
        except (TypeError, ValueError):
            price = 0.0

        if price > 0:
            return price

        def _safe_float(val: Any) -> float:
            try:
                return float(val)
            except (TypeError, ValueError):
                return 0.0

        best_bid = _safe_float(data.get('best_bid') or data.get('bid'))
        best_ask = _safe_float(data.get('best_ask') or data.get('ask'))
        if best_bid > 0 and best_ask > 0:
            return (best_bid + best_ask) / 2
        return 0.0

    def _extract_entry_signals(self, raw: Any) -> List[str]:
        """Best-effort parse of entry signal payloads from DB fields."""
        payload = raw
        if isinstance(payload, str):
            text = payload.strip()
            if not text:
                return []
            try:
                payload = json.loads(text)
            except json.JSONDecodeError:
                return []

        if isinstance(payload, list):
            out: List[str] = []
            for item in payload:
                s = str(item or "").strip()
                if s:
                    out.append(s)
            return out

        if isinstance(payload, dict):
            return [str(k) for k, v in payload.items() if v]

        return []

    def _safe_load_json(self, path: Path) -> Optional[Dict[str, Any]]:
        if not path.exists():
            return None
        try:
            return json.loads(path.read_text())
        except Exception as e:
            self.log.debug(f"Failed to load {path}: {e}")
            return None

    def _safe_load_json_url(self, url: str) -> Optional[Dict[str, Any]]:
        if not url:
            return None
        try:
            request = urllib.request.Request(url, headers={"User-Agent": "EVClawContextBuilder/1.0"})
            with urllib.request.urlopen(request, timeout=5) as response:
                raw = response.read()
                if not raw:
                    return None
                return json.loads(raw.decode("utf-8", errors="replace"))
        except (OSError, urllib.error.URLError, ValueError) as e:
            self.log.debug(f"Failed to load HIP3 state from URL {url}: {e}")
            return None

    def _load_hip3_state(self) -> Optional[Dict[str, Any]]:
        if self.hip3_state_url:
            payload = self._safe_load_json_url(self.hip3_state_url)
            if payload is not None:
                return payload
        return None

    def merge_worker_signals(self, sse_data: Dict[str, Dict]) -> None:
        """Merge worker outputs (trend/VP/SR) into per-symbol SSE data."""
        if not isinstance(sse_data, dict) or not sse_data:
            return

        def _merge(symbols_map: Any, field: str, transform=None) -> None:
            if not isinstance(symbols_map, dict):
                return
            for symbol, payload in symbols_map.items():
                if not isinstance(payload, dict):
                    continue
                target_payload = transform(payload) if callable(transform) else payload
                for key in self._sse_symbol_variants(symbol):
                    if key in sse_data:
                        sse_data[key][field] = target_payload
                        break

        trend_path = self.signals_dir / "trend_state.json"
        trend_data = self._safe_load_json(trend_path) or {}
        _merge((trend_data.get("symbols", {}) if isinstance(trend_data, dict) else {}), "trend_state")

        vp_path = self.signals_dir / "volume_profile.json"
        vp_data = self._safe_load_json(vp_path) or {}
        _merge((vp_data.get("symbols", {}) if isinstance(vp_data, dict) else {}), "volume_profile")

        sr_path = self.signals_dir / "sr_levels.json"
        sr_data = self._safe_load_json(sr_path) or {}

        def _slim_sr(payload: Dict[str, Any]) -> Dict[str, Any]:
            slim = dict(payload)
            slim.pop("levels", None)
            return slim

        _merge((sr_data.get("symbols", {}) if isinstance(sr_data, dict) else {}), "sr_levels", transform=_slim_sr)

        # HIP3 Predator signals (FLOW + OFM for xyz: symbols)
        hip3_data = self._load_hip3_state() or {}
        hip3_symbols = hip3_data.get("symbols", {}) if isinstance(hip3_data, dict) else {}
        if isinstance(hip3_symbols, dict):
            for symbol, payload in hip3_symbols.items():
                if not isinstance(payload, dict):
                    continue
                for key in self._sse_symbol_variants(symbol):
                    if key in sse_data and "hip3_predator" not in sse_data[key]:
                        sse_data[key]["hip3_predator"] = payload
                        break

    def augment_dead_capital_learning(self, sse_data: Dict[str, Dict]) -> None:
        """Attach internal dead_capital stats (imbalance z-score) onto sse_data in-place.

        Notes:
        - Uses ai_trader.db as persistent learning store.
        - Safe no-op if DB isn't configured.
        - Does NOT decide trades; it only enriches context/scoring.

        IMPORTANT: cycle_trigger builds both TXT + JSON contexts in the same Python
        process. This function may be called multiple times per snapshot. To avoid
        double-counting samples, we make it idempotent per (process, sse_data) for
        a short window.
        """
        if self.db is None:
            return
        if not isinstance(sse_data, dict) or not sse_data:
            return

        now_ts = time.time()

        try:
            last_ref = getattr(self, "_dead_capital_last_symbols_ref", None)
            last_ts = float(getattr(self, "_dead_capital_last_update_ts", 0.0) or 0.0)
        except Exception:
            last_ref = None
            last_ts = 0.0

        # If we already augmented this exact symbols dict very recently, skip.
        if last_ref is sse_data and (now_ts - last_ts) < 120.0:
            return

        self._dead_capital_last_symbols_ref = sse_data
        self._dead_capital_last_update_ts = now_ts

        for symbol, data in sse_data.items():
            if not isinstance(data, dict):
                continue
            perp = data.get('perp_signals')
            if not isinstance(perp, dict):
                continue
            dead = perp.get('dead_capital')
            if not isinstance(dead, dict) or not dead:
                continue

            # Only learn when we have the locked_* magnitudes.
            if 'locked_long_pct' not in dead and 'locked_short_pct' not in dead:
                continue

            try:
                locked_long = float(dead.get('locked_long_pct', 0) or 0)
            except (TypeError, ValueError):
                locked_long = 0.0
            try:
                locked_short = float(dead.get('locked_short_pct', 0) or 0)
            except (TypeError, ValueError):
                locked_short = 0.0

            imbalance = locked_short - locked_long

            # Dead-capital is currently Hyperliquid-sourced.
            stats = self.db.update_dead_capital_imbalance_stats(
                symbol=str(symbol).upper(),
                venue='hyperliquid',
                ts=now_ts,
                imbalance=imbalance,
                override_z=3.5,
            )

            # Attach learning outputs for downstream scoring/brain.
            dead['imbalance'] = imbalance
            dead['z_imbalance'] = stats.get('z', 0.0)
            dead['z_abs_imbalance'] = stats.get('z_abs', 0.0)
            dead['override_streak'] = stats.get('override_streak', 0)
            dead['override_trigger'] = bool(stats.get('override_trigger', False))
            dead['samples'] = stats.get('n', 0)
            dead['mean_imbalance'] = stats.get('mean', 0.0)
            dead['std_imbalance'] = stats.get('std', 0.0)

    def build_full_context(
        self,
        sse_data: Dict[str, Dict],
        safety_state: Optional[Dict] = None,
        active_positions: Optional[List[Dict]] = None
    ) -> str:
        """
        Build complete trading context for AI decision making.
        
        Args:
            sse_data: Full SSE data (all 210 symbols)
            safety_state: Current safety/risk state
            active_positions: List of active position dicts (auto-loaded if None)
            
        Returns:
            Formatted context string (~100k tokens)
        """
        if safety_state is None:
            safety_state = {'current_tier': 1, 'daily_pnl': 0, 'consecutive_losses': 0}
        if active_positions is None:
            # Auto-load positions from positions.yaml
            active_positions = self.load_active_positions(sse_data=sse_data)

        # Merge worker outputs (trend/VP/SR) if present.
        self.merge_worker_signals(sse_data)

        # Learning augmentation (dead_capital internal z-score over time)
        self.augment_dead_capital_learning(sse_data)
        
        sections = []
        
        # Header
        timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        cycle_id = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:00Z")
        sections.append(f"# Captain EVP Trading Context")
        sections.append(f"Generated: {timestamp} | Symbols analyzed: {len(sse_data)}")
        sections.append("")
        
        # Safety state
        sections.append(self.formatter.format_safety_state(safety_state))
        sections.append("")
        
        # Market overview
        sections.append(self.formatter.format_market_overview(sse_data))
        sections.append("")

        # Global context (Deribit IV + net exposure)
        if _global_context_provider is not None:
            try:
                gc = _global_context_provider.get_global_context()
                compact = gc.format_compact()
                if compact and compact.strip() and compact.strip().lower() != "global context unavailable":
                    sections.append("## Global Context")
                    sections.append(compact)
                    sections.append("")
            except Exception as e:
                # Best-effort; never fail context generation
                self.log.warning(f"Failed to get global context: {e}")

        # Active positions
        sections.append(self.formatter.format_active_positions(active_positions))
        sections.append("")
        
        # Load memory context
        memory_context = self.memory.load_context()
        memory_text = self.memory.format_for_context(memory_context)
        if memory_text:
            sections.append(memory_text)
        
        # Get symbols with open positions (always include in context)
        open_position_symbols: set = set()
        for p in active_positions:
            sym = p.get('symbol')
            for key in self._sse_symbol_variants(sym):
                open_position_symbols.add(key)
        
        # Score and select opportunities
        opportunities = self.scorer.select_top_n(
            sse_data, 
            n=self.max_opportunities,
            min_score=self.min_score,
            must_include_symbols=open_position_symbols,
        )
        
        # Format opportunities
        sections.append(f"## Top Opportunities ({len(opportunities)})")
        sections.append("")
        
        for i, opp in enumerate(opportunities, 1):
            sections.append(f"**#{i}**")
            sections.append(self.formatter.format_opportunity(opp))
            sections.append("")
        
        return "\n".join(sections)

    def get_opportunities(self, sse_data: Dict[str, Dict], must_include_symbols: Optional[set] = None) -> List[ScoredOpportunity]:
        """Get scored opportunities without building full context."""
        self.merge_worker_signals(sse_data)
        self.augment_dead_capital_learning(sse_data)
        
        # Get open position symbols from db if not provided
        if must_include_symbols is None and self.db:
            try:
                open_trades = self.db.get_open_trades()
                must_include_symbols = set()
                for t in open_trades:
                    sym = str(getattr(t, 'symbol', '') or '')
                    for key in self._sse_symbol_variants(sym):
                        must_include_symbols.add(key)
            except Exception:
                must_include_symbols = set()
        
        return self.scorer.select_top_n(
            sse_data,
            n=self.max_opportunities,
            min_score=self.min_score,
            must_include_symbols=must_include_symbols or set(),
        )
    
    
    def estimate_tokens(self, context: str) -> int:
        """Estimate token count (rough: chars/4)."""
        return len(context) // 4


# =============================================================================
# Test
# =============================================================================

if __name__ == "__main__":
    import argparse as _argparse
    import sys as _sys

    def _run_cli():
        parser = _argparse.ArgumentParser(
            description="Build filtered trading context from cycle data"
        )
        parser.add_argument(
            "--cycle-file",
            help="Path to cycle JSON file (e.g. <runtime_dir>/evclaw_cycle_9405.json)"
        )
        parser.add_argument(
            "--output", "-o",
            default="-",
            help="Output file path, or '-' for stdout (default: -)"
        )
        parser.add_argument(
            "--json-output",
            default=None,
            help="Optional JSON output path."
        )
        parser.add_argument(
            "--test",
            action="store_true",
            help="Run interactive test (connects to live SSE)"
        )

        args = parser.parse_args()

        if args.test:
            _run_test()
            return

        if not args.cycle_file:
            parser.error("--cycle-file is required (or use --test for interactive test)")

        # Load cycle JSON
        cycle_path = Path(args.cycle_file)
        if not cycle_path.exists():
            print(f"Error: cycle file not found: {args.cycle_file}", file=_sys.stderr)
            _sys.exit(1)

        try:
            with open(cycle_path) as f:
                cycle_data = json.load(f)
        except (json.JSONDecodeError, IOError) as e:
            print(f"Error reading cycle file: {e}", file=_sys.stderr)
            _sys.exit(1)

        # Extract symbols dict from cycle data
        symbols = cycle_data.get("symbols", {})
        if not symbols:
            print("Error: no 'symbols' key found in cycle file", file=_sys.stderr)
            _sys.exit(1)

        seq = cycle_data.get("sequence", "?")
        print(f"Loaded cycle seq={seq}: {len(symbols)} symbols", file=_sys.stderr)

        # Build context
        db_path = env_str("EVCLAW_DB_PATH", EVCLAW_DB_PATH)
        skill_cfg: Dict[str, Any] = {}
        try:
            import yaml as _yaml

            skill_path = Path(__file__).resolve().with_name("skill.yaml")
            if skill_path.exists():
                with open(skill_path, "r") as _f:
                    skill_cfg = _yaml.safe_load(_f) or {}
        except Exception:
            skill_cfg = {}
        context_cfg = ((skill_cfg.get("config") or {}).get("context_builder") or {})
        # Top opportunities width: increase to reduce NO_CANDIDATES when the top list
        # is dominated by existing positions and MIXED signals.
        try:
            max_opps = int(context_cfg.get("max_opportunities", 30) or 30)
        except Exception:
            max_opps = 30
        max_opps = max(5, min(100, max_opps))

        try:
            min_score = float(context_cfg.get("min_score", 35.0) or 35.0)
        except Exception:
            min_score = 35.0

        builder = ContextBuilderV2(
            db_path=str(db_path),
            max_opportunities=max_opps,
            min_score=min_score,
        )

        # Build safety + active positions context from DB (best-effort)
        safety_state = None
        active_positions = None
        try:
            if builder.db is not None:
                ss = builder.db.get_safety_state()
                safety_state = {
                    'current_tier': getattr(ss, 'current_tier', 1),
                    'daily_pnl': float(getattr(ss, 'daily_pnl', 0.0) or 0.0),
                    'consecutive_losses': int(getattr(ss, 'consecutive_losses', 0) or 0),
                }

                snap = builder.db.get_latest_monitor_snapshot()
                if isinstance(snap, dict) and snap:
                    hl_eq = float(snap.get('hl_equity') or snap.get('total_equity') or 0.0)
                    hl_long = float(snap.get('hl_long_notional') or 0.0)
                    hl_short = float(snap.get('hl_short_notional') or 0.0)
                    gross_exposure = max(0.0, hl_long) + max(0.0, hl_short)

                    max_exposure = CB_EXECUTOR_MAX_TOTAL_EXPOSURE_USD

                    safety_state['current_exposure'] = gross_exposure
                    safety_state['max_exposure'] = max_exposure

                # Build active positions (approx) from open trades + current prices
                active_positions = []
                for t in builder.db.get_open_trades():
                    sym = str(getattr(t, 'symbol', '') or '').upper()
                    if not sym:
                        continue
                    direction = str(getattr(t, 'direction', '') or '').upper() or 'UNKNOWN'
                    entry = float(getattr(t, 'entry_price', 0.0) or 0.0)
                    notional = float(getattr(t, 'notional_usd', 0.0) or 0.0)
                    entry_time = float(getattr(t, 'entry_time', 0.0) or 0.0)
                    price = builder._derive_mid_price(sym, symbols)  # noqa: SLF001

                    base_size = (notional / entry) if entry > 0 else 0.0
                    if direction == 'SHORT':
                        pnl = (entry - price) * base_size
                    else:
                        pnl = (price - entry) * base_size

                    age_hours = (time.time() - entry_time) / 3600.0 if entry_time > 0 else 0.0

                    sigs = getattr(t, 'signals_agreed', None) or getattr(t, 'signals_snapshot', None)
                    entry_signals = builder._extract_entry_signals(sigs)  # noqa: SLF001

                    active_positions.append({
                        'symbol': sym,
                        'direction': direction,
                        'entry_price': entry,
                        'notional_usd': notional,
                        'unrealized_pnl': pnl,
                        'age_hours': age_hours,
                        'entry_signals': entry_signals,
                    })
        except Exception:
            safety_state = None
            active_positions = None

        context = builder.build_full_context(
            sse_data=symbols,
            safety_state=safety_state,
            active_positions=active_positions,
        )

        # Write output
        if args.output == "-":
            print(context)
        else:
            out_path = Path(args.output)
            out_path.write_text(context)
            chars = len(context)
            tokens_est = chars // 4
            print(f"Written {chars} chars (~{tokens_est} tokens) to {args.output}", file=_sys.stderr)

        if args.json_output:
            try:
                opportunities = builder.get_opportunities(sse_data=symbols)

                symbol_directions: Dict[str, str] = {}
                symbol_trends: Dict[str, Dict[str, Any]] = {}
                try:
                    if builder.db is not None:
                        for t in builder.db.get_open_trades():
                            sym = str(getattr(t, 'symbol', '') or '').upper()
                            if not sym:
                                continue
                            data = builder._lookup_symbol_data(symbols, sym)
                            if not isinstance(data, dict):
                                continue
                            perp = data.get('perp_signals', {})
                            if not isinstance(perp, dict):
                                perp = {}
                            perp = builder.scorer._with_hip3_main(sym, perp, data)
                            if not perp:
                                continue
                            symbol_directions[sym] = builder.scorer._determine_direction(perp, data)
                            
                            # Extract trend data
                            trend_state = data.get('trend_state') or {}
                            if not trend_state:
                                km = data.get('key_metrics', {})
                                trend_state = km.get('trend_state', {})
                            if isinstance(trend_state, dict) and trend_state:
                                symbol_trends[sym] = {
                                    'score': trend_state.get('trend_score'),
                                    'direction': trend_state.get('direction'),
                                    'regime': trend_state.get('regime'),
                                }
                except Exception:
                    symbol_directions = {}
                    symbol_trends = {}

                # Fetch global context (Deribit IV + net exposure)
                global_context = None
                global_context_compact = None
                if _global_context_provider is not None:
                    try:
                        gc = _global_context_provider.get_global_context()
                        global_context = gc.to_dict()
                        global_context_compact = gc.format_compact()
                    except Exception as e:
                        print(f"Warning: Failed to get global context: {e}", file=_sys.stderr)

                generated_at_dt = datetime.now(timezone.utc)
                payload = {
                    "generated_at": generated_at_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "generated_at_ts": generated_at_dt.timestamp(),
                    "cycle_file": str(cycle_path),
                    "symbol_count": len(symbols),
                    "symbol_directions": symbol_directions,
                    "symbol_trends": symbol_trends,
                    "global_context": global_context,
                    "global_context_compact": global_context_compact,
                    "selected_opportunities": [
                        {
                            "symbol": opp.symbol,
                            "score": opp.score,
                            "direction": opp.direction,
                            "signals": opp.signals,
                            "key_metrics": opp.key_metrics,
                        }
                        for opp in opportunities
                    ],
                }
                json_text = json.dumps(
                    payload,
                    sort_keys=True,
                    separators=(",", ":"),
                    ensure_ascii=True,
                )
                json_path = Path(args.json_output)
                json_path.write_text(json_text)
                print(f"Written JSON context to {args.json_output}", file=_sys.stderr)
            except Exception as e:
                print(f"Error writing JSON output: {e}", file=_sys.stderr)
                _sys.exit(1)

    def _run_test():
        """Original interactive test - connects to live SSE."""
        import asyncio

        async def test():
            print("Context Builder V2 Test")
            print("=" * 60)

            try:
                from sse_consumer import TrackerSSEClient

                data_received = asyncio.Event()
                sample_data = {}

                def on_data(symbols):
                    sample_data.update(symbols)
                    data_received.set()

                client = TrackerSSEClient(on_data=on_data)
                task = asyncio.create_task(client.connect())

                try:
                    await asyncio.wait_for(data_received.wait(), timeout=15)
                    print(f"Received {len(sample_data)} symbols from SSE")
                finally:
                    await client.disconnect()

            except Exception as e:
                print(f"Could not connect to SSE: {e}")
                print("Using mock data...")
                sample_data = {}

            if not sample_data:
                sample_data = {
                    "BTC": {
                        "symbol": "BTC",
                        "price": 87500,
                        "perp_signals": {
                            "cvd": {"signal": "NEUTRAL", "z_smart": 0.5, "z_dumb": -0.3},
                            "fade": {"signal": "LONG", "z_score": 1.8},
                            "whale": {"signal": "SHORT", "strength": 0.5},
                        },
                        "atr": {"atr_pct": 1.5},
                        "funding": {"rate_bps": 0.05, "direction": "LONGS_PAY"},
                        "summary": {"net_bias": "LONG", "net_bias_pct": 60, "hot_zone_pct": 5},
                        "smart_dumb_cvd": {"smart_cvd": 1000, "dumb_cvd": -500, "divergence_z": 1.2},
                        "price_change": {"volume_24h": 5_000_000_000, "pct_24h": 2.5},
                        "fragile_wallets": {"count": 50, "total_notional": 5_000_000},
                        "liquidation_heatmap": {"long_total_pnl": -1000000, "short_total_pnl": -500000},
                        "cohort_delta": {"smart_vs_dumb_delta": 0.15},
                    },
                    "ETH": {
                        "symbol": "ETH",
                        "price": 2650,
                        "perp_signals": {
                            "cvd": {"signal": "BULLISH", "z_smart": 2.8, "z_dumb": 1.2},
                            "fade": {"signal": "LONG", "z_score": 2.4},
                            "liq_pnl": {"signal": "SHORT", "z_score": 1.5},
                            "ofm": {"signal": "LONG", "z_score": 2.1},
                        },
                        "atr": {"atr_pct": 1.8},
                        "funding": {"rate_bps": -0.02, "direction": "SHORTS_PAY"},
                        "summary": {"net_bias": "LONG", "net_bias_pct": 65, "hot_zone_pct": 8},
                        "smart_dumb_cvd": {"smart_cvd": 2300000, "dumb_cvd": -1100000, "divergence_z": 2.8},
                        "price_change": {"volume_24h": 2_100_000_000, "pct_24h": 3.2},
                        "fragile_wallets": {"count": 45, "total_notional": 12_000_000},
                        "liquidation_heatmap": {"long_total_pnl": -1200000, "short_total_pnl": -800000},
                        "cohort_delta": {"smart_vs_dumb_delta": 0.25},
                    },
                    "SOL": {
                        "symbol": "SOL",
                        "price": 185,
                        "perp_signals": {
                            "cvd": {"signal": "BEARISH", "z_smart": -2.1, "z_dumb": 0.8},
                            "fade": {"signal": "SHORT", "z_score": 2.2},
                            "whale": {"signal": "SHORT", "strength": 1.2},
                        },
                        "atr": {"atr_pct": 3.1},
                        "funding": {"rate_bps": 0.12, "direction": "LONGS_PAY"},
                        "summary": {"net_bias": "SHORT", "net_bias_pct": 35, "hot_zone_pct": 12},
                        "smart_dumb_cvd": {"smart_cvd": -800000, "dumb_cvd": 300000, "divergence_z": -2.1},
                        "price_change": {"volume_24h": 800_000_000, "pct_24h": -4.5},
                        "fragile_wallets": {"count": 80, "total_notional": 8_000_000},
                        "liquidation_heatmap": {"long_total_pnl": -2000000, "short_total_pnl": -400000},
                        "cohort_delta": {"smart_vs_dumb_delta": -0.2},
                    },
                }

            import tempfile
            with tempfile.TemporaryDirectory() as tmpdir:
                memory_dir = Path(tmpdir) / "memory"

                builder = ContextBuilderV2(
                    db_path="/tmp/test.db",
                    memory_dir=memory_dir,
                    max_opportunities=10,
                    min_score=30.0
                )

                safety_state = {
                    'current_tier': 1,
                    'daily_pnl': 45.20,
                    'consecutive_losses': 0,
                    'current_exposure': 1200,
                    'max_exposure': 10000
                }

                active_positions = [
                    {
                        'symbol': 'ETH',
                        'direction': 'LONG',
                        'entry_price': 2620,
                        'notional_usd': 500,
                        'unrealized_pnl': 5.70,
                        'age_hours': 1.0,
                        'entry_signals': ['CVD', 'FADE']
                    }
                ]

                context = builder.build_full_context(
                    sse_data=sample_data,
                    safety_state=safety_state,
                    active_positions=active_positions
                )

                print(f"\n=== Generated Context ===")
                print(f"Length: {len(context)} chars")
                print(f"Estimated tokens: ~{builder.estimate_tokens(context):,}")
                print(f"\n{'-' * 60}")
                print(context[:8000])
                if len(context) > 8000:
                    print(f"\n... [{len(context) - 8000} more chars]")
                print(f"{'-' * 60}")

                print(f"\n=== Opportunity Scores ===")
                opps = builder.get_opportunities(sample_data)
                for opp in opps:
                    print(f"{opp.symbol}: {opp.score:.1f} ({opp.direction})")

        asyncio.run(test())

    _run_cli()
