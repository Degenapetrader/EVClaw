#!/usr/bin/env python3
"""Shared conviction scoring for live AGI entry flow."""

from __future__ import annotations

import math
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Optional

import yaml

DEFAULT_CONVICTION_WEIGHTS: Dict[str, float] = {
    "cvd": 0.10,
    "dead_capital": 0.35,
    "fade": 0.07,
    "ofm": 0.22,
    "hip3_main": 0.65,
    "whale": 0.26,
}

_DIRECTION_MAP = {
    "LONG": "LONG",
    "SHORT": "SHORT",
    "BULLISH": "LONG",
    "BEARISH": "SHORT",
}


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        out = float(value)
    except Exception:
        return float(default)
    if not math.isfinite(out):
        return float(default)
    return out


def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None:
        return default
    return _safe_float(raw, default)


def _clamp01(value: float) -> float:
    return max(0.0, min(1.0, float(value)))


def _normalize_direction(value: Any) -> str:
    return _DIRECTION_MAP.get(str(value or "").upper(), "")


def _load_skill_brain_cfg() -> Dict[str, Any]:
    try:
        cfg_file = Path(__file__).with_name("skill.yaml")
        if not cfg_file.exists():
            return {}
        with cfg_file.open("r", encoding="utf-8") as f:
            raw = yaml.safe_load(f) or {}
        if not isinstance(raw, dict):
            return {}
        cfg = raw.get("config") or {}
        if not isinstance(cfg, dict):
            return {}
        brain = cfg.get("brain") or {}
        return brain if isinstance(brain, dict) else {}
    except Exception:
        return {}


def _skill_float(cfg: Dict[str, Any], key: str, default: float) -> float:
    try:
        raw = cfg.get(key, default)
        return _safe_float(default if raw is None else raw, default)
    except Exception:
        return float(default)


_SKILL_BRAIN_CFG = _load_skill_brain_cfg()


@dataclass(frozen=True)
class ConvictionConfig:
    """Configuration for deterministic conviction + order-type mapping."""

    weights: Dict[str, float] = field(default_factory=lambda: dict(DEFAULT_CONVICTION_WEIGHTS))
    blend_pipeline: float = 0.20
    chase_threshold: float = 0.70
    limit_min: float = 0.20
    conviction_z_denom: float = 3.0
    strength_z_mult: float = 2.0
    agree_bonus_4: float = 0.10
    agree_bonus_5: float = 0.05
    agree_mult_3: float = 1.05
    agree_mult_4: float = 1.10
    smart_adj_cap: float = 0.15
    cohort_align_mult: float = 1.15
    cohort_counter_mult: float = 0.85
    vol_penalty_mult: float = 0.85
    vol_penalty_atr_min: float = 0.3
    vol_penalty_atr_max: float = 6.0

    @classmethod
    def load(cls) -> "ConvictionConfig":
        weights = dict(DEFAULT_CONVICTION_WEIGHTS)
        skill_weights = _SKILL_BRAIN_CFG.get("conviction_weights")
        if isinstance(skill_weights, dict):
            for key, value in skill_weights.items():
                if key in weights:
                    parsed = _safe_float(value, weights[key])
                    if parsed >= 0:
                        weights[key] = parsed

        blend_pipeline = _clamp01(_skill_float(_SKILL_BRAIN_CFG, "conviction_blend_pipeline", 0.20))
        chase_threshold = _clamp01(_skill_float(_SKILL_BRAIN_CFG, "conviction_chase_threshold", 0.70))
        limit_min = _clamp01(
            _env_float("EVCLAW_CONVICTION_LIMIT_MIN", 0.20)
        )
        if limit_min >= chase_threshold:
            # Keep deterministic ordering even under bad env input.
            limit_min = min(0.69, max(0.0, chase_threshold - 0.01))

        return cls(
            weights=weights,
            blend_pipeline=blend_pipeline,
            chase_threshold=chase_threshold,
            limit_min=limit_min,
            conviction_z_denom=3.0,
            strength_z_mult=2.0,
            agree_bonus_4=_env_float("EVCLAW_CONVICTION_AGREE_BONUS_4", 0.10),
            agree_bonus_5=_env_float("EVCLAW_CONVICTION_AGREE_BONUS_5", 0.05),
            agree_mult_3=max(0.0, _env_float("EVCLAW_CONVICTION_AGREE_MULT_3", 1.05)),
            agree_mult_4=max(0.0, _env_float("EVCLAW_CONVICTION_AGREE_MULT_4", 1.10)),
            smart_adj_cap=max(0.0, _env_float("EVCLAW_CONVICTION_SMART_ADJ_CAP", 0.15)),
            cohort_align_mult=max(0.0, _env_float("EVCLAW_CONVICTION_COHORT_ALIGN_MULT", 1.15)),
            cohort_counter_mult=max(0.0, _env_float("EVCLAW_CONVICTION_COHORT_COUNTER_MULT", 0.85)),
            vol_penalty_mult=max(0.0, _env_float("EVCLAW_CONVICTION_VOL_PENALTY_MULT", 0.85)),
            vol_penalty_atr_min=max(0.0, _env_float("EVCLAW_CONVICTION_VOL_ATR_MIN", 0.3)),
            vol_penalty_atr_max=max(0.0, _env_float("EVCLAW_CONVICTION_VOL_ATR_MAX", 6.0)),
        )


def _signal_z(signal_name: str, payload: Dict[str, Any], cfg: ConvictionConfig) -> float:
    z = abs(_safe_float(payload.get("z_score"), 0.0))
    if signal_name == "cvd":
        z = max(
            z,
            abs(_safe_float(payload.get("z_smart"), 0.0)),
            abs(_safe_float(payload.get("z_dumb"), 0.0)),
        )
    if signal_name in {"whale", "dead_capital"} and z <= 0:
        z = abs(_safe_float(payload.get("strength"), 0.0)) * cfg.strength_z_mult
    return max(0.0, z)


def _smart_factor(metrics: Dict[str, Any], direction: str, cfg: ConvictionConfig) -> float:
    smart_cvd = _safe_float(metrics.get("smart_cvd"), 0.0)
    div_z = _safe_float(metrics.get("divergence_z"), 0.0)
    cap = cfg.smart_adj_cap
    if direction == "LONG":
        if smart_cvd > 0 and div_z > 0:
            return 1.0 + min(cap, div_z / 10.0)
        if smart_cvd < 0:
            return 1.0 - min(cap, abs(div_z) / 10.0)
    elif direction == "SHORT":
        if smart_cvd < 0 and div_z < 0:
            return 1.0 + min(cap, abs(div_z) / 10.0)
        if smart_cvd > 0:
            return 1.0 - min(cap, abs(div_z) / 10.0)
    return 1.0


def _cohort_factor(metrics: Dict[str, Any], direction: str, cfg: ConvictionConfig) -> float:
    cohort = str(metrics.get("cohort_signal") or "").upper()
    if not cohort:
        return 1.0
    if (direction == "LONG" and "ACCUMULATING" in cohort) or (
        direction == "SHORT" and "DISTRIBUTING" in cohort
    ):
        return cfg.cohort_align_mult
    if "ACCUMULATING" in cohort or "DISTRIBUTING" in cohort:
        return cfg.cohort_counter_mult
    return 1.0


def _vol_factor(metrics: Dict[str, Any], cfg: ConvictionConfig) -> float:
    atr_pct = _safe_float(metrics.get("atr_pct"), 0.0)
    if atr_pct > 0 and (
        atr_pct < cfg.vol_penalty_atr_min or atr_pct > cfg.vol_penalty_atr_max
    ):
        return cfg.vol_penalty_mult
    return 1.0


def compute_brain_conviction_no_floor(
    *,
    signals_snapshot: Optional[Dict[str, Any]],
    key_metrics: Optional[Dict[str, Any]],
    direction: str,
    config: Optional[ConvictionConfig] = None,
) -> float:
    """Compute brain conviction from signal snapshots without dead-capital floor."""
    cfg = config or ConvictionConfig.load()
    trade_dir = _normalize_direction(direction)
    if trade_dir not in {"LONG", "SHORT"}:
        return 0.0
    if not isinstance(signals_snapshot, dict) or not signals_snapshot:
        return 0.0

    total_weight = 0.0
    weighted_strength = 0.0
    agree_count = 0

    for signal_name, weight in cfg.weights.items():
        payload = signals_snapshot.get(signal_name)
        if not isinstance(payload, dict):
            continue
        z_val = _signal_z(signal_name, payload, cfg)
        if z_val <= 0:
            continue
        strength = min(1.0, z_val / max(1e-9, cfg.conviction_z_denom))
        signal_dir = _normalize_direction(payload.get("direction") or payload.get("signal"))

        total_weight += max(0.0, float(weight))
        if signal_dir == trade_dir:
            weighted_strength += float(weight) * strength
            agree_count += 1
        elif signal_dir in {"LONG", "SHORT"}:
            weighted_strength -= float(weight) * strength * 0.5

    if total_weight <= 0:
        return 0.0

    base = weighted_strength / total_weight
    if base > 0:
        if agree_count >= 4:
            base += cfg.agree_bonus_4
        if agree_count >= 5:
            base += cfg.agree_bonus_5
    base = _clamp01(base)

    metrics = key_metrics if isinstance(key_metrics, dict) else {}
    adjusted = base
    adjusted *= _smart_factor(metrics, trade_dir, cfg)
    adjusted *= _cohort_factor(metrics, trade_dir, cfg)
    adjusted *= _vol_factor(metrics, cfg)

    if agree_count >= 4:
        adjusted *= cfg.agree_mult_4
    elif agree_count >= 3:
        adjusted *= cfg.agree_mult_3

    return _clamp01(adjusted)


def compute_blended_conviction(
    pipeline_conviction: float,
    brain_conviction: float,
    *,
    blend_pipeline: Optional[float] = None,
    config: Optional[ConvictionConfig] = None,
) -> float:
    """Blend pipeline + brain conviction where blend is pipeline weight."""
    cfg = config or ConvictionConfig.load()
    blend = _clamp01(
        _safe_float(cfg.blend_pipeline if blend_pipeline is None else blend_pipeline, cfg.blend_pipeline)
    )
    pipe = _clamp01(_safe_float(pipeline_conviction, 0.0))
    brain = _clamp01(_safe_float(brain_conviction, 0.0))
    return _clamp01((blend * pipe) + ((1.0 - blend) * brain))


def resolve_order_type(
    blended_conviction: float,
    *,
    config: Optional[ConvictionConfig] = None,
) -> str:
    """Resolve deterministic order type from blended conviction.

    Policy:
    - blended >= chase_threshold: chase_limit
    - limit_min < blended < chase_threshold: limit
    - blended <= limit_min: reject
    """
    cfg = config or ConvictionConfig.load()
    blended = _clamp01(_safe_float(blended_conviction, 0.0))
    if blended >= cfg.chase_threshold:
        return "chase_limit"
    if cfg.limit_min < blended < cfg.chase_threshold:
        return "limit"
    return "reject"


def load_runtime_conviction_config(
    *,
    db: Optional[Any] = None,
    config: Optional[ConvictionConfig] = None,
) -> ConvictionConfig:
    """Load conviction config using DB active snapshot when available."""
    cfg = config or ConvictionConfig.load()
    if db is None or not hasattr(db, "get_active_conviction_config"):
        return cfg

    try:
        active = db.get_active_conviction_config()
    except Exception:
        active = None
    if not isinstance(active, dict):
        return cfg

    params = active.get("params")
    if not isinstance(params, dict):
        return cfg

    chase_threshold = _clamp01(_safe_float(params.get("chase_threshold"), cfg.chase_threshold))
    limit_min = _clamp01(_safe_float(params.get("limit_min"), cfg.limit_min))
    if limit_min >= chase_threshold:
        limit_min = min(0.69, max(0.0, chase_threshold - 0.01))

    weights = params.get("weights")
    if not isinstance(weights, dict):
        weights = cfg.weights

    return ConvictionConfig(
        weights=weights,
        blend_pipeline=_clamp01(_safe_float(params.get("blend_pipeline"), cfg.blend_pipeline)),
        chase_threshold=chase_threshold,
        limit_min=limit_min,
        conviction_z_denom=cfg.conviction_z_denom,
        strength_z_mult=cfg.strength_z_mult,
        agree_bonus_4=cfg.agree_bonus_4,
        agree_bonus_5=cfg.agree_bonus_5,
        agree_mult_3=cfg.agree_mult_3,
        agree_mult_4=cfg.agree_mult_4,
        smart_adj_cap=cfg.smart_adj_cap,
        cohort_align_mult=cfg.cohort_align_mult,
        cohort_counter_mult=cfg.cohort_counter_mult,
        vol_penalty_mult=cfg.vol_penalty_mult,
        vol_penalty_atr_min=cfg.vol_penalty_atr_min,
        vol_penalty_atr_max=cfg.vol_penalty_atr_max,
    )


def resolve_order_type_runtime(
    blended_conviction: float,
    *,
    db: Optional[Any] = None,
    config: Optional[ConvictionConfig] = None,
) -> str:
    """Resolve order type using runtime conviction config source."""
    cfg = load_runtime_conviction_config(db=db, config=config)
    return resolve_order_type(blended_conviction, config=cfg)


def get_conviction_config(config: Optional[ConvictionConfig] = None) -> Dict[str, Any]:
    """Return conviction settings for journaling and metadata."""
    cfg = config or ConvictionConfig.load()
    return {
        "weights": dict(cfg.weights),
        "blend_pipeline": float(cfg.blend_pipeline),
        "chase_threshold": float(cfg.chase_threshold),
        "limit_min": float(cfg.limit_min),
        "conviction_z_denom": float(cfg.conviction_z_denom),
        "strength_z_mult": float(cfg.strength_z_mult),
        "agree_bonus_4": float(cfg.agree_bonus_4),
        "agree_bonus_5": float(cfg.agree_bonus_5),
        "agree_mult_3": float(cfg.agree_mult_3),
        "agree_mult_4": float(cfg.agree_mult_4),
        "smart_adj_cap": float(cfg.smart_adj_cap),
    }


__all__ = [
    "ConvictionConfig",
    "DEFAULT_CONVICTION_WEIGHTS",
    "compute_blended_conviction",
    "compute_brain_conviction_no_floor",
    "load_runtime_conviction_config",
    "get_conviction_config",
    "resolve_order_type",
    "resolve_order_type_runtime",
]
