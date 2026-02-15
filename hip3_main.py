#!/usr/bin/env python3
"""HIP3_MAIN signal construction helpers.

Builds a single HIP3_MAIN signal from HIP3 predator payloads (FLOW + OFM).

Current design intent:
- FLOW or OFM can independently drive a HIP3 signal (OR semantics).
- If both pass with opposite directions, block (conflict guard).
- Dead-capital is telemetry-only for HIP3_MAIN (no trigger boost).
- Optional REST boosters adjust confidence/size hints only.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import yaml


_SKILL_FILE = Path(__file__).with_name("skill.yaml")


def _load_skill_hip3_cfg() -> Dict[str, Any]:
    try:
        with _SKILL_FILE.open("r", encoding="utf-8") as f:
            raw = yaml.safe_load(f) or {}
        if not isinstance(raw, dict):
            return {}
        cfg = raw.get("config") or {}
        if not isinstance(cfg, dict):
            return {}
        hip3 = cfg.get("hip3") or {}
        return hip3 if isinstance(hip3, dict) else {}
    except Exception:
        return {}


def _skill_float(cfg: Dict[str, Any], key: str, default: float) -> float:
    try:
        value = cfg.get(key, default)
        return float(default if value is None else value)
    except Exception:
        return float(default)


_HIP3_CFG = _load_skill_hip3_cfg()
HIP3_MAIN_THRESHOLD_MULT_DEFAULT = _skill_float(_HIP3_CFG, "main_threshold_mult", 1.0)
HIP3_MAIN_FLOW_Z_THRESHOLD_DEFAULT = _skill_float(_HIP3_CFG, "main_flow_z_threshold", 2.0)
HIP3_MAIN_OFM_CONF_THRESHOLD_DEFAULT = _skill_float(_HIP3_CFG, "main_ofm_conf_threshold", 0.65)


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return float(default)


def _norm_dir(value: Any) -> str:
    raw = str(value or "").upper()
    if raw in ("LONG", "SHORT"):
        return raw
    if raw in ("BULLISH", "BULL"):
        return "LONG"
    if raw in ("BEARISH", "BEAR"):
        return "SHORT"
    return ""


def _deadcap_strength(dead: Optional[Dict[str, Any]]) -> float:
    if not isinstance(dead, dict):
        return 0.0
    z_score = _safe_float(dead.get("z_score"), 0.0)
    if z_score != 0.0:
        return abs(z_score)
    strength = _safe_float(dead.get("strength"), 0.0)
    if strength != 0.0:
        # Align with existing z-equivalent convention (strength ~ z/2).
        return abs(strength) * 2.0
    return 0.0


def _clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, float(value)))


def _pick_float(*values: Any, default: float = 0.0) -> float:
    for value in values:
        try:
            if value is None:
                continue
            return float(value)
        except Exception:
            continue
    return float(default)


def _extract_booster_scores(
    hip3_payload: Dict[str, Any],
    *,
    direction: str,
) -> Dict[str, float]:
    """Extract up to 5 numeric booster z-scores from REST feature blobs.

    Accepted payload keys:
    - rest_boosters
    - boosters
    - rest_features
    """
    out: Dict[str, float] = {}
    for key in ("rest_boosters", "boosters", "rest_features"):
        raw = hip3_payload.get(key)
        if isinstance(raw, dict):
            for name, obj in raw.items():
                if len(out) >= 5:
                    break
                score: Optional[float] = None
                bdir = ""
                if isinstance(obj, dict):
                    score = _pick_float(
                        obj.get("z_score"),
                        obj.get("z"),
                        obj.get("score"),
                        obj.get("value"),
                        default=0.0,
                    )
                    bdir = _norm_dir(obj.get("direction") or obj.get("signal"))
                else:
                    try:
                        score = float(obj)
                    except Exception:
                        score = None
                if score is None:
                    continue
                score = _clamp(float(score), -5.0, 5.0)
                if bdir in ("LONG", "SHORT") and direction in ("LONG", "SHORT"):
                    if bdir == direction:
                        score = abs(score)
                    else:
                        score = -abs(score)
                out[str(name)] = float(score)
            if out:
                break
    return out


def compute_hip3_main(
    hip3_payload: Optional[Dict[str, Any]],
    *,
    dead_capital: Optional[Dict[str, Any]] = None,
    threshold_mult: Optional[float] = None,
    require_ofm: bool = False,
) -> Tuple[Optional[Dict[str, Any]], Dict[str, Any]]:
    """Compute HIP3_MAIN signal payload.

    Returns (signal_payload_or_none, meta) where meta always includes gating info.
    """
    meta: Dict[str, Any] = {}
    if threshold_mult is None:
        threshold_mult = HIP3_MAIN_THRESHOLD_MULT_DEFAULT
    threshold_mult = max(0.0, float(threshold_mult))
    flow_threshold_default = HIP3_MAIN_FLOW_Z_THRESHOLD_DEFAULT
    ofm_conf_threshold = HIP3_MAIN_OFM_CONF_THRESHOLD_DEFAULT
    ofm_conf_threshold = _clamp(ofm_conf_threshold, 0.0, 1.0)

    if not isinstance(hip3_payload, dict):
        meta["blocked_reason"] = "missing_hip3_payload"
        return None, meta

    flow = hip3_payload.get("flow") or {}
    ofm = hip3_payload.get("ofm_pred") or {}
    if not isinstance(flow, dict):
        flow = {}
    if not isinstance(ofm, dict):
        ofm = {}

    # Market-hours pause guard from HIP3 worker (per-symbol payload metadata).
    paused = bool(
        hip3_payload.get("paused")
        or flow.get("paused")
        or ofm.get("paused")
    )
    entry_allowed_raw = hip3_payload.get("entry_allowed")
    entry_allowed = None if entry_allowed_raw is None else bool(entry_allowed_raw)
    market_session = str(hip3_payload.get("market_session") or flow.get("market_session") or ofm.get("market_session") or "")
    pause_reason = str(hip3_payload.get("pause_reason") or flow.get("pause_reason") or ofm.get("pause_reason") or market_session)
    if paused or (entry_allowed is False):
        meta.update(
            {
                "paused": True,
                "entry_allowed": False if entry_allowed is False else bool(entry_allowed_raw),
                "market_session": market_session,
                "pause_reason": pause_reason,
            }
        )
        meta["blocked_reason"] = "market_hours_paused"
        return None, meta

    flow_skipped = bool(flow.get("skipped"))
    flow_dir = _norm_dir(flow.get("direction"))
    ofm_dir = _norm_dir(ofm.get("direction"))
    z_signed = _pick_float(flow.get("z_signed"), default=0.0)
    flow_z = _pick_float(flow.get("z_score"), flow.get("z"), default=0.0)
    if z_signed == 0.0:
        z_signed = abs(flow_z) if flow_dir == "LONG" else -abs(flow_z) if flow_dir == "SHORT" else float(flow_z)
    abs_z = abs(z_signed) if z_signed != 0.0 else abs(flow_z)
    threshold = _pick_float(flow.get("dynamic_threshold"), flow.get("threshold"), default=flow_threshold_default)
    threshold = max(0.0, threshold)
    flow_req = threshold_mult * threshold if threshold > 0 else 0.0
    flow_pass = bool((not flow_skipped) and flow_dir and flow_req > 0 and abs_z >= flow_req)

    ofm_conf = _pick_float(ofm.get("confidence"), default=0.0)
    ofm_conf = _clamp(ofm_conf, 0.0, 1.0)
    ofm_pass = bool(ofm_dir and ofm_conf >= ofm_conf_threshold)
    conflict = bool(flow_pass and ofm_pass and flow_dir and ofm_dir and flow_dir != ofm_dir)
    ofm_component_z = ofm_conf * 3.0

    meta.update(
        {
            "flow_z_signed": z_signed,
            "flow_z_score": flow_z,
            "flow_abs_z": abs_z,
            "flow_threshold": threshold,
            "flow_required_z": flow_req,
            "threshold_mult": threshold_mult,
            "flow_direction": flow_dir,
            "ofm_direction": ofm_dir,
            "ofm_conf": ofm_conf,
            "ofm_conf_threshold": ofm_conf_threshold,
            "flow_skipped": flow_skipped,
            "skip_reason": flow.get("skip_reason"),
            "flow_pass": flow_pass,
            "ofm_pass": ofm_pass,
            "conflict": conflict,
            "require_ofm": bool(require_ofm),
        }
    )

    flow_fail_reason = ""
    if flow_skipped:
        flow_fail_reason = "flow_skipped"
    elif not flow_dir:
        flow_fail_reason = "missing_flow_direction"
    elif threshold <= 0:
        flow_fail_reason = "missing_flow_threshold"
    elif abs_z < flow_req:
        flow_fail_reason = "below_flow_threshold"

    ofm_fail_reason = ""
    if not ofm_dir:
        ofm_fail_reason = "missing_ofm_direction"
    elif ofm_conf < ofm_conf_threshold:
        ofm_fail_reason = "below_ofm_threshold"

    if conflict:
        meta["blocked_reason"] = "flow_ofm_direction_conflict"
        return None, meta

    driver_type = ""
    direction = ""
    base_z = 0.0
    if require_ofm:
        # Legacy strict mode: FLOW must pass and OFM must align/pass.
        if flow_pass and ofm_pass and flow_dir == ofm_dir:
            driver_type = "both"
            direction = flow_dir
            base_z = max(abs_z, ofm_component_z) * 1.05
        else:
            meta["blocked_reason"] = flow_fail_reason or ofm_fail_reason or "flow_ofm_gate_failed"
            return None, meta
    else:
        # New OR mode: either FLOW or OFM can drive.
        if flow_pass and ofm_pass and flow_dir == ofm_dir:
            driver_type = "both"
            direction = flow_dir
            base_z = max(abs_z, ofm_component_z) * 1.05
        elif flow_pass:
            driver_type = "flow"
            direction = flow_dir
            base_z = abs_z
        elif ofm_pass:
            driver_type = "ofm"
            direction = ofm_dir
            base_z = ofm_component_z
        else:
            meta["blocked_reason"] = flow_fail_reason or ofm_fail_reason or "flow_ofm_gate_failed"
            return None, meta

    dead_strength = _deadcap_strength(dead_capital)
    dead_dir = _norm_dir((dead_capital or {}).get("signal") or (dead_capital or {}).get("direction"))
    dead_aligned = bool(dead_strength > 0 and dead_dir == direction)
    # Dead-capital is telemetry-only in HIP3_MAIN and no longer boosts trigger strength.

    booster_scores = _extract_booster_scores(hip3_payload, direction=direction)
    booster_count = len(booster_scores)
    booster_score = (sum(booster_scores.values()) / booster_count) if booster_count > 0 else 0.0
    booster_conf_mult = _clamp(1.0 + (0.06 * booster_score), 0.85, 1.20)
    booster_size_mult = _clamp(1.0 + (0.10 * booster_score), 0.70, 1.40)
    z_effective = max(0.0, base_z * booster_conf_mult)

    meta.update(
        {
            "driver_type": driver_type,
            "direction": direction,
            "base_z": base_z,
            "dead_strength": dead_strength,
            "dead_aligned": dead_aligned,
            "booster_count": booster_count,
            "booster_score": booster_score,
            "booster_conf_mult": booster_conf_mult,
            "booster_size_mult": booster_size_mult,
        }
    )

    signal_payload = {
        "direction": direction,
        "signal": direction,
        "driver_type": driver_type,
        "flow_pass": bool(flow_pass),
        "ofm_pass": bool(ofm_pass),
        "ofm_confidence": ofm_conf,
        "flow_strength": abs_z,
        "ofm_strength": ofm_component_z,
        "confidence": _clamp(z_effective / 3.0, 0.0, 1.0),
        "size_mult_hint": booster_size_mult,
        "z_score": z_effective,
        "z_score_effective": z_effective,
        "z_score_raw": base_z,
        "components": {
            "flow_z_signed": z_signed,
            "flow_z_score": flow_z,
            "flow_abs_z": abs_z,
            "flow_threshold": threshold,
            "flow_required_z": flow_req,
            "flow_pass": bool(flow_pass),
            "threshold_mult": threshold_mult,
            "spread_bps": _safe_float(flow.get("spread_bps"), 0.0),
            "mispricing_bps": _safe_float(flow.get("mispricing_bps"), 0.0),
            "ofm_dir": ofm_dir or "NEUTRAL",
            "ofm_pass": bool(ofm_pass),
            "ofm_conf": ofm_conf,
            "ofm_conf_threshold": ofm_conf_threshold,
            "ofm_component_z": ofm_component_z,
            "dead_strength": dead_strength,
            "dead_aligned": dead_aligned,
            "driver_type": driver_type,
            "base_z": base_z,
            "rest_boosters": booster_scores,
            "rest_booster_score": booster_score,
            "rest_booster_conf_mult": booster_conf_mult,
            "rest_booster_size_mult": booster_size_mult,
            "flow_skipped": flow_skipped,
            "skip_reason": flow.get("skip_reason"),
        },
    }
    if market_session:
        signal_payload["market_session"] = market_session
    if entry_allowed is not None:
        signal_payload["entry_allowed"] = bool(entry_allowed)
    return signal_payload, meta
