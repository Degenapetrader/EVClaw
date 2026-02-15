from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import yaml

from agi_context import build_agi_candidate
from ai_trader_db import AITraderDB
from constraints import compile_constraints
from context_learning import ContextLearningEngine
from live_agent_utils import MAX_CANDIDATES


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


def _load_skill_exposure_cfg() -> Dict[str, Any]:
    try:
        with _SKILL_FILE.open("r", encoding="utf-8") as f:
            raw = yaml.safe_load(f) or {}
        if not isinstance(raw, dict):
            return {}
        cfg = raw.get("config") or {}
        if not isinstance(cfg, dict):
            return {}
        exposure = cfg.get("exposure") or {}
        return exposure if isinstance(exposure, dict) else {}
    except Exception:
        return {}


def _load_skill_brain_cfg() -> Dict[str, Any]:
    try:
        with _SKILL_FILE.open("r", encoding="utf-8") as f:
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
        value = cfg.get(key, default)
        return float(default if value is None else value)
    except Exception:
        return float(default)


def _skill_int(cfg: Dict[str, Any], key: str, default: int) -> int:
    try:
        value = cfg.get(key, default)
        return int(default if value is None else value)
    except Exception:
        return int(default)


_HIP3_CFG = _load_skill_hip3_cfg()
_EXPOSURE_CFG = _load_skill_exposure_cfg()
_BRAIN_CFG = _load_skill_brain_cfg()

def _brain_runtime_float(key: str, default: float) -> float:
    try:
        from mode_controller import get_param as _mode_get_param

        value = _mode_get_param("brain", key)
        if value is not None:
            return float(value)
    except Exception:
        pass
    return _skill_float(_BRAIN_CFG, key, default)


def _brain_runtime_int(key: str, default: int) -> int:
    try:
        from mode_controller import get_param as _mode_get_param

        value = _mode_get_param("brain", key)
        if value is not None:
            return int(value)
    except Exception:
        pass
    return _skill_int(_BRAIN_CFG, key, default)


CANDIDATE_MIN_CONVICTION = _brain_runtime_float("candidate_min_conviction", 0.1)
CANDIDATE_SCORE_DIVISOR = _brain_runtime_float("candidate_score_divisor", 100.0)
CANDIDATE_TOPK_MIN = _brain_runtime_int("candidate_topk_min", 4)
CANDIDATE_TOPK_MAX = _brain_runtime_int("candidate_topk_max", MAX_CANDIDATES)
CANDIDATE_TOPK_SCORE_GATE = _brain_runtime_float("candidate_topk_score_gate", 70.0)
SYMBOL_DOSSIER_MAX_CHARS = 600
HIP3_FLOW_SL_ATR_MULT = _skill_float(_HIP3_CFG, "flow_sl_atr_mult", 1.4)
HIP3_FLOW_TP_ATR_MULT = _skill_float(_HIP3_CFG, "flow_tp_atr_mult", 2.0)
HIP3_OFM_SL_ATR_MULT = _skill_float(_HIP3_CFG, "ofm_sl_atr_mult", 1.8)
HIP3_OFM_TP_ATR_MULT = _skill_float(_HIP3_CFG, "ofm_tp_atr_mult", 2.8)
HIP3_BOOSTER_MULT_MIN = _skill_float(_HIP3_CFG, "booster_size_mult_min", 0.70)
HIP3_BOOSTER_MULT_MAX = _skill_float(_HIP3_CFG, "booster_size_mult_max", 1.40)
NET_EXPOSURE_BIAS_THRESHOLD_USD = _skill_float(_EXPOSURE_CFG, "net_exposure_bias_threshold_usd", 15000.0)
NET_EXPOSURE_PENALTY = _skill_float(_EXPOSURE_CFG, "net_exposure_penalty", 0.85)
NET_EXPOSURE_BONUS = _skill_float(_EXPOSURE_CFG, "net_exposure_bonus", 1.05)
LOG = logging.getLogger("candidate_pipeline")

_CONTEXT_LEARNING_SINGLETON: Optional[ContextLearningEngine] = None
_CONTEXT_LEARNING_INIT_FAILED = False


def _conviction_score_divisor() -> float:
    """Return a safe conviction divisor; never zero/negative."""
    try:
        value = float(CANDIDATE_SCORE_DIVISOR)
    except Exception:
        value = 100.0
    if value <= 0:
        return 100.0
    return value


def _adaptive_topk_settings() -> Tuple[int, int, float]:
    min_k = int(CANDIDATE_TOPK_MIN)
    max_k = int(CANDIDATE_TOPK_MAX)
    score_gate = float(CANDIDATE_TOPK_SCORE_GATE)
    if min_k < 1:
        min_k = 1
    if max_k < min_k:
        max_k = min_k
    return min_k, max_k, score_gate


def _adaptive_candidate_top_k(
    *,
    opportunities: List[Dict[str, Any]],
    base_limit: int,
    strong_count: int,
) -> int:
    """Adaptive candidate count based on cycle quality and strong-signal floor."""
    min_k, max_k, score_gate = _adaptive_topk_settings()
    max_cap = max(1, max_k, int(strong_count))
    min_cap = max(1, min(min_k, max_cap))

    quality_count = 0
    for opp in opportunities:
        if not isinstance(opp, dict):
            continue
        if opp.get("strong_signals"):
            quality_count += 1
            continue
        try:
            score = float(opp.get("score") or 0.0)
        except Exception:
            score = 0.0
        if score >= score_gate:
            quality_count += 1

    base_cap = max(int(base_limit or 0), int(strong_count), 1)
    target = max(min_cap, quality_count, int(strong_count))
    return max(1, min(target, max_cap, base_cap))


def _is_hip3(symbol: str, data: Optional[Dict[str, Any]] = None) -> bool:
    s = str(symbol or "")
    if ":" in s:
        prefix = s.split(":", 1)[0].strip().lower()
        if prefix == "xyz":
            return True
    if isinstance(data, dict):
        hip3_info = data.get("hip3_info") or {}
        if hip3_info.get("is_hip3") is True:
            return True
    return False


def _find_symbol_data(symbols: Dict[str, Any], symbol: str) -> Optional[Dict[str, Any]]:
    """Case-insensitive symbol lookup without importing cli (side-effect free)."""
    if not symbols:
        return None
    if symbol in symbols:
        return symbols.get(symbol)
    symbol_upper = symbol.upper()
    if symbol_upper in symbols:
        return symbols.get(symbol_upper)
    for key, value in symbols.items():
        if str(key).upper() == symbol_upper:
            return value
    return None


def _hip3_driver_sltp(hip3_main: Dict[str, Any]) -> Tuple[float, float, str]:
    driver = str(hip3_main.get("driver_type") or "").strip().lower()
    if driver == "flow":
        return float(HIP3_FLOW_SL_ATR_MULT), float(HIP3_FLOW_TP_ATR_MULT), driver
    if driver == "both":
        return (
            float((HIP3_FLOW_SL_ATR_MULT + HIP3_OFM_SL_ATR_MULT) / 2.0),
            float((HIP3_FLOW_TP_ATR_MULT + HIP3_OFM_TP_ATR_MULT) / 2.0),
            driver,
        )
    # Default to OFM profile when explicitly OFM or unknown.
    return float(HIP3_OFM_SL_ATR_MULT), float(HIP3_OFM_TP_ATR_MULT), (driver or "ofm")


def _hip3_booster_size_mult(hip3_main: Dict[str, Any]) -> float:
    try:
        size_mult = float(hip3_main.get("size_mult_hint") or 1.0)
    except Exception:
        size_mult = 1.0
    lo = float(min(HIP3_BOOSTER_MULT_MIN, HIP3_BOOSTER_MULT_MAX))
    hi = float(max(HIP3_BOOSTER_MULT_MIN, HIP3_BOOSTER_MULT_MAX))
    return max(lo, min(hi, float(size_mult)))


def _default_context_learning_engine() -> Optional[ContextLearningEngine]:
    """Lazily initialize a process-local context-learning engine once."""
    global _CONTEXT_LEARNING_SINGLETON, _CONTEXT_LEARNING_INIT_FAILED
    if _CONTEXT_LEARNING_SINGLETON is not None:
        return _CONTEXT_LEARNING_SINGLETON
    if _CONTEXT_LEARNING_INIT_FAILED:
        return None
    try:
        _CONTEXT_LEARNING_SINGLETON = ContextLearningEngine()
    except Exception:
        _CONTEXT_LEARNING_INIT_FAILED = True
        return None
    return _CONTEXT_LEARNING_SINGLETON


def _build_signals_list(opportunity: Dict[str, Any], expected_direction: Optional[str] = None) -> List[str]:
    """Build a compact signals list for attribution.

    If expected_direction is provided, we only include signals that point in the
    same direction as the candidate/trade. Full raw signals remain available in
    signals_snapshot.
    """
    signals: List[str] = []

    exp_dir = (expected_direction or "").upper()

    # Context JSON can provide either:
    # - top_opportunities[*].perp_signals (newer schema)
    # - selected_opportunities[*].signals (older / compact schema)
    perp_signals = opportunity.get("perp_signals")
    if not isinstance(perp_signals, dict):
        perp_signals = opportunity.get("signals")
    perp_signals = perp_signals or {}

    if isinstance(perp_signals, dict):
        for name, payload in perp_signals.items():
            if not isinstance(payload, dict):
                continue
            direction = str(payload.get("direction") or payload.get("signal") or "").upper()
            if direction not in ("LONG", "SHORT"):
                continue
            if exp_dir in ("LONG", "SHORT") and direction != exp_dir:
                continue
            if name in ("dead_capital", "whale"):
                strength = payload.get("strength")
                if strength is None:
                    z_score = payload.get("z_score")
                    if z_score is not None:
                        try:
                            strength = float(z_score) / 2.0
                        except (TypeError, ValueError):
                            strength = None
                elif strength is not None:
                    try:
                        strength = float(strength)
                    except (TypeError, ValueError):
                        strength = None
                signals.append(f"{name.upper()}:{direction} str={strength}")
            else:
                z_score = payload.get("z_score")
                if z_score is None and name == "cvd":
                    try:
                        z_smart = float(payload.get("z_smart", 0) or 0.0)
                    except Exception:
                        z_smart = 0.0
                    try:
                        z_dumb = float(payload.get("z_dumb", 0) or 0.0)
                    except Exception:
                        z_dumb = 0.0
                    # Preserve directional sign from the stronger cohort.
                    z_score = z_smart if abs(z_smart) >= abs(z_dumb) else z_dumb
                signals.append(f"{name.upper()}:{direction} z={z_score}")

    worker = opportunity.get("worker_signals_summary") or {}
    if isinstance(worker, dict):
        trend = worker.get("trend")
        if isinstance(trend, dict) and trend:
            signals.append(
                f"TREND:{trend.get('direction')}/{trend.get('regime')} score={trend.get('trend_score')}"
            )
        vp = worker.get("volume_profile")
        if isinstance(vp, dict) and vp:
            signals.append(
                f"VP:{vp.get('timeframe')} dist={vp.get('distance_to_poc')}"
            )
        sr = worker.get("sr_levels")
        if isinstance(sr, dict) and sr:
            signals.append("SR:nearest")

    return signals


def _find_sr_for_limit(
    key_metrics: Dict[str, Any],
    direction: str,
    current_price: float,
    atr_pct: float,
    min_distance_atr: float = 0.3,
    max_distance_atr: float = 1.0,
    min_strength_z: float = 2.0,
) -> Optional[float]:
    """Find nearby SR level suitable for limit entry.

    For LONG: look for support within 0.3-1.0 ATR below current price
    For SHORT: look for resistance within 0.3-1.0 ATR above current price

    Returns: SR level price or None if no suitable level found.
    """
    sr = (key_metrics or {}).get("sr_levels", {})
    nearest = sr.get("nearest", {})
    d = direction.upper()

    if d == "LONG":
        level_data = nearest.get("support", {})
        if not level_data:
            return None
        level_price = float(level_data.get("price", 0))
        strength = float(level_data.get("strength_z", 0))
        # Support should be BELOW current price
        if level_price <= 0 or level_price >= current_price:
            return None
    elif d == "SHORT":
        level_data = nearest.get("resistance", {})
        if not level_data:
            return None
        level_price = float(level_data.get("price", 0))
        strength = float(level_data.get("strength_z", 0))
        # Resistance should be ABOVE current price
        if level_price <= 0 or level_price <= current_price:
            return None
    else:
        return None

    if strength < min_strength_z:
        return None

    # Check distance in ATR units
    if atr_pct <= 0:
        return None
    distance_pct = abs(level_price - current_price) / current_price * 100.0
    distance_atr = distance_pct / atr_pct

    if distance_atr < min_distance_atr or distance_atr > max_distance_atr:
        return None

    return level_price


def build_candidates_from_context(
    *,
    seq: int,
    context_payload: Dict[str, Any],
    cycle_symbols: Dict[str, Any],
    db: AITraderDB,
    context_learning: Optional[ContextLearningEngine] = None,
) -> List[Dict[str, Any]]:
    # Context builder payloads have historically used `top_opportunities`.
    # Some newer builders emit `selected_opportunities` instead.
    top_opps = context_payload.get("top_opportunities")
    if not isinstance(top_opps, list) or not top_opps:
        top_opps = context_payload.get("selected_opportunities")
    top_opps = top_opps or []
    if not isinstance(top_opps, list):
        return []

    # Get global context for AGI decisions
    global_context = context_payload.get("global_context")

    # Net exposure bias: when the book is already heavily net-long/short, we
    # down-rank new candidates that push further in the same direction.
    try:
        _exp = (global_context or {}).get("exposure") or {}
        _cur = (_exp.get("current") or {}) if isinstance(_exp, dict) else {}
        net_exposure_usd = float(_cur.get("net") or 0.0)
    except Exception:
        net_exposure_usd = 0.0

    bias_threshold = float(NET_EXPOSURE_BIAS_THRESHOLD_USD)
    bias_penalty = float(NET_EXPOSURE_PENALTY if NET_EXPOSURE_PENALTY > 0 else 0.85)
    bias_bonus = float(NET_EXPOSURE_BONUS if NET_EXPOSURE_BONUS > 0 else 1.05)

    # Initialize context learning engine if not provided
    if context_learning is None:
        context_learning = _default_context_learning_engine()

    dossier_getter = None
    dossier_db_path = ""
    dossier_max_chars = max(128, int(SYMBOL_DOSSIER_MAX_CHARS))
    try:
        dossier_db_path = str(getattr(db, "db_path", "") or "")
    except Exception:
        dossier_db_path = ""
    if dossier_db_path:
        try:
            from learning_dossier_aggregator import get_dossier_snippet
            dossier_getter = get_dossier_snippet
        except Exception:
            dossier_getter = None

    open_trades = db.get_open_trades() if db else []
    open_pairs = {(t.symbol.upper(), t.direction.upper()) for t in open_trades}

    candidates: List[Dict[str, Any]] = []
    seen_symbols: set[str] = set()

    constraints = compile_constraints(top_opps, MAX_CANDIDATES)
    strong_count = len(list(constraints.get("strong_symbols") or []))
    limit = _adaptive_candidate_top_k(
        opportunities=top_opps,
        base_limit=int(constraints.get("top_k") or MAX_CANDIDATES),
        strong_count=strong_count,
    )

    must_trade_symbols = set(constraints.get("must_trade_symbols") or [])
    strong_by_symbol = constraints.get("strong_by_symbol") or {}
    whale_prefer_symbols = set(constraints.get("whale_prefer_symbols") or [])

    def _base_score(opp: Dict[str, Any]) -> float:
        try:
            return float(opp.get("score") or 0.0)
        except Exception:
            return 0.0

    def _effective_score(symbol: str, direction: str, base: float) -> float:
        # Apply net exposure bias only when meaningful.
        if abs(net_exposure_usd) < bias_threshold or base <= 0:
            return base
        # Net short (negative): penalize more shorts; slightly favor longs.
        if net_exposure_usd < 0:
            if direction == "SHORT":
                return base * bias_penalty
            if direction == "LONG":
                return base * bias_bonus
        # Net long (positive): penalize more longs; slightly favor shorts.
        if net_exposure_usd > 0:
            if direction == "LONG":
                return base * bias_penalty
            if direction == "SHORT":
                return base * bias_bonus
        return base

    # Build candidate selection order: must-trade first, then by effective score.
    scored: List[Tuple[float, Dict[str, Any]]] = []
    musts: List[Dict[str, Any]] = []

    for opp in top_opps:
        if not isinstance(opp, dict):
            continue
        symbol = str(opp.get("symbol") or "").upper()
        if not symbol:
            continue
        direction = str(opp.get("direction") or "").upper()
        if direction not in ("LONG", "SHORT"):
            continue
        if (symbol, direction) in open_pairs:
            continue
        if symbol in must_trade_symbols:
            musts.append(opp)
        else:
            base = _base_score(opp)
            scored.append((_effective_score(symbol, direction, base), opp))

    # Sort by effective score descending.
    scored.sort(key=lambda t: t[0], reverse=True)

    selection: List[Dict[str, Any]] = []
    for opp in musts:
        if len(selection) >= limit:
            break
        selection.append(opp)
    for _score, opp in scored:
        if len(selection) >= limit:
            break
        selection.append(opp)

    for opp in selection:
        if len(candidates) >= limit:
            break
        symbol = str(opp.get("symbol") or "").upper()
        if not symbol or symbol in seen_symbols:
            continue
        direction = str(opp.get("direction") or "").upper()
        if direction not in ("LONG", "SHORT"):
            continue
        if (symbol, direction) in open_pairs:
            continue

        symbol_data = _find_symbol_data(cycle_symbols, symbol)
        if not symbol_data:
            continue
        # HL-only trading note:
        # Do NOT gate candidates on snapshot mid availability / dual-venue mids.
        # Venue availability is enforced later via `check_symbol_on_venues(...)`
        # against the actually-enabled venues (hl_enabled/lighter_enabled).

        score_val = opp.get("score") or 0.0
        try:
            score = float(score_val)
        except (TypeError, ValueError):
            score = 0.0

        # Apply exposure bias to conviction as well (selection already used effective_score).
        eff_score = _effective_score(symbol, direction, score)
        conviction_divisor = _conviction_score_divisor()
        conviction = max(CANDIDATE_MIN_CONVICTION, min(1.0, eff_score / conviction_divisor))

        strong_signals = list(opp.get("strong_signals") or [])
        if not strong_signals:
            strong_signals = list(strong_by_symbol.get(symbol) or [])
        strong_signals = [str(s).lower() for s in strong_signals if str(s or "").strip()]
        must_trade = symbol in must_trade_symbols
        constraint_reason = None
        if must_trade:
            if "dead_capital" in strong_signals and "whale" in strong_signals:
                constraint_reason = "whale_dead_capital_strong"
            elif "dead_capital" in strong_signals:
                constraint_reason = "dead_capital_strong"
            elif "whale" in strong_signals:
                constraint_reason = "whale_strong"
            else:
                constraint_reason = "must_trade_strong"
        elif symbol in whale_prefer_symbols:
            constraint_reason = "whale_strong_default"

        # Get context-based adjustment from historical performance
        ctx_adjustment = 1.0
        ctx_breakdown = {}
        if context_learning:
            try:
                ctx_adjustment, ctx_breakdown = context_learning.get_context_adjustment(opp, direction)
            except Exception:
                pass

        # Build AGI-style candidate with simplified top-level data
        candidate = build_agi_candidate(
            opp=opp,
            rank=len(candidates) + 1,
            signals_list=_build_signals_list(opp, expected_direction=direction),
            strong_signals=strong_signals,
            must_trade=must_trade,
            constraint_reason=constraint_reason,
            conviction=conviction,
            global_context=global_context,
            context_adjustment=ctx_adjustment,
            context_breakdown=ctx_breakdown,
        )
        sig_snap: Dict[str, Any] = {}
        # Preserve full raw signals + key metrics for learning and DB journaling.
        # This is REQUIRED for:
        # - learning overlay (needs signals_snapshot)
        # - entry reasoning + SR logic (needs context_snapshot.key_metrics)
        try:
            sig_snap = opp.get("perp_signals")
            if not isinstance(sig_snap, dict):
                sig_snap = opp.get("signals")
            if isinstance(sig_snap, dict) and sig_snap:
                candidate["signals_snapshot"] = sig_snap
            else:
                sig_snap = {}
        except Exception:
            sig_snap = {}

        # Policy gate: XYZ symbols are HIP3_MAIN-only.
        if _is_hip3(symbol, symbol_data):
            hip3_main = sig_snap.get("hip3_main") if isinstance(sig_snap, dict) else None
            if not isinstance(hip3_main, dict) or not hip3_main:
                LOG.info(
                    "CANDIDATE_SKIPPED symbol=%s dir=%s reason=hip3_main_required_for_xyz",
                    symbol,
                    direction,
                )
                continue
            sl_mult, tp_mult, driver_type = _hip3_driver_sltp(hip3_main)
            booster_mult = _hip3_booster_size_mult(hip3_main)
            candidate_risk = dict(candidate.get("risk") or {})
            candidate_risk["sl_atr_mult"] = float(sl_mult)
            candidate_risk["tp_atr_mult"] = float(tp_mult)
            candidate_risk["hip3_driver"] = driver_type
            candidate_risk["hip3_booster_size_mult"] = float(booster_mult)
            candidate["risk"] = candidate_risk
            candidate["hip3_driver"] = driver_type
            try:
                comps = hip3_main.get("components") or {}
                if isinstance(comps, dict):
                    candidate["hip3_booster_score"] = float(comps.get("rest_booster_score") or 0.0)
            except Exception:
                pass

        try:
            km = opp.get("key_metrics") or {}
            if not isinstance(km, dict):
                km = {}
            ws = opp.get("worker_signals_summary") or {}
            if not isinstance(ws, dict):
                ws = {}
            candidate["context_snapshot"] = {
                "key_metrics": km,
                "worker_signals_summary": ws,
                "score": opp.get("score"),
                "direction": opp.get("direction"),
                "symbol": opp.get("symbol"),
                "seq": int(seq),
            }
        except Exception:
            pass

        if dossier_getter is not None:
            try:
                dossier = dossier_getter(
                    db_path=dossier_db_path,
                    symbol=symbol,
                    max_chars=dossier_max_chars,
                )
                if isinstance(dossier, str) and dossier.strip():
                    candidate["symbol_learning_dossier"] = dossier
            except Exception:
                pass

        candidates.append(candidate)
        seen_symbols.add(symbol)

    return candidates
