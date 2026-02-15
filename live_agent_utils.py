#!/usr/bin/env python3
"""
Utility helpers for hl-trader live agent mode.

Includes candidate schema validation and sizing/risk helpers.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import yaml


CANDIDATE_SCHEMA_VERSION = 1
VALID_DIRECTIONS = {"LONG", "SHORT"}
_SKILL_FILE = Path(__file__).with_name("skill.yaml")


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


def _skill_int(cfg: Dict[str, Any], key: str, default: int) -> int:
    try:
        value = cfg.get(key, default)
        return int(default if value is None else value)
    except Exception:
        return int(default)


_BRAIN_CFG = _load_skill_brain_cfg()
MAX_CANDIDATES = _skill_int(_BRAIN_CFG, "max_candidates", 6)


def _as_float(value: Any) -> Tuple[float, bool]:
    try:
        return float(value), True
    except (TypeError, ValueError):
        return 0.0, False


def dual_venue_mids_ok(hl_mid: Any, lighter_mid: Any) -> bool:
    """Return True when both mid prices are present and > 0."""
    hl_val, _ = _as_float(hl_mid)
    lt_val, _ = _as_float(lighter_mid)
    return hl_val > 0 and lt_val > 0


def cap_size_usd(size_usd: float, cap_usd: float) -> Tuple[float, bool]:
    """Clamp size_usd to cap_usd. Returns (new_size, was_clamped)."""
    if cap_usd <= 0:
        return size_usd, False
    if size_usd > cap_usd:
        return cap_usd, True
    return size_usd, False


def validate_candidates_payload(payload: Any) -> Dict[str, Any]:
    """Validate candidate payload and normalize candidates.

    Returns dict with keys:
      - valid: list[dict]
      - invalid: list[dict] with 'candidate' + 'errors'
      - errors: list[str] (top-level errors)
    """
    result = {"valid": [], "invalid": [], "errors": []}

    if not isinstance(payload, dict):
        result["errors"].append("payload is not an object")
        return result

    schema_version = payload.get("schema_version")
    if schema_version != CANDIDATE_SCHEMA_VERSION:
        result["errors"].append(
            f"schema_version must be {CANDIDATE_SCHEMA_VERSION}, got {schema_version}"
        )

    candidates = payload.get("candidates")
    if not isinstance(candidates, list):
        result["errors"].append("candidates must be a list")
        return result

    for idx, candidate in enumerate(candidates):
        errors: List[str] = []
        if not isinstance(candidate, dict):
            result["invalid"].append({"candidate": candidate, "errors": ["candidate is not an object"]})
            continue

        symbol = candidate.get("symbol")
        if not isinstance(symbol, str) or not symbol.strip():
            errors.append("symbol missing or empty")

        direction = str(candidate.get("direction", "")).upper()
        if direction not in VALID_DIRECTIONS:
            errors.append(f"direction must be LONG or SHORT, got {direction or 'empty'}")

        size_usd = 0.0
        size_raw = candidate.get("size_usd")
        if size_raw is not None:
            size_usd, size_ok = _as_float(size_raw)
            if not size_ok or size_usd < 0:
                errors.append(f"size_usd must be >= 0, got {size_raw}")

        conviction_raw = candidate.get("conviction")
        conviction, conv_ok = _as_float(conviction_raw)
        if not conv_ok or not (0.0 <= conviction <= 1.0):
            errors.append(f"conviction must be between 0 and 1, got {conviction_raw}")

        reason_short = candidate.get("reason_short")
        if not isinstance(reason_short, str) or not reason_short.strip():
            errors.append("reason_short missing or empty")

        signals = candidate.get("signals")
        if not isinstance(signals, list):
            errors.append("signals must be a list")

        if errors:
            result["invalid"].append({"candidate": candidate, "errors": errors, "index": idx})
            continue

        normalized = {
            "symbol": symbol.strip().upper(),
            "direction": direction,
            "size_usd": float(size_usd),
            "conviction": float(conviction),
            "reason_short": reason_short.strip(),
            "signals": signals,
            "index": idx,
        }

        # Preserve optional order-type hints for audit/journaling.
        order_type_hint = candidate.get("order_type_hint")
        if isinstance(order_type_hint, str) and order_type_hint.strip():
            normalized["order_type_hint"] = order_type_hint.strip()
        order_type_reason = candidate.get("order_type_reason")
        if isinstance(order_type_reason, str) and order_type_reason.strip():
            normalized["order_type_reason"] = order_type_reason.strip()

        # Optional fields (preserve if valid types)
        strong_signals = candidate.get("strong_signals")
        if isinstance(strong_signals, list):
            normalized["strong_signals"] = strong_signals

        # Allow forcing candidate-provided size_usd (used for HIP3 test trades).
        force_size = candidate.get("force_size_usd")
        if force_size is not None:
            normalized["force_size_usd"] = bool(force_size)

        must_trade = candidate.get("must_trade")
        if must_trade is not None:
            normalized["must_trade"] = bool(must_trade)

        constraint_reason = candidate.get("constraint_reason")
        if isinstance(constraint_reason, str) and constraint_reason.strip():
            normalized["constraint_reason"] = constraint_reason.strip()

        signals_snapshot = candidate.get("signals_snapshot")
        if isinstance(signals_snapshot, dict):
            normalized["signals_snapshot"] = signals_snapshot

        context_snapshot = candidate.get("context_snapshot")
        if isinstance(context_snapshot, dict):
            normalized["context_snapshot"] = context_snapshot

        symbol_learning_dossier = candidate.get("symbol_learning_dossier")
        if isinstance(symbol_learning_dossier, str) and symbol_learning_dossier.strip():
            normalized["symbol_learning_dossier"] = symbol_learning_dossier.strip()[:2000]

        playbook_id = candidate.get("playbook_id")
        if playbook_id is not None:
            normalized["playbook_id"] = playbook_id

        memory_ids = candidate.get("memory_ids")
        if isinstance(memory_ids, list):
            normalized["memory_ids"] = memory_ids

        execution = candidate.get("execution")
        if isinstance(execution, dict):
            normalized["execution"] = execution

        risk = candidate.get("risk")
        if isinstance(risk, dict):
            normalized["risk"] = risk

        sl_raw = candidate.get("sl")
        if sl_raw is not None:
            sl_val, sl_ok = _as_float(sl_raw)
            if sl_ok:
                normalized["sl"] = sl_val

        tp_raw = candidate.get("tp")
        if tp_raw is not None:
            tp_val, tp_ok = _as_float(tp_raw)
            if tp_ok:
                normalized["tp"] = tp_val

        rank_raw = candidate.get("rank")
        if rank_raw is not None:
            try:
                normalized["rank"] = int(rank_raw)
            except (TypeError, ValueError):
                pass

        result["valid"].append(normalized)

    return result


def enforce_max_candidates(
    candidates: List[Dict[str, Any]],
    max_candidates: int = MAX_CANDIDATES,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """Return (kept, dropped) candidates honoring max count."""
    if max_candidates <= 0:
        return [], candidates
    kept = list(candidates[:max_candidates])
    dropped = list(candidates[max_candidates:])
    return kept, dropped


def clamp_risk_pct(
    risk_pct: float,
    min_pct: float = 0.5,
    max_pct: float = 2.5,
) -> Tuple[float, bool]:
    """Clamp risk percent to a safe range."""
    try:
        value = float(risk_pct)
    except (TypeError, ValueError):
        return min_pct, True
    if value < min_pct:
        return min_pct, True
    if value > max_pct:
        return max_pct, True
    return value, False


def compute_risk_size_usd(
    equity: float,
    risk_pct: float,
    atr: Optional[float],
    sl_multiplier: float,
    price: float,
) -> float:
    """Compute size_usd from equity, risk_pct, ATR stop distance, and price."""
    try:
        equity_val = float(equity)
        risk_val = float(risk_pct)
        price_val = float(price)
        sl_mult = float(sl_multiplier)
    except (TypeError, ValueError):
        return 0.0

    if equity_val <= 0 or risk_val <= 0 or price_val <= 0 or sl_mult <= 0:
        return 0.0
    if atr is None:
        return 0.0
    try:
        atr_val = float(atr)
    except (TypeError, ValueError):
        return 0.0
    if atr_val <= 0:
        return 0.0

    risk_usd = equity_val * (risk_val / 100.0)
    stop_distance = atr_val * sl_mult
    if stop_distance <= 0:
        return 0.0
    qty = risk_usd / stop_distance
    return qty * price_val


# Direction normalization map (BULLISH->LONG, BEARISH->SHORT)
_DIRECTION_NORMALIZE = {
    "LONG": "LONG",
    "SHORT": "SHORT",
    "BULLISH": "LONG",
    "BEARISH": "SHORT",
}


def apply_learning_overlay(
    candidate: Dict[str, Any],
    learning_engine: Any,
) -> Dict[str, Any]:
    """
    Apply learning-based conviction penalties to a candidate.

    Multipliers applied:
    - pattern_mult: 0.7 if pattern should be avoided, else 1.0
    - signal_mult: product of individual signal adjustments
    - symbol_mult: symbol-specific adjustment

    Final: conviction = clamp(conviction * pattern_mult * signal_mult * symbol_mult, 0, 1)

    Returns modified candidate dict with updated conviction and learning_note.
    Never throws; returns unchanged candidate on any error.
    """
    if not learning_engine:
        return candidate

    try:
        symbol = candidate.get("symbol", "")
        direction = candidate.get("direction", "").upper()
        conviction = float(candidate.get("conviction", 0.0))
        signals_snapshot = candidate.get("signals_snapshot") or {}

        # Extract signals that agree with direction
        # Handle both 'direction' and 'signal' keys, normalize BULLISH/BEARISH
        signals_for = []
        for sig, data in signals_snapshot.items():
            if not isinstance(data, dict):
                continue
            raw_dir = str(data.get("direction") or data.get("signal") or "").upper()
            normalized_dir = _DIRECTION_NORMALIZE.get(raw_dir, "")
            if normalized_dir == direction:
                signals_for.append(sig)

        original_conviction = conviction
        notes = []

        # Pattern avoidance penalty (only if we have matching signals)
        pattern_mult = 1.0
        if signals_for and learning_engine.should_avoid_pattern(signals_for, direction):
            pattern_mult = 0.7
            notes.append("pattern_penalty=0.7")

        # Signal adjustment (product of matching signals)
        signal_mult = 1.0
        for sig in signals_for:
            adj = learning_engine.get_signal_adjustment(sig)
            if adj < 1.0:
                signal_mult *= adj
                notes.append(f"{sig}_adj={adj:.2f}")

        # Symbol adjustment (ALWAYS applies regardless of signals)
        symbol_mult = learning_engine.get_symbol_adjustment(symbol)
        if symbol_mult < 1.0:
            notes.append(f"symbol_adj={symbol_mult:.2f}")

        # Apply multipliers with clamp to [0, 1]
        final_conviction = max(0.0, min(1.0, conviction * pattern_mult * signal_mult * symbol_mult))

        # Create shallow copy with updated conviction
        result = dict(candidate)
        result["conviction"] = final_conviction

        if notes:
            learning_note = f"learning: {' '.join(notes)} ({original_conviction:.2f}->{final_conviction:.2f})"
            result["learning_note"] = learning_note

        return result

    except Exception:
        # Never crash the cycle
        return candidate
