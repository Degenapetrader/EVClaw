#!/usr/bin/env python3
"""Shared defaults/helpers for symbol R/R category policy."""

from __future__ import annotations

from typing import Dict

from mode_controller import get_param


CATEGORY_DEFAULTS = get_param("symbol_rr_learning", "category_defaults")
OI_LARGE_CAP = get_param("symbol_rr_learning", "oi_large_cap")
OI_MID_CAP = get_param("symbol_rr_learning", "oi_mid_cap")
OI_SMALL_CAP = get_param("symbol_rr_learning", "oi_small_cap")


def classify_symbol_category(symbol: str, oi_usd: float) -> str:
    """Classify symbol into category bucket used by RR defaults."""
    sym = str(symbol or "").upper()
    if sym in ("BTC", "ETH"):
        return "MAJOR"
    if oi_usd >= OI_LARGE_CAP:
        return "LARGE_CAP"
    if oi_usd >= OI_MID_CAP:
        return "MID_CAP"
    if oi_usd >= OI_SMALL_CAP:
        return "SMALL_CAP"
    return "MICRO_CAP"


def defaults_for_category(category: str, defaults: Dict[str, Dict[str, float]] = CATEGORY_DEFAULTS) -> Dict[str, float]:
    """Return SL/TP defaults for category with safe fallback."""
    cat = str(category or "").upper()
    if cat in defaults and isinstance(defaults.get(cat), dict):
        return defaults[cat]
    return defaults.get("SMALL_CAP", {"sl": 2.0, "tp": 3.0})
