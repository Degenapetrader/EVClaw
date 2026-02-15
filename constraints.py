#!/usr/bin/env python3
"""
Deterministic constraints and priority compiler for hl-trader.

Enforces strong-signal inclusion and must-trade rules independent of LLM logic.
"""

from __future__ import annotations

from typing import Any, Dict, Iterable, List


def detect_strong_signals(perp_signals: Dict[str, Any]) -> List[str]:
    """Return signal names that should be treated as *strong* for constraints.

    IMPORTANT (boss directive 2026-02-01):
    - Whale + DeadCap banners are mandatory-trade triggers.
    - Other signals are context-only (they can still contribute to scoring),
      but should NOT force must-trade behavior.

    Therefore:
    - whale/dead_capital count as strong ONLY when banner_trigger=true.
    - other signals are NOT treated as strong by this constraints layer.
    """
    strong: List[str] = []
    for name, payload in (perp_signals or {}).items():
        if not isinstance(payload, dict):
            continue
        n = str(name or "").lower()
        if n not in {"whale", "dead_capital"}:
            continue
        if bool(payload.get("banner_trigger", False)):
            strong.append(n)
    return strong


def compile_constraints(
    top_opportunities: Iterable[Dict[str, Any]],
    max_candidates: int,
) -> Dict[str, Any]:
    """Compile deterministic constraint outputs from a top opportunities list."""
    strong_symbols: List[str] = []
    must_trade_symbols: List[str] = []
    whale_pref_symbols: List[str] = []
    strong_by_symbol: Dict[str, List[str]] = {}

    for opp in top_opportunities or []:
        if not isinstance(opp, dict):
            continue
        symbol = str(opp.get("symbol") or "").upper()
        if not symbol:
            continue
        strong_signals = list(opp.get("strong_signals") or [])
        if not strong_signals:
            perp = opp.get("perp_signals") or {}
            strong_signals = detect_strong_signals(perp)

        if strong_signals:
            strong_symbols.append(symbol)
            strong_by_symbol[symbol] = strong_signals

        # Mandatory *include* triggers: whale + dead_capital banners
        # (Boss directive: banner is a strong context flag, but does NOT auto-pass gates;
        # brain decides trade vs avoid at runtime.)
        if "dead_capital" in strong_signals or "whale" in strong_signals:
            must_trade_symbols.append(symbol)
        if "whale" in strong_signals:
            whale_pref_symbols.append(symbol)

    top_k = max(int(max_candidates or 0), len(strong_symbols)) if max_candidates is not None else len(strong_symbols)

    return {
        "strong_symbols": strong_symbols,
        "must_trade_symbols": must_trade_symbols,
        "whale_prefer_symbols": whale_pref_symbols,
        "strong_by_symbol": strong_by_symbol,
        "top_k": top_k,
    }
