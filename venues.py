#!/usr/bin/env python3
"""Venue normalization and selection helpers."""

from __future__ import annotations

from typing import Iterable, List

VENUE_HYPERLIQUID = "hyperliquid"
VENUE_HYPERLIQUID_WALLET = VENUE_HYPERLIQUID
VENUE_LIGHTER = "lighter"

_ALIASES = {
    "hl_wallet": VENUE_HYPERLIQUID,
    "hip3": VENUE_HYPERLIQUID,
    "wallet": VENUE_HYPERLIQUID,
    "lighter_wallet": VENUE_LIGHTER,
    # Allow canonical values to pass through unchanged.
    VENUE_HYPERLIQUID: VENUE_HYPERLIQUID,
    VENUE_HYPERLIQUID_WALLET: VENUE_HYPERLIQUID_WALLET,
    VENUE_LIGHTER: VENUE_LIGHTER,
}


def normalize_venue(value: str) -> str:
    """Normalize venue aliases to canonical strings."""
    raw = str(value or "").strip().lower()
    if not raw:
        return ""
    return _ALIASES.get(raw, raw)


def parse_enabled_venues(value: str) -> List[str]:
    """Parse comma-delimited venues into canonical list."""
    raw = str(value or "").strip()
    if not raw:
        return []
    parts = [p.strip() for p in raw.split(",") if p.strip()]
    out: List[str] = []
    seen = set()
    for part in parts:
        norm = normalize_venue(part)
        if not norm or norm in seen:
            continue
        seen.add(norm)
        out.append(norm)
    return out




def venues_for_symbol(
    symbol: str,
    *,
    enabled_venues: Iterable[str],
    default_perps: str,
    default_hip3: str,
    mirror_wallet: bool,
    perps_venues: Iterable[str] | None = None,
) -> List[str]:
    """Return canonical venues for a symbol based on config."""
    enabled = [normalize_venue(v) for v in (enabled_venues or []) if normalize_venue(v)]
    enabled_set = set(enabled)

    sym = str(symbol or "").strip().lower()
    is_hip3 = sym.startswith("xyz:")

    if is_hip3:
        target = normalize_venue(default_hip3) or VENUE_HYPERLIQUID_WALLET
        if target != VENUE_HYPERLIQUID:
            target = VENUE_HYPERLIQUID
        return [target] if target in enabled_set else []

    # Perps: prefer explicit perps_venues list when provided.
    if perps_venues:
        out: List[str] = []
        seen = set()
        for v in perps_venues:
            norm = normalize_venue(v)
            if not norm or norm not in enabled_set or norm in seen:
                continue
            seen.add(norm)
            out.append(norm)
        return out

    venues: List[str] = []
    default = normalize_venue(default_perps)
    if default and default in enabled_set:
        venues.append(default)
    if mirror_wallet and VENUE_HYPERLIQUID in enabled_set and VENUE_HYPERLIQUID not in venues:
        venues.append(VENUE_HYPERLIQUID)
    return venues
