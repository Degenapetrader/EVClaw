#!/usr/bin/env python3
"""Tests for venue normalization and selection helpers."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from venues import (
    normalize_venue,
    venues_for_symbol,
    VENUE_HYPERLIQUID,
    VENUE_HYPERLIQUID_WALLET,
    VENUE_LIGHTER,
)


def test_normalize_venue_aliases() -> None:
    assert normalize_venue("hl_wallet") == VENUE_HYPERLIQUID_WALLET
    assert normalize_venue("lighter_wallet") == VENUE_LIGHTER
    assert normalize_venue("hyperliquid") == VENUE_HYPERLIQUID


def test_venues_for_symbol_hip3_wallet_only() -> None:
    venues = venues_for_symbol(
        "xyz:NVDA",
        enabled_venues=[VENUE_HYPERLIQUID, VENUE_HYPERLIQUID_WALLET],
        default_perps=VENUE_HYPERLIQUID,
        default_hip3=VENUE_HYPERLIQUID_WALLET,
        mirror_wallet=False,
    )
    assert venues == [VENUE_HYPERLIQUID_WALLET]


def test_venues_for_symbol_hip3_wallet_disabled() -> None:
    venues = venues_for_symbol(
        "xyz:NVDA",
        enabled_venues=[VENUE_HYPERLIQUID],
        default_perps=VENUE_HYPERLIQUID,
        default_hip3=VENUE_HYPERLIQUID_WALLET,
        mirror_wallet=False,
    )
    assert venues == [VENUE_HYPERLIQUID]


def test_venues_for_symbol_perps_default_only() -> None:
    venues = venues_for_symbol(
        "ETH",
        enabled_venues=[VENUE_HYPERLIQUID, VENUE_HYPERLIQUID_WALLET],
        default_perps=VENUE_HYPERLIQUID,
        default_hip3=VENUE_HYPERLIQUID_WALLET,
        mirror_wallet=False,
    )
    assert venues == [VENUE_HYPERLIQUID]


def test_venues_for_symbol_perps_mirror() -> None:
    venues = venues_for_symbol(
        "ETH",
        enabled_venues=[VENUE_HYPERLIQUID, VENUE_HYPERLIQUID_WALLET],
        default_perps=VENUE_HYPERLIQUID,
        default_hip3=VENUE_HYPERLIQUID_WALLET,
        mirror_wallet=True,
    )
    assert venues == [VENUE_HYPERLIQUID]


def test_venues_for_symbol_perps_all_three() -> None:
    venues = venues_for_symbol(
        "BTC",
        enabled_venues=[VENUE_HYPERLIQUID, VENUE_HYPERLIQUID_WALLET, VENUE_LIGHTER],
        default_perps=VENUE_HYPERLIQUID,
        default_hip3=VENUE_HYPERLIQUID_WALLET,
        mirror_wallet=False,
        perps_venues=[VENUE_HYPERLIQUID, VENUE_HYPERLIQUID_WALLET, VENUE_LIGHTER],
    )
    assert venues == [VENUE_HYPERLIQUID, VENUE_LIGHTER]


def test_venues_for_symbol_default_wallet_no_duplicate() -> None:
    venues = venues_for_symbol(
        "ETH",
        enabled_venues=[VENUE_HYPERLIQUID_WALLET],
        default_perps=VENUE_HYPERLIQUID_WALLET,
        default_hip3=VENUE_HYPERLIQUID_WALLET,
        mirror_wallet=True,
    )
    assert venues == [VENUE_HYPERLIQUID_WALLET]


if __name__ == "__main__":
    test_normalize_venue_aliases()
    print("[PASS] normalize venue")
    test_venues_for_symbol_hip3_wallet_only()
    print("[PASS] hip3 wallet only")
    test_venues_for_symbol_hip3_wallet_disabled()
    print("[PASS] hip3 wallet disabled")
    test_venues_for_symbol_perps_default_only()
    print("[PASS] perps default only")
    test_venues_for_symbol_perps_mirror()
    print("[PASS] perps mirror")
    test_venues_for_symbol_perps_all_three()
    print("[PASS] perps all three")
    test_venues_for_symbol_default_wallet_no_duplicate()
    print("[PASS] perps default wallet no duplicate")
