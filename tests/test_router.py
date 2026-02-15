#!/usr/bin/env python3
"""Tests for venue validation and routing."""

import sys
from pathlib import Path
from unittest.mock import MagicMock

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from exchanges.router import (
    ExchangeRouter,
    ExchangeRoutingError,
    RouterConfig,
    VENUE_HYPERLIQUID,
    VENUE_HYPERLIQUID_WALLET,
    VENUE_LIGHTER,
    VALID_VENUES,
)


def _make_router(lighter_enabled=True, hl_enabled=True, wallet_enabled=True, mirror=False) -> ExchangeRouter:
    return ExchangeRouter(
        config=RouterConfig(
            lighter_enabled=lighter_enabled,
            hyperliquid_enabled=hl_enabled,
            hip3_wallet_enabled=wallet_enabled,
            mirror_hl_wallet=mirror,
        ),
        lighter=MagicMock(),
        hyperliquid=MagicMock(),
        hip3_wallet=MagicMock(),
    )


def test_venue_constants() -> None:
    assert VENUE_HYPERLIQUID == "hyperliquid"
    assert VENUE_HYPERLIQUID_WALLET == "hyperliquid"
    assert VENUE_LIGHTER == "lighter"
    assert VENUE_HYPERLIQUID in VALID_VENUES
    assert VENUE_HYPERLIQUID_WALLET in VALID_VENUES
    assert VENUE_LIGHTER in VALID_VENUES
    assert len(VALID_VENUES) == 2


def test_validate_unknown_venue() -> None:
    router = _make_router()
    try:
        router.validate("binance", "ETH")
        assert False, "Expected ExchangeRoutingError"
    except ExchangeRoutingError as e:
        assert "Unknown venue" in str(e)


def test_validate_hip3_on_lighter() -> None:
    router = _make_router()
    try:
        router.validate("lighter", "xyz:NVDA")
        assert False, "Expected ExchangeRoutingError"
    except ExchangeRoutingError as e:
        assert "cannot trade on Lighter" in str(e)


def test_validate_hip3_on_hyperliquid_allowed() -> None:
    router = _make_router()
    router.validate("hyperliquid", "xyz:NVDA")


def test_validate_hip3_on_wallet() -> None:
    router = _make_router()
    router.validate("hip3", "xyz:NVDA")


def test_validate_perp_on_lighter() -> None:
    router = _make_router()
    # Should not raise
    router.validate("lighter", "ETH")


def test_validate_perp_on_hyperliquid() -> None:
    router = _make_router()
    # Should not raise — perps can go on either venue
    router.validate("hyperliquid", "ETH")


def test_validate_perp_on_wallet_allowed() -> None:
    router = _make_router(mirror=False)
    router.validate("hip3", "ETH")


def test_validate_disabled_hyperliquid() -> None:
    router = _make_router(hl_enabled=False)
    try:
        router.validate("hyperliquid", "ETH")
        assert False, "Expected ExchangeRoutingError"
    except ExchangeRoutingError as e:
        assert "Hyperliquid disabled" in str(e)


def test_validate_disabled_lighter() -> None:
    router = _make_router(lighter_enabled=False)
    try:
        router.validate("lighter", "ETH")
        assert False, "Expected ExchangeRoutingError"
    except ExchangeRoutingError as e:
        assert "Lighter disabled" in str(e)


def test_validate_alias_normalization() -> None:
    router = _make_router()
    router.validate("hl_wallet", "xyz:NVDA")


def test_select_builder_symbol_routes_to_wallet_case_insensitive() -> None:
    router = _make_router()
    selected = router.select("XYZ:NVDA")
    assert selected is router.hyperliquid


if __name__ == "__main__":
    test_venue_constants()
    print("[PASS] venue constants")
    test_validate_unknown_venue()
    print("[PASS] validate unknown venue")
    test_validate_hip3_on_lighter()
    print("[PASS] validate HIP3 on lighter")
    test_validate_hip3_on_hyperliquid_allowed()
    print("[PASS] validate HIP3 on hyperliquid allowed")
    test_validate_hip3_on_wallet()
    print("[PASS] validate HIP3 on wallet")
    test_validate_perp_on_lighter()
    print("[PASS] validate perp on lighter")
    test_validate_perp_on_hyperliquid()
    print("[PASS] validate perp on hyperliquid")
    test_validate_perp_on_wallet_allowed()
    print("[PASS] validate perp on wallet allowed")
    test_validate_disabled_hyperliquid()
    print("[PASS] validate disabled hyperliquid")
    test_validate_disabled_lighter()
    print("[PASS] validate disabled lighter")
    test_validate_alias_normalization()
    print("[PASS] validate alias normalization")
