#!/usr/bin/env python3
"""Exchange router for symbol-based adapter selection."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, Optional

from .base import ExchangeAdapter
from venues import (
    VENUE_HYPERLIQUID,
    VENUE_HYPERLIQUID_WALLET,
    VENUE_LIGHTER,
    normalize_venue,
)

# VENUE_HYPERLIQUID_WALLET is intentionally aliased to VENUE_HYPERLIQUID
# via venues.py. It is kept only for backward compatibility.
VALID_VENUES = frozenset({VENUE_HYPERLIQUID, VENUE_LIGHTER})


class ExchangeRoutingError(RuntimeError):
    """Raised when routing cannot select a viable exchange."""


@dataclass
class RouterConfig:
    lighter_enabled: bool = True
    hyperliquid_enabled: bool = True
    hip3_wallet_enabled: bool = True
    mirror_hl_wallet: bool = False
    overrides: Dict[str, str] = field(default_factory=dict)


class ExchangeRouter:
    """Routes symbols to exchange adapters with optional overrides."""

    def __init__(
        self,
        config: RouterConfig,
        lighter: ExchangeAdapter,
        hyperliquid: ExchangeAdapter,
        hip3_wallet: Optional[ExchangeAdapter] = None,
        log=None,
    ) -> None:
        self.config = config
        self.lighter = lighter
        self.hyperliquid = hyperliquid
        self.hip3_wallet = hip3_wallet
        self.log = log

        self._overrides: Dict[str, str] = {}
        for symbol, target in (config.overrides or {}).items():
            if not symbol or not target:
                continue
            self._overrides[symbol.upper()] = str(target).lower()

    def _select_target(self, symbol: str) -> str:
        override = self._overrides.get(symbol.upper())
        if override:
            if override in ("hl", "hyperliquid"):
                return "hyperliquid"
            if override in ("hl_wallet", "hip3", "wallet"):
                return "hyperliquid"
            if override in ("lighter", "light", "lt"):
                return "lighter"
            raise ExchangeRoutingError(f"Unknown router override '{override}' for {symbol}")

        if symbol.lower().startswith("xyz:"):
            return "hyperliquid"
        return "lighter"

    def validate(self, venue: str, symbol: str) -> None:
        """Validate venue+symbol compatibility.

        Raises ExchangeRoutingError if the combination is invalid.
        """
        venue = normalize_venue(venue)
        if venue not in VALID_VENUES:
            raise ExchangeRoutingError(f"Unknown venue: {venue}")

        if venue == VENUE_LIGHTER and symbol.lower().startswith("xyz:"):
            raise ExchangeRoutingError(
                f"Symbol {symbol} (HIP3) cannot trade on Lighter; use --venue hyperliquid"
            )

        if venue == VENUE_HYPERLIQUID and not self.config.hyperliquid_enabled:
            raise ExchangeRoutingError(f"Hyperliquid disabled, cannot trade {symbol}")

        if venue == VENUE_LIGHTER and not self.config.lighter_enabled:
            raise ExchangeRoutingError(f"Lighter disabled, cannot trade {symbol}")

    def select(self, symbol: str) -> ExchangeAdapter:
        """Auto-select adapter by symbol. Deprecated for execution path — use validate() + direct adapter."""
        target = self._select_target(symbol)

        if target == "hyperliquid":
            if not self.config.hyperliquid_enabled:
                raise ExchangeRoutingError(f"Hyperliquid disabled, cannot trade {symbol}")
            return self.hyperliquid

        if target == "lighter":
            if not self.config.lighter_enabled:
                raise ExchangeRoutingError(f"Lighter disabled, cannot trade {symbol}")
            return self.lighter

        raise ExchangeRoutingError(f"No exchange route for {symbol}")
