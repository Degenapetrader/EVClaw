#!/usr/bin/env python3
"""
Shared exchange adapter interface and dataclasses.

Provides abstract base class for exchange adapters with support for:
- Market orders (legacy, kept for compatibility)
- Limit orders (used by Chase Limit algorithm)
- Stop/TP trigger orders (ATR-based SL/TP)
- Order status checks and mid-price/tick-size queries
"""

from __future__ import annotations

import abc
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple


@dataclass
class OrderResult:
    """Result of an order placement."""
    success: bool
    order_id: Optional[str] = None
    filled_size: float = 0.0
    filled_price: float = 0.0
    error: str = ""


@dataclass
class Position:
    """Unified position representation across exchanges."""
    symbol: str
    direction: str  # LONG, SHORT, FLAT
    size: float
    entry_price: float
    unrealized_pnl: float = 0.0
    sl_price: float = 0.0
    tp_price: float = 0.0
    sl_order_id: Optional[str] = None
    tp_order_id: Optional[str] = None
    opened_at: str = ""  # ISO timestamp
    state: str = "IDLE"  # IDLE, ENTERING, PLACING_SLTP, ACTIVE, EXITING, COMPLETED
    signals_agreeing: List[str] = field(default_factory=list)
    venue: str = ""  # 'lighter' | 'hyperliquid' | 'hip3'
    trade_id: Optional[int] = None  # AITraderDB trade id for learning/attribution

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for persistence."""
        return {
            'symbol': self.symbol,
            'direction': self.direction,
            'size': self.size,
            'entry_price': self.entry_price,
            'unrealized_pnl': self.unrealized_pnl,
            'sl_price': self.sl_price,
            'tp_price': self.tp_price,
            'sl_order_id': self.sl_order_id,
            'tp_order_id': self.tp_order_id,
            'opened_at': self.opened_at,
            'state': self.state,
            'signals_agreeing': self.signals_agreeing,
            'venue': self.venue,
            'trade_id': self.trade_id,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Position':
        """Create from dictionary."""
        return cls(
            symbol=data.get('symbol', ''),
            direction=data.get('direction', 'FLAT'),
            size=data.get('size', 0.0),
            entry_price=data.get('entry_price', 0.0),
            unrealized_pnl=data.get('unrealized_pnl', 0.0),
            sl_price=data.get('sl_price', 0.0),
            tp_price=data.get('tp_price', 0.0),
            sl_order_id=data.get('sl_order_id'),
            tp_order_id=data.get('tp_order_id'),
            opened_at=data.get('opened_at', ''),
            state=data.get('state', 'IDLE'),
            signals_agreeing=data.get('signals_agreeing', []),
            venue=data.get('venue', ''),
            trade_id=data.get('trade_id'),
        )


class ExchangeAdapter(abc.ABC):
    """Base class for exchange adapters."""

    def __init__(self, log):
        self.log = log
        self._initialized = False

    @property
    def name(self) -> str:
        return self.__class__.__name__

    @abc.abstractmethod
    async def initialize(self) -> bool:
        """Initialize the exchange connection."""
        raise NotImplementedError

    @abc.abstractmethod
    async def place_market_order(
        self,
        symbol: str,
        side: str,  # 'buy' or 'sell'
        size: float,
        reduce_only: bool = False,
        best_bid: float = 0.0,
        best_ask: float = 0.0,
    ) -> Tuple[bool, Optional[str], float, float]:
        """Place market order. Returns (success, order_id, filled_size, filled_price)."""
        raise NotImplementedError

    @abc.abstractmethod
    async def place_stop_order(
        self,
        symbol: str,
        side: str,  # 'buy' or 'sell'
        size: float,
        trigger_price: float,
        order_type: str = 'sl',  # 'sl' or 'tp'
    ) -> Tuple[bool, Optional[str]]:
        """Place stop/take-profit order. Returns (success, order_id)."""
        raise NotImplementedError

    @abc.abstractmethod
    async def cancel_order(self, symbol: str, order_id: str) -> bool:
        """Cancel an order. Returns success."""
        raise NotImplementedError

    @abc.abstractmethod
    async def get_position(self, symbol: str) -> Optional[Position]:
        """Get current position for symbol."""
        raise NotImplementedError

    @abc.abstractmethod
    async def get_all_positions(self) -> Dict[str, Position]:
        """Get all positions."""
        raise NotImplementedError

    @abc.abstractmethod
    async def get_account_trades(self, limit: int = 50) -> List[Dict[str, Any]]:
        """Get recent account trades for fill reconciliation."""
        raise NotImplementedError

    async def get_account_trades_page(
        self,
        limit: int = 100,
        cursor: Optional[str] = None,
    ) -> Tuple[List[Dict[str, Any]], Optional[str]]:
        """Get a page of account trades with optional cursor.

        Default implementation returns a single page from get_account_trades().
        """
        return await self.get_account_trades(limit=limit), None

    # ------------------------------------------------------------------
    # Chase Limit support methods
    # ------------------------------------------------------------------

    @abc.abstractmethod
    async def place_limit_order(
        self,
        symbol: str,
        side: str,  # 'buy' or 'sell'
        size: float,
        price: float,
        reduce_only: bool = False,
        tif: str = "Gtc",  # "Gtc" | "Alo" (post-only) | "Ioc"
    ) -> Tuple[bool, Optional[str]]:
        """Place a GTC limit order.

        Returns:
            (success, order_id)
        """
        raise NotImplementedError

    @abc.abstractmethod
    async def get_tick_size(self, symbol: str) -> float:
        """Return the minimum price increment (tick) for *symbol*."""
        raise NotImplementedError

    @abc.abstractmethod
    async def get_mid_price(self, symbol: str) -> float:
        """Return the current mid / last-trade price for *symbol*."""
        raise NotImplementedError

    @abc.abstractmethod
    async def check_order_status(
        self,
        symbol: str,
        order_id: str,
    ) -> Dict[str, Any]:
        """Check the status of an order.

        Returns a dict with at least:
            'status': str  — 'open', 'partially_filled', 'filled', 'canceled', 'unknown'
            'filled_size': float
            'remaining_size': float
            'avg_price': float
        """
        raise NotImplementedError
