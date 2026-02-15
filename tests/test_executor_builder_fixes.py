#!/usr/bin/env python3

import asyncio
import sys
from dataclasses import dataclass
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from executor import ExecutionConfig, Executor
from exchanges.base import Position


class _DummyAdapter:
    def __init__(self):
        self.last_order_error = ""
        self.calls = []

    async def get_tick_size(self, symbol: str) -> float:
        return 0.01

    async def get_best_bid_ask(self, symbol: str):
        return 100.0, 100.1

    async def get_mid_price(self, symbol: str) -> float:
        return 100.05

    async def get_position(self, symbol: str):
        # Exchange truth: open SHORT means close must BUY.
        return Position(symbol=symbol, direction="SHORT", size=2.5, entry_price=100.0, unrealized_pnl=0.0)

    async def place_limit_order(self, symbol: str, side: str, size: float, price: float, reduce_only: bool = False, tif: str = "Gtc"):
        self.calls.append({"side": side, "size": float(size), "reduce_only": bool(reduce_only)})
        if len(self.calls) == 1:
            self.last_order_error = "reduce-only would increase position"
            return False, None
        self.last_order_error = ""
        return True, "oid-1"

    async def check_order_status(self, symbol: str, order_id: str):
        return {"status": "filled", "filled_size": 2.5, "avg_price": 100.1}


def test_builder_symbol_hyperliquid_venue_is_auto_remapped() -> None:
    ex = Executor(ExecutionConfig(dry_run=True))
    out = ex._coerce_venue_for_symbol("XYZ:NVDA", "hyperliquid")
    assert out == "hyperliquid"


def test_reduce_only_self_heal_uses_exchange_direction_and_size() -> None:
    ex = Executor(ExecutionConfig(dry_run=True, chase_timeout=2.0, chase_poll_interval=0.01))
    adapter = _DummyAdapter()

    ok, filled_size, _ = asyncio.run(
        ex._chase_limit_fill(
            adapter=adapter,
            symbol="xyz:NVDA",
            side="sell",  # wrong on purpose for an exchange SHORT
            size=1.0,
            reduce_only=True,
            fallback_bid=100.0,
            fallback_ask=100.1,
        )
    )

    assert ok is True
    assert filled_size == 2.5
    assert len(adapter.calls) >= 2
    assert adapter.calls[0]["side"] == "sell"
    assert adapter.calls[1]["side"] == "buy"
    assert adapter.calls[1]["size"] == 2.5


class _MaskedErrorAdapter(_DummyAdapter):
    async def place_limit_order(self, symbol: str, side: str, size: float, price: float, reduce_only: bool = False, tif: str = "Gtc"):
        self.calls.append({"side": side, "size": float(size), "reduce_only": bool(reduce_only)})
        if len(self.calls) == 1:
            # Simulates adapter fallback masking the original error text.
            self.last_order_error = ""
            return False, None
        self.last_order_error = ""
        return True, "oid-2"


def test_reduce_only_self_heal_runs_even_when_error_hint_is_masked() -> None:
    ex = Executor(ExecutionConfig(dry_run=True, chase_timeout=2.0, chase_poll_interval=0.01))
    adapter = _MaskedErrorAdapter()

    ok, filled_size, _ = asyncio.run(
        ex._chase_limit_fill(
            adapter=adapter,
            symbol="xyz:TSLA",
            side="sell",  # wrong for exchange SHORT; retry should fix
            size=1.0,
            reduce_only=True,
            fallback_bid=100.0,
            fallback_ask=100.1,
        )
    )

    assert ok is True
    assert filled_size == 2.5
    assert len(adapter.calls) >= 2
    assert adapter.calls[1]["side"] == "buy"
    assert adapter.calls[1]["size"] == 2.5


class _InvalidSizeSameQtyAdapter(_DummyAdapter):
    async def get_position(self, symbol: str):
        return Position(symbol=symbol, direction="SHORT", size=37.722, entry_price=100.0, unrealized_pnl=0.0)

    async def place_limit_order(self, symbol: str, side: str, size: float, price: float, reduce_only: bool = False, tif: str = "Gtc"):
        self.calls.append({"side": side, "size": float(size), "reduce_only": bool(reduce_only)})
        if len(self.calls) == 1:
            self.last_order_error = "Order has invalid size."
            return False, None
        self.last_order_error = ""
        return True, "oid-3"

    async def check_order_status(self, symbol: str, order_id: str):
        return {"status": "filled", "filled_size": 37.721, "avg_price": 100.1}


def test_reduce_only_invalid_size_retries_with_slightly_smaller_size() -> None:
    ex = Executor(ExecutionConfig(dry_run=True, chase_timeout=2.0, chase_poll_interval=0.01))
    adapter = _InvalidSizeSameQtyAdapter()

    ok, filled_size, _ = asyncio.run(
        ex._chase_limit_fill(
            adapter=adapter,
            symbol="xyz:NVDA",
            side="buy",
            size=37.722,
            reduce_only=True,
            fallback_bid=100.0,
            fallback_ask=100.1,
        )
    )

    assert ok is True
    assert filled_size == 37.721
    assert len(adapter.calls) >= 2
    assert adapter.calls[1]["size"] < adapter.calls[0]["size"]


@dataclass
class _TradeRow:
    id: int
    symbol: str
    venue: str
    direction: str
    size: float
    entry_price: float
    entry_time: float
    sl_price: float | None = None
    tp_price: float | None = None
    sl_order_id: str | None = None
    tp_order_id: str | None = None
    state: str = "ACTIVE"

    def get_signals_agreed_list(self):
        return []


class _FakeDb:
    def __init__(self, rows):
        self._rows = rows
        self.closed = []

    def get_open_trades(self):
        return self._rows

    def mark_trade_closed_externally(self, trade_id: int, exit_reason: str = "EXTERNAL"):
        self.closed.append((int(trade_id), str(exit_reason)))


def test_load_positions_prefers_wallet_venue_for_builder_symbols() -> None:
    # Unified venue keeps newest row and closes stale duplicate row.
    rows = [
        _TradeRow(
            id=2,
            symbol="XYZ:NVDA",
            venue="hyperliquid",
            direction="SHORT",
            size=1.0,
            entry_price=100.0,
            entry_time=200.0,
        ),
        _TradeRow(
            id=1,
            symbol="XYZ:NVDA",
            venue="hip3",
            direction="SHORT",
            size=1.0,
            entry_price=100.0,
            entry_time=100.0,
        ),
    ]

    ex = Executor(ExecutionConfig(dry_run=True))
    ex.db = _FakeDb(rows)  # type: ignore[assignment]
    ex.persistence.save_positions_atomic = lambda _positions: None  # type: ignore[method-assign]

    positions = ex._load_positions_from_db()

    assert "hyperliquid:XYZ:NVDA" in positions
    assert positions["hyperliquid:XYZ:NVDA"].trade_id == 2
    assert ex.db.closed == [(1, "RECONCILE_DUPLICATE_OPEN_TRADE")]
