#!/usr/bin/env python3
"""Tests for websocket fill streamer parsing + processing."""

import asyncio
import json
import sqlite3
import tempfile
from pathlib import Path
import sys
from types import SimpleNamespace
from typing import Optional

import aiohttp

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from ai_trader_db import AITraderDB
from fill_reconciler import FillReconciler
from fill_streamer import LighterFillStreamer, HyperliquidFillStreamer


class DummyAdapter:
    async def get_all_positions(self):
        return {}


class DummyLighterAdapter(DummyAdapter):
    def __init__(self, account_index: int, market_id: int, symbol: str):
        self._account_index = account_index
        self._market_ids = {market_id: SimpleNamespace(symbol=symbol)}


class DummyHyperliquidAdapter(DummyAdapter):
    def __init__(self, address: str, vault_address: Optional[str] = None, hip3_address: Optional[str] = None):
        self._address = address
        self._vault_address = vault_address
        self._hip3_address = hip3_address

    def _detect_sign_change(self, fill):
        return False


class DummyWS:
    def __init__(self, messages):
        self._messages = list(messages)
        self.closed = False

    def __aiter__(self):
        return self

    async def __anext__(self):
        if not self._messages:
            raise StopAsyncIteration
        return self._messages.pop(0)

    async def ping(self):
        return None

    async def close(self):
        self.closed = True


def _create_trade(db_path: str, symbol: str, venue: str) -> int:
    db = AITraderDB(db_path)
    trade_id = db.log_trade_entry(
        symbol=symbol,
        direction="LONG",
        entry_price=100.0,
        size=1.0,
        venue=venue,
    )
    with sqlite3.connect(db_path) as conn:
        conn.execute("UPDATE trades SET entry_time = ? WHERE id = ?", (1000.0, trade_id))
        conn.commit()
    return trade_id


def test_lighter_ws_stream_parses_and_processes_fill() -> None:
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    _create_trade(db_path, "ETH", "lighter")

    adapter = DummyLighterAdapter(account_index=42, market_id=1, symbol="ETH")
    reconciler = FillReconciler(db_path=db_path, exchange_adapter=adapter, venue="lighter")
    streamer = LighterFillStreamer(reconciler, adapter)

    message = {
        "type": "channel_data",
        "channel": "account_all_trades/42",
        "data": {
            "trades": {
                "1": [
                    {
                        "trade_id": 123,
                        "market_id": 1,
                        "size": 1.0,
                        "price": 100.0,
                        "ask_account_id": 99,
                        "bid_account_id": 42,
                        "is_maker_ask": True,
                        "taker_fee": 0.01,
                        "maker_fee": 0.0,
                        "timestamp": 2000,
                        "taker_position_sign_changed": False,
                        "maker_position_sign_changed": False,
                    }
                ]
            }
        },
    }

    dummy_msg = SimpleNamespace(type=aiohttp.WSMsgType.TEXT, data=json.dumps(message))
    ws = DummyWS([dummy_msg])

    async def run() -> None:
        streamer._running = True
        worker = asyncio.create_task(streamer._process_queue())
        await streamer._consume_ws(ws, enable_tasks=False)
        await streamer._queue.join()
        streamer._running = False
        worker.cancel()
        try:
            await worker
        except asyncio.CancelledError:
            pass

    asyncio.run(run())

    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            "SELECT exchange_trade_id FROM fills WHERE venue = ? AND exchange_trade_id = ?",
            ("lighter", "123"),
        ).fetchone()
        assert row is not None


def test_hyperliquid_ws_stream_parses_and_processes_fill() -> None:
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    _create_trade(db_path, "ETH", "hyperliquid")

    adapter = DummyHyperliquidAdapter(address="0xabc")
    reconciler = FillReconciler(db_path=db_path, exchange_adapter=adapter, venue="hyperliquid")
    streamer = HyperliquidFillStreamer(reconciler, adapter)

    message = {
        "channel": "userFills",
        "data": {
            "isSnapshot": False,
            "user": "0xabc",
            "fills": [
                {
                    "coin": "ETH",
                    "px": "100",
                    "sz": "1",
                    "side": "B",
                    "time": 2000000,
                    "hash": "0xhash",
                    "startPosition": "0",
                    "dir": "Open Long",
                    "closedPnl": "0",
                    "oid": 111,
                    "crossed": True,
                    "fee": "0.01",
                    "tid": 555,
                }
            ],
        },
    }

    dummy_msg = SimpleNamespace(type=aiohttp.WSMsgType.TEXT, data=json.dumps(message))
    ws = DummyWS([dummy_msg])

    async def run() -> None:
        streamer._running = True
        worker = asyncio.create_task(streamer._process_queue())
        await streamer._consume_ws(ws, enable_tasks=False)
        await streamer._queue.join()
        streamer._running = False
        worker.cancel()
        try:
            await worker
        except asyncio.CancelledError:
            pass

    asyncio.run(run())

    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            "SELECT exchange_trade_id FROM fills WHERE venue = ? AND exchange_trade_id = ?",
            ("hyperliquid", "555"),
        ).fetchone()
        assert row is not None


def test_hyperliquid_streamer_uses_wallet_address_for_wallet_venue() -> None:
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    adapter = DummyHyperliquidAdapter(
        address="0xwallet",
        vault_address="0xvault",
        hip3_address="0xhip3",
    )
    rec_hl = FillReconciler(db_path=db_path, exchange_adapter=adapter, venue="hyperliquid")
    rec_wallet = FillReconciler(db_path=db_path, exchange_adapter=adapter, venue="hip3")

    hl_stream = HyperliquidFillStreamer(rec_hl, adapter, "hyperliquid")
    wallet_stream = HyperliquidFillStreamer(rec_wallet, adapter, "hip3")

    assert hl_stream._address == "0xvault"
    assert wallet_stream._address == "0xvault"
