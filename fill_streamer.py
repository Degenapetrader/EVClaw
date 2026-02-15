#!/usr/bin/env python3
"""WebSocket fill streaming for Lighter + Hyperliquid."""

import asyncio
import json
from logging_utils import get_logger
import random
import time
from typing import Any, Dict, List, Optional

import aiohttp

from exchanges import VENUE_HYPERLIQUID, VENUE_HYPERLIQUID_WALLET, VENUE_LIGHTER
from fill_reconciler import FillReconciler


LIGHTER_WS_URL = "wss://mainnet.zklighter.elliot.ai/stream"
HYPERLIQUID_WS_URL = "wss://api.hyperliquid.xyz/ws"



class BaseFillStreamer:
    """Base WebSocket fill streamer with reconnect + queue backpressure."""

    RECONNECT_MIN_DELAY = 2.0
    RECONNECT_MAX_DELAY = 60.0
    HEARTBEAT_INTERVAL = 15.0
    # NOTE: Some venues (notably Hyperliquid userFills) can be quiet for long
    # periods with no messages. A too-short watchdog causes unnecessary reconnect
    # churn. Keep this conservative; per-venue streamers can override.
    WATCHDOG_TIMEOUT = 600.0
    QUEUE_MAXSIZE = 1000
    QUEUE_PUT_TIMEOUT = 2.0

    def __init__(self, venue: str, reconciler: FillReconciler, adapter: Any, ws_url: str):
        self.venue = venue
        self.reconciler = reconciler
        self.adapter = adapter
        self.ws_url = ws_url
        self.log = get_logger(f"fill_streamer.{venue}")

        self._queue: asyncio.Queue = asyncio.Queue(maxsize=self.QUEUE_MAXSIZE)
        self._running = False
        self._worker_task: Optional[asyncio.Task] = None
        self._reconnect_delay = self.RECONNECT_MIN_DELAY
        self._last_message = 0.0
        self._ws: Optional[aiohttp.ClientWebSocketResponse] = None

    async def start(self) -> None:
        """Start websocket streaming loop."""
        self._running = True
        self._worker_task = asyncio.create_task(self._process_queue())

        while self._running:
            try:
                await self._connect_once()
                self._reconnect_delay = self.RECONNECT_MIN_DELAY
            except asyncio.CancelledError:
                break
            except Exception as exc:
                self.log.error(f"WS connection error: {exc}")

            if not self._running:
                break

            delay = min(self._reconnect_delay, self.RECONNECT_MAX_DELAY)
            delay *= random.uniform(0.5, 1.0)
            self.log.info(f"Reconnecting in {delay:.1f}s...")
            await asyncio.sleep(delay)
            self._reconnect_delay = min(self._reconnect_delay * 2, self.RECONNECT_MAX_DELAY)

        await self._shutdown_worker()


    def stop(self) -> None:
        """Signal streamer to stop."""
        self._running = False
        if self._ws and not self._ws.closed:
            try:
                asyncio.create_task(self._ws.close())
            except Exception:
                pass

    async def _shutdown_worker(self) -> None:
        if self._worker_task:
            self._worker_task.cancel()
            try:
                await self._worker_task
            except asyncio.CancelledError:
                pass
            self._worker_task = None

    async def _connect_once(self) -> None:
        timeout = aiohttp.ClientTimeout(total=None)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            self.log.info(f"Connecting to {self.ws_url}")
            async with session.ws_connect(self.ws_url, heartbeat=self.HEARTBEAT_INTERVAL) as ws:
                self._ws = ws
                self._last_message = time.time()
                await self._subscribe(ws)
                await self._consume_ws(ws)
        self._ws = None

    async def _consume_ws(self, ws: aiohttp.ClientWebSocketResponse, enable_tasks: bool = True) -> None:
        heartbeat_task = None
        watchdog_task = None

        if enable_tasks:
            heartbeat_task = asyncio.create_task(self._heartbeat_loop(ws))
            watchdog_task = asyncio.create_task(self._watchdog_loop(ws))

        try:
            async for msg in ws:
                if msg.type == aiohttp.WSMsgType.TEXT:
                    self._last_message = time.time()
                    await self._handle_text(msg.data)
                elif msg.type == aiohttp.WSMsgType.ERROR:
                    raise ConnectionError(f"WS error: {ws.exception()}")
                elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.CLOSING):
                    try:
                        self.log.info(f"WS closed (type={msg.type})")
                    except Exception:
                        pass
                    break
        finally:
            for task in (heartbeat_task, watchdog_task):
                if task:
                    task.cancel()
            if heartbeat_task or watchdog_task:
                await asyncio.gather(
                    *(t for t in (heartbeat_task, watchdog_task) if t),
                    return_exceptions=True,
                )

    async def _heartbeat_loop(self, ws: aiohttp.ClientWebSocketResponse) -> None:
        while self._running and not ws.closed:
            await asyncio.sleep(self.HEARTBEAT_INTERVAL)
            try:
                await ws.ping()
            except Exception:
                break

    async def _watchdog_loop(self, ws: aiohttp.ClientWebSocketResponse) -> None:
        while self._running and not ws.closed:
            await asyncio.sleep(self.WATCHDOG_TIMEOUT / 2)
            if time.time() - self._last_message > self.WATCHDOG_TIMEOUT:
                self.log.warning("WS watchdog timeout; reconnecting")
                try:
                    await ws.close()
                except Exception:
                    pass
                break

    async def _handle_text(self, payload: str) -> None:
        try:
            message = json.loads(payload)
        except (TypeError, ValueError):
            self.log.debug("Skipping non-JSON WS message")
            return

        raw_fills = self._parse_message(message)
        if not raw_fills:
            return

        for raw in raw_fills:
            await self._enqueue_raw(raw)

    async def _enqueue_raw(self, raw: Dict[str, Any]) -> None:
        try:
            await asyncio.wait_for(self._queue.put(raw), timeout=self.QUEUE_PUT_TIMEOUT)
        except asyncio.TimeoutError:
            self.log.warning("Fill queue backpressure; dropping fill")

    async def _process_queue(self) -> None:
        while self._running or not self._queue.empty():
            try:
                raw = await asyncio.wait_for(self._queue.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue

            try:
                fill = self.reconciler._build_fill(raw)
                if not fill:
                    continue
                pair = (fill.venue, fill.exchange_trade_id)
                if self.reconciler._fills_exist_batch([pair]):
                    continue
                await self.reconciler._process_fill(fill)
            except Exception as exc:
                self.log.error(f"Failed to process streamed fill: {exc}")
            finally:
                self._queue.task_done()

    # ------------------------------------------------------------------
    # Implemented by subclasses
    # ------------------------------------------------------------------

    async def _subscribe(self, ws: aiohttp.ClientWebSocketResponse) -> None:
        raise NotImplementedError

    def _parse_message(self, message: Dict[str, Any]) -> List[Dict[str, Any]]:
        raise NotImplementedError


class LighterFillStreamer(BaseFillStreamer):
    """Lighter account trades websocket streamer."""

    def __init__(self, reconciler: FillReconciler, adapter: Any):
        super().__init__(VENUE_LIGHTER, reconciler, adapter, LIGHTER_WS_URL)
        self._account_index = getattr(adapter, "_account_index", None)

    async def _subscribe(self, ws: aiohttp.ClientWebSocketResponse) -> None:
        if self._account_index is None:
            raise RuntimeError("Missing Lighter account index for WS fill stream")
        auth = None
        if hasattr(self.adapter, "_auth_token"):
            auth = await self.adapter._auth_token()
        if not auth:
            raise RuntimeError("Failed to acquire Lighter auth token for WS fill stream")
        channel = f"account_all_trades/{self._account_index}"
        payload = {"type": "subscribe", "channel": channel, "auth": auth}
        await ws.send_str(json.dumps(payload))
        self.log.info(f"Subscribed to {channel}")

    def _parse_message(self, message: Dict[str, Any]) -> List[Dict[str, Any]]:
        if message.get("type") != "channel_data":
            return []
        channel = str(message.get("channel", ""))
        if not channel.startswith("account_all_trades"):
            return []

        data = message.get("data") or {}
        trades_by_market = data.get("trades") or {}
        fills: List[Dict[str, Any]] = []

        for market_key, trades in trades_by_market.items():
            if not isinstance(trades, list):
                continue
            for trade in trades:
                raw = self._convert_trade(trade, market_key)
                if raw:
                    fills.append(raw)
        return fills

    def _convert_trade(self, trade: Dict[str, Any], market_key: Any) -> Optional[Dict[str, Any]]:
        try:
            market_id = trade.get("market_id")
            if market_id is None:
                try:
                    market_id = int(market_key)
                except (TypeError, ValueError):
                    market_id = None

            symbol = ""
            if market_id is not None and hasattr(self.adapter, "_market_ids"):
                market = self.adapter._market_ids.get(int(market_id))
                if market:
                    symbol = market.symbol
            if not symbol:
                symbol = str(market_id or "")

            is_maker_ask = bool(trade.get("is_maker_ask", False))
            ask_account = trade.get("ask_account_id")
            bid_account = trade.get("bid_account_id")

            our_is_maker = False
            if self._account_index is not None:
                if is_maker_ask and int(ask_account) == self._account_index:
                    our_is_maker = True
                elif not is_maker_ask and int(bid_account) == self._account_index:
                    our_is_maker = True

            if self._account_index is not None:
                if int(ask_account) == self._account_index:
                    side = "sell"
                elif int(bid_account) == self._account_index:
                    side = "buy"
                else:
                    side = "sell" if is_maker_ask else "buy"
            else:
                side = "sell" if is_maker_ask else "buy"

            if our_is_maker:
                fee = 0.0
                fee_maker = float(trade.get("maker_fee", 0) or 0)
                position_sign_changed = bool(trade.get("maker_position_sign_changed", False))
            else:
                fee = float(trade.get("taker_fee", 0) or 0)
                fee_maker = 0.0
                position_sign_changed = bool(trade.get("taker_position_sign_changed", False))

            return {
                "trade_id": trade.get("trade_id"),
                "symbol": symbol,
                "side": side,
                "size": float(trade.get("size", 0)),
                "price": float(trade.get("price", 0)),
                "fee": fee,
                "fee_maker": fee_maker,
                "timestamp": trade.get("timestamp", 0),
                "position_sign_changed": position_sign_changed,
            }
        except (TypeError, ValueError):
            return None


class HyperliquidFillStreamer(BaseFillStreamer):
    """Hyperliquid userFills websocket streamer.

    Hyperliquid's userFills channel is often idle when there are no fills.
    Do not aggressively reconnect on "no messages".
    """

    # Very tolerant watchdog: rely primarily on websocket heartbeat/ping.
    WATCHDOG_TIMEOUT = 3600.0

    def __init__(self, reconciler: FillReconciler, adapter: Any, venue: str = VENUE_HYPERLIQUID):
        super().__init__(venue, reconciler, adapter, HYPERLIQUID_WS_URL)
        # Single Hyperliquid venue is used for both canonical and alias inputs.
        self._address = getattr(adapter, "_vault_address", None) or getattr(adapter, "_address", None)

    async def _subscribe(self, ws: aiohttp.ClientWebSocketResponse) -> None:
        if not self._address:
            raise RuntimeError("Missing Hyperliquid address for WS fill stream")
        payload = {
            "method": "subscribe",
            "subscription": {
                "type": "userFills",
                "user": self._address,
            },
        }
        await ws.send_str(json.dumps(payload))
        self.log.info("Subscribed to userFills")

    def _parse_message(self, message: Dict[str, Any]) -> List[Dict[str, Any]]:
        if message.get("channel") != "userFills":
            return []

        data = message.get("data") or {}
        fills = data.get("fills") or []
        if not isinstance(fills, list):
            return []

        parsed: List[Dict[str, Any]] = []
        for fill in fills:
            raw = self._convert_fill(fill)
            if raw:
                parsed.append(raw)
        return parsed

    def _normalize_hl_symbol(self, coin: str) -> str:
        try:
            c = str(coin or "").strip()
        except Exception:
            return ""
        if not c:
            return ""
        if c.lower().startswith("xyz:") and hasattr(self.adapter, "_coin_key"):
            try:
                return self.adapter._coin_key(c)
            except Exception:
                return c
        return c

    def _convert_fill(self, fill: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        try:
            trade_id = fill.get("tid", fill.get("oid", "")) or fill.get("hash", "")
            side = "buy" if str(fill.get("side", "")).upper() == "B" else "sell"
            position_sign_changed = False
            if hasattr(self.adapter, "_detect_sign_change"):
                try:
                    position_sign_changed = bool(self.adapter._detect_sign_change(fill))
                except Exception:
                    position_sign_changed = False

            symbol = self._normalize_hl_symbol(fill.get("coin", ""))
            return {
                "trade_id": str(trade_id),
                "symbol": symbol,
                "side": side,
                "size": float(fill.get("sz", 0)),
                "price": float(fill.get("px", 0)),
                "fee": float(fill.get("fee", 0) or 0),
                "fee_maker": 0.0,
                "timestamp": float(fill.get("time", 0)) / 1000.0,
                "position_sign_changed": position_sign_changed,
            }
        except (TypeError, ValueError):
            return None


def build_fill_streamer(venue: str, reconciler: FillReconciler, adapter: Any) -> BaseFillStreamer:
    if venue == VENUE_LIGHTER:
        return LighterFillStreamer(reconciler, adapter)
    if venue == VENUE_HYPERLIQUID:
        return HyperliquidFillStreamer(reconciler, adapter, VENUE_HYPERLIQUID)
    raise ValueError(f"Unknown venue for fill stream: {venue}")
