#!/usr/bin/env python3
"""
SSE Consumer for Tracker Signals.

Connects to the symbol_watcher SSE server (default tracker.evplus.ai:8443) and receives
real-time tracker data including perp_signals for all 207 symbols.

Phase 1 of HyperLighter Trading Skill.
"""

import asyncio
import base64
import gzip
import inspect
import json
from logging_utils import get_logger
import ssl
import time
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Dict, Optional, Union

import aiohttp

from env_utils import env_int, env_str

# SSE Server Configuration
SSE_HOST = env_str(
    "EVCLAW_SSE_HOST",
    "tracker.evplus.ai",
)
SSE_PORT = env_int("EVCLAW_SSE_PORT", 8443)
SSE_ENDPOINT = env_str(
    "EVCLAW_SSE_ENDPOINT",
    "/sse/tracker",
)

# Reconnection settings
RECONNECT_MIN_DELAY = 2.0  # seconds (hard-coded)
RECONNECT_MAX_DELAY = 30.0  # seconds (hard-coded)
WATCHDOG_TIMEOUT = 60.0  # seconds - trigger reconnect if no activity (hard-coded)

# Default wallet falls back to an empty value when not configured.
DEFAULT_WALLET = env_str(
    "HYPERLIQUID_ADDRESS", env_str("EVCLAW_WALLET_ADDRESS", "")
)



@dataclass
class SSEMessage:
    """Parsed SSE message from tracker."""
    event_type: str  # 'connected', 'tracker-data', 'heartbeat'
    msg_type: Optional[str] = None  # 'snapshot' or 'delta' for tracker-data
    sequence: int = 0
    data: Dict[str, Any] = field(default_factory=dict)
    removed: list = field(default_factory=list)  # Symbols removed in delta
    timestamp: float = field(default_factory=time.time)


class TrackerSSEClient:
    """
    SSE client for connecting to the tracker symbol_watcher server.

    Handles:
    - SSL with self-signed certificates
    - GZIP decompression (base64 encoded)
    - Snapshot vs delta message types
    - Reconnection with exponential backoff
    - Activity watchdog

    Usage:
        client = TrackerSSEClient(
            wallet_address="0x...",
            on_data=my_callback
        )
        await client.connect()
    """

    def __init__(
        self,
        wallet_address: str = DEFAULT_WALLET,
        on_data: Optional[Callable[[Dict[str, Any]], Union[None, Awaitable[None]]]] = None,
        on_signal: Optional[Callable[[str, Dict], Union[None, Awaitable[None]]]] = None,
        host: str = SSE_HOST,
        port: int = SSE_PORT,
        endpoint: str = SSE_ENDPOINT,
    ):
        """
        Initialize SSE client.

        Args:
            wallet_address: Ethereum address for auth (format: 0x + 40 hex chars)
            on_data: Callback for full symbol data updates (all 207 symbols)
            on_signal: Callback for individual signal updates (symbol, signal_data)
            host: SSE server host
            port: SSE server port
            endpoint: SSE endpoint path
        """
        self.wallet_address = wallet_address
        self.on_data = on_data
        self.on_signal = on_signal
        self.host = host
        self.port = port
        self.endpoint = endpoint

        self.log = get_logger("sse_consumer")

        # Connection state
        self._connected = False
        self._should_run = False
        self._session: Optional[aiohttp.ClientSession] = None
        self._response: Optional[aiohttp.ClientResponse] = None
        self._connect_task: Optional[asyncio.Task] = None

        # Data state
        self._symbols: Dict[str, Any] = {}  # Current symbol data (merged from snapshots/deltas)
        self._last_sequence = -1
        self._last_hip3_sequence = -1
        self._last_activity = 0.0

        # Reconnection state
        self._reconnect_delay = RECONNECT_MIN_DELAY

    @property
    def url(self) -> str:
        """Build SSE URL with auth key."""
        return f"https://{self.host}:{self.port}{self.endpoint}?key={self.wallet_address}"




    async def connect(self) -> None:
        """
        Start SSE connection with automatic reconnection.

        Runs until disconnect() is called.
        """
        self._should_run = True
        self._connect_task = asyncio.create_task(self._connection_loop())
        await self._connect_task

    async def disconnect(self) -> None:
        """Disconnect from SSE server and stop reconnection."""
        self._should_run = False
        self._connected = False

        if self._response:
            self._response.close()
            self._response = None

        if self._session:
            await self._session.close()
            self._session = None

        if self._connect_task:
            self._connect_task.cancel()
            try:
                await self._connect_task
            except asyncio.CancelledError:
                pass
            self._connect_task = None

        self.log.info("Disconnected from SSE server")

    async def _connection_loop(self) -> None:
        """Main connection loop with reconnection logic."""
        while self._should_run:
            try:
                await self._connect_once()
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.log.error(f"Connection error: {e}")

            if not self._should_run:
                break

            # Exponential backoff for reconnection
            self.log.info(f"Reconnecting in {self._reconnect_delay:.1f}s...")
            await asyncio.sleep(self._reconnect_delay)
            self._reconnect_delay = min(
                self._reconnect_delay * 2,
                RECONNECT_MAX_DELAY
            )

    async def _connect_once(self) -> None:
        """Establish single SSE connection and process messages."""
        # Create SSL context that accepts self-signed certificates
        ssl_ctx = ssl.create_default_context()
        ssl_ctx.check_hostname = False
        ssl_ctx.verify_mode = ssl.CERT_NONE

        # Create session with custom SSL and large read buffer for SSE snapshots
        # SSE snapshots can be 2.6MB+ compressed, need larger chunk limit
        connector = aiohttp.TCPConnector(ssl=ssl_ctx)
        timeout = aiohttp.ClientTimeout(total=None, sock_read=WATCHDOG_TIMEOUT)

        async with aiohttp.ClientSession(
            connector=connector,
            timeout=timeout,
            read_bufsize=64 * 1024 * 1024  # 64MB buffer for large SSE payloads
        ) as session:
            self._session = session

            self.log.info(f"Connecting to {self.url}")

            async with session.get(self.url) as response:
                self._response = response

                if response.status != 200:
                    error_text = await response.text()
                    raise ConnectionError(f"HTTP {response.status}: {error_text}")

                self._connected = True
                self._reconnect_delay = RECONNECT_MIN_DELAY  # Reset backoff on success
                self._last_activity = time.time()

                self.log.info("Connected to SSE server")

                # Process SSE stream
                await self._process_stream(response)

    async def _process_stream(self, response: aiohttp.ClientResponse) -> None:
        """Process incoming SSE messages from the stream."""
        event_type = None
        data_buffer = []
        line_buffer = b''

        # Use raw read with explicit chunk handling for large SSE payloads
        async for chunk in response.content.iter_any():
            self._last_activity = time.time()

            # Accumulate chunks into lines
            line_buffer += chunk

            while b'\n' in line_buffer:
                line_bytes, line_buffer = line_buffer.split(b'\n', 1)
                line = line_bytes.decode('utf-8').rstrip('\r')

                if not line:
                    # Empty line = end of message
                    if event_type and data_buffer:
                        data_str = ''.join(data_buffer)
                        await self._handle_message(event_type, data_str)
                    event_type = None
                    data_buffer = []
                    continue

                if line.startswith('event:'):
                    event_type = line[6:].strip()
                elif line.startswith('data:'):
                    data_buffer.append(line[5:].strip())

    async def _handle_message(self, event_type: str, data_str: str) -> None:
        """Handle a complete SSE message."""
        try:
            msg = self._parse_message(event_type, data_str)

            if msg.event_type == 'connected':
                self.log.info(f"SSE canary received: client_id in data")

            elif msg.event_type == 'heartbeat':
                self.log.debug("Heartbeat received")

            elif msg.event_type == 'tracker-data':
                await self._handle_tracker_data(msg)
            elif msg.event_type == 'hip3-data':
                await self._handle_hip3_data(msg)

        except Exception as e:
            self.log.error(f"Error handling {event_type} message: {e}")

    def _parse_message(self, event_type: str, data_str: str) -> SSEMessage:
        """Parse SSE message, handling GZIP compression."""
        msg = SSEMessage(event_type=event_type)

        # Decompress if GZIP encoded
        if data_str.startswith('GZIP:'):
            try:
                compressed = base64.b64decode(data_str[5:])
                data_str = gzip.decompress(compressed).decode('utf-8')
            except Exception as e:
                self.log.warning(f"GZIP decompression failed: {e}")
                return msg

        # Parse JSON
        try:
            data = json.loads(data_str)
        except json.JSONDecodeError as e:
            self.log.warning(f"JSON parse failed: {e}")
            return msg

        # Extract message fields
        if isinstance(data, dict):
            msg.msg_type = data.get('type')  # 'snapshot' or 'delta'
            msg.sequence = data.get('seq', 0)
            msg.data = data.get('data', {})
            msg.removed = data.get('removed', [])

        return msg

    async def _invoke_callback(self, callback: Callable[..., Any], *args: Any, context: str) -> None:
        """Invoke callback supporting both sync and async handlers."""
        try:
            result = callback(*args)
            if inspect.isawaitable(result):
                await result
        except Exception as e:
            self.log.error(f"{context}: {e}")

    async def _handle_tracker_data(self, msg: SSEMessage) -> None:
        """Handle tracker-data message (snapshot or delta)."""
        # Check for sequence gaps
        if self._last_sequence >= 0 and msg.sequence > self._last_sequence + 1:
            gap = msg.sequence - self._last_sequence - 1
            self.log.warning(f"Sequence gap detected: missing {gap} messages")
            await self._request_resync("tracker sequence gap")
            return

        self._last_sequence = msg.sequence

        # Apply data based on message type
        if msg.msg_type == 'snapshot':
            # Replace all symbol data
            self._symbols = msg.data.copy()
            self.log.info(f"Snapshot received: {len(self._symbols)} symbols (seq={msg.sequence})")

        elif msg.msg_type == 'delta':
            # Merge delta into existing data
            for symbol, symbol_data in msg.data.items():
                self._symbols[symbol] = symbol_data

            # Remove deleted symbols
            for symbol in msg.removed:
                self._symbols.pop(symbol, None)

            self.log.debug(f"Delta received: {len(msg.data)} updated, {len(msg.removed)} removed (seq={msg.sequence})")

        # Invoke callbacks
        if self.on_data:
            await self._invoke_callback(
                self.on_data,
                self._symbols,
                context="on_data callback error",
            )

        if self.on_signal:
            # Call on_signal for each updated symbol with perp_signals
            for symbol, symbol_data in msg.data.items():
                perp_signals = symbol_data.get('perp_signals') if symbol_data else None
                if perp_signals:
                    await self._invoke_callback(
                        self.on_signal,
                        symbol,
                        perp_signals,
                        context=f"on_signal callback error for {symbol}",
                    )

    async def _handle_hip3_data(self, msg: SSEMessage) -> None:
        """Handle hip3-data message (snapshot or delta)."""
        if self._last_hip3_sequence >= 0 and msg.sequence > self._last_hip3_sequence + 1:
            gap = msg.sequence - self._last_hip3_sequence - 1
            self.log.warning(f"HIP3 sequence gap detected: missing {gap} messages")
            await self._request_resync("hip3 sequence gap")
            return

        self._last_hip3_sequence = msg.sequence

        if msg.msg_type in ("snapshot", "delta"):
            for symbol, symbol_data in msg.data.items():
                if not isinstance(symbol_data, dict):
                    continue
                existing = self._symbols.get(symbol)
                if not isinstance(existing, dict):
                    existing = {}
                hip3_payload = symbol_data.get("hip3_predator")
                if hip3_payload is None and symbol_data:
                    hip3_payload = symbol_data
                if isinstance(hip3_payload, dict):
                    merged = dict(existing)
                    merged["hip3_predator"] = hip3_payload
                    self._symbols[symbol] = merged

            for symbol in msg.removed:
                existing = self._symbols.get(symbol)
                if isinstance(existing, dict):
                    existing.pop("hip3_predator", None)

            if msg.msg_type == "snapshot":
                self.log.info(f"HIP3 snapshot received: {len(msg.data)} symbols (seq={msg.sequence})")
            else:
                self.log.debug(
                    f"HIP3 delta received: {len(msg.data)} updated, {len(msg.removed)} removed (seq={msg.sequence})"
                )

        if self.on_data:
            await self._invoke_callback(
                self.on_data,
                self._symbols,
                context="on_data callback error (hip3)",
            )

    async def _request_resync(self, reason: str) -> None:
        """Force reconnect so the server sends a fresh snapshot."""
        self.log.warning(f"Forcing SSE reconnect for resync: {reason}")
        self._connected = False
        self._symbols = {}
        self._last_sequence = -1
        self._last_hip3_sequence = -1
        try:
            if self._response:
                self._response.close()
        except Exception:
            pass


async def main():
    """Test SSE consumer connection."""
    def on_data(symbols: Dict[str, Any]):
        print(f"[DATA] Received {len(symbols)} symbols")
        # Print sample signal
        if symbols:
            sample = next(iter(symbols.keys()))
            perp_signals = symbols[sample].get('perp_signals')
            if perp_signals:
                print(f"[SAMPLE] {sample} signals: {list(perp_signals.keys())}")

    def on_signal(symbol: str, signals: Dict):
        # Only print strong signals
        for sig_name, sig_data in signals.items():
            if isinstance(sig_data, dict):
                z = sig_data.get('z_score') or sig_data.get('z_smart', 0)
                if abs(z) >= 2.0:
                    direction = sig_data.get('signal', 'N/A')
                    print(f"[SIGNAL] {symbol} {sig_name}: {direction} (z={z:.2f})")

    client = TrackerSSEClient(
        on_data=on_data,
        on_signal=on_signal
    )

    try:
        print("Starting SSE consumer...")
        await client.connect()
    except KeyboardInterrupt:
        print("\nShutting down...")
    finally:
        await client.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
