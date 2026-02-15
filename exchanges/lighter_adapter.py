#!/usr/bin/env python3
"""Lighter exchange adapter (crypto perps)."""

from __future__ import annotations

import asyncio
import logging
import math
import os
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

from .base import ExchangeAdapter, Position


@dataclass
class LighterMarket:
    market_id: int
    symbol: str
    size_decimals: int
    price_decimals: int
    min_base_amount: float
    last_trade_price: float


class LighterAdapter(ExchangeAdapter):
    """Adapter for Lighter exchange (crypto perps)."""

    def __init__(self, log: logging.Logger, dry_run: bool = False):
        super().__init__(log)
        self.dry_run = dry_run
        self._client = None
        self._order_api = None
        self._account_api = None
        self._markets: Dict[str, LighterMarket] = {}
        self._market_ids: Dict[int, LighterMarket] = {}
        self._account_index: Optional[int] = None
        self._api_key_index: Optional[int] = None
        self._client_order_index = int(time.time() * 1000) % 1_000_000

        # ------------------------------------------------------------------
        # Rate-limit guard (Lighter enforces per-L1Address limits, so proxies
        # won't help). We throttle requests and backoff on 429s to avoid
        # SL/TP failures and order spam from chase-limit logic.
        # ------------------------------------------------------------------
        self._rl_lock = asyncio.Lock()
        self._rl_window_seconds = 60.0
        self._rl_max_requests = int(os.getenv("LIGHTER_RL_MAX_REQUESTS", "35"))
        self._rl_timestamps: List[float] = []

        # Dry-run order bookkeeping for unit tests (Executor chase-limit expects
        # check_order_status to return filled_size/avg_price).
        self._dry_run_orders: Dict[str, Dict[str, Any]] = {}

    @property
    def name(self) -> str:
        return "Lighter"

    def _next_client_order_index(self) -> int:
        self._client_order_index += 1
        return self._client_order_index

    # ============================================================ rate limit
    @staticmethod
    def _is_rate_limit_error(exc: Exception) -> bool:
        text = str(exc)
        return ("Too Many Requests" in text) or ("ratelimit" in text.lower()) or ("(429)" in text)

    async def _throttle(self) -> None:
        """Simple sliding-window throttle."""
        if self._rl_max_requests <= 0:
            return

        async with self._rl_lock:
            while True:
                now = time.time()
                window_start = now - self._rl_window_seconds
                self._rl_timestamps = [t for t in self._rl_timestamps if t >= window_start]

                if len(self._rl_timestamps) < self._rl_max_requests:
                    self._rl_timestamps.append(now)
                    return

                # Need to wait until oldest timestamp exits window
                oldest = min(self._rl_timestamps) if self._rl_timestamps else now
                sleep_for = max(0.05, (oldest + self._rl_window_seconds) - now)
                await asyncio.sleep(sleep_for)

    async def _client_call(self, fn, *args, **kwargs):
        """Call Lighter client with throttle + 429 backoff."""
        max_retries = 4
        backoffs = [1.0, 2.0, 4.0, 8.0]

        last_exc: Optional[Exception] = None
        for attempt in range(1, max_retries + 1):
            await self._throttle()
            try:
                return await fn(*args, **kwargs)
            except Exception as exc:
                last_exc = exc
                if self._is_rate_limit_error(exc) and attempt < max_retries:
                    sleep_for = backoffs[min(attempt - 1, len(backoffs) - 1)]
                    self.log.warning(f"Lighter 429 rate limit hit; backing off {sleep_for:.1f}s (attempt {attempt}/{max_retries})")
                    await asyncio.sleep(sleep_for)
                    continue
                raise

        # Shouldn't reach, but keep mypy happy
        if last_exc:
            raise last_exc

    def _lookup_market(self, symbol: str) -> Optional[LighterMarket]:
        key = symbol.upper()
        if key in self._markets:
            return self._markets[key]
        alt = f"{key}-PERP"
        if alt in self._markets:
            return self._markets[alt]
        if key.endswith("-PERP") and key[:-5] in self._markets:
            return self._markets[key[:-5]]
        return None

    def get_min_base_amount(self, symbol: str) -> Optional[float]:
        market = self._lookup_market(symbol)
        if not market:
            return None
        return market.min_base_amount

    @staticmethod
    def _to_scaled(value: float, decimals: int, round_down: bool = False) -> int:
        if decimals < 0:
            return int(round(value))
        factor = 10 ** decimals
        scaled = value * factor
        if round_down:
            return int(math.floor(scaled + 1e-12))
        return int(round(scaled))

    @staticmethod
    def _from_scaled(value: int, decimals: int) -> float:
        if decimals <= 0:
            return float(value)
        return float(value) / (10 ** decimals)

    def _normalize_base_amount(self, value: Any, market: Optional[LighterMarket]) -> float:
        """Normalize base amounts from API payloads (often scaled ints)."""
        if value is None:
            return 0.0
        try:
            if isinstance(value, str) and value.isdigit():
                value = int(value)
            if isinstance(value, int) and market and market.size_decimals >= 0:
                return self._from_scaled(value, market.size_decimals)
            return float(value)
        except (TypeError, ValueError):
            return 0.0

    def normalize_base_size(
        self,
        symbol: str,
        size: float,
        round_down: bool = True,
    ) -> Tuple[float, Optional[float]]:
        """Normalize size to Lighter size decimals (optionally rounding down)."""
        market = self._lookup_market(symbol)
        if not market:
            return size, None
        scaled = self._to_scaled(size, market.size_decimals, round_down=round_down)
        normalized = self._from_scaled(scaled, market.size_decimals)
        return normalized, market.min_base_amount

    def classify_dust(
        self,
        symbol: str,
        size: float,
        round_down: bool = True,
    ) -> Tuple[bool, str, Optional[float], float]:
        """Classify dust sizes for Lighter.

        Returns:
            (is_dust, reason, min_base_amount, normalized_size)
        """
        market = self._lookup_market(symbol)
        if not market:
            return False, "", None, size
        scaled = self._to_scaled(size, market.size_decimals, round_down=round_down)
        normalized = self._from_scaled(scaled, market.size_decimals)
        min_base = market.min_base_amount
        if normalized <= 0:
            return True, "size rounded to zero", min_base, normalized
        if min_base is not None and normalized < min_base:
            return (
                True,
                f"size below min base amount ({normalized:.6f} < {min_base:.6f})",
                min_base,
                normalized,
            )
        return False, "", min_base, normalized

    @staticmethod
    def _signed_position_size(position: Optional[Position]) -> float:
        if not position or position.size <= 0:
            return 0.0
        if position.direction == "SHORT":
            return -abs(position.size)
        if position.direction == "LONG":
            return abs(position.size)
        return 0.0

    async def initialize(self) -> bool:
        if self.dry_run:
            self._initialized = True
            self.log.info("Lighter adapter initialized (dry run)")
            return True

        base_url = os.getenv("LIGHTER_BASE_URL", "https://mainnet.zklighter.elliot.ai")
        account_index = os.getenv("LIGHTER_ACCOUNT_INDEX", "")
        api_key = os.getenv("LIGHTER_API_KEY_PRIVATE_KEY", "")
        api_key_index = os.getenv("LIGHTER_API_KEY_INDEX", "")

        if not account_index or not api_key or not api_key_index:
            self.log.error("Lighter env missing: LIGHTER_ACCOUNT_INDEX/LIGHTER_API_KEY_PRIVATE_KEY/LIGHTER_API_KEY_INDEX")
            return False

        try:
            from lighter import SignerClient

            self._account_index = int(account_index)
            self._api_key_index = int(api_key_index)
            self._client = SignerClient(
                url=base_url,
                account_index=self._account_index,
                api_private_keys={self._api_key_index: api_key},
            )
            self._order_api = self._client.order_api
            import lighter
            self._account_api = lighter.AccountApi(self._client.api_client)

            await self._load_markets()
            self._initialized = True
            self.log.info("Lighter exchange initialized")
            return True
        except Exception as e:
            self.log.error(f"Failed to initialize Lighter: {e}")
            return False

    async def close(self) -> None:
        """Close the underlying aiohttp session to prevent resource warnings."""
        if self._client:
            try:
                await self._client.close()
                self.log.info("Lighter connection closed")
            except Exception as e:
                self.log.warning(f"Lighter close exception: {e}")

    async def _load_markets(self) -> None:
        if not self._order_api:
            return

        try:
            details = await self._fetch_order_book_details(filter="perp")
            if not details:
                return
            for item in details.order_book_details:
                market = LighterMarket(
                    market_id=int(item.market_id),
                    symbol=str(item.symbol).upper(),
                    size_decimals=int(item.size_decimals),
                    price_decimals=int(item.price_decimals),
                    min_base_amount=float(item.min_base_amount),
                    last_trade_price=float(item.last_trade_price),
                )
                self._markets[market.symbol] = market
                self._market_ids[market.market_id] = market

                if market.symbol.endswith("-PERP"):
                    self._markets[market.symbol[:-5]] = market
        except Exception as e:
            self.log.error(f"Failed to load Lighter markets: {e}")

    async def _order_book_details_from_raw(
        self,
        market_id: Optional[int] = None,
        filter: Optional[str] = None,
    ) -> Optional[Any]:
        if not self._order_api:
            return None

        try:
            response = await self._order_api.order_book_details_without_preload_content(
                market_id=market_id,
                filter=filter,
            )
            payload = await response.json(content_type=None)
        except Exception as e:
            self.log.warning(f"Lighter order_book_details raw fetch failed: {e}")
            return None

        if not isinstance(payload, dict):
            self.log.warning("Lighter order_book_details raw payload was not a dict")
            return None

        if payload.get("order_book_details") is None:
            payload["order_book_details"] = []
        if payload.get("spot_order_book_details") is None:
            payload["spot_order_book_details"] = []

        try:
            from lighter.models.order_book_details import OrderBookDetails

            return OrderBookDetails.from_dict(payload)
        except Exception as e:
            self.log.warning(f"Lighter order_book_details parse failed: {e}")
            return None

    async def _fetch_order_book_details(
        self,
        market_id: Optional[int] = None,
        filter: Optional[str] = None,
    ) -> Optional[Any]:
        if not self._order_api:
            return None

        try:
            return await self._order_api.order_book_details(
                market_id=market_id,
                filter=filter,
            )
        except Exception as e:
            if "spot_order_book_details" not in str(e) and "order_book_details" not in str(e):
                self.log.warning(f"Lighter order_book_details exception: {e}")
                return None

            self.log.warning("Lighter order_book_details validation failed; retrying with raw payload")
            return await self._order_book_details_from_raw(market_id=market_id, filter=filter)

    async def _auth_token(self) -> Optional[str]:
        if not self._client:
            return None
        try:
            auth, err = await asyncio.to_thread(self._client.create_auth_token_with_expiry)
            if err:
                self.log.error(f"Lighter auth token error: {err}")
                return None
            return auth
        except Exception as e:
            self.log.error(f"Lighter auth token exception: {e}")
            return None

    async def place_market_order(
        self,
        symbol: str,
        side: str,
        size: float,
        reduce_only: bool = False,
        best_bid: float = 0.0,
        best_ask: float = 0.0,
    ) -> Tuple[bool, Optional[str], float, float]:
        if self.dry_run:
            price = best_ask if side == 'buy' else best_bid
            self.log.info(f"[DRY_RUN] Lighter {symbol} MARKET {side.upper()} {size:.4f} @ ${price:.2f}")
            return True, f"dry_run_{int(time.time() * 1000)}", size, price

        if not self._initialized or not self._client:
            return False, None, 0.0, 0.0

        market = self._lookup_market(symbol)
        if not market:
            self.log.error(f"Lighter market not found for {symbol}")
            return False, None, 0.0, 0.0

        is_dust, dust_reason, min_base, normalized_size = self.classify_dust(
            symbol, size, round_down=reduce_only
        )
        if is_dust:
            self.log.error(
                f"Lighter market order rejected (dust): {symbol} size={size} "
                f"normalized={normalized_size:.6f} min={min_base}. {dust_reason}"
            )
            return False, None, 0.0, 0.0

        is_ask = side.lower() == 'sell'
        price = best_ask if side.lower() == 'buy' else best_bid
        if price <= 0:
            price = market.last_trade_price

        size_scaled = self._to_scaled(size, market.size_decimals, round_down=reduce_only)
        price_scaled = self._to_scaled(price, market.price_decimals)

        if size_scaled <= 0 or price_scaled <= 0:
            self.log.error(f"Invalid size/price for {symbol}: size={size}, price={price}")
            return False, None, 0.0, 0.0

        pre_position: Optional[Position] = None
        try:
            pre_position = await self.get_position(symbol)
        except Exception:
            pre_position = None

        client_order_index = self._next_client_order_index()
        try:
            _, resp, err = await self._client_call(
                self._client.create_market_order,
                market_index=market.market_id,
                client_order_index=client_order_index,
                base_amount=size_scaled,
                avg_execution_price=price_scaled,
                is_ask=is_ask,
                reduce_only=reduce_only,
            )
            if err or resp is None or resp.code != 200:
                self.log.error(f"Lighter market order failed: {err or (resp.message if resp else 'unknown error')}")
                return False, None, 0.0, 0.0

            order_id = f"{market.market_id}:{client_order_index}"
            filled, actual_size, actual_price, source = await self._verify_order_fill(
                market=market,
                client_order_index=client_order_index,
                side=side,
                requested_size=size,
                estimated_price=price,
                pre_position=pre_position,
            )
            if not filled or actual_size <= 0:
                self.log.error(
                    f"Lighter market order has no verified fills for {symbol} "
                    f"(client_order_index={client_order_index}, source={source})"
                )
                return False, order_id, 0.0, 0.0
            return True, order_id, actual_size, actual_price
        except Exception as e:
            self.log.error(f"Lighter market order exception: {e}")
            return False, None, 0.0, 0.0

    async def _query_fill_after_order(
        self,
        market: LighterMarket,
        client_order_index: int,
        requested_size: float,
        estimated_price: float,
        log_no_fill: bool = True,
    ) -> Tuple[float, float]:
        """Query actual fill data after placing a market order.

        Matches fills by ask_client_id or bid_client_id to find our order.
        Aggregates partial fills. Returns 0.0/0.0 when no fills found.
        """
        try:
            auth = await self._auth_token()
            if not auth:
                self.log.warning("H1: No auth token for fill query; cannot verify fills")
                return 0.0, 0.0

            trades = await self._order_api.trades(
                sort_by="timestamp",
                sort_dir="desc",
                limit=10,
                auth=auth,
                account_index=self._account_index,
            )
            total_size = 0.0
            weighted_price = 0.0
            for trade in trades.trades:
                if int(trade.market_id) != market.market_id:
                    continue
                # Match by client order index on either side
                if (int(trade.ask_client_id) == client_order_index or
                        int(trade.bid_client_id) == client_order_index):
                    fill_size = float(trade.size)
                    fill_price = float(trade.price)
                    total_size += fill_size
                    weighted_price += fill_size * fill_price

            if total_size > 0:
                avg_price = weighted_price / total_size
                return total_size, avg_price
            else:
                msg = (
                    f"H1: No fills found for client_order_index={client_order_index} "
                    f"(requested size={requested_size}, price={estimated_price})"
                )
                if log_no_fill:
                    self.log.warning(msg)
                else:
                    self.log.debug(msg)
                return 0.0, 0.0
        except Exception as e:
            self.log.warning(f"H1: Fill query failed ({e}); cannot verify fills")
            return 0.0, 0.0

    async def _verify_order_fill(
        self,
        market: LighterMarket,
        client_order_index: int,
        side: str,
        requested_size: float,
        estimated_price: float,
        pre_position: Optional[Position] = None,
    ) -> Tuple[bool, float, float, str]:
        """Verify fills using recent trades and optional position delta."""
        fill_size, fill_price = await self._query_fill_after_order(
            market,
            client_order_index,
            requested_size,
            estimated_price,
            log_no_fill=True,
        )
        if fill_size > 0:
            return True, fill_size, fill_price, "trades"

        if pre_position is None:
            return False, 0.0, 0.0, "no_fill"

        try:
            post_position = await self.get_position(market.symbol)
        except Exception:
            post_position = None

        signed_before = self._signed_position_size(pre_position)
        signed_after = self._signed_position_size(post_position)
        delta = signed_after - signed_before
        expected_sign = 1.0 if side.lower() == "buy" else -1.0

        if delta * expected_sign > 0:
            delta_size = abs(delta)
            price_hint = estimated_price
            if post_position and post_position.entry_price > 0 and signed_after * expected_sign > 0:
                price_hint = post_position.entry_price
            return True, delta_size, price_hint, "position_delta"

        return False, 0.0, 0.0, "no_fill"

    async def place_stop_order(
        self,
        symbol: str,
        side: str,
        size: float,
        trigger_price: float,
        order_type: str = 'sl',
    ) -> Tuple[bool, Optional[str]]:
        if self.dry_run:
            self.log.info(f"[DRY_RUN] Lighter {symbol} {order_type.upper()} {side.upper()} {size:.4f} @ ${trigger_price:.2f}")
            return True, f"dry_run_{order_type}_{int(time.time() * 1000)}"

        if not self._initialized or not self._client:
            return False, None

        market = self._lookup_market(symbol)
        if not market:
            self.log.error(f"Lighter market not found for {symbol}")
            return False, None

        is_ask = side.lower() == 'sell'
        # For reduce-only stops, never round UP beyond position size.
        size_scaled = self._to_scaled(size, market.size_decimals, round_down=True)
        trigger_scaled = self._to_scaled(trigger_price, market.price_decimals)

        if size_scaled <= 0 or trigger_scaled <= 0:
            self.log.error(f"Invalid stop order for {symbol}: size={size}, trigger={trigger_price}")
            return False, None

        is_dust, dust_reason, min_base, normalized_size = self.classify_dust(
            symbol, size, round_down=True
        )
        if is_dust:
            self.log.error(
                f"Lighter stop order rejected (dust): {symbol} size={size} "
                f"normalized={normalized_size:.6f} min={min_base}. {dust_reason}"
            )
            return False, None

        # M3: For SL orders, add slippage buffer so limit price is worse than trigger
        # to ensure fill in fast markets. TP orders use trigger as limit (price is favorable).
        SL_SLIPPAGE = 0.02  # 2% buffer
        if order_type == 'sl':
            if is_ask:  # selling (long SL) — limit below trigger
                limit_price = trigger_price * (1 - SL_SLIPPAGE)
            else:  # buying (short SL) — limit above trigger
                limit_price = trigger_price * (1 + SL_SLIPPAGE)
        else:
            limit_price = trigger_price

        limit_scaled = self._to_scaled(limit_price, market.price_decimals)
        
        # CRITICAL: Use _limit versions (GOOD_TILL_TIME) instead of non-limit versions (IOC).
        # IOC orders cancel immediately if not fillable, which fails for resting stop orders.
        
        # NONCE FIX: Retry with fresh client_order_index on invalid nonce errors.
        # The Lighter SDK hard_refreshes the nonce on first "invalid nonce" error,
        # but we need a NEW client_order_index to avoid duplicate order conflicts.
        max_nonce_retries = 3
        last_error = None
        
        for attempt in range(1, max_nonce_retries + 1):
            client_order_index = self._next_client_order_index()
            
            try:
                if order_type == 'tp':
                    _, resp, err = await self._client_call(
                        self._client.create_tp_limit_order,
                        market_index=market.market_id,
                        client_order_index=client_order_index,
                        base_amount=size_scaled,
                        trigger_price=trigger_scaled,
                        price=limit_scaled,
                        is_ask=is_ask,
                        reduce_only=True,
                    )
                else:
                    _, resp, err = await self._client_call(
                        self._client.create_sl_limit_order,
                        market_index=market.market_id,
                        client_order_index=client_order_index,
                        base_amount=size_scaled,
                        trigger_price=trigger_scaled,
                        price=limit_scaled,
                        is_ask=is_ask,
                        reduce_only=True,
                    )

                # Check for success
                if resp and resp.code == 200:
                    order_id = f"{market.market_id}:{client_order_index}"
                    # Verify immediate fills (if any) without assuming success.
                    await self._query_fill_after_order(
                        market,
                        client_order_index,
                        size,
                        trigger_price,
                        log_no_fill=False,
                    )
                    if attempt > 1:
                        self.log.info(f"Lighter {order_type.upper()} succeeded on retry {attempt}/{max_nonce_retries}")
                    return True, order_id
                
                # Check if error is nonce-related
                err_str = str(err or (resp.message if resp else 'unknown error'))
                if 'invalid nonce' in err_str.lower() or 'nonce' in err_str.lower():
                    last_error = err_str
                    self.log.warning(
                        f"Lighter {order_type.upper()} nonce error (attempt {attempt}/{max_nonce_retries}): {err_str}"
                    )
                    # Small delay before retry to allow nonce sync
                    await asyncio.sleep(0.5 * attempt)
                    continue
                else:
                    # Non-nonce error, fail immediately
                    self.log.error(f"Lighter stop order failed: {err_str}")
                    return False, None
                    
            except Exception as e:
                err_str = str(e)
                if 'invalid nonce' in err_str.lower() or 'nonce' in err_str.lower():
                    last_error = err_str
                    self.log.warning(
                        f"Lighter {order_type.upper()} nonce exception (attempt {attempt}/{max_nonce_retries}): {e}"
                    )
                    await asyncio.sleep(0.5 * attempt)
                    continue
                else:
                    self.log.error(f"Lighter stop order exception: {e}")
                    return False, None
        
        # All retries exhausted
        self.log.error(
            f"Lighter {order_type.upper()} failed after {max_nonce_retries} retries (nonce errors): {last_error}"
        )
        return False, None

    async def _resolve_order_index(self, market_id: int, client_order_index: int) -> Optional[int]:
        if not self._order_api or self._account_index is None:
            return None

        auth = await self._auth_token()
        if not auth:
            return None

        try:
            orders = await self._order_api.account_active_orders(
                market_id=market_id,
                account_index=self._account_index,
                auth=auth,
            )
            for order in orders.orders:
                if int(order.client_order_index) == int(client_order_index):
                    return int(order.order_index)
        except Exception as e:
            self.log.error(f"Lighter resolve order index exception: {e}")

        return None

    async def cancel_order(self, symbol: str, order_id: str) -> bool:
        if self.dry_run:
            self.log.info(f"[DRY_RUN] Lighter cancel {symbol} order {order_id}")
            return True

        if not self._initialized or not self._client:
            return False

        try:
            market_id_str, client_index_str = order_id.split(":", 1)
            market_id = int(market_id_str)
            client_index = int(client_index_str)
        except (ValueError, AttributeError):
            self.log.error(f"Lighter cancel expects order_id 'market_id:client_index', got {order_id}")
            return False

        order_index = await self._resolve_order_index(market_id, client_index)
        if order_index is None:
            # If we can't find it in active orders, cancellation is effectively a no-op.
            # Treat as success so higher-level logic can proceed without thrashing.
            self.log.warning(
                f"Lighter cancel: order {order_id} not in active orders "
                f"(likely already filled, cancelled, or rate-limited)"
            )
            return True

        try:
            _, resp, err = await self._client_call(
                self._client.cancel_order,
                market_index=market_id,
                order_index=order_index,
            )
            if err or resp is None or resp.code != 200:
                err_msg = str(err or (resp.message if resp else 'unknown error'))
                if 'not found' in err_msg.lower() or 'already' in err_msg.lower():
                    self.log.warning(f"Lighter cancel: order already filled/cancelled: {err_msg}")
                else:
                    self.log.error(f"Lighter cancel failed: {err_msg}")
                return False
            return True
        except Exception as e:
            self.log.error(f"Lighter cancel exception: {e}")
            return False

    async def cancel_all_orders(self, symbol: str) -> int:
        """Best-effort cancel of all active orders for a symbol."""
        if self.dry_run:
            self.log.info(f"[DRY_RUN] Lighter cancel_all_orders {symbol}")
            return 0

        if not self._initialized or not self._order_api or self._account_index is None:
            return 0

        sym = str(symbol or "").upper()
        market = self._lookup_market(sym)
        if not market:
            return 0
        market_id = market.market_id

        auth = await self._auth_token()
        if not auth:
            return 0

        attempts = 0
        try:
            orders = await self._order_api.account_active_orders(
                market_id=int(market_id),
                account_index=self._account_index,
                auth=auth,
            )
            for order in getattr(orders, "orders", []) or []:
                try:
                    oid = f"{int(market_id)}:{int(order.client_order_index)}"
                except Exception:
                    continue
                attempts += 1
                try:
                    await self.cancel_order(sym, oid)
                except Exception:
                    pass
        except Exception as e:
            self.log.warning(f"Lighter cancel_all_orders exception: {e}")

        return attempts

    async def get_position(self, symbol: str) -> Optional[Position]:
        if not self._initialized or not self._account_api or self._account_index is None:
            return None

        try:
            account = await self._account_api.account(by="index", value=str(self._account_index))
            for acct in account.accounts:
                for pos in acct.positions:
                    if pos.symbol.upper() != symbol.upper():
                        continue
                    size = float(pos.position)
                    direction = "FLAT"
                    if int(pos.sign) > 0:
                        direction = "LONG"
                    elif int(pos.sign) < 0:
                        direction = "SHORT"
                    entry_price = float(pos.avg_entry_price)
                    unrealized = float(pos.unrealized_pnl)
                    return Position(
                        symbol=pos.symbol,
                        direction=direction,
                        size=abs(size),
                        entry_price=entry_price,
                        unrealized_pnl=unrealized,
                    )
        except Exception as e:
            self.log.error(f"Lighter get_position exception: {e}")

        return None

    async def get_all_positions(self) -> Dict[str, Position]:
        if not self._initialized or not self._account_api or self._account_index is None:
            return {}

        result: Dict[str, Position] = {}
        try:
            account = await self._account_api.account(by="index", value=str(self._account_index))
            for acct in account.accounts:
                for pos in acct.positions:
                    size = float(pos.position)
                    direction = "FLAT"
                    if int(pos.sign) > 0:
                        direction = "LONG"
                    elif int(pos.sign) < 0:
                        direction = "SHORT"
                    result[pos.symbol] = Position(
                        symbol=pos.symbol,
                        direction=direction,
                        size=abs(size),
                        entry_price=float(pos.avg_entry_price),
                        unrealized_pnl=float(pos.unrealized_pnl),
                    )
        except Exception as e:
            self.log.error(f"Lighter get_all_positions exception: {e}")

        return result

    # ------------------------------------------------------------------
    # Chase Limit support
    # ------------------------------------------------------------------

    async def place_limit_order(
        self,
        symbol: str,
        side: str,
        size: float,
        price: float,
        reduce_only: bool = False,
        tif: str = "Gtc",  # ignored for now (Lighter uses GTT/limit semantics)
    ) -> Tuple[bool, Optional[str]]:
        if self.dry_run:
            self.log.info(
                f"[DRY_RUN] Lighter {symbol} LIMIT {side.upper()} {size:.4f} @ ${price:.4f}"
                f"{' (reduce_only)' if reduce_only else ''}"
            )
            oid = f"dry_run_limit_{int(time.time() * 1000)}"
            # Treat dry-run orders as immediately filled.
            self._dry_run_orders[oid] = {
                "symbol": str(symbol),
                "side": str(side),
                "size": float(size),
                "price": float(price),
                "reduce_only": bool(reduce_only),
                "status": "filled",
            }
            return True, oid

        if not self._initialized or not self._client:
            return False, None

        market = self._lookup_market(symbol)
        if not market:
            self.log.error(f"Lighter market not found for {symbol}")
            return False, None

        is_dust, dust_reason, min_base, normalized_size = self.classify_dust(
            symbol, size, round_down=reduce_only
        )
        if is_dust:
            self.log.error(
                f"Lighter limit order rejected (dust): {symbol} size={size} "
                f"normalized={normalized_size:.6f} min={min_base}. {dust_reason}"
            )
            return False, None

        is_ask = side.lower() == 'sell'
        size_scaled = self._to_scaled(size, market.size_decimals)
        price_scaled = self._to_scaled(price, market.price_decimals)

        if size_scaled <= 0 or price_scaled <= 0:
            self.log.error(f"Invalid limit order for {symbol}: size={size}, price={price}")
            return False, None

        client_order_index = self._next_client_order_index()
        try:
            from lighter.signer_client import SignerClient as SC
            _, resp, err = await self._client_call(
                self._client.create_order,
                market_index=market.market_id,
                client_order_index=client_order_index,
                base_amount=size_scaled,
                price=price_scaled,
                is_ask=1 if is_ask else 0,
                order_type=SC.ORDER_TYPE_LIMIT,
                time_in_force=SC.ORDER_TIME_IN_FORCE_GOOD_TILL_TIME,
                reduce_only=reduce_only,
            )
            if err or resp is None or resp.code != 200:
                self.log.error(
                    f"Lighter limit order failed: "
                    f"{err or (resp.message if resp else 'unknown error')}"
                )
                return False, None

            order_id = f"{market.market_id}:{client_order_index}"
            # Verify immediate fills (if any) without assuming success.
            await self._query_fill_after_order(
                market,
                client_order_index,
                size,
                price,
                log_no_fill=False,
            )
            return True, order_id
        except Exception as e:
            self.log.error(f"Lighter limit order exception: {e}")
            return False, None

    async def get_tick_size(self, symbol: str) -> float:
        if self.dry_run:
            return 0.01  # Sensible dry-run default

        market = self._lookup_market(symbol)
        if not market:
            self.log.warning(f"Lighter tick_size: market not found for {symbol}, using 0.01")
            return 0.01
        return 1.0 / (10 ** market.price_decimals)

    async def get_mid_price(self, symbol: str) -> float:
        if self.dry_run:
            market = self._lookup_market(symbol)
            if market:
                return market.last_trade_price
            return 0.0

        market = self._lookup_market(symbol)
        if not market:
            return 0.0

        # Refresh from order book details
        try:
            details = await self._fetch_order_book_details(market_id=market.market_id)
            if not details:
                return market.last_trade_price
            for item in details.order_book_details:
                if int(item.market_id) == market.market_id:
                    price = float(item.last_trade_price)
                    market.last_trade_price = price  # cache
                    return price
        except Exception as e:
            self.log.warning(f"Lighter get_mid_price exception: {e}")

        return market.last_trade_price  # fallback to cached

    async def check_order_status(
        self,
        symbol: str,
        order_id: str,
    ) -> Dict[str, Any]:
        """Check order status for Chase Limit algorithm.

        For Lighter, order_id format is 'market_id:client_order_index'.
        We check active orders first.  If order is not there, we check
        recent trades to determine fill.
        """
        result: Dict[str, Any] = {
            'status': 'unknown',
            'filled_size': 0.0,
            'remaining_size': 0.0,
            'avg_price': 0.0,
        }

        if self.dry_run:
            info = self._dry_run_orders.get(order_id) or {}
            result['status'] = str(info.get('status') or 'filled')
            result['filled_size'] = float(info.get('size') or 0.0)
            result['remaining_size'] = 0.0
            result['avg_price'] = float(info.get('price') or 0.0)
            return result

        if not self._initialized or not self._order_api or self._account_index is None:
            return result

        try:
            market_id_str, client_index_str = order_id.split(":", 1)
            market_id = int(market_id_str)
            client_index = int(client_index_str)
        except (ValueError, AttributeError):
            self.log.error(f"Lighter check_order_status: bad order_id format {order_id}")
            return result

        market = self._market_ids.get(market_id)

        # 1. Check active orders
        auth = await self._auth_token()
        if not auth:
            return result

        try:
            orders = await self._order_api.account_active_orders(
                market_id=market_id,
                account_index=self._account_index,
                auth=auth,
            )
            for order in orders.orders:
                if int(order.client_order_index) != client_index:
                    continue

                # Found in active orders
                initial_base = self._normalize_base_amount(
                    getattr(order, 'base_amount', None), market
                )
                remaining = self._normalize_base_amount(
                    getattr(order, 'remaining_base_amount', None), market
                ) if hasattr(order, 'remaining_base_amount') else initial_base

                if remaining < initial_base and remaining > 0:
                    result['status'] = 'partially_filled'
                else:
                    result['status'] = 'open'
                result['remaining_size'] = remaining
                result['filled_size'] = max(0.0, initial_base - remaining)
                if hasattr(order, 'price'):
                    price_raw = order.price
                    if isinstance(price_raw, int) and market:
                        result['avg_price'] = self._from_scaled(price_raw, market.price_decimals)
                    else:
                        result['avg_price'] = float(price_raw)
                return result
        except Exception as e:
            self.log.warning(f"Lighter check_order_status active orders exception: {e}")

        # 2. Not in active orders — check inactive orders (filled/canceled)
        try:
            inactive = await self._order_api.account_inactive_orders(
                market_id=market_id,
                account_index=self._account_index,
                auth=auth,
                limit=50,  # Recent inactive orders
            )
            for order in inactive.orders:
                if int(order.client_order_index) != client_index:
                    continue

                # Found in inactive orders
                status_str = str(order.status).lower() if hasattr(order, 'status') else 'unknown'
                filled_base = self._normalize_base_amount(
                    getattr(order, 'filled_base_amount', None), market
                )
                remaining_base = self._normalize_base_amount(
                    getattr(order, 'remaining_base_amount', None), market
                )

                if status_str == 'filled':
                    result['status'] = 'filled'
                    result['filled_size'] = filled_base
                    result['remaining_size'] = 0.0
                    # avg_filled_price may not exist; fall back to limit price
                    if hasattr(order, 'avg_filled_price') and order.avg_filled_price:
                        result['avg_price'] = float(order.avg_filled_price)
                    elif hasattr(order, 'price'):
                        price_raw = order.price
                        if isinstance(price_raw, int) and market:
                            result['avg_price'] = self._from_scaled(price_raw, market.price_decimals)
                        else:
                            result['avg_price'] = float(price_raw)
                    return result
                elif status_str == 'canceled':
                    result['status'] = 'canceled'
                    result['filled_size'] = filled_base
                    result['remaining_size'] = remaining_base
                    return result
                else:
                    # Partially filled or unknown status
                    result['status'] = status_str
                    result['filled_size'] = filled_base
                    result['remaining_size'] = remaining_base
                    return result
        except Exception as e:
            self.log.warning(f"Lighter check_order_status inactive orders exception: {e}")

        # 3. Fallback: not in active or inactive.
        # Do NOT assume canceled — the order may have filled and aged out of the
        # inactive orders window. Try a lightweight trades lookup by client id.
        if market is not None:
            try:
                fill_size, fill_price = await self._query_fill_after_order(
                    market=market,
                    client_order_index=client_index,
                    requested_size=0.0,
                    estimated_price=0.0,
                    log_no_fill=False,
                )
                if fill_size > 0:
                    result['status'] = 'filled'
                    result['filled_size'] = float(fill_size)
                    result['remaining_size'] = 0.0
                    result['avg_price'] = float(fill_price)
                    return result
            except Exception:
                pass

        result['status'] = 'unknown'
        return result

    # ------------------------------------------------------------------
    # Account trades (existing)
    # ------------------------------------------------------------------

    async def get_account_trades(self, limit: int = 50) -> List[Dict[str, Any]]:
        results, _ = await self.get_account_trades_page(limit=limit, cursor=None)
        return results

    async def get_account_trades_page(
        self,
        limit: int = 100,
        cursor: Optional[str] = None,
    ) -> Tuple[List[Dict[str, Any]], Optional[str]]:
        if self.dry_run:
            return [], None

        if not self._initialized or not self._order_api or self._account_index is None:
            return [], None

        auth = await self._auth_token()
        if not auth:
            return [], None

        try:
            trades = await self._order_api.trades(
                sort_by="timestamp",
                sort_dir="desc",
                limit=min(limit, 100),
                auth=auth,
                account_index=self._account_index,
                cursor=cursor,
            )
            results = []
            for trade in trades.trades:
                market = self._market_ids.get(int(trade.market_id))
                symbol = market.symbol if market else str(trade.market_id)

                # M5: Determine our role (maker/taker) and correct fee/side
                our_is_maker = False
                if self._account_index is not None:
                    if trade.is_maker_ask and int(trade.ask_account_id) == self._account_index:
                        our_is_maker = True
                    elif not trade.is_maker_ask and int(trade.bid_account_id) == self._account_index:
                        our_is_maker = True

                if our_is_maker:
                    fee = 0.0  # fee_maker carries the cost; fee must be 0 to avoid double-count in fill_reconciler
                    fee_maker = float(trade.maker_fee) if trade.maker_fee is not None else 0.0
                else:
                    fee = float(trade.taker_fee) if trade.taker_fee is not None else 0.0
                    fee_maker = 0.0

                # Determine our side based on account
                if self._account_index is not None:
                    if int(trade.ask_account_id) == self._account_index:
                        our_side = 'sell'
                    elif int(trade.bid_account_id) == self._account_index:
                        our_side = 'buy'
                    else:
                        our_side = 'sell' if trade.is_maker_ask else 'buy'
                else:
                    our_side = 'sell' if trade.is_maker_ask else 'buy'

                results.append({
                    'trade_id': trade.trade_id,
                    'symbol': symbol,
                    'side': our_side,
                    'size': float(trade.size),
                    'price': float(trade.price),
                    'fee': fee,
                    'fee_maker': fee_maker,
                    'timestamp': trade.timestamp,
                    'position_sign_changed': bool(trade.taker_position_sign_changed) if trade.taker_position_sign_changed is not None else False,
                })
            return results, getattr(trades, "next_cursor", None)
        except Exception as e:
            self.log.error(f"Lighter get_account_trades exception: {e}")
            return [], None
