#!/usr/bin/env python3
"""
Fill Reconciler for Captain EVP AI Trading Agent.

Adapts the signal_tester's 100% fill tracking pattern.
Since we trade from a single account (not 6), simplifies to:
- Single account polling
- Fill classification (ENTRY/EXIT/SL/TP)
- Trade closure on position_sign_changed
- Net PnL calculation (fees deducted)

Pattern from fill-tracking-flow.md:
- 60s reconciliation cycle
- Batch idempotency check
- Accurate fill classification using stored SL/TP prices
"""

import asyncio
import json
import os
from logging_utils import get_logger
import random
import sqlite3
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, Tuple

from venues import normalize_venue


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or str(raw).strip() == "":
        return int(default)
    try:
        return int(float(str(raw).strip()))
    except Exception:
        return int(default)


BACKSTOP_POSITIONS_TIMEOUT_SEC = 10.0
ORPHAN_MATCH_WINDOW_SEC = 300.0
ORPHAN_AMBIGUITY_GAP_SEC = 5.0
BACKOFF_ALERT_THRESHOLD_SEC = 120.0
BACKOFF_FORCE_LOOKBACK_BUFFER_SEC = 60.0
RECONCILE_INTERVAL_SEC = max(
    1,
    _env_int("EVCLAW_FILL_RECONCILE_INTERVAL_SEC", 60),
)
RECONCILE_JITTER_SEC = 0.0
BACKSTOP_EVERY_CYCLES = max(
    1,
    _env_int("EVCLAW_FILL_BACKSTOP_EVERY_CYCLES", 5),
)
PENDING_EXIT_REASON_MAX_AGE_SEC = 21600.0


@dataclass
class Fill:
    """Fill from exchange trades API."""
    venue: str
    exchange_trade_id: str
    symbol: str
    fill_time: float
    fill_price: float
    fill_size: float
    side: str  # 'buy', 'sell'
    fee: float
    fee_maker: float
    position_sign_changed: bool
    raw_json: str


@dataclass
class ReconcileState:
    """Checkpoint for fill reconciliation paging."""
    last_fill_time: Optional[float] = None
    last_exchange_trade_id: Optional[str] = None


class FillReconciler:
    """
    Polls exchange trades API and reconciles fills with trades.

    Pattern from fill-tracking-flow.md:
    - 60s cycle
    - Batch idempotency check
    - Classify fills (ENTRY/EXIT/SL/TP)
    - Close trades on position_sign_changed
    - Net PnL = gross PnL - total fees
    """

    RECONCILE_INTERVAL = max(1, RECONCILE_INTERVAL_SEC)
    DEFAULT_PAGE_LIMIT = 200
    DEFAULT_MAX_PAGES = 50
    DEFAULT_OVERLAP_SECONDS = 300
    DEFAULT_BACKOFF_BASE_SECONDS = 5.0
    DEFAULT_BACKOFF_MAX_SECONDS = 300.0

    def __init__(
        self,
        db_path: str,
        exchange_adapter,
        venue: str = "lighter",
        on_trade_close: Optional[Callable[[Dict[str, Any]], None]] = None,
        page_limit: int = DEFAULT_PAGE_LIMIT,
        max_pages: int = DEFAULT_MAX_PAGES,
        overlap_seconds: int = DEFAULT_OVERLAP_SECONDS,
    ):
        """
        Initialize fill reconciler.

        Args:
            db_path: Path to SQLite database
            exchange_adapter: ExchangeAdapter with get_account_trades()
            venue: Exchange venue name (e.g., 'lighter', 'hyperliquid')
            on_trade_close: Callback when trade is closed (for learning/safety)
        """
        self.db_path = db_path
        self.exchange = exchange_adapter
        self.venue = normalize_venue(venue)
        self.on_trade_close = on_trade_close
        self.log = get_logger(f"fill_reconciler.{self.venue}")
        self._running = False
        self.page_limit = page_limit
        self.max_pages = max_pages
        self.overlap_seconds = overlap_seconds
        self._reconcile_state_ready = False
        self._conn: Optional[sqlite3.Connection] = None
        self._conn_lock = threading.Lock()
        self._has_trade_state_column = self._check_trade_state_column()
        self._has_trade_partial_pnl_columns = self._check_trade_partial_pnl_columns()
        self._backoff_base_seconds = float(self.DEFAULT_BACKOFF_BASE_SECONDS)
        self._backoff_max_seconds = float(self.DEFAULT_BACKOFF_MAX_SECONDS)
        self._backoff_seconds = 0.0
        self._backoff_until = 0.0
        self._backoff_started_at = 0.0
        self._backoff_alerted = False
        self._force_since_time: Optional[float] = None
        self._symbol_lock_owner = f"fill_reconciler:{os.getpid()}:{id(self)}"
        self._cycle_count = 0
        self._backstop_every_cycles = int(BACKSTOP_EVERY_CYCLES)

    def _get_persistent_conn(self) -> sqlite3.Connection:
        with self._conn_lock:
            conn = self._conn
            if conn is not None:
                return conn
            conn = sqlite3.connect(self.db_path, timeout=30.0, check_same_thread=False)
            conn.execute("PRAGMA busy_timeout=30000")
            self._conn = conn
            return conn

    def _close_db_conn(self) -> None:
        with self._conn_lock:
            conn = self._conn
            self._conn = None
        if conn is None:
            return
        try:
            conn.close()
        except Exception:
            pass

    @contextmanager
    def _db_conn(self):
        """Centralized sqlite connection policy for reconciler operations.

        Reuses a process-local connection to reduce open/close churn each cycle.
        """
        conn = self._get_persistent_conn()
        try:
            yield conn
            try:
                conn.commit()
            except Exception:
                pass
        except Exception:
            try:
                conn.rollback()
            except Exception:
                pass
            raise

    def _acquire_symbol_lock(
        self,
        symbol: str,
        venue: str,
        *,
        lock_type: str = "RECONCILE_CLOSE",
        reason: str = "",
        ttl_seconds: float = 120.0,
    ) -> bool:
        """Best-effort cross-process lock shared with executor close paths."""
        sym = str(symbol or "").upper().strip()
        ven = normalize_venue(venue)
        if not sym or not ven:
            return False
        now = time.time()
        exp = now + float(ttl_seconds or 0.0)

        try:
            with self._db_conn() as conn:
                conn.execute("BEGIN IMMEDIATE")
                conn.execute("DELETE FROM symbol_locks WHERE expires_at < ?", (now,))
                row = conn.execute(
                    "SELECT owner FROM symbol_locks WHERE symbol=? AND venue=?",
                    (sym, ven),
                ).fetchone()
                if row and str(row[0] or "") == self._symbol_lock_owner:
                    conn.execute(
                        "UPDATE symbol_locks SET expires_at=?, lock_type=?, reason=? WHERE symbol=? AND venue=?",
                        (exp, lock_type, reason, sym, ven),
                    )
                    conn.commit()
                    return True
                try:
                    conn.execute(
                        """
                        INSERT INTO symbol_locks (symbol, venue, owner, lock_type, reason, acquired_at, expires_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                        """,
                        (sym, ven, self._symbol_lock_owner, lock_type, reason, now, exp),
                    )
                    conn.commit()
                    return True
                except sqlite3.IntegrityError:
                    return False
        except Exception as e:
            self.log.debug(f"Symbol lock acquire failed for {ven}/{sym}: {e}")
            return False

    def _release_symbol_lock(self, symbol: str, venue: str) -> None:
        sym = str(symbol or "").upper().strip()
        ven = normalize_venue(venue)
        if not sym or not ven:
            return
        try:
            with self._db_conn() as conn:
                conn.execute(
                    "DELETE FROM symbol_locks WHERE symbol=? AND venue=? AND owner=?",
                    (sym, ven, self._symbol_lock_owner),
                )
                conn.commit()
        except Exception as e:
            self.log.debug(f"Symbol lock release failed for {ven}/{sym}: {e}")

    async def start(self) -> None:
        """Start reconciliation loop."""
        self._running = True
        self.log.info(f"Starting fill reconciliation loop ({self.RECONCILE_INTERVAL}s interval)")
        try:
            while self._running:
                try:
                    await self._reconcile_cycle()
                except Exception as e:
                    self.log.error(f"Reconciliation error: {e}")
                sleep_for = float(self.RECONCILE_INTERVAL)
                if RECONCILE_JITTER_SEC > 0:
                    sleep_for += random.uniform(0.0, RECONCILE_JITTER_SEC)
                await asyncio.sleep(sleep_for)
        finally:
            self._close_db_conn()

    def stop(self) -> None:
        """Stop reconciliation loop."""
        self._running = False
        self.log.info("Fill reconciler stopped")

    def __del__(self) -> None:
        try:
            self._close_db_conn()
        except Exception:
            pass


    async def reconcile_now(self) -> int:
        """Run single reconciliation cycle immediately. Returns number of fills processed."""
        return await self._reconcile_cycle()

    async def _reconcile_cycle(self) -> int:
        """Single reconciliation cycle. Returns number of new fills processed."""
        self._cycle_count += 1
        state = self._get_reconcile_state()

        # 1. Fetch recent fills from exchange (paged)
        fills, latest_fill = await self._fetch_fills(state)

        # 2. Batch check which fills already exist (idempotency)
        processed = 0
        if fills:
            fill_pairs = [(f.venue, f.exchange_trade_id) for f in fills]
            existing_pairs = self._fills_exist_batch_chunked(fill_pairs)
            new_fills = [f for f in fills if (f.venue, f.exchange_trade_id) not in existing_pairs]

            if new_fills:
                self.log.info(f"Processing {len(new_fills)} new fills")
                for fill in new_fills:
                    await self._process_fill(fill)
                processed = len(new_fills)
            else:
                self.log.debug("No new fills")

        # 3. Persist reconcile state heartbeat every cycle.
        #
        # Root cause fixed: previously updated_at advanced ONLY when a newer fill
        # was observed. Quiet venues (no recent fills) looked stale and triggered
        # repeated external restarts despite the reconciler being healthy.
        checkpoint_fill_time = state.last_fill_time
        checkpoint_exchange_trade_id = state.last_exchange_trade_id
        if latest_fill and (not checkpoint_fill_time or latest_fill.fill_time > checkpoint_fill_time):
            checkpoint_fill_time = latest_fill.fill_time
            checkpoint_exchange_trade_id = latest_fill.exchange_trade_id
        self._upsert_reconcile_state(checkpoint_fill_time, checkpoint_exchange_trade_id)

        # 4. Attempt to attach orphan fills (race: fill may arrive before trade row is logged)
        self._reattach_orphan_fills(lookback_seconds=max(self.overlap_seconds, 1800))

        # 5. Retry deferred fill-driven closes (e.g. symbol lock contention).
        await self._retry_deferred_trade_closes()

        # 6. Backstop: close trades that are open in DB but flat on exchange.
        # This is intentionally throttled (default every 5 cycles) because the
        # full-position snapshot call is heavy and does not need 60s cadence.
        should_run_backstop = (self._cycle_count % max(1, int(self._backstop_every_cycles))) == 0
        if should_run_backstop:
            await self._backstop_external_closes()

        return processed

    async def _fetch_fills(self, state: ReconcileState) -> Tuple[List[Fill], Optional[Fill]]:
        """Fetch fills from exchange API with paging + overlap window."""
        now = time.time()
        if self._backoff_seconds > 0 and self._backoff_until <= now:
            self._on_backoff_end()
        since_time = self._compute_since_time(state)
        if self._force_since_time is not None:
            since_time = min(since_time, self._force_since_time)
            self.log.warning(
                f"Forcing extended lookback after backoff: since_time={since_time:.0f}"
            )
            self._force_since_time = None
        cursor = None
        pages = 0
        fills: List[Fill] = []
        latest_fill: Optional[Fill] = None
        seen_pairs = set()

        while pages < self.max_pages:
            page_fills, next_cursor = await self._fetch_fills_page(limit=self.page_limit, cursor=cursor)
            if not page_fills:
                break

            pages += 1
            oldest_time = None

            for raw in page_fills:
                fill = self._build_fill(raw)
                if not fill:
                    continue

                if latest_fill is None or fill.fill_time > latest_fill.fill_time:
                    latest_fill = fill

                if oldest_time is None or fill.fill_time < oldest_time:
                    oldest_time = fill.fill_time

                if fill.fill_time < since_time:
                    continue

                pair = (fill.venue, fill.exchange_trade_id)
                if pair in seen_pairs:
                    continue
                seen_pairs.add(pair)
                fills.append(fill)

            if oldest_time is not None and oldest_time <= since_time:
                break

            if not next_cursor:
                if oldest_time is not None and oldest_time > since_time:
                    self.log.warning(
                        "Pagination ended before reaching since_time; "
                        "older fills may be missed"
                    )
                break

            if next_cursor == cursor:
                self.log.warning("Pagination cursor did not advance; stopping")
                break

            cursor = next_cursor

        # IMPORTANT: Ordering within identical timestamps matters.
        # Hyperliquid can emit multiple fills with the *same* fill_time for a stop/market
        # order. If we process the `position_sign_changed=True` fill first, we may close the
        # trade in DB and then fail to match the remaining fills (they become ORPHAN).
        #
        # So: process non-sign-changing fills first, then sign-changing fills.
        fills.sort(
            key=lambda f: (
                f.fill_time,
                str(f.symbol or "").upper(),
                1 if f.position_sign_changed else 0,
                str(f.exchange_trade_id or ""),
            )
        )
        return fills, latest_fill

    async def _fetch_fills_page(
        self,
        limit: int,
        cursor: Optional[str],
    ) -> Tuple[List[Dict[str, Any]], Optional[str]]:
        """Fetch a single page of raw fills."""
        now = time.time()
        if self._backoff_until > now:
            remaining = self._backoff_until - now
            if self._backoff_started_at > 0:
                elapsed = now - self._backoff_started_at
                if elapsed >= BACKOFF_ALERT_THRESHOLD_SEC and not self._backoff_alerted:
                    self._backoff_alerted = True
                    self.log.warning(
                        f"Backoff active for {elapsed:.1f}s on {self.venue}; "
                        "ENTRY fills may be delayed"
                    )
            self.log.warning(
                f"Rate limit backoff active for {self.venue} fills; "
                f"skipping fetch for {remaining:.1f}s"
            )
            return [], None

        try:
            if hasattr(self.exchange, "get_account_trades_page"):
                result = await self.exchange.get_account_trades_page(limit=limit, cursor=cursor)
            else:
                raw_fills = await self.exchange.get_account_trades(limit=limit)
                result = (raw_fills, None)
            self._reset_backoff()
            return result
        except Exception as e:
            if self._is_rate_limit_error(e):
                self._apply_backoff()
            else:
                self.log.error(f"Failed to fetch fills page: {e}")
            return [], None

    def _is_rate_limit_error(self, exc: Exception) -> bool:
        """Detect rate limit errors (429 / Too Many Requests)."""
        msg = str(exc).lower()
        return "too many requests" in msg or "429" in msg

    def _apply_backoff(self) -> None:
        """Apply exponential backoff with jitter for rate limits."""
        if self._backoff_started_at <= 0:
            self._backoff_started_at = time.time()
        if self._backoff_seconds <= 0:
            next_base = self._backoff_base_seconds
        else:
            next_base = min(self._backoff_seconds * 2, self._backoff_max_seconds)
        self._backoff_seconds = next_base
        jitter = random.uniform(0.5, 1.0)
        delay = min(self._backoff_max_seconds, next_base * jitter)
        self._backoff_until = time.time() + delay
        self.log.warning(
            f"Rate limited fetching fills for {self.venue}; "
            f"backing off {delay:.1f}s (base={next_base:.1f}s)"
        )

    def _reset_backoff(self) -> None:
        """Reset backoff after a successful fetch."""
        if self._backoff_seconds <= 0 and self._backoff_until <= 0:
            return
        self._backoff_seconds = 0.0
        self._backoff_until = 0.0
        self._backoff_started_at = 0.0
        self._backoff_alerted = False

    def _on_backoff_end(self) -> None:
        if self._backoff_started_at <= 0:
            self._reset_backoff()
            return
        now = time.time()
        gap = max(0.0, now - self._backoff_started_at)
        lookback = max(self.overlap_seconds, gap + BACKOFF_FORCE_LOOKBACK_BUFFER_SEC)
        self._force_since_time = now - lookback
        self.log.warning(
            f"Backoff ended; forcing lookback window {lookback:.1f}s"
        )
        self._reset_backoff()

    def _build_fill(self, raw: Dict[str, Any]) -> Optional[Fill]:
        """Convert raw exchange fill dict into Fill dataclass.

        IMPORTANT: normalize timestamps to *seconds* here so both:
        - filtering (since_time)
        - reconcile checkpoint (fill_reconcile_state)
        are consistent.
        """
        try:
            exchange_trade_id = str(raw.get('trade_id', ''))
            if not exchange_trade_id:
                return None

            fill_time = float(raw.get('timestamp', 0) or 0)
            if fill_time > 1e12:
                fill_time = fill_time / 1000.0

            return Fill(
                venue=self.venue,
                exchange_trade_id=exchange_trade_id,
                symbol=str(raw.get('symbol', '')),
                fill_time=fill_time,
                fill_price=float(raw.get('price', 0)),
                fill_size=float(raw.get('size', 0)),
                side=str(raw.get('side', '')).lower(),
                fee=float(raw.get('fee', 0) or 0),
                fee_maker=float(raw.get('fee_maker', 0) or 0),
                position_sign_changed=bool(raw.get('position_sign_changed', False)),
                raw_json=json.dumps(raw),
            )
        except (TypeError, ValueError) as e:
            self.log.debug(f"Skipping malformed fill: {e}")
            return None

    def _compute_since_time(self, state: ReconcileState) -> float:
        """Compute lower bound for fill fetch (with overlap window)."""
        earliest_open = self._get_earliest_open_trade_entry()

        # If we have a checkpoint, don't rewind earlier than the earliest open trade.
        # This avoids huge backfills (and orphan spam) after restarts / bad checkpoints.
        if state.last_fill_time and state.last_fill_time > 0:
            base = max(0.0, state.last_fill_time - self.overlap_seconds)
            if earliest_open:
                base = max(base, max(0.0, earliest_open - self.overlap_seconds))
            return base

        if earliest_open:
            return max(0.0, earliest_open - self.overlap_seconds)

        return 0.0

    def _get_reconcile_state(self) -> ReconcileState:
        """Load reconcile state from DB, with fallback to latest fill."""
        self._ensure_reconcile_state_table()
        with self._db_conn() as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                "SELECT last_fill_time, last_exchange_trade_id FROM fill_reconcile_state WHERE venue = ?",
                (self.venue,),
            ).fetchone()
            if row:
                last_fill_time = row["last_fill_time"]
                # Self-heal: some historical checkpoints were stored in ms.
                try:
                    if last_fill_time is not None and float(last_fill_time) > 1e12:
                        last_fill_time = float(last_fill_time) / 1000.0
                except Exception:
                    pass

                return ReconcileState(
                    last_fill_time=last_fill_time,
                    last_exchange_trade_id=row["last_exchange_trade_id"],
                )

            # Fallback to latest fill already in DB
            row = conn.execute(
                "SELECT fill_time, exchange_trade_id FROM fills WHERE venue = ? ORDER BY fill_time DESC LIMIT 1",
                (self.venue,),
            ).fetchone()
            if row:
                state = ReconcileState(
                    last_fill_time=row["fill_time"],
                    last_exchange_trade_id=row["exchange_trade_id"],
                )
                self._upsert_reconcile_state(state.last_fill_time, state.last_exchange_trade_id)
                return state

        return ReconcileState()

    def _upsert_reconcile_state(
        self,
        last_fill_time: Optional[float],
        last_exchange_trade_id: Optional[str],
    ) -> None:
        """Persist reconcile checkpoint for this venue."""
        self._ensure_reconcile_state_table()
        with self._db_conn() as conn:
            conn.execute(
                """
                INSERT INTO fill_reconcile_state (
                    venue, last_fill_time, last_exchange_trade_id, updated_at
                ) VALUES (?, ?, ?, strftime('%s', 'now'))
                ON CONFLICT(venue) DO UPDATE SET
                    last_fill_time = COALESCE(excluded.last_fill_time, fill_reconcile_state.last_fill_time),
                    last_exchange_trade_id = COALESCE(
                        excluded.last_exchange_trade_id,
                        fill_reconcile_state.last_exchange_trade_id
                    ),
                    updated_at = excluded.updated_at
                """,
                (self.venue, last_fill_time, last_exchange_trade_id),
            )
            conn.commit()

    def _ensure_reconcile_state_table(self) -> None:
        """Ensure fill_reconcile_state table exists."""
        if self._reconcile_state_ready:
            return
        with self._db_conn() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS fill_reconcile_state (
                    venue TEXT PRIMARY KEY,
                    last_fill_time REAL,
                    last_exchange_trade_id TEXT,
                    updated_at REAL DEFAULT (strftime('%s', 'now'))
                )
                """
            )
            conn.commit()
        self._reconcile_state_ready = True

    def _get_earliest_open_trade_entry(self) -> Optional[float]:
        """Find earliest entry_time for open trades on this venue."""
        with self._db_conn() as conn:
            row = conn.execute(
                "SELECT MIN(entry_time) FROM trades WHERE venue = ? AND exit_time IS NULL",
                (self.venue,),
            ).fetchone()
            if row and row[0] is not None:
                return float(row[0])
        return None

    def _fills_exist_batch_chunked(self, pairs: list, chunk_size: int = 200) -> set:
        """Chunked batch check for fill idempotency."""
        if not pairs:
            return set()
        existing = set()
        for idx in range(0, len(pairs), chunk_size):
            existing |= self._fills_exist_batch(pairs[idx:idx + chunk_size])
        return existing

    @staticmethod
    def _symbol_base(symbol: str) -> str:
        s = str(symbol or "").strip().upper()
        if not s:
            return s
        if ":" in s:
            return s.split(":", 1)[1].strip().upper()
        return s

    @classmethod
    def _match_symbol_variants(cls, symbol: str) -> List[str]:
        """Return symbol candidates for matching fills to trades.

        Safety rule (critical): **never** match builder/DEX-prefixed fills (anything
        containing `:` like `xyz:ADA`) to plain perps symbols (`ADA`).

        We only match the exact uppercased symbol for prefixed fills.
        """
        exact = str(symbol or "").strip().upper()
        if not exact:
            return []
        if ":" in exact:
            return [exact]
        return [exact]

    async def _backstop_external_closes(self) -> int:
        """Close trades that are open in DB but flat on exchange."""
        open_trades = self._get_open_trades()
        if not open_trades:
            return 0

        try:
            positions = await asyncio.wait_for(
                self.exchange.get_all_positions(),
                timeout=BACKSTOP_POSITIONS_TIMEOUT_SEC,
            )
        except asyncio.TimeoutError:
            self.log.error(
                f"Failed to fetch positions for backstop: timeout after "
                f"{BACKSTOP_POSITIONS_TIMEOUT_SEC:.1f}s"
            )
            return 0
        except Exception as e:
            self.log.error(f"Failed to fetch positions for backstop: {e}")
            return 0

        active_symbols = {
            symbol.upper()
            for symbol, pos in positions.items()
            if pos and pos.direction != "FLAT" and pos.size > 0
        }
        # Builder/base matching is only safe when the trade itself is a builder symbol.
        active_builder_bases = {
            self._symbol_base(sym)
            for sym in active_symbols
            if ":" in str(sym or "")
        }

        closed = 0
        for trade in open_trades:
            trade_sym = str(trade.get("symbol") or "").upper()
            if not trade_sym:
                continue

            # Exact match always.
            if trade_sym in active_symbols:
                continue

            # Builder symbols: tolerate prefix drift by matching on base.
            if ":" in trade_sym:
                trade_base = self._symbol_base(trade_sym)
                if trade_base and trade_base in active_builder_bases:
                    continue

            inferred = self._infer_exit_from_fills(trade)
            self._close_trade_external(trade, inferred)
            closed += 1

        if closed:
            self.log.warning(
                f"Externally closed {closed} trade(s) on {self.venue} (exchange flat)"
            )
        return closed

    async def _retry_deferred_trade_closes(self, limit: int = 100) -> int:
        """Retry close finalization for open trades with sign-changing exit fills.

        This recovers from temporary symbol-lock contention without forcing
        an EXTERNAL close attribution path.
        """
        with self._db_conn() as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                """
                SELECT t.id AS trade_id,
                       f.venue,
                       f.exchange_trade_id,
                       f.symbol,
                       f.fill_time,
                       f.fill_price,
                       f.fill_size,
                       f.side,
                       COALESCE(f.fee, 0.0) AS fee,
                       COALESCE(f.fee_maker, 0.0) AS fee_maker,
                       COALESCE(f.position_sign_changed, 0) AS position_sign_changed,
                       COALESCE(f.raw_json, '{}') AS raw_json,
                       COALESCE(f.fill_type, 'EXIT') AS fill_type
                FROM trades t
                JOIN (
                    SELECT trade_id, MAX(fill_time) AS max_fill_time
                    FROM fills
                    WHERE venue = ?
                      AND fill_type IN ('EXIT', 'SL', 'TP')
                      AND COALESCE(position_sign_changed, 0) = 1
                      AND trade_id IS NOT NULL
                    GROUP BY trade_id
                ) latest ON latest.trade_id = t.id
                JOIN fills f
                  ON f.trade_id = t.id
                 AND f.fill_time = latest.max_fill_time
                 AND f.venue = ?
                WHERE t.venue = ?
                  AND t.exit_time IS NULL
                ORDER BY f.fill_time DESC
                LIMIT ?
                """,
                (self.venue, self.venue, self.venue, int(limit)),
            ).fetchall()

        closed = 0
        for row in rows:
            fill = Fill(
                venue=str(row["venue"] or self.venue),
                exchange_trade_id=str(row["exchange_trade_id"] or ""),
                symbol=str(row["symbol"] or ""),
                fill_time=float(row["fill_time"] or 0.0),
                fill_price=float(row["fill_price"] or 0.0),
                fill_size=float(row["fill_size"] or 0.0),
                side=str(row["side"] or ""),
                fee=float(row["fee"] or 0.0),
                fee_maker=float(row["fee_maker"] or 0.0),
                position_sign_changed=bool(int(row["position_sign_changed"] or 0)),
                raw_json=str(row["raw_json"] or "{}"),
            )
            if await self._close_trade_from_fill(
                int(row["trade_id"]),
                fill,
                str(row["fill_type"] or "EXIT"),
            ):
                closed += 1

        if closed:
            self.log.info(f"Recovered {closed} deferred fill close(s) on {self.venue}")
        return closed

    def _get_open_trades(self) -> List[dict]:
        """Load open trades for this venue."""
        with self._db_conn() as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute(
                "SELECT * FROM trades WHERE venue = ? AND exit_time IS NULL ORDER BY entry_time DESC",
                (self.venue,),
            )
            return [dict(row) for row in cursor.fetchall()]

    def _infer_exit_from_fills(self, trade: dict) -> Optional[Dict[str, Any]]:
        """Try to infer exit price/time from logged fills."""
        with self._db_conn() as conn:
            conn.row_factory = sqlite3.Row

            row = conn.execute(
                """
                SELECT fill_price, fill_time, exchange_trade_id
                FROM fills
                WHERE trade_id = ?
                  AND (position_sign_changed = 1 OR fill_type IN ('EXIT', 'SL', 'TP'))
                ORDER BY fill_time DESC
                LIMIT 1
                """,
                (trade["id"],),
            ).fetchone()
            if row:
                return dict(row)

            row = conn.execute(
                """
                SELECT fill_price, fill_time, exchange_trade_id
                FROM fills
                WHERE trade_id IS NULL
                  AND venue = ?
                  AND symbol = ?
                  AND position_sign_changed = 1
                  AND fill_time >= ?
                ORDER BY fill_time DESC
                LIMIT 1
                """,
                (self.venue, trade["symbol"].upper(), trade["entry_time"]),
            ).fetchone()
            if row:
                return dict(row)

        return None

    def _close_trade_external(self, trade: dict, inferred: Optional[Dict[str, Any]]) -> None:
        """Close trade with unknown exit price (manual/external close)."""
        exit_price = None
        exit_time = time.time()
        source = "external"
        net_pnl: Optional[float] = None
        net_pct: Optional[float] = None
        gross_pnl: Optional[float] = None

        if inferred:
            exit_price = inferred.get("fill_price")
            exit_time = inferred.get("fill_time", exit_time)
            source = "inferred"

        closed = False
        with self._db_conn() as conn:
            cursor = conn.execute(
                "SELECT COALESCE(SUM(fee + fee_maker), 0) FROM fills WHERE trade_id = ?",
                (trade["id"],),
            )
            total_fees = cursor.fetchone()[0]

            partial_exit_size = 0.0
            if self._has_trade_partial_pnl_columns:
                cursor = conn.execute(
                    "SELECT COALESCE(SUM(ABS(fill_size)), 0) FROM fills WHERE trade_id = ? AND fill_type = 'PARTIAL_EXIT'",
                    (trade["id"],),
                )
                partial_exit_size = float(cursor.fetchone()[0] or 0.0)
            partial_realized = float(trade.get("realized_pnl_partial_usd") or 0.0)

            # Best-effort PnL for inferred external closes.
            try:
                exit_px = float(exit_price) if exit_price is not None else 0.0
                entry_px = float(trade.get("entry_price") or 0.0)
                size = float(trade.get("size") or 0.0)
                direction = str(trade.get("direction") or "").upper()
                if exit_px > 0 and entry_px > 0 and size > 0:
                    if direction == "LONG":
                        gross_pnl = partial_realized + ((exit_px - entry_px) * size)
                    else:
                        gross_pnl = partial_realized + ((entry_px - exit_px) * size)
                    net_pnl = float(gross_pnl) - float(total_fees or 0.0)
                    notional = entry_px * (size + partial_exit_size)
                    net_pct = (net_pnl / notional * 100.0) if notional > 0 else 0.0
            except Exception:
                net_pnl = None
                net_pct = None
                gross_pnl = None

            if self._has_trade_state_column:
                if self._has_trade_partial_pnl_columns:
                    cur = conn.execute(
                        """
                        UPDATE trades SET
                            exit_time = ?,
                            exit_price = ?,
                            exit_reason = ?,
                            realized_pnl = ?,
                            realized_pnl_pct = ?,
                            total_fees = ?,
                            state = ?,
                            realized_pnl_partial_usd = 0.0,
                            exit_fees_partial_usd = 0.0,
                            pending_exit_reason = NULL,
                            pending_exit_detail = NULL,
                            pending_exit_set_at = NULL,
                            updated_at = strftime('%s', 'now')
                        WHERE id = ? AND exit_time IS NULL
                        """,
                        (
                            exit_time,
                            exit_price,
                            "EXTERNAL",
                            net_pnl,
                            net_pct,
                            total_fees,
                            "CLOSED_EXTERNALLY",
                            trade["id"],
                        ),
                    )
                else:
                    cur = conn.execute(
                        """
                        UPDATE trades SET
                            exit_time = ?,
                            exit_price = ?,
                            exit_reason = ?,
                            realized_pnl = ?,
                            realized_pnl_pct = ?,
                            total_fees = ?,
                            state = ?,
                            pending_exit_reason = NULL,
                            pending_exit_detail = NULL,
                            pending_exit_set_at = NULL,
                            updated_at = strftime('%s', 'now')
                        WHERE id = ? AND exit_time IS NULL
                        """,
                        (
                            exit_time,
                            exit_price,
                            "EXTERNAL",
                            net_pnl,
                            net_pct,
                            total_fees,
                            "CLOSED_EXTERNALLY",
                            trade["id"],
                        ),
                    )
                closed = cur.rowcount > 0
            else:
                if self._has_trade_partial_pnl_columns:
                    cur = conn.execute(
                        """
                        UPDATE trades SET
                            exit_time = ?,
                            exit_price = ?,
                            exit_reason = ?,
                            realized_pnl = ?,
                            realized_pnl_pct = ?,
                            total_fees = ?,
                            realized_pnl_partial_usd = 0.0,
                            exit_fees_partial_usd = 0.0,
                            pending_exit_reason = NULL,
                            pending_exit_detail = NULL,
                            pending_exit_set_at = NULL,
                            updated_at = strftime('%s', 'now')
                        WHERE id = ? AND exit_time IS NULL
                        """,
                        (
                            exit_time,
                            exit_price,
                            "EXTERNAL",
                            net_pnl,
                            net_pct,
                            total_fees,
                            trade["id"],
                        ),
                    )
                else:
                    cur = conn.execute(
                        """
                        UPDATE trades SET
                            exit_time = ?,
                            exit_price = ?,
                            exit_reason = ?,
                            realized_pnl = ?,
                            realized_pnl_pct = ?,
                            total_fees = ?,
                            pending_exit_reason = NULL,
                            pending_exit_detail = NULL,
                            pending_exit_set_at = NULL,
                            updated_at = strftime('%s', 'now')
                        WHERE id = ? AND exit_time IS NULL
                        """,
                        (
                            exit_time,
                            exit_price,
                            "EXTERNAL",
                            net_pnl,
                            net_pct,
                            total_fees,
                            trade["id"],
                        ),
                    )
                closed = cur.rowcount > 0
            conn.commit()

        if not closed:
            # Already closed; avoid duplicate callbacks/events.
            return

        self.log.warning(
            f"Trade {trade['id']} closed externally ({source} price) on {trade['symbol']} "
            f"exit_price={exit_price}"
        )

        if self.on_trade_close:
            try:
                self.on_trade_close({
                    'trade_id': trade['id'],
                    'symbol': trade['symbol'],
                    'venue': self.venue,
                    'direction': trade['direction'],
                    'pnl': net_pnl,
                    'gross_pnl': gross_pnl,
                    'total_fees': total_fees,
                    'exit_reason': 'EXTERNAL',
                    'exit_price': exit_price,
                    'signals_agreed': json.loads(trade['signals_agreed']) if trade.get('signals_agreed') else [],
                })
            except Exception as e:
                self.log.error(f"on_trade_close callback error: {e}")

    def _check_trade_state_column(self) -> bool:
        """Check if trades table has a state column (older DBs may not)."""
        try:
            with self._db_conn() as conn:
                cursor = conn.execute("PRAGMA table_info(trades)")
                return any(row[1] == "state" for row in cursor.fetchall())
        except Exception:
            return False

    def _check_trade_partial_pnl_columns(self) -> bool:
        """Check if trades table has cumulative partial-exit pnl columns."""
        try:
            with self._db_conn() as conn:
                cols = {row[1] for row in conn.execute("PRAGMA table_info(trades)").fetchall()}
                return {"realized_pnl_partial_usd", "exit_fees_partial_usd"}.issubset(cols)
        except Exception:
            return False

    def _fills_exist_batch(self, pairs: list) -> set:
        """Batch check which fills already recorded (idempotency).

        Args:
            pairs: List of (venue, exchange_trade_id) tuples

        Returns:
            Set of (venue, exchange_trade_id) tuples that exist
        """
        if not pairs:
            return set()

        with self._db_conn() as conn:
            conditions = " OR ".join(
                "(venue = ? AND exchange_trade_id = ?)" for _ in pairs
            )
            params = []
            for venue, eid in pairs:
                params.extend([venue, eid])
            cursor = conn.execute(
                f"SELECT venue, exchange_trade_id FROM fills WHERE {conditions}",
                params
            )
            return {(row[0], row[1]) for row in cursor.fetchall()}

    async def _process_fill(self, fill: Fill) -> None:
        """Process a single fill."""
        # Normalize fill_time to seconds (some venues may emit ms timestamps).
        try:
            if getattr(fill, "fill_time", None) is not None and float(fill.fill_time) > 1e12:
                fill.fill_time = float(fill.fill_time) / 1000.0
        except Exception:
            pass

        # 1. Find matching trade (open trade for this symbol on this venue)
        trade = self._find_trade_for_fill(fill.symbol, fill.fill_time)
        if not trade:
            if self._log_orphan_fill(fill):
                self.log.warning(f"No matching trade for fill {fill.venue}/{fill.exchange_trade_id} on {fill.symbol}")
            return

        trade_id = trade['id']
        direction = trade['direction']

        # 2. Classify fill type
        fill_type = self._classify_fill(fill, direction, trade)

        # 3. Log fill to database
        inserted = self._log_fill(trade_id, fill, fill_type)

        # 4. Handle partial exits by reducing open size.
        if fill_type == 'PARTIAL_EXIT':
            if not inserted:
                return
            await self._apply_partial_exit_fill(trade_id, trade, fill)
            return

        # 5. If exit fill, close the trade.
        # IMPORTANT: position_sign_changed can be True for entry fills (0 -> position opened).
        # Never close a trade on an ENTRY fill.
        is_exit = fill_type in ('EXIT', 'SL', 'TP')
        is_sign_exit = bool(fill.position_sign_changed) and fill_type != 'ENTRY'
        if is_exit or is_sign_exit:
            await self._close_trade_from_fill(trade_id, fill, fill_type)

    def _find_trade_for_fill(self, symbol: str, fill_time: float) -> Optional[dict]:
        """Find open trade for this symbol on this venue.

        Note: exchange fill timestamps can arrive slightly *before* we commit the
        trade entry row (local logging happens after execution confirms fills).
        Allow a small skew window so entry fills still match the newly-opened
        trade instead of being logged as orphan fills.
        """
        # IMPORTANT:
        # We previously allowed fills up to 60s *before* the local entry_time to match
        # (clock skew + post-fill logging latency). That is helpful for ENTRY fills,
        # but it can mis-attribute EXIT fills during fast signal-flips:
        #   - old trade closes (exit fill)
        #   - new trade entry is logged seconds later
        # If we allow a "future" trade (entry_time > fill_time) to match, an EXIT fill
        # can be incorrectly logged as an ENTRY for the new trade.
        #
        # Fix: prefer a strict match where the trade existed at fill_time.
        # Only if nothing matches do we allow the skew window.
        FILL_MATCH_SKEW_SECONDS = 60.0
        fill_ts = float(fill_time)

        match_symbols = self._match_symbol_variants(symbol)
        if not match_symbols:
            return None
        exact_symbol = match_symbols[0]
        fallback_symbols = match_symbols[1:]

        with self._db_conn() as conn:
            conn.row_factory = sqlite3.Row

            def _query(symbol_key: str, cutoff_ts: float) -> Optional[dict]:
                cursor = conn.execute(
                    """
                    SELECT * FROM trades
                    WHERE symbol = ? AND venue = ? AND exit_time IS NULL
                    AND entry_time <= ?
                    ORDER BY entry_time DESC
                    LIMIT 1
                    """,
                    (symbol_key, self.venue, cutoff_ts),
                )
                row = cursor.fetchone()
                return dict(row) if row else None

            # 1) Strict exact match: trade must already exist at fill timestamp.
            row = _query(exact_symbol, fill_ts)
            if row:
                return row

            # 1b) Strict fallback: allow prefixed fill symbol -> plain trade symbol.
            for alt_symbol in fallback_symbols:
                row = _query(alt_symbol, fill_ts)
                if row:
                    return row

            # 2) Skewed exact match: tolerate fill timestamps slightly before entry log.
            cutoff = fill_ts + FILL_MATCH_SKEW_SECONDS
            row = _query(exact_symbol, cutoff)
            if row:
                return row

            # 2b) Skewed fallback for prefixed/plain namespace drift.
            for alt_symbol in fallback_symbols:
                row = _query(alt_symbol, cutoff)
                if row:
                    return row

            return None

    def _classify_fill(self, fill: Fill, direction: str, trade: dict) -> str:
        """
        Classify fill type (ENTRY/PARTIAL_EXIT/EXIT/SL/TP).

        Pattern from fill-tracking-flow.md:172-213
        """
        entry_side = 'buy' if direction == 'LONG' else 'sell'

        # Entry fills are same side as direction
        if fill.side.lower() == entry_side:
            return 'ENTRY'

        # Exit fills
        if fill.position_sign_changed:
            # Full exit - classify by price
            return self._classify_exit_by_price(fill, trade)

        return 'PARTIAL_EXIT'

    def _classify_exit_by_price(self, fill: Fill, trade: dict) -> str:
        """
        Classify exit as SL/TP/EXIT based on fill price.

        Uses ACTUAL SL/TP prices stored in trade record (not hardcoded percentages).
        """
        entry_price = trade['entry_price']
        direction = trade['direction']

        # Get actual SL/TP prices from trade record
        sl_price = trade.get('sl_price')
        tp_price = trade.get('tp_price')

        # 0.1% tolerance for price matching
        TOLERANCE = 0.001

        if sl_price and tp_price:
            # Use actual stored prices
            if direction == 'LONG':
                if fill.fill_price <= sl_price * (1 + TOLERANCE):
                    return 'SL'
                elif fill.fill_price >= tp_price * (1 - TOLERANCE):
                    return 'TP'
            else:  # SHORT
                if fill.fill_price >= sl_price * (1 - TOLERANCE):
                    return 'SL'
                elif fill.fill_price <= tp_price * (1 + TOLERANCE):
                    return 'TP'
        else:
            # Fallback: use conservative 2%/3% if SL/TP not stored
            self.log.warning(f"Trade {trade['id']} missing SL/TP prices, using fallback classification")
            if direction == 'LONG':
                sl_level = entry_price * 0.98
                tp_level = entry_price * 1.03
                if fill.fill_price <= sl_level * (1 + TOLERANCE):
                    return 'SL'
                elif fill.fill_price >= tp_level * (1 - TOLERANCE):
                    return 'TP'
            else:
                sl_level = entry_price * 1.02
                tp_level = entry_price * 0.97
                if fill.fill_price >= sl_level * (1 - TOLERANCE):
                    return 'SL'
                elif fill.fill_price <= tp_level * (1 + TOLERANCE):
                    return 'TP'

        return 'EXIT'

    def _classify_orphan_reattach_fill(
        self,
        *,
        side: str,
        position_sign_changed: bool,
        fill_price: float,
        trade: dict,
    ) -> str:
        """Classify an orphan fill being reattached using trade direction + fill metadata."""
        direction = str(trade.get("direction") or "").upper()
        entry_side = 'buy' if direction == 'LONG' else 'sell'
        if str(side or "").lower() == entry_side:
            return 'ENTRY'

        if position_sign_changed:
            pseudo_fill = Fill(
                venue=self.venue,
                exchange_trade_id="",
                symbol=str(trade.get("symbol") or ""),
                fill_time=float(trade.get("entry_time") or 0.0),
                fill_price=float(fill_price or 0.0),
                fill_size=0.0,
                side=str(side or ""),
                fee=0.0,
                fee_maker=0.0,
                position_sign_changed=True,
                raw_json="{}",
            )
            return self._classify_exit_by_price(pseudo_fill, trade)

        return 'PARTIAL_EXIT'

    def _log_fill(self, trade_id: int, fill: Fill, fill_type: str) -> bool:
        """Record fill in database.

        Returns:
            True when inserted, False when duplicate already exists.
        """
        with self._db_conn() as conn:
            cursor = conn.execute("""
                INSERT OR IGNORE INTO fills (
                    trade_id, venue, exchange_trade_id, symbol, fill_time, fill_price,
                    fill_size, fill_type, side, fee, fee_maker,
                    position_sign_changed, raw_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                trade_id,
                fill.venue,
                fill.exchange_trade_id,
                fill.symbol.upper(),
                fill.fill_time,
                fill.fill_price,
                fill.fill_size,
                fill_type,
                fill.side.upper(),
                fill.fee,
                fill.fee_maker,
                1 if fill.position_sign_changed else 0,
                fill.raw_json,
            ))
            inserted = cursor.rowcount > 0
            conn.commit()
        if inserted:
            self.log.debug(f"Logged fill {fill.venue}/{fill.exchange_trade_id} as {fill_type} for trade {trade_id}")
        else:
            self.log.debug(
                f"Duplicate fill ignored for insert {fill.venue}/{fill.exchange_trade_id} "
                f"(trade_id={trade_id}, type={fill_type})"
            )
        return inserted

    def _log_orphan_fill(self, fill: Fill) -> bool:
        """Log fill without matching trade.

        This can happen legitimately due to a race:
        - entry fill hits the exchange
        - reconciler polls fills and sees it
        - the live-agent logs the trade row in SQLite a few seconds later

        We persist the fill as ORPHAN and later attempt to attach it via
        `_reattach_orphan_fills()`.
        """
        with self._db_conn() as conn:
            cursor = conn.execute("""
                INSERT OR IGNORE INTO fills (
                    trade_id, venue, exchange_trade_id, symbol, fill_time, fill_price,
                    fill_size, fill_type, side, fee, fee_maker,
                    position_sign_changed, raw_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                None,  # No trade_id for orphans (trade_id is nullable)
                fill.venue,
                fill.exchange_trade_id,
                fill.symbol.upper(),
                fill.fill_time,
                fill.fill_price,
                fill.fill_size,
                'ORPHAN',
                fill.side.upper(),
                fill.fee,
                fill.fee_maker,
                1 if fill.position_sign_changed else 0,
                fill.raw_json,
            ))
            inserted = cursor.rowcount > 0
            conn.commit()
        if inserted:
            self.log.warning(f"Logged orphan fill {fill.venue}/{fill.exchange_trade_id} on {fill.symbol}")
        else:
            self.log.debug(f"Duplicate orphan fill ignored for insert {fill.venue}/{fill.exchange_trade_id} on {fill.symbol}")
        return inserted

    async def _apply_partial_exit_fill(self, trade_id: int, trade: dict, fill: Fill) -> None:
        """Reduce the tracked trade size after a partial exit fill."""
        old_size = abs(float(trade.get('size', 0.0) or 0.0))
        exit_size = abs(float(fill.fill_size or 0.0))
        if old_size <= 0 or exit_size <= 0:
            return

        realized_size = min(old_size, exit_size)
        new_size = max(0.0, old_size - realized_size)
        if new_size <= 1e-9:
            await self._close_trade_from_fill(trade_id, fill, "EXIT")
            return

        entry_price = float(trade.get('entry_price', 0.0) or 0.0)
        direction = str(trade.get("direction") or "").upper()
        partial_gross_pnl = 0.0
        if entry_price > 0 and fill.fill_price > 0 and realized_size > 0:
            if direction == "LONG":
                partial_gross_pnl = (float(fill.fill_price) - entry_price) * realized_size
            else:
                partial_gross_pnl = (entry_price - float(fill.fill_price)) * realized_size
        partial_fees = float(fill.fee or 0.0) + float(fill.fee_maker or 0.0)
        with self._db_conn() as conn:
            if self._has_trade_partial_pnl_columns:
                conn.execute(
                    """
                    UPDATE trades
                    SET size = ?,
                        notional_usd = ?,
                        realized_pnl_partial_usd = COALESCE(realized_pnl_partial_usd, 0.0) + ?,
                        exit_fees_partial_usd = COALESCE(exit_fees_partial_usd, 0.0) + ?,
                        updated_at = strftime('%s', 'now')
                    WHERE id = ? AND exit_time IS NULL
                    """,
                    (new_size, new_size * entry_price, partial_gross_pnl, partial_fees, trade_id),
                )
            else:
                conn.execute(
                    """
                    UPDATE trades
                    SET size = ?,
                        notional_usd = ?,
                        updated_at = strftime('%s', 'now')
                    WHERE id = ? AND exit_time IS NULL
                    """,
                    (new_size, new_size * entry_price, trade_id),
                )
            conn.commit()

        self.log.info(
            f"Trade {trade_id} partial exit: size {old_size:.6f} -> {new_size:.6f} "
            f"({fill.symbol} fill={exit_size:.6f})"
        )

    def _reattach_orphan_fills(self, lookback_seconds: int = 1800) -> int:
        """Try to attach orphan fills to their corresponding trade.

        Primary goal: ensure ENTRY fill fees are attributed to the correct trade
        (otherwise net PnL will be wrong on close).

        Matching heuristic (safe under our 'no averaging' constraint):
        - match by (venue, symbol)
        - nearest trade where entry_time is within ±ORPHAN_MATCH_WINDOW_SEC of fill_time
        - attach as fill_type=ENTRY

        Returns number of fills attached.
        """
        now = time.time()
        since = now - float(lookback_seconds or 0)
        attached = 0

        with self._db_conn() as conn:
            conn.row_factory = sqlite3.Row
            orphan_rows = conn.execute(
                """
                SELECT id, venue, exchange_trade_id, symbol, fill_time, fill_price, side,
                       COALESCE(position_sign_changed, 0) AS position_sign_changed
                FROM fills
                WHERE trade_id IS NULL
                  AND venue = ?
                  AND fill_type = 'ORPHAN'
                  AND fill_time >= ?
                ORDER BY fill_time DESC
                LIMIT 500
                """,
                (self.venue, since),
            ).fetchall()

            for r in orphan_rows:
                sym = str(r["symbol"] or "").upper()
                ft = float(r["fill_time"] or 0.0)
                if not sym or ft <= 0:
                    continue

                # Find the nearest trade entry around this fill.
                window = max(1.0, float(ORPHAN_MATCH_WINDOW_SEC or 0.0))
                match_symbols = self._match_symbol_variants(sym)
                if not match_symbols:
                    continue
                placeholders = ",".join("?" for _ in match_symbols)
                candidates = conn.execute(
                    f"""
                    SELECT id, entry_time, symbol, direction, entry_price, sl_price, tp_price
                    FROM trades
                    WHERE venue = ?
                      AND symbol IN ({placeholders})
                      AND exit_time IS NULL
                      AND entry_time BETWEEN ? AND ?
                    ORDER BY ABS(entry_time - ?) ASC
                    LIMIT 2
                    """,
                    (self.venue, *match_symbols, ft - window, ft + window, ft),
                ).fetchall()

                if len(candidates) == 0:
                    continue
                if len(candidates) > 1:
                    try:
                        dt0 = abs(float(candidates[0]["entry_time"] or 0.0) - ft)
                        dt1 = abs(float(candidates[1]["entry_time"] or 0.0) - ft)
                    except Exception:
                        continue
                    # Safety-first: only auto-attach multi-candidate fills when nearest
                    # trade is clearly closer than runner-up.
                    if (dt1 - dt0) < max(0.0, float(ORPHAN_AMBIGUITY_GAP_SEC or 0.0)):
                        continue

                trade = dict(candidates[0])
                trade_id = int(trade["id"])
                inferred_fill_type = self._classify_orphan_reattach_fill(
                    side=str(r["side"] or ""),
                    position_sign_changed=bool(int(r["position_sign_changed"] or 0)),
                    fill_price=float(r["fill_price"] or 0.0),
                    trade=trade,
                )

                # Attach the fill.
                conn.execute(
                    """
                    UPDATE fills
                    SET trade_id = ?, fill_type = ?
                    WHERE id = ?
                      AND trade_id IS NULL
                      AND fill_type = 'ORPHAN'
                    """,
                    (trade_id, inferred_fill_type, int(r["id"])),
                )
                attached += 1

            if attached:
                conn.commit()

        if attached:
            self.log.info(f"Reattached {attached} orphan fills")
        return attached

    async def _close_trade_from_fill(self, trade_id: int, fill: Fill, exit_reason: str) -> bool:
        """
        Close trade using actual fill price.

        Calculates net PnL = gross PnL - total fees (all fills for this trade).
        """
        # Try to lock upfront to reduce race window with executor close/SLTP actions.
        lock_acquired = self._acquire_symbol_lock(
            fill.symbol,
            self.venue,
            reason=f"preclose_trade_id={trade_id}",
        )
        if not lock_acquired:
            self.log.warning(
                f"[{fill.symbol}] Defer fill-close for trade {trade_id}: symbol lock busy"
            )
            return False

        closed = False
        direction = ""
        signals_agreed = None
        final_reason = str(exit_reason or "").upper() or None
        net_pnl = 0.0
        net_pct = 0.0
        gross_pct = 0.0
        total_fees = 0.0
        gross_pnl_total = 0.0
        sl_order_id = None
        tp_order_id = None
        close_symbol = str(fill.symbol or "")

        try:
            with self._db_conn() as conn:
                # Get trade for PnL calculation
                if self._has_trade_partial_pnl_columns:
                    cursor = conn.execute(
                        """
                        SELECT direction, entry_price, size, signals_agreed, sl_order_id, tp_order_id,
                               exit_reason, pending_exit_reason, pending_exit_set_at,
                               COALESCE(realized_pnl_partial_usd, 0.0),
                               COALESCE(exit_fees_partial_usd, 0.0)
                        FROM trades WHERE id = ?
                        """,
                        (trade_id,),
                    )
                else:
                    cursor = conn.execute(
                        """
                        SELECT direction, entry_price, size, signals_agreed, sl_order_id, tp_order_id,
                               exit_reason, pending_exit_reason, pending_exit_set_at,
                               0.0, 0.0
                        FROM trades WHERE id = ?
                        """,
                        (trade_id,),
                    )
                row = cursor.fetchone()
                if not row:
                    self.log.error(f"Trade {trade_id} not found for closure")
                    return False

                (
                    direction,
                    entry_price,
                    size,
                    signals_agreed,
                    sl_order_id,
                    tp_order_id,
                    existing_exit_reason,
                    pending_exit_reason,
                    pending_exit_set_at,
                    partial_realized_pnl,
                    partial_exit_fees,
                ) = row
                tracked_size = abs(float(size or 0.0))
                fill_size = abs(float(fill.fill_size or 0.0))
                # Prefer the executed fill quantity for close PnL attribution.
                # Bound by tracked size when both are available to avoid over-attribution
                # on sign-flip fills that can include new opposite-side exposure.
                if fill_size > 0.0:
                    close_size = min(fill_size, tracked_size) if tracked_size > 0.0 else fill_size
                else:
                    close_size = tracked_size

                # Price guards
                if entry_price <= 0 or fill.fill_price <= 0 or close_size <= 0:
                    self.log.warning(
                        f"Invalid close inputs for trade {trade_id}: "
                        f"entry={entry_price}, exit={fill.fill_price}, close_size={close_size}"
                    )
                    close_gross_pnl = 0.0
                elif direction == "LONG":
                    close_gross_pnl = (fill.fill_price - entry_price) * close_size
                else:
                    close_gross_pnl = (entry_price - fill.fill_price) * close_size

                gross_pnl_total = float(partial_realized_pnl or 0.0) + float(close_gross_pnl)

                # Get total fees from all fills for this trade.
                cursor = conn.execute(
                    "SELECT COALESCE(SUM(fee + fee_maker), 0) FROM fills WHERE trade_id = ?",
                    (trade_id,),
                )
                total_fees = float(cursor.fetchone()[0] or 0.0)
                partial_fee_component = float(partial_exit_fees or 0.0)
                if partial_fee_component > 0.0 and total_fees + 1e-9 < partial_fee_component:
                    # Fallback safety: if fill rows are missing/late, do not undercount known partial fees.
                    self.log.warning(
                        f"Trade {trade_id} total fee sum {total_fees:.6f} below partial accumulator "
                        f"{partial_fee_component:.6f}; using accumulator floor"
                    )
                    total_fees = partial_fee_component

                # Use full closed notional (partial exits + final chunk) for pct.
                cursor = conn.execute(
                    "SELECT COALESCE(SUM(ABS(fill_size)), 0) FROM fills WHERE trade_id = ? AND fill_type = 'PARTIAL_EXIT'",
                    (trade_id,),
                )
                partial_exit_size = float(cursor.fetchone()[0] or 0.0)
                total_closed_size = float(close_size) + float(partial_exit_size)
                notional = float(entry_price or 0.0) * float(total_closed_size or 0.0)

                # Net PnL = gross - fees
                net_pnl = float(gross_pnl_total) - float(total_fees)
                net_pct = (net_pnl / notional * 100.0) if notional > 0 else 0.0
                gross_pct = (gross_pnl_total / notional * 100.0) if notional > 0 else 0.0

                # Select a final exit reason.
                # - SL/TP always win (detected from price)
                # - If the agent/CLI explicitly requested a close reason, persist it
                #   even if the fee-accurate close is finalized later.
                # - Never overwrite an already-set exit_reason (idempotency).
                final_reason = str(exit_reason or "").upper() or None
                existing_er = str(existing_exit_reason or "").upper() or None
                pending_er_raw = str(pending_exit_reason or "").upper() or None
                pending_ts = float(pending_exit_set_at or 0.0)
                ref_ts = float(fill.fill_time or 0.0) or time.time()
                pending_age_sec = max(0.0, ref_ts - pending_ts) if pending_ts > 0 else float("inf")
                pending_er = (
                    pending_er_raw
                    if pending_er_raw and pending_age_sec <= float(PENDING_EXIT_REASON_MAX_AGE_SEC)
                    else None
                )
                if pending_er_raw and pending_er is None:
                    self.log.info(
                        f"Ignore stale pending_exit_reason for trade {trade_id}: reason={pending_er_raw} "
                        f"age_sec={pending_age_sec:.1f} max_age_sec={PENDING_EXIT_REASON_MAX_AGE_SEC:.1f}"
                    )

                if existing_er:
                    # If an earlier close wrote the generic EXIT but we have a pending override,
                    # prefer the override (root cause of exit_reason persistence bug).
                    if existing_er == "EXIT" and pending_er and final_reason not in ("SL", "TP"):
                        final_reason = pending_er
                    else:
                        final_reason = existing_er
                elif final_reason not in ("SL", "TP") and pending_er:
                    # Most manual closes reconcile as fill_type='EXIT'
                    final_reason = pending_er

                # Update trade with net PnL
                if self._has_trade_partial_pnl_columns:
                    cur = conn.execute(
                        """
                        UPDATE trades SET
                            exit_time = ?,
                            exit_price = ?,
                            exit_reason = ?,
                            realized_pnl = ?,
                            realized_pnl_pct = ?,
                            total_fees = ?,
                            realized_pnl_partial_usd = 0.0,
                            exit_fees_partial_usd = 0.0,
                            sl_order_id = NULL,
                            tp_order_id = NULL,
                            pending_exit_reason = NULL,
                            pending_exit_detail = NULL,
                            pending_exit_set_at = NULL,
                            updated_at = strftime('%s', 'now')
                        WHERE id = ? AND exit_time IS NULL
                        """,
                        (
                            fill.fill_time,
                            fill.fill_price,
                            final_reason,
                            net_pnl,  # Net PnL after fees
                            net_pct,
                            total_fees,
                            trade_id,
                        ),
                    )
                else:
                    cur = conn.execute(
                        """
                        UPDATE trades SET
                            exit_time = ?,
                            exit_price = ?,
                            exit_reason = ?,
                            realized_pnl = ?,
                            realized_pnl_pct = ?,
                            total_fees = ?,
                            sl_order_id = NULL,
                            tp_order_id = NULL,
                            pending_exit_reason = NULL,
                            pending_exit_detail = NULL,
                            pending_exit_set_at = NULL,
                            updated_at = strftime('%s', 'now')
                        WHERE id = ? AND exit_time IS NULL
                        """,
                        (
                            fill.fill_time,
                            fill.fill_price,
                            final_reason,
                            net_pnl,  # Net PnL after fees
                            net_pct,
                            total_fees,
                            trade_id,
                        ),
                    )

                closed = cur.rowcount > 0
                # Mark closed state when available
                if closed and self._has_trade_state_column:
                    conn.execute(
                        "UPDATE trades SET state = ?, updated_at = strftime('%s','now') WHERE id = ?",
                        ("CLOSED", trade_id),
                    )
                conn.commit()

            if not closed:
                # Already closed; avoid duplicate callbacks/events.
                return False

            self.log.info(
                f"Closed trade {trade_id}: {final_reason}, net PnL=${net_pnl:.2f} "
                f"(net_pct={net_pct:+.2f}%, gross_pct={gross_pct:+.2f}%, fees=${total_fees:.4f})"
            )

            # Safety: cancel the sibling protection order so it can't open a reverse position later.
            # This is critical on Hyperliquid when TP is implemented as a resting limit order.
            try:
                er = str(final_reason or "").upper()
                to_cancel = []
                if er == "SL":
                    to_cancel = [tp_order_id]
                elif er == "TP":
                    to_cancel = [sl_order_id]
                else:
                    to_cancel = [sl_order_id, tp_order_id]

                adapter = getattr(self, "exchange", None) or getattr(self, "exchange_adapter", None)
                if adapter and hasattr(adapter, "cancel_order"):
                    for oid in to_cancel:
                        if oid is None:
                            continue
                        oid_str = str(oid).strip()
                        if not oid_str:
                            continue
                        ok = await adapter.cancel_order(close_symbol, oid_str)
                        if ok:
                            self.log.info(
                                f"[{close_symbol}] Cancelled sibling order {oid_str} after {er or 'EXIT'} close (trade_id={trade_id})"
                            )
                        else:
                            self.log.warning(
                                f"[{close_symbol}] Failed to cancel sibling order {oid_str} after {er or 'EXIT'} close (trade_id={trade_id})"
                            )
            except Exception as e:
                self.log.warning(f"Sibling order cancellation best-effort failed for trade {trade_id}: {e}")

            # Notify callbacks
            if self.on_trade_close:
                try:
                    self.on_trade_close({
                        'trade_id': trade_id,
                        'symbol': fill.symbol,
                        'venue': self.venue,
                        'direction': direction,
                        'pnl': net_pnl,
                        'gross_pnl': gross_pnl_total,
                        'total_fees': total_fees,
                        'exit_reason': final_reason,
                        'exit_price': fill.fill_price,
                        'signals_agreed': json.loads(signals_agreed) if signals_agreed else [],
                    })
                except Exception as e:
                    self.log.error(f"on_trade_close callback error: {e}")

            return True
        finally:
            self._release_symbol_lock(fill.symbol, self.venue)


# ============================================================================
# Test
# ============================================================================

if __name__ == "__main__":
    import tempfile
    from pathlib import Path

    print("Fill Reconciler Test")
    print("=" * 60)

    # Create temp database
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.db"

        # Mock exchange adapter
        class MockAdapter:
            async def get_account_trades(self, limit=50):
                return [
                    {
                        'trade_id': 12345,
                        'symbol': 'ETH',
                        'side': 'sell',
                        'size': 0.5,
                        'price': 2576.00,  # Above TP
                        'fee': 0.12,
                        'timestamp': 1706303600,
                        'position_sign_changed': True,
                    }
                ]

        # Create reconciler
        close_events = []
        def on_close(data):
            close_events.append(data)

        reconciler = FillReconciler(
            db_path=str(db_path),
            exchange_adapter=MockAdapter(),
            venue="lighter",
            on_trade_close=on_close,
        )

        # Insert test trade
        with reconciler._db_conn() as conn:
            conn.execute(
                """
                INSERT INTO trades (symbol, direction, venue, entry_time, entry_price, size, sl_price, tp_price, signals_agreed)
                VALUES ('ETH', 'LONG', 'lighter', 1706300000, 2500.00, 0.5, 2450.00, 2575.00, '["cvd", "fade"]')
                """
            )
            conn.commit()

        # Run reconciliation
        async def test():
            processed = await reconciler.reconcile_now()
            print(f"Fills processed: {processed}")
            return processed

        processed = asyncio.run(test())

        # Verify results
        with reconciler._db_conn() as conn:
            conn.row_factory = sqlite3.Row

            # Check trade was closed
            trade = dict(conn.execute("SELECT * FROM trades WHERE id = 1").fetchone())
            print(f"Trade exit_reason: {trade['exit_reason']}")
            print(f"Trade exit_price: ${trade['exit_price']:.2f}")
            print(f"Trade realized_pnl: ${trade['realized_pnl']:.2f}")
            print(f"Trade total_fees: ${trade['total_fees']:.4f}")

            # Check fill was logged
            fill = dict(conn.execute("SELECT * FROM fills WHERE exchange_trade_id = '12345' AND venue = 'lighter'").fetchone())
            print(f"Fill type: {fill['fill_type']}")
            print(f"Fill price: ${fill['fill_price']:.2f}")

        # Check callback
        print(f"Close events: {len(close_events)}")
        if close_events:
            event = close_events[0]
            print(f"Callback pnl: ${event['pnl']:.2f}")
            print(f"Callback exit_reason: {event['exit_reason']}")

        # Verify idempotency
        async def test_idempotency():
            processed = await reconciler.reconcile_now()
            return processed

        processed2 = asyncio.run(test_idempotency())
        print(f"Second reconcile (should be 0): {processed2}")

        print("=" * 60)
        if trade['exit_reason'] == 'TP' and processed2 == 0:
            print("All tests passed!")
        else:
            print("TESTS FAILED")
