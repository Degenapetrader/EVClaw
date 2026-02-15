"""SR-only resting limit manager for v0.

Design note (multi-process):
- `hl-live-agent` (loop mode) monitors pending SR-limit orders via `check_all()`.
- `cli guardian` places new SR-limit orders via `place()` when executing proposals.

We therefore MUST NOT hold the global file lock for the lifetime of a process.
The lock must be acquired for short critical sections and released immediately,
otherwise the long-running live-agent starves the guardian.
"""

from contextlib import contextmanager

from dataclasses import dataclass
from typing import Dict, Optional, Callable, Any, Tuple, List, Iterable
import asyncio
import fcntl
import os
import time
import logging
from pathlib import Path
from env_utils import EVCLAW_RUNTIME_DIR

SR_BROKEN_LONG_MULT = 0.995
SR_BROKEN_SHORT_MULT = 1.005
RUNTIME_DIR = Path(EVCLAW_RUNTIME_DIR)

# Lock acquisition tuning.
# - place(): may wait longer (called from guardian execution path)
# - check_all()/sync_startup(): best-effort and skip if lock is busy
SR_LIMIT_LOCK_TIMEOUT_SEC = 2.0
SR_LIMIT_LOCK_CHECK_TIMEOUT_SEC = 0.1

@dataclass
class PendingLimit:
    symbol: str
    venue: str
    direction: str              # LONG/SHORT
    limit_price: float
    intended_size: float        # What we requested
    exchange_order_id: str
    sr_level: float
    placed_at: float
    expires_at: float
    entry_direction: str        # For signal flip detection

    # Learning/journaling context captured at placement time
    signals_snapshot: Optional[str] = None  # JSON string
    signals_agreed: Optional[str] = None    # JSON string (list)
    context_snapshot: Optional[str] = None  # JSON string
    conviction: Optional[float] = None
    reason: Optional[str] = None

    # v0.2 (HL only): prefilled SL stop
    sl_order_id: Optional[str] = None
    sl_price: Optional[float] = None

    state: str = "PENDING"      # PENDING/FILLED/CANCELLED/EXPIRED
    filled_size: float = 0.0    # Actual filled
    filled_price: float = 0.0   # Actual avg price

    @staticmethod
    def from_db_row(row: Dict[str, Any]) -> "PendingLimit":
        """Explicit mapping from DB row to PendingLimit with type coercion."""
        return PendingLimit(
            symbol=str(row["symbol"]),
            venue=str(row["venue"]),
            direction=str(row["direction"]),
            limit_price=float(row["limit_price"]),
            intended_size=float(row["intended_size"]),
            exchange_order_id=str(row["exchange_order_id"]),
            sr_level=float(row["sr_level"]),
            placed_at=float(row["placed_at"]),
            expires_at=float(row["expires_at"]),
            entry_direction=str(row["entry_direction"]),
            signals_snapshot=row.get("signals_snapshot"),
            signals_agreed=row.get("signals_agreed"),
            context_snapshot=row.get("context_snapshot"),
            conviction=float(row["conviction"]) if row.get("conviction") is not None else None,
            reason=row.get("reason"),
            sl_order_id=row.get("sl_order_id"),
            sl_price=float(row["sl_price"]) if row.get("sl_price") is not None else None,
            state=str(row.get("state", "PENDING")),
            filled_size=float(row.get("filled_size", 0.0)),
            filled_price=float(row.get("filled_price", 0.0)),
        )


class PendingLimitManager:
    """Manages SR-anchored pending limit orders.

    Source of truth is the SQLite `pending_orders` table (via AITraderDB).

    Multi-process safety:
    - use a shared file lock to serialize place/check/sync operations
    - do NOT keep the lock held across the lifetime of a long-running process
    """

    def __init__(self, config: Dict[str, Any], log: logging.Logger, *, lock_path: Optional[str] = None):
        self.cfg = config or {}
        self.log = log
        self.lock_path = (
            Path(lock_path)
            if lock_path
            else Path(str(RUNTIME_DIR / "pending_limit.lock"))
        )

        # Keyed by (SYMBOL, venue)
        self._pending: Dict[Tuple[str, str], PendingLimit] = {}

    @property
    def enabled(self) -> bool:
        return bool(self.cfg.get("sr_limit_enabled", False))

    @staticmethod
    def _k(symbol: str, venue: str) -> Tuple[str, str]:
        return (str(symbol or "").strip().upper(), str(venue or "").strip().lower())

    def _acquire_lock(self, timeout_sec: float) -> Optional[object]:
        """Acquire the shared SR_LIMIT lock.

        Returns an open file handle if lock acquired; None otherwise.
        """
        try:
            self.lock_path.parent.mkdir(parents=True, exist_ok=True)
            fp = open(self.lock_path, "a+")
        except Exception as e:
            self.log.warning(f"Pending limit lock open failed: {e}")
            return None

        deadline = time.time() + max(0.0, float(timeout_sec or 0.0))
        while True:
            try:
                fcntl.flock(fp.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                return fp
            except BlockingIOError:
                if time.time() >= deadline:
                    try:
                        fp.close()
                    except Exception:
                        pass
                    return None
                time.sleep(0.05)
                continue
            except Exception as e:
                self.log.warning(f"Pending limit lock failed: {e}")
                try:
                    fp.close()
                except Exception:
                    pass
                return None

    @staticmethod
    def _release_lock(fp: object) -> None:
        try:
            fcntl.flock(fp.fileno(), fcntl.LOCK_UN)
        except Exception:
            pass
        try:
            fp.close()
        except Exception:
            pass

    @contextmanager
    def _locked(self, *, timeout_sec: float) -> Iterable[bool]:
        fp = self._acquire_lock(timeout_sec)
        if not fp:
            yield False
            return
        try:
            yield True
        finally:
            self._release_lock(fp)

    def _refresh_pending_from_db(self, db) -> None:
        """Refresh in-memory pending cache from DB."""
        try:
            rows = db.get_pending_orders(state="PENDING") if db else []
        except Exception:
            rows = []

        pending: Dict[Tuple[str, str], PendingLimit] = {}
        for row in rows or []:
            try:
                p = PendingLimit.from_db_row(row)
            except Exception:
                continue
            pending[self._k(p.symbol, p.venue)] = p
        self._pending = pending

    def _is_current_pending(
        self,
        key: Tuple[str, str],
        exchange_order_id: str,
        *,
        timeout_sec: float,
    ) -> bool:
        with self._locked(timeout_sec=timeout_sec) as ok:
            if not ok:
                return False
            cur = self._pending.get(key)
            if not cur:
                return False
            return str(cur.exchange_order_id) == str(exchange_order_id)

    def _remove_pending_if_current(
        self,
        key: Tuple[str, str],
        exchange_order_id: str,
        *,
        timeout_sec: float,
    ) -> bool:
        with self._locked(timeout_sec=timeout_sec) as ok:
            if not ok:
                return False
            cur = self._pending.get(key)
            if not cur:
                return False
            if str(cur.exchange_order_id) != str(exchange_order_id):
                return False
            self._pending.pop(key, None)
            return True

    def get_pending(self, symbol: str, venue: Optional[str] = None, db=None) -> Optional[PendingLimit]:
        if db is not None:
            self._refresh_pending_from_db(db)
        sym = str(symbol or "").strip().upper()
        if not sym:
            return None
        if venue:
            return self._pending.get(self._k(sym, venue))
        for (s, _v), p in self._pending.items():
            if s == sym:
                return p
        return None


    def can_place(self, symbol: str, venue: str, notional: float, equity: float, *, db=None) -> bool:
        if not self.enabled:
            return False

        if db is not None:
            self._refresh_pending_from_db(db)

        key = self._k(symbol, venue)
        if key in self._pending:
            return False

        # Caps are enforced per-venue (equity pool differs between vault vs wallet).
        venue_norm = str(venue or "").strip().lower()
        pending_same_venue = [p for p in self._pending.values() if str(p.venue).strip().lower() == venue_norm]

        max_pending = int(self.cfg.get("sr_limit_max_pending", 2) or 2)
        if len(pending_same_venue) >= max_pending:
            return False

        pending_notional = 0.0
        for p in pending_same_venue:
            try:
                pending_notional += float(p.limit_price) * float(p.intended_size)
            except Exception as e:
                self.log.warning(
                    f"[{getattr(p, 'symbol', '?')}] Ignoring malformed pending notional row: {e}"
                )

        try:
            max_pct = float(self.cfg.get("sr_limit_max_notional_pct", 25) or 25) / 100.0
        except Exception:
            max_pct = 0.25

        # If equity is unavailable, do NOT block placement (we can still enforce max_pending + per-symbol dedup).
        try:
            eq = float(equity) if equity is not None else 0.0
        except Exception:
            eq = 0.0

        try:
            if eq > 0 and (float(pending_notional) + float(notional)) > eq * float(max_pct):
                return False
        except Exception:
            # Malformed numbers: be conservative on caps, but don't hard-block all placements.
            return True

        return True

    async def cancel_existing(
        self,
        symbol: str,
        venue: str,
        adapter,
        *,
        db=None,
        reason: str = "REPLACED",
    ) -> bool:
        """Cancel an existing pending SR-limit for (symbol, venue) if present.

        Called from the guardian execution path before placing a new SR-limit.
        Safe to call even if nothing exists.

        Returns True if something was cancelled.
        """
        if db is None:
            return False

        timeout = float(self.cfg.get("sr_limit_lock_timeout_sec", SR_LIMIT_LOCK_TIMEOUT_SEC) or SR_LIMIT_LOCK_TIMEOUT_SEC)
        with self._locked(timeout_sec=timeout) as ok:
            if not ok:
                self.log.warning("[SR_LIMIT] Lock held by another process; cannot cancel_existing")
                return False

            self._refresh_pending_from_db(db)
            key = self._k(symbol, venue)
            p = self._pending.get(key)
            if not p:
                return False

            # Best-effort cancel entry + prefilled SL.
            try:
                await adapter.cancel_order(p.symbol, p.exchange_order_id)
            except Exception as e:
                self.log.warning(f"[{p.symbol}] Failed to cancel existing SR limit {p.exchange_order_id}: {e}")

            try:
                if p.sl_order_id:
                    await adapter.cancel_order(p.symbol, str(p.sl_order_id))
            except Exception as e:
                self.log.warning(f"[{p.symbol}] Failed to cancel existing prefilled SL {p.sl_order_id}: {e}")
                try:
                    db.log_orphan_order(
                        symbol=p.symbol,
                        venue=p.venue,
                        order_id=str(p.sl_order_id),
                        order_type="SL_PREFILLED",
                        reason="CANCEL_FAILED",
                        meta={"entry_order_id": p.exchange_order_id, "error": str(e), "cancel_reason": reason},
                    )
                except Exception:
                    pass

            try:
                db.update_pending_state(p.exchange_order_id, "CANCELLED", reason, 0.0, 0.0)
            except Exception:
                pass

            self._pending.pop(key, None)
            self.log.info(f"[{p.symbol}] Cancelled existing SR limit order_id={p.exchange_order_id} reason={reason}")
            return True

    async def place(
        self,
        symbol: str,
        direction: str,
        sr_level: float,
        size: float,
        adapter,
        venue: str,
        db,
        *,
        signals_snapshot: Optional[str] = None,
        signals_agreed: Optional[str] = None,
        context_snapshot: Optional[str] = None,
        conviction: Optional[float] = None,
        reason: Optional[str] = None,
        sl_order_id: Optional[str] = None,
        sl_price: Optional[float] = None,
        notional: Optional[float] = None,
        equity: Optional[float] = None,
    ) -> bool:
        """Place SR-anchored limit. Returns True if placed.

        Note: lock is held for the duration of placement (including exchange call)
        to prevent concurrent processes from exceeding pending caps.
        """
        if not self.enabled:
            return False

        timeout = float(self.cfg.get("sr_limit_lock_timeout_sec", SR_LIMIT_LOCK_TIMEOUT_SEC) or SR_LIMIT_LOCK_TIMEOUT_SEC)
        with self._locked(timeout_sec=timeout) as ok:
            if not ok:
                self.log.warning("[SR_LIMIT] Lock held by another process; refusing to place")
                return False

            # Refresh pending snapshot under lock.
            self._refresh_pending_from_db(db)

            # Enforce caps again under lock (caller may have checked already).
            try:
                n = float(notional) if notional is not None else float(sr_level) * float(size)
            except Exception:
                n = float(sr_level) * float(size)
            try:
                eq = float(equity) if equity is not None else 0.0
            except Exception:
                eq = 0.0
            if eq > 0 and not self.can_place(symbol, venue, n, eq, db=None):
                return False

            side = "buy" if str(direction).upper() == "LONG" else "sell"
            ok2, oid = await adapter.place_limit_order(symbol, side, float(size), float(sr_level), tif="Alo")
            if not ok2:
                self.log.warning(f"[{symbol}] SR limit placement failed")
                return False

            timeout_min = float(self.cfg.get("sr_limit_timeout_minutes", 30) or 30)
            p = PendingLimit(
                symbol=str(symbol),
                venue=str(venue),
                direction=str(direction),
                limit_price=float(sr_level),
                intended_size=float(size),
                exchange_order_id=str(oid),
                sr_level=float(sr_level),
                placed_at=time.time(),
                expires_at=time.time() + timeout_min * 60,
                entry_direction=str(direction),
                signals_snapshot=signals_snapshot,
                signals_agreed=signals_agreed,
                context_snapshot=context_snapshot,
                conviction=conviction,
                reason=reason,
                sl_order_id=sl_order_id,
                sl_price=sl_price,
            )

            # Persist + cache
            journal_attempts = max(1, int(self.cfg.get("sr_limit_journal_retry_attempts", 3) or 3))
            retry_sleep_sec = max(0.0, float(self.cfg.get("sr_limit_journal_retry_sleep_sec", 0.2) or 0.2))
            journal_ok = False
            last_journal_err: Optional[Exception] = None
            for attempt in range(1, journal_attempts + 1):
                try:
                    db.insert_pending_order(p)
                    journal_ok = True
                    break
                except Exception as e:
                    last_journal_err = e
                    self.log.warning(
                        f"[{symbol}] Failed to journal pending order "
                        f"(attempt {attempt}/{journal_attempts}): {e}"
                    )
                    if attempt < journal_attempts and retry_sleep_sec > 0:
                        await asyncio.sleep(retry_sleep_sec)

            if not journal_ok:
                cancel_ok = False
                try:
                    cancel_ok = bool(await adapter.cancel_order(symbol, str(oid)))
                except Exception as ce:
                    self.log.error(f"[{symbol}] Failed to cancel non-journaled SR limit {oid}: {ce}")

                if hasattr(db, "log_orphan_order"):
                    try:
                        db.log_orphan_order(
                            symbol=str(symbol),
                            venue=str(venue),
                            order_id=str(oid),
                            order_type="SR_LIMIT_ENTRY",
                            reason="JOURNAL_FAILED",
                            meta={
                                "journal_error": str(last_journal_err) if last_journal_err else "",
                                "cancel_ok": bool(cancel_ok),
                            },
                        )
                    except Exception as oe:
                        self.log.warning(f"[{symbol}] Failed to log orphan SR order {oid}: {oe}")

                if not cancel_ok:
                    self.log.error(f"[{symbol}] CRITICAL: non-journaled SR order {oid} could not be cancelled")
                return False

            self._pending[self._k(p.symbol, p.venue)] = p

            self.log.info(f"[{symbol}] SR limit @ {sr_level:.6g} {direction} size={size:.6f}")
            return True

    def _get_adapter_for_venue(self, venue: str, adapters: Dict[str, Any]):
        """Get correct adapter for venue."""
        return adapters.get(venue)

    async def check_all(
        self,
        adapters: Dict[str, Any],  # {'lighter': adapter, 'hyperliquid': adapter}
        db,
        get_signal_direction: Callable[[str], Optional[str]],
        get_current_price: Callable[[str], float],
    ) -> list:
        """Check all pending orders. Returns list of filled PendingLimit for executor."""
        # IMPORTANT: do NOT hold the SR_LIMIT lock across network calls.
        # The guardian places new SR-limits (place()) and needs the lock to
        # enforce caps. check_all() should never block trading decisions.
        check_timeout = float(self.cfg.get("sr_limit_lock_check_timeout_sec", SR_LIMIT_LOCK_CHECK_TIMEOUT_SEC) or SR_LIMIT_LOCK_CHECK_TIMEOUT_SEC)
        with self._locked(timeout_sec=check_timeout) as ok:
            if not ok:
                # Best-effort: if we can't lock quickly, just skip this check.
                self.log.warning("[SR_LIMIT] Lock held by another process; skipping check_all")
                return []
            self._refresh_pending_from_db(db)
            items = list(self._pending.items())

        filled_orders: List[PendingLimit] = []

        for key, p in items:
            try:
                adapter = self._get_adapter_for_venue(p.venue, adapters)
                if not adapter:
                    self.log.error(f"[{p.symbol}] No adapter for venue {p.venue}")
                    continue

                status = await adapter.check_order_status(p.symbol, p.exchange_order_id)
                filled_size = float(status.get("filled_size", 0.0) or 0.0)
                remaining = float(status.get("remaining_size", 0.0) or 0.0)

                # ANY fill > 0 → cancel remainder, mark filled, hand to executor
                if filled_size > 0:
                    if not self._is_current_pending(key, p.exchange_order_id, timeout_sec=check_timeout):
                        self.log.debug(f"[{p.symbol}] Skipping stale pending fill update for {p.exchange_order_id}")
                        continue
                    if remaining > 0:
                        try:
                            await adapter.cancel_order(p.symbol, p.exchange_order_id)
                        except Exception as e:
                            self.log.warning(f"[{p.symbol}] Failed to cancel partial remainder: {e}")
                        self.log.info(
                            f"[{p.symbol}] SR limit partial fill {filled_size:.6f}, cancelled remainder {remaining:.6f}"
                        )
                    p.filled_size = filled_size
                    p.filled_price = float(status.get("avg_price", p.limit_price) or p.limit_price)
                    p.state = "FILLED"
                    db.update_pending_state(p.exchange_order_id, "FILLED", None, filled_size, p.filled_price)
                    filled_orders.append(p)
                    self._remove_pending_if_current(key, p.exchange_order_id, timeout_sec=check_timeout)
                    continue

                # Signal flip check
                cur_dir = get_signal_direction(p.symbol)
                if cur_dir and cur_dir != p.entry_direction:
                    if not self._is_current_pending(key, p.exchange_order_id, timeout_sec=check_timeout):
                        self.log.debug(f"[{p.symbol}] Skipping stale SIGNAL_FLIP cancel for {p.exchange_order_id}")
                        continue
                    filled_after_cancel = await self._cancel(adapter, db, p, "SIGNAL_FLIP")
                    if filled_after_cancel is not None:
                        filled_orders.append(filled_after_cancel)
                    self._remove_pending_if_current(key, p.exchange_order_id, timeout_sec=check_timeout)
                    continue

                # SR broken check (price through level by threshold)
                cur_price = float(get_current_price(p.symbol) or 0.0)
                if cur_price > 0:
                    broken = (
                        (p.direction == "LONG" and cur_price < p.sr_level * SR_BROKEN_LONG_MULT)
                        or (p.direction == "SHORT" and cur_price > p.sr_level * SR_BROKEN_SHORT_MULT)
                    )
                    if broken:
                        if not self._is_current_pending(key, p.exchange_order_id, timeout_sec=check_timeout):
                            self.log.debug(f"[{p.symbol}] Skipping stale SR_BROKEN cancel for {p.exchange_order_id}")
                            continue
                        filled_after_cancel = await self._cancel(adapter, db, p, "SR_BROKEN")
                        if filled_after_cancel is not None:
                            filled_orders.append(filled_after_cancel)
                        self._remove_pending_if_current(key, p.exchange_order_id, timeout_sec=check_timeout)
                        continue

                # Timeout check
                if time.time() > float(p.expires_at or 0.0):
                    if not self._is_current_pending(key, p.exchange_order_id, timeout_sec=check_timeout):
                        self.log.debug(f"[{p.symbol}] Skipping stale EXPIRED cancel for {p.exchange_order_id}")
                        continue
                    filled_after_cancel = await self._cancel(adapter, db, p, "EXPIRED")
                    if filled_after_cancel is not None:
                        filled_orders.append(filled_after_cancel)
                    self._remove_pending_if_current(key, p.exchange_order_id, timeout_sec=check_timeout)
            except Exception as e:
                self.log.warning(f"[{p.symbol}] check_all processing error: {e}")

        return filled_orders

    async def _cancel(self, adapter, db, p: PendingLimit, reason: str) -> Optional[PendingLimit]:
        # Cancel entry
        try:
            await adapter.cancel_order(p.symbol, p.exchange_order_id)
        except Exception as e:
            self.log.warning(f"[{p.symbol}] Failed to cancel pending entry {p.exchange_order_id}: {e}")

        # v0.2 HL: cancel prefilled SL too (avoid stale protection orders)
        try:
            if p.sl_order_id:
                await adapter.cancel_order(p.symbol, str(p.sl_order_id))
        except Exception as e:
            self.log.warning(
                f"[{p.symbol}] Failed to cancel prefilled SL order {p.sl_order_id}: {e}"
            )
            try:
                db.log_orphan_order(
                    symbol=p.symbol,
                    venue=p.venue,
                    order_id=str(p.sl_order_id),
                    order_type="SL_PREFILLED",
                    reason="CANCEL_FAILED",
                    meta={
                        "entry_order_id": p.exchange_order_id,
                        "error": str(e),
                        "cancel_reason": reason,
                    },
                )
            except Exception:
                pass

        # Re-check final status after cancel attempt: partials can race with cancellation.
        filled_size = 0.0
        filled_price = 0.0
        try:
            status = await adapter.check_order_status(p.symbol, p.exchange_order_id)
            filled_size = float(status.get("filled_size", 0.0) or 0.0)
            filled_price = float(status.get("avg_price", p.limit_price) or p.limit_price)
        except Exception as e:
            self.log.debug(f"[{p.symbol}] Post-cancel status check failed for {p.exchange_order_id}: {e}")

        if filled_size > 0:
            p.filled_size = filled_size
            p.filled_price = filled_price if filled_price > 0 else float(p.limit_price)
            p.state = "FILLED"
            try:
                db.update_pending_state(
                    p.exchange_order_id,
                    "FILLED",
                    reason,
                    p.filled_size,
                    p.filled_price,
                )
            except Exception:
                pass
            self.log.warning(
                f"[{p.symbol}] SR limit filled during cancel ({reason}): "
                f"size={p.filled_size:.6f} price={p.filled_price:.6f}"
            )
            return p

        p.state = "EXPIRED" if reason == "EXPIRED" else "CANCELLED"
        try:
            db.update_pending_state(p.exchange_order_id, p.state, reason, 0.0, 0.0)
        except Exception:
            pass
        self.log.info(f"[{p.symbol}] SR limit {p.state}: {reason}")
        return None

    async def sync_startup(self, adapters: Dict[str, Any], db) -> list:
        """Crash recovery: reconcile DB pending with exchange.

        Returns list of PendingLimit that filled during downtime (for executor).
        """
        timeout = float(self.cfg.get("sr_limit_lock_check_timeout_sec", SR_LIMIT_LOCK_CHECK_TIMEOUT_SEC) or SR_LIMIT_LOCK_CHECK_TIMEOUT_SEC)
        with self._locked(timeout_sec=timeout) as ok:
            if not ok:
                self.log.warning("[SR_LIMIT] Lock held by another process; skipping startup sync")
                return []

            # IMPORTANT: do NOT hold the SR_LIMIT lock across network calls.
            filled_during_downtime: List[PendingLimit] = []

            # Load pending from DB under lock then release.
            self._refresh_pending_from_db(db)
            items = list(self._pending.items())
            checked_keys = {k for k, _p in items}

        open_pending: Dict[Tuple[str, str], PendingLimit] = {}

        for key, p in items:
            adapter = self._get_adapter_for_venue(p.venue, adapters)
            if not adapter:
                self.log.error(f"[{p.symbol}] No adapter for venue {p.venue} on startup")
                db.update_pending_state(p.exchange_order_id, "CANCELLED", "NO_ADAPTER", 0, 0)
                continue

            status = await adapter.check_order_status(p.symbol, p.exchange_order_id)

            if status.get("status") == "open":
                open_pending[key] = p
            elif float(status.get("filled_size", 0) or 0) > 0:
                p.filled_size = float(status["filled_size"])
                p.filled_price = float(status.get("avg_price", p.limit_price) or p.limit_price)
                p.state = "FILLED"
                db.update_pending_state(
                    p.exchange_order_id,
                    "FILLED",
                    "FILLED_DURING_RESTART",
                    p.filled_size,
                    p.filled_price,
                )
                filled_during_downtime.append(p)
            else:
                db.update_pending_state(p.exchange_order_id, "CANCELLED", "STALE_ON_RESTART", 0, 0)

        timeout = float(self.cfg.get("sr_limit_lock_check_timeout_sec", SR_LIMIT_LOCK_CHECK_TIMEOUT_SEC) or SR_LIMIT_LOCK_CHECK_TIMEOUT_SEC)
        with self._locked(timeout_sec=timeout) as ok:
            if ok:
                # Merge update instead of blind overwrite so new pending entries
                # created during sync are not dropped.
                self._refresh_pending_from_db(db)
                merged = dict(self._pending)
                for key in checked_keys:
                    merged.pop(key, None)
                for key, pending in open_pending.items():
                    merged[key] = pending
                self._pending = merged
        return filled_during_downtime
