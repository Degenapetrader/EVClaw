#!/usr/bin/env python3
"""ATR Service (ATR(14) 1h) with caching.

Primary source: Binance USDT-M futures klines (fapi/v1/klines)
Fallback: Hyperliquid info candleSnapshot

Why:
- Tracker SSE currently reports atr.available=false everywhere.
- We still need ATR for risk sizing and SL/TP backstop.

Caching:
- 1h TTL (configurable) stored in SQLite (ai_trader.db) when available.
- Negative caching for unsupported Binance symbols to avoid repeated 400s.

Notes:
- This module is used by live_agent.py and cli.py.
- Keep logs minimal; never log proxy URLs.
"""

from __future__ import annotations

import asyncio
from logging_utils import get_logger
import os
import sqlite3
import time
from dataclasses import dataclass
from datetime import datetime, time as dtime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import aiohttp

try:
    from zoneinfo import ZoneInfo
except Exception:  # pragma: no cover
    ZoneInfo = None  # type: ignore

DEFAULT_INTERVAL = "1h"
DEFAULT_PERIOD = 14
DEFAULT_CACHE_TTL_SECONDS = 3600  # 1h
DEFAULT_NEGATIVE_TTL_SECONDS = 21600
ATR_LOCK_TTL_SECONDS = 3600
ATR_LOCK_MAX = 2000

BINANCE_BASE_URL = os.getenv("BINANCE_FAPI_BASE_URL", "https://fapi.binance.com").rstrip("/")
HYPERLIQUID_BASE_URL = os.getenv("HYPERLIQUID_PUBLIC_URL", "https://api.hyperliquid.xyz").rstrip("/")

# Massive (stocks OHLC) — used for HIP3 (XYZ:*) ATR fallback.
MASSIVE_BASE_URL = os.getenv("MASSIVE_BASE_URL", "https://api.massive.com").rstrip("/")
MASSIVE_API_KEY = (os.getenv("MASSIVE_API_KEY") or "").strip()

# HIP3 (XYZ:*) equities ATR should not be inflated by extended-hours noise.
# When enabled, we filter Massive hourly aggregates to regular trading hours (RTH).
HIP3_ATR_RTH_ONLY = True

MAX_RETRIES = 3



@dataclass
class ATRResult:
    symbol: str
    interval: str
    period: int
    atr: float
    atr_pct: Optional[float]
    close: Optional[float]
    source: str  # binance|hyperliquid|massive
    computed_at: float


class _ProxyRotator:
    def __init__(self, proxies: Optional[List[str]] = None):
        self._proxies = proxies or []
        self._idx = 0

    @classmethod
    def from_env(cls) -> "_ProxyRotator":
        raw = os.getenv("HYPERLIQUID_PROXIES", "")
        proxies = [p.strip() for p in raw.split(",") if p.strip()]
        return cls(proxies)

    def next(self) -> Optional[str]:
        if not self._proxies:
            return None
        proxy = self._proxies[self._idx % len(self._proxies)]
        self._idx = (self._idx + 1) % len(self._proxies)
        return proxy


class ATRService:
    # US equities regular trading hours (NYSE/Nasdaq): 09:30–16:00 America/New_York.
    _RTH_TZ = "America/New_York"
    _RTH_OPEN = dtime(9, 30)
    _RTH_CLOSE = dtime(16, 0)

    def __init__(
        self,
        *,
        db_path: Optional[str] = None,
        cache_ttl_seconds: int = DEFAULT_CACHE_TTL_SECONDS,
        negative_ttl_seconds: int = DEFAULT_NEGATIVE_TTL_SECONDS,
    ):
        self.log = get_logger("atr_service")
        self.db_path = str(db_path) if db_path else None
        self.cache_ttl_seconds = int(cache_ttl_seconds)
        self.negative_ttl_seconds = int(negative_ttl_seconds)

        self._session: Optional[aiohttp.ClientSession] = None
        self._proxy = _ProxyRotator.from_env()

        # per-key de-dupe (avoid concurrent duplicate fetches)
        self._locks: Dict[str, asyncio.Lock] = {}
        self._lock_last_used: Dict[str, float] = {}
        self._locks_guard = asyncio.Lock()

        if self.db_path:
            self._init_db()

    @staticmethod
    def _normalize_symbol_for_atr(symbol: str) -> str:
        """Canonicalize symbol casing for cache keys and HL API requests.

        Hyperliquid candleSnapshot is case-sensitive for builder symbols:
        - required: `xyz:HOOD`
        - fails: `XYZ:HOOD`, `xyz:hood`
        """
        sym = str(symbol or "").strip()
        if not sym:
            return ""
        if ":" not in sym:
            return sym.upper()
        prefix, base = sym.split(":", 1)
        return f"{prefix.strip().lower()}:{base.strip().upper()}"

    async def initialize(self) -> None:
        if self._session and not self._session.closed:
            return
        timeout = aiohttp.ClientTimeout(total=15)
        self._session = aiohttp.ClientSession(timeout=timeout)

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()

    # --------------------------------------------------------------------- DB
    def _get_conn(self) -> sqlite3.Connection:
        if not self.db_path:
            raise RuntimeError("ATRService db_path not set")
        conn = sqlite3.connect(self.db_path, timeout=30.0)
        conn.row_factory = sqlite3.Row
        # keep consistent with AITraderDB
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=30000")
        conn.execute("PRAGMA synchronous=NORMAL")
        return conn

    def _init_db(self) -> None:
        if not self.db_path:
            return
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        with self._get_conn() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS atr_cache (
                    symbol TEXT NOT NULL,
                    interval TEXT NOT NULL,
                    period INTEGER NOT NULL,
                    source TEXT NOT NULL,
                    atr REAL,
                    atr_pct REAL,
                    close REAL,
                    status TEXT NOT NULL DEFAULT 'OK',
                    updated_at REAL NOT NULL,
                    PRIMARY KEY (symbol, interval, period)
                )
                """
            )
            conn.execute("CREATE INDEX IF NOT EXISTS idx_atr_cache_updated_at ON atr_cache (updated_at DESC)")

    def _get_cached(self, symbol: str, interval: str, period: int) -> Tuple[Optional[ATRResult], Optional[str]]:
        if not self.db_path:
            return None, None
        now = time.time()
        with self._get_conn() as conn:
            row = conn.execute(
                """
                SELECT symbol, interval, period, source, atr, atr_pct, close, status, updated_at
                FROM atr_cache
                WHERE symbol=? AND interval=? AND period=?
                """,
                (symbol, interval, int(period)),
            ).fetchone()

        if not row:
            return None, None

        status = str(row["status"] or "OK")
        updated_at = float(row["updated_at"] or 0.0)
        ttl = self.cache_ttl_seconds if status == "OK" else self.negative_ttl_seconds
        if updated_at <= 0 or (now - updated_at) > ttl:
            return None, status

        if status != "OK":
            return None, status

        atr = float(row["atr"] or 0.0)
        if atr <= 0:
            return None, None

        atr_pct = float(row["atr_pct"]) if row["atr_pct"] is not None else None
        close = float(row["close"]) if row["close"] is not None else None

        return (
            ATRResult(
                symbol=symbol,
                interval=interval,
                period=int(period),
                atr=atr,
                atr_pct=atr_pct,
                close=close,
                source=str(row["source"]),
                computed_at=updated_at,
            ),
            status,
        )

    def _set_cached(
        self,
        *,
        symbol: str,
        interval: str,
        period: int,
        source: str,
        atr: Optional[float],
        atr_pct: Optional[float],
        close: Optional[float],
        status: str,
    ) -> None:
        if not self.db_path:
            return
        now = time.time()
        with self._get_conn() as conn:
            conn.execute(
                """
                INSERT INTO atr_cache(symbol, interval, period, source, atr, atr_pct, close, status, updated_at)
                VALUES(?,?,?,?,?,?,?,?,?)
                ON CONFLICT(symbol, interval, period)
                DO UPDATE SET source=excluded.source, atr=excluded.atr, atr_pct=excluded.atr_pct,
                              close=excluded.close, status=excluded.status, updated_at=excluded.updated_at
                """,
                (
                    symbol,
                    interval,
                    int(period),
                    source,
                    float(atr) if atr is not None else None,
                    float(atr_pct) if atr_pct is not None else None,
                    float(close) if close is not None else None,
                    status,
                    now,
                ),
            )

    # ------------------------------------------------------------------- Public
    @staticmethod
    def compute_atr_from_candles(
        candles: List[dict],
        period: int = DEFAULT_PERIOD,
        price: Optional[float] = None,
    ) -> Optional["ATRResult"]:
        """Compute ATR from pre-fetched candles (no API call).

        This is for callers who already have candle data and don't want
        to hit the API again. Uses same Wilder smoothing as get_atr().

        Args:
            candles: List of dicts with 'h', 'l', 'c' keys
            period: ATR period (default 14)
            price: Optional current price for atr_pct calculation

        Returns:
            ATRResult or None if insufficient data
        """
        if not candles or len(candles) < period + 2:
            return None

        # Extract OHLC
        highs: List[float] = []
        lows: List[float] = []
        closes: List[float] = []
        for c in candles:
            try:
                highs.append(float(c.get("h", 0)))
                lows.append(float(c.get("l", 0)))
                closes.append(float(c.get("c", 0)))
            except (TypeError, ValueError):
                continue

        if len(closes) < period + 2:
            return None

        # Compute TR
        trs: List[float] = []
        for i in range(1, len(closes)):
            high = highs[i]
            low = lows[i]
            prev_close = closes[i - 1]
            tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
            trs.append(tr)

        if len(trs) < period + 1:
            return None

        # Wilder smoothing
        atr = sum(trs[:period]) / float(period)
        for tr in trs[period:]:
            atr = (atr * (period - 1) + tr) / float(period)

        close = closes[-1] if closes else None
        base = price or close
        atr_pct = (atr / base * 100.0) if (base and base > 0) else None

        return ATRResult(
            symbol="",  # Caller should set if needed
            interval="1h",
            period=period,
            atr=float(atr),
            atr_pct=float(atr_pct) if atr_pct is not None else None,
            close=float(close) if close is not None else None,
            source="precomputed",
            computed_at=time.time(),
        )

    async def get_atr(
        self,
        symbol: str,
        *,
        price: Optional[float] = None,
        interval: str = DEFAULT_INTERVAL,
        period: int = DEFAULT_PERIOD,
    ) -> Optional[ATRResult]:
        """Get ATR (absolute) for symbol.

        Order:
        1) cache
        2) primary source by symbol class
           - xyz:* (HIP3): Hyperliquid candleSnapshot
           - perps: Binance futures
        3) fallback source
           - xyz:*: Massive (optional, if configured)
           - perps: Hyperliquid candleSnapshot
        """
        symbol = self._normalize_symbol_for_atr(symbol)
        if not symbol:
            return None

        await self.initialize()

        key = f"{symbol}|{interval}|{int(period)}"
        lock = await self._get_lock(key)
        async with lock:
            cached, status = self._get_cached(symbol, interval, int(period))
            if cached:
                return cached

            # HIP3 / non-crypto symbols: prefer Hyperliquid candleSnapshot.
            if ":" in symbol:
                result = await self._try_hyperliquid(symbol, interval=interval, period=period, price=price)
                if result:
                    self._set_cached(
                        symbol=symbol,
                        interval=interval,
                        period=period,
                        source=result.source,
                        atr=result.atr,
                        atr_pct=result.atr_pct,
                        close=result.close,
                        status="OK",
                    )
                    return result

                # Optional fallback for HIP3 if Massive is configured.
                result = await self._try_massive(symbol, interval=interval, period=period, price=price)
                if result:
                    self._set_cached(
                        symbol=symbol,
                        interval=interval,
                        period=period,
                        source=result.source,
                        atr=result.atr,
                        atr_pct=result.atr_pct,
                        close=result.close,
                        status="OK",
                    )
                    return result
            else:
                # Try Binance first
                result = await self._try_binance(symbol, interval=interval, period=period, price=price)
                if result:
                    self._set_cached(
                        symbol=symbol,
                        interval=interval,
                        period=period,
                        source=result.source,
                        atr=result.atr,
                        atr_pct=result.atr_pct,
                        close=result.close,
                        status="OK",
                    )
                    return result

            # Fallback to HL (perps-only candleSnapshot)
            result = await self._try_hyperliquid(symbol, interval=interval, period=period, price=price)
            if result:
                self._set_cached(
                    symbol=symbol,
                    interval=interval,
                    period=period,
                    source=result.source,
                    atr=result.atr,
                    atr_pct=result.atr_pct,
                    close=result.close,
                    status="OK",
                )
                return result

            # Cache negative outcome to avoid tight loops.
            src = "hyperliquid" if ":" in symbol else "binance"
            self._set_cached(
                symbol=symbol,
                interval=interval,
                period=period,
                source=src,
                atr=None,
                atr_pct=None,
                close=None,
                status="UNAVAILABLE",
            )
            return None

    async def _get_lock(self, key: str) -> asyncio.Lock:
        async with self._locks_guard:
            now = time.time()
            self._prune_locks(now)
            if key not in self._locks:
                self._locks[key] = asyncio.Lock()
            self._lock_last_used[key] = now
            return self._locks[key]

    def _prune_locks(self, now: Optional[float] = None) -> None:
        if now is None:
            now = time.time()
        if ATR_LOCK_TTL_SECONDS <= 0 and (ATR_LOCK_MAX <= 0 or len(self._locks) <= ATR_LOCK_MAX):
            return
        # TTL-based prune
        if ATR_LOCK_TTL_SECONDS > 0:
            cutoff = now - ATR_LOCK_TTL_SECONDS
            for key, lock in list(self._locks.items()):
                if lock.locked():
                    continue
                last_used = self._lock_last_used.get(key, now)
                if last_used < cutoff:
                    self._locks.pop(key, None)
                    self._lock_last_used.pop(key, None)
        # Size-based prune (oldest unlocked)
        if ATR_LOCK_MAX > 0 and len(self._locks) > ATR_LOCK_MAX:
            candidates = [
                (self._lock_last_used.get(k, now), k)
                for k, lock in self._locks.items()
                if not lock.locked()
            ]
            if candidates:
                candidates.sort()
                excess = len(self._locks) - ATR_LOCK_MAX
                for _last, key in candidates[:excess]:
                    self._locks.pop(key, None)
                    self._lock_last_used.pop(key, None)

    # ----------------------------------------------------------------- Binance
    @staticmethod
    def _binance_symbol_candidates(symbol: str) -> List[str]:
        """Return candidate Binance perp symbols for a given internal symbol.

        We prefer a conservative mapping:
        - Normal symbols: BTC -> BTCUSDT
        - HL "k"-prefixed symbols (e.g. kPEPE) map to Binance 1000x contracts.

        Important: do NOT treat every leading 'K' as a 1000x contract. Real
        tickers like KAITO/KAVA would be mis-mapped otherwise.
        """
        raw = str(symbol or "").strip()
        sym = raw.upper()
        if not sym or ":" in sym:
            return []

        # HL uses a literal lowercase 'k' prefix to denote 1000x contract symbols.
        if raw.startswith("k") and len(raw) > 1:
            base = sym[1:]
            # Prefer 1000x contract, but keep a fallback to non-1000 if it exists.
            out = [f"1000{base}USDT", f"{base}USDT"]
        else:
            out = [f"{sym}USDT"]

        # Deduplicate while preserving order.
        seen = set()
        deduped: List[str] = []
        for s in out:
            if s and s not in seen:
                seen.add(s)
                deduped.append(s)
        return deduped

    @staticmethod
    def _to_binance_symbol(symbol: str) -> Optional[str]:
        """Backward-compatible primary mapping for unit tests / callers."""
        cands = ATRService._binance_symbol_candidates(symbol)
        return cands[0] if cands else None

    async def _try_binance(
        self,
        symbol: str,
        *,
        interval: str,
        period: int,
        price: Optional[float],
    ) -> Optional[ATRResult]:
        assert self._session is not None

        bsymbols = self._binance_symbol_candidates(symbol)
        if not bsymbols:
            return None
        limit = max(60, period * 4)  # enough history for Wilder smoothing

        url = f"{BINANCE_BASE_URL}/fapi/v1/klines"

        last_error: Optional[str] = None
        all_invalid = True

        for bsymbol in bsymbols:
            params = {"symbol": bsymbol, "interval": interval, "limit": str(limit)}

            for attempt in range(MAX_RETRIES):
                proxy = self._proxy.next()
                try:
                    async with self._session.get(url, params=params, proxy=proxy) as resp:
                        if resp.status in (418, 429):
                            all_invalid = False
                            await asyncio.sleep(0.5 * (attempt + 1))
                            last_error = f"rate_limited_{resp.status}"
                            continue
                        if resp.status != 200:
                            body = await resp.text()
                            # Binance returns 400 with JSON {"code":-1121,"msg":"Invalid symbol."}
                            if resp.status == 400 and "Invalid symbol" in body:
                                break  # try next candidate symbol
                            all_invalid = False
                            last_error = f"http_{resp.status}"
                            continue

                        all_invalid = False
                        data = await resp.json()
                        atr, close = self._atr_from_binance_klines(data, period=period)
                        if atr and atr > 0:
                            close_price = close
                            # Prefer using provided price to compute pct if close is missing.
                            base = close_price or (float(price) if price else None)
                            atr_pct = (atr / base * 100.0) if (base and base > 0) else None
                            return ATRResult(
                                symbol=symbol,
                                interval=interval,
                                period=period,
                                atr=float(atr),
                                atr_pct=float(atr_pct) if atr_pct is not None else None,
                                close=float(close_price) if close_price is not None else None,
                                source="binance",
                                computed_at=time.time(),
                            )
                except Exception as exc:
                    all_invalid = False
                    last_error = str(exc)
                    await asyncio.sleep(0.25)
                    continue

        # If ALL candidates are invalid, mark as unsupported to avoid tight loops.
        if all_invalid:
            self._set_cached(
                symbol=symbol,
                interval=interval,
                period=period,
                source="binance",
                atr=None,
                atr_pct=None,
                close=None,
                status="UNSUPPORTED",
            )

        if last_error:
            self.log.debug(f"Binance ATR failed for {symbol}: {last_error}")
        return None

    @staticmethod
    def _atr_from_binance_klines(klines: Any, *, period: int) -> Tuple[Optional[float], Optional[float]]:
        if not isinstance(klines, list) or len(klines) < (period + 2):
            return None, None

        highs: List[float] = []
        lows: List[float] = []
        closes: List[float] = []
        for row in klines:
            if not isinstance(row, list) or len(row) < 5:
                continue
            try:
                highs.append(float(row[2]))
                lows.append(float(row[3]))
                closes.append(float(row[4]))
            except Exception:
                continue

        if len(closes) < (period + 2):
            return None, None

        trs: List[float] = []
        for i in range(1, len(closes)):
            high = highs[i]
            low = lows[i]
            prev_close = closes[i - 1]
            tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
            trs.append(tr)

        if len(trs) < (period + 1):
            return None, None

        # Wilder smoothing
        atr = sum(trs[:period]) / float(period)
        for tr in trs[period:]:
            atr = (atr * (period - 1) + tr) / float(period)

        return float(atr), float(closes[-1])

    # ----------------------------------------------------------------- Massive
    @staticmethod
    def _hip3_to_ticker(symbol: str) -> str:
        """Convert HIP3 symbol (xyz:TICKER) to Massive ticker."""
        sym = str(symbol or "").strip()
        if ":" in sym:
            # HIP3 uses xyz: prefix (case-insensitive). We keep the RHS as ticker.
            return sym.split(":", 1)[1].strip().upper()
        return sym.strip().upper()

    @staticmethod
    def _parse_interval_to_massive(interval: str) -> Tuple[int, str]:
        """Map interval like '1h' to Massive (multiplier, timespan)."""
        raw = str(interval or "").strip().lower()
        if not raw:
            return 1, "hour"
        # Common formats: 1h, 1hr, 60m, 1d
        import re
        m = re.match(r"^(\d+)([a-z]+)$", raw)
        if not m:
            return 1, "hour"
        mult = int(m.group(1))
        unit = m.group(2)
        if unit in {"m", "min", "mins", "minute", "minutes"}:
            return mult, "minute"
        if unit in {"h", "hr", "hrs", "hour", "hours"}:
            return mult, "hour"
        if unit in {"d", "day", "days"}:
            return mult, "day"
        return 1, "hour"

    @staticmethod
    def _massive_interval_delta(multiplier: int, timespan: str) -> timedelta:
        ts = str(timespan or "").strip().lower()
        mult = int(multiplier) if multiplier else 1
        if ts == "minute":
            return timedelta(minutes=mult)
        if ts == "hour":
            return timedelta(hours=mult)
        if ts == "day":
            return timedelta(days=mult)
        return timedelta(hours=1)

    @classmethod
    def _filter_massive_results_rth(
        cls,
        results: List[Any],
        *,
        multiplier: int,
        timespan: str,
    ) -> List[Any]:
        """Filter Massive aggregates rows to US regular trading hours (RTH).

        Massive returns `t` in milliseconds. We treat it as the *start* of the
        aggregate bar and include the bar if it overlaps [09:30, 16:00) ET on a
        weekday.
        """
        if not results:
            return []
        if ZoneInfo is None:
            return list(results)

        tz = ZoneInfo(cls._RTH_TZ)
        delta = cls._massive_interval_delta(multiplier, timespan)

        out: List[Any] = []
        for r in results:
            if not isinstance(r, dict):
                continue
            try:
                t_ms = int(r.get("t", 0) or 0)
            except Exception:
                continue
            if t_ms <= 0:
                continue

            start_utc = datetime.fromtimestamp(t_ms / 1000.0, tz=timezone.utc)
            end_utc = start_utc + delta

            start_et = start_utc.astimezone(tz)
            end_et = end_utc.astimezone(tz)

            # Weekday guard (ignore weekends).
            if start_et.weekday() >= 5:
                continue

            day = start_et.date()
            rth_open = datetime.combine(day, cls._RTH_OPEN, tzinfo=tz)
            rth_close = datetime.combine(day, cls._RTH_CLOSE, tzinfo=tz)

            # Overlap check: [start,end) intersects [open,close)
            if start_et < rth_close and end_et > rth_open:
                out.append(r)

        return out

    @staticmethod
    def _atr_from_massive_aggs(payload: Any, *, period: int) -> Tuple[Optional[float], Optional[float]]:
        if not isinstance(payload, dict):
            return None, None
        results = payload.get("results")
        if not isinstance(results, list) or len(results) < (period + 2):
            return None, None
        try:
            results = sorted(results, key=lambda r: int((r or {}).get("t", 0) or 0))
        except Exception:
            pass

        highs: List[float] = []
        lows: List[float] = []
        closes: List[float] = []
        for r in results:
            if not isinstance(r, dict):
                continue
            try:
                highs.append(float(r.get("h")))
                lows.append(float(r.get("l")))
                closes.append(float(r.get("c")))
            except Exception:
                continue
        if len(closes) < (period + 2):
            return None, None

        trs: List[float] = []
        for i in range(1, len(closes)):
            high = highs[i]
            low = lows[i]
            prev_close = closes[i - 1]
            tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
            trs.append(tr)

        if len(trs) < (period + 1):
            return None, None

        atr = sum(trs[:period]) / float(period)
        for tr in trs[period:]:
            atr = (atr * (period - 1) + tr) / float(period)

        return float(atr), float(closes[-1])

    async def _try_massive(
        self,
        symbol: str,
        *,
        interval: str,
        period: int,
        price: Optional[float],
    ) -> Optional[ATRResult]:
        assert self._session is not None

        if not MASSIVE_API_KEY:
            return None

        ticker = self._hip3_to_ticker(symbol)
        multiplier, timespan = self._parse_interval_to_massive(interval)

        # Pull ~2-3x the ATR period to stabilize Wilder smoothing.
        # NOTE: With RTH-only filtering, we need a much longer wall-clock window
        # to accumulate enough in-session bars (only ~6.5h/day).
        hours = max(40, period * 3)
        if HIP3_ATR_RTH_ONLY:
            hours = max(hours, period * 24)  # ~2 weeks for ATR(14) hourly bars
        now_ms = int(time.time() * 1000)
        start_ms = now_ms - int(hours * 3600 * 1000)

        url = (
            f"{MASSIVE_BASE_URL}/v2/aggs/ticker/{ticker}/range/"
            f"{int(multiplier)}/{timespan}/{start_ms}/{now_ms}"
        )
        params = {
            "sort": "asc",
            "limit": "50000",
            "adjusted": "true",
        }

        key = MASSIVE_API_KEY
        auth = key if key.lower().startswith("bearer ") else f"Bearer {key}"
        headers = {"Authorization": auth}

        last_error: Optional[str] = None
        for attempt in range(MAX_RETRIES):
            proxy = self._proxy.next()
            try:
                async with self._session.get(url, params=params, headers=headers, proxy=proxy) as resp:
                    if resp.status in (401, 403):
                        last_error = f"auth_{resp.status}"
                        break
                    if resp.status == 429:
                        await asyncio.sleep(0.5 * (attempt + 1))
                        last_error = "rate_limited_429"
                        continue
                    if resp.status != 200:
                        last_error = f"http_{resp.status}"
                        await asyncio.sleep(0.25)
                        continue

                    data = await resp.json()

                    # HIP3 equities: optionally filter Massive aggregates to US RTH.
                    if HIP3_ATR_RTH_ONLY:
                        try:
                            results = data.get("results") if isinstance(data, dict) else None
                            if isinstance(results, list) and results:
                                filtered = self._filter_massive_results_rth(
                                    results,
                                    multiplier=int(multiplier),
                                    timespan=str(timespan),
                                )
                                # Only apply filter if it leaves enough bars to compute ATR.
                                if len(filtered) >= (int(period) + 2):
                                    data = dict(data)
                                    data["results"] = filtered
                        except Exception:
                            pass

                    atr, close = self._atr_from_massive_aggs(data, period=period)
                    if atr and atr > 0:
                        base = close or (float(price) if price else None)
                        atr_pct = (atr / base * 100.0) if (base and base > 0) else None
                        return ATRResult(
                            symbol=symbol,
                            interval=interval,
                            period=period,
                            atr=float(atr),
                            atr_pct=float(atr_pct) if atr_pct is not None else None,
                            close=float(close) if close is not None else None,
                            source="massive",
                            computed_at=time.time(),
                        )
            except Exception as exc:
                last_error = str(exc)
                await asyncio.sleep(0.25)
                continue

        if last_error:
            self.log.debug(f"Massive ATR failed for {symbol}: {last_error}")
        return None

    # -------------------------------------------------------------- Hyperliquid
    async def _try_hyperliquid(
        self,
        symbol: str,
        *,
        interval: str,
        period: int,
        price: Optional[float],
    ) -> Optional[ATRResult]:
        assert self._session is not None

        # Request <= ~60 candles to avoid extra candleSnapshot weight scaling.
        hours = max(40, period * 3)  # 42 for period=14
        # If we filter HIP3 candles to regular US trading hours, widen lookback.
        if HIP3_ATR_RTH_ONLY and ":" in symbol:
            hours = max(hours, period * 24)
        now_ms = int(time.time() * 1000)
        start_ms = now_ms - int(hours * 3600 * 1000)

        url = f"{HYPERLIQUID_BASE_URL}/info"
        payload = {
            "type": "candleSnapshot",
            "req": {
                "coin": symbol,
                "interval": interval,
                "startTime": start_ms,
                "endTime": now_ms,
            },
        }

        last_error: Optional[str] = None
        for attempt in range(MAX_RETRIES):
            proxy = self._proxy.next()
            try:
                async with self._session.post(url, json=payload, proxy=proxy) as resp:
                    if resp.status == 429:
                        await asyncio.sleep(0.5 * (attempt + 1))
                        last_error = "rate_limited_429"
                        continue
                    if resp.status != 200:
                        last_error = f"http_{resp.status}"
                        await asyncio.sleep(0.25)
                        continue
                    candles = await resp.json()

                    # HIP3 equities: optionally filter HL aggregates to US RTH.
                    if HIP3_ATR_RTH_ONLY and ":" in symbol:
                        try:
                            multiplier, timespan = self._parse_interval_to_massive(interval)
                            filtered = self._filter_massive_results_rth(
                                candles if isinstance(candles, list) else [],
                                multiplier=int(multiplier),
                                timespan=str(timespan),
                            )
                            # Only apply filter if enough bars remain for ATR.
                            if len(filtered) >= (int(period) + 2):
                                candles = filtered
                        except Exception:
                            pass

                    atr, close = self._atr_from_hl_candles(candles, period=period)
                    if atr and atr > 0:
                        base = close or (float(price) if price else None)
                        atr_pct = (atr / base * 100.0) if (base and base > 0) else None
                        return ATRResult(
                            symbol=symbol,
                            interval=interval,
                            period=period,
                            atr=float(atr),
                            atr_pct=float(atr_pct) if atr_pct is not None else None,
                            close=float(close) if close is not None else None,
                            source="hyperliquid",
                            computed_at=time.time(),
                        )
            except Exception as exc:
                last_error = str(exc)
                await asyncio.sleep(0.25)
                continue

        if last_error:
            self.log.debug(f"Hyperliquid ATR failed for {symbol}: {last_error}")
        return None

    @staticmethod
    def _atr_from_hl_candles(candles: Any, *, period: int) -> Tuple[Optional[float], Optional[float]]:
        if not isinstance(candles, list) or len(candles) < (period + 2):
            return None, None

        # Ensure time-sorted
        try:
            candles = sorted(candles, key=lambda c: int(c.get("t", 0) or 0))
        except Exception:
            pass

        highs: List[float] = []
        lows: List[float] = []
        closes: List[float] = []
        for c in candles:
            if not isinstance(c, dict):
                continue
            try:
                highs.append(float(c.get("h")))
                lows.append(float(c.get("l")))
                closes.append(float(c.get("c")))
            except Exception:
                continue

        if len(closes) < (period + 2):
            return None, None

        trs: List[float] = []
        for i in range(1, len(closes)):
            high = highs[i]
            low = lows[i]
            prev_close = closes[i - 1]
            tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
            trs.append(tr)

        if len(trs) < (period + 1):
            return None, None

        atr = sum(trs[:period]) / float(period)
        for tr in trs[period:]:
            atr = (atr * (period - 1) + tr) / float(period)

        return float(atr), float(closes[-1])


# ------------------------------------------------------------------- Singleton
_ATR_SINGLETON: Optional[ATRService] = None


def get_atr_service(db_path: Optional[str] = None) -> ATRService:
    global _ATR_SINGLETON
    if _ATR_SINGLETON is None:
        _ATR_SINGLETON = ATRService(db_path=db_path)
        return _ATR_SINGLETON

    # Allow late-binding/rebinding db_path (important for tests and one-shot invocations
    # that switch runtime DB paths between calls).
    if db_path:
        resolved = str(db_path)
        current = str(_ATR_SINGLETON.db_path or "")
        if current != resolved:
            _ATR_SINGLETON.db_path = resolved
            _ATR_SINGLETON._init_db()

    return _ATR_SINGLETON
