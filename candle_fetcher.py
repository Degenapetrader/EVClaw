#!/usr/bin/env python3
"""
Binance Futures-first candle fetcher with Hyperliquid fallback.

Provides 1h candles for aggregation into 8h/24h/7d windows.
Designed for background workers that need reliable OHLCV data.
"""

import asyncio
import hashlib
import json
import logging
import os
import time
from typing import Dict, List, Optional, Tuple

import aiohttp

from env_utils import env_str


class _ProxyRotator:
    """Round-robin proxy rotator.

    Boss directive: keep ONE proxy source.
    We use `HYPERLIQUID_PROXIES` from `.env` for all public HTTP calls here
    (Binance + HL candles) to reduce rate-limit risk.
    """

    def __init__(self, proxies: Optional[List[str]] = None):
        self._proxies = proxies or []
        self._idx = 0

    @classmethod
    def from_env(cls) -> "_ProxyRotator":
        raw = (os.getenv("HYPERLIQUID_PROXIES") or "").strip()
        proxies = [p.strip() for p in raw.split(",") if p.strip()]
        return cls(proxies)

    def next(self) -> Optional[str]:
        if not self._proxies:
            return None
        proxy = self._proxies[self._idx % len(self._proxies)]
        self._idx = (self._idx + 1) % len(self._proxies)
        return proxy


_PROXY = _ProxyRotator.from_env()


# Massive (stocks OHLC) for HIP3 candles (xyz:* symbols)
MASSIVE_BASE_URL = env_str("MASSIVE_BASE_URL", "https://api.massive.com").rstrip("/")
MASSIVE_API_KEY = (os.getenv("MASSIVE_API_KEY") or "").strip()

_binance_base = env_str("BINANCE_FAPI_BASE_URL", "https://fapi.binance.com").rstrip("/")
BINANCE_FUTURES_KLINES_URL = f"{_binance_base}/fapi/v1/klines"
_hl_public_base = env_str("HYPERLIQUID_PUBLIC_URL", "https://api.hyperliquid.xyz").rstrip("/")
HL_PUBLIC_API_URL = _hl_public_base if _hl_public_base.endswith("/info") else f"{_hl_public_base}/info"
BINANCE_MAX_LIMIT = 1500

# Retries/backoff for flaky public endpoints.
CANDLE_FETCH_MAX_RETRIES = 3
CANDLE_FETCH_BACKOFF_BASE_SEC = 0.6
CANDLE_FETCH_BACKOFF_MAX_SEC = 5.0

# Optional: reduce API load by caching 1h candles to disk for a short TTL.
# Helps because 3 workers fetch the same candles and trend_filter runs frequently.
CANDLE_CACHE_TTL_SEC = 180
CANDLE_CACHE_DIR = "/tmp"
CANDLE_CACHE_LOCK_STALE_SEC = 120
CANDLE_CACHE_RETENTION_SEC = 86400
CANDLE_CACHE_MAX_FILES = 500
CANDLE_CACHE_BAD_EMPTY_RATIO = 0.5
CANDLE_CACHE_BAD_TTL_SEC = 60

# Binance bans fast when we stampede it. If we detect a ban/rate-limit, we stop
# using Binance until the reported timestamp.
_DISABLE_BINANCE_UNTIL_MS = 0
BINANCE_COOLDOWN_FILE = "/tmp/evclaw_binance_cooldown_until_ms.txt"

INTERVAL_MS = {
    "1m": 60_000,
    "3m": 180_000,
    "5m": 300_000,
    "15m": 900_000,
    "30m": 1_800_000,
    "1h": 3_600_000,
    "2h": 7_200_000,
    "4h": 14_400_000,
    "6h": 21_600_000,
    "8h": 28_800_000,
    "12h": 43_200_000,
    "1d": 86_400_000,
    "3d": 259_200_000,
    "1w": 604_800_000,
}


def _get_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            "%(asctime)s [%(name)s] %(levelname)s: %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
    return logger


def _to_binance_symbol(symbol: str) -> Optional[str]:
    """
    Convert HL symbol to Binance Futures symbol.

    Notes:
    - HL "k" prefix maps to Binance 1000x contracts (e.g., kPEPE -> 1000PEPEUSDT).
    - HIP-3 symbols (dex:ABC) are not on Binance.
    """
    if not symbol:
        return None
    if ":" in symbol:
        return None
    sym = symbol.upper()
    if sym.startswith("K") and len(sym) > 1:
        return f"1000{sym[1:]}USDT"
    return f"{sym}USDT"


def _normalize_hl_candles(raw: List[dict]) -> List[dict]:
    candles: List[dict] = []
    for c in raw or []:
        try:
            candles.append({
                "t": int(c.get("t")),
                "o": float(c.get("o", 0)),
                "h": float(c.get("h", 0)),
                "l": float(c.get("l", 0)),
                "c": float(c.get("c", 0)),
                "v": float(c.get("v", 0)),
            })
        except Exception:
            continue
    candles.sort(key=lambda x: x["t"])
    return candles


def _normalize_binance_klines(raw: List[list]) -> List[dict]:
    candles: List[dict] = []
    for row in raw or []:
        try:
            candles.append({
                "t": int(row[0]),  # open time
                "o": float(row[1]),
                "h": float(row[2]),
                "l": float(row[3]),
                "c": float(row[4]),
                "v": float(row[5]),
            })
        except Exception:
            continue
    candles.sort(key=lambda x: x["t"])
    return candles


def _normalize_massive_aggs(payload: object) -> List[dict]:
    """Normalize Massive aggregates response into the common candle format."""
    if not isinstance(payload, dict):
        return []
    results = payload.get("results")
    if not isinstance(results, list):
        return []
    candles: List[dict] = []
    for r in results:
        if not isinstance(r, dict):
            continue
        try:
            candles.append({
                "t": int(r.get("t")),
                "o": float(r.get("o", 0)),
                "h": float(r.get("h", 0)),
                "l": float(r.get("l", 0)),
                "c": float(r.get("c", 0)),
                "v": float(r.get("v", 0) or 0.0),
            })
        except Exception:
            continue
    candles.sort(key=lambda x: x["t"])
    return candles


def _backoff_sec(attempt: int) -> float:
    # 0 -> base, 1 -> 2*base, ... with hard cap
    return min(CANDLE_FETCH_BACKOFF_MAX_SEC, CANDLE_FETCH_BACKOFF_BASE_SEC * (2 ** max(0, attempt)))


def _short_body(text: str, limit: int = 160) -> str:
    t = (text or "").replace("\n", " ").strip()
    return t[:limit]


def _read_shared_binance_cooldown_until_ms() -> int:
    global _DISABLE_BINANCE_UNTIL_MS
    file_until = 0
    try:
        with open(BINANCE_COOLDOWN_FILE, "r") as f:
            raw = f.read().strip()
        if raw:
            file_until = int(raw)
    except Exception:
        file_until = 0
    if file_until > int(_DISABLE_BINANCE_UNTIL_MS or 0):
        _DISABLE_BINANCE_UNTIL_MS = int(file_until)
    return int(_DISABLE_BINANCE_UNTIL_MS or 0)


def _set_shared_binance_cooldown_until_ms(until_ms: int) -> None:
    global _DISABLE_BINANCE_UNTIL_MS
    try:
        target = int(until_ms)
    except Exception:
        return
    if target <= 0:
        return
    if target > int(_DISABLE_BINANCE_UNTIL_MS or 0):
        _DISABLE_BINANCE_UNTIL_MS = target
    try:
        parent = os.path.dirname(BINANCE_COOLDOWN_FILE) or "/tmp"
        os.makedirs(parent, exist_ok=True)
        tmp = f"{BINANCE_COOLDOWN_FILE}.tmp.{os.getpid()}"
        with open(tmp, "w") as f:
            f.write(str(int(_DISABLE_BINANCE_UNTIL_MS)))
        os.replace(tmp, BINANCE_COOLDOWN_FILE)
    except Exception:
        return


async def _fetch_binance_klines(
    session: aiohttp.ClientSession,
    symbol: str,
    interval: str,
    start_ms: int,
    end_ms: int,
    log: logging.Logger,
) -> Optional[List[dict]]:
    global _DISABLE_BINANCE_UNTIL_MS

    if env_str("EVCLAW_DISABLE_BINANCE", "0") == "1":
        return None
    now_ms = int(time.time() * 1000)
    disable_until_ms = _read_shared_binance_cooldown_until_ms()
    if disable_until_ms and now_ms < disable_until_ms:
        return None

    binance_symbol = _to_binance_symbol(symbol)
    if not binance_symbol:
        return None

    interval_ms = INTERVAL_MS.get(interval)
    if not interval_ms:
        return None

    candles: List[dict] = []
    current_start = start_ms
    safety = 0

    while current_start < end_ms and safety < 20:
        safety += 1
        params = {
            "symbol": binance_symbol,
            "interval": interval,
            "startTime": current_start,
            "endTime": end_ms,
            "limit": BINANCE_MAX_LIMIT,
        }

        data = None
        status = None
        for attempt in range(CANDLE_FETCH_MAX_RETRIES):
            try:
                proxy = _PROXY.next()
                async with session.get(BINANCE_FUTURES_KLINES_URL, params=params, proxy=proxy) as resp:
                    status = resp.status
                    if status == 200:
                        data = await resp.json()
                        break
                    body = await resp.text()
                    if status in (418, 429):
                        # Binance may ban the IP for heavy use; parse ban-until if present.
                        ban_until_ms = None
                        try:
                            j = json.loads(body) if body else {}
                            msg = str(j.get("msg") or "")
                            if "banned until" in msg:
                                ban_until_ms = int(msg.split("banned until", 1)[1].strip().split(" ")[0])
                        except Exception:
                            ban_until_ms = None
                        _set_shared_binance_cooldown_until_ms(
                            ban_until_ms or (int(time.time() * 1000) + 60 * 60 * 1000)
                        )
                        log.warning(
                            f"Binance rate-limited/banned; disabling binance until {_DISABLE_BINANCE_UNTIL_MS}ms"
                        )
                        return None

                    if status in (500, 502, 503, 504):
                        log.warning(
                            f"Binance klines retry {attempt+1}/{CANDLE_FETCH_MAX_RETRIES} {symbol} status={status} body={_short_body(body)}"
                        )
                        await asyncio.sleep(_backoff_sec(attempt))
                        continue

                    log.warning(
                        f"Binance klines failed {symbol} status={status} body={_short_body(body)}"
                    )
                    return None
            except Exception as exc:
                log.warning(f"Binance klines error {symbol} attempt {attempt+1}: {exc}")
                await asyncio.sleep(_backoff_sec(attempt))

        if data is None:
            # Give up this symbol on Binance; caller will try HL fallback.
            return None

        batch = _normalize_binance_klines(data if isinstance(data, list) else [])
        if not batch:
            break
        candles.extend(batch)
        last_open = batch[-1]["t"]
        current_start = last_open + interval_ms
        if len(batch) < BINANCE_MAX_LIMIT:
            break

    return candles


async def _fetch_hl_candles(
    session: aiohttp.ClientSession,
    symbol: str,
    interval: str,
    start_ms: int,
    end_ms: int,
    log: Optional[logging.Logger] = None,
) -> Optional[List[dict]]:
    payload = {
        "type": "candleSnapshot",
        "req": {
            "coin": symbol,
            "interval": interval,
            "startTime": start_ms,
            "endTime": end_ms,
        },
    }

    for attempt in range(CANDLE_FETCH_MAX_RETRIES):
        try:
            proxy = _PROXY.next()
            async with session.post(HL_PUBLIC_API_URL, json=payload, proxy=proxy) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return _normalize_hl_candles(data if isinstance(data, list) else [])
                body = await resp.text()
                if log:
                    log.warning(
                        f"HL candles retry {attempt+1}/{CANDLE_FETCH_MAX_RETRIES} {symbol} status={resp.status} body={_short_body(body)}"
                    )
                if resp.status in (429, 500, 502, 503, 504):
                    await asyncio.sleep(_backoff_sec(attempt))
                    continue
                return None
        except Exception as exc:
            if log:
                log.warning(f"HL candles error {symbol} attempt {attempt+1}: {exc}")
            await asyncio.sleep(_backoff_sec(attempt))

    return None


async def fetch_candles(
    session: aiohttp.ClientSession,
    symbol: str,
    interval: str,
    start_ms: int,
    end_ms: int,
    log: logging.Logger,
) -> Tuple[str, Optional[List[dict]]]:
    """Fetch candles for a symbol.

    Order:
      - HIP3 (xyz:*): Massive aggregates
      - Crypto: Binance Futures primary, HL public fallback

    Returns (source, candles) where source is "massive", "binance", "hyperliquid", or "none".
    """

    # HIP3 symbols: use Massive (stocks OHLC)
    if ":" in (symbol or ""):
        if not MASSIVE_API_KEY:
            return "none", None
        try:
            # We only support 1h in workers today.
            if interval != "1h":
                return "none", None
            ticker = str(symbol).split(":", 1)[1].strip().upper()
            url = f"{MASSIVE_BASE_URL}/v2/aggs/ticker/{ticker}/range/1/hour/{int(start_ms)}/{int(end_ms)}"
            params = {"sort": "asc", "limit": "50000", "adjusted": "true"}
            key = MASSIVE_API_KEY
            auth = key if key.lower().startswith("bearer ") else f"Bearer {key}"
            headers = {"Authorization": auth}
            for attempt in range(CANDLE_FETCH_MAX_RETRIES):
                try:
                    proxy = _PROXY.next()
                    async with session.get(url, params=params, headers=headers, proxy=proxy) as resp:
                        if resp.status == 200:
                            data = await resp.json()
                            candles = _normalize_massive_aggs(data)
                            if candles:
                                return "massive", candles
                            return "none", None
                        if resp.status in (429, 500, 502, 503, 504):
                            await asyncio.sleep(_backoff_sec(attempt))
                            continue
                        body = await resp.text()
                        log.warning(f"Massive aggs failed {symbol} status={resp.status} body={_short_body(body)}")
                        return "none", None
                except Exception as exc:
                    log.warning(f"Massive aggs error {symbol} attempt {attempt+1}: {exc}")
                    await asyncio.sleep(_backoff_sec(attempt))
            return "none", None
        except Exception:
            return "none", None

    candles = await _fetch_binance_klines(session, symbol, interval, start_ms, end_ms, log)
    if candles:
        return "binance", candles

    candles = await _fetch_hl_candles(session, symbol, interval, start_ms, end_ms, log=log)
    if candles:
        return "hyperliquid", candles

    # Avoid log spam on per-symbol failures; summary is logged in fetch_1h_candles_for_symbols.
    return "none", None


def _cache_path(hours_back: int, symbols_hash: str) -> str:
    return os.path.join(CANDLE_CACHE_DIR, f"evclaw_candles_1h_{hours_back}_{symbols_hash}.json")


def _lock_path(hours_back: int, symbols_hash: str) -> str:
    return os.path.join(CANDLE_CACHE_DIR, f"evclaw_candles_1h_{hours_back}_{symbols_hash}.lock")


def _symbols_hash(symbols: List[str]) -> str:
    blob = ",".join(sorted((symbols or [])))
    return hashlib.sha1(blob.encode("utf-8")).hexdigest()[:12]


def _load_cached_1h(hours_back: int, symbols: List[str]) -> Optional[Dict[str, Dict[str, object]]]:
    if CANDLE_CACHE_TTL_SEC <= 0:
        return None
    try:
        h = _symbols_hash(symbols)
        path = _cache_path(hours_back, h)
        if not os.path.exists(path):
            return None
        with open(path, "r") as f:
            payload = json.load(f)
        ts = float(payload.get("ts") or 0)
        if ts <= 0:
            return None

        # If the cached fetch was mostly-empty (rate limit / outage), expire it faster
        # so the system can recover quickly without waiting full TTL.
        n = int(payload.get("n") or 0)
        empty = int(payload.get("empty") or 0)
        bad_ratio = CANDLE_CACHE_BAD_EMPTY_RATIO
        bad_ttl = CANDLE_CACHE_BAD_TTL_SEC
        ttl = CANDLE_CACHE_TTL_SEC
        if n > 0 and (empty / float(n)) >= bad_ratio:
            ttl = min(ttl, bad_ttl)

        if (time.time() - ts) > ttl:
            return None

        results = payload.get("results")
        return results if isinstance(results, dict) else None
    except Exception:
        return None


def _write_cached_1h(hours_back: int, symbols: List[str], results: Dict[str, Dict[str, object]]) -> None:
    if CANDLE_CACHE_TTL_SEC <= 0:
        return
    try:
        os.makedirs(CANDLE_CACHE_DIR, exist_ok=True)
        h = _symbols_hash(symbols)
        path = _cache_path(hours_back, h)
        tmp = f"{path}.tmp"

        counts = {"binance": 0, "hyperliquid": 0, "massive": 0, "none": 0}
        empty = 0
        for _, payload in (results or {}).items():
            src = str(payload.get("source") or "none")
            if src not in counts:
                counts[src] = 0
            counts[src] += 1
            if not payload.get("candles"):
                empty += 1

        with open(tmp, "w") as f:
            json.dump(
                {
                    "ts": time.time(),
                    "hours_back": hours_back,
                    "n": len(results),
                    "empty": empty,
                    "sources": counts,
                    "results": results,
                },
                f,
            )
        os.replace(tmp, path)
        _prune_cache_artifacts()
    except Exception:
        pass


def _prune_cache_artifacts() -> None:
    """Best-effort pruning for stale cache files to prevent /tmp growth."""
    try:
        retention = int(CANDLE_CACHE_RETENTION_SEC or 0)
    except Exception:
        retention = 0
    try:
        max_files = int(CANDLE_CACHE_MAX_FILES or 0)
    except Exception:
        max_files = 0

    if retention <= 0 and max_files <= 0:
        return

    try:
        names = os.listdir(CANDLE_CACHE_DIR)
    except Exception:
        return

    now = time.time()
    managed: List[Tuple[float, str]] = []
    for name in names:
        if not name.startswith("evclaw_candles_1h_"):
            continue
        if not (name.endswith(".json") or name.endswith(".lock")):
            continue
        path = os.path.join(CANDLE_CACHE_DIR, name)
        try:
            mtime = float(os.path.getmtime(path))
        except Exception:
            continue
        managed.append((mtime, path))

    if retention > 0:
        cutoff = now - float(retention)
        for mtime, path in managed:
            if mtime < cutoff:
                try:
                    os.remove(path)
                except Exception:
                    pass

    if max_files > 0:
        try:
            remaining: List[Tuple[float, str]] = []
            for name in os.listdir(CANDLE_CACHE_DIR):
                if not name.startswith("evclaw_candles_1h_"):
                    continue
                if not (name.endswith(".json") or name.endswith(".lock")):
                    continue
                path = os.path.join(CANDLE_CACHE_DIR, name)
                try:
                    remaining.append((float(os.path.getmtime(path)), path))
                except Exception:
                    continue
            if len(remaining) > max_files:
                remaining.sort(key=lambda x: x[0])  # oldest first
                overflow = len(remaining) - max_files
                for _, path in remaining[:overflow]:
                    try:
                        os.remove(path)
                    except Exception:
                        pass
        except Exception:
            return


async def fetch_1h_candles_for_symbols(
    symbols: List[str],
    hours_back: int,
    concurrency: int = 10,
    timeout_sec: int = 10,
) -> Dict[str, Dict[str, object]]:
    """Fetch 1h candles for all symbols (Binance Futures primary).

    Returns: {symbol: {"source": str, "candles": List[dict]}}
    """
    log = _get_logger("candle_fetcher")

    # Cache hit fast-path.
    cached = _load_cached_1h(hours_back, symbols)
    if cached is not None:
        log.info(f"Fetched 1h candles: cache_hit=1 hours_back={hours_back} n={len(cached)}")
        return cached

    # Avoid stampedes: if another worker is already fetching this exact universe/hours_back,
    # wait briefly for cache to appear.
    leader = False
    lock_fd = None
    lock_path = None
    if CANDLE_CACHE_TTL_SEC > 0:
        try:
            h = _symbols_hash(symbols)
            lock_path = _lock_path(hours_back, h)
            os.makedirs(CANDLE_CACHE_DIR, exist_ok=True)
            lock_fd = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            try:
                os.write(lock_fd, str(time.time()).encode("utf-8"))
            finally:
                os.close(lock_fd)
                lock_fd = None
            leader = True
        except FileExistsError:
            # Recover from orphaned lock files (e.g. process crash while holding lock).
            stale_sec = max(10, int(CANDLE_CACHE_LOCK_STALE_SEC or 0))
            try:
                age = time.time() - float(os.path.getmtime(lock_path))
                if age > stale_sec:
                    os.remove(lock_path)
                    lock_fd = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
                    try:
                        os.write(lock_fd, str(time.time()).encode("utf-8"))
                    finally:
                        os.close(lock_fd)
                        lock_fd = None
                    leader = True
            except FileNotFoundError:
                pass
            except Exception:
                pass
            if leader:
                pass
            else:
                # Someone else is fetching. Wait up to 30s for the cache file to populate.
                for _ in range(60):
                    await asyncio.sleep(0.5)
                    cached2 = _load_cached_1h(hours_back, symbols)
                    if cached2 is not None:
                        log.info(f"Fetched 1h candles: cache_hit=1(wait) hours_back={hours_back} n={len(cached2)}")
                        return cached2
        except Exception:
            leader = False

    try:
        end_ms = int(time.time() * 1000)
        start_ms = end_ms - int(hours_back * 3600 * 1000)

        results: Dict[str, Dict[str, object]] = {}
        semaphore = asyncio.Semaphore(concurrency)
        timeout = aiohttp.ClientTimeout(total=timeout_sec)

        async with aiohttp.ClientSession(timeout=timeout) as session:
            async def _worker(sym: str) -> None:
                async with semaphore:
                    source, candles = await fetch_candles(
                        session=session,
                        symbol=sym,
                        interval="1h",
                        start_ms=start_ms,
                        end_ms=end_ms,
                        log=log,
                    )
                    results[sym] = {"source": source, "candles": candles or []}

            tasks = [asyncio.create_task(_worker(sym)) for sym in symbols]
            await asyncio.gather(*tasks)

        # One-line summary to make worker health obvious.
        counts = {"binance": 0, "hyperliquid": 0, "massive": 0, "none": 0}
        empty = 0
        for sym, payload in results.items():
            src = str(payload.get("source") or "none")
            if src not in counts:
                counts[src] = 0
            counts[src] += 1
            if not payload.get("candles"):
                empty += 1
        log.info(
            f"Fetched 1h candles: cache_hit=0 n={len(results)} hours_back={hours_back} "
            f"binance={counts.get('binance',0)} hl={counts.get('hyperliquid',0)} none={counts.get('none',0)} empty={empty}"
        )

        _write_cached_1h(hours_back, symbols, results)
        return results
    finally:
        # Release leader lock.
        if leader and lock_path:
            try:
                os.remove(lock_path)
            except Exception:
                pass


def aggregate_candles(
    candles_1h: List[dict],
    bucket_hours: int,
    include_partial: bool = True,
) -> List[dict]:
    """
    Aggregate 1h candles into larger buckets (e.g., 8h/24h/7d).

    Returns list of dicts with:
    - t (bucket start ms), o, h, l, c, v, count, complete
    """
    if not candles_1h or bucket_hours <= 1:
        return []

    bucket_ms = bucket_hours * 3600 * 1000
    interval_ms = INTERVAL_MS["1h"]

    grouped: Dict[int, List[dict]] = {}
    for c in candles_1h:
        ts = int(c["t"])
        bucket_start = ts - (ts % bucket_ms)
        grouped.setdefault(bucket_start, []).append(c)

    aggregated: List[dict] = []
    for bucket_start in sorted(grouped.keys()):
        group = sorted(grouped[bucket_start], key=lambda x: x["t"])
        if not group:
            continue
        # Check contiguity
        contiguous = True
        for i in range(1, len(group)):
            if group[i]["t"] - group[i - 1]["t"] != interval_ms:
                contiguous = False
                break
        complete = contiguous and len(group) == bucket_hours
        if not include_partial and not complete:
            continue
        aggregated.append({
            "t": bucket_start,
            "o": float(group[0]["o"]),
            "h": max(float(c["h"]) for c in group),
            "l": min(float(c["l"]) for c in group),
            "c": float(group[-1]["c"]),
            "v": sum(float(c["v"]) for c in group),
            "count": len(group),
            "complete": complete,
        })

    return aggregated


def resample_timeframes(candles_1h: List[dict]) -> Dict[str, List[dict]]:
    """
    Convenience helper to build session/daily/weekly buckets from 1h candles.
    """
    return {
        "session_8h": aggregate_candles(candles_1h, 8, include_partial=True),
        "daily_24h": aggregate_candles(candles_1h, 24, include_partial=True),
        "weekly_7d": aggregate_candles(candles_1h, 168, include_partial=True),
    }
