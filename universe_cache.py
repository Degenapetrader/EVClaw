#!/usr/bin/env python3
"""
Cached dual-venue symbol universe builder.

Builds a list of symbols tradable on both Hyperliquid and Lighter, writes
`signals/symbol_universe_dual_venue.json`, and refreshes at most daily.
"""

from __future__ import annotations

import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import aiohttp
import yaml

from env_utils import EVCLAW_SIGNALS_DIR, env_str

from signal_utils import ensure_dir, load_json, write_json_atomic

def _append_private_key_if_needed(url: str) -> str:
    key = str(env_str("HYPERLIQUID_ADDRESS", "") or "").strip()
    if not key:
        return url
    if "node2.evplus.ai" not in url or "/evclaw/info" not in url:
        return url
    parsed = urlsplit(url)
    params = dict(parse_qsl(parsed.query, keep_blank_values=True))
    if params.get("key"):
        return url
    params["key"] = key
    return urlunsplit((parsed.scheme, parsed.netloc, parsed.path, urlencode(params), parsed.fragment))


LOCAL_NODE_URL = _append_private_key_if_needed(
    env_str("HYPERLIQUID_PRIVATE_NODE", "https://node2.evplus.ai/evclaw/info")
)
_hl_public_base = env_str("HYPERLIQUID_PUBLIC_URL", "https://api.hyperliquid.xyz")
HL_PUBLIC_API_URL = _hl_public_base.rstrip("/")
if not HL_PUBLIC_API_URL.endswith("/info"):
    HL_PUBLIC_API_URL = f"{HL_PUBLIC_API_URL}/info"
LIGHTER_BASE_URL = env_str("LIGHTER_BASE_URL", "https://mainnet.zklighter.elliot.ai")
LIGHTER_ORDERBOOK_PATH = "/api/v1/orderBookDetails"

_SKILL_FILE = Path(__file__).with_name("skill.yaml")


def _load_skill_context_builder_cfg() -> Dict[str, object]:
    try:
        with _SKILL_FILE.open("r", encoding="utf-8") as f:
            raw = yaml.safe_load(f) or {}
        if not isinstance(raw, dict):
            return {}
        cfg = raw.get("config") or {}
        if not isinstance(cfg, dict):
            return {}
        context_cfg = cfg.get("context_builder") or {}
        return context_cfg if isinstance(context_cfg, dict) else {}
    except Exception:
        return {}


def _skill_int(cfg: Dict[str, object], key: str, default: int) -> int:
    try:
        value = cfg.get(key, default)
        return int(default if value is None else value)
    except Exception:
        return int(default)


_CONTEXT_BUILDER_CFG = _load_skill_context_builder_cfg()
SIGNALS_DIR = EVCLAW_SIGNALS_DIR
CACHE_FILE = os.path.join(SIGNALS_DIR, "symbol_universe_dual_venue.json")
TTL_DAYS = _skill_int(_CONTEXT_BUILDER_CFG, "universe_cache_ttl_days", 1)


def _cache_is_fresh(path: str, ttl_days: int) -> bool:
    if ttl_days <= 0:
        return False
    try:
        mtime = os.path.getmtime(path)
    except OSError:
        return False
    age_seconds = time.time() - mtime
    return age_seconds < ttl_days * 86400


def _normalize_lighter_symbol(symbol: str) -> Optional[str]:
    if not symbol:
        return None
    sym = str(symbol).strip()
    if not sym:
        return None
    sym = sym.upper()
    if sym.endswith("-PERP"):
        sym = sym[:-5]
    return sym or None


def _normalize_hl_symbol(symbol: str) -> Optional[str]:
    if not symbol:
        return None
    sym = str(symbol).strip()
    if not sym:
        return None
    return sym.upper()


def _dedupe_hl_symbols(symbols: List[str]) -> Dict[str, str]:
    """Map normalized symbol -> original symbol (preserve HL casing)."""
    mapping: Dict[str, str] = {}
    for sym in symbols:
        if not sym:
            continue
        if ":" in sym:
            continue
        normalized = _normalize_hl_symbol(sym)
        if not normalized:
            continue
        if ":" in normalized:
            continue
        if normalized not in mapping:
            mapping[normalized] = sym
    return mapping


def _dedupe_lighter_symbols(symbols: List[str]) -> List[str]:
    cleaned: List[str] = []
    seen = set()
    for sym in symbols:
        normalized = _normalize_lighter_symbol(sym)
        if not normalized:
            continue
        if ":" in normalized:
            continue
        if normalized in seen:
            continue
        seen.add(normalized)
        cleaned.append(normalized)
    return cleaned


def _load_cached_symbols(path: str, ttl_days: int) -> Optional[List[str]]:
    if not _cache_is_fresh(path, ttl_days):
        return None
    data = load_json(path)
    if not isinstance(data, dict):
        return None
    symbols = data.get("dual_symbols")
    if not isinstance(symbols, list):
        return None
    return [s for s in symbols if isinstance(s, str)]


def load_dual_venue_symbols_cached(
    cache_path: str = CACHE_FILE,
    ttl_days: int = TTL_DAYS,
) -> List[str]:
    """Synchronous (no-event-loop) dual-venue universe load.

    IMPORTANT:
    - This only reads the on-disk cache.
    - It does *not* refresh from network.

    This is safe to call from code that might already be running inside an
    asyncio event loop (where asyncio.run() would crash).
    """
    cached = _load_cached_symbols(cache_path, ttl_days)
    return cached or []


async def fetch_hl_symbols(session: aiohttp.ClientSession) -> List[str]:
    payload = {"type": "meta"}
    for url in (LOCAL_NODE_URL, HL_PUBLIC_API_URL):
        try:
            async with session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status != 200:
                    continue
                data = await resp.json()
                universe = data.get("universe", [])
                symbols = [u.get("name") for u in universe if u.get("name")]
                return [s for s in symbols if ":" not in s]
        except Exception:
            continue
    return []


async def fetch_lighter_symbols(
    session: aiohttp.ClientSession,
    base_url: Optional[str] = None,
) -> List[str]:
    base = (base_url or LIGHTER_BASE_URL).rstrip("/")
    url = f"{base}{LIGHTER_ORDERBOOK_PATH}"
    params = {"filter": "perp"}

    try:
        async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=10)) as resp:
            if resp.status != 200:
                return []
            data = await resp.json()
    except Exception:
        return []

    if not isinstance(data, dict):
        return []

    details = data.get("order_book_details")
    if details is None:
        details = data.get("orderBookDetails")
    if details is None:
        details = []

    symbols: List[str] = []
    for item in details if isinstance(details, list) else []:
        if not isinstance(item, dict):
            continue
        symbol = (
            item.get("symbol")
            or item.get("market_symbol")
            or item.get("marketSymbol")
            or item.get("name")
        )
        if symbol:
            symbols.append(str(symbol))
    return symbols


def _build_dual_symbols(hl_symbols: List[str], lighter_symbols: List[str]) -> Tuple[int, int, List[str]]:
    hl_map = _dedupe_hl_symbols(hl_symbols)
    lighter_clean = _dedupe_lighter_symbols(lighter_symbols)
    lighter_set = set(lighter_clean)

    dual_norm = sorted(sym for sym in hl_map if sym in lighter_set)
    dual_symbols = [hl_map[sym] for sym in dual_norm]

    return len(hl_map), len(lighter_set), dual_symbols


async def load_dual_venue_symbols(
    session: Optional[aiohttp.ClientSession] = None,
    cache_path: str = CACHE_FILE,
    ttl_days: int = TTL_DAYS,
    base_url: Optional[str] = None,
    force_refresh: bool = False,
) -> List[str]:
    """Load cached dual-venue symbols or build + cache if stale."""
    if not force_refresh:
        cached = _load_cached_symbols(cache_path, ttl_days)
        if cached is not None:
            return cached

    owns_session = session is None
    if session is None:
        session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10))

    try:
        hl_symbols = await fetch_hl_symbols(session)
        lighter_symbols = await fetch_lighter_symbols(session, base_url=base_url)
    finally:
        if owns_session:
            await session.close()

    if not hl_symbols or not lighter_symbols:
        raise RuntimeError("Failed to build dual-venue universe (missing HL or Lighter symbols)")

    hl_count, lighter_count, dual_symbols = _build_dual_symbols(hl_symbols, lighter_symbols)

    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "ttl_days": ttl_days,
        "hl_symbols_count": hl_count,
        "lighter_symbols_count": lighter_count,
        "dual_symbols": dual_symbols,
    }

    ensure_dir(os.path.dirname(cache_path) or ".")
    write_json_atomic(cache_path, payload)

    return dual_symbols


__all__ = [
    "CACHE_FILE",
    "TTL_DAYS",
    "load_dual_venue_symbols_cached",
    "fetch_hl_symbols",
    "fetch_lighter_symbols",
    "load_dual_venue_symbols",
]
