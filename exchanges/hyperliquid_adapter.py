#!/usr/bin/env python3
"""Hyperliquid exchange adapter — raw signing + direct HTTP POST.

Bypasses the broken SDK Exchange.order() (422 errors in v0.21.0) by using
the same pattern as the predator bot: eth_account wallet + signing utils +
aiohttp POST to /exchange.

SDK Info is still used for metadata (coin_to_asset, sz_decimals).
"""

from __future__ import annotations

import asyncio
import json
import logging
import math
import os
import time
import urllib.request
from decimal import Decimal, ROUND_DOWN
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import aiohttp
import requests
from eth_account import Account

from env_utils import (
    EVCLAW_TRACKER_HIP3_SYMBOLS_URL,
    env_int,
    env_str,
)

from hyperliquid.info import Info as HLInfo
from hyperliquid.exchange import Exchange
from hyperliquid.utils.signing import (
    get_timestamp_ms,
    order_request_to_order_wire,
    order_wires_to_order_action,
    sign_l1_action,
    OrderRequest,
    OrderType,
)

from .base import ExchangeAdapter, Position

# Max retries on 429 before giving up
MAX_PUBLIC_RETRIES = 3
MAX_EXCHANGE_RETRIES = 3
RETRY_EXCHANGE_ON_429 = True
ORDER_STATUS_FILLS_LIMIT = env_int(
    "EVCLAW_HL_ORDER_STATUS_FILLS_LIMIT",
    200,
)
PUBLIC_CIRCUIT_BREAKER_ENABLED = True
PUBLIC_CIRCUIT_BREAKER_FAILS = 8
PUBLIC_CIRCUIT_BREAKER_COOLDOWN_SEC = 30

# Private node supports only a read-only subset of /info.
# Keep this whitelist explicit to prevent accidental routing of unsupported types.
PRIVATE_INFO_TYPES = frozenset(
    {
        "meta",
        "spotmeta",
        "spotclearinghousestate",
        "clearinghousestate",
        "openorders",
        "frontendopenorders",
        "userratelimit",
    }
)

# Builder fee (legacy address alignment from older HL integration)
BUILDER_ADDRESS = "0xc1a2f762f67af72fd05e79afa23f8358a4d7dbaf"
BUILDER_FEE_INT = 20  # 2 bps in tenths of basis points


class HyperliquidAdapter(ExchangeAdapter):
    """Adapter for Hyperliquid exchange using raw signing + direct HTTP."""

    BUILDER_DEXES = {"xyz", "km", "flx", "cash", "hyna", "vntl", "abcd"}

    async def get_best_bid_ask(self, symbol: str) -> Tuple[float, float]:
        """Fetch best bid/ask via /info l2Book, falling back to allMids for builder symbols.

        Used primarily for post-only (Alo) orders to avoid `badAloPxRejected`.
        Returns (bid, ask), 0.0 when unavailable.
        """
        if self.dry_run:
            return 0.0, 0.0

        try:
            payload = {"type": "l2Book", "coin": self._l2_coin(symbol)}
            book = await self._post_public(payload)
            if isinstance(book, dict):
                levels = book.get("levels")
                if isinstance(levels, list) and len(levels) >= 2:
                    bids = levels[0] if isinstance(levels[0], list) else []
                    asks = levels[1] if isinstance(levels[1], list) else []
                    best_bid = float((bids[0] or {}).get("px", 0) or 0) if bids else 0.0
                    best_ask = float((asks[0] or {}).get("px", 0) or 0) if asks else 0.0
                    if best_bid > 0 and best_ask > 0:
                        return best_bid, best_ask
        except Exception:
            pass

        # Fallback: allMids (essential for builder/AMM symbols with no orderbook).
        # Builder allMids keys are prefixed: "xyz:SOL", "cash:SILVER", etc.
        try:
            coin_key = self._coin_key(symbol)  # e.g. "CASH:SILVER"
            dex = ""
            if ":" in coin_key:
                dex = coin_key.split(":", 1)[0].lower()
            mids_payload = {"type": "allMids"}
            if dex:
                mids_payload["dex"] = dex
            # Builder allMids not supported on private node; post directly to mainnet.
            mainnet_url = "https://api.hyperliquid.xyz/info"
            async with self._session.post(mainnet_url, json=mids_payload) as resp:
                mids = await resp.json() if resp.status == 200 else {}
            if isinstance(mids, dict):
                # Keys from HL are lowercase-prefix: "cash:SILVER", "xyz:PLTR"
                # Our coin_key is uppercase: "CASH:SILVER", "XYZ:PLTR"
                lookup_key = f"{dex}:{coin_key.split(':', 1)[1]}" if ":" in coin_key else coin_key
                mid = float(mids.get(lookup_key, 0) or 0)
                if mid <= 0:
                    mid = float(mids.get(coin_key, 0) or 0)
                if mid <= 0:
                    bare = coin_key.split(":", 1)[1] if ":" in coin_key else coin_key
                    mid = float(mids.get(bare, 0) or 0)
                if mid > 0:
                    spread = mid * 0.001  # 0.1% synthetic spread for AMM
                    return mid - spread, mid + spread
        except Exception:
            pass

        return 0.0, 0.0


    # ------------------------------------------------------------------ init
    def __init__(self, log: logging.Logger, dry_run: bool = False, account_mode: str = "wallet"):
        super().__init__(log)
        self.dry_run = dry_run
        normalized_mode = (account_mode or "wallet").strip().lower()
        if normalized_mode != "wallet":
            self.log.warning(
                f"Unsupported account_mode={normalized_mode!r}; forcing unified wallet mode"
            )
        self.account_mode = "wallet"

        # Last error string from order placement (best-effort; for executor retries)
        self.last_order_error: Optional[str] = None

        # Wallet / addresses
        self.wallet = None                         # eth_account LocalAccount
        self._address: Optional[str] = None        # HYPERLIQUID_ADDRESS
        # HIP3 wallet override (xyz: symbols only)
        self._hip3_wallet = None                   # eth_account LocalAccount
        self._hip3_address: Optional[str] = None   # HIP3 wallet address (same as _address in unified mode)
        self._wallet_fallback = None               # Optional fallback signer (typically HYPERLIQUID_AGENT_PRIVATE_KEY)

        # SDK Info — metadata only (no Exchange class!)
        self._info: Optional[HLInfo] = None

        # aiohttp session for raw HTTP POST
        self._session: Optional[aiohttp.ClientSession] = None
        # IMPORTANT:
        # - /exchange MUST always hit public API (private node returns 404).
        # - private node can be used only for a small read-only /info subset.
        self._base_url: str = "https://api.hyperliquid.xyz"
        self._private_info_base_url: Optional[str] = None
        self._private_info_key: Optional[str] = None

        # Proxy rotation for public API rate limits
        self._proxies: List[str] = []
        self._proxy_idx: int = 0
        self._public_cb_failures: int = 0
        self._public_cb_open_until: float = 0.0

        # Asset metadata caches (populated during initialize)
        self._asset_map: Dict[str, int] = {}   # symbol -> asset_id
        self._sz_decimals: Dict[str, int] = {} # symbol -> sz_decimals (keyed by name)
        self._sz_decimals_by_id: Dict[int, int] = {} # asset_id -> sz_decimals
        self._tick_size_cache: Dict[str, float] = {}  # symbol coin_key -> tick_size

        # Metadata refresh throttling (HIP3 listings can change; cache may go stale)
        self._last_meta_refresh_ts: float = 0.0

    # ------------------------------------------------------------------ name
    @property
    def name(self) -> str:
        return "Hyperliquid"

    # ============================================================ helpers
    @staticmethod
    def _normalize_symbol(symbol: str) -> str:
        if ':' in symbol:
            return symbol.split(':', 1)[1]
        return symbol

    @staticmethod
    def _normalize_base_url(base_url: str) -> str:
        base_url = base_url.strip()
        if base_url.endswith('/info'):
            base_url = base_url[:-5]
        return base_url.rstrip('/')

    @staticmethod
    def _can_use_private_info(payload: dict) -> bool:
        """Return True when payload type is known to work on local private /info."""
        try:
            req_type = str((payload or {}).get("type") or "").strip().lower()
        except Exception:
            req_type = ""
        return req_type in PRIVATE_INFO_TYPES

    @staticmethod
    def _coin_key(symbol: str) -> str:
        """Return canonical coin key for perps/builder symbols.

        Builder symbols use `DEX:BASE` (all uppercase) to match DB convention.
        This method canonicalizes case-insensitively for all known builder dexes.

        NOTE: This remains a staticmethod because some callers reference it via the class.
        """
        s = str(symbol or "").strip()
        if ":" in s:
            prefix, rest = s.split(":", 1)
            p = prefix.strip().lower()
            if p in HyperliquidAdapter.BUILDER_DEXES:
                return f"{p.upper()}:{rest.strip().upper()}"
        return s

    @staticmethod
    def _l2_coin(symbol: str) -> str:
        """Return coin key for /info l2Book requests.

        Builder l2Book is case-sensitive on dex prefix in practice, so send
        lowercase `dex:BASE` (for example `xyz:PLTR`, `cash:SILVER`).
        """
        coin = HyperliquidAdapter._coin_key(symbol)
        if ":" not in coin:
            return coin
        prefix, rest = coin.split(":", 1)
        if prefix.strip().lower() in HyperliquidAdapter.BUILDER_DEXES:
            return f"{prefix.strip().lower()}:{rest.strip().upper()}"
        return coin

    # --------------------------------------------------- HIP3 / builder account routing
    @staticmethod
    def _is_hip3(symbol: str) -> bool:
        """Check if symbol is builder-dex scoped (dex:*), case-insensitive prefix."""
        s = str(symbol or "").strip()
        if ":" not in s:
            return False
        prefix = s.split(":", 1)[0].strip().lower()
        return prefix in HyperliquidAdapter.BUILDER_DEXES

    @staticmethod
    def _is_unknown_api_wallet_error(err_txt: str) -> bool:
        low = str(err_txt or "").lower()
        return ("api wallet" in low and "does not exist" in low) or ("user or api wallet" in low and "does not exist" in low)

    def _wallet_for_symbol(self, symbol: str):
        """Select signing wallet for a symbol.

        Unified routing keeps one signer for all symbols.
        """
        if self.wallet is None:
            raise RuntimeError("Hyperliquid wallet not initialized")
        return self.wallet

    def _user_address_for_symbol(self, symbol: str) -> str:
        """Get user address for queries."""
        return self._address or ""

    def _signing_subaccount_for_symbol(self, symbol: str) -> Optional[str]:
        """Return signing subaccount override (unused in unified wallet mode)."""
        return None

    def _hip3_coin_set(self) -> set:
        """Return known builder-dex base tickers (uppercase, prefix stripped)."""
        hip3 = set()
        try:
            for key in self._asset_map.keys():
                s = str(key)
                if ":" not in s:
                    continue
                pfx = s.split(":", 1)[0].strip().lower()
                if pfx in self.BUILDER_DEXES:
                    hip3.add(self._normalize_symbol(s).upper())
        except Exception:
            return set()
        return hip3

    def _builder_dexes(self) -> List[str]:
        """Best-effort known builder dex prefixes from metadata cache."""
        found: set[str] = set()
        try:
            for key in self._asset_map.keys():
                s = str(key)
                if ":" not in s:
                    continue
                pfx = s.split(":", 1)[0].strip().lower()
                if pfx in self.BUILDER_DEXES:
                    found.add(pfx)
        except Exception:
            pass
        if not found:
            found = set(self.BUILDER_DEXES)
        return sorted(found)

    @classmethod
    def _builder_dex_offsets(cls, dex_list: Any) -> Dict[str, int]:
        """Compute builder asset-id offsets from raw perpDexs payload.

        Hyperliquid commonly returns a sentinel entry first (name=None), then
        builder dexes. Runtime asset IDs resolve with xyz at 110000, flx at
        120000, etc. We therefore keep the raw index (including sentinel when
        present) and map as:
            offset = 100000 + effective_index * 10000
        """
        if not isinstance(dex_list, list) or not dex_list:
            return {}

        first = dex_list[0]
        first_name = ""
        if isinstance(first, dict):
            first_name = str(first.get("name") or "").strip().lower()
        sentinel_present = (not isinstance(first, dict)) or (not first_name)
        shift = 0 if sentinel_present else 1

        offsets: Dict[str, int] = {}
        for raw_idx, item in enumerate(dex_list):
            if not isinstance(item, dict):
                continue
            dex = str(item.get("name") or "").strip().lower()
            if dex in cls.BUILDER_DEXES:
                offsets[dex] = int(100000 + (raw_idx + shift) * 10000)
        return offsets

    # --------------------------------------------------- rounding helpers
    def _get_sz_decimals(self, symbol: str) -> int:
        """Get size decimals for *symbol* (coin name like 'HYPE' or 'xyz:NVDA')."""
        sym_key = self._coin_key(symbol)

        # Try full symbol first (HIP3: xyz:NVDA)
        if sym_key in self._sz_decimals:
            return self._sz_decimals[sym_key]

        norm = self._normalize_symbol(sym_key).upper()
        if norm in self._sz_decimals:
            return self._sz_decimals[norm]

        # Try asset_id lookup via full symbol, then normalized
        for key in (sym_key, norm):
            aid = self._asset_map.get(key)
            if aid is not None and aid in self._sz_decimals_by_id:
                return self._sz_decimals_by_id[aid]
        return 2  # safe default

    def _round_size(self, size: float, symbol: str) -> float:
        """Round order size DOWN to exchange precision (never up-round)."""
        decimals = max(0, int(self._get_sz_decimals(symbol)))
        try:
            q = Decimal("1").scaleb(-decimals)  # 10^-decimals
            d = Decimal(str(size)).quantize(q, rounding=ROUND_DOWN)
            return float(d)
        except Exception:
            return round(float(size), decimals)

    def _round_price(self, price: float, symbol: str) -> float:
        coin_key = self._coin_key(symbol)
        tick = float(self._tick_size_cache.get(coin_key) or 0.0)
        if tick > 0:
            try:
                return round(round(float(price) / tick) * tick, 10)
            except Exception:
                pass
        sz_decimals = self._get_sz_decimals(symbol)
        px_decimals = 6 - sz_decimals
        if self._is_hip3(symbol):
            # Builder-dex tick: 5 significant figures determines the actual precision.
            # Don't pad with extra decimals beyond what 5-sig-fig produces.
            # e.g. $664.787 → 5g → "664.79" (2 dec), $78.395 → 5g → "78.395" (3 dec)
            sig5 = float(f"{price:.5g}")
            sig5_str = f"{sig5:g}"
            if "." in sig5_str:
                actual_dec = len(sig5_str.split(".")[1])
            else:
                actual_dec = 0
            px_decimals = min(px_decimals, actual_dec)
        return round(float(f"{price:.5g}"), px_decimals)

    # --------------------------------------------------- asset helpers
    def _refresh_metadata_best_effort(self) -> None:
        """Best-effort refresh of HL meta/spotMeta and rebuild local mappings.

        Builder-dex listings can change intra-day. We cache metadata on disk,
        but if a symbol is missing we refresh once (throttled) and rebuild
        perps + builder asset mappings without instantiating SDK Info.

        Never raises.
        """
        try:
            now = time.time()
            if (now - float(getattr(self, "_last_meta_refresh_ts", 0.0) or 0.0)) < 60.0:
                return
            self._last_meta_refresh_ts = now

            info_url = self._base_url

            def _post(payload: dict) -> Optional[Any]:
                try:
                    r = requests.post(f"{info_url}/info", json=payload, timeout=5)
                    if r.status_code != 200:
                        return None
                    return r.json()
                except Exception:
                    return None

            meta = _post({"type": "meta"})
            spot_meta = _post({"type": "spotMeta"})
            if not isinstance(meta, dict) or not isinstance(spot_meta, dict):
                return

            dex_list = _post({"type": "perpDexs"})
            dex_names: List[str] = []
            if isinstance(dex_list, list):
                dex_names = [str(x.get("name") or "").strip().lower() for x in dex_list if isinstance(x, dict)]

            # Builder dexes known by chain + strategy constraints.
            builders: List[str] = []
            if dex_names:
                builders = [d for d in dex_names[1:] if d in self.BUILDER_DEXES]
            if not builders:
                builders = ["xyz"]

            meta_by_dex: Dict[str, Dict[str, Any]] = {}
            for dex in builders:
                fetched = _post({"type": "meta", "dex": dex})
                if isinstance(fetched, dict):
                    meta_by_dex[dex] = fetched

            # Persist caches so next init can avoid network.
            try:
                cache_dir = Path(__file__).resolve().parents[1] / "memory" / "cache"
                cache_dir.mkdir(parents=True, exist_ok=True)
                (cache_dir / "hyperliquid_meta.json").write_text(json.dumps(meta))
                (cache_dir / "hyperliquid_spot_meta.json").write_text(json.dumps(spot_meta))
                if isinstance(dex_list, list):
                    (cache_dir / "hyperliquid_perp_dexs.json").write_text(json.dumps(dex_list))
                for dex, payload in meta_by_dex.items():
                    (cache_dir / f"hyperliquid_meta_{dex}.json").write_text(json.dumps(payload))
                    if dex == "xyz":
                        (cache_dir / "hyperliquid_meta_xyz.json").write_text(json.dumps(payload))
            except Exception:
                pass

            self._info = None
            self._asset_map = {}
            self._sz_decimals = {}
            self._sz_decimals_by_id = {}

            # Base perps (no dex prefix)
            try:
                for asset_id, item in enumerate((meta or {}).get("universe", []) or []):
                    if not isinstance(item, dict):
                        continue
                    name = str(item.get("name") or "")
                    if not name:
                        continue
                    self._asset_map[name] = asset_id
                    if ":" in name:
                        self._asset_map[self._coin_key(name)] = asset_id
                    else:
                        self._asset_map[name.upper()] = asset_id
                    sz_dec = item.get("szDecimals", 2)
                    if sz_dec is None:
                        sz_dec = 2
                    self._sz_decimals[name.upper()] = int(sz_dec)
                    self._sz_decimals_by_id[int(asset_id)] = int(sz_dec)
            except Exception:
                pass

            # Builder dex offsets derived from raw perpDexs list (including sentinel).
            dex_offsets: Dict[str, int] = {}
            try:
                dex_offsets = self._builder_dex_offsets(dex_list)
            except Exception:
                dex_offsets = {}

            # Fallback deterministic offsets if perpDexs unavailable.
            if not dex_offsets:
                for i, dex in enumerate(sorted(meta_by_dex.keys())):
                    dex_offsets[dex] = int(110000 + i * 10000)

            for dex, md in meta_by_dex.items():
                offset = int(dex_offsets.get(dex, 110000))
                for j, item in enumerate((md or {}).get("universe", []) or []):
                    if not isinstance(item, dict):
                        continue
                    raw_name = str(item.get("name") or "").strip()
                    if not raw_name:
                        continue
                    name = raw_name if ":" in raw_name else f"{dex}:{raw_name}"
                    coin = self._coin_key(name)
                    asset_id = int(offset + j)
                    self._asset_map[coin] = asset_id
                    sz_dec = item.get("szDecimals", 2)
                    if sz_dec is None:
                        sz_dec = 2
                    self._sz_decimals[coin] = int(sz_dec)
                    self._sz_decimals_by_id[asset_id] = int(sz_dec)
        except Exception:
            return

    def _get_asset_id(self, symbol: str) -> int:
        """Look up asset_id.

        IMPORTANT: HIP3 tickers use the `xyz:` prefix (lowercase) in HL metadata.
        Accept both `xyz:NVDA` and `XYZ:NVDA` inputs.
        """
        sym_key = self._coin_key(symbol)
        is_hip3 = self._is_hip3(sym_key)

        def _is_valid_builder_asset(a: int) -> bool:
            # Builder assets are expected in the 100000+ range.
            # Guard against stale/legacy low offsets (e.g. 20015) that trigger
            # "Asset ... is out of range" rejects.
            return (not is_hip3) or int(a) >= 100000

        # Try full symbol first (HIP3 keys like 'xyz:NVDA')
        aid = self._asset_map.get(sym_key)
        if aid is not None and _is_valid_builder_asset(int(aid)):
            return int(aid)

        # Try normalized (strip prefix, uppercase)
        norm = self._normalize_symbol(sym_key).upper()
        aid = self._asset_map.get(norm)
        if aid is not None and _is_valid_builder_asset(int(aid)):
            return int(aid)

        # Refresh from Info cache and retry
        if self._info:
            self._asset_map = dict(self._info.coin_to_asset)
            for k, v in list(self._asset_map.items()):
                if ':' not in k:
                    self._asset_map[k.upper()] = v
            aid = self._asset_map.get(sym_key) or self._asset_map.get(norm)
            if aid is not None and _is_valid_builder_asset(int(aid)):
                return int(aid)

        # If this is HIP3, do a best-effort metadata refresh for stale mapping.
        if is_hip3:
            self._refresh_metadata_best_effort()
            aid = self._asset_map.get(sym_key) or self._asset_map.get(norm)
            if aid is not None and _is_valid_builder_asset(int(aid)):
                return int(aid)

            if aid is not None:
                raise ValueError(f"Stale builder asset id for {symbol}: {aid}")

        raise ValueError(f"Unknown symbol: {symbol}")

    # ============================================================ initialize
    async def initialize(self) -> bool:
        # Track last init failure for higher-level tooling (guardian/live_agent)
        self._last_init_error = None

        if self.dry_run:
            self._initialized = True
            self.log.info("Hyperliquid adapter initialized (dry run)")
            return True

        address = os.getenv("HYPERLIQUID_ADDRESS", "").strip()
        agent_signer_key = os.getenv("HYPERLIQUID_AGENT_PRIVATE_KEY", "").strip()
        legacy_private_key = os.getenv("HYPERLIQUID_API", "").strip()
        private_node = os.getenv("HYPERLIQUID_PRIVATE_NODE", "https://node2.evplus.ai/evclaw/info").strip()
        self._private_info_base_url = (
            self._normalize_base_url(private_node) if private_node else None
        )
        self._private_info_key = address or None

        # Load proxies for public API rate limit rotation
        proxy_str = os.getenv("HYPERLIQUID_PROXIES", "").strip()
        if proxy_str:
            self._proxies = [p.strip() for p in proxy_str.split(",") if p.strip()]

        if legacy_private_key:
            self._last_init_error = "legacy_env_name"
            self.log.error(
                "Unsupported legacy env var HYPERLIQUID_API detected. "
                "Use HYPERLIQUID_AGENT_PRIVATE_KEY (delegated agent signer key for HYPERLIQUID_ADDRESS)."
            )
            return False

        if not address or not agent_signer_key:
            self._last_init_error = "missing_env"
            self.log.error(
                "Hyperliquid env missing: HYPERLIQUID_ADDRESS / HYPERLIQUID_AGENT_PRIVATE_KEY. "
                "Set main wallet address + delegated agent signer key (not main wallet private key)."
            )
            return False

        try:
            # 1. Wallet from delegated agent signer key
            self.wallet = Account.from_key(agent_signer_key)
            self._address = address
            # Unified wallet model: same signer/address for all venue flows.
            self._hip3_wallet = self.wallet
            self._hip3_address = self._address
            self._wallet_fallback = None

            # 2. Pre-fetch meta/spotMeta with caching + proxy rotation (avoids 429 at init)
            cache_dir = Path(__file__).resolve().parents[1] / "memory" / "cache"
            cache_dir.mkdir(parents=True, exist_ok=True)
            meta_cache_path = cache_dir / "hyperliquid_meta.json"
            spot_cache_path = cache_dir / "hyperliquid_spot_meta.json"
            perp_dexs_cache_path = cache_dir / "hyperliquid_perp_dexs.json"

            def _load_json(path: Path):
                try:
                    if path.exists():
                        return json.loads(path.read_text())
                except Exception:
                    return None
                return None

            def _save_json(path: Path, data: Any) -> None:
                try:
                    path.write_text(json.dumps(data))
                except Exception:
                    pass

            meta = _load_json(meta_cache_path)
            spot_meta = _load_json(spot_cache_path)
            dex_list = _load_json(perp_dexs_cache_path)

            # Backward compatibility: old cache used only hyperliquid_meta_xyz.json.
            legacy_xyz = _load_json(cache_dir / "hyperliquid_meta_xyz.json")
            meta_by_dex: Dict[str, Dict[str, Any]] = {}
            if isinstance(legacy_xyz, dict):
                meta_by_dex["xyz"] = legacy_xyz

            for dex in sorted(self.BUILDER_DEXES):
                cached = _load_json(cache_dir / f"hyperliquid_meta_{dex}.json")
                if isinstance(cached, dict):
                    meta_by_dex[dex] = cached

            info_url = self._normalize_base_url(private_node) if private_node else self._base_url

            def _post_info(base: str, payload: dict, use_proxies: bool) -> Optional[dict]:
                for attempt in range(MAX_PUBLIC_RETRIES):
                    proxy = self._next_proxy() if use_proxies else None
                    proxies = {"http": proxy, "https": proxy} if proxy else None
                    params = None
                    try:
                        normalized = self._normalize_base_url(base)
                        if (
                            self._private_info_base_url
                            and normalized == self._private_info_base_url
                            and self._private_info_key
                        ):
                            params = {"key": self._private_info_key}
                    except Exception:
                        params = None
                    try:
                        r = requests.post(
                            f"{base}/info",
                            json=payload,
                            timeout=5,
                            proxies=proxies,
                            params=params,
                        )
                    except Exception as exc:
                        self.log.warning(
                            f"Could not pre-fetch {payload.get('type')} from {base}: {exc}"
                        )
                        return None

                    if r.status_code == 200:
                        try:
                            return r.json()
                        except Exception:
                            return None

                    if r.status_code == 429:
                        self.log.warning(
                            f"429 rate limit on /info {payload.get('type')} (attempt {attempt + 1}/{MAX_PUBLIC_RETRIES})"
                        )
                        time.sleep(0.5 * (attempt + 1))
                        continue

                    self.log.warning(
                        f"/info {payload.get('type')} non-200 status={r.status_code} from {base}"
                    )
                    return None

                return None

            dex_names: List[str] = []

            # Refresh from private node first (no proxies), then public with proxies.
            for base, use_proxies in [(info_url, False), (self._base_url, True)]:
                if meta is None:
                    fetched = _post_info(base, {"type": "meta"}, use_proxies)
                    if fetched is not None:
                        meta = fetched
                        _save_json(meta_cache_path, meta)

                if spot_meta is None:
                    fetched = _post_info(base, {"type": "spotMeta"}, use_proxies)
                    if fetched is not None:
                        spot_meta = fetched
                        _save_json(spot_cache_path, spot_meta)

                if dex_list is None:
                    fetched = _post_info(base, {"type": "perpDexs"}, use_proxies)
                    if fetched is not None:
                        dex_list = fetched
                        _save_json(perp_dexs_cache_path, dex_list)

                if isinstance(dex_list, list):
                    dex_names = [
                        str(x.get("name") or "").strip().lower()
                        for x in dex_list
                        if isinstance(x, dict)
                    ]

                target_builders: List[str]
                if dex_names:
                    target_builders = [d for d in dex_names[1:] if d in self.BUILDER_DEXES]
                else:
                    target_builders = sorted(self.BUILDER_DEXES)

                for dex in target_builders:
                    if dex in meta_by_dex:
                        continue
                    fetched = _post_info(base, {"type": "meta", "dex": dex}, use_proxies)
                    if isinstance(fetched, dict):
                        meta_by_dex[dex] = fetched
                        _save_json(cache_dir / f"hyperliquid_meta_{dex}.json", fetched)
                        if dex == "xyz":
                            _save_json(cache_dir / "hyperliquid_meta_xyz.json", fetched)

                if meta is not None and spot_meta is not None and bool(meta_by_dex):
                    break

            if meta is None or spot_meta is None or not meta_by_dex:
                self._last_init_error = "meta_fetch_failed_or_rate_limited"
                self.log.error(
                    "Failed to fetch Hyperliquid meta/spotMeta/builder meta (rate limited?) and no cache available"
                )
                return False

            # 3. Metadata for asset ids + size decimals.
            #
            # IMPORTANT: Do NOT construct HLInfo during init.
            # The upstream SDK performs additional /info calls (not proxy-rotated) and can 429 this VPS.
            # We rely entirely on cached meta + our own proxy-rotated HTTP calls.
            self._info = None

            # Build asset map + sz_decimals caches (perps + builder dexes)
            self._asset_map = {}
            self._sz_decimals = {}
            self._sz_decimals_by_id = {}

            # Base perps: asset_id = enumerate(meta['universe'])
            try:
                for asset_id, item in enumerate((meta or {}).get('universe', []) or []):
                    if not isinstance(item, dict):
                        continue
                    name = str(item.get('name') or '')
                    if not name:
                        continue
                    self._asset_map[name] = asset_id
                    if ':' in name:
                        self._asset_map[self._coin_key(name)] = asset_id
                    else:
                        self._asset_map[name.upper()] = asset_id
                    sz_dec = item.get('szDecimals', 2)
                    if sz_dec is None:
                        sz_dec = 2
                    self._sz_decimals[name.upper()] = int(sz_dec)
                    self._sz_decimals_by_id[int(asset_id)] = int(sz_dec)
            except Exception:
                pass

            # Builder dex assets: asset_id = offset(dex) + j
            dex_offsets: Dict[str, int] = {}
            if dex_list:
                try:
                    dex_offsets = self._builder_dex_offsets(dex_list)
                except Exception:
                    dex_offsets = {}
            if not dex_offsets:
                for i, dex in enumerate(sorted(meta_by_dex.keys())):
                    dex_offsets[dex] = int(110000 + i * 10000)

            try:
                for dex, md in meta_by_dex.items():
                    offset = int(dex_offsets.get(dex, 110000))
                    for j, item in enumerate((md or {}).get('universe', []) or []):
                        if not isinstance(item, dict):
                            continue
                        raw_name = str(item.get('name') or '').strip()
                        if not raw_name:
                            continue
                        name = raw_name if ':' in raw_name else f"{dex}:{raw_name}"
                        coin = self._coin_key(name)
                        asset_id = int(offset + j)
                        self._asset_map[coin] = asset_id
                        sz_dec = item.get('szDecimals', 2)
                        if sz_dec is None:
                            sz_dec = 2
                        self._sz_decimals[coin] = int(sz_dec)
                        self._sz_decimals_by_id[asset_id] = int(sz_dec)
            except Exception:
                pass

            # 4. aiohttp session for raw HTTP POST
            connector = aiohttp.TCPConnector(limit=20, keepalive_timeout=30)
            timeout = aiohttp.ClientTimeout(total=30)
            self._session = aiohttp.ClientSession(
                headers={"Content-Type": "application/json"},
                connector=connector,
                timeout=timeout,
            )

            self._initialized = True
            self.log.info(f"Hyperliquid adapter initialized (raw signing, {len(self._asset_map)} symbols)")
            self.log.info(f"  Wallet: {self.wallet.address[:10]}...")
            if self._hip3_address and self._hip3_address.lower() != (self._address or "").lower():
                self.log.info(f"  HIP3 Wallet: {self._hip3_address[:10]}...")
            if self._proxies:
                self.log.info(f"  Proxies: {len(self._proxies)} for public API rotation")
            if self._private_info_base_url:
                self.log.info(
                    "  Private /info: %s (read-only subset only; /exchange always public)",
                    self._private_info_base_url,
                )
            return True

        except Exception as e:
            # Preserve a short error for guardian to report.
            try:
                self._last_init_error = str(e)
            except Exception:
                self._last_init_error = "init_exception"
            self.log.error(f"Failed to initialize Hyperliquid: {e}")
            import traceback; traceback.print_exc()
            return False

    # ============================================================ HTTP helpers
    async def _post_exchange(self, payload: dict) -> dict:
        """POST to /exchange with proxy rotation and 429 retry.

        By default, 429 on /exchange is treated as non-retriable to avoid duplicate
        submissions when exchange-side acceptance is ambiguous.
        Retries exchange endpoint calls up to MAX_EXCHANGE_RETRIES when 429 is hit.
        Network errors and non-429 HTTP responses are NOT retried to avoid
        duplicate order submission (the exchange may have already processed the order).
        """
        if not self._session or self._session.closed:
            raise RuntimeError("HTTP session not available (not initialized or closed)")

        url = f"{self._base_url}/exchange"

        for attempt in range(MAX_EXCHANGE_RETRIES):
            proxy = self._next_proxy()
            async with self._session.post(url, json=payload, proxy=proxy) as resp:
                if resp.status == 429:
                    if not RETRY_EXCHANGE_ON_429:
                        raise Exception("Exchange rate limited (429); retry disabled to avoid duplicate order risk")
                    self.log.warning(
                        f"429 rate limit on /exchange (attempt {attempt + 1}/{MAX_EXCHANGE_RETRIES})"
                    )
                    await asyncio.sleep(0.5 * (attempt + 1))
                    continue
                return await resp.json()

        raise Exception("Exchange rate limited (429) after all retries")

    # ============================================================ proxy rotation
    def _next_proxy(self) -> Optional[str]:
        """Get next proxy via round-robin, or None if no proxies configured."""
        if not self._proxies:
            return None
        proxy = self._proxies[self._proxy_idx % len(self._proxies)]
        self._proxy_idx = (self._proxy_idx + 1) % len(self._proxies)
        return proxy

    def _is_public_circuit_open(self) -> bool:
        if not PUBLIC_CIRCUIT_BREAKER_ENABLED:
            return False
        return time.time() < float(self._public_cb_open_until or 0.0)

    def _public_circuit_remaining_sec(self) -> float:
        remaining = float(self._public_cb_open_until or 0.0) - time.time()
        return max(0.0, remaining)

    def _record_public_failure(self) -> None:
        if not PUBLIC_CIRCUIT_BREAKER_ENABLED:
            return
        self._public_cb_failures = int(self._public_cb_failures or 0) + 1
        if self._public_cb_failures < PUBLIC_CIRCUIT_BREAKER_FAILS:
            return
        self._public_cb_failures = 0
        self._public_cb_open_until = time.time() + float(PUBLIC_CIRCUIT_BREAKER_COOLDOWN_SEC)
        self.log.warning(
            "Public API circuit opened for %.1fs after repeated failures",
            self._public_circuit_remaining_sec(),
        )

    def _record_public_success(self) -> None:
        if self._public_cb_failures or self._public_cb_open_until:
            self._public_cb_failures = 0
            self._public_cb_open_until = 0.0

    async def _post_public(self, payload: dict) -> Any:
        """POST to /info with proxy rotation and 429 retry.

        Routing rule is intentionally strict to avoid future confusion:
        - private node: only for explicit read-only /info types in PRIVATE_INFO_TYPES
        - public API: everything else (and always for order placement via /exchange)

        On 429 or error, rotates to next proxy and retries up to MAX_PUBLIC_RETRIES times.
        """
        if not self._session or self._session.closed:
            raise RuntimeError("HTTP session not available (not initialized or closed)")

        # Private-node fast path for supported read-only types.
        # We do this before the public circuit-breaker check so allowed calls can still work
        # when public API is rate limited.
        req_type = str((payload or {}).get("type") or "").strip()
        if self._private_info_base_url and self._can_use_private_info(payload):
            private_url = f"{self._private_info_base_url}/info"
            params = {"key": self._private_info_key} if self._private_info_key else None
            try:
                async with self._session.post(
                    private_url,
                    json=payload,
                    proxy=None,
                    params=params,
                ) as resp:
                    if resp.status == 200:
                        return await resp.json()
                    text = await resp.text()
                    self.log.debug(
                        "Private /info returned HTTP %s for type=%s; fallback to public. body=%s",
                        resp.status,
                        req_type,
                        (text or "")[:200],
                    )
            except Exception as e:
                self.log.debug(
                    "Private /info request failed for type=%s; fallback to public: %s",
                    req_type,
                    e,
                )

        if self._is_public_circuit_open():
            raise Exception(
                f"Public API circuit open ({self._public_circuit_remaining_sec():.1f}s remaining)"
            )

        url = f"{self._base_url}/info"
        last_error: Optional[Exception] = None

        for attempt in range(MAX_PUBLIC_RETRIES):
            proxy = self._next_proxy()
            try:
                async with self._session.post(url, json=payload, proxy=proxy) as resp:
                    if resp.status == 429:
                        self.log.warning(
                            f"429 rate limit on /info (attempt {attempt + 1}/{MAX_PUBLIC_RETRIES})"
                        )
                        last_error = Exception("Rate limited (429)")
                        self._record_public_failure()
                        if self._is_public_circuit_open():
                            break
                        await asyncio.sleep(0.5 * (attempt + 1))
                        continue
                    if resp.status != 200:
                        text = await resp.text()
                        self.log.warning(
                            f"HTTP {resp.status} on /info (attempt {attempt + 1}/{MAX_PUBLIC_RETRIES})"
                        )
                        last_error = Exception(f"HTTP {resp.status}: {text}")
                        self._record_public_failure()
                        if self._is_public_circuit_open():
                            break
                        await asyncio.sleep(0.5 * (attempt + 1))
                        continue
                    result = await resp.json()
                    self._record_public_success()
                    return result
            except Exception as e:
                last_error = e
                self.log.warning(f"Public API error (attempt {attempt + 1}/{MAX_PUBLIC_RETRIES}): {e}")
                self._record_public_failure()
                if self._is_public_circuit_open():
                    break
                await asyncio.sleep(0.5)
                continue

        if self._is_public_circuit_open():
            raise Exception(
                f"Public API circuit open ({self._public_circuit_remaining_sec():.1f}s remaining)"
            )
        raise last_error or Exception("Public API request failed after retries")

    # ============================================================ signing helpers
    def _build_order_payload(
        self,
        symbol: str,
        is_buy: bool,
        size: float,
        price: float,
        order_type: OrderType,
        reduce_only: bool = False,
    ) -> dict:
        """Build a signed order payload (same pattern as predator)."""
        asset_id = self._get_asset_id(symbol)

        coin = self._coin_key(symbol)
        order_req: OrderRequest = {
            "coin": coin,
            "is_buy": is_buy,
            "sz": size,
            "limit_px": price,
            "order_type": order_type,
            "reduce_only": reduce_only,
        }

        order_wire = order_request_to_order_wire(order_req, asset_id)

        # Unified routing uses the same wallet for all symbols.
        subaccount_for_symbol = self._signing_subaccount_for_symbol(symbol)
        is_hip3 = self._is_hip3(symbol)
        if is_hip3:
            self.log.debug(f"HIP3 routing: {symbol} → unified wallet")
        builder_info = None if subaccount_for_symbol else {
            "b": BUILDER_ADDRESS.lower(),
            "f": BUILDER_FEE_INT,
        }
        order_action = order_wires_to_order_action([order_wire], builder_info, "na")

        timestamp = get_timestamp_ms()
        wallet_for_symbol = self._wallet_for_symbol(symbol)
        signature = sign_l1_action(
            wallet_for_symbol,
            order_action,
            subaccount_for_symbol,
            timestamp,
            None,   # expires_after
            True,   # is_mainnet
        )

        return {
            "action": order_action,
            "nonce": timestamp,
            "signature": signature,
            "vaultAddress": subaccount_for_symbol,
            "expiresAfter": None,
        }

    # ============================================================ response parsing
    def _parse_order_response(
        self, result: dict, default_price: float = 0
    ) -> Tuple[bool, str, float, float, str]:
        """Parse order response from Hyperliquid.

        Returns: (success, order_id, filled_size, filled_price, error)
        """
        if result is None:
            return (False, "", 0, 0, "No response from exchange")

        status = result.get('status', '')
        response = result.get('response', {})

        if status == 'ok':
            data = response.get('data', {}) if isinstance(response, dict) else {}
            statuses = data.get('statuses', [{}]) if isinstance(data, dict) else [{}]

            if statuses:
                fill_info = statuses[0] if isinstance(statuses[0], dict) else {}

                if 'filled' in fill_info:
                    filled = fill_info['filled']
                    oid = filled.get('oid', '')
                    filled_size = float(filled.get('totalSz', 0))
                    filled_price = float(filled.get('avgPx', default_price))
                    if filled_size > 0 and filled_price <= 0:
                        filled_price = default_price
                    return (True, str(oid), filled_size, filled_price, "")

                elif 'resting' in fill_info:
                    resting = fill_info['resting']
                    oid = resting.get('oid', '')
                    return (True, str(oid), 0, default_price, "")

                elif 'oid' in fill_info:
                    oid = fill_info.get('oid', '')
                    return (True, str(oid), 0, default_price, "")

                elif 'error' in fill_info:
                    return (False, "", 0, 0, fill_info['error'])

            # status ok but no parseable statuses — try extracting oid from data/response
            if isinstance(data, dict):
                oid = data.get('oid', '')
                if oid:
                    return (True, str(oid), 0, default_price, "")
            if isinstance(response, dict):
                oid = response.get('oid', '')
                if oid:
                    return (True, str(oid), 0, default_price, "")
            return (True, "", 0, default_price, "")

        # Generic error
        error = result.get('error', str(result))
        return (False, "", 0, 0, error)

    # ============================================================ public info helpers
    def _ensure_public_info(self):
        """DEPRECATED.

        Do not instantiate the upstream HLInfo SDK in production paths.
        It performs its own HTTP calls without our proxy rotation and can 429.
        """
        raise RuntimeError("HLInfo disabled; use _post_public/_fetch_all_mids instead")

    def _fetch_all_mids_sync(self, dex: str) -> Dict[str, Any]:
        """Sync fallback for allMids.

        Avoids the upstream HLInfo SDK; uses direct HTTP POST.
        (Best-effort; used only when async session is unavailable.)
        """
        try:
            url = f"{self._base_url}/info"
            payload: Dict[str, Any] = {"type": "allMids"}
            if dex:
                payload["dex"] = dex
            r = requests.post(url, json=payload, timeout=5)
            if r.status_code != 200:
                return {}
            data = r.json()
            return data if isinstance(data, dict) else {}
        except Exception:
            return {}

    async def _fetch_all_mids(self, dex: str) -> Dict[str, Any]:
        """Fetch allMids via proxy-rotated HTTP POST."""
        if self._session:
            payload: Dict[str, Any] = {"type": "allMids"}
            if dex:
                payload["dex"] = dex
            result = await self._post_public(payload)
            return result if result is not None else {}
        # If no session, fallback to sync HTTP (shouldn't happen after init)
        return self._fetch_all_mids_sync(dex)

    # ============================================================ ORDER METHODS

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
            self.log.info(f"[DRY_RUN] Hyperliquid {symbol} MARKET {side.upper()} {size:.4f} @ ${price:.2f}")
            return True, f"dry_run_{int(time.time() * 1000)}", size, price

        if not self._initialized or not self.wallet:
            return False, None, 0.0, 0.0

        is_buy = side.lower() == 'buy'
        ref_price = float(best_ask if is_buy else best_bid) if (best_ask or best_bid) else 0.0

        try:
            # If no price provided, fetch mid
            if ref_price <= 0:
                mid = await self.get_mid_price(symbol)
                ref_price = mid

            if ref_price <= 0:
                self.log.error(f"No price available for {symbol}")
                return False, None, 0.0, 0.0

            # Apply 5% slippage for IOC fill
            slippage_price = ref_price * 1.05 if is_buy else ref_price * 0.95

            # Round (pass original symbol — helpers handle prefix)
            rounded_size = self._round_size(size, symbol)
            rounded_price = self._round_price(slippage_price, symbol)

            if rounded_size <= 0:
                self.log.error(f"Rounded size is zero for {symbol}")
                return False, None, 0.0, 0.0

            # Build signed payload and POST (pass original symbol for coin field)
            order_type: OrderType = {"limit": {"tif": "Ioc"}}
            payload = self._build_order_payload(
                symbol, is_buy, rounded_size, rounded_price, order_type,
                reduce_only=reduce_only,
            )
            result = await self._post_exchange(payload)

            success, order_id, filled_size, filled_price, error = self._parse_order_response(result, ref_price)
            if not success or error:
                self.log.error(f"Hyperliquid market order error: {error or result}")
                return False, None, 0.0, 0.0

            if filled_size <= 0:
                self.log.warning(
                    f"[{symbol}] MARKET {side.upper()} returned zero fill size (oid={order_id}); "
                    "deferring exact fill sizing to reconciler"
                )
            if filled_price <= 0 and filled_size > 0:
                filled_price = ref_price
            self.log.info(f"[{symbol}] MARKET {side.upper()} {filled_size} @ ${filled_price:.4f} oid={order_id}")
            return True, order_id, filled_size, filled_price

        except Exception as e:
            self.log.error(f"Hyperliquid market order exception: {e}")
            return False, None, 0.0, 0.0

    async def place_limit_order(
        self,
        symbol: str,
        side: str,
        size: float,
        price: float,
        reduce_only: bool = False,
        tif: str = "Gtc",  # "Gtc" | "Alo" (post-only) | "Ioc"
    ) -> Tuple[bool, Optional[str]]:
        if self.dry_run:
            self.log.info(
                f"[DRY_RUN] Hyperliquid {symbol} LIMIT {side.upper()} {size:.4f} @ ${price:.4f}"
                f"{' (reduce_only)' if reduce_only else ''}"
            )
            return True, f"dry_run_limit_{int(time.time() * 1000)}"

        if not self._initialized or not self.wallet:
            return False, None

        is_buy = side.lower() == 'buy'

        try:
            rounded_size = self._round_size(size, symbol)
            rounded_price = self._round_price(price, symbol)

            if rounded_size <= 0:
                self.log.error(f"Rounded size is zero for {symbol} (raw={size})")
                return False, None

            tif_norm = str(tif or "Gtc").strip().title()
            if tif_norm not in {"Gtc", "Alo", "Ioc"}:
                tif_norm = "Gtc"
            order_type: OrderType = {"limit": {"tif": tif_norm}}
            payload = self._build_order_payload(
                symbol, is_buy, rounded_size, rounded_price, order_type,
                reduce_only=reduce_only,
            )
            result = await self._post_exchange(payload)

            success, order_id, _, _, error = self._parse_order_response(result, rounded_price)
            if not success or error:
                # Capture for caller retry logic (e.g. post-only immediate match)
                err_txt = str(error or result)
                primary_error = err_txt
                self.last_order_error = primary_error
                self.log.error(f"Hyperliquid limit order error: {err_txt}")

                # Builder self-heal: metadata can drift (new listings / dex offset changes).
                # On asset-range/mapping rejects, refresh metadata + retry raw signing once.
                if self._is_hip3(symbol):
                    err_low = err_txt.lower()
                    mapping_error = (
                        ("asset" in err_low and "range" in err_low)
                        or "20015" in err_low
                        or "20018" in err_low
                        or "unknown symbol" in err_low
                    )
                    if mapping_error:
                        try:
                            self._refresh_metadata_best_effort()
                            payload_retry = self._build_order_payload(
                                symbol, is_buy, rounded_size, rounded_price, order_type,
                                reduce_only=reduce_only,
                            )
                            result_retry = await self._post_exchange(payload_retry)
                            s1, oid1, _, _, e1 = self._parse_order_response(result_retry, rounded_price)
                            if s1 and oid1 and not e1:
                                self.last_order_error = None
                                self.log.info(f"[{symbol}] LIMIT {side.upper()} {rounded_size} @ ${rounded_price:.4f} oid={oid1} (meta-refresh retry)")
                                return True, oid1
                            self.last_order_error = str(e1 or result_retry)
                        except Exception as refresh_exc:
                            self.last_order_error = str(refresh_exc)

                    # Reduce-only invalid-size self-heal: requery live position and
                    # retry with a safely clamped quantity.
                    invalid_size_error = "invalid size" in err_low
                    if reduce_only and invalid_size_error:
                        try:
                            live_pos = await self.get_position(symbol)
                        except Exception:
                            live_pos = None

                        if live_pos and (live_pos.direction or "").upper() in ("LONG", "SHORT") and float(live_pos.size or 0.0) > 0:
                            live_size = float(live_pos.size)
                            sz_dec = max(0, int(self._get_sz_decimals(symbol)))

                            # Try a small ladder of decreasingly strict quantities.
                            # Some builder instruments can reject a nominally valid size,
                            # so we progressively clamp precision/step downward.
                            candidates: List[float] = []
                            seen = set()

                            def _push(v: float) -> None:
                                vv = float(self._round_size(max(0.0, v), symbol))
                                key = round(vv, 8)
                                if vv > 0 and key not in seen and abs(vv - float(rounded_size)) > 1e-12:
                                    seen.add(key)
                                    candidates.append(vv)

                            base = min(float(rounded_size), live_size)
                            step_native = 10 ** (-sz_dec) if sz_dec > 0 else 1.0
                            _push(base - step_native)
                            for dec in range(sz_dec, -1, -1):
                                scale = 10 ** dec
                                floored = math.floor(live_size * scale) / scale
                                step = 10 ** (-dec) if dec > 0 else 1.0
                                _push(floored)
                                _push(floored - step)

                            for retry_size in candidates[:8]:
                                try:
                                    payload_retry_sz = self._build_order_payload(
                                        symbol, is_buy, retry_size, rounded_price, order_type,
                                        reduce_only=reduce_only,
                                    )
                                    result_retry_sz = await self._post_exchange(payload_retry_sz)
                                    s_sz, oid_sz, _, _, e_sz = self._parse_order_response(result_retry_sz, rounded_price)
                                    if s_sz and oid_sz and not e_sz:
                                        self.last_order_error = None
                                        self.log.info(f"[{symbol}] LIMIT {side.upper()} {retry_size} @ ${rounded_price:.4f} oid={oid_sz} (reduce-only size self-heal)")
                                        return True, oid_sz
                                    self.last_order_error = str(e_sz or result_retry_sz)
                                except Exception as size_exc:
                                    self.last_order_error = str(size_exc)

                    # Final fallback: SDK path.
                    try:
                        acct = self._user_address_for_symbol(symbol)
                        ex = Exchange(
                            wallet=self._wallet_for_symbol(symbol),
                            base_url=self._base_url,
                            account_address=acct or None,
                        )
                        # Use canonical builder coin key (e.g. xyz:GOOGL), not uppercased prefix.
                        sdk_name = self._coin_key(symbol)
                        if not sdk_name:
                            sdk_name = str(symbol or '').strip()
                        sdk_resp = ex.order(
                            name=sdk_name,
                            is_buy=is_buy,
                            sz=float(rounded_size),
                            limit_px=float(rounded_price),
                            order_type={"limit": {"tif": tif_norm}},
                            reduce_only=bool(reduce_only),
                        )
                        s2, oid2, _, _, e2 = self._parse_order_response(sdk_resp, rounded_price)
                        if s2 and oid2 and not e2:
                            self.last_order_error = None
                            self.log.info(f"[{symbol}] LIMIT {side.upper()} {rounded_size} @ ${rounded_price:.4f} oid={oid2} (sdk-fallback)")
                            return True, oid2
                        fallback_error = str(e2 or sdk_resp)
                        self.last_order_error = f"{primary_error} | sdk_fallback={fallback_error}"
                        self.log.error(f"Hyperliquid sdk fallback limit order error: {fallback_error}")
                    except Exception as sdk_exc:
                        self.last_order_error = f"{primary_error} | sdk_fallback_exception={sdk_exc}"
                        self.log.error(f"Hyperliquid sdk fallback exception: {sdk_exc}")

                return False, None

            self.last_order_error = None

            self.log.info(f"[{symbol}] LIMIT {side.upper()} {rounded_size} @ ${rounded_price:.4f} oid={order_id}")
            return True, order_id

        except Exception as e:
            self.last_order_error = str(e)
            self.log.error(f"Hyperliquid limit order exception: {e}")
            return False, None

    async def place_stop_order(
        self,
        symbol: str,
        side: str,
        size: float,
        trigger_price: float,
        order_type: str = 'sl',
    ) -> Tuple[bool, Optional[str]]:
        if self.dry_run:
            self.log.info(
                f"[DRY_RUN] Hyperliquid {symbol} {order_type.upper()} {side.upper()} "
                f"{size:.4f} @ ${trigger_price:.2f}"
            )
            return True, f"dry_run_{order_type}_{int(time.time() * 1000)}"

        if not self._initialized or not self.wallet:
            return False, None

        is_buy = side.lower() == 'buy'

        try:
            rounded_size = self._round_size(size, symbol)
            rounded_trigger = self._round_price(trigger_price, symbol)

            if rounded_size <= 0:
                self.log.error(f"Rounded size is zero for {symbol}")
                return False, None

            asset_id = self._get_asset_id(symbol)
            tpsl = "tp" if order_type == 'tp' else "sl"

            # SL uses market order (guaranteed fill for risk management)
            # TP uses limit order (better price for profit taking)
            is_market = order_type != 'tp'

            coin = self._coin_key(symbol)
            order_req: OrderRequest = {
                "coin": coin,
                "is_buy": is_buy,
                "sz": rounded_size,
                "limit_px": rounded_trigger,
                "order_type": {
                    "trigger": {
                        "isMarket": is_market,
                        "triggerPx": rounded_trigger,
                        "tpsl": tpsl,
                    }
                },
                "reduce_only": True,
            }

            order_wire = order_request_to_order_wire(order_req, asset_id)

            # Unified routing uses the same wallet for all symbols.
            subaccount_for_symbol = self._signing_subaccount_for_symbol(symbol)
            builder_info = None if subaccount_for_symbol else {
                "b": BUILDER_ADDRESS.lower(),
                "f": BUILDER_FEE_INT,
            }
            order_action = order_wires_to_order_action([order_wire], builder_info, "na")

            timestamp = get_timestamp_ms()
            wallet_for_symbol = self._wallet_for_symbol(symbol)
            signature = sign_l1_action(
                wallet_for_symbol, order_action,
                subaccount_for_symbol,
                timestamp, None, True,
            )

            payload = {
                "action": order_action,
                "nonce": timestamp,
                "signature": signature,
                "vaultAddress": subaccount_for_symbol,
                "expiresAfter": None,
            }

            result = await self._post_exchange(payload)
            success, order_id, _, _, error = self._parse_order_response(result, rounded_trigger)
            if not success or error:
                self.log.error(f"Hyperliquid stop order error: {error or result}")
                return False, None

            self.log.info(
                f"[{symbol}] STOP {order_type.upper()} {side.upper()} "
                f"{rounded_size} trigger@${rounded_trigger:.4f} oid={order_id}"
            )
            return True, order_id

        except Exception as e:
            self.log.error(f"Hyperliquid stop order exception: {e}")
            return False, None

    async def cancel_order(self, symbol: str, order_id: str) -> bool:
        if self.dry_run:
            self.log.info(f"[DRY_RUN] Hyperliquid cancel {symbol} order {order_id}")
            return True

        if not self._initialized or not self.wallet:
            return False

        try:
            oid = int(order_id)
        except (TypeError, ValueError):
            self.log.error(f"Hyperliquid cancel requires numeric order id, got {order_id}")
            return False

        try:
            asset_id = self._get_asset_id(symbol)

            cancel_action = {
                "type": "cancel",
                "cancels": [{"a": asset_id, "o": oid}],
            }

            # Unified routing uses the same wallet for all symbols.
            subaccount_for_symbol = self._signing_subaccount_for_symbol(symbol)

            timestamp = get_timestamp_ms()
            wallet_for_symbol = self._wallet_for_symbol(symbol)
            signature = sign_l1_action(
                wallet_for_symbol, cancel_action,
                subaccount_for_symbol,
                timestamp, None, True,
            )

            payload = {
                "action": cancel_action,
                "nonce": timestamp,
                "signature": signature,
                "vaultAddress": subaccount_for_symbol,
                "expiresAfter": None,
            }

            result = await self._post_exchange(payload)

            status = result.get('status', '')
            if status == 'ok':
                self.log.info(f"[{symbol}] Cancelled order {order_id}")
                return True

            error = str(result)
            if 'already' in error.lower() or 'not found' in error.lower():
                self.log.debug(f"[{symbol}] Order {order_id} already cancelled/filled")
                return True

            self.log.error(f"Hyperliquid cancel failed: {result}")
            return False

        except Exception as e:
            err_str = str(e).lower()
            if 'already canceled' in err_str or 'already filled' in err_str:
                return True
            self.log.error(f"Hyperliquid cancel exception: {e}")
            return False

    async def cancel_all_orders(self, symbol: str) -> int:
        """Best-effort cancel of all *open* orders for symbol.

        Used for hardening on closes/flips to avoid stale chase/protection orders.
        Returns number of cancel attempts.
        """
        if self.dry_run:
            self.log.info(f"[DRY_RUN] Hyperliquid cancel_all_orders {symbol}")
            return 0
        if not self._initialized:
            return 0

        target_coin = self._coin_key(symbol)
        if ":" not in target_coin:
            target_coin = target_coin.upper()
        address = self._user_address_for_symbol(symbol)
        if not address:
            return 0

        attempts = 0
        try:
            open_orders = await self._post_public({"type": "frontendOpenOrders", "user": address})
            if not isinstance(open_orders, list):
                return 0
            for order in open_orders:
                if not isinstance(order, dict):
                    continue
                coin = str(order.get("coin", "")).strip()
                coin_key = self._coin_key(coin) if ":" in coin else coin.upper()
                if coin_key != target_coin:
                    continue
                oid = order.get("oid")
                if oid is None:
                    continue
                attempts += 1
                try:
                    await self.cancel_order(symbol, str(int(oid)))
                except Exception:
                    pass
        except Exception as e:
            self.log.warning(f"Hyperliquid cancel_all_orders exception: {e}")

        return attempts

    # ============================================================ POSITION / INFO (unchanged logic)

    async def get_position(self, symbol: str) -> Optional[Position]:
        if not self._initialized:
            return None

        # Builder symbols require dex-scoped clearinghouseState and canonical coin keys.
        is_hip3 = self._is_hip3(symbol)
        target_coin = self._coin_key(symbol) if is_hip3 else self._normalize_symbol(symbol).upper()

        # Unified account for both perps and builder symbols.
        address = self._user_address_for_symbol(symbol)
        if not address:
            return None

        try:
            payload: Dict[str, Any] = {"type": "clearinghouseState", "user": address}
            if is_hip3:
                payload["dex"] = str(symbol).split(":", 1)[0].strip().lower()

            state = await self._post_public(payload)
            positions = state.get("assetPositions", []) if isinstance(state, dict) else []
            for item in positions:
                pos = item.get("position", item) if isinstance(item, dict) else {}
                coin_raw = str(pos.get("coin", ""))
                if not coin_raw:
                    continue

                coin_key = self._coin_key(coin_raw) if (":" in coin_raw or is_hip3) else coin_raw.upper()
                if coin_key != target_coin:
                    continue

                szi = float(pos.get("szi", pos.get("size", 0.0)))
                direction = "FLAT"
                if szi > 0:
                    direction = "LONG"
                elif szi < 0:
                    direction = "SHORT"
                entry = float(pos.get("entryPx", pos.get("entry_price", 0.0)))
                unrealized = float(pos.get("unrealizedPnl", pos.get("unrealized_pnl", 0.0)))
                return Position(
                    symbol=symbol,
                    direction=direction,
                    size=abs(szi),
                    entry_price=entry,
                    unrealized_pnl=unrealized,
                )
        except Exception as e:
            self.log.error(f"Hyperliquid get_position exception: {e}")

        return None

    async def get_all_positions(self) -> Dict[str, Position]:
        if not self._initialized:
            return {}

        address = self._address
        if not address:
            return {}

        result: Dict[str, Position] = {}

        def _parse_positions(positions_data, prefix: str = "", *, hip3_only: bool = False):
            positions = positions_data.get("assetPositions", []) if isinstance(positions_data, dict) else []
            hip3_set = set()
            if hip3_only:
                try:
                    hip3_set = self._hip3_coin_set()
                except Exception:
                    hip3_set = set()

            for item in positions:
                pos = item.get("position", item) if isinstance(item, dict) else {}
                coin = str(pos.get("coin", ""))
                if not coin:
                    continue

                # If this is the "HIP3" pass, ignore any normal perps coins that
                # happen to live in the wallet account.
                #
                # NOTE: hip3_set can be empty if metadata is stale/unavailable.
                # In that case we accept all positions from the HIP3 dex call.
                if hip3_only and hip3_set:
                    coin_norm = coin.split(":", 1)[1] if ":" in coin else coin
                    if str(coin_norm).strip().upper() not in hip3_set:
                        continue

                szi = float(pos.get("szi", pos.get("size", 0.0)))
                direction = "FLAT"
                if szi > 0:
                    direction = "LONG"
                elif szi < 0:
                    direction = "SHORT"
                entry = float(pos.get("entryPx", pos.get("entry_price", 0.0)))
                unrealized = float(pos.get("unrealizedPnl", pos.get("unrealized_pnl", 0.0)))

                # Canonical keying:
                # - Perps: "BTC", "ETH"...
                # - Builder symbols: canonicalized `dex:BASE` via _coin_key.
                if ":" in coin:
                    key = self._coin_key(str(coin))
                else:
                    raw = f"{prefix}{str(coin).strip().upper()}" if prefix else str(coin).strip().upper()
                    key = self._coin_key(raw) if ":" in raw else raw

                result[key] = Position(
                    symbol=key,
                    direction=direction,
                    size=abs(szi),
                    entry_price=entry,
                    unrealized_pnl=unrealized,
                )

        # CRITICAL: Fetch main perp positions
        try:
            state = await self._post_public({"type": "clearinghouseState", "user": address})
            _parse_positions(state)
            self.log.debug(f"Fetched {len(result)} main positions from Hyperliquid (perps)")
        except Exception as e:
            self.log.error(f"🚨 Hyperliquid get_all_positions FAILED: {e}")
            # RAISE instead of silently returning empty - reconciliation needs to know about failures
            raise

        # Builder-dex positions - optional, don't fail if unavailable.
        try:
            for dex in self._builder_dexes():
                try:
                    dex_state = await self._post_public(
                        {"type": "clearinghouseState", "user": address, "dex": dex}
                    )
                    _parse_positions(dex_state, prefix=f"{dex.upper()}:", hip3_only=True)
                except Exception:
                    continue
            self.log.debug(
                f"Fetched {len([k for k in result if ':' in str(k)])} builder-dex positions"
            )
        except Exception as e:
            self.log.debug(f"Hyperliquid builder positions fetch failed (non-fatal): {e}")

        self.log.info(f"✅ Successfully fetched {len(result)} total positions from Hyperliquid")

        return result

    # ============================================================ tick / mid / order status

    async def get_tick_size(self, symbol: str) -> float:
        if self.dry_run:
            return 0.01
        if not self._initialized:
            return 0.01

        coin_key = self._coin_key(symbol)
        cached = float(self._tick_size_cache.get(coin_key) or 0.0)
        if cached > 0:
            return cached

        # Prefer live L2 precision for ALL symbols (perps + builder).
        # The legacy sz-decimal heuristic can produce invalid ticks (e.g. WIF/LINK/TON),
        # which then causes "Price must be divisible by tick size" rejects.
        try:
            book = await self._post_public({"type": "l2Book", "coin": self._l2_coin(symbol)})
            if isinstance(book, dict):
                levels = book.get("levels") if isinstance(book.get("levels"), list) else []
                px_vals = []
                for side in levels[:2]:
                    if isinstance(side, list) and side:
                        px_raw = (side[0] or {}).get("px")
                        if px_raw is not None:
                            px_vals.append(str(px_raw))
                decimals = []
                for pv in px_vals:
                    if "." in pv:
                        decimals.append(len(pv.split(".", 1)[1].rstrip("0")))
                    else:
                        decimals.append(0)
                if decimals:
                    d = max(0, max(decimals))
                    tick = 10 ** (-d)
                    if tick > 0:
                        self._tick_size_cache[coin_key] = float(tick)
                        return float(tick)
        except Exception:
            pass

        hl_symbol = self._normalize_symbol(symbol).upper()
        sz_dec = self._get_sz_decimals(hl_symbol)
        px_dec = 6 - sz_dec
        # Builder-dex assets have tighter tick grids than 6-szDec implies.
        # Use a conservative 2-decimal cap for tick size calc (0.01 tick).
        # _round_price uses 5-sig-fig for finer precision where possible.
        if self._is_hip3(symbol):
            px_dec = min(px_dec, 2)
        tick = 10 ** (-px_dec)
        self._tick_size_cache[coin_key] = float(tick)
        return float(tick)

    async def get_mid_price(self, symbol: str) -> float:
        if self.dry_run:
            # Dry-run mode: return non-zero sentinel so executor tests can pass
            # pre-trade symbol validation without live market data.
            return 1.0
        if not self._initialized:
            return 0.0

        # HIP3 stocks (xyz:*) are not reliably present in allMids.
        # Use l2Book midpoint instead.
        if self._is_hip3(symbol):
            try:
                bid, ask = await self.get_best_bid_ask(symbol)
                if bid > 0 and ask > 0:
                    return (bid + ask) / 2.0
            except Exception:
                return 0.0
            return 0.0

        hl_symbol = self._normalize_symbol(symbol).upper()
        dex = symbol.split(':', 1)[0] if ':' in symbol else ""

        try:
            mids = await self._fetch_all_mids(dex)
            if mids is None:
                self.log.warning(f"Hyperliquid get_mid_price: allMids returned None for {hl_symbol}")
                return 0.0
            return float(mids.get(hl_symbol, 0))
        except Exception as e:
            self.log.warning(f"Hyperliquid get_mid_price exception: {e}")
            return 0.0

    async def check_order_status(
        self,
        symbol: str,
        order_id: str,
    ) -> Dict[str, Any]:
        result: Dict[str, Any] = {
            'status': 'unknown',
            'filled_size': 0.0,
            'remaining_size': 0.0,
            'avg_price': 0.0,
        }

        if self.dry_run:
            # Dry-run mode: treat orders as fully filled.
            result['status'] = 'filled'
            result['filled_size'] = 1e18
            result['avg_price'] = 1.0
            result['remaining_size'] = 0.0
            return result

        if not self._initialized:
            return result

        # Unified routing uses the same wallet for all symbols.
        address = self._user_address_for_symbol(symbol)
        if not address:
            return result

        try:
            oid = int(order_id)
        except (TypeError, ValueError):
            self.log.error(f"Hyperliquid check_order_status: bad order_id {order_id}")
            return result

        try:
            # Use frontendOpenOrders for reliable open order checking
            open_orders = await self._post_public(
                {"type": "frontendOpenOrders", "user": address}
            )

            if isinstance(open_orders, list):
                for order in open_orders:
                    if not isinstance(order, dict):
                        continue
                    if int(order.get("oid", -1)) != oid:
                        continue

                    orig_sz = float(order.get("origSz", order.get("sz", 0)))
                    current_sz = float(order.get("sz", 0))
                    filled = orig_sz - current_sz if orig_sz > current_sz else 0.0

                    if filled > 0 and current_sz > 0:
                        result['status'] = 'partially_filled'
                    else:
                        result['status'] = 'open'
                    result['filled_size'] = filled
                    result['remaining_size'] = current_sz
                    result['avg_price'] = float(order.get("limitPx", 0))
                    return result

            # Not in open orders — check orderStatus first (authoritative)
            status_payload = await self._post_public(
                {"type": "orderStatus", "user": address, "oid": oid}
            )

            if isinstance(status_payload, dict) and status_payload.get('status') == 'order':
                order_wrap = status_payload.get('order', {}) or {}
                order_obj = order_wrap.get('order', {}) or {}
                st = str(order_wrap.get('status', 'unknown')).lower()

                orig_sz = float(order_obj.get('origSz', order_obj.get('sz', 0)) or 0)
                remaining_sz = float(order_obj.get('sz', 0) or 0)
                filled_sz = max(0.0, orig_sz - remaining_sz)
                px = float(order_obj.get('limitPx', 0) or 0)

                if st in ('filled', 'filledorresting'):
                    # `filledorresting` can still have remaining size; only treat as filled
                    # when the remaining size is effectively zero.
                    if remaining_sz <= 0 or (orig_sz > 0 and filled_sz >= (orig_sz - 1e-9)):
                        result['status'] = 'filled'
                        result['filled_size'] = filled_sz if filled_sz > 0 else orig_sz
                        result['remaining_size'] = 0.0
                        result['avg_price'] = px
                        return result
                    result['status'] = 'partially_filled'
                    result['filled_size'] = filled_sz
                    result['remaining_size'] = remaining_sz
                    result['avg_price'] = px
                    return result
                if st in ('resting', 'open'):
                    result['status'] = 'open'
                    result['filled_size'] = filled_sz
                    result['remaining_size'] = remaining_sz
                    result['avg_price'] = px
                    return result
                if st in ('canceled', 'cancelled', 'rejected'):
                    # IMPORTANT: a canceled order may still have partial (or even full)
                    # fills before cancellation. Preserve filled/remaining so callers
                    # (e.g., Chase Limit) can correctly account for fills and avoid
                    # double-ordering the full size.
                    result['status'] = 'canceled'
                    result['filled_size'] = filled_sz
                    result['remaining_size'] = remaining_sz
                    result['avg_price'] = px
                    return result
                # Otherwise fall through to fills lookup

            # Fallback: check fills via public API
            fills = await self._post_public({"type": "userFills", "user": address})
            if isinstance(fills, list):
                total_size = 0.0
                weighted_price = 0.0
                for f in fills[-ORDER_STATUS_FILLS_LIMIT:]:
                    if str(f.get('oid', '')) == str(oid):
                        sz = float(f.get('sz', 0))
                        px = float(f.get('px', 0))
                        total_size += sz
                        weighted_price += sz * px

                if total_size > 0:
                    result['status'] = 'filled'
                    result['filled_size'] = total_size
                    result['remaining_size'] = 0.0
                    result['avg_price'] = weighted_price / total_size
                else:
                    # Not found: could be eventual consistency; don't assume cancel
                    result['status'] = 'unknown'

        except Exception as e:
            self.log.warning(f"Hyperliquid check_order_status exception: {e}")

        return result

    # ============================================================ cleanup

    async def close(self):
        """Close aiohttp session to avoid resource leaks."""
        if self._session and not self._session.closed:
            await self._session.close()
            self._session = None

    # ============================================================ account trades

    @staticmethod
    def _detect_sign_change(fill: dict) -> bool:
        try:
            start_pos = float(fill.get('startPosition', 0))
            fill_sz = float(fill.get('sz', 0))
            side = fill.get('side', '').upper()

            if start_pos == 0:
                return False

            if side == 'B':
                end_pos = start_pos + fill_sz
            else:
                end_pos = start_pos - fill_sz

            if end_pos == 0:
                return True
            return (start_pos > 0) != (end_pos > 0)
        except (TypeError, ValueError):
            try:
                return abs(float(fill.get('closedPnl', 0))) > 0
            except (TypeError, ValueError):
                return False

    async def get_account_trades(self, limit: int = 50) -> List[Dict[str, Any]]:
        if self.dry_run:
            return []

        if not self._initialized or not self._address:
            return []

        try:
            # Unified wallet mode: one address for both perps and HIP3 fills.
            # For now, we primarily reconcile fills from the unified account.
            # Optionally include wallet fills for known HIP3 tickers.
            address = self._address

            raw_fills = await self._post_public({"type": "userFills", "user": address})
            if not isinstance(raw_fills, list):
                raw_fills = []

            # Optional: include HIP3 fills from wallet, but ONLY for tickers in hip3 symbol feed.
            hip3_coins = set()
            try:
                hip3_coins.update(self._hip3_coin_set())
            except Exception:
                pass
            try:
                hip3_data = {}
                if EVCLAW_TRACKER_HIP3_SYMBOLS_URL:
                    req = urllib.request.Request(
                        EVCLAW_TRACKER_HIP3_SYMBOLS_URL,
                        headers={"User-Agent": "EVClawHyperliquidAdapter/1.0"},
                    )
                    with urllib.request.urlopen(req, timeout=5) as response:
                        raw = response.read()
                        if raw:
                            hip3_data = json.loads(raw.decode("utf-8", errors="replace"))

                if isinstance(hip3_data, dict):
                    for sym in (hip3_data.get("all_symbols") or []):
                        if not isinstance(sym, str) or ":" not in sym:
                            continue
                        pfx = sym.split(":", 1)[0].strip().lower()
                        if pfx in self.BUILDER_DEXES:
                            hip3_coins.add(sym.split(":", 1)[1].upper())
            except Exception:
                pass

            wallet_raw = []
            include_wallet_hip3_fills = (
                env_str("EVCLAW_INCLUDE_WALLET_HIP3_FILLS", "0").strip().lower()
                in ("1", "true", "yes", "y", "on")
            )
            hip3_user = (self._hip3_address or self._address or "").strip()
            perps_user = (address or "").strip()
            if include_wallet_hip3_fills and hip3_coins and hip3_user and hip3_user.lower() != perps_user.lower():
                try:
                    wallet_raw = await self._post_public({"type": "userFills", "user": hip3_user})
                    if not isinstance(wallet_raw, list):
                        wallet_raw = []
                except Exception:
                    wallet_raw = []

            # Hyperliquid's userFills ordering is not guaranteed. Sort by fill time.
            def _t_ms(x: Dict[str, Any]) -> float:
                try:
                    return float(x.get("time", 0) or 0)
                except Exception:
                    return 0.0

            try:
                raw_fills = sorted(raw_fills, key=_t_ms)
            except Exception:
                pass
            try:
                wallet_raw = sorted(wallet_raw, key=_t_ms)
            except Exception:
                pass

            results: List[Dict[str, Any]] = []

            def _is_hip3_coin(coin: str) -> bool:
                try:
                    c = str(coin or "").strip()
                except Exception:
                    return False
                if not c:
                    return False
                if ":" in c:
                    pfx = c.split(":", 1)[0].strip().lower()
                    if pfx in self.BUILDER_DEXES:
                        return True
                # IMPORTANT:
                # Do not infer builder symbols from bare coin names (e.g. "ZEC") using
                # hip3 base-name membership. That mislabels normal perps as xyz:* when a
                # builder venue lists the same basename, which breaks fill->trade matching.
                return False

            def _append_fill(f: Dict[str, Any], *, account: str, force_builder: bool) -> None:
                try:
                    crossed = bool(f.get("crossed", False))
                    coin = str(f.get("coin", "") or "")
                    sym_out = coin
                    if force_builder:
                        # Only prefix when we are confident it's a builder-dex ticker.
                        if ":" in coin:
                            sym_out = self._coin_key(coin)
                        else:
                            # Bare symbol from wallet fills: keep legacy xyz default.
                            sym_out = self._coin_key(f"xyz:{coin}")
                    trade_id = f.get("tid", f.get("oid", ""))
                    results.append({
                        # Namespace wallet fills to avoid any possible tid collisions across accounts.
                        'trade_id': f"{account}:{trade_id}" if account else str(trade_id),
                        'symbol': sym_out,
                        'side': 'buy' if f.get('side', '').upper() == 'B' else 'sell',
                        'size': float(f.get('sz', 0)),
                        'price': float(f.get('px', 0)),
                        'fee': float(f.get('fee', 0)),
                        'fee_maker': 0.0,
                        'timestamp': float(f.get('time', 0)) / 1000.0,
                        'position_sign_changed': self._detect_sign_change(f),
                        'crossed': crossed,
                        'liquidity': 'TAKER' if crossed else 'MAKER',
                        'account': account,
                    })
                except (TypeError, ValueError, KeyError) as e:
                    self.log.debug(f"Skipping malformed HL fill: {e}")

            # Add latest wallet fills
            for f in raw_fills[-limit:]:
                coin = str(f.get("coin", "") or "")
                _append_fill(
                    f,
                    account="wallet",
                    force_builder=_is_hip3_coin(coin),
                )

            # Add latest HIP3 fills (wallet), filtered to known hip3 tickers only.
            if wallet_raw and hip3_coins:
                hip3_filtered = []
                for f in wallet_raw:
                    try:
                        coin = str(f.get("coin", "") or "").upper()
                    except Exception:
                        coin = ""
                    if not coin:
                        continue
                    if ":" in coin and coin.split(":", 1)[0].lower() in self.BUILDER_DEXES:
                        coin = coin.split(":", 1)[1]
                    if coin in hip3_coins:
                        hip3_filtered.append(f)
                for f in hip3_filtered[-limit:]:
                    _append_fill(f, account="hip3_wallet", force_builder=True)

            # Sort combined fills by timestamp and return newest `limit`
            try:
                results = sorted(results, key=lambda x: float(x.get("timestamp", 0) or 0))
            except Exception:
                pass

            return results[-limit:]
        except Exception as e:
            self.log.error(f"Hyperliquid get_account_trades exception: {e}")
            return []
