#!/usr/bin/env python3
"""
Support/Resistance Levels Worker

Builds S/R levels from rolling highs/lows, volume extremes, round numbers,
volume profile levels, and optional orderbook heatmap buckets.

Binance Futures-first candle fetcher with Hyperliquid fallback.
"""

import asyncio
import logging
import os
import time
from datetime import datetime, timezone
from typing import List, Tuple
import urllib.request
import json

import aiohttp

from candle_fetcher import fetch_1h_candles_for_symbols
from signal_utils import ensure_dir, load_json, write_json_atomic
from universe_cache import fetch_hl_symbols, load_dual_venue_symbols
from env_utils import (
    EVCLAW_SIGNALS_DIR,
    EVCLAW_TRACKER_HIP3_PREDATOR_URL,
    EVCLAW_TRACKER_HIP3_SYMBOLS_URL,
    EVCLAW_TRACKER_SYMBOL_URL_TEMPLATE,
)

SIGNALS_DIR = EVCLAW_SIGNALS_DIR
OUTPUT_FILE = os.path.join(SIGNALS_DIR, "sr_levels.json")
VOLUME_PROFILE_FILE = os.path.join(SIGNALS_DIR, "volume_profile.json")

_LOOKBACK_DEFAULT = [1, 4, 24, 168]
_TIMEFRAME_WEIGHTS_DEFAULT = {
    "1h": 1.0,
    "4h": 1.5,
    "24h": 2.0,
    "168h": 3.0,
    "session_8h": 1.25,
    "round": 0.5,
    "vp": 1.5,
    "vol_extreme": 1.75,
    "orderbook": 2.5,
}
LOOKBACK_HOURS = list(_LOOKBACK_DEFAULT)
SESSION_HOURS = 8
TOUCH_TOL_PCT = 0.002  # 0.2%
TIMEFRAME_WEIGHTS = dict(_TIMEFRAME_WEIGHTS_DEFAULT)

CANDLE_CONCURRENCY = 10
CANDLE_TIMEOUT_SEC = 10
HIP3_STATE_URL = EVCLAW_TRACKER_HIP3_PREDATOR_URL
HIP3_SYMBOLS_URL = EVCLAW_TRACKER_HIP3_SYMBOLS_URL
SYMBOL_URL_TEMPLATE = EVCLAW_TRACKER_SYMBOL_URL_TEMPLATE


def _get_logger() -> logging.Logger:
    logger = logging.getLogger("sr_levels_worker")
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




def _merge_levels(levels: List[dict], tol_pct: float) -> List[dict]:
    if not levels:
        return []
    levels.sort(key=lambda x: x["price"])
    merged: List[dict] = []
    current = dict(levels[0])
    for lvl in levels[1:]:
        price = lvl["price"]
        if abs(price - current["price"]) / current["price"] <= tol_pct:
            # merge
            current["price"] = (current["price"] + price) / 2
            current["sources"].extend(lvl["sources"])
            current["timeframe_weight"] = max(current["timeframe_weight"], lvl["timeframe_weight"])
            if current.get("timeframe") != lvl.get("timeframe"):
                current["timeframe"] = "mixed"
        else:
            merged.append(current)
            current = dict(lvl)
    merged.append(current)
    return merged


def _touch_stats(level_price: float, candles: List[dict], tol_pct: float) -> Tuple[int, float, int]:
    if level_price <= 0 or not candles:
        return 0, 0.0, 0
    total_vol = sum(c["v"] for c in candles) / max(len(candles), 1)
    touch_count = 0
    touch_vol = 0.0
    last_touch_ts = 0
    for c in candles:
        if abs(c["c"] - level_price) / level_price <= tol_pct:
            touch_count += 1
            touch_vol += c["v"]
            last_touch_ts = max(last_touch_ts, int(c["t"]))
    avg_touch_vol = touch_vol / touch_count if touch_count else 0.0
    vol_weight = avg_touch_vol / total_vol if total_vol > 0 else 0.0
    return touch_count, vol_weight, last_touch_ts


def _flip_stats(level_price: float, candles: List[dict]) -> Tuple[bool, int]:
    if level_price <= 0 or len(candles) < 5:
        return False, 0
    signs = []
    for c in candles[-50:]:
        diff = c["c"] - level_price
        if diff == 0:
            continue
        signs.append(1 if diff > 0 else -1)
    flips = 0
    for i in range(1, len(signs)):
        if signs[i] != signs[i - 1]:
            flips += 1
    return flips >= 2, flips


def _load_json_from_url(url: str) -> dict:
    if not url:
        return {}
    request = urllib.request.Request(url, headers={"User-Agent": "EVClawSRLevels/1.0"})
    with urllib.request.urlopen(request, timeout=5) as response:
        raw = response.read()
        if not raw:
            return {}
        return json.loads(raw.decode("utf-8", errors="replace"))


def _load_json_from_sources(*, url: str, fallback_path: str) -> dict:
    if url:
        try:
            data = _load_json_from_url(url)
            if isinstance(data, dict):
                return data
        except Exception as exc:
            _get_logger().debug(f"Failed to load remote JSON from {url}: {exc}")
    if fallback_path:
        try:
            data = load_json(fallback_path)
            if isinstance(data, dict):
                return data
        except Exception as exc:
            _get_logger().debug(f"Failed to load local JSON from {fallback_path}: {exc}")
    return {}


def _load_orderbook_levels(symbol: str, price: float) -> List[dict]:
    levels = []
    data = {}
    if SYMBOL_URL_TEMPLATE:
        symbol_template_path = SYMBOL_URL_TEMPLATE
        try:
            url = symbol_template_path.format(symbol=symbol, symbol_upper=symbol.upper(), symbol_lower=str(symbol).lower())
            data = _load_json_from_sources(url=url, fallback_path="")
        except Exception:
            data = {}
    if not isinstance(data, dict):
        data = {}
    if not data:
        return levels
    heatmap = data.get("orderbook_heatmap") or data.get("orderbook_heatmap_all")
    if not isinstance(heatmap, dict):
        return levels
    for side in ("bids", "asks"):
        buckets = heatmap.get(side, [])
        if not isinstance(buckets, list):
            continue
        # top 2 buckets by total value
        top = sorted(buckets, key=lambda b: b.get("total_value", 0), reverse=True)[:2]
        for b in top:
            lvl_price = float(b.get("price_bucket", 0))
            if lvl_price <= 0:
                continue
            levels.append({
                "price": lvl_price,
                "type": "support" if lvl_price < price else "resistance",
                "sources": ["orderbook"],
                "timeframe_weight": TIMEFRAME_WEIGHTS["orderbook"],
                "timeframe": "orderbook",
            })
    return levels


async def run_once() -> None:
    log = _get_logger()
    ensure_dir(SIGNALS_DIR)

    timeout = aiohttp.ClientTimeout(total=10)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        try:
            symbols = await load_dual_venue_symbols(session=session)
        except Exception as exc:
            log.warning(f"Universe cache unavailable ({exc}); falling back to HL meta")
            symbols = await fetch_hl_symbols(session)

        # Add HIP3 symbols (xyz:*) from the hip3 predator state so SR levels exist
        # for stocks too (Massive candles are used in candle_fetcher).
        hip3_url = (HIP3_STATE_URL or "").strip()
        try:
            hip3_data = _load_json_from_sources(url=hip3_url, fallback_path="") or {}
            hip3_symbols = hip3_data.get("symbols", {}) if isinstance(hip3_data, dict) else {}
            if isinstance(hip3_symbols, dict):
                for sym in hip3_symbols.keys():
                    s = str(sym or "")
                    if s and s.lower().startswith("xyz:"):
                        symbols.append(s.upper())
            if not hip3_symbols and isinstance(hip3_data, list):
                for item in hip3_data:
                    s = str(item.get("symbol", "") or item if isinstance(item, str) else "")
                    if s.lower().startswith("xyz:"):
                        symbols.append(s.upper())
        except Exception:
            pass

        # de-dupe
        symbols = sorted(set([str(s).upper() for s in (symbols or []) if str(s).strip()]))

        if not symbols:
            log.warning("No symbols found from HL meta")
            return

    candles_by_symbol = await fetch_1h_candles_for_symbols(
        symbols,
        hours_back=168,
        concurrency=CANDLE_CONCURRENCY,
        timeout_sec=CANDLE_TIMEOUT_SEC,
    )
    vp_data = load_json(VOLUME_PROFILE_FILE) or {}
    vp_symbols = vp_data.get("symbols", {})

    output = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "symbols": {},
    }

    for symbol, payload in candles_by_symbol.items():
        # FIX CRITICAL-2: Per-symbol error isolation
        try:
            candles_1h = payload.get("candles") or []
            if len(candles_1h) < 10:
                continue
            price = float(candles_1h[-1]["c"])
            if price <= 0:
                continue

            levels: List[dict] = []

            # Rolling highs/lows
            for hours in LOOKBACK_HOURS:
                window = candles_1h[-hours:] if len(candles_1h) >= hours else candles_1h
                if not window:
                    continue
                hi = max(c["h"] for c in window)
                lo = min(c["l"] for c in window)
                tf_key = f"{hours}h"
                levels.append({
                    "price": hi,
                    "type": "resistance",
                    "sources": [f"{tf_key}_high"],
                    "timeframe_weight": TIMEFRAME_WEIGHTS.get(tf_key, 1.0),
                    "timeframe": tf_key,
                })
                levels.append({
                    "price": lo,
                    "type": "support",
                    "sources": [f"{tf_key}_low"],
                    "timeframe_weight": TIMEFRAME_WEIGHTS.get(tf_key, 1.0),
                    "timeframe": tf_key,
                })

            # Session highs/lows (8h)
            session_window = candles_1h[-SESSION_HOURS:] if len(candles_1h) >= SESSION_HOURS else candles_1h
            if session_window:
                hi = max(c["h"] for c in session_window)
                lo = min(c["l"] for c in session_window)
                levels.append({
                    "price": hi,
                    "type": "resistance",
                    "sources": ["session_high"],
                    "timeframe_weight": TIMEFRAME_WEIGHTS["session_8h"],
                    "timeframe": "session_8h",
                })
                levels.append({
                    "price": lo,
                    "type": "support",
                    "sources": ["session_low"],
                    "timeframe_weight": TIMEFRAME_WEIGHTS["session_8h"],
                    "timeframe": "session_8h",
                })

            # Round numbers
            step = price * 0.005
            if step > 0:
                base = round(price / step) * step
                for i in range(-5, 6):
                    lvl_price = base + i * step
                    if lvl_price <= 0:
                        continue
                    levels.append({
                        "price": lvl_price,
                        "type": "support" if lvl_price < price else "resistance",
                        "sources": ["round"],
                        "timeframe_weight": TIMEFRAME_WEIGHTS["round"],
                        "timeframe": "round",
                    })

            # Volume-weighted extremes (>2x avg volume)
            avg_vol = sum(c["v"] for c in candles_1h[-24:]) / max(len(candles_1h[-24:]), 1)
            for c in candles_1h[-24:]:
                if avg_vol > 0 and c["v"] >= 2 * avg_vol:
                    levels.append({
                        "price": c["h"],
                        "type": "resistance",
                        "sources": ["vol_extreme_high"],
                        "timeframe_weight": TIMEFRAME_WEIGHTS["vol_extreme"],
                        "timeframe": "vol_extreme",
                    })
                    levels.append({
                        "price": c["l"],
                        "type": "support",
                        "sources": ["vol_extreme_low"],
                        "timeframe_weight": TIMEFRAME_WEIGHTS["vol_extreme"],
                        "timeframe": "vol_extreme",
                    })

            # Volume profile levels (POC/VAH/VAL)
            vp_symbol = vp_symbols.get(symbol, {}).get("timeframes", {})
            for tf_name in ("session_8h", "daily_24h"):
                tf = vp_symbol.get(tf_name, {})
                for key in ("poc", "vah", "val"):
                    lvl_price = float(tf.get(key, 0) or 0)
                    if lvl_price <= 0:
                        continue
                    levels.append({
                        "price": lvl_price,
                        "type": "support" if lvl_price < price else "resistance",
                        "sources": [f"vp_{key}"],
                        "timeframe_weight": TIMEFRAME_WEIGHTS["vp"],
                        "timeframe": tf_name,
                    })

            # Orderbook heatmap levels (optional)
            levels.extend(_load_orderbook_levels(symbol, price))

            levels = _merge_levels(levels, TOUCH_TOL_PCT)

            # Compute strength
            strength_vals = []
            for lvl in levels:
                touch_count, vol_weight, last_touch_ts = _touch_stats(lvl["price"], candles_1h, TOUCH_TOL_PCT)
                flip, flip_count = _flip_stats(lvl["price"], candles_1h)
                age_hours = 0.0
                if last_touch_ts:
                    age_hours = max(0.0, (time.time() * 1000 - last_touch_ts) / 3600_000)
                recency_penalty = min(1.0, age_hours / 168.0)

                raw_strength = (
                    touch_count * lvl["timeframe_weight"]
                    + min(2.0, vol_weight)
                    + (0.5 * flip_count if flip else 0.0)
                    - recency_penalty
                )

                lvl.update({
                    "strength_raw": raw_strength,
                    "touch_count": touch_count,
                    "volume_weight": round(vol_weight, 4),
                    "last_touch_ms": int(last_touch_ts) if last_touch_ts else 0,
                    "flip": bool(flip),
                    "flip_count": int(flip_count),
                })
                strength_vals.append(raw_strength)

            # Z-score across levels
            if strength_vals:
                mean = sum(strength_vals) / len(strength_vals)
                var = sum((x - mean) ** 2 for x in strength_vals) / max(len(strength_vals) - 1, 1)
                std = var ** 0.5
            else:
                mean = 0.0
                std = 0.0

            for lvl in levels:
                if std > 0:
                    lvl["strength_z"] = (lvl["strength_raw"] - mean) / std
                else:
                    lvl["strength_z"] = 0.0
                lvl["strength_z"] = round(lvl["strength_z"], 4)
                lvl["strength_raw"] = round(lvl["strength_raw"], 4)

            # Nearest support/resistance
            supports = [l for l in levels if l["price"] < price]
            resistances = [l for l in levels if l["price"] > price]
            nearest_support = max(supports, key=lambda l: l["strength_z"], default=None)
            nearest_resistance = max(resistances, key=lambda l: l["strength_z"], default=None)

            signal_dir = "NEUTRAL"
            signal_z = 0.0
            if nearest_support and (price - nearest_support["price"]) / price <= 0.005 and nearest_support["strength_z"] >= 2.0:
                signal_dir = "LONG"
                signal_z = nearest_support["strength_z"]
            elif nearest_resistance and (nearest_resistance["price"] - price) / price <= 0.005 and nearest_resistance["strength_z"] >= 2.0:
                signal_dir = "SHORT"
                signal_z = -nearest_resistance["strength_z"]

            output["symbols"][symbol] = {
                "price": round(price, 6),
                "nearest": {
                    "support": nearest_support and {
                        "price": round(nearest_support["price"], 6),
                        "strength_z": nearest_support["strength_z"],
                        "sources": nearest_support.get("sources", []),
                    },
                    "resistance": nearest_resistance and {
                        "price": round(nearest_resistance["price"], 6),
                        "strength_z": nearest_resistance["strength_z"],
                        "sources": nearest_resistance.get("sources", []),
                    },
                },
                "levels": levels,
                "signal": {"direction": signal_dir, "z_score": round(signal_z, 4)},
            }
        except Exception as e:
            log.warning(f"Error processing {symbol}: {e}")
            continue

    write_json_atomic(OUTPUT_FILE, output)
    log.info(f"Wrote S/R levels for {len(output['symbols'])} symbols")


async def main() -> None:
    log = _get_logger()
    interval_sec = 300
    while True:
        started = time.time()
        try:
            await run_once()
        except Exception as e:
            log.error(f"sr_levels_worker error: {e}")
        elapsed = time.time() - started
        await asyncio.sleep(max(0, interval_sec - elapsed))


if __name__ == "__main__":
    asyncio.run(main())
