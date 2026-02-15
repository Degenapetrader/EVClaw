#!/usr/bin/env python3
"""
Volume Profile Worker

Computes volume profiles for session (8h), daily (24h), and weekly (7d)
using Binance Futures 1h candles (HL fallback). Writes JSON snapshot.
"""

import asyncio
import logging
import math
import os
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional

import aiohttp

from env_utils import env_float, env_int

from candle_fetcher import fetch_1h_candles_for_symbols, resample_timeframes
from signal_utils import RollingZScore, ensure_dir, write_json_atomic
from universe_cache import fetch_hl_symbols, load_dual_venue_symbols
from env_utils import EVCLAW_SIGNALS_DIR

SIGNALS_DIR = EVCLAW_SIGNALS_DIR
OUTPUT_FILE = os.path.join(SIGNALS_DIR, "volume_profile.json")

SESSION_HOURS = env_int("EVCLAW_VOLUME_PROFILE_SESSION_HOURS", 8)
DAILY_HOURS = 24
WEEKLY_HOURS = env_int("EVCLAW_VOLUME_PROFILE_WEEKLY_HOURS", 168)

DEFAULT_BIN_PCT = 0.001  # 0.10%
VALUE_AREA_PCT = env_float("EVCLAW_VOLUME_PROFILE_VALUE_AREA_PCT", 0.70)
MAX_BINS = 10000  # FIX HIGH-4: Cap bin count to prevent memory explosion

CANDLE_CONCURRENCY = 10
CANDLE_TIMEOUT_SEC = 10


def _get_logger() -> logging.Logger:
    logger = logging.getLogger("volume_profile_worker")
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


def _compute_histogram(candles: List[dict], bin_size: float) -> Optional[Dict[str, object]]:
    if not candles or bin_size <= 0:
        return None
    low = min(c["l"] for c in candles)
    high = max(c["h"] for c in candles)
    if high <= low:
        return None

    bins = int(math.ceil((high - low) / bin_size))
    bins = max(bins, 1)
    bins = min(bins, MAX_BINS)  # FIX HIGH-4: Prevent memory explosion for micro-cap tokens
    volumes = [0.0 for _ in range(bins)]

    for c in candles:
        c_low = c["l"]
        c_high = c["h"]
        if c_high < c_low:
            continue
        start_idx = int((c_low - low) / bin_size)
        end_idx = int((c_high - low) / bin_size)
        start_idx = max(0, min(start_idx, bins - 1))
        end_idx = max(0, min(end_idx, bins - 1))
        count = end_idx - start_idx + 1
        if count <= 0:
            continue
        vol_per = c["v"] / count
        for i in range(start_idx, end_idx + 1):
            volumes[i] += vol_per

    total_volume = sum(volumes)
    if total_volume <= 0:
        return None
    return {
        "low": low,
        "high": high,
        "bin_size": bin_size,
        "volumes": volumes,
        "total_volume": total_volume,
    }


def _price_at_bin(low: float, bin_size: float, idx: int) -> float:
    return low + (idx + 0.5) * bin_size


def _compute_profile(candles: List[dict], price: float, bin_size: float) -> Optional[Dict[str, object]]:
    hist = _compute_histogram(candles, bin_size)
    if not hist:
        return None

    volumes = hist["volumes"]
    low = hist["low"]
    bin_size = hist["bin_size"]
    total_volume = hist["total_volume"]

    poc_idx = max(range(len(volumes)), key=lambda i: volumes[i])
    poc = _price_at_bin(low, bin_size, poc_idx)

    # Value area: accumulate bins by volume until threshold
    sorted_idxs = sorted(range(len(volumes)), key=lambda i: volumes[i], reverse=True)
    target = total_volume * VALUE_AREA_PCT
    acc = 0.0
    selected = []
    for idx in sorted_idxs:
        acc += volumes[idx]
        selected.append(idx)
        if acc >= target:
            break
    vah_idx = max(selected) if selected else poc_idx
    val_idx = min(selected) if selected else poc_idx
    vah = _price_at_bin(low, bin_size, vah_idx)
    val = _price_at_bin(low, bin_size, val_idx)

    # HVN/LVN via smoothed local extrema (FIX HIGH-2: correct indexing)
    hvn = []
    lvn = []
    if len(volumes) >= 5:  # Need at least 5 bins for meaningful smoothing
        # Build smooth array: smooth[j] corresponds to volumes[j+1]
        smooth = []
        for i in range(1, len(volumes) - 1):
            smooth.append((volumes[i - 1] + volumes[i] + volumes[i + 1]) / 3.0)

        # Iterate over smooth array indices (not volumes indices)
        for j in range(1, len(smooth) - 1):
            s_prev = smooth[j - 1]
            s_curr = smooth[j]
            s_next = smooth[j + 1]
            # Original bin index is j + 1 (since smooth[j] corresponds to volumes[j+1])
            orig_bin_idx = j + 1
            if s_curr > s_prev and s_curr > s_next:
                hvn.append((orig_bin_idx, volumes[orig_bin_idx]))
            if s_curr < s_prev and s_curr < s_next:
                lvn.append((orig_bin_idx, volumes[orig_bin_idx]))

    hvn_sorted = sorted(hvn, key=lambda x: x[1], reverse=True)[:3]
    lvn_sorted = sorted(lvn, key=lambda x: x[1])[:3]

    return {
        "poc": round(poc, 6),
        "vah": round(vah, 6),
        "val": round(val, 6),
        "hvn": [{"price": round(_price_at_bin(low, bin_size, i), 6), "volume": round(v, 4)} for i, v in hvn_sorted],
        "lvn": [{"price": round(_price_at_bin(low, bin_size, i), 6), "volume": round(v, 4)} for i, v in lvn_sorted],
        "total_volume": round(total_volume, 4),
        "distance_to_poc": round((price - poc) / poc if poc else 0.0, 6),
    }


async def run_once(zscore_state: Dict[str, Dict[str, RollingZScore]]) -> None:
    log = _get_logger()
    ensure_dir(SIGNALS_DIR)

    timeout = aiohttp.ClientTimeout(total=10)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        try:
            symbols = await load_dual_venue_symbols(session=session)
        except Exception as exc:
            log.warning(f"Universe cache unavailable ({exc}); falling back to HL meta")
            symbols = await fetch_hl_symbols(session)
        if not symbols:
            log.warning("No symbols found from HL meta")
            return

    hours_back = WEEKLY_HOURS
    candles_by_symbol = await fetch_1h_candles_for_symbols(
        symbols,
        hours_back=hours_back,
        concurrency=CANDLE_CONCURRENCY,
        timeout_sec=CANDLE_TIMEOUT_SEC,
    )

    output = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "symbols": {},
    }

    # Track symbols seen this cycle for pruning (fixes HIGH-1: memory leak)
    symbols_seen = set()

    for symbol, payload in candles_by_symbol.items():
        # FIX CRITICAL-2: Per-symbol error isolation
        try:
            candles_1h = payload.get("candles") or []
            if len(candles_1h) < 10:
                continue
            price = float(candles_1h[-1]["c"])
            if price <= 0:
                continue

            symbols_seen.add(symbol)

            bin_size = max(price * DEFAULT_BIN_PCT, price * 1e-6)
            tf_candles = resample_timeframes(candles_1h)

            tf_output = {}
            for tf_name, candles in tf_candles.items():
                if not candles:
                    continue
                last = candles[-1]
                window_hours = SESSION_HOURS if tf_name == "session_8h" else DAILY_HOURS if tf_name == "daily_24h" else WEEKLY_HOURS
                status = "developing" if not last.get("complete", True) else "fixed"
                profile = _compute_profile(candles, price, bin_size)
                if not profile:
                    continue

                zscores = zscore_state.setdefault(symbol, {})
                zcalc = zscores.setdefault(tf_name, RollingZScore(maxlen=240))
                z_raw = zcalc.add(profile["distance_to_poc"])
                # FIX HIGH-3: Handle cold start - use 0.0 if None
                z = z_raw if z_raw is not None else 0.0
                profile["distance_to_poc_z"] = round(z, 4)
                profile["signal"] = {
                    "direction": "LONG" if z >= 2.0 else "SHORT" if z <= -2.0 else "NEUTRAL",
                    "z_score": round(z, 4),
                    "z_valid": z_raw is not None,  # Indicates if z-score has enough history
                }
                profile["status"] = status
                profile["start_ms"] = int(candles[0]["t"])
                profile["end_ms"] = int(last["t"] + window_hours * 3600 * 1000)
                tf_output[tf_name] = profile

            if tf_output:
                output["symbols"][symbol] = {"timeframes": tf_output}
        except Exception as e:
            log.warning(f"Error processing {symbol}: {e}")
            continue

    # FIX HIGH-1: Prune zscore_state for symbols no longer in universe
    stale_symbols = set(zscore_state.keys()) - symbols_seen
    for sym in stale_symbols:
        del zscore_state[sym]
    if stale_symbols:
        log.debug(f"Pruned {len(stale_symbols)} stale symbols from zscore_state")

    write_json_atomic(OUTPUT_FILE, output)
    log.info(f"Wrote volume profile for {len(output['symbols'])} symbols")


async def main() -> None:
    log = _get_logger()
    zscore_state: Dict[str, Dict[str, RollingZScore]] = {}
    interval_sec = 300

    while True:
        started = time.time()
        try:
            await run_once(zscore_state)
        except Exception as e:
            log.error(f"volume_profile_worker error: {e}")
        elapsed = time.time() - started
        sleep_for = max(0, interval_sec - elapsed)
        await asyncio.sleep(sleep_for)


if __name__ == "__main__":
    asyncio.run(main())
