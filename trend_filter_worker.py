#!/usr/bin/env python3
"""
Trend Filter Worker

Computes a composite trend score (-100 to 100) from:
- EMA 9/21 (1h + 4h)
- Supertrend (ATR-based)
- ADX + DI
- Price vs VWAP
- MTF alignment

Uses Binance Futures 1h candles (HL fallback).
"""

import asyncio
import logging
import os
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

import aiohttp

from candle_fetcher import fetch_1h_candles_for_symbols, aggregate_candles
from signal_utils import RollingZScore, ensure_dir, write_json_atomic
from universe_cache import fetch_hl_symbols, load_dual_venue_symbols
from env_utils import EVCLAW_SIGNALS_DIR

SIGNALS_DIR = EVCLAW_SIGNALS_DIR
OUTPUT_FILE = os.path.join(SIGNALS_DIR, "trend_state.json")

EMA_FAST = 9
EMA_SLOW = 21
ADX_PERIOD = 14
SUPER_ATR = 10
SUPER_MULT = 3.0
SESSION_HOURS = 8

CANDLE_CONCURRENCY = 10
CANDLE_TIMEOUT_SEC = 10


def _get_logger() -> logging.Logger:
    logger = logging.getLogger("trend_filter_worker")
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


def _ema(values: List[float], period: int) -> Optional[float]:
    """EMA with proper SMA seed (fixes CRITICAL-3)."""
    if len(values) < period:
        return None
    k = 2 / (period + 1)
    # FIX: Use SMA of first `period` values as seed (standard EMA initialization)
    ema = sum(values[:period]) / period
    for v in values[period:]:
        ema = v * k + ema * (1 - k)
    return ema




def _supertrend(candles: List[dict], period: int, mult: float) -> Tuple[int, float]:
    """Supertrend with rolling ATR per bar (fixes CRITICAL-1).

    Returns (trend_direction, final_atr) where:
    - trend_direction: 1 (bullish), -1 (bearish), 0 (insufficient data)
    - final_atr: ATR at the last bar
    """
    if len(candles) < period + 2:
        return 0, 0.0

    # Pre-compute TR for all bars
    trs = [0.0]  # First bar has no TR
    for i in range(1, len(candles)):
        high = float(candles[i]["h"])
        low = float(candles[i]["l"])
        prev_close = float(candles[i - 1]["c"])
        tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
        trs.append(tr)

    # Compute rolling ATR using Wilder smoothing
    atrs = [0.0] * len(candles)
    if len(trs) >= period + 1:
        # Seed with SMA
        atrs[period] = sum(trs[1:period + 1]) / period
        # Wilder smoothing for subsequent bars
        for i in range(period + 1, len(candles)):
            atrs[i] = (atrs[i - 1] * (period - 1) + trs[i]) / period

    final_upper = 0.0
    final_lower = 0.0
    trend = 1

    for i in range(period + 1, len(candles)):
        c = candles[i]
        prev = candles[i - 1]
        atr = atrs[i]
        if atr <= 0:
            continue

        hl2 = (float(c["h"]) + float(c["l"])) / 2
        upper = hl2 + mult * atr
        lower = hl2 - mult * atr

        if i == period + 1:
            final_upper = upper
            final_lower = lower
            trend = 1
            continue

        # FIX: Correct band update logic (standard Supertrend)
        # Upper band: keep previous if new is higher, or if prev close broke above
        if upper < final_upper or float(prev["c"]) > final_upper:
            final_upper = upper

        # Lower band: keep previous if new is lower, or if prev close broke below
        if lower > final_lower or float(prev["c"]) < final_lower:
            final_lower = lower

        # Trend change detection
        close = float(c["c"])
        if trend == -1 and close > final_upper:
            trend = 1
        elif trend == 1 and close < final_lower:
            trend = -1

    final_atr = atrs[-1] if atrs else 0.0
    return trend, final_atr


def _adx(candles: List[dict], period: int) -> Tuple[float, float, float]:
    if len(candles) < period + 2:
        return 0.0, 0.0, 0.0
    plus_dm = []
    minus_dm = []
    tr_list = []
    for i in range(1, len(candles)):
        high = candles[i]["h"]
        low = candles[i]["l"]
        prev = candles[i - 1]
        up_move = high - prev["h"]
        down_move = prev["l"] - low
        plus = up_move if up_move > down_move and up_move > 0 else 0.0
        minus = down_move if down_move > up_move and down_move > 0 else 0.0
        plus_dm.append(plus)
        minus_dm.append(minus)
        tr = max(high - low, abs(high - prev["c"]), abs(low - prev["c"]))
        tr_list.append(tr)

    # Wilder smoothing
    def _smoothed(values: List[float]) -> List[float]:
        if len(values) < period:
            return []
        smoothed = [sum(values[:period])]
        for i in range(period, len(values)):
            prev = smoothed[-1]
            smoothed.append(prev - (prev / period) + values[i])
        return smoothed

    tr_s = _smoothed(tr_list)
    plus_s = _smoothed(plus_dm)
    minus_s = _smoothed(minus_dm)
    if not tr_s or len(plus_s) != len(tr_s) or len(minus_s) != len(tr_s):
        return 0.0, 0.0, 0.0

    di_plus = [100 * (p / t) if t > 0 else 0.0 for p, t in zip(plus_s, tr_s)]
    di_minus = [100 * (m / t) if t > 0 else 0.0 for m, t in zip(minus_s, tr_s)]
    dx = []
    for p, m in zip(di_plus, di_minus):
        denom = p + m
        dx.append(100 * abs(p - m) / denom if denom > 0 else 0.0)
    if len(dx) < period:
        return 0.0, 0.0, 0.0
    adx = sum(dx[-period:]) / period
    return adx, di_plus[-1], di_minus[-1]


def _vwap(candles: List[dict]) -> float:
    if not candles:
        return 0.0
    total = 0.0
    vtotal = 0.0
    for c in candles:
        tp = (c["h"] + c["l"] + c["c"]) / 3
        total += tp * c["v"]
        vtotal += c["v"]
    return total / vtotal if vtotal > 0 else 0.0


def _regime(score: float) -> Tuple[str, str]:
    if score >= 60:
        return "LONG_ONLY", "LONG"
    if score >= 20:
        return "LONG_PREFERRED", "LONG"
    if score <= -60:
        return "SHORT_ONLY", "SHORT"
    if score <= -20:
        return "SHORT_PREFERRED", "SHORT"
    return "CHOPPY", "NEUTRAL"


async def run_once(zscore_state: Dict[str, RollingZScore]) -> None:
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

    candles_by_symbol = await fetch_1h_candles_for_symbols(
        symbols,
        hours_back=200,
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
            if len(candles_1h) < 30:
                continue
            price = float(candles_1h[-1]["c"])
            if price <= 0:
                continue

            symbols_seen.add(symbol)

            candles_4h = aggregate_candles(candles_1h, 4, include_partial=True)
            closes_1h = [float(c["c"]) for c in candles_1h]
            closes_4h = [float(c["c"]) for c in candles_4h] if candles_4h else []

            ema9_1h = _ema(closes_1h, EMA_FAST)
            ema21_1h = _ema(closes_1h, EMA_SLOW)
            ema9_4h = _ema(closes_4h, EMA_FAST) if closes_4h else None
            ema21_4h = _ema(closes_4h, EMA_SLOW) if closes_4h else None

            sign_1h = 1 if ema9_1h and ema21_1h and ema9_1h > ema21_1h else -1 if ema9_1h and ema21_1h and ema9_1h < ema21_1h else 0
            sign_4h = 1 if ema9_4h and ema21_4h and ema9_4h > ema21_4h else -1 if ema9_4h and ema21_4h and ema9_4h < ema21_4h else 0

            if sign_1h == sign_4h and sign_1h != 0:
                ema_score = sign_1h
            elif sign_1h != 0:
                ema_score = 0.5 * sign_1h
            else:
                ema_score = 0.0

            st_1h, atr_1h = _supertrend(candles_1h, SUPER_ATR, SUPER_MULT)
            st_4h, atr_4h = _supertrend(candles_4h, SUPER_ATR, SUPER_MULT) if candles_4h else (0, 0.0)
            if st_1h == st_4h and st_1h != 0:
                super_score = st_1h
            elif st_1h != 0:
                super_score = 0.5 * st_1h
            else:
                super_score = 0.0

            adx, di_plus, di_minus = _adx(candles_1h, ADX_PERIOD)
            if adx >= 25:
                adx_score = 1 if di_plus > di_minus else -1 if di_minus > di_plus else 0
            else:
                adx_score = 0.0

            vwap_val = _vwap(candles_1h[-SESSION_HOURS:] if len(candles_1h) >= 8 else candles_1h)
            vwap_score = 1 if price > vwap_val else -1 if price < vwap_val else 0.0

            mtf_score = 0.0
            if sign_1h == sign_4h and sign_1h != 0:
                mtf_score = sign_1h

            composite = (
                0.25 * ema_score +
                0.20 * super_score +
                0.20 * adx_score +
                0.20 * vwap_score +
                0.15 * mtf_score
            )
            trend_score = max(-100, min(100, composite * 100))

            zcalc = zscore_state.setdefault(symbol, RollingZScore(maxlen=240))
            z_raw = zcalc.add(trend_score)
            # FIX HIGH-3: Handle cold start - use 0.0 if None (insufficient data)
            z = z_raw if z_raw is not None else 0.0

            regime, direction = _regime(trend_score)

            output["symbols"][symbol] = {
                "trend_score": round(trend_score, 2),
                "regime": regime,
                "direction": direction,
                "components": {
                    "ema": {
                        "score": round(ema_score, 4),
                        "ema_1h": [round(ema9_1h, 6) if ema9_1h else 0.0, round(ema21_1h, 6) if ema21_1h else 0.0],
                        "ema_4h": [round(ema9_4h, 6) if ema9_4h else 0.0, round(ema21_4h, 6) if ema21_4h else 0.0],
                    },
                    "supertrend": {
                        "score": round(super_score, 4),
                        "trend": "LONG" if st_1h > 0 else "SHORT" if st_1h < 0 else "NEUTRAL",
                        "atr": round(atr_1h, 6),
                    },
                    "adx": {
                        "score": round(adx_score, 4),
                        "adx": round(adx, 4),
                        "di_plus": round(di_plus, 4),
                        "di_minus": round(di_minus, 4),
                    },
                    "vwap": {
                        "score": round(vwap_score, 4),
                        "vwap": round(vwap_val, 6),
                        "price": round(price, 6),
                    },
                    "mtf": {"score": round(mtf_score, 4)},
                },
                "signal": {
                    "direction": direction,
                    "z_score": round(z, 4),
                    "z_valid": z_raw is not None,  # Indicates if z-score has enough history
                },
            }
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
    log.info(f"Wrote trend state for {len(output['symbols'])} symbols")


async def main() -> None:
    log = _get_logger()
    zscore_state: Dict[str, RollingZScore] = {}
    interval_sec = 60
    while True:
        started = time.time()
        try:
            await run_once(zscore_state)
        except Exception as e:
            log.error(f"trend_filter_worker error: {e}")
        elapsed = time.time() - started
        await asyncio.sleep(max(0, interval_sec - elapsed))


if __name__ == "__main__":
    asyncio.run(main())
