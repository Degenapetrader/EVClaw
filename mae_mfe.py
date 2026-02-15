#!/usr/bin/env python3
"""MAE/MFE computation helpers.

Lightweight runtime module used by fill reconciler close callbacks.
"""

from __future__ import annotations

import asyncio
import sqlite3
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import aiohttp

from ai_trader_db import AITraderDB
from env_utils import env_str

_HTTP_SESSION: Optional[aiohttp.ClientSession] = None
_HTTP_SESSION_LOOP_ID: Optional[int] = None
_HTTP_SESSION_TIMEOUT_SEC: float = 0.0
MAE_MFE_RETRIES = 3
MAE_MFE_HTTP_TIMEOUT_SEC = 12.0
MAE_MFE_BACKOFF_BASE_SEC = 0.6


@dataclass
class TradeWindow:
    trade_id: int
    symbol: str
    direction: str
    entry_price: float
    entry_time: float
    exit_time: float
    mae_pct: Optional[float]
    mfe_pct: Optional[float]


def _coin_for_symbol(symbol: str) -> str:
    s = str(symbol or "").strip()
    if ":" in s:
        return s.split(":", 1)[1].strip().upper()
    return s.upper()


def _load_trade_window(db_path: str, trade_id: int) -> Optional[TradeWindow]:
    conn = sqlite3.connect(db_path, timeout=30.0)
    conn.execute("PRAGMA busy_timeout=30000")
    conn.row_factory = sqlite3.Row
    try:
        row = conn.execute(
            """
            SELECT id, symbol, direction, entry_price, entry_time, exit_time, mae_pct, mfe_pct
            FROM trades
            WHERE id = ?
            """,
            (int(trade_id),),
        ).fetchone()
    finally:
        conn.close()

    if not row:
        return None
    try:
        entry_price = float(row["entry_price"] or 0.0)
        entry_time = float(row["entry_time"] or 0.0)
        exit_time = float(row["exit_time"] or 0.0)
    except Exception:
        return None
    if entry_price <= 0 or entry_time <= 0 or exit_time <= 0:
        return None

    return TradeWindow(
        trade_id=int(row["id"]),
        symbol=str(row["symbol"] or ""),
        direction=str(row["direction"] or "").upper(),
        entry_price=entry_price,
        entry_time=entry_time,
        exit_time=exit_time,
        mae_pct=row["mae_pct"],
        mfe_pct=row["mfe_pct"],
    )


async def _get_http_session(timeout_sec: float) -> aiohttp.ClientSession:
    """Return a loop-local reusable aiohttp session."""
    global _HTTP_SESSION, _HTTP_SESSION_LOOP_ID, _HTTP_SESSION_TIMEOUT_SEC
    loop_id = id(asyncio.get_running_loop())
    if (
        _HTTP_SESSION is not None
        and not _HTTP_SESSION.closed
        and _HTTP_SESSION_LOOP_ID == loop_id
        and abs(float(_HTTP_SESSION_TIMEOUT_SEC) - float(timeout_sec)) < 1e-9
    ):
        return _HTTP_SESSION

    if _HTTP_SESSION is not None and not _HTTP_SESSION.closed:
        try:
            await _HTTP_SESSION.close()
        except Exception:
            pass

    _HTTP_SESSION = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=float(timeout_sec)))
    _HTTP_SESSION_LOOP_ID = loop_id
    _HTTP_SESSION_TIMEOUT_SEC = float(timeout_sec)
    return _HTTP_SESSION


async def close_http_session() -> None:
    """Close module-level HTTP session (used by tests/shutdown hooks)."""
    global _HTTP_SESSION, _HTTP_SESSION_LOOP_ID, _HTTP_SESSION_TIMEOUT_SEC
    if _HTTP_SESSION is not None and not _HTTP_SESSION.closed:
        try:
            await _HTTP_SESSION.close()
        except Exception:
            pass
    _HTTP_SESSION = None
    _HTTP_SESSION_LOOP_ID = None
    _HTTP_SESSION_TIMEOUT_SEC = 0.0


async def _fetch_candles(
    symbol: str,
    start_ms: int,
    end_ms: int,
    interval: str = "1m",
) -> List[Dict[str, Any]]:
    url = (
        env_str(
            "EVCLAW_MAE_MFE_INFO_URL",
            "https://api.hyperliquid.xyz/info",
        )
    ).strip()
    coin = _coin_for_symbol(symbol)
    payload = {
        "type": "candleSnapshot",
        "req": {
            "coin": coin,
            "interval": interval,
            "startTime": int(start_ms),
            "endTime": int(end_ms),
        },
    }

    retries = max(1, int(MAE_MFE_RETRIES))
    timeout_sec = max(3.0, float(MAE_MFE_HTTP_TIMEOUT_SEC))
    backoff_base = MAE_MFE_BACKOFF_BASE_SEC

    for attempt in range(1, retries + 1):
        try:
            session = await _get_http_session(timeout_sec)
            async with session.post(url, json=payload) as resp:
                if resp.status != 200:
                    if attempt >= retries:
                        return []
                    await asyncio.sleep(backoff_base * attempt)
                    continue
                raw = await resp.json()
                if not isinstance(raw, list):
                    return []
                out: List[Dict[str, Any]] = []
                for c in raw:
                    if not isinstance(c, dict):
                        continue
                    try:
                        out.append(
                            {
                                "t": int(c.get("t", 0) or 0),
                                "h": float(c.get("h", 0) or 0),
                                "l": float(c.get("l", 0) or 0),
                            }
                        )
                    except Exception:
                        continue
                return out
        except Exception:
            if attempt >= retries:
                return []
            await asyncio.sleep(backoff_base * attempt)
    return []


async def fetch_candles(
    symbol: str,
    start_ms: int,
    end_ms: int,
    interval: str = "1m",
) -> List[Dict[str, Any]]:
    """Public candle fetch wrapper used by learning modules."""
    return await _fetch_candles(symbol=symbol, start_ms=start_ms, end_ms=end_ms, interval=interval)


def compute_mae_mfe(
    *,
    direction: str,
    entry_price: float,
    candles: List[Dict[str, Any]],
) -> Tuple[Optional[float], Optional[float], Optional[float], Optional[float]]:
    if entry_price <= 0 or not candles:
        return None, None, None, None
    lows = [float(c.get("l", 0) or 0) for c in candles if float(c.get("l", 0) or 0) > 0]
    highs = [float(c.get("h", 0) or 0) for c in candles if float(c.get("h", 0) or 0) > 0]
    if not lows or not highs:
        return None, None, None, None
    min_low = min(lows)
    max_high = max(highs)

    d = str(direction or "").upper()
    if d == "SHORT":
        mae_pct = max(0.0, (max_high - entry_price) / entry_price * 100.0)
        mfe_pct = max(0.0, (entry_price - min_low) / entry_price * 100.0)
        mae_price = max_high
        mfe_price = min_low
    else:
        mae_pct = max(0.0, (entry_price - min_low) / entry_price * 100.0)
        mfe_pct = max(0.0, (max_high - entry_price) / entry_price * 100.0)
        mae_price = min_low
        mfe_price = max_high

    return mae_pct, mfe_pct, mae_price, mfe_price


async def compute_and_store_mae_mfe(db_path: str, trade_id: int) -> bool:
    trade = _load_trade_window(db_path, int(trade_id))
    if not trade:
        return False
    # Idempotency: nothing to do if both already present.
    if trade.mae_pct is not None and trade.mfe_pct is not None:
        return True

    start_ms = int(max(0.0, trade.entry_time) * 1000)
    end_ms = int(max(trade.entry_time + 60.0, trade.exit_time) * 1000)
    candles = await _fetch_candles(trade.symbol, start_ms, end_ms)
    mae, mfe, mae_price, mfe_price = compute_mae_mfe(
        direction=trade.direction,
        entry_price=trade.entry_price,
        candles=candles,
    )
    if mae is None or mfe is None:
        return False

    db = AITraderDB(db_path)
    return db.update_trade_mae_mfe(
        trade_id=trade.trade_id,
        mae_pct=mae,
        mfe_pct=mfe,
        mae_price=mae_price,
        mfe_price=mfe_price,
    )
