#!/usr/bin/env python3
"""
Shared helpers for signal workers.
"""

import json
import os
from collections import deque
from typing import Deque, List, Optional


def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def write_json_atomic(path: str, data: dict) -> None:
    tmp_path = f"{path}.tmp"
    with open(tmp_path, "w") as f:
        json.dump(data, f, indent=2)
    os.replace(tmp_path, path)


def load_json(path: str) -> Optional[dict]:
    try:
        with open(path, "r") as f:
            return json.load(f)
    except Exception:
        return None


class RollingZScore:
    """Rolling z-score calculator with configurable window."""

    # Minimum samples required for valid z-score
    MIN_SAMPLES = 10

    def __init__(self, maxlen: int = 120):
        self.history: Deque[float] = deque(maxlen=maxlen)

    def add(self, value: float) -> Optional[float]:
        """Add value and return z-score, or None if insufficient data."""
        self.history.append(value)
        if len(self.history) < self.MIN_SAMPLES:
            return None  # Caller should handle None (insufficient data)
        mean = sum(self.history) / len(self.history)
        variance = sum((x - mean) ** 2 for x in self.history) / (len(self.history) - 1)
        std = variance ** 0.5
        if std == 0:
            return 0.0
        return (value - mean) / std



def compute_atr_wilder(candles: List[dict], period: int = 14) -> Optional[float]:
    """Compute ATR with Wilder smoothing from candle list.

    Candles must have 'h', 'l', 'c' keys. Reuses logic from atr_service.py.
    This is the SINGLE source of truth for ATR calculation in signal workers.
    """
    if not candles or len(candles) < period + 2:
        return None

    trs = []
    for i in range(1, len(candles)):
        high = float(candles[i]["h"])
        low = float(candles[i]["l"])
        prev_close = float(candles[i - 1]["c"])
        tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
        trs.append(tr)

    if len(trs) < period + 1:
        return None

    # Wilder smoothing (same as atr_service.py:437-439)
    atr = sum(trs[:period]) / float(period)
    for tr in trs[period:]:
        atr = (atr * (period - 1) + tr) / float(period)

    return float(atr)
