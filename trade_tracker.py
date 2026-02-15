#!/usr/bin/env python3
"""Trade tracker + SL/TP stats provider for EVClaw.

This module exists primarily to support the *adaptive* parameter layer.

Key requirement:
- `AdaptiveSLTPManager` expects a tracker that can provide per-symbol/per-regime
  SL/TP hit-rate statistics via `get_sltp_stats()`.

Implementation note:
- We derive stats directly from `ai_trader.db` trades (using `protection_snapshot`
  + `exit_reason`). This avoids introducing a second outcomes DB and works with
  existing production data.

The tracker is intentionally read-driven for runtime learning.
"""

from __future__ import annotations

from logging_utils import get_logger
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Optional



@dataclass
class SLTPStats:
    symbol: str
    regime: str
    total_trades: int
    sl_hits: int
    tp_hits: int
    sl_hit_rate: float
    tp_hit_rate: float
    avg_sl_mult: float
    avg_tp_mult: float


class TradeTracker:
    """Provides adaptive-learning stats from the EVClaw SQLite DB."""

    def __init__(
        self,
        *,
        db_path: str,
        memory_dir: Optional[Path] = None,
    ):
        self.db_path = str(db_path)
        self.memory_dir = Path(memory_dir) if memory_dir else Path(__file__).parent / "memory"
        self.memory_dir.mkdir(parents=True, exist_ok=True)
        self.log = get_logger("trade_tracker")
    # -------------------------------------------------------------- SLTP stats
    def get_sltp_stats(
        self,
        *,
        symbol: str,
        regime: str = "NORMAL",
        min_trades: int = 10,
        lookback_days: int = 60,
        include_fallback: bool = False,
    ) -> Optional[SLTPStats]:
        """Compute SL/TP hit rates and average multipliers for a symbol+regime.

        Data source:
        - trades.exit_reason (SL/TP/EXIT)
        - trades.protection_snapshot JSON (sl_mult/tp_mult/volatility_regime/used_fallback)

        Returns None if insufficient samples.
        """
        symbol_u = str(symbol or "").upper()
        regime_u = str(regime or "NORMAL").upper()
        if not symbol_u:
            return None

        cutoff = time.time() - float(lookback_days) * 86400.0

        rows = []
        with sqlite3.connect(self.db_path, timeout=30.0) as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                """
                SELECT exit_reason, protection_snapshot
                FROM trades
                WHERE symbol = ?
                  AND exit_time IS NOT NULL
                  AND exit_time >= ?
                  AND protection_snapshot IS NOT NULL
                  AND protection_snapshot != ''
                ORDER BY exit_time DESC
                """,
                (symbol_u, cutoff),
            ).fetchall()

        total = 0
        sl_hits = 0
        tp_hits = 0
        sl_sum = 0.0
        tp_sum = 0.0

        for r in rows:
            try:
                ps = json.loads(r["protection_snapshot"])
            except Exception:
                continue

            ps_regime = str(ps.get("volatility_regime") or "").upper()
            if ps_regime and ps_regime != regime_u:
                continue

            used_fallback = bool(ps.get("used_fallback", False))
            if used_fallback and not include_fallback:
                continue

            sl_mult = ps.get("sl_mult")
            tp_mult = ps.get("tp_mult")
            try:
                sl_mult_f = float(sl_mult)
                tp_mult_f = float(tp_mult)
            except (TypeError, ValueError):
                # Overrides or malformed snapshots can have None multipliers.
                continue

            if sl_mult_f <= 0 or tp_mult_f <= 0:
                continue

            exit_reason = str(r["exit_reason"] or "").upper()

            total += 1
            sl_sum += sl_mult_f
            tp_sum += tp_mult_f

            if exit_reason == "SL":
                sl_hits += 1
            elif exit_reason == "TP":
                tp_hits += 1

        if total < int(min_trades):
            return None

        avg_sl = sl_sum / total if total else 0.0
        avg_tp = tp_sum / total if total else 0.0

        return SLTPStats(
            symbol=symbol_u,
            regime=regime_u,
            total_trades=total,
            sl_hits=sl_hits,
            tp_hits=tp_hits,
            sl_hit_rate=(sl_hits / total) if total else 0.0,
            tp_hit_rate=(tp_hits / total) if total else 0.0,
            avg_sl_mult=avg_sl,
            avg_tp_mult=avg_tp,
        )


__all__ = ["TradeTracker", "SLTPStats"]
