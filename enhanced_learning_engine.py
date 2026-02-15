#!/usr/bin/env python3
"""Enhanced learning engine primitives for closed-trade combo tracking.

Reflection generation is handled by the external AGI reflection event pipeline.
This module now focuses only on persistent combo performance aggregation.
"""

import sqlite3
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass
from typing import List, Optional

from logging_utils import get_logger


@dataclass
class ComboStats:
    """Statistics for a signal combination."""

    combo_key: str
    trades: int
    wins: int
    total_pnl: float
    avg_pnl: float
    win_rate: float


class EnhancedLearningEngine:
    """Base learning engine with signal-combo performance tracking."""

    def __init__(self, db_path: str):
        self.db_path = db_path
        self.log = get_logger("enhanced_learning")
        self._conn: Optional[sqlite3.Connection] = None
        self._conn_lock = threading.Lock()

    def _get_persistent_conn(self) -> sqlite3.Connection:
        with self._conn_lock:
            conn = self._conn
            if conn is not None:
                try:
                    conn.execute("SELECT 1")
                    return conn
                except Exception:
                    try:
                        conn.close()
                    except Exception:
                        pass
                    self._conn = None
            conn = sqlite3.connect(self.db_path, timeout=30.0, check_same_thread=False)
            conn.execute("PRAGMA busy_timeout=30000")
            self._conn = conn
            return conn

    def close(self) -> None:
        with self._conn_lock:
            conn = self._conn
            self._conn = None
        if conn is None:
            return
        try:
            conn.close()
        except Exception:
            pass

    def __del__(self) -> None:
        try:
            self.close()
        except Exception:
            pass

    @contextmanager
    def _db_conn(self):
        """Centralized sqlite connection policy for learning reads/writes."""
        yield self._get_persistent_conn()

    def _get_trade(self, trade_id: int) -> Optional[dict]:
        """Get trade from database."""
        with self._db_conn() as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute("SELECT * FROM trades WHERE id = ?", (trade_id,))
            row = cursor.fetchone()
            return dict(row) if row else None

    def update_combo_stats(self, trade_id: int) -> Optional[str]:
        """Update signal combo statistics after trade close."""
        trade = self._get_trade(trade_id)
        if not trade or not trade.get("exit_time"):
            return None

        signals_agreed = []
        raw = trade.get("signals_agreed")
        if raw:
            try:
                import json

                signals_agreed = json.loads(raw)
            except Exception:
                signals_agreed = []

        if not signals_agreed:
            return None

        combo_key = "+".join(sorted(s.lower() for s in signals_agreed))
        pnl = trade.get("realized_pnl", 0) or 0

        with self._db_conn() as conn:
            conn.execute("BEGIN IMMEDIATE")
            win_inc = 1 if pnl > 0 else 0
            now_ts = time.time()
            conn.execute(
                """
                INSERT INTO signal_combos (
                    combo_key, trades, wins, total_pnl, avg_pnl, win_rate, last_trade_at, updated_at
                ) VALUES (?, 1, ?, ?, ?, ?, ?, strftime('%s', 'now'))
                ON CONFLICT(combo_key) DO UPDATE SET
                    trades = signal_combos.trades + 1,
                    wins = signal_combos.wins + excluded.wins,
                    total_pnl = signal_combos.total_pnl + excluded.total_pnl,
                    avg_pnl = (signal_combos.total_pnl + excluded.total_pnl) * 1.0 / (signal_combos.trades + 1),
                    win_rate = (signal_combos.wins + excluded.wins) * 1.0 / (signal_combos.trades + 1),
                    last_trade_at = excluded.last_trade_at,
                    updated_at = strftime('%s', 'now')
                """,
                (
                    combo_key,
                    win_inc,
                    pnl,
                    pnl,
                    1.0 if win_inc else 0.0,
                    now_ts,
                ),
            )
            row = conn.execute(
                "SELECT trades, wins, total_pnl, avg_pnl, win_rate FROM signal_combos WHERE combo_key = ?",
                (combo_key,),
            ).fetchone()
            conn.commit()

        if row:
            trades = int(row[0] or 0)
            win_rate = float(row[4] or 0.0)
        else:
            trades = 0
            win_rate = 0.0

        self.log.debug(f"Updated combo stats: {combo_key} ({trades} trades, {win_rate:.0%} win rate)")
        return combo_key

    def get_combo_stats(self, min_trades: int = 5) -> List[ComboStats]:
        """Get signal combo performance stats sorted by win rate."""
        with self._db_conn() as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute(
                """
                SELECT * FROM signal_combos
                WHERE trades >= ?
                ORDER BY win_rate DESC, trades DESC
                """,
                (min_trades,),
            )
            return [
                ComboStats(
                    combo_key=row["combo_key"],
                    trades=row["trades"],
                    wins=row["wins"],
                    total_pnl=row["total_pnl"],
                    avg_pnl=row["avg_pnl"],
                    win_rate=row["win_rate"],
                )
                for row in cursor.fetchall()
            ]

    async def process_closed_trade(self, trade_id: int) -> None:
        """Process a closed trade for combo learning."""
        self.update_combo_stats(trade_id)
