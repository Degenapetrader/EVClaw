#!/usr/bin/env python3
"""Unified symbol-learning dossier aggregation.

This module maintains a per-symbol SQLite dossier that fuses:
- closed-trade performance (win rate / expectancy / avg pnl%)
- MAE/MFE summaries
- SL/TP behavior summaries
- signal and combo rankings (instrumented trades only)
- reflection lesson notes
"""

from __future__ import annotations

import json
import math
import os
import sqlite3
import time
import threading
from logging_utils import get_logger
from typing import Any, Dict, List, Optional, Tuple

from ai_trader_db import AITraderDB


DEFAULT_NOTES_CAP = 1200
DEFAULT_SNIPPET_MAX_CHARS = 1200
RECENCY_WINDOW_SEC = 30 * 86400
_INIT_CACHE: set[str] = set()
_REFLECTION_REFRESH_TS: Dict[str, float] = {}
_STATE_LOCK = threading.RLock()
_LOG = get_logger("learning_dossier")


def _as_float(value: Any) -> Optional[float]:
    try:
        if value is None:
            return None
        return float(value)
    except Exception:
        return None


def _safe_json(raw: Any, default: Any) -> Any:
    if raw is None:
        return default
    if isinstance(raw, (dict, list)):
        return raw
    try:
        return json.loads(str(raw))
    except Exception:
        return default


def _mean(values: List[float]) -> float:
    if not values:
        return 0.0
    return sum(values) / float(len(values))


def _percentile(values: List[float], pct: float) -> float:
    if not values:
        return 0.0
    arr = sorted(float(v) for v in values)
    if len(arr) == 1:
        return arr[0]
    p = max(0.0, min(1.0, float(pct)))
    rank = (len(arr) - 1) * p
    lo = int(math.floor(rank))
    hi = int(math.ceil(rank))
    if lo == hi:
        return arr[lo]
    frac = rank - lo
    return arr[lo] + ((arr[hi] - arr[lo]) * frac)


def _expectancy(values: List[float]) -> float:
    if not values:
        return 0.0
    wins = [v for v in values if v > 0]
    losses = [abs(v) for v in values if v < 0]
    win_rate = (len(wins) / float(len(values))) if values else 0.0
    avg_win = _mean(wins) if wins else 0.0
    avg_loss = _mean(losses) if losses else 0.0
    return (win_rate * avg_win) - ((1.0 - win_rate) * avg_loss)


def _normalize_signal_name(signal: Any) -> Optional[str]:
    if isinstance(signal, dict):
        cand = str(signal.get("name") or signal.get("signal") or "").strip()
    else:
        cand = str(signal or "").strip()
    if not cand:
        return None
    head = cand.split(":", 1)[0].strip().upper()
    if not head:
        return None
    cleaned = "".join(ch for ch in head if (ch.isalnum() or ch in {"_", "-"}))
    return cleaned or None


def _extract_reflection_lesson(lesson_text: Any, reflection_json: Any) -> str:
    lesson = str(lesson_text or "").strip()
    if lesson:
        return " ".join(lesson.split())

    data = _safe_json(reflection_json, default=None)
    if isinstance(data, dict):
        for key in ("lesson_text", "lesson", "takeaway", "summary"):
            txt = str(data.get(key) or "").strip()
            if txt:
                return " ".join(txt.split())
        lessons = data.get("lessons")
        if isinstance(lessons, list):
            joined = " | ".join(str(x).strip() for x in lessons if str(x).strip())
            if joined:
                return " ".join(joined.split())
    if isinstance(data, list):
        joined = " | ".join(str(x).strip() for x in data if str(x).strip())
        if joined:
            return " ".join(joined.split())
    return ""


def _cap_text(text: str, max_chars: int) -> str:
    s = str(text or "")
    n = max(64, int(max_chars))
    if len(s) <= n:
        return s
    if n <= 3:
        return s[:n]
    return s[: n - 3] + "..."


def _ensure_db(db_path: str) -> str:
    resolved = str(db_path or "")
    if not resolved:
        resolved = "ai_trader.db"
    input_key = resolved
    with _STATE_LOCK:
        needs_init = input_key not in _INIT_CACHE and resolved not in _INIT_CACHE
    if needs_init:
        db = AITraderDB(resolved)
        resolved = str(db.db_path)
        with _STATE_LOCK:
            _INIT_CACHE.add(input_key)
            _INIT_CACHE.add(resolved)
    return resolved


def _expectancy_from_parts(
    *,
    n_total: int,
    pos_sum: float,
    pos_count: int,
    neg_abs_sum: float,
    neg_count: int,
) -> float:
    if n_total <= 0:
        return 0.0
    win_rate = (float(pos_count) / float(n_total)) if n_total > 0 else 0.0
    avg_win = (float(pos_sum) / float(pos_count)) if pos_count > 0 else 0.0
    avg_loss = (float(neg_abs_sum) / float(neg_count)) if neg_count > 0 else 0.0
    return (win_rate * avg_win) - ((1.0 - win_rate) * avg_loss)


def _ensure_incremental_tables(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS symbol_learning_rollups (
            symbol TEXT PRIMARY KEY,
            updated_at REAL DEFAULT (strftime('%s', 'now')),
            last_processed_trade_id INTEGER DEFAULT 0,
            n_closed INTEGER DEFAULT 0,
            wins INTEGER DEFAULT 0,
            pnl_count INTEGER DEFAULT 0,
            pnl_sum REAL DEFAULT 0.0,
            pnl_pos_count INTEGER DEFAULT 0,
            pnl_pos_sum REAL DEFAULT 0.0,
            pnl_neg_count INTEGER DEFAULT 0,
            pnl_neg_abs_sum REAL DEFAULT 0.0,
            mae_count INTEGER DEFAULT 0,
            mae_sum REAL DEFAULT 0.0,
            mfe_count INTEGER DEFAULT 0,
            mfe_sum REAL DEFAULT 0.0,
            sltp_rows INTEGER DEFAULT 0,
            sltp_fallback_rows INTEGER DEFAULT 0,
            sl_mult_count INTEGER DEFAULT 0,
            sl_mult_sum REAL DEFAULT 0.0,
            tp_mult_count INTEGER DEFAULT 0,
            tp_mult_sum REAL DEFAULT 0.0
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS symbol_learning_processed (
            symbol TEXT NOT NULL,
            trade_id INTEGER NOT NULL,
            processed_at REAL NOT NULL,
            PRIMARY KEY(symbol, trade_id)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS combo_symbol_stats (
            symbol TEXT NOT NULL,
            combo TEXT NOT NULL,
            direction TEXT NOT NULL,
            n INTEGER DEFAULT 0,
            wins INTEGER DEFAULT 0,
            win_rate REAL DEFAULT 0.0,
            avg_pnl_pct REAL DEFAULT 0.0,
            expectancy REAL DEFAULT 0.0,
            pnl_sum REAL DEFAULT 0.0,
            pnl_pos_count INTEGER DEFAULT 0,
            pnl_pos_sum REAL DEFAULT 0.0,
            pnl_neg_count INTEGER DEFAULT 0,
            pnl_neg_abs_sum REAL DEFAULT 0.0,
            last_updated REAL,
            PRIMARY KEY(symbol, combo, direction)
        )
        """
    )

    sig_cols = {row[1] for row in conn.execute("PRAGMA table_info(signal_symbol_stats)").fetchall()}
    for col, ddl in (
        ("pnl_sum", "ALTER TABLE signal_symbol_stats ADD COLUMN pnl_sum REAL DEFAULT 0.0"),
        ("pnl_pos_count", "ALTER TABLE signal_symbol_stats ADD COLUMN pnl_pos_count INTEGER DEFAULT 0"),
        ("pnl_pos_sum", "ALTER TABLE signal_symbol_stats ADD COLUMN pnl_pos_sum REAL DEFAULT 0.0"),
        ("pnl_neg_count", "ALTER TABLE signal_symbol_stats ADD COLUMN pnl_neg_count INTEGER DEFAULT 0"),
        ("pnl_neg_abs_sum", "ALTER TABLE signal_symbol_stats ADD COLUMN pnl_neg_abs_sum REAL DEFAULT 0.0"),
    ):
        if col not in sig_cols:
            conn.execute(ddl)

    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_symbol_learning_processed_symbol_trade ON symbol_learning_processed(symbol, trade_id)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_combo_symbol_last_updated ON combo_symbol_stats(symbol, last_updated DESC)"
    )


def _update_signal_stat(
    conn: sqlite3.Connection,
    *,
    symbol: str,
    signal: str,
    direction: str,
    pnl_pct: float,
    now_ts: float,
) -> None:
    row = conn.execute(
        """
        SELECT n, wins, pnl_sum, pnl_pos_count, pnl_pos_sum, pnl_neg_count, pnl_neg_abs_sum
        FROM signal_symbol_stats
        WHERE symbol = ? AND signal = ? AND direction = ?
        """,
        (symbol, signal, direction),
    ).fetchone()
    n_prev = int(row["n"] or 0) if row else 0
    wins_prev = int(row["wins"] or 0) if row else 0
    pnl_sum_prev = float(row["pnl_sum"] or 0.0) if row else 0.0
    pos_count_prev = int(row["pnl_pos_count"] or 0) if row else 0
    pos_sum_prev = float(row["pnl_pos_sum"] or 0.0) if row else 0.0
    neg_count_prev = int(row["pnl_neg_count"] or 0) if row else 0
    neg_sum_prev = float(row["pnl_neg_abs_sum"] or 0.0) if row else 0.0

    n_new = n_prev + 1
    wins_new = wins_prev + (1 if pnl_pct > 0 else 0)
    pnl_sum_new = pnl_sum_prev + float(pnl_pct)
    pos_count_new = pos_count_prev + (1 if pnl_pct > 0 else 0)
    pos_sum_new = pos_sum_prev + (float(pnl_pct) if pnl_pct > 0 else 0.0)
    neg_count_new = neg_count_prev + (1 if pnl_pct < 0 else 0)
    neg_sum_new = neg_sum_prev + (abs(float(pnl_pct)) if pnl_pct < 0 else 0.0)

    win_rate_new = (float(wins_new) / float(n_new)) if n_new > 0 else 0.0
    avg_new = (pnl_sum_new / float(n_new)) if n_new > 0 else 0.0
    exp_new = _expectancy_from_parts(
        n_total=n_new,
        pos_sum=pos_sum_new,
        pos_count=pos_count_new,
        neg_abs_sum=neg_sum_new,
        neg_count=neg_count_new,
    )

    conn.execute(
        """
        INSERT INTO signal_symbol_stats (
            symbol, signal, direction, n, wins, win_rate, avg_pnl_pct, expectancy,
            last_updated, pnl_sum, pnl_pos_count, pnl_pos_sum, pnl_neg_count, pnl_neg_abs_sum
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(symbol, signal, direction) DO UPDATE SET
            n = excluded.n,
            wins = excluded.wins,
            win_rate = excluded.win_rate,
            avg_pnl_pct = excluded.avg_pnl_pct,
            expectancy = excluded.expectancy,
            last_updated = excluded.last_updated,
            pnl_sum = excluded.pnl_sum,
            pnl_pos_count = excluded.pnl_pos_count,
            pnl_pos_sum = excluded.pnl_pos_sum,
            pnl_neg_count = excluded.pnl_neg_count,
            pnl_neg_abs_sum = excluded.pnl_neg_abs_sum
        """,
        (
            symbol,
            signal,
            direction,
            n_new,
            wins_new,
            win_rate_new,
            avg_new,
            exp_new,
            now_ts,
            pnl_sum_new,
            pos_count_new,
            pos_sum_new,
            neg_count_new,
            neg_sum_new,
        ),
    )


def _update_combo_stat(
    conn: sqlite3.Connection,
    *,
    symbol: str,
    combo: str,
    direction: str,
    pnl_pct: float,
    now_ts: float,
) -> None:
    row = conn.execute(
        """
        SELECT n, wins, pnl_sum, pnl_pos_count, pnl_pos_sum, pnl_neg_count, pnl_neg_abs_sum
        FROM combo_symbol_stats
        WHERE symbol = ? AND combo = ? AND direction = ?
        """,
        (symbol, combo, direction),
    ).fetchone()
    n_prev = int(row["n"] or 0) if row else 0
    wins_prev = int(row["wins"] or 0) if row else 0
    pnl_sum_prev = float(row["pnl_sum"] or 0.0) if row else 0.0
    pos_count_prev = int(row["pnl_pos_count"] or 0) if row else 0
    pos_sum_prev = float(row["pnl_pos_sum"] or 0.0) if row else 0.0
    neg_count_prev = int(row["pnl_neg_count"] or 0) if row else 0
    neg_sum_prev = float(row["pnl_neg_abs_sum"] or 0.0) if row else 0.0

    n_new = n_prev + 1
    wins_new = wins_prev + (1 if pnl_pct > 0 else 0)
    pnl_sum_new = pnl_sum_prev + float(pnl_pct)
    pos_count_new = pos_count_prev + (1 if pnl_pct > 0 else 0)
    pos_sum_new = pos_sum_prev + (float(pnl_pct) if pnl_pct > 0 else 0.0)
    neg_count_new = neg_count_prev + (1 if pnl_pct < 0 else 0)
    neg_sum_new = neg_sum_prev + (abs(float(pnl_pct)) if pnl_pct < 0 else 0.0)

    win_rate_new = (float(wins_new) / float(n_new)) if n_new > 0 else 0.0
    avg_new = (pnl_sum_new / float(n_new)) if n_new > 0 else 0.0
    exp_new = _expectancy_from_parts(
        n_total=n_new,
        pos_sum=pos_sum_new,
        pos_count=pos_count_new,
        neg_abs_sum=neg_sum_new,
        neg_count=neg_count_new,
    )

    conn.execute(
        """
        INSERT INTO combo_symbol_stats (
            symbol, combo, direction, n, wins, win_rate, avg_pnl_pct, expectancy,
            pnl_sum, pnl_pos_count, pnl_pos_sum, pnl_neg_count, pnl_neg_abs_sum, last_updated
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(symbol, combo, direction) DO UPDATE SET
            n = excluded.n,
            wins = excluded.wins,
            win_rate = excluded.win_rate,
            avg_pnl_pct = excluded.avg_pnl_pct,
            expectancy = excluded.expectancy,
            pnl_sum = excluded.pnl_sum,
            pnl_pos_count = excluded.pnl_pos_count,
            pnl_pos_sum = excluded.pnl_pos_sum,
            pnl_neg_count = excluded.pnl_neg_count,
            pnl_neg_abs_sum = excluded.pnl_neg_abs_sum,
            last_updated = excluded.last_updated
        """,
        (
            symbol,
            combo,
            direction,
            n_new,
            wins_new,
            win_rate_new,
            avg_new,
            exp_new,
            pnl_sum_new,
            pos_count_new,
            pos_sum_new,
            neg_count_new,
            neg_sum_new,
            now_ts,
        ),
    )


def update_from_trade_close(db_path: str, trade_id: int) -> bool:
    """Incrementally update symbol dossier aggregates after a trade closes."""
    resolved_db = _ensure_db(db_path)
    conn = sqlite3.connect(resolved_db, timeout=30.0)
    conn.row_factory = sqlite3.Row
    try:
        _ensure_incremental_tables(conn)
        row = conn.execute(
            "SELECT symbol FROM trades WHERE id = ?",
            (int(trade_id),),
        ).fetchone()
        if not row:
            return False
        symbol = str(row["symbol"] or "").strip().upper()
        if not symbol:
            return False

        now_ts = time.time()
        cutoff_30d = now_ts - RECENCY_WINDOW_SEC

        conn.execute(
            """
            INSERT OR IGNORE INTO symbol_learning_rollups(symbol, updated_at, last_processed_trade_id)
            VALUES (?, ?, 0)
            """,
            (symbol, now_ts),
        )
        roll = conn.execute(
            "SELECT * FROM symbol_learning_rollups WHERE symbol = ?",
            (symbol,),
        ).fetchone()
        if not roll:
            return False

        last_processed = int(roll["last_processed_trade_id"] or 0)
        rows = conn.execute(
            """
            SELECT
                id, direction, exit_time, realized_pnl, realized_pnl_pct,
                mae_pct, mfe_pct, protection_snapshot, signals_snapshot, signals_agreed
            FROM trades
            WHERE symbol = ?
              AND exit_time IS NOT NULL
              AND id > ?
            ORDER BY id ASC
            """,
            (symbol, last_processed),
        ).fetchall()

        # Catch late-close edge case when a lower-id trade closes after newer ids.
        if int(trade_id) <= last_processed:
            late = conn.execute(
                """
                SELECT
                    t.id, t.direction, t.exit_time, t.realized_pnl, t.realized_pnl_pct,
                    t.mae_pct, t.mfe_pct, t.protection_snapshot, t.signals_snapshot, t.signals_agreed
                FROM trades t
                LEFT JOIN symbol_learning_processed p
                  ON p.symbol = t.symbol AND p.trade_id = t.id
                WHERE t.id = ?
                  AND t.symbol = ?
                  AND t.exit_time IS NOT NULL
                  AND p.trade_id IS NULL
                LIMIT 1
                """,
                (int(trade_id), symbol),
            ).fetchone()
            if late:
                rows = list(rows) + [late]

        if not rows:
            return False

        n_closed = int(roll["n_closed"] or 0)
        wins = int(roll["wins"] or 0)
        pnl_count = int(roll["pnl_count"] or 0)
        pnl_sum = float(roll["pnl_sum"] or 0.0)
        pnl_pos_count = int(roll["pnl_pos_count"] or 0)
        pnl_pos_sum = float(roll["pnl_pos_sum"] or 0.0)
        pnl_neg_count = int(roll["pnl_neg_count"] or 0)
        pnl_neg_abs_sum = float(roll["pnl_neg_abs_sum"] or 0.0)
        mae_count = int(roll["mae_count"] or 0)
        mae_sum = float(roll["mae_sum"] or 0.0)
        mfe_count = int(roll["mfe_count"] or 0)
        mfe_sum = float(roll["mfe_sum"] or 0.0)
        sltp_rows = int(roll["sltp_rows"] or 0)
        sltp_fallback_rows = int(roll["sltp_fallback_rows"] or 0)
        sl_mult_count = int(roll["sl_mult_count"] or 0)
        sl_mult_sum = float(roll["sl_mult_sum"] or 0.0)
        tp_mult_count = int(roll["tp_mult_count"] or 0)
        tp_mult_sum = float(roll["tp_mult_sum"] or 0.0)

        max_processed = last_processed
        processed_any = False
        seen_trade_ids = set()
        for tr in rows:
            tid = int(tr["id"])
            if tid in seen_trade_ids:
                continue
            seen_trade_ids.add(tid)
            already = conn.execute(
                "SELECT 1 FROM symbol_learning_processed WHERE symbol = ? AND trade_id = ? LIMIT 1",
                (symbol, tid),
            ).fetchone()
            if already:
                continue

            direction = str(tr["direction"] or "").upper()
            pnl = _as_float(tr["realized_pnl"])
            pnl_pct = _as_float(tr["realized_pnl_pct"])
            outcome = pnl if pnl is not None else pnl_pct

            n_closed += 1
            if outcome is not None and outcome > 0:
                wins += 1

            if pnl_pct is not None:
                pnl_count += 1
                pnl_sum += float(pnl_pct)
                if pnl_pct > 0:
                    pnl_pos_count += 1
                    pnl_pos_sum += float(pnl_pct)
                elif pnl_pct < 0:
                    pnl_neg_count += 1
                    pnl_neg_abs_sum += abs(float(pnl_pct))

            mae = _as_float(tr["mae_pct"])
            if mae is not None:
                mae_count += 1
                mae_sum += max(0.0, float(mae))
            mfe = _as_float(tr["mfe_pct"])
            if mfe is not None:
                mfe_count += 1
                mfe_sum += max(0.0, float(mfe))

            protection = _safe_json(tr["protection_snapshot"], default=None)
            if isinstance(protection, dict):
                sltp_rows += 1
                if bool(protection.get("used_fallback")):
                    sltp_fallback_rows += 1
                slm = _as_float(protection.get("sl_mult"))
                if slm is not None and slm > 0:
                    sl_mult_count += 1
                    sl_mult_sum += float(slm)
                tpm = _as_float(protection.get("tp_mult"))
                if tpm is not None and tpm > 0:
                    tp_mult_count += 1
                    tp_mult_sum += float(tpm)

            sig_snap = _safe_json(tr["signals_snapshot"], default=None)
            sig_agreed = _safe_json(tr["signals_agreed"], default=[])
            if isinstance(sig_snap, dict) and sig_snap and isinstance(sig_agreed, list) and sig_agreed and pnl_pct is not None:
                normalized = sorted(
                    {
                        sn
                        for sn in (_normalize_signal_name(x) for x in sig_agreed)
                        if sn
                    }
                )
                for sig in normalized:
                    _update_signal_stat(
                        conn,
                        symbol=symbol,
                        signal=sig,
                        direction=direction,
                        pnl_pct=float(pnl_pct),
                        now_ts=now_ts,
                    )
                if normalized:
                    _update_combo_stat(
                        conn,
                        symbol=symbol,
                        combo="+".join(normalized),
                        direction=direction,
                        pnl_pct=float(pnl_pct),
                        now_ts=now_ts,
                    )

            conn.execute(
                """
                INSERT OR IGNORE INTO symbol_learning_processed(symbol, trade_id, processed_at)
                VALUES (?, ?, ?)
                """,
                (symbol, tid, now_ts),
            )
            max_processed = max(max_processed, tid)
            processed_any = True

        if not processed_any:
            return False

        conn.execute(
            """
            UPDATE symbol_learning_rollups
            SET
                updated_at = ?,
                last_processed_trade_id = ?,
                n_closed = ?,
                wins = ?,
                pnl_count = ?,
                pnl_sum = ?,
                pnl_pos_count = ?,
                pnl_pos_sum = ?,
                pnl_neg_count = ?,
                pnl_neg_abs_sum = ?,
                mae_count = ?,
                mae_sum = ?,
                mfe_count = ?,
                mfe_sum = ?,
                sltp_rows = ?,
                sltp_fallback_rows = ?,
                sl_mult_count = ?,
                sl_mult_sum = ?,
                tp_mult_count = ?,
                tp_mult_sum = ?
            WHERE symbol = ?
            """,
            (
                now_ts,
                max_processed,
                n_closed,
                wins,
                pnl_count,
                pnl_sum,
                pnl_pos_count,
                pnl_pos_sum,
                pnl_neg_count,
                pnl_neg_abs_sum,
                mae_count,
                mae_sum,
                mfe_count,
                mfe_sum,
                sltp_rows,
                sltp_fallback_rows,
                sl_mult_count,
                sl_mult_sum,
                tp_mult_count,
                tp_mult_sum,
                symbol,
            ),
        )

        # 30d stats are computed from a bounded recency window (no full-history rescan).
        s30 = conn.execute(
            """
            SELECT
                SUM(CASE WHEN realized_pnl_pct IS NOT NULL THEN 1 ELSE 0 END) AS n,
                SUM(CASE WHEN realized_pnl_pct > 0 THEN 1 ELSE 0 END) AS wins,
                AVG(realized_pnl_pct) AS avg_pnl_pct,
                SUM(CASE WHEN realized_pnl_pct > 0 THEN realized_pnl_pct ELSE 0 END) AS pos_sum,
                SUM(CASE WHEN realized_pnl_pct > 0 THEN 1 ELSE 0 END) AS pos_count,
                SUM(CASE WHEN realized_pnl_pct < 0 THEN -realized_pnl_pct ELSE 0 END) AS neg_abs_sum,
                SUM(CASE WHEN realized_pnl_pct < 0 THEN 1 ELSE 0 END) AS neg_count
            FROM trades
            WHERE symbol = ?
              AND exit_time IS NOT NULL
              AND exit_time >= ?
            """,
            (symbol, cutoff_30d),
        ).fetchone()
        n_closed_30d = int((s30["n"] or 0) if s30 else 0)
        wins_30d = int((s30["wins"] or 0) if s30 else 0)
        avg_pnl_pct_30d = float((s30["avg_pnl_pct"] or 0.0) if s30 else 0.0)
        expectancy_30d = _expectancy_from_parts(
            n_total=n_closed_30d,
            pos_sum=float((s30["pos_sum"] or 0.0) if s30 else 0.0),
            pos_count=int((s30["pos_count"] or 0) if s30 else 0),
            neg_abs_sum=float((s30["neg_abs_sum"] or 0.0) if s30 else 0.0),
            neg_count=int((s30["neg_count"] or 0) if s30 else 0),
        )
        win_rate_30d = (float(wins_30d) / float(n_closed_30d)) if n_closed_30d > 0 else 0.0

        signal_rows = [
            dict(r)
            for r in conn.execute(
                """
                SELECT signal, direction, n, wins, win_rate, avg_pnl_pct, expectancy
                FROM signal_symbol_stats
                WHERE symbol = ?
                ORDER BY expectancy DESC, win_rate DESC, n DESC
                LIMIT 64
                """,
                (symbol,),
            ).fetchall()
        ]
        combo_rows = [
            dict(r)
            for r in conn.execute(
                """
                SELECT combo, direction, n, wins, win_rate, avg_pnl_pct, expectancy
                FROM combo_symbol_stats
                WHERE symbol = ?
                ORDER BY expectancy DESC, win_rate DESC, n DESC
                LIMIT 64
                """,
                (symbol,),
            ).fetchall()
        ]

        recent = conn.execute(
            """
            SELECT mae_pct, mfe_pct, protection_snapshot
            FROM trades
            WHERE symbol = ? AND exit_time IS NOT NULL
            ORDER BY id DESC
            LIMIT 512
            """,
            (symbol,),
        ).fetchall()
        recent_mae: List[float] = []
        recent_mfe: List[float] = []
        recent_sl: List[float] = []
        recent_tp: List[float] = []
        for rr in recent:
            v_mae = _as_float(rr["mae_pct"])
            if v_mae is not None:
                recent_mae.append(max(0.0, v_mae))
            v_mfe = _as_float(rr["mfe_pct"])
            if v_mfe is not None:
                recent_mfe.append(max(0.0, v_mfe))
            protection = _safe_json(rr["protection_snapshot"], default=None)
            if isinstance(protection, dict):
                slm = _as_float(protection.get("sl_mult"))
                if slm is not None and slm > 0:
                    recent_sl.append(float(slm))
                tpm = _as_float(protection.get("tp_mult"))
                if tpm is not None and tpm > 0:
                    recent_tp.append(float(tpm))

        avg_pnl_pct = (pnl_sum / float(pnl_count)) if pnl_count > 0 else 0.0
        expectancy = _expectancy_from_parts(
            n_total=pnl_count,
            pos_sum=pnl_pos_sum,
            pos_count=pnl_pos_count,
            neg_abs_sum=pnl_neg_abs_sum,
            neg_count=pnl_neg_count,
        )
        win_rate = (float(wins) / float(n_closed)) if n_closed > 0 else 0.0
        avg_mae_pct = (mae_sum / float(mae_count)) if mae_count > 0 else 0.0
        avg_mfe_pct = (mfe_sum / float(mfe_count)) if mfe_count > 0 else 0.0
        p90_mae_pct = _percentile(recent_mae, 0.90) if recent_mae else 0.0
        p90_mfe_pct = _percentile(recent_mfe, 0.90) if recent_mfe else 0.0
        sltp_fallback_rate = (float(sltp_fallback_rows) / float(sltp_rows)) if sltp_rows > 0 else 0.0
        typical_sl_mult = _percentile(recent_sl, 0.50) if recent_sl else ((sl_mult_sum / float(sl_mult_count)) if sl_mult_count > 0 else None)
        typical_tp_mult = _percentile(recent_tp, 0.50) if recent_tp else ((tp_mult_sum / float(tp_mult_count)) if tp_mult_count > 0 else None)

        existing = conn.execute(
            "SELECT notes_summary, last_reflection_id_seen FROM symbol_learning_state WHERE symbol = ?",
            (symbol,),
        ).fetchone()
        notes_summary = str(existing["notes_summary"] or "") if existing else ""
        last_reflection_id_seen = int(existing["last_reflection_id_seen"] or 0) if existing else 0

        conn.execute(
            """
            INSERT INTO symbol_learning_state (
                symbol, updated_at, n_closed, win_rate, expectancy, avg_pnl_pct,
                n_closed_30d, win_rate_30d, expectancy_30d, avg_pnl_pct_30d,
                avg_mae_pct, p90_mae_pct, avg_mfe_pct, p90_mfe_pct,
                sltp_fallback_rate, typical_sl_mult, typical_tp_mult,
                signal_rank_json, combo_rank_json, notes_summary, last_reflection_id_seen
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(symbol) DO UPDATE SET
                updated_at = excluded.updated_at,
                n_closed = excluded.n_closed,
                win_rate = excluded.win_rate,
                expectancy = excluded.expectancy,
                avg_pnl_pct = excluded.avg_pnl_pct,
                n_closed_30d = excluded.n_closed_30d,
                win_rate_30d = excluded.win_rate_30d,
                expectancy_30d = excluded.expectancy_30d,
                avg_pnl_pct_30d = excluded.avg_pnl_pct_30d,
                avg_mae_pct = excluded.avg_mae_pct,
                p90_mae_pct = excluded.p90_mae_pct,
                avg_mfe_pct = excluded.avg_mfe_pct,
                p90_mfe_pct = excluded.p90_mfe_pct,
                sltp_fallback_rate = excluded.sltp_fallback_rate,
                typical_sl_mult = excluded.typical_sl_mult,
                typical_tp_mult = excluded.typical_tp_mult,
                signal_rank_json = excluded.signal_rank_json,
                combo_rank_json = excluded.combo_rank_json,
                notes_summary = excluded.notes_summary,
                last_reflection_id_seen = excluded.last_reflection_id_seen
            """,
            (
                symbol,
                now_ts,
                n_closed,
                win_rate,
                expectancy,
                avg_pnl_pct,
                n_closed_30d,
                win_rate_30d,
                expectancy_30d,
                avg_pnl_pct_30d,
                avg_mae_pct,
                p90_mae_pct,
                avg_mfe_pct,
                p90_mfe_pct,
                sltp_fallback_rate,
                typical_sl_mult,
                typical_tp_mult,
                json.dumps(signal_rows, separators=(",", ":")),
                json.dumps(combo_rows, separators=(",", ":")),
                notes_summary,
                last_reflection_id_seen,
            ),
        )
        conn.commit()
        ok = True
    finally:
        conn.close()

    # Pull new reflections incrementally (if any) after metric update.
    try:
        update_from_new_reflections(resolved_db, symbol=symbol)
    except Exception as exc:
        _LOG.warning(f"symbol dossier reflection merge failed for {symbol}: {exc}")
    return bool(ok)


def update_from_new_reflections(db_path: str, symbol: str) -> int:
    """Incrementally append new reflection lessons to symbol notes_summary."""
    resolved_db = _ensure_db(db_path)
    sym = str(symbol or "").strip().upper()
    if not sym:
        return 0

    conn = sqlite3.connect(resolved_db, timeout=30.0)
    conn.row_factory = sqlite3.Row
    try:
        now = time.time()
        conn.execute(
            """
            INSERT OR IGNORE INTO symbol_learning_state (
                symbol, updated_at, notes_summary, last_reflection_id_seen
            ) VALUES (?, ?, '', 0)
            """,
            (sym, now),
        )

        current = conn.execute(
            "SELECT notes_summary, last_reflection_id_seen FROM symbol_learning_state WHERE symbol = ?",
            (sym,),
        ).fetchone()
        notes_summary = str((current["notes_summary"] if current else "") or "")
        last_seen = int((current["last_reflection_id_seen"] if current else 0) or 0)

        rows = conn.execute(
            """
            SELECT
                r.id AS reflection_id,
                r.trade_id,
                r.lesson_text,
                r.reflection_json,
                t.realized_pnl_pct,
                t.exit_reason
            FROM reflections_v2 r
            JOIN trades t ON t.id = r.trade_id
            WHERE t.symbol = ?
              AND r.id > ?
            ORDER BY r.id ASC
            """,
            (sym, last_seen),
        ).fetchall()

        if not rows:
            return 0

        snippets: List[str] = []
        seen_texts = set()
        for row in rows:
            lesson = _extract_reflection_lesson(row["lesson_text"], row["reflection_json"])
            if not lesson:
                continue
            pnl_pct = _as_float(row["realized_pnl_pct"])
            exit_reason = str(row["exit_reason"] or "").upper()
            prefix = f"t{int(row['trade_id'])}"
            if pnl_pct is not None:
                prefix += f" {pnl_pct:+.2f}%"
            if exit_reason:
                prefix += f" {exit_reason}"
            line = _cap_text(f"{prefix}: {lesson}", 240)
            if line in seen_texts:
                continue
            seen_texts.add(line)
            snippets.append(line)

        if snippets:
            existing_parts = [p.strip() for p in str(notes_summary or "").split("|") if p.strip()]
            deduped: List[str] = []
            seen = set()
            # Keep newest reflection snippets first so they survive length-capping.
            for part in snippets + existing_parts:
                norm = " ".join(str(part).split())
                if not norm or norm in seen:
                    continue
                seen.add(norm)
                deduped.append(norm)
            merged = " | ".join(deduped)
        else:
            merged = notes_summary

        merged = _cap_text(merged, DEFAULT_NOTES_CAP)
        last_id = int(rows[-1]["reflection_id"])
        conn.execute(
            """
            UPDATE symbol_learning_state
            SET notes_summary = ?, last_reflection_id_seen = ?, updated_at = ?
            WHERE symbol = ?
            """,
            (merged, last_id, now, sym),
        )
        conn.commit()
        return len(rows)
    finally:
        conn.close()


def get_dossier_snippet(db_path: str, symbol: str, max_chars: int = DEFAULT_SNIPPET_MAX_CHARS) -> str:
    """Return compact, LLM-ready dossier text for a symbol.

    Important: symbol_learning_state may exist with notes_summary filled but
    n_closed still 0 (e.g., after a deploy where we start ingesting reflections
    before any new trade closes). In that case we do a best-effort recompute so
    dossier stats are populated for historical symbols.
    """
    resolved_db = _ensure_db(db_path)
    sym = str(symbol or "").strip().upper()
    if not sym:
        return ""

    cache_key = f"{resolved_db}:{sym}"
    now = time.time()
    try:
        refresh_sec = 30.0
    except Exception:
        refresh_sec = 30.0
    with _STATE_LOCK:
        last_refresh = float(_REFLECTION_REFRESH_TS.get(cache_key, 0.0) or 0.0)
    should_refresh = refresh_sec <= 0 or (now - last_refresh) >= refresh_sec
    if should_refresh:
        try:
            update_from_new_reflections(resolved_db, sym)
        except Exception as exc:
            _LOG.warning(f"dossier reflection refresh failed for {sym}: {exc}")
        with _STATE_LOCK:
            _REFLECTION_REFRESH_TS[cache_key] = now

    conn = sqlite3.connect(resolved_db, timeout=30.0)
    conn.row_factory = sqlite3.Row
    try:
        row = conn.execute(
            "SELECT * FROM symbol_learning_state WHERE symbol = ?",
            (sym,),
        ).fetchone()

        if not row:
            return ""
        if int(row["n_closed"] or 0) <= 0:
            return ""

        signal_rank = _safe_json(row["signal_rank_json"], default=[])
        combo_rank = _safe_json(row["combo_rank_json"], default=[])
        if not isinstance(signal_rank, list):
            signal_rank = []
        if not isinstance(combo_rank, list):
            combo_rank = []

        top_signals = []
        for item in signal_rank[:3]:
            if not isinstance(item, dict):
                continue
            top_signals.append(
                f"{item.get('signal')}:{item.get('direction')} n={int(item.get('n') or 0)} "
                f"wr={float(item.get('win_rate') or 0.0)*100:.0f}% "
                f"exp={float(item.get('expectancy') or 0.0):+.2f}%"
            )

        top_combos = []
        for item in combo_rank[:2]:
            if not isinstance(item, dict):
                continue
            top_combos.append(
                f"{item.get('combo')}:{item.get('direction')} n={int(item.get('n') or 0)} "
                f"wr={float(item.get('win_rate') or 0.0)*100:.0f}%"
            )

        # Optional: per-symbol rolling conclusion (A→B), kept separate from raw reflection snippets.
        conclusion = ""
        try:
            c_row = conn.execute(
                "SELECT conclusion_text, confidence FROM symbol_conclusions_v1 WHERE symbol = ?",
                (sym,),
            ).fetchone()
            if c_row and str(c_row["conclusion_text"] or "").strip():
                conclusion = str(c_row["conclusion_text"] or "").strip()
                c_conf = _as_float(c_row["confidence"])
                if c_conf is not None:
                    conclusion = f"{conclusion} (conf={c_conf:.2f})"
        except Exception:
            conclusion = ""

        header = (
            f"{sym} n={int(row['n_closed'] or 0)} "
            f"wr={float(row['win_rate'] or 0.0)*100:.1f}% "
            f"exp={float(row['expectancy'] or 0.0):+.2f}% "
            f"avg={float(row['avg_pnl_pct'] or 0.0):+.2f}%"
        )

        parts = [
            header,
            (
                f"30d n={int(row['n_closed_30d'] or 0)} "
                f"wr={float(row['win_rate_30d'] or 0.0)*100:.1f}% "
                f"exp={float(row['expectancy_30d'] or 0.0):+.2f}%"
            ),
            (
                f"mae avg/p90={float(row['avg_mae_pct'] or 0.0):.2f}/{float(row['p90_mae_pct'] or 0.0):.2f}% "
                f"mfe avg/p90={float(row['avg_mfe_pct'] or 0.0):.2f}/{float(row['p90_mfe_pct'] or 0.0):.2f}%"
            ),
        ]
        if conclusion:
            parts.insert(1, "conclusion: " + conclusion)

        sl_mult = _as_float(row["typical_sl_mult"])
        tp_mult = _as_float(row["typical_tp_mult"])
        if sl_mult is not None and tp_mult is not None:
            parts.append(
                f"sltp fallback={float(row['sltp_fallback_rate'] or 0.0)*100:.0f}% "
                f"typ={sl_mult:.2f}/{tp_mult:.2f}"
            )
        elif _as_float(row["sltp_fallback_rate"]) is not None:
            parts.append(f"sltp fallback={float(row['sltp_fallback_rate'] or 0.0)*100:.0f}%")

        if top_signals:
            parts.append("signals: " + "; ".join(top_signals))
        if top_combos:
            parts.append("combos: " + "; ".join(top_combos))

        notes = str(row["notes_summary"] or "").strip()
        if notes:
            parts.append("notes: " + notes)

        snippet = " | ".join(part for part in parts if part)
        return _cap_text(snippet, max(128, int(max_chars)))
    finally:
        conn.close()
