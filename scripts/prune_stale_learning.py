#!/usr/bin/env python3
"""Prune stale learning artifacts from EVClaw DB.

Default behavior is DRY-RUN.

Safe defaults:
- Remove stale symbol conclusions (symbol_conclusions_v1) older than cutoff.
- Clear stale dossier notes text (symbol_learning_state.notes_summary) older than cutoff.

Optional:
- Also delete stale reflection rows via --include-reflections.
"""

from __future__ import annotations

import argparse
import sqlite3
import time
from pathlib import Path
from typing import List, Sequence, Tuple


def _parse_symbols(raw: str) -> List[str]:
    out: List[str] = []
    for piece in str(raw or "").split(","):
        sym = piece.strip().upper()
        if sym:
            out.append(sym)
    return sorted(set(out))


def _days_from_args(window: str, days_override: int | None) -> int:
    if days_override is not None:
        return max(1, int(days_override))
    w = str(window or "month").strip().lower()
    if w == "week":
        return 7
    return 30


def _symbol_clause(symbols: Sequence[str], column_expr: str) -> Tuple[str, List[str]]:
    if not symbols:
        return "", []
    placeholders = ",".join(["?"] * len(symbols))
    return f" AND UPPER({column_expr}) IN ({placeholders})", list(symbols)


def main() -> int:
    root = Path(__file__).resolve().parents[1]
    ap = argparse.ArgumentParser(description="Prune stale EVClaw learning rows (dry-run by default).")
    ap.add_argument("--db", default=str(root / "ai_trader.db"), help="Path to ai_trader.db")
    ap.add_argument("--window", choices=["week", "month"], default="month", help="Stale age preset")
    ap.add_argument("--days", type=int, default=None, help="Override stale age in days")
    ap.add_argument(
        "--symbols",
        default="",
        help="Optional comma-separated symbols to scope pruning (e.g. BTC,ETH,XYZ:NVDA)",
    )
    ap.add_argument(
        "--include-reflections",
        action="store_true",
        help="Also delete stale rows from reflections_v2/trade_reflections",
    )
    ap.add_argument("--apply", action="store_true", help="Apply changes (default is dry-run)")
    args = ap.parse_args()

    db_path = Path(args.db).expanduser().resolve()
    if not db_path.exists():
        print(f"DB not found: {db_path}")
        return 2

    days = _days_from_args(args.window, args.days)
    now = time.time()
    cutoff = now - (days * 86400.0)
    symbols = _parse_symbols(args.symbols)

    concl_clause, concl_params = _symbol_clause(symbols, "symbol")
    state_clause, state_params = _symbol_clause(symbols, "symbol")
    trade_symbol_clause, trade_symbol_params = _symbol_clause(symbols, "t.symbol")

    with sqlite3.connect(str(db_path), timeout=30.0) as conn:
        conn.row_factory = sqlite3.Row

        stale_conclusions = int(
            conn.execute(
                f"""
                SELECT COUNT(*) AS n
                FROM symbol_conclusions_v1
                WHERE COALESCE(updated_at, 0) > 0
                  AND updated_at < ?
                  {concl_clause}
                """,
                [cutoff, *concl_params],
            ).fetchone()["n"]
            or 0
        )
        stale_notes = int(
            conn.execute(
                f"""
                SELECT COUNT(*) AS n
                FROM symbol_learning_state
                WHERE COALESCE(updated_at, 0) > 0
                  AND updated_at < ?
                  AND TRIM(COALESCE(notes_summary, '')) <> ''
                  {state_clause}
                """,
                [cutoff, *state_params],
            ).fetchone()["n"]
            or 0
        )

        stale_reflections = 0
        stale_trade_reflections = 0
        if args.include_reflections:
            stale_reflections = int(
                conn.execute(
                    f"""
                    SELECT COUNT(*) AS n
                    FROM reflections_v2 r
                    JOIN trades t ON t.id = r.trade_id
                    WHERE COALESCE(r.created_at, 0) > 0
                      AND r.created_at < ?
                      {trade_symbol_clause}
                    """,
                    [cutoff, *trade_symbol_params],
                ).fetchone()["n"]
                or 0
            )
            stale_trade_reflections = int(
                conn.execute(
                    f"""
                    SELECT COUNT(*) AS n
                    FROM trade_reflections tr
                    JOIN trades t ON t.id = tr.trade_id
                    WHERE COALESCE(tr.created_at, 0) > 0
                      AND tr.created_at < ?
                      {trade_symbol_clause}
                    """,
                    [cutoff, *trade_symbol_params],
                ).fetchone()["n"]
                or 0
            )

        print("Prune stale learning")
        print(f"- db: {db_path}")
        print(f"- mode: {'apply' if args.apply else 'dry-run'}")
        print(f"- cutoff_days: {days}")
        print(f"- cutoff_unix: {cutoff:.0f}")
        print(f"- scope_symbols: {','.join(symbols) if symbols else 'ALL'}")
        print(f"- stale_conclusions: {stale_conclusions}")
        print(f"- stale_notes_summary: {stale_notes}")
        print(f"- include_reflections: {bool(args.include_reflections)}")
        if args.include_reflections:
            print(f"- stale_reflections_v2: {stale_reflections}")
            print(f"- stale_trade_reflections: {stale_trade_reflections}")

        if not args.apply:
            return 0

        conn.execute("BEGIN IMMEDIATE")
        deleted_conclusions = conn.execute(
            f"""
            DELETE FROM symbol_conclusions_v1
            WHERE COALESCE(updated_at, 0) > 0
              AND updated_at < ?
              {concl_clause}
            """,
            [cutoff, *concl_params],
        ).rowcount
        cleared_notes = conn.execute(
            f"""
            UPDATE symbol_learning_state
            SET notes_summary = NULL,
                updated_at = ?
            WHERE COALESCE(updated_at, 0) > 0
              AND updated_at < ?
              AND TRIM(COALESCE(notes_summary, '')) <> ''
              {state_clause}
            """,
            [now, cutoff, *state_params],
        ).rowcount

        deleted_reflections = 0
        deleted_trade_reflections = 0
        if args.include_reflections:
            deleted_reflections = conn.execute(
                f"""
                DELETE FROM reflections_v2
                WHERE id IN (
                    SELECT r.id
                    FROM reflections_v2 r
                    JOIN trades t ON t.id = r.trade_id
                    WHERE COALESCE(r.created_at, 0) > 0
                      AND r.created_at < ?
                      {trade_symbol_clause}
                )
                """,
                [cutoff, *trade_symbol_params],
            ).rowcount
            deleted_trade_reflections = conn.execute(
                f"""
                DELETE FROM trade_reflections
                WHERE id IN (
                    SELECT tr.id
                    FROM trade_reflections tr
                    JOIN trades t ON t.id = tr.trade_id
                    WHERE COALESCE(tr.created_at, 0) > 0
                      AND tr.created_at < ?
                      {trade_symbol_clause}
                )
                """,
                [cutoff, *trade_symbol_params],
            ).rowcount
        conn.commit()

    print("Applied")
    print(f"- deleted_conclusions: {int(deleted_conclusions or 0)}")
    print(f"- cleared_notes_summary: {int(cleared_notes or 0)}")
    if args.include_reflections:
        print(f"- deleted_reflections_v2: {int(deleted_reflections or 0)}")
        print(f"- deleted_trade_reflections: {int(deleted_trade_reflections or 0)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
