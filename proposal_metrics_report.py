#!/usr/bin/env python3
"""Proposal-layer observability report (funnel + gate decisions)."""

from __future__ import annotations

import argparse
import json
import sqlite3
import time
from pathlib import Path
from typing import Any, Dict, List


def _has_table(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
        (table_name,),
    ).fetchone()
    return row is not None


def _to_counts(rows: List[sqlite3.Row], key_name: str) -> Dict[str, int]:
    out: Dict[str, int] = {}
    for row in rows:
        key = str(row[key_name] or "").upper()
        if not key:
            key = "UNKNOWN"
        out[key] = int(row["count"] or 0)
    return out


def _safe_pct(numer: float, denom: float) -> float:
    d = float(denom or 0.0)
    if d <= 0:
        return 0.0
    return round((100.0 * float(numer or 0.0) / d), 3)


def collect_proposal_metrics(
    *,
    db_path: str,
    window_hours: float = 24.0,
    top_n: int = 10,
) -> Dict[str, Any]:
    now_ts = time.time()
    start_ts = now_ts - (max(0.0, float(window_hours)) * 3600.0)

    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row

        status_rows = conn.execute(
            """
            SELECT UPPER(COALESCE(status, 'UNKNOWN')) AS status, COUNT(*) AS count
            FROM trade_proposals
            WHERE created_at >= ?
            GROUP BY UPPER(COALESCE(status, 'UNKNOWN'))
            ORDER BY count DESC
            """,
            (start_ts,),
        ).fetchall()
        status_counts = _to_counts(status_rows, "status")

        block_reason_rows = conn.execute(
            """
            SELECT COALESCE(status_reason, 'unknown') AS reason, COUNT(*) AS count
            FROM trade_proposals
            WHERE created_at >= ?
              AND UPPER(COALESCE(status, '')) = 'BLOCKED'
            GROUP BY COALESCE(status_reason, 'unknown')
            ORDER BY count DESC, reason ASC
            LIMIT ?
            """,
            (start_ts, int(max(1, top_n))),
        ).fetchall()

        gate_counts: Dict[str, int] = {}
        gate_reason_rows: List[Dict[str, Any]] = []
        symbol_gate_rows: List[Dict[str, Any]] = []
        gate_pick_outcomes: Dict[str, Any] = {}
        gate_pick_rate_window: Dict[str, Any] = {}
        if _has_table(conn, "gate_decisions_v1"):
            gate_rows = conn.execute(
                """
                SELECT UPPER(COALESCE(decision, 'UNKNOWN')) AS decision, COUNT(*) AS count
                FROM gate_decisions_v1
                WHERE created_at >= ?
                GROUP BY UPPER(COALESCE(decision, 'UNKNOWN'))
                ORDER BY count DESC
                """,
                (start_ts,),
            ).fetchall()
            gate_counts = _to_counts(gate_rows, "decision")

            gate_reason_rows_db = conn.execute(
                """
                SELECT COALESCE(reason, 'unknown') AS reason, COUNT(*) AS count
                FROM gate_decisions_v1
                WHERE created_at >= ?
                GROUP BY COALESCE(reason, 'unknown')
                ORDER BY count DESC, reason ASC
                LIMIT ?
                """,
                (start_ts, int(max(1, top_n))),
            ).fetchall()
            gate_reason_rows = [
                {"reason": str(r["reason"]), "count": int(r["count"] or 0)}
                for r in gate_reason_rows_db
            ]

            symbol_gate_rows_db = conn.execute(
                """
                SELECT
                  UPPER(COALESCE(symbol, 'UNKNOWN')) AS symbol,
                  UPPER(COALESCE(direction, 'UNKNOWN')) AS direction,
                  UPPER(COALESCE(decision, 'UNKNOWN')) AS decision,
                  COUNT(*) AS count
                FROM gate_decisions_v1
                WHERE created_at >= ?
                GROUP BY UPPER(COALESCE(symbol, 'UNKNOWN')),
                         UPPER(COALESCE(direction, 'UNKNOWN')),
                         UPPER(COALESCE(decision, 'UNKNOWN'))
                ORDER BY count DESC, symbol ASC
                LIMIT ?
                """,
                (start_ts, int(max(1, top_n))),
            ).fetchall()
            symbol_gate_rows = [
                {
                    "symbol": str(r["symbol"]),
                    "direction": str(r["direction"]),
                    "decision": str(r["decision"]),
                    "count": int(r["count"] or 0),
                }
                for r in symbol_gate_rows_db
            ]

            pick_rate_row = conn.execute(
                """
                SELECT
                  SUM(CASE WHEN UPPER(COALESCE(decision, '')) = 'PICK' THEN 1 ELSE 0 END) AS picks,
                  SUM(CASE WHEN UPPER(COALESCE(decision, '')) = 'REJECT' THEN 1 ELSE 0 END) AS rejects,
                  SUM(
                    CASE
                      WHEN UPPER(COALESCE(decision, '')) IN ('PICK', 'REJECT') THEN 1
                      ELSE 0
                    END
                  ) AS candidates_decided
                FROM gate_decisions_v1
                WHERE created_at >= ?
                """,
                (start_ts,),
            ).fetchone()
            picks = int(pick_rate_row["picks"] or 0) if pick_rate_row else 0
            rejects = int(pick_rate_row["rejects"] or 0) if pick_rate_row else 0
            candidates_decided = int(pick_rate_row["candidates_decided"] or 0) if pick_rate_row else 0
            gate_pick_rate_window = {
                "candidates_decided": candidates_decided,
                "picks": picks,
                "rejects": rejects,
                "pick_rate_pct": _safe_pct(picks, candidates_decided),
            }

            pick_outcome_row = conn.execute(
                """
                SELECT
                  COUNT(*) AS picks_total,
                  SUM(CASE WHEN UPPER(COALESCE(outcome_status, '')) IN ('WIN', 'LOSS', 'FLAT') THEN 1 ELSE 0 END) AS picks_with_outcome,
                  SUM(CASE WHEN UPPER(COALESCE(outcome_status, '')) = 'WIN' THEN 1 ELSE 0 END) AS wins,
                  COALESCE(SUM(outcome_pnl), 0) AS pick_total_pnl_usd,
                  COALESCE(AVG(outcome_pnl_pct), 0) AS pick_avg_pnl_pct
                FROM gate_decisions_v1
                WHERE created_at >= ?
                  AND UPPER(COALESCE(decision, '')) = 'PICK'
                """,
                (start_ts,),
            ).fetchone()
            picks_total = int(pick_outcome_row["picks_total"] or 0) if pick_outcome_row else 0
            picks_with_outcome = int(pick_outcome_row["picks_with_outcome"] or 0) if pick_outcome_row else 0
            wins = int(pick_outcome_row["wins"] or 0) if pick_outcome_row else 0
            total_pnl = float(pick_outcome_row["pick_total_pnl_usd"] or 0.0) if pick_outcome_row else 0.0
            avg_pnl_pct = float(pick_outcome_row["pick_avg_pnl_pct"] or 0.0) if pick_outcome_row else 0.0
            gate_pick_outcomes = {
                "picks_total": picks_total,
                "picks_with_outcome": picks_with_outcome,
                "pick_win_rate_pct": _safe_pct(wins, picks_with_outcome),
                "pick_total_pnl_usd": round(total_pnl, 6),
                "pick_avg_pnl_pct": round(avg_pnl_pct, 6),
            }

    return {
        "window_hours": float(window_hours),
        "window_start_ts": start_ts,
        "window_end_ts": now_ts,
        "proposal_status_counts": status_counts,
        "top_block_reasons": [
            {"reason": str(r["reason"]), "count": int(r["count"] or 0)}
            for r in block_reason_rows
        ],
        "gate_decision_counts": gate_counts,
        "top_gate_reasons": gate_reason_rows,
        "top_symbol_gate_stats": symbol_gate_rows,
        "gate_pick_outcomes": gate_pick_outcomes,
        "gate_pick_rate_window": gate_pick_rate_window,
    }


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Proposal-layer metrics report")
    parser.add_argument(
        "--db-path",
        default=str(Path(__file__).parent / "ai_trader.db"),
        help="SQLite DB path",
    )
    parser.add_argument(
        "--window-hours",
        type=float,
        default=24.0,
        help="Lookback window in hours",
    )
    parser.add_argument(
        "--top-n",
        type=int,
        default=10,
        help="Top-N rows for reasons/symbol stats",
    )
    parser.add_argument(
        "--pretty",
        action="store_true",
        help="Pretty-print JSON",
    )
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    metrics = collect_proposal_metrics(
        db_path=str(args.db_path),
        window_hours=float(args.window_hours),
        top_n=int(args.top_n),
    )
    if bool(args.pretty):
        print(json.dumps(metrics, indent=2, sort_keys=True))
    else:
        print(json.dumps(metrics, separators=(",", ":"), sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
