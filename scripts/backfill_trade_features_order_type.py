#!/usr/bin/env python3
"""Backfill trade_features.order_type from full trades dataset.

Default mode is dry-run. Use --apply to upsert trade_features rows.
"""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path
from typing import Dict, List


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ai_trader_db import AITraderDB


def _closed_trade_ids(db_path: str) -> List[int]:
    with sqlite3.connect(db_path) as conn:
        rows = conn.execute(
            """
            SELECT id
            FROM trades
            WHERE exit_time IS NOT NULL
            ORDER BY id ASC
            """
        ).fetchall()
    return [int(r[0]) for r in rows]


def main() -> int:
    parser = argparse.ArgumentParser(description="Backfill trade_features.order_type for all closed trades.")
    parser.add_argument(
        "--db-path",
        default=str(ROOT / "ai_trader.db"),
        help="Path to ai_trader.db (default: %(default)s)",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Write/upsert into trade_features (default is dry-run).",
    )
    args = parser.parse_args()

    db = AITraderDB(args.db_path)
    trade_ids = _closed_trade_ids(args.db_path)

    total = len(trade_ids)
    missing_source = 0
    missing_order_type = 0
    source_counts: Dict[str, int] = {}
    written = 0
    write_errors = 0

    for trade_id in trade_ids:
        feature = db.get_trade_feature_source(int(trade_id))
        if not isinstance(feature, dict):
            missing_source += 1
            continue

        src = str(feature.get("order_type_source") or "none")
        source_counts[src] = int(source_counts.get(src, 0)) + 1
        if feature.get("order_type") is None:
            missing_order_type += 1

        if not args.apply:
            continue

        try:
            db.insert_trade_features(
                trade_id=int(feature["trade_id"]),
                symbol=str(feature.get("symbol") or ""),
                direction=feature.get("direction"),
                venue=feature.get("venue"),
                closed_at=float(feature.get("closed_at") or 0.0),
                conviction=feature.get("conviction"),
                order_type=feature.get("order_type"),
                order_type_source=feature.get("order_type_source"),
                risk_pct_used=feature.get("risk_pct_used"),
                equity_at_entry=feature.get("equity_at_entry"),
                mae_pct=feature.get("mae_pct"),
                mfe_pct=feature.get("mfe_pct"),
                pnl_usd=feature.get("pnl_usd"),
                pnl_r=feature.get("pnl_r"),
                exit_reason=feature.get("exit_reason"),
            )
            written += 1
        except Exception:
            write_errors += 1

    mode = "APPLY" if args.apply else "DRY_RUN"
    print(f"mode={mode}")
    print(f"db_path={args.db_path}")
    print(f"closed_trades={total}")
    print(f"missing_feature_source={missing_source}")
    print(f"missing_order_type={missing_order_type}")
    print(f"source_counts={source_counts}")
    if args.apply:
        print(f"rows_upserted={written}")
        print(f"write_errors={write_errors}")
        if write_errors:
            return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
