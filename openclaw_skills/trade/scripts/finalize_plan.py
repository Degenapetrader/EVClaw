#!/usr/bin/env python3
"""Finalize a manual trade plan: store plan_json + mark READY."""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
import time
from pathlib import Path

SCRIPT_PATH = Path(__file__).resolve()
EVCLAW_ROOT = Path(os.getenv("EVCLAW_ROOT") or SCRIPT_PATH.parents[3])
DB_PATH_DEFAULT = str(Path(os.getenv("EVCLAW_DB_PATH") or (EVCLAW_ROOT / "ai_trader.db")))


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("display_id")
    ap.add_argument("json_file")
    ap.add_argument("--db", default=DB_PATH_DEFAULT)
    args = ap.parse_args()

    display_id = str(args.display_id or "").strip().upper()
    if not display_id:
        print("display_id required", file=sys.stderr)
        return 2

    raw = open(args.json_file, "r").read()
    # validate json
    try:
        obj = json.loads(raw)
    except Exception as e:
        print(f"invalid json_file: {e}", file=sys.stderr)
        return 2

    # ensure display_id matches
    obj_id = str(obj.get("display_id") or "").strip().upper()
    if obj_id and obj_id != display_id:
        print(f"display_id mismatch: file has {obj_id} but arg is {display_id}", file=sys.stderr)
        return 2

    conn = sqlite3.connect(args.db, timeout=30.0)
    try:
        conn.execute("PRAGMA busy_timeout=30000")
        conn.execute("BEGIN IMMEDIATE")
        row = conn.execute(
            "SELECT id, expires_at, status FROM manual_trade_plans WHERE display_id=?",
            (display_id,),
        ).fetchone()
        if not row:
            print(f"plan not found: {display_id}", file=sys.stderr)
            conn.rollback()
            return 1

        now = time.time()
        conn.execute(
            """
            UPDATE manual_trade_plans
            SET plan_json=?, status='READY', error=NULL
            WHERE display_id=?
            """,
            (json.dumps(obj, separators=(",", ":"), sort_keys=True), display_id),
        )
        conn.commit()
        print(f"OK stored {display_id}")
        return 0
    finally:
        try:
            conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    raise SystemExit(main())
