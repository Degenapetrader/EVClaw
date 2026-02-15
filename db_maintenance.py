#!/usr/bin/env python3
"""EVClaw DB maintenance

- Atomic SQLite backups (via sqlite3 backup API)
- Lightweight sanity checks for known corruption patterns
- Optional auto-repair (authorized in AGI mode)

This targets ai_trader.db (EVClaw) only.
"""

import argparse
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Tuple

SKILL_DIR = Path(__file__).resolve().parent
DEFAULT_DB = SKILL_DIR / "ai_trader.db"
BACKUP_DIR = SKILL_DIR / "backups"


def utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")


def atomic_backup(db_path: Path, backup_dir: Path, keep: int) -> Path:
    backup_dir.mkdir(parents=True, exist_ok=True)
    out = backup_dir / f"{db_path.name}.bak-{utc_stamp()}"

    src = sqlite3.connect(str(db_path), timeout=30.0)
    try:
        dst = sqlite3.connect(str(out), timeout=30.0)
        try:
            src.backup(dst)
            dst.commit()
        finally:
            dst.close()
    finally:
        src.close()

    # rotate
    backups = sorted(backup_dir.glob(f"{db_path.name}.bak-*"), key=lambda p: p.stat().st_mtime, reverse=True)
    for old in backups[keep:]:
        try:
            old.unlink()
        except Exception:
            pass

    return out


def fetch_issues(conn: sqlite3.Connection) -> List[Tuple[str, int]]:
    """Return list of (issue_name, count)."""
    issues: List[Tuple[str, int]] = []

    def count(sql: str) -> int:
        cur = conn.execute(sql)
        row = cur.fetchone()
        return int(row[0] or 0)

    issues.append((
        "active_with_exit_time",
        count("SELECT COUNT(*) FROM trades WHERE state='ACTIVE' AND exit_time IS NOT NULL"),
    ))
    issues.append((
        "entry_reason_has_exit_time",
        count("SELECT COUNT(*) FROM trades WHERE exit_reason='ENTRY' AND exit_time IS NOT NULL"),
    ))
    issues.append((
        "exit_time_ms_suspected",
        count("SELECT COUNT(*) FROM trades WHERE exit_time IS NOT NULL AND exit_time > 1000000000000"),
    ))

    return issues


def repair(conn: sqlite3.Connection) -> List[Tuple[str, int]]:
    """Apply narrow repairs for known corruption patterns. Returns list of (action, rows)."""
    actions: List[Tuple[str, int]] = []

    # 1) ENTRY is not an exit; clear bogus exit fields.
    cur = conn.execute(
        """
        UPDATE trades
        SET exit_time = NULL,
            exit_price = NULL,
            exit_reason = NULL,
            realized_pnl = NULL,
            realized_pnl_pct = NULL
        WHERE exit_reason = 'ENTRY'
          AND exit_time IS NOT NULL
        """
    )
    actions.append(("cleared_entry_exit_markers", cur.rowcount))

    # 2) Normalize ms->sec for trades that are not ACTIVE.
    cur = conn.execute(
        """
        UPDATE trades
        SET exit_time = exit_time / 1000.0
        WHERE exit_time IS NOT NULL
          AND exit_time > 1000000000000
          AND state IS NOT NULL
          AND state != 'ACTIVE'
        """
    )
    actions.append(("normalized_ms_exit_time_non_active", cur.rowcount))

    # 3) Safety: if state is ACTIVE but exit_time is set, clear it.
    cur = conn.execute(
        """
        UPDATE trades
        SET exit_time = NULL,
            exit_price = NULL,
            exit_reason = NULL,
            realized_pnl = NULL,
            realized_pnl_pct = NULL,
            total_fees = 0.0
        WHERE state = 'ACTIVE'
          AND exit_time IS NOT NULL
        """
    )
    actions.append(("cleared_active_with_exit_time", cur.rowcount))

    conn.commit()
    return actions


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=str(DEFAULT_DB), help="Path to ai_trader.db")
    ap.add_argument("--backup", action="store_true", help="Create an atomic backup")
    ap.add_argument("--keep", type=int, default=14, help="How many backups to keep")
    ap.add_argument("--check", action="store_true", help="Run sanity checks")
    ap.add_argument("--repair", action="store_true", help="Apply safe repairs for known corrupt patterns")
    args = ap.parse_args()

    db_path = Path(args.db)
    if not db_path.exists():
        print(f"ERROR: DB not found: {db_path}")
        return 2

    if args.backup:
        out = atomic_backup(db_path, BACKUP_DIR, keep=args.keep)
        print(f"backup: {out}")

    conn = sqlite3.connect(str(db_path), timeout=30.0)
    try:
        if args.check:
            issues = fetch_issues(conn)
            for name, n in issues:
                print(f"check:{name}={n}")

        if args.repair:
            actions = repair(conn)
            for name, n in actions:
                print(f"repair:{name}={n}")

        # final sanity
        issues = fetch_issues(conn)
        bad = [(n, c) for n, c in issues if c > 0]
        if bad:
            print("WARNING: remaining issues:")
            for name, n in bad:
                print(f"  - {name}: {n}")
            return 1

        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
