#!/usr/bin/env python3
"""Import optional learning seed archive into EVClaw.

This is opt-in by design and is NOT called by bootstrap.
Run only when the user explicitly agrees.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shutil
import sqlite3
import sys
import tarfile
import tempfile
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Tuple


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ai_trader_db import AITraderDB


DEFAULT_DB_PATH = ROOT / "ai_trader.db"
DEFAULT_MEMORY_DIR = ROOT / "memory"
DEFAULT_RELEASE_TAG = "evclaw-learning-seed-v2-20260220"
DEFAULT_RELEASE_BASE = (
    f"https://github.com/Degenapetrader/EVClaw/releases/download/{DEFAULT_RELEASE_TAG}"
)
DEFAULT_RELEASE_SEED_URL = f"{DEFAULT_RELEASE_BASE}/evclaw-learning-seed.tgz"
DEFAULT_RELEASE_SHA256_URL = f"{DEFAULT_RELEASE_BASE}/evclaw-learning-seed.tgz.sha256"

LEARNING_TABLES: List[str] = [
    "learning_state_kv",
    "symbol_learning_state",
    "signal_symbol_stats",
    "symbol_conclusions_v1",
    "symbol_policy",
    "trade_features",
    "reflections_v2",
    "trade_reflections",
    "exit_decision_outcomes_v1",
    "adaptation_runs",
    "conviction_config_history",
    "dead_capital_stats",
    "signal_combos",
    "trades",  # imported as CLOSED trades only
]


def _normalize_create_table_sql(sql: str) -> str:
    s = (sql or "").strip()
    if not s:
        return ""
    return re.sub(r"(?i)^CREATE\s+TABLE\s+", "CREATE TABLE IF NOT EXISTS ", s, count=1)


def _safe_extract_tar(src: Path, dst_dir: Path) -> None:
    dst_root = dst_dir.resolve()
    with tarfile.open(str(src), "r:*") as tf:
        for member in tf.getmembers():
            target = (dst_root / member.name).resolve()
            if not str(target).startswith(str(dst_root) + os.sep) and target != dst_root:
                raise RuntimeError(f"unsafe tar entry: {member.name}")
        tf.extractall(path=str(dst_root))


def _download_file(url: str, dst: Path) -> None:
    req = urllib.request.Request(
        url,
        headers={"User-Agent": "EVClawLearningSeedImporter/1.0"},
    )
    with urllib.request.urlopen(req, timeout=30) as response, dst.open("wb") as out:
        shutil.copyfileobj(response, out)


def _file_sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _parse_sha256_file(path: Path) -> str:
    txt = path.read_text(encoding="utf-8", errors="replace").strip()
    if not txt:
        raise RuntimeError(f"empty sha256 file: {path}")
    token = txt.split()[0].strip().lower()
    if not re.fullmatch(r"[a-f0-9]{64}", token):
        raise RuntimeError(f"invalid sha256 content: {txt[:120]}")
    return token


def _table_columns(conn: sqlite3.Connection, schema: str, table: str) -> List[str]:
    rows = conn.execute(f'PRAGMA {schema}.table_info("{table}")').fetchall()
    return [str(r[1]) for r in rows]


def _ensure_target_table(
    conn: sqlite3.Connection,
    *,
    table: str,
    target_tables: set[str],
) -> Tuple[bool, str]:
    if table in target_tables:
        return True, "exists"
    row = conn.execute(
        "SELECT sql FROM src.sqlite_master WHERE type='table' AND name=?",
        (table,),
    ).fetchone()
    if not row or not row[0]:
        return False, "missing_source_schema"
    create_sql = _normalize_create_table_sql(str(row[0]))
    if not create_sql:
        return False, "empty_schema_sql"
    conn.execute(create_sql)
    target_tables.add(table)
    return True, "created"


def _import_table(
    conn: sqlite3.Connection,
    *,
    table: str,
    src_tables: set[str],
    target_tables: set[str],
    apply_changes: bool,
) -> Tuple[int, str]:
    if table not in src_tables:
        return 0, "missing_in_seed"

    ok, status = _ensure_target_table(conn, table=table, target_tables=target_tables)
    if not ok:
        return 0, status

    src_cols = _table_columns(conn, "src", table)
    tgt_cols = _table_columns(conn, "main", table)
    if not src_cols or not tgt_cols:
        return 0, "no_columns"

    common_cols = [c for c in src_cols if c in set(tgt_cols)]
    if not common_cols:
        return 0, "no_common_columns"

    where = ""
    if table == "trades" and "exit_time" in src_cols:
        where = 'WHERE "exit_time" IS NOT NULL'
    elif "trade_id" in src_cols and "trades" in src_tables:
        where = 'WHERE "trade_id" IN (SELECT "id" FROM src."trades" WHERE "exit_time" IS NOT NULL)'

    count_sql = f'SELECT COUNT(*) FROM src."{table}" {where}'
    src_count = int(conn.execute(count_sql).fetchone()[0] or 0)
    if src_count <= 0:
        return 0, "empty_after_filter"

    if not apply_changes:
        return src_count, "dry_run"

    cols_sql = ", ".join(f'"{c}"' for c in common_cols)
    insert_sql = (
        f'INSERT OR REPLACE INTO main."{table}" ({cols_sql}) '
        f'SELECT {cols_sql} FROM src."{table}" {where}'
    )
    conn.execute(insert_sql)
    return src_count, "ok"


def main() -> int:
    parser = argparse.ArgumentParser(description="Import optional EVClaw learning seed.")
    parser.add_argument(
        "--seed",
        required=False,
        help="Path to learning seed archive (.tgz/.tar.gz) or learning_seed.db. If omitted, downloads official release seed.",
    )
    parser.add_argument(
        "--seed-url",
        default=os.getenv("EVCLAW_LEARNING_SEED_URL", DEFAULT_RELEASE_SEED_URL),
        help="Seed archive URL used when --seed is omitted (default: official EVClaw release).",
    )
    parser.add_argument(
        "--seed-sha256-url",
        default=os.getenv("EVCLAW_LEARNING_SEED_SHA256_URL", DEFAULT_RELEASE_SHA256_URL),
        help="SHA256 URL used to verify downloaded seed when --seed is omitted.",
    )
    parser.add_argument(
        "--skip-sha256-verify",
        action="store_true",
        help="Skip SHA256 verification for downloaded seed (not recommended).",
    )
    parser.add_argument(
        "--db-path",
        default=str(DEFAULT_DB_PATH),
        help="Target EVClaw ai_trader.db path (default: %(default)s)",
    )
    parser.add_argument(
        "--memory-dir",
        default=str(DEFAULT_MEMORY_DIR),
        help="Target EVClaw memory dir (default: %(default)s)",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Apply import. Without this flag, script runs in dry-run mode.",
    )
    args = parser.parse_args()

    apply_changes = bool(args.apply)

    target_db = Path(args.db_path).expanduser().resolve()
    target_mem = Path(args.memory_dir).expanduser().resolve()

    # Ensure target DB/tables exist first.
    AITraderDB(str(target_db))

    with tempfile.TemporaryDirectory(prefix="evclaw-learning-seed-") as td:
        tmp_dir = Path(td)
        seed_source = ""
        if args.seed:
            seed_input = Path(args.seed).expanduser().resolve()
            if not seed_input.exists():
                raise SystemExit(f"seed file not found: {seed_input}")
            seed_source = f"path:{seed_input}"
        else:
            seed_url = str(args.seed_url or "").strip()
            if not seed_url:
                raise SystemExit("seed not provided and seed URL is empty")
            seed_input = tmp_dir / "evclaw-learning-seed.tgz"
            try:
                _download_file(seed_url, seed_input)
            except urllib.error.URLError as exc:
                raise SystemExit(f"failed to download seed from {seed_url}: {exc}") from exc
            except Exception as exc:
                raise SystemExit(f"failed to download seed from {seed_url}: {exc}") from exc
            seed_source = f"url:{seed_url}"

            if not bool(args.skip_sha256_verify):
                sha_url = str(args.seed_sha256_url or "").strip()
                if not sha_url:
                    raise SystemExit("seed SHA256 URL is empty; set --seed-sha256-url or use --skip-sha256-verify")
                sha_path = tmp_dir / "evclaw-learning-seed.tgz.sha256"
                try:
                    _download_file(sha_url, sha_path)
                except urllib.error.URLError as exc:
                    raise SystemExit(f"failed to download seed sha256 from {sha_url}: {exc}") from exc
                except Exception as exc:
                    raise SystemExit(f"failed to download seed sha256 from {sha_url}: {exc}") from exc
                expected = _parse_sha256_file(sha_path)
                actual = _file_sha256(seed_input)
                if actual.lower() != expected.lower():
                    raise SystemExit(
                        "seed sha256 mismatch: "
                        f"expected={expected} actual={actual} source={seed_url}"
                    )

        if seed_input.suffix.lower() == ".db":
            seed_db = seed_input
            seed_manifest = None
            seed_context = seed_input.parent / "context_feature_stats.json"
        else:
            _safe_extract_tar(seed_input, tmp_dir)
            seed_db = tmp_dir / "learning_seed.db"
            seed_manifest = tmp_dir / "manifest.json"
            seed_context = tmp_dir / "context_feature_stats.json"
        if not seed_db.exists():
            raise SystemExit(f"learning_seed.db not found in seed source: {seed_input}")

        exported_manifest: Dict[str, object] = {}
        if seed_manifest and seed_manifest.exists():
            try:
                exported_manifest = json.loads(seed_manifest.read_text(encoding="utf-8"))
            except Exception:
                exported_manifest = {}

        imported: Dict[str, int] = {}
        skipped: Dict[str, str] = {}

        with sqlite3.connect(str(target_db)) as conn:
            conn.execute("PRAGMA foreign_keys=OFF")
            conn.execute("ATTACH DATABASE ? AS src", (str(seed_db),))
            src_tables = {
                str(r[0])
                for r in conn.execute("SELECT name FROM src.sqlite_master WHERE type='table'")
            }
            target_tables = {
                str(r[0])
                for r in conn.execute("SELECT name FROM main.sqlite_master WHERE type='table'")
            }

            for table in LEARNING_TABLES:
                try:
                    count, status = _import_table(
                        conn,
                        table=table,
                        src_tables=src_tables,
                        target_tables=target_tables,
                        apply_changes=apply_changes,
                    )
                    if status in {"ok", "dry_run"}:
                        imported[table] = count
                    else:
                        skipped[table] = status
                except Exception as exc:
                    skipped[table] = f"error:{exc}"

            if apply_changes:
                conn.commit()
            conn.execute("DETACH DATABASE src")

        context_status = "not_found_in_seed"
        if seed_context.exists():
            if apply_changes:
                target_mem.mkdir(parents=True, exist_ok=True)
                dst = target_mem / "context_feature_stats.json"
                if dst.exists():
                    backup = dst.with_name(f"context_feature_stats.json.bak-{int(datetime.now(timezone.utc).timestamp())}")
                    shutil.copy2(dst, backup)
                shutil.copy2(seed_context, dst)
                context_status = f"copied_to:{dst}"
            else:
                context_status = "would_copy_on_apply"

    mode = "APPLY" if apply_changes else "DRY_RUN"
    print(f"mode={mode}")
    print(f"target_db={target_db}")
    print(f"seed_source={seed_source}")
    print(f"seed_input={seed_input}")
    if exported_manifest:
        print("seed_manifest_kind=" + str(exported_manifest.get("kind") or "unknown"))
    print("imported_rows_by_table=" + json.dumps(imported, sort_keys=True))
    print("skipped_tables=" + json.dumps(skipped, sort_keys=True))
    print(f"context_feature_stats={context_status}")
    if "learning_state_kv" in imported:
        print(f"learning_state_kv_rows={int(imported.get('learning_state_kv') or 0)}")
    symbol_policy_rows = int(imported.get("symbol_policy") or 0)
    if symbol_policy_rows > 0:
        print(f"symbol_policy_rows={symbol_policy_rows}")
    else:
        symbol_policy_status = str(skipped.get("symbol_policy") or "0_rows")
        print(f"symbol_policy_rows=0")
        print(
            "note=symbol_policy is optional; empty rows are valid depending on seed version/source"
        )
        print(f"symbol_policy_status={symbol_policy_status}")
    if not apply_changes:
        print("note=rerun with --apply to execute the import")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
