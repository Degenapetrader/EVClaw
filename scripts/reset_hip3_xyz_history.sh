#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DB_PATH="${EVCLAW_DB_PATH:-${EVCLAW_DB_PATH:-$ROOT_DIR/ai_trader.db}}"
SQL_FILE="$ROOT_DIR/scripts/reset_hip3_xyz_history.sql"
EXECUTE=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --execute)
      EXECUTE=1
      shift
      ;;
    --db)
      DB_PATH="$2"
      shift 2
      ;;
    *)
      echo "Unknown arg: $1" >&2
      echo "Usage: $0 [--execute] [--db /path/to/ai_trader.db]" >&2
      exit 1
      ;;
  esac
done

if [[ ! -f "$DB_PATH" ]]; then
  echo "DB not found: $DB_PATH" >&2
  exit 1
fi

if [[ ! -f "$SQL_FILE" ]]; then
  echo "SQL file not found: $SQL_FILE" >&2
  exit 1
fi

report_counts() {
  sqlite3 -readonly "$DB_PATH" <<'SQL'
SELECT 'trades_xyz_total', COUNT(*) FROM trades WHERE symbol LIKE 'XYZ:%';
SELECT 'trades_xyz_open', COUNT(*) FROM trades WHERE symbol LIKE 'XYZ:%' AND exit_time IS NULL;
SELECT 'trade_proposals_xyz_total', COUNT(*) FROM trade_proposals WHERE symbol LIKE 'XYZ:%';
SELECT 'trade_features_xyz_total', COUNT(*) FROM trade_features WHERE symbol LIKE 'XYZ:%';
SELECT 'reflections_v2_xyz_total', COUNT(*)
FROM reflections_v2 r JOIN trades t ON t.id = r.trade_id
WHERE t.symbol LIKE 'XYZ:%';
SELECT 'reflection_tasks_xyz_total', COUNT(*)
FROM reflection_tasks_v1 rt LEFT JOIN trades t ON t.id = rt.trade_id
WHERE (t.symbol LIKE 'XYZ:%') OR (rt.symbol LIKE 'XYZ:%');
SELECT 'symbol_learning_state_xyz_total', COUNT(*) FROM symbol_learning_state WHERE symbol LIKE 'XYZ:%';
SELECT 'signal_symbol_stats_xyz_total', COUNT(*) FROM signal_symbol_stats WHERE symbol LIKE 'XYZ:%';
SELECT 'combo_symbol_stats_xyz_total', COUNT(*) FROM combo_symbol_stats WHERE symbol LIKE 'XYZ:%';
SQL
}

open_count="$(sqlite3 -readonly "$DB_PATH" "SELECT COUNT(*) FROM trades WHERE symbol LIKE 'XYZ:%' AND exit_time IS NULL;")"

echo "DB: $DB_PATH"
echo "Pre-reset counts:"
report_counts

if [[ "$open_count" != "0" ]]; then
  echo
  echo "Refusing reset: open XYZ positions remain ($open_count)." >&2
  echo "Close these first:" >&2
  sqlite3 -readonly "$DB_PATH" "SELECT id, symbol, direction, venue, entry_time FROM trades WHERE symbol LIKE 'XYZ:%' AND exit_time IS NULL ORDER BY entry_time;" >&2
  exit 2
fi

if [[ "$EXECUTE" != "1" ]]; then
  echo
  echo "Dry-run only. Re-run with --execute to perform backup + reset." >&2
  exit 0
fi

backup_dir="$ROOT_DIR/backups/hip3-reset"
mkdir -p "$backup_dir"
ts="$(date -u +%Y%m%dT%H%M%SZ)"
backup_path="$backup_dir/ai_trader.db.${ts}.pre_xyz_reset.sqlite"

echo
echo "Creating online backup: $backup_path"
sqlite3 "$DB_PATH" "PRAGMA wal_checkpoint(FULL);"
sqlite3 "$DB_PATH" ".backup '$backup_path'"

echo "Applying reset SQL..."
sqlite3 "$DB_PATH" < "$SQL_FILE"

echo "Vacuuming DB..."
sqlite3 "$DB_PATH" "VACUUM;"

echo
echo "Post-reset counts:"
report_counts

echo
echo "Reset complete. Backup saved at: $backup_path"
