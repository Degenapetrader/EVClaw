#!/usr/bin/env bash
set -euo pipefail

SCRIPT_ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ROOT_DIR="${EVCLAW_ROOT:-$SCRIPT_ROOT_DIR}"
ROOT_DIR="$(cd "$ROOT_DIR" && pwd)"
cd "$ROOT_DIR"

OPENCLAW_CMD="${OPENCLAW_CMD:-openclaw}"
CRON_AGENT="${EVCLAW_OPENCLAW_CRON_AGENT:-main}"
CRON_CHANNEL="${EVCLAW_OPENCLAW_CRON_CHANNEL:-}"
CRON_TO="${EVCLAW_OPENCLAW_CRON_TO:-}"
REMOVE_LEGACY_HLTRADER_CRONS="${EVCLAW_REMOVE_LEGACY_HLTRADER_CRONS:-0}"

DB_PATH="${EVCLAW_DB_PATH:-$ROOT_DIR/ai_trader.db}"
DOCS_DIR="${EVCLAW_DOCS_DIR:-$ROOT_DIR/docs}"

if ! command -v "$OPENCLAW_CMD" >/dev/null 2>&1; then
  echo "openclaw not found; skipping cron install"
  exit 0
fi

if ! "$OPENCLAW_CMD" cron list --json >/dev/null 2>&1; then
  echo "openclaw cron is not available/configured; skipping cron install"
  exit 0
fi

HEALTH_NAME="EVClaw AGI Flow Health Check (every 15min)"
HOURLY_NAME="EVClaw AGI Trader Hourly (consolidated)"
LEGACY_HEALTH_NAME="AGI Flow Health Check (every 15min)"
LEGACY_HOURLY_NAME="AGI Trader Hourly (consolidated)"

health_message="$(cat <<MSG
EVCLAW AGI FLOW HEALTH CHECK — Self-heal mode.

Workdir: $ROOT_DIR
DB: $DB_PATH

Run all checks:
1) Verify tmux sessions:
   - evclaw-cycle-trigger
   - evclaw-live-agent
   - evclaw-exit-decider
   - evclaw-hip3-exit-decider
   - evclaw-exit-outcome
   - evclaw-decay
   - evclaw-review
   - evclaw-fill-reconciler
   - evclaw-learning-reflector
   If missing, run: cd $ROOT_DIR && bash restart.sh <session_name>
2) Cycle freshness: check /tmp/evclaw_cycle_latest.json age; warn if > 600s.
3) Run healthcheck: cd $ROOT_DIR && bash _agi_flow_healthcheck.sh
4) DB integrity quick check:
   python3 - <<'PY'
import sqlite3
db_path = "$DB_PATH"
conn = sqlite3.connect(db_path)
ok = conn.execute("PRAGMA integrity_check").fetchone()[0]
print(ok)
conn.close()
PY
5) SL/TP check:
   SELECT count(*) FROM trades
   WHERE exit_time IS NULL
     AND (sl_order_id IS NULL OR tp_order_id IS NULL)
     AND state NOT IN ('UNDERFILLED','PROPOSED');

Output:
- If healthy: one short line.
- If broken: explain issue + what you fixed.
MSG
)"

hourly_message="$(cat <<MSG
EVCLAW AGI TRADER HOURLY SUMMARY + MAINTENANCE

Workdir: $ROOT_DIR
DB: $DB_PATH
Diary: $DOCS_DIR/openclawdiary.md

Tasks:
1) Health + process check:
   cd $ROOT_DIR && bash _agi_flow_healthcheck.sh
2) DB maintenance check:
   cd $ROOT_DIR && python3 db_maintenance.py --check
3) Last 60m closed trades summary:
   SELECT symbol, venue, realized_pnl
   FROM trades
   WHERE exit_time >= strftime('%s','now')-3600
   ORDER BY exit_time DESC
   LIMIT 20;
4) Equity/exposure snapshot from latest monitor snapshots.
5) Append one short diary line to $DOCS_DIR/openclawdiary.md

Output format:
- 2-4 short lines suitable for ops notifications.
- Line 1: health status and any critical issues.
- Line 2: closes last 60m summary.
- Line 3: equity/exposure summary.
MSG
)"

find_job_ids_by_name() {
  local name="$1"
  "$OPENCLAW_CMD" cron list --json | python3 -c '
import json
import sys

target = sys.argv[1]
raw = sys.stdin.read()
if not raw.strip():
    raise SystemExit(0)
try:
    payload = json.loads(raw)
except Exception:
    raise SystemExit(0)
jobs = payload.get("jobs", [])
for job in jobs:
    if str(job.get("name") or "") == target:
        jid = str(job.get("id") or "").strip()
        if jid:
            print(jid)
' "$name"
}

remove_jobs_by_name() {
  local name="$1"
  local ids
  ids="$(find_job_ids_by_name "$name" || true)"
  if [[ -z "${ids:-}" ]]; then
    return 0
  fi
  while IFS= read -r jid; do
    [[ -z "$jid" ]] && continue
    "$OPENCLAW_CMD" cron remove "$jid" >/dev/null
  done <<< "$ids"
}

add_cron_job() {
  local name="$1"
  local expr="$2"
  local message="$3"

  local cmd=(
    "$OPENCLAW_CMD" cron add
    --name "$name"
    --cron "$expr"
    --message "$message"
    --agent "$CRON_AGENT"
  )
  if [[ -n "$CRON_CHANNEL" ]]; then
    cmd+=(--channel "$CRON_CHANNEL")
  fi
  if [[ -n "$CRON_TO" ]]; then
    cmd+=(--to "$CRON_TO")
  fi
  "${cmd[@]}" >/dev/null
}

remove_jobs_by_name "$HEALTH_NAME"
remove_jobs_by_name "$HOURLY_NAME"

if [[ "$REMOVE_LEGACY_HLTRADER_CRONS" == "1" || "$REMOVE_LEGACY_HLTRADER_CRONS" == "true" ]]; then
  remove_jobs_by_name "$LEGACY_HEALTH_NAME"
  remove_jobs_by_name "$LEGACY_HOURLY_NAME"
fi

add_cron_job "$HEALTH_NAME" "*/15 * * * *" "$health_message"
add_cron_job "$HOURLY_NAME" "0 * * * *" "$hourly_message"

echo "OpenClaw cron jobs installed:"
echo "- $HEALTH_NAME"
echo "- $HOURLY_NAME"
