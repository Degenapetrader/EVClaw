#!/usr/bin/env bash
set -euo pipefail

SCRIPT_ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ROOT_DIR="${EVCLAW_ROOT:-$SCRIPT_ROOT_DIR}"
ROOT_DIR="$(cd "$ROOT_DIR" && pwd)"
cd "$ROOT_DIR"

# Resolve python: venv > system.
EVCLAW_PYTHON="$ROOT_DIR/.venv/bin/python3"
if [[ ! -x "$EVCLAW_PYTHON" ]]; then
  EVCLAW_PYTHON="python3"
fi

OPENCLAW_CMD="${OPENCLAW_CMD:-openclaw}"
CRON_AGENT="${EVCLAW_OPENCLAW_CRON_AGENT:-main}"
CRON_CHANNEL="${EVCLAW_OPENCLAW_CRON_CHANNEL:-main}"
CRON_TO="${EVCLAW_OPENCLAW_CRON_TO:-}"
if [[ -z "${CRON_CHANNEL//[[:space:]]/}" && -z "${CRON_TO//[[:space:]]/}" ]]; then
  CRON_CHANNEL="main"
fi

DB_PATH="${EVCLAW_DB_PATH:-$ROOT_DIR/ai_trader.db}"
RUNTIME_DIR="${EVCLAW_RUNTIME_DIR:-$ROOT_DIR/state}"
REPORT_PATH="${EVCLAW_HOURLY_REPORT_PATH:-$RUNTIME_DIR/hourly_ops_report.json}"
SUMMARY_PATH="${EVCLAW_HOURLY_SUMMARY_PATH:-$RUNTIME_DIR/hourly_ops_summary.txt}"

JOB_NAME="EVClaw AGI Trader Hourly (deterministic)"
REPORT_JOB_NAME="EVClaw AGI Trader Hourly Report (system-event)"
LEGACY_HEALTH_NAME="EVClaw AGI Flow Health Check (every 15min)"
LEGACY_HOURLY_NAME="EVClaw AGI Trader Hourly (consolidated)"

if ! command -v "$OPENCLAW_CMD" >/dev/null 2>&1; then
  echo "openclaw not found; skipping cron install"
  exit 0
fi

if ! "$OPENCLAW_CMD" cron list --json >/dev/null 2>&1; then
  echo "openclaw cron is not available/configured; skipping cron install"
  exit 0
fi

hourly_message="$(cat <<MSG
EVCLAW DETERMINISTIC HOURLY OPS

Workdir: $ROOT_DIR
DB: $DB_PATH

Run:
1) cd $ROOT_DIR && $EVCLAW_PYTHON hourly_ops.py --db $DB_PATH --json-out $REPORT_PATH --summary-out $SUMMARY_PATH
2) Read and return ONLY summary lines from $SUMMARY_PATH (max 4 lines).
3) If runner exits non-zero, report failure and include top error from $REPORT_PATH.
4) Do not request, print, or modify secret keys. Runtime auth uses HYPERLIQUID_AGENT_PRIVATE_KEY (agent signer key for HYPERLIQUID_ADDRESS).
MSG
)"

report_message="$(cat <<MSG
EVCLAW HOURLY REPORT (DOUBLE-CHECK)

Workdir: $ROOT_DIR
DB: $DB_PATH
Report: $REPORT_PATH
Summary: $SUMMARY_PATH

Read report + summary and post a compact operational check:
1) Confirm latest deterministic run freshness and status.
2) Confirm key counters (open_trades, pending_limit_cancels, unprotected_perps, unprotected_builder, protection_unknown).
3) Highlight any WARN/CRIT conditions and required operator actions.
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
for job in payload.get("jobs", []):
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

add_system_event_job() {
  local name="$1"
  local expr="$2"
  local text="$3"

  local payload
  payload="$(
    NAME="$name" EXPR="$expr" TEXT="$text" python3 - <<'PY'
import json
import os

print(
    json.dumps(
        {
            "name": os.environ.get("NAME", ""),
            "schedule": {"kind": "cron", "expr": os.environ.get("EXPR", "0 * * * *")},
            "payload": {"kind": "systemEvent", "text": os.environ.get("TEXT", "")},
            "sessionTarget": "isolated",
            "delivery": {"mode": "announce", "channel": "main"},
        }
    )
)
PY
  )"

  if "$OPENCLAW_CMD" cron add --job "$payload" >/dev/null 2>&1; then
    return 0
  fi

  # Fallback for OpenClaw builds that only support --message.
  add_cron_job "$name" "$expr" "$text"
}

remove_jobs_by_name "$JOB_NAME"
remove_jobs_by_name "$REPORT_JOB_NAME"
remove_jobs_by_name "$LEGACY_HEALTH_NAME"
remove_jobs_by_name "$LEGACY_HOURLY_NAME"

add_cron_job "$JOB_NAME" "*/15 * * * *" "$hourly_message"
add_system_event_job "$REPORT_JOB_NAME" "0 * * * *" "$report_message"

echo "OpenClaw cron jobs installed:"
echo "- $JOB_NAME"
echo "- $REPORT_JOB_NAME"
