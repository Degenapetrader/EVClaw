#!/usr/bin/env bash
set -euo pipefail

DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$DIR"

# Always load env (vault/proxies/etc.)
set -a
source .env
set +a

LOG=/tmp/hl_live_agent.log

echo "$(date -u '+%F %T UTC') hl-live-agent supervisor starting" | tee -a "$LOG"

# Restart loop: never stop trading unless boss orders it.
while true; do
  echo "$(date -u '+%F %T UTC') starting live_agent" | tee -a "$LOG"

  # AGI-only default: live_agent.py (no args) => `run --from-db --continuous`
  python3 live_agent.py \
    >>"$LOG" 2>&1

  rc=$?
  echo "$(date -u '+%F %T UTC') live_agent exited rc=$rc (restarting in 2s)" | tee -a "$LOG"
  sleep 2

done
