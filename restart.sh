#!/usr/bin/env bash
# restart.sh — Rolling restart of all EVClaw tmux sessions.
# Usage: ./restart.sh [session_name]  (no arg = restart all)
#
# Rolling: restarts one at a time with a 2s pause between each,
# so the system is never fully down.

set -euo pipefail
DIR="$(cd "$(dirname "$0")" && pwd)"

declare -A SESSIONS
SESSIONS=(
  # Core EVClaw
  [evclaw-cycle-trigger]="python3 cycle_trigger.py 2>&1"
  [evclaw-live-agent]="bash -c './run_hl_live_agent.sh 2>&1'"
  [evclaw-exit-decider]="bash -c 'set -a && source .env && set +a && export EVCLAW_EXIT_DECIDER_VENUES=\"\${EVCLAW_EXIT_DECIDER_VENUES:-\${EVCLAW_PERPS_EXIT_DECIDER_VENUES:-hyperliquid,lighter}}\" && export EVCLAW_EXIT_DECIDER_SYMBOL_PREFIXES=\"\${EVCLAW_EXIT_DECIDER_SYMBOL_PREFIXES:-\${EVCLAW_PERPS_EXIT_DECIDER_SYMBOL_PREFIXES:-}}\" && python3 -u llm_exit_decider.py 2>&1'"
  # Dedicated HIP3 exit decider (XYZ + hip3, isolated from global EXIT_DECIDER env).
  [evclaw-hip3-exit-decider]="bash -c 'set -a && source .env && set +a && python3 -u hip3_exit_decider.py 2>&1'"
  [evclaw-exit-outcome]="bash -c 'set -a && source .env && set +a && python3 -u exit_outcome_worker.py 2>&1'"
  [evclaw-decay]="python3 decay_worker.py --signal-flip-only --notify-only 2>&1"
  [evclaw-review]="python3 position_review_worker.py --record-holds 2>&1"
  [evclaw-fill-reconciler]="python3 run_fill_reconciler.py --mode hybrid 2>&1"
  [evclaw-learning-reflector]="python3 learning_reflector_worker.py 2>&1"
)

# Order matters:
# - reconciler/learning early (tracking)
# - producers/deciders next
# - trigger last
ORDER=(
  evclaw-fill-reconciler
  evclaw-learning-reflector
  evclaw-decay
  evclaw-review
  evclaw-exit-decider
  evclaw-hip3-exit-decider
  evclaw-exit-outcome
  evclaw-live-agent
  evclaw-cycle-trigger
)

restart_session() {
  local name="$1"
  local cmd="${SESSIONS[$name]}"
  
  if tmux has-session -t "$name" 2>/dev/null; then
    echo "[$name] killing..."
    tmux kill-session -t "$name"
    sleep 1
  fi
  
  echo "[$name] starting: $cmd"
  tmux new-session -d -s "$name" "cd $DIR && $cmd"
  sleep 2
  
  # Verify it's alive
  if tmux has-session -t "$name" 2>/dev/null; then
    echo "[$name] ✅ running"
  else
    echo "[$name] ❌ FAILED TO START"
  fi
}

if [ $# -ge 1 ]; then
  # Restart specific session
  target="$1"
  if [[ -v "SESSIONS[$target]" ]]; then
    restart_session "$target"
  else
    echo "Unknown session: $target"
    echo "Valid: ${!SESSIONS[*]}"
    exit 1
  fi
else
  # Rolling restart all
  echo "=== EVClaw rolling restart ==="
  echo "Dir: $DIR"
  echo ""
  for name in "${ORDER[@]}"; do
    restart_session "$name"
  done
  echo ""
  echo "=== All sessions restarted ==="
  tmux list-sessions 2>/dev/null | grep -E "^(evclaw-)" || true
fi
