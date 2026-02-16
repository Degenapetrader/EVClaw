#!/usr/bin/env bash
set -euo pipefail
DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$DIR"

DB="${EVCLAW_DB_PATH:-$DIR/ai_trader.db}"
RUNTIME_DIR="${EVCLAW_RUNTIME_DIR:-$DIR/state}"
DOCS_DIR="${EVCLAW_DOCS_DIR:-$DIR/docs}"
export RUNTIME_DIR
export EVCLAW_DB_PATH="${DB}"
export EVCLAW_RUNTIME_DIR="${RUNTIME_DIR}"
export EVCLAW_DOCS_DIR="${DOCS_DIR}"
REQ=(evclaw-cycle-trigger evclaw-live-agent evclaw-live-monitor evclaw-fill-reconciler evclaw-exit-decider evclaw-hip3-exit-decider evclaw-exit-outcome evclaw-decay evclaw-review evclaw-learning-reflector)

restarted=()
missing=()
for s in "${REQ[@]}"; do
  if tmux has-session -t "$s" 2>/dev/null; then
    :
  else
    missing+=("$s")
  fi
done

if [ ${#missing[@]} -gt 0 ]; then
  for s in "${missing[@]}"; do
    echo "ACTION: restarting tmux session $s"
    bash restart.sh "$s" >/tmp/agi_health_restart_${s}.log 2>&1 || true
    if tmux has-session -t "$s" 2>/dev/null; then
      restarted+=("$s")
    else
      echo "ERROR: failed to restart $s (see /tmp/agi_health_restart_${s}.log)"
    fi
  done
fi

# print session status after any restarts
for s in "${REQ[@]}"; do
  if tmux has-session -t "$s" 2>/dev/null; then
    echo "TMUX:$s=UP"
  else
    echo "TMUX:$s=DOWN"
  fi
done

echo "RESTARTED:${restarted[*]:-NONE}"

python3 - <<'PY'
import json
import os
import time
from datetime import datetime
from urllib.error import URLError
from urllib.request import Request, urlopen

def age(path):
    if not os.path.exists(path):
        return None
    return int(time.time() - os.stat(path).st_mtime)

runtime_dir = os.environ.get("RUNTIME_DIR") or os.environ.get("EVCLAW_RUNTIME_DIR") or ""

paths = {
  'cycle_latest': '/tmp/evclaw_cycle_latest.json',
  'hip3_window_state': f'{runtime_dir}/hip3_window_state.json',
  'hip3_flow_ofm_stats': f'{runtime_dir}/hip3_flow_ofm_stats.json',
}
for k,p in paths.items():
    a = age(p)
    print(f'AGE:{k}={a if a is not None else "NOFILE"}')

pred_url = os.environ.get("EVCLAW_TRACKER_HIP3_PREDATOR_URL") or ""


def _load_predator_payload():
    if pred_url:
        try:
            req = Request(pred_url, headers={"User-Agent": "EVClawHealthcheck/1.0"})
            with urlopen(req, timeout=5) as response:
                return json.load(response)
        except (OSError, URLError, ValueError, json.JSONDecodeError):
            return None
    return None


j = _load_predator_payload()
if not j:
    print('HIP3_PREDATOR:NOFILE')
else:
    gen = j.get('generated_at', j.get('generatedAt'))
    now=time.time()
    if gen is None:
        gen_age='MISSING'
    elif isinstance(gen,(int,float)):
        if gen>1e12:
            gen/=1000.0
        gen_age=int(now-gen)
    elif isinstance(gen,str):
        from datetime import datetime
        try:
            dt=datetime.fromisoformat(gen.replace('Z','+00:00'))
            gen_age=int(now-dt.timestamp())
        except Exception:
            gen_age='UNPARSEABLE'
    else:
        gen_age='UNSUPPORTED'

    market_session = j.get('market_session', j.get('marketSession'))
    signals_paused = j.get('signals_paused', j.get('signalsPaused'))

    symbols = j.get('symbols') or j.get('data') or []
    # symbols may be list[dict] or dict[symbol->dict]
    if isinstance(symbols, dict):
        items = list(symbols.values())
        sym_count = len(symbols)
    elif isinstance(symbols, list):
        items = symbols
        sym_count = len(symbols)
    else:
        items = []
        sym_count = 0

    non_neutral=0
    for it in items:
        if not isinstance(it, dict):
            continue
        flow = it.get('flow') or {}
        ofm = it.get('ofm_pred') or it.get('ofmPred') or {}
        fd = str(flow.get('direction','NEUTRAL')).upper()
        od = str(ofm.get('direction','NEUTRAL')).upper()
        if fd!='NEUTRAL' or od!='NEUTRAL':
            non_neutral += 1

    print(f'HIP3_PREDATOR:generated_at_age_s={gen_age}')
    print(f'HIP3_PREDATOR:market_session={market_session}')
    print(f'HIP3_PREDATOR:signals_paused={signals_paused}')
    print(f'HIP3_PREDATOR:symbol_count={sym_count}')
    print(f'HIP3_PREDATOR:non_neutral_count={non_neutral}')
    if pred_url:
        print(f'HIP3_PREDATOR:source=url:{pred_url}')
    else:
        print('HIP3_PREDATOR:source=unset')
PY

# ENTRY GATE: last 15m cycle runs
echo "SQL:CYCLE_RUNS"
sqlite3 "$DB" "SELECT seq, processed_status, substr(processed_summary,1,200), substr(processed_error,1,100) FROM cycle_runs WHERE timestamp >= strftime('%s','now')-900 ORDER BY seq DESC LIMIT 5;" || true

# Exit errors last 15m
echo "SQL:RECENT_EXIT_ERRORS"
sqlite3 "$DB" "SELECT exit_reason, count(*) FROM trades WHERE exit_time >= strftime('%s','now')-900 AND (exit_reason LIKE '%ERROR%' OR exit_reason LIKE '%FAIL%') GROUP BY exit_reason;" || true

# Underfills last 15m
echo "SQL:UNDERFILL_COUNT"
sqlite3 "$DB" "SELECT count(*) FROM trades WHERE entry_time >= strftime('%s','now')-900 AND (state='UNDERFILLED' OR exit_reason LIKE '%UNDERFILL%');" || true

# SL/TP missing
echo "SQL:SLTP_MISSING"
sqlite3 "$DB" "SELECT id,symbol,direction,venue FROM trades WHERE exit_time IS NULL AND (sl_order_id IS NULL OR tp_order_id IS NULL) AND state NOT IN ('UNDERFILLED','PROPOSED');" || true

# Exit decider tails (last 30 lines) + simple error scan
for t in evclaw-exit-decider evclaw-hip3-exit-decider; do
  echo "TMUX_TAIL:$t"
  if tmux has-session -t "$t" 2>/dev/null; then
    tailtxt=$(tmux capture-pane -t "$t" -p -S -30 2>/dev/null || true)
    # Print only if suspicious
    if echo "$tailtxt" | egrep -i -q "Traceback|Exception|MODEL_ERROR|EXECUTION_ERROR|SLTP_FAILED|FATAL"; then
      echo "TMUX_TAIL_SUSPECT:$t=YES"
      echo "$tailtxt" | tail -n 30
    else
      echo "TMUX_TAIL_SUSPECT:$t=NO"
    fi
  else
    echo "TMUX_TAIL_SUSPECT:$t=DOWN"
  fi
  echo "--"
done
