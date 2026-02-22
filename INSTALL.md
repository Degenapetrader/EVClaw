# EVClaw Installation Guide

## Prerequisites
- `python3` 3.10+
- `pip` and `venv` (`python3 -m venv`)
- `tmux`
- `git`
- `openclaw` installed and configured with a model provider
- Lighter SDK is optional (only if you enable Lighter venue)

## Quick Install
```bash
cd /path/to/evclaw
python3 -m venv .venv
source .venv/bin/activate
python3 -m pip install -r requirements.txt
cp .env.example .env
# edit .env (required: HYPERLIQUID_ADDRESS, HYPERLIQUID_AGENT_PRIVATE_KEY)
# HYPERLIQUID_ADDRESS = main wallet address
# HYPERLIQUID_AGENT_PRIVATE_KEY = delegated agent signer key
# Do not use your main wallet private key.
# approve builder fee for your wallet: https://atsetup.evplus.ai/
./bootstrap.sh
# Optional warm-start preview (dry-run):
# EVCLAW_ROOT="${EVCLAW_ROOT:-$PWD}" python3 "$EVCLAW_ROOT/scripts/import_learning_seed.py"
./start.sh
```

By default, `bootstrap.sh` also provisions two OpenClaw cron jobs for ops coverage:
- `EVClaw AGI Trader Hourly (deterministic)` (every 15 minutes)
- `EVClaw AGI Trader Hourly Report (system-event)` (every 1 hour)

Cron context source:
- Scheduled cron prompts read only `AGI_SUPERVISOR_MODE` in `AGENTS.md` (ignore `MANUAL_COMMANDS`).

Set `EVCLAW_INSTALL_OPENCLAW_CRONS=0` if you want to skip cron installation.

By default, `bootstrap.sh` also installs helper skills into OpenClaw:
- `trade`, `execute`, `best3`, `hedge`, `stats`

By default, `bootstrap.sh` also sets/provisions isolated OpenClaw agent IDs:
- `evclaw-entry-gate`
- `evclaw-hip3-entry-gate`
- `evclaw-exit-decider`
- `evclaw-hip3-exit-decider`
- `evclaw-learning-reflector`

Control helper skill installation with:
- `EVCLAW_INSTALL_EXTRA_SKILLS=0` to skip
- `EVCLAW_EXTRA_SKILLS=trade,execute,best3,stats,hedge` to customize
- `EVCLAW_OPENCLAW_SKILLS_DIR=/your/path` to change target skills directory
- `EVCLAW_INSTALL_ISOLATED_AGENTS=0` to skip isolated-agent provisioning

Optional Lighter dependency install:
- `EVCLAW_INSTALL_LIGHTER_DEPS=1 ./bootstrap.sh`

`bootstrap.sh` auto-sets `EVCLAW_ROOT` in `.env` to the current repo path so path-dependent defaults stay portable on any machine/location.

## Optional learning warm-start (user-consent only)
Not included in bootstrap by design.
Core learning state is carried in `learning_state_kv` (`patterns`/`adjustments`) rather than `pattern_stats`.
`symbol_policy` rows are optional and can be empty depending on source history/release.

Release seed URL:
- `https://github.com/Degenapetrader/EVClaw/releases/tag/evclaw-learning-seed-v3-20260222`

Current official release seed note (v3, 2026-02-22):
- `learning_state_kv`: populated
- `symbol_policy`: populated

Learning bootstrap options:
```bash
EVCLAW_ROOT="/path/to/evclaw"
# Recommended default for new users: start fresh (no import command needed).

# Optional import path (historical bootstrap): dry-run first.
# Dry-run (default): auto-downloads official release seed + verifies SHA256.
python3 "$EVCLAW_ROOT/scripts/import_learning_seed.py"

# Apply after explicit user approval.
python3 "$EVCLAW_ROOT/scripts/import_learning_seed.py" --apply
```

Dry-run preview:
```bash
EVCLAW_ROOT="/path/to/evclaw"
python3 "$EVCLAW_ROOT/scripts/import_learning_seed.py"
```

Custom seed file (optional):
```bash
EVCLAW_ROOT="/path/to/evclaw"
python3 "$EVCLAW_ROOT/scripts/import_learning_seed.py" --seed /path/to/evclaw-learning-seed.tgz
python3 "$EVCLAW_ROOT/scripts/import_learning_seed.py" --seed /path/to/evclaw-learning-seed.tgz --apply
```

For OpenClaw agents:
- Require user approval first.
- Run dry-run command first.
- Only run `--apply` after user confirms.
- Do not present import as mandatory; fresh start is the recommended default.

Pros:
- Better trading knowledge from real trade history.

Cons:
- Usually fewer trades than fresh-start profile.
- If many users import the same seed, behavior can converge.

## Required EVPlus Endpoints
EVClaw is network-first and expects EVPlus services by default:
- Tracker SSE/API: `tracker.evplus.ai` (port `8443`, endpoint `/sse/tracker`)
- Private node: `https://node2.evplus.ai/evclaw/info`

Node2 auth test (RIGHT way):
```bash
curl -X POST "https://node2.evplus.ai/evclaw/info?key=$HYPERLIQUID_ADDRESS" \
  -H "Content-Type: application/json" \
  --data '{"type":"meta"}'
```

Wrong patterns (do not use):
- `POST /evclaw/meta`
- `POST /evclaw/status`
- `POST /evclaw/info` without `?key=...`
- putting wallet address in JSON body instead of query `key`

## Run Without tmux (Optional)
You can run each service in its own terminal:
- `python3 cycle_trigger.py` (`evclaw-cycle-trigger`)
- `bash run_hl_live_agent.sh` (`evclaw-live-agent`)
- `python3 live_monitor.py` (`evclaw-live-monitor`)
- `python3 -u llm_exit_decider.py` (`evclaw-exit-decider`)
- `python3 -u hip3_exit_decider.py` (`evclaw-hip3-exit-decider`)
- `python3 -u exit_outcome_worker.py` (`evclaw-exit-outcome`)
- `python3 decay_worker.py --signal-flip-only --notify-only` (`evclaw-decay`)
- `python3 position_review_worker.py --record-holds` (`evclaw-review`)
- `python3 run_fill_reconciler.py --mode hybrid` (`evclaw-fill-reconciler`)
- `python3 learning_reflector_worker.py` (`evclaw-learning-reflector`)

## Verify It Is Working
1. Check tmux sessions:
```bash
tmux ls | rg 'evclaw-'
```
2. Check cycle freshness file:
```bash
ls -l /tmp/evclaw_cycle_latest.json
```
3. Run healthcheck:
```bash
bash _agi_flow_healthcheck.sh
```
4. Check logs quickly:
```bash
tmux capture-pane -pt evclaw-cycle-trigger -S -80 | tail -n 40
tmux capture-pane -pt evclaw-live-agent -S -80 | tail -n 40
```

## Troubleshooting
- `openclaw` errors:
  - Ensure `openclaw` is installed and authenticated.
  - Verify agent model routing in `~/.openclaw/openclaw.json` (`agents.list[].model` / `agents.defaults.model`).
- Missing required env vars:
  - Set `HYPERLIQUID_ADDRESS` (main wallet address) and `HYPERLIQUID_AGENT_PRIVATE_KEY` (delegated signer key) in `.env`.
  - Remove legacy `HYPERLIQUID_API` if present.
- Tracker unreachable:
  - Check `EVCLAW_SSE_HOST`, `EVCLAW_SSE_PORT`, `EVCLAW_SSE_ENDPOINT`.
  - Verify `curl -ks https://tracker.evplus.ai/health`.
  - If tracker/node2 auth returns 401/403, approve builder fee at `https://atsetup.evplus.ai/`.
- Node endpoint issues:
  - Check `HYPERLIQUID_PRIVATE_NODE` (default `https://node2.evplus.ai/evclaw/info`).
- SR-limit blocked with `sr_limit_equity_missing`:
  - Ensure `evclaw-live-monitor` is running (it writes `monitor_snapshots` used by SR-limit equity safety checks).
- Proxy/network issues:
  - If needed, set `HYPERLIQUID_PROXIES`.
