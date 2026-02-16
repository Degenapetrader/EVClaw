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
./start.sh
```

By default, `bootstrap.sh` also provisions one OpenClaw cron job for ops coverage:
- `EVClaw AGI Trader Hourly (deterministic)`

Set `EVCLAW_INSTALL_OPENCLAW_CRONS=0` if you want to skip cron installation.

By default, `bootstrap.sh` also installs helper skills into OpenClaw:
- `trade`, `execute`, `best3`, `hedge`, `stats`

Control helper skill installation with:
- `EVCLAW_INSTALL_EXTRA_SKILLS=0` to skip
- `EVCLAW_EXTRA_SKILLS=trade,execute,best3,stats,hedge` to customize
- `EVCLAW_OPENCLAW_SKILLS_DIR=/your/path` to change target skills directory

Optional Lighter dependency install:
- `EVCLAW_INSTALL_LIGHTER_DEPS=1 ./bootstrap.sh`

`bootstrap.sh` auto-sets `EVCLAW_ROOT` in `.env` to the current repo path so path-dependent defaults stay portable on any machine/location.

## Optional learning warm-start (user-consent only)
Not included in bootstrap by design.
Seed includes learned `symbol_policy` rows (per-symbol SL/TP adjustments).

Release seed URL:
- `https://github.com/Degenapetrader/EVClaw/releases/tag/evclaw-learning-seed-v1-20260216`

Import command:
```bash
curl -L -o /tmp/evclaw-learning-seed.tgz \
  https://github.com/Degenapetrader/EVClaw/releases/download/evclaw-learning-seed-v1-20260216/evclaw-learning-seed.tgz
curl -L -o /tmp/evclaw-learning-seed.tgz.sha256 \
  https://github.com/Degenapetrader/EVClaw/releases/download/evclaw-learning-seed-v1-20260216/evclaw-learning-seed.tgz.sha256
cd /tmp && sha256sum -c evclaw-learning-seed.tgz.sha256
cd /path/to/evclaw
python3 scripts/import_learning_seed.py --seed /path/to/evclaw-learning-seed.tgz --apply
```

Dry-run preview:
```bash
python3 scripts/import_learning_seed.py --seed /tmp/evclaw-learning-seed.tgz
```

For OpenClaw agents:
- Require user approval first.
- Run dry-run command first.
- Only run `--apply` after user confirms.

Pros:
- Better trading knowledge from real trade history.

Cons:
- Usually fewer trades than fresh-start profile.
- If many users import the same seed, behavior can converge.

## Required EVPlus Endpoints
EVClaw is network-first and expects EVPlus services by default:
- Tracker SSE/API: `tracker.evplus.ai` (port `8443`, endpoint `/sse/tracker`)
- Private node: `https://node2.evplus.ai/evclaw/info`

## Run Without tmux (Optional)
You can run each service in its own terminal:
- `python3 cycle_trigger.py` (`evclaw-cycle-trigger`)
- `bash run_hl_live_agent.sh` (`evclaw-live-agent`)
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
  - Verify agent model names in `.env` (`EVCLAW_*_MODEL`).
- Missing required env vars:
  - Set `HYPERLIQUID_ADDRESS` (main wallet address) and `HYPERLIQUID_AGENT_PRIVATE_KEY` (delegated signer key) in `.env`.
  - Remove legacy `HYPERLIQUID_API` if present.
- Tracker unreachable:
  - Check `EVCLAW_SSE_HOST`, `EVCLAW_SSE_PORT`, `EVCLAW_SSE_ENDPOINT`.
  - Verify `curl -ks https://tracker.evplus.ai/health`.
  - If tracker/node2 auth returns 401/403, approve builder fee at `https://atsetup.evplus.ai/`.
- Node endpoint issues:
  - Check `HYPERLIQUID_PRIVATE_NODE` (default `https://node2.evplus.ai/evclaw/info`).
- Proxy/network issues:
  - If needed, set `HYPERLIQUID_PROXIES`.
