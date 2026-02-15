# EVClaw Installation Guide

## Prerequisites
- `python3` 3.10+
- `pip` and `venv` (`python3 -m venv`)
- `tmux`
- `git`
- `openclaw` configured with a model provider (`bootstrap.sh` can auto-install CLI if missing and `npm` exists)

## Quick Install
```bash
cd /path/to/evclaw
python3 -m venv .venv
source .venv/bin/activate
python3 -m pip install -r requirements.txt
cp .env.example .env
# edit .env (required: HYPERLIQUID_ADDRESS, HYPERLIQUID_API)
./bootstrap.sh
./start.sh
```

By default, `bootstrap.sh` also provisions one OpenClaw cron job for ops coverage:
- `EVClaw AGI Trader Hourly (deterministic)`

Set `EVCLAW_INSTALL_OPENCLAW_CRONS=0` if you want to skip cron installation.

`bootstrap.sh` auto-sets `EVCLAW_ROOT` in `.env` to the current repo path so path-dependent defaults stay portable on any machine/location.

## Required EVPlus Endpoints
EVClaw is network-first and expects EVPlus services by default:
- Tracker SSE/API: `tracker.evplus.ai` (port `8443`, endpoint `/sse/tracker`)
- Private node: `https://node2.evplus/info`

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
  - Set `HYPERLIQUID_ADDRESS` and `HYPERLIQUID_API` in `.env`.
- Tracker unreachable:
  - Check `EVCLAW_SSE_HOST`, `EVCLAW_SSE_PORT`, `EVCLAW_SSE_ENDPOINT`.
  - Verify `curl -ks https://tracker.evplus.ai/health`.
- Node endpoint issues:
  - Check `HYPERLIQUID_PRIVATE_NODE` (default `https://node2.evplus/info`).
- Proxy/network issues:
  - If needed, set `HYPERLIQUID_PROXIES`.
