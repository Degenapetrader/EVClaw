# EVClaw

EVClaw is an autonomous AGI trading skill for Hyperliquid:
- Perps trading
- HIP3 builder stocks
- Deterministic ops + OpenClaw agent supervision

It is designed to run on a fresh Linux VPS with no dependency on your private local file layout.

## What EVClaw does

- Ingests market/tracker data from internet endpoints.
- Builds opportunities and ranks candidates.
- Uses OpenClaw agents for entry and exit decisions.
- Executes through one wallet identity (`HYPERLIQUID_ADDRESS` + `HYPERLIQUID_API`).
- Runs deterministic hourly/15m maintenance checks for safety.

## AGI flow

`cycle_trigger` -> context builder -> entry gate -> executor -> position tracking -> exit producers -> exit decider -> executor close

Rules:
- Producers do not execute orders directly.
- Executor is limit-first/chase-limit workflow.
- DB is the source of truth for active trades and reconciliation.

## Requirements

- Linux VPS
- `python3` (3.10+ recommended)
- `pip` + virtualenv
- `tmux`
- `git`
- OpenClaw installed and configured with a working LLM provider

## Quick start

```bash
git clone <your-repo-url> EVClaw
cd EVClaw
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
# edit .env with your values
./bootstrap.sh
./start.sh
```

## Required environment variables

At minimum set:
- `HYPERLIQUID_ADDRESS`
- `HYPERLIQUID_API`

Common network defaults:
- tracker SSE host: `tracker.evplus.ai:8443`
- HL private node/info endpoint: `node2.evplus.ai` (as configured in `.env`)

## Trading modes (important)

EVClaw supports 3 top-level trading modes:

1. `conservative`
2. `balanced` (default)
3. `aggressive`

Behavior intent:
- `conservative`: fewer trades, stricter entry filters, smaller risk and sizing.
- `balanced`: production baseline.
- `aggressive`: more trades, looser entry filters, higher risk and sizing.

Where to change mode:
- Edit `config.mode_controller.mode` in `skill.yaml`.

For OpenClaw agents and human operators:
- First change only `config.mode_controller.mode`.
- Do not change individual sliders/overrides unless explicitly asked.

Example:

```yaml
config:
  mode_controller:
    mode: aggressive
```

## Process model

Default runtime uses tmux sessions started by `./start.sh` (wrapper over `restart.sh`).

Core sessions include:
- `hl-cycle-trigger`
- `hl-live-agent`
- `hl-decay`
- `hl-review`
- `hl-exit-decider`
- `hl-hip3-exit-decider`
- `hl-learning-reflector`
- `hl-exit-outcome`
- `fill-reconciler`
- `hl-vp-worker`
- `hl-sr-worker`
- `hl-trend-worker`

## Operations and health

- Deterministic ops job runs on schedule for reconciliation/maintenance.
- OpenClaw cron job posts hourly status summary to main chat.
- Health/ops output is written to local runtime files and logs.

## Troubleshooting

- If no trades appear, verify OpenClaw agent IDs and provider config.
- If SSE fails, verify tracker endpoint/key in `.env`.
- If HL auth fails, verify `HYPERLIQUID_ADDRESS` and `HYPERLIQUID_API`.
- If processes are missing, run `./start.sh` again and inspect tmux sessions.

## Safety notice

This is real-money trading software. Start with small size, confirm live behavior, and monitor continuously before scaling.
