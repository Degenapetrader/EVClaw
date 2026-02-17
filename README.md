# EVClaw

EVClaw is an autonomous AGI trading skill for Hyperliquid:
- Perps trading
- HIP3 builder stocks
- Deterministic ops + OpenClaw agent supervision

It is designed to run on a fresh Linux VPS with no dependency on your private local file layout.

AI/operator runtime contract:
- `AGENTS.md` is the canonical context map for this repo.
- Scheduled cron prompts read only `CRON_CONTEXT` in `AGENTS.md` (not `MANUAL_COMMANDS`).

## What EVClaw does

- Ingests market/tracker data from internet endpoints.
- Builds opportunities and ranks candidates.
- Uses OpenClaw agents for entry and exit decisions.
- Executes with one wallet identity: `HYPERLIQUID_ADDRESS` + delegated signer `HYPERLIQUID_AGENT_PRIVATE_KEY`.
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
- Lighter SDK is optional (only needed if you enable Lighter venue)

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
# Optional warm-start preview (dry-run):
# EVCLAW_ROOT="${EVCLAW_ROOT:-$PWD}" python3 "$EVCLAW_ROOT/scripts/import_learning_seed.py"
./start.sh
```

`bootstrap.sh` installs EVClaw into OpenClaw `skills/` and also installs helper skills:
- `trade`
- `execute`
- `best3`
- `hedge`
- `stats`

Default target is `~/.openclaw/skills` (override with `EVCLAW_OPENCLAW_SKILLS_DIR`).

`bootstrap.sh` also sets/provisions isolated OpenClaw agent IDs (no `default` routing):
- `evclaw-entry-gate`
- `evclaw-hip3-entry-gate`
- `evclaw-exit-decider`
- `evclaw-hip3-exit-decider`
- `evclaw-learning-reflector`

## Optional: import historical learning (opt-in)

This is optional and intentionally NOT part of `bootstrap.sh`.
Use it only when the user explicitly agrees.
Core learning state imports from `learning_state_kv` (`patterns`/`adjustments`), not a `pattern_stats` SQL table.
`symbol_policy` rows are optional and may be empty depending on source history/release.

Seed publishing is maintainer-managed; end users only need the import flow below.

User side (import into EVClaw from release):

```bash
EVCLAW_ROOT="/path/to/EVClaw"
# Dry-run (default): auto-downloads official release seed + verifies SHA256.
python3 "$EVCLAW_ROOT/scripts/import_learning_seed.py"

# Apply after explicit user approval.
python3 "$EVCLAW_ROOT/scripts/import_learning_seed.py" --apply
```

Current official release seed note (v1, 2026-02-16):
- `learning_state_kv`: populated
- `symbol_policy`: `0` rows (expected for this version)

OpenClaw agent instruction:
- Ask for explicit user consent before importing learning seed.
- Run dry-run first.
- Run `--apply` only after user confirms.
- If using a custom seed file instead of official release, pass `--seed /path/to/seed.tgz`.

Pros:
- Better trading knowledge from real trade history.

Cons:
- Usually fewer trades than a fresh start profile.
- If many users import the same seed, behavior can become similar across users.

## Required environment variables

At minimum set:
- `HYPERLIQUID_ADDRESS`
- `HYPERLIQUID_AGENT_PRIVATE_KEY`
- `HYPERLIQUID_ADDRESS` is the main wallet address being traded.
- `HYPERLIQUID_AGENT_PRIVATE_KEY` is the delegated agent signer key authorized for that wallet.
- Do not use your main wallet private key.
- Keep EVClaw agent IDs as dedicated IDs (defaults above), not `default`.

Common network defaults:
- tracker SSE host: `tracker.evplus.ai:8443`
- HL private node/info endpoint: `https://node2.evplus.ai/evclaw/info` (as configured in `.env`)
- Before first run, approve builder fee for your wallet: `https://atsetup.evplus.ai/`

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
- `evclaw-cycle-trigger`
- `evclaw-live-agent`
- `evclaw-live-monitor`
- `evclaw-exit-decider`
- `evclaw-hip3-exit-decider`
- `evclaw-exit-outcome`
- `evclaw-decay`
- `evclaw-review`
- `evclaw-fill-reconciler`
- `evclaw-learning-reflector`

## OpenClaw helper commands

After bootstrap, these user-facing helper skills are available:
- `/trade <SYMBOL>`
- `/execute <PLAN_ID> chase|limit [ttl]`
- `/best3`
- `/hedge`
- `/stats`

Command split:
- `/execute <PLAN_ID> chase|limit` is helper-skill manual plan execution.
- `python3 cli.py execute --cycle-file ...` is the low-level cycle executor path.

Operator note:
- This section is manual/interactive guidance.
- Scheduled cron jobs must use `AGENTS.md` `CRON_CONTEXT` only.

## Operations and health

- Deterministic ops cron runs every 15 minutes for reconciliation/maintenance.
- OpenClaw hourly report cron posts a status summary to main chat.
- Health/ops output is written to local runtime files and logs.

## Troubleshooting

- If no trades appear, verify OpenClaw agent IDs and provider config.
- If SSE fails, verify tracker endpoint/key in `.env`.
- If SSE/node2 returns 401/403, approve builder fee for your wallet at `https://atsetup.evplus.ai/`.
- If HL auth fails, verify `HYPERLIQUID_ADDRESS` and `HYPERLIQUID_AGENT_PRIVATE_KEY`.
- If `HYPERLIQUID_API` appears in your `.env`, remove it and use `HYPERLIQUID_AGENT_PRIVATE_KEY`.
- If you see `sr_limit_equity_missing`, check that `evclaw-live-monitor` is running (it writes `monitor_snapshots` used for SR-limit equity caps).
- If processes are missing, run `./start.sh` again and inspect tmux sessions.

## Safety notice

This is real-money trading software. Start with small size, confirm live behavior, and monitor continuously before scaling.
