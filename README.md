# EVClaw

EVClaw is an autonomous AGI trading skill for Hyperliquid:
- Perps trading
- HIP3 builder stocks
- Deterministic ops + OpenClaw agent supervision

It is designed to run on a fresh Linux VPS with no dependency on your private local file layout.

AI/operator runtime contract:
- `AGENTS.md` is the canonical context map for this repo.
- Scheduled cron prompts read only `AGI_SUPERVISOR_MODE` block in `AGENTS.md` (inside `CRON_CONTEXT`) and `Tool.md` `CRON_SAFE_TOOLS` (not `MANUAL_COMMANDS`).

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
# Required after install/update: ensure OpenClaw cron jobs are installed/active.
./scripts/install_openclaw_crons.sh
openclaw cron list --json
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

Cron install is critical:
- `bootstrap.sh` attempts cron install automatically.
- Always run `./scripts/install_openclaw_crons.sh` manually after fresh setup or git update.
- Verify `openclaw cron list --json` contains both:
  - `EVClaw AGI Trader Hourly (deterministic)`
  - `EVClaw AGI Trader Hourly Report (system-event)`
- If jobs are missing, deterministic safety checks and repo update notifications will not run.

## Optional: import historical learning (opt-in)

This is optional and intentionally NOT part of `bootstrap.sh`.
Use it only when the user explicitly agrees.
Core learning state imports from `learning_state_kv` (`patterns`/`adjustments`), not a `pattern_stats` SQL table.
`symbol_policy` rows are optional and may be empty depending on source history/release.

Seed publishing is maintainer-managed. For most users, start fresh and do not import.

User side options:

```bash
EVCLAW_ROOT="/path/to/EVClaw"
# Recommended default for new users: start fresh (no import command needed).

# Optional import path (historical bootstrap): dry-run first.
# Dry-run (default): auto-downloads official release seed + verifies SHA256.
python3 "$EVCLAW_ROOT/scripts/import_learning_seed.py"

# Apply after explicit user approval.
python3 "$EVCLAW_ROOT/scripts/import_learning_seed.py" --apply
```

Current official release seed note (v3, 2026-02-22):
- `learning_state_kv`: populated
- `symbol_policy`: populated

OpenClaw agent instruction:
- Ask for explicit user consent before importing learning seed.
- Run dry-run first.
- Run `--apply` only after user confirms.
- If using a custom seed file instead of official release, pass `--seed /path/to/seed.tgz`.
- Do not present import as mandatory; fresh start is the recommended default.

Pros:
- Better trading knowledge from real trade history.

Cons:
- Usually fewer trades than a fresh start profile.
- If many users import the same seed, behavior can become similar across users.

Optional maintenance: prune stale learning text/conclusions
```bash
EVCLAW_ROOT="/path/to/EVClaw"
# Dry-run (default), month window:
python3 "$EVCLAW_ROOT/scripts/prune_stale_learning.py"

# Week window:
python3 "$EVCLAW_ROOT/scripts/prune_stale_learning.py" --window week

# Apply (conclusions + notes_summary only):
python3 "$EVCLAW_ROOT/scripts/prune_stale_learning.py" --window month --apply

# Optional deeper cleanup (also delete stale reflections rows):
python3 "$EVCLAW_ROOT/scripts/prune_stale_learning.py" --window month --include-reflections --apply
```

Optional maintenance: cleanup Python/npm caches (dry-run default)
```bash
EVCLAW_ROOT="/path/to/EVClaw"
python3 "$EVCLAW_ROOT/scripts/cleanup_runtime_artifacts.py"
python3 "$EVCLAW_ROOT/scripts/cleanup_runtime_artifacts.py" --apply
python3 "$EVCLAW_ROOT/scripts/cleanup_runtime_artifacts.py" --npm-cache
python3 "$EVCLAW_ROOT/scripts/cleanup_runtime_artifacts.py" --apply --npm-cache --npm-cache-clean
```

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

Tracker data contract (critical):
- Primary real-time feed (drives trading/interrupts): `https://tracker.evplus.ai:8443/sse/tracker?key=$HYPERLIQUID_ADDRESS`
- EVClaw default also appends `&profile=evclaw-lite` on the same endpoint to reduce payload size.
- Fallback: set `EVCLAW_SSE_PROFILE=full` to disable lite profile instantly.
- HIP3 REST (context/enrichment endpoints): `https://tracker.evplus.ai/api/hip3/predator-state?key=$HYPERLIQUID_ADDRESS`
- HIP3 symbols REST: `https://tracker.evplus.ai/api/hip3-symbols?key=$HYPERLIQUID_ADDRESS`
- If you override `EVCLAW_TRACKER_HIP3_PREDATOR_URL` or `EVCLAW_TRACKER_HIP3_SYMBOLS_URL`, include `?key=<wallet>` in the URL.
- Do not use shortened links for HIP3 REST URLs. Use full endpoint URLs with `?key=...`.

Quick endpoint checks:
```bash
curl -ksS "https://tracker.evplus.ai/api/hip3/predator-state?key=$HYPERLIQUID_ADDRESS" | head -c 200
curl -ksS "https://tracker.evplus.ai/api/hip3-symbols?key=$HYPERLIQUID_ADDRESS" | head -c 200
curl -ksS "https://tracker.evplus.ai:8443/sse/tracker?key=$HYPERLIQUID_ADDRESS" --max-time 5 | head -c 200
```

HIP3 runtime flow (AI quick map):
1. `sse_consumer.py` connects to SSE and merges `hip3-data` into each symbol payload as `hip3_predator`.
2. `cycle_trigger.py` consumes HIP3 snapshots from SSE and runs candidate generation on that merged payload.
3. `compute_hip3_main()` runs OR-driver semantics: FLOW pass or OFM pass can drive; direction conflict blocks.
4. `context_builder_v2.py` uses SSE payload first and only attempts REST enrichment if `hip3_predator` is missing.
5. REST fetch failures are debug-logged and non-fatal; they can still reduce enrichment quality.

Failure implication:
- If both FLOW and OFM gates fail, `compute_hip3_main()` emits a `blocked_reason` (for example `missing_flow_direction`) and that symbol produces no HIP3 candidate in that snapshot.

Entry-gate safety controls (AI/operator quick map):
1. `config.global_pause.enabled=true` blocks all new entries immediately.
2. `config.entry_gate_bypass_guard` controls deterministic fallback risk:
   - `size_mult_cap` (default `0.5`)
   - `max_entries_per_window` + `window_minutes`
   - `hard_block_unreachable_after_minutes`
3. Trades/proposals carry `entry_gate_execution_type` (`llm`, `bypass`, `deterministic`) and optional `entry_gate_bypass_reason`.
4. Learning ignores bypass-tagged trades for signal/symbol adjustments and pattern updates.

HIP3 enablement (currently `xyz:SYMBOL` only):
- `EVCLAW_ENABLED_VENUES=hyperliquid,hip3`
- `EVCLAW_HIP3_TRADING_ENABLED=1`
- `EVCLAW_HIP3_DEXES=xyz`
- Symbols outside `xyz:*` are not supported for HIP3 trading yet.

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
- `/execute <long|short> <SYMBOL> <chase|limit> [size_usd] [ttl]` (asks missing size and requires explicit confirm)
- `/best3`
- `/hedge`
- `/stats` (live perps + builder/HIP3, DB fallback only when live fails)

Command split:
- `/execute <PLAN_ID> chase|limit` is helper-skill manual plan execution.
- `python3 cli.py execute --cycle-file ...` is the low-level cycle executor path.

Operator note:
- This section is manual/interactive guidance.
- Scheduled cron jobs must use `AGENTS.md` `AGI_SUPERVISOR_MODE` + `Tool.md` `CRON_SAFE_TOOLS` only.

## Operations and health

- Deterministic ops cron runs every 15 minutes for reconciliation/maintenance.
- OpenClaw hourly report cron posts a status summary to main chat.
- Hourly report cron also runs deterministic repo update check and asks user for explicit yes/no before any update action.
- Health/ops output is written to local runtime files and logs.
- Cycle/context/candidate artifacts are pruned every cycle by `cycle_trigger.py`.
- Retention is fixed in code to keep last `50` files per artifact class (safety floor: `20`).

## Troubleshooting

- If no trades appear, verify OpenClaw agent IDs and provider config.
- If SSE fails, verify tracker endpoint/key in `.env`.
- If SSE/node2 returns 401/403, approve builder fee for your wallet at `https://atsetup.evplus.ai/`.
Status guide:
- `401`: missing/invalid wallet `key` query parameter.
- `402`: wallet has not approved builder fee yet (SSE path).
- `403`: wrong host/path/routing policy; re-check exact endpoint and port.
- If HL auth fails, verify `HYPERLIQUID_ADDRESS` and `HYPERLIQUID_AGENT_PRIVATE_KEY`.
- If `HYPERLIQUID_API` appears in your `.env`, remove it and use `HYPERLIQUID_AGENT_PRIVATE_KEY`.
- If you see `sr_limit_equity_missing`, check that `evclaw-live-monitor` is running (it writes `monitor_snapshots` used for SR-limit equity caps).
- If processes are missing, run `./start.sh` again and inspect tmux sessions.
- If OpenClaw cron jobs are missing (`openclaw cron list --json` shows no EVClaw jobs), run `./scripts/install_openclaw_crons.sh` and re-check.

## Safety notice

This is real-money trading software. Start with small size, confirm live behavior, and monitor continuously before scaling.
