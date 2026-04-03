# EVClaw Tool Routing

Purpose: single source of truth for what command/tool to run, when to run it, and what output behavior to expect.

Scope:
- Manual/advisor user commands (`/trade`, `/best3`, `/execute`, `/hedge`, `/stats`)
- Deterministic ops tools (`hourly_ops.py`, repo update check, learning import/prune)
- Rust Hyperliquid perp bot runtime tools (`scripts/evclaw_rust_*.sh`)
- Runtime cache cleanup helper (`scripts/cleanup_runtime_artifacts.py`)

Rules:
- Use exact command forms below. Do not invent syntax.
- For scheduled jobs, use only `CRON_SAFE_TOOLS` block.
- For manual user turns, use manual tools only.

## Quick decision map
- User wants a fresh trade idea for specific symbol(s): use `/trade`.
- User wants top few opportunities quickly: use `/best3`.
- User wants to execute a plan or ad-hoc manual trade: use `/execute`.
- User wants to reduce portfolio directional risk: use `/hedge`.
- User wants current wallet/equity/positions status: use `/stats`.
- User wants Rust bot process/account/log status: use `scripts/evclaw_rust_status.sh`.
- User wants Rust bot restart/start/log tail: use `scripts/evclaw_rust_restart.sh`, `scripts/evclaw_rust_start.sh`, `scripts/evclaw_rust_logs.sh`.
- Scheduled health/reconcile/check loop: use `hourly_ops.py`.
- Scheduled update-check only (no auto-update): use `scripts/check_repo_update.py`.
- Optional learning bootstrap import: use `scripts/import_learning_seed.py`.
- Optional stale-learning cleanup: use `scripts/prune_stale_learning.py`.

## Manual tools

`/trade`
- When to use: user asks for advisor plan on one or more symbols.
- Run:
```bash
EVCLAW_ROOT="${EVCLAW_ROOT:-$HOME/.openclaw/skills/EVClaw}" \
python3 "$EVCLAW_ROOT/openclaw_skills/trade/scripts/generate_plans.py" ETH BTC
```
- What it does:
1. Loads latest candidates/context.
2. Runs manual trade gate.
3. Stores READY plan files for `/execute`.
- Code truth:
`openclaw_skills/trade/scripts/generate_plans.py:281`, `openclaw_skills/trade/scripts/generate_plans.py:283`, `openclaw_skills/trade/scripts/generate_plans.py:518`, `openclaw_skills/trade/scripts/generate_plans.py:692`, `openclaw_skills/trade/scripts/generate_plans.py:694`.

`/best3`
- When to use: user wants deterministic top picks quickly, no LLM gate.
- Run:
```bash
EVCLAW_ROOT="${EVCLAW_ROOT:-$HOME/.openclaw/skills/EVClaw}" \
python3 "$EVCLAW_ROOT/openclaw_skills/best3/scripts/generate_best3.py" --n 3
```
- What it does:
1. Reads latest candidate file.
2. Ranks by blended conviction.
3. Writes READY manual plans for `/execute`.
- Code truth:
`openclaw_skills/best3/scripts/generate_best3.py:2`, `openclaw_skills/best3/scripts/generate_best3.py:234`, `openclaw_skills/best3/scripts/generate_best3.py:236`, `openclaw_skills/best3/scripts/generate_best3.py:253`, `openclaw_skills/best3/scripts/generate_best3.py:9`.

`/execute`
- When to use: user wants to execute existing plan ID or ad-hoc manual order.
- Supported forms:
1. `/execute <PLAN_ID> chase|limit [ttl]`
2. `/execute long ETH chase [size_usd] [ttl] [confirm]`
3. `/execute ETH long limit [size_usd] [ttl] [confirm]`
- Run bridge:
```bash
EVCLAW_ROOT="${EVCLAW_ROOT:-$HOME/.openclaw/skills/EVClaw}" \
python3 "$EVCLAW_ROOT/openclaw_skills/execute/scripts/execute_bridge.py" <ARGS_FROM_/execute>
```
- Behavior:
1. Plan mode executes immediately.
2. Ad-hoc mode asks for size if missing.
3. Ad-hoc mode requires explicit confirm before execution.
4. Ad-hoc mode still creates a plan first, then executes.
- Code truth:
`openclaw_skills/execute/scripts/execute_bridge.py:4`, `openclaw_skills/execute/scripts/execute_bridge.py:8`, `openclaw_skills/execute/scripts/execute_bridge.py:229`, `openclaw_skills/execute/scripts/execute_bridge.py:259`, `openclaw_skills/execute/scripts/execute_bridge.py:275`, `openclaw_skills/execute/scripts/execute_bridge.py:293`, `openclaw_skills/execute/scripts/execute_bridge.py:315`.

`/hedge`
- When to use: user asks to reduce directional exposure.
- Run:
```bash
EVCLAW_ROOT="${EVCLAW_ROOT:-$HOME/.openclaw/skills/EVClaw}" \
python3 "$EVCLAW_ROOT/openclaw_skills/hedge/scripts/generate_hedge.py" --symbol BTC --pct 100
```
- Behavior:
1. Deterministic hedge only (no LLM).
2. Default neutralize target is 100% of current net notional.
3. Stores manual plan for `/execute`.
- Code truth:
`openclaw_skills/hedge/scripts/generate_hedge.py:2`, `openclaw_skills/hedge/scripts/generate_hedge.py:132`, `openclaw_skills/hedge/scripts/generate_hedge.py:135`, `openclaw_skills/hedge/scripts/generate_hedge.py:163`, `openclaw_skills/hedge/scripts/generate_hedge.py:229`.

`/stats`
- When to use: user asks wallet/equity/positions/status.
- Run:
```bash
EVCLAW_ROOT="${EVCLAW_ROOT:-$HOME/.openclaw/skills/EVClaw}" \
python3 "$EVCLAW_ROOT/openclaw_skills/stats/scripts/generate_stats.py"
```
- Behavior:
1. Live-first fetch from Hyperliquid API.
2. DB fallback only when live is missing.
3. Includes perps and builder/HIP3 positions.
- Code truth:
`openclaw_skills/stats/scripts/generate_stats.py:2`, `openclaw_skills/stats/scripts/generate_stats.py:9`, `openclaw_skills/stats/scripts/generate_stats.py:123`, `openclaw_skills/stats/scripts/generate_stats.py:185`, `openclaw_skills/stats/scripts/generate_stats.py:418`.

## Deterministic ops tools

`scripts/evclaw_rust_status.sh`
- When to use: inspect the embedded Rust Hyperliquid perp bot runtime.
- Run:
```bash
EVCLAW_ROOT="${EVCLAW_ROOT:-$HOME/.openclaw/skills/EVClaw}" \
"$EVCLAW_ROOT/scripts/evclaw_rust_status.sh"
```
- Behavior:
1. Reports tmux session state.
2. Reports configured wallet and latest log.
3. Fetches live HL equity, positions, and open order counts using the Rust bot env.

`scripts/evclaw_rust_start.sh`
- When to use: start the embedded Rust bot if it is not already running.
- Run:
```bash
EVCLAW_ROOT="${EVCLAW_ROOT:-$HOME/.openclaw/skills/EVClaw}" \
"$EVCLAW_ROOT/scripts/evclaw_rust_start.sh"
```

`scripts/evclaw_rust_restart.sh`
- When to use: restart the embedded Rust bot after approved changes.
- Run:
```bash
EVCLAW_ROOT="${EVCLAW_ROOT:-$HOME/.openclaw/skills/EVClaw}" \
"$EVCLAW_ROOT/scripts/evclaw_rust_restart.sh"
```

`scripts/evclaw_rust_logs.sh`
- When to use: tail the latest Rust bot log.
- Run:
```bash
EVCLAW_ROOT="${EVCLAW_ROOT:-$HOME/.openclaw/skills/EVClaw}" \
"$EVCLAW_ROOT/scripts/evclaw_rust_logs.sh"
```

`hourly_ops.py`
- When to use: deterministic maintenance, audit, reconcile, safety checks.
- Run:
```bash
python3 hourly_ops.py --db ./ai_trader.db --json-out ./state/hourly_ops_report.json --summary-out ./state/hourly_ops_summary.txt
```
- Code truth:
`hourly_ops.py:2`, `hourly_ops.py:1631`, `hourly_ops.py:1632`, `hourly_ops.py:1634`, `hourly_ops.py:1644`.

`scripts/check_repo_update.py`
- When to use: check upstream updates and produce changelog summary; never auto-update.
- Run:
```bash
python3 scripts/check_repo_update.py --json-out ./state/repo_update_report.json --summary-out ./state/repo_update_summary.txt --changelog-md-out ./state/repo_update_changelog.md
```
- Behavior:
1. Compares local HEAD with `origin/main`.
2. Throttles notify via state file.
3. Emits `notify_user=yes|no`.
- Code truth:
`scripts/check_repo_update.py:2`, `scripts/check_repo_update.py:139`, `scripts/check_repo_update.py:142`, `scripts/check_repo_update.py:219`, `scripts/check_repo_update.py:223`.

`scripts/import_learning_seed.py`
- When to use: optional historical learning bootstrap import.
- Run dry-run first:
```bash
python3 scripts/import_learning_seed.py
```
- Apply only with explicit user approval:
```bash
python3 scripts/import_learning_seed.py --apply
```
- Code truth:
`scripts/import_learning_seed.py:2`, `scripts/import_learning_seed.py:179`, `scripts/import_learning_seed.py:184`, `scripts/import_learning_seed.py:212`, `scripts/import_learning_seed.py:356`.

`scripts/prune_stale_learning.py`
- When to use: optional cleanup of stale conclusions/notes.
- Safe default (dry-run):
```bash
python3 scripts/prune_stale_learning.py --window month
```
- Apply:
```bash
python3 scripts/prune_stale_learning.py --window month --apply
```
- Optional deeper cleanup (also reflection rows):
```bash
python3 scripts/prune_stale_learning.py --window month --include-reflections --apply
```
- Code truth:
`scripts/prune_stale_learning.py:2`, `scripts/prune_stale_learning.py:52`, `scripts/prune_stale_learning.py:60`, `scripts/prune_stale_learning.py:64`, `scripts/prune_stale_learning.py:163`, `scripts/prune_stale_learning.py:172`, `scripts/prune_stale_learning.py:186`.

`scripts/cleanup_runtime_artifacts.py`
- When to use: optional cache cleanup on user VPS (`__pycache__`, `.pyc/.pyo`, optional npm cache).
- Safety: defaults to dry-run. Use `--apply` to execute.
- Commands:
```bash
python3 scripts/cleanup_runtime_artifacts.py
python3 scripts/cleanup_runtime_artifacts.py --apply
python3 scripts/cleanup_runtime_artifacts.py --npm-cache
python3 scripts/cleanup_runtime_artifacts.py --apply --npm-cache --npm-cache-clean
```
- Note: cycle/context/candidate runtime artifacts are already auto-pruned by `cycle_trigger.py` (fixed keep `50`, floor `20`).

## CRON_SAFE_TOOLS_START
- Allowed commands in scheduled jobs:
1. `python3 hourly_ops.py ...`
2. `python3 scripts/check_repo_update.py ...`
3. Read only: `state/hourly_ops_summary.txt`, `state/hourly_ops_report.json`, `state/repo_update_summary.txt`, `state/repo_update_changelog.md`
- Required behavior:
1. If update check says `notify_user=yes`, ask user yes/no.
2. Never auto-run `git pull`, `bootstrap.sh`, `start.sh`, `restart.sh`.
3. Never run manual interactive commands (`/trade`, `/best3`, `/execute`, `/hedge`) in cron turns.
- Code truth:
`scripts/install_openclaw_crons.sh:80`, `scripts/install_openclaw_crons.sh:84`, `scripts/install_openclaw_crons.sh:86`, `scripts/install_openclaw_crons.sh:87`, `scripts/install_openclaw_crons.sh:205`.
## CRON_SAFE_TOOLS_END
