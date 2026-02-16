---
name: EVClaw
description: Trading skill for executing live agent trades on Lighter (crypto perps) and Hyperliquid (HIP3 stocks). Use when user asks about trading, positions, signals, executing trades, live agent mode, cycle files, or managing EVClaw operations. Supports (1) Running live agent cycles via /live-agent, (2) Executing manual trades via /trade and /execute, (3) Viewing signals/positions, (4) Managing trading configuration, (5) Understanding signal types and decision logic, (6) Agent-driven incident response and triage.
---

# EVClaw Trading Skill

**Mode**: Live Agent (context → main agent → execute)
**Exchanges**: Lighter (crypto perps), Hyperliquid (HIP3 stocks)

## Architecture

```
Cycle Trigger → Cycle File + Context → System Event → Main Agent (gpt-5.2)
   ↓               ↓                         ↓                 ↓
 tracker:8443   <runtime_dir>/evclaw_*  live_agent.py    JSON context selection
                                                     ↓
                                     <runtime_dir>/evclaw_candidates_*.json
                                                      ↓
                                     Main Agent validate + record proposals
                                                      ↓
                                     OpenClaw agent executes proposals
                                                      ↓
                                              Executor → HL and/or Lighter
```

**See [references/architecture.md](references/architecture.md) for detailed system architecture.**

## Key Features

- Live agent trading (context → main agent → execute)
- Snapshot-driven cycle handling + context builder
- Per-venue execution (no requirement that a symbol exists on both exchanges)
- Dynamic sizing + exposure limits via DynamicRiskManager
- Decay-based exits as primary exit logic
- Optional SL/TP emergency backstop (config flag)
- Token bucket rate limiting (40 req/60s)
- Atomic position persistence for crash recovery
- Agent-driven incident response (triage + remediation workflow)

## Commands

### `/live-agent run/execute`

Main agent workflow (context → validate → record proposals; OpenClaw agent executes in AGI-only mode).

```bash
# Record proposals from the most recent pending cycle
python3 ${EVCLAW_ROOT}/live_agent.py run --from-pending
```

**Execute from candidates (manual review first):**
```bash
python3 ${EVCLAW_ROOT}/live_agent.py execute \
  --seq 12345 \
  --cycle-file ${EVCLAW_RUNTIME_DIR}/evclaw_cycle_12345.json \
  --candidates-file ${EVCLAW_RUNTIME_DIR}/evclaw_candidates_12345.json
```

### `/execute --cycle-file <path> --symbol <symbol> --direction LONG|SHORT --size-usd <float>`

Execute a decision from a cycle file (execution-by-call).

**Parameters:**
| Param | Type | Default | Description |
|-------|------|---------|-------------|
| --cycle-file | string | required | Path to cycle JSON from `cycle_trigger.py` |
| --symbol | string | required | Trading symbol (e.g., ETH, xyz:NVDA) |
| --direction | string | required | LONG or SHORT |
| --size-usd | float | required | Position size in USD |
| --dry-run | flag | false | Dry run (no real orders) |

```bash
python3 ${EVCLAW_ROOT}/cli.py execute \
  --cycle-file ${EVCLAW_RUNTIME_DIR}/evclaw_cycle_123.json \
  --symbol ETH --direction LONG --size-usd 500
```

### `/trade <symbol> <direction> <size_usd>`

Execute a manual trade decision.

```bash
python3 ${EVCLAW_ROOT}/cli.py trade <symbol> <direction> <size_usd>
```

### `/signals [symbol] [--min-z <float>] [--top <int>]`

View current actionable signals from the tracker.

```bash
python3 ${EVCLAW_ROOT}/cli.py signals [symbol] [--min-z <float>] [--top <int>]
```

### `/positions [--all] [--close <symbol>] [--export]`

View and manage open positions.

```bash
python3 ${EVCLAW_ROOT}/cli.py positions [--all] [--close <symbol>] [--export]
```

## Python Scripts

| Script | Purpose | Invocation |
|--------|---------|------------|
| `main.py` | Analysis/report loop (no execution) | `python3 main.py` |
| `sse_consumer.py` | SSE client for tracker stream | `python3 sse_consumer.py` |
| `context_builder_v2.py` | Build full cycle context + opportunities | Imported by main |
| `trading_brain.py` | Conviction-based decisioning | Imported by main |
| `risk_manager.py` | Dynamic sizing + decay exits | Imported by main |
| `executor.py` | Order execution | Used by cli.py execute |
| `learning_engine.py` | Signal weight learning | `python3 learning_engine.py` |
| `cli.py` | Command-line interface | `python3 cli.py <command>` |

### Analysis Mode (No Execution)

```bash
# Analyze a saved cycle file
python3 ${EVCLAW_ROOT}/main.py --cycle-file ${EVCLAW_RUNTIME_DIR}/evclaw_cycle_123.json

# With custom config
python3 ${EVCLAW_ROOT}/main.py --config custom.yaml --cycle-file ${EVCLAW_RUNTIME_DIR}/evclaw_cycle_123.json

# Live SSE analysis (reports only)
python3 ${EVCLAW_ROOT}/main.py
```

### Testing

```bash
python3 ${EVCLAW_ROOT}/tests/test_trading_brain.py
python3 ${EVCLAW_ROOT}/tests/test_executor.py
```

## Configuration

Configuration is loaded from `skill.yaml`. Key settings include:

- `config.mode_controller.mode`: Trading mode (`conservative`, `balanced`, `aggressive`)
- `config.brain`: Conviction thresholds and weights
- `config.risk`: Position sizing and exposure limits
- `config.executor`: SL/TP, chase settings, SR-limit config

**See [references/configuration.md](references/configuration.md) for complete config reference.**

## Symbol Routing

| Pattern | Exchange | Example |
|---------|----------|---------|
| `xyz:<TICKER>` | HIP3 wallet (HIP3 stocks) | xyz:NVDA, xyz:TSLA |
| `<SYMBOL>` | Lighter (crypto perps) | ETH, BTC, kPEPE |

Overrides can be configured in `skill.yaml` under `exchanges.router.overrides`.

**HIP3 Live Agent:** HIP3 symbols (`xyz:`) are **HIP3 wallet-only**. If `executor.hl_wallet_enabled: true`, live mode can execute HIP3 without requiring Lighter availability.

## Decision Logic

### Conviction Scoring
- Each signal contributes to a conviction score (0.0 - 1.0)
- Signals are weighted (CVD/FADE/LIQ_PNL/WHALE/DEAD/OFM/HIP3_MAIN)
- Recommendations are produced above the brain confidence threshold

### Veto Conditions
| Condition | Effect |
|-----------|--------|
| WHALE opposite direction | VETO trade |
| CVD opposite with strong z-score | VETO trade |

### Risk & Exits
- Dynamic risk sizing based on conviction and exposure
- Decay-based exits are primary (trigger signal flip)
- SL/TP optional emergency backstop (`executor.enable_sltp_backstop`)

**See [references/signals.md](references/signals.md) for signal types and scoring.**

## Context Learning

The learning engine tracks win rates by context conditions and adjusts sizing accordingly:

- When enough data exists (≥10 trades per condition), the learning engine provides a multiplier
- Win rate > 45% → boost sizing (up to 1.5x)
- Win rate < 45% → reduce sizing (down to 0.5x)

Context features tracked: `trend_alignment`, `vol_regime`, `funding_alignment`, `smart_money`, `signal_strength`.

**See [references/signals.md](references/signals.md) for context features details.**

## Order Type Selection

| Order Type | When to Use |
|------------|-------------|
| **CHASE LIMIT** | High conviction (≥0.6), trend-aligned, time-sensitive |
| **LIMIT ORDER** | Lower conviction (<0.6), counter-trend, patient fill preferred |

## Error Handling

| Error | Handling |
|-------|----------|
| SSE disconnect | Exponential backoff reconnect (2s-30s) |
| Rate limit (429) | Wait for token bucket refill |
| Order rejected | Log and abort (don't retry) |
| SL/TP placement fail | Log warning, position remains open |
| Position fetch fail | Use cached state |

## Logging

Logs are written to stdout with format:
```
2026-01-25 12:00:00 [executor] INFO: Executing LONG for ETH
```

Log levels:
- **INFO**: Trade execution, position changes
- **WARNING**: Rate limits, SL/TP failures
- **ERROR**: Order failures, exchange errors

## Memory Files

Persistent state is stored in `memory/`:

| File | Purpose |
|------|---------|
| `signal_weights.yaml` | Per-signal confidence multipliers |
| `trade_journal.yaml` | Trade history (rolling 1000) |
| `circuit_breaker.yaml` | Daily loss, loss streak state |
| `positions.yaml` | Active positions |
| `symbol_blacklist.yaml` | Blacklisted symbols |
| `context_feature_stats.json` | Win rates by context condition |
| `mistakes.json` | Classified trading mistakes |

**See [references/memory-files.md](references/memory-files.md) for file formats.**

## Requirements

### Python Packages
- aiohttp>=3.9.0
- pyyaml>=6.0
- requests>=2.31.0
- python-dotenv>=1.0.1
- hyperliquid-python-sdk>=0.21.0
- lighter-sdk (optional, installed from `requirements-lighter.txt` when Lighter venue is enabled)

### System Dependencies
- Python 3.10+
- Lighter env vars: `LIGHTER_BASE_URL`, `LIGHTER_ACCOUNT_INDEX`, `LIGHTER_API_KEY_PRIVATE_KEY`, `LIGHTER_API_KEY_INDEX`
- Hyperliquid env vars: `HYPERLIQUID_ADDRESS` (main wallet address), `HYPERLIQUID_AGENT_PRIVATE_KEY` (delegated signer key; not main wallet private key)
- Optional Hyperliquid: `HYPERLIQUID_PRIVATE_NODE`, `EVCLAW_INCLUDE_WALLET_HIP3_FILLS`
- Venue controls: `EVCLAW_ENABLED_VENUES`
- Massive env var (HIP3 predator signals): `MASSIVE_API_KEY`
- Tracker APIs (hosted externally via evplus tracker):
  - `EVCLAW_TRACKER_BASE_URL`
  - `EVCLAW_TRACKER_HIP3_PREDATOR_URL`
  - `EVCLAW_TRACKER_HIP3_SYMBOLS_URL`
  - `EVCLAW_TRACKER_SYMBOL_URL_TEMPLATE`
- EVClaw OSS is network-only: all tracker data is fetched from `tracker.evplus.ai`
- SSE tracker running on tracker.evplus.ai:8443

## Sub-Skills

EVClaw includes sub-skills in `openclaw_skills/`:

- **trade**: Manual trade planner (`/trade <SYMBOL>`)
- **execute**: Execute stored trade plans (`/execute <PLAN_ID>`)
- **hedge**: Portfolio hedging operations
- **stats**: Trading statistics and reports
- **best3**: Top 3 opportunities display
