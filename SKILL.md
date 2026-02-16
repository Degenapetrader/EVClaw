# EVClaw Trading Skill

## Metadata

| Field | Value |
|-------|-------|
| **Name** | EVClaw |
| **Version** | 0.6.1 |
| **Author** | Claude |
| **Category** | Trading |
| **Exchanges** | Lighter (crypto perps), Hyperliquid (HIP3 stocks) |
| **Mode** | Live Agent (context → main agent) |

## Description

Consumes real-time signals from the tracker SSE stream and enables live agent trading for Lighter (crypto perpetuals) and Hyperliquid (HIP3 stocks).

- **Live agent mode**: main agent (gpt-5.2) is the only authority; OpenClaw agent executes proposals in AGI-only mode (no user approval; `review_only` is deprecated/unsupported)
- **Main agent (gpt-5.2)** selects candidates directly from JSON context
- **Per-venue execution**: each approved trade executes on any enabled venue where the symbol is tradable (HL and/or Lighter)
- **Agent-driven incident response** (human-in-the-loop triage and remediation)

**Architecture:**
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

**Key Features:**
- Live agent trading (context → main agent → execute)
- Snapshot-driven cycle handling + context builder
- Per-venue execution (no requirement that a symbol exists on both exchanges)
- Dynamic sizing + exposure limits via DynamicRiskManager
- Decay-based exits as primary exit logic
- Optional SL/TP emergency backstop (config flag)
- Token bucket rate limiting (40 req/60s)
- Atomic position persistence for crash recovery
- Agent-driven incident response (triage + remediation workflow)

---

## Commands

### `/live-agent run/execute`

Main agent workflow (context → validate → record proposals; OpenClaw agent executes in AGI-only mode).

**Typical flow (from system event):**
```bash
# Record proposals from the most recent pending cycle
python3 ${EVCLAW_ROOT}/live_agent.py run \
  --from-pending
```

Notes:
- AGI-only: OpenClaw executes proposals directly (no user confirmation).

**Execute from candidates (manual review first):**
```bash
# Ingest candidates and record PROPOSED proposals (no auto-execute)
python3 ${EVCLAW_ROOT}/live_agent.py execute \
  --seq 12345 \
  --cycle-file ${EVCLAW_RUNTIME_DIR}/evclaw_cycle_12345.json \
  --candidates-file ${EVCLAW_RUNTIME_DIR}/evclaw_candidates_12345.json
```

Notes:
- `--from-pending` pulls the most recent queued cycle when the main agent was busy.
- Trades are blocked only if a symbol is not available on **any enabled venue**.

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

**Invocation:**
```bash
python3 ${EVCLAW_ROOT}/cli.py execute --cycle-file ${EVCLAW_RUNTIME_DIR}/evclaw_cycle_123.json --symbol ETH --direction LONG --size-usd 500
```

---

### `/trade <symbol> <direction> <size_usd>`

Execute a manual trade decision.

**Parameters:**
| Param | Type | Default | Description |
|-------|------|---------|-------------|
| symbol | string | required | Trading symbol (e.g., ETH, xyz:NVDA) |
| direction | string | required | LONG or SHORT |
| size_usd | float | required | Position notional in USD (> 0) |

**Invocation:**
```bash
python3 ${EVCLAW_ROOT}/cli.py trade <symbol> <direction> <size_usd>
```

---

### `/signals [symbol] [--min-z <float>] [--top <int>]`

View current actionable signals from the tracker.

**Parameters:**
| Param | Type | Default | Description |
|-------|------|---------|-------------|
| symbol | string | all | Filter by symbol |
| --min-z | float | 2.0 | Minimum z-score threshold |
| --top | int | 20 | Number of signals to show |

**Invocation:**
```bash
python3 ${EVCLAW_ROOT}/cli.py signals [symbol] [--min-z <float>] [--top <int>]
```

---

### `/positions [--all] [--close <symbol>] [--export]`

View and manage open positions.

**Invocation:**
```bash
python3 ${EVCLAW_ROOT}/cli.py positions [--all] [--close <symbol>] [--export]
```

---

## Python Scripts

### Entry Points

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
# Minimal unit tests
python3 ${EVCLAW_ROOT}/tests/test_trading_brain.py
python3 ${EVCLAW_ROOT}/tests/test_executor.py
```

---

## Configuration

Configuration is loaded from `skill.yaml`. Key settings:

```yaml
config:
  sse_host: tracker.evplus.ai
  sse_port: 8443
  sse_endpoint: /sse/tracker
  wallet_address: "0x0000000000000000000000000000000000000001"
  db_path: ${EVCLAW_ROOT}/ai_trader.db
  mode: execution_by_call

  context_builder:
    max_opportunities: 10
    min_score: 35.0

  brain:
    base_confidence_threshold: 0.55
    high_conviction_threshold: 0.75
    degen_mode: true
    degen_multiplier: 1.5

  risk:
    starting_equity: 10000.0
    min_risk_pct: 0.5
    max_risk_pct: 2.5
    max_concurrent_positions: 5
    no_hard_stops: true
    max_hold_hours: 24.0

  executor:
    base_position_size_usd: 500.0
    max_position_per_symbol_usd: 1000.0
    enable_sltp_backstop: false
    sl_atr_multiplier: 1.5
    tp_atr_multiplier: 2.0
    memory_dir: ${EVCLAW_ROOT}/memory

exchanges:
  router:
    overrides: {}
```

---

## Memory Files

Persistent state is stored in `memory/`:

| File | Purpose | Format |
|------|---------|--------|
| `signal_weights.yaml` | Per-signal confidence multipliers | YAML |
| `trade_journal.yaml` | Trade history (rolling 1000) | YAML |
| `circuit_breaker.yaml` | Daily loss, loss streak state | YAML |
| `positions.yaml` | Active positions | YAML |
| `symbol_blacklist.yaml` | Blacklisted symbols | YAML |
| `context_feature_stats.json` | Win rates by context condition | JSON |
| `mistakes.json` | Classified trading mistakes | JSON |
| `patterns.json` | Signal pattern performance | JSON |
| `adjustments.json` | Signal/symbol adjustments | JSON |

---

## Requirements

### Python Packages

```txt
aiohttp>=3.9.0
pyyaml>=6.0
hyperliquid
lighter
```

### System Dependencies

- Python 3.10+
- Lighter env vars: `LIGHTER_BASE_URL`, `LIGHTER_ACCOUNT_INDEX`, `LIGHTER_API_KEY_PRIVATE_KEY`, `LIGHTER_API_KEY_INDEX`
- Hyperliquid env vars: `HYPERLIQUID_ADDRESS` (main wallet address), `HYPERLIQUID_AGENT_PRIVATE_KEY` (delegated signer key; not main wallet private key), optional `HYPERLIQUID_PRIVATE_NODE`, optional `EVCLAW_INCLUDE_WALLET_HIP3_FILLS`
- Venue controls: `EVCLAW_ENABLED_VENUES`, `HYPERLIQUID_PRIVATE_NODE`
- Massive env var (HIP3 predator signals): `MASSIVE_API_KEY`
- Tracker APIs are hosted externally via evplus tracker:
  - `EVCLAW_TRACKER_BASE_URL`
  - `EVCLAW_TRACKER_HIP3_PREDATOR_URL`
  - `EVCLAW_TRACKER_HIP3_SYMBOLS_URL`
  - `EVCLAW_TRACKER_SYMBOL_URL_TEMPLATE`
- EVClaw OSS is network-only: all tracker data is fetched from `tracker.evplus.ai`.
- SSE tracker running on tracker.evplus.ai:8443

---

## Symbol Routing

Symbols are automatically routed to the appropriate exchange:

| Pattern | Exchange | Example |
|---------|----------|---------|
| `xyz:<TICKER>` | HIP3 wallet (HIP3 stocks) | xyz:NVDA, xyz:TSLA |
| `<SYMBOL>` | Lighter (crypto perps) | ETH, BTC, kPEPE |

Overrides can be configured in `skill.yaml` under `exchanges.router.overrides` (e.g., force BTC to Hyperliquid).

**Live agent (HIP3):** HIP3 symbols (`xyz:`) are **HIP3 wallet-only**. If `executor.hl_wallet_enabled: true`, live mode can execute HIP3 without requiring Lighter availability.

---

## Signal Types

The skill processes 8 signal types from the tracker and workers:

| Signal | Description | Z-Score Field | Source |
|--------|-------------|---------------|--------|
| CVD | Cumulative Volume Delta | z_smart, z_dumb | Tracker |
| FADE | Smart money fade signal | z_score | Tracker |
| LIQ_PNL | Liquidation PnL flow | z_score | Tracker |
| WHALE | Large trader activity | strength (0-1) | Tracker |
| DEAD_CAPITAL | Trapped capital detection | strength (0-1) | Tracker |
| OFM | Order Flow Momentum | z_score | Tracker |
| HIP3_MAIN | FLOW divergence gate + OFM alignment (xyz: only) | z_score_effective | hip3_predator_worker |

### HIP3 Predator Signals

For HIP3 symbols (`xyz:`), the worker provides FLOW + OFM components which are combined into a single **HIP3_MAIN** signal:

- **FLOW gate**: HL vs Massive spread divergence (z_signed vs dynamic_threshold).
- **OFM confirm**: Predator OFM must align with FLOW direction.

HIP3_MAIN is PRIMARY for HIP3 symbols and can trigger trades independently.
HIP3 signals stream over SSE as event `hip3-data` and are merged into context in real time.

**HIP3 data source:**
- EVClaw does not run tracker-side HIP3 workers locally.
- EVClaw reads HIP3 data from tracker endpoints (for example `https://tracker.evplus.ai/api/hip3/predator-state` and SSE `hip3-data`).

---

## Decision Logic

### Conviction Scoring
- Each signal contributes to a conviction score (0.0 - 1.0)
- Signals are weighted (CVD/FADE/LIQ_PNL/WHALE/DEAD/OFM)
- Recommendations are produced above the brain confidence threshold; execution requires `cli.py execute`

### Veto Conditions
| Condition | Effect |
|-----------|--------|
| WHALE opposite direction | VETO trade |
| CVD opposite with strong z-score | VETO trade |

### Risk & Exits
- Dynamic risk sizing based on conviction and exposure
- Decay-based exits are primary (trigger signal flip)
- SL/TP optional emergency backstop (`executor.enable_sltp_backstop`)

---

## Context Learning (v0.6.0)

The learning engine now tracks win rates by context conditions and adjusts sizing accordingly.

### Context Features Tracked

| Feature | Conditions | Meaning |
|---------|------------|---------|
| `trend_alignment` | aligned / counter / neutral | Does trend_score agree with trade direction? |
| `vol_regime` | high / mid / low | ATR-based volatility bucket |
| `funding_alignment` | aligned / counter / neutral | Does funding favor our direction? |
| `smart_money` | aligned / counter / neutral | Does smart money divergence favor us? |
| `signal_strength` | strong / moderate / weak | Max z-score bucket |

### Context Adjustment

When enough data exists (≥10 trades per condition), the learning engine provides a multiplier:
- Win rate > 45% → boost sizing (up to 1.5x)
- Win rate < 45% → reduce sizing (down to 0.5x)

This multiplier is applied in `risk_manager.calculate_risk_budget()` when `context_snapshot` and `learning_engine` are provided.

### Stats File

Context stats are persisted to `memory/context_feature_stats.json`.

---

## Order Type Selection (v0.6.0)

AGI context now includes order type guidance:

| Order Type | When to Use |
|------------|-------------|
| **CHASE LIMIT** | High conviction (≥0.6), trend-aligned, time-sensitive |
| **LIMIT ORDER** | Lower conviction (<0.6), counter-trend, patient fill preferred |

The AGI context includes `order_type_hint` and `order_type_reason` fields. The AGI trader can override based on judgment.

---

## Error Handling

| Error | Handling |
|-------|----------|
| SSE disconnect | Exponential backoff reconnect (2s-30s) |
| Rate limit (429) | Wait for token bucket refill |
| Order rejected | Log and abort (don't retry) |
| SL/TP placement fail | Log warning, position remains open |
| Position fetch fail | Use cached state |

---

## Logging

Logs are written to stdout with format:
```
2026-01-25 12:00:00 [executor] INFO: Executing LONG for ETH
```

Log levels:
- **INFO**: Trade execution, position changes
- **WARNING**: Rate limits, SL/TP failures
- **ERROR**: Order failures, exchange errors

---

## Version History

| Version | Date | Changes |
|---------|------|---------|
| 0.6.2 | 2026-02-03 | HIP3 signals now flow into tracking/learning: candidates include `signals_snapshot`/`context_snapshot`, learning engine tracks `hip3_main`, live monitor tracks HIP3 wallet equity, HIP3 signals stream over SSE (`hip3-data`) |
| 0.6.1 | 2026-02-02 | HIP3 routing cleanup, Predator-exact HIP3 signals worker (Massive WS+REST failover + HL WS, FLOW/OFM with TFI+CVD+VPIN) |
| 0.6.0 | 2026-02-02 | Context learning (win rate by context conditions), order type guidance |
| 0.5.0 | 2026-01-27 | Single-brain pipeline with dynamic risk + decay exits |
| 0.4.0 | 2026-01-25 | Learning engine + signal weight adaptation |
| 0.3.0 | 2026-01-25 | Executor, rate limiting, position tracking |
| 0.2.0 | 2026-01-25 | Decision engine (legacy) |
| 0.1.0 | 2026-01-25 | SSE consumer, signal parser |
