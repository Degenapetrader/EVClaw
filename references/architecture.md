# Architecture Reference

## System Architecture

EVClaw now contains two separate runtime surfaces:

1. The Python/OpenClaw live-agent system for tracker-driven trading and ops.
2. The embedded Rust Hyperliquid perp bot under `evclaw_rust/`, which is operated from this repo but is not part of the Python live-agent decision chain.

```
┌─────────────────────────────────────────────────────────────────────┐
│                         EVClaw System                                │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  ┌──────────────┐     ┌──────────────┐     ┌──────────────┐         │
│  │   Tracker    │────▶│    SSE       │────▶│    Main      │         │
│  │  (External)  │     │  Consumer    │     │   Agent      │         │
│  └──────────────┘     └──────────────┘     └──────────────┘         │
│                              │                    │                  │
│                              ▼                    ▼                  │
│                       ┌──────────────┐     ┌──────────────┐         │
│                       │   Context    │     │   Trading    │         │
│                       │   Builder    │     │    Brain     │         │
│                       └──────────────┘     └──────────────┘         │
│                              │                    │                  │
│                              ▼                    ▼                  │
│                       ┌──────────────┐     ┌──────────────┐         │
│                       │    Risk      │     │   Executor   │         │
│                       │   Manager    │     │              │         │
│                       └──────────────┘     └──────────────┘         │
│                                                   │                  │
│                                                   ▼                  │
│                       ┌──────────────────────────────────────┐      │
│                       │         Exchange Router              │      │
│                       ├─────────────────┬────────────────────┤      │
│                       │     Lighter     │    Hyperliquid     │      │
│                       │  (crypto perps) │   (HIP3 stocks)    │      │
│                       └─────────────────┴────────────────────┘      │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘
```

## Embedded Rust Runtime

```
┌─────────────────────────────────────────────────────────────────────┐
│                    Embedded Rust Perp Bot                            │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  Wallet labels + HL /info + Binance/HL ATR → Combined signal        │
│                                     ↓                               │
│                          `evclaw_rust/src/runtime.rs`               │
│                                     ↓                               │
│                       Chase-limit executor + SL/TP repair           │
│                                     ↓                               │
│                         Hyperliquid perp account                    │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

Canonical operator surface for the Rust bot:

- `scripts/evclaw_rust_status.sh`
- `scripts/evclaw_rust_start.sh`
- `scripts/evclaw_rust_restart.sh`
- `scripts/evclaw_rust_logs.sh`

## Live Agent Flow

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

## Key Components

### Entry Points

| Component | File | Purpose |
|-----------|------|---------|
| Main Agent | `live_agent.py` | Live trading loop with main agent |
| SSE Consumer | `sse_consumer.py` | Real-time signal stream client |
| CLI | `cli.py` | Manual command interface |
| Cycle Trigger | `cycle_trigger.py` | Periodic cycle generation |

### Core Modules

| Module | File | Purpose |
|--------|------|---------|
| Context Builder | `context_builder_v2.py` | Build cycle context from signals |
| Trading Brain | `trading_brain.py` | Conviction scoring and decisions |
| Risk Manager | `risk_manager.py` | Position sizing, exposure limits |
| Executor | `executor.py` | Order execution, SL/TP management |
| Learning Engine | `learning_engine.py` | Signal weight adaptation |

### Workers

| Worker | File | Purpose |
|--------|------|---------|
| Decay Worker | `decay_worker.py` | Signal decay tracking |
| Position Review | `position_review_worker.py` | Position health monitoring |
| Exit Outcome | `exit_outcome_worker.py` | Exit quality analysis |
| Fill Reconciler | `fill_reconciler.py` | Order fill reconciliation |
| HIP3 Main | `hip3_main.py` | HIP3 signal processing |

### Embedded Rust Bot

| Component | File/Path | Purpose |
|-----------|-----------|---------|
| Runtime | `evclaw_rust/src/runtime.rs` | Main cycle loop, exits, entries, SL/TP reconcile |
| HL client | `evclaw_rust/src/hyperliquid.rs` | Hyperliquid data/execution client |
| Journal | `evclaw_rust/src/journal.rs` | SQLite trade/fill journal |
| Config | `evclaw_rust/.env` + `evclaw_rust/src/config.rs` | Rust bot runtime config |
| Ops wrappers | `scripts/evclaw_rust_*.sh` | Start/restart/status/log commands |

## Data Flow

1. **Signal Ingestion**: SSE consumer receives signals from tracker
2. **Context Building**: Signals aggregated into cycle context
3. **Decision Making**: Trading brain scores opportunities
4. **Entry Gating**: LLM gate approves/rejects, or deterministic fallback applies
5. **Risk Assessment**: Risk manager validates and sizes positions
6. **Execution**: Executor places orders on appropriate venue
7. **Learning**: Learning engine updates weights from outcomes

Rust bot data flow:

1. **Wallet scan**: Hyperliquid `/info` wallet/account snapshot
2. **Signal evaluation**: combined `dead_cap` + `whale` logic in Rust
3. **Execution**: chase-limit entries/exits on Hyperliquid perps
4. **Protection**: exchange-native SL/TP placement and periodic repair
5. **Persistence**: JSON state + SQLite journal under `evclaw_rust/state-live/`

## Entry Gate Runtime Controls

1. `global_pause.enabled=true` hard-blocks all new entries (universal pause switch).
2. `entry_gate_bypass_guard` applies when candidates are tagged as gate bypass:
   - size multiplier cap (default `0.5x`)
   - rolling window cap (`max_entries_per_window`)
   - optional hard block after prolonged gate unreachability
3. Each proposal/trade carries gate attribution fields:
   - `entry_gate_execution_type` (`llm` | `bypass` | `deterministic`)
   - `entry_gate_bypass_reason` (when relevant)

## Learning Attribution Guard

- Learning skips adaptive signal/symbol updates and pattern updates for trades tagged `entry_gate_execution_type=bypass`.
- This prevents tunnel/outage fallback behavior from corrupting signal coefficients.

## HIP3 Runtime Path (Critical)

1. **Primary feed**: `sse_consumer.py` connects to `https://tracker.evplus.ai:8443/sse/tracker?key=<wallet>` and merges `hip3-data` into `symbol["hip3_predator"]`.
2. **Trigger path**: `cycle_trigger.py` consumes HIP3 snapshots from SSE and builds candidates from `hip3_predator`.
3. **Signal logic**: `hip3_main.py` computes `hip3_main` with OR semantics (FLOW pass or OFM pass), while direction conflicts are blocked.
4. **Context enrichment**: `context_builder_v2.py` uses SSE payload first; if missing, it optionally fetches `EVCLAW_TRACKER_HIP3_PREDATOR_URL`.
5. **Failure behavior**: REST auth/routing issues (`401/403`) are non-fatal but can reduce enrichment. If FLOW/OFM gates fail, HIP3 candidates are suppressed for that symbol.

## Memory Architecture

```
memory/
├── signal_weights.yaml      # Learned signal multipliers
├── trade_journal.yaml       # Historical trades
├── circuit_breaker.yaml     # Risk state
├── positions.yaml           # Active positions
├── symbol_blacklist.yaml    # Excluded symbols
├── context_feature_stats.json  # Context win rates
├── mistakes.json            # Mistake classification
├── patterns.json            # Pattern performance
└── adjustments.json         # Manual adjustments
```
