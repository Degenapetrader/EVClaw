# Architecture Reference

## System Architecture

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

## Data Flow

1. **Signal Ingestion**: SSE consumer receives signals from tracker
2. **Context Building**: Signals aggregated into cycle context
3. **Decision Making**: Trading brain scores opportunities
4. **Risk Assessment**: Risk manager validates and sizes positions
5. **Execution**: Executor places orders on appropriate venue
6. **Learning**: Learning engine updates weights from outcomes

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
