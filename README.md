# EVClaw — Open-Source AGI Trading System for Hyperliquid

EVClaw is an autonomous trading system for Hyperliquid (perps + HIP3 stocks) that uses LLM-based, human-like decisions to run a full AGI-style flow.

It builds real-time context, asks LLM gate/decision agents for entries and exits, and executes through exchange adapters.

Runtime model: native Linux process stack (`python3` + `tmux`). Docker support is intentionally not included.

```text
Signal Sources (SSE/Tracker) → Cycle Trigger → Context Builder → Proposal Writer
  → LLM Entry Gate (pick/reject) → Executor (chase limit orders) → SL/TP placement

Running positions → Decay Worker + Position Review → LLM Exit Decider (close/hold) → Executor

Closed trades → Fill Reconciler → Learning Reflector → Symbol Conclusions (feedback loop)
```

## What EVClaw does

EVClaw is a production-style AGI trading stack with:

- LLM-gated entries and exits (no blind automation)
- Chase-limit execution only (no market or IOC orders)
- Multi-venue support (Hyperliquid perp, HIP3 stocks, Lighter)
- Conviction-based size and confidence filtering
- Adaptive SL/TP from ATR + per-symbol learning
- Fill reconciliation + PnL tracking
- Learning loop (reflections, dossier, symbol conclusions)
- Guardian pre-filters before every LLM step
- Sector exposure controls
- SR-level resting-entry support

## Quick Start

```bash
git clone <your-repo-url>
cd evclaw
python3 -m venv .venv
source .venv/bin/activate
cp .env.example .env
# Edit .env with your Hyperliquid wallet + API key
./bootstrap.sh
./start.sh
```

`bootstrap.sh` also installs two OpenClaw cron jobs by default (health check every 15m and hourly maintenance summary). Set `EVCLAW_INSTALL_OPENCLAW_CRONS=0` to skip.
If `openclaw` is missing, `bootstrap.sh` will try to install it via `npm i -g openclaw`.

## Prerequisites

- Linux server/VM
- `python3` (3.10+), `pip`, `venv`
- `tmux`
- `openclaw` installed and provider-authenticated
- Network access to EVPlus endpoints (`tracker.evplus.ai`, `node2.evplus`)

## Required tmux sessions

- `evclaw-cycle-trigger`: writes cycle/context artifacts (~every 5m)
- `evclaw-live-agent`: builds proposals and runs LLM entry gate
- `evclaw-exit-decider`: LLM close/hold decisions for normal flow
- `evclaw-hip3-exit-decider`: dedicated HIP3/builder close/hold decider
- `evclaw-decay`: plan-only producer for decays and flips
- `evclaw-review`: plan-only periodic reviewer
- `evclaw-fill-reconciler`: tracks fills/PNL and emits learning tasks
- `evclaw-exit-outcome`: evaluates delayed exit outcomes
- `evclaw-learning-reflector`: LLM reflection + symbol conclusions

## Key environment variables

See `.env.example` for all configurable variables and `docs/ENV_VARIABLES_FULL_REFERENCE.md` for an expanded list.

EVClaw is network-first and depends on EVPlus services:
- Tracker SSE/API via `tracker.evplus.ai`
- Hyperliquid private node via `node2.evplus`

- `HYPERLIQUID_ADDRESS` (required)
- `HYPERLIQUID_API` (required)
- `EVCLAW_ROOT` (optional; auto-set by `bootstrap.sh`)
- `EVCLAW_ENABLED_VENUES`
- `EVCLAW_TRACKER_BASE_URL`
- `EVCLAW_DB_PATH`

## LLM provider

EVClaw delegates LLM decisions to OpenClaw (`openclaw agent`), with separate agents for entry and exit steps.

Install and configure OpenClaw (OpenAI, Anthropic, etc.) in your environment before running.

Required OpenClaw assumptions:

- OpenClaw runtime is installed and usable from shell.
- At least one LLM provider is configured and working.
- Shell/exec permissions are allowed for EVClaw worker scripts.
- By default EVClaw uses OpenClaw's default agent. You can override per workflow via:
  `EVCLAW_LLM_GATE_AGENT_ID`,
  `EVCLAW_HIP3_LLM_GATE_AGENT_ID`,
  `EVCLAW_EXIT_DECIDER_AGENT_ID`,
  `EVCLAW_HIP3_EXIT_DECIDER_AGENT_ID`,
  `EVCLAW_LEARNING_REFLECTOR_AGENT_ID`.

## License

See [LICENSE](LICENSE).

## Disclaimer

This software is provided as-is. Trading cryptocurrencies involves substantial risk. Use at your own risk. Not financial advice.
