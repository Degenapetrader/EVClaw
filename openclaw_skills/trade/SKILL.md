---
name: trade
description: "Advisor-mode manual trade planner. `/trade <SYMBOL> [long|short] [$SIZE]` returns a compact plan with 2 entry styles (FAST chase vs RESTING limit) and stores it for `/execute`."
user-invocable: true
metadata: {"openclaw":{"requires":{"bins":["python3"]}}}
---

# /trade (advisor-mode)

Goal: when the user types `/trade ETH`, generate a **compact but informative** trade plan immediately (direction + confidence + sizing + SL/TP + key context) and always present **two execution options**:
- **FAST** = chase_limit entry
- **RESTING** = SR-anchored resting limit (pending SR limit) with **auto-cancel** (default 60m)

This command is **manual/advisor**: the user decides whether to execute.

## Workflow (must follow)

This uses an **LLM gate** (same idea as EVClaw entry gate) but restricted to the symbol(s) the user requested.

1) Generate + store plan(s) via the trade gate:

```bash
EVCLAW_ROOT="${EVCLAW_ROOT:-$HOME/.openclaw/skills/EVClaw}" \
python3 "$EVCLAW_ROOT/openclaw_skills/trade/scripts/generate_plans.py" <SYMBOL...> [--direction long|short] [--size-usd 5000]
```

Examples:
- Single symbol: `EVCLAW_ROOT="${EVCLAW_ROOT:-$HOME/.openclaw/skills/EVClaw}" python3 "$EVCLAW_ROOT/openclaw_skills/trade/scripts/generate_plans.py" ETH`
- Multi-symbol: `EVCLAW_ROOT="${EVCLAW_ROOT:-$HOME/.openclaw/skills/EVClaw}" python3 "$EVCLAW_ROOT/openclaw_skills/trade/scripts/generate_plans.py" ETH BTC`

This script will:
- allocate monotonic ids (e.g. `ETH-06`)
- load latest `evclaw_candidates_*.json`
- filter candidates to requested symbols
- call the OpenClaw manual trade gate agent (default id: `default`)
- write `/tmp/manual_trade_plan_<ID>.json`
- store plan_json into `manual_trade_plans` (status=READY)

2) Reply to the user with a short plan summary for each symbol and the execute commands (ONE-CLICK COPY):
- Put the exact command on its own line (prefer a code block), e.g. /execute <ID> chase
- And /execute <ID> limit

Output rules:
- Confidence displayed as **percent** (0–100) mapped from our conviction model.
- RESTING must include the explicit cancel timing string ("Auto-cancels ~60m after placing (around ... UTC)").
- Do NOT display raw pipeline score (misleading).
- Do NOT display funding rate or 24h price change.
- Direction MUST respect the dossier: if dossier vetoes a direction (e.g. "hard-veto longs"), flip direction or reject the trade with explanation.

Keep it compact (aim: <25 lines) but include a bigger-picture snapshot sourced from our internal SSE-derived artifacts:
- SSE micro (for symbol): zone, net_bias/net_bias_pct, ATR%, HL-vs-ref spread, CVD divergence signal
- SSE tape (universe): zone counts + most-lopsided long/short crowding
- venue equity + current HL net exposure + top open positions (context)
- 1-line dossier conclusion (if present)

## Notes
- Plans expire (default 60 minutes). If expired, tell the user to rerun `/trade`.
- Manual plans/trades must be tagged `strategy=manual` and linked via `pair_id=<DISPLAY_ID>`.
