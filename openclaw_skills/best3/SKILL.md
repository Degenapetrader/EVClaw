---
name: best3
description: Deterministic manual trade picker. `/best3` returns up to 3 Hyperliquid perp setups (FAST chase vs RESTING SR/ATR limit) and stores each plan for `/execute`.
user-invocable: true
metadata: {"openclaw":{"requires":{"bins":["python3"]}}}
---

# /best3 (deterministic)

Goal: when the user types `/best3`, pick the **best up to 3** current Hyperliquid perp opportunities using our **existing candidate snapshot** and produce **manual plans** (no LLM).

## How it works
Run:

```bash
python3 scripts/generate_best3.py
```

Optional:
- `--n 3` (default 3)
- `--db /path/to/ai_trader.db`
- `--runtime /path/to/evclaw/state`

The script will:
- Load latest `evclaw_candidates_*.json`
- Rank candidates by `blended_conviction` (fallbacks to `conviction`)
- Allocate plan IDs in `manual_trade_plans`
- Build deterministic setups with:
  - LIVE HL mid + BBO
  - RESTING entry = SR support/resistance when available; fallback to 1×ATR from mid when SR missing/too far
  - Post-only safe limit nudging
- Write `/tmp/manual_trade_plan_<ID>.json`
- Store READY rows in `manual_trade_plans` for `/execute <ID> chase|limit`

## Output rules
- One-shot, normie-friendly.
- No internal jargon.
- Display **live price** (HL mid) for each pick.
- Always include true one-click copy for Telegram: **EACH execute command gets its own single-line code block**.

Example:
```
/execute <ID> chase
```
```
/execute <ID> limit
```

- Do NOT add labels/bullets/extra text inside those code blocks.
