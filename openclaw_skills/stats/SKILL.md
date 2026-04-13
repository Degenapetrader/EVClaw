---
name: stats
description: "Deterministic wallet dashboard for Hyperliquid perps and HIP3 builder stocks. Use when the user asks for portfolio overview, account balance, PnL, position health, or types /stats. Shows live equity, exposure, open positions, and health metrics with DB fallback when live fetch fails."
user-invocable: true
metadata:
  version: "1.0.0"
  openclaw: '{"requires":{"bins":["python3"]}}'
---

# /stats (deterministic, live-first)

Goal: one-shot snapshot of the user wallet.

- **Live-first**: fetch from Hyperliquid API.
- **DB fallback** only when live fetch fails.

Run:
```bash
EVCLAW_ROOT="${EVCLAW_ROOT:-$HOME/.openclaw/skills/EVClaw}" \
python3 "$EVCLAW_ROOT/openclaw_skills/stats/scripts/generate_stats.py"
```

Options:
- `--wallet 0x...` (defaults to `HYPERLIQUID_ADDRESS`)
- `--db /path/to/evclaw/ai_trader.db`

Output format (example):
```
Equity: $12,450.32
Net Exposure: $3,200 (25.7%)
Positions: 3 open (2 perps, 1 HIP3)
  ETH LONG  $1,500  +2.3%
  BTC SHORT $1,200  -0.8%
  xyz:NVDA LONG $500  +1.1%
Health: OK (no alerts)
```

Related skills: see [/trade](../trade/SKILL.md) for planning trades, [/best3](../best3/SKILL.md) for top opportunities.
