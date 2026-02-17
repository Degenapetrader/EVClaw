---
name: stats
description: "Deterministic wallet dashboard. `/stats` shows live equity/exposure/positions/health for the user wallet (Hyperliquid + DB fallback) and highlights any missing SL/TP."
user-invocable: true
metadata: {"openclaw":{"requires":{"bins":["python3"]}}}
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

Output: compact, actionable, normie-friendly.
