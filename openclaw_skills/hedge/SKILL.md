---
name: hedge
description: Deterministic portfolio hedge planner. `/hedge` proposes a single BTC/ETH hedge sized to cut Hyperliquid net notional by ~50%, and stores the plan for `/execute`.
user-invocable: true
metadata: {"openclaw":{"requires":{"bins":["python3"]}}}
---

# /hedge (deterministic)

Goal: when the user types `/hedge`, propose **one** conservative hedge trade sized to reduce HL net notional by a target percent.

Default: **100% (neutralize)**. The user can choose a smaller hedge (e.g., 50%).

## Run

```bash
python3 scripts/generate_hedge.py
```

Options:
- `--symbol BTC|ETH` (default BTC)
- `--pct 100` (default 100; hedge size as % of current HL net notional)
- `--db ...` `--runtime ...`

## Logic (no LLM)
- Reads latest portfolio snapshot (HL net notional + live equity) from the same context sources as `/trade`.
- Chooses hedge direction opposite current net exposure.
- Sizes hedge notional to ~50% of current net notional (with a safety cap).
- Builds FAST + RESTING options and stores a manual plan for `/execute`.

## Output formatting (Telegram)
When replying, always use true one-click copy for Telegram: **EACH execute command gets its own single-line code block**:
```
/execute <ID> chase
```
```
/execute <ID> limit
```
