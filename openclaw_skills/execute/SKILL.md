---
name: execute
description: Execute a stored manual `/trade` plan by id. Usage: `/execute <PLAN_ID> chase|limit [ttl=60m|4h]`.
user-invocable: true
metadata: {"openclaw":{"requires":{"bins":["python3"]}}}
---

# /execute (manual plan execution)

This command executes a previously generated `/trade` plan.

## Usage

- Fast fill:
  - `/execute ETH-01 chase`
- RESTING limit (pending SR limit; default cancel timing comes from the stored plan, typically 60m):
  - `/execute ETH-01 limit`
- Override limit TTL:
  - `/execute ETH-01 limit 4h`

## Workflow

1) Run the executor script (deterministic):

```bash
python3 ../trade/scripts/execute_plan.py <PLAN_ID> <chase|limit> [ttl]
```

2) Reply with the result:
- If chase: fill price/size + SL/TP ids
- If limit: confirm pending order placed (price + expiry)

## Safety
- Refuse if plan is expired or missing.
- Never run on behalf of non-owner senders.
