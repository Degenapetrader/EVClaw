---
name: execute
description: Execute manual trades via plan ID or ad-hoc input. Supports `/execute <PLAN_ID> chase|limit [ttl]` and `/execute long ETH chase [size] [ttl]`.
user-invocable: true
metadata: {"openclaw":{"requires":{"bins":["python3"]}}}
---

# /execute (manual plan execution)

This command supports two manual modes:
- Plan mode (existing): execute a previously generated `/trade` plan.
- Ad-hoc mode: parse direction/symbol/mode and generate+execute a plan deterministically.

## Usage

- Fast fill:
  - `/execute ETH-01 chase`
- RESTING limit (pending SR limit; default cancel timing comes from the stored plan, typically 60m):
  - `/execute ETH-01 limit`
- Override limit TTL:
  - `/execute ETH-01 limit 4h`

Ad-hoc examples:
- `/execute long ETH chase`
- `/execute ETH long limit 350`
- `/execute short ETH chase 250`
- `/execute long ETH chase 300 confirm`

## Workflow

1) Run the execute bridge script (deterministic):

```bash
EVCLAW_ROOT="${EVCLAW_ROOT:-$HOME/.openclaw/skills/EVClaw}" \
python3 "$EVCLAW_ROOT/openclaw_skills/execute/scripts/execute_bridge.py" <ARGS_FROM_/execute>
```

2) Handle JSON response deterministically:
- If `ok=true`: return execution result.
- If `needs=size_usd`: ask one question: "What size in USD?"
- If `needs=confirm`: show preview + ask for explicit confirm command.
  - Use returned `confirm_command` exactly.

3) Confirmation behavior for ad-hoc mode:
- Do not execute ad-hoc input without explicit confirmation.
- After user confirms, run bridge again with `confirm` token.

## Safety
- Refuse if plan is expired or missing.
- Never run on behalf of non-owner senders.
- Keep audit trail: ad-hoc mode must generate a plan first, then execute that plan.
