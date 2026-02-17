# EVClaw AGENTS

Purpose: compact runtime contract for AI agents/operators working in this repo.

## Mission
- Run EVClaw as an OSS, network-first AGI trading system.
- Keep behavior deterministic where possible (ops/health/reconcile).
- Keep LLM decisions constrained to entry/exit decision tasks.
- Never assume private VPS-local files/services beyond this repo defaults.

## System map
- `cycle_trigger.py`: ingest cycle + build context.
- `live_agent.py` / `live_agent_process.py`: entry pipeline + execution path.
- `llm_exit_decider.py` / `hip3_exit_decider.py`: exit decisions.
- `hourly_ops.py`: deterministic safety/reconcile/health runner.
- `run_fill_reconciler.py`: fill tracking + reconciliation.
- `live_monitor.py`: equity snapshots used by SR/equity safety checks.

Runtime artifacts:
- DB: `${EVCLAW_DB_PATH:-./ai_trader.db}`
- Ops JSON: `${EVCLAW_RUNTIME_DIR:-./state}/hourly_ops_report.json`
- Ops summary: `${EVCLAW_RUNTIME_DIR:-./state}/hourly_ops_summary.txt`

## AGI flow contract (high level)
- Entry: cycle trigger -> context -> entry gate -> executor.
- Exit: producers (plan-only) -> exit decider -> executor close.
- Producers never place orders directly.
- Executor stays limit/chase-limit oriented.

## Hyperliquid + node2 protocol rules
- Private node base (EVClaw-gated): `https://node2.evplus.ai/evclaw/info?key=<wallet>`
- Wallet key query param is required when using node2 EVClaw path.
- Canonical wallet env: `HYPERLIQUID_ADDRESS`
- Delegated signer env: `HYPERLIQUID_AGENT_PRIVATE_KEY` (never main wallet private key)

Allowed private `/info` request `type` values:
- `meta`
- `spotMeta`
- `spotClearinghouseState`
- `clearinghouseState`
- `openOrders`
- `frontendOpenOrders`
- `userRateLimit`

Public/fallback (do not assume private coverage):
- `allMids`
- `userFills`
- any unsupported private-node type

Canonical payload templates:
- Equity (perps): `{"type":"clearinghouseState","user":"0x..."}`
- Builder equity/state: `{"type":"clearinghouseState","user":"0x...","dex":"xyz"}`
- Open orders: `{"type":"openOrders","user":"0x..."}`
- Builder open orders: `{"type":"frontendOpenOrders","user":"0x...","dex":"xyz"}`
- Meta: `{"type":"meta"}`

Do not invent payload keys/endpoints.

## Failure map (quick)
- `401/403` node2/tracker auth: wallet not approved/authorized (builder approval flow).
- `422` deserialize: malformed JSON body or wrong endpoint usage.
- `sr_limit_equity_missing`: monitor snapshots not fresh/missing (`evclaw-live-monitor` path).

## MANUAL_COMMANDS_START
Manual/interactive commands (not for scheduled cron turns):
- Idea/plan: `/trade <SYMBOL>` or `/best3`
- Execute existing plan: `/execute <PLAN_ID> chase|limit [ttl]`
- Hedge proposal: `/hedge`
- Status snapshot: `/stats`

If `PLAN_ID` is missing, generate/refresh with `/trade` first.
## MANUAL_COMMANDS_END

## CRON_CONTEXT_START
Cron scope (scheduled jobs) is ops/report only.

Use only:
- `hourly_ops.py` deterministic runner
- `hourly_ops_report.json` + `hourly_ops_summary.txt` outputs

Do:
- report freshness, health, and key counters
- highlight WARN/CRIT with concrete operator actions
- keep output compact and deterministic

Do NOT:
- run user-interactive manual commands from `MANUAL_COMMANDS`
- invent new endpoints or JSON payload shapes
- request/print/modify secret keys

Protocol reminders:
- node2 EVClaw path requires `?key=<wallet>`
- if private request type unsupported, use public fallback path
## CRON_CONTEXT_END
