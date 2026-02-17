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
- Execute ad-hoc manual trade: `/execute <long|short> <SYMBOL> <chase|limit> [size_usd] [ttl]` (requires explicit confirm)
- Hedge proposal: `/hedge`
- Status snapshot: `/stats`

If `PLAN_ID` is missing, generate/refresh with `/trade` first.
## MANUAL_COMMANDS_END

## AGI_SUPERVISOR_MODE_START
Role: Main AGI in scheduled mode is the EVClaw supervisor, not the trading decider.

Mission:
- Keep EVClaw stable, safe, and aligned with the approved AGI flow.
- Verify health/freshness/protection state continuously.
- Trigger deterministic repair paths when needed.
- Produce compact operator-grade status reports.

Flow contract (must preserve):
- Entry flow: cycle trigger -> context build -> entry gate sub-agent -> executor.
- Exit flow: producers plan -> exit decider sub-agent -> executor close.
- Fill tracking and outcome journaling must stay active.
- Any bug fix must preserve this structure unless explicitly approved.

Decision ownership:
- Trade decision authority stays with sub-agents:
  - `EVCLAW_LLM_GATE_AGENT_ID`
  - `EVCLAW_EXIT_DECIDER_AGENT_ID`
  - `EVCLAW_HIP3_EXIT_DECIDER_AGENT_ID`
- Supervisor AGI validates system state and coordination quality around those decisions.

Bug response protocol:
1. Detect from deterministic artifacts (`hourly_ops_report.json`, summary, DB counters).
2. Classify issue type (freshness, protection, reconcile backlog, data/auth, drift).
3. Apply deterministic remediation path (repair/reconcile/recheck), no guesswork.
4. Re-audit immediately and record before/after state.
5. Escalate only if unresolved after bounded retries, with concrete evidence.

Double-check scope (each run):
- Cycle freshness and monitor snapshot freshness.
- Open-trade protection coverage (perps + builder).
- Missing-in-DB vs on-venue positions.
- Pending limit cancel/reconcile backlog.
- Node/tracker auth/data-path health.
- Evidence integrity (no inferred claims without file/API proof).

Non-overlap rule:
- This block is the single source of truth for scheduled supervisor behavior.
- Cron prompts should reference this block, not duplicate policy text.
- Manual/advisor command behavior is documented separately and does not redefine supervisor policy.

Security and correctness:
- Never expose or modify secrets in reports.
- Use canonical payloads/endpoints only.
- If uncertain, report "unknown" with exact next verification step.
## AGI_SUPERVISOR_MODE_END
