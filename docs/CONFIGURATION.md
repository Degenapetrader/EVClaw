# EVClaw configuration

## Files and precedence

EVClaw uses **both**:

- `skill.yaml` (canonical, versioned defaults + structured config)
- `.env` (machine-local secrets + optional overrides)

**Effective config precedence**:

1) `skill.yaml` defaults
2) `config_env.py:apply_env_overrides()` applies selected env overrides (connectivity + runtime wiring only, e.g. SSE host/port, DB path)
3) Some subsystems also support additional env-only knobs (legacy/runtime flags)

## `.env.min`

`.env.min` is a *minimal* env file intended to replace `.env` once you’re ready.

Goal: keep only:

- Secrets (private keys, API tokens)
- Endpoints (HL/Lighter base URLs, private node URL)
- Wiring that is truly host-specific (DB path, SSE host/port)

Everything else (risk caps, executor caps, AGI caps) should live in `skill.yaml`.

Suggested cutover:

1) Backup current `.env` → `.env.full.bak.<timestamp>`
2) Copy `.env.min` → `.env`
3) Restart EVClaw tmux processes

## Ownership Guardrails

Runtime ownership consistency is enforced by:

- `python3 tools/check_config_ownership.py`
- `pytest -q tests/test_config_ownership_guardrails.py`

## Trade styles (Safe / Balanced / Aggressive)

Trade style is controlled by the **mode controller** in `mode_controller.py`.

### Mapping

- **Safe** → `conservative`
- **Balanced** → `balanced`
- **Aggressive** → `aggressive`

### YAML-first configuration

Default tuning mode and slider values live in `skill.yaml`:

```yaml
config:
  mode_controller:
    mode: balanced
    sliders:
      risk_appetite: 50
      trade_frequency: 50
      aggression: 50
      signal_weighting: 50
      learning_speed: 50
```

### No backward compatibility

Legacy mode aliases are removed.

Trade style/tuning mode uses **only** `skill.yaml: config.mode_controller.mode`.

## Single Exit Flow (Plan -> LLM Gate -> Execute -> Track)

Exit flow is intentionally single-path:

1) Producers emit plans only:
- `decay_worker.py` emits `DECAY_EXIT` events and writes `decay_worker_notify` HOLD plan rows.
- `position_review_worker.py` emits `POSITION_REVIEW_PLAN` events with `plan_only: true` and writes HOLD plan rows.
2) LLM gate decides:
- `llm_exit_decider.py` consumes producer HOLD plans and decides `CLOSE` vs `HOLD`.
3) Execution:
- `llm_exit_decider.py` executes CLOSE via `python3 cli.py positions --close ...`.
4) Tracking linkage:
- `fill-reconciler` should run in hybrid mode (`python3 run_fill_reconciler.py --mode hybrid`) for faster fill detection.
- `exit_outcome_worker.py` evaluates delayed outcomes and writes `exit_decision_outcomes_v1`.

Definitions:
- Plan event: producer candidate only; no order was placed.
- Decision: `CLOSE`/`HOLD` audit row in `decay_decisions`.
- Execution: real close reflected in `trades.exit_time` and `trades.exit_reason`.
- Tracking outcome: delayed classification row in `exit_decision_outcomes_v1`.

`plan_only: true` means the producer did not execute; execution is centralized in `llm_exit_decider.py`.

Non-plan exits (`TP`, `SL`, `EXTERNAL`, manual CLI) remain valid and are not part of this producer-plan gate.

### How To Verify Who Executed (SQL)

Producer plan rows:

```sql
SELECT source, action, reason, COUNT(*) AS cnt
FROM decay_decisions
WHERE source IN ('decay_worker_notify', 'position_review_worker')
GROUP BY source, action, reason
ORDER BY cnt DESC;
```

Decider decisions:

```sql
SELECT source, action, reason, COUNT(*) AS cnt
FROM decay_decisions
WHERE source LIKE 'hl_exit_decider%'
GROUP BY source, action, reason
ORDER BY cnt DESC;
```

Who executed for producer-plan exits (latest CLOSE decision per trade):

```sql
WITH target_exits AS (
  SELECT id, symbol, venue, exit_reason, exit_time
  FROM trades
  WHERE exit_time IS NOT NULL
    AND exit_reason IN (
      'DECAY_EXIT',
      'HOURLY_REVIEW_DEADLY_LOSER',
      'HOURLY_REVIEW_DEAD_FLAT',
      'HOURLY_REVIEW_EXPOSURE_REDUCE',
      'HOURLY_REVIEW_NO_PROGRESS'
    )
),
latest_close AS (
  SELECT trade_id, source, reason, ts,
         ROW_NUMBER() OVER (PARTITION BY trade_id ORDER BY ts DESC, id DESC) AS rn
  FROM decay_decisions
  WHERE action = 'CLOSE'
)
SELECT t.id AS trade_id,
       t.symbol,
       t.venue,
       t.exit_reason,
       datetime(t.exit_time, 'unixepoch') AS exit_ts,
       COALESCE(l.source, '<none>') AS latest_close_source,
       COALESCE(l.reason, '<none>') AS latest_close_reason,
       CASE WHEN l.ts IS NOT NULL THEN datetime(l.ts, 'unixepoch') END AS close_decision_ts
FROM target_exits t
LEFT JOIN latest_close l
  ON l.trade_id = t.id AND l.rn = 1
ORDER BY t.exit_time DESC;
```

### Entry Gate Note

Entry gate is hard-enabled in code. By default it uses OpenClaw's default agent; set `EVCLAW_LLM_GATE_AGENT_ID` (and `EVCLAW_HIP3_LLM_GATE_AGENT_ID` for HIP3 mode) to force a specific agent id. `.env` also controls model, thinking, timeout, and max-keep.
