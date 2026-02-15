# Live Agent Mode (Context → Proposal Writer → Entry Gate → Execute)

This repo now runs in **live agent** mode:

- `cycle_trigger.py` saves the cycle, builds context (text + JSON), then emits a small system event.
- `live_agent.py` builds scored candidates via `proposal_writer.py`.
- `llm_entry_gate.py` (`hl-entry-gate`, gpt-5.2) chooses PICK/REJECT from those candidates.
- Each approved pick executes on any **enabled** venue where the symbol is tradable.

## Cycle Trigger Behavior

On each SSE snapshot:
1. Save cycle to `<runtime_dir>/evclaw_cycle_<seq>.json`
2. Build context to `<runtime_dir>/evclaw_context_<seq>.txt`
3. Build JSON context to `<runtime_dir>/evclaw_context_<seq>.json`
4. Send a system event with payload:
   ```json
   {"seq":123,"cycle_file":"${EVCLAW_RUNTIME_DIR}/evclaw_cycle_123.json","context_file":"${EVCLAW_RUNTIME_DIR}/evclaw_context_123.txt","context_json_file":"${EVCLAW_RUNTIME_DIR}/evclaw_context_123.json","ts_utc":"..."}
   ```

If the main agent is busy, the newest cycle payload is stored in:
`${EVCLAW_RUNTIME_DIR}/evclaw_main_agent_pending.json`

## Main Agent Workflow

Default runner (AGI-only: OpenClaw agent executes; no manual approval prompts):
```bash
python3 ${EVCLAW_ROOT}/live_agent.py
```

Consume pending payload (record proposals):
```bash
python3 ${EVCLAW_ROOT}/live_agent.py run --from-pending
```

Ingest candidates (manual/test; records PROPOSED proposals):
```bash
python3 ${EVCLAW_ROOT}/live_agent.py execute \
  --seq 123 \
  --cycle-file ${EVCLAW_RUNTIME_DIR}/evclaw_cycle_123.json \
  --candidates-file ${EVCLAW_RUNTIME_DIR}/evclaw_candidates_123.json
```

Run with explicit JSON context (records PROPOSED proposals):
```bash
python3 ${EVCLAW_ROOT}/live_agent.py run \
  --seq 123 \
  --cycle-file ${EVCLAW_RUNTIME_DIR}/evclaw_cycle_123.json \
  --context-json-file ${EVCLAW_RUNTIME_DIR}/evclaw_context_123.json
```

Notes:
- AGI-only: OpenClaw executes proposals directly (no user confirmation).
- Entry gate is hard-enabled in code; `.env` controls model/thinking/timeout/max-keep only.
- Manual approve/execute CLI commands are intentionally removed in AGI-only mode.

## Hard Gates

- Trades must be available on **at least one enabled venue**; otherwise they are blocked.
- If `use_fill_reconciler: true`, the reconciler heartbeat must be active (`restart.sh` launcher default: `--mode hybrid`).
- DB/exchange positions must reconcile before new trades proceed.

## Heartbeat Files

- Main agent busy/pending state:
  - `${EVCLAW_RUNTIME_DIR}/evclaw_main_agent_state.json`
  - `${EVCLAW_RUNTIME_DIR}/evclaw_main_agent_pending.json`
- Fill reconciler heartbeat:
  - `/tmp/evclaw_fill_reconciler_heartbeat.json`

Runtime paths are hard-coded to `${EVCLAW_RUNTIME_DIR}`.
