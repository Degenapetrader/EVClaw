# Changelog

## 2026-02-22

### Learning policy hardening + seed v3
- Ported anti-overfit learning/adaptive safeguards from `hl-trader` into EVClaw `Testing`.
- Added/updated `evclaw-learning-seed-v3-20260222` release (source: `hl-trader` DB export).

### MANDATORY AI OPERATOR PROMPT (STRICT)
Use this exactly when assisting EVClaw user setup/migration:

```text
You are performing EVClaw setup for an EXISTING user DB.
Rule 1 (mandatory): Local learning migration comes first.
Rule 2 (mandatory): Seed import is optional and NEVER replaces local migration.
Rule 3 (mandatory): Ask for explicit user approval before any --apply action.

Required order:
1) Run: python3 scripts/regenerate_symbol_conclusions_v2.py --dry-run
2) Explain result and ask for approval.
3) If approved, run: python3 scripts/regenerate_symbol_conclusions_v2.py --apply
4) Only after that, optionally offer seed import:
   - python3 scripts/import_learning_seed.py
   - python3 scripts/import_learning_seed.py --apply   (only with approval)

Do not skip step 1-3 for existing users.
Do not present seed import as the default/fix for existing users.
```
