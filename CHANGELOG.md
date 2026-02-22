# Changelog

## 2026-02-22

### Learning policy hardening + seed v3
- Ported anti-overfit learning/adaptive safeguards from `hl-trader` into EVClaw `Testing`.
- Added/updated `evclaw-learning-seed-v3-20260222` release (source: `hl-trader` DB export).

### MANDATORY AI OPERATOR PROMPT (STRICT)
Use this exactly when assisting EVClaw user setup/migration:

```text
You are performing EVClaw setup for an EXISTING user DB.
Rule 1 (mandatory): Importing learning seed is optional.
Rule 2 (mandatory): Recommended default for new users is start fresh (no import).
Rule 3 (mandatory): Ask for explicit user approval before any --apply action.

If user explicitly wants historical bootstrap:
1) Run: python3 scripts/import_learning_seed.py
2) Explain dry-run result and ask for approval.
3) If approved, run: python3 scripts/import_learning_seed.py --apply

Never present seed import as mandatory.
```
