#!/usr/bin/env python3
"""Guardrail: prevent reintroducing removed legacy env aliases in runtime files."""

from __future__ import annotations

from pathlib import Path
from typing import Dict, List

ROOT = Path(__file__).resolve().parents[1]

RUNTIME_BANNED: Dict[str, List[str]] = {
    "execution_dispatch.py": ["CLAWDBOT_CMD"],
    "llm_entry_gate.py": ["CLAWDBOT_CMD"],
    "llm_exit_decider.py": ["CLAWDBOT_CMD"],
    "config_env.py": ['"ENABLED_VENUES"'],
    "context_builder_v2.py": ['"ENABLED_VENUES"'],
    "conviction_model.py": [
        "EVCLAW_BRAIN_CONVICTION_WEIGHTS",
        "EVCLAW_BRAIN_BLEND_PIPELINE",
        "EVCLAW_BRAIN_CONVICTION_THRESHOLD",
        "EVCLAW_BRAIN_CONVICTION_Z_DENOM",
        "EVCLAW_BRAIN_STRENGTH_Z_MULT",
    ],
    "cycle_trigger.py": [
        '"HIP3_TRIGGER_GLOBAL_COOLDOWN_SEC"',
        '"HIP3_TRIGGER_SYMBOL_COOLDOWN_SEC"',
        '"HIP3_TRIGGER_TTL_SEC"',
    ],
}

ENV_TEMPLATE_BANNED: Dict[str, List[str]] = {
    ".env": ["CLAWDBOT_CMD="],
    ".env.min": ["CLAWDBOT_CMD="],
    ".env.example": ["CLAWDBOT_CMD="],
}


def collect_violations() -> List[str]:
    violations: List[str] = []

    for rel_path, banned_tokens in RUNTIME_BANNED.items():
        path = ROOT / rel_path
        text = path.read_text(encoding="utf-8")
        for token in banned_tokens:
            if token in text:
                violations.append(f"{rel_path}: found banned token {token}")

    for rel_path, banned_tokens in ENV_TEMPLATE_BANNED.items():
        path = ROOT / rel_path
        if not path.exists():
            continue
        text = path.read_text(encoding="utf-8")
        for token in banned_tokens:
            if token in text:
                violations.append(f"{rel_path}: found banned token {token}")

    return violations


def main() -> int:
    violations = collect_violations()
    if violations:
        print("[FAIL] Legacy env alias guardrail violations detected:")
        for item in violations:
            print(f" - {item}")
        return 1

    print("[PASS] Legacy env alias guardrail clean")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
