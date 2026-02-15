#!/usr/bin/env python3
"""Guardrail tests for config ownership manifest enforcement."""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "tools"))

from check_config_ownership import (
    EFFECTIVE_FROM_ALLOWED,
    collect_hardcode_key_leaks,
    collect_invalid_effective_from_values,
    collect_removed_env_lookup_violations,
    load_manifest_rows,
)


def test_hardcode_keys_not_in_env_or_skill() -> None:
    rows = load_manifest_rows()
    leaks = collect_hardcode_key_leaks(rows)
    assert leaks == []


def test_removed_aliases_not_used_runtime() -> None:
    rows = load_manifest_rows()
    violations = collect_removed_env_lookup_violations(rows)
    assert violations == []


def test_csv_owner_manifest_has_valid_effective_from_values() -> None:
    rows = load_manifest_rows()
    invalid = collect_invalid_effective_from_values(rows, allowed=tuple(EFFECTIVE_FROM_ALLOWED))
    assert invalid == []
