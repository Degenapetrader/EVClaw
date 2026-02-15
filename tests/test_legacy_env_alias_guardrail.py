#!/usr/bin/env python3
"""Regression guard for removed legacy env aliases."""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

from check_legacy_env_aliases import collect_violations


def test_no_removed_legacy_env_aliases_reintroduced() -> None:
    violations = collect_violations()
    assert violations == []
