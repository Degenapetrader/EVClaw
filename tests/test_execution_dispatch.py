#!/usr/bin/env python3
"""Tests for execution_dispatch command resolution hardening."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from execution_dispatch import _resolve_dispatch_cmd


def test_resolve_dispatch_cmd_defaults_when_unset(monkeypatch) -> None:
    monkeypatch.delenv("OPENCLAW_CMD", raising=False)
    assert _resolve_dispatch_cmd() == "openclaw"


def test_resolve_dispatch_cmd_accepts_bare_allowed_name(monkeypatch) -> None:
    monkeypatch.setenv("OPENCLAW_CMD", "clawdbot")
    assert _resolve_dispatch_cmd() == "clawdbot"


def test_resolve_dispatch_cmd_rejects_untrusted_path(monkeypatch) -> None:
    monkeypatch.setenv("OPENCLAW_CMD", "/tmp/openclaw")
    assert _resolve_dispatch_cmd() == "openclaw"


def test_resolve_dispatch_cmd_rejects_tokenized_string(monkeypatch) -> None:
    monkeypatch.setenv("OPENCLAW_CMD", "openclaw --mode now")
    assert _resolve_dispatch_cmd() == "openclaw"
