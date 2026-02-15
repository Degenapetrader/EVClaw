#!/usr/bin/env python3
"""Tests for CLI order-type routing fallback resolution."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import cli


def test_resolve_effective_order_type_prefers_metadata() -> None:
    order_type, source = cli._resolve_effective_order_type({"order_type": "limit"}, conviction=0.95)
    assert order_type == "limit"
    assert source == "metadata"


def test_resolve_effective_order_type_uses_conviction_fallback_when_missing() -> None:
    order_type, source = cli._resolve_effective_order_type({}, conviction=0.55)
    assert order_type == "limit"
    assert source == "fallback_conviction"


def test_resolve_effective_order_type_uses_conviction_fallback_when_invalid_metadata() -> None:
    order_type, source = cli._resolve_effective_order_type({"order_type": "ioc"}, conviction=0.8)
    assert order_type == "chase_limit"
    assert source == "fallback_invalid_metadata"


def test_resolve_effective_order_type_rejects_missing_conviction() -> None:
    order_type, source = cli._resolve_effective_order_type({}, conviction=None)
    assert order_type == "reject"
    assert source == "fallback_conviction_missing_conviction"


def test_resolve_effective_order_type_uses_runtime_db_config() -> None:
    class _DB:
        @staticmethod
        def get_active_conviction_config():
            return {"params": {"chase_threshold": 0.85, "limit_min": 0.40}}

    order_type, source = cli._resolve_effective_order_type({}, conviction=0.75, db=_DB())
    assert order_type == "limit"
    assert source == "fallback_conviction"
