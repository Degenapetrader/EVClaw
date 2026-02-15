#!/usr/bin/env python3
"""Targeted tests for deterministic execution metadata backfill."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from conviction_model import ConvictionConfig, resolve_order_type
import live_agent_process


def test_ensure_deterministic_execution_metadata_sets_limit_fields() -> None:
    candidate = {"conviction": 0.55}
    order_type = live_agent_process._ensure_deterministic_execution_metadata(
        candidate,
        resolve_order_type_fn=resolve_order_type,
        conviction_config=ConvictionConfig(chase_threshold=0.7, limit_min=0.2),
    )
    assert order_type == "limit"
    execution = candidate["execution"]
    assert execution["order_type"] == "limit"
    assert execution["source"] == "deterministic_policy"
    assert execution["conviction_source"] == "proposal_conviction"
    assert execution["limit_style"] == "sr_limit"
    assert execution["limit_fallback"] == "atr_1x"


def test_ensure_deterministic_execution_metadata_preserves_existing_source() -> None:
    candidate = {
        "conviction": 0.4,
        "blended_conviction": 0.82,
        "execution": {"source": "llm_gate", "pick_reason": "x"},
    }
    order_type = live_agent_process._ensure_deterministic_execution_metadata(
        candidate,
        resolve_order_type_fn=resolve_order_type,
        conviction_config=ConvictionConfig(chase_threshold=0.7, limit_min=0.2),
    )
    assert order_type == "chase_limit"
    execution = candidate["execution"]
    assert execution["order_type"] == "chase_limit"
    assert execution["source"] == "llm_gate"
    assert execution["conviction_source"] == "blended"
    assert execution["blended_conviction"] == 0.82


def test_ensure_deterministic_execution_metadata_returns_reject() -> None:
    candidate = {"conviction": 0.2}
    order_type = live_agent_process._ensure_deterministic_execution_metadata(
        candidate,
        resolve_order_type_fn=resolve_order_type,
        conviction_config=ConvictionConfig(chase_threshold=0.7, limit_min=0.2),
    )
    assert order_type == "reject"
    assert candidate["execution"]["order_type"] == "reject"
