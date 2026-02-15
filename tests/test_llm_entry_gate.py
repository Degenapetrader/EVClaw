#!/usr/bin/env python3
"""LLM entry-gate normalization tests."""

import sys
from pathlib import Path
import asyncio
import json

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import llm_entry_gate
from llm_entry_gate import _normalize_gate_decision, build_entry_gate_prompt
from openclaw_agent_client import safe_json_loads


def test_normalize_gate_decision_tolerates_partial_schema_errors() -> None:
    candidates = [
        {"symbol": "BTC", "direction": "LONG"},
        {"symbol": "ETH", "direction": "SHORT"},
    ]
    picks_raw = [
        {"symbol": "BTC", "direction": "LONG", "reason": "strong setup"},
        {"symbol": "ETH", "direction": "SHORT"},  # missing reason -> dropped
    ]
    rejects_raw = [
        {"symbol": "DOGE", "direction": "LONG", "reason": "not in candidates"},  # dropped
    ]

    picks, rejects = _normalize_gate_decision(
        candidates=candidates,
        picks_raw=picks_raw,
        rejects_raw=rejects_raw,
        max_pick=4,
    )

    assert picks == [{"symbol": "BTC", "direction": "LONG", "reason": "strong setup"}]
    assert rejects == [{"symbol": "ETH", "direction": "SHORT", "reason": "rejected_by_gate_default"}]


def test_normalize_gate_decision_caps_pick_count_and_fills_coverage() -> None:
    candidates = [
        {"symbol": "BTC", "direction": "LONG"},
        {"symbol": "ETH", "direction": "SHORT"},
        {"symbol": "SOL", "direction": "LONG"},
    ]
    picks_raw = [
        {"symbol": "BTC", "direction": "LONG", "reason": "A"},
        {"symbol": "ETH", "direction": "SHORT", "reason": "B"},
        {"symbol": "SOL", "direction": "LONG", "reason": "C"},
    ]

    picks, rejects = _normalize_gate_decision(
        candidates=candidates,
        picks_raw=picks_raw,
        rejects_raw=[],
        max_pick=2,
    )

    assert len(picks) == 2
    assert {f"{p['symbol']}:{p['direction']}" for p in picks} == {"BTC:LONG", "ETH:SHORT"}
    assert rejects == [{"symbol": "SOL", "direction": "LONG", "reason": "rejected_by_gate_default"}]


def test_normalize_gate_decision_clamps_size_mult() -> None:
    candidates = [{"symbol": "BTC", "direction": "LONG"}]
    picks_raw = [{"symbol": "BTC", "direction": "LONG", "reason": "ok", "size_mult": 10}]
    picks, rejects = _normalize_gate_decision(
        candidates=candidates,
        picks_raw=picks_raw,
        rejects_raw=[],
        max_pick=4,
    )
    assert rejects == []
    assert picks and picks[0]["size_mult"] == 2.0


def test_normalize_gate_decision_clamps_size_mult_lower_bound() -> None:
    candidates = [{"symbol": "BTC", "direction": "LONG"}]
    picks_raw = [{"symbol": "BTC", "direction": "LONG", "reason": "ok", "size_mult": 0.01}]
    picks, rejects = _normalize_gate_decision(
        candidates=candidates,
        picks_raw=picks_raw,
        rejects_raw=[],
        max_pick=4,
    )
    assert rejects == []
    assert picks and picks[0]["size_mult"] == 0.5


def test_safe_json_loads_handles_fenced_json() -> None:
    raw = "```json\n{\"picks\":[],\"rejects\":[]}\n```"
    parsed = safe_json_loads(raw)
    assert isinstance(parsed, dict)
    assert parsed.get("picks") == []
    assert parsed.get("rejects") == []


def test_build_entry_gate_prompt_omits_llm_order_type_policy() -> None:
    prompt = build_entry_gate_prompt(
        seq=1,
        global_context_compact="ctx",
        exposure_context={},
        candidates=[{"symbol": "BTC", "direction": "LONG"}],
        max_pick=1,
    )
    assert "Do not decide order_type." in prompt
    assert "3-tier framework" not in prompt
    assert '"order_type"' not in prompt


def test_build_entry_gate_prompt_includes_hip3_priority_policy() -> None:
    prompt = build_entry_gate_prompt(
        seq=1,
        global_context_compact="ctx",
        exposure_context={},
        candidates=[{"symbol": "XYZ:NVDA", "direction": "LONG", "signals": ["HIP3_MAIN:LONG z=2.0"]}],
        max_pick=1,
    )
    assert "Priority: if any HIP3_MAIN / XYZ candidates are present" in prompt


def test_build_entry_gate_prompt_hip3_mode_includes_balance_policy() -> None:
    prompt = build_entry_gate_prompt(
        seq=1,
        global_context_compact="ctx",
        exposure_context={
            "hip3_balance": {
                "target_net_abs_pct": 20.0,
                "soft_band_pct": 15.0,
                "hard_band_pct": 30.0,
                "current_net_pct": -12.4,
            }
        },
        candidates=[{"symbol": "XYZ:NVDA", "direction": "LONG", "signals": ["HIP3_MAIN:LONG z=2.0"]}],
        max_pick=1,
        mode="hip3",
    )
    assert "HIP3 mode: FLOW/OFM are primary drivers" in prompt
    payload = json.loads(prompt.split("\n", 1)[1])
    assert payload["gate_mode"] == "hip3"
    assert payload["exposure"]["hip3_balance"]["target_net_abs_pct"] == 20.0


def test_compact_candidate_truncates_dossier_and_extracts_sr_metrics() -> None:
    long_dossier = "X" * (llm_entry_gate._MAX_DOSSIER_CHARS + 80)
    candidate = {
        "symbol": "eth",
        "direction": "long",
        "pipeline_conviction": 0.41,
        "brain_conviction": 0.62,
        "blended_conviction": 0.58,
        "symbol_learning_dossier": long_dossier,
        "funding_rate": {"value": 0.0012},
        "volume_24h_usd": {"value": 1_500_000},
        "context_snapshot": {
            "key_metrics": {
                "price": 2450.5,
                "trend_state": {"regime": "uptrend"},
                "sr_levels": {
                    "price": 2450.5,
                    "nearest": {
                        "support": {"price": 2420.0},
                        "resistance": {"price": 2480.0},
                    },
                },
            }
        },
    }

    compact = llm_entry_gate._compact_candidate(candidate)
    assert compact["symbol"] == "ETH"
    assert compact["direction"] == "LONG"
    assert compact["conviction"] == 0.58
    assert compact["pipeline_conviction"] == 0.41
    assert compact["brain_conviction"] == 0.62
    assert compact["blended_conviction"] == 0.58
    assert compact["symbol_learning_dossier"].startswith("...")
    assert len(compact["symbol_learning_dossier"]) <= llm_entry_gate._MAX_DOSSIER_CHARS
    assert compact["funding_rate"] == 0.0012
    assert compact["volume_24h_usd"] == 1_500_000
    assert compact["trend_regime"] == "uptrend"
    assert compact["sr"] == {"price": 2450.5, "support": 2420.0, "resistance": 2480.0}


def test_compact_candidate_computes_blended_conviction_when_missing() -> None:
    compact = llm_entry_gate._compact_candidate(
        {
            "symbol": "SOL",
            "direction": "LONG",
            "conviction": 0.9,
            "signals_snapshot": {
                "dead_capital": {"direction": "LONG", "z_score": 2.5},
            },
            "context_snapshot": {
                "key_metrics": {
                    "atr_pct": 1.1,
                    "smart_cvd": 1,
                    "divergence_z": 0.5,
                    "cohort_signal": "SMART_ACCUMULATING",
                }
            },
        }
    )
    assert 0.0 <= float(compact["pipeline_conviction"]) <= 1.0
    assert 0.0 <= float(compact["brain_conviction"]) <= 1.0
    assert 0.0 <= float(compact["blended_conviction"]) <= 1.0
    assert compact["conviction"] == compact["blended_conviction"]


def test_build_entry_gate_prompt_embeds_exposure_and_compact_candidates() -> None:
    prompt = build_entry_gate_prompt(
        seq=101,
        global_context_compact="regime=trend_up",
        exposure_context={
            "hl_net_exposure_usd": -2500.0,
            "open_positions": [{"symbol": "ETH", "direction": "LONG", "venue": "hyperliquid"}],
        },
        candidates=[
            {
                "symbol": "BTC",
                "direction": "SHORT",
                "price": 100000.0,
                "signals": ["cvd"],
            }
        ],
        max_pick=2,
    )

    # Prompt format is fixed prefix + compact JSON payload.
    payload = json.loads(prompt.split("\n", 1)[1])
    assert payload["seq"] == 101
    assert payload["global_context_compact"] == "regime=trend_up"
    assert payload["exposure"]["open_positions"][0]["symbol"] == "ETH"
    assert payload["candidates"][0]["symbol"] == "BTC"
    assert payload["candidates"][0]["direction"] == "SHORT"
    assert payload["max_pick"] == 2


def test_run_entry_gate_defaults_enabled_when_env_unset() -> None:
    import os

    original = os.environ.pop("EVCLAW_LLM_GATE_ENABLED", None)
    try:
        decision = asyncio.run(
            llm_entry_gate.run_entry_gate(
                seq=1,
                global_context_compact="ctx",
                exposure_context={},
                candidates=[],
            )
        )
        assert decision.enabled is True
        assert decision.picks == []
        assert decision.rejects == []
    finally:
        if original is not None:
            os.environ["EVCLAW_LLM_GATE_ENABLED"] = original


def test_run_entry_gate_reports_error_kind_from_client() -> None:
    import os

    async def fake_turn(**_kwargs):
        return (
            {
                "error_kind": "timeout",
                "timed_out": True,
                "return_code": None,
                "stderr": "timeout on upstream",
                "payload": None,
            },
            "",
        )

    original_env = os.environ.get("EVCLAW_LLM_GATE_ENABLED")
    original_turn = llm_entry_gate.openclaw_agent_turn
    os.environ["EVCLAW_LLM_GATE_ENABLED"] = "1"
    llm_entry_gate.openclaw_agent_turn = fake_turn
    try:
        decision = asyncio.run(
            llm_entry_gate.run_entry_gate(
                seq=2,
                global_context_compact="ctx",
                exposure_context={},
                candidates=[{"symbol": "BTC", "direction": "LONG"}],
            )
        )
        assert decision.enabled is True
        assert decision.error == "empty_reply:timeout"
        assert decision.error_kind == "timeout"
        assert "timeout on upstream" in (decision.stderr_excerpt or "")
    finally:
        llm_entry_gate.openclaw_agent_turn = original_turn
        if original_env is None:
            os.environ.pop("EVCLAW_LLM_GATE_ENABLED", None)
        else:
            os.environ["EVCLAW_LLM_GATE_ENABLED"] = original_env


def test_entry_gate_config_default_timeout_is_120s() -> None:
    import os

    old_timeout = os.environ.pop("EVCLAW_LLM_GATE_TIMEOUT_SEC", None)
    old_enabled = os.environ.pop("EVCLAW_LLM_GATE_ENABLED", None)
    try:
        cfg = llm_entry_gate.EntryGateConfig.load()
        assert cfg.timeout_sec == 120.0
    finally:
        if old_timeout is not None:
            os.environ["EVCLAW_LLM_GATE_TIMEOUT_SEC"] = old_timeout
        if old_enabled is not None:
            os.environ["EVCLAW_LLM_GATE_ENABLED"] = old_enabled


def test_run_entry_gate_retries_once_on_invalid_json() -> None:
    import os

    calls = {"n": 0}
    retry_timeout = {"value": None}

    async def fake_turn(**_kwargs):
        calls["n"] += 1
        if calls["n"] == 2:
            retry_timeout["value"] = float(_kwargs.get("timeout_sec") or 0.0)
        if calls["n"] == 1:
            return (
                {"error_kind": "none", "timed_out": False, "return_code": 0, "stderr": "", "payload": {}},
                "not-json",
            )
        return (
            {"error_kind": "none", "timed_out": False, "return_code": 0, "stderr": "", "payload": {}},
            '{"picks":[{"symbol":"BTC","direction":"LONG","reason":"ok"}],"rejects":[]}',
        )

    original_env_enabled = os.environ.get("EVCLAW_LLM_GATE_ENABLED")
    original_turn = llm_entry_gate.openclaw_agent_turn
    os.environ["EVCLAW_LLM_GATE_ENABLED"] = "1"
    llm_entry_gate.openclaw_agent_turn = fake_turn
    try:
        decision = asyncio.run(
            llm_entry_gate.run_entry_gate(
                seq=99,
                global_context_compact="ctx",
                exposure_context={},
                candidates=[{"symbol": "BTC", "direction": "LONG"}],
            )
        )
        assert calls["n"] == 2
        assert retry_timeout["value"] == 8.0
        assert decision.error is None
        assert len(decision.picks) == 1
        assert decision.picks[0]["symbol"] == "BTC"
    finally:
        llm_entry_gate.openclaw_agent_turn = original_turn
        if original_env_enabled is None:
            os.environ.pop("EVCLAW_LLM_GATE_ENABLED", None)
        else:
            os.environ["EVCLAW_LLM_GATE_ENABLED"] = original_env_enabled


def test_run_entry_gate_retry_both_fail_returns_invalid_json() -> None:
    import os

    calls = {"n": 0}

    async def fake_turn(**_kwargs):
        calls["n"] += 1
        if calls["n"] == 1:
            return (
                {"error_kind": "none", "timed_out": False, "return_code": 0, "stderr": "", "payload": {}},
                "not-json",
            )
        return (
            {"error_kind": "invalid_json", "timed_out": False, "return_code": 0, "stderr": "bad", "payload": {}},
            "still-not-json",
        )

    original_env_enabled = os.environ.get("EVCLAW_LLM_GATE_ENABLED")
    original_turn = llm_entry_gate.openclaw_agent_turn
    os.environ["EVCLAW_LLM_GATE_ENABLED"] = "1"
    llm_entry_gate.openclaw_agent_turn = fake_turn
    try:
        decision = asyncio.run(
            llm_entry_gate.run_entry_gate(
                seq=100,
                global_context_compact="ctx",
                exposure_context={},
                candidates=[{"symbol": "BTC", "direction": "LONG"}],
            )
        )
        assert calls["n"] == 2
        assert decision.error == "invalid_json:invalid_json"
        assert decision.error_kind == "invalid_json"
    finally:
        llm_entry_gate.openclaw_agent_turn = original_turn
        if original_env_enabled is None:
            os.environ.pop("EVCLAW_LLM_GATE_ENABLED", None)
        else:
            os.environ["EVCLAW_LLM_GATE_ENABLED"] = original_env_enabled


def test_run_entry_gate_session_id_is_unique_per_invocation() -> None:
    import os

    seen_session_ids = []

    async def fake_turn(**kwargs):
        seen_session_ids.append(str(kwargs.get("session_id") or ""))
        return (
            {"error_kind": "none", "timed_out": False, "return_code": 0, "stderr": "", "payload": {}},
            '{"picks":[{"symbol":"BTC","direction":"LONG","reason":"ok"}],"rejects":[]}',
        )

    original_env_enabled = os.environ.get("EVCLAW_LLM_GATE_ENABLED")
    original_turn = llm_entry_gate.openclaw_agent_turn
    os.environ["EVCLAW_LLM_GATE_ENABLED"] = "1"
    llm_entry_gate.openclaw_agent_turn = fake_turn
    try:
        d1 = asyncio.run(
            llm_entry_gate.run_entry_gate(
                seq=501,
                global_context_compact="ctx",
                exposure_context={},
                candidates=[{"symbol": "BTC", "direction": "LONG"}],
            )
        )
        d2 = asyncio.run(
            llm_entry_gate.run_entry_gate(
                seq=501,
                global_context_compact="ctx",
                exposure_context={},
                candidates=[{"symbol": "BTC", "direction": "LONG"}],
            )
        )
        assert len(seen_session_ids) == 2
        assert seen_session_ids[0] != seen_session_ids[1]
        assert d1.session_id == seen_session_ids[0]
        assert d2.session_id == seen_session_ids[1]
        assert seen_session_ids[0].startswith("hl_entry_gate_501_")
    finally:
        llm_entry_gate.openclaw_agent_turn = original_turn
        if original_env_enabled is None:
            os.environ.pop("EVCLAW_LLM_GATE_ENABLED", None)
        else:
            os.environ["EVCLAW_LLM_GATE_ENABLED"] = original_env_enabled
