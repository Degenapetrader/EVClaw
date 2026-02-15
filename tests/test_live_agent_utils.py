#!/usr/bin/env python3
"""Tests for live agent utilities (candidate validation + venue gate)."""

import asyncio
import json
import sqlite3
import sys
import tempfile
from argparse import Namespace
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import live_agent
from conviction_model import ConvictionConfig
from ai_trader_db import AITraderDB
from live_agent_utils import (
    enforce_max_candidates,
    validate_candidates_payload,
)


def test_check_positions_reconciled_detects_direction_mismatch() -> None:
    class DummyPos:
        def __init__(self, symbol: str, direction: str, size: float = 1.0) -> None:
            self.symbol = symbol
            self.direction = direction
            self.size = size

    class DummyAdapter:
        async def get_all_positions(self):
            return {"p1": DummyPos(symbol="ETH", direction="SHORT")}

    class DummyExecutor:
        def __init__(self) -> None:
            self.config = SimpleNamespace(
                lighter_enabled=False,
                hl_enabled=True,
                hl_wallet_enabled=False,
            )
            self.hyperliquid = DummyAdapter()
            self.hip3_wallet = None
            self.lighter = None

    class DummyTrade:
        venue = "hyperliquid"
        symbol = "ETH"
        direction = "LONG"

    class DummyDB:
        def get_open_trades(self):
            return [DummyTrade()]

    ok, detail = asyncio.run(
        live_agent.check_positions_reconciled(
            executor=DummyExecutor(),
            db=DummyDB(),
            allowed_venues=["hyperliquid"],
        )
    )
    assert not ok
    assert "direction_mismatch=" in detail


def test_get_learning_engine_cache_is_per_db_path() -> None:
    import types

    class FakeLearningEngine:
        def __init__(self, db_path, memory_dir):
            self.db_path = str(db_path)
            self.memory_dir = str(memory_dir)

    original_module = sys.modules.get("learning_engine")
    original_cache = dict(getattr(live_agent, "_LEARNING_ENGINES", {}))
    live_agent._LEARNING_ENGINES.clear()
    sys.modules["learning_engine"] = types.SimpleNamespace(LearningEngine=FakeLearningEngine)
    try:
        eng_a1 = live_agent._get_learning_engine("/tmp/a.db")
        eng_a2 = live_agent._get_learning_engine("/tmp/a.db")
        eng_b = live_agent._get_learning_engine("/tmp/b.db")
        assert eng_a1 is eng_a2
        assert eng_a1 is not eng_b
        assert eng_a1.db_path == "/tmp/a.db"
        assert eng_b.db_path == "/tmp/b.db"
    finally:
        live_agent._LEARNING_ENGINES.clear()
        live_agent._LEARNING_ENGINES.update(original_cache)
        if original_module is not None:
            sys.modules["learning_engine"] = original_module
        else:
            sys.modules.pop("learning_engine", None)


def test_sync_risk_manager_state_from_safety_applies_values() -> None:
    mgr = live_agent.DynamicRiskManager(config=live_agent.RiskConfig(), equity=10_000.0)

    class _State:
        daily_pnl = -123.45
        consecutive_wins = 2
        consecutive_losses = 4

    class _Safety:
        def get_state(self):
            return _State()

    live_agent._sync_risk_manager_state_from_safety(mgr, _Safety())
    assert mgr.daily_pnl == -123.45
    assert mgr.win_streak == 2
    assert mgr.loss_streak == 4


def test_sync_risk_manager_state_from_safety_is_safe_on_failures() -> None:
    mgr = live_agent.DynamicRiskManager(config=live_agent.RiskConfig(), equity=10_000.0)
    mgr.daily_pnl = 5.0
    mgr.win_streak = 1
    mgr.loss_streak = 1

    class _BrokenSafety:
        def get_state(self):
            raise RuntimeError("boom")

    live_agent._sync_risk_manager_state_from_safety(mgr, _BrokenSafety())
    assert mgr.daily_pnl == 5.0
    assert mgr.win_streak == 1
    assert mgr.loss_streak == 1
    live_agent._sync_risk_manager_state_from_safety(None, _BrokenSafety())  # no-op
    live_agent._sync_risk_manager_state_from_safety(mgr, None)  # no-op


def test_build_executor_with_learning_passes_db_path() -> None:
    captured = {"db_path": None}

    def fake_build_hybrid(exec_config, tracker, db_path=None):
        captured["db_path"] = db_path
        return object()

    class FakeExecutor:
        @staticmethod
        async def create(config, adaptive_sltp, trade_tracker):
            return {"config": config, "adaptive_sltp": adaptive_sltp, "trade_tracker": trade_tracker}

    original_build = live_agent._build_hybrid_sltp
    original_executor = live_agent.Executor
    live_agent._build_hybrid_sltp = fake_build_hybrid
    live_agent.Executor = FakeExecutor
    try:
        result = asyncio.run(
            live_agent._build_executor_with_learning(
                exec_config=SimpleNamespace(),
                tracker=object(),
                db_path="/tmp/override.db",
            )
        )
        assert captured["db_path"] == "/tmp/override.db"
        assert isinstance(result, dict)
    finally:
        live_agent._build_hybrid_sltp = original_build
        live_agent.Executor = original_executor


def test_run_cycle_marks_failure_when_auto_guardian_raises() -> None:
    import cli
    import live_agent_cycle

    async def fake_process_candidates(**_kwargs):
        return 0, {"status": "PROPOSED", "reason": None}

    async def fake_cmd_guardian(_args, **_kwargs):
        raise RuntimeError("guardian exploded")

    original_process = live_agent.process_candidates
    original_guardian = cli.cmd_guardian
    original_auto_guardian = live_agent_cycle.AUTO_GUARDIAN_ENABLED
    live_agent.process_candidates = fake_process_candidates
    cli.cmd_guardian = fake_cmd_guardian
    live_agent_cycle.AUTO_GUARDIAN_ENABLED = True
    try:
        with tempfile.TemporaryDirectory() as tmpdir:
            cycle_file = Path(tmpdir) / "cycle.json"
            context_json = Path(tmpdir) / "context.json"
            candidates_file = Path(tmpdir) / "candidates.json"
            cycle_file.write_text(json.dumps({"symbols": {"ETH": {}}}))
            context_json.write_text(json.dumps({"ok": True}))
            candidates_file.write_text(json.dumps({"schema_version": 1, "candidates": []}))

            rc, summary = asyncio.run(
                live_agent.run_cycle(
                    seq=42,
                    cycle_file=str(cycle_file),
                    context_json_file=str(context_json),
                    dry_run=False,
                    risk_pct_lighter=1.0,
                    risk_pct_hyperliquid=1.0,
                    candidates_file=str(candidates_file),
                    reuse_existing=True,
                )
            )
            assert rc == 1
            assert summary.get("auto_guardian") is False
            assert summary.get("reason") == "auto_guardian_failed"
    finally:
        live_agent.process_candidates = original_process
        cli.cmd_guardian = original_guardian
        live_agent_cycle.AUTO_GUARDIAN_ENABLED = original_auto_guardian


def test_run_cycle_marks_llm_gate_bypassed_when_gate_payload_has_error() -> None:
    import os

    async def fake_process_candidates(**_kwargs):
        return 0, {"status": "PROPOSED", "reason": None}

    original_process = live_agent.process_candidates
    original_gate_enabled = os.environ.get("EVCLAW_LLM_GATE_ENABLED")
    live_agent.process_candidates = fake_process_candidates
    os.environ["EVCLAW_LLM_GATE_ENABLED"] = "1"
    try:
        with tempfile.TemporaryDirectory() as tmpdir:
            cycle_file = Path(tmpdir) / "cycle.json"
            context_json = Path(tmpdir) / "context.json"
            candidates_file = Path(tmpdir) / "candidates.json"
            cycle_file.write_text(json.dumps({"symbols": {"ETH": {}}}))
            context_json.write_text(json.dumps({"ok": True}))
            candidates_file.write_text(
                json.dumps(
                    {
                        "schema_version": 1,
                        "candidates": [],
                        "llm_gate": {"enabled": False, "error": "gateway timeout"},
                    }
                )
            )

            rc, summary = asyncio.run(
                live_agent.run_cycle(
                    seq=52,
                    cycle_file=str(cycle_file),
                    context_json_file=str(context_json),
                    dry_run=True,
                    risk_pct_lighter=1.0,
                    risk_pct_hyperliquid=1.0,
                    candidates_file=str(candidates_file),
                    reuse_existing=True,
                )
            )
            assert rc == 0
            assert summary.get("llm_gate_config_enabled") is True
            assert summary.get("llm_gate_enabled") is False
            assert summary.get("llm_gate_bypassed") is True
            assert summary.get("llm_gate_bypass_reason") == "gateway timeout"
    finally:
        live_agent.process_candidates = original_process
        if original_gate_enabled is None:
            os.environ.pop("EVCLAW_LLM_GATE_ENABLED", None)
        else:
            os.environ["EVCLAW_LLM_GATE_ENABLED"] = original_gate_enabled


def test_run_cycle_marks_llm_gate_not_bypassed_when_enabled_payload_exists() -> None:
    import os

    async def fake_process_candidates(**_kwargs):
        return 0, {"status": "PROPOSED", "reason": None}

    original_process = live_agent.process_candidates
    original_gate_enabled = os.environ.get("EVCLAW_LLM_GATE_ENABLED")
    live_agent.process_candidates = fake_process_candidates
    os.environ["EVCLAW_LLM_GATE_ENABLED"] = "1"
    try:
        with tempfile.TemporaryDirectory() as tmpdir:
            cycle_file = Path(tmpdir) / "cycle.json"
            context_json = Path(tmpdir) / "context.json"
            candidates_file = Path(tmpdir) / "candidates.json"
            cycle_file.write_text(json.dumps({"symbols": {"ETH": {}}}))
            context_json.write_text(json.dumps({"ok": True}))
            candidates_file.write_text(
                json.dumps(
                    {
                        "schema_version": 1,
                        "candidates": [],
                        "llm_gate": {"enabled": True, "picks": [], "rejects": []},
                    }
                )
            )

            rc, summary = asyncio.run(
                live_agent.run_cycle(
                    seq=53,
                    cycle_file=str(cycle_file),
                    context_json_file=str(context_json),
                    dry_run=True,
                    risk_pct_lighter=1.0,
                    risk_pct_hyperliquid=1.0,
                    candidates_file=str(candidates_file),
                    reuse_existing=True,
                )
            )
            assert rc == 0
            assert summary.get("llm_gate_config_enabled") is True
            assert summary.get("llm_gate_enabled") is True
            assert summary.get("llm_gate_bypassed") is False
            assert summary.get("llm_gate_bypass_reason") is None
    finally:
        live_agent.process_candidates = original_process
        if original_gate_enabled is None:
            os.environ.pop("EVCLAW_LLM_GATE_ENABLED", None)
        else:
            os.environ["EVCLAW_LLM_GATE_ENABLED"] = original_gate_enabled


def test_run_cycle_passes_open_positions_into_llm_gate_exposure_context() -> None:
    import os

    captured: dict = {}

    async def fake_process_candidates(**_kwargs):
        return 0, {"status": "PROPOSED", "reason": None}

    async def fake_run_entry_gate(**kwargs):
        captured["exposure"] = kwargs.get("exposure_context")
        return SimpleNamespace(enabled=True, picks=[], rejects=[], error=None, raw_text="{}")

    original_process = live_agent.process_candidates
    original_run_entry_gate = live_agent.run_entry_gate
    original_build_candidates = live_agent.build_candidates_from_context
    original_gate_enabled = os.environ.get("EVCLAW_LLM_GATE_ENABLED")
    live_agent.process_candidates = fake_process_candidates
    live_agent.run_entry_gate = fake_run_entry_gate
    live_agent.build_candidates_from_context = lambda **_kwargs: [{"symbol": "ETH", "direction": "LONG"}]
    os.environ["EVCLAW_LLM_GATE_ENABLED"] = "1"
    try:
        with tempfile.TemporaryDirectory() as tmpdir:
            db = AITraderDB(str(Path(tmpdir) / "test.db"))
            db.log_trade_entry(
                symbol="ETH",
                direction="LONG",
                entry_price=2000.0,
                size=0.5,
                venue="hyperliquid",
            )

            cycle_file = Path(tmpdir) / "cycle.json"
            context_json = Path(tmpdir) / "context.json"
            candidates_file = Path(tmpdir) / "candidates.json"
            cycle_file.write_text(json.dumps({"symbols": {"ETH": {}}}))
            context_json.write_text(json.dumps({"ok": True}))
            candidates_file.write_text(
                json.dumps(
                    {
                        "schema_version": 1,
                        "global_context_compact": "ctx",
                        "candidates": [{"symbol": "ETH", "direction": "LONG"}],
                    }
                )
            )

            rc, _summary = asyncio.run(
                live_agent.run_cycle(
                    seq=66,
                    cycle_file=str(cycle_file),
                    context_json_file=str(context_json),
                    dry_run=True,
                    risk_pct_lighter=1.0,
                    risk_pct_hyperliquid=1.0,
                    candidates_file=str(candidates_file),
                    reuse_existing=True,
                    db=db,
                )
            )
            assert rc == 0

            exposure = captured.get("exposure") or {}
            open_positions = exposure.get("open_positions") or []
            assert open_positions
            assert open_positions[0].get("symbol") == "ETH"
            assert open_positions[0].get("direction") == "LONG"
            assert open_positions[0].get("venue") == "hyperliquid"
    finally:
        live_agent.process_candidates = original_process
        live_agent.run_entry_gate = original_run_entry_gate
        live_agent.build_candidates_from_context = original_build_candidates
        if original_gate_enabled is None:
            os.environ.pop("EVCLAW_LLM_GATE_ENABLED", None)
        else:
            os.environ["EVCLAW_LLM_GATE_ENABLED"] = original_gate_enabled


def test_context_max_age_default_is_600_seconds() -> None:
    assert float(live_agent.DEFAULT_CONTEXT_MAX_AGE_SECONDS) == 600.0


def test_run_cycle_fails_fast_when_context_is_stale() -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        cycle_file = Path(tmpdir) / "cycle.json"
        context_json = Path(tmpdir) / "context.json"
        cycle_file.write_text(json.dumps({"symbols": {"ETH": {}}}))
        context_json.write_text(
            json.dumps(
                {
                    "generated_at": "2000-01-01T00:00:00Z",
                    "selected_opportunities": [],
                }
            )
        )

        rc, summary = asyncio.run(
            live_agent.run_cycle(
                seq=999,
                cycle_file=str(cycle_file),
                context_json_file=str(context_json),
                dry_run=True,
                risk_pct_lighter=1.0,
                risk_pct_hyperliquid=1.0,
                reuse_existing=False,
                db=None,
            )
        )

    assert rc == 1
    assert summary.get("reason") == "context_stale"
    assert float(summary.get("context_max_age_sec") or 0.0) == 600.0
    assert float(summary.get("context_age_sec") or 0.0) > 600.0


def test_apply_llm_gate_selection_uses_deterministic_blended_thresholds() -> None:
    blocked = []

    def fake_block_candidate(**kwargs):
        blocked.append(str(kwargs.get("reason") or ""))
        summary = kwargs.get("summary") or {}
        counts = summary.setdefault("counts", {})
        counts["blocked"] = int(counts.get("blocked", 0)) + 1

    original_block = live_agent._block_candidate
    try:
        live_agent._block_candidate = fake_block_candidate
        summary = live_agent._build_process_summary(101)
        valid_candidates = [
            {"symbol": "BTC", "direction": "LONG", "blended_conviction": 0.70},
            {"symbol": "ETH", "direction": "SHORT", "blended_conviction": 0.30},
            {"symbol": "SOL", "direction": "LONG", "blended_conviction": 0.20},
        ]
        payload = {
            "llm_gate": {
                "enabled": True,
                "picks": [
                    {"symbol": "BTC", "direction": "LONG", "reason": "A"},
                    {"symbol": "ETH", "direction": "SHORT", "reason": "B"},
                    {"symbol": "SOL", "direction": "LONG", "reason": "C"},
                ],
                "rejects": [],
            }
        }
        kept, terminal = live_agent._apply_llm_gate_selection(
            payload=payload,
            db=None,
            summary=summary,
            seq=101,
            active_venues=["hyperliquid"],
            valid_candidates=valid_candidates,
            invalid_candidates=[],
        )
        assert terminal is False
        assert len(kept) == 2
        by_symbol = {str(c.get("symbol")): c for c in kept}
        assert by_symbol["BTC"]["execution"]["order_type"] == "chase_limit"
        assert by_symbol["ETH"]["execution"]["order_type"] == "limit"
        assert by_symbol["BTC"]["execution"]["conviction_source"] == "blended"
        assert by_symbol["ETH"]["execution"]["conviction_source"] == "blended"
        assert any("blended_conviction_below_min:0.2000" in reason for reason in blocked)
    finally:
        live_agent._block_candidate = original_block


def test_runtime_conviction_config_uses_active_db_snapshot() -> None:
    class _DB:
        def get_active_conviction_config(self):
            return {"params": {"limit_min": 0.25, "chase_threshold": 0.72}}

    cfg = live_agent._runtime_conviction_config(_DB(), ttl_seconds=0.0)
    assert cfg.limit_min == 0.25
    assert cfg.chase_threshold == 0.72


def test_apply_llm_gate_selection_uses_runtime_config_thresholds() -> None:
    summary = live_agent._build_process_summary(202)
    payload = {
        "llm_gate": {
            "enabled": True,
            "picks": [
                {"symbol": "BTC", "direction": "LONG", "reason": "A"},
            ],
            "rejects": [],
        }
    }
    kept, terminal = live_agent._apply_llm_gate_selection(
        payload=payload,
        db=None,
        summary=summary,
        seq=202,
        active_venues=["hyperliquid"],
        valid_candidates=[{"symbol": "BTC", "direction": "LONG", "blended_conviction": 0.71}],
        invalid_candidates=[],
        conviction_config=ConvictionConfig(chase_threshold=0.75, limit_min=0.2),
    )
    assert terminal is False
    assert kept[0]["execution"]["order_type"] == "limit"


def test_apply_llm_gate_selection_persists_gate_decisions() -> None:
    inserted = []

    class _DB:
        def insert_gate_decision(self, **kwargs):
            inserted.append(kwargs)
            return len(inserted)

    blocked = []

    def fake_block_candidate(**kwargs):
        blocked.append(str(kwargs.get("reason") or ""))
        summary = kwargs.get("summary") or {}
        counts = summary.setdefault("counts", {})
        counts["blocked"] = int(counts.get("blocked", 0)) + 1

    original_block = live_agent._block_candidate
    try:
        live_agent._block_candidate = fake_block_candidate
        summary = live_agent._build_process_summary(303)
        payload = {
            "llm_gate": {
                "enabled": True,
                "agent_id": "hl-entry-gate",
                "model": "openai-codex/gpt-5.2",
                "session_id": "hl_entry_gate_303",
                "picks": [
                    {"symbol": "BTC", "direction": "LONG", "reason": "A", "gate_mode": "normal"},
                ],
                "rejects": [
                    {"symbol": "ETH", "direction": "SHORT", "reason": "B"},
                ],
            }
        }
        kept, terminal = live_agent._apply_llm_gate_selection(
            payload=payload,
            db=_DB(),
            summary=summary,
            seq=303,
            active_venues=["hyperliquid"],
            valid_candidates=[
                {"symbol": "BTC", "direction": "LONG", "blended_conviction": 0.71, "rank": 1},
                {"symbol": "ETH", "direction": "SHORT", "blended_conviction": 0.62, "rank": 2},
            ],
            invalid_candidates=[],
            conviction_config=ConvictionConfig(chase_threshold=0.75, limit_min=0.2),
        )
        assert terminal is False
        assert len(kept) == 1
        assert len(inserted) == 2
        pick = next(x for x in inserted if x.get("decision") == "PICK")
        rej = next(x for x in inserted if x.get("decision") == "REJECT")
        assert pick["symbol"] == "BTC"
        assert rej["symbol"] == "ETH"
        assert pick["llm_agent_id"] == "hl-entry-gate"
        assert pick["llm_model"] == "openai-codex/gpt-5.2"
        assert pick["llm_session_id"] == "hl_entry_gate_303"
        assert kept[0]["gate_decision_id"] == 1
        assert kept[0]["gate_session_id"] == "hl_entry_gate_303"
        assert kept[0]["gate_decision_reason"] == "A"
        assert kept[0]["gate_mode"] == "normal"
        assert kept[0]["entry_gate_mode"] == "normal"
        assert kept[0]["execution"]["gate_mode"] == "normal"
    finally:
        live_agent._block_candidate = original_block


def test_apply_llm_gate_selection_marks_invalid_schema_fallback() -> None:
    summary = live_agent._build_process_summary(404)
    payload = {
        "llm_gate": {
            "enabled": True,
            "picks": "not-a-list",
            "rejects": [],
        }
    }
    candidates = [{"symbol": "BTC", "direction": "LONG", "blended_conviction": 0.71}]
    kept, terminal = live_agent._apply_llm_gate_selection(
        payload=payload,
        db=None,
        summary=summary,
        seq=404,
        active_venues=["hyperliquid"],
        valid_candidates=candidates,
        invalid_candidates=[],
    )
    assert terminal is False
    assert kept == candidates
    assert summary["llm_gate"]["status"] == "INVALID_SCHEMA"
    assert summary["llm_gate"]["fallback_mode"] == "deterministic_policy"
    assert summary["llm_gate"]["fallback_reason"] == "picks_rejects_not_lists"


def test_apply_llm_gate_selection_marks_coverage_mismatch_fallback() -> None:
    summary = live_agent._build_process_summary(405)
    payload = {
        "llm_gate": {
            "enabled": True,
            "picks": [{"symbol": "BTC", "direction": "LONG", "reason": "only one"}],
            "rejects": [],
        }
    }
    candidates = [
        {"symbol": "BTC", "direction": "LONG", "blended_conviction": 0.71},
        {"symbol": "ETH", "direction": "SHORT", "blended_conviction": 0.61},
    ]
    kept, terminal = live_agent._apply_llm_gate_selection(
        payload=payload,
        db=None,
        summary=summary,
        seq=405,
        active_venues=["hyperliquid"],
        valid_candidates=candidates,
        invalid_candidates=[],
    )
    assert terminal is False
    assert kept == candidates
    assert summary["llm_gate"]["status"] == "INVALID_COVERAGE"
    assert summary["llm_gate"]["fallback_mode"] == "deterministic_policy"
    assert summary["llm_gate"]["fallback_reason"] == "coverage_mismatch"


def test_adaptive_candidate_limit_uses_min_for_weak_cycle(monkeypatch) -> None:
    monkeypatch.setattr(live_agent, "CANDIDATE_TOPK_MIN", 2)
    monkeypatch.setattr(live_agent, "CANDIDATE_TOPK_MAX", 5)
    monkeypatch.setattr(live_agent, "CANDIDATE_TOPK_SCORE_GATE", 70.0)

    cands = [
        {"symbol": "A1", "conviction": 0.40, "context_snapshot": {"score": 61}},
        {"symbol": "A2", "conviction": 0.35, "context_snapshot": {"score": 55}},
        {"symbol": "A3", "conviction": 0.45, "context_snapshot": {"score": 58}},
    ]
    assert live_agent._adaptive_candidate_limit(cands) == 2


def test_adaptive_candidate_limit_expands_with_quality(monkeypatch) -> None:
    monkeypatch.setattr(live_agent, "CANDIDATE_TOPK_MIN", 2)
    monkeypatch.setattr(live_agent, "CANDIDATE_TOPK_MAX", 5)
    monkeypatch.setattr(live_agent, "CANDIDATE_TOPK_SCORE_GATE", 70.0)

    cands = [
        {"symbol": "B1", "conviction": 0.55, "context_snapshot": {"score": 71}},
        {"symbol": "B2", "conviction": 0.75, "context_snapshot": {"score": 40}},
        {"symbol": "B3", "conviction": 0.30, "context_snapshot": {"score": 82}},
        {"symbol": "B4", "conviction": 0.20, "context_snapshot": {"score": 51}},
    ]
    assert live_agent._adaptive_candidate_limit(cands) == 3


def test_adaptive_candidate_limit_keeps_strong_floor(monkeypatch) -> None:
    monkeypatch.setattr(live_agent, "CANDIDATE_TOPK_MIN", 1)
    monkeypatch.setattr(live_agent, "CANDIDATE_TOPK_MAX", 2)
    monkeypatch.setattr(live_agent, "CANDIDATE_TOPK_SCORE_GATE", 90.0)

    cands = [
        {"symbol": "C1", "must_trade": True, "conviction": 0.30, "context_snapshot": {"score": 10}},
        {"symbol": "C2", "strong_signals": ["whale"], "conviction": 0.20, "context_snapshot": {"score": 15}},
        {"symbol": "C3", "must_trade": True, "conviction": 0.25, "context_snapshot": {"score": 12}},
        {"symbol": "C4", "conviction": 0.10, "context_snapshot": {"score": 8}},
    ]
    assert live_agent._adaptive_candidate_limit(cands) == 3


def test_run_command_does_not_clear_pending_on_validation_failure() -> None:
    calls = {"clear": 0, "notify": 0}

    def fake_load_pending():
        return {"seq": 7, "context_json_file": "/tmp/ctx.json"}

    def fake_clear_pending():
        calls["clear"] += 1

    def fake_set_last_notified(_seq):
        calls["notify"] += 1

    original_load_pending = live_agent.load_pending
    original_clear_pending = live_agent.clear_pending
    original_set_last_notified = live_agent.set_last_notified
    original_resolve_db_path = live_agent._resolve_db_path
    original_load_pct = live_agent.load_pct_24h_history_from_trades
    live_agent.load_pending = fake_load_pending
    live_agent.clear_pending = fake_clear_pending
    live_agent.set_last_notified = fake_set_last_notified
    live_agent._resolve_db_path = lambda _dry_run: "/tmp/fake.db"
    live_agent.load_pct_24h_history_from_trades = lambda *_args, **_kwargs: None
    try:
        args = Namespace(
            command="run",
            from_db=False,
            loop=False,
            continuous=False,
            from_pending=True,
            dry_run=True,
            seq=None,
            cycle_file=None,
            context_file=None,
            context_json_file=None,
            output=None,
            risk_pct_lighter=1.0,
            risk_pct_hyperliquid=1.0,
            use_llm=False,
            llm_model=None,
        )
        rc = asyncio.run(live_agent.run_command(args))
        assert rc == 1
        assert calls["clear"] == 0
        assert calls["notify"] == 0
    finally:
        live_agent.load_pending = original_load_pending
        live_agent.clear_pending = original_clear_pending
        live_agent.set_last_notified = original_set_last_notified
        live_agent._resolve_db_path = original_resolve_db_path
        live_agent.load_pct_24h_history_from_trades = original_load_pct


def test_get_hl_equity_for_account_wallet_uses_unified_equity() -> None:
    class DummyHLAdapter:
        _address = "0xabc"
        _hip3_address = "0xabc"

        async def _post_public(self, payload):
            typ = str((payload or {}).get("type") or "")
            if typ == "clearinghouseState":
                dex = str((payload or {}).get("dex") or "").lower()
                if not dex:
                    return {"marginSummary": {"accountValue": 0.0}}
                if dex == "xyz":
                    return {"marginSummary": {"accountValue": 1200.0}}
                if dex == "cash":
                    return {"marginSummary": {"accountValue": 800.0}}
                return {"marginSummary": {"accountValue": 0.0}}
            if typ == "spotClearinghouseState":
                return {
                    "balances": [
                        {"coin": "USDC", "total": 500.0, "hold": 100.0},
                        {"coin": "BTC", "total": 1.0, "hold": 0.0},
                    ]
                }
            return {}

        def _builder_dexes(self):
            return ["xyz", "cash"]

    class DummyExecutor:
        def __init__(self):
            self.hyperliquid = DummyHLAdapter()
            self.hip3_wallet = SimpleNamespace(
                _initialized=True,
                _address="0xabc",
                _hip3_address="0xabc",
                _post_public=self.hyperliquid._post_public,
                _builder_dexes=self.hyperliquid._builder_dexes,
            )

    equity, source = asyncio.run(
        live_agent.get_hl_equity_for_account(
            executor=DummyExecutor(),
            use_wallet=True,
            fallback=0.0,
            dry_run=False,
        )
    )
    # 1200 + 800 + (500 - 100)
    assert equity == 2400.0
    assert source == "wallet_unified"


def test_run_command_uses_resolved_db_for_history_warmup() -> None:
    observed = {"path": None}

    def fake_resolve_db_path(_dry_run):
        return "/tmp/custom_history.db"

    def fake_load_pct(path, days=7):
        observed["path"] = path
        observed["days"] = days

    original_resolve_db_path = live_agent._resolve_db_path
    original_load_pct = live_agent.load_pct_24h_history_from_trades
    live_agent._resolve_db_path = fake_resolve_db_path
    live_agent.load_pct_24h_history_from_trades = fake_load_pct
    try:
        args = Namespace(
            command="execute",
            from_pending=False,
            dry_run=True,
            seq=None,
            cycle_file=None,
            candidates_file=None,
            risk_pct_lighter=1.0,
            risk_pct_hyperliquid=1.0,
        )
        rc = asyncio.run(live_agent.run_command(args))
        assert rc == 1
        assert observed["path"] == "/tmp/custom_history.db"
    finally:
        live_agent._resolve_db_path = original_resolve_db_path
        live_agent.load_pct_24h_history_from_trades = original_load_pct


def test_run_command_clears_pending_after_validation() -> None:
    calls = {"clear": 0, "notify": 0}

    def fake_load_pending():
        return {
            "seq": 11,
            "cycle_file": "/tmp/cycle.json",
            "context_file": "/tmp/context.txt",
            "context_json_file": "/tmp/context.json",
        }

    def fake_clear_pending():
        calls["clear"] += 1

    def fake_set_last_notified(_seq):
        calls["notify"] += 1

    async def fake_run_cycle(**_kwargs):
        return 0, {"status": "PROPOSED"}

    original_load_pending = live_agent.load_pending
    original_clear_pending = live_agent.clear_pending
    original_set_last_notified = live_agent.set_last_notified
    original_resolve_db_path = live_agent._resolve_db_path
    original_load_pct = live_agent.load_pct_24h_history_from_trades
    original_run_cycle = live_agent.run_cycle
    live_agent.load_pending = fake_load_pending
    live_agent.clear_pending = fake_clear_pending
    live_agent.set_last_notified = fake_set_last_notified
    live_agent._resolve_db_path = lambda _dry_run: "/tmp/fake.db"
    live_agent.load_pct_24h_history_from_trades = lambda *_args, **_kwargs: None
    live_agent.run_cycle = fake_run_cycle
    try:
        args = Namespace(
            command="run",
            from_db=False,
            loop=False,
            continuous=False,
            from_pending=True,
            dry_run=True,
            seq=None,
            cycle_file=None,
            context_file=None,
            context_json_file=None,
            output=None,
            risk_pct_lighter=1.0,
            risk_pct_hyperliquid=1.0,
            use_llm=False,
            llm_model=None,
        )
        rc = asyncio.run(live_agent.run_command(args))
        assert rc == 0
        assert calls["clear"] == 1
        assert calls["notify"] == 1
    finally:
        live_agent.load_pending = original_load_pending
        live_agent.clear_pending = original_clear_pending
        live_agent.set_last_notified = original_set_last_notified
        live_agent._resolve_db_path = original_resolve_db_path
        live_agent.load_pct_24h_history_from_trades = original_load_pct
        live_agent.run_cycle = original_run_cycle

def test_validate_candidates_payload_valid() -> None:
    payload = {
        "schema_version": 1,
        "cycle_seq": 123,
        "context_file": "/tmp/x.txt",
        "generated_at": "2026-01-01T00:00:00Z",
        "candidates": [
            {
                "symbol": "eth",
                "direction": "LONG",
                "size_usd": 50,
                "conviction": 0.7,
                "reason_short": "test",
                "signals": ["cvd"],
            }
        ],
        "notes": "",
        "errors": [],
    }
    result = validate_candidates_payload(payload)
    assert len(result["errors"]) == 0
    assert len(result["valid"]) == 1
    assert result["valid"][0]["symbol"] == "ETH"


def test_validate_candidates_payload_invalid() -> None:
    payload = {
        "schema_version": 1,
        "candidates": [
            {"direction": "SIDEWAYS", "size_usd": -5, "conviction": 2, "signals": "bad"}
        ],
    }
    result = validate_candidates_payload(payload)
    assert len(result["valid"]) == 0
    assert len(result["invalid"]) == 1


def test_validate_candidates_payload_without_size() -> None:
    payload = {
        "schema_version": 1,
        "candidates": [
            {
                "symbol": "btc",
                "direction": "SHORT",
                "conviction": 0.6,
                "reason_short": "ok",
                "signals": ["cvd"],
            }
        ],
    }
    result = validate_candidates_payload(payload)
    assert len(result["errors"]) == 0
    assert len(result["valid"]) == 1


def test_enforce_max_candidates() -> None:
    candidates = [{"symbol": f"S{i}", "direction": "LONG", "size_usd": 1, "conviction": 0.5, "reason_short": "x", "signals": []} for i in range(4)]
    kept, dropped = enforce_max_candidates(candidates, 3)
    assert len(kept) == 3
    assert len(dropped) == 1


def test_agi_records_proposals() -> None:
    class DummyExecutor:
        def __init__(self, config, *_, **__):
            self.config = config
            self.hyperliquid = object()
            self.lighter = object()
            self.router = type("R", (), {"validate": lambda *a, **k: None})()

        async def initialize(self) -> bool:
            return True

        async def close(self) -> None:
            return None

        def get_total_exposure(self) -> float:
            return 0.0

        async def get_all_positions(self):
            return {}

    async def dummy_check_positions_reconciled(_executor, _db, allowed_venues=None):
        return True, ""

    async def dummy_check_symbol_on_venues(_symbol, _executor, _venues):
        return {"ok": True, "hl_mid": 100.0, "lighter_mid": 100.0}

    def dummy_check_fill_reconciler(_venues, max_age: int = 90):
        return True, ""

    def dummy_load_config():
        return {
            "config": {
                "executor": {
                    "use_fill_reconciler": False,
                    "lighter_enabled": True,
                    "hl_enabled": True,
                    "hl_wallet_enabled": False,
                    "hl_mirror_wallet": False,
                    "default_venue_perps": "hyperliquid",
                },
                "risk": {"starting_equity": 10000.0},
            }
        }

    import os

    original_executor = live_agent.Executor
    original_check_positions = live_agent.check_positions_reconciled
    original_check_symbol = live_agent.check_symbol_on_venues
    original_check_fill = live_agent.check_fill_reconciler
    original_get_equity = live_agent.get_equity_for_venue
    original_load_config = live_agent.load_config

    live_agent.Executor = DummyExecutor
    live_agent.check_positions_reconciled = dummy_check_positions_reconciled
    live_agent.check_symbol_on_venues = dummy_check_symbol_on_venues
    live_agent.check_fill_reconciler = dummy_check_fill_reconciler

    async def dummy_get_equity_for_venue(*_args, **_kwargs):
        return 10000.0, "unit_test"

    live_agent.get_equity_for_venue = dummy_get_equity_for_venue
    live_agent.load_config = dummy_load_config
    try:
        with tempfile.TemporaryDirectory() as tmpdir:
            cycle_path = Path(tmpdir) / "cycle.json"
            candidates_path = Path(tmpdir) / "candidates.json"
            db_path = Path(tmpdir) / "test.db"

            cycle_data = {
                "symbols": {
                    "ETH": {
                        "price": 2000,
                        "best_bid": 1990,
                        "best_ask": 2010,
                        "atr_pct": 2.0,
                    }
                }
            }
            cycle_path.write_text(json.dumps(cycle_data))

            candidates_data = {
                "schema_version": 1,
                "cycle_seq": 999,
                "context_file": "/tmp/context.txt",
                "generated_at": "2026-01-01T00:00:00Z",
                "candidates": [
                    {
                        "symbol": "ETH",
                        "direction": "LONG",
                        "conviction": 0.7,
                        "reason_short": "unit-test",
                        "signals": ["cvd"],
                    }
                ],
                "notes": "",
                "errors": [],
            }
            candidates_path.write_text(json.dumps(candidates_data))

            rc, summary = asyncio.run(
                live_agent.process_candidates(
                    seq=999,
                    cycle_file=str(cycle_path),
                    candidates_file=str(candidates_path),
                    dry_run=True,
                    risk_pct_lighter=1.0,
                    risk_pct_hyperliquid=1.0,
                    db_path_override=str(db_path),
                )
            )

            assert rc == 0
            assert summary["status"] == "PROPOSED"

            db = AITraderDB(str(db_path))
            proposals = db.get_proposals_for_cycle(999)
            assert len(proposals) == 1
            for proposal in proposals:
                assert proposal["status"] == "PROPOSED"
                assert float(proposal["size_usd"] or 0.0) > 0.0

            with sqlite3.connect(str(db_path)) as conn:
                exec_count = conn.execute(
                    "SELECT COUNT(*) FROM proposal_executions"
                ).fetchone()[0]
            assert exec_count == 0
    finally:
        live_agent.Executor = original_executor
        live_agent.check_positions_reconciled = original_check_positions
        live_agent.check_symbol_on_venues = original_check_symbol
        live_agent.check_fill_reconciler = original_check_fill
        live_agent.get_equity_for_venue = original_get_equity
        live_agent.load_config = original_load_config


if __name__ == "__main__":
    test_validate_candidates_payload_valid()
    test_validate_candidates_payload_invalid()
    test_enforce_max_candidates()
    test_agi_records_proposals()
    print("[PASS] live agent utils")
