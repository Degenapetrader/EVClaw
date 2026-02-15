#!/usr/bin/env python3
"""Tests for live agent DB queue and size caps."""

import asyncio
import sys
import tempfile
import time
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import live_agent
from ai_trader_db import AITraderDB
from live_agent import build_deps, _build_executor_with_learning
from live_agent_utils import cap_size_usd


def _fetch_cycle_run(db: AITraderDB, seq: int):
    with db._get_connection() as conn:
        row = conn.execute("SELECT * FROM cycle_runs WHERE seq = ?", (int(seq),)).fetchone()
    return dict(row) if row else None


def test_cycle_claim_order_and_process() -> None:
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    db = AITraderDB(db_path)
    now = time.time()
    db.insert_cycle_run(1, cycle_file="cycle1.json", context_file="ctx1.txt", timestamp=now)
    db.insert_cycle_run(2, cycle_file="cycle2.json", context_file="ctx2.txt", timestamp=now)
    db.insert_cycle_run(3, cycle_file="cycle3.json", context_file="ctx3.txt", timestamp=now)

    claim = db.claim_next_cycle("worker1", stale_after_seconds=9999)
    assert claim is not None
    assert claim["seq"] == 3

    assert db.mark_cycle_processed(3, "SUCCESS", summary="{}", processed_by="worker1")
    row = _fetch_cycle_run(db, 3)
    assert row["processed_at"] is not None

    claim2 = db.claim_next_cycle("worker1", stale_after_seconds=9999)
    assert claim2 is not None
    assert claim2["seq"] == 2
    assert db.mark_cycle_processed(2, "SUCCESS", summary="{}", processed_by="worker1")

    claim3 = db.claim_next_cycle("worker1", stale_after_seconds=9999)
    assert claim3 is not None
    assert claim3["seq"] == 1
    assert db.mark_cycle_processed(1, "SUCCESS", summary="{}", processed_by="worker1")

    claim4 = db.claim_next_cycle("worker1", stale_after_seconds=9999)
    assert claim4 is None


def test_claim_idempotent() -> None:
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    db = AITraderDB(db_path)
    db.insert_cycle_run(10, cycle_file="cycle10.json", context_file="ctx10.txt", timestamp=time.time())

    claim = db.claim_next_cycle("workerA", stale_after_seconds=9999)
    assert claim is not None
    assert claim["seq"] == 10

    claim2 = db.claim_next_cycle("workerB", stale_after_seconds=9999)
    assert claim2 is None


def test_retryable_failure_keeps_cycle_unprocessed_and_reclaimable() -> None:
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    db = AITraderDB(db_path)
    db.insert_cycle_run(42, cycle_file="cycle42.json", context_file="ctx42.txt", timestamp=time.time())

    claim = db.claim_next_cycle("workerA", stale_after_seconds=10.0)
    assert claim is not None and claim["seq"] == 42

    ok = db.mark_cycle_retryable_failure(
        seq=42,
        status="FAILED",
        summary='{"reason":"transient"}',
        processed_by="workerA",
        error="transient",
        stale_after_seconds=10.0,
        retry_delay_seconds=0.2,
    )
    assert ok is True

    row = _fetch_cycle_run(db, 42)
    assert row is not None
    assert row["processed_at"] is None
    assert row["processed_status"] == "FAILED"

    immediate = db.claim_next_cycle("workerB", stale_after_seconds=10.0)
    assert immediate is None

    time.sleep(0.25)
    retry = db.claim_next_cycle("workerB", stale_after_seconds=10.0)
    assert retry is not None
    assert retry["seq"] == 42


def test_loop_mode_executor_wiring_includes_adaptive_and_tracker() -> None:
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    deps = build_deps(dry_run=True, db_path_override=db_path)
    executor = asyncio.run(_build_executor_with_learning(deps.exec_config, deps.tracker))

    assert executor.adaptive_sltp is not None
    assert executor.trade_tracker is deps.tracker


def test_resolve_sqlite_db_path_falls_back_to_default_db() -> None:
    resolved = live_agent._resolve_sqlite_db_path(SimpleNamespace())
    assert resolved == live_agent.DEFAULT_DB


def test_get_equity_for_venue_hl_uses_http_state_fetch(monkeypatch) -> None:
    class DummyDB:
        def get_latest_equity(self, _venue):
            return None

    class DummyHLAdapter:
        _vault_address = "0xabc"
        _address = "0xdef"

        async def _post_public(self, _payload):  # pragma: no cover - should not be used here
            raise AssertionError("_post_public should not be called by get_equity_for_venue")

    class DummyExecutor:
        hyperliquid = DummyHLAdapter()
        hip3_wallet = None
        lighter = None

    async def _fake_state(_address: str, _base_url: str):
        return {"marginSummary": {"accountValue": "123.45"}}

    monkeypatch.setattr(live_agent, "_fetch_hl_state_http", _fake_state)
    equity, source = asyncio.run(
        live_agent.get_equity_for_venue(
            db=DummyDB(),
            executor=DummyExecutor(),
            venue=live_agent.VENUE_HYPERLIQUID,
            fallback=50.0,
            dry_run=False,
        )
    )
    assert equity == 123.45
    assert source == "adapter"


def test_get_equity_for_venue_lighter_ignores_collateral_only() -> None:
    class DummyDB:
        def get_latest_equity(self, _venue):
            return None

    class AccountResponse:
        accounts = [SimpleNamespace(collateral=777.0)]

    class AccountAPI:
        async def account(self, by: str, value: str):
            assert by == "index"
            assert value == "1"
            return AccountResponse()

    class DummyLighterAdapter:
        _account_api = AccountAPI()
        _account_index = 1

    class DummyExecutor:
        lighter = DummyLighterAdapter()
        hyperliquid = SimpleNamespace()
        hip3_wallet = None

    equity, source = asyncio.run(
        live_agent.get_equity_for_venue(
            db=DummyDB(),
            executor=DummyExecutor(),
            venue=live_agent.VENUE_LIGHTER,
            fallback=55.0,
            dry_run=False,
        )
    )
    assert equity == 55.0
    assert source == "fallback"


def test_run_command_from_pending_invalid_seq_is_safe(monkeypatch, capsys) -> None:
    calls = {"cleared": 0, "notified": []}

    monkeypatch.setattr(
        live_agent,
        "load_pending",
        lambda: {
            "cycle_file": "/tmp/cycle.json",
            "context_file": "/tmp/context.txt",
            "context_json_file": "/tmp/context.json",
        },
    )

    def _clear():
        calls["cleared"] += 1

    def _notify(seq: int):
        calls["notified"].append(seq)

    monkeypatch.setattr(live_agent, "clear_pending", _clear)
    monkeypatch.setattr(live_agent, "set_last_notified", _notify)

    args = SimpleNamespace(command="execute", from_pending=True, seq=None)
    rc = asyncio.run(live_agent.run_command(args))
    assert rc == 1
    assert calls["cleared"] == 0
    assert calls["notified"] == []
    out = capsys.readouterr().out.lower()
    assert "invalid seq" in out


def test_size_cap_clamp() -> None:
    cap = min(500.0, 1000.0)
    size, clamped = cap_size_usd(1200.0, cap)
    assert clamped is True
    assert size == 500.0

    size2, clamped2 = cap_size_usd(400.0, cap)
    assert clamped2 is False
    assert size2 == 400.0

    cap2 = min(500.0, 1000.0, 50.0)
    size3, clamped3 = cap_size_usd(120.0, cap2)
    assert clamped3 is True
    assert size3 == 50.0


if __name__ == "__main__":
    test_cycle_claim_order_and_process()
    test_claim_idempotent()
    test_size_cap_clamp()
    print("[PASS] live agent queue + size caps")
