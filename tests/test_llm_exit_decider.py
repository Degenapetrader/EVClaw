#!/usr/bin/env python3
"""LLM exit decider utility regressions."""

import asyncio
import datetime
import json
import os
import sqlite3
import sys
import tempfile
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from ai_trader_db import AITraderDB
from llm_exit_decider import (
    _build_plan_payloads,
    _agent_audit_payload,
    _already_processed,
    _compute_next_cursor,
    _decision_backoff_remaining,
    _effective_max_actions,
    _fetch_pending_plans,
    _load_cursor_state,
    _load_latest_context_compact,
    _load_exposure_context,
    _normalize_batch_decision,
    _run_cli_close,
    _run_loop_once,
    _utc_now,
    _load_trade_cache_for_ids,
    ExitRuntimeConfig,
    PlanRow,
)


def test_utc_now_returns_parseable_utc_string() -> None:
    s = _utc_now()
    assert s.endswith(" UTC")
    parsed = datetime.datetime.strptime(s, "%Y-%m-%d %H:%M:%S UTC")
    assert parsed.tzinfo is None


def test_already_processed_ignores_detail_without_source_plan_id() -> None:
    conn = sqlite3.connect(":memory:")
    conn.execute(
        """
        CREATE TABLE decay_decisions (
            id INTEGER PRIMARY KEY,
            ts REAL,
            source TEXT,
            source_plan_id INTEGER,
            detail TEXT
        )
        """
    )
    conn.execute(
        "INSERT INTO decay_decisions (ts, source, source_plan_id, detail) VALUES (1.0, 'hl_exit_decider', NULL, 'plan_id=123 action=CLOSE')"
    )
    conn.commit()
    assert _already_processed(conn, 123) is False


def test_already_processed_uses_source_plan_id_exact_match() -> None:
    conn = sqlite3.connect(":memory:")
    conn.execute(
        """
        CREATE TABLE decay_decisions (
            id INTEGER PRIMARY KEY,
            ts REAL,
            source TEXT,
            source_plan_id INTEGER,
            detail TEXT
        )
        """
    )
    conn.execute(
        "INSERT INTO decay_decisions (ts, source, source_plan_id, detail) VALUES (?, ?, ?, ?)",
        (1.0, "hl_exit_decider_fail", 456, "noise"),
    )
    conn.commit()
    assert _already_processed(conn, 456) is True
    assert _already_processed(conn, 457) is False


def test_compute_next_cursor_keeps_unresolved_plan_revisitable() -> None:
    plans = [
        PlanRow(id=11, ts=1.0, symbol="ETH", venue="hyperliquid", trade_id=1, reason="DECAY_EXIT", detail="", source="decay_worker_notify"),
        PlanRow(id=12, ts=2.0, symbol="BTC", venue="hyperliquid", trade_id=2, reason="DECAY_EXIT", detail="", source="decay_worker_notify"),
        PlanRow(id=13, ts=3.0, symbol="SOL", venue="hyperliquid", trade_id=3, reason="DECAY_EXIT", detail="", source="decay_worker_notify"),
    ]
    next_cursor = _compute_next_cursor(last_id=10, plans=plans, resolved_ids={11, 13})
    assert next_cursor == 11  # unresolved id=12 stays visible next poll

    advanced = _compute_next_cursor(last_id=10, plans=plans, resolved_ids={11, 12, 13})
    assert advanced == 13


def test_run_cli_close_times_out_and_kills_process() -> None:
    import llm_exit_decider as mod

    class DummyProc:
        def __init__(self):
            self.returncode = None
            self.killed = False

        async def communicate(self):
            await asyncio.sleep(3600)
            return b"", b""

        def kill(self):
            self.killed = True

        async def wait(self):
            self.returncode = -9
            return self.returncode

    proc_holder = {"proc": None}

    async def fake_create_subprocess_exec(*_args, **_kwargs):
        proc_holder["proc"] = DummyProc()
        return proc_holder["proc"]

    original_create = mod.asyncio.create_subprocess_exec
    mod.asyncio.create_subprocess_exec = fake_create_subprocess_exec
    try:
        ok, msg = asyncio.run(
            _run_cli_close(
                symbol="ETH",
                venue="hyperliquid",
                reason="DECAY_EXIT",
                detail="x",
                timeout_sec=0.01,
            )
        )
    finally:
        mod.asyncio.create_subprocess_exec = original_create

    assert ok is False
    assert "close_timeout_after" in msg
    assert proc_holder["proc"] is not None and proc_holder["proc"].killed is True


def test_agent_audit_payload_redacts_raw_text_by_default() -> None:
    payload = _agent_audit_payload("agent-x", "sess-1", "secret strategy text")
    assert payload["agent_id"] == "agent-x"
    assert payload["session_id"] == "sess-1"
    assert payload["raw_text_len"] == len("secret strategy text")
    assert isinstance(payload.get("raw_text_sha256"), str)
    assert "raw_text" not in payload


def test_load_exposure_context_skips_stale_snapshot() -> None:
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE monitor_snapshots (
                id INTEGER PRIMARY KEY,
                ts REAL,
                ts_iso TEXT,
                hl_equity REAL,
                hl_net_notional REAL
            )
            """
        )
        old_ts = time.time() - 7200.0
        conn.execute(
            "INSERT INTO monitor_snapshots (ts, ts_iso, hl_equity, hl_net_notional) VALUES (?, ?, ?, ?)",
            (old_ts, "old", 1000.0, 50.0),
        )
        conn.commit()

    assert _load_exposure_context(db_path, max_age_sec=300.0) == {}
    with sqlite3.connect(db_path) as conn:
        fresh_ts = time.time()
        conn.execute(
            "INSERT INTO monitor_snapshots (ts, ts_iso, hl_equity, hl_net_notional) VALUES (?, ?, ?, ?)",
            (fresh_ts, "fresh", 1200.0, -25.0),
        )
        conn.commit()
    ctx = _load_exposure_context(db_path, max_age_sec=300.0)
    assert float(ctx.get("hl_equity_usd") or 0.0) == 1200.0


def test_normalize_batch_decision_rejects_overlap_and_duplicates() -> None:
    decision_overlap = {
        "closes": [{"plan_id": 101, "reason": "close fast"}],
        "holds": [{"plan_id": 101, "reason": "hold too"}],
    }
    ok, closes, holds = _normalize_batch_decision(decision_overlap, input_ids={101}, max_close=1)
    assert ok is False
    assert closes == {}
    assert holds == {}

    decision_duplicate = {
        "closes": [
            {"plan_id": 101, "reason": "close fast"},
            {"plan_id": 101, "reason": "close again"},
        ],
        "holds": [],
    }
    ok, closes, holds = _normalize_batch_decision(decision_duplicate, input_ids={101}, max_close=1)
    assert ok is False
    assert closes == {}
    assert holds == {}


def test_normalize_batch_decision_respects_zero_max_close() -> None:
    decision_with_close = {
        "closes": [{"plan_id": 101, "reason": "close now"}],
        "holds": [{"plan_id": 102, "reason": "hold"}],
    }
    ok, closes, holds = _normalize_batch_decision(
        decision_with_close,
        input_ids={101, 102},
        max_close=0,
    )
    assert ok is False
    assert closes == {}
    assert holds == {}

    hold_only = {
        "closes": [],
        "holds": [
            {"plan_id": 101, "reason": "hold now"},
            {"plan_id": 102, "reason": "hold"},
        ],
    }
    ok, closes, holds = _normalize_batch_decision(hold_only, input_ids={101, 102}, max_close=0)
    assert ok is True
    assert closes == {}
    assert holds[101][0] == "hold now"
    assert holds[102][0] == "hold"


def test_fetch_pending_plans_accepts_producer_close_intent_rows() -> None:
    conn = sqlite3.connect(":memory:")
    conn.execute(
        """
        CREATE TABLE decay_decisions (
            id INTEGER PRIMARY KEY,
            ts REAL,
            symbol TEXT,
            venue TEXT,
            trade_id INTEGER,
            action TEXT,
            reason TEXT,
            detail TEXT,
            source TEXT
        )
        """
    )
    conn.execute(
        """
        INSERT INTO decay_decisions (id, ts, symbol, venue, trade_id, action, reason, detail, source)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (10, 1000.0, "ETH", "hyperliquid", 42, "CLOSE", "DECAY_EXIT", "{}", "decay_worker_notify"),
    )
    conn.commit()

    rows = _fetch_pending_plans(conn, after_id=0, limit=10)
    assert len(rows) == 1
    assert rows[0].id == 10
    assert rows[0].trade_id == 42


def test_decision_backoff_remaining_hold_and_close_failed_streak() -> None:
    conn = sqlite3.connect(":memory:")
    conn.execute(
        """
        CREATE TABLE decay_decisions (
            id INTEGER PRIMARY KEY,
            ts REAL,
            trade_id INTEGER,
            action TEXT,
            source TEXT
        )
        """
    )

    now = 2000.0
    # Recent HOLD should apply hold backoff.
    conn.execute(
        "INSERT INTO decay_decisions (ts, trade_id, action, source) VALUES (?, ?, ?, ?)",
        (now - 100.0, 7, "HOLD", "hl_exit_decider"),
    )
    rem_hold, mode_hold = _decision_backoff_remaining(
        conn,
        trade_id=7,
        now_ts=now,
        hold_backoff_sec=300.0,
        close_failed_backoff_sec=900.0,
        close_failed_streak_threshold=3,
        close_failed_streak_backoff_sec=1800.0,
    )
    assert mode_hold == "hold"
    assert 199.0 <= rem_hold <= 201.0

    # Consecutive CLOSE_FAILED should escalate to streak backoff window.
    conn.execute(
        "INSERT INTO decay_decisions (ts, trade_id, action, source) VALUES (?, ?, ?, ?)",
        (now - 150.0, 8, "HOLD", "hl_exit_decider"),
    )
    conn.execute(
        "INSERT INTO decay_decisions (ts, trade_id, action, source) VALUES (?, ?, ?, ?)",
        (now - 90.0, 8, "CLOSE_FAILED", "hl_exit_decider_fail"),
    )
    conn.execute(
        "INSERT INTO decay_decisions (ts, trade_id, action, source) VALUES (?, ?, ?, ?)",
        (now - 70.0, 8, "CLOSE_FAILED", "hl_exit_decider_fail"),
    )
    conn.execute(
        "INSERT INTO decay_decisions (ts, trade_id, action, source) VALUES (?, ?, ?, ?)",
        (now - 50.0, 8, "CLOSE_FAILED", "hl_exit_decider_fail"),
    )
    conn.commit()

    rem_fail, mode_fail = _decision_backoff_remaining(
        conn,
        trade_id=8,
        now_ts=now,
        hold_backoff_sec=300.0,
        close_failed_backoff_sec=600.0,
        close_failed_streak_threshold=3,
        close_failed_streak_backoff_sec=1200.0,
    )
    assert mode_fail == "close_failed"
    # latest failure was 50s ago; escalated window 1200s => ~1150s left
    assert 1149.0 <= rem_fail <= 1151.0


def test_decision_backoff_remaining_returns_zero_when_no_recent_decider_action() -> None:
    conn = sqlite3.connect(":memory:")
    conn.execute(
        """
        CREATE TABLE decay_decisions (
            id INTEGER PRIMARY KEY,
            ts REAL,
            trade_id INTEGER,
            action TEXT,
            source TEXT
        )
        """
    )
    conn.execute(
        "INSERT INTO decay_decisions (ts, trade_id, action, source) VALUES (?, ?, ?, ?)",
        (100.0, 11, "HOLD", "position_review_worker"),
    )
    conn.commit()
    rem, mode = _decision_backoff_remaining(
        conn,
        trade_id=11,
        now_ts=200.0,
        hold_backoff_sec=300.0,
        close_failed_backoff_sec=600.0,
        close_failed_streak_threshold=3,
        close_failed_streak_backoff_sec=1200.0,
    )
    assert rem == 0.0
    assert mode == ""


def test_load_latest_context_compact_prefers_latest_pointer() -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        runtime = Path(tmpdir)
        older = runtime / "evclaw_context_1.json"
        newer = runtime / "evclaw_context_2.json"
        pointer = runtime / "evclaw_context_latest.json"

        older.write_text(json.dumps({"global_context_compact": "OLD", "symbols": {"OLD": {}}}))
        newer.write_text(json.dumps({"global_context_compact": "NEW", "symbols": {"NEW": {}}}))
        pointer.write_text(json.dumps({"global_context_compact": "PTR", "symbols": {"PTR": {}}}))

        compact, symbols = _load_latest_context_compact(runtime_dir=str(runtime))
        assert compact == "PTR"
        assert isinstance(symbols, dict)
        assert "PTR" in symbols


def test_load_latest_context_compact_falls_back_to_newest_scan() -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        runtime = Path(tmpdir)
        older = runtime / "evclaw_context_100.json"
        newer = runtime / "evclaw_context_200.json"

        older.write_text(json.dumps({"global_context_compact": "OLD", "symbols": {"OLD": {}}}))
        time.sleep(0.01)
        newer.write_text(json.dumps({"global_context_compact": "NEW", "symbols": {"NEW": {}}}))

        compact, symbols = _load_latest_context_compact(runtime_dir=str(runtime))
        assert compact == "NEW"
        assert isinstance(symbols, dict)
        assert "NEW" in symbols


def test_build_plan_payloads_omits_null_heavy_trade_fields() -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = str(Path(tmpdir) / "llm_exit_decider_payloads.db")
        db = AITraderDB(db_path)
        trade_id = db.log_trade_entry(
            symbol="ETH",
            direction="LONG",
            entry_price=100.0,
            size=1.0,
            venue="hyperliquid",
        )
        trade = db.get_trade(int(trade_id))
        assert trade is not None
        batch = [
            PlanRow(
                id=1,
                ts=time.time(),
                symbol="ETH",
                venue="hyperliquid",
                trade_id=int(trade_id),
                reason="DECAY_EXIT",
                detail="{}",
                source="decay_worker_notify",
            )
        ]
        payloads, plan_by_id = _build_plan_payloads(
            batch=batch,
            db=db,
            db_path=db_path,
            runtime_dir=tmpdir,
            enable_dossier=False,
            dossier_max_chars=480,
            trade_cache={int(trade_id): trade},
            resolved_plan_ids=set(),
            db_conn=None,
        )
        assert int(plan_by_id[1].trade_id) == int(trade_id)
        assert len(payloads) == 1
        trade_blob = payloads[0]["trade"]
        for key in (
            "unrealized_pnl_usd",
            "unrealized_pnl_pct",
            "mark_price",
            "sl_price",
            "tp_price",
            "sl_order_id",
            "tp_order_id",
        ):
            assert key not in trade_blob


def test_build_plan_payloads_includes_signal_context_for_hip3() -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = str(Path(tmpdir) / "llm_exit_decider_signal_ctx.db")
        db = AITraderDB(db_path)
        trade_id = db.log_trade_entry(
            symbol="XYZ:ABC",
            direction="SHORT",
            entry_price=100.0,
            size=1.0,
            venue="hip3",
            signals_snapshot={"hip3_main": {"direction": "SHORT", "z_score": 2.7}, "cvd": {"direction": "SHORT"}},
            signals_agreed=["hip3_main", "cvd"],
        )
        batch = [
            PlanRow(
                id=1,
                ts=time.time(),
                symbol="XYZ:ABC",
                venue="hip3",
                trade_id=int(trade_id),
                reason="DECAY_EXIT",
                detail="{}",
                source="decay_worker_notify",
            )
        ]
        with sqlite3.connect(db_path) as conn:
            trade_cache = _load_trade_cache_for_ids(conn, [int(trade_id)])
        payloads, _plan_by_id = _build_plan_payloads(
            batch=batch,
            db=db,
            db_path=db_path,
            runtime_dir=tmpdir,
            enable_dossier=False,
            dossier_max_chars=480,
            trade_cache=trade_cache,
            resolved_plan_ids=set(),
            db_conn=None,
        )
        assert len(payloads) == 1
        trade_blob = payloads[0]["trade"]
        signal_ctx = trade_blob.get("signal_context")
        assert isinstance(signal_ctx, dict)
        assert signal_ctx.get("has_hip3_main") is True
        hip3 = signal_ctx.get("hip3_main")
        assert isinstance(hip3, dict)
        assert hip3.get("direction") == "SHORT"


def test_run_loop_once_timeout_records_close_failed_and_backoff_defers_without_cursor_persist() -> None:
    import llm_exit_decider as mod

    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = str(Path(tmpdir) / "llm_exit_decider_loop.db")
        db = AITraderDB(db_path)
        trade_id = db.log_trade_entry(
            symbol="ETH",
            direction="LONG",
            entry_price=100.0,
            size=1.0,
            venue="hyperliquid",
        )
        plan1_id = db.record_decay_decision(
            symbol="ETH",
            venue="hyperliquid",
            trade_id=int(trade_id),
            action="CLOSE",
            reason="DECAY_EXIT",
            detail="{}",
            source="decay_worker_notify",
            dedupe_seconds=0.0,
        )

        state_path = Path(tmpdir) / "exit_cursor.json"
        cfg = ExitRuntimeConfig(
            agent_id="hl-exit-decider-test",
            thinking="minimal",
            model=None,
            timeout_sec=30.0,
            close_timeout_sec=1.0,
            poll_sec=0.0,
            hold_backoff_sec=0.0,
            close_failed_backoff_sec=600.0,
            close_failed_streak_threshold=0,
            close_failed_streak_backoff_sec=0.0,
            max_per_loop=1,
            dynamic_max_actions_enabled=False,
            dynamic_max_actions_cap=3,
            dynamic_backlog_two=10,
            dynamic_backlog_three=25,
            exit_enable_dossier=False,
            exit_dossier_max_chars=480,
            exposure_max_age_sec=900.0,
            state_path=state_path,
            runtime_dir=tmpdir,
            openclaw_cmd="openclaw",
            decisions_jsonl_path=str(Path(tmpdir) / "decisions.jsonl"),
            diary_path=str(Path(tmpdir) / "diary.md"),
            lock_path=Path(tmpdir) / "lock",
        )

        calls = {"agent": 0}

        async def fake_openclaw_agent_turn(**_kwargs):
            calls["agent"] += 1
            return {}, json.dumps(
                {
                    "closes": [{"plan_id": int(plan1_id), "reason": "timeout regression close"}],
                    "holds": [],
                }
            )

        async def fake_run_cli_close(**_kwargs):
            return False, "close_timeout_after_1.0s"

        async def fake_sleep(_secs: float):
            return None

        orig_turn = mod.openclaw_agent_turn
        orig_close = mod._run_cli_close
        orig_sleep = mod.asyncio.sleep
        mod.openclaw_agent_turn = fake_openclaw_agent_turn
        mod._run_cli_close = fake_run_cli_close
        mod.asyncio.sleep = fake_sleep
        try:
            next_id_1 = asyncio.run(
                _run_loop_once(
                    db_path=db_path,
                    db=db,
                    cfg=cfg,
                    last_id=0,
                )
            )
            assert calls["agent"] == 1
            with sqlite3.connect(db_path) as conn:
                row = conn.execute(
                    """
                    SELECT action, source
                    FROM decay_decisions
                    WHERE source_plan_id = ?
                    ORDER BY id DESC
                    LIMIT 1
                    """,
                    (int(plan1_id),),
                ).fetchone()
            assert row is not None
            assert str(row[0]).upper() == "CLOSE_FAILED"
            assert str(row[1]) == "hl_exit_decider_fail"
            assert _load_cursor_state(state_path) == int(next_id_1)

            # Create ID gap so compute_next_cursor can move in-memory cursor on defer.
            db.record_decay_decision(
                symbol="ETH",
                venue="hyperliquid",
                trade_id=int(trade_id),
                action="HOLD",
                reason="MANUAL",
                detail="filler",
                source="manual",
                dedupe_seconds=0.0,
            )
            plan2_id = db.record_decay_decision(
                symbol="ETH",
                venue="hyperliquid",
                trade_id=int(trade_id),
                action="CLOSE",
                reason="DECAY_EXIT",
                detail="{}",
                source="decay_worker_notify",
                dedupe_seconds=0.0,
            )
            assert int(plan2_id) >= int(plan1_id) + 2

            persisted_before = _load_cursor_state(state_path)

            async def unexpected_openclaw_agent_turn(**_kwargs):
                raise AssertionError("agent should not be called while close_failed backoff is active")

            mod.openclaw_agent_turn = unexpected_openclaw_agent_turn
            next_id_2 = asyncio.run(
                _run_loop_once(
                    db_path=db_path,
                    db=db,
                    cfg=cfg,
                    last_id=int(next_id_1),
                )
            )

            # In-memory cursor can move, but persisted cursor must not move in no-actionable branch.
            assert int(next_id_2) == max(int(next_id_1), int(plan2_id) - 1)
            assert _load_cursor_state(state_path) == int(persisted_before)
            with sqlite3.connect(db_path) as conn:
                processed_count = int(
                    conn.execute(
                        """
                        SELECT COUNT(1)
                        FROM decay_decisions
                        WHERE source_plan_id = ?
                          AND source IN ('hl_exit_decider', 'hl_exit_decider_fail')
                        """,
                        (int(plan2_id),),
                    ).fetchone()[0]
                )
            assert processed_count == 0
        finally:
            mod.openclaw_agent_turn = orig_turn
            mod._run_cli_close = orig_close
            mod.asyncio.sleep = orig_sleep


def test_effective_max_actions_tiers() -> None:
    assert _effective_max_actions(
        base=1,
        backlog=5,
        enabled=False,
        cap=3,
        backlog_to_two=10,
        backlog_to_three=25,
    ) == 1
    assert _effective_max_actions(
        base=1,
        backlog=12,
        enabled=True,
        cap=3,
        backlog_to_two=10,
        backlog_to_three=25,
    ) == 2
    assert _effective_max_actions(
        base=1,
        backlog=40,
        enabled=True,
        cap=3,
        backlog_to_two=10,
        backlog_to_three=25,
    ) == 3
    assert _effective_max_actions(
        base=1,
        backlog=40,
        enabled=True,
        cap=2,
        backlog_to_two=10,
        backlog_to_three=25,
    ) == 2


def test_build_plan_payloads_includes_symbol_learning_dossier_when_available() -> None:
    import llm_exit_decider as mod

    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = str(Path(tmpdir) / "llm_exit_decider_dossier.db")
        db = AITraderDB(db_path)
        trade_id = db.log_trade_entry(
            symbol="ETH",
            direction="LONG",
            entry_price=100.0,
            size=1.0,
            venue="hyperliquid",
        )
        trade = db.get_trade(int(trade_id))
        assert trade is not None
        batch = [
            PlanRow(
                id=1,
                ts=time.time(),
                symbol="ETH",
                venue="hyperliquid",
                trade_id=int(trade_id),
                reason="DECAY_EXIT",
                detail="{}",
                source="decay_worker_notify",
            )
        ]

        orig_get_dossier = getattr(mod, "_get_dossier_snippet", None)
        mod._get_dossier_snippet = lambda _db_path, symbol, max_chars=480: f"{symbol} conclusion note"
        try:
            payloads, _plan_by_id = _build_plan_payloads(
                batch=batch,
                db=db,
                db_path=db_path,
                runtime_dir=tmpdir,
                enable_dossier=True,
                dossier_max_chars=180,
                trade_cache={int(trade_id): trade},
                resolved_plan_ids=set(),
                db_conn=None,
            )
        finally:
            mod._get_dossier_snippet = orig_get_dossier

        assert len(payloads) == 1
        trade_blob = payloads[0]["trade"]
        assert trade_blob.get("symbol_learning_dossier") == "ETH conclusion note"
