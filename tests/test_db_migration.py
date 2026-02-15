#!/usr/bin/env python3
"""Tests for AI Trader DB multi-venue schema and migration."""

import json
import sqlite3
import sys
import tempfile
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from ai_trader_db import AITraderDB


def test_fresh_db_schema() -> None:
    """Fresh DB should have v2 schema (venue, exchange_trade_id, nullable trade_id)."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    db = AITraderDB(db_path)

    conn = sqlite3.connect(db_path)
    # Check fills table has venue + exchange_trade_id
    cursor = conn.execute("PRAGMA table_info(fills)")
    columns = {row[1] for row in cursor.fetchall()}
    assert "venue" in columns, f"Missing 'venue' column, got: {columns}"
    assert "exchange_trade_id" in columns, f"Missing 'exchange_trade_id', got: {columns}"
    assert "lighter_trade_id" not in columns, f"Old column 'lighter_trade_id' still present"

    # Check trade_id is nullable (notnull=0)
    cursor = conn.execute("PRAGMA table_info(fills)")
    for row in cursor.fetchall():
        if row[1] == "trade_id":
            assert row[3] == 0, f"trade_id should be nullable, got notnull={row[3]}"

    # Check trades table has venue column
    cursor = conn.execute("PRAGMA table_info(trades)")
    trades_columns = {row[1] for row in cursor.fetchall()}
    assert "venue" in trades_columns, f"Missing 'venue' in trades, got: {trades_columns}"
    assert "realized_pnl_partial_usd" in trades_columns
    assert "exit_fees_partial_usd" in trades_columns

    # Phase 4: unified dossier tables must exist on fresh DBs.
    cursor = conn.execute("PRAGMA table_info(symbol_learning_state)")
    symbol_state_cols = {row[1] for row in cursor.fetchall()}
    assert "symbol" in symbol_state_cols
    assert "signal_rank_json" in symbol_state_cols
    assert "combo_rank_json" in symbol_state_cols
    assert "last_reflection_id_seen" in symbol_state_cols

    cursor = conn.execute("PRAGMA table_info(signal_symbol_stats)")
    signal_stats_cols = {row[1] for row in cursor.fetchall()}
    assert "symbol" in signal_stats_cols
    assert "signal" in signal_stats_cols
    assert "direction" in signal_stats_cols
    assert "expectancy" in signal_stats_cols

    # Check user_version is set
    version = conn.execute("PRAGMA user_version").fetchone()[0]
    assert version >= 32, f"Expected user_version >= 32, got {version}"

    cursor = conn.execute("PRAGMA table_info(decay_decisions)")
    decision_cols = {row[1] for row in cursor.fetchall()}
    assert "source_plan_id" in decision_cols
    for table_name, required_col in (
        ("conviction_config_history", "params_json"),
        ("adaptation_runs", "fingerprint"),
        ("trade_features", "trade_id"),
        ("gate_decisions_v1", "decision"),
    ):
        cols = {row[1] for row in conn.execute(f"PRAGMA table_info({table_name})").fetchall()}
        assert required_col in cols
    trade_feature_cols = {row[1] for row in conn.execute("PRAGMA table_info(trade_features)").fetchall()}
    assert "order_type_source" in trade_feature_cols
    for col in (
        "strategy_segment",
        "entry_gate_mode",
        "hip3_driver",
        "hip3_flow_pass",
        "hip3_ofm_pass",
        "hip3_booster_score",
        "hip3_booster_size_mult",
    ):
        assert col in trade_feature_cols
    gate_cols = {row[1] for row in conn.execute("PRAGMA table_info(gate_decisions_v1)").fetchall()}
    assert "proposal_id" in gate_cols
    assert "trade_id" in gate_cols
    assert "outcome_status" in gate_cols

    conn.close()


def test_conviction_config_snapshot_activation_and_read() -> None:
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    db = AITraderDB(db_path)

    first_id = db.insert_conviction_config_snapshot(
        params={"limit_min": 0.2, "chase_threshold": 0.7},
        source="seed",
        activate=True,
    )
    second_id = db.insert_conviction_config_snapshot(
        params={"limit_min": 0.25, "chase_threshold": 0.72},
        source="adaptive",
        activate=True,
    )
    assert second_id > first_id

    active = db.get_active_conviction_config()
    assert active is not None
    assert active["id"] == second_id
    assert active["is_active"] == 1
    assert active["params"]["limit_min"] == 0.25


def test_migrate_v35_repairs_gate_decisions_missing_updated_at() -> None:
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE gate_decisions_v1 (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                cycle_seq INTEGER NOT NULL,
                symbol TEXT NOT NULL,
                direction TEXT NOT NULL,
                decision TEXT NOT NULL,
                reason TEXT,
                candidate_rank INTEGER,
                conviction REAL,
                blended_conviction REAL,
                pipeline_conviction REAL,
                brain_conviction REAL,
                llm_agent_id TEXT,
                llm_model TEXT,
                llm_session_id TEXT,
                created_at REAL DEFAULT (strftime('%s', 'now')),
                proposal_id INTEGER,
                venue TEXT,
                trade_id INTEGER,
                outcome_status TEXT,
                outcome_pnl REAL,
                outcome_pnl_pct REAL,
                outcome_closed_at REAL
            )
            """
        )
        conn.execute("PRAGMA user_version = 34")
        conn.commit()

    AITraderDB(db_path)
    with sqlite3.connect(db_path) as conn:
        cols = {row[1] for row in conn.execute("PRAGMA table_info(gate_decisions_v1)").fetchall()}
        version = conn.execute("PRAGMA user_version").fetchone()[0]

    assert "updated_at" in cols
    assert int(version) >= 35


def test_adaptation_run_dedupes_by_fingerprint() -> None:
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    db = AITraderDB(db_path)

    run_id = db.create_adaptation_run(
        window_start=1000.0,
        window_end=2000.0,
        fingerprint="fp:test:1",
    )
    assert run_id is not None

    dup = db.create_adaptation_run(
        window_start=1000.0,
        window_end=2000.0,
        fingerprint="fp:test:1",
    )
    assert dup is None

    ok = db.finish_adaptation_run(int(run_id), status="success", result={"updated": True})
    assert ok is True
    assert db.get_latest_adaptation_fingerprint() == "fp:test:1"
    assert db.get_latest_adaptation_started_at() is not None


def test_gate_decision_insert_and_fetch() -> None:
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    db = AITraderDB(db_path)

    row_id = db.insert_gate_decision(
        cycle_seq=1234,
        symbol="ETH",
        direction="LONG",
        decision="PICK",
        reason="trend+flow aligned",
        candidate_rank=1,
        conviction=0.77,
        blended_conviction=0.77,
        pipeline_conviction=0.62,
        brain_conviction=0.81,
        llm_agent_id="hl-entry-gate",
        llm_model="openai-codex/gpt-5.2",
        llm_session_id="hl_entry_gate_1234",
    )
    assert row_id > 0

    rows = db.get_gate_decisions_for_cycle(1234)
    assert len(rows) == 1
    row = rows[0]
    assert row["symbol"] == "ETH"
    assert row["direction"] == "LONG"
    assert row["decision"] == "PICK"
    assert row["reason"] == "trend+flow aligned"


def test_log_trade_entry_links_gate_decision_to_trade() -> None:
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    db = AITraderDB(db_path)

    gate_decision_id = db.insert_gate_decision(
        cycle_seq=1235,
        symbol="BTC",
        direction="SHORT",
        decision="PICK",
        reason="entry gate approved",
        candidate_rank=1,
        conviction=0.82,
    )

    trade_id = db.log_trade_entry(
        symbol="BTC",
        direction="SHORT",
        entry_price=99000.0,
        size=0.1,
        venue="hyperliquid",
        context_snapshot={
            "entry_gate": {
                "gate_decision_id": int(gate_decision_id),
                "proposal_id": 4321,
                "venue": "hyperliquid",
            }
        },
    )

    rows = db.get_gate_decisions_for_cycle(1235)
    assert len(rows) == 1
    row = rows[0]
    assert int(row["id"]) == int(gate_decision_id)
    assert int(row["trade_id"] or 0) == int(trade_id)
    assert int(row["proposal_id"] or 0) == 4321
    assert str(row["venue"] or "").lower() == "hyperliquid"


def test_proposal_status_transition_guard_blocks_terminal_reopen() -> None:
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    db = AITraderDB(db_path)

    proposal_id = db.insert_proposal(
        cycle_seq=77,
        symbol="ETH",
        venue="hyperliquid",
        direction="LONG",
        size_usd=250.0,
        conviction=0.72,
        reason_short="status-guard",
        signals=["CVD:LONG z=2.0"],
    )
    assert db.update_proposal_status(proposal_id, "EXECUTED", "ok") is True
    # Invalid regression transition should be rejected.
    assert db.update_proposal_status(proposal_id, "PROPOSED", "reopen") is False

    rows = db.get_proposals_for_cycle(77)
    assert len(rows) == 1
    assert rows[0]["status"] == "EXECUTED"


def test_proposal_status_transition_rejects_unknown_status() -> None:
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    db = AITraderDB(db_path)

    proposal_id = db.insert_proposal(
        cycle_seq=78,
        symbol="BTC",
        venue="hyperliquid",
        direction="SHORT",
        size_usd=300.0,
        conviction=0.64,
        reason_short="invalid-status",
        signals=["FADE:SHORT z=2.1"],
    )
    assert db.update_proposal_status(proposal_id, "NOT_A_STATUS", "bad") is False

    rows = db.get_proposals_for_cycle(78)
    assert len(rows) == 1
    assert rows[0]["status"] == "PROPOSED"


def test_trade_features_upsert_and_window_fetch() -> None:
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    db = AITraderDB(db_path)

    now = time.time()
    row_id = db.insert_trade_features(
        trade_id=101,
        symbol="ETH",
        direction="LONG",
        venue="hyperliquid",
        closed_at=now - 5,
        conviction=0.74,
        order_type="chase_limit",
        order_type_source="context_snapshot",
        strategy_segment="perp",
        entry_gate_mode="normal",
        risk_pct_used=0.015,
        equity_at_entry=12000.0,
        mae_pct=0.8,
        mfe_pct=1.7,
        pnl_usd=45.0,
        pnl_r=0.25,
        exit_reason="TP",
    )
    assert row_id > 0

    updated_id = db.insert_trade_features(
        trade_id=101,
        symbol="ETH",
        direction="LONG",
        venue="hyperliquid",
        closed_at=now - 4,
        conviction=0.76,
        order_type="limit",
        order_type_source="confidence_policy",
        strategy_segment="perp",
        entry_gate_mode="normal",
        pnl_usd=50.0,
        pnl_r=0.30,
        exit_reason="TP",
    )
    assert updated_id == row_id

    rows = db.get_trade_features_window(window_start=now - 60, window_end=now + 1, limit=10)
    assert len(rows) == 1
    assert rows[0]["trade_id"] == 101
    assert rows[0]["order_type"] == "limit"
    assert rows[0]["order_type_source"] == "confidence_policy"
    assert rows[0]["conviction"] == 0.76
    assert rows[0]["strategy_segment"] == "perp"
    assert rows[0]["entry_gate_mode"] == "normal"


def test_get_trade_feature_source_normalizes_trade_fields() -> None:
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    db = AITraderDB(db_path)

    trade_id = db.log_trade_entry(
        symbol="BTC",
        direction="LONG",
        entry_price=100000.0,
        size=0.1,
        venue="hyperliquid",
        confidence="0.71",
        risk_pct_used=0.01,
        equity_at_entry=10000.0,
    )
    db.log_trade_exit(trade_id, exit_price=101000.0, exit_reason="TP", total_fees=1.0)

    feature = db.get_trade_feature_source(trade_id)
    assert feature is not None
    assert feature["trade_id"] == trade_id
    assert feature["conviction"] == 0.71
    assert feature["order_type"] == "chase_limit"
    assert feature["pnl_r"] is not None


def test_get_trade_feature_source_uses_runtime_conviction_config_for_confidence_policy() -> None:
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    db = AITraderDB(db_path)

    db.insert_conviction_config_snapshot(
        params={"chase_threshold": 0.90, "limit_min": 0.40},
        source="test_runtime",
        activate=True,
    )

    trade_id = db.log_trade_entry(
        symbol="BTC",
        direction="LONG",
        entry_price=100000.0,
        size=0.1,
        venue="hyperliquid",
        confidence="0.75",
        context_snapshot={"execution_route": "executor_chase"},
        risk_pct_used=0.01,
        equity_at_entry=10000.0,
    )
    db.log_trade_exit(trade_id, exit_price=101000.0, exit_reason="TP", total_fees=1.0)

    feature = db.get_trade_feature_source(trade_id)
    assert feature is not None
    assert feature["order_type"] == "limit"
    assert feature["order_type_source"] == "confidence_policy"


def test_get_trade_feature_source_prefers_context_order_type() -> None:
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    db = AITraderDB(db_path)

    trade_id = db.log_trade_entry(
        symbol="SOL",
        direction="LONG",
        entry_price=200.0,
        size=5.0,
        venue="hyperliquid",
        confidence="0.85",
        context_snapshot={"order_type": "sr_limit", "execution_route": "pending_sr_limit_fill"},
        risk_pct_used=0.01,
        equity_at_entry=10000.0,
    )
    db.log_trade_exit(trade_id, exit_price=210.0, exit_reason="TP", total_fees=1.0)

    feature = db.get_trade_feature_source(trade_id)
    assert feature is not None
    assert feature["order_type"] == "sr_limit"
    assert feature["order_type_source"] == "context_snapshot"
    assert feature["conviction"] == 0.85


def test_get_trade_feature_source_extracts_hip3_fields_from_context_and_signals() -> None:
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    db = AITraderDB(db_path)

    trade_id = db.log_trade_entry(
        symbol="XYZ:NVDA",
        direction="LONG",
        entry_price=500.0,
        size=10.0,
        venue="hip3",
        confidence="0.81",
        context_snapshot={
            "entry_gate_mode": "hip3",
            "strategy_segment": "hip3",
            "risk": {
                "hip3_driver": "flow",
                "hip3_booster_size_mult": 1.25,
            },
        },
        signals_snapshot={
            "hip3_main": {
                "driver_type": "flow",
                "flow_pass": True,
                "ofm_pass": False,
                "components": {
                    "rest_booster_score": 1.8,
                    "rest_booster_size_mult": 1.25,
                },
            }
        },
        risk_pct_used=0.02,
        equity_at_entry=20000.0,
    )
    db.log_trade_exit(trade_id, exit_price=510.0, exit_reason="TP", total_fees=1.0)

    feature = db.get_trade_feature_source(trade_id)
    assert feature is not None
    assert feature["strategy_segment"] == "hip3"
    assert feature["entry_gate_mode"] == "hip3"
    assert feature["hip3_driver"] == "flow"
    assert feature["hip3_flow_pass"] is True
    assert feature["hip3_ofm_pass"] is False
    assert feature["hip3_booster_score"] == 1.8
    assert feature["hip3_booster_size_mult"] == 1.25


def test_fill_composite_uniqueness() -> None:
    """UNIQUE(venue, exchange_trade_id) constraint works correctly."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    db = AITraderDB(db_path)

    # Log a fill on lighter
    fill_id = db.log_fill(
        trade_id=None,
        venue="lighter",
        exchange_trade_id="12345",
        symbol="ETH",
        fill_time=time.time(),
        fill_price=3000.0,
        fill_size=0.1,
        fill_type="ENTRY",
        side="BUY",
    )
    assert fill_id > 0

    # Same venue + same exchange_trade_id should be duplicate
    dup = db.log_fill(
        trade_id=None,
        venue="lighter",
        exchange_trade_id="12345",
        symbol="ETH",
        fill_time=time.time(),
        fill_price=3000.0,
        fill_size=0.1,
        fill_type="ENTRY",
        side="BUY",
    )
    assert dup == -1, f"Expected -1 for duplicate, got {dup}"

    # Different venue + same exchange_trade_id should succeed
    fill_id2 = db.log_fill(
        trade_id=None,
        venue="hyperliquid",
        exchange_trade_id="12345",
        symbol="ETH",
        fill_time=time.time(),
        fill_price=3000.0,
        fill_size=0.1,
        fill_type="ENTRY",
        side="BUY",
    )
    assert fill_id2 > 0, f"Different venue should not be duplicate, got {fill_id2}"


def test_log_trade_entry_with_venue() -> None:
    """log_trade_entry() accepts venue parameter."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    db = AITraderDB(db_path)

    trade_id = db.log_trade_entry(
        symbol="ETH",
        direction="LONG",
        entry_price=3000.0,
        size=1.0,
        venue="hyperliquid",
    )
    assert trade_id > 0

    # Verify venue stored correctly
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    row = conn.execute("SELECT venue FROM trades WHERE id = ?", (trade_id,)).fetchone()
    assert row["venue"] == "hyperliquid"
    conn.close()


def test_log_trade_entry_preserves_empty_snapshot_dicts() -> None:
    """Regression: {} snapshots must not be dropped to NULL."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    db = AITraderDB(db_path)
    trade_id = db.log_trade_entry(
        symbol="SOL",
        direction="LONG",
        entry_price=100.0,
        size=1.0,
        venue="lighter",
        signals_snapshot={},
        context_snapshot={},
        protection_snapshot={},
    )
    assert trade_id > 0

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    row = conn.execute(
        "SELECT signals_snapshot, context_snapshot, protection_snapshot FROM trades WHERE id = ?",
        (trade_id,),
    ).fetchone()
    conn.close()

    assert row["signals_snapshot"] == "{}", f"Expected '{{}}', got {row['signals_snapshot']}"
    assert row["context_snapshot"] == "{}", f"Expected '{{}}', got {row['context_snapshot']}"
    assert row["protection_snapshot"] == "{}", f"Expected '{{}}', got {row['protection_snapshot']}"


def test_update_trade_mae_mfe_clamps_non_negative() -> None:
    """update_trade_mae_mfe() should floor negative values to 0."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    db = AITraderDB(db_path)
    trade_id = db.log_trade_entry(
        symbol="ETH",
        direction="SHORT",
        entry_price=3000.0,
        size=1.0,
        venue="hyperliquid",
    )
    assert trade_id > 0

    ok = db.update_trade_mae_mfe(
        trade_id=trade_id,
        mae_pct=-1.25,
        mfe_pct=-9.5,
        mae_price=3010.0,
        mfe_price=2950.0,
    )
    assert ok

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    row = conn.execute(
        "SELECT mae_pct, mfe_pct, mae_price, mfe_price FROM trades WHERE id = ?",
        (trade_id,),
    ).fetchone()
    conn.close()

    assert row["mae_pct"] == 0.0
    assert row["mfe_pct"] == 0.0
    assert row["mae_price"] == 3010.0
    assert row["mfe_price"] == 2950.0


def test_find_recent_filled_pending_and_executed_proposal() -> None:
    """DB helper lookups should recover attribution context."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    db = AITraderDB(db_path)
    now = time.time()

    class _P:
        symbol = "DOGE"
        venue = "hyperliquid"
        direction = "LONG"
        limit_price = 0.2
        intended_size = 1000.0
        exchange_order_id = "oid-test-1"
        sr_level = 0.2
        placed_at = now - 15
        expires_at = now + 600
        entry_direction = "LONG"
        state = "FILLED"
        signals_snapshot = json.dumps({"cvd": {"direction": "LONG", "z_score": 2.2}})
        signals_agreed = json.dumps(["CVD:LONG z=2.2"])
        context_snapshot = json.dumps({"key_metrics": {"price": 0.2}})
        conviction = 0.73
        reason = "SR_LIMIT_FILLED"
        sl_order_id = None
        sl_price = None

    db.insert_pending_order(_P())
    pending = db.find_recent_filled_pending_order("DOGE", "hyperliquid", around_ts=now, window_sec=300)
    assert pending is not None
    assert pending["source"] == "pending_orders_filled"
    assert isinstance(pending.get("signals_snapshot"), dict)
    assert isinstance(pending.get("context_snapshot"), dict)
    assert pending.get("signals_agreed")

    proposal_id = db.insert_proposal(
        cycle_seq=999001,
        symbol="DOGE",
        venue="hyperliquid",
        direction="LONG",
        size_usd=500.0,
        sl=None,
        tp=None,
        conviction=0.66,
        reason_short="EXEC_TEST",
        signals=["CVD:LONG z=2.2"],
    )
    db.update_proposal_status(proposal_id, "EXECUTED", "ok")
    db.insert_proposal_metadata(
        proposal_id,
        {
            "signals_snapshot": {"whale": {"direction": "LONG", "strength": 0.8}},
            "context_snapshot": {"key_metrics": {"price": 0.2, "atr_pct": 1.5}},
            "execution": {"order_type": "limit", "source": "llm_gate", "limit_style": "sr_limit"},
        },
    )

    prop = db.find_recent_executed_proposal("DOGE", "hyperliquid", around_ts=time.time(), window_sec=600)
    assert prop is not None
    assert prop["proposal_id"] == proposal_id
    assert isinstance(prop.get("signals_snapshot"), dict)
    assert isinstance(prop.get("context_snapshot"), dict)
    assert prop.get("execution_order_type") == "limit"
    assert prop.get("execution_source") == "llm_gate"
    assert prop.get("execution_limit_style") == "sr_limit"


def test_insert_pending_order_duplicate_exchange_order_id_is_idempotent() -> None:
    """Duplicate pending-order journal writes should upsert, not raise/duplicate."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    db = AITraderDB(db_path)
    now = time.time()

    class _P:
        symbol = "SOL"
        venue = "lighter"
        direction = "LONG"
        limit_price = 101.2
        intended_size = 1.75
        exchange_order_id = "oid-sol-idempotent-1"
        sr_level = 101.2
        placed_at = now - 20
        expires_at = now + 600
        entry_direction = "LONG"
        state = "PENDING"
        signals_snapshot = json.dumps({"cvd": {"direction": "LONG", "z_score": 2.1}})
        signals_agreed = json.dumps(["CVD:LONG z=2.1"])
        context_snapshot = json.dumps({"key_metrics": {"price": 101.25}})
        conviction = 0.55
        reason = "FIRST_WRITE"
        sl_order_id = None
        sl_price = None

    first_id = db.insert_pending_order(_P())
    assert first_id > 0

    class _P2(_P):
        state = "FILLED"
        reason = "SECOND_WRITE"
        conviction = 0.77

    second_id = db.insert_pending_order(_P2())
    assert second_id == first_id

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        row = conn.execute(
            "SELECT id, state, reason, conviction FROM pending_orders WHERE exchange_order_id = ?",
            (_P.exchange_order_id,),
        ).fetchone()
        cnt = conn.execute(
            "SELECT COUNT(*) FROM pending_orders WHERE exchange_order_id = ?",
            (_P.exchange_order_id,),
        ).fetchone()[0]
    finally:
        conn.close()

    assert row is not None
    assert int(row["id"]) == first_id
    assert row["state"] == "FILLED"
    assert row["reason"] == "SECOND_WRITE"
    assert float(row["conviction"]) == 0.77
    assert cnt == 1


def test_recent_metadata_helpers_do_not_fallback_out_of_window_by_default() -> None:
    """Out-of-window rows must not be attached unless explicitly requested."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    db = AITraderDB(db_path)
    now = time.time()
    old_ts = now - 7200.0

    class _P:
        symbol = "DOGE"
        venue = "hyperliquid"
        direction = "LONG"
        limit_price = 0.2
        intended_size = 1000.0
        exchange_order_id = "oid-old-1"
        sr_level = 0.2
        placed_at = old_ts
        expires_at = old_ts + 600
        entry_direction = "LONG"
        state = "FILLED"
        signals_snapshot = json.dumps({"cvd": {"direction": "LONG", "z_score": 1.9}})
        signals_agreed = json.dumps(["CVD:LONG z=1.9"])
        context_snapshot = json.dumps({"key_metrics": {"price": 0.2}})
        conviction = 0.65
        reason = "OLD_PENDING"
        sl_order_id = None
        sl_price = None

    db.insert_pending_order(_P())
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "UPDATE pending_orders SET updated_at = ? WHERE exchange_order_id = ?",
            (old_ts, _P.exchange_order_id),
        )
        conn.commit()

    proposal_id = db.insert_proposal(
        cycle_seq=999002,
        symbol="DOGE",
        venue="hyperliquid",
        direction="LONG",
        size_usd=500.0,
        sl=None,
        tp=None,
        conviction=0.6,
        reason_short="OLD_PROPOSAL",
        signals=["CVD:LONG z=1.9"],
    )
    db.update_proposal_status(proposal_id, "EXECUTED", "ok")
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "UPDATE trade_proposals SET created_at = ?, updated_at = ? WHERE id = ?",
            (old_ts, old_ts, proposal_id),
        )
        conn.commit()

    pending_default = db.find_recent_filled_pending_order(
        "DOGE", "hyperliquid", around_ts=now, window_sec=300
    )
    proposal_default = db.find_recent_executed_proposal(
        "DOGE", "hyperliquid", around_ts=now, window_sec=300
    )
    assert pending_default is None
    assert proposal_default is None

    pending_opt_in = db.find_recent_filled_pending_order(
        "DOGE", "hyperliquid", around_ts=now, window_sec=300, allow_out_of_window_nearest=True
    )
    proposal_opt_in = db.find_recent_executed_proposal(
        "DOGE", "hyperliquid", around_ts=now, window_sec=300, allow_out_of_window_nearest=True
    )
    assert pending_opt_in is not None
    assert proposal_opt_in is not None
    assert pending_opt_in.get("reason") == "OLD_PENDING"
    assert proposal_opt_in.get("proposal_id") == proposal_id


def test_update_symbol_policy_preserves_zero_values() -> None:
    """Regression: 0.0 values must not be overwritten by truthy defaults."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    db = AITraderDB(db_path)
    ok = db.update_symbol_policy(
        symbol="XRP",
        sl_mult_adjustment=0.0,
        tp_mult_adjustment=0.0,
        size_adjustment=0.0,
        stop_out_rate=0.0,
        win_rate=0.0,
        samples=0,
        notes="zero-values",
    )
    assert ok

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    row = conn.execute("SELECT * FROM symbol_policy WHERE symbol = ?", ("XRP",)).fetchone()
    conn.close()
    assert row is not None
    assert float(row["sl_mult_adjustment"]) == 0.0
    assert float(row["tp_mult_adjustment"]) == 0.0
    assert float(row["size_adjustment"]) == 0.0
    assert float(row["stop_out_rate"]) == 0.0
    assert float(row["win_rate"]) == 0.0
    assert int(row["samples"]) == 0


def test_duplicate_open_trade_does_not_overwrite_entry_fields() -> None:
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    db = AITraderDB(db_path)
    trade_id_1 = db.log_trade_entry(
        symbol="ADA",
        direction="LONG",
        entry_price=1.0,
        size=100.0,
        venue="lighter",
    )
    trade_id_2 = db.log_trade_entry(
        symbol="ADA",
        direction="LONG",
        entry_price=2.0,
        size=200.0,
        venue="lighter",
    )
    assert trade_id_2 == trade_id_1

    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT entry_price, size, notional_usd FROM trades WHERE id = ?",
            (trade_id_1,),
        ).fetchone()

    assert row is not None
    assert float(row["entry_price"]) == 1.0
    assert float(row["size"]) == 100.0
    assert float(row["notional_usd"]) == 100.0


def test_nullable_trade_id_in_fills() -> None:
    """Fills with trade_id=None should be allowed (orphan fills)."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    db = AITraderDB(db_path)

    fill_id = db.log_fill(
        trade_id=None,
        venue="lighter",
        exchange_trade_id="orphan_001",
        symbol="SOL",
        fill_time=time.time(),
        fill_price=100.0,
        fill_size=10.0,
        fill_type="ORPHAN",
        side="SELL",
    )
    assert fill_id > 0, f"Orphan fill with trade_id=None should succeed, got {fill_id}"


def test_migration_from_v1_schema() -> None:
    """Migration from v1 (lighter_trade_id) to v2 (venue + exchange_trade_id)."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    # Create a v1-style database manually
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")

    # v1 trades table (no venue column)
    conn.execute("""
        CREATE TABLE trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            direction TEXT NOT NULL,
            entry_time REAL NOT NULL,
            entry_price REAL NOT NULL,
            size REAL NOT NULL,
            notional_usd REAL NOT NULL,
            exit_time REAL,
            exit_price REAL,
            exit_reason TEXT,
            realized_pnl REAL,
            realized_pnl_pct REAL,
            total_fees REAL DEFAULT 0.0,
            sl_price REAL,
            tp_price REAL,
            sl_order_id TEXT,
            tp_order_id TEXT,
            signals_snapshot TEXT,
            signals_agreed TEXT,
            ai_reasoning TEXT,
            confidence TEXT,
            size_multiplier REAL,
            context_snapshot TEXT,
            safety_tier INTEGER,
            created_at REAL DEFAULT (strftime('%s', 'now')),
            updated_at REAL DEFAULT (strftime('%s', 'now'))
        )
    """)

    # v1 fills table (lighter_trade_id, NOT NULL trade_id)
    conn.execute("""
        CREATE TABLE fills (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            trade_id INTEGER NOT NULL,
            lighter_trade_id INTEGER NOT NULL UNIQUE,
            symbol TEXT NOT NULL,
            fill_time REAL NOT NULL,
            fill_price REAL NOT NULL,
            fill_size REAL NOT NULL,
            fill_type TEXT NOT NULL,
            side TEXT NOT NULL,
            fee REAL DEFAULT 0.0,
            fee_maker REAL DEFAULT 0.0,
            position_sign_changed INTEGER DEFAULT 0,
            raw_json TEXT,
            created_at REAL DEFAULT (strftime('%s', 'now')),
            FOREIGN KEY (trade_id) REFERENCES trades(id)
        )
    """)

    # Insert v1 data
    conn.execute("""
        INSERT INTO trades (symbol, direction, entry_time, entry_price, size, notional_usd)
        VALUES ('ETH', 'LONG', 1700000000, 3000.0, 1.0, 3000.0)
    """)

    conn.execute("""
        INSERT INTO fills (trade_id, lighter_trade_id, symbol, fill_time, fill_price, fill_size, fill_type, side)
        VALUES (1, 42, 'ETH', 1700000000, 3000.0, 1.0, 'ENTRY', 'BUY')
    """)

    # Safety state table (needed by AITraderDB)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS safety_state (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            current_tier INTEGER DEFAULT 1,
            daily_pnl REAL DEFAULT 0.0,
            daily_pnl_reset_at TEXT,
            consecutive_losses INTEGER DEFAULT 0,
            cooldown_until TEXT,
            max_drawdown_pct REAL DEFAULT 0.0,
            peak_equity REAL DEFAULT 10000.0,
            current_equity REAL DEFAULT 10000.0,
            paused INTEGER DEFAULT 0,
            updated_at REAL DEFAULT (strftime('%s', 'now'))
        )
    """)
    conn.execute(
        "INSERT OR IGNORE INTO safety_state (id, peak_equity, current_equity) VALUES (1, 10000.0, 10000.0)"
    )

    conn.execute("PRAGMA user_version = 0")
    conn.commit()
    conn.close()

    # Now open with AITraderDB — should trigger migration
    db = AITraderDB(db_path)

    # Verify migration happened
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    # Check user_version
    version = conn.execute("PRAGMA user_version").fetchone()[0]
    assert version >= 2, f"Expected user_version >= 2 after migration, got {version}"

    # Check fills table has new columns
    cursor = conn.execute("PRAGMA table_info(fills)")
    columns = {row[1] for row in cursor.fetchall()}
    assert "venue" in columns, f"Missing 'venue' after migration"
    assert "exchange_trade_id" in columns, f"Missing 'exchange_trade_id' after migration"
    assert "lighter_trade_id" not in columns, f"Old column still present after migration"

    # Check migrated data
    row = conn.execute("SELECT venue, exchange_trade_id, trade_id FROM fills WHERE id = 1").fetchone()
    assert row["venue"] == "lighter", f"Expected venue='lighter', got {row['venue']}"
    assert row["exchange_trade_id"] == "42", f"Expected exchange_trade_id='42', got {row['exchange_trade_id']}"
    assert row["trade_id"] == 1

    # Check trades table has venue column
    cursor = conn.execute("PRAGMA table_info(trades)")
    trades_cols = {row[1] for row in cursor.fetchall()}
    assert "venue" in trades_cols, f"Missing 'venue' in trades after migration"

    # Check default venue on old trade
    trade_row = conn.execute("SELECT venue FROM trades WHERE id = 1").fetchone()
    assert trade_row["venue"] == "lighter"

    conn.close()

    # Verify the migrated DB is fully functional
    fill_id = db.log_fill(
        trade_id=1,
        venue="hyperliquid",
        exchange_trade_id="hl_001",
        symbol="ETH",
        fill_time=time.time(),
        fill_price=3100.0,
        fill_size=0.5,
        fill_type="ENTRY",
        side="BUY",
    )
    assert fill_id > 0, f"Post-migration fill insert should work, got {fill_id}"


if __name__ == "__main__":
    test_fresh_db_schema()
    print("[PASS] fresh DB schema")
    test_fill_composite_uniqueness()
    print("[PASS] fill composite uniqueness")
    test_fill_exists_composite()
    print("[PASS] fill_exists composite key")
    test_log_trade_entry_with_venue()
    print("[PASS] log_trade_entry with venue")
    test_log_trade_entry_preserves_empty_snapshot_dicts()
    print("[PASS] empty snapshot dict persistence")
    test_update_trade_mae_mfe_clamps_non_negative()
    print("[PASS] update_trade_mae_mfe floors negatives")
    test_find_recent_filled_pending_and_executed_proposal()
    print("[PASS] recent pending/proposal attribution lookups")
    test_update_signal_combo_accumulates_consistently()
    print("[PASS] signal combo arithmetic consistency")
    test_update_symbol_policy_preserves_zero_values()
    print("[PASS] symbol policy preserves zero values")
    test_duplicate_open_trade_does_not_overwrite_entry_fields()
    print("[PASS] duplicate open trade does not overwrite")
    test_nullable_trade_id_in_fills()
    print("[PASS] nullable trade_id in fills")
    test_migration_from_v1_schema()
    print("[PASS] migration from v1 schema")
