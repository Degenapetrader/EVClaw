import sqlite3
import tempfile
import time
from pathlib import Path

from ai_trader_db import AITraderDB


def test_exit_outcome_roundtrip_insert() -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = str(Path(tmpdir) / "exit_outcome_roundtrip.db")
        db = AITraderDB(db_path)
        trade_id = db.log_trade_entry(
            symbol="ETH",
            direction="LONG",
            entry_price=100.0,
            size=1.0,
            venue="hyperliquid",
        )
        decision_id = db.record_decay_decision(
            symbol="ETH",
            venue="hyperliquid",
            trade_id=int(trade_id),
            source_plan_id=77,
            action="CLOSE",
            reason="DECAY_EXIT",
            detail="plan_id=77 action=CLOSE",
            source="hl_exit_decider",
            dedupe_seconds=0.0,
            ts=time.time() - 7200.0,
        )
        row_id = db.record_exit_decision_outcome(
            decision_rowid=int(decision_id),
            source_plan_id=77,
            trade_id=int(trade_id),
            symbol="ETH",
            venue="hyperliquid",
            action="CLOSE",
            outcome_kind="CLOSE_THEN_FLAT",
            horizon_sec=3600,
            decision_ts=time.time() - 7200.0,
            evaluated_ts=time.time(),
            decision_mark=100.0,
            horizon_mark=100.1,
            mark_move_pct=0.1,
            meta_json='{"k":"v"}',
        )
        assert row_id > 0

        with sqlite3.connect(db_path) as conn:
            row = conn.execute(
                """
                SELECT outcome_kind, action, decision_rowid, horizon_sec
                FROM exit_decision_outcomes_v1
                WHERE id = ?
                """,
                (int(row_id),),
            ).fetchone()
        assert row is not None
        assert str(row[0]).upper() == "CLOSE_THEN_FLAT"
        assert str(row[1]).upper() == "CLOSE"
        assert int(row[2]) == int(decision_id)
        assert int(row[3]) == 3600


def test_fetch_unevaluated_exit_decisions_skips_scored_rows() -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = str(Path(tmpdir) / "exit_outcome_candidates.db")
        db = AITraderDB(db_path)
        trade_id = db.log_trade_entry(
            symbol="BTC",
            direction="SHORT",
            entry_price=50000.0,
            size=0.1,
            venue="hyperliquid",
        )
        ts_old = time.time() - 7200.0
        decision_id = db.record_decay_decision(
            symbol="BTC",
            venue="hyperliquid",
            trade_id=int(trade_id),
            source_plan_id=1234,
            action="HOLD",
            reason="DECAY_EXIT",
            detail="plan_id=1234 action=HOLD",
            source="hl_exit_decider",
            dedupe_seconds=0.0,
            ts=ts_old,
        )
        # Not eligible: missing source_plan_id
        db.record_decay_decision(
            symbol="BTC",
            venue="hyperliquid",
            trade_id=int(trade_id),
            source_plan_id=None,
            action="HOLD",
            reason="DECAY_EXIT",
            detail="manual hold",
            source="hl_exit_decider",
            dedupe_seconds=0.0,
            ts=ts_old,
        )

        rows = db.fetch_unevaluated_exit_decisions(
            horizon_sec=3600,
            as_of_ts=time.time(),
            limit=50,
        )
        assert any(int(r["decision_rowid"]) == int(decision_id) for r in rows)

        db.record_exit_decision_outcome(
            decision_rowid=int(decision_id),
            source_plan_id=1234,
            trade_id=int(trade_id),
            symbol="BTC",
            venue="hyperliquid",
            action="HOLD",
            outcome_kind="HOLD_NO_EVENT",
            horizon_sec=3600,
            decision_ts=ts_old,
            evaluated_ts=time.time(),
        )
        rows_after = db.fetch_unevaluated_exit_decisions(
            horizon_sec=3600,
            as_of_ts=time.time(),
            limit=50,
        )
        assert not any(int(r["decision_rowid"]) == int(decision_id) for r in rows_after)

