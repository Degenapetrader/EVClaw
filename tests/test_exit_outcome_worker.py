import sqlite3
import tempfile
import time
from pathlib import Path

from ai_trader_db import AITraderDB
from exit_outcome_worker import OutcomeConfig, evaluate_once


def test_evaluate_once_records_hold_then_sl() -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = str(Path(tmpdir) / "outcome_hold_sl.db")
        db = AITraderDB(db_path)
        trade_id = db.log_trade_entry(
            symbol="ETH",
            direction="LONG",
            entry_price=100.0,
            size=1.0,
            venue="hyperliquid",
        )
        ts_old = time.time() - 7200.0
        decision_id = db.record_decay_decision(
            symbol="ETH",
            venue="hyperliquid",
            trade_id=int(trade_id),
            source_plan_id=1,
            action="HOLD",
            reason="DECAY_EXIT",
            detail="plan_id=1 action=HOLD",
            source="hl_exit_decider",
            ts=ts_old,
            dedupe_seconds=0.0,
        )

        # Simulate SL exit that happened within the horizon window after decision.
        with sqlite3.connect(db_path) as conn:
            conn.execute(
                """
                UPDATE trades
                SET exit_time = ?, exit_reason = ?, exit_price = ?, state = 'CLOSED'
                WHERE id = ?
                """,
                (ts_old + 1200.0, "SL", 98.0, int(trade_id)),
            )
            conn.commit()

        cfg = OutcomeConfig(
            interval_sec=60.0,
            horizon_sec=3600,
            batch_size=50,
            mark_window_sec=1800.0,
            close_regret_move_pct=0.8,
        )
        stats = evaluate_once(db=db, db_path=db_path, cfg=cfg)
        assert stats["candidates"] >= 1
        assert stats["recorded"] >= 1

        with sqlite3.connect(db_path) as conn:
            row = conn.execute(
                """
                SELECT outcome_kind
                FROM exit_decision_outcomes_v1
                WHERE decision_rowid = ?
                """,
                (int(decision_id),),
            ).fetchone()
        assert row is not None
        assert str(row[0]).upper() == "HOLD_THEN_SL"


def test_evaluate_once_records_close_favorable_move_and_is_idempotent() -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = str(Path(tmpdir) / "outcome_close_move.db")
        db = AITraderDB(db_path)
        trade_id = db.log_trade_entry(
            symbol="BTC",
            direction="LONG",
            entry_price=100.0,
            size=1.0,
            venue="hyperliquid",
        )
        ts_old = time.time() - 7200.0
        horizon_sec = 3600
        decision_id = db.record_decay_decision(
            symbol="BTC",
            venue="hyperliquid",
            trade_id=int(trade_id),
            source_plan_id=99,
            action="CLOSE",
            reason="DECAY_EXIT",
            detail="plan_id=99 action=CLOSE",
            source="hl_exit_decider",
            ts=ts_old,
            dedupe_seconds=0.0,
        )

        with sqlite3.connect(db_path) as conn:
            conn.execute(
                """
                INSERT INTO monitor_snapshots (ts, ts_iso)
                VALUES (?, ?)
                """,
                (ts_old, "decision"),
            )
            snap_decision_id = int(conn.execute("SELECT last_insert_rowid()").fetchone()[0])
            conn.execute(
                """
                INSERT INTO monitor_positions (
                    snapshot_id, source, symbol, direction, size, entry_price, unrealized_pnl, venue
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (snap_decision_id, "hyperliquid", "BTC", "LONG", 1.0, 100.0, 0.0, "hyperliquid"),
            )

            conn.execute(
                """
                INSERT INTO monitor_snapshots (ts, ts_iso)
                VALUES (?, ?)
                """,
                (ts_old + horizon_sec, "horizon"),
            )
            snap_horizon_id = int(conn.execute("SELECT last_insert_rowid()").fetchone()[0])
            conn.execute(
                """
                INSERT INTO monitor_positions (
                    snapshot_id, source, symbol, direction, size, entry_price, unrealized_pnl, venue
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (snap_horizon_id, "hyperliquid", "BTC", "LONG", 1.0, 100.0, 2.0, "hyperliquid"),
            )
            conn.commit()

        cfg = OutcomeConfig(
            interval_sec=60.0,
            horizon_sec=horizon_sec,
            batch_size=50,
            mark_window_sec=600.0,
            close_regret_move_pct=0.8,
        )

        stats1 = evaluate_once(db=db, db_path=db_path, cfg=cfg)
        assert stats1["recorded"] >= 1
        stats2 = evaluate_once(db=db, db_path=db_path, cfg=cfg)
        assert stats2["recorded"] == 0

        with sqlite3.connect(db_path) as conn:
            row = conn.execute(
                """
                SELECT outcome_kind
                FROM exit_decision_outcomes_v1
                WHERE decision_rowid = ?
                """,
                (int(decision_id),),
            ).fetchone()
        assert row is not None
        assert str(row[0]).upper() == "CLOSE_THEN_FAVORABLE_MOVE"

