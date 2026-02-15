#!/usr/bin/env python3
"""Tests for unified symbol-learning dossier aggregation."""

import json
import sqlite3
import sys
import tempfile
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import learning_dossier_aggregator as lda
from ai_trader_db import AITraderDB
from learning_dossier_aggregator import (
    get_dossier_snippet,
    update_from_new_reflections,
    update_from_trade_close,
)


def _close_trade(db_path: str, trade_id: int, pnl: float, pnl_pct: float, reason: str) -> None:
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            UPDATE trades
            SET
                exit_time = ?,
                exit_price = ?,
                exit_reason = ?,
                realized_pnl = ?,
                realized_pnl_pct = ?,
                state = 'CLOSED'
            WHERE id = ?
            """,
            (
                time.time(),
                101.0,
                str(reason),
                float(pnl),
                float(pnl_pct),
                int(trade_id),
            ),
        )
        conn.commit()


def test_update_from_trade_close_builds_symbol_state_and_signal_stats() -> None:
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    db = AITraderDB(db_path)

    t1 = db.log_trade_entry(
        symbol="ETH",
        direction="LONG",
        entry_price=100.0,
        size=1.0,
        venue="hyperliquid",
        signals_snapshot={"cvd": {"direction": "LONG", "z_score": 2.1}},
        signals_agreed=["CVD:LONG z=2.1", "WHALE:LONG str=0.7"],
        context_snapshot={"key_metrics": {"price": 100.0}},
    )
    db.update_trade_sltp(
        t1,
        sl_price=98.0,
        tp_price=103.0,
        protection_snapshot={"sl_mult": 2.0, "tp_mult": 3.0, "used_fallback": False},
    )
    _close_trade(db_path, t1, pnl=2.0, pnl_pct=2.0, reason="TP")
    db.update_trade_mae_mfe(t1, mae_pct=1.0, mfe_pct=4.5, mae_price=99.0, mfe_price=104.5)

    t2 = db.log_trade_entry(
        symbol="ETH",
        direction="SHORT",
        entry_price=120.0,
        size=1.0,
        venue="hyperliquid",
        signals_snapshot={"cvd": {"direction": "SHORT", "z_score": 2.0}},
        signals_agreed=["CVD:SHORT z=2.0"],
        context_snapshot={"key_metrics": {"price": 120.0}},
    )
    db.update_trade_sltp(
        t2,
        sl_price=123.0,
        tp_price=116.0,
        protection_snapshot={"sl_mult": 1.5, "tp_mult": 2.0, "used_fallback": True},
    )
    _close_trade(db_path, t2, pnl=-1.0, pnl_pct=-1.0, reason="SL")
    db.update_trade_mae_mfe(t2, mae_pct=2.1, mfe_pct=0.8, mae_price=122.5, mfe_price=119.0)

    # Uninstrumented trade should count for overall stats but not signal rankings.
    t3 = db.log_trade_entry(
        symbol="ETH",
        direction="LONG",
        entry_price=130.0,
        size=1.0,
        venue="hyperliquid",
        signals_snapshot=None,
        signals_agreed=[],
    )
    _close_trade(db_path, t3, pnl=0.5, pnl_pct=0.5, reason="EXIT")

    ok = update_from_trade_close(db_path, t2)
    assert ok

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        state = conn.execute(
            """
            SELECT
                symbol, n_closed, win_rate, expectancy, avg_pnl_pct,
                sltp_fallback_rate, typical_sl_mult, typical_tp_mult
            FROM symbol_learning_state
            WHERE symbol = 'ETH'
            """
        ).fetchone()
        assert state is not None
        assert state["symbol"] == "ETH"
        assert int(state["n_closed"]) == 3
        assert float(state["win_rate"]) == 2.0 / 3.0
        assert float(state["sltp_fallback_rate"]) == 0.5
        assert float(state["typical_sl_mult"]) == 1.75
        assert float(state["typical_tp_mult"]) == 2.5

        sig_rows = conn.execute(
            "SELECT signal, direction, n FROM signal_symbol_stats WHERE symbol = 'ETH' ORDER BY signal, direction"
        ).fetchall()
        # Instrumented trades only: CVD/LONG, CVD/SHORT, WHALE/LONG.
        assert len(sig_rows) == 3
        keys = {(str(r["signal"]), str(r["direction"])) for r in sig_rows}
        assert ("CVD", "LONG") in keys
        assert ("CVD", "SHORT") in keys
        assert ("WHALE", "LONG") in keys
    finally:
        conn.close()


def test_reflection_incremental_update_and_snippet_cap() -> None:
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    db = AITraderDB(db_path)
    trade_id = db.log_trade_entry(
        symbol="SOL",
        direction="LONG",
        entry_price=100.0,
        size=1.0,
        venue="lighter",
        signals_snapshot={"cvd": {"direction": "LONG", "z_score": 2.2}},
        signals_agreed=["CVD:LONG z=2.2"],
    )
    _close_trade(db_path, trade_id, pnl=1.0, pnl_pct=1.0, reason="TP")
    db.update_trade_mae_mfe(trade_id, mae_pct=0.7, mfe_pct=2.2, mae_price=99.3, mfe_price=102.2)

    assert update_from_trade_close(db_path, trade_id)

    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO reflections_v2 (trade_id, reflection_json, lesson_text, confidence, created_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                trade_id,
                json.dumps({"lesson": "Only enter after SR reclaim confirms trend."}),
                "Wait for SR reclaim before entry.",
                "HIGH",
                time.time(),
            ),
        )
        conn.commit()

    updated = update_from_new_reflections(db_path, "SOL")
    assert updated == 1
    # Incremental behavior: second run should do nothing.
    assert update_from_new_reflections(db_path, "SOL") == 0

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        state = conn.execute(
            "SELECT notes_summary, last_reflection_id_seen FROM symbol_learning_state WHERE symbol = 'SOL'"
        ).fetchone()
        assert state is not None
        assert int(state["last_reflection_id_seen"]) > 0
        assert "SR reclaim" in str(state["notes_summary"] or "")
    finally:
        conn.close()

    snippet = get_dossier_snippet(db_path, "SOL", max_chars=200)
    assert isinstance(snippet, str) and snippet
    assert len(snippet) <= 200
    assert "SOL" in snippet


def test_dossier_snippet_throttles_reflection_refresh() -> None:
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    db = AITraderDB(db_path)
    trade_id = db.log_trade_entry(
        symbol="ADA",
        direction="LONG",
        entry_price=1.0,
        size=100.0,
        venue="lighter",
        signals_snapshot={"cvd": {"direction": "LONG", "z_score": 2.0}},
        signals_agreed=["CVD:LONG z=2.0"],
    )
    _close_trade(db_path, trade_id, pnl=5.0, pnl_pct=5.0, reason="TP")
    assert update_from_trade_close(db_path, trade_id)

    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO reflections_v2 (trade_id, reflection_json, lesson_text, confidence, created_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (trade_id, json.dumps({"lesson": "FIRST LESSON"}), "FIRST LESSON", "HIGH", time.time()),
        )
        conn.commit()

    first = get_dossier_snippet(db_path, "ADA", max_chars=600)
    assert "FIRST LESSON" in first

    trade_id_2 = db.log_trade_entry(
        symbol="ADA",
        direction="SHORT",
        entry_price=1.1,
        size=100.0,
        venue="lighter",
        signals_snapshot={"ofm": {"direction": "SHORT", "z_score": 2.1}},
        signals_agreed=["OFM:SHORT z=2.1"],
    )
    _close_trade(db_path, trade_id_2, pnl=-2.0, pnl_pct=-2.0, reason="SL")
    assert update_from_trade_close(db_path, trade_id_2)

    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO reflections_v2 (trade_id, reflection_json, lesson_text, confidence, created_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (trade_id_2, json.dumps({"lesson": "SECOND LESSON"}), "SECOND LESSON", "HIGH", time.time() + 1.0),
        )
        conn.commit()

    second = get_dossier_snippet(db_path, "ADA", max_chars=600)
    # Refresh is throttled; second lesson should not appear yet.
    assert "SECOND LESSON" not in second

    assert update_from_new_reflections(db_path, "ADA") == 1
    third = get_dossier_snippet(db_path, "ADA", max_chars=600)
    assert "SECOND LESSON" in third


def test_get_dossier_snippet_does_not_lazy_recompute_on_entry_path(monkeypatch) -> None:
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    db = AITraderDB(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            INSERT OR REPLACE INTO symbol_learning_state (
                symbol, updated_at, n_closed, win_rate, expectancy, avg_pnl_pct,
                notes_summary, last_reflection_id_seen
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            ("DOGE", time.time(), 0, 0.0, 0.0, 0.0, "notes only", 0),
        )
        conn.commit()

    def _boom(*_args, **_kwargs):
        raise AssertionError("entry path should not trigger trade-close recompute")

    monkeypatch.setattr(lda, "update_from_trade_close", _boom)
    assert get_dossier_snippet(db_path, "DOGE", max_chars=300) == ""
