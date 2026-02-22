#!/usr/bin/env python3
"""Learning engine policy regressions (win/loss neutrality + symmetric updates)."""

import json
import sqlite3
import tempfile
import threading
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from ai_trader_db import AITraderDB
from learning_engine import LearningEngine, Mistake


def _mk_engine(tmpdir: str) -> LearningEngine:
    db_path = str(Path(tmpdir) / "ai_trader.db")
    # Ensure schema exists before LearningEngine accesses DB.
    AITraderDB(db_path=db_path)
    return LearningEngine(db_path=db_path, memory_dir=Path(tmpdir) / "memory")


def test_learning_engine_win_loss_policy_helpers() -> None:
    with tempfile.TemporaryDirectory() as td:
        eng = _mk_engine(td)
        assert eng._is_win_pnl(1.0) is True
        assert eng._is_win_pnl(0.0) is False
        assert eng._is_win_pnl(-1.0) is False
        assert eng._is_loss_pnl(-1.0) is True
        assert eng._is_loss_pnl(0.0) is False
        assert eng._is_loss_pnl(1.0) is False


def test_learning_engine_adjustments_are_symmetric() -> None:
    with tempfile.TemporaryDirectory() as td:
        eng = _mk_engine(td)
        # New policy gates adjustments by minimum sample counts.
        # Force this unit test onto the core symmetric update path.
        eng._closed_trades_for_symbol = lambda _sym: 100  # type: ignore[assignment]
        eng._closed_trades_for_signal_direction = lambda _sig, _dir: 100  # type: ignore[assignment]
        eng._symbol_adjustments["ETH"] = 1.0

        mistake = Mistake(
            trade_id=1,
            symbol="ETH",
            direction="LONG",
            mistake_type="WRONG_DIRECTION",
            pnl=-10.0,
            pnl_pct=-1.0,
            signals_at_entry={"cvd": "LONG"},
            signals_at_exit={},
            entry_price=100.0,
            exit_price=99.0,
            hold_hours=1.0,
            lesson="test",
            avoidable=True,
        )

        eng._update_adjustments_from_mistake(mistake)
        assert abs(eng._symbol_adjustments["ETH"] - 0.98) < 1e-9
        assert abs(eng._signal_adjustments["cvd:LONG"] - 0.98) < 1e-9

        eng._update_adjustments_from_win("ETH", {"cvd": "LONG"}, "LONG")
        # 0.98 * 1.02 -> back near neutral (small residual only from compounding).
        assert abs(eng._symbol_adjustments["ETH"] - 0.9996) < 1e-9
        assert abs(eng._signal_adjustments["cvd:LONG"] - 0.9996) < 1e-9


def test_parse_signals_snapshot_invalid_json_returns_empty_dict() -> None:
    with tempfile.TemporaryDirectory() as td:
        eng = _mk_engine(td)
        assert eng._parse_signals_snapshot("{bad-json") == {}


def test_learning_engine_state_store_remains_valid_under_concurrent_updates() -> None:
    with tempfile.TemporaryDirectory() as td:
        eng = _mk_engine(td)

        mistake = Mistake(
            trade_id=1,
            symbol="SOL",
            direction="LONG",
            mistake_type="WRONG_DIRECTION",
            pnl=-5.0,
            pnl_pct=-0.5,
            signals_at_entry={"cvd": "LONG", "dead_capital": "LONG"},
            signals_at_exit={},
            entry_price=100.0,
            exit_price=99.5,
            hold_hours=0.2,
            lesson="test",
            avoidable=True,
        )

        errors: list[Exception] = []

        def _worker(i: int) -> None:
            try:
                if i % 2 == 0:
                    eng._update_adjustments_from_mistake(mistake)
                else:
                    eng._update_adjustments_from_win("SOL", {"cvd": "LONG", "dead_capital": "LONG"}, "LONG")
            except Exception as exc:  # pragma: no cover - assertion below catches failures
                errors.append(exc)

        threads = [threading.Thread(target=_worker, args=(i,)) for i in range(20)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert not errors
        # DB-backed state rows must stay parseable JSON after concurrent updates.
        db_path = str(Path(td) / "ai_trader.db")
        with sqlite3.connect(db_path) as conn:
            rows = conn.execute(
                """
                SELECT key, value_json
                FROM learning_state_kv
                WHERE key IN ('mistakes', 'patterns', 'adjustments')
                """
            ).fetchall()
        assert {k for k, _ in rows} == {"mistakes", "patterns", "adjustments"}
        for _, payload in rows:
            _ = json.loads(payload)
