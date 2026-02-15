from __future__ import annotations

from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from ai_trader_db import AITraderDB


def _make_db(tmp_path: Path) -> AITraderDB:
    return AITraderDB(str(tmp_path / "decay_flags_test.db"))


def test_has_recent_decay_flag_true_within_window(tmp_path: Path) -> None:
    db = _make_db(tmp_path)
    db.record_decay_flag(
        symbol="ETH",
        venue="hyperliquid",
        trade_id=42,
        db_direction="LONG",
        live_direction="SHORT",
        reason="TEST",
        ts=1000.0,
    )

    assert db.has_recent_decay_flag(trade_id=42, since_ts=999.0) is True


def test_has_recent_decay_flag_false_outside_window_or_trade(tmp_path: Path) -> None:
    db = _make_db(tmp_path)
    db.record_decay_flag(
        symbol="ETH",
        venue="hyperliquid",
        trade_id=42,
        db_direction="LONG",
        live_direction="SHORT",
        reason="TEST",
        ts=1000.0,
    )

    assert db.has_recent_decay_flag(trade_id=42, since_ts=1001.0) is False
    assert db.has_recent_decay_flag(trade_id=99, since_ts=0.0) is False
