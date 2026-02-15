#!/usr/bin/env python3
"""Tests for MAE/MFE runtime helpers."""

import sqlite3
import sys
import tempfile
import time
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from ai_trader_db import AITraderDB
import mae_mfe


def test_compute_mae_mfe_long() -> None:
    candles = [
        {"h": 105.0, "l": 97.5},
        {"h": 106.2, "l": 99.1},
    ]
    mae, mfe, mae_price, mfe_price = mae_mfe.compute_mae_mfe(
        direction="LONG",
        entry_price=100.0,
        candles=candles,
    )
    assert mae == pytest.approx(2.5, rel=1e-6)
    assert mfe == pytest.approx(6.2, rel=1e-6)
    assert mae_price == pytest.approx(97.5, rel=1e-6)
    assert mfe_price == pytest.approx(106.2, rel=1e-6)


@pytest.mark.asyncio
async def test_compute_and_store_mae_mfe_updates_trade() -> None:
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    db = AITraderDB(db_path)
    trade_id = db.log_trade_entry(
        symbol="ETH",
        direction="LONG",
        entry_price=100.0,
        size=1.0,
        venue="hyperliquid",
    )
    now = time.time()
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            UPDATE trades
            SET exit_time = ?, exit_price = ?, exit_reason = ?, state = 'CLOSED'
            WHERE id = ?
            """,
            (now, 103.0, "TP", trade_id),
        )
        conn.commit()

    async def _fake_fetch(_symbol: str, _start_ms: int, _end_ms: int, interval: str = "1m"):
        return [
            {"h": 104.0, "l": 98.0},
            {"h": 105.0, "l": 99.0},
        ]

    mae_mfe._fetch_candles = _fake_fetch  # type: ignore[assignment]

    ok = await mae_mfe.compute_and_store_mae_mfe(db_path=db_path, trade_id=trade_id)
    assert ok

    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT mae_pct, mfe_pct, mae_price, mfe_price FROM trades WHERE id = ?",
            (trade_id,),
        ).fetchone()

    assert row["mae_pct"] == pytest.approx(2.0, rel=1e-6)
    assert row["mfe_pct"] == pytest.approx(5.0, rel=1e-6)
    assert row["mae_price"] == pytest.approx(98.0, rel=1e-6)
    assert row["mfe_price"] == pytest.approx(105.0, rel=1e-6)


def test_load_trade_window_uses_30s_sqlite_timeout(monkeypatch) -> None:
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE trades (
                id INTEGER PRIMARY KEY,
                symbol TEXT,
                direction TEXT,
                entry_price REAL,
                entry_time REAL,
                exit_time REAL,
                mae_pct REAL,
                mfe_pct REAL
            )
            """
        )
        conn.execute(
            """
            INSERT INTO trades (id, symbol, direction, entry_price, entry_time, exit_time, mae_pct, mfe_pct)
            VALUES (1, 'ETH', 'LONG', 100.0, ?, ?, NULL, NULL)
            """,
            (time.time() - 60.0, time.time()),
        )
        conn.commit()

    original_connect = mae_mfe.sqlite3.connect
    seen: dict[str, float] = {}

    def _wrapped_connect(*args, **kwargs):
        seen["timeout"] = float(kwargs.get("timeout", 0.0) or 0.0)
        return original_connect(*args, **kwargs)

    monkeypatch.setattr(mae_mfe.sqlite3, "connect", _wrapped_connect)

    tw = mae_mfe._load_trade_window(db_path, 1)
    assert tw is not None
    assert seen.get("timeout") == pytest.approx(30.0, rel=1e-9)
