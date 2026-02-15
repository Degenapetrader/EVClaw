#!/usr/bin/env python3
"""Regression tests for reconcile-untracked metadata attribution."""

import json
import sqlite3
import sys
import tempfile
import time
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from exchanges import Position
from executor import ExecutionConfig, Executor


class MockPositionsAdapter:
    def __init__(self, positions):
        self._positions = positions
        self._initialized = True

    async def get_all_positions(self):
        return self._positions


@pytest.mark.asyncio
async def test_reconcile_untracked_attaches_pending_metadata() -> None:
    with tempfile.TemporaryDirectory() as td:
        db_path = Path(td) / "test.db"
        cfg = ExecutionConfig(
            dry_run=True,
            db_path=db_path,
            lighter_enabled=True,
            hl_enabled=False,
            hl_wallet_enabled=False,
            enable_sltp_backstop=False,
            memory_dir=Path(td),
        )
        ex = Executor(config=cfg)

        # Avoid hitting SL/TP placement code in this test; we only verify entry attribution.
        async def _noop_protection():
            return None

        ex._place_protection_for_unprotected = _noop_protection  # type: ignore[assignment]

        # One open exchange position that is not yet in DB tracking.
        ex.lighter = MockPositionsAdapter(
            {
                "SOL": Position(
                    symbol="SOL",
                    direction="LONG",
                    size=1.75,
                    entry_price=101.25,
                    venue="lighter",
                )
            }
        )

        now = time.time()

        class _Pending:
            symbol = "SOL"
            venue = "lighter"
            direction = "LONG"
            limit_price = 101.2
            intended_size = 1.75
            exchange_order_id = "oid-sol-1"
            sr_level = 101.2
            placed_at = now - 20
            expires_at = now + 600
            entry_direction = "LONG"
            state = "FILLED"
            signals_snapshot = json.dumps({"cvd": {"direction": "LONG", "z_score": 2.4}})
            signals_agreed = json.dumps(["CVD:LONG z=2.4"])
            context_snapshot = json.dumps({"key_metrics": {"price": 101.25, "atr_pct": 1.1}})
            conviction = 0.79
            reason = "PENDING_FILL_CTX"
            sl_order_id = None
            sl_price = None

        ex.db.insert_pending_order(_Pending())

        await ex._reconcile_positions_from_exchange()

        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        try:
            row = conn.execute(
                """
                SELECT symbol, venue, signals_snapshot, signals_agreed, context_snapshot, ai_reasoning, confidence
                FROM trades
                WHERE symbol = 'SOL' AND venue = 'lighter' AND exit_time IS NULL
                ORDER BY id DESC
                LIMIT 1
                """
            ).fetchone()
        finally:
            conn.close()

        assert row is not None
        assert row["signals_snapshot"] is not None
        assert row["context_snapshot"] is not None
        assert row["signals_agreed"] not in (None, "[]")
        assert row["ai_reasoning"] == "PENDING_FILL_CTX"
        assert row["confidence"] == "0.79"

        snap = json.loads(row["signals_snapshot"])
        ctx = json.loads(row["context_snapshot"])
        agreed = json.loads(row["signals_agreed"])

        assert isinstance(snap, dict)
        assert isinstance(ctx, dict)
        assert isinstance(agreed, list) and agreed
        assert ctx.get("key_metrics", {}).get("price") == 101.25

