#!/usr/bin/env python3
"""Tests for fill reconciler paging + external close backstop."""

import asyncio
import sqlite3
import tempfile
import time
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from ai_trader_db import AITraderDB
from fill_reconciler import Fill, FillReconciler


class MockPagedAdapter:
    def __init__(self, fills):
        self._fills = fills

    async def get_account_trades_page(self, limit=100, cursor=None):
        start = int(cursor) if cursor else 0
        end = start + limit
        page = self._fills[start:end]
        next_cursor = str(end) if end < len(self._fills) else None
        return page, next_cursor

    async def get_all_positions(self):
        return {}


class MockFlatAdapter:
    async def get_account_trades_page(self, limit=100, cursor=None):
        return [], None

    async def get_all_positions(self):
        return {}


class MockCancelTrackingAdapter(MockPagedAdapter):
    def __init__(self):
        super().__init__([])
        self.cancel_calls = []

    async def cancel_order(self, symbol: str, order_id: str) -> bool:
        self.cancel_calls.append((symbol, order_id))
        return True


class RaceDuplicateReconciler(FillReconciler):
    """Simulate race where duplicate insert happens after idempotency precheck."""

    def _fills_exist_batch_chunked(self, pairs: list, chunk_size: int = 200) -> set:
        return set()


def _build_fill(trade_id: str, symbol: str, ts: float, side: str, price: float, size: float, sign_changed: bool) -> dict:
    return {
        "trade_id": trade_id,
        "symbol": symbol,
        "side": side,
        "size": size,
        "price": price,
        "fee": 0.0,
        "fee_maker": 0.0,
        "timestamp": ts,
        "position_sign_changed": sign_changed,
    }


def test_fill_reconciler_stores_net_pct_after_fees() -> None:
    """realized_pnl_pct should be net-of-fees to match realized_pnl semantics."""
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

    with sqlite3.connect(db_path) as conn:
        conn.execute("UPDATE trades SET entry_time = ? WHERE id = ?", (1000.0, trade_id))
        conn.commit()

    fill = _build_fill("exit-fee-1", "ETH", 2000.0, "sell", 105.0, 1.0, True)
    fill["fee"] = 1.0
    fill["fee_maker"] = 0.0
    adapter = MockPagedAdapter([fill])
    reconciler = FillReconciler(
        db_path=db_path,
        exchange_adapter=adapter,
        venue="hyperliquid",
        page_limit=50,
        max_pages=2,
        overlap_seconds=0,
    )

    processed = asyncio.run(reconciler.reconcile_now())
    assert processed > 0

    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT realized_pnl, realized_pnl_pct, total_fees FROM trades WHERE id = ?",
            (trade_id,),
        ).fetchone()

    assert row is not None
    assert float(row["realized_pnl"]) == 4.0  # gross 5 - fee 1
    assert float(row["realized_pnl_pct"]) == 4.0  # net 4 / notional 100
    assert float(row["total_fees"]) == 1.0


def test_fill_reconciler_partial_exit_reduces_open_size() -> None:
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
    with sqlite3.connect(db_path) as conn:
        conn.execute("UPDATE trades SET entry_time = ? WHERE id = ?", (1000.0, trade_id))
        conn.commit()

    partial = _build_fill("partial-1", "ETH", 2000.0, "sell", 102.0, 0.4, False)
    class _KeepOpenAdapter(MockPagedAdapter):
        async def get_all_positions(self):
            class _Pos:
                direction = "LONG"
                size = 0.6
            return {"ETH": _Pos()}

    adapter = _KeepOpenAdapter([partial])
    reconciler = FillReconciler(
        db_path=db_path,
        exchange_adapter=adapter,
        venue="hyperliquid",
        page_limit=50,
        max_pages=2,
        overlap_seconds=0,
    )
    processed = asyncio.run(reconciler.reconcile_now())
    assert processed > 0

    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT size, notional_usd, exit_time FROM trades WHERE id = ?",
            (trade_id,),
        ).fetchone()
    assert row is not None
    assert abs(float(row["size"]) - 0.6) < 1e-9
    assert abs(float(row["notional_usd"]) - 60.0) < 1e-9
    assert row["exit_time"] is None


def test_fill_reconciler_partial_exit_pnl_carries_into_final_close() -> None:
    """Final close must include realized pnl from earlier partial exits and reset partial accumulators."""
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
    with sqlite3.connect(db_path) as conn:
        conn.execute("UPDATE trades SET entry_time = ? WHERE id = ?", (1000.0, trade_id))
        conn.commit()

    fills = [
        _build_fill("partial-pnl-1", "ETH", 2000.0, "sell", 110.0, 0.4, False),  # +4.0
        _build_fill("partial-pnl-2", "ETH", 2001.0, "sell", 95.0, 0.6, True),   # -3.0
    ]
    reconciler = FillReconciler(
        db_path=db_path,
        exchange_adapter=MockPagedAdapter(fills),
        venue="hyperliquid",
        page_limit=50,
        max_pages=2,
        overlap_seconds=0,
    )

    processed = asyncio.run(reconciler.reconcile_now())
    assert processed == 2

    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT exit_time, realized_pnl, realized_pnl_pct, total_fees, "
            "realized_pnl_partial_usd, exit_fees_partial_usd FROM trades WHERE id = ?",
            (trade_id,),
        ).fetchone()

    assert row is not None
    assert row["exit_time"] is not None
    assert abs(float(row["realized_pnl"]) - 1.0) < 1e-9
    assert abs(float(row["realized_pnl_pct"]) - 1.0) < 1e-9
    assert abs(float(row["total_fees"]) - 0.0) < 1e-9
    assert abs(float(row["realized_pnl_partial_usd"]) - 0.0) < 1e-9
    assert abs(float(row["exit_fees_partial_usd"]) - 0.0) < 1e-9


def test_fill_reconciler_stale_pending_exit_reason_does_not_override_exit() -> None:
    """Stale pending_exit_reason must not override a non-SL/TP close reason."""
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

    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            UPDATE trades
            SET entry_time = ?, pending_exit_reason = ?, pending_exit_detail = ?, pending_exit_set_at = ?
            WHERE id = ?
            """,
            (1000.0, "DECAY_EXIT", "", 1.0, trade_id),
        )
        conn.commit()

    # Very old fill timestamp relative to pending_exit_set_at => stale pending reason.
    fills = [_build_fill("exit-stale-1", "ETH", 10_000_000_000.0, "sell", 101.0, 1.0, True)]
    reconciler = FillReconciler(
        db_path=db_path,
        exchange_adapter=MockPagedAdapter(fills),
        venue="hyperliquid",
        page_limit=50,
        max_pages=2,
        overlap_seconds=0,
    )

    processed = asyncio.run(reconciler.reconcile_now())
    assert processed == 1

    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT exit_time, exit_reason, pending_exit_reason, pending_exit_set_at FROM trades WHERE id = ?",
            (trade_id,),
        ).fetchone()

    assert row is not None
    assert row["exit_time"] is not None
    assert str(row["exit_reason"]).upper() == "EXIT"
    assert row["pending_exit_reason"] is None
    assert row["pending_exit_set_at"] is None


def test_fill_reconciler_does_not_match_prefixed_fill_to_plain_trade_symbol() -> None:
    """Safety: prefixed builder fills must NOT be attached to plain perps trades by base symbol."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    db = AITraderDB(db_path)
    trade_id = db.log_trade_entry(
        symbol="ZEC",
        direction="SHORT",
        entry_price=250.0,
        size=13.83,
        venue="hyperliquid",
    )
    with sqlite3.connect(db_path) as conn:
        conn.execute("UPDATE trades SET entry_time = ? WHERE id = ?", (1000.0, trade_id))
        conn.commit()

    # Builder fill should be logged as ORPHAN (no trade match) rather than closing the perps trade.
    exit_fill = _build_fill("prefixed-exit-1", "XYZ:ZEC", 2000.0, "buy", 242.07, 13.83, True)
    adapter = MockPagedAdapter([exit_fill])
    reconciler = FillReconciler(
        db_path=db_path,
        exchange_adapter=adapter,
        venue="hyperliquid",
        page_limit=50,
        max_pages=2,
        overlap_seconds=0,
    )

    processed = asyncio.run(reconciler.reconcile_now())
    assert processed > 0

    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        trade = conn.execute(
            "SELECT state, exit_reason, exit_time FROM trades WHERE id = ?",
            (trade_id,),
        ).fetchone()
        fill = conn.execute(
            "SELECT trade_id, fill_type, symbol FROM fills WHERE exchange_trade_id = ?",
            ("prefixed-exit-1",),
        ).fetchone()

    assert trade is not None
    assert str(trade["state"]).upper() == "ACTIVE"
    assert trade["exit_time"] is None

    assert fill is not None
    assert fill["trade_id"] is None
    assert str(fill["fill_type"]).upper() == "ORPHAN"
    assert str(fill["symbol"]).upper() == "XYZ:ZEC"


def test_reconcile_state_heartbeat_advances_without_new_fills() -> None:
    """Quiet venues should still refresh fill_reconcile_state.updated_at each cycle."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    AITraderDB(db_path)
    with sqlite3.connect(db_path) as conn:
        conn.execute("DELETE FROM fill_reconcile_state WHERE venue = ?", ("hyperliquid",))
        conn.commit()

    reconciler = FillReconciler(
        db_path=db_path,
        exchange_adapter=MockFlatAdapter(),
        venue="hyperliquid",
        overlap_seconds=0,
    )

    processed = asyncio.run(reconciler.reconcile_now())
    assert processed == 0

    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT last_fill_time, last_exchange_trade_id, updated_at FROM fill_reconcile_state WHERE venue = ?",
            ("hyperliquid",),
        ).fetchone()
        assert row is not None
        assert row["last_fill_time"] is None
        assert row["last_exchange_trade_id"] is None

        # Force an old timestamp, then ensure next quiet cycle bumps heartbeat.
        conn.execute(
            "UPDATE fill_reconcile_state SET updated_at = ? WHERE venue = ?",
            (1.0, "hyperliquid"),
        )
        conn.commit()

    processed = asyncio.run(reconciler.reconcile_now())
    assert processed == 0

    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            "SELECT updated_at FROM fill_reconcile_state WHERE venue = ?",
            ("hyperliquid",),
        ).fetchone()
        assert row is not None
        assert float(row[0]) > 1.0


def test_backstop_runs_on_configured_cycle_interval() -> None:
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    AITraderDB(db_path)

    class BackstopCountingReconciler(FillReconciler):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.backstop_calls = 0

        async def _backstop_external_closes(self) -> int:
            self.backstop_calls += 1
            return 0

    reconciler = BackstopCountingReconciler(
        db_path=db_path,
        exchange_adapter=MockFlatAdapter(),
        venue="hip3",
        overlap_seconds=0,
    )
    reconciler._backstop_every_cycles = 3

    asyncio.run(reconciler.reconcile_now())
    asyncio.run(reconciler.reconcile_now())
    assert reconciler.backstop_calls == 0

    asyncio.run(reconciler.reconcile_now())
    assert reconciler.backstop_calls == 1

    asyncio.run(reconciler.reconcile_now())
    asyncio.run(reconciler.reconcile_now())
    asyncio.run(reconciler.reconcile_now())
    assert reconciler.backstop_calls == 2


def test_fill_reconciler_paging_closes_trade() -> None:
    """Exit fill beyond first page still closes the trade."""
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

    with sqlite3.connect(db_path) as conn:
        conn.execute("UPDATE trades SET entry_time = ? WHERE id = ?", (1000.0, trade_id))
        conn.commit()

    # Simulate a manual/agent-requested close reason that must persist even
    # when the fee-accurate close is finalized later via fill reconciliation.
    db.set_pending_exit(trade_id, reason="DECAY_EXIT", detail="")

    fills = []
    for i in range(150):
        ts = 2000 - i
        if i == 120:
            fills.append(_build_fill("exit-120", "ETH", ts, "sell", 100.5, 1.0, True))
        else:
            fills.append(_build_fill(f"orphan-{i}", "ABC", ts, "buy", 1.0, 1.0, False))

    adapter = MockPagedAdapter(fills)
    reconciler = FillReconciler(
        db_path=db_path,
        exchange_adapter=adapter,
        venue="hyperliquid",
        page_limit=50,
        max_pages=10,
        overlap_seconds=0,
    )

    processed = asyncio.run(reconciler.reconcile_now())
    assert processed > 0

    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute("SELECT exit_time, exit_reason FROM trades WHERE id = ?", (trade_id,)).fetchone()
        assert row["exit_time"] is not None
        assert row["exit_reason"] == "DECAY_EXIT"

        state = conn.execute(
            "SELECT last_fill_time FROM fill_reconcile_state WHERE venue = ?",
            ("hyperliquid",),
        ).fetchone()
        assert state is not None
        assert state[0] == 2000


def test_fill_reconciler_duplicate_exit_fill_does_not_abort_cycle() -> None:
    """Duplicate EXIT fill races should not crash reconciliation and should still close."""
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
    with sqlite3.connect(db_path) as conn:
        conn.execute("UPDATE trades SET entry_time = ? WHERE id = ?", (1000.0, trade_id))
        conn.commit()

    # Pre-insert the same fill to emulate another worker inserting between
    # idempotency check and local insert.
    db.log_fill(
        trade_id=trade_id,
        venue="hyperliquid",
        exchange_trade_id="dup-exit-1",
        symbol="ETH",
        fill_time=2000.0,
        fill_price=105.0,
        fill_size=1.0,
        fill_type="EXIT",
        side="SELL",
        fee=0.0,
        fee_maker=0.0,
        position_sign_changed=True,
        raw_json="{}",
    )

    adapter = MockPagedAdapter([_build_fill("dup-exit-1", "ETH", 2000.0, "sell", 105.0, 1.0, True)])
    reconciler = RaceDuplicateReconciler(
        db_path=db_path,
        exchange_adapter=adapter,
        venue="hyperliquid",
        page_limit=50,
        max_pages=2,
        overlap_seconds=0,
    )

    processed = asyncio.run(reconciler.reconcile_now())
    assert processed == 1

    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        trade_row = conn.execute(
            "SELECT exit_time, exit_reason FROM trades WHERE id = ?",
            (trade_id,),
        ).fetchone()
        fill_count = conn.execute(
            "SELECT COUNT(*) FROM fills WHERE venue = ? AND exchange_trade_id = ?",
            ("hyperliquid", "dup-exit-1"),
        ).fetchone()[0]

    assert trade_row is not None
    assert trade_row["exit_time"] is not None
    assert trade_row["exit_reason"] is not None
    assert fill_count == 1


def test_orphan_reattach_classifies_exit_not_entry() -> None:
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
    db.update_trade_sltp(trade_id=trade_id, sl_price=95.0, tp_price=110.0)

    now_ts = time.time()
    with sqlite3.connect(db_path) as conn:
        conn.execute("UPDATE trades SET entry_time = ? WHERE id = ?", (now_ts - 5.0, trade_id))
        conn.execute(
            """
            INSERT INTO fills (
                trade_id, venue, exchange_trade_id, symbol, fill_time, fill_price,
                fill_size, fill_type, side, fee, fee_maker, position_sign_changed, raw_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                None,
                "hyperliquid",
                "orphan-exit-1",
                "ETH",
                now_ts - 4.0,
                111.0,
                1.0,
                "ORPHAN",
                "SELL",
                0.0,
                0.0,
                1,
                "{}",
            ),
        )
        conn.commit()

    adapter = MockPagedAdapter([])
    reconciler = FillReconciler(
        db_path=db_path,
        exchange_adapter=adapter,
        venue="hyperliquid",
        overlap_seconds=0,
    )

    attached = reconciler._reattach_orphan_fills(lookback_seconds=3600)
    assert attached == 1

    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT trade_id, fill_type FROM fills WHERE exchange_trade_id = ? AND venue = ?",
            ("orphan-exit-1", "hyperliquid"),
        ).fetchone()

    assert row is not None
    assert int(row["trade_id"]) == trade_id
    assert row["fill_type"] == "TP"


def test_orphan_reattach_prefers_clear_nearest_candidate() -> None:
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    db = AITraderDB(db_path)
    far_trade = db.log_trade_entry(
        symbol="ETH",
        direction="LONG",
        entry_price=100.0,
        size=1.0,
        venue="hyperliquid",
    )
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "UPDATE trades SET exit_time = ?, state = 'CLOSED' WHERE id = ?",
            (time.time() - 30.0, far_trade),
        )
        conn.commit()
    near_trade = db.log_trade_entry(
        symbol="ETH",
        direction="LONG",
        entry_price=101.0,
        size=1.0,
        venue="hyperliquid",
    )

    now_ts = time.time()
    with sqlite3.connect(db_path) as conn:
        conn.execute("UPDATE trades SET entry_time = ? WHERE id = ?", (now_ts - 90.0, far_trade))
        conn.execute("UPDATE trades SET entry_time = ? WHERE id = ?", (now_ts - 1.0, near_trade))
        conn.execute(
            """
            INSERT INTO fills (
                trade_id, venue, exchange_trade_id, symbol, fill_time, fill_price,
                fill_size, fill_type, side, fee, fee_maker, position_sign_changed, raw_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                None,
                "hyperliquid",
                "orphan-nearest-1",
                "ETH",
                now_ts,
                101.0,
                1.0,
                "ORPHAN",
                "BUY",
                0.0,
                0.0,
                0,
                "{}",
            ),
        )
        conn.commit()

    reconciler = FillReconciler(
        db_path=db_path,
        exchange_adapter=MockPagedAdapter([]),
        venue="hyperliquid",
        overlap_seconds=0,
    )
    attached = reconciler._reattach_orphan_fills(lookback_seconds=3600)
    assert attached == 1

    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT trade_id, fill_type FROM fills WHERE exchange_trade_id = ?",
            ("orphan-nearest-1",),
        ).fetchone()
    assert row is not None
    assert int(row["trade_id"]) == near_trade
    assert row["fill_type"] == "ENTRY"


def test_orphan_reattach_ignores_closed_candidates() -> None:
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    db = AITraderDB(db_path)
    t1 = db.log_trade_entry(
        symbol="ETH",
        direction="LONG",
        entry_price=100.0,
        size=1.0,
        venue="hyperliquid",
    )
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "UPDATE trades SET exit_time = ?, state = 'CLOSED' WHERE id = ?",
            (time.time() - 30.0, t1),
        )
        conn.commit()
    t2 = db.log_trade_entry(
        symbol="ETH",
        direction="LONG",
        entry_price=100.5,
        size=1.0,
        venue="hyperliquid",
    )

    now_ts = time.time()
    with sqlite3.connect(db_path) as conn:
        conn.execute("UPDATE trades SET entry_time = ? WHERE id = ?", (now_ts - 1.0, t1))
        conn.execute("UPDATE trades SET entry_time = ? WHERE id = ?", (now_ts + 1.5, t2))
        conn.execute(
            """
            INSERT INTO fills (
                trade_id, venue, exchange_trade_id, symbol, fill_time, fill_price,
                fill_size, fill_type, side, fee, fee_maker, position_sign_changed, raw_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                None,
                "hyperliquid",
                "orphan-ambiguous-1",
                "ETH",
                now_ts,
                100.2,
                1.0,
                "ORPHAN",
                "BUY",
                0.0,
                0.0,
                0,
                "{}",
            ),
        )
        conn.commit()

    reconciler = FillReconciler(
        db_path=db_path,
        exchange_adapter=MockPagedAdapter([]),
        venue="hyperliquid",
        overlap_seconds=0,
    )
    attached = reconciler._reattach_orphan_fills(lookback_seconds=3600)
    assert attached == 1

    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT trade_id, fill_type FROM fills WHERE exchange_trade_id = ?",
            ("orphan-ambiguous-1",),
        ).fetchone()
    assert row is not None
    assert int(row["trade_id"]) == t2
    assert row["fill_type"] == "ENTRY"


def test_fill_reconciler_external_close_backstop() -> None:
    """Open DB trades close when exchange shows flat."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    db = AITraderDB(db_path)
    trade_id = db.log_trade_entry(
        symbol="SOL",
        direction="LONG",
        entry_price=50.0,
        size=2.0,
        venue="lighter",
    )

    adapter = MockFlatAdapter()
    reconciler = FillReconciler(
        db_path=db_path,
        exchange_adapter=adapter,
        venue="lighter",
        overlap_seconds=0,
    )
    reconciler._backstop_every_cycles = 1

    processed = asyncio.run(reconciler.reconcile_now())
    assert processed == 0

    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT exit_time, exit_reason, exit_price, realized_pnl, state FROM trades WHERE id = ?",
            (trade_id,),
        ).fetchone()
        assert row["exit_time"] is not None
        assert row["exit_reason"] == "EXTERNAL"
        assert row["exit_price"] is None
        assert row["realized_pnl"] is None
        assert row["state"] == "CLOSED_EXTERNALLY"


def test_fill_reconciler_external_backstop_does_not_treat_prefixed_position_as_plain_symbol() -> None:
    """Safety: a live prefixed builder position must NOT prevent externally-closing a plain perps trade."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    db = AITraderDB(db_path)
    trade_id = db.log_trade_entry(
        symbol="ZEC",
        direction="SHORT",
        entry_price=200.0,
        size=1.0,
        venue="hyperliquid",
    )

    class _VariantPosAdapter(MockFlatAdapter):
        async def get_all_positions(self):
            class _Pos:
                direction = "SHORT"
                size = 1.0

            # Exchange shows only a builder-prefixed position (different instrument).
            return {"XYZ:ZEC": _Pos()}

    reconciler = FillReconciler(
        db_path=db_path,
        exchange_adapter=_VariantPosAdapter(),
        venue="hyperliquid",
        overlap_seconds=0,
    )
    reconciler._backstop_every_cycles = 1

    processed = asyncio.run(reconciler.reconcile_now())
    assert processed == 0

    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT exit_time, exit_reason, state FROM trades WHERE id = ?",
            (trade_id,),
        ).fetchone()
    assert row is not None
    assert row["exit_time"] is not None
    assert str(row["exit_reason"]).upper() == "EXTERNAL"
    assert str(row["state"]).upper() == "CLOSED_EXTERNALLY"


def test_fill_reconciler_external_close_inferred_price_computes_pnl() -> None:
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    db = AITraderDB(db_path)
    trade_id = db.log_trade_entry(
        symbol="SOL",
        direction="LONG",
        entry_price=50.0,
        size=2.0,
        venue="lighter",
    )
    db.log_fill(
        trade_id=trade_id,
        venue="lighter",
        exchange_trade_id="external-exit-1",
        symbol="SOL",
        fill_time=time.time(),
        fill_price=55.0,
        fill_size=2.0,
        fill_type="EXIT",
        side="SELL",
        fee=1.0,
        fee_maker=0.0,
        position_sign_changed=True,
    )

    adapter = MockFlatAdapter()
    reconciler = FillReconciler(
        db_path=db_path,
        exchange_adapter=adapter,
        venue="lighter",
        overlap_seconds=0,
    )

    processed = asyncio.run(reconciler.reconcile_now())
    assert processed == 0

    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT exit_time, exit_price, realized_pnl, realized_pnl_pct, total_fees, state FROM trades WHERE id = ?",
            (trade_id,),
        ).fetchone()
        assert row["exit_time"] is not None
        assert float(row["exit_price"]) == 55.0
        assert float(row["realized_pnl"]) == 9.0
        assert float(row["realized_pnl_pct"]) == 9.0
        assert float(row["total_fees"]) == 1.0
        assert row["state"] == "CLOSED"


def test_close_trade_from_fill_defers_when_symbol_lock_busy_then_recovers() -> None:
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    db = AITraderDB(db_path)
    trade_id = db.log_trade_entry(
        symbol="ETH",
        direction="LONG",
        entry_price=100.0,
        size=1.0,
        venue="hyperliquid",
        sl_order_id="sl_1",
        tp_order_id="tp_1",
        sl_price=95.0,
        tp_price=110.0,
    )

    now_ts = time.time()
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO symbol_locks (symbol, venue, owner, lock_type, reason, acquired_at, expires_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            ("ETH", "hyperliquid", "busy-owner", "CLOSE", "manual", now_ts, now_ts + 300.0),
        )
        conn.commit()

    adapter = MockCancelTrackingAdapter()
    reconciler = FillReconciler(
        db_path=db_path,
        exchange_adapter=adapter,
        venue="hyperliquid",
        overlap_seconds=0,
    )
    fill = Fill(
        venue="hyperliquid",
        exchange_trade_id="close-1",
        symbol="ETH",
        fill_time=now_ts,
        fill_price=105.0,
        fill_size=1.0,
        side="sell",
        fee=0.0,
        fee_maker=0.0,
        position_sign_changed=True,
        raw_json="{}",
    )

    db.log_fill(
        trade_id=trade_id,
        venue="hyperliquid",
        exchange_trade_id="close-1",
        symbol="ETH",
        fill_time=now_ts,
        fill_price=105.0,
        fill_size=1.0,
        fill_type="EXIT",
        side="SELL",
        fee=0.0,
        fee_maker=0.0,
        position_sign_changed=True,
        raw_json="{}",
    )

    closed_now = asyncio.run(reconciler._close_trade_from_fill(trade_id, fill, "EXIT"))
    assert closed_now is False

    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT exit_time, exit_reason, sl_order_id, tp_order_id FROM trades WHERE id = ?",
            (trade_id,),
        ).fetchone()

    assert row is not None
    assert row["exit_time"] is None
    assert adapter.cancel_calls == []

    with sqlite3.connect(db_path) as conn:
        conn.execute("DELETE FROM symbol_locks WHERE symbol = ? AND venue = ?", ("ETH", "hyperliquid"))
        conn.commit()

    recovered = asyncio.run(reconciler._retry_deferred_trade_closes())
    assert recovered == 1

    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT exit_time, exit_reason, sl_order_id, tp_order_id FROM trades WHERE id = ?",
            (trade_id,),
        ).fetchone()

    assert row is not None
    assert row["exit_time"] is not None
    assert row["exit_reason"] == "EXIT"
    assert row["sl_order_id"] is None
    assert row["tp_order_id"] is None
    assert sorted(adapter.cancel_calls) == [("ETH", "sl_1"), ("ETH", "tp_1")]


def test_close_trade_from_fill_releases_owned_symbol_lock() -> None:
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    db = AITraderDB(db_path)
    trade_id = db.log_trade_entry(
        symbol="ETH",
        direction="LONG",
        entry_price=100.0,
        size=1.0,
        venue="hyperliquid",
        sl_order_id="sl_lock_1",
        tp_order_id="tp_lock_1",
        sl_price=95.0,
        tp_price=110.0,
    )
    now_ts = time.time()
    adapter = MockCancelTrackingAdapter()
    reconciler = FillReconciler(
        db_path=db_path,
        exchange_adapter=adapter,
        venue="hyperliquid",
        overlap_seconds=0,
    )
    fill = Fill(
        venue="hyperliquid",
        exchange_trade_id="close-lock-1",
        symbol="ETH",
        fill_time=now_ts,
        fill_price=104.0,
        fill_size=1.0,
        side="sell",
        fee=0.0,
        fee_maker=0.0,
        position_sign_changed=True,
        raw_json="{}",
    )

    asyncio.run(reconciler._close_trade_from_fill(trade_id, fill, "EXIT"))

    with sqlite3.connect(db_path) as conn:
        lock_count = conn.execute(
            "SELECT COUNT(*) FROM symbol_locks WHERE symbol = ? AND venue = ?",
            ("ETH", "hyperliquid"),
        ).fetchone()[0]
    assert lock_count == 0
    assert len(adapter.cancel_calls) == 2


def test_close_trade_from_fill_uses_exit_fill_size_for_pnl() -> None:
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    db = AITraderDB(db_path)
    trade_id = db.log_trade_entry(
        symbol="ETH",
        direction="LONG",
        entry_price=100.0,
        size=2.0,
        venue="hyperliquid",
    )
    now_ts = time.time()
    reconciler = FillReconciler(
        db_path=db_path,
        exchange_adapter=MockCancelTrackingAdapter(),
        venue="hyperliquid",
        overlap_seconds=0,
    )
    fill = Fill(
        venue="hyperliquid",
        exchange_trade_id="close-size-1",
        symbol="ETH",
        fill_time=now_ts,
        fill_price=110.0,
        fill_size=0.5,
        side="sell",
        fee=0.0,
        fee_maker=0.0,
        position_sign_changed=True,
        raw_json="{}",
    )

    asyncio.run(reconciler._close_trade_from_fill(trade_id, fill, "EXIT"))

    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT realized_pnl, realized_pnl_pct FROM trades WHERE id = ?",
            (trade_id,),
        ).fetchone()

    assert row is not None
    assert abs(float(row["realized_pnl"]) - 5.0) < 1e-9
    assert abs(float(row["realized_pnl_pct"]) - 10.0) < 1e-9
