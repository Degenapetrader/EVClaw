#!/usr/bin/env python3
"""
Executor tests (dry run only).

Tests Chase Limit execution, ATR-based SL/TP, and position lifecycle.
"""

import asyncio
import json
import sqlite3
import sys
import tempfile
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from executor import ExecutionConfig, ExecutionDecision, Executor
from exchanges import Position, VENUE_HYPERLIQUID, VENUE_HYPERLIQUID_WALLET, VENUE_LIGHTER


class _CloseAdapter:
    def __init__(self, name: str, *, direction: str = "LONG", size: float = 1.0) -> None:
        self.name = name
        self._initialized = True
        self._direction = direction
        self._size = size
        self._pos_calls = 0
        self.cancel_all_calls = []

    async def cancel_all_orders(self, symbol: str) -> bool:
        self.cancel_all_calls.append(symbol)
        return True

    async def get_position(self, symbol: str):
        self._pos_calls += 1
        if self._pos_calls == 1:
            return Position(
                symbol=symbol,
                direction=self._direction,
                size=self._size,
                entry_price=100.0,
                unrealized_pnl=0.0,
            )
        return Position(
            symbol=symbol,
            direction="FLAT",
            size=0.0,
            entry_price=0.0,
            unrealized_pnl=0.0,
        )


async def test_executor_create_initializes_instance() -> None:
    calls = {"count": 0}

    async def _fake_initialize(self) -> bool:
        calls["count"] += 1
        self._initialized = True
        return True

    original = Executor.initialize
    Executor.initialize = _fake_initialize  # type: ignore[assignment]
    try:
        ex = await Executor.create(config=ExecutionConfig(dry_run=True))
        assert isinstance(ex, Executor)
        assert calls["count"] == 1
    finally:
        Executor.initialize = original  # type: ignore[assignment]


async def test_executor_create_raises_on_initialize_failure() -> None:
    async def _fake_initialize(_self) -> bool:
        return False

    original = Executor.initialize
    Executor.initialize = _fake_initialize  # type: ignore[assignment]
    try:
        with pytest.raises(RuntimeError, match="initialization failed"):
            await Executor.create(config=ExecutionConfig(dry_run=True))
    finally:
        Executor.initialize = original  # type: ignore[assignment]


async def test_executor_chase_limit_dry_run() -> None:
    """Chase Limit entry + close via dry-run adapters."""
    with tempfile.TemporaryDirectory() as tmpdir:
        config = ExecutionConfig(
            dry_run=True,
            enable_sltp_backstop=False,
            memory_dir=Path(tmpdir),
            # Fast timeouts for test
            chase_poll_interval=0.1,
            chase_timeout=5.0,
        )
        executor = Executor(config=config)
        await executor.initialize()

        decision = ExecutionDecision(
            symbol="ETH",
            direction="LONG",
            size_multiplier=1.0,
            size_usd=1000.0,
            signals_agreeing=["cvd"],
            conviction=0.8,
            reason="test",
        )

        result = await executor.execute(
            decision=decision,
            best_bid=3000.0,
            best_ask=3001.0,
            atr=50.0,
            venue="lighter",
        )

        assert result.success, f"Entry failed: {result.error}"
        assert result.entry_price > 0
        assert result.size > 0

        positions = await executor.get_all_positions()
        assert "lighter:ETH" in positions

        closed = await executor.close_position(
            symbol="ETH",
            reason="test",
            best_bid=3002.0,
            best_ask=3003.0,
            venue="lighter",
        )
        assert closed

        positions = await executor.get_all_positions()
        assert "lighter:ETH" not in positions


async def test_executor_sltp_backstop() -> None:
    """SL/TP are placed when enable_sltp_backstop=True."""
    with tempfile.TemporaryDirectory() as tmpdir:
        config = ExecutionConfig(
            dry_run=True,
            enable_sltp_backstop=True,
            sl_atr_multiplier=2.0,
            tp_atr_multiplier=3.0,
            memory_dir=Path(tmpdir),
            chase_poll_interval=0.1,
            chase_timeout=5.0,
        )
        executor = Executor(config=config)
        await executor.initialize()

        decision = ExecutionDecision(
            symbol="ETH",
            direction="LONG",
            size_multiplier=1.0,
            size_usd=1000.0,
            signals_agreeing=["cvd"],
            conviction=0.8,
            reason="test",
        )

        result = await executor.execute(
            decision=decision,
            best_bid=3000.0,
            best_ask=3001.0,
            atr=50.0,
            venue="lighter",
        )

        assert result.success, f"Entry failed: {result.error}"
        # SL = entry - (ATR × 2.0) = 3001 - 100 = ~2901
        # TP = entry + (ATR × 3.0) = 3001 + 150 = ~3151
        assert result.sl_price > 0, "SL should be set"
        assert result.tp_price > 0, "TP should be set"
        assert result.sl_price < result.entry_price, "LONG SL below entry"
        assert result.tp_price > result.entry_price, "LONG TP above entry"


async def test_executor_short_sltp() -> None:
    """SL/TP directions correct for SHORT."""
    with tempfile.TemporaryDirectory() as tmpdir:
        config = ExecutionConfig(
            dry_run=True,
            enable_sltp_backstop=True,
            sl_atr_multiplier=2.0,
            tp_atr_multiplier=3.0,
            memory_dir=Path(tmpdir),
            chase_poll_interval=0.1,
            chase_timeout=5.0,
        )
        executor = Executor(config=config)
        await executor.initialize()

        decision = ExecutionDecision(
            symbol="BTC",
            direction="SHORT",
            size_multiplier=1.0,
            size_usd=1000.0,
            signals_agreeing=["ofm"],
            conviction=0.7,
            reason="test",
        )

        result = await executor.execute(
            decision=decision,
            best_bid=60000.0,
            best_ask=60010.0,
            atr=500.0,
            venue="lighter",
        )

        assert result.success, f"Entry failed: {result.error}"
        # SHORT: SL above entry, TP below entry
        assert result.sl_price > result.entry_price, "SHORT SL above entry"
        assert result.tp_price < result.entry_price, "SHORT TP below entry"


def test_extract_sizing_tracking_uses_venue_maps() -> None:
    decision = ExecutionDecision(
        symbol="ETH",
        direction="LONG",
        size_usd=1000.0,
    )
    decision.risk = {
        "risk_pct_used_by_venue": {"hyperliquid": 1.7},
        "equity_at_entry_by_venue": {"hyperliquid": 14000.0},
        "size_multiplier_effective": 0.92,
    }
    risk_pct, equity, size_mult = Executor._extract_sizing_tracking_from_decision(
        decision,
        venue="hyperliquid",
    )
    assert risk_pct == 1.7
    assert equity == 14000.0
    assert size_mult == 0.92


def test_calculate_sl_tp_applies_adaptive_scaling_on_agi_override() -> None:
    class _Adaptive:
        def get_multipliers(self, symbol, regime):
            assert symbol == "XYZ:NVDA"
            assert regime == "NORMAL"
            return 1.5, 2.0

    with tempfile.TemporaryDirectory() as tmpdir:
        config = ExecutionConfig(
            dry_run=True,
            enable_sltp_backstop=True,
            sl_atr_multiplier=2.0,
            tp_atr_multiplier=3.0,
            memory_dir=Path(tmpdir),
        )
        executor = Executor(config=config, adaptive_sltp=_Adaptive())
        _sl, _tp, meta = executor._calculate_sl_tp(
            direction="LONG",
            entry_price=100.0,
            atr=10.0,
            symbol="XYZ:NVDA",
            volatility_regime="NORMAL",
            sl_mult_override=2.0,
            tp_mult_override=3.0,
        )

    assert meta["agi_override_used"] is True
    assert meta["adaptive_used"] is True
    assert meta["adaptive_on_override"] is True
    assert meta["sl_mult"] == pytest.approx(1.5)
    assert meta["tp_mult"] == pytest.approx(2.0)


def test_build_learning_context_snapshot_includes_hip3_fields() -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        config = ExecutionConfig(
            dry_run=True,
            enable_sltp_backstop=False,
            memory_dir=Path(tmpdir),
        )
        executor = Executor(config=config)
        decision = ExecutionDecision(
            symbol="XYZ:NVDA",
            direction="LONG",
            size_usd=1000.0,
        )
        decision.risk = {
            "hip3_driver": "flow",
            "hip3_booster_size_mult": 1.2,
        }
        decision.signals_snapshot = {
            "hip3_main": {
                "flow_pass": True,
                "ofm_pass": False,
                "components": {
                    "rest_booster_score": 1.7,
                    "rest_booster_size_mult": 1.2,
                },
            }
        }
        decision.context_snapshot = {"key_metrics": {"price": 500.0}}

        context = executor._build_learning_context_snapshot(
            decision=decision,
            symbol="XYZ:NVDA",
            direction="LONG",
            venue="hip3",
            order_type="chase_limit",
            execution_route="executor_chase",
        )

    assert context["entry_gate_mode"] == "hip3"
    assert context["strategy_segment"] == "hip3"
    assert context["hip3_driver"] == "flow"
    assert context["hip3_flow_pass"] is True
    assert context["hip3_ofm_pass"] is False
    assert context["hip3_booster_score"] == pytest.approx(1.7)
    assert context["hip3_booster_size_mult"] == pytest.approx(1.2)
    assert isinstance(context.get("risk"), dict)


async def test_executor_logs_sizing_tracking_fields() -> None:
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    with tempfile.TemporaryDirectory() as tmpdir:
        config = ExecutionConfig(
            dry_run=True,
            enable_sltp_backstop=False,
            db_path=db_path,
            memory_dir=Path(tmpdir),
            chase_poll_interval=0.1,
            chase_timeout=5.0,
        )
        executor = Executor(config=config)
        await executor.initialize()

        decision = ExecutionDecision(
            symbol="ETH",
            direction="LONG",
            size_multiplier=1.0,
            size_usd=1000.0,
            signals_agreeing=["cvd"],
            conviction=0.8,
            reason="tracking test",
        )
        decision.risk = {
            "risk_pct_used": 1.8,
            "equity_at_entry": 14000.0,
            "size_multiplier_effective": 0.91,
        }

        result = await executor.execute(
            decision=decision,
            best_bid=3000.0,
            best_ask=3001.0,
            atr=50.0,
            venue="lighter",
        )
        assert result.success, f"Entry failed: {result.error}"

        with sqlite3.connect(db_path) as conn:
            row = conn.execute(
                """
                SELECT risk_pct_used, equity_at_entry, size_multiplier, context_snapshot
                FROM trades
                ORDER BY id DESC
                LIMIT 1
                """
            ).fetchone()
        assert row is not None
        assert float(row[0]) == 1.8
        assert float(row[1]) == 14000.0
        assert float(row[2]) == 0.91
        ctx = json.loads(row[3]) if row[3] else {}
        assert ctx.get("order_type") == "chase_limit"
        assert ctx.get("execution_route") == "executor_chase"


async def test_executor_venue_validation() -> None:
    """Venue validation rejects invalid venue+symbol combos."""
    with tempfile.TemporaryDirectory() as tmpdir:
        config = ExecutionConfig(
            dry_run=True,
            enable_sltp_backstop=False,
            hl_wallet_enabled=True,
            memory_dir=Path(tmpdir),
            chase_poll_interval=0.1,
            chase_timeout=5.0,
        )
        executor = Executor(config=config)
        await executor.initialize()

        decision = ExecutionDecision(
            symbol="xyz:NVDA",
            direction="LONG",
            size_multiplier=1.0,
            size_usd=1000.0,
            signals_agreeing=["cvd"],
            conviction=0.8,
            reason="test",
        )

        # HIP3 symbol on Lighter is auto-remapped to Hyperliquid.
        result = await executor.execute(
            decision=decision,
            best_bid=100.0,
            best_ask=100.1,
            venue="lighter",
        )
        assert result.success

        # Unknown venue should fail
        perp_decision = ExecutionDecision(
            symbol="ETH",
            direction="LONG",
            size_multiplier=1.0,
            size_usd=1000.0,
            signals_agreeing=["cvd"],
            conviction=0.8,
            reason="test",
        )
        result = await executor.execute(
            decision=perp_decision,
            best_bid=100.0,
            best_ask=100.1,
            venue="binance",
        )
        assert not result.success
        assert "Unknown venue" in result.error

        # Valid venue alias should succeed.
        decision2 = ExecutionDecision(
            symbol="xyz:TSLA",
            direction="LONG",
            size_multiplier=1.0,
            size_usd=1000.0,
            signals_agreeing=["cvd"],
            conviction=0.8,
            reason="test",
        )
        result = await executor.execute(
            decision=decision2,
            best_bid=100.0,
            best_ask=100.1,
            venue="hip3",
        )
        assert result.success


async def test_executor_auto_resolves_hip3_wallet_venue() -> None:
    """HIP3 execution without explicit venue should map to unified hyperliquid key."""
    with tempfile.TemporaryDirectory() as tmpdir:
        config = ExecutionConfig(
            dry_run=True,
            enable_sltp_backstop=False,
            hl_wallet_enabled=True,
            memory_dir=Path(tmpdir),
            chase_poll_interval=0.1,
            chase_timeout=5.0,
        )
        executor = Executor(config=config)
        await executor.initialize()

        decision = ExecutionDecision(
            symbol="xyz:NVDA",
            direction="LONG",
            size_multiplier=1.0,
            size_usd=1000.0,
            signals_agreeing=["cvd"],
            conviction=0.8,
            reason="auto venue test",
        )

        result = await executor.execute(
            decision=decision,
            best_bid=100.0,
            best_ask=100.1,
            atr=1.0,
            venue=None,
        )
        assert result.success, f"Entry failed: {result.error}"

        positions = await executor.get_all_positions()
        assert "hyperliquid:xyz:NVDA" in positions


async def test_chase_limit_config_defaults() -> None:
    """Verify Chase Limit config defaults match requirements."""
    config = ExecutionConfig()
    assert config.chase_tick_offset == 2, "Chase offset should be ±2 ticks"
    assert config.chase_poll_interval == 5.0, "Chase poll should be 5s"
    assert config.chase_timeout == 300.0, "Chase timeout should be 300s"
    assert config.chase_retrigger_ticks == 2, "Chase retrigger should be 2 ticks"
    assert config.enable_sltp_backstop is True, "SL/TP should be enabled by default"
    assert config.sl_atr_multiplier == 2.0, "SL multiplier should be 2.0"
    assert config.tp_atr_multiplier == 3.0, "TP multiplier should be 3.0"


async def test_round_to_tick() -> None:
    """Verify tick rounding utility."""
    r = Executor._round_to_tick
    assert r(100.123, 0.01) == 100.12
    assert r(100.126, 0.01) == 100.13
    assert r(100.1, 0.5) == 100.0
    assert r(100.3, 0.5) == 100.5
    assert r(0.0, 0.01) == 0.0
    # Zero tick_size returns price unchanged
    assert r(100.123, 0.0) == 100.123


async def test_close_position_prefers_tracked_position_venue_when_unspecified() -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        config = ExecutionConfig(
            dry_run=True,
            lighter_enabled=False,
            hl_enabled=True,
            hl_wallet_enabled=True,
            write_positions_yaml=False,
            memory_dir=Path(tmpdir),
        )
        executor = Executor(config=config)

        hl_adapter = _CloseAdapter("hl", direction="LONG", size=1.0)
        wallet_adapter = _CloseAdapter("hl_wallet", direction="LONG", size=1.0)
        executor.hyperliquid = hl_adapter
        executor.hip3_wallet = wallet_adapter
        executor.router.hyperliquid = hl_adapter
        executor.router.hip3_wallet = wallet_adapter
        executor.router.select = lambda _symbol: hl_adapter  # would be wrong for wallet position

        key = executor._position_key("ETH", VENUE_HYPERLIQUID_WALLET)
        executor._positions[key] = Position(
            symbol="ETH",
            direction="LONG",
            size=1.0,
            entry_price=100.0,
            state="ACTIVE",
            venue=VENUE_HYPERLIQUID_WALLET,
        )

        captured = {}

        async def _fake_chase(*, adapter, **kwargs):
            captured["adapter"] = adapter
            return True, 1.0, 101.0

        executor._chase_limit_fill = _fake_chase
        executor.persistence.append_trade = lambda _payload: True
        executor.persistence.save_positions_atomic = lambda _positions: True

        ok = await executor.close_position(
            symbol="ETH",
            reason="test",
            best_bid=100.0,
            best_ask=101.0,
            venue=None,
        )
        assert ok is True
        assert captured.get("adapter") is hl_adapter
        assert hl_adapter.cancel_all_calls == ["ETH"]
        assert wallet_adapter.cancel_all_calls == []


async def test_close_position_releases_symbol_lock_when_chase_raises() -> None:
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    with tempfile.TemporaryDirectory() as tmpdir:
        config = ExecutionConfig(
            dry_run=True,
            lighter_enabled=True,
            hl_enabled=False,
            db_path=db_path,
            write_positions_yaml=False,
            memory_dir=Path(tmpdir),
        )
        executor = Executor(config=config)
        adapter = _CloseAdapter("lighter", direction="LONG", size=1.0)
        executor.lighter = adapter
        executor.router.lighter = adapter

        key = executor._position_key("ETH", VENUE_LIGHTER)
        executor._positions[key] = Position(
            symbol="ETH",
            direction="LONG",
            size=1.0,
            entry_price=100.0,
            state="ACTIVE",
            venue=VENUE_LIGHTER,
        )

        async def _raise_chase(**_kwargs):
            raise RuntimeError("boom")

        executor._chase_limit_fill = _raise_chase
        ok = await executor.close_position(
            symbol="ETH",
            reason="test",
            best_bid=99.5,
            best_ask=100.5,
            venue=VENUE_LIGHTER,
        )
        assert ok is False

        with sqlite3.connect(db_path) as conn:
            c = conn.execute(
                "SELECT COUNT(*) FROM symbol_locks WHERE symbol=? AND venue=?",
                ("ETH", VENUE_LIGHTER),
            ).fetchone()[0]
        assert int(c) == 0


async def test_get_position_truth_reports_exchange_confirmation_and_mismatch() -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        config = ExecutionConfig(
            dry_run=True,
            lighter_enabled=True,
            hl_enabled=False,
            write_positions_yaml=False,
            memory_dir=Path(tmpdir),
        )
        executor = Executor(config=config)
        adapter = _CloseAdapter("lighter", direction="SHORT", size=2.0)
        executor.lighter = adapter
        executor.router.lighter = adapter

        key = executor._position_key("ETH", VENUE_LIGHTER)
        executor._positions[key] = Position(
            symbol="ETH",
            direction="LONG",
            size=1.0,
            entry_price=100.0,
            state="ACTIVE",
            venue=VENUE_LIGHTER,
        )

        truth = await executor.get_position_truth("ETH", venue=VENUE_LIGHTER)
        assert truth["tracked_direction"] == "LONG"
        assert truth["exchange_direction"] == "SHORT"
        assert truth["exchange_confirmed"] is True
        assert truth["confirm_error"] is None


async def test_get_live_positions_by_venue_surfaces_adapter_failure() -> None:
    class _FailingAdapter(_CloseAdapter):
        async def get_all_positions(self):
            raise RuntimeError("snapshot error")

    with tempfile.TemporaryDirectory() as tmpdir:
        config = ExecutionConfig(
            dry_run=True,
            lighter_enabled=True,
            hl_enabled=False,
            write_positions_yaml=False,
            memory_dir=Path(tmpdir),
        )
        executor = Executor(config=config)
        adapter = _FailingAdapter("lighter")
        executor.lighter = adapter
        executor.router.lighter = adapter

        with pytest.raises(RuntimeError, match="get_all_positions failed"):
            await executor.get_live_positions_by_venue(VENUE_LIGHTER)


async def run() -> None:
    await test_executor_create_initializes_instance()
    print("[PASS] executor create initializes instance")
    await test_executor_create_raises_on_initialize_failure()
    print("[PASS] executor create raises on init failure")
    await test_executor_chase_limit_dry_run()
    print("[PASS] executor chase limit dry run")
    await test_executor_sltp_backstop()
    print("[PASS] executor SL/TP backstop")
    await test_executor_short_sltp()
    print("[PASS] executor short SL/TP directions")
    await test_executor_venue_validation()
    print("[PASS] executor venue validation")
    await test_executor_auto_resolves_hip3_wallet_venue()
    print("[PASS] executor auto venue resolves hip3 wallet")
    await test_chase_limit_config_defaults()
    print("[PASS] chase limit config defaults")
    await test_round_to_tick()
    print("[PASS] round_to_tick utility")
    await test_close_position_prefers_tracked_position_venue_when_unspecified()
    print("[PASS] close_position prefers tracked venue when unspecified")
    await test_close_position_releases_symbol_lock_when_chase_raises()
    print("[PASS] close_position releases lock on chase exception")
    await test_get_position_truth_reports_exchange_confirmation_and_mismatch()
    print("[PASS] get_position_truth reports tracked vs exchange state")
    await test_get_live_positions_by_venue_surfaces_adapter_failure()
    print("[PASS] get_live_positions_by_venue surfaces adapter failure")


if __name__ == "__main__":
    asyncio.run(run())
