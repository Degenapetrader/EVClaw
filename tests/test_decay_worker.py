import json
import sys
import time
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from ai_trader_db import TradeRecord
from decay_worker import ContextSnapshot, build_parser, run_decay_once, should_close_trade
from exchanges import Position
from risk_manager import RiskConfig


def _trade(entry_offset_hours: float, direction: str = "LONG", state: str = "ACTIVE") -> TradeRecord:
    now = time.time()
    entry_time = now - entry_offset_hours * 3600
    return TradeRecord(
        id=1,
        symbol="EIGEN",
        direction=direction,
        venue="lighter",
        state=state,
        entry_time=entry_time,
        entry_price=10.0,
        size=1.0,
        notional_usd=10.0,
    )


def test_decay_max_hold_triggers():
    now = time.time()
    trade = _trade(entry_offset_hours=3.0)
    risk_cfg = RiskConfig(min_hold_hours=1.0, max_hold_hours=2.0)
    ctx = ContextSnapshot(path=None, generated_at=now, age_seconds=60.0, symbol_directions={})

    should_close, reason, _ = should_close_trade(trade, risk_cfg, ctx, now)
    assert should_close is True
    assert reason == "MAX_HOLD"


def test_decay_stale_context_triggers():
    now = time.time()
    trade = _trade(entry_offset_hours=3.0)
    risk_cfg = RiskConfig(min_hold_hours=1.0, max_hold_hours=24.0, stale_signal_max_minutes=5.0)
    ctx = ContextSnapshot(path=None, generated_at=now - 600, age_seconds=600.0, symbol_directions={})

    should_close, reason, detail = should_close_trade(trade, risk_cfg, ctx, now)
    assert should_close is True
    assert reason == "DECAY_EXIT"
    assert "stale_context" in detail


def test_decay_signal_flip_triggers():
    now = time.time()
    trade = _trade(entry_offset_hours=3.0, direction="LONG")
    risk_cfg = RiskConfig(min_hold_hours=1.0, max_hold_hours=24.0, stale_signal_max_minutes=60.0)
    ctx = ContextSnapshot(
        path=None,
        generated_at=now,
        age_seconds=30.0,
        symbol_directions={"EIGEN": "SHORT"},
    )

    should_close, reason, detail = should_close_trade(trade, risk_cfg, ctx, now)
    assert should_close is True
    assert reason == "DECAY_EXIT"
    assert "signal_flip" in detail


def test_decay_signal_flip_uses_current_direction_over_db_direction():
    now = time.time()
    trade = _trade(entry_offset_hours=3.0, direction="LONG")
    risk_cfg = RiskConfig(min_hold_hours=1.0, max_hold_hours=24.0, stale_signal_max_minutes=60.0)
    ctx = ContextSnapshot(
        path=None,
        generated_at=now,
        age_seconds=30.0,
        symbol_directions={"EIGEN": "LONG"},
    )

    should_close, reason, detail = should_close_trade(
        trade,
        risk_cfg,
        ctx,
        now,
        current_direction="SHORT",
        signal_flip_only=False,
    )
    assert should_close is True
    assert reason == "DECAY_EXIT"
    assert "SHORT->LONG" in detail


def test_decay_parser_defaults_signal_flip_only_true():
    parser = build_parser()
    args = parser.parse_args([])
    assert args.signal_flip_only is True


def test_decay_parser_allow_stale_exits_disables_signal_flip_only():
    parser = build_parser()
    args = parser.parse_args(["--allow-stale-exits"])
    assert args.signal_flip_only is False


def test_decay_parser_rejects_removed_execute_flag():
    parser = build_parser()
    with pytest.raises(SystemExit):
        parser.parse_args(["--execute"])


class _MockDB:
    def __init__(self, trades):
        self._trades = list(trades)
        self.flags = []
        self.decisions = []

    def get_open_trades(self):
        return list(self._trades)

    def record_decay_flag(self, **kwargs):
        self.flags.append(kwargs)
        return len(self.flags)

    def record_decay_decision(self, **kwargs):
        self.decisions.append(kwargs)
        return len(self.decisions)


class _MockExecutor:
    def __init__(self, *, tracked_pos: Position, exchange_pos: Position | None, exchange_error: Exception | None = None):
        self._tracked_pos = tracked_pos
        self._exchange_pos = exchange_pos
        self._exchange_error = exchange_error

    async def get_position_truth(self, symbol: str, venue: str | None = None):
        tracked = self._tracked_pos
        tracked_dir = str(getattr(tracked, "direction", "") or "").upper() if tracked else ""
        tracked_size = float(getattr(tracked, "size", 0.0) or 0.0) if tracked else 0.0
        if self._exchange_error is not None:
            return {
                "tracked_position": tracked,
                "tracked_direction": tracked_dir,
                "tracked_size": tracked_size,
                "exchange_position": None,
                "exchange_direction": "",
                "exchange_size": 0.0,
                "exchange_confirmed": False,
                "confirm_error": str(self._exchange_error),
            }
        exchange = self._exchange_pos
        exchange_dir = str(getattr(exchange, "direction", "") or "").upper() if exchange else ""
        exchange_size = float(getattr(exchange, "size", 0.0) or 0.0) if exchange else 0.0
        return {
            "tracked_position": tracked,
            "tracked_direction": tracked_dir,
            "tracked_size": tracked_size,
            "exchange_position": exchange,
            "exchange_direction": exchange_dir,
            "exchange_size": exchange_size,
            "exchange_confirmed": True,
            "confirm_error": None,
        }


def _context_file(tmp_path, *, symbol: str, direction: str):
    p = tmp_path / "context.json"
    p.write_text(
        json.dumps(
            {
                "generated_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
                "symbol_directions": {symbol: direction},
            }
        )
    )
    return p


def _pos(symbol: str, direction: str, size: float = 1.0, venue: str = "hyperliquid"):
    return Position(
        symbol=symbol,
        direction=direction,
        size=size,
        entry_price=10.0,
        unrealized_pnl=0.0,
        venue=venue,
    )


@pytest.mark.asyncio
async def test_decay_mismatch_not_confirmed_emits_reconcile_needed_no_close(tmp_path):
    trade = TradeRecord(
        id=11,
        symbol="EIGEN",
        direction="LONG",
        venue="hyperliquid",
        state="ACTIVE",
        entry_time=time.time() - 3 * 3600,
        entry_price=10.0,
        size=1.0,
        notional_usd=10.0,
    )
    db = _MockDB([trade])
    executor = _MockExecutor(
        tracked_pos=_pos("EIGEN", "SHORT"),
        exchange_pos=_pos("EIGEN", "LONG"),
    )
    risk_cfg = RiskConfig(min_hold_hours=1.0, stale_signal_max_minutes=60.0)
    context_path = _context_file(tmp_path, symbol="EIGEN", direction="LONG")

    await run_decay_once(
        executor=executor,
        db=db,
        risk_cfg=risk_cfg,
        dry_run=True,
        context_path=context_path,
        signal_flip_only=True,
        notify_only=True,
        notify_state={},
    )

    assert any(d.get("reason") == "RECONCILE_NEEDED" and d.get("action") == "HOLD" for d in db.decisions)
    assert not any(d.get("action") == "CLOSE" for d in db.decisions)


@pytest.mark.asyncio
async def test_decay_confirmed_mismatch_emits_close_plan(tmp_path):
    trade = TradeRecord(
        id=12,
        symbol="EIGEN",
        direction="LONG",
        venue="hyperliquid",
        state="ACTIVE",
        entry_time=time.time() - 3 * 3600,
        entry_price=10.0,
        size=1.0,
        notional_usd=10.0,
    )
    db = _MockDB([trade])
    executor = _MockExecutor(
        tracked_pos=_pos("EIGEN", "SHORT"),
        exchange_pos=_pos("EIGEN", "SHORT"),
    )
    risk_cfg = RiskConfig(min_hold_hours=1.0, stale_signal_max_minutes=60.0)
    context_path = _context_file(tmp_path, symbol="EIGEN", direction="LONG")

    await run_decay_once(
        executor=executor,
        db=db,
        risk_cfg=risk_cfg,
        dry_run=True,
        context_path=context_path,
        signal_flip_only=True,
        notify_only=True,
        notify_state={},
    )

    close_rows = [d for d in db.decisions if d.get("action") == "CLOSE"]
    assert len(close_rows) == 1
    payload = json.loads(close_rows[0]["detail"])
    assert payload["confirmation_status"] == "confirmed"
    assert payload["direction_source"] == "exchange"
    assert payload["live_direction"] == "SHORT"


@pytest.mark.asyncio
async def test_decay_fetch_failure_keeps_close_with_unconfirmed_tag(tmp_path):
    trade = TradeRecord(
        id=13,
        symbol="EIGEN",
        direction="LONG",
        venue="hyperliquid",
        state="ACTIVE",
        entry_time=time.time() - 3 * 3600,
        entry_price=10.0,
        size=1.0,
        notional_usd=10.0,
    )
    db = _MockDB([trade])
    executor = _MockExecutor(
        tracked_pos=_pos("EIGEN", "SHORT"),
        exchange_pos=None,
        exchange_error=RuntimeError("hl timeout"),
    )
    risk_cfg = RiskConfig(min_hold_hours=1.0, stale_signal_max_minutes=60.0)
    context_path = _context_file(tmp_path, symbol="EIGEN", direction="LONG")

    await run_decay_once(
        executor=executor,
        db=db,
        risk_cfg=risk_cfg,
        dry_run=True,
        context_path=context_path,
        signal_flip_only=True,
        notify_only=True,
        notify_state={},
    )

    close_rows = [d for d in db.decisions if d.get("action") == "CLOSE"]
    assert len(close_rows) == 1
    payload = json.loads(close_rows[0]["detail"])
    assert payload["confirmation_status"] == "unconfirmed_fetch_failed"
    assert payload["confirm_error"] == "hl timeout"
