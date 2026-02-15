"""Tests for SR-anchored pending limit manager (v0)."""

import pytest
import logging
import sys
from pathlib import Path

# Add parent dir to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from pending_limit import PendingLimit, PendingLimitManager


class MockDB:
    """Mock database for testing (stores pending_orders rows as dicts)."""

    def __init__(self):
        self.pending_orders: list[dict] = []

    def insert_pending_order(self, p):
        row = {
            'symbol': p.symbol,
            'venue': p.venue,
            'direction': p.direction,
            'limit_price': p.limit_price,
            'intended_size': p.intended_size,
            'exchange_order_id': p.exchange_order_id,
            'sr_level': p.sr_level,
            'placed_at': p.placed_at,
            'expires_at': p.expires_at,
            'entry_direction': p.entry_direction,
            'signals_snapshot': getattr(p, 'signals_snapshot', None),
            'signals_agreed': getattr(p, 'signals_agreed', None),
            'context_snapshot': getattr(p, 'context_snapshot', None),
            'conviction': getattr(p, 'conviction', None),
            'reason': getattr(p, 'reason', None),
            'sl_order_id': getattr(p, 'sl_order_id', None),
            'sl_price': getattr(p, 'sl_price', None),
            'state': getattr(p, 'state', 'PENDING'),
            'filled_size': getattr(p, 'filled_size', 0.0),
            'filled_price': getattr(p, 'filled_price', 0.0),
        }
        self.pending_orders.append(row)
        return len(self.pending_orders)

    def update_pending_state(self, order_id: str, state: str, reason=None, filled_size: float = 0.0, filled_price: float = 0.0):
        for row in self.pending_orders:
            if row.get('exchange_order_id') == order_id:
                row['state'] = state
                row['cancel_reason'] = reason
                row['filled_size'] = filled_size
                row['filled_price'] = filled_price

    def get_pending_orders(self, state=None):
        if state:
            return [r for r in self.pending_orders if r.get('state') == state]
        return list(self.pending_orders)


class MockAdapter:
    """Mock exchange adapter for testing."""
    def __init__(self, place_ok=True, status='open', filled=0.0):
        self._place_ok = place_ok
        self._status = status
        self._filled = filled

    async def place_limit_order(self, *args, **kwargs):
        return self._place_ok, 'oid_123' if self._place_ok else None

    async def check_order_status(self, symbol, oid):
        return {
            'status': self._status,
            'filled_size': self._filled,
            'remaining_size': 0.1,
            'avg_price': 80000.0
        }

    async def cancel_order(self, symbol, oid):
        return True


class MockCancelErrorAdapter(MockAdapter):
    async def cancel_order(self, symbol, oid):
        raise RuntimeError("cancel failed")


class MockPartialAfterCancelAdapter(MockAdapter):
    """Simulate partial fill discovered only after cancel attempt."""

    def __init__(self):
        super().__init__(status='open', filled=0.0)
        self._cancelled = False

    async def cancel_order(self, symbol, oid):
        self._cancelled = True
        return True

    async def check_order_status(self, symbol, oid):
        if self._cancelled:
            return {
                'status': 'canceled',
                'filled_size': 0.25,
                'remaining_size': 0.0,
                'avg_price': 79950.0,
            }
        return {
            'status': 'open',
            'filled_size': 0.0,
            'remaining_size': 1.0,
            'avg_price': 0.0,
        }


class MockSwapAdapter(MockAdapter):
    """Simulate a concurrent replacement of the same symbol/venue pending row."""

    def __init__(self, mgr):
        super().__init__(status='partially_filled', filled=0.2)
        self._mgr = mgr
        self._swapped = False

    async def check_order_status(self, symbol, oid):
        if not self._swapped:
            self._swapped = True
            key = (str(symbol).upper(), "lighter")
            with self._mgr._locked(timeout_sec=1.0) as ok:
                assert ok
                cur = self._mgr._pending.get(key)
                assert cur is not None
                self._mgr._pending[key] = PendingLimit(
                    symbol=cur.symbol,
                    venue=cur.venue,
                    direction=cur.direction,
                    limit_price=cur.limit_price,
                    intended_size=cur.intended_size,
                    exchange_order_id="oid_new",
                    sr_level=cur.sr_level,
                    placed_at=cur.placed_at,
                    expires_at=cur.expires_at,
                    entry_direction=cur.entry_direction,
                    signals_snapshot=cur.signals_snapshot,
                    signals_agreed=cur.signals_agreed,
                    context_snapshot=cur.context_snapshot,
                    conviction=cur.conviction,
                    reason=cur.reason,
                    sl_order_id=cur.sl_order_id,
                    sl_price=cur.sl_price,
                    state="PENDING",
                )
        return {
            'status': self._status,
            'filled_size': self._filled,
            'remaining_size': 0.8,
            'avg_price': 80000.0
        }


@pytest.mark.asyncio
async def test_place_when_enabled(tmp_path):
    """Test placing SR limit when feature is enabled."""
    cfg = {
        'sr_limit_enabled': True,
        'sr_limit_max_pending': 2,
        'sr_limit_timeout_minutes': 30
    }
    mgr = PendingLimitManager(cfg, logging.getLogger(), lock_path=str(tmp_path / 'pending_limit.lock'))
    assert mgr.can_place('BTC', 'lighter', 1000, 10000)

    ok = await mgr.place(
        'BTC', 'LONG', 80000, 0.01,
        MockAdapter(), 'lighter', MockDB(),
        signals_snapshot='{}',
        signals_agreed='["CVD:LONG"]',
        context_snapshot='{}',
        conviction=0.5,
        reason='test'
    )
    assert ok
    assert mgr.get_pending('BTC', 'lighter') is not None


@pytest.mark.asyncio
async def test_disabled_by_default(tmp_path):
    """Test that SR limits are disabled by default."""
    cfg = {}
    mgr = PendingLimitManager(cfg, logging.getLogger(), lock_path=str(tmp_path / 'pending_limit.lock'))
    assert not mgr.can_place('BTC', 'lighter', 1000, 10000)


@pytest.mark.asyncio
async def test_partial_fill_cancels_remainder(tmp_path):
    """Test that partial fills cancel the remaining order."""
    cfg = {
        'sr_limit_enabled': True,
        'sr_limit_max_pending': 2,
        'sr_limit_timeout_minutes': 30
    }
    mgr = PendingLimitManager(cfg, logging.getLogger(), lock_path=str(tmp_path / 'pending_limit.lock'))
    db = MockDB()
    await mgr.place('BTC', 'LONG', 80000, 1.0, MockAdapter(), 'lighter', db)

    # Simulate partial fill - use adapters dict for explicit venue routing
    adapter = MockAdapter(status='partially_filled', filled=0.3)
    adapters = {'lighter': adapter}
    filled = await mgr.check_all(adapters, db, lambda s: 'LONG', lambda s: 81000)

    assert len(filled) == 1
    assert filled[0].filled_size == 0.3
    assert mgr.get_pending('BTC') is None


@pytest.mark.asyncio
async def test_from_db_row():
    """Test explicit DB row mapping."""
    row = {
        'symbol': 'ETH',
        'venue': 'hyperliquid',
        'direction': 'SHORT',
        'limit_price': 3000.0,
        'intended_size': 0.5,
        'exchange_order_id': 'oid_456',
        'sr_level': 3000.0,
        'placed_at': 1700000000.0,
        'expires_at': 1700001800.0,
        'entry_direction': 'SHORT',
        'signals_snapshot': '{}',
        'signals_agreed': '[]',
        'context_snapshot': '{}',
        'conviction': 0.4,
        'reason': 'test',
        'sl_order_id': 'oid_sl',
        'sl_price': 3100.0,
        'state': 'PENDING',
        'filled_size': 0.0,
        'filled_price': 0.0,
    }
    p = PendingLimit.from_db_row(row)
    assert p.symbol == 'ETH'
    assert p.venue == 'hyperliquid'
    assert isinstance(p.limit_price, float)
    assert p.limit_price == 3000.0


@pytest.mark.asyncio
async def test_max_pending_limit(tmp_path):
    """Test that max pending limit is enforced."""
    cfg = {
        'sr_limit_enabled': True,
        'sr_limit_max_pending': 2,
        'sr_limit_timeout_minutes': 30
    }
    mgr = PendingLimitManager(cfg, logging.getLogger(), lock_path=str(tmp_path / 'pending_limit.lock'))
    db = MockDB()

    # Place first two
    await mgr.place('BTC', 'LONG', 80000, 0.01, MockAdapter(), 'lighter', db)
    await mgr.place('ETH', 'LONG', 3000, 0.1, MockAdapter(), 'lighter', db)

    # Third should fail can_place check
    assert not mgr.can_place('SOL', 'lighter', 1000, 10000)


@pytest.mark.asyncio
async def test_signal_flip_cancels(tmp_path):
    """Test that signal flip cancels pending order."""
    cfg = {
        'sr_limit_enabled': True,
        'sr_limit_max_pending': 2,
        'sr_limit_timeout_minutes': 30
    }
    mgr = PendingLimitManager(cfg, logging.getLogger(), lock_path=str(tmp_path / 'pending_limit.lock'))
    db = MockDB()
    await mgr.place('BTC', 'LONG', 80000, 0.01, MockAdapter(), 'lighter', db)

    # Check with flipped signal direction
    adapters = {'lighter': MockAdapter(status='open', filled=0.0)}
    filled = await mgr.check_all(
        adapters, db,
        lambda s: 'SHORT',  # Signal flipped to SHORT
        lambda s: 81000
    )

    assert len(filled) == 0
    assert mgr.get_pending('BTC') is None  # Should be cancelled


@pytest.mark.asyncio
async def test_check_all_survives_cancel_exception(tmp_path):
    cfg = {
        'sr_limit_enabled': True,
        'sr_limit_max_pending': 2,
        'sr_limit_timeout_minutes': 30
    }
    mgr = PendingLimitManager(cfg, logging.getLogger(), lock_path=str(tmp_path / 'pending_limit.lock'))
    db = MockDB()
    await mgr.place('BTC', 'LONG', 80000, 0.01, MockAdapter(), 'lighter', db)

    # Force SIGNAL_FLIP path, and cancel_order raises.
    adapters = {'lighter': MockCancelErrorAdapter(status='open', filled=0.0)}
    filled = await mgr.check_all(
        adapters, db,
        lambda s: 'SHORT',
        lambda s: 81000
    )
    assert len(filled) == 0
    # Manager should still mark it removed despite cancel failure.
    assert mgr.get_pending('BTC') is None


@pytest.mark.asyncio
async def test_cancel_path_captures_partial_fill_for_executor(tmp_path):
    cfg = {
        'sr_limit_enabled': True,
        'sr_limit_max_pending': 2,
        'sr_limit_timeout_minutes': 30
    }
    mgr = PendingLimitManager(cfg, logging.getLogger(), lock_path=str(tmp_path / 'pending_limit.lock'))
    db = MockDB()
    await mgr.place('BTC', 'LONG', 80000, 1.0, MockAdapter(), 'lighter', db)

    adapters = {'lighter': MockPartialAfterCancelAdapter()}
    filled = await mgr.check_all(
        adapters, db,
        lambda s: 'SHORT',  # force SIGNAL_FLIP cancellation path
        lambda s: 81000
    )

    assert len(filled) == 1
    assert filled[0].state == "FILLED"
    assert filled[0].filled_size == pytest.approx(0.25)
    assert filled[0].filled_price == pytest.approx(79950.0)
    assert mgr.get_pending('BTC') is None
    assert db.pending_orders[0]['state'] == 'FILLED'
    assert db.pending_orders[0]['filled_size'] == pytest.approx(0.25)
    assert db.pending_orders[0]['filled_price'] == pytest.approx(79950.0)


@pytest.mark.asyncio
async def test_check_all_ignores_stale_snapshot_when_key_replaced(tmp_path):
    cfg = {
        'sr_limit_enabled': True,
        'sr_limit_max_pending': 2,
        'sr_limit_timeout_minutes': 30
    }
    mgr = PendingLimitManager(cfg, logging.getLogger(), lock_path=str(tmp_path / 'pending_limit.lock'))
    db = MockDB()
    await mgr.place('BTC', 'LONG', 80000, 0.01, MockAdapter(), 'lighter', db)

    adapters = {'lighter': MockSwapAdapter(mgr)}
    filled = await mgr.check_all(
        adapters, db,
        lambda s: 'LONG',
        lambda s: 81000
    )
    assert len(filled) == 0
    pending = mgr.get_pending('BTC')
    assert pending is not None
    assert pending.exchange_order_id == "oid_new"


@pytest.mark.asyncio
async def test_can_place_ignores_malformed_pending_rows_without_zeroing_notional(tmp_path):
    cfg = {
        'sr_limit_enabled': True,
        'sr_limit_max_pending': 5,
        'sr_limit_max_notional_pct': 10,  # cap = $100 on $1,000 equity
        'sr_limit_timeout_minutes': 30,
    }
    mgr = PendingLimitManager(cfg, logging.getLogger(), lock_path=str(tmp_path / 'pending_limit.lock'))

    bad = PendingLimit(
        symbol='BAD',
        venue='lighter',
        direction='LONG',
        limit_price='oops',  # malformed numeric
        intended_size=1.0,
        exchange_order_id='oid_bad',
        sr_level=1.0,
        placed_at=1.0,
        expires_at=2.0,
        entry_direction='LONG',
    )
    good = PendingLimit(
        symbol='GOOD',
        venue='lighter',
        direction='LONG',
        limit_price=90.0,
        intended_size=1.0,
        exchange_order_id='oid_good',
        sr_level=90.0,
        placed_at=1.0,
        expires_at=2.0,
        entry_direction='LONG',
    )
    mgr._pending = {
        mgr._k('GOOD', 'lighter'): good,
        mgr._k('BAD', 'lighter'): bad,
    }

    # Existing valid pending notional is $90; adding $20 should exceed $100 cap.
    assert not mgr.can_place('NEW', 'lighter', notional=20.0, equity=1000.0)


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
