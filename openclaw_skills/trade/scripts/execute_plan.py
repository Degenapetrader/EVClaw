#!/usr/bin/env python3
"""Execute a stored manual plan.

- chase: immediate chase_limit entry via Executor.execute
- limit: SR-style pending limit via PendingLimitManager (auto expiry)

This script is meant to be called by the OpenClaw `/execute` skill.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sqlite3
import sys
import time
import logging

# Keep stdout clean JSON.
logging.basicConfig(level=logging.CRITICAL)
logging.getLogger().setLevel(logging.CRITICAL)
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

SCRIPT_PATH = Path(__file__).resolve()
EVCLAW_ROOT = Path(os.getenv("EVCLAW_ROOT") or SCRIPT_PATH.parents[3])
DB_PATH_DEFAULT = str(Path(os.getenv("EVCLAW_DB_PATH") or (EVCLAW_ROOT / "ai_trader.db")))
EVCLAW_ROOT_STR = str(EVCLAW_ROOT)
if EVCLAW_ROOT_STR not in sys.path:
    sys.path.insert(0, EVCLAW_ROOT_STR)

# Prevent config_env from printing its one-line startup summary.
os.environ.setdefault("_EVCLAW_CONFIG_LOGGED", "1")
os.environ.setdefault("EVCLAW_LOG_LEVEL", "CRITICAL")

from cli import load_config, build_execution_config  # type: ignore
from executor import Executor, ExecutionDecision  # type: ignore
from venues import normalize_venue  # type: ignore


def _parse_ttl(s: Optional[str]) -> Optional[int]:
    if not s:
        return None
    txt = str(s).strip().lower()
    if not txt:
        return None
    try:
        if txt.endswith('m'):
            return int(float(txt[:-1]) * 60)
        if txt.endswith('h'):
            return int(float(txt[:-1]) * 3600)
        if txt.endswith('d'):
            return int(float(txt[:-1]) * 86400)
        # raw minutes
        return int(float(txt) * 60)
    except Exception:
        return None


async def _fetch_current_prices(executor: Executor, symbol: str, venue: Optional[str]) -> Tuple[float, float]:
    """Borrowed from cli.py (kept local to avoid import cycles)."""
    try:
        ven = normalize_venue(venue) if venue else None
        adapter = None
        if ven in ('hyperliquid', 'hip3', 'hl_wallet', 'wallet', 'hyperliquid_wallet'):
            adapter = (
                getattr(executor, 'hyperliquid', None)
                or getattr(executor, 'hyperliquid_wallet', None)
                or getattr(executor, '_hl_adapter', None)
            )
        elif ven == 'lighter':
            adapter = getattr(executor, 'lighter', None) or getattr(executor, '_lighter_adapter', None)
        elif hasattr(executor, 'router'):
            try:
                adapter = executor.router.select(symbol)
            except Exception:
                adapter = None
        if adapter is None:
            return 0.0, 0.0
        if hasattr(adapter, 'get_best_bid_ask'):
            try:
                bid, ask = await adapter.get_best_bid_ask(symbol)
                if float(bid or 0) > 0 and float(ask or 0) > 0:
                    return float(bid), float(ask)
            except Exception:
                pass
        info = getattr(adapter, '_info', None)
        if info is not None:
            hl_symbol = symbol.split(':', 1)[1] if ':' in symbol else symbol
            dex = symbol.split(':', 1)[0] if ':' in symbol else ""
            try:
                mids = info.all_mids(dex)
                mid = float(mids.get(hl_symbol, 0))
                if mid > 0:
                    spread = mid * 0.001
                    return mid - spread, mid + spread
            except Exception:
                pass
        if hasattr(adapter, '_lookup_market'):
            market = adapter._lookup_market(symbol)
            if market and market.last_trade_price > 0:
                mid = market.last_trade_price
                spread = mid * 0.001
                return mid - spread, mid + spread
        return 0.0, 0.0
    except Exception:
        return 0.0, 0.0


def _load_plan(conn: sqlite3.Connection, display_id: str) -> Dict[str, Any]:
    row = conn.execute(
        "SELECT display_id, plan_json, created_at, expires_at, status FROM manual_trade_plans WHERE display_id=?",
        (display_id,),
    ).fetchone()
    if not row:
        raise RuntimeError(f"plan not found: {display_id}")
    plan_json = row[1]
    if not plan_json:
        raise RuntimeError(f"plan not ready: {display_id} (status={row[4]})")
    try:
        plan = json.loads(plan_json)
    except Exception as e:
        raise RuntimeError(f"plan_json invalid for {display_id}: {e}")
    return plan


async def _execute_chase(plan: Dict[str, Any], *, db_path: str) -> Dict[str, Any]:
    symbol = str(plan.get('symbol') or '').upper()
    venue = normalize_venue(plan.get('venue') or 'hyperliquid')
    direction = str(plan.get('direction_idea') or plan.get('direction') or '').upper()
    size_usd = float(plan.get('size_usd') or 0.0)
    sl = float(((plan.get('sltp') or {}) if isinstance(plan.get('sltp'), dict) else {}).get('sl_price') or 0.0)
    tp = float(((plan.get('sltp') or {}) if isinstance(plan.get('sltp'), dict) else {}).get('tp_price') or 0.0)

    if direction not in {'LONG','SHORT'}:
        raise RuntimeError(f"invalid direction in plan: {direction}")
    if size_usd <= 0:
        raise RuntimeError(f"invalid size_usd in plan: {size_usd}")

    config = load_config()
    exec_config = build_execution_config(config, dry_run=False)
    executor = Executor(config=exec_config)
    await executor.initialize()
    try:
        bid, ask = await _fetch_current_prices(executor, symbol, venue)
        if bid <= 0 or ask <= 0:
            raise RuntimeError(f"failed to fetch live bid/ask for {symbol}")

        decision = ExecutionDecision(
            symbol=symbol,
            direction=direction,
            size_multiplier=1.0,
            signals_agreeing=['manual'],
            conviction=float(plan.get('confidence') or 0.5),
            reason=str((plan.get('why') or ['manual_plan'])[0]) if isinstance(plan.get('why'), list) and plan.get('why') else 'manual_plan',
        )
        decision.size_usd = float(size_usd)
        if sl > 0 and tp > 0:
            decision.sl_price = sl
            decision.tp_price = tp

        decision.context_snapshot = {
            'manual': True,
            'strategy': 'manual',
            'manual_plan_id': str(plan.get('display_id') or ''),
            'order_type': 'chase_limit',
        }

        res = await executor.execute(
            decision=decision,
            best_bid=float(bid),
            best_ask=float(ask),
            atr=None,
            venue=venue,
        )
        if not res.success:
            raise RuntimeError(res.error or 'execute failed')

        return {
            'mode': 'chase',
            'symbol': symbol,
            'venue': venue,
            'direction': direction,
            'size_usd': size_usd,
            'entry_price': res.entry_price,
            'sl_price': res.sl_price,
            'tp_price': res.tp_price,
        }
    finally:
        try:
            await executor.close()
        except Exception:
            pass


async def _execute_limit(plan: Dict[str, Any], *, db_path: str, ttl_override_sec: Optional[int]) -> Dict[str, Any]:
    symbol = str(plan.get('symbol') or '').upper()
    venue = normalize_venue(plan.get('venue') or 'hyperliquid')
    direction = str(plan.get('direction_idea') or plan.get('direction') or '').upper()
    size_usd = float(plan.get('size_usd') or 0.0)

    opt = (plan.get('options') or {}) if isinstance(plan.get('options'), dict) else {}
    limit_opt = (opt.get('limit') or {}) if isinstance(opt.get('limit'), dict) else {}
    limit_price = float(limit_opt.get('limit_price') or 0.0)
    if limit_price <= 0:
        raise RuntimeError('limit option missing limit_price')

    ttl_override_min: Optional[int] = None
    if ttl_override_sec is not None:
        ttl_override_min = max(1, int(ttl_override_sec / 60))

    config = load_config()
    exec_config = build_execution_config(config, dry_run=False)
    if ttl_override_min is not None:
        exec_config.sr_limit_timeout_minutes = ttl_override_min
    executor = Executor(config=exec_config)
    await executor.initialize()
    try:
        pending_mgr = getattr(executor, 'pending_mgr', None)
        if not pending_mgr or not getattr(pending_mgr, 'enabled', False):
            raise RuntimeError('pending sr_limit manager disabled')

        adapter = None
        if venue in ('hyperliquid', 'hip3', 'hl_wallet', 'wallet', 'hyperliquid_wallet'):
            adapter = getattr(executor, 'hyperliquid', None) or getattr(executor, 'hyperliquid_wallet', None)
        elif venue == 'lighter':
            adapter = getattr(executor, 'lighter', None)
        if adapter is None:
            raise RuntimeError(f'no adapter for venue={venue}')

        # size in base units
        size = float(size_usd) / float(limit_price)

        db = executor.db
        if db is None:
            raise RuntimeError('executor db not initialized')

        snap = db.get_latest_monitor_snapshot() or {}
        equity = float((snap.get('hl_equity') or 0.0))

        ok = await pending_mgr.place(
            symbol=symbol,
            direction=direction,
            sr_level=float(limit_price),
            size=float(size),
            adapter=adapter,
            venue=str(venue),
            db=db,
            conviction=float(plan.get('confidence') or 0.5),
            reason='manual_limit',
            context_snapshot=json.dumps({
                'manual': True,
                'strategy': 'manual',
                'manual_plan_id': str(plan.get('display_id') or ''),
                'pair_id': str(plan.get('display_id') or ''),
                'order_type': 'sr_limit',
            }),
            notional=float(size_usd),
            equity=float(equity) if equity > 0 else None,
        )
        if not ok:
            extra = ''
            try:
                last_err = getattr(adapter, 'last_order_error', None)
                if last_err:
                    extra = f" | venue_error={last_err}"
            except Exception:
                extra = ''
            raise RuntimeError('pending limit placement rejected (caps/lock/venue)' + extra)

        return {
            'mode': 'limit',
            'symbol': symbol,
            'venue': venue,
            'direction': direction,
            'size_usd': size_usd,
            'limit_price': limit_price,
            'ttl_seconds': ttl_override_sec,
        }
    finally:
        try:
            await executor.close()
        except Exception:
            pass


async def main_async() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument('display_id')
    ap.add_argument('mode', choices=['chase','limit'])
    ap.add_argument('ttl', nargs='?')
    ap.add_argument('--db', default=DB_PATH_DEFAULT)
    args = ap.parse_args()

    display_id = str(args.display_id or '').strip().upper()
    ttl_override = _parse_ttl(args.ttl)

    conn = sqlite3.connect(args.db, timeout=30.0)
    try:
        conn.execute('PRAGMA busy_timeout=30000')
        plan = _load_plan(conn, display_id)
        # expiry
        row = conn.execute('SELECT expires_at FROM manual_trade_plans WHERE display_id=?',(display_id,)).fetchone()
        if row and float(row[0] or 0) > 0 and time.time() > float(row[0]):
            raise RuntimeError(f'plan expired: {display_id}')

        # TTL defaulting: if user did not provide a ttl override for limit mode,
        # honor the plan's cancel_after_minutes (so /execute <ID> limit works as advertised).
        if args.mode == 'limit' and ttl_override is None:
            try:
                opt = plan.get('options') if isinstance(plan, dict) else None
                opt = opt if isinstance(opt, dict) else {}
                limit_opt = opt.get('limit') if isinstance(opt.get('limit'), dict) else {}
                cam = limit_opt.get('cancel_after_minutes')
                if cam is not None:
                    ttl_override = int(float(cam)) * 60
            except Exception:
                ttl_override = ttl_override

        if args.mode == 'chase':
            result = await _execute_chase(plan, db_path=args.db)
            conn.execute(
                "UPDATE manual_trade_plans SET status='EXECUTED', executed_at=?, executed_option='chase', error=NULL WHERE display_id=?",
                (time.time(), display_id),
            )
            conn.commit()
            print(json.dumps({'ok': True, 'display_id': display_id, 'result': result}, indent=2, sort_keys=True))
            return 0

        result = await _execute_limit(plan, db_path=args.db, ttl_override_sec=ttl_override)
        conn.execute(
            "UPDATE manual_trade_plans SET status='PENDING', executed_at=?, executed_option='limit', error=NULL WHERE display_id=?",
            (time.time(), display_id),
        )
        conn.commit()
        print(json.dumps({'ok': True, 'display_id': display_id, 'result': result}, indent=2, sort_keys=True))
        return 0
    except Exception as e:
        try:
            conn.execute(
                "UPDATE manual_trade_plans SET error=? WHERE display_id=?",
                (str(e), display_id),
            )
            conn.commit()
        except Exception:
            pass
        print(json.dumps({'ok': False, 'display_id': display_id, 'error': str(e)}, indent=2, sort_keys=True))
        # Always exit 0 so OpenClaw tool execution doesn't swallow the payload.
        return 0
    finally:
        try:
            conn.close()
        except Exception:
            pass


def main() -> int:
    return asyncio.run(main_async())


if __name__ == '__main__':
    raise SystemExit(main())
