#!/usr/bin/env python3
"""
Live monitoring for Hyperliquid + Lighter.

Collects every interval (default 60s):
- Live positions from both exchanges
- Account balances (equity = balance + unrealized PnL)
- SL/TP status checks from ai_trader.db (SLTP_FAILED, PLACING_SLTP)
Stores snapshots into ai_trader.db and prints the full data to stdout.
"""

from __future__ import annotations

import argparse
import asyncio
import json
from logging_utils import get_logger
import sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

from dotenv import load_dotenv

from exchanges import HyperliquidAdapter, LighterAdapter, Position

# Reuse skill.yaml config parsing for venue enable flags
from cli import load_config, build_execution_config

# SQLite is the SINGLE source of truth for tracking. Do not remove DB usage.
from ai_trader_db import AITraderDB


DEFAULT_INTERVAL_SEC = 60
DEFAULT_DB_PATH = Path(__file__).parent / "ai_trader.db"
DEFAULT_TRACKING_DB_PATH = Path(__file__).parent / "ai_trader.db"
MONITOR_RETENTION_DAYS = 30



def init_db(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path), timeout=30.0)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("PRAGMA synchronous=NORMAL")

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS monitor_snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts REAL NOT NULL,
            ts_iso TEXT NOT NULL,
            hl_equity REAL,
            hl_balance REAL,
            hl_unrealized REAL,
            hl_wallet_equity REAL,
            hl_wallet_balance REAL,
            hl_wallet_unrealized REAL,
            hip3_equity REAL,
            hip3_balance REAL,
            hip3_unrealized REAL,
            hl_long_notional REAL,
            hl_short_notional REAL,
            hl_net_notional REAL,
            hl_long_count INTEGER,
            hl_short_count INTEGER,
            hl_wallet_long_notional REAL,
            hl_wallet_short_notional REAL,
            hl_wallet_net_notional REAL,
            hl_wallet_long_count INTEGER,
            hl_wallet_short_count INTEGER,
            hip3_long_notional REAL,
            hip3_short_notional REAL,
            hip3_net_notional REAL,
            hip3_long_count INTEGER,
            hip3_short_count INTEGER,
            lighter_equity REAL,
            lighter_balance REAL,
            lighter_unrealized REAL,
            lighter_long_notional REAL,
            lighter_short_notional REAL,
            lighter_net_notional REAL,
            lighter_long_count INTEGER,
            lighter_short_count INTEGER,
            total_equity REAL,
            sltp_failed_count INTEGER,
            placing_sltp_count INTEGER
        )
        """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS monitor_positions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            snapshot_id INTEGER NOT NULL,
            source TEXT NOT NULL,
            symbol TEXT NOT NULL,
            direction TEXT,
            size REAL,
            entry_price REAL,
            unrealized_pnl REAL,
            state TEXT,
            sl_price REAL,
            tp_price REAL,
            sl_order_id TEXT,
            tp_order_id TEXT,
            opened_at TEXT,
            signals_agreeing TEXT,
            venue TEXT,
            FOREIGN KEY (snapshot_id) REFERENCES monitor_snapshots(id)
        )
        """
    )

    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_monitor_positions_snapshot "
        "ON monitor_positions (snapshot_id)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_monitor_positions_symbol "
        "ON monitor_positions (symbol)"
    )

    # Lightweight migrations for older DBs
    cols = {row[1] for row in conn.execute("PRAGMA table_info(monitor_snapshots)").fetchall()}
    for name, ddl in (
        ("hl_wallet_equity", "ALTER TABLE monitor_snapshots ADD COLUMN hl_wallet_equity REAL"),
        ("hl_wallet_balance", "ALTER TABLE monitor_snapshots ADD COLUMN hl_wallet_balance REAL"),
        ("hl_wallet_unrealized", "ALTER TABLE monitor_snapshots ADD COLUMN hl_wallet_unrealized REAL"),
        ("hip3_equity", "ALTER TABLE monitor_snapshots ADD COLUMN hip3_equity REAL"),
        ("hip3_balance", "ALTER TABLE monitor_snapshots ADD COLUMN hip3_balance REAL"),
        ("hip3_unrealized", "ALTER TABLE monitor_snapshots ADD COLUMN hip3_unrealized REAL"),
        ("hl_long_notional", "ALTER TABLE monitor_snapshots ADD COLUMN hl_long_notional REAL"),
        ("hl_short_notional", "ALTER TABLE monitor_snapshots ADD COLUMN hl_short_notional REAL"),
        ("hl_net_notional", "ALTER TABLE monitor_snapshots ADD COLUMN hl_net_notional REAL"),
        ("hl_long_count", "ALTER TABLE monitor_snapshots ADD COLUMN hl_long_count INTEGER"),
        ("hl_short_count", "ALTER TABLE monitor_snapshots ADD COLUMN hl_short_count INTEGER"),
        ("hl_wallet_long_notional", "ALTER TABLE monitor_snapshots ADD COLUMN hl_wallet_long_notional REAL"),
        ("hl_wallet_short_notional", "ALTER TABLE monitor_snapshots ADD COLUMN hl_wallet_short_notional REAL"),
        ("hl_wallet_net_notional", "ALTER TABLE monitor_snapshots ADD COLUMN hl_wallet_net_notional REAL"),
        ("hl_wallet_long_count", "ALTER TABLE monitor_snapshots ADD COLUMN hl_wallet_long_count INTEGER"),
        ("hl_wallet_short_count", "ALTER TABLE monitor_snapshots ADD COLUMN hl_wallet_short_count INTEGER"),
        ("hip3_long_notional", "ALTER TABLE monitor_snapshots ADD COLUMN hip3_long_notional REAL"),
        ("hip3_short_notional", "ALTER TABLE monitor_snapshots ADD COLUMN hip3_short_notional REAL"),
        ("hip3_net_notional", "ALTER TABLE monitor_snapshots ADD COLUMN hip3_net_notional REAL"),
        ("hip3_long_count", "ALTER TABLE monitor_snapshots ADD COLUMN hip3_long_count INTEGER"),
        ("hip3_short_count", "ALTER TABLE monitor_snapshots ADD COLUMN hip3_short_count INTEGER"),
        ("lighter_long_notional", "ALTER TABLE monitor_snapshots ADD COLUMN lighter_long_notional REAL"),
        ("lighter_short_notional", "ALTER TABLE monitor_snapshots ADD COLUMN lighter_short_notional REAL"),
        ("lighter_net_notional", "ALTER TABLE monitor_snapshots ADD COLUMN lighter_net_notional REAL"),
        ("lighter_long_count", "ALTER TABLE monitor_snapshots ADD COLUMN lighter_long_count INTEGER"),
        ("lighter_short_count", "ALTER TABLE monitor_snapshots ADD COLUMN lighter_short_count INTEGER"),
    ):
        if name not in cols:
            conn.execute(ddl)
    conn.commit()
    return conn


def cleanup_monitor_snapshots(conn: sqlite3.Connection, now_ts: float) -> None:
    if MONITOR_RETENTION_DAYS <= 0:
        return
    cutoff = now_ts - (MONITOR_RETENTION_DAYS * 86400)
    # Delete child rows first to avoid FK issues.
    conn.execute(
        "DELETE FROM monitor_positions WHERE snapshot_id IN (SELECT id FROM monitor_snapshots WHERE ts < ?)",
        (cutoff,),
    )
    conn.execute("DELETE FROM monitor_snapshots WHERE ts < ?", (cutoff,))


def load_tracking_positions(db_path: Path) -> Dict[str, Dict[str, Any]]:
    """Load tracked positions from SQLite (source of truth)."""
    if not db_path.exists():
        return {}
    try:
        db = AITraderDB(str(db_path))
        open_trades = db.get_open_trades()
        positions: Dict[str, Dict[str, Any]] = {}
        for trade in open_trades:
            key = f"{trade.symbol.upper()}::{str(trade.venue or '').lower()}::{int(trade.id)}"
            positions[key] = {
                "trade_id": int(trade.id),
                "symbol": trade.symbol,
                "direction": trade.direction,
                "size": trade.size,
                "entry_price": trade.entry_price,
                "unrealized_pnl": 0.0,
                "state": getattr(trade, "state", "ACTIVE") or "ACTIVE",
                "sl_price": trade.sl_price or 0.0,
                "tp_price": trade.tp_price or 0.0,
                "sl_order_id": trade.sl_order_id,
                "tp_order_id": trade.tp_order_id,
                "opened_at": datetime.fromtimestamp(trade.entry_time, tz=timezone.utc).isoformat(),
                "signals_agreeing": trade.get_signals_agreed_list(),
                "venue": trade.venue,
            }
        return positions
    except Exception:
        return {}


def extract_sltp_states(
    tracking_positions: Dict[str, Dict[str, Any]],
) -> Tuple[List[str], List[str]]:
    sltp_failed: List[str] = []
    placing_sltp: List[str] = []
    for symbol, data in tracking_positions.items():
        state = str(data.get("state", "")).upper()
        if state == "SLTP_FAILED":
            sltp_failed.append(symbol)
        elif state == "PLACING_SLTP":
            placing_sltp.append(symbol)
    return sltp_failed, placing_sltp


async def get_hyperliquid_equity(hl: HyperliquidAdapter, *, address: Optional[str] = None, dex: Optional[str] = None) -> float:
    address = address or hl._vault_address or hl._address
    if not address:
        return 0.0
    try:
        payload: Dict[str, Any] = {"type": "clearinghouseState", "user": address}
        if dex:
            payload["dex"] = dex
        state = await hl._post_public(payload)
        margin_summary = state.get("marginSummary", {}) if isinstance(state, dict) else {}
        return float(margin_summary.get("accountValue", 0.0))
    except Exception:
        return 0.0


async def get_unified_wallet_equity(
    hl: HyperliquidAdapter,
    *,
    wallet_address: str,
    builder_dexes: Iterable[str] = ("xyz", "cash", "km", "flx", "hyna", "vntl", "abcd"),
) -> float:
    """Compute total wallet equity under Unified Account mode.

    Under unifiedAccount, wallet clearinghouseState (no dex) only returns
    equity for positions on the main perp dex. Builder-dex collateral is
    held as spot stablecoin balances. This function sums:
      1. wallet main accountValue (perp positions on wallet)
      2. each builder-dex accountValue (collateral + unrealized)
      3. free spot stablecoin balances (not held as builder collateral)
    """
    total = 0.0

    # 1. Wallet main perps equity
    try:
        state = await hl._post_public({"type": "clearinghouseState", "user": wallet_address})
        if isinstance(state, dict):
            total += float(state.get("marginSummary", {}).get("accountValue", 0.0))
    except Exception:
        pass

    # 2. Builder-dex equities
    for dex in builder_dexes:
        try:
            dex_state = await hl._post_public(
                {"type": "clearinghouseState", "user": wallet_address, "dex": dex}
            )
            if isinstance(dex_state, dict):
                total += float(dex_state.get("marginSummary", {}).get("accountValue", 0.0))
        except Exception:
            continue

    # 3. Free spot stablecoin balances (not locked as builder collateral)
    _STABLES = {"USDC", "USDT0", "USDT", "USDE", "USDH", "USDHL"}
    try:
        spot_state = await hl._post_public(
            {"type": "spotClearinghouseState", "user": wallet_address}
        )
        if isinstance(spot_state, dict):
            for bal in spot_state.get("balances", []):
                coin = str(bal.get("coin", "")).strip().upper()
                if coin in _STABLES:
                    bal_total = float(bal.get("total", 0.0))
                    bal_hold = float(bal.get("hold", 0.0))
                    free = max(0.0, bal_total - bal_hold)
                    total += free
    except Exception:
        pass

    return total


async def get_lighter_equity(lighter: LighterAdapter, fallback: float) -> float:
    if not lighter._initialized or not lighter._account_api or lighter._account_index is None:
        return 0.0

    try:
        account_response = await lighter._account_api.account(
            by="index",
            value=str(lighter._account_index),
        )
    except Exception:
        return 0.0

    total_equity: Optional[float] = None

    if hasattr(account_response, "accounts"):
        for acct in account_response.accounts:
            for field in (
                "total_asset_value",
                "equity",
                "account_value",
                "balance",
                "collateral",
                "wallet_balance",
                "margin_balance",
            ):
                if hasattr(acct, field):
                    try:
                        total_equity = float(getattr(acct, field))
                        break
                    except Exception:
                        continue
            if total_equity is not None:
                break

            margin = 0.0
            if hasattr(acct, "margin"):
                try:
                    margin = float(acct.margin)
                except Exception:
                    margin = 0.0
            total_equity = margin + fallback
            break

    return total_equity if total_equity is not None else fallback


async def fetch_exchange_positions(
    hl: Optional[HyperliquidAdapter],
    hl_wallet: Optional[HyperliquidAdapter],
    lighter: Optional[LighterAdapter],
) -> Tuple[Dict[str, Position], Dict[str, Position], Dict[str, Position]]:
    hl_positions: Dict[str, Position] = {}
    hl_wallet_positions: Dict[str, Position] = {}
    lighter_positions: Dict[str, Position] = {}
    if hl is not None:
        hl_positions = await hl.get_all_positions()
    if hl_wallet is not None:
        hl_wallet_positions = await hl_wallet.get_all_positions()
    if lighter is not None:
        lighter_positions = await lighter.get_all_positions()
    return hl_positions, hl_wallet_positions, lighter_positions


def sum_unrealized(positions: Dict[str, Position]) -> float:
    return float(sum(pos.unrealized_pnl for pos in positions.values()))


def _sync_safety_state_equity(conn: sqlite3.Connection, total_equity: float) -> None:
    """Best-effort sync: keep safety_state.updated_at/current_equity fresh.

    Why: trading can run without SafetyManager ever being invoked (e.g. execution-by-call).
    In that case, safety_state.updated_at can go stale even while monitor_snapshots updates.

    This is *observability only* (does not change tiers / paused / daily_pnl).
    """
    if total_equity is None:
        return
    try:
        equity = float(total_equity)
    except (TypeError, ValueError):
        return
    if equity <= 0:
        return

    try:
        row = conn.execute(
            "SELECT peak_equity, max_drawdown_pct FROM safety_state WHERE id = 1"
        ).fetchone()
        if row is None:
            # Should not happen because AITraderDB() init inserts singleton, but be defensive.
            conn.execute(
                "INSERT OR IGNORE INTO safety_state (id, peak_equity, current_equity) VALUES (1, ?, ?)",
                (equity, equity),
            )
            peak = equity
            prev_max_drawdown = 0.0
        else:
            peak = float(row[0] or 0.0)
            prev_max_drawdown = float(row[1] or 0.0)
            if equity > peak:
                peak = equity

        drawdown_pct = ((peak - equity) / peak) * 100.0 if peak > 0 else 0.0
        max_drawdown_pct = max(prev_max_drawdown, drawdown_pct)

        conn.execute(
            """
            UPDATE safety_state
            SET current_equity = ?,
                peak_equity = ?,
                max_drawdown_pct = ?,
                updated_at = strftime('%s', 'now')
            WHERE id = 1
            """,
            (equity, peak, max_drawdown_pct),
        )
    except Exception:
        # Never let monitoring fail because of safety-state syncing.
        return


def insert_snapshot(
    conn: sqlite3.Connection,
    ts: float,
    hl_equity: float,
    hl_balance: float,
    hl_unrealized: float,
    hl_wallet_equity: float,
    hl_wallet_balance: float,
    hl_wallet_unrealized: float,
    hip3_equity: float,
    hip3_balance: float,
    hip3_unrealized: float,
    hl_long_notional: float,
    hl_short_notional: float,
    hl_long_count: int,
    hl_short_count: int,
    hl_wallet_long_notional: float,
    hl_wallet_short_notional: float,
    hl_wallet_long_count: int,
    hl_wallet_short_count: int,
    hip3_long_notional: float,
    hip3_short_notional: float,
    hip3_long_count: int,
    hip3_short_count: int,
    lighter_equity: float,
    lighter_balance: float,
    lighter_unrealized: float,
    lighter_long_notional: float,
    lighter_short_notional: float,
    lighter_long_count: int,
    lighter_short_count: int,
    total_equity: float,
    sltp_failed: Iterable[str],
    placing_sltp: Iterable[str],
    hl_positions: Dict[str, Position],
    hl_wallet_positions: Dict[str, Position],
    lighter_positions: Dict[str, Position],
    tracking_positions: Dict[str, Dict[str, Any]],
) -> int:
    ts_iso = datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()
    sltp_failed_list = list(sltp_failed)
    placing_sltp_list = list(placing_sltp)

    cur = conn.execute(
        """
        INSERT INTO monitor_snapshots (
            ts, ts_iso,
            hl_equity, hl_balance, hl_unrealized,
            hl_wallet_equity, hl_wallet_balance, hl_wallet_unrealized,
            hip3_equity, hip3_balance, hip3_unrealized,
            hl_long_notional, hl_short_notional, hl_net_notional,
            hl_long_count, hl_short_count,
            hl_wallet_long_notional, hl_wallet_short_notional, hl_wallet_net_notional,
            hl_wallet_long_count, hl_wallet_short_count,
            hip3_long_notional, hip3_short_notional, hip3_net_notional,
            hip3_long_count, hip3_short_count,
            lighter_equity, lighter_balance, lighter_unrealized,
            lighter_long_notional, lighter_short_notional, lighter_net_notional,
            lighter_long_count, lighter_short_count,
            total_equity,
            sltp_failed_count, placing_sltp_count
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            ts,
            ts_iso,
            hl_equity,
            hl_balance,
            hl_unrealized,
            hl_wallet_equity,
            hl_wallet_balance,
            hl_wallet_unrealized,
            hip3_equity,
            hip3_balance,
            hip3_unrealized,
            hl_long_notional,
            hl_short_notional,
            (hl_long_notional - hl_short_notional),
            int(hl_long_count),
            int(hl_short_count),
            hl_wallet_long_notional,
            hl_wallet_short_notional,
            (hl_wallet_long_notional - hl_wallet_short_notional),
            int(hl_wallet_long_count),
            int(hl_wallet_short_count),
            hip3_long_notional,
            hip3_short_notional,
            (hip3_long_notional - hip3_short_notional),
            int(hip3_long_count),
            int(hip3_short_count),
            lighter_equity,
            lighter_balance,
            lighter_unrealized,
            lighter_long_notional,
            lighter_short_notional,
            (lighter_long_notional - lighter_short_notional),
            int(lighter_long_count),
            int(lighter_short_count),
            total_equity,
            len(sltp_failed_list),
            len(placing_sltp_list),
        ),
    )
    snapshot_id = int(cur.lastrowid)

    rows: List[Tuple[Any, ...]] = []

    for pos in hl_positions.values():
        rows.append(
            (
                snapshot_id,
                "hyperliquid",
                pos.symbol,
                pos.direction,
                pos.size,
                pos.entry_price,
                pos.unrealized_pnl,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                "hyperliquid",
            )
        )

    for pos in hl_wallet_positions.values():
        rows.append(
            (
                snapshot_id,
                "hip3",
                pos.symbol,
                pos.direction,
                pos.size,
                pos.entry_price,
                pos.unrealized_pnl,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                "hip3",
            )
        )

    for pos in lighter_positions.values():
        rows.append(
            (
                snapshot_id,
                "lighter",
                pos.symbol,
                pos.direction,
                pos.size,
                pos.entry_price,
                pos.unrealized_pnl,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                "lighter",
            )
        )

    for key, data in tracking_positions.items():
        symbol = str(data.get("symbol") or key)
        signals = data.get("signals_agreeing")
        signals_json = json.dumps(signals) if isinstance(signals, list) else None
        rows.append(
            (
                snapshot_id,
                "trades_db",
                symbol,
                data.get("direction"),
                data.get("size"),
                data.get("entry_price"),
                data.get("unrealized_pnl"),
                data.get("state"),
                data.get("sl_price"),
                data.get("tp_price"),
                data.get("sl_order_id"),
                data.get("tp_order_id"),
                data.get("opened_at"),
                signals_json,
                data.get("venue"),
            )
        )

    conn.executemany(
        """
        INSERT INTO monitor_positions (
            snapshot_id, source, symbol, direction, size, entry_price, unrealized_pnl,
            state, sl_price, tp_price, sl_order_id, tp_order_id, opened_at,
            signals_agreeing, venue
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )

    cleanup_monitor_snapshots(conn, ts)

    # Keep safety_state fresh for observability (does not affect execution).
    _sync_safety_state_equity(conn, total_equity)

    conn.commit()
    return snapshot_id


def format_money(value: float) -> str:
    return f"${value:,.2f}"


def print_positions(title: str, positions: Dict[str, Position]) -> None:
    print(title)
    if not positions:
        print("  (none)")
        return
    for symbol in sorted(positions.keys()):
        pos = positions[symbol]
        print(
            f"  {symbol:<12} {pos.direction:<5} size={pos.size:.6f} "
            f"entry={pos.entry_price:.6f} upnl={pos.unrealized_pnl:.6f}"
        )


def print_tracking_positions(tracking_positions: Dict[str, Dict[str, Any]]) -> None:
    print("Tracked positions (SQLite):")
    if not tracking_positions:
        print("  (none)")
        return
    sorted_items = sorted(
        tracking_positions.items(),
        key=lambda item: (
            str((item[1] or {}).get("symbol") or item[0]),
            str((item[1] or {}).get("venue") or ""),
            str((item[1] or {}).get("trade_id") or ""),
        ),
    )
    for key, data in sorted_items:
        symbol = str(data.get("symbol") or key)
        state = data.get("state", "")
        venue = data.get("venue", "")
        size = float(data.get("size") or 0.0)
        entry = float(data.get("entry_price") or 0.0)
        sl = float(data.get("sl_price") or 0.0)
        tp = float(data.get("tp_price") or 0.0)
        print(
            f"  {symbol:<12} {state:<12} {venue:<11} "
            f"size={size:.6f} entry={entry:.6f} sl={sl:.6f} tp={tp:.6f}"
        )


def print_snapshot(
    ts: float,
    hl_equity: float,
    hl_balance: float,
    hl_unrealized: float,
    hl_wallet_equity: float,
    hl_wallet_balance: float,
    hl_wallet_unrealized: float,
    hip3_equity: float,
    hip3_balance: float,
    hip3_unrealized: float,
    hl_long_notional: float,
    hl_short_notional: float,
    hl_long_count: int,
    hl_short_count: int,
    hl_wallet_long_notional: float,
    hl_wallet_short_notional: float,
    hl_wallet_long_count: int,
    hl_wallet_short_count: int,
    hip3_long_notional: float,
    hip3_short_notional: float,
    hip3_long_count: int,
    hip3_short_count: int,
    lighter_equity: float,
    lighter_balance: float,
    lighter_unrealized: float,
    lighter_long_notional: float,
    lighter_short_notional: float,
    lighter_long_count: int,
    lighter_short_count: int,
    total_equity: float,
    sltp_failed: List[str],
    placing_sltp: List[str],
    hl_positions: Dict[str, Position],
    hl_wallet_positions: Dict[str, Position],
    lighter_positions: Dict[str, Position],
    tracking_positions: Dict[str, Dict[str, Any]],
) -> None:
    ts_iso = datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()
    print("=" * 90)
    print(f"Live Monitor Snapshot @ {ts_iso} (UTC)")
    print("=" * 90)
    hl_net_not = hl_long_notional - hl_short_notional
    hl_wallet_net_not = hl_wallet_long_notional - hl_wallet_short_notional
    hip3_net_not = hip3_long_notional - hip3_short_notional
    lt_net_not = lighter_long_notional - lighter_short_notional

    print(
        f"Hyperliquid: equity={format_money(hl_equity)} "
        f"balance={format_money(hl_balance)} "
        f"unrealized={format_money(hl_unrealized)} "
        f"positions={len(hl_positions)} "
        f"L/S={hl_long_count}/{hl_short_count} "
        f"net_notional={format_money(hl_net_not)}"
    )
    print(
        f"HL Wallet:   equity={format_money(hl_wallet_equity)} "
        f"balance={format_money(hl_wallet_balance)} "
        f"unrealized={format_money(hl_wallet_unrealized)} "
        f"positions={len(hl_wallet_positions)} "
        f"L/S={hl_wallet_long_count}/{hl_wallet_short_count} "
        f"net_notional={format_money(hl_wallet_net_not)}"
    )
    print(
        f"HIP3 wallet: equity={format_money(hip3_equity)} "
        f"balance={format_money(hip3_balance)} "
        f"unrealized={format_money(hip3_unrealized)} "
        f"L/S={hip3_long_count}/{hip3_short_count} "
        f"net_notional={format_money(hip3_net_not)}"
    )
    print(
        f"Lighter:     equity={format_money(lighter_equity)} "
        f"balance={format_money(lighter_balance)} "
        f"unrealized={format_money(lighter_unrealized)} "
        f"positions={len(lighter_positions)} "
        f"L/S={lighter_long_count}/{lighter_short_count} "
        f"net_notional={format_money(lt_net_not)}"
    )
    print(f"PERPS EQUITY (HL): {format_money(total_equity)}")
    print(f"GRAND (HL + WALLET):    {format_money(total_equity + hl_wallet_equity)}")
    print(
        f"SLTP_FAILED={len(sltp_failed)} "
        f"PLACING_SLTP={len(placing_sltp)} "
        f"tracked_positions={len(tracking_positions)}"
    )
    if sltp_failed:
        print(f"  SLTP_FAILED symbols: {', '.join(sorted(sltp_failed))}")
    if placing_sltp:
        print(f"  PLACING_SLTP symbols: {', '.join(sorted(placing_sltp))}")
    print()
    print_positions("HIP3 positions:", hl_positions)
    print_positions("HIP3 wallet positions:", hl_wallet_positions)
    if lighter_positions:
        print()
        print_positions("Lighter positions:", lighter_positions)
    print()
    print_tracking_positions(tracking_positions)
    print()


def compute_next_sleep(start_ts: float, interval: int) -> float:
    next_ts = start_ts - (start_ts % interval) + interval
    return max(0.0, next_ts - time.time())


async def _compute_notional_distribution(
    adapter: Any,
    positions: Dict[str, Position],
) -> Tuple[float, float, int, int]:
    """Compute (long_notional, short_notional, long_count, short_count) using mid prices."""
    long_notional = 0.0
    short_notional = 0.0
    long_count = 0
    short_count = 0

    for pos in positions.values():
        if pos.size <= 0 or (pos.direction or "").upper() == "FLAT":
            continue
        try:
            mid = float(await adapter.get_mid_price(pos.symbol) or 0.0)
        except Exception:
            mid = 0.0
        if mid <= 0:
            mid = float(pos.entry_price or 0.0)
        notional = abs(float(pos.size)) * float(mid)
        if (pos.direction or "").upper() == "LONG":
            long_notional += notional
            long_count += 1
        else:
            short_notional += notional
            short_count += 1

    return long_notional, short_notional, long_count, short_count


async def run_monitor(
    interval: int,
    once: bool,
    db_path: Path,
    tracking_db_path: Path,
    venues: Optional[str] = None,
) -> None:
    env_path = Path(__file__).parent / ".env"
    load_dotenv(env_path)

    cfg = load_config()
    exec_cfg = build_execution_config(cfg, dry_run=False)

    # Default to the skill config flags, but allow explicit override.
    if venues:
        v = venues.strip().lower()
        if v in ("hl", "hyperliquid"):
            exec_cfg.hl_enabled = True
            exec_cfg.lighter_enabled = False
        elif v in ("lighter", "lt"):
            exec_cfg.hl_enabled = False
            exec_cfg.lighter_enabled = True
        elif v in ("both", "dual"):
            exec_cfg.hl_enabled = True
            exec_cfg.lighter_enabled = True
        else:
            raise ValueError(f"Invalid --venues: {venues}")

    hl_log = get_logger("hyperliquid")
    hl_wallet_log = get_logger("hip3")
    lighter_log = get_logger("lighter")

    hl: Optional[HyperliquidAdapter] = None
    hl_wallet: Optional[HyperliquidAdapter] = None
    lighter: Optional[LighterAdapter] = None

    if exec_cfg.hl_enabled:
        hl = HyperliquidAdapter(log=hl_log, dry_run=False)
        await hl.initialize()

    if exec_cfg.hl_wallet_enabled:
        hl_wallet = HyperliquidAdapter(log=hl_wallet_log, dry_run=False, account_mode="wallet")
        await hl_wallet.initialize()

    if exec_cfg.lighter_enabled:
        lighter = LighterAdapter(log=lighter_log, dry_run=False)
        await lighter.initialize()

    # Ensure ai_trader.db migrations include monitoring tables
    AITraderDB(str(db_path))
    conn = init_db(db_path)

    try:
        while True:
            start_ts = time.time()

            tracking_positions = load_tracking_positions(tracking_db_path)
            sltp_failed, placing_sltp = extract_sltp_states(tracking_positions)

            hl_positions_all, hl_wallet_positions_all, lighter_positions = await fetch_exchange_positions(
                hl, hl_wallet, lighter
            )

            wallet_positions_all = hl_wallet_positions_all or {}

            # Split Hyperliquid positions into perps vs HIP3 (xyz:*) for correct equity/unrealized attribution.
            hl_positions = {k: v for k, v in (hl_positions_all or {}).items() if not str(k).lower().startswith("xyz:")}
            hl_wallet_positions = {k: v for k, v in wallet_positions_all.items() if not str(k).lower().startswith("xyz:")}
            hip3_positions = (
                {k: v for k, v in wallet_positions_all.items() if str(k).lower().startswith("xyz:")}
                if wallet_positions_all
                else {k: v for k, v in (hl_positions_all or {}).items() if str(k).lower().startswith("xyz:")}
            )

            hl_unrealized = sum_unrealized(hl_positions)
            hl_wallet_unrealized = sum_unrealized(wallet_positions_all) if wallet_positions_all else 0.0
            hip3_unrealized = sum_unrealized(hip3_positions)
            lighter_unrealized = sum_unrealized(lighter_positions)

            # Perps equity from wallet (legacy VAULT_ADDRESS still supported).
            hl_equity = (
                await get_hyperliquid_equity(hl, address=(hl._vault_address or hl._address), dex=None)
                if hl is not None
                else 0.0
            )
            hl_balance = hl_equity - hl_unrealized

            # Wallet equity from HIP3 wallet adapter (perps + builder dex + spot stables).
            # Under Unified Account mode, wallet clearinghouseState (no dex) only returns
            # equity for main perp positions; builder collateral lives in spot balances.
            # Use get_unified_wallet_equity to sum wallet perps + builder dexes + free spot stables.
            _wallet_adapter = hl_wallet if hl_wallet is not None else hl
            _wallet_addr = (
                getattr(hl_wallet, "_address", None)
                if hl_wallet is not None
                else (getattr(hl, "_hip3_address", None) or getattr(hl, "_address", None))
            )
            if _wallet_adapter is not None and _wallet_addr:
                hl_wallet_equity = await get_unified_wallet_equity(
                    _wallet_adapter,
                    wallet_address=_wallet_addr,
                    builder_dexes=getattr(_wallet_adapter, "_builder_dexes", lambda: ["xyz", "cash", "km", "flx", "hyna", "vntl", "abcd"])(),
                )
            else:
                hl_wallet_equity = 0.0
            hl_wallet_balance = hl_wallet_equity - hl_wallet_unrealized

            # HIP3 equity = same as unified wallet equity (builder dex positions are part of it).
            hip3_equity = hl_wallet_equity
            hip3_balance = hip3_equity - hip3_unrealized

            lighter_equity = (
                await get_lighter_equity(lighter, lighter_unrealized)
                if lighter is not None
                else 0.0
            )
            lighter_balance = lighter_equity - lighter_unrealized

            # Net exposure by notional (uses mid prices)
            hl_long_not, hl_short_not, hl_long_n, hl_short_n = (0.0, 0.0, 0, 0)
            if hl is not None:
                hl_long_not, hl_short_not, hl_long_n, hl_short_n = await _compute_notional_distribution(
                    hl, hl_positions
                )

            hl_wallet_long_not, hl_wallet_short_not, hl_wallet_long_n, hl_wallet_short_n = (0.0, 0.0, 0, 0)
            if hl_wallet is not None:
                hl_wallet_long_not, hl_wallet_short_not, hl_wallet_long_n, hl_wallet_short_n = await _compute_notional_distribution(
                    hl_wallet, hl_wallet_positions
                )

            hip3_long_not, hip3_short_not, hip3_long_n, hip3_short_n = (0.0, 0.0, 0, 0)
            if hip3_positions:
                hip3_adapter = hl_wallet if hl_wallet is not None else hl
                if hip3_adapter is not None:
                    hip3_long_not, hip3_short_not, hip3_long_n, hip3_short_n = await _compute_notional_distribution(
                        hip3_adapter, hip3_positions
                    )

            lt_long_not, lt_short_not, lt_long_n, lt_short_n = (0.0, 0.0, 0, 0)
            if lighter is not None:
                lt_long_not, lt_short_not, lt_long_n, lt_short_n = await _compute_notional_distribution(
                    lighter, lighter_positions
                )

            # NOTE (2026-02-04): `total_equity` is HL (PERPS) ONLY.
            # HIP3 stays separate in `hip3_equity`. Lighter is ignored for now.
            total_equity = hl_equity

            try:
                insert_snapshot(
                    conn=conn,
                    ts=start_ts,
                    hl_equity=hl_equity,
                    hl_balance=hl_balance,
                    hl_unrealized=hl_unrealized,
                    hl_wallet_equity=hl_wallet_equity,
                    hl_wallet_balance=hl_wallet_balance,
                    hl_wallet_unrealized=hl_wallet_unrealized,
                    hip3_equity=hip3_equity,
                    hip3_balance=hip3_balance,
                    hip3_unrealized=hip3_unrealized,
                    hl_long_notional=hl_long_not,
                    hl_short_notional=hl_short_not,
                    hl_long_count=hl_long_n,
                    hl_short_count=hl_short_n,
                    hl_wallet_long_notional=hl_wallet_long_not,
                    hl_wallet_short_notional=hl_wallet_short_not,
                    hl_wallet_long_count=hl_wallet_long_n,
                    hl_wallet_short_count=hl_wallet_short_n,
                    hip3_long_notional=hip3_long_not,
                    hip3_short_notional=hip3_short_not,
                    hip3_long_count=hip3_long_n,
                    hip3_short_count=hip3_short_n,
                    lighter_equity=lighter_equity,
                    lighter_balance=lighter_balance,
                    lighter_unrealized=lighter_unrealized,
                    lighter_long_notional=lt_long_not,
                    lighter_short_notional=lt_short_not,
                    lighter_long_count=lt_long_n,
                    lighter_short_count=lt_short_n,
                    total_equity=total_equity,
                    sltp_failed=sltp_failed,
                    placing_sltp=placing_sltp,
                    hl_positions=hl_positions_all,
                    hl_wallet_positions=wallet_positions_all,
                    lighter_positions=lighter_positions,
                    tracking_positions=tracking_positions,
                )
            except sqlite3.OperationalError as e:
                if "database is locked" in str(e).lower():
                    # Another writer is holding the lock; don't crash the monitor.
                    print(f"[WARN] live_monitor: snapshot write skipped (db locked): {e}")
                    await asyncio.sleep(1.0)
                    continue
                raise

            print_snapshot(
                ts=start_ts,
                hl_equity=hl_equity,
                hl_balance=hl_balance,
                hl_unrealized=hl_unrealized,
                hl_wallet_equity=hl_wallet_equity,
                hl_wallet_balance=hl_wallet_balance,
                hl_wallet_unrealized=hl_wallet_unrealized,
                hip3_equity=hip3_equity,
                hip3_balance=hip3_balance,
                hip3_unrealized=hip3_unrealized,
                hl_long_notional=hl_long_not,
                hl_short_notional=hl_short_not,
                hl_long_count=hl_long_n,
                hl_short_count=hl_short_n,
                hl_wallet_long_notional=hl_wallet_long_not,
                hl_wallet_short_notional=hl_wallet_short_not,
                hl_wallet_long_count=hl_wallet_long_n,
                hl_wallet_short_count=hl_wallet_short_n,
                hip3_long_notional=hip3_long_not,
                hip3_short_notional=hip3_short_not,
                hip3_long_count=hip3_long_n,
                hip3_short_count=hip3_short_n,
                lighter_equity=lighter_equity,
                lighter_balance=lighter_balance,
                lighter_unrealized=lighter_unrealized,
                lighter_long_notional=lt_long_not,
                lighter_short_notional=lt_short_not,
                lighter_long_count=lt_long_n,
                lighter_short_count=lt_short_n,
                total_equity=total_equity,
                sltp_failed=sltp_failed,
                placing_sltp=placing_sltp,
                hl_positions=hl_positions_all,
                hl_wallet_positions=wallet_positions_all,
                lighter_positions=lighter_positions,
                tracking_positions=tracking_positions,
            )

            if once:
                break

            sleep_s = compute_next_sleep(start_ts, interval)
            await asyncio.sleep(sleep_s)
    finally:
        try:
            if lighter is not None:
                await lighter.close()
        except Exception:
            pass
        try:
            if hl_wallet is not None:
                await hl_wallet.close()
        except Exception:
            pass
        try:
            if hl is not None:
                await hl.close()
        except Exception:
            pass
        conn.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Live monitor for HL + Lighter")
    parser.add_argument(
        "--venues",
        default=None,
        help="Override enabled venues: hl|lighter|both (default: read from skill.yaml)",
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=DEFAULT_INTERVAL_SEC,
        help="Polling interval in seconds (default: 60)",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Run once and exit",
    )
    parser.add_argument(
        "--db",
        type=str,
        default=str(DEFAULT_DB_PATH),
        help="SQLite DB path for snapshots (default: ai_trader.db)",
    )
    parser.add_argument(
        "--tracking-db",
        type=str,
        default=str(DEFAULT_TRACKING_DB_PATH),
        help="ai_trader.db path for SL/TP status checks (default: ai_trader.db)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    interval = max(10, int(args.interval))
    asyncio.run(
        run_monitor(
            interval=interval,
            once=bool(args.once),
            db_path=Path(args.db),
            tracking_db_path=Path(args.tracking_db),
            venues=getattr(args, "venues", None),
        )
    )


if __name__ == "__main__":
    main()
