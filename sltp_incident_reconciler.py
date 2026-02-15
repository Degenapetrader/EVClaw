#!/usr/bin/env python3
"""Reconcile OPEN SL/TP incidents against live Hyperliquid open orders.

Problem: SL/TP placement can succeed on-exchange but fail to record/resolve the
DB incident (e.g., TP order exists but incident remains OPEN).

This script:
- Loads OPEN incidents from `sltp_incidents_v1`
- Fetches live `frontendOpenOrders` for wallet + vault
- For each incident, checks if the expected order exists (best-effort)
- If found, resolves the incident in DB with a note.

It is SAFE: it never places/cancels orders; it only resolves DB incident rows.
"""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import urllib.request

from env_utils import EVCLAW_DB_PATH, env_str


def _normalize_info_url(raw: Optional[str]) -> str:
    base = (str(raw or "").strip()).rstrip("/")
    if not base:
        return "https://node2.evplus/info"
    if base.endswith("/info"):
        return base
    return f"{base}/info"


HL_INFO_URL = _normalize_info_url(env_str("HYPERLIQUID_PRIVATE_NODE", "https://node2.evplus/info"))
DB_PATH_DEFAULT = str(Path(EVCLAW_DB_PATH).expanduser())
DEFAULT_WALLET = (os.getenv("HYPERLIQUID_ADDRESS", "") or "").strip()


def _hl_post(payload: Dict[str, Any], *, info_url: str, timeout: float = 10.0) -> Any:
    req = urllib.request.Request(
        info_url,
        data=json.dumps(payload).encode(),
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read().decode("utf-8"))


def _hl_frontend_open_orders(user: str, *, info_url: str) -> List[Dict[str, Any]]:
    out = _hl_post({"type": "frontendOpenOrders", "user": user}, info_url=info_url)
    return out if isinstance(out, list) else []


def _pct_diff(a: float, b: float) -> float:
    if a == 0 or b == 0:
        return 999.0
    return abs(a - b) / abs(b)


def _normalize_addr(a: str) -> str:
    s = (a or '').strip()
    if s.startswith('0x'):
        return s.lower()
    return s


@dataclass
class Incident:
    id: int
    trade_id: int
    symbol: str
    leg: str  # SL or TP


def _load_open_incidents(conn: sqlite3.Connection, *, symbol: Optional[str] = None, limit: int = 200) -> List[Incident]:
    q = """
    SELECT id, trade_id, symbol, leg
    FROM sltp_incidents_v1
    WHERE state='OPEN' AND trade_id IS NOT NULL
    """
    args: List[Any] = []
    if symbol:
        q += " AND symbol=?"
        args.append(str(symbol).upper())
    q += " ORDER BY opened_at ASC LIMIT ?"
    args.append(int(limit))

    rows = conn.execute(q, args).fetchall()
    out: List[Incident] = []
    for r in rows:
        try:
            out.append(Incident(id=int(r[0]), trade_id=int(r[1]), symbol=str(r[2]).upper(), leg=str(r[3]).upper()))
        except Exception:
            continue
    return out


def _trade_sltp(conn: sqlite3.Connection, trade_id: int) -> Tuple[Optional[float], Optional[float]]:
    r = conn.execute("SELECT sl_price, tp_price FROM trades WHERE id=?", (int(trade_id),)).fetchone()
    if not r:
        return None, None
    try:
        sl = float(r[0]) if r[0] is not None else None
    except Exception:
        sl = None
    try:
        tp = float(r[1]) if r[1] is not None else None
    except Exception:
        tp = None
    return sl, tp


def _has_matching_tp(orders: List[Dict[str, Any]], *, coin: str, tp_price: float, tol_pct: float) -> Optional[int]:
    """TP is a reduce-only LIMIT (non-trigger) per executor.
    Return oid if found.
    """
    for o in orders:
        try:
            if str(o.get('coin') or '').upper() != coin:
                continue
            if not bool(o.get('reduceOnly')):
                continue
            if bool(o.get('isTrigger')):
                continue
            px = float(o.get('limitPx') or 0.0)
            if px <= 0:
                continue
            if _pct_diff(px, tp_price) <= tol_pct:
                return int(o.get('oid')) if o.get('oid') is not None else None
        except Exception:
            continue
    return None


def _has_matching_sl(orders: List[Dict[str, Any]], *, coin: str, sl_price: float, tol_pct: float) -> Optional[int]:
    """SL is a trigger order (stop market) reduce-only.
    Return oid if found.
    """
    for o in orders:
        try:
            if str(o.get('coin') or '').upper() != coin:
                continue
            if not bool(o.get('reduceOnly')):
                continue
            if not bool(o.get('isTrigger')):
                continue
            tpx = o.get('triggerPx')
            px = float(tpx) if tpx is not None else float(o.get('limitPx') or 0.0)
            if px <= 0:
                continue
            if _pct_diff(px, sl_price) <= tol_pct:
                return int(o.get('oid')) if o.get('oid') is not None else None
        except Exception:
            continue
    return None


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument('--db', default=DB_PATH_DEFAULT)
    ap.add_argument('--wallet', default=DEFAULT_WALLET)
    ap.add_argument('--info-url', default=HL_INFO_URL)
    ap.add_argument('--symbol', default=None)
    ap.add_argument('--tol-pct', type=float, default=0.002)  # 0.2%
    ap.add_argument('--limit', type=int, default=200)
    args = ap.parse_args()

    wallet = _normalize_addr(args.wallet)
    if not wallet:
        print("ERROR: missing wallet; set HYPERLIQUID_ADDRESS or pass --wallet", file=sys.stderr)
        return 2
    info_url = _normalize_info_url(args.info_url)
    accounts: List[str] = [wallet]

    # Live orders
    live_orders: List[Dict[str, Any]] = []
    for addr in accounts:
        try:
            live_orders.extend(_hl_frontend_open_orders(addr, info_url=info_url))
        except Exception:
            pass

    db_path = str(args.db)
    sym_filter = str(args.symbol).upper() if args.symbol else None

    resolved = 0
    checked = 0

    conn = sqlite3.connect(db_path, timeout=30.0)
    try:
        conn.execute('PRAGMA busy_timeout=30000')
        incs = _load_open_incidents(conn, symbol=sym_filter, limit=int(args.limit))
        for inc in incs:
            checked += 1
            sl_price, tp_price = _trade_sltp(conn, inc.trade_id)

            found_oid: Optional[int] = None
            if inc.leg == 'TP' and tp_price and tp_price > 0:
                found_oid = _has_matching_tp(live_orders, coin=inc.symbol, tp_price=float(tp_price), tol_pct=float(args.tol_pct))
            elif inc.leg == 'SL' and sl_price and sl_price > 0:
                found_oid = _has_matching_sl(live_orders, coin=inc.symbol, sl_price=float(sl_price), tol_pct=float(args.tol_pct))

            if found_oid:
                conn.execute(
                    """
                    UPDATE sltp_incidents_v1
                    SET state='RESOLVED',
                        resolved_at=strftime('%s','now'),
                        resolution_note=COALESCE(resolution_note, ?),
                        updated_at=strftime('%s','now')
                    WHERE id=? AND state='OPEN'
                    """,
                    (f"order_seen_open_on_exchange oid={found_oid}", int(inc.id)),
                )
                resolved += int(conn.total_changes > 0)
        conn.commit()
    finally:
        try:
            conn.close()
        except Exception:
            pass

    print(json.dumps({
        'ok': True,
        'checked': checked,
        'resolved': resolved,
        'symbol_filter': sym_filter,
        'accounts': accounts,
        'info_url': info_url,
    }, indent=2, sort_keys=True))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
