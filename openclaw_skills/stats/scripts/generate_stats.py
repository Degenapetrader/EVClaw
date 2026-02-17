#!/usr/bin/env python3
"""/stats: live-first wallet dashboard.

Live sources (Hyperliquid /info):
- userState (equity + positions)
- openOrders (to detect trigger/SLTP presence best-effort)
- allMids (live mids for notional)

DB fallback:
- monitor_snapshot from EVClaw context builder outputs (via trades DB)
- recent closes from trades table

This is deterministic (no LLM).
"""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import urllib.request

SCRIPT_PATH = Path(__file__).resolve()
# Find EVClaw root by walking up to find .env or ai_trader.db
def _find_evclaw_root(start_path: Path) -> Path:
    """Find EVClaw root directory by searching for .env or ai_trader.db."""
    current = start_path
    for _ in range(10):  # Max 10 levels up
        if (current / ".env").exists() or (current / "ai_trader.db").exists():
            return current
        parent = current.parent
        if parent == current:  # Reached root
            break
        current = parent
    # Fallback to known path
    return Path("/root/.openclaw/workspace-router/job_company_starter/EVClaw")

EVCLAW_ROOT = _find_evclaw_root(SCRIPT_PATH)
TRADE_SCRIPTS_DIR = EVCLAW_ROOT / "openclaw_skills" / "trade" / "scripts"
HL_INFO_URL = "https://api.hyperliquid.xyz/info"
DB_PATH_DEFAULT = str(Path(os.getenv("EVCLAW_DB_PATH") or (EVCLAW_ROOT / "ai_trader.db")))
DEFAULT_WALLET = (os.getenv("HYPERLIQUID_ADDRESS") or "").strip()
DEFAULT_VAULT = os.getenv("VAULT_ADDRESS", "").strip() or None
DOTENV_PATH = str(EVCLAW_ROOT / ".env")


def _dotenv_get(path: str, key: str) -> Optional[str]:
    try:
        p = Path(path)
        if not p.exists():
            return None
        for ln in p.read_text().splitlines():
            s = ln.strip()
            if not s or s.startswith('#') or '=' not in s:
                continue
            k, v = s.split('=', 1)
            if k.strip() != key:
                continue
            return v.strip().strip('"').strip("'")
    except Exception:
        return None
    return None



def _utc_now() -> float:
    return time.time()


def _iso(ts: float) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _fmt_usd(x: float) -> str:
    s = "-" if x < 0 else ""
    x = abs(x)
    if x >= 1_000_000:
        return f"{s}${x/1_000_000:.2f}M"
    if x >= 1_000:
        return f"{s}${x/1_000:.2f}K"
    return f"{s}${x:.2f}"


def _hl_post(payload: Dict[str, Any], timeout: float = 10.0) -> Any:
    req = urllib.request.Request(
        HL_INFO_URL,
        data=json.dumps(payload).encode(),
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read().decode("utf-8"))


def _hl_all_mids() -> Dict[str, float]:
    out = _hl_post({"type": "allMids"})
    mids: Dict[str, float] = {}
    if isinstance(out, dict):
        for k, v in out.items():
            try:
                mids[str(k).upper()] = float(v)
            except Exception:
                continue
    return mids


def _hl_clearinghouse_state(user: str, dex: Optional[str] = None) -> Dict[str, Any]:
    payload: Dict[str, Any] = {"type": "clearinghouseState", "user": user}
    if dex:
        payload["dex"] = str(dex).strip().lower()
    out = _hl_post(payload)
    return out if isinstance(out, dict) else {}


def _hl_open_orders(user: str) -> List[Dict[str, Any]]:
    out = _hl_post({"type": "openOrders", "user": user})
    return out if isinstance(out, list) else []


def _hl_meta_and_ctxs() -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    out = _hl_post({"type": "metaAndAssetCtxs"})
    if isinstance(out, list) and len(out) == 2 and isinstance(out[0], dict) and isinstance(out[1], list):
        return out[0], [x for x in out[1] if isinstance(x, dict)]
    return {}, []


def _hl_user_fills_by_time(user: str, *, start_ms: int, end_ms: Optional[int] = None, aggregate_by_time: bool = True) -> List[Dict[str, Any]]:
    payload: Dict[str, Any] = {
        "type": "userFillsByTime",
        "user": user,
        "startTime": int(start_ms),
        "aggregateByTime": bool(aggregate_by_time),
    }
    if end_ms is not None:
        payload["endTime"] = int(end_ms)
    out = _hl_post(payload)
    return out if isinstance(out, list) else []


@dataclass
class LivePosition:
    symbol: str
    szi: float
    entry_px: float
    mid: float
    notional: float
    direction: str
    unrealized_pnl: Optional[float]


def _parse_positions(state: Dict[str, Any], mids: Dict[str, float]) -> List[LivePosition]:
    out: List[LivePosition] = []
    aps = state.get("assetPositions")
    if not isinstance(aps, list):
        return out

    for it in aps:
        if not isinstance(it, dict):
            continue
        pos = it.get("position") if isinstance(it.get("position"), dict) else {}
        coin = str(pos.get("coin") or "").strip().upper()
        if not coin:
            continue
        try:
            szi = float(pos.get("szi") or 0.0)
        except Exception:
            szi = 0.0
        if szi == 0.0:
            continue
        try:
            entry = float(pos.get("entryPx") or 0.0)
        except Exception:
            entry = 0.0
        mid = float(mids.get(coin, 0.0) or 0.0)
        notional = abs(szi) * (mid if mid > 0 else entry)
        direction = "LONG" if szi > 0 else "SHORT"
        upnl = None
        for k in ("unrealizedPnl", "unrealizedPnL", "uPnl"):
            if pos.get(k) is not None:
                try:
                    upnl = float(pos.get(k))
                except Exception:
                    upnl = None
                break
        out.append(
            LivePosition(
                symbol=coin,
                szi=szi,
                entry_px=entry,
                mid=mid,
                notional=float(notional),
                direction=direction,
                unrealized_pnl=upnl,
            )
        )

    out.sort(key=lambda p: p.notional, reverse=True)
    return out


def _sum_exposure(positions: List[LivePosition]) -> Tuple[float, float, float]:
    long_notional = sum(p.notional for p in positions if p.direction == "LONG")
    short_notional = sum(p.notional for p in positions if p.direction == "SHORT")
    net = long_notional - short_notional
    return float(net), float(long_notional), float(short_notional)


def _detect_missing_sltp(positions: List[LivePosition], open_orders: List[Dict[str, Any]]) -> List[str]:
    """Best-effort: if a coin has an open position but no trigger orders, flag it.

    Hyperliquid API order schema can vary; we only use loose checks.
    """
    # Collect trigger orders by coin.
    trig_by_coin: Dict[str, int] = {}
    for o in open_orders or []:
        if not isinstance(o, dict):
            continue
        coin = str(o.get("coin") or "").strip().upper()
        if not coin:
            continue
        ot = o.get("orderType")
        is_trigger = False
        if isinstance(ot, dict) and ("trigger" in ot or ot.get("t") == "trigger"):
            is_trigger = True
        if o.get("triggerPx") is not None:
            is_trigger = True
        if not is_trigger:
            continue
        trig_by_coin[coin] = trig_by_coin.get(coin, 0) + 1

    missing: List[str] = []
    for p in positions:
        if trig_by_coin.get(p.symbol, 0) <= 0:
            missing.append(p.symbol)
    return sorted(list(dict.fromkeys(missing)))


def _db_recent_closes(db_path: str, minutes: int = 60, limit: int = 10) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    since = _utc_now() - (minutes * 60)
    try:
        with sqlite3.connect(db_path, timeout=10.0) as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                """
                SELECT symbol, venue, direction, pnl_usd, exit_time
                FROM trades
                WHERE exit_time IS NOT NULL AND exit_time >= ?
                ORDER BY exit_time DESC
                LIMIT ?
                """,
                (float(since), int(limit)),
            ).fetchall()
        for r in rows or []:
            out.append(
                {
                    "symbol": str(r[0] or "").upper(),
                    "venue": str(r[1] or "").lower(),
                    "direction": str(r[2] or "").upper(),
                    "pnl_usd": float(r[3] or 0.0),
                    "exit_time": float(r[4] or 0.0),
                }
            )
    except Exception:
        return []
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--wallet", default=DEFAULT_WALLET)
    ap.add_argument("--db", default=DB_PATH_DEFAULT)
    args = ap.parse_args()

    wallet = str(args.wallet or "").strip()
    if not wallet:
        wallet = (_dotenv_get(DOTENV_PATH, "HYPERLIQUID_ADDRESS") or "").strip()
    if not wallet:
        print("missing wallet: set HYPERLIQUID_ADDRESS or pass --wallet", file=sys.stderr)
        return 2
    # Hyperliquid API is picky; normalize to lowercase.
    if wallet.startswith("0x"):
        wallet = wallet.lower()
    db_path = str(args.db or DB_PATH_DEFAULT).strip()

    live_err = None
    mids: Dict[str, float] = {}

    # Live-first accounts: wallet + optional vault.
    vault = (os.getenv("VAULT_ADDRESS") or "").strip() or None
    if not vault:
        vault = _dotenv_get(DOTENV_PATH, "VAULT_ADDRESS")
    if vault and vault.startswith("0x"):
        vault = vault.lower()

    accounts: List[Tuple[str, str]] = [("wallet", wallet)]
    if vault and vault != wallet:
        accounts.append(("vault", vault))

    all_positions: List[LivePosition] = []
    all_open_orders: List[Dict[str, Any]] = []
    equity_by_acct: Dict[str, float] = {}

    try:
        mids = _hl_all_mids()
        for label, addr in accounts:
            st = _hl_clearinghouse_state(addr)
            try:
                ms = st.get("marginSummary") if isinstance(st.get("marginSummary"), dict) else {}
                if ms.get("accountValue") is not None:
                    equity_by_acct[label] = float(ms.get("accountValue"))
            except Exception:
                pass
            try:
                all_positions.extend(_parse_positions(st, mids))
            except Exception:
                pass
            try:
                all_open_orders.extend(_hl_open_orders(addr))
            except Exception:
                pass
    except Exception as e:
        live_err = str(e)

    # De-dup positions by (symbol,direction): keep max notional
    pos_map: Dict[Tuple[str, str], LivePosition] = {}
    for p in all_positions:
        k = (p.symbol, p.direction)
        if k not in pos_map or p.notional > pos_map[k].notional:
            pos_map[k] = p
    positions = sorted(pos_map.values(), key=lambda p: p.notional, reverse=True)

    equity = float(sum(equity_by_acct.values())) if equity_by_acct else None

    net, long_notional, short_notional = _sum_exposure(positions)
    # NOTE: We no longer surface SL/TP presence as a warning in /stats output.
    missing_sltp: List[str] = []

    closes_60m = _db_recent_closes(db_path, minutes=60, limit=12)

    # DB fallback snapshot (only if live is missing)
    db_fallback_equity = None
    db_fallback_net = None
    try:
        TRADE_DIR = str(TRADE_SCRIPTS_DIR)
        if TRADE_DIR not in sys.path:
            sys.path.insert(0, TRADE_DIR)
        from allocate_context import build_context  # type: ignore

        runtime_dir = str(Path(os.getenv("EVCLAW_RUNTIME_DIR") or (EVCLAW_ROOT / "state")))
        ctx = build_context(db_path, runtime_dir, "BTC")
        mon = ctx.get("monitor_snapshot") if isinstance(ctx.get("monitor_snapshot"), dict) else {}
        if isinstance(mon, dict):
            try:
                db_fallback_equity = float(mon.get("hl_equity_live") or mon.get("hl_equity") or 0.0) or None
            except Exception:
                db_fallback_equity = None
            try:
                db_fallback_net = float(mon.get("hl_net_notional") or 0.0)
            except Exception:
                db_fallback_net = None
    except Exception:
        pass

    # If live fetch produced no usable state, fallback to DB/context snapshot.
    if (equity is None or equity <= 0) and db_fallback_equity and db_fallback_equity > 0:
        live_err = live_err or "no_live_equity"
        equity = db_fallback_equity

    if (not positions) and db_fallback_net is not None:
        # We can't rebuild live positions safely; but at least reflect net exposure.
        if net == 0.0:
            live_err = live_err or "no_live_positions"
        net = float(db_fallback_net)

    # Build a compact text report too (for chat), but also output structured json.
    lines: List[str] = []
    lines.append("Wallet stats")
    lines.append(f"- Wallet: {wallet[:6]}…{wallet[-4:]}")
    if vault and vault != wallet:
        lines.append(f"- Vault: {vault[:6]}…{vault[-4:]}")

    if equity is not None:
        lines.append(f"- Equity (HL, live): {_fmt_usd(float(equity))}")
    else:
        lines.append("- Equity (HL, live): unavailable")

    lines.append(
        f"- Exposure (HL): net {_fmt_usd(net)} | long {_fmt_usd(long_notional)} | short {_fmt_usd(short_notional)}"
    )

    # 24h account volume (live): sum(|px*sz|) from userFillsByTime for wallet + vault.
    vol24_usd = None
    try:
        end_ms = int(_utc_now() * 1000)
        start_ms = end_ms - int(24 * 60 * 60 * 1000)
        tot = 0.0
        for _label, addr in accounts:
            fills = _hl_user_fills_by_time(addr, start_ms=start_ms, end_ms=end_ms, aggregate_by_time=True)
            for f in fills:
                if not isinstance(f, dict):
                    continue
                try:
                    px = float(f.get('px') or 0.0)
                    sz = float(f.get('sz') or 0.0)
                    if px > 0 and sz != 0:
                        tot += abs(px * sz)
                except Exception:
                    continue
        vol24_usd = tot
    except Exception:
        vol24_usd = None

    if vol24_usd is not None and vol24_usd > 0:
        lines.append(f"- Volume 24h (account, live): {_fmt_usd(vol24_usd)}")

    if positions:
        lines.append("Open positions (top)")
        for p in positions[:8]:
            mid_txt = f"{p.mid:.4f}" if p.mid and p.mid > 0 else "?"
            upnl_txt = _fmt_usd(p.unrealized_pnl) if p.unrealized_pnl is not None else "n/a"
            lines.append(f"- {p.symbol} {p.direction} notional {_fmt_usd(p.notional)} | mid {mid_txt} | uPnL {upnl_txt}")
    else:
        lines.append("Open positions: none (or live fetch failed)")

    # Intentionally omit SL/TP trigger inference (too noisy / schema-dependent).

    if closes_60m:
        lines.append("Closes (last 60m, DB fallback)")
        for c in closes_60m[:8]:
            lines.append(f"- {c['symbol']} {c['direction']} {(_fmt_usd(c['pnl_usd']))}")

    if live_err:
        lines.append(f"NOTE: live fetch had an error; used partial/fallback data ({live_err}).")

    report_text = "\n".join(lines).strip() + "\n"

    live_ok = True
    if live_err:
        live_ok = False

    out = {
        "ok": True,
        "wallet": wallet,
        "generated_at_iso": _iso(_utc_now()),
        "live": {
            "ok": bool(live_ok),
            "error": live_err,
            "equity_hl": equity,
            "net_notional": net,
            "long_notional": long_notional,
            "short_notional": short_notional,
            "missing_sltp_symbols": missing_sltp,
            "positions": [p.__dict__ for p in positions],
        },
        "db_fallback": {
            "equity_hl": db_fallback_equity,
            "net_notional": db_fallback_net,
            "closes_60m": closes_60m,
        },
        "text": report_text,
    }

    print(json.dumps(out, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
