#!/usr/bin/env python3
"""Generate a deterministic hedge plan to reduce HL net notional by ~50%.

No LLM. Stores a manual plan for /execute.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import urllib.request

SCRIPT_PATH = Path(__file__).resolve()
EVCLAW_ROOT = Path(os.getenv("EVCLAW_ROOT") or SCRIPT_PATH.parents[3])
TRADE_DIR = EVCLAW_ROOT / "openclaw_skills" / "trade" / "scripts"
if str(TRADE_DIR) not in sys.path:
    sys.path.insert(0, str(TRADE_DIR))
EVCLAW_ROOT_STR = str(EVCLAW_ROOT)
if EVCLAW_ROOT_STR not in sys.path:
    sys.path.insert(0, EVCLAW_ROOT_STR)

from allocate_context import allocate_plan_id, build_context  # type: ignore
from finalize_plan import main as finalize_main  # type: ignore

RUNTIME_DIR_DEFAULT = str(Path(os.getenv("EVCLAW_RUNTIME_DIR") or (EVCLAW_ROOT / "state")))
DB_PATH_DEFAULT = str(Path(os.getenv("EVCLAW_DB_PATH") or (EVCLAW_ROOT / "ai_trader.db")))
HL_INFO_URL = "https://api.hyperliquid.xyz/info"


def _utc_now() -> float:
    return time.time()


def _iso(ts: float) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _hl_post(payload: Dict[str, Any], timeout: float = 10.0) -> Any:
    req = urllib.request.Request(
        HL_INFO_URL,
        data=json.dumps(payload).encode(),
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read().decode("utf-8"))


def _hl_all_mids() -> Dict[str, str]:
    out = _hl_post({"type": "allMids"})
    return out if isinstance(out, dict) else {}


def _hl_l2_bbo(symbol: str) -> Tuple[float, float]:
    try:
        book = _hl_post({"type": "l2Book", "coin": symbol})
        levels = book.get("levels") if isinstance(book, dict) else None
        if not isinstance(levels, list) or len(levels) < 2:
            return 0.0, 0.0
        bids = levels[0] if isinstance(levels[0], list) else []
        asks = levels[1] if isinstance(levels[1], list) else []
        best_bid = float(bids[0]["px"]) if bids and isinstance(bids[0], dict) and bids[0].get("px") is not None else 0.0
        best_ask = float(asks[0]["px"]) if asks and isinstance(asks[0], dict) and asks[0].get("px") is not None else 0.0
        return best_bid, best_ask
    except Exception:
        return 0.0, 0.0


def _maker_safe_limit_price(*, direction: str, suggested: float, best_bid: Optional[float], best_ask: Optional[float]) -> float:
    d = (direction or "").upper()
    px = float(suggested or 0.0)
    if px <= 0:
        return px
    if best_bid is None or best_ask is None:
        return px
    bid = float(best_bid or 0.0)
    ask = float(best_ask or 0.0)
    if bid <= 0 or ask <= 0:
        return px
    mid = (bid + ask) / 2.0
    step = max(1e-6, mid * 0.0001)
    if d == "LONG" and px >= ask:
        return max(0.0, ask - step)
    if d == "SHORT" and px <= bid:
        return bid + step
    return px


def _pick_sr_price(ctx: Dict[str, Any], *, direction: str) -> Optional[float]:
    try:
        sse = ctx.get("sse_snapshot") if isinstance(ctx.get("sse_snapshot"), dict) else {}
        micro = sse.get("symbol_micro") if isinstance(sse.get("symbol_micro"), dict) else {}
        sup = micro.get("sr_support")
        res = micro.get("sr_resistance")
        d = (direction or "").upper()
        px = sup if d == "LONG" else res
        if px is None:
            return None
        px = float(px)
        return px if px > 0 else None
    except Exception:
        return None


def _det_sltp(*, direction: str, entry: float, atr_abs: float) -> Tuple[float, float]:
    a = max(float(atr_abs), max(entry * 0.01, 1.0))
    sl_dist = 1.0 * a
    tp_dist = 1.5 * a
    d = (direction or "").upper()
    if d == "LONG":
        return max(0.0, entry - sl_dist), max(0.0, entry + tp_dist)
    return max(0.0, entry + sl_dist), max(0.0, entry - tp_dist)


def _finalize_plan(display_id: str, json_file: str, *, db_path: str) -> None:
    argv0 = sys.argv[:]
    try:
        sys.argv = ["finalize_plan.py", str(display_id), str(json_file), "--db", str(db_path)]
        rc = finalize_main()
        if int(rc) != 0:
            raise RuntimeError(f"finalize failed rc={rc}")
    finally:
        sys.argv = argv0


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--symbol", default="BTC")
    ap.add_argument("--pct", type=float, default=100.0)
    ap.add_argument("--db", default=DB_PATH_DEFAULT)
    ap.add_argument("--runtime", default=RUNTIME_DIR_DEFAULT)
    args = ap.parse_args()

    sym = str(args.symbol or "").strip().upper() or "BTC"
    if sym not in {"BTC", "ETH"}:
        sym = "BTC"

    # Grab portfolio snapshot from context builder
    ctx = build_context(args.db, args.runtime, sym)
    mon = ctx.get("monitor_snapshot") if isinstance(ctx.get("monitor_snapshot"), dict) else {}

    try:
        net = float(mon.get("hl_net_notional") or 0.0)
    except Exception:
        net = 0.0

    try:
        eq = mon.get("hl_equity_live")
        eq = float(eq) if eq is not None else float(mon.get("hl_equity") or 0.0)
    except Exception:
        eq = 0.0

    if net == 0.0:
        print(json.dumps({"ok": False, "error": "net_notional_zero_or_missing", "monitor": mon}, indent=2, sort_keys=True))
        return 1

    direction = "LONG" if net < 0 else "SHORT"  # opposite of exposure

    # Size to reduce net by target percent (default 100% neutralize)
    pct = float(args.pct or 100.0)
    if pct <= 0:
        pct = 0.0
    if pct > 200:
        pct = 200.0
    size_target = abs(net) * (pct / 100.0)

    cap_pct = float(os.getenv("HL_HEDGE_MAX_NOTIONAL_PCT", "200") or 200.0)
    cap = (eq * (cap_pct / 100.0)) if eq and eq > 0 else None
    if cap is not None:
        size_usd = float(min(size_target, cap))
    else:
        size_usd = float(size_target)

    mids = _hl_all_mids()
    try:
        mid = float(mids.get(sym, 0.0))
    except Exception:
        mid = 0.0

    best_bid, best_ask = _hl_l2_bbo(sym)

    # ATR abs: use sse micro if present else 0.7% of mid
    atr_abs = None
    try:
        sse = ctx.get("sse_snapshot") if isinstance(ctx.get("sse_snapshot"), dict) else {}
        micro = sse.get("symbol_micro") if isinstance(sse.get("symbol_micro"), dict) else {}
        if micro.get("atr_abs") is not None:
            atr_abs = float(micro.get("atr_abs"))
    except Exception:
        atr_abs = None
    if atr_abs is None:
        atr_abs = max(mid * 0.007, 1.0) if mid > 0 else 1.0

    sr_px = _pick_sr_price(ctx, direction=direction)

    # SR/ATR resting entry (same rule as /trade)
    resting = None
    if mid > 0:
        if sr_px is not None:
            if direction == "LONG" and sr_px >= mid:
                sr_px = None
            if direction == "SHORT" and sr_px <= mid:
                sr_px = None

        use_atr = False
        if sr_px is None:
            use_atr = True
        elif atr_abs and abs(float(sr_px) - float(mid)) > float(atr_abs):
            use_atr = True

        if use_atr:
            resting = (mid - atr_abs) if direction == "LONG" else (mid + atr_abs)
        else:
            resting = float(sr_px)

    if resting is None or resting <= 0:
        resting = mid

    resting = _maker_safe_limit_price(direction=direction, suggested=float(resting), best_bid=best_bid, best_ask=best_ask)

    sl, tp = _det_sltp(direction=direction, entry=float(resting), atr_abs=float(atr_abs))

    meta = allocate_plan_id(args.db, sym, int(os.getenv("EVCLAW_MANUAL_PLAN_TTL_SECONDS", "3600") or 3600))
    display_id = str(meta.get("display_id") or "").strip().upper()
    expires_at = float(meta.get("expires_at") or (_utc_now() + 3600))

    plan = {
        "display_id": display_id,
        "symbol": sym,
        "venue": "hyperliquid",
        "direction_idea": direction,
        "confidence_pct": 15,
        "size_usd": float(size_usd),
        "live": {
            "mid": float(mid) if mid > 0 else None,
            "best_bid": float(best_bid) if best_bid > 0 else None,
            "best_ask": float(best_ask) if best_ask > 0 else None,
            "atr_abs": float(atr_abs),
            "sr_support": (sr_px if direction == "LONG" else None),
            "sr_resistance": (sr_px if direction == "SHORT" else None),
            "hl_net_notional": float(net),
            "hl_equity_live": float(eq) if eq else None,
        },
        "options": {
            "chase": {"label": "FAST", "note": "FAST: hedge only if tape confirms; do not chase into a spike."},
            "limit": {
                "label": "RESTING",
                "limit_price": float(resting),
                "entry": float(resting),
                "cancel_after_minutes": int(os.getenv("EVCLAW_MANUAL_TRADE_CANCEL_AFTER_MINUTES", "60") or 60),
                "cancel_note": f"Auto-cancels ~60m after placing (around {_iso(_utc_now()+3600)} UTC).",
                "note": "RESTING: maker hedge near SR/ATR level; cancel if not filled or if the level breaks.",
            },
        },
        "sltp": {"sl_price": float(sl), "tp_price": float(tp)},
        "why": [
            f"HL net exposure is {'SHORT' if net < 0 else 'LONG'} (~{abs(net):,.0f} notional); this hedge targets ~{pct:.0f}% reduction.",
            "Keep it conservative: hedge is for squeeze protection, not max PnL.",
        ],
        "created_at_iso": _iso(_utc_now()),
        "expires_at_iso": _iso(expires_at),
    }

    json_path = f"/tmp/manual_trade_plan_{display_id}.json"
    with open(json_path, "w") as f:
        json.dump(plan, f, indent=2, sort_keys=True)

    _finalize_plan(display_id, json_path, db_path=args.db)

    print(json.dumps({"ok": True, "stored": {"display_id": display_id, "json_path": json_path, "symbol": sym}, "plan": plan}, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
