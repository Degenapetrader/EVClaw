#!/usr/bin/env python3
"""Generate up to N deterministic manual trade plans from latest EVClaw candidates.

No LLM. Uses:
- latest evclaw_candidates_*.json
- SR levels from candidate context_snapshot (preferred)
- HL live mid (allMids) + BBO (l2Book)

Writes /tmp/manual_trade_plan_<ID>.json and stores READY rows for /execute.
"""

from __future__ import annotations

import argparse
import glob
import json
import os
import sqlite3
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import urllib.request

SCRIPT_PATH = Path(__file__).resolve()
EVCLAW_ROOT = Path(os.getenv("EVCLAW_ROOT") or SCRIPT_PATH.parents[3])
EVCLAW_ROOT_STR = str(EVCLAW_ROOT)
TRADE_DIR = EVCLAW_ROOT / "openclaw_skills" / "trade" / "scripts"
if str(TRADE_DIR) not in sys.path:
    sys.path.insert(0, str(TRADE_DIR))
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


def _load_latest_candidates(runtime_dir: str) -> Optional[Dict[str, Any]]:
    rt = Path(runtime_dir)
    files = sorted(rt.glob("evclaw_candidates_*.json"), key=lambda p: p.stat().st_mtime, reverse=True)
    if not files:
        return None
    path = files[0]
    try:
        obj = json.load(open(path))
        if isinstance(obj, dict):
            obj["_candidates_file"] = str(path)
        return obj if isinstance(obj, dict) else None
    except Exception:
        return None


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
    # Best bid/ask from l2Book levels.
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
    """Nudge limit away from immediate match for post-only safety."""
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

    # Tiny step (1bp of mid) to avoid crossing.
    mid = (bid + ask) / 2.0
    step = max(1e-6, mid * 0.0001)

    if d == "LONG":
        # Must be strictly below ask.
        if px >= ask:
            return max(0.0, ask - step)
    elif d == "SHORT":
        # Must be strictly above bid.
        if px <= bid:
            return bid + step
    return px


def _pick_sr_price(candidate: Dict[str, Any], *, direction: str) -> Optional[float]:
    try:
        km = (candidate.get("context_snapshot") or {}).get("key_metrics") or {}
        sr = (km.get("sr_levels") or {}).get("nearest") or {}
        d = (direction or "").upper()
        if d == "LONG":
            sup = sr.get("support") if isinstance(sr.get("support"), dict) else {}
            px = sup.get("price")
        else:
            res = sr.get("resistance") if isinstance(sr.get("resistance"), dict) else {}
            px = res.get("price")
        if px is None:
            return None
        px = float(px)
        return px if px > 0 else None
    except Exception:
        return None


def _atr_abs_from(candidate: Dict[str, Any], mid: float) -> Optional[float]:
    """Best-effort ATR abs.

    Prefer atr_pct from candidate/context; fallback to a small mid-based proxy (0.7%)
    so SL/TP are never absurd for low-priced coins.
    """
    try:
        atr_pct = candidate.get("atr_pct")
        if atr_pct is None:
            km = (candidate.get("context_snapshot") or {}).get("key_metrics") or {}
            atr_pct = km.get("atr_pct")
        if atr_pct is not None:
            atr_pct = float(atr_pct)
            if atr_pct > 0 and mid > 0:
                return mid * (atr_pct / 100.0)
        # fallback
        if mid > 0:
            return mid * 0.007
        return None
    except Exception:
        return (mid * 0.007) if mid and mid > 0 else None


def _choose_resting_entry(*, direction: str, mid: float, atr_abs: Optional[float], sr_px: Optional[float]) -> Optional[float]:
    d = (direction or "").upper()
    if mid <= 0:
        return None

    # SR sanity: must be on the correct side of mid.
    if sr_px is not None:
        if d == "LONG" and sr_px >= mid:
            sr_px = None
        if d == "SHORT" and sr_px <= mid:
            sr_px = None

    use_atr = False
    if sr_px is None:
        use_atr = True
    elif atr_abs is not None and atr_abs > 0 and abs(sr_px - mid) > atr_abs:
        # SR is too far: fallback to 1x ATR from mid.
        use_atr = True

    if use_atr and atr_abs is not None and atr_abs > 0:
        return (mid - atr_abs) if d == "LONG" else (mid + atr_abs)

    if sr_px is not None:
        return sr_px

    return None


def _det_sltp(*, direction: str, entry: float, atr_abs: Optional[float]) -> Tuple[float, float]:
    d = (direction or "").upper()
    a = float(atr_abs or 0.0)
    if entry <= 0:
        entry = 0.0
    if a <= 0 and entry > 0:
        # relative fallback (1% of entry), never a hard $1.0 floor (breaks low-price coins)
        a = max(entry * 0.01, entry * 0.001)

    sl_dist = 1.2 * a
    tp_dist = 2.0 * a

    if d == "LONG":
        sl = entry - sl_dist
        tp = entry + tp_dist
    else:
        sl = entry + sl_dist
        tp = entry - tp_dist

    # Keep sane positive.
    sl = max(0.0, float(sl))
    tp = max(0.0, float(tp))
    return sl, tp


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
    ap.add_argument("--n", type=int, default=3)
    ap.add_argument("--db", default=DB_PATH_DEFAULT)
    ap.add_argument("--runtime", default=RUNTIME_DIR_DEFAULT)
    args = ap.parse_args()

    n = max(1, min(10, int(args.n)))

    payload = _load_latest_candidates(args.runtime)
    if not isinstance(payload, dict):
        print(json.dumps({"ok": False, "error": "no_candidates"}))
        return 1

    cands = payload.get("candidates") if isinstance(payload.get("candidates"), list) else []
    if not cands:
        print(json.dumps({"ok": False, "error": "no_candidates_in_file", "candidates_file": payload.get("_candidates_file")}))
        return 1

    # rank by blended_conviction (fallback conviction)
    def score(c: Dict[str, Any]) -> float:
        v = c.get("blended_conviction")
        if v is None:
            v = c.get("conviction")
        try:
            return float(v)
        except Exception:
            return 0.0

    ranked = [c for c in cands if isinstance(c, dict) and str(c.get("symbol") or "").strip()]
    ranked.sort(key=score, reverse=True)

    mids = _hl_all_mids()

    # HL perp only: ignore builder symbols like XYZ:NVDA
    def is_hl_perp(sym: str) -> bool:
        return bool(sym) and (":" not in sym)

    filtered: List[Dict[str, Any]] = []
    for c in ranked:
        sym = str(c.get("symbol") or "").strip().upper()
        if not is_hl_perp(sym):
            continue
        try:
            mid = float(mids.get(sym, 0.0))
        except Exception:
            mid = 0.0
        if mid <= 0:
            continue
        filtered.append(c)

    picks = filtered[:n]

    stored: List[Dict[str, Any]] = []
    plans: List[Dict[str, Any]] = []

    for c in picks:
        sym = str(c.get("symbol") or "").strip().upper()
        direction = str(c.get("direction") or "").strip().upper()
        if direction not in {"LONG", "SHORT"}:
            continue

        mid = 0.0
        try:
            mid = float(mids.get(sym, 0.0))
        except Exception:
            mid = 0.0

        best_bid, best_ask = _hl_l2_bbo(sym)

        atr_abs = _atr_abs_from(c, mid)
        sr_px = _pick_sr_price(c, direction=direction)
        resting = _choose_resting_entry(direction=direction, mid=mid, atr_abs=atr_abs, sr_px=sr_px)
        if resting is None or resting <= 0:
            # fallback: use mid if everything missing
            resting = mid

        resting = _maker_safe_limit_price(direction=direction, suggested=float(resting), best_bid=best_bid, best_ask=best_ask)

        # size
        rec_size = None
        try:
            sizing = ((c.get("context_snapshot") or {}).get("sizing") or {}) if isinstance((c.get("context_snapshot") or {}).get("sizing"), dict) else {}
            rec_size = sizing.get("recommended_size_usd")
        except Exception:
            rec_size = None
        if rec_size is None:
            # build_context has sizing too; use that as fallback
            try:
                ctx = build_context(args.db, args.runtime, sym)
                rec_size = ((ctx.get("sizing") or {}) if isinstance(ctx.get("sizing"), dict) else {}).get("recommended_size_usd")
            except Exception:
                rec_size = None

        default_size = float(os.getenv("EVCLAW_MANUAL_TRADE_DEFAULT_SIZE_USD", "5000") or 5000)
        try:
            size_usd = float(rec_size) if rec_size is not None else default_size
        except Exception:
            size_usd = default_size

        sl, tp = _det_sltp(direction=direction, entry=float(resting), atr_abs=atr_abs)

        # allocate id
        meta = allocate_plan_id(args.db, sym, int(os.getenv("EVCLAW_MANUAL_PLAN_TTL_SECONDS", "3600") or 3600))
        display_id = str(meta.get("display_id") or "").strip().upper()
        expires_at = float(meta.get("expires_at") or (_utc_now() + 3600))

        conf = score(c)
        conf_pct = int(max(0, min(100, round(conf * 100))))

        plan = {
            "display_id": display_id,
            "symbol": sym,
            "venue": "hyperliquid",
            "direction_idea": direction,
            "confidence_pct": conf_pct,
            "size_usd": float(size_usd),
            # convenience alias for UI/renderers
            "live_price": float(mid) if mid > 0 else None,
            "live": {
                "mid": float(mid) if mid > 0 else None,
                "best_bid": float(best_bid) if best_bid > 0 else None,
                "best_ask": float(best_ask) if best_ask > 0 else None,
                "atr_abs": float(atr_abs) if atr_abs is not None else None,
                "sr_support": (sr_px if direction == "LONG" else None),
                "sr_resistance": (sr_px if direction == "SHORT" else None),
            },
            "options": {
                "chase": {
                    "label": "FAST",
                    "note": "FAST: only chase if price starts moving your way; avoid forcing entry into chop.",
                },
                "limit": {
                    "label": "RESTING",
                    "limit_price": float(resting),
                    "entry": float(resting),
                    "cancel_after_minutes": int(os.getenv("EVCLAW_MANUAL_TRADE_CANCEL_AFTER_MINUTES", "60") or 60),
                    "cancel_note": f"Auto-cancels ~60m after placing (around {_iso(_utc_now()+3600)} UTC).",
                    "note": "RESTING: place a maker limit at the level; cancel if not filled or if the level breaks.",
                },
            },
            "sltp": {"sl_price": float(sl), "tp_price": float(tp)},
            "why": [
                f"Top-ranked setup by current conviction (~{conf_pct}%).",
                "RESTING entry anchored to nearby support/resistance when available; otherwise uses 1×ATR from mid.",
            ],
            "created_at_iso": _iso(_utc_now()),
            "expires_at_iso": _iso(expires_at),
        }

        json_path = f"/tmp/manual_trade_plan_{display_id}.json"
        with open(json_path, "w") as f:
            json.dump(plan, f, indent=2, sort_keys=True)

        _finalize_plan(display_id, json_path, db_path=args.db)

        stored.append({"display_id": display_id, "json_path": json_path, "symbol": sym, "venue": "hyperliquid"})
        plans.append(plan)

    print(
        json.dumps(
            {
                "ok": True,
                "candidates_file": payload.get("_candidates_file"),
                "seq": payload.get("cycle_seq"),
                "stored": stored,
                "plans": plans,
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
