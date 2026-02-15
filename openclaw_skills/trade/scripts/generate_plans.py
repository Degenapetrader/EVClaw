#!/usr/bin/env python3
"""Generate and store manual /trade plans via LLM gate.

This is the missing deterministic wiring so `/trade` is no longer "hand-written".

Workflow:
- Allocate plan ids (monotonic per symbol) in manual_trade_plans table.
- Load latest evclaw_candidates_*.json (same object used by entry gate).
- Filter candidates to only requested symbols.
- Compact candidates using llm_entry_gate._compact_candidate (proven schema).
- Send payload to the manual trade gate agent.
- Normalize plans + write to /tmp/manual_trade_plan_<ID>.json
- finalize_plan.py stores plan_json and marks READY.

Output: JSON to stdout.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import sqlite3
import sys
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# Keep stdout clean JSON
logging.basicConfig(level=logging.CRITICAL)
logging.getLogger().setLevel(logging.CRITICAL)

SCRIPT_PATH = Path(__file__).resolve()
EVCLAW_ROOT = Path(os.getenv("EVCLAW_ROOT") or SCRIPT_PATH.parents[3])
DB_PATH_DEFAULT = str(Path(os.getenv("EVCLAW_DB_PATH") or (EVCLAW_ROOT / "ai_trader.db")))
RUNTIME_DIR_DEFAULT = str(Path(os.getenv("EVCLAW_RUNTIME_DIR") or (EVCLAW_ROOT / "state")))

HL_INFO_URL = "https://api.hyperliquid.xyz/info"

TRADE_DIR = Path(__file__).resolve().parent

# Import allocate_context helpers for plan id allocation + sizing/exposure snapshot.
from allocate_context import allocate_plan_id, build_context  # type: ignore
from finalize_plan import main as finalize_main  # type: ignore

EVCLAW_ROOT_STR = str(EVCLAW_ROOT)
if EVCLAW_ROOT_STR not in sys.path:
    sys.path.insert(0, EVCLAW_ROOT_STR)

from openclaw_agent_client import safe_json_loads  # type: ignore

# Reuse entry-gate compaction to avoid schema drift.
from llm_entry_gate import _compact_candidate  # type: ignore

from trade_gate import run_trade_gate  # type: ignore

from conviction_model import compute_brain_conviction_no_floor  # type: ignore


def _iso(ts: float) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _hl_all_mids() -> Dict[str, str]:
    payload = {"type": "allMids"}
    req = urllib.request.Request(
        HL_INFO_URL,
        data=json.dumps(payload).encode(),
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=10) as r:
        return json.loads(r.read().decode("utf-8"))


def _hl_mid(symbol: str) -> Optional[float]:
    """Fetch live mid from Hyperliquid allMids."""
    sym = str(symbol or "").strip().upper()
    if not sym:
        return None
    try:
        mids = _hl_all_mids()
        raw = mids.get(sym)
        if raw is None:
            return None
        px = float(raw)
        return px if px > 0 else None
    except Exception:
        return None


def _hl_l2_best_bid_ask(symbol: str) -> Tuple[Optional[float], Optional[float]]:
    """Fetch best bid/ask from Hyperliquid l2Book. Returns (bid, ask)."""
    sym = str(symbol or "").strip().upper()
    if not sym:
        return None, None

    payload = {"type": "l2Book", "coin": sym}
    req = urllib.request.Request(
        HL_INFO_URL,
        data=json.dumps(payload).encode(),
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            data = json.loads(r.read().decode("utf-8"))
    except Exception:
        return None, None

    try:
        levels = data.get("levels") or []
        bids = levels[0] if len(levels) > 0 else []
        asks = levels[1] if len(levels) > 1 else []
        best_bid = float(bids[0]["px"]) if bids else None
        best_ask = float(asks[0]["px"]) if asks else None
        if best_bid is not None and best_bid <= 0:
            best_bid = None
        if best_ask is not None and best_ask <= 0:
            best_ask = None
        return best_bid, best_ask
    except Exception:
        return None, None


def _maker_safe_limit_price(
    *,
    direction: str,
    suggested: float,
    best_bid: Optional[float],
    best_ask: Optional[float],
) -> float:
    """Adjust limit price so a post-only order won't immediately match."""
    d = str(direction or "").upper()
    px = float(suggested)

    # Small buffer to stay safely outside the spread.
    ref = best_ask if d == "SHORT" else best_bid
    if ref is None or ref <= 0:
        return px

    buffer_abs = max(0.0001, ref * 0.0005)  # 5 bps or $0.0001

    if d == "SHORT":
        # Sell post-only must be >= best_ask + buffer.
        min_px = float(ref) + buffer_abs
        return max(px, min_px)
    if d == "LONG":
        # Buy post-only must be <= best_bid - buffer.
        max_px = float(ref) - buffer_abs
        return min(px, max_px)

    return px


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


def _load_latest_cycle(runtime_dir: str) -> Optional[Dict[str, Any]]:
    rt = Path(runtime_dir)
    files = sorted(rt.glob("evclaw_cycle_*.json"), key=lambda p: p.stat().st_mtime, reverse=True)
    if not files:
        return None
    path = files[0]
    try:
        obj = json.load(open(path))
        if isinstance(obj, dict):
            obj["_cycle_file"] = str(path)
        return obj if isinstance(obj, dict) else None
    except Exception:
        return None


def _load_latest_context(runtime_dir: str) -> Optional[Dict[str, Any]]:
    rt = Path(runtime_dir)
    files = sorted(rt.glob("evclaw_context_*.json"), key=lambda p: p.stat().st_mtime, reverse=True)
    if not files:
        return None
    path = files[0]
    try:
        obj = json.load(open(path))
        if isinstance(obj, dict):
            obj["_context_file"] = str(path)
        return obj if isinstance(obj, dict) else None
    except Exception:
        return None


def _compact_open_positions(db_path: str, limit: int = 12) -> List[Dict[str, Any]]:
    rows = []
    try:
        with sqlite3.connect(db_path, timeout=10.0) as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                """
                SELECT symbol, direction, venue, COALESCE(notional_usd, ABS(size*entry_price), 0) AS notional
                FROM trades
                WHERE exit_time IS NULL
                ORDER BY notional DESC
                LIMIT ?
                """,
                (int(limit),),
            ).fetchall()
    except Exception:
        rows = []
    out: List[Dict[str, Any]] = []
    for r in rows or []:
        try:
            out.append(
                {
                    "symbol": str(r[0] or "").upper(),
                    "direction": str(r[1] or "").upper(),
                    "venue": str(r[2] or "").lower(),
                    "notional_usd": float(r[3] or 0.0),
                }
            )
        except Exception:
            continue
    return out


def _build_plan_meta(*, symbol: str, display_id: str, expires_at: float, ctx: Dict[str, Any], user_size_usd: Optional[float], direction_override: Optional[str]) -> Dict[str, Any]:
    sym = str(symbol or "").upper()

    sizing = (ctx.get("sizing") or {}) if isinstance(ctx.get("sizing"), dict) else {}
    rec_size = sizing.get("recommended_size_usd")

    # Default confidence = conviction% from candidate (filled later); placeholder 50.
    out = {
        "symbol": sym,
        "display_id": str(display_id).upper(),
        "venue": "hyperliquid",
        "expires_at": float(expires_at),
        "expires_at_iso": _iso(float(expires_at)),
        "cancel_after_minutes": int(os.getenv("EVCLAW_MANUAL_TRADE_CANCEL_AFTER_MINUTES", "60") or 60),
        "recommended_size_usd": float(rec_size) if rec_size is not None else None,
        "user_size_usd": float(user_size_usd) if user_size_usd is not None else None,
        "direction_hint": str(direction_override).upper() if direction_override else None,
        "confidence_pct": 50,
        "live_prices": ctx.get("live_prices"),
        "sse_snapshot": ctx.get("sse_snapshot"),
        "monitor_snapshot": ctx.get("monitor_snapshot"),
    }
    return out


def _write_plan_file(display_id: str, plan: Dict[str, Any]) -> str:
    did = str(display_id or "").strip().upper()
    path = f"/tmp/manual_trade_plan_{did}.json"
    with open(path, "w") as f:
        json.dump(plan, f, indent=2, sort_keys=True)
    return path


def _finalize_plan(display_id: str, json_file: str, *, db_path: str) -> None:
    # Call finalize_plan.py main() in-process for speed.
    argv0 = sys.argv[:]
    try:
        sys.argv = ["finalize_plan.py", str(display_id), str(json_file), "--db", str(db_path)]
        rc = finalize_main()
        if int(rc) != 0:
            raise RuntimeError(f"finalize failed rc={rc}")
    finally:
        sys.argv = argv0


async def main_async() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("symbols", nargs="+", help="One or more symbols, e.g. ETH BTC")
    ap.add_argument("--direction", choices=["long", "short", "LONG", "SHORT"], default=None)
    ap.add_argument("--size-usd", type=float, default=None)
    ap.add_argument("--db", default=DB_PATH_DEFAULT)
    ap.add_argument("--runtime", default=RUNTIME_DIR_DEFAULT)
    args = ap.parse_args()

    syms = [str(s).strip().upper() for s in (args.symbols or []) if str(s).strip()]
    syms = list(dict.fromkeys(syms))
    if not syms:
        print(json.dumps({"ok": False, "error": "no_symbols"}))
        return 2

    direction_override = None
    if args.direction:
        direction_override = str(args.direction).upper()

    cand_payload = _load_latest_candidates(args.runtime)
    if not isinstance(cand_payload, dict):
        print(json.dumps({"ok": False, "error": "no_candidates_file"}))
        return 1

    context_file = str(cand_payload.get("context_file") or "").strip()
    global_context_compact = ""
    seq = None
    if cand_payload.get("cycle_seq") is not None:
        try:
            seq = int(cand_payload.get("cycle_seq"))
        except Exception:
            seq = None

    sr_map: Dict[str, Dict[str, float]] = {}
    if context_file and Path(context_file).exists():
        try:
            ctx_obj = json.load(open(context_file))
            global_context_compact = str(ctx_obj.get("global_context_compact") or "") if isinstance(ctx_obj, dict) else ""

            # SR levels map from context selected_opportunities (nearest support/resistance).
            sel = (ctx_obj.get("selected_opportunities") if isinstance(ctx_obj, dict) else None)
            if isinstance(sel, list):
                for it in sel:
                    if not isinstance(it, dict):
                        continue
                    sym = str(it.get("symbol") or "").strip().upper()
                    km = it.get("key_metrics") if isinstance(it.get("key_metrics"), dict) else {}
                    sr = km.get("sr_levels") if isinstance(km.get("sr_levels"), dict) else {}
                    near = sr.get("nearest") if isinstance(sr.get("nearest"), dict) else {}
                    sup = near.get("support") if isinstance(near.get("support"), dict) else {}
                    res = near.get("resistance") if isinstance(near.get("resistance"), dict) else {}
                    out: Dict[str, float] = {}
                    if sup.get("price") is not None:
                        out["support"] = float(sup.get("price"))
                    if res.get("price") is not None:
                        out["resistance"] = float(res.get("price"))
                    if sym and out:
                        sr_map[sym] = out
        except Exception:
            global_context_compact = ""
            sr_map = {}

    candidates = cand_payload.get("candidates") or []
    if not isinstance(candidates, list):
        candidates = []

    candidates_by_symbol: Dict[str, Dict[str, Any]] = {}
    for c in candidates:
        if not isinstance(c, dict):
            continue
        sym = str(c.get("symbol") or "").strip().upper()
        if sym:
            candidates_by_symbol[sym] = c

    missing = [s for s in syms if s not in candidates_by_symbol]
    if missing:
        # Fallback: build candidate-shaped blobs from the raw SSE cycle snapshot.
        cycle = _load_latest_cycle(args.runtime)
        symbols_map = cycle.get("symbols") if isinstance(cycle, dict) else None
        symbols_map = symbols_map if isinstance(symbols_map, dict) else {}

        for sym in list(missing):
            blob = symbols_map.get(sym)
            if not isinstance(blob, dict):
                continue

            perp = blob.get("perp_signals") if isinstance(blob.get("perp_signals"), dict) else {}
            smart = blob.get("smart_dumb_cvd") if isinstance(blob.get("smart_dumb_cvd"), dict) else {}
            atr = blob.get("atr") if isinstance(blob.get("atr"), dict) else {}

            # Build signals_snapshot in the same shape conviction_model expects.
            signals_snapshot = {}
            for k in ("cvd", "fade", "ofm", "whale", "dead_capital"):
                p = perp.get(k)
                if not isinstance(p, dict):
                    continue
                # Keep the raw signal string in `signal`; conviction_model normalizes it.
                payload = dict(p)
                # Normalize direction field when present.
                sig = str(payload.get("signal") or payload.get("direction") or "").upper()
                if sig in ("LONG", "SHORT"):
                    payload["direction"] = sig
                signals_snapshot[k] = payload

            key_metrics = {
                "atr_pct": atr.get("atr_pct"),
                "smart_cvd": smart.get("smart_cvd"),
                "divergence_z": smart.get("divergence_z"),
                "cohort_signal": (blob.get("by_cohort") or {}).get("signal") if isinstance(blob.get("by_cohort"), dict) else None,
            }

            # Decide direction by max brain conviction.
            conv_long = compute_brain_conviction_no_floor(
                signals_snapshot=signals_snapshot,
                key_metrics=key_metrics,
                direction="LONG",
            )
            conv_short = compute_brain_conviction_no_floor(
                signals_snapshot=signals_snapshot,
                key_metrics=key_metrics,
                direction="SHORT",
            )
            direction = "LONG" if conv_long >= conv_short else "SHORT"
            brain_conv = max(conv_long, conv_short)

            # Attach dossier (from build_context) later via plan_meta; here keep placeholder.
            candidate = {
                "symbol": sym,
                "direction": direction,
                "conviction": float(brain_conv),
                "pipeline_conviction": 0.0,
                "brain_conviction": float(brain_conv),
                "blended_conviction": float(brain_conv),
                "reason_short": "manual_trade_gate_fallback_from_cycle",
                "signals": [],
                "strong_signals": [],
                "rank": 999,
                "signals_snapshot": signals_snapshot,
                "context_snapshot": {"key_metrics": key_metrics},
            }
            candidates_by_symbol[sym] = candidate

        still_missing = [s for s in syms if s not in candidates_by_symbol]
        if still_missing:
            print(
                json.dumps(
                    {
                        "ok": False,
                        "error": "symbols_not_in_candidates_or_cycle",
                        "missing": still_missing,
                        "candidates_file": cand_payload.get("_candidates_file"),
                        "cycle_file": cycle.get("_cycle_file") if isinstance(cycle, dict) else None,
                    },
                    indent=2,
                )
            )
            return 1

    # allocate plan ids + build per-symbol context
    plan_metas: List[Dict[str, Any]] = []
    for sym in syms:
        plan = allocate_plan_id(args.db, sym, int(os.getenv("EVCLAW_MANUAL_PLAN_TTL_SECONDS", "3600") or 3600))
        ctx = build_context(args.db, args.runtime, sym)
        plan_metas.append(
            _build_plan_meta(
                symbol=sym,
                display_id=str(plan.get("display_id")),
                expires_at=float(plan.get("expires_at")),
                ctx=ctx,
                user_size_usd=args.size_usd,
                direction_override=direction_override,
            )
        )

    # build compact candidates (inject dossier so LLM gate can respect vetoes)
    compact: List[Dict[str, Any]] = []
    for i, sym in enumerate(syms):
        c = dict(candidates_by_symbol[sym])
        if direction_override in ("LONG", "SHORT"):
            c["direction"] = direction_override
        cc = _compact_candidate(c)
        # Inject dossier from plan_meta context (learning conclusion for this symbol)
        if i < len(plan_metas) and isinstance(plan_metas[i], dict):
            meta_ctx = plan_metas[i]
            sse = meta_ctx.get("sse_snapshot")
            if isinstance(sse, dict) and sse.get("symbol_micro"):
                cc["sse_micro"] = sse["symbol_micro"]
        # Inject dossier from build_context (already computed per symbol)
        try:
            from learning_dossier_aggregator import get_dossier_snippet  # type: ignore
            dossier = get_dossier_snippet(args.db, sym, max_chars=500) or ""
            if dossier:
                cc["dossier"] = dossier
        except Exception:
            pass
        compact.append(cc)

    # Map confidence% directly from our conviction model (blended_conviction preferred).
    conv_by_symbol: Dict[str, int] = {}
    for sym in syms:
        c = candidates_by_symbol.get(sym) or {}
        val = c.get("blended_conviction", c.get("conviction"))
        try:
            pct = int(round(float(val) * 100))
        except Exception:
            pct = 50
        conv_by_symbol[sym] = max(0, min(100, pct))

    for m in plan_metas:
        if not isinstance(m, dict):
            continue
        sym = str(m.get("symbol") or "").upper()
        if sym in conv_by_symbol:
            m["confidence_pct"] = int(conv_by_symbol[sym])

    # exposure snapshot: keep simple (manual skill)
    exposure = {}
    # Use monitor snapshot as exposure: DB already has it
    try:
        from ai_trader_db import AITraderDB  # type: ignore

        db = AITraderDB(args.db)
        snap = db.get_latest_monitor_snapshot() or {}
        if isinstance(snap, dict):
            exposure = {
                "hl_equity_usd": snap.get("hl_equity"),
                "hl_net_exposure_usd": snap.get("hl_net_notional"),
                "hl_long_notional": snap.get("hl_long_notional"),
                "hl_short_notional": snap.get("hl_short_notional"),
                "ts_iso": snap.get("ts_iso"),
            }
    except Exception:
        exposure = {}

    open_positions = _compact_open_positions(args.db)

    # Run trade-gate
    gate = await run_trade_gate(
        seq=seq,
        global_context_compact=global_context_compact,
        exposure=exposure,
        open_positions=open_positions,
        candidates_compact=compact,
        plan_metas=plan_metas,
    )

    plans = gate.get("plans") if isinstance(gate, dict) else None
    if not isinstance(plans, list) or not plans:
        print(json.dumps({"ok": False, "error": gate.get("error") if isinstance(gate, dict) else "gate_failed", "gate": gate}, indent=2))
        return 1

    stored: List[Dict[str, Any]] = []
    now = time.time()

    meta_by_symbol: Dict[str, Dict[str, Any]] = {}
    for m in plan_metas:
        if isinstance(m, dict) and m.get('symbol'):
            meta_by_symbol[str(m.get('symbol')).upper()] = m

    for p in plans:
        if not isinstance(p, dict):
            continue
        did = str(p.get("display_id") or "").strip().upper()
        if not did:
            continue

        # Live HL price context (mid + BBO).
        mid = None
        best_bid = None
        best_ask = None
        atr_abs = None
        sr_support = None
        sr_resistance = None
        try:
            sym0 = str(p.get("symbol") or "").strip().upper()
            mid = _hl_mid(sym0)
            best_bid, best_ask = _hl_l2_best_bid_ask(sym0)

            meta = meta_by_symbol.get(sym0) or {}
            sse = meta.get('sse_snapshot') if isinstance(meta, dict) else None
            sse_micro = (sse.get('symbol_micro') if isinstance(sse, dict) else None) if sse else None
            if isinstance(sse_micro, dict):
                if sse_micro.get('atr_abs') is not None:
                    atr_abs = float(sse_micro.get('atr_abs'))

            # Prefer SR from context selected_opportunities map; fallback to sse_micro (if present)
            try:
                sr_support = (sr_map.get(sym0, {}) or {}).get('support')
                sr_resistance = (sr_map.get(sym0, {}) or {}).get('resistance')
            except Exception:
                sr_support = None
                sr_resistance = None

            if sr_support is None and isinstance(sse_micro, dict):
                sr_support = sse_micro.get('sr_support')
            if sr_resistance is None and isinstance(sse_micro, dict):
                sr_resistance = sse_micro.get('sr_resistance')
        except Exception:
            mid, best_bid, best_ask, atr_abs = None, None, None, None

        # Compute cancel note now (time of planning) for user-facing clarity.
        cancel_min = 60
        try:
            cancel_min = int(((p.get("options") or {}).get("limit") or {}).get("cancel_after_minutes") or 60)
        except Exception:
            cancel_min = 60
        cancel_at = now + cancel_min * 60
        cancel_iso = _iso(cancel_at)

        # Force RESTING limit to be post-only safe (avoid immediate-match rejection).
        try:
            p.setdefault("options", {})
            p["options"].setdefault("limit", {})
            lim = p["options"]["limit"]
            raw_lim = float(lim.get("limit_price") or 0.0)
            if raw_lim > 0:
                # 1) Ensure post-only safe relative to BBO.
                safe_lim = _maker_safe_limit_price(
                    direction=str(p.get("direction_idea") or ""),
                    suggested=raw_lim,
                    best_bid=best_bid,
                    best_ask=best_ask,
                )

                # 2) SR-limit entry logic (boss rule):
                # - If we have SR levels: rest at SR (support for LONG, resistance for SHORT).
                # - If SR is missing OR too far: use 1x ATR from live mid.
                # - Always keep post-only safe.

                d = str(p.get("direction_idea") or "").upper()

                sr_px = None
                try:
                    if d == 'LONG' and sr_support is not None:
                        sr_px = float(sr_support)
                    elif d == 'SHORT' and sr_resistance is not None:
                        sr_px = float(sr_resistance)
                    if sr_px is not None and sr_px <= 0:
                        sr_px = None
                except Exception:
                    sr_px = None

                use_atr = False
                if mid is None or mid <= 0:
                    use_atr = False
                else:
                    # Validate SR is on correct side of mid.
                    if sr_px is not None:
                        if d == "LONG" and sr_px >= float(mid):
                            sr_px = None
                        if d == "SHORT" and sr_px <= float(mid):
                            sr_px = None

                    if sr_px is None:
                        use_atr = True
                    elif atr_abs is not None and atr_abs > 0 and abs(float(sr_px) - float(mid)) > float(atr_abs):
                        # SR is too far away -> 1x ATR fallback.
                        use_atr = True

                if mid is not None and mid > 0 and atr_abs is not None and atr_abs > 0 and use_atr:
                    if d == "LONG":
                        safe_lim = float(mid) - float(atr_abs)
                    elif d == "SHORT":
                        safe_lim = float(mid) + float(atr_abs)
                elif sr_px is not None:
                    safe_lim = float(sr_px)

                # Re-apply post-only safety after we set SR/ATR entry.
                safe_lim = _maker_safe_limit_price(
                    direction=d,
                    suggested=float(safe_lim),
                    best_bid=best_bid,
                    best_ask=best_ask,
                )

                lim["limit_price"] = float(safe_lim)

            lim["cancel_note"] = f"Auto-cancels ~{cancel_min}m after placing (around {cancel_iso} UTC)."
            # Add explicit entry display fields for UI output.
            lim["entry"] = float(lim.get("limit_price") or 0.0)
            p.setdefault("live", {})
            p["live"] = {
                "mid": mid,
                "best_bid": best_bid,
                "best_ask": best_ask,
                "atr_abs": atr_abs,
                "sr_support": sr_support,
                "sr_resistance": sr_resistance,
            }
        except Exception:
            pass

        # Sanitize why bullets: never mention dossier/override in user output.
        try:
            why = p.get("why")
            if isinstance(why, list):
                cleaned = []
                for b in why:
                    s = str(b or "").strip()
                    if not s:
                        continue
                    upper = s.upper()
                    if "DOSSIER" in upper:
                        # Replace with plain-English learned-edge explanation.
                        s = "Recent performance/learned edge suggests the original side is higher risk, so this plan prefers the opposite direction."
                    s = s.replace("DOSSIER OVERRIDE:", "").replace("dossier", "").strip()
                    cleaned.append(s)
                p["why"] = cleaned[:5]
        except Exception:
            pass

        json_path = _write_plan_file(did, p)
        _finalize_plan(did, json_path, db_path=args.db)
        stored.append({"display_id": did, "json_path": json_path, "symbol": p.get("symbol"), "venue": p.get("venue")})

    print(
        json.dumps(
            {
                "ok": True,
                "symbols": syms,
                "candidates_file": cand_payload.get("_candidates_file"),
                "seq": seq,
                "stored": stored,
                "gate": {"session_id": gate.get("session_id"), "enabled": gate.get("enabled"), "error": gate.get("error")},
                "plans": plans,
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


def main() -> int:
    return asyncio.run(main_async())


if __name__ == "__main__":
    raise SystemExit(main())
