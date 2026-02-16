#!/usr/bin/env python3
"""Allocate a monotonic manual plan id and print a compact context blob.

Output: JSON to stdout.
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
RUNTIME_DIR_DEFAULT = str(Path(os.getenv("EVCLAW_RUNTIME_DIR") or (EVCLAW_ROOT / "state")))
PLAN_TTL_SECONDS_DEFAULT = 60 * 60

EVCLAW_ROOT_STR = str(EVCLAW_ROOT)
if EVCLAW_ROOT_STR not in sys.path:
    sys.path.insert(0, EVCLAW_ROOT_STR)

# Prevent config_env from printing its one-line startup summary.
os.environ.setdefault("_EVCLAW_CONFIG_LOGGED", "1")
os.environ.setdefault("EVCLAW_LOG_LEVEL", "CRITICAL")

from ai_trader_db import AITraderDB  # type: ignore
from context_runtime import load_latest_context_payload  # type: ignore
from learning_dossier_aggregator import get_dossier_snippet  # type: ignore

from cli import load_config, build_execution_config  # type: ignore
from executor import Executor  # type: ignore


def _now() -> float:
    return time.time()


def _iso(ts: float) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()


def _ensure_manual_tables(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS manual_plan_counters (
            symbol TEXT PRIMARY KEY,
            next_seq INTEGER NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS manual_trade_plans (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            display_id TEXT NOT NULL UNIQUE,
            symbol TEXT NOT NULL,
            seq INTEGER NOT NULL,
            status TEXT NOT NULL DEFAULT 'DRAFT',
            created_at REAL NOT NULL,
            expires_at REAL NOT NULL,
            plan_json TEXT,
            executed_at REAL,
            executed_option TEXT,
            executed_trade_id INTEGER,
            error TEXT
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_manual_trade_plans_symbol_created ON manual_trade_plans(symbol, created_at)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_manual_trade_plans_expires ON manual_trade_plans(expires_at)"
    )


def allocate_plan_id(db_path: str, symbol: str, ttl_seconds: int) -> Dict[str, Any]:
    sym = str(symbol or "").strip().upper()
    if not sym:
        raise SystemExit("symbol required")

    now = _now()
    exp = now + int(ttl_seconds)

    conn = sqlite3.connect(db_path, timeout=30.0)
    try:
        conn.execute("PRAGMA busy_timeout=30000")
        _ensure_manual_tables(conn)
        conn.execute("BEGIN IMMEDIATE")

        row = conn.execute(
            "SELECT next_seq FROM manual_plan_counters WHERE symbol = ?",
            (sym,),
        ).fetchone()
        if row is None:
            seq = 1
            conn.execute(
                "INSERT INTO manual_plan_counters(symbol,next_seq) VALUES(?,?)",
                (sym, 2),
            )
        else:
            seq = int(row[0] or 1)
            conn.execute(
                "UPDATE manual_plan_counters SET next_seq = ? WHERE symbol = ?",
                (seq + 1, sym),
            )

        display_id = f"{sym}-{seq:02d}"

        conn.execute(
            """
            INSERT INTO manual_trade_plans(display_id,symbol,seq,status,created_at,expires_at,plan_json)
            VALUES(?,?,?,?,?,?,NULL)
            """,
            (display_id, sym, int(seq), "DRAFT", float(now), float(exp)),
        )

        conn.commit()
        return {
            "display_id": display_id,
            "symbol": sym,
            "seq": int(seq),
            "created_at": float(now),
            "expires_at": float(exp),
        }
    finally:
        try:
            conn.close()
        except Exception:
            pass


def _normalize_hl_base_url(raw: str) -> str:
    base = (raw or "").strip()
    if not base:
        return "https://node2.evplus.ai/evclaw"
    if base.endswith("/info"):
        base = base[:-5]
    return base.rstrip("/")


def _normalize_hl_public_base_url(raw: str) -> str:
    base = (raw or "").strip()
    if not base:
        return "https://api.hyperliquid.xyz"
    if base.endswith("/info"):
        base = base[:-5]
    return base.rstrip("/")


def _private_node_wallet_key() -> str:
    return os.getenv("HYPERLIQUID_ADDRESS", "").strip()


def _hl_info_targets(base_url: str) -> List[Tuple[str, Optional[Dict[str, str]]]]:
    key = _private_node_wallet_key()
    primary_params: Optional[Dict[str, str]] = None
    if key and "node2.evplus.ai" in base_url:
        primary_params = {"key": key}

    targets: List[Tuple[str, Optional[Dict[str, str]]]] = [
        (f"{base_url}/info", primary_params),
    ]
    public_base = _normalize_hl_public_base_url(os.getenv("HYPERLIQUID_PUBLIC_URL", ""))
    if public_base and public_base != base_url:
        targets.append((f"{public_base}/info", None))
    return targets


def _resolve_hl_equity_address() -> Optional[str]:
    # Unified account mode: HYPERLIQUID_ADDRESS is the primary wallet address.
    # VAULT_ADDRESS is checked for backward compatibility but will be None in unified mode.
    for k in ("VAULT_ADDRESS", "HYPERLIQUID_ADDRESS", "HL_ADDRESS"):
        v = (os.getenv(k) or "").strip()
        if v:
            return v
    return None


async def _fetch_live_hl_equity() -> Optional[Dict[str, Any]]:
    """Best-effort live HL account equity from the API (not DB snapshots)."""
    address = _resolve_hl_equity_address()
    if not address:
        return None
    base_url = _normalize_hl_base_url(os.getenv("HYPERLIQUID_PRIVATE_NODE", ""))

    def _do() -> Optional[Dict[str, Any]]:
        import requests  # type: ignore

        payload = {"type": "clearinghouseState", "user": address}
        for info_url, params in _hl_info_targets(base_url):
            try:
                kwargs: Dict[str, Any] = {"json": payload, "timeout": 5}
                if params:
                    kwargs["params"] = params
                resp = requests.post(info_url, **kwargs)
                resp.raise_for_status()
                state = resp.json() if hasattr(resp, "json") else None
                if not isinstance(state, dict):
                    continue
                margin = state.get("marginSummary") if isinstance(state.get("marginSummary"), dict) else {}
                eq = float(margin.get("accountValue", 0.0) or 0.0)
                if eq <= 0:
                    continue
                ts = _now()
                return {"equity": eq, "address": address, "base_url": info_url, "ts": ts, "ts_iso": _iso(ts)}
            except Exception:
                continue
        return None

    try:
        return await asyncio.to_thread(_do)
    except Exception:
        return None


async def _fetch_live_prices(symbol: str) -> Optional[Dict[str, Any]]:
    """Best-effort live bid/ask snapshot (read-only)."""
    sym = str(symbol or "").strip().upper()
    if not sym:
        return None

    executor: Optional[Executor] = None
    try:
        config = load_config()
        exec_config = build_execution_config(config, dry_run=False)
        executor = Executor(config=exec_config)
        await executor.initialize()
        bid, ask = await executor.hyperliquid.get_best_bid_ask(sym)
        bid_f = float(bid or 0.0)
        ask_f = float(ask or 0.0)
        if bid_f <= 0 or ask_f <= 0:
            return None
        mid = (bid_f + ask_f) / 2.0
        spread_bps = ((ask_f - bid_f) / mid) * 10_000 if mid > 0 else None
        now = _now()
        return {
            "venue": "hyperliquid",
            "bid": bid_f,
            "ask": ask_f,
            "mid": mid,
            "spread_bps": float(spread_bps) if spread_bps is not None else None,
            "ts": now,
            "ts_iso": _iso(now),
        }
    except Exception:
        return None
    finally:
        if executor is not None:
            try:
                await executor.close()
            except Exception:
                pass


def _extract_cycle_symbol_micro(blob: Dict[str, Any]) -> Dict[str, Any]:
    """Compact symbol view sourced from the raw cycle SSE snapshot."""
    out: Dict[str, Any] = {}

    out["price"] = blob.get("price")

    price_change = blob.get("price_change") if isinstance(blob.get("price_change"), dict) else {}
    if isinstance(price_change, dict):
        out["pct_24h"] = price_change.get("pct_24h")

    atr = blob.get("atr") if isinstance(blob.get("atr"), dict) else {}
    if isinstance(atr, dict):
        out["atr_pct"] = atr.get("atr_pct")
        out["atr_abs"] = atr.get("atr_abs")

    spread = blob.get("spread") if isinstance(blob.get("spread"), dict) else {}
    if isinstance(spread, dict):
        out["hl_ref_spread_bps"] = spread.get("spread_bps")
        out["hl_ref_spread_z"] = spread.get("zscore")

    # SR levels (nearest support/resistance)
    sr = blob.get("sr_levels") if isinstance(blob.get("sr_levels"), dict) else {}
    if isinstance(sr, dict):
        nearest = sr.get("nearest") if isinstance(sr.get("nearest"), dict) else {}
        if isinstance(nearest, dict):
            sup = nearest.get("support") if isinstance(nearest.get("support"), dict) else {}
            res = nearest.get("resistance") if isinstance(nearest.get("resistance"), dict) else {}
            if isinstance(sup, dict) and sup.get("price") is not None:
                out["sr_support"] = sup.get("price")
            if isinstance(res, dict) and res.get("price") is not None:
                out["sr_resistance"] = res.get("price")

    cvd = blob.get("smart_dumb_cvd") if isinstance(blob.get("smart_dumb_cvd"), dict) else {}
    if isinstance(cvd, dict):
        out["cvd_divergence_signal"] = cvd.get("divergence_signal")
        out["cvd_divergence_z"] = cvd.get("divergence_z")
        out["cvd_significance"] = cvd.get("significance")

    summary = blob.get("summary") if isinstance(blob.get("summary"), dict) else {}
    if isinstance(summary, dict):
        out["zone"] = summary.get("zone")
        out["net_bias"] = summary.get("net_bias")
        out["net_bias_pct"] = summary.get("net_bias_pct")
        out["long_count"] = summary.get("long_count")
        out["short_count"] = summary.get("short_count")
        out["avg_leverage"] = summary.get("avg_leverage")
        out["margin_pct"] = summary.get("margin_pct")

    perp = blob.get("perp_signals") if isinstance(blob.get("perp_signals"), dict) else {}
    if isinstance(perp, dict):
        dead = perp.get("dead_capital") if isinstance(perp.get("dead_capital"), dict) else {}
        whale = perp.get("whale") if isinstance(perp.get("whale"), dict) else {}
        if isinstance(dead, dict) and dead:
            out["dead_capital"] = {
                "locked_long_pct": dead.get("locked_long_pct"),
                "locked_short_pct": dead.get("locked_short_pct"),
                "z_imbalance": dead.get("z_imbalance"),
                "override_trigger": dead.get("override_trigger"),
                "banner_trigger": dead.get("banner_trigger"),
            }
        if isinstance(whale, dict) and whale:
            out["whale"] = {
                "strength": whale.get("strength"),
                "banner_trigger": whale.get("banner_trigger"),
            }

    return out


def _extract_cycle_market_snapshot(cycle: Dict[str, Any], *, top_n: int = 5) -> Dict[str, Any]:
    """Compact market view sourced from the raw cycle SSE snapshot."""
    syms = cycle.get("symbols") if isinstance(cycle.get("symbols"), dict) else {}
    if not isinstance(syms, dict) or not syms:
        return {}

    zone_counts: Dict[str, int] = {}
    bias_counts: Dict[str, int] = {}
    leverage_vals = []

    lopsided_longs = []  # (net_bias_pct, symbol)
    lopsided_shorts = []

    for sym, blob in syms.items():
        if not isinstance(blob, dict):
            continue
        summary = blob.get("summary") if isinstance(blob.get("summary"), dict) else {}
        if not isinstance(summary, dict) or not summary:
            continue

        zone = str(summary.get("zone") or "").strip().lower() or "unknown"
        zone_counts[zone] = zone_counts.get(zone, 0) + 1

        bias = str(summary.get("net_bias") or "").strip().upper() or "UNKNOWN"
        bias_counts[bias] = bias_counts.get(bias, 0) + 1

        try:
            lev = float(summary.get("avg_leverage"))
            if lev > 0:
                leverage_vals.append(lev)
        except Exception:
            pass

        try:
            pct = float(summary.get("net_bias_pct"))
        except Exception:
            pct = None
        if pct is None:
            continue

        if bias == "LONG":
            lopsided_longs.append((pct, str(sym).upper()))
        elif bias == "SHORT":
            lopsided_shorts.append((pct, str(sym).upper()))

    lopsided_longs.sort(reverse=True)
    lopsided_shorts.sort(reverse=True)

    avg_lev = (sum(leverage_vals) / len(leverage_vals)) if leverage_vals else None

    return {
        "symbol_count": cycle.get("symbol_count"),
        "zone_counts": zone_counts,
        "net_bias_counts": bias_counts,
        "avg_leverage": round(float(avg_lev), 2) if avg_lev is not None else None,
        "most_lopsided_longs": [{"symbol": s, "net_bias_pct": p} for p, s in lopsided_longs[:top_n]],
        "most_lopsided_shorts": [{"symbol": s, "net_bias_pct": p} for p, s in lopsided_shorts[:top_n]],
    }


def _load_latest_cycle_snapshot(runtime_dir: str) -> Optional[Dict[str, Any]]:
    rt = Path(runtime_dir)
    if not rt.exists():
        return None
    files = sorted(rt.glob("evclaw_cycle_*.json"), key=lambda p: p.stat().st_mtime, reverse=True)
    if not files:
        return None
    path = files[0]
    try:
        with open(path, "r") as f:
            obj = json.load(f)
        if isinstance(obj, dict):
            obj["_cycle_file"] = str(path)
        return obj if isinstance(obj, dict) else None
    except Exception:
        return None


def build_context(db_path: str, runtime_dir: str, symbol: str) -> Dict[str, Any]:
    sym = str(symbol or "").strip().upper()

    ctx_payload = {}
    try:
        ctx_payload = load_latest_context_payload(Path(runtime_dir)) or {}
    except Exception:
        ctx_payload = {}

    # best-effort dossier
    dossier = ""
    try:
        dossier = get_dossier_snippet(db_path, sym, max_chars=700) or ""
    except Exception:
        dossier = ""

    # NOTE: We intentionally do NOT pull from selected_opportunities here.
    # That list is an early-stage unfiltered pipeline output that can suggest
    # misleading directions (e.g. LONG when dossier vetoes longs).
    # The /trade LLM gate should derive direction from SSE micro + dossier + market context.

    # Load raw SSE cycle snapshot for true "tape" view.
    cycle = _load_latest_cycle_snapshot(runtime_dir)
    cycle_file = cycle.get("_cycle_file") if isinstance(cycle, dict) else None
    cycle_seq = cycle.get("sequence") if isinstance(cycle, dict) else None
    cycle_ts = cycle.get("timestamp") if isinstance(cycle, dict) else None

    symbol_micro_sse = None
    market_sse = None
    if isinstance(cycle, dict) and isinstance(cycle.get("symbols"), dict):
        blob = cycle["symbols"].get(sym)
        if isinstance(blob, dict):
            symbol_micro_sse = _extract_cycle_symbol_micro(blob)
        market_sse = _extract_cycle_market_snapshot(cycle)

    db = AITraderDB(db_path)
    snap = {}
    try:
        snap = db.get_latest_monitor_snapshot() or {}
    except Exception:
        snap = {}

    # Top open positions (portfolio context)
    open_positions = []
    try:
        with sqlite3.connect(db_path, timeout=10.0) as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                """
                SELECT id, symbol, direction, venue,
                       COALESCE(notional_usd, ABS(size * entry_price)) AS notional
                FROM trades
                WHERE exit_time IS NULL
                ORDER BY notional DESC
                LIMIT 5
                """
            ).fetchall()
        for r in rows or []:
            open_positions.append(
                {
                    "trade_id": int(r[0]),
                    "symbol": str(r[1] or "").upper(),
                    "direction": str(r[2] or "").upper(),
                    "venue": str(r[3] or "").lower(),
                    "notional_usd": float(r[4] or 0.0),
                }
            )
    except Exception:
        open_positions = []

    live_prices = None
    live_equity = None
    try:
        # build_context can be called from within an async runner (e.g. generate_plans.py).
        # If an event loop is already running, execute coroutines in a dedicated thread.
        try:
            asyncio.get_running_loop()
            running = True
        except Exception:
            running = False

        if not running:
            live_prices = asyncio.run(_fetch_live_prices(sym))
            live_equity = asyncio.run(_fetch_live_hl_equity())
        else:
            import threading

            box: Dict[str, Any] = {}

            def _runner() -> None:
                try:
                    box["prices"] = asyncio.run(_fetch_live_prices(sym))
                except Exception:
                    box["prices"] = None
                try:
                    box["equity"] = asyncio.run(_fetch_live_hl_equity())
                except Exception:
                    box["equity"] = None

            t = threading.Thread(target=_runner, daemon=True)
            t.start()
            t.join(timeout=10.0)
            live_prices = box.get("prices")
            live_equity = box.get("equity")
    except Exception:
        live_prices = None
        live_equity = None

    # Sizing hint: compute a risk-based default size.
    try:
        risk_pct = float(os.getenv("EVCLAW_MANUAL_TRADE_RISK_PCT", "1.0") or 1.0)
    except Exception:
        risk_pct = 1.0
    risk_pct = max(0.05, min(2.5, risk_pct))

    default_sl_pct = 1.5 if sym in {"BTC", "ETH"} else 2.0

    venue_eq = 0.0
    venue_eq_source = "monitor_snapshot"
    try:
        venue_eq = float(snap.get("hl_equity") or 0.0)
    except Exception:
        venue_eq = 0.0

    # Override with live API equity if available.
    try:
        if isinstance(live_equity, dict) and float(live_equity.get("equity") or 0.0) > 0:
            venue_eq = float(live_equity.get("equity") or venue_eq)
            venue_eq_source = "hl_api"
    except Exception:
        venue_eq_source = venue_eq_source
    risk_usd = venue_eq * (risk_pct / 100.0) if venue_eq > 0 else 0.0
    recommended_size_usd = (risk_usd / (default_sl_pct / 100.0)) if risk_usd > 0 else None

    return {
        "symbol": sym,
        "generated_at": ctx_payload.get("generated_at"),
        "cycle_seq": ctx_payload.get("sequence") or ctx_payload.get("seq"),
        "live_prices": live_prices,
        "dossier": dossier,
        "monitor_snapshot": {
            "ts_iso": snap.get("ts_iso"),
            "hl_equity": snap.get("hl_equity"),
            "hl_equity_live": (live_equity.get("equity") if isinstance(live_equity, dict) else None),
            "hl_equity_live_ts_iso": (live_equity.get("ts_iso") if isinstance(live_equity, dict) else None),
            "hl_equity_source": venue_eq_source,
            "hip3_equity": snap.get("hip3_equity"),
            "grand_equity": (float(venue_eq or 0) + float(snap.get("hip3_equity") or 0)) if snap else None,
            "hl_net_notional": snap.get("hl_net_notional"),
            "hl_long_notional": snap.get("hl_long_notional"),
            "hl_short_notional": snap.get("hl_short_notional"),
            "open_positions_top": open_positions,
        },
        "sizing": {
            "default_risk_pct": risk_pct,
            "default_sl_pct": float(default_sl_pct),
            "venue_equity_usd": venue_eq if venue_eq > 0 else None,
            "risk_usd": risk_usd if risk_usd > 0 else None,
            "recommended_size_usd": recommended_size_usd,
            "formula": "risk_usd = equity * risk_pct; size_usd = risk_usd / (sl_pct) (sl_pct as decimal)",
        },
        "sse_snapshot": {
            "cycle_file": cycle_file,
            "cycle_seq": cycle_seq,
            "cycle_timestamp": cycle_ts,
            "symbol_micro": symbol_micro_sse,
            "market": market_sse,
        },
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("symbol")
    ap.add_argument("--db", default=DB_PATH_DEFAULT)
    ap.add_argument("--runtime", default=RUNTIME_DIR_DEFAULT)
    ap.add_argument("--ttl-seconds", type=int, default=int(os.getenv("EVCLAW_MANUAL_PLAN_TTL_SECONDS", PLAN_TTL_SECONDS_DEFAULT)))
    args = ap.parse_args()

    plan = allocate_plan_id(args.db, args.symbol, args.ttl_seconds)
    ctx = build_context(args.db, args.runtime, args.symbol)

    out = {
        "plan": {
            **plan,
            "created_at_iso": _iso(plan["created_at"]),
            "expires_at_iso": _iso(plan["expires_at"]),
            "ttl_seconds": int(args.ttl_seconds),
        },
        "context": ctx,
    }
    print(json.dumps(out, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
