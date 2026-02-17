#!/usr/bin/env python3
"""Deterministic position-protection self-heal for EVClaw.

Modes:
- detect: read-only incident detection
- fix: detect + deterministic repair prep + SL/TP re-placement + post-check

This tool is intentionally non-LLM and idempotent-oriented.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import sqlite3
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Sequence, Tuple

from ai_trader_db import AITraderDB

if TYPE_CHECKING:
    from executor import Executor


ROOT_DIR = Path(__file__).resolve().parent
DOTENV_PATH = ROOT_DIR / ".env"
DEFAULT_DB_PATH = ROOT_DIR / "ai_trader.db"
DEFAULT_INFO_URL = "https://node2.evplus.ai/evclaw/info"
DEFAULT_PUBLIC_INFO_URL = "https://api.hyperliquid.xyz/info"
DEFAULT_JSON_OUT = ROOT_DIR / "state" / "self_heal_report.json"

VENUE_PERPS = "hyperliquid"
VENUE_BUILDER = "hyperliquid_wallet"
VENUE_HIP3 = "hip3"

OPENISH = {"open", "resting", "partially_filled", "filledorresting"}

LOG = logging.getLogger("self_heal")


def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        return float(v)
    except Exception:
        return default


def _load_dotenv(path: Path) -> Dict[str, str]:
    data: Dict[str, str] = {}
    if not path.exists():
        return data
    try:
        for line in path.read_text(encoding="utf-8").splitlines():
            s = line.strip()
            if not s or s.startswith("#") or "=" not in s:
                continue
            k, v = s.split("=", 1)
            data[k.strip()] = v.strip().strip('"').strip("'")
    except Exception:
        return data
    return data


def _env(name: str, dotenv: Dict[str, str], default: str = "") -> str:
    v = (os.getenv(name) or "").strip()
    if v:
        return v
    return (dotenv.get(name) or default).strip()


def _inject_env_defaults(dotenv: Dict[str, str], keys: Sequence[str]) -> None:
    for key in keys:
        if (os.getenv(key) or "").strip():
            continue
        val = (dotenv.get(key) or "").strip()
        if val:
            os.environ[key] = val


def _symbol_to_coin(symbol: str) -> str:
    s = str(symbol or "").strip().upper()
    if ":" in s:
        return s.split(":", 1)[1].strip().upper()
    return s


def _normalize_status(raw: Any) -> str:
    s = str(raw or "").strip().lower()
    if s == "cancelled":
        s = "canceled"
    if s in {"open", "resting", "partially_filled", "filledorresting", "filled", "canceled", "rejected"}:
        return s
    return "unknown"


def _derive_default_sltp(entry_price: float, direction: str) -> Tuple[float, float]:
    entry = max(0.0, float(entry_price or 0.0))
    d = str(direction or "").upper()
    if entry <= 0 or d not in {"LONG", "SHORT"}:
        return 0.0, 0.0
    if d == "LONG":
        return entry * 0.985, entry * 1.020
    return entry * 1.015, entry * 0.980


def _append_private_key_if_needed(info_url: str, wallet_key: str) -> str:
    key = (wallet_key or "").strip()
    if not key:
        return info_url
    if "node2.evplus.ai" not in info_url or "/evclaw/info" not in info_url:
        return info_url
    parsed = urllib.parse.urlsplit(info_url)
    params = dict(urllib.parse.parse_qsl(parsed.query, keep_blank_values=True))
    if params.get("key"):
        return info_url
    params["key"] = key
    query = urllib.parse.urlencode(params)
    return urllib.parse.urlunsplit((parsed.scheme, parsed.netloc, parsed.path, query, parsed.fragment))


def _post_info(
    url: str,
    payload: Dict[str, Any],
    timeout: float = 10.0,
    *,
    fallback_url: str = DEFAULT_PUBLIC_INFO_URL,
) -> Any:
    urls: List[str] = [url]
    if fallback_url and fallback_url != url:
        urls.append(fallback_url)
    last_exc: Optional[Exception] = None
    for target in urls:
        try:
            req = urllib.request.Request(
                target,
                data=json.dumps(payload).encode("utf-8"),
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                raw = resp.read().decode("utf-8")
            return json.loads(raw) if raw else {}
        except Exception as exc:
            last_exc = exc
            continue
    if last_exc is not None:
        raise last_exc
    return {}


def _fetch_positions(
    *,
    info_url: str,
    public_info_url: str,
    vault_user: str,
    wallet_user: str,
    builder_dex: str,
    scope: str,
) -> Dict[Tuple[str, str], Dict[str, Any]]:
    out: Dict[Tuple[str, str], Dict[str, Any]] = {}

    def _add_positions(state: Dict[str, Any], venue: str, dex: str = "") -> None:
        for item in state.get("assetPositions", []) if isinstance(state, dict) else []:
            pos = item.get("position", item) if isinstance(item, dict) else {}
            coin_raw = str(pos.get("coin") or "").strip().upper()
            if not coin_raw:
                continue
            size_signed = _safe_float(pos.get("szi", pos.get("size", 0.0)), 0.0)
            if size_signed == 0:
                continue
            venue_resolved = venue
            if ":" in coin_raw:
                venue_resolved = VENUE_BUILDER
            if venue_resolved == VENUE_BUILDER:
                symbol = coin_raw if ":" in coin_raw else f"{dex.upper()}:{coin_raw}"
            else:
                symbol = coin_raw.split(":", 1)[-1] if ":" in coin_raw else coin_raw
            key = (symbol.upper(), venue_resolved)
            out[key] = {
                "symbol": symbol.upper(),
                "coin": _symbol_to_coin(symbol),
                "venue": venue_resolved,
                "direction": "LONG" if size_signed > 0 else "SHORT",
                "size": abs(size_signed),
                "entry_price": _safe_float(pos.get("entryPx", pos.get("entry_price", 0.0)), 0.0),
            }

    if scope in {"all", "perps"} and vault_user:
        st = _post_info(info_url, {"type": "clearinghouseState", "user": vault_user})
        _add_positions(st if isinstance(st, dict) else {}, VENUE_PERPS)

    if scope in {"all", "builder"} and wallet_user:
        st = _post_info(
            public_info_url,
            {"type": "clearinghouseState", "user": wallet_user, "dex": builder_dex},
        )
        _add_positions(st if isinstance(st, dict) else {}, VENUE_BUILDER, dex=builder_dex)

    return out


def _fetch_open_orders(
    *,
    info_url: str,
    public_info_url: str,
    vault_user: str,
    wallet_user: str,
    builder_dex: str,
    scope: str,
) -> Dict[Tuple[str, str], List[Dict[str, Any]]]:
    out: Dict[Tuple[str, str], List[Dict[str, Any]]] = {}

    def _add_orders(rows: List[Dict[str, Any]], venue: str) -> None:
        for o in rows:
            if not isinstance(o, dict):
                continue
            coin_raw = str(o.get("coin") or "").strip().upper()
            coin = _symbol_to_coin(coin_raw)
            if not coin:
                continue
            venue_resolved = VENUE_BUILDER if ":" in coin_raw else venue
            key = (coin, venue_resolved)
            out.setdefault(key, []).append(o)

    if scope in {"all", "perps"} and vault_user:
        rows = _post_info(info_url, {"type": "openOrders", "user": vault_user})
        _add_orders(rows if isinstance(rows, list) else [], VENUE_PERPS)

    if scope in {"all", "builder"} and wallet_user:
        rows = _post_info(
            public_info_url,
            {"type": "frontendOpenOrders", "user": wallet_user, "dex": builder_dex},
        )
        _add_orders(rows if isinstance(rows, list) else [], VENUE_BUILDER)

    return out


def _load_open_trades(db_path: Path, scope: str) -> List[Dict[str, Any]]:
    conn = sqlite3.connect(str(db_path), timeout=30.0)
    conn.row_factory = sqlite3.Row
    try:
        where_extra = ""
        if scope == "perps":
            where_extra = " AND venue='hyperliquid' AND symbol NOT LIKE '%:%'"
        elif scope == "builder":
            where_extra = " AND (venue IN ('hyperliquid_wallet','hip3') OR symbol LIKE '%:%')"
        rows = conn.execute(
            f"""
            SELECT id, symbol, venue, direction, size, entry_price, sl_price, tp_price, sl_order_id, tp_order_id, state
            FROM trades
            WHERE exit_time IS NULL
              AND venue IN ('hyperliquid','hyperliquid_wallet','hip3')
              {where_extra}
            ORDER BY entry_time DESC
            """
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def _order_status(
    *,
    public_info_url: str,
    user: str,
    oid: str,
    cache: Dict[Tuple[str, str], Dict[str, Any]],
) -> Dict[str, Any]:
    k = (str(user).lower(), str(oid))
    if k in cache:
        return cache[k]
    out = {
        "status": "unknown",
        "orig_size": 0.0,
        "open_size": 0.0,
        "filled_size": 0.0,
    }
    try:
        payload = {"type": "orderStatus", "user": user, "oid": int(oid)}
        data = _post_info(public_info_url, payload)
        wrap = (data or {}).get("order", {}) if isinstance(data, dict) else {}
        order = wrap.get("order", {}) if isinstance(wrap, dict) else {}
        st = _normalize_status(wrap.get("status"))
        orig = _safe_float(order.get("origSz", order.get("sz", 0.0)), 0.0)
        open_sz = _safe_float(order.get("sz", 0.0), 0.0)
        filled = max(0.0, orig - open_sz)
        out = {"status": st, "orig_size": orig, "open_size": open_sz, "filled_size": filled}
    except Exception:
        pass
    cache[k] = out
    return out


def _evaluate_trade_protection(
    *,
    trade: Dict[str, Any],
    live_pos: Dict[str, Any],
    orders_by_key: Dict[Tuple[str, str], List[Dict[str, Any]]],
    public_info_url: str,
    user_by_venue: Dict[str, str],
    status_cache: Dict[Tuple[str, str], Dict[str, Any]],
) -> Dict[str, Any]:
    venue = str(trade["venue"])
    symbol = str(trade["symbol"]).upper()
    coin = _symbol_to_coin(symbol)
    expected_size = _safe_float(live_pos.get("size"), 0.0)

    orders = orders_by_key.get((coin, venue), [])
    if not orders and venue == VENUE_HIP3:
        orders = orders_by_key.get((coin, VENUE_BUILDER), [])
    covered_size = 0.0
    for o in orders:
        if bool(o.get("reduceOnly")):
            covered_size = max(covered_size, _safe_float(o.get("sz"), 0.0))
    gap_size = max(0.0, expected_size - covered_size)

    sl_oid = str(trade.get("sl_order_id") or "").strip()
    tp_oid = str(trade.get("tp_order_id") or "").strip()
    user = user_by_venue.get(venue, "")
    sl_status = "missing_oid"
    tp_status = "missing_oid"
    if sl_oid and user:
        sl_status = _order_status(
            public_info_url=public_info_url,
            user=user,
            oid=sl_oid,
            cache=status_cache,
        )["status"]
    if tp_oid and user:
        tp_status = _order_status(
            public_info_url=public_info_url,
            user=user,
            oid=tp_oid,
            cache=status_cache,
        )["status"]

    stale_statuses = {"canceled", "rejected", "filled"}
    stale_oid = (
        (bool(sl_oid) and sl_status in stale_statuses)
        or (bool(tp_oid) and tp_status in stale_statuses)
    )
    partial_coverage = gap_size > 1e-6
    base_unprotected = "unprotected_builder" if ":" in symbol else "unprotected_perps"
    incident_type = ""
    if stale_oid:
        incident_type = "stale_oid"
    elif partial_coverage:
        incident_type = "partial_coverage"
    elif not ((sl_status in OPENISH) and (tp_status in OPENISH)):
        incident_type = base_unprotected
    protected = incident_type == ""

    return {
        "protected": protected,
        "incident_type": incident_type,
        "symbol": symbol,
        "coin": coin,
        "venue": venue,
        "trade_id": int(trade["id"]),
        "direction": str(live_pos.get("direction") or trade.get("direction") or ""),
        "expected_size": expected_size,
        "covered_size": covered_size,
        "gap_size": gap_size,
        "sl_order_id": sl_oid or None,
        "tp_order_id": tp_oid or None,
        "sl_status": sl_status,
        "tp_status": tp_status,
        "entry_price": _safe_float(live_pos.get("entry_price"), _safe_float(trade.get("entry_price"), 0.0)),
        "sl_price": _safe_float(trade.get("sl_price"), 0.0),
        "tp_price": _safe_float(trade.get("tp_price"), 0.0),
    }


def _detect_incidents(
    *,
    db_trades: List[Dict[str, Any]],
    live_positions: Dict[Tuple[str, str], Dict[str, Any]],
    orders_by_key: Dict[Tuple[str, str], List[Dict[str, Any]]],
    public_info_url: str,
    user_by_venue: Dict[str, str],
) -> List[Dict[str, Any]]:
    incidents: List[Dict[str, Any]] = []
    status_cache: Dict[Tuple[str, str], Dict[str, Any]] = {}

    db_by_key: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for t in db_trades:
        symbol = str(t["symbol"]).upper()
        venue = str(t["venue"])
        if ":" in symbol and venue == VENUE_HIP3:
            venue = VENUE_BUILDER
        key = (symbol, venue)
        norm = dict(t)
        norm["venue"] = venue
        # Use newest row per key (input already sorted DESC)
        db_by_key.setdefault(key, norm)

    for key, lp in live_positions.items():
        if key not in db_by_key:
            incidents.append(
                {
                    "type": "missing_in_db",
                    "symbol": key[0],
                    "venue": key[1],
                    "trade_id": None,
                    "direction": lp["direction"],
                    "expected_size": _safe_float(lp["size"]),
                    "covered_size": 0.0,
                    "gap_size": _safe_float(lp["size"]),
                    "entry_price": _safe_float(lp.get("entry_price"), 0.0),
                    "sl_price": 0.0,
                    "tp_price": 0.0,
                    "sl_order_id": None,
                    "tp_order_id": None,
                    "sl_status": "missing_trade",
                    "tp_status": "missing_trade",
                }
            )

    for key, trade in db_by_key.items():
        lp = live_positions.get(key)
        if not lp:
            continue
        ev = _evaluate_trade_protection(
            trade=trade,
            live_pos=lp,
            orders_by_key=orders_by_key,
            public_info_url=public_info_url,
            user_by_venue=user_by_venue,
            status_cache=status_cache,
        )
        if not ev["protected"]:
            ev["type"] = ev["incident_type"]
            incidents.append(ev)

    # deterministic order
    incidents.sort(key=lambda x: (str(x.get("symbol")), str(x.get("venue")), int(x.get("trade_id") or 0)))
    return incidents


async def _best_effort_close(
    *,
    executor: "Executor",
    symbol: str,
    venue: str,
    direction: str,
    size: float,
) -> bool:
    _ = venue
    adapter = executor.hyperliquid
    side = "sell" if str(direction).upper() == "LONG" else "buy"
    try:
        bid, ask = await adapter.get_best_bid_ask(symbol)
        px = _safe_float(ask if side == "buy" else bid, 0.0)
        if px <= 0:
            return False
        ok, _oid = await adapter.place_limit_order(
            symbol=symbol,
            side=side,
            size=float(size),
            price=float(px),
            reduce_only=True,
            tif="Ioc",
        )
        return bool(ok)
    except Exception:
        return False


async def _apply_fixes(
    *,
    db_path: Path,
    incidents: List[Dict[str, Any]],
    close_on_fail: bool,
    actions: List[str],
) -> Dict[str, Any]:
    out = {"prepared": 0, "reconciled_missing": 0, "cancelled_stale": 0}
    if not incidents:
        return out

    db = AITraderDB(str(db_path))

    # reuse existing trading-protection logic via executor
    from executor import ExecutionConfig, Executor

    cfg = ExecutionConfig(
        dry_run=False,
        lighter_enabled=False,
        hl_enabled=True,
        hl_wallet_enabled=True,
        db_path=str(db_path),
        write_positions_yaml=False,
        enable_trade_journal=False,
        enable_trade_tracker=False,
        use_fill_reconciler=False,
    )
    executor = Executor(config=cfg)
    ok = await executor.initialize()
    if not ok:
        raise RuntimeError("executor_initialize_failed")

    try:
        for inc in incidents:
            symbol = str(inc["symbol"]).upper()
            venue = str(inc["venue"])
            direction = str(inc.get("direction") or "").upper()
            expected_size = _safe_float(inc.get("expected_size"), 0.0)
            entry_price = _safe_float(inc.get("entry_price"), 0.0)
            sl_price = _safe_float(inc.get("sl_price"), 0.0)
            tp_price = _safe_float(inc.get("tp_price"), 0.0)
            trade_id = inc.get("trade_id")

            if trade_id is None:
                if entry_price <= 0 or expected_size <= 0 or direction not in {"LONG", "SHORT"}:
                    actions.append(f"skip_missing_in_db_invalid_live:{symbol}:{venue}")
                    continue
                dsl, dtp = _derive_default_sltp(entry_price, direction)
                if sl_price <= 0:
                    sl_price = dsl
                if tp_price <= 0:
                    tp_price = dtp
                trade_id = db.log_trade_entry(
                    symbol=symbol,
                    direction=direction,
                    entry_price=entry_price,
                    size=expected_size,
                    venue=venue,
                    state="NEEDS_PROTECTION",
                    sl_price=sl_price if sl_price > 0 else None,
                    tp_price=tp_price if tp_price > 0 else None,
                    signals_snapshot={"self_heal": True, "source": "missing_in_db"},
                    signals_agreed=[],
                    ai_reasoning="SELF_HEAL_RECONCILE_UNTRACKED",
                    strategy="self_heal",
                )
                out["reconciled_missing"] += 1
                actions.append(f"reconcile_missing_in_db:{symbol}:{venue}:trade_id={trade_id}")

            try:
                tid = int(trade_id)
            except Exception:
                actions.append(f"skip_invalid_trade_id:{symbol}:{venue}:{trade_id}")
                continue

            # sync trade position to live truth
            if expected_size > 0:
                db.update_trade_position(tid, size=expected_size, entry_price=entry_price if entry_price > 0 else None)
            if direction in {"LONG", "SHORT"}:
                db.update_trade_direction(tid, direction)

            # ensure SL/TP prices exist
            if sl_price <= 0 or tp_price <= 0:
                base_entry = entry_price if entry_price > 0 else _safe_float(inc.get("entry_price"), 0.0)
                dsl, dtp = _derive_default_sltp(base_entry, direction)
                if sl_price <= 0:
                    sl_price = dsl
                if tp_price <= 0:
                    tp_price = dtp

            # cancel known stale legs if currently open-ish
            adapter = executor.hyperliquid
            for leg_key in ("sl_order_id", "tp_order_id"):
                oid = str(inc.get(leg_key) or "").strip()
                st = _normalize_status(inc.get("sl_status") if leg_key == "sl_order_id" else inc.get("tp_status"))
                if oid and st in OPENISH:
                    try:
                        if await adapter.cancel_order(symbol, oid):
                            out["cancelled_stale"] += 1
                            actions.append(f"cancel_stale_leg:{symbol}:{venue}:{leg_key}:{oid}")
                    except Exception:
                        actions.append(f"cancel_stale_leg_failed:{symbol}:{venue}:{leg_key}:{oid}")

            db.set_trade_sltp(
                tid,
                sl_price=sl_price if sl_price > 0 else None,
                tp_price=tp_price if tp_price > 0 else None,
                sl_order_id=None,
                tp_order_id=None,
                protection_snapshot={"source": "self_heal_reset"},
            )
            db.update_trade_state(tid, "NEEDS_PROTECTION")
            out["prepared"] += 1
            actions.append(f"prepared_for_reprotect:{symbol}:{venue}:trade_id={tid}")

        # Use canonical executor path for placing fresh protection.
        await executor._place_protection_for_unprotected()
        actions.append("executor_reprotect_completed")

        if close_on_fail:
            # best-effort de-risk only for residual unresolved incidents
            # (post-check decides which remain; close attempts are logged there)
            actions.append("close_on_fail_enabled")
    finally:
        try:
            await executor.close()
        except Exception:
            pass

    return out


def _summarize(incidents: List[Dict[str, Any]]) -> Dict[str, int]:
    out = {
        "missing_in_db": 0,
        "unprotected_perps": 0,
        "unprotected_builder": 0,
        "partial_coverage": 0,
        "stale_oid": 0,
        "total": len(incidents),
    }
    for inc in incidents:
        t = str(inc.get("type") or "")
        if t in out:
            out[t] += 1
    return out


def _parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="EVClaw deterministic self-heal")
    ap.add_argument("--mode", choices=["detect", "fix"], default="detect")
    ap.add_argument("--scope", choices=["perps", "builder", "all"], default="all")
    ap.add_argument("--db", default=str(DEFAULT_DB_PATH))
    ap.add_argument("--info-url", default="")
    ap.add_argument("--public-info-url", default=DEFAULT_PUBLIC_INFO_URL)
    ap.add_argument("--wallet", default="")
    ap.add_argument("--close-on-fail", type=int, default=0)
    ap.add_argument("--json-out", default=str(DEFAULT_JSON_OUT))
    ap.add_argument("--verbose", action="store_true")
    return ap.parse_args()


async def _run(args: argparse.Namespace) -> int:
    logging.basicConfig(
        level=(logging.DEBUG if args.verbose else logging.INFO),
        format="%(asctime)s %(levelname)s %(message)s",
    )

    dotenv = _load_dotenv(DOTENV_PATH)
    _inject_env_defaults(
        dotenv,
        [
            "HYPERLIQUID_ADDRESS",
            "HYPERLIQUID_API",
            "HYPERLIQUID_PRIVATE_NODE",
        ],
    )

    info_url = (args.info_url or _env("HYPERLIQUID_PRIVATE_NODE", dotenv, DEFAULT_INFO_URL)).strip()
    if not info_url.endswith("/info"):
        info_url = info_url.rstrip("/") + "/info"
    info_url = _append_private_key_if_needed(info_url, _env("HYPERLIQUID_ADDRESS", dotenv))
    public_info_url = (args.public_info_url or DEFAULT_PUBLIC_INFO_URL).strip()
    if not public_info_url.endswith("/info"):
        public_info_url = public_info_url.rstrip("/") + "/info"

    wallet = (args.wallet or _env("HYPERLIQUID_ADDRESS", dotenv)).strip()
    vault = wallet
    builder_dex = "xyz"

    report: Dict[str, Any] = {
        "tool": "evclaw-self-heal",
        "started_at": _now_utc_iso(),
        "settings": {
            "mode": args.mode,
            "scope": args.scope,
            "db_path": str(Path(args.db).expanduser()),
            "info_url": info_url,
            "public_info_url": public_info_url,
            "wallet": wallet,
            "builder_dex": builder_dex,
            "close_on_fail": bool(args.close_on_fail),
        },
        "actions": [],
        "warnings": [],
        "errors": [],
    }

    db_path = Path(args.db).expanduser()
    if not db_path.exists():
        report["errors"].append(f"db_not_found:{db_path}")
        report["finished_at"] = _now_utc_iso()
        Path(args.json_out).write_text(json.dumps(report, indent=2, sort_keys=True), encoding="utf-8")
        print(json.dumps(report, indent=2, sort_keys=True))
        return 2

    if args.scope in {"all", "builder"} and not wallet:
        report["errors"].append("missing_wallet_for_builder_scope")
    if args.scope in {"all", "perps"} and not vault:
        report["errors"].append("missing_vault_for_perps_scope")
    if report["errors"]:
        report["finished_at"] = _now_utc_iso()
        Path(args.json_out).write_text(json.dumps(report, indent=2, sort_keys=True), encoding="utf-8")
        print(json.dumps(report, indent=2, sort_keys=True))
        return 2

    # Phase 0 preflight: always detect from fresh live truth.
    try:
        live_positions = _fetch_positions(
            info_url=info_url,
            public_info_url=public_info_url,
            vault_user=vault,
            wallet_user=wallet,
            builder_dex=builder_dex,
            scope=args.scope,
        )
        orders_by_key = _fetch_open_orders(
            info_url=info_url,
            public_info_url=public_info_url,
            vault_user=vault,
            wallet_user=wallet,
            builder_dex=builder_dex,
            scope=args.scope,
        )
    except Exception as exc:
        report["errors"].append(f"live_fetch_failed:{exc}")
        report["finished_at"] = _now_utc_iso()
        Path(args.json_out).write_text(json.dumps(report, indent=2, sort_keys=True), encoding="utf-8")
        print(json.dumps(report, indent=2, sort_keys=True))
        return 1

    db_trades = _load_open_trades(db_path, args.scope)
    user_by_venue = {
        VENUE_PERPS: wallet,
        VENUE_HIP3: wallet,
        VENUE_BUILDER: wallet,
    }
    incidents_before = _detect_incidents(
        db_trades=db_trades,
        live_positions=live_positions,
        orders_by_key=orders_by_key,
        public_info_url=public_info_url,
        user_by_venue=user_by_venue,
    )
    report["incidents_before"] = incidents_before
    report["summary_before"] = _summarize(incidents_before)

    if args.mode == "fix" and incidents_before:
        try:
            fix_stats = await _apply_fixes(
                db_path=db_path,
                incidents=incidents_before,
                close_on_fail=bool(args.close_on_fail),
                actions=report["actions"],
            )
            report["fix_stats"] = fix_stats
        except Exception as exc:
            report["errors"].append(f"apply_fixes_failed:{exc}")

    # Post-check from fresh live truth
    try:
        live_positions_after = _fetch_positions(
            info_url=info_url,
            public_info_url=public_info_url,
            vault_user=vault,
            wallet_user=wallet,
            builder_dex=builder_dex,
            scope=args.scope,
        )
        orders_by_key_after = _fetch_open_orders(
            info_url=info_url,
            public_info_url=public_info_url,
            vault_user=vault,
            wallet_user=wallet,
            builder_dex=builder_dex,
            scope=args.scope,
        )
        db_trades_after = _load_open_trades(db_path, args.scope)
        incidents_after = _detect_incidents(
            db_trades=db_trades_after,
            live_positions=live_positions_after,
            orders_by_key=orders_by_key_after,
            public_info_url=public_info_url,
            user_by_venue=user_by_venue,
        )
    except Exception as exc:
        report["errors"].append(f"post_check_failed:{exc}")
        incidents_after = []

    report["incidents_after"] = incidents_after
    report["summary_after"] = _summarize(incidents_after)

    # Optional de-risk close after post-check.
    if args.mode == "fix" and bool(args.close_on_fail) and incidents_after:
        from executor import ExecutionConfig, Executor

        cfg = ExecutionConfig(
            dry_run=False,
            lighter_enabled=False,
            hl_enabled=True,
            hl_wallet_enabled=True,
            db_path=str(db_path),
            write_positions_yaml=False,
            enable_trade_journal=False,
            enable_trade_tracker=False,
            use_fill_reconciler=False,
        )
        ex = Executor(config=cfg)
        if await ex.initialize():
            try:
                for inc in incidents_after:
                    if str(inc.get("type")) not in {
                        "unprotected_perps",
                        "unprotected_builder",
                        "partial_coverage",
                        "stale_oid",
                    }:
                        continue
                    ok = await _best_effort_close(
                        executor=ex,
                        symbol=str(inc.get("symbol") or ""),
                        venue=str(inc.get("venue") or ""),
                        direction=str(inc.get("direction") or ""),
                        size=_safe_float(inc.get("expected_size"), 0.0),
                    )
                    report["actions"].append(
                        f"close_on_fail_attempt:{inc.get('symbol')}:{inc.get('venue')}:{'ok' if ok else 'failed'}"
                    )
            finally:
                try:
                    await ex.close()
                except Exception:
                    pass

    report["ok"] = (len(report["errors"]) == 0) and (report["summary_after"]["total"] == 0)
    report["finished_at"] = _now_utc_iso()
    Path(args.json_out).write_text(json.dumps(report, indent=2, sort_keys=True), encoding="utf-8")
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if report["ok"] else 1


def main() -> int:
    args = _parse_args()
    try:
        return int(asyncio.run(_run(args)))
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
