#!/usr/bin/env python3
"""Runtime/dependency helpers extracted from live_agent."""

from __future__ import annotations

import asyncio
import json
import os
import socket
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

import requests

from ai_trader_db import AITraderDB
from adaptive_sltp import AdaptiveSLTPConfig, AdaptiveSLTPManager
from exchanges import VENUE_HYPERLIQUID, VENUE_LIGHTER
from executor import Executor
from live_agent_deps import LiveAgentDeps
from symbol_rr_learning import HybridSLTPAdapter
from trade_tracker import TradeTracker
from env_utils import env_str
from venues import normalize_venue

HEARTBEAT_FILE = Path("/tmp/evclaw_fill_reconciler_heartbeat.json")


def normalize_hl_base_url(raw: str) -> str:
    base = (raw or "").strip()
    if not base:
        return "https://node2.evplus.ai/evclaw"
    if base.endswith("/info"):
        base = base[:-5]
    return base.rstrip("/")


def normalize_hl_public_base_url(raw: str) -> str:
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
    public_base = normalize_hl_public_base_url(os.getenv("HYPERLIQUID_PUBLIC_URL", ""))
    if public_base and public_base != base_url:
        targets.append((f"{public_base}/info", None))
    return targets


def resolve_hl_equity_address() -> Optional[str]:
    address = os.getenv("HYPERLIQUID_ADDRESS", "").strip()
    return address or None


def resolve_sqlite_db_path(db: Any, *, default_db: str) -> str:
    path = str(getattr(db, "db_path", "") or "").strip()
    return path or str(default_db)


def extract_lighter_equity(account_response: Any) -> Optional[float]:
    accounts = getattr(account_response, "accounts", None)
    if not accounts:
        return None
    preferred_fields = (
        "total_asset_value",
        "equity",
        "account_value",
        "wallet_balance",
        "margin_balance",
    )
    for acct in accounts:
        for field in preferred_fields:
            try:
                if isinstance(acct, dict):
                    raw = acct.get(field)
                else:
                    if not hasattr(acct, field):
                        continue
                    raw = getattr(acct, field)
                value = float(raw)
            except Exception:
                continue
            if value > 0:
                return value
    return None


async def fetch_hl_state_http(
    address: str,
    base_url: str,
    *,
    request_post: Callable[..., Any] = requests.post,
) -> Optional[Dict[str, Any]]:
    payload = {"type": "clearinghouseState", "user": address}

    def _do_request() -> Optional[Dict[str, Any]]:
        errors: List[str] = []
        for info_url, params in _hl_info_targets(base_url):
            try:
                kwargs: Dict[str, Any] = {"json": payload, "timeout": 5}
                if params:
                    kwargs["params"] = params
                resp = request_post(info_url, **kwargs)
                resp.raise_for_status()
                state = resp.json()
                return state if isinstance(state, dict) else None
            except Exception as exc:
                errors.append(f"{info_url}: {exc}")
                continue
        if errors:
            print(f"HL state fetch failed for {address}: {' | '.join(errors)}")
        return None

    try:
        return await asyncio.to_thread(_do_request)
    except Exception as exc:
        print(f"HL state fetch failed for {address}: {exc}")
        return None


async def fetch_hl_equity_http(
    address: str,
    base_url: str,
    *,
    fetch_hl_state_http_fn: Callable[[str, str], Any] = fetch_hl_state_http,
) -> float:
    state = await fetch_hl_state_http_fn(address, base_url)
    margin = state.get("marginSummary", {}) if isinstance(state, dict) else {}
    return float(margin.get("accountValue", 0.0) or 0.0)


async def fetch_hl_wallet_unified_equity(adapter: Any, address: str) -> float:
    if not address:
        return 0.0

    total = 0.0
    dexes: List[str] = []
    try:
        if hasattr(adapter, "_builder_dexes"):
            dexes = [str(d).lower() for d in adapter._builder_dexes() if str(d).strip()]
    except Exception as exc:
        print(f"Warning: failed to load builder dexes for wallet equity fetch: {exc}")
        dexes = []
    if not dexes:
        raw = getattr(adapter, "BUILDER_DEXES", [])
        dexes = [str(d).lower() for d in raw if str(d).strip()]

    reqs: List[Tuple[str, Dict[str, Any], Optional[str]]] = [
        ("main", {"type": "clearinghouseState", "user": address}, None),
        ("spot", {"type": "spotClearinghouseState", "user": address}, None),
    ]
    reqs.extend(
        ("dex", {"type": "clearinghouseState", "user": address, "dex": dex}, dex)
        for dex in dexes
    )

    async def _fetch(
        kind: str, payload: Dict[str, Any], dex: Optional[str]
    ) -> Tuple[str, Optional[str], Any, Optional[Exception]]:
        try:
            state = await adapter._post_public(payload)
            return kind, dex, state, None
        except Exception as exc:
            return kind, dex, None, exc

    responses = await asyncio.gather(*(_fetch(kind, payload, dex) for kind, payload, dex in reqs))
    stable_coins = {"USDC", "USDT0", "USDT", "USDE", "USDH", "USDHL"}

    for kind, _dex, state, err in responses:
        if err is not None:
            if kind == "main":
                print(f"Warning: wallet unified equity main clearinghouseState failed: {err}")
            elif kind == "spot":
                print(f"Warning: wallet unified equity spotClearinghouseState failed: {err}")
            continue
        if not isinstance(state, dict):
            continue
        if kind in {"main", "dex"}:
            total += float(state.get("marginSummary", {}).get("accountValue", 0.0) or 0.0)
            continue
        for bal in state.get("balances", []):
            coin = str((bal or {}).get("coin", "")).strip().upper()
            if coin in stable_coins:
                bal_total = float((bal or {}).get("total", 0.0) or 0.0)
                bal_hold = float((bal or {}).get("hold", 0.0) or 0.0)
                total += max(0.0, bal_total - bal_hold)

    return max(0.0, float(total))


def build_processor_id(explicit: Optional[str] = None) -> str:
    if explicit:
        return explicit
    return (
        env_str("EVCLAW_PROCESSOR_ID", "")
        or f"{socket.gethostname()}:{os.getpid()}"
    )


def build_deps(
    *,
    dry_run: bool,
    db_path_override: Optional[str],
    load_config_fn: Callable[[], Dict[str, Any]],
    build_execution_config_fn: Callable[..., Any],
    default_db: str,
    skill_dir: Path,
) -> LiveAgentDeps:
    config = load_config_fn()
    exec_config = build_execution_config_fn(config, dry_run=dry_run)
    db_path = str(db_path_override or exec_config.db_path or default_db)
    try:
        exec_config.db_path = db_path
    except Exception as exc:
        print(f"Warning: failed to set execution config db_path={db_path}: {exc}")
    db = AITraderDB(db_path)
    tracker = TradeTracker(db_path=db_path, memory_dir=skill_dir / "memory")
    return LiveAgentDeps(
        config=config,
        exec_config=exec_config,
        db=db,
        db_path=db_path,
        tracker=tracker,
    )


def build_hybrid_sltp(
    exec_config: Any,
    tracker: TradeTracker,
    *,
    db_path: Optional[str],
    skill_dir: Path,
) -> HybridSLTPAdapter:
    adaptive_cfg = AdaptiveSLTPConfig(
        default_sl_mult=float(exec_config.sl_atr_multiplier),
        default_tp_mult=float(exec_config.tp_atr_multiplier),
    )
    adaptive_sltp = AdaptiveSLTPManager(
        config=adaptive_cfg,
        trade_tracker=tracker,
        memory_dir=skill_dir / "memory" / "adaptive",
    )
    resolved_db = str(db_path or getattr(exec_config, "db_path", "") or (skill_dir / "ai_trader.db"))
    return HybridSLTPAdapter(db_path=resolved_db, fallback_manager=adaptive_sltp)


async def build_executor_with_learning(
    exec_config: Any,
    tracker: TradeTracker,
    *,
    db_path: Optional[str],
    build_hybrid_sltp_fn: Callable[..., HybridSLTPAdapter],
    executor_cls: Any,
) -> Executor:
    hybrid_sltp = build_hybrid_sltp_fn(exec_config, tracker, db_path=db_path)
    factory = getattr(executor_cls, "create", None)
    if callable(factory):
        return await factory(
            config=exec_config,
            adaptive_sltp=hybrid_sltp,
            trade_tracker=tracker,
        )

    executor = executor_cls(
        config=exec_config,
        adaptive_sltp=hybrid_sltp,
        trade_tracker=tracker,
    )
    ok = await executor.initialize()
    if not ok:
        raise RuntimeError("Executor initialization failed")
    return executor


def check_fill_reconciler(
    required_venues: List[str],
    *,
    max_age: int = 90,
    heartbeat_file: Path = HEARTBEAT_FILE,
) -> Tuple[bool, str]:
    if not heartbeat_file.exists():
        return False, "fill_reconciler heartbeat missing"
    try:
        data = json.loads(heartbeat_file.read_text())
        ts = float(data.get("ts") or 0.0)
        venues = data.get("venues") or []
        if time.time() - ts > max_age:
            return False, "fill_reconciler heartbeat stale"
        missing = [v for v in required_venues if v not in venues]
        if missing:
            return False, f"fill_reconciler missing venues: {','.join(missing)}"
        return True, ""
    except Exception as exc:
        return False, f"fill_reconciler heartbeat error: {exc}"


async def ensure_fill_reconciler(
    required_venues: List[str],
    *,
    max_age: int = 90,
    skill_dir: Path,
    check_fill_reconciler_fn: Callable[..., Tuple[bool, str]] = check_fill_reconciler,
) -> Tuple[bool, str]:
    ok, reason = check_fill_reconciler_fn(required_venues, max_age=max_age)
    if ok:
        return True, ""

    script = skill_dir / "run_fill_reconciler.py"
    req = [v for v in (required_venues or []) if v]
    venue_arg = "all" if len(set(req)) > 1 else (req[0] if req else "all")
    try:
        print(f"Fill reconciler unhealthy ({reason}); running one-shot reconcile for venue={venue_arg}")
        result = await asyncio.to_thread(
            subprocess.run,
            [sys.executable, str(script), "--venue", venue_arg, "--once"],
            capture_output=True,
            text=True,
            timeout=180,
        )
        if result.returncode != 0:
            return (
                False,
                f"{reason}; one-shot reconcile failed rc={result.returncode}: "
                f"{result.stderr.strip() or result.stdout.strip()}",
            )
    except Exception as exc:
        return False, f"{reason}; one-shot reconcile error: {exc}"

    ok2, reason2 = check_fill_reconciler_fn(required_venues, max_age=max_age)
    return (ok2, reason2 if not ok2 else "")


async def ensure_positions_reconciled(
    executor: Executor,
    db: AITraderDB,
    *,
    allowed_venues: Optional[List[str]] = None,
    skill_dir: Path,
    check_positions_reconciled_fn: Callable[..., Any],
) -> Tuple[bool, str]:
    ok, detail = await check_positions_reconciled_fn(
        executor, db, allowed_venues=allowed_venues
    )
    if ok:
        return True, ""

    req = [v.lower() for v in (allowed_venues or []) if v]
    venue_arg = "all" if len(set(req)) > 1 else (req[0] if req else "all")
    script = skill_dir / "run_fill_reconciler.py"
    try:
        print(f"Positions not reconciled ({detail}); running one-shot fill reconcile for venue={venue_arg}")
        result = await asyncio.to_thread(
            subprocess.run,
            [sys.executable, str(script), "--venue", venue_arg, "--once"],
            capture_output=True,
            text=True,
            timeout=180,
        )
        if result.returncode != 0:
            return (
                False,
                f"{detail}; one-shot fill reconcile failed rc={result.returncode}: "
                f"{result.stderr.strip() or result.stdout.strip()}",
            )
    except Exception as exc:
        return False, f"{detail}; one-shot fill reconcile error: {exc}"

    ok2, detail2 = await check_positions_reconciled_fn(
        executor, db, allowed_venues=allowed_venues
    )
    return (ok2, detail2 if not ok2 else "")


async def check_symbol_on_venues(symbol: str, executor: Executor, venues: List[str]) -> Dict[str, Any]:
    hl_mid = 0.0
    lt_mid = 0.0
    ok = True

    if VENUE_HYPERLIQUID in venues:
        try:
            hl_mid = float(await executor.hyperliquid.get_mid_price(symbol) or 0.0)
        except Exception:
            hl_mid = 0.0
        ok = ok and hl_mid > 0

    if VENUE_LIGHTER in venues:
        try:
            lt_mid = float(await executor.lighter.get_mid_price(symbol) or 0.0)
        except Exception:
            lt_mid = 0.0
        ok = ok and lt_mid > 0

    return {
        "symbol": symbol,
        "hl_mid": hl_mid,
        "lighter_mid": lt_mid,
        "ok": ok,
    }


async def check_positions_reconciled(
    executor: Executor,
    db: AITraderDB,
    *,
    allowed_venues: Optional[List[str]] = None,
    db_call_fn: Optional[Callable[..., Any]] = None,
) -> Tuple[bool, str]:
    try:
        if db_call_fn is not None:
            db_trades = await db_call_fn(db.get_open_trades)
        else:
            db_trades = await asyncio.to_thread(db.get_open_trades)
    except Exception as exc:
        return False, f"failed to load open trades: {exc}"

    exchange_positions: Dict[Tuple[str, str], str] = {}
    allowed_set = {normalize_venue(v) for v in allowed_venues if normalize_venue(v)} if allowed_venues else None
    venues: List[Tuple[str, Any]] = []
    if executor.config.lighter_enabled and (allowed_set is None or VENUE_LIGHTER in allowed_set):
        venues.append((VENUE_LIGHTER, executor.lighter))
    if executor.config.hl_enabled and (allowed_set is None or VENUE_HYPERLIQUID in allowed_set):
        venues.append((VENUE_HYPERLIQUID, executor.hyperliquid))

    for venue, adapter in venues:
        try:
            positions = await adapter.get_all_positions()
        except Exception as exc:
            return False, f"failed to fetch positions from {venue}: {exc}"
        for pos in positions.values():
            if pos.size <= 0 or pos.direction == "FLAT":
                continue
            exchange_positions[(venue, pos.symbol.upper())] = pos.direction.upper()

    missing: List[str] = []
    direction_mismatch: List[str] = []
    for trade in db_trades:
        venue = normalize_venue(trade.venue or "")
        if allowed_set is not None and venue not in allowed_set:
            continue
        key = (venue, trade.symbol.upper())
        exchange_dir = exchange_positions.get(key)
        if exchange_dir is None:
            missing.append(f"{trade.venue}:{trade.symbol}")
            continue
        trade_dir = str(getattr(trade, "direction", "") or "").upper()
        if trade_dir in {"LONG", "SHORT"} and exchange_dir in {"LONG", "SHORT"} and trade_dir != exchange_dir:
            direction_mismatch.append(
                f"{trade.venue}:{trade.symbol} db={trade_dir} exchange={exchange_dir}"
            )

    extras: List[str] = []
    db_keys = {
        (normalize_venue(t.venue or ""), t.symbol.upper())
        for t in db_trades
        if allowed_set is None or normalize_venue(t.venue or "") in allowed_set
    }
    db_symbols_any_venue = {t.symbol.upper() for t in db_trades if t.symbol and ":" in t.symbol}
    for key in exchange_positions.keys():
        if allowed_set is not None and key[0] not in allowed_set:
            continue
        if key not in db_keys:
            sym = key[1]
            if ":" in sym and sym in db_symbols_any_venue:
                continue
            extras.append(f"{key[0]}:{key[1]}")

    if missing or extras or direction_mismatch:
        details: List[str] = []
        if missing:
            details.append(f"missing_in_exchange={missing}")
        if extras:
            details.append(f"missing_in_db={extras}")
        if direction_mismatch:
            details.append(f"direction_mismatch={direction_mismatch}")
        return False, "; ".join(details)
    return True, ""


async def get_equity_for_venue(
    db: AITraderDB,
    executor: Executor,
    venue: str,
    fallback: float,
    dry_run: bool,
    *,
    db_call_fn: Optional[Callable[..., Any]] = None,
    normalize_hl_base_url_fn: Callable[[str], str] = normalize_hl_base_url,
    fetch_hl_state_http_fn: Callable[[str, str], Any] = fetch_hl_state_http,
    get_hl_equity_for_account_fn: Optional[Callable[..., Any]] = None,
    extract_lighter_equity_fn: Callable[[Any], Optional[float]] = extract_lighter_equity,
) -> Tuple[float, str]:
    try:
        if db_call_fn is not None:
            eq = await db_call_fn(db.get_latest_equity, venue)
        else:
            eq = await asyncio.to_thread(db.get_latest_equity, venue)
        if eq is not None and float(eq) > 0:
            return float(eq), "db_snapshot"
    except Exception:
        pass

    if dry_run:
        return float(fallback), "fallback_dry_run"

    hl_base_url = normalize_hl_base_url_fn(os.getenv("HYPERLIQUID_PRIVATE_NODE", ""))

    if venue == VENUE_HYPERLIQUID:
        adapter = executor.hyperliquid
        address = getattr(adapter, "_address", None)
        if address:
            try:
                state = await fetch_hl_state_http_fn(address, hl_base_url)
                margin = state.get("marginSummary", {}) if isinstance(state, dict) else {}
                account_value = float(margin.get("accountValue", 0.0))
                if account_value > 0:
                    return account_value, "adapter"
            except Exception:
                pass

    if venue == VENUE_HYPERLIQUID:
        fn = get_hl_equity_for_account_fn or get_hl_equity_for_account
        return await fn(executor=executor, use_wallet=True, fallback=fallback, dry_run=dry_run)

    if venue == VENUE_LIGHTER:
        adapter = executor.lighter
        if getattr(adapter, "_account_api", None) is not None and getattr(adapter, "_account_index", None) is not None:
            try:
                account_response = await adapter._account_api.account(
                    by="index",
                    value=str(adapter._account_index),
                )
                total_equity = extract_lighter_equity_fn(account_response)
                if total_equity is not None and total_equity > 0:
                    return total_equity, "adapter"
            except Exception:
                pass

    return float(fallback), "fallback"


async def get_hl_equity_for_account(
    executor: Executor,
    use_wallet: bool,
    fallback: float,
    dry_run: bool,
    *,
    fetch_hl_wallet_unified_equity_fn: Callable[[Any, str], Any] = fetch_hl_wallet_unified_equity,
    normalize_hl_base_url_fn: Callable[[str], str] = normalize_hl_base_url,
    fetch_hl_state_http_fn: Callable[[str, str], Any] = fetch_hl_state_http,
) -> Tuple[float, str]:
    if dry_run:
        return float(fallback), "fallback_dry_run"

    adapter = executor.hyperliquid
    address = getattr(adapter, "_address", None)

    if not address:
        return float(fallback), "missing_address"

    if use_wallet:
        # Backward-compatible hook: allow callers to request the same
        # unified fetch path while still using the merged Hyperliquid venue.
        try:
            unified_equity = await fetch_hl_wallet_unified_equity_fn(adapter, str(address))
            if unified_equity > 0:
                return unified_equity, "wallet_unified"
        except Exception as exc:
            print(f"Warning: wallet unified equity fetch failed for {address}: {exc}")

    try:
        base_url = normalize_hl_base_url_fn(os.getenv("HYPERLIQUID_PRIVATE_NODE", ""))
        state = await fetch_hl_state_http_fn(address, base_url)
        margin = state.get("marginSummary", {}) if isinstance(state, dict) else {}
        account_value = float(margin.get("accountValue", 0.0))
        if account_value > 0:
            source = "wallet_unified" if use_wallet else "hyperliquid"
            return account_value, source
    except Exception as exc:
        print(f"Warning: wallet clearinghouse equity fetch failed for {address}: {exc}")

    return float(fallback), "fallback"
