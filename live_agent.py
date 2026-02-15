#!/usr/bin/env python3
"""
Live Agent runner for EVClaw (context -> execute).

This script is intended for the main agent to:
  - select candidates from JSON context
  - validate candidates + apply hard gates
  - record PROPOSED proposals for OpenClaw execution (no user approval prompts)
  - write authoritative records to SQLite
"""

from __future__ import annotations

import asyncio
import json
import os
import random
import requests
import socket
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from threading import Lock
from types import SimpleNamespace
from typing import Any, Dict, List, Optional, Set, Tuple

from dotenv import load_dotenv

from ai_trader_db import AITraderDB
from agi_context import load_pct_24h_history_from_trades
from executor import Executor
from adaptive_sltp import AdaptiveSLTPManager, AdaptiveSLTPConfig
from symbol_rr_learning import HybridSLTPAdapter
from trade_tracker import TradeTracker
from exchanges import VENUE_HYPERLIQUID, VENUE_HYPERLIQUID_WALLET, VENUE_LIGHTER
from risk_manager import DynamicRiskManager, RiskConfig
from safety_manager import SafetyManager
from live_agent_state import (
    clear_pending,
    load_pending,
    mark_busy,
    mark_idle,
    set_last_notified,
)
from live_agent_utils import (
    MAX_CANDIDATES,
    apply_learning_overlay,
    cap_size_usd,
    clamp_risk_pct,
    # check_symbol_on_both_venues removed (single-venue allowed)
    compute_risk_size_usd,
    enforce_max_candidates,
    validate_candidates_payload,
)
from llm_entry_gate import entry_gate_enabled_env, run_entry_gate
from candidate_pipeline import (
    CANDIDATE_TOPK_MAX,
    CANDIDATE_TOPK_MIN,
    CANDIDATE_TOPK_SCORE_GATE,
    _is_hip3,
    build_candidates_from_context,
)
from cycle_io import load_candidates_file, load_context_file, load_cycle_file
from execution_dispatch import send_system_event
from jsonl_io import append_jsonl
from live_agent_deps import LiveAgentDeps
from proposal_writer import insert_proposals
from conviction_model import (
    ConvictionConfig,
    compute_blended_conviction,
    compute_brain_conviction_no_floor,
    get_conviction_config,
    resolve_order_type,
)
from venues import normalize_venue, venues_for_symbol

# Reuse CLI helpers for price/ATR derivation and config
from cli import (
    build_execution_config,
    derive_prices_from_symbol_data,
    find_symbol_data,
    load_config,
)

from atr_service import get_atr_service
import live_agent_runtime
import live_agent_gate
from env_utils import EVCLAW_RUNTIME_DIR
import live_agent_process
import live_agent_cycle
import live_agent_runner


SKILL_DIR = Path(__file__).parent
load_dotenv(SKILL_DIR / ".env")

DEFAULT_DB = str(SKILL_DIR / "ai_trader.db")


def _env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None:
        return default
    try:
        return int(value)
    except ValueError:
        return default


def _env_float(name: str, default: float) -> float:
    value = os.getenv(name)
    if value is None:
        return default
    try:
        return float(value)
    except ValueError:
        return default


def _env_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return bool(default)
    return str(value).strip().lower() in ("1", "true", "yes", "y", "on")


def _normalize_pct_cap(value: Any, default_pct: float = 25.0) -> float:
    try:
        raw = float(value)
    except Exception:
        raw = float(default_pct)
    if raw <= 0:
        raw = float(default_pct)
    if 0 < raw <= 1.0:
        raw = raw * 100.0
    return max(0.0, min(100.0, raw))


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return float(default)


def _parse_iso_timestamp(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        if isinstance(value, (int, float)):
            ts = float(value)
            return ts if ts > 0 else None
        text = str(value).strip()
        if not text:
            return None
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        return datetime.fromisoformat(text).timestamp()
    except Exception:
        return None


def _resolve_context_generated_ts(context_payload: Dict[str, Any], context_path: Path) -> Optional[float]:
    """Best-effort context generation timestamp for staleness gating."""
    if isinstance(context_payload, dict):
        generated_ts = _parse_iso_timestamp(context_payload.get("generated_at_ts"))
        if generated_ts:
            return generated_ts
        generated_ts = _parse_iso_timestamp(context_payload.get("generated_at"))
        if generated_ts:
            return generated_ts
    try:
        mtime = float(context_path.stat().st_mtime)
        return mtime if mtime > 0 else None
    except Exception:
        return None


def _candidate_key_metrics(candidate: Dict[str, Any]) -> Dict[str, Any]:
    ctx = candidate.get("context_snapshot")
    if not isinstance(ctx, dict):
        return {}
    km = ctx.get("key_metrics")
    if not isinstance(km, dict):
        return {}
    return km


def _annotate_conviction_fields(
    candidates: List[Dict[str, Any]],
    *,
    conviction_config: Optional[ConvictionConfig] = None,
) -> None:
    """Compute pipeline/brain/blended conviction fields for each candidate."""
    cfg = conviction_config or ConvictionConfig.load()
    conviction_cfg = get_conviction_config(config=cfg)
    for candidate in candidates:
        if not isinstance(candidate, dict):
            continue
        direction = str(candidate.get("direction") or "").upper()
        pipeline_conv = max(0.0, min(1.0, _safe_float(candidate.get("conviction"), 0.0)))
        signals_snapshot = candidate.get("signals_snapshot")
        if not isinstance(signals_snapshot, dict):
            signals_snapshot = {}
        key_metrics = _candidate_key_metrics(candidate)
        brain_conv = compute_brain_conviction_no_floor(
            signals_snapshot=signals_snapshot,
            key_metrics=key_metrics,
            direction=direction,
        )
        blended_conv = compute_blended_conviction(pipeline_conv, brain_conv, config=cfg)

        candidate["pipeline_conviction"] = float(pipeline_conv)
        candidate["brain_conviction"] = float(brain_conv)
        candidate["blended_conviction"] = float(blended_conv)
        # Canonical conviction for downstream sizing/proposal logs.
        candidate["conviction"] = float(blended_conv)
        candidate["conviction_source"] = "blended"
        if "conviction_model" not in candidate:
            candidate["conviction_model"] = {
                "weights": dict(conviction_cfg.get("weights") or {}),
                "blend_pipeline": conviction_cfg.get("blend_pipeline"),
                "chase_threshold": conviction_cfg.get("chase_threshold"),
                "limit_min": conviction_cfg.get("limit_min"),
            }


@dataclass(frozen=True)
class LiveAgentEnvConfig:
    runtime_dir: Path
    claim_stale_seconds: int
    cycle_retry_delay_seconds: float
    loop_sleep_seconds: float
    loop_sleep_jitter_seconds: float
    pending_check_interval_seconds: float
    hip3_flip_emit_events: bool
    hip3_flip_notify_cooldown_seconds: float
    hip3_trading_enabled: bool

    @classmethod
    def from_env(cls) -> "LiveAgentEnvConfig":
        return cls(
            runtime_dir=Path(EVCLAW_RUNTIME_DIR),
            claim_stale_seconds=900,
            cycle_retry_delay_seconds=_env_float("EVCLAW_CYCLE_RETRY_DELAY_SECONDS", 30.0),
            loop_sleep_seconds=2.0,
            loop_sleep_jitter_seconds=_env_float("EVCLAW_LOOP_SLEEP_JITTER_SECONDS", 0.0),
            pending_check_interval_seconds=_env_float("EVCLAW_PENDING_CHECK_INTERVAL_SEC", 30.0),
            hip3_flip_emit_events=_env_bool("EVCLAW_HIP3_FLIP_EMIT_EVENTS", True),
            hip3_flip_notify_cooldown_seconds=_env_float(
                "EVCLAW_HIP3_FLIP_NOTIFY_COOLDOWN_SECONDS", 900.0
            ),
            hip3_trading_enabled=_env_bool("EVCLAW_HIP3_TRADING_ENABLED", True),
        )


LIVE_AGENT_ENV = LiveAgentEnvConfig.from_env()
RUNTIME_DIR = LIVE_AGENT_ENV.runtime_dir
HEARTBEAT_FILE = Path("/tmp/evclaw_fill_reconciler_heartbeat.json")
DEFAULT_CLAIM_STALE_SECONDS = LIVE_AGENT_ENV.claim_stale_seconds

# Module-level lazy cache for learning engines (per-db path)
_LEARNING_ENGINES: Dict[str, Any] = {}
_LEARNING_ENGINE_LOCK = Lock()
DEFAULT_CYCLE_RETRY_DELAY_SECONDS = LIVE_AGENT_ENV.cycle_retry_delay_seconds
_CONVICTION_CFG_CACHE_LOCK = Lock()
_CONVICTION_CFG_CACHE: Dict[str, Any] = {"expires_at": 0.0, "config": None}


def _runtime_conviction_config(db: Optional[AITraderDB], ttl_seconds: float = 60.0) -> ConvictionConfig:
    """Load conviction config from DB snapshot with short TTL cache."""
    ttl = float(ttl_seconds)
    if ttl > 0:
        now = time.time()
        with _CONVICTION_CFG_CACHE_LOCK:
            cached = _CONVICTION_CFG_CACHE.get("config")
            expires = float(_CONVICTION_CFG_CACHE.get("expires_at") or 0.0)
            if cached is not None and now < expires:
                return cached
    else:
        now = time.time()

    cfg = ConvictionConfig.load()
    if db is not None and hasattr(db, "get_active_conviction_config"):
        try:
            active = db.get_active_conviction_config()
        except Exception:
            active = None
        if isinstance(active, dict):
            params = active.get("params")
            if isinstance(params, dict):
                cfg = ConvictionConfig(
                    weights=params.get("weights", cfg.weights),
                    blend_pipeline=_safe_float(params.get("blend_pipeline"), cfg.blend_pipeline),
                    chase_threshold=_safe_float(params.get("chase_threshold"), cfg.chase_threshold),
                    limit_min=_safe_float(params.get("limit_min"), cfg.limit_min),
                    conviction_z_denom=cfg.conviction_z_denom,
                    strength_z_mult=cfg.strength_z_mult,
                    agree_bonus_4=cfg.agree_bonus_4,
                    agree_bonus_5=cfg.agree_bonus_5,
                    agree_mult_3=cfg.agree_mult_3,
                    agree_mult_4=cfg.agree_mult_4,
                    smart_adj_cap=cfg.smart_adj_cap,
                )

    if ttl > 0:
        with _CONVICTION_CFG_CACHE_LOCK:
            _CONVICTION_CFG_CACHE["config"] = cfg
            _CONVICTION_CFG_CACHE["expires_at"] = now + ttl
    return cfg


def _get_learning_engine(db_path: str):
    """Get or create a module-level learning engine bound to a DB path."""
    key = str(db_path or DEFAULT_DB).strip() or str(DEFAULT_DB)
    cached = _LEARNING_ENGINES.get(key)
    if cached is not None:
        return cached
    with _LEARNING_ENGINE_LOCK:
        cached = _LEARNING_ENGINES.get(key)
        if cached is not None:
            return cached
        engine = None
        try:
            from learning_engine import LearningEngine
            memory_dir = SKILL_DIR / "memory"
            memory_dir.mkdir(exist_ok=True)
            engine = LearningEngine(db_path=key, memory_dir=memory_dir)
        except Exception:
            engine = None  # Don't crash if learning engine unavailable
        _LEARNING_ENGINES[key] = engine
        return engine
DEFAULT_LOOP_SLEEP_SECONDS = LIVE_AGENT_ENV.loop_sleep_seconds
DEFAULT_LOOP_SLEEP_JITTER_SECONDS = LIVE_AGENT_ENV.loop_sleep_jitter_seconds
DEFAULT_PENDING_CHECK_INTERVAL_SECONDS = LIVE_AGENT_ENV.pending_check_interval_seconds
DEFAULT_CONTEXT_MAX_AGE_SECONDS = _env_float("EVCLAW_CONTEXT_MAX_AGE_SEC", 600.0)


async def _run_db_call(method, /, *args, **kwargs):
    """Run blocking SQLite calls off the event loop."""
    return await asyncio.to_thread(method, *args, **kwargs)


# HIP3 flip review / decay notifications
HIP3_FLIP_EMIT_EVENTS = LIVE_AGENT_ENV.hip3_flip_emit_events
HIP3_FLIP_NOTIFY_COOLDOWN_SECONDS = LIVE_AGENT_ENV.hip3_flip_notify_cooldown_seconds

# TEMP kill-switch to stop *new* HIP3 (xyz:*) trading while keeping normal perps running.
HIP3_TRADING_ENABLED = LIVE_AGENT_ENV.hip3_trading_enabled


def _format_hip3_flip_detail(
    *,
    existing_dir: str,
    new_dir: str,
    hip3_main: Optional[Dict[str, Any]],
) -> str:
    comp = hip3_main.get("components", {}) if isinstance(hip3_main, dict) else {}
    return (
        f"hip3_main_flip {existing_dir}->{new_dir} "
        f"z_signed={comp.get('flow_z_signed')} thr={comp.get('flow_threshold')} "
        f"mult={comp.get('threshold_mult')} ofm_dir={comp.get('ofm_dir')} "
        f"ofm_conf={comp.get('ofm_conf')} dead_strength={comp.get('dead_strength')} "
        f"dead_boost={comp.get('dead_boost_mult')}"
    )


async def _maybe_emit_hip3_flip_review(
    *,
    db: AITraderDB,
    symbol: str,
    venue: str,
    trade_id: Optional[int],
    existing_dir: str,
    new_dir: str,
    hip3_main: Optional[Dict[str, Any]],
) -> None:
    now_ts = time.time()
    try:
        last_flag = await _run_db_call(db.get_latest_decay_flag, symbol=symbol, venue=venue)
        if last_flag:
            last_ts = float(last_flag.get("ts") or 0.0)
            if (now_ts - last_ts) < HIP3_FLIP_NOTIFY_COOLDOWN_SECONDS:
                return
    except Exception as exc:
        print(f"Warning: failed to load latest decay flag for {venue}/{symbol}: {exc}")

    detail = _format_hip3_flip_detail(existing_dir=existing_dir, new_dir=new_dir, hip3_main=hip3_main)
    try:
        await _run_db_call(
            db.record_decay_flag,
            symbol=symbol,
            venue=venue,
            trade_id=trade_id,
            db_direction=existing_dir,
            live_direction=new_dir,
            reason="HIP3_MAIN_FLIP",
            detail=detail,
            signal_flip_only=True,
            notify_only=True,
        )
    except Exception as exc:
        print(f"Warning: failed to persist hip3 flip review flag for {venue}/{symbol}: {exc}")

    if HIP3_FLIP_EMIT_EVENTS:
        send_system_event(
            {
                "event": "DECAY_EXIT",
                "symbol": symbol,
                "venue": venue,
                "trade_id": trade_id,
                "reason": "HIP3_MAIN_FLIP",
                "detail": detail,
                "plan_only": True,
            }
        )

_STARTING_EQUITY_CACHE: Optional[float] = None
_STARTING_EQUITY_CACHE_TS: float = 0.0
_STARTING_EQUITY_CACHE_TTL_SECONDS = _env_float("EVCLAW_STARTING_EQUITY_CACHE_TTL_SECONDS", 300.0)


def _normalize_hl_base_url(raw: str) -> str:
    return live_agent_runtime.normalize_hl_base_url(raw)


def _resolve_hl_equity_address() -> Optional[str]:
    return live_agent_runtime.resolve_hl_equity_address()


def _resolve_sqlite_db_path(db: Any) -> str:
    return live_agent_runtime.resolve_sqlite_db_path(db, default_db=DEFAULT_DB)


def _extract_lighter_equity(account_response: Any) -> Optional[float]:
    return live_agent_runtime.extract_lighter_equity(account_response)


async def _fetch_hl_state_http(address: str, base_url: str) -> Optional[Dict[str, Any]]:
    return await live_agent_runtime.fetch_hl_state_http(address, base_url)


async def _fetch_hl_equity_http(address: str, base_url: str) -> float:
    return await live_agent_runtime.fetch_hl_equity_http(
        address,
        base_url,
        fetch_hl_state_http_fn=_fetch_hl_state_http,
    )


async def _fetch_hl_wallet_unified_equity(adapter: Any, address: str) -> float:
    return await live_agent_runtime.fetch_hl_wallet_unified_equity(adapter, address)


async def _resolve_starting_equity(config: Dict[str, Any]) -> float:
    global _STARTING_EQUITY_CACHE, _STARTING_EQUITY_CACHE_TS
    now = time.time()
    if (
        _STARTING_EQUITY_CACHE is not None
        and _STARTING_EQUITY_CACHE_TS > 0
        and (now - _STARTING_EQUITY_CACHE_TS) < max(5.0, float(_STARTING_EQUITY_CACHE_TTL_SECONDS))
    ):
        return _STARTING_EQUITY_CACHE

    print("Starting equity mode=auto-fetch")

    address = _resolve_hl_equity_address()
    base_url = _normalize_hl_base_url(os.getenv("HYPERLIQUID_PRIVATE_NODE", ""))
    if address:
        fetched = await _fetch_hl_equity_http(address, base_url)
    else:
        fetched = 0.0

    if fetched > 0:
        _STARTING_EQUITY_CACHE = fetched
        _STARTING_EQUITY_CACHE_TS = now
        print(f"Starting equity auto-fetched from Hyperliquid: {fetched:.2f}")
        return fetched

    fallback = float(
        (config.get("config", {}).get("risk", {}) or {}).get("starting_equity", 10000.0) or 0.0
    )
    if fallback <= 0:
        fallback = 10000.0
    # Do not cache fallback from failed fetches; retry auto-fetch next cycle.
    _STARTING_EQUITY_CACHE = None
    _STARTING_EQUITY_CACHE_TS = 0.0
    if not address:
        print(
            "Starting equity fallback (missing HYPERLIQUID_ADDRESS). "
            f"value={fallback:.2f}"
        )
    else:
        print(f"Starting equity fallback (auto-fetch failed). value={fallback:.2f}")
    return fallback


def _utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def _sync_risk_manager_state_from_safety(
    risk_mgr: Optional[DynamicRiskManager],
    safety_mgr: Optional[SafetyManager],
) -> None:
    """Copy persisted safety state into an in-memory DynamicRiskManager instance."""
    if risk_mgr is None or safety_mgr is None:
        return
    try:
        state = safety_mgr.get_state()
    except Exception as exc:
        print(f"Warning: failed to load safety manager state for risk sync: {exc}")
        return
    try:
        risk_mgr.daily_pnl = float(getattr(state, "daily_pnl", 0.0) or 0.0)
    except Exception:
        risk_mgr.daily_pnl = 0.0
    try:
        risk_mgr.win_streak = int(getattr(state, "consecutive_wins", 0) or 0)
    except Exception:
        risk_mgr.win_streak = 0
    try:
        risk_mgr.loss_streak = int(getattr(state, "consecutive_losses", 0) or 0)
    except Exception:
        risk_mgr.loss_streak = 0


def _default_candidates_file(seq: int) -> str:
    return str(RUNTIME_DIR / f"evclaw_candidates_{seq}.json")


def _resolve_db_path(dry_run: bool) -> str:
    config = load_config()
    exec_config = build_execution_config(config, dry_run=dry_run)
    return str(exec_config.db_path or DEFAULT_DB)


def _compact_open_positions_for_gate(db: Optional[AITraderDB], limit: int = 12) -> List[Dict[str, Any]]:
    """Build a compact open-position list for LLM gate exposure context."""
    if db is None:
        return []
    try:
        trades = db.get_open_trades()
    except Exception as exc:
        print(f"Warning: failed to load open trades for gate context: {exc}")
        return []

    compact: List[Dict[str, Any]] = []
    for t in trades or []:
        try:
            symbol = str(getattr(t, "symbol", "") or "").upper()
            direction = str(getattr(t, "direction", "") or "").upper()
            venue = normalize_venue(getattr(t, "venue", "") or "")
            notional = abs(float(getattr(t, "notional_usd", 0.0) or 0.0))
        except Exception:
            continue
        if not symbol or direction not in ("LONG", "SHORT"):
            continue
        compact.append(
            {
                "symbol": symbol,
                "direction": direction,
                "venue": venue,
                "notional_usd": notional,
            }
        )

    compact.sort(key=lambda row: float(row.get("notional_usd") or 0.0), reverse=True)
    return compact[: max(0, int(limit))]


def _build_processor_id(explicit: Optional[str] = None) -> str:
    return live_agent_runtime.build_processor_id(explicit)


def build_deps(dry_run: bool, db_path_override: Optional[str] = None) -> LiveAgentDeps:
    return live_agent_runtime.build_deps(
        dry_run=dry_run,
        db_path_override=db_path_override,
        load_config_fn=load_config,
        build_execution_config_fn=build_execution_config,
        default_db=DEFAULT_DB,
        skill_dir=SKILL_DIR,
    )


def _build_hybrid_sltp(exec_config: Any, tracker: TradeTracker, db_path: Optional[str] = None) -> HybridSLTPAdapter:
    return live_agent_runtime.build_hybrid_sltp(
        exec_config,
        tracker,
        db_path=db_path,
        skill_dir=SKILL_DIR,
    )


async def _build_executor_with_learning(
    exec_config: Any,
    tracker: TradeTracker,
    db_path: Optional[str] = None,
) -> Executor:
    return await live_agent_runtime.build_executor_with_learning(
        exec_config,
        tracker,
        db_path=db_path,
        build_hybrid_sltp_fn=_build_hybrid_sltp,
        executor_cls=Executor,
    )


def check_fill_reconciler(required_venues: List[str], max_age: int = 90) -> Tuple[bool, str]:
    return live_agent_runtime.check_fill_reconciler(required_venues, max_age=max_age)


async def ensure_fill_reconciler(required_venues: List[str], max_age: int = 90) -> Tuple[bool, str]:
    return await live_agent_runtime.ensure_fill_reconciler(
        required_venues,
        max_age=max_age,
        skill_dir=SKILL_DIR,
        check_fill_reconciler_fn=check_fill_reconciler,
    )


async def ensure_positions_reconciled(
    executor: Executor,
    db: AITraderDB,
    allowed_venues: Optional[List[str]] = None,
) -> Tuple[bool, str]:
    return await live_agent_runtime.ensure_positions_reconciled(
        executor,
        db,
        allowed_venues=allowed_venues,
        skill_dir=SKILL_DIR,
        check_positions_reconciled_fn=check_positions_reconciled,
    )


async def check_symbol_on_venues(
    symbol: str,
    executor: Executor,
    venues: List[str],
) -> Dict[str, Any]:
    return await live_agent_runtime.check_symbol_on_venues(symbol, executor, venues)


async def check_positions_reconciled(
    executor: Executor,
    db: AITraderDB,
    allowed_venues: Optional[List[str]] = None,
) -> Tuple[bool, str]:
    return await live_agent_runtime.check_positions_reconciled(
        executor,
        db,
        allowed_venues=allowed_venues,
        db_call_fn=_run_db_call,
    )



async def get_equity_for_venue(
    db: AITraderDB,
    executor: Executor,
    venue: str,
    fallback: float,
    dry_run: bool,
) -> Tuple[float, str]:
    return await live_agent_runtime.get_equity_for_venue(
        db,
        executor,
        venue,
        fallback,
        dry_run,
        db_call_fn=_run_db_call,
        normalize_hl_base_url_fn=_normalize_hl_base_url,
        fetch_hl_state_http_fn=_fetch_hl_state_http,
        get_hl_equity_for_account_fn=get_hl_equity_for_account,
        extract_lighter_equity_fn=_extract_lighter_equity,
    )


async def get_hl_equity_for_account(
    executor: Executor,
    use_wallet: bool,
    fallback: float,
    dry_run: bool,
) -> Tuple[float, str]:
    return await live_agent_runtime.get_hl_equity_for_account(
        executor,
        use_wallet,
        fallback,
        dry_run,
        fetch_hl_wallet_unified_equity_fn=_fetch_hl_wallet_unified_equity,
        normalize_hl_base_url_fn=_normalize_hl_base_url,
        fetch_hl_state_http_fn=_fetch_hl_state_http,
    )


def _block_candidate(
    *,
    db: AITraderDB,
    summary: Dict[str, Any],
    seq: int,
    candidate: Dict[str, Any],
    reason: str,
    venues: List[str],
    size_overrides: Optional[Dict[str, float]] = None,
    block_count: int = 1,
) -> None:
    """Record BLOCKED proposal with consistent blocked-counter updates."""
    insert_proposals(
        db,
        seq,
        candidate,
        "BLOCKED",
        reason,
        venues=venues,
        size_overrides=size_overrides,
    )
    counts = summary.setdefault("counts", {})
    counts["blocked"] = int(counts.get("blocked", 0)) + max(0, int(block_count))


def _resolve_active_venues(exec_config) -> List[str]:
    """Resolve enabled venues once per cycle from execution config."""
    active: List[str] = []
    if getattr(exec_config, "lighter_enabled", False):
        active.append(VENUE_LIGHTER)
    if getattr(exec_config, "hl_enabled", False) or getattr(exec_config, "hl_wallet_enabled", False):
        active.append(VENUE_HYPERLIQUID)
    # Canonicalize to dedupe aliases like VENUE_HYPERLIQUID_WALLET.
    return list(dict.fromkeys(active))


def _entry_gate_enabled() -> bool:
    return entry_gate_enabled_env(False)


def _should_reuse_candidates_file(output_file: str, *, gate_enabled: bool) -> bool:
    use_existing = Path(output_file).exists() and Path(output_file).stat().st_size > 10
    if gate_enabled and use_existing:
        existing_payload = load_candidates_file(output_file)
        if not (isinstance(existing_payload, dict) and "llm_gate" in existing_payload):
            return False
    return use_existing


def _record_invalid_candidates(
    *,
    db: AITraderDB,
    summary: Dict[str, Any],
    seq: int,
    invalid_candidates: List[Dict[str, Any]],
    venues: List[str],
) -> None:
    for bad in invalid_candidates:
        candidate = bad.get("candidate") or {}
        reason = "; ".join(bad.get("errors") or ["invalid_candidate"])
        _block_candidate(
            db=db,
            summary=summary,
            seq=seq,
            candidate=candidate,
            reason=reason,
            venues=venues,
        )


def _build_process_summary(seq: int) -> Dict[str, Any]:
    return live_agent_gate.build_process_summary(seq)


def _load_and_validate_candidates_payload(
    *,
    candidates_file: str,
    db: AITraderDB,
    summary: Dict[str, Any],
    seq: int,
    venues: List[str],
) -> Tuple[Optional[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]:
    return live_agent_gate.load_and_validate_candidates_payload(
        candidates_file=candidates_file,
        db=db,
        summary=summary,
        seq=seq,
        venues=venues,
        load_candidates_file_fn=load_candidates_file,
        validate_candidates_payload_fn=validate_candidates_payload,
        block_candidate_fn=_block_candidate,
    )


def _apply_llm_gate_selection(
    *,
    payload: Dict[str, Any],
    db: AITraderDB,
    summary: Dict[str, Any],
    seq: int,
    active_venues: List[str],
    valid_candidates: List[Dict[str, Any]],
    invalid_candidates: List[Dict[str, Any]],
    conviction_config: Optional[ConvictionConfig] = None,
) -> Tuple[List[Dict[str, Any]], bool]:
    return live_agent_gate.apply_llm_gate_selection(
        payload=payload,
        db=db,
        summary=summary,
        seq=seq,
        active_venues=active_venues,
        valid_candidates=valid_candidates,
        invalid_candidates=invalid_candidates,
        safe_float_fn=_safe_float,
        block_candidate_fn=_block_candidate,
        record_invalid_candidates_fn=_record_invalid_candidates,
        resolve_order_type_fn=resolve_order_type,
        conviction_config=conviction_config,
    )


def _enforce_candidate_limit_and_audit(
    *,
    db: AITraderDB,
    summary: Dict[str, Any],
    seq: int,
    active_venues: List[str],
    valid_candidates: List[Dict[str, Any]],
    invalid_candidates: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    return live_agent_gate.enforce_candidate_limit_and_audit(
        db=db,
        summary=summary,
        seq=seq,
        active_venues=active_venues,
        valid_candidates=valid_candidates,
        invalid_candidates=invalid_candidates,
        adaptive_candidate_limit_fn=_adaptive_candidate_limit,
        enforce_max_candidates_fn=enforce_max_candidates,
        block_candidate_fn=_block_candidate,
        record_invalid_candidates_fn=_record_invalid_candidates,
    )


def _adaptive_candidate_limit(candidates: List[Dict[str, Any]]) -> int:
    return live_agent_gate.adaptive_candidate_limit(
        candidates,
        min_k=int(CANDIDATE_TOPK_MIN),
        max_k=int(CANDIDATE_TOPK_MAX),
        score_gate=float(CANDIDATE_TOPK_SCORE_GATE),
        conviction_gate=0.60,
        max_candidates=MAX_CANDIDATES,
        safe_float_fn=_safe_float,
    )


async def _resolve_global_block_reason(
    *,
    db: AITraderDB,
    executor: Executor,
    exec_config: Any,
    active_venues: List[str],
) -> Optional[str]:
    if exec_config.use_fill_reconciler:
        ok, reason = await ensure_fill_reconciler(active_venues)
        if not ok:
            return f"fill_reconciler_not_running: {reason}"

    if exec_config.dry_run:
        return None

    reconciled, detail = await ensure_positions_reconciled(
        executor, db, allowed_venues=active_venues
    )
    if not reconciled:
        return f"positions_not_reconciled: {detail}"
    return None


def _append_reason_note(base: Optional[str], note: str) -> Optional[str]:
    piece = str(note or "").strip()
    if not piece:
        return base
    return (base + "; " if base else "") + piece


def _apply_position_pct_cap(
    *,
    size_usd: float,
    equity_usd: float,
    pct_cap: float,
    reason_tag: str,
    clamp_reason: Optional[str],
) -> Tuple[float, Optional[str]]:
    if pct_cap <= 0 or equity_usd <= 0:
        return size_usd, clamp_reason
    size_cap = equity_usd * (pct_cap / 100.0)
    if size_usd > size_cap:
        return size_cap, _append_reason_note(clamp_reason, f"{reason_tag} {pct_cap:.3f}")
    return size_usd, clamp_reason


def _apply_sanity_cap_for_venues(
    *,
    size_by_venue: Dict[str, float],
    enabled_venues: List[str],
    sanity_cap: float,
) -> Tuple[Dict[str, float], List[str]]:
    updated = dict(size_by_venue)
    notes: List[str] = []
    if sanity_cap <= 0:
        return updated, notes

    venue_labels = (
        (VENUE_LIGHTER, "lighter"),
        (VENUE_HYPERLIQUID, "hl"),
    )
    for venue, label in venue_labels:
        if venue not in enabled_venues:
            continue
        capped, clamped = cap_size_usd(float(updated.get(venue) or 0.0), sanity_cap)
        updated[venue] = capped
        if clamped:
            notes.append(f"{label}_capped {capped:.2f} cap={sanity_cap:.2f}")
    return updated, notes


def _build_requested_size_overrides(
    *,
    candidate_venues: List[str],
    size_by_venue: Dict[str, float],
) -> Tuple[float, Dict[str, float]]:
    requested_total = 0.0
    size_overrides: Dict[str, float] = {}
    for venue in candidate_venues:
        size = float(size_by_venue.get(venue) or 0.0)
        requested_total += size
        size_overrides[venue] = size
    return requested_total, size_overrides


async def _apply_exit_cooldown(
    *,
    db: AITraderDB,
    summary: Dict[str, Any],
    seq: int,
    candidate: Dict[str, Any],
    symbol: str,
    direction: str,
    candidate_venues: List[str],
    label: str,
    reasons: List[str],
    cooldown_minutes: float,
) -> List[str]:
    """Apply per-reason re-entry cooldown and return allowed venues."""
    cooldown_seconds = max(0.0, float(cooldown_minutes) * 60.0)
    if cooldown_seconds <= 0:
        return list(candidate_venues)

    now_ts = time.time()
    allowed_venues: List[str] = []
    blocked_venues: List[Dict[str, Any]] = []

    for v in list(candidate_venues):
        last_exit_row = await _run_db_call(
            db.get_last_exit_by_reasons,
            symbol=symbol,
            venue=v,
            direction=direction,
            reasons=reasons,
        )
        if not last_exit_row:
            allowed_venues.append(v)
            continue

        last_exit = float(last_exit_row.get("exit_time") or 0.0)
        age_s = now_ts - last_exit
        if age_s < cooldown_seconds:
            blocked_venues.append({"venue": v, **last_exit_row})
        else:
            allowed_venues.append(v)

    if blocked_venues:
        for item in blocked_venues:
            v = str(item.get("venue") or "")
            last_exit = float(item.get("exit_time") or 0.0)
            age_min = max(0.0, (now_ts - last_exit) / 60.0)
            remain_min = max(0.0, (cooldown_seconds - (now_ts - last_exit)) / 60.0)
            reason = (
                f"{label}_cooldown_active (last_trade_id={item.get('trade_id')} "
                f"{age_min:.1f}m ago; wait {remain_min:.1f}m)"
            )
            _block_candidate(
                db=db,
                summary=summary,
                seq=seq,
                candidate=candidate,
                reason=reason,
                venues=[v],
            )
            summary["counts"][f"{label}_cooldown_blocked"] = (
                summary["counts"].get(f"{label}_cooldown_blocked", 0) + 1
            )

    if not allowed_venues:
        return []

    return allowed_venues


async def _process_candidates_impl(
    seq: int,
    cycle_file: str,
    candidates_file: str,
    dry_run: bool,
    risk_pct_lighter: float,
    risk_pct_hyperliquid: float,
    db_path_override: Optional[str] = None,
    deps: Optional[LiveAgentDeps] = None,
    executor: Optional[Executor] = None,
) -> Tuple[int, Dict[str, Any]]:
    api = {
        "build_deps": build_deps,
        "build_process_summary": _build_process_summary,
        "clamp_risk_pct": clamp_risk_pct,
        "resolve_active_venues": _resolve_active_venues,
        "runtime_conviction_config": _runtime_conviction_config,
        "build_executor_with_learning": _build_executor_with_learning,
        "load_cycle_file": load_cycle_file,
        "load_and_validate_candidates_payload": _load_and_validate_candidates_payload,
        "apply_llm_gate_selection": _apply_llm_gate_selection,
        "enforce_candidate_limit_and_audit": _enforce_candidate_limit_and_audit,
        "resolve_global_block_reason": _resolve_global_block_reason,
        "send_system_event": send_system_event,
        "block_candidate": _block_candidate,
        "resolve_starting_equity": _resolve_starting_equity,
        "SafetyManager": SafetyManager,
        "RiskConfig": RiskConfig,
        "DynamicRiskManager": DynamicRiskManager,
        "get_equity_for_venue": get_equity_for_venue,
        "VENUE_LIGHTER": VENUE_LIGHTER,
        "VENUE_HYPERLIQUID": VENUE_HYPERLIQUID,
        "VENUE_HYPERLIQUID_WALLET": VENUE_HYPERLIQUID_WALLET,
        "is_hip3": _is_hip3,
        "sync_risk_manager_state_from_safety": _sync_risk_manager_state_from_safety,
        "run_db_call": _run_db_call,
        "find_symbol_data": find_symbol_data,
        "derive_prices_from_symbol_data": derive_prices_from_symbol_data,
        "venues_for_symbol": venues_for_symbol,
        "check_symbol_on_venues": check_symbol_on_venues,
        "apply_exit_cooldown": _apply_exit_cooldown,
        "normalize_pct_cap": _normalize_pct_cap,
        "apply_position_pct_cap": _apply_position_pct_cap,
        "apply_sanity_cap_for_venues": _apply_sanity_cap_for_venues,
        "append_reason_note": _append_reason_note,
        "build_requested_size_overrides": _build_requested_size_overrides,
        "get_atr_service": get_atr_service,
        "append_jsonl": append_jsonl,
        "utc_now": _utc_now,
        "maybe_emit_hip3_flip_review": _maybe_emit_hip3_flip_review,
        "insert_proposals": insert_proposals,
        "HIP3_TRADING_ENABLED": HIP3_TRADING_ENABLED,
        "compute_risk_size_usd": compute_risk_size_usd,
        "resolve_order_type": resolve_order_type,
    }
    return await live_agent_process.process_candidates_impl(
        seq=seq,
        cycle_file=cycle_file,
        candidates_file=candidates_file,
        dry_run=dry_run,
        risk_pct_lighter=risk_pct_lighter,
        risk_pct_hyperliquid=risk_pct_hyperliquid,
        db_path_override=db_path_override,
        deps=deps,
        executor=executor,
        api=SimpleNamespace(**api),
    )


async def process_candidates(
    seq: int,
    cycle_file: str,
    candidates_file: str,
    dry_run: bool,
    risk_pct_lighter: float,
    risk_pct_hyperliquid: float,
    db_path_override: Optional[str] = None,
    deps: Optional[LiveAgentDeps] = None,
    executor: Optional[Executor] = None,
) -> Tuple[int, Dict[str, Any]]:
    """Thin wrapper around candidate processing implementation."""
    return await _process_candidates_impl(
        seq=seq,
        cycle_file=cycle_file,
        candidates_file=candidates_file,
        dry_run=dry_run,
        risk_pct_lighter=risk_pct_lighter,
        risk_pct_hyperliquid=risk_pct_hyperliquid,
        db_path_override=db_path_override,
        deps=deps,
        executor=executor,
    )


def _serialize_summary(summary: Dict[str, Any]) -> str:
    return json.dumps(summary, separators=(",", ":"), sort_keys=True, default=str)


def _atomic_write_json(path: str, payload: Dict[str, Any]) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_name(f".{target.name}.tmp.{os.getpid()}.{int(time.time() * 1000)}")
    data = json.dumps(payload, separators=(",", ":"), sort_keys=False, default=str)
    with tmp.open("w", encoding="utf-8") as f:
        f.write(data)
        f.flush()
        os.fsync(f.fileno())
    tmp.replace(target)


def _candidate_atr_pct_value(c: Dict[str, Any]) -> float:
    v = c.get("atr_pct")
    try:
        if isinstance(v, dict) and "value" in v:
            return float(v.get("value") or 0.0)
        return float(v or 0.0)
    except Exception:
        return 0.0


def _candidate_price_hint(c: Dict[str, Any]) -> float:
    """Best-effort price hint for ATR pct computation."""
    try:
        ctx = c.get("context_snapshot") or {}
        if isinstance(ctx, dict):
            km = ctx.get("key_metrics") or {}
            if isinstance(km, dict):
                p = km.get("price") or km.get("mark") or km.get("mid") or 0
                return float(p or 0.0)
    except Exception as exc:
        symbol = str(c.get("symbol") or "")
        print(f"Warning: failed to read candidate price hint for {symbol}: {exc}")
    return 0.0


async def enrich_candidates_atr_pct(
    candidates: Optional[List[Dict[str, Any]]], *, db_path: str
) -> None:
    """Fill missing/zero candidate atr_pct using ATRService before LLM entry gate.

    Rationale: tracker/context may omit ATR (atr_pct=0) for some symbols; the LLM gate
    uses candidate.atr_pct to veto trades. ATRService can fetch/cached ATR reliably.

    We enrich perps and HIP3 (XYZ:*) now that ATRService supports Massive OHLC for stocks.
    """
    if not candidates:
        return

    atr_service = get_atr_service(str(db_path or "").strip() or None)

    for c in candidates:
        if not isinstance(c, dict):
            continue
        sym = str(c.get("symbol") or "").strip().upper()
        if not sym:
            continue
        if ":" in sym and not sym.startswith("XYZ:"):
            # Unknown namespace: skip to avoid nonsense lookups.
            continue

        cur = _candidate_atr_pct_value(c)
        if cur > 0:
            continue

        price = _candidate_price_hint(c)
        try:
            atr_res = await atr_service.get_atr(sym, price=price if price > 0 else None)
        except Exception as exc:
            print(f"Warning: ATR enrichment failed for {sym}: {exc}")
            atr_res = None

        if not atr_res:
            continue

        atr_pct = None
        try:
            atr_pct = float(atr_res.atr_pct) if atr_res.atr_pct is not None else None
        except Exception:
            atr_pct = None

        if (atr_pct is None or atr_pct <= 0) and getattr(atr_res, "atr", None) and price > 0:
            try:
                atr_pct = (float(atr_res.atr) / float(price)) * 100.0
            except Exception:
                atr_pct = None

        if not atr_pct or atr_pct <= 0:
            continue

        # Top-level candidate field used by llm_entry_gate._compact_candidate
        if isinstance(c.get("atr_pct"), dict):
            c["atr_pct"]["value"] = float(atr_pct)
        else:
            c["atr_pct"] = {
                "value": float(atr_pct),
                "meaning": "Average true range as % of price. Higher=more volatile.",
            }

        # Keep context snapshot consistent (used elsewhere for SR-limit / display)
        try:
            ctx = c.get("context_snapshot")
            if isinstance(ctx, dict):
                km = ctx.get("key_metrics")
                if isinstance(km, dict):
                    km["atr_pct"] = float(atr_pct)
                    if "atr_pct_source" not in km:
                        km["atr_pct_source"] = f"atr_service:{getattr(atr_res, 'source', 'unknown')}"
        except Exception as exc:
            print(f"Warning: failed to mirror atr_pct into context snapshot for {sym}: {exc}")


def _build_cycle_api() -> SimpleNamespace:
    return SimpleNamespace(
        load_context_file=load_context_file,
        load_cycle_file=load_cycle_file,
        resolve_db_path=_resolve_db_path,
        ai_trader_db_cls=AITraderDB,
        build_candidates_from_context=build_candidates_from_context,
        MAX_CANDIDATES=MAX_CANDIDATES,
        get_learning_engine=_get_learning_engine,
        apply_learning_overlay=apply_learning_overlay,
        runtime_conviction_config=_runtime_conviction_config,
        annotate_conviction_fields=_annotate_conviction_fields,
        run_db_call=_run_db_call,
        compact_open_positions_for_gate=_compact_open_positions_for_gate,
        enrich_candidates_atr_pct=enrich_candidates_atr_pct,
        run_entry_gate=run_entry_gate,
        append_jsonl=append_jsonl,
        utc_now=_utc_now,
        atomic_write_json=_atomic_write_json,
        load_candidates_file=load_candidates_file,
        process_candidates=process_candidates,
        resolve_context_generated_ts=_resolve_context_generated_ts,
        DEFAULT_CONTEXT_MAX_AGE_SECONDS=DEFAULT_CONTEXT_MAX_AGE_SECONDS,
        default_candidates_file=_default_candidates_file,
        entry_gate_enabled=_entry_gate_enabled,
        mark_busy=mark_busy,
        mark_idle=mark_idle,
        should_reuse_candidates_file=_should_reuse_candidates_file,
        build_deps=build_deps,
        build_executor_with_learning=_build_executor_with_learning,
        get_atr_service=get_atr_service,
    )


async def _generate_cycle_candidates_file(
    *,
    seq: int,
    cycle_file: str,
    context_json_file: str,
    output_file: str,
    db: Optional[AITraderDB],
    db_path_override: Optional[str],
    dry_run: bool,
    use_llm: bool,
    llm_model: Optional[str],
    summary: Dict[str, Any],
) -> bool:
    return await live_agent_cycle.generate_cycle_candidates_file(
        seq=seq,
        cycle_file=cycle_file,
        context_json_file=context_json_file,
        output_file=output_file,
        db=db,
        db_path_override=db_path_override,
        dry_run=dry_run,
        use_llm=use_llm,
        llm_model=llm_model,
        summary=summary,
        api=_build_cycle_api(),
    )



def _annotate_cycle_llm_gate_status(
    *,
    proc_summary: Dict[str, Any],
    output_file: str,
    gate_enabled: bool,
) -> None:
    live_agent_cycle.annotate_cycle_llm_gate_status(
        proc_summary=proc_summary,
        output_file=output_file,
        gate_enabled=gate_enabled,
        api=_build_cycle_api(),
    )


async def _run_cycle_gate_step(
    *,
    seq: int,
    cycle_file: str,
    output_file: str,
    dry_run: bool,
    risk_pct_lighter: float,
    risk_pct_hyperliquid: float,
    db_path_override: Optional[str],
    shared_deps: Optional[LiveAgentDeps],
    shared_executor: Optional[Executor],
    gate_enabled: bool,
) -> Tuple[int, Dict[str, Any]]:
    return await live_agent_cycle.run_cycle_gate_step(
        seq=seq,
        cycle_file=cycle_file,
        output_file=output_file,
        dry_run=dry_run,
        risk_pct_lighter=risk_pct_lighter,
        risk_pct_hyperliquid=risk_pct_hyperliquid,
        db_path_override=db_path_override,
        shared_deps=shared_deps,
        shared_executor=shared_executor,
        gate_enabled=gate_enabled,
        api=_build_cycle_api(),
    )


async def _run_cycle_impl(
    seq: int,
    cycle_file: str,
    context_json_file: str,
    dry_run: bool,
    risk_pct_lighter: float,
    risk_pct_hyperliquid: float,
    candidates_file: Optional[str] = None,
    db: Optional[AITraderDB] = None,
    reuse_existing: bool = True,
    db_path_override: Optional[str] = None,
    context_file: Optional[str] = None,
    use_llm: bool = False,
    llm_model: Optional[str] = None,
) -> Tuple[int, Dict[str, Any]]:
    return await live_agent_cycle.run_cycle_impl(
        seq=seq,
        cycle_file=cycle_file,
        context_json_file=context_json_file,
        dry_run=dry_run,
        risk_pct_lighter=risk_pct_lighter,
        risk_pct_hyperliquid=risk_pct_hyperliquid,
        candidates_file=candidates_file,
        db=db,
        reuse_existing=reuse_existing,
        db_path_override=db_path_override,
        context_file=context_file,
        use_llm=use_llm,
        llm_model=llm_model,
        api=_build_cycle_api(),
    )


async def run_cycle(
    seq: int,
    cycle_file: str,
    context_json_file: str,
    dry_run: bool,
    risk_pct_lighter: float,
    risk_pct_hyperliquid: float,
    candidates_file: Optional[str] = None,
    db: Optional[AITraderDB] = None,
    reuse_existing: bool = True,
    db_path_override: Optional[str] = None,
    context_file: Optional[str] = None,
    use_llm: bool = False,
    llm_model: Optional[str] = None,
) -> Tuple[int, Dict[str, Any]]:
    """Thin wrapper around cycle orchestration implementation."""
    return await _run_cycle_impl(
        seq=seq,
        cycle_file=cycle_file,
        context_json_file=context_json_file,
        dry_run=dry_run,
        risk_pct_lighter=risk_pct_lighter,
        risk_pct_hyperliquid=risk_pct_hyperliquid,
        candidates_file=candidates_file,
        db=db,
        reuse_existing=reuse_existing,
        db_path_override=db_path_override,
        context_file=context_file,
        use_llm=use_llm,
        llm_model=llm_model,
    )


def _build_runner_api() -> SimpleNamespace:
    return SimpleNamespace(
        resolve_db_path=_resolve_db_path,
        ai_trader_db_cls=AITraderDB,
        build_processor_id=_build_processor_id,
        DEFAULT_CLAIM_STALE_SECONDS=DEFAULT_CLAIM_STALE_SECONDS,
        run_db_call=_run_db_call,
        RUNTIME_DIR=RUNTIME_DIR,
        run_cycle=run_cycle,
        serialize_summary=_serialize_summary,
        DEFAULT_CYCLE_RETRY_DELAY_SECONDS=DEFAULT_CYCLE_RETRY_DELAY_SECONDS,
        build_deps=build_deps,
        build_executor_with_learning=_build_executor_with_learning,
        get_learning_engine=_get_learning_engine,
        DEFAULT_PENDING_CHECK_INTERVAL_SECONDS=DEFAULT_PENDING_CHECK_INTERVAL_SECONDS,
        VENUE_LIGHTER=VENUE_LIGHTER,
        VENUE_HYPERLIQUID=VENUE_HYPERLIQUID,
        VENUE_HYPERLIQUID_WALLET=VENUE_HYPERLIQUID_WALLET,
        resolve_sqlite_db_path=_resolve_sqlite_db_path,
        DEFAULT_LOOP_SLEEP_SECONDS=DEFAULT_LOOP_SLEEP_SECONDS,
        DEFAULT_LOOP_SLEEP_JITTER_SECONDS=DEFAULT_LOOP_SLEEP_JITTER_SECONDS,
        load_pct_24h_history_from_trades=load_pct_24h_history_from_trades,
        load_pending=load_pending,
        clear_pending=clear_pending,
        set_last_notified=set_last_notified,
        process_candidates=process_candidates,
        mark_busy=mark_busy,
        mark_idle=mark_idle,
    )


async def run_from_db_once(args, db: Optional[AITraderDB] = None, db_path: Optional[str] = None) -> int:
    """Thin wrapper around DB cycle claim/process implementation."""
    return await live_agent_runner.run_from_db_once(
        args,
        db=db,
        db_path=db_path,
        api=_build_runner_api(),
    )


async def run_db_loop(args) -> int:
    """Thin wrapper around continuous DB loop implementation."""
    return await live_agent_runner.run_db_loop(args, api=_build_runner_api())


async def run_command(args) -> int:
    """Thin wrapper around CLI command router implementation."""
    return await live_agent_runner.run_command(args, api=_build_runner_api())


def main() -> int:
    """CLI entrypoint wrapper."""
    return live_agent_runner.main(api=_build_runner_api())


if __name__ == "__main__":
    sys.exit(main())
