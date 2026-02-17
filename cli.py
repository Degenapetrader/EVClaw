#!/usr/bin/env python3
"""
CLI for HyperLighter Trading Skill.

Commands:
- execute: Execute a cycle decision (execution-by-call)
- guardian: Validate + execute proposals for a cycle
- trade: Execute a manual trade
- signals: View current signals
- positions: View/manage positions
"""

import argparse
import asyncio
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Tuple, Dict, Any

import yaml
from dotenv import load_dotenv

from config_env import apply_env_overrides
from env_utils import env_int
from venues import normalize_venue

# Load environment variables from .env BEFORE any imports that use them
load_dotenv(Path(__file__).parent / '.env')

# Add skill path
sys.path.insert(0, str(Path(__file__).parent))

from signal_parser import get_actionable_signals
from executor import Executor, ExecutionConfig, ExecutionDecision
from conviction_model import resolve_order_type_runtime
from risk_manager import get_sector

from atr_service import get_atr_service

# SQLite is the SINGLE source of truth for tracking. Do not remove DB usage.
from ai_trader_db import AITraderDB


SKILL_DIR = Path(__file__).parent
MEMORY_DIR = SKILL_DIR / 'memory'
EXECUTOR_DB_PATH = os.getenv("EVCLAW_DB_PATH", str(SKILL_DIR / "ai_trader.db"))
EXECUTOR_RATE_LIMIT_TOKENS = 40
EXECUTOR_RATE_LIMIT_WINDOW_SECONDS = 60
EXECUTOR_USE_DB_POSITIONS = True
EXECUTOR_WRITE_POSITIONS_YAML = False
EXECUTOR_ENABLE_TRADE_JOURNAL = False
EXECUTOR_ENABLE_TRADE_TRACKER = False
EXECUTOR_USE_FILL_RECONCILER = True
CONFIG_FILE = SKILL_DIR / 'skill.yaml'


def _resolve_effective_order_type(
    exec_meta: Dict[str, Any],
    conviction: Any,
    *,
    db: Optional[AITraderDB] = None,
) -> Tuple[str, str]:
    """Resolve execution route from metadata first, then deterministic conviction fallback."""
    meta_order_type = ""
    if isinstance(exec_meta, dict):
        meta_order_type = str(exec_meta.get("order_type") or "").strip().lower()
    if meta_order_type in {"limit", "chase_limit"}:
        return meta_order_type, "metadata"

    fallback_source = "fallback_invalid_metadata" if meta_order_type else "fallback_conviction"
    try:
        conv_val = float(conviction)
        if conv_val != conv_val:
            raise ValueError("nan conviction")
    except Exception:
        return "reject", f"{fallback_source}_missing_conviction"

    resolved = str(resolve_order_type_runtime(conv_val, db=db) or "reject").strip().lower()
    if resolved in {"limit", "chase_limit"}:
        return resolved, fallback_source
    return "reject", fallback_source


def load_config() -> dict:
    """Load skill configuration."""
    if CONFIG_FILE.exists():
        with open(CONFIG_FILE, 'r') as f:
            raw = yaml.safe_load(f) or {}
            return apply_env_overrides(raw)
    return apply_env_overrides({})


def build_execution_config(config: Dict[str, Any], dry_run: bool) -> ExecutionConfig:
    """Build ExecutionConfig from skill.yaml config."""
    cfg = config.get('config', {})
    exec_cfg = cfg.get('executor', {})
    router_overrides = config.get('exchanges', {}).get('router', {}).get('overrides', {}) or {}

    lighter_enabled = bool(exec_cfg.get('lighter_enabled', True))
    hl_enabled = bool(exec_cfg.get('hl_enabled', True))

    # Base position size from skill config.
    base_position_size_usd = float(exec_cfg.get('base_position_size_usd', cfg.get('base_position_size_usd', 500.0)))

    hl_mirror_wallet = bool(exec_cfg.get("hl_mirror_wallet", False))
    hl_wallet_enabled = False
    if bool(exec_cfg.get("hl_wallet_enabled", False)):
        print("Config note: hl_wallet_enabled is deprecated; unified Hyperliquid venue is used.")
    default_venue_perps = normalize_venue(exec_cfg.get("default_venue_perps", "hyperliquid"))
    default_venue_hip3 = normalize_venue(exec_cfg.get("default_venue_hip3", "hip3"))
    perps_venues = exec_cfg.get("perps_venues", []) or []

    resolved_db_path = str(exec_cfg.get("db_path") or cfg.get("db_path") or EXECUTOR_DB_PATH)

    return ExecutionConfig(
        lighter_enabled=lighter_enabled,
        hl_enabled=hl_enabled,
        hl_wallet_enabled=hl_wallet_enabled,
        hl_mirror_wallet=hl_mirror_wallet,
        default_venue_perps=default_venue_perps,
        default_venue_hip3=default_venue_hip3,
        perps_venues=perps_venues,
        base_position_size_usd=float(base_position_size_usd),
        max_position_per_symbol_usd=float(exec_cfg.get('max_position_per_symbol_usd', cfg.get('max_position_usd', 0.0))),
        max_total_exposure_usd=float(exec_cfg.get('max_total_exposure_usd', cfg.get('max_total_exposure_usd', 100000000.0))),
        enable_sltp_backstop=bool(exec_cfg.get('enable_sltp_backstop', cfg.get('enable_sltp_backstop', True))),
        adaptive_on_override=bool(exec_cfg.get('adaptive_on_override', cfg.get('adaptive_on_override', True))),
        sl_atr_multiplier=float(exec_cfg.get('sl_atr_multiplier', cfg.get('sl_atr_multiplier', 2.0))),
        tp_atr_multiplier=float(exec_cfg.get('tp_atr_multiplier', cfg.get('tp_atr_multiplier', 3.0))),
        sl_fallback_pct=float(exec_cfg.get('sl_fallback_pct', cfg.get('sl_fallback_pct', 2.0))),
        sltp_delay_seconds=float(exec_cfg.get('sltp_delay_seconds', cfg.get('sltp_delay_seconds', 1.5))),
        rate_limit_tokens=EXECUTOR_RATE_LIMIT_TOKENS,
        rate_limit_window_seconds=EXECUTOR_RATE_LIMIT_WINDOW_SECONDS,
        dry_run=bool(dry_run),
        memory_dir=MEMORY_DIR,
        router_overrides=router_overrides,
        db_path=resolved_db_path,
        use_db_positions=EXECUTOR_USE_DB_POSITIONS,
        write_positions_yaml=EXECUTOR_WRITE_POSITIONS_YAML,
        enable_trade_journal=EXECUTOR_ENABLE_TRADE_JOURNAL,
        enable_trade_tracker=EXECUTOR_ENABLE_TRADE_TRACKER,
        use_fill_reconciler=EXECUTOR_USE_FILL_RECONCILER,
        dust_notional_usd=float(exec_cfg.get('dust_notional_usd', cfg.get('dust_notional_usd', 10.0))),
        min_position_notional_usd=float(exec_cfg.get('min_position_notional_usd', cfg.get('min_position_notional_usd', 20.0))),
        min_entry_fill_ratio=float(exec_cfg.get('min_entry_fill_ratio', cfg.get('min_entry_fill_ratio', 0.05))),
        min_entry_fill_notional_usd=float(exec_cfg.get('min_entry_fill_notional_usd', cfg.get('min_entry_fill_notional_usd', 250.0))),
        chase_entry_tif=str(exec_cfg.get('chase_entry_tif', cfg.get('chase_entry_tif', 'Alo'))),
        chase_exit_tif=str(exec_cfg.get('chase_exit_tif', cfg.get('chase_exit_tif', 'Alo'))),
        chase_entry_fallback_after_seconds=float(exec_cfg.get('chase_entry_fallback_after_seconds', cfg.get('chase_entry_fallback_after_seconds', 180.0))),
        chase_entry_fallback_tif=str(exec_cfg.get('chase_entry_fallback_tif', cfg.get('chase_entry_fallback_tif', 'Gtc'))),
        chase_exit_fallback_after_seconds=float(exec_cfg.get('chase_exit_fallback_after_seconds', cfg.get('chase_exit_fallback_after_seconds', 60.0))),
        chase_exit_fallback_tif=str(exec_cfg.get('chase_exit_fallback_tif', cfg.get('chase_exit_fallback_tif', 'Gtc'))),

        # SR-anchored limit entries (v0)
        sr_limit_enabled=bool(exec_cfg.get('sr_limit_enabled', cfg.get('sr_limit_enabled', True))),
        sr_limit_max_pending=int(exec_cfg.get('sr_limit_max_pending', cfg.get('sr_limit_max_pending', 20))),
        sr_limit_max_notional_pct=float(exec_cfg.get('sr_limit_max_notional_pct', cfg.get('sr_limit_max_notional_pct', 200.0))),
        sr_limit_timeout_minutes=int(exec_cfg.get('sr_limit_timeout_minutes', cfg.get('sr_limit_timeout_minutes', 60))),
        sr_limit_min_distance_atr=float(exec_cfg.get('sr_limit_min_distance_atr', cfg.get('sr_limit_min_distance_atr', 0.3))),
        sr_limit_max_distance_atr=float(exec_cfg.get('sr_limit_max_distance_atr', cfg.get('sr_limit_max_distance_atr', 1.0))),
        sr_limit_min_strength_z=float(exec_cfg.get('sr_limit_min_strength_z', cfg.get('sr_limit_min_strength_z', 0.5))),
        sr_limit_chase_conviction_threshold=float(exec_cfg.get('sr_limit_chase_conviction_threshold', cfg.get('sr_limit_chase_conviction_threshold', 0.5))),
        sr_limit_fallback_chase_min_conviction=float(exec_cfg.get('sr_limit_fallback_chase_min_conviction', cfg.get('sr_limit_fallback_chase_min_conviction', 0.3))),
        sr_limit_reachable_atr_per_hour=float(exec_cfg.get('sr_limit_reachable_atr_per_hour', cfg.get('sr_limit_reachable_atr_per_hour', 2.0))),
        sr_limit_prefill_sl_hl=bool(exec_cfg.get('sr_limit_prefill_sl_hl', cfg.get('sr_limit_prefill_sl_hl', True))),
    )


def get_db(config: Dict[str, Any]) -> Optional[AITraderDB]:
    """Get AITraderDB instance from config (if configured)."""
    cfg = config.get("config", {}) if isinstance(config, dict) else {}
    exec_cfg = cfg.get("executor", {}) if isinstance(cfg, dict) else {}
    db_path = str(exec_cfg.get("db_path") or cfg.get("db_path") or EXECUTOR_DB_PATH)
    return AITraderDB(db_path)


def derive_prices_from_symbol_data(symbol_data: Dict[str, Any]) -> Tuple[float, float, float]:
    """Derive price, best_bid, best_ask with explicit fallbacks."""
    if not symbol_data:
        return 0.0, 0.0, 0.0

    price = float(symbol_data.get('price') or 0.0)
    best_bid = float(symbol_data.get('best_bid') or 0.0)
    best_ask = float(symbol_data.get('best_ask') or 0.0)

    if best_bid <= 0:
        best_bid = float(symbol_data.get('bid') or 0.0)
    if best_ask <= 0:
        best_ask = float(symbol_data.get('ask') or 0.0)

    if price <= 0 and best_bid > 0 and best_ask > 0:
        price = (best_bid + best_ask) / 2

    if price > 0:
        if best_bid <= 0:
            best_bid = price * 0.999
        if best_ask <= 0:
            best_ask = price * 1.001

    if best_bid >= best_ask and price > 0:
        spread = price * 0.001
        best_bid = price - spread
        best_ask = price + spread

    return float(price), float(best_bid), float(best_ask)


def derive_atr_from_symbol_data(symbol_data: Dict[str, Any], price: float) -> Optional[float]:
    """Derive ATR (absolute) from symbol data.

    Supports both snapshot schemas:
    - legacy: symbol_data['atr_pct']
    - current: symbol_data['atr'] is a dict containing 'atr_pct' and/or 'atr_abs'
    """
    if not symbol_data:
        return None

    # 1) Legacy top-level atr_pct
    atr_pct = symbol_data.get('atr_pct')

    # 2) Nested atr dict (preferred)
    atr_obj = symbol_data.get('atr')
    if atr_pct is None and isinstance(atr_obj, dict):
        atr_pct = atr_obj.get('atr_pct')

    # Try pct first
    try:
        atr_pct_f = float(atr_pct) if atr_pct is not None else 0.0
    except (TypeError, ValueError):
        atr_pct_f = 0.0

    if atr_pct_f > 0 and price > 0:
        return float(price) * (atr_pct_f / 100.0)

    # Fallback to absolute ATR if provided
    if isinstance(atr_obj, dict):
        atr_abs = atr_obj.get('atr_abs')
        try:
            atr_abs_f = float(atr_abs) if atr_abs is not None else 0.0
        except (TypeError, ValueError):
            atr_abs_f = 0.0
        if atr_abs_f > 0:
            return atr_abs_f

    return None


def load_cycle_file(path: Path) -> Dict[str, Any]:
    """Load cycle JSON file."""
    with open(path, 'r') as f:
        return json.load(f)


def find_symbol_data(symbols: Dict[str, Any], symbol: str) -> Optional[Dict[str, Any]]:
    """Find symbol data in cycle symbols dict with case-insensitive lookup."""
    if not symbols:
        return None
    if symbol in symbols:
        return symbols.get(symbol)
    symbol_upper = symbol.upper()
    if symbol_upper in symbols:
        return symbols.get(symbol_upper)
    for key, value in symbols.items():
        if str(key).upper() == symbol_upper:
            return value
    return None




def load_memory_file(filename: str) -> dict:
    """Load data from memory file."""
    path = MEMORY_DIR / filename
    if path.exists():
        with open(path, 'r') as f:
            return yaml.safe_load(f) or {}
    return {}


# ============================================================================
# /execute command
# ============================================================================

async def cmd_execute(args):
    """Execute a cycle decision from a cycle file."""
    symbol = args.symbol.upper()
    direction = args.direction.upper()
    size_usd = float(args.size_usd)

    if direction not in ('LONG', 'SHORT'):
        print(f"Error: direction must be LONG or SHORT, got {direction}")
        return 1

    if size_usd <= 0:
        print(f"Error: size_usd must be > 0, got {size_usd}")
        return 1

    cycle_path = Path(args.cycle_file)
    if not cycle_path.exists():
        print(f"Error: cycle file not found: {cycle_path}")
        return 1

    try:
        with open(cycle_path, 'r') as f:
            cycle_data = json.load(f)
    except Exception as e:
        print(f"Error: failed to load cycle file: {e}")
        return 1

    symbols = cycle_data.get('symbols', {})
    symbol_data = symbols.get(symbol)
    if symbol_data is None:
        for key, value in symbols.items():
            if str(key).upper() == symbol:
                symbol_data = value
                break

    if symbol_data is None:
        print(f"Error: symbol {symbol} not found in cycle file")
        return 1

    price, best_bid, best_ask = derive_prices_from_symbol_data(symbol_data)
    if best_bid <= 0 or best_ask <= 0:
        print(f"Error: unable to derive bid/ask for {symbol}")
        return 1

    if best_bid >= best_ask and price > 0:
        spread = price * 0.001
        best_bid = price - spread
        best_ask = price + spread

    config = load_config()
    exec_config = build_execution_config(config, dry_run=args.dry_run)

    atr_service = get_atr_service(str(exec_config.db_path or (SKILL_DIR / "ai_trader.db")))
    atr_result = await atr_service.get_atr(symbol, price=price)
    atr = atr_result.atr if atr_result else derive_atr_from_symbol_data(symbol_data, price)

    seq = cycle_data.get('sequence', 'unknown')
    decision = ExecutionDecision(
        symbol=symbol,
        direction=direction,
        size_multiplier=1.0,
        signals_agreeing=[f"cycle_{seq}"],
        conviction=None,
        reason=f"cycle={seq}",
    )
    decision.size_usd = size_usd
    if isinstance(symbol_data, dict):
        sig_snap = symbol_data.get("perp_signals")
        if not isinstance(sig_snap, dict):
            sig_snap = symbol_data.get("signals")
        if isinstance(sig_snap, dict):
            decision.signals_snapshot = sig_snap
        decision.context_snapshot = {
            "manual_execute": True,
            "cycle_seq": seq,
            "symbol": symbol,
            "price": price,
        }

    executor = Executor(config=exec_config)
    try:
        await executor.initialize()

        venue = normalize_venue(args.venue)

        result = await executor.execute(
            decision=decision,
            best_bid=best_bid,
            best_ask=best_ask,
            atr=atr,
            venue=venue,
        )

        if result.success:
            print("Execution summary:")
            print(f"  Symbol: {result.symbol}")
            print(f"  Side: {result.direction}")
            print(f"  Size USD: {size_usd:.2f}")
            print(f"  Bid/Ask: {best_bid:.4f} / {best_ask:.4f}")
            print(f"  Entry: {result.entry_price:.4f}")
            print(f"  Exchange: {result.exchange}")
            print(f"  Venue: {venue}")
            return 0

        print(f"Execution failed: {result.error}")
        return 1
    finally:
        # Close exchange sessions.
        try:
            await executor.close()
        except Exception:
            pass
        # Close aiohttp session used by ATRService singleton (prevents "Unclosed client session").
        try:
            await get_atr_service().close()
        except Exception:
            pass


# ============================================================================
# /guardian command
# ============================================================================

def _normalize_sector_symbol(symbol: str) -> str:
    """Normalize symbol for sector mapping.

    - Most venue-prefixed symbols use the part after ':' (e.g. 'hl:BTC' -> 'BTC')
    - HIP3 uses `xyz:` and MUST keep the prefix so it can map to the HIP3 sector.
    """
    if ":" in symbol:
        pfx, rest = symbol.split(":", 1)
        if pfx.strip().lower() == "xyz":
            return f"xyz:{rest.strip()}"
        return rest
    return symbol


async def cmd_guardian(
    args,
    preloaded_db: Optional[AITraderDB] = None,
    preloaded_executor: Optional[Executor] = None,
):
    """Guardian: validate + execute proposals from SQLite."""
    cycle_path = Path(args.cycle_file)
    if not cycle_path.exists():
        print(f"Error: cycle file not found: {cycle_path}")
        return 1

    try:
        cycle_data = load_cycle_file(cycle_path)
    except Exception as e:
        print(f"Error: failed to load cycle file: {e}")
        return 1

    symbols = cycle_data.get("symbols", {}) or {}
    seq = args.seq

    config = load_config()
    db = preloaded_db or get_db(config)
    if not db:
        print("Error: DB not configured; cannot run guardian")
        return 1

    # Guardian should only validate/execute actionable proposals.
    # We still store BLOCKED proposals (e.g., LLM vetoes) for audit, and those often have size_usd=0.0.
    # Do NOT re-validate them here or overwrite their original reject_reason.
    processor_id = f"guardian:{os.uname().nodename}:{os.getpid()}"

    all_proposals = db.get_proposals_for_cycle(seq)
    if not all_proposals:
        # Ensure cycle_runs does not remain unprocessed/claimed when upstream emits an empty cycle.
        try:
            db.mark_cycle_processed(
                seq,
                status="PROPOSED",
                summary="guardian:no_proposals_found",
                processed_by=processor_id,
                error="no_proposals",
            )
        except Exception:
            pass
        print(f"No proposals found for cycle seq={seq}")
        return 0

    proposals = [
        p for p in all_proposals
        if str(p.get('status') or '').upper() in ('PROPOSED', 'APPROVED')
    ]
    if not proposals:
        # Cycle may contain only BLOCKED/FAILED/EXECUTED proposals; mark processed so cycle_runs doesn't stick.
        try:
            # summarize counts for debugging
            st_counts = {}
            for p in all_proposals:
                st = str(p.get('status') or '').upper() or 'UNKNOWN'
                st_counts[st] = st_counts.get(st, 0) + 1
            summary = "guardian:no_actionable " + ",".join(f"{k}={v}" for k, v in sorted(st_counts.items()))

            db.mark_cycle_processed(
                seq,
                status="PROPOSED",
                summary=summary,
                processed_by=processor_id,
            )
        except Exception:
            pass
        print(f"No actionable proposals to execute for cycle seq={seq}")
        return 0

    own_executor = preloaded_executor is None
    atr_service = None
    if own_executor:
        exec_config = build_execution_config(config, dry_run=args.dry_run)
        if args.dry_run:
            # Avoid live exchange calls in dry-run guardian mode
            exec_config.lighter_enabled = False
            exec_config.hl_enabled = False
            exec_config.hl_wallet_enabled = False

        executor = Executor(config=exec_config)
    else:
        executor = preloaded_executor
        exec_config = executor.config
    assert executor is not None

    # ATR service is always needed (for SL/TP sizing), even when executor is preloaded.
    atr_service = get_atr_service(str(exec_config.db_path or (SKILL_DIR / "ai_trader.db")))
    await atr_service.initialize()

    # Ensure we always close sessions (prevents "Unclosed client session" spam)
    try:
        ok_init = True
        if own_executor:
            ok_init = await executor.initialize()

        # If an exchange failed to initialize, block proposals on that venue with the real reason
        # (instead of mutating config flags and later showing misleading "disabled" messages).
        if not ok_init:
            # Determine which venues were requested by proposals
            venues = {normalize_venue(p.get('venue') or '') for p in proposals}
            # Hyperliquid failure
            if 'hyperliquid' in venues and exec_config.hl_enabled and not getattr(executor.hyperliquid, '_initialized', False):
                err = getattr(executor.hyperliquid, '_last_init_error', None) or 'hyperliquid_init_failed'
                for p in proposals:
                    if normalize_venue(p.get('venue') or '') == 'hyperliquid':
                        pid = p.get('id')
                        if pid is not None:
                            db.update_proposal_status(int(pid), 'BLOCKED', f"Hyperliquid init failed: {err}")
                print(f"Guardian results for seq={seq}: approved=0 blocked={len([p for p in proposals if normalize_venue(p.get('venue') or '')=='hyperliquid'])} executed=0 failed=0")
                return 0
            # Lighter failure
            if 'lighter' in venues and exec_config.lighter_enabled and not getattr(executor.lighter, '_initialized', False):
                err = getattr(executor.lighter, '_last_init_error', None) or 'lighter_init_failed'
                for p in proposals:
                    if normalize_venue(p.get('venue') or '') == 'lighter':
                        pid = p.get('id')
                        if pid is not None:
                            db.update_proposal_status(int(pid), 'BLOCKED', f"Lighter init failed: {err}")
                print(f"Guardian results for seq={seq}: approved=0 blocked={len([p for p in proposals if normalize_venue(p.get('venue') or '')=='lighter'])} executed=0 failed=0")
                return 0

        positions = await executor.get_all_positions()

        dust_notional = float(getattr(exec_config, "dust_notional_usd", 0.0) or 0.0)
        # NOTE: duplicates must be venue-aware because we support Hyperliquid perps + HIP3 wallet
        # running the same symbol/direction in parallel when mirroring is enabled.
        existing_same_dir = {
            (pos.symbol.upper(), pos.direction.upper(), normalize_venue(getattr(pos, 'venue', '') or ''))
            for pos in positions.values()
            if pos.direction
            and pos.direction.upper() in ("LONG", "SHORT")
            and (pos.state or "").upper() != "DUST"
            and ((pos.size or 0.0) * (pos.entry_price or 0.0) >= dust_notional if dust_notional > 0 else True)
        }

        # Hardening: symbol busy guard.
        # Prevent opposite-direction entries while a close/entry is in-flight.
        busy_states = {"ENTERING", "EXITING", "PLACING_SLTP"}
        symbol_busy: Dict[Tuple[str, str], str] = {}
        for pos in positions.values():
            try:
                sym = str(pos.symbol or "").upper()
                ven = normalize_venue(getattr(pos, "venue", "") or "")
                st = str(getattr(pos, "state", "") or "").upper()
            except Exception:
                continue
            if not sym or not ven:
                continue
            if st in busy_states:
                symbol_busy[(sym, ven)] = st

        projected_exposure = executor.get_total_exposure()
        risk_cfg = config.get("config", {}).get("risk", {})
        max_sector = int(risk_cfg.get("max_sector_concentration", 3))

        sector_counts: Dict[str, int] = {}
        for pos in positions.values():
            if pos.direction and pos.direction.upper() in ("LONG", "SHORT"):
                sector = get_sector(_normalize_sector_symbol(pos.symbol))
                sector_counts[sector] = sector_counts.get(sector, 0) + 1

        def _proposal_has_successful_execution(pid: int) -> bool:
            try:
                with db._get_connection() as conn:
                    row = conn.execute(
                        "SELECT success FROM proposal_executions WHERE proposal_id=? ORDER BY id DESC LIMIT 1",
                        (int(pid),),
                    ).fetchone()
                return bool(row) and int(row["success"] or 0) == 1
            except Exception:
                return False

        approved_count = 0
        blocked_count = 0
        executed_count = 0
        failed_count = 0
        execution_work: List[Dict[str, Any]] = []
        guardian_max_concurrency = 2
        sem = asyncio.Semaphore(max(1, guardian_max_concurrency))
        db_lock = asyncio.Lock()
        counter_lock = asyncio.Lock()

        for proposal in proposals:
            proposal_id = proposal.get("id")
            if proposal_id is None:
                continue

            # Idempotency: if this proposal already has a successful execution record,
            # do NOT re-validate and do NOT downgrade it to BLOCKED due to duplicates.
            if _proposal_has_successful_execution(int(proposal_id)):
                db.update_proposal_status(int(proposal_id), "EXECUTED", "executed")
                executed_count += 1
                continue

            symbol = str(proposal.get("symbol", "")).upper()
            venue = normalize_venue(proposal.get("venue", ""))
            direction = str(proposal.get("direction", "")).upper()
            size_usd = float(proposal.get("size_usd") or 0.0)
            sl = proposal.get("sl")
            tp = proposal.get("tp")
            conviction = proposal.get("conviction")
            reason_short = proposal.get("reason_short") or ""
            signals = proposal.get("signals") or []
            proposal_meta: Dict[str, Any] = {}
            try:
                proposal_meta = (
                    db.get_proposal_metadata(int(proposal_id))
                    if hasattr(db, "get_proposal_metadata")
                    else {}
                )
            except Exception:
                proposal_meta = {}
            sig_snapshot = proposal_meta.get("signals_snapshot")
            if not isinstance(sig_snapshot, dict):
                sig_snapshot = {}

            block_reason = None

            if direction not in ("LONG", "SHORT"):
                block_reason = f"Invalid direction: {direction}"
            elif size_usd <= 0:
                # size_usd can be 0 when upstream sizing gates kick in (e.g., exposure limit).
                # Provide a more actionable reason than a raw validation error.
                try:
                    snap = db.get_latest_monitor_snapshot() if hasattr(db, 'get_latest_monitor_snapshot') else None
                except Exception:
                    snap = None

                if isinstance(snap, dict) and snap:
                    hl_long = float(snap.get('hl_long_notional') or 0.0)
                    hl_short = float(snap.get('hl_short_notional') or 0.0)
                    gross = max(0.0, hl_long) + max(0.0, hl_short)
                    max_exp = float(getattr(exec_config, 'max_total_exposure_usd', 0.0) or 0.0)
                    if max_exp > 0 and gross > max_exp:
                        block_reason = (
                            f"Exposure limit reached: gross=${gross:,.0f} > max=${max_exp:,.0f} "
                            "(executor.max_total_exposure_usd)."
                        )
                    else:
                        block_reason = f"Invalid size_usd: {size_usd}"
                else:
                    block_reason = f"Invalid size_usd: {size_usd}"
            elif venue not in ("lighter", "hyperliquid"):
                block_reason = f"Invalid venue: {venue}"

            if not block_reason:
                if venue == "lighter" and not exec_config.lighter_enabled and not args.dry_run:
                    block_reason = "Lighter disabled in config"
                if venue == "hyperliquid" and not exec_config.hl_enabled and not args.dry_run:
                    block_reason = "Hyperliquid disabled in config"

            # If venue is enabled in config but the adapter isn't initialized, block with the real reason.
            if not block_reason and not args.dry_run:
                if venue == 'hyperliquid' and exec_config.hl_enabled and not getattr(executor.hyperliquid, '_initialized', False):
                    err = getattr(executor.hyperliquid, '_last_init_error', None) or 'hyperliquid_init_failed'
                    block_reason = f"Hyperliquid init failed: {err}"
                if venue == 'lighter' and exec_config.lighter_enabled and not getattr(executor.lighter, '_initialized', False):
                    err = getattr(executor.lighter, '_last_init_error', None) or 'lighter_init_failed'
                    block_reason = f"Lighter init failed: {err}"

            if not block_reason:
                try:
                    lock = db.get_symbol_lock(symbol, venue)
                except Exception:
                    lock = None
                if lock:
                    lt = lock.get('lock_type') or ''
                    lr = lock.get('reason') or ''
                    block_reason = f"SYMBOL_BUSY (lock {lt}): {lr}".strip()

            if not block_reason and (symbol, venue) in symbol_busy:
                block_reason = f"SYMBOL_BUSY (state {symbol_busy[(symbol, venue)]})"

            if not block_reason and (symbol, direction, venue) in existing_same_dir:
                block_reason = f"Duplicate {direction} position exists for {symbol} on {venue}"

            # Policy backstop: XYZ trades must be explicitly HIP3_MAIN-driven.
            if not block_reason and symbol.startswith("XYZ:"):
                hip3_main = sig_snapshot.get("hip3_main") if isinstance(sig_snapshot, dict) else None
                if not (isinstance(hip3_main, dict) and hip3_main):
                    block_reason = "hip3_main_required_for_xyz"

            symbol_data = find_symbol_data(symbols, symbol) if symbols else None
            price, best_bid, best_ask = derive_prices_from_symbol_data(symbol_data or {})
            if not block_reason and (best_bid <= 0 or best_ask <= 0):
                # HIP3-triggered cycles may omit bid/ask in the cycle artifact.
                # Best-effort fetch from the venue adapter to avoid blocking execution.
                try:
                    adapter = executor._get_adapter_by_venue(venue)
                    bb, ba = await adapter.get_best_bid_ask(symbol)
                    if bb and ba and float(bb) > 0 and float(ba) > 0:
                        best_bid, best_ask = float(bb), float(ba)
                        if price <= 0:
                            price = (best_bid + best_ask) / 2.0
                except Exception:
                    pass

            if not block_reason and (best_bid <= 0 or best_ask <= 0):
                block_reason = f"No bid/ask data for {symbol} in cycle file"

            if not block_reason and not args.dry_run:
                try:
                    adapter = executor._get_adapter_by_venue(venue)
                    mid = await adapter.get_mid_price(symbol)
                    if mid <= 0:
                        block_reason = f"Symbol {symbol} not available on {venue}"
                except Exception as e:
                    block_reason = f"Symbol validation error: {e}"

            if not block_reason:
                entry_ref = best_ask if direction == "LONG" else best_bid
                if entry_ref <= 0:
                    block_reason = f"Invalid entry price for {symbol}"
                else:
                    sl_val = float(sl) if sl is not None else None
                    tp_val = float(tp) if tp is not None else None
                    if sl_val is not None and tp_val is not None:
                        if direction == "LONG" and not (sl_val < entry_ref < tp_val):
                            block_reason = f"SL/TP invalid for LONG {symbol}"
                        if direction == "SHORT" and not (sl_val > entry_ref > tp_val):
                            block_reason = f"SL/TP invalid for SHORT {symbol}"

            if not block_reason:
                if projected_exposure + size_usd > exec_config.max_total_exposure_usd:
                    block_reason = (
                        f"Exposure cap exceeded ({projected_exposure + size_usd:.0f} > "
                        f"{exec_config.max_total_exposure_usd:.0f})"
                    )

            if not block_reason:
                sector = get_sector(_normalize_sector_symbol(symbol))

                # HIP3 is its own sector and (per Degen directive) should not be capped.
                if sector != "HIP3":
                    sector_count = sector_counts.get(sector, 0)
                    if sector_count >= max_sector:
                        block_reason = f"Sector limit reached for {sector} ({max_sector})"

            if block_reason:
                db.update_proposal_status(int(proposal_id), "BLOCKED", block_reason)
                blocked_count += 1
                continue

            # Store approval rationale for audit (reason_short is the "why approved").
            db.update_proposal_status(int(proposal_id), "APPROVED", reason_short or None)
            approved_count += 1
            projected_exposure += size_usd
            sector = get_sector(_normalize_sector_symbol(symbol))
            sector_counts[sector] = sector_counts.get(sector, 0) + 1
            existing_same_dir.add((symbol, direction, venue))

            execution_work.append({
                "proposal_id": int(proposal_id),
                "symbol": symbol,
                "venue": venue,
                "direction": direction,
                "size_usd": size_usd,
                "sl": sl,
                "tp": tp,
                "conviction": conviction,
                "reason_short": reason_short,
                "signals": signals,
                "best_bid": best_bid,
                "best_ask": best_ask,
                "price": price,
                "symbol_data": symbol_data or {},
            })
            continue

        async def _record_execution_result(**kwargs):
            async with db_lock:
                db.insert_execution_result(**kwargs)

        async def _update_status(pid: int, status: str, reason: str) -> None:
            async with db_lock:
                db.update_proposal_status(int(pid), status, reason)

        async def _execute_work(item: Dict[str, Any]) -> None:
            nonlocal executed_count, failed_count, blocked_count
            async with sem:
                proposal_id = int(item["proposal_id"])
                symbol = item["symbol"]
                venue = item["venue"]
                direction = item["direction"]
                size_usd = float(item["size_usd"] or 0.0)
                sl = item.get("sl")
                tp = item.get("tp")
                conviction = item.get("conviction")
                reason_short = item.get("reason_short") or ""
                signals = item.get("signals") or []
                best_bid = float(item.get("best_bid") or 0.0)
                best_ask = float(item.get("best_ask") or 0.0)
                price = float(item.get("price") or 0.0)
                symbol_data = item.get("symbol_data") or {}

                proposal_meta: Dict[str, Any] = {}
                try:
                    proposal_meta = db.get_proposal_metadata(int(proposal_id)) if hasattr(db, 'get_proposal_metadata') else {}
                except Exception:
                    proposal_meta = {}
                proposal_meta = dict(proposal_meta or {})

                # Carry gate linkage into execution context for downstream outcome attribution.
                gate_decision_id = proposal_meta.get("gate_decision_id")
                try:
                    gate_decision_id = int(gate_decision_id) if gate_decision_id is not None else None
                except Exception:
                    gate_decision_id = None
                gate_session_id = str(proposal_meta.get("gate_session_id") or "").strip() or None
                gate_decision_reason = str(proposal_meta.get("gate_decision_reason") or "").strip() or None
                if gate_decision_id is not None:
                    if hasattr(db, "link_gate_decision_to_proposal"):
                        try:
                            db.link_gate_decision_to_proposal(
                                gate_decision_id=int(gate_decision_id),
                                proposal_id=int(proposal_id),
                                venue=str(venue or "").lower() or None,
                            )
                        except Exception:
                            pass
                    context_snapshot = proposal_meta.get("context_snapshot")
                    context_snapshot = dict(context_snapshot) if isinstance(context_snapshot, dict) else {}
                    entry_gate = context_snapshot.get("entry_gate")
                    entry_gate = dict(entry_gate) if isinstance(entry_gate, dict) else {}
                    entry_gate["gate_decision_id"] = int(gate_decision_id)
                    entry_gate["proposal_id"] = int(proposal_id)
                    entry_gate["venue"] = str(venue or "").lower() or None
                    if gate_session_id:
                        entry_gate["gate_session_id"] = gate_session_id
                    if gate_decision_reason:
                        entry_gate["gate_decision_reason"] = gate_decision_reason
                    context_snapshot["entry_gate"] = entry_gate
                    proposal_meta["context_snapshot"] = context_snapshot

                atr_result = await atr_service.get_atr(symbol, price=price)
                atr = atr_result.atr if atr_result else derive_atr_from_symbol_data(symbol_data or {}, price)

                decision = ExecutionDecision(
                    symbol=symbol,
                    direction=direction,
                    size_multiplier=1.0,
                    signals_agreeing=list(signals),
                    conviction=float(conviction) if conviction is not None else None,
                    reason=reason_short or f"cycle={seq}",
                )
                decision.size_usd = size_usd
                if isinstance(proposal_meta.get("risk"), dict):
                    risk_meta = dict(proposal_meta.get("risk") or {})
                    venue_key = str(venue or "").lower()
                    risk_by_venue = risk_meta.get("risk_pct_used_by_venue")
                    equity_by_venue = risk_meta.get("equity_at_entry_by_venue")
                    if isinstance(risk_by_venue, dict):
                        venue_risk_pct = risk_by_venue.get(venue_key)
                        if venue_risk_pct is not None:
                            risk_meta["risk_pct_used"] = venue_risk_pct
                    if isinstance(equity_by_venue, dict):
                        venue_equity = equity_by_venue.get(venue_key)
                        if venue_equity is not None:
                            risk_meta["equity_at_entry"] = venue_equity
                    decision.risk = risk_meta
                    try:
                        effective_mult = float(
                            risk_meta.get("size_multiplier_effective", risk_meta.get("size_multiplier", 1.0))
                        )
                        if effective_mult > 0:
                            decision.size_multiplier = effective_mult
                    except Exception:
                        pass
                if isinstance(proposal_meta.get("signals_snapshot"), dict):
                    decision.signals_snapshot = proposal_meta.get("signals_snapshot")
                if isinstance(proposal_meta.get("context_snapshot"), dict):
                    decision.context_snapshot = proposal_meta.get("context_snapshot")
                if sl is not None and tp is not None:
                    try:
                        decision.sl_price = float(sl)
                        decision.tp_price = float(tp)
                    except (TypeError, ValueError):
                        decision.sl_price = None
                        decision.tp_price = None

                start_ts = time.time()

                if args.dry_run:
                    await _record_execution_result(
                        proposal_id=proposal_id,
                        started_at=start_ts,
                        finished_at=start_ts,
                        success=False,
                        error="dry_run",
                        exchange=None,
                        entry_price=None,
                        size=None,
                        sl_price=None,
                        tp_price=None,
                    )
                    await _update_status(proposal_id, "FAILED", "dry_run")
                    async with counter_lock:
                        failed_count += 1
                    return

                # Optional execution overrides (Opt2): order_type=limit uses SR_LIMIT pending workflow.
                exec_meta: Dict[str, Any] = {}
                try:
                    if isinstance(proposal_meta, dict) and isinstance(proposal_meta.get('execution'), dict):
                        exec_meta = dict(proposal_meta.get('execution') or {})
                except Exception:
                    exec_meta = {}

                order_type, order_type_source = _resolve_effective_order_type(exec_meta, conviction, db=db)
                if order_type_source != "metadata":
                    print(
                        f"Routing fallback for proposal {proposal_id}: "
                        f"source={order_type_source} conviction={conviction} -> order_type={order_type}"
                    )

                if order_type == "reject":
                    await _record_execution_result(
                        proposal_id=proposal_id,
                        started_at=start_ts,
                        finished_at=time.time(),
                        success=False,
                        error='conviction_below_min',
                        exchange=venue,
                        entry_price=None,
                        size=None,
                        sl_price=None,
                        tp_price=None,
                    )
                    await _update_status(proposal_id, 'BLOCKED', f'conviction_below_min ({order_type_source})')
                    async with counter_lock:
                        blocked_count += 1
                    return

                if order_type == 'limit':
                    pending_mgr = getattr(executor, 'pending_mgr', None)
                    if not pending_mgr or not getattr(pending_mgr, 'enabled', False):
                        await _record_execution_result(
                            proposal_id=proposal_id,
                            started_at=start_ts,
                            finished_at=time.time(),
                            success=False,
                            error='sr_limit_disabled',
                            exchange=venue,
                            entry_price=None,
                            size=None,
                            sl_price=None,
                            tp_price=None,
                        )
                        await _update_status(proposal_id, 'FAILED', 'sr_limit_disabled')
                        async with counter_lock:
                            failed_count += 1
                        return

                    adapter = executor._get_adapter_by_venue(venue)
                    if not adapter:
                        await _record_execution_result(
                            proposal_id=proposal_id,
                            started_at=start_ts,
                            finished_at=time.time(),
                            success=False,
                            error='no_adapter_for_venue',
                            exchange=venue,
                            entry_price=None,
                            size=None,
                            sl_price=None,
                            tp_price=None,
                        )
                        await _update_status(proposal_id, 'FAILED', 'no_adapter_for_venue')
                        async with counter_lock:
                            failed_count += 1
                        return

                    # Compute SR level if possible from proposal metadata context; else fallback to 1x ATR from current price.
                    sr_level = None
                    cur_price = (best_bid + best_ask) / 2.0 if best_bid > 0 and best_ask > 0 else 0.0
                    if cur_price <= 0:
                        try:
                            cur_price = float(await adapter.get_mid_price(symbol))
                        except Exception:
                            cur_price = 0.0

                    key_metrics = {}
                    try:
                        ctx = proposal_meta.get('context_snapshot') if isinstance(proposal_meta, dict) else None
                        if isinstance(ctx, dict):
                            key_metrics = ctx.get('key_metrics') or {}
                    except Exception:
                        key_metrics = {}

                    # Try SR levels first
                    if cur_price > 0:
                        try:
                            from candidate_pipeline import _find_sr_for_limit

                            # Realism cap: if SR is too far given timeout, treat SR-limit as not viable.
                            timeout_min = float(pending_mgr.cfg.get('sr_limit_timeout_minutes', 30) or 30)
                            atr_per_hour = float(pending_mgr.cfg.get('sr_limit_reachable_atr_per_hour', 2.0) or 2.0)
                            reachable_atr = max(0.0, (timeout_min / 60.0) * atr_per_hour)
                            eff_max_dist_atr = min(
                                float(pending_mgr.cfg.get('sr_limit_max_distance_atr', 1.0) or 1.0),
                                reachable_atr if reachable_atr > 0 else 1.0,
                            )

                            atr_pct = 0.0
                            try:
                                atr_pct = float(key_metrics.get('atr_pct', 0) or 0.0)
                            except Exception:
                                atr_pct = 0.0
                            if atr_pct <= 0 and atr and cur_price > 0:
                                atr_pct = (float(abs(atr)) / float(cur_price)) * 100.0

                            sr_level = _find_sr_for_limit(
                                key_metrics=key_metrics,
                                direction=direction,
                                current_price=float(cur_price),
                                atr_pct=float(atr_pct),
                                min_distance_atr=pending_mgr.cfg.get('sr_limit_min_distance_atr', 0.3),
                                max_distance_atr=eff_max_dist_atr,
                                min_strength_z=pending_mgr.cfg.get('sr_limit_min_strength_z', 0.5),
                            )
                        except Exception:
                            sr_level = None

                    # Fallback: 1x ATR from current price
                    if not sr_level:
                        try:
                            atr_abs = float(abs(atr or 0.0))
                        except Exception:
                            atr_abs = 0.0
                        if cur_price > 0 and atr_abs > 0:
                            sr_level = (cur_price - atr_abs) if direction == 'LONG' else (cur_price + atr_abs)

                    if not sr_level or float(sr_level) <= 0:
                        await _record_execution_result(
                            proposal_id=proposal_id,
                            started_at=start_ts,
                            finished_at=time.time(),
                            success=False,
                            error='limit_requested_but_no_sr_or_atr',
                            exchange=venue,
                            entry_price=None,
                            size=None,
                            sl_price=None,
                            tp_price=None,
                        )
                        await _update_status(proposal_id, 'FAILED', 'limit_requested_but_no_sr_or_atr')
                        async with counter_lock:
                            failed_count += 1
                        return

                    # Pending limit sizing: convert USD notional into base size at the limit price.
                    base_size = float(size_usd) / float(sr_level)

                    # Equity for pending-notional cap (best-effort)
                    equity = 0.0
                    try:
                        snap = db.get_latest_monitor_snapshot() if hasattr(db, 'get_latest_monitor_snapshot') else None
                        if isinstance(snap, dict) and snap:
                            if venue == 'hyperliquid':
                                equity = float(snap.get('hl_equity') or 0.0)
                                if equity <= 0:
                                    equity = float(snap.get('hl_wallet_equity') or 0.0)
                            elif venue == 'lighter':
                                equity = float(snap.get('lighter_equity') or 0.0)
                            if equity <= 0:
                                equity = float(snap.get('total_equity') or 0.0)
                    except Exception:
                        equity = 0.0

                    equity_source = None
                    if equity <= 0:
                        # Safety: if latest snapshot is missing, try the previous snapshot.
                        try:
                            with db._get_connection() as conn:
                                rows = conn.execute(
                                    """
                                    SELECT ts_iso, total_equity, hl_equity, hl_wallet_equity, lighter_equity
                                    FROM monitor_snapshots
                                    ORDER BY ts DESC
                                    LIMIT 2
                                    """
                                ).fetchall()
                            for r in rows:
                                if venue == 'hyperliquid':
                                    eq_try = float(r['hl_equity'] or 0.0)
                                    if eq_try <= 0:
                                        eq_try = float(r['hl_wallet_equity'] or 0.0)
                                elif venue == 'lighter':
                                    eq_try = float(r['lighter_equity'] or 0.0)
                                else:
                                    eq_try = float(r['total_equity'] or 0.0)
                                if eq_try > 0:
                                    equity = eq_try
                                    equity_source = str(r['ts_iso'] or 'snapshot')
                                    break
                        except Exception:
                            pass

                    if equity <= 0:
                        # Fail-closed: without equity we cannot safely enforce pending caps.
                        await _record_execution_result(
                            proposal_id=proposal_id,
                            started_at=start_ts,
                            finished_at=time.time(),
                            success=False,
                            error='sr_limit_equity_missing',
                            exchange=venue,
                            entry_price=float(sr_level),
                            size=float(base_size),
                            sl_price=None,
                            tp_price=None,
                        )
                        await _update_status(proposal_id, 'BLOCKED', 'sr_limit_equity_missing')
                        async with counter_lock:
                            blocked_count += 1
                        return

                    if not pending_mgr.can_place(symbol, venue, float(size_usd), float(equity), db=db):
                        await _record_execution_result(
                            proposal_id=proposal_id,
                            started_at=start_ts,
                            finished_at=time.time(),
                            success=False,
                            error='sr_limit_pending_cap_reached',
                            exchange=venue,
                            entry_price=float(sr_level),
                            size=float(base_size),
                            sl_price=None,
                            tp_price=None,
                        )
                        await _update_status(proposal_id, 'FAILED', 'sr_limit_pending_cap_reached')
                        async with counter_lock:
                            failed_count += 1
                        return

                    # Used for logging, if equity_source is known (so we can debug unexpected caps).
                    if equity_source:
                        pending_mgr.log.info(
                            f"SR limit pending using equity {equity:.2f} from {equity_source} (venue={venue})"
                        )

                    # Safety: enforce a minimum size before placing the limit order.
                    if float(base_size) <= 0:
                        await _record_execution_result(
                            proposal_id=proposal_id,
                            started_at=start_ts,
                            finished_at=time.time(),
                            success=False,
                            error='sr_limit_bad_size',
                            exchange=venue,
                            entry_price=float(sr_level),
                            size=float(base_size),
                            sl_price=None,
                            tp_price=None,
                        )
                        await _update_status(proposal_id, 'FAILED', 'sr_limit_bad_size')
                        async with counter_lock:
                            failed_count += 1
                        return

                    # Cancel any lingering pending orders for this symbol before placing new ones.
                    await pending_mgr.cancel_existing(symbol, venue, adapter, db=db)

                    pending_context: Dict[str, Any] = {}
                    if isinstance(proposal_meta, dict) and isinstance(proposal_meta.get("context_snapshot"), dict):
                        pending_context = dict(proposal_meta.get("context_snapshot") or {})
                    pending_risk: Dict[str, Any] = {}
                    if isinstance(proposal_meta, dict) and isinstance(proposal_meta.get("risk"), dict):
                        pending_risk = dict(proposal_meta.get("risk") or {})
                        risk_by_venue = pending_risk.get("risk_pct_used_by_venue")
                        equity_by_venue = pending_risk.get("equity_at_entry_by_venue")
                        if isinstance(risk_by_venue, dict):
                            rv = risk_by_venue.get(str(venue or "").lower())
                            if rv is not None:
                                pending_risk["risk_pct_used"] = rv
                        if isinstance(equity_by_venue, dict):
                            ev = equity_by_venue.get(str(venue or "").lower())
                            if ev is not None:
                                pending_risk["equity_at_entry"] = ev
                    if pending_risk:
                        pending_context["risk"] = pending_risk

                    ok_place = False
                    try:
                        ok_place = await pending_mgr.place(
                            symbol,
                            direction,
                            float(sr_level),
                            float(base_size),
                            adapter,
                            venue,
                            db,
                            # Extra context for journaling + correct cap enforcement
                            signals_agreed=json.dumps(list(signals) if isinstance(signals, (list, tuple)) else []),
                            context_snapshot=json.dumps(pending_context) if pending_context else None,
                            conviction=float(conviction) if conviction is not None else None,
                            reason=str(reason_short or ''),
                            notional=float(size_usd),
                            equity=float(equity),
                        )
                    except Exception:
                        ok_place = False

                    if not ok_place:
                        await _record_execution_result(
                            proposal_id=proposal_id,
                            started_at=start_ts,
                            finished_at=time.time(),
                            success=False,
                            error='sr_limit_place_failed',
                            exchange=venue,
                            entry_price=float(sr_level),
                            size=float(base_size),
                            sl_price=None,
                            tp_price=None,
                        )
                        await _update_status(proposal_id, 'FAILED', 'sr_limit_place_failed')
                        async with counter_lock:
                            failed_count += 1
                        return

                    await _record_execution_result(
                        proposal_id=proposal_id,
                        started_at=start_ts,
                        finished_at=time.time(),
                        success=True,
                        error=None,
                        exchange=venue,
                        entry_price=float(sr_level),
                        size=float(base_size),
                        sl_price=None,
                        tp_price=None,
                    )
                    await _update_status(
                        proposal_id,
                        'EXECUTED',
                        f"sr_limit_pending @ {float(sr_level):.6g} (fallback=atr_1x if no_sr; route={order_type_source})",
                    )
                    async with counter_lock:
                        executed_count += 1
                    return

                result = await executor.execute(
                    decision=decision,
                    best_bid=best_bid,
                    best_ask=best_ask,
                    atr=atr,
                    venue=venue,
                )

                finished_ts = time.time()
                await _record_execution_result(
                    proposal_id=proposal_id,
                    started_at=start_ts,
                    finished_at=finished_ts,
                    success=bool(result.success),
                    error=result.error or None,
                    exchange=result.exchange or None,
                    entry_price=result.entry_price or None,
                    size=result.size or None,
                    sl_price=result.sl_price or None,
                    tp_price=result.tp_price or None,
                )

                if result.success:
                    await _update_status(proposal_id, "EXECUTED", "executed")
                    async with counter_lock:
                        executed_count += 1
                else:
                    await _update_status(proposal_id, "FAILED", result.error)
                    async with counter_lock:
                        failed_count += 1

        await asyncio.gather(*[_execute_work(item) for item in execution_work])

        print(
            f"Guardian results for seq={seq}: "
            f"approved={approved_count} blocked={blocked_count} "
            f"executed={executed_count} failed={failed_count}"
        )
        return 0

    finally:
        if atr_service is not None:
            try:
                await atr_service.close()
            except Exception:
                pass
        if own_executor:
            try:
                await executor.close()
            except Exception:
                pass

# ============================================================================
# /trade command
# ============================================================================

async def _fetch_current_prices(executor, symbol: str, venue: Optional[str]) -> Tuple[float, float]:
    """Fetch current bid/ask from the exchange.

    Safety note: closes must use *live* prices. Prefer the adapter's l2Book
    best-bid/ask when available; fall back to mid-price discovery only if needed.
    """
    try:
        venue = normalize_venue(venue) if venue else None
        adapter = None
        if venue in ('hyperliquid', 'hip3'):
            # Canonical venue normalization maps builder/legacy aliases (including `hip3`)
            # to `hyperliquid` adapters.
            adapter = getattr(executor, 'hyperliquid', None) or getattr(executor, '_hl_adapter', None)
        elif venue == 'lighter':
            adapter = getattr(executor, 'lighter', None) or getattr(executor, '_lighter_adapter', None)
        elif hasattr(executor, 'router'):
            try:
                adapter = executor.router.select(symbol)
            except Exception:
                adapter = None

        if adapter is None:
            return 0.0, 0.0

        # Prefer true bid/ask via adapter (HL l2Book, etc.)
        if hasattr(adapter, 'get_best_bid_ask'):
            try:
                bid, ask = await adapter.get_best_bid_ask(symbol)
                if float(bid or 0) > 0 and float(ask or 0) > 0:
                    return float(bid), float(ask)
            except Exception:
                pass

        # Fallback: HL Info.all_mids() for price discovery
        info = getattr(adapter, '_info', None)
        if info is not None:
            hl_symbol = symbol.split(':', 1)[1] if ':' in symbol else symbol
            dex = symbol.split(':', 1)[0] if ':' in symbol else ""
            try:
                mids = info.all_mids(dex)
                mid = float(mids.get(hl_symbol, 0))
                if mid > 0:
                    spread = mid * 0.001  # 0.1% synthetic spread
                    return mid - spread, mid + spread
            except Exception:
                pass

        # Fallback: Lighter market last_trade_price
        if hasattr(adapter, '_lookup_market'):
            market = adapter._lookup_market(symbol)
            if market and market.last_trade_price > 0:
                mid = market.last_trade_price
                spread = mid * 0.001
                return mid - spread, mid + spread

        return 0.0, 0.0
    except Exception:
        return 0.0, 0.0


async def cmd_trade(args):
    """Execute a manual trade."""
    symbol = args.symbol.upper()
    direction = args.direction.upper()
    size_usd = float(args.size_usd)

    if direction not in ('LONG', 'SHORT'):
        print(f"Error: direction must be LONG or SHORT, got {direction}")
        return 1

    if size_usd <= 0:
        print(f"Error: size_usd must be > 0, got {size_usd}")
        return 1

    # Load config
    config = load_config()

    # Create execution decision for manual trade
    decision = ExecutionDecision(
        symbol=symbol,
        direction=direction,
        size_multiplier=1.0,
        size_usd=size_usd,
        signals_agreeing=['manual'],
        conviction=1.0,
        reason="Manual trade via CLI",
    )

    # Check if dry run
    dry_run = args.dry_run if hasattr(args, 'dry_run') else False

    # Create executor
    exec_config = build_execution_config(config, dry_run=dry_run)
    executor = Executor(config=exec_config)
    try:
        await executor.initialize()

        venue = normalize_venue(args.venue) if hasattr(args, 'venue') else None

        print(f"Executing {direction} {symbol} with ${size_usd:.2f} notional...")
        if dry_run:
            print("[DRY RUN - no real orders]")

        # M6: Fetch real prices from the exchange
        best_bid, best_ask = await _fetch_current_prices(executor, symbol, venue)
        if best_bid <= 0 or best_ask <= 0:
            print(f"Error: Could not fetch current prices for {symbol}.")
            print("Use 'execute' command with a cycle file for price data.")
            return 1

        result = await executor.execute(
            decision=decision,
            best_bid=best_bid,
            best_ask=best_ask,
            atr=None,  # Use fallback SL/TP
            venue=venue,
        )

        if result.success:
            print(f"\n✓ Trade executed successfully!")
            print(f"  Symbol: {result.symbol}")
            print(f"  Direction: {result.direction}")
            print(f"  Entry Price: ${result.entry_price:.4f}")
            print(f"  Size: {result.size:.6f}")
            print(f"  SL Price: ${result.sl_price:.4f}")
            print(f"  TP Price: ${result.tp_price:.4f}")
            print(f"  Exchange: {result.exchange}")
            return 0

        print(f"\n✗ Trade failed: {result.error}")
        return 1
    finally:
        await executor.close()
        try:
            await get_atr_service().close()
        except Exception:
            pass


# ============================================================================
# /signals command
# ============================================================================

async def cmd_signals(args):
    """View current signals."""
    from sse_consumer import TrackerSSEClient

    symbol_filter = args.symbol.upper() if args.symbol else None
    min_z = args.min_z
    top_n = args.top

    print(f"Fetching signals (min_z={min_z}, top={top_n})...")
    if symbol_filter:
        print(f"Filtering for: {symbol_filter}")

    # Connect to SSE and get one snapshot
    received = []
    done = asyncio.Event()

    def on_data(symbols):
        received.append(symbols)
        done.set()

    client = TrackerSSEClient(on_data=on_data)

    try:
        connect_task = asyncio.create_task(client.connect())

        # Wait for first data (timeout 10s)
        try:
            await asyncio.wait_for(done.wait(), timeout=10.0)
        except asyncio.TimeoutError:
            print("Error: Timeout waiting for SSE data")
            return 1

        if not received:
            print("Error: No data received")
            return 1

        symbols = received[0]
        print(f"\nReceived {len(symbols)} symbols\n")

        # Get actionable signals
        actionable = get_actionable_signals(symbols, min_z_score=min_z)

        # Filter by symbol if specified
        if symbol_filter:
            actionable = [s for s in actionable if s.symbol == symbol_filter]

        # Show top N
        actionable = actionable[:top_n]

        if not actionable:
            print("No actionable signals found.")
            return 0

        print(f"{'Symbol':<12} {'Signal':<12} {'Direction':<8} {'Z-Score':>8} {'Confidence':<10}")
        print("-" * 60)

        for sig in actionable:
            print(f"{sig.symbol:<12} {sig.signal_type:<12} {sig.direction:<8} {sig.z_score:>8.2f} {sig.confidence:<10}")

        return 0

    finally:
        await client.disconnect()


# ============================================================================
# /positions command
# ============================================================================

async def cmd_positions(args):
    """View and manage positions."""
    config = load_config()
    use_db_positions = EXECUTOR_USE_DB_POSITIONS
    db = get_db(config)

    positions_data: Dict[str, Any] = {}
    positions_source = "yaml"
    db_loaded = False

    if use_db_positions and db:
        try:
            open_trades = db.get_open_trades()
            positions_source = "db"
            db_loaded = True
            for trade in open_trades:
                key = f"{trade.venue}:{trade.symbol}" if trade.venue else trade.symbol
                positions_data[key] = {
                    'symbol': trade.symbol,
                    'direction': trade.direction,
                    'size': trade.size,
                    'entry_price': trade.entry_price,
                    'sl_price': trade.sl_price or 0.0,
                    'tp_price': trade.tp_price or 0.0,
                    'state': getattr(trade, "state", "ACTIVE") or "ACTIVE",
                    'venue': getattr(trade, "venue", ""),
                    'trade_id': trade.id,
                }
        except Exception as e:
            print(f"Warning: Failed to load positions from DB ({e}); falling back to positions.yaml")

    if not positions_data and not db_loaded:
        positions_data = load_memory_file('positions.yaml')
        positions_source = "yaml"

    if args.close:
        # Close position
        symbol = args.close.upper()
        dry_run = args.dry_run if hasattr(args, 'dry_run') else False

        exec_config = build_execution_config(config, dry_run=dry_run)
        executor = Executor(config=exec_config)

        try:
            await executor.initialize()

            venue = normalize_venue(args.venue) if hasattr(args, 'venue') else None
            pos = await executor.get_position(symbol, venue=venue)
            if not pos:
                print(f"Error: No open position for {symbol} (or multiple venues; specify --venue)")
                return 1

            print(f"Closing {symbol} position...")
            if dry_run:
                print("[DRY RUN - no real orders]")

            if not venue and pos:
                venue = normalize_venue(pos.venue)

            best_bid, best_ask = await _fetch_current_prices(executor, symbol, venue)
            if best_bid <= 0 or best_ask <= 0:
                print(
                    f"Error: Could not fetch live prices for {symbol}; refusing unsafe close "
                    "(no hardcoded fallback)."
                )
                return 1

            close_reason = args.reason or "manual_cli"
            close_detail = (args.detail or "").strip()

            success = await executor.close_position(
                symbol=symbol,
                reason=close_reason,
                best_bid=best_bid,
                best_ask=best_ask,
                venue=venue,
            )

            # Explicit decay decision audit trail for manual closes.
            if success and str(close_reason).upper() in {"DECAY_EXIT", "MAX_HOLD"}:
                try:
                    if db:
                        # trade_id may already be closed; still record with NULL if unknown.
                        db.record_decay_decision(
                            symbol=symbol,
                            venue=venue,
                            trade_id=None,
                            action="CLOSE",
                            reason=str(close_reason).upper(),
                            detail=close_detail,
                            decided_by=None,
                            source="manual_cli",
                        )
                except Exception:
                    pass

            if success:
                print(f"✓ Position closed for {symbol}")
                return 0
            else:
                print(f"✗ Failed to close position for {symbol}")
                return 1
        finally:
            await executor.close()
            # Close aiohttp session used by ATRService singleton (prevents "Unclosed client session").
            try:
                await get_atr_service().close()
            except Exception:
                pass

    if args.export:
        # Export to JSON
        print(json.dumps(positions_data, indent=2))
        return 0

    # Show positions
    if not positions_data:
        print("\nNo open positions.")
        return 0

    print(f"\n=== Open Positions ({positions_source}) ===\n")
    print(f"{'Symbol':<12} {'Direction':<8} {'Size':>12} {'Entry':>12} {'SL':>10} {'TP':>10} {'State':<10}")
    print("-" * 80)

    for symbol, pos in positions_data.items():
        print(
            f"{symbol:<12} "
            f"{pos.get('direction', 'FLAT'):<8} "
            f"{pos.get('size', 0):.6f}  "
            f"${pos.get('entry_price', 0):.4f}  "
            f"${pos.get('sl_price', 0):.2f}  "
            f"${pos.get('tp_price', 0):.2f}  "
            f"{pos.get('state', 'UNKNOWN'):<10}"
        )

    # Show journal if --all
    if args.all:
        if use_db_positions and db:
            closed_trades = db.get_recent_closed_trades(limit=10)
            if closed_trades:
                print("\n=== Recent Trades (db) ===\n")
                for trade in closed_trades:
                    exit_time = datetime.fromtimestamp(trade.exit_time, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S") if trade.exit_time else ""
                    pnl = trade.realized_pnl or 0.0
                    print(f"  [{exit_time}] EXIT: {trade.symbol} PnL=${pnl:.2f} ({trade.exit_reason})")
        else:
            journal = load_memory_file('trade_journal.yaml')
            trades = journal.get('trades', [])
            if trades:
                print("\n=== Recent Trades ===\n")
                for trade in trades[-10:]:
                    event = trade.get('event', 'unknown')
                    symbol = trade.get('symbol', '?')
                    direction = trade.get('direction', '?')
                    timestamp = trade.get('timestamp', '')[:19]
                    if event == 'entry':
                        price = trade.get('price', 0)
                        print(f"  [{timestamp}] {event.upper()}: {direction} {symbol} @ ${price:.4f}")
                    elif event == 'exit':
                        pnl = trade.get('pnl', 0)
                        reason = trade.get('reason', '?')
                        print(f"  [{timestamp}] {event.upper()}: {symbol} PnL=${pnl:.2f} ({reason})")

    return 0


# ============================================================================
# /reconcile command
# ============================================================================

async def cmd_reconcile(args):
    """Manually reconcile positions from exchange."""
    config = load_config()
    dry_run = args.dry_run if hasattr(args, 'dry_run') else False

    exec_config = build_execution_config(config, dry_run=dry_run)
    executor = Executor(config=exec_config)

    print("Initializing executor and reconciling positions from exchange...")
    if dry_run:
        print("[DRY RUN - will show what would be reconciled but not save]")

    try:
        # Snapshot tracked positions BEFORE initialize() runs its built-in reconcile.
        positions_before = {}
        try:
            if exec_config.use_db_positions and executor.db:
                positions_before = dict(executor._load_positions_from_db())
            elif executor.persistence.enable_positions:
                positions_before = dict(executor.persistence.load_positions())
        except Exception:
            positions_before = {}

        await executor.initialize()

        # Run an explicit reconcile pass against exchange state.
        await executor._reconcile_positions_from_exchange()

        print(f"\nBefore reconciliation: {len(positions_before)} tracked positions")

        positions_after = dict(executor._positions)

        new_positions = {key: pos for key, pos in positions_after.items() if key not in positions_before}
        updated_positions = {
            key: pos
            for key, pos in positions_after.items()
            if key in positions_before
            and (
                positions_before[key].size != pos.size
                or positions_before[key].entry_price != pos.entry_price
            )
        }

        print(f"After reconciliation: {len(positions_after)} tracked positions\n")

        if new_positions:
            print(f"✅ Found {len(new_positions)} NEW untracked positions:")
            for key, pos in new_positions.items():
                print(f"  {key}: {pos.direction} {pos.size:.6f} @ ${pos.entry_price:.4f} ({pos.venue})")
        else:
            print("No new untracked positions found.")

        if updated_positions:
            print(f"\n⚠️  Updated {len(updated_positions)} positions:")
            for key, pos in updated_positions.items():
                old = positions_before[key]
                print(
                    f"  {key}: size {old.size:.6f} -> {pos.size:.6f}, entry ${old.entry_price:.4f} -> ${pos.entry_price:.4f}"
                )

        if not new_positions and not updated_positions:
            print("\n✓ All positions are already in sync.")

        return 0
    finally:
        await executor.close()
        # Close aiohttp session used by ATRService singleton (prevents "Unclosed client session").
        try:
            await get_atr_service().close()
        except Exception:
            pass


# ============================================================================
# Main
# ============================================================================

def main():
    parser = argparse.ArgumentParser(
        description='HyperLighter Trading Skill CLI',
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    subparsers = parser.add_subparsers(dest='command', help='Available commands')

    # /execute command
    exec_parser = subparsers.add_parser('execute', help='Execute a cycle decision')
    exec_parser.add_argument('--cycle-file', required=True, help='Path to cycle file JSON')
    exec_parser.add_argument('--symbol', required=True, help='Trading symbol')
    exec_parser.add_argument('--direction', required=True, help='Trade direction (LONG or SHORT)')
    exec_parser.add_argument('--size-usd', required=True, type=float, help='Position size in USD')
    exec_parser.add_argument('--venue', required=True, choices=[
        'hyperliquid', 'hip3', 'lighter',
        'hl_vault', 'hl_wallet', 'lighter_wallet',
    ],
                              help='Target exchange venue')
    exec_parser.add_argument('--dry-run', action='store_true', help='Dry run (no real orders)')

    # /guardian command
    guardian_parser = subparsers.add_parser('guardian', help='Execute proposals for a cycle')
    guardian_parser.add_argument('--cycle-file', required=True, help='Path to cycle file JSON')
    guardian_parser.add_argument('--seq', required=True, type=int, help='Cycle sequence number')
    guardian_parser.add_argument('--dry-run', action='store_true', help='Dry run (no live exchange calls)')

    # /trade command
    trade_parser = subparsers.add_parser('trade', help='Execute a manual trade')
    trade_parser.add_argument('symbol', help='Trading symbol (e.g., ETH, xyz:NVDA)')
    trade_parser.add_argument('direction', help='Trade direction (LONG or SHORT)')
    trade_parser.add_argument('size_usd', type=float, help='Position notional in USD')
    trade_parser.add_argument('--venue', required=True, choices=[
        'hyperliquid', 'hip3', 'lighter',
        'hl_vault', 'hl_wallet', 'lighter_wallet',
    ],
                              help='Target exchange venue')
    trade_parser.add_argument('--dry-run', action='store_true', help='Dry run (no real orders)')

    # /signals command
    signals_parser = subparsers.add_parser('signals', help='View current signals')
    signals_parser.add_argument('symbol', nargs='?', help='Filter by symbol')
    signals_parser.add_argument('--min-z', type=float, default=2.0, help='Minimum z-score (default: 2.0)')
    signals_parser.add_argument('--top', type=int, default=20, help='Number of signals to show (default: 20)')

    # /positions command
    pos_parser = subparsers.add_parser('positions', help='View/manage positions')
    pos_parser.add_argument('--all', action='store_true', help='Show all positions including closed')
    pos_parser.add_argument('--close', metavar='SYMBOL', help='Close position for symbol')
    pos_parser.add_argument('--venue', choices=[
        'hyperliquid', 'hip3', 'lighter',
        'hl_vault', 'hl_wallet', 'lighter_wallet',
    ],
                             help='Target exchange venue (required with --close)')
    pos_parser.add_argument('--reason', default=None, help='Exit reason override (e.g., DECAY_EXIT)')
    pos_parser.add_argument('--detail', default=None, help='Optional detail/rationale for the close')
    pos_parser.add_argument('--export', action='store_true', help='Export positions to JSON')
    pos_parser.add_argument('--dry-run', action='store_true', help='Dry run (no real orders)')

    # /reconcile command
    reconcile_parser = subparsers.add_parser('reconcile', help='Manually reconcile positions from exchange')
    reconcile_parser.add_argument('--dry-run', action='store_true', help='Dry run (show what would be reconciled)')

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return 1

    # Run command
    if args.command == 'execute':
        return asyncio.run(cmd_execute(args))
    elif args.command == 'guardian':
        return asyncio.run(cmd_guardian(args))
    elif args.command == 'trade':
        return asyncio.run(cmd_trade(args))
    elif args.command == 'signals':
        return asyncio.run(cmd_signals(args))
    elif args.command == 'positions':
        return asyncio.run(cmd_positions(args))
    elif args.command == 'reconcile':
        return asyncio.run(cmd_reconcile(args))
    else:
        parser.print_help()
        return 1


if __name__ == '__main__':
    sys.exit(main())
