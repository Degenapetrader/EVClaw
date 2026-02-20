#!/usr/bin/env python3
"""
Run fill reconciliation for fee-accurate tracking.

SQLite is the SINGLE source of truth for trades/fills.
"""

import argparse
import asyncio
import json
import logging
from logging_utils import get_logger
import signal
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

import yaml
from dotenv import load_dotenv

SKILL_DIR = Path(__file__).parent
CONFIG_FILE = SKILL_DIR / "skill.yaml"
HEARTBEAT_FILE = Path("/tmp/evclaw_fill_reconciler_heartbeat.json")
HEARTBEAT_INTERVAL = 15  # seconds
MAE_MFE_MAX_CONCURRENCY = 2
MAE_MFE_TIMEOUT_SEC = 45.0
MAE_MFE_MIN_INTERVAL_SEC = 0.75

# Load environment variables from .env BEFORE any imports that use them
load_dotenv(SKILL_DIR / ".env")

from fill_reconciler import FillReconciler
from fill_streamer import build_fill_streamer
from ai_trader_db import AITraderDB
from adaptive_conviction_updater import run_adaptive_conviction_update
from exchanges import (
    LighterAdapter,
    HyperliquidAdapter,
    VENUE_LIGHTER,
    VENUE_HYPERLIQUID,
)
from learning_engine import LearningEngine
from symbol_rr_learning import update_symbol_policy_for_symbol
from config_env import apply_env_overrides
from venues import normalize_venue


def load_config() -> Dict[str, Any]:
    """Load skill configuration."""
    if CONFIG_FILE.exists():
        with open(CONFIG_FILE, "r") as f:
            return apply_env_overrides(yaml.safe_load(f) or {})
    return apply_env_overrides({})


def build_learning_engine(db_path: str, config: Dict[str, Any]) -> LearningEngine:
    """Create learning engine for close-time processing."""
    return LearningEngine(
        db_path=db_path,
        memory_dir=Path(config.get("config", {}).get("context_builder", {}).get("memory_dir", SKILL_DIR / "memory")),
    )


def build_on_trade_close(
    db_path: str,
    learning_engine: Optional[LearningEngine],
    log: logging.Logger,
    config: Optional[Dict[str, Any]] = None,
):
    """Callback for fill reconciler to trigger learning updates.

    This runs inside the fill reconciler process. It should:
    - be best-effort (never crash reconciler)
    - enqueue reflection tasks for the dedicated reflector worker
    - optionally queue the local learning engine (if enabled)
    """
    db = AITraderDB(db_path)
    mae_max_concurrency = max(1, int(MAE_MFE_MAX_CONCURRENCY))
    mae_timeout_sec = max(5.0, float(MAE_MFE_TIMEOUT_SEC))
    mae_min_interval_sec = max(0.0, float(MAE_MFE_MIN_INTERVAL_SEC))

    mae_sem = asyncio.Semaphore(mae_max_concurrency)
    mae_rate_lock = asyncio.Lock()
    last_mae_start = 0.0

    dossier_max_concurrency = 1
    dossier_timeout_sec = 20.0
    dossier_sem = asyncio.Semaphore(dossier_max_concurrency)
    rr_policy_timeout_sec = 20.0
    cfg = config or {}
    fill_reconciler_cfg = (cfg.get("config", {}).get("fill_reconciler", {}) or {})

    try:
        adaptive_max_concurrency = max(
            1,
            int(fill_reconciler_cfg.get("adaptive_max_concurrency", 1) or 1),
        )
    except Exception:
        adaptive_max_concurrency = 1
    adaptive_sem = asyncio.Semaphore(adaptive_max_concurrency)

    def _queue_task(coro: asyncio.Future, *, label: str) -> None:
        """Best-effort background scheduler that never blocks close path."""
        try:
            loop = asyncio.get_running_loop()
            loop.create_task(coro)
        except RuntimeError:
            log.debug(f"Skipping async task without running loop: {label}")
        except Exception as exc:
            log.error(f"Failed to queue {label}: {exc}")

    async def _compute_mae_mfe(trade_id: int) -> None:
        nonlocal last_mae_start
        async with mae_sem:
            if mae_min_interval_sec > 0:
                async with mae_rate_lock:
                    now = time.monotonic()
                    wait = mae_min_interval_sec - (now - last_mae_start)
                    if wait > 0:
                        await asyncio.sleep(wait)
                    last_mae_start = time.monotonic()

            try:
                from mae_mfe import compute_and_store_mae_mfe
                await asyncio.wait_for(
                    compute_and_store_mae_mfe(db_path=db_path, trade_id=int(trade_id)),
                    timeout=mae_timeout_sec,
                )
            except asyncio.TimeoutError:
                log.warning(f"MAE/MFE compute timed out for trade {trade_id} after {mae_timeout_sec:.1f}s")
            except Exception as exc:
                log.warning(f"MAE/MFE compute failed for trade {trade_id}: {exc}")

    async def _update_symbol_dossier(trade_id: int) -> None:
        async with dossier_sem:
            try:
                from learning_dossier_aggregator import update_from_trade_close
                await asyncio.wait_for(
                    asyncio.to_thread(update_from_trade_close, db_path, int(trade_id)),
                    timeout=dossier_timeout_sec,
                )
            except asyncio.TimeoutError:
                log.warning(f"Symbol dossier update timed out for trade {trade_id} after {dossier_timeout_sec:.1f}s")
            except Exception as exc:
                log.warning(f"Symbol dossier update failed for trade {trade_id}: {exc}")

    def _store_trade_features(trade_id: int) -> None:
        feature = None
        try:
            feature = db.get_trade_feature_source(trade_id)
            if not feature:
                return
            db.insert_trade_features(**feature)
        except Exception as exc:
            feature_keys = sorted(list(feature.keys())) if isinstance(feature, dict) else []
            log.warning(
                "Trade feature capture failed for trade %s keys=%s: %s",
                trade_id,
                feature_keys,
                exc,
            )

    def _run_adaptive_update() -> None:
        try:
            result = run_adaptive_conviction_update(db)
            log.info(
                "Adaptive conviction update status=%s updated=%s reason=%s samples=%s",
                result.status,
                result.updated,
                result.reason,
                result.sample_count,
            )
        except Exception as exc:
            log.warning(f"Adaptive conviction update failed: {exc}")

    async def _store_features_then_adapt(trade_id: int) -> None:
        async with adaptive_sem:
            await asyncio.to_thread(_store_trade_features, trade_id)
            await asyncio.to_thread(_run_adaptive_update)

    async def _finalize_entry_gate_outcome(trade_id: int) -> None:
        try:
            if hasattr(db, "finalize_gate_outcome_from_trade"):
                await asyncio.to_thread(db.finalize_gate_outcome_from_trade, int(trade_id))
        except Exception as exc:
            log.warning(f"Entry-gate outcome finalize failed for trade {trade_id}: {exc}")

    async def _update_symbol_rr_policy(symbol: Optional[str]) -> None:
        sym = str(symbol or "").strip().upper()
        if not sym:
            return
        try:
            updated = await asyncio.wait_for(
                asyncio.to_thread(update_symbol_policy_for_symbol, db_path, sym),
                timeout=rr_policy_timeout_sec,
            )
            log.info("Symbol RR policy refresh symbol=%s updated=%s", sym, bool(updated))
        except asyncio.TimeoutError:
            log.warning(
                "Symbol RR policy refresh timed out for %s after %.1fs",
                sym,
                rr_policy_timeout_sec,
            )
        except Exception as exc:
            log.warning("Symbol RR policy refresh failed for %s: %s", sym, exc)

    async def _refresh_symbol_rr(trade_id: int, symbol: Optional[str]) -> None:
        await _compute_mae_mfe(trade_id)
        await _update_symbol_rr_policy(symbol)

    def _enqueue_reflection_task(trade_id: int, symbol: Optional[str]) -> None:
        try:
            db.enqueue_reflection_task(trade_id=int(trade_id), symbol=symbol)
        except Exception as exc:
            log.warning("Failed to enqueue reflection task for trade %s: %s", trade_id, exc)

    def _on_trade_close(payload: Dict[str, Any]) -> None:
        raw_trade_id = payload.get("trade_id")
        if not raw_trade_id:
            return
        try:
            trade_id = int(raw_trade_id)
        except Exception:
            log.warning(f"Invalid trade_id in close callback payload: {raw_trade_id!r}")
            return

        # Manual trades are advisor-mode; they should NOT drive learning/adaptation.
        try:
            t = db.get_trade(trade_id)
            if t and str(getattr(t, "strategy", "core") or "core").strip().lower() == "manual":
                log.info("Skipping learning/adaptive for manual trade_id=%s symbol=%s", trade_id, payload.get("symbol"))
                return
        except Exception:
            pass

        # 1) Enqueue reflection task for the learning reflector worker (non-blocking).
        _queue_task(
            asyncio.to_thread(_enqueue_reflection_task, trade_id, payload.get("symbol")),
            label=f"reflection enqueue for trade {trade_id}",
        )

        # 2) Queue built-in learning engine (optional).
        if learning_engine and hasattr(learning_engine, "process_closed_trade"):
            _queue_task(
                learning_engine.process_closed_trade(trade_id),
                label=f"learning close processing for trade {trade_id}",
            )

        # 3) Best-effort Symbol RR refresh (MAE/MFE + policy update).
        _queue_task(
            _refresh_symbol_rr(trade_id, payload.get("symbol")),
            label=f"symbol RR refresh for trade {trade_id}",
        )

        # 4) Best-effort symbol dossier refresh.
        _queue_task(
            _update_symbol_dossier(trade_id),
            label=f"symbol dossier update for trade {trade_id}",
        )

        # 5) Persist close features for adaptive conviction learning.
        _queue_task(
            _store_features_then_adapt(trade_id),
            label=f"trade feature capture + adaptive update for trade {trade_id}",
        )

        # 6) Link closed-trade outcomes back to entry-gate decisions.
        _queue_task(
            _finalize_entry_gate_outcome(trade_id),
            label=f"entry gate outcome finalize for trade {trade_id}",
        )

    return _on_trade_close


def write_heartbeat(venues: list) -> None:
    """Write a lightweight heartbeat file for reconciler health checks."""
    payload = {
        "ts": time.time(),
        "ts_iso": datetime.now(timezone.utc).isoformat(),
        "venues": venues,
    }
    HEARTBEAT_FILE.write_text(json.dumps(payload, sort_keys=True))


async def heartbeat_loop(venues: list, stop_event: asyncio.Event) -> None:
    """Update heartbeat file periodically until stop_event is set."""
    while not stop_event.is_set():
        try:
            write_heartbeat(venues)
        except Exception:
            pass
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=HEARTBEAT_INTERVAL)
        except asyncio.TimeoutError:
            continue


async def build_reconciler(
    venue: str,
    db_path: str,
    dry_run: bool,
    on_trade_close,
) -> tuple[FillReconciler, Any]:
    """Initialize adapter + reconciler for a venue."""
    log = get_logger(f"fill_runner.{venue}")

    if venue == VENUE_LIGHTER:
        adapter = LighterAdapter(log, dry_run=dry_run)
    elif venue == VENUE_HYPERLIQUID:
        adapter = HyperliquidAdapter(log, dry_run=dry_run, account_mode="wallet")
    else:
        raise ValueError(f"Unknown venue: {venue}")

    ok = await adapter.initialize()
    if not ok:
        raise RuntimeError(f"Failed to initialize adapter for {venue}")

    return FillReconciler(
        db_path=db_path,
        exchange_adapter=adapter,
        venue=venue,
        on_trade_close=on_trade_close,
    ), adapter


async def run_once(reconcilers: list, log: logging.Logger) -> int:
    """Run one reconciliation cycle for each venue."""
    try:
        write_heartbeat([r.venue for r in reconcilers])
    except Exception:
        pass
    total = 0
    for reconciler in reconcilers:
        total += await reconciler.reconcile_now()
    log.info(f"Reconciled {total} fills across {len(reconcilers)} venue(s)")
    return total


async def run_loop(reconcilers: list, streamers: list, log: logging.Logger) -> None:
    """Run continuous reconciliation for all venues."""
    stop_event = asyncio.Event()

    def _stop() -> None:
        for reconciler in reconcilers:
            reconciler.stop()
        for streamer in streamers:
            streamer.stop()
        stop_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _stop)
        except NotImplementedError:
            pass

    tasks = [asyncio.create_task(reconciler.start()) for reconciler in reconcilers]
    tasks += [asyncio.create_task(streamer.start()) for streamer in streamers]
    venues = [r.venue for r in reconcilers] or [s.venue for s in streamers]
    hb_task = asyncio.create_task(heartbeat_loop(venues, stop_event))

    async def _monitor_workers() -> None:
        if not tasks:
            return
        done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        if stop_event.is_set():
            return
        for task in done:
            if task.cancelled():
                log.warning("Fill worker task was cancelled unexpectedly; stopping loop")
                continue
            exc = task.exception()
            if exc is None:
                log.warning("Fill worker task exited unexpectedly; stopping loop")
            else:
                log.error(f"Fill worker task crashed; stopping loop: {exc}")
        _stop()

    monitor_task = asyncio.create_task(_monitor_workers())
    await stop_event.wait()

    hb_task.cancel()
    monitor_task.cancel()
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, hb_task, monitor_task, return_exceptions=True)
    log.info("Fill reconciler stopped")


async def main() -> int:
    parser = argparse.ArgumentParser(description="Run EVClaw fill reconciler")
    parser.add_argument(
        "--venue",
        choices=[
            "lighter", "hyperliquid", "hip3",
            "hl_wallet", "hyperliquid_wallet", "lighter_wallet",
            "all",
        ],
        default="all",
    )
    parser.add_argument(
        "--mode",
        choices=["rest", "ws", "hybrid"],
        default="rest",
        help="Fill ingestion mode: rest (poll), ws (stream), hybrid (both)",
    )
    parser.add_argument("--once", action="store_true", help="Run one reconciliation cycle and exit")
    args = parser.parse_args()

    log = get_logger("fill_runner")
    config = load_config()
    cfg = config.get("config", {})
    exec_cfg = cfg.get("executor", {})

    db_path = cfg.get("db_path", str(SKILL_DIR / "ai_trader.db"))
    if not db_path:
        log.error("No db_path configured; cannot run fill reconciler")
        return 1

    dry_run = bool(exec_cfg.get("dry_run", False))
    learning_engine = build_learning_engine(db_path, config)
    on_trade_close = build_on_trade_close(db_path, learning_engine, log, config)

    reconcilers = []
    streamers = []
    adapters = []
    requested = args.venue
    if requested != "all":
        requested = normalize_venue(requested)

    if requested in ("lighter", "all") and exec_cfg.get("lighter_enabled", True):
        reconciler, adapter = await build_reconciler(VENUE_LIGHTER, db_path, dry_run, on_trade_close)
        reconcilers.append(reconciler)
        adapters.append(adapter)
        if args.mode in ("ws", "hybrid"):
            streamers.append(build_fill_streamer(VENUE_LIGHTER, reconciler, adapter))

    if requested in ("hyperliquid", "all") and exec_cfg.get("hl_enabled", True):
        reconciler, adapter = await build_reconciler(VENUE_HYPERLIQUID, db_path, dry_run, on_trade_close)
        reconcilers.append(reconciler)
        adapters.append(adapter)
        if args.mode in ("ws", "hybrid"):
            streamers.append(build_fill_streamer(VENUE_HYPERLIQUID, reconciler, adapter))

    if not reconcilers:
        log.error("No venues enabled or selected; nothing to reconcile/stream")
        return 1

    try:
        if args.once:
            if args.mode != "rest":
                log.warning("--once only applies to REST mode; streaming disabled for this run")
            await run_once(reconcilers, log)
            return 0

        if args.mode == "rest":
            log.info("Starting fill reconciliation loop (REST)")
        elif args.mode == "ws":
            log.info("Starting fill stream loop (WS)")
        else:
            log.info("Starting fill reconciliation loop (HYBRID)")
        await run_loop(reconcilers if args.mode in ("rest", "hybrid") else [], streamers, log)
        return 0

    finally:
        # Best-effort close adapters (avoid aiohttp 'Unclosed client session' warnings)
        for a in adapters:
            try:
                await a.close()
            except Exception:
                pass


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
