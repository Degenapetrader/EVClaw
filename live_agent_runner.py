#!/usr/bin/env python3
"""DB loop and CLI routing helpers extracted from live_agent."""

from __future__ import annotations

import argparse
import asyncio
import random
import sys
import time
from typing import Any, Dict, Optional


async def run_from_db_once(
    args,
    db: Optional[Any] = None,
    db_path: Optional[str] = None,
    *,
    api: Any,
) -> int:
    resolve_db_path = api.resolve_db_path
    ai_trader_db_cls = api.ai_trader_db_cls
    build_processor_id = api.build_processor_id
    default_claim_stale_seconds = api.DEFAULT_CLAIM_STALE_SECONDS
    run_db_call = api.run_db_call
    runtime_dir = api.RUNTIME_DIR
    run_cycle = api.run_cycle
    serialize_summary = api.serialize_summary
    default_retry_delay_seconds = api.DEFAULT_CYCLE_RETRY_DELAY_SECONDS

    # In continuous loop mode we reuse a single DB handle to avoid
    # re-initializing (and spamming logs) on every poll.
    resolved_path = db_path or resolve_db_path(args.dry_run)
    db = db or ai_trader_db_cls(resolved_path)

    processor_id = build_processor_id(getattr(args, "processor_id", None))
    stale_after = float(getattr(args, "claim_stale_seconds", default_claim_stale_seconds))

    claim = await run_db_call(
        db.claim_next_cycle,
        processor_id=processor_id,
        stale_after_seconds=stale_after,
    )
    if not claim:
        print("No unprocessed cycles in DB.")
        return 0

    seq = int(claim["seq"])
    cycle_file = claim.get("cycle_file") or str(runtime_dir / f"evclaw_cycle_{seq}.json")
    context_file = claim.get("context_file") or str(runtime_dir / f"evclaw_context_{seq}.txt")
    context_json_file = claim.get("context_json_file") or str(runtime_dir / f"evclaw_context_{seq}.json")
    candidates_file = claim.get("candidates_file") or None

    rc = 1
    summary: Dict[str, Any] = {"seq": seq, "status": "FAILED", "reason": "unknown"}
    try:
        rc, summary = await run_cycle(
            seq=seq,
            cycle_file=cycle_file,
            context_json_file=context_json_file,
            dry_run=args.dry_run,
            risk_pct_lighter=args.risk_pct_lighter,
            risk_pct_hyperliquid=args.risk_pct_hyperliquid,
            candidates_file=candidates_file,
            db=db,
            db_path_override=db_path,
            context_file=context_file,
            use_llm=getattr(args, "use_llm", False),
            llm_model=getattr(args, "llm_model", None),
        )
    except Exception as exc:
        summary = {"seq": seq, "status": "FAILED", "reason": f"exception:{exc}"}
        rc = 1

    status = summary.get("status") or ("SUCCESS" if rc == 0 else "FAILED")
    error = summary.get("reason")
    
    # Non-retryable errors: missing/invalid files cannot be recovered
    non_retryable_errors = {
        "cycle_file_missing",
        "context_json_missing",
        "invalid_context_json",
    }
    is_permanent_failure = error in non_retryable_errors
    
    if rc == 0 or is_permanent_failure:
        # Success or permanent failure: consume the cycle
        await run_db_call(
            db.mark_cycle_processed,
            seq=seq,
            status=status,
            summary=serialize_summary(summary),
            processed_by=processor_id,
            error=error,
        )
    else:
        # Retryable failure: do not consume the cycle permanently.
        await run_db_call(
            db.mark_cycle_retryable_failure,
            seq=seq,
            status=status,
            summary=serialize_summary(summary),
            processed_by=processor_id,
            error=error,
            stale_after_seconds=stale_after,
            retry_delay_seconds=float(getattr(args, "retry_delay_seconds", default_retry_delay_seconds)),
        )
    return rc


async def run_db_loop(args, *, api: Any) -> int:
    build_deps = api.build_deps
    build_executor_with_learning = api.build_executor_with_learning
    get_learning_engine = api.get_learning_engine
    default_pending_check_interval_seconds = api.DEFAULT_PENDING_CHECK_INTERVAL_SECONDS
    venue_lighter = api.VENUE_LIGHTER
    venue_hyperliquid = api.VENUE_HYPERLIQUID
    venue_hip3_wallet = api.VENUE_HYPERLIQUID_WALLET
    resolve_sqlite_db_path = api.resolve_sqlite_db_path
    default_loop_sleep_seconds = api.DEFAULT_LOOP_SLEEP_SECONDS
    default_loop_sleep_jitter_seconds = api.DEFAULT_LOOP_SLEEP_JITTER_SECONDS

    sleep_seconds = float(getattr(args, "sleep_seconds", default_loop_sleep_seconds))

    # Reuse DB handle across polls to reduce WAL churn and log spam.
    deps = build_deps(dry_run=args.dry_run)
    db = deps.db
    db_path = deps.db_path

    # Reuse executor in loop mode so SR-limit pending orders are monitored even
    # when there are no new cycles to process.
    exec_config = deps.exec_config
    try:
        executor = await build_executor_with_learning(exec_config, deps.tracker, db_path=db_path)
    except Exception as exc:
        print(f"❌ Failed to initialize executor in loop mode: {exc}")
        return 1

    # Learning engine for daily decay
    learning_engine = get_learning_engine(db_path)

    last_pending_check_ts = 0.0
    pending_check_interval_s = max(1.0, float(default_pending_check_interval_seconds or 30.0))
    pending_synced = False

    async def _process_pending_sr_limits() -> None:
        nonlocal last_pending_check_ts, pending_synced
        pending_mgr = getattr(executor, "pending_mgr", None)
        if not pending_mgr or not pending_mgr.enabled:
            return

        now_ts = time.time()
        if (now_ts - last_pending_check_ts) < pending_check_interval_s:
            return
        last_pending_check_ts = now_ts

        adapters = {}
        if executor.lighter:
            adapters[venue_lighter] = executor.lighter
        if executor.hyperliquid:
            adapters[venue_hyperliquid] = executor.hyperliquid
        if getattr(executor, "hip3_wallet", None) is not None and getattr(
            executor.hip3_wallet, "_initialized", False
        ):
            adapters[venue_hip3_wallet] = executor.hip3_wallet

        # One-time startup reconciliation for pending SR limits.
        if not pending_synced:
            try:
                filled = await pending_mgr.sync_startup(adapters, db)
                pending_synced = True
                for p in filled:
                    await executor.handle_sr_limit_fill(p, db_path=resolve_sqlite_db_path(db))
            except Exception as exc:
                print(f"Pending SR sync_startup failed: {exc}")

        # Price map for SR-broken checks (best-effort).
        prices: Dict[str, float] = {}
        try:
            for k, p in list(getattr(pending_mgr, "_pending", {}).items()):
                # pending_mgr._pending is keyed by (symbol, venue)
                sym = k[0] if isinstance(k, tuple) and k else str(k)
                ad = adapters.get(p.venue)
                if not ad:
                    continue
                try:
                    mid = await ad.get_mid_price(sym)
                    prices[sym] = float(mid or 0.0)
                except Exception:
                    prices[sym] = 0.0
        except Exception:
            pass

        try:
            filled_orders = await pending_mgr.check_all(
                adapters=adapters,
                db=db,
                get_signal_direction=lambda _s: None,
                get_current_price=lambda s: prices.get(s, 0.0),
            )
            for p in filled_orders:
                await executor.handle_sr_limit_fill(p, db_path=resolve_sqlite_db_path(db))
        except Exception as exc:
            print(f"Pending SR check_all failed: {exc}")

    try:
        while True:
            # Daily decay check for learning adjustments (runs once per UTC day)
            if learning_engine and hasattr(learning_engine, "daily_decay_check"):
                try:
                    if learning_engine.daily_decay_check():
                        print("📉 Learning adjustments decay applied (daily reset)")
                except Exception as exc:
                    print(f"Learning decay check failed: {exc}")

            # Always monitor SR pending orders even if there are no new cycles.
            await _process_pending_sr_limits()

            rc = await run_from_db_once(args, db=db, db_path=db_path, api=api)
            if not args.loop:
                return rc

            base_sleep = max(0.1, sleep_seconds)
            jitter = max(0.0, float(default_loop_sleep_jitter_seconds or 0.0))
            delay = base_sleep + (random.uniform(0.0, jitter) if jitter > 0 else 0.0)
            await asyncio.sleep(delay)
    finally:
        try:
            await executor.close()
        except Exception:
            pass


async def run_command(args, *, api: Any) -> int:
    resolve_db_path = api.resolve_db_path
    load_pct_24h_history_from_trades = api.load_pct_24h_history_from_trades
    runtime_dir = api.RUNTIME_DIR
    load_pending = api.load_pending
    clear_pending = api.clear_pending
    set_last_notified = api.set_last_notified
    run_cycle = api.run_cycle
    process_candidates = api.process_candidates
    mark_busy = api.mark_busy
    mark_idle = api.mark_idle

    # Load historical pct_24h data for z-score calculation
    try:
        history_db_path = resolve_db_path(getattr(args, "dry_run", False))
        load_pct_24h_history_from_trades(str(history_db_path), days=7)
    except Exception:
        pass  # Non-critical, history will build up over time

    if args.command == "run" and (
        getattr(args, "from_db", False)
        or getattr(args, "loop", False)
        or getattr(args, "continuous", False)
    ):
        # DB runner modes:
        # - --from-db (single cycle)
        # - --from-db --loop (continuous)
        # - --continuous (alias for --from-db --loop)
        if getattr(args, "continuous", False):
            args.loop = True
            args.from_db = True
        if getattr(args, "loop", False):
            return await run_db_loop(args, api=api)
        return await run_from_db_once(args, api=api)

    pending_payload_loaded = False
    if getattr(args, "from_pending", False):
        payload = load_pending()
        if not payload:
            print("No pending payload found.")
            return 1
        seq_raw = payload.get("seq")
        try:
            args.seq = int(seq_raw)
        except (TypeError, ValueError):
            print(f"Pending payload missing/invalid seq: {seq_raw!r}")
            return 1
        args.cycle_file = payload.get("cycle_file")
        args.context_file = payload.get("context_file")
        args.context_json_file = payload.get("context_json_file")
        pending_payload_loaded = True

    if getattr(args, "seq", None) and not getattr(args, "context_json_file", None):
        args.context_json_file = str(runtime_dir / f"evclaw_context_{int(args.seq)}.json")

    if not getattr(args, "seq", None):
        print("Missing --seq")
        return 1

    if args.command in ("run",) and not args.context_json_file:
        print("Missing --context-json-file")
        return 1

    if args.command in ("execute", "run") and not args.cycle_file:
        print("Missing --cycle-file")
        return 1

    if pending_payload_loaded:
        clear_pending()
        set_last_notified(int(args.seq))

    if args.command == "run":
        rc, _summary = await run_cycle(
            seq=int(args.seq),
            cycle_file=args.cycle_file,
            context_json_file=args.context_json_file,
            dry_run=args.dry_run,
            risk_pct_lighter=args.risk_pct_lighter,
            risk_pct_hyperliquid=args.risk_pct_hyperliquid,
            candidates_file=args.output,
            reuse_existing=False,
            context_file=args.context_file,
            use_llm=getattr(args, "use_llm", False),
            llm_model=getattr(args, "llm_model", None),
        )
        return rc

    if args.command == "execute":
        mark_busy(int(args.seq))
        try:
            rc, _summary = await process_candidates(
                seq=int(args.seq),
                cycle_file=args.cycle_file,
                candidates_file=args.candidates_file,
                dry_run=args.dry_run,
                risk_pct_lighter=args.risk_pct_lighter,
                risk_pct_hyperliquid=args.risk_pct_hyperliquid,
            )
            return rc
        finally:
            mark_idle(int(args.seq))

    return 0


def main(*, api: Any) -> int:
    default_loop_sleep_seconds = api.DEFAULT_LOOP_SLEEP_SECONDS
    default_claim_stale_seconds = api.DEFAULT_CLAIM_STALE_SECONDS

    parser = argparse.ArgumentParser(description="hl-trader live agent runner")
    subparsers = parser.add_subparsers(dest="command", required=True)

    execute = subparsers.add_parser(
        "execute",
        help="Validate candidates and record PROPOSED proposals for OpenClaw execution",
    )
    execute.add_argument("--seq", type=int, required=False)
    execute.add_argument("--cycle-file", required=False)
    execute.add_argument("--candidates-file", required=True)
    execute.add_argument("--dry-run", action="store_true")
    execute.add_argument("--from-pending", action="store_true")
    execute.add_argument("--risk-pct-lighter", type=float, default=1.0)
    execute.add_argument("--risk-pct-hyperliquid", type=float, default=1.0)

    run = subparsers.add_parser(
        "run",
        help="Select from JSON context and record PROPOSED proposals (AGI-only mode)",
    )
    run.add_argument("--seq", type=int, required=False)
    run.add_argument("--cycle-file", required=False)
    run.add_argument("--context-json-file", required=False)
    run.add_argument("--context-file", required=False)
    run.add_argument("--output", help="Output candidates file")
    run.add_argument("--dry-run", action="store_true")
    run.add_argument("--from-pending", action="store_true")
    run.add_argument("--risk-pct-lighter", type=float, default=1.0)
    run.add_argument("--risk-pct-hyperliquid", type=float, default=1.0)
    run.add_argument("--from-db", action="store_true")
    run.add_argument("--loop", action="store_true")
    run.add_argument("--continuous", action="store_true", help="Continuously poll DB until interrupted")
    run.add_argument("--sleep-seconds", type=float, default=default_loop_sleep_seconds)
    run.add_argument("--claim-stale-seconds", type=float, default=default_claim_stale_seconds)
    run.add_argument("--processor-id", default=None)
    run.add_argument("--use-llm", action="store_true", help="Use LLM actor/critic for candidates")
    run.add_argument("--llm-model", default=None, help="Override LLM model name")

    # AGI trader default: no-args runs DB continuous mode.
    # Equivalent to: `python3 live_agent.py run --from-db --continuous`
    argv = None
    if len(sys.argv) <= 1:
        argv = ["run", "--from-db", "--continuous"]
    args = parser.parse_args(argv)
    return asyncio.run(run_command(args, api=api))
