#!/usr/bin/env python3
"""Candidate processing implementation extracted from live_agent."""

from __future__ import annotations

import json
import os
import sqlite3
import time
from pathlib import Path
from typing import Any, Dict, Optional, Set, Tuple

from env_utils import EVCLAW_DOCS_DIR, env_str


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        out = float(value)
    except Exception:
        return float(default)
    if out != out:
        return float(default)
    return out


def build_live_risk_config(config: Dict[str, Any], *, risk_config_cls: Any) -> Any:
    """Build live risk config from skill config (YAML-owned in live path)."""
    risk_cfg = config.get("config", {}).get("risk", {}) or {}
    return risk_config_cls(
        min_risk_pct=float(risk_cfg.get("min_risk_pct", 0.5)),
        max_risk_pct=float(risk_cfg.get("max_risk_pct", 2.5)),
        base_risk_pct=float(risk_cfg.get("base_risk_pct", 1.0)),
        equity_floor_pct=float(risk_cfg.get("equity_floor_pct", 80.0)),
        daily_drawdown_limit_pct=float(risk_cfg.get("daily_drawdown_limit_pct", 10.0)),
        max_concurrent_positions=int(risk_cfg.get("max_concurrent_positions", 100)),
        max_sector_concentration=int(risk_cfg.get("max_sector_concentration", 30)),
        max_position_pct=float(risk_cfg.get("max_position_pct", 0.5)),
        no_hard_stops=bool(risk_cfg.get("no_hard_stops", False)),
        # max_hold_hours removed by design
        min_hold_hours=float(risk_cfg.get("min_hold_hours", 2.0)),
        emergency_loss_pct=float(risk_cfg.get("emergency_loss_pct", 15.0)),
        emergency_portfolio_loss_pct=float(risk_cfg.get("emergency_portfolio_loss_pct", 20.0)),
        soft_stop_atr_mult=float(risk_cfg.get("soft_stop_atr_mult", 2.0)),
        soft_stop_alert=bool(risk_cfg.get("soft_stop_alert", True)),
        stale_signal_max_minutes=float(risk_cfg.get("stale_signal_max_minutes", 10.0)),
    )


def _ensure_deterministic_execution_metadata(
    candidate: Dict[str, Any],
    *,
    resolve_order_type_fn,
    conviction_config: Any,
) -> str:
    """Backfill execution metadata from deterministic conviction policy."""
    execution = dict(candidate.get("execution") or {})
    blended_conv = candidate.get("blended_conviction")
    route_conv = blended_conv if blended_conv is not None else candidate.get("conviction")
    conv_val = _safe_float(route_conv, 0.0)
    order_type = str(resolve_order_type_fn(conv_val, config=conviction_config) or "reject").strip().lower()
    if order_type not in {"limit", "chase_limit", "reject"}:
        order_type = "reject"

    conviction_source = "blended" if blended_conv is not None else "proposal_conviction"
    source = str(execution.get("source") or "").strip() or "deterministic_policy"
    execution.update(
        {
            "source": source,
            "order_type": order_type,
            "conviction_source": conviction_source,
            "blended_conviction": float(conv_val),
        }
    )
    if order_type == "limit":
        execution["limit_style"] = "sr_limit"
        execution["limit_fallback"] = "atr_1x"
    candidate["execution"] = execution
    return order_type


async def process_candidates_impl(
    seq: int,
    cycle_file: str,
    candidates_file: str,
    dry_run: bool,
    risk_pct_lighter: float,
    risk_pct_hyperliquid: float,
    db_path_override: Optional[str] = None,
    deps: Optional[Any] = None,
    executor: Optional[Any] = None,
    api: Optional[Any] = None,
) -> Tuple[int, Dict[str, Any]]:
    if api is None:
        raise RuntimeError("process_candidates_impl requires api")

    build_deps = api.build_deps
    _build_process_summary = api.build_process_summary
    clamp_risk_pct = api.clamp_risk_pct
    _resolve_active_venues = api.resolve_active_venues
    _runtime_conviction_config = api.runtime_conviction_config
    _build_executor_with_learning = api.build_executor_with_learning
    load_cycle_file = api.load_cycle_file
    _load_and_validate_candidates_payload = api.load_and_validate_candidates_payload
    _apply_llm_gate_selection = api.apply_llm_gate_selection
    _enforce_candidate_limit_and_audit = api.enforce_candidate_limit_and_audit
    _resolve_global_block_reason = api.resolve_global_block_reason
    send_system_event = api.send_system_event
    _block_candidate = api.block_candidate
    _resolve_starting_equity = api.resolve_starting_equity
    SafetyManager = api.SafetyManager
    RiskConfig = api.RiskConfig
    DynamicRiskManager = api.DynamicRiskManager
    get_equity_for_venue = api.get_equity_for_venue
    VENUE_LIGHTER = api.VENUE_LIGHTER
    VENUE_HYPERLIQUID = api.VENUE_HYPERLIQUID
    VENUE_HYPERLIQUID_WALLET = api.VENUE_HYPERLIQUID_WALLET
    _is_hip3 = api.is_hip3
    _sync_risk_manager_state_from_safety = api.sync_risk_manager_state_from_safety
    _run_db_call = api.run_db_call
    find_symbol_data = api.find_symbol_data
    derive_prices_from_symbol_data = api.derive_prices_from_symbol_data
    venues_for_symbol = api.venues_for_symbol
    check_symbol_on_venues = api.check_symbol_on_venues
    _apply_exit_cooldown = api.apply_exit_cooldown
    _normalize_pct_cap = api.normalize_pct_cap
    _apply_position_pct_cap = api.apply_position_pct_cap
    _apply_sanity_cap_for_venues = api.apply_sanity_cap_for_venues
    _append_reason_note = api.append_reason_note
    _build_requested_size_overrides = api.build_requested_size_overrides
    get_atr_service = api.get_atr_service
    append_jsonl = api.append_jsonl
    _utc_now = api.utc_now
    _maybe_emit_hip3_flip_review = api.maybe_emit_hip3_flip_review
    insert_proposals = api.insert_proposals
    HIP3_TRADING_ENABLED = api.HIP3_TRADING_ENABLED
    compute_risk_size_usd = api.compute_risk_size_usd
    resolve_order_type = api.resolve_order_type
    deps = deps or build_deps(dry_run=dry_run, db_path_override=db_path_override)
    config = deps.config
    exec_config = deps.exec_config
    summary: Dict[str, Any] = _build_process_summary(seq)

    # Clamp risk pct inputs. Note: Lighter can be disabled in config; in that
    # case, we still clamp but won't print noisy messages.
    risk_pct_lighter, clamp_lighter = clamp_risk_pct(risk_pct_lighter)
    risk_pct_hyperliquid, clamp_hl = clamp_risk_pct(risk_pct_hyperliquid)
    if clamp_lighter and exec_config.lighter_enabled:
        print(f"Risk pct lighter clamped to {risk_pct_lighter:.2f}%")
    if clamp_hl:
        print(f"Risk pct hyperliquid clamped to {risk_pct_hyperliquid:.2f}%")

    # Determine active venues based on config (boss can explicitly disable a venue).
    active_venues: List[str] = _resolve_active_venues(exec_config)

    if not active_venues:
        print("No venues enabled for live agent mode.")
        summary["status"] = "FAILED"
        summary["reason"] = "venues_not_enabled"
        return 1, summary

    db = deps.db
    conviction_cfg = _runtime_conviction_config(db)

    # Adaptive SL/TP layer (per-symbol, per-regime) using DB-derived outcomes.
    tracker = deps.tracker
    own_executor = executor is None
    if own_executor:
        try:
            executor = await _build_executor_with_learning(exec_config, tracker, db_path=deps.db_path)
        except Exception as exc:
            summary["status"] = "FAILED"
            summary["reason"] = f"executor_init_failed:{exc}"
            return 1, summary
    assert executor is not None
    if getattr(exec_config, "hl_wallet_enabled", False):
        # Maintain compatibility with legacy flag, but prevent split venue execution.
        exec_config.hl_wallet_enabled = False
    try:
        cycle_data = load_cycle_file(cycle_file)
        symbols = cycle_data.get("symbols", {}) or {}

        payload, valid_candidates, invalid_candidates = _load_and_validate_candidates_payload(
            candidates_file=candidates_file,
            db=db,
            summary=summary,
            seq=seq,
            venues=active_venues,
        )
        if payload is None:
            return 1, summary

        valid_candidates, llm_gate_terminal = _apply_llm_gate_selection(
            payload=payload,
            db=db,
            summary=summary,
            seq=seq,
            active_venues=active_venues,
            valid_candidates=valid_candidates,
            invalid_candidates=invalid_candidates,
            conviction_config=conviction_cfg,
        )
        llm_gate_summary = summary.get("llm_gate") if isinstance(summary, dict) else None
        if isinstance(llm_gate_summary, dict):
            gate_status = str(llm_gate_summary.get("status") or "").upper()
            if gate_status in {"INVALID_SCHEMA", "INVALID_COVERAGE"}:
                summary["llm_gate_fallback_mode"] = "deterministic_policy"
                summary["llm_gate_fallback_reason"] = llm_gate_summary.get("fallback_reason")
        if llm_gate_terminal:
            return 0, summary

        kept = _enforce_candidate_limit_and_audit(
            db=db,
            summary=summary,
            seq=seq,
            active_venues=active_venues,
            valid_candidates=valid_candidates,
            invalid_candidates=invalid_candidates,
        )

        global_block_reason = await _resolve_global_block_reason(
            db=db,
            executor=executor,
            exec_config=exec_config,
            active_venues=active_venues,
        )

        if global_block_reason:
            print(f"Global block: {global_block_reason}")
            if global_block_reason.startswith("fill_reconciler_not_running"):
                send_system_event({"event": "evclaw_blocked", "seq": seq, "reason": global_block_reason})
            for candidate in kept:
                _block_candidate(
                    db=db,
                    summary=summary,
                    seq=seq,
                    candidate=candidate,
                    reason=global_block_reason,
                    venues=active_venues,
                )
            summary["status"] = "BLOCKED"
            summary["reason"] = global_block_reason
            return 1, summary

        if not kept:
            print("No valid candidates to process.")
            summary["status"] = "NO_CANDIDATES"
            return 0, summary

        print("📋 AGI MODE: proposals recorded for OpenClaw execution (no user approval).")

        safety_cfg = config.get("config", {}).get("safety", {}) or {}
        exposure_cfg = config.get("config", {}).get("exposure", {}) or {}

        def _cfg_float(cfg: Dict[str, Any], key: str, default: float) -> float:
            try:
                raw = cfg.get(key, default) if isinstance(cfg, dict) else default
                return float(default if raw is None else raw)
            except Exception:
                return float(default)

        countertrend_cfg = (config.get("config", {}).get("countertrend_guard", {}) or {})
        ct_enabled = bool(countertrend_cfg.get("enabled", True))
        ct_threshold_abs = max(1.0, abs(_cfg_float(countertrend_cfg, "threshold_abs", 40.0)))
        ct_min_non_dead_confirmations = max(
            1, int(countertrend_cfg.get("min_non_dead_confirmations", 2) or 2)
        )
        ct_confirmation_z_min = max(0.1, _cfg_float(countertrend_cfg, "confirmation_z_min", 2.0))
        ct_require_positive_expectancy = bool(countertrend_cfg.get("require_positive_expectancy", True))
        ct_expectancy_min_samples = max(
            1, int(countertrend_cfg.get("expectancy_min_samples", 12) or 12)
        )
        ct_expectancy_lookback_hours = max(
            1.0, _cfg_float(countertrend_cfg, "expectancy_lookback_hours", 168.0)
        )
        ct_risk_multiplier = max(0.05, min(1.0, _cfg_float(countertrend_cfg, "risk_multiplier", 0.40)))
        ct_risk_cap_pct = max(0.05, _cfg_float(countertrend_cfg, "risk_cap_pct", 0.60))
        ct_max_picks = max(0, int(countertrend_cfg.get("max_countertrend_picks_per_cycle", 1) or 1))
        ct_kill_sl_count = max(1, int(countertrend_cfg.get("kill_switch_sl_count", 3) or 3))
        ct_kill_sl_window_hours = max(
            0.5, _cfg_float(countertrend_cfg, "kill_switch_sl_window_hours", 6.0)
        )
        ct_kill_loss_r = _cfg_float(countertrend_cfg, "kill_switch_loss_r", -2.0)
        ct_kill_loss_window_hours = max(
            0.5, _cfg_float(countertrend_cfg, "kill_switch_loss_window_hours", 12.0)
        )
        ct_cooldown_hours = max(0.25, _cfg_float(countertrend_cfg, "cooldown_hours", 8.0))
        ct_manual_block_cfg = bool(countertrend_cfg.get("manual_block", False))

        def _ct_countertrend(direction_raw: Any, trend_score_raw: Any) -> bool:
            try:
                d = str(direction_raw or "").upper().strip()
                ts = float(trend_score_raw)
            except Exception:
                return False
            return (d == "LONG" and ts <= -ct_threshold_abs) or (d == "SHORT" and ts >= ct_threshold_abs)

        def _ct_non_dead_confirms(candidate_obj: Dict[str, Any], direction_raw: Any) -> int:
            direction_norm = str(direction_raw or "").upper().strip()
            sig_snap = candidate_obj.get("signals_snapshot")
            if not isinstance(sig_snap, dict):
                return 0
            count = 0
            for name in ("whale", "ofm", "cvd", "fade", "hip3_main"):
                payload = sig_snap.get(name)
                if not isinstance(payload, dict):
                    continue
                sig_dir = str(payload.get("direction") or payload.get("signal") or "").upper().strip()
                if sig_dir in ("LONG", "SHORT") and direction_norm in ("LONG", "SHORT") and sig_dir != direction_norm:
                    continue
                z_vals = []
                for key in ("z_score", "z_smart", "z_dumb", "strength"):
                    try:
                        if payload.get(key) is not None:
                            z_vals.append(abs(float(payload.get(key))))
                    except Exception:
                        continue
                z_val = max(z_vals) if z_vals else 0.0
                banner = bool(payload.get("banner_trigger", False))
                if name == "hip3_main":
                    flow_pass = bool(payload.get("flow_pass"))
                    ofm_pass = payload.get("ofm_pass")
                    if flow_pass and (ofm_pass is None or bool(ofm_pass)):
                        count += 1
                        continue
                if banner or z_val >= ct_confirmation_z_min:
                    count += 1
            return int(count)

        def _ct_dead_only(candidate_obj: Dict[str, Any]) -> bool:
            sigs = [
                str(s).upper().strip()
                for s in (candidate_obj.get("signals") or [])
                if isinstance(s, str) and str(s).strip()
            ]
            if sigs:
                return bool(sigs) and all(s.startswith("DEAD_CAPITAL") for s in sigs)
            sig_snap = candidate_obj.get("signals_snapshot")
            if not isinstance(sig_snap, dict):
                return False
            directional_keys = []
            for name, payload in sig_snap.items():
                if not isinstance(payload, dict):
                    continue
                sig_dir = str(payload.get("direction") or payload.get("signal") or "").upper().strip()
                if sig_dir in ("LONG", "SHORT"):
                    directional_keys.append(str(name or "").lower())
            return bool(directional_keys) and all(k == "dead_capital" for k in directional_keys)

        def _ct_ensure_table(conn: sqlite3.Connection) -> None:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS countertrend_guard_state_v1 (
                    id INTEGER PRIMARY KEY CHECK(id = 1),
                    cooldown_until REAL,
                    manual_block INTEGER DEFAULT 0,
                    last_trigger_reason TEXT,
                    last_trigger_ts REAL,
                    updated_at REAL DEFAULT (strftime('%s','now'))
                )
                """
            )

        def _ct_read_state(now_ts: float) -> Dict[str, Any]:
            out: Dict[str, Any] = {
                "enabled": bool(ct_enabled),
                "active": False,
                "manual_block": False,
                "cooldown_until": None,
                "remaining_sec": 0.0,
                "last_trigger_reason": None,
                "last_trigger_ts": None,
            }
            if not ct_enabled:
                return out
            try:
                with sqlite3.connect(str(deps.db_path), timeout=30.0) as conn:
                    conn.row_factory = sqlite3.Row
                    _ct_ensure_table(conn)
                    row = conn.execute(
                        """
                        SELECT cooldown_until, manual_block, last_trigger_reason, last_trigger_ts
                        FROM countertrend_guard_state_v1
                        WHERE id = 1
                        """
                    ).fetchone()
                    if row is None:
                        conn.execute(
                            """
                            INSERT INTO countertrend_guard_state_v1 (
                                id, cooldown_until, manual_block, updated_at
                            ) VALUES (1, NULL, ?, ?)
                            """,
                            (1 if ct_manual_block_cfg else 0, float(now_ts)),
                        )
                        conn.commit()
                        row = conn.execute(
                            """
                            SELECT cooldown_until, manual_block, last_trigger_reason, last_trigger_ts
                            FROM countertrend_guard_state_v1
                            WHERE id = 1
                            """
                        ).fetchone()
                    cooldown_until = (
                        float(row["cooldown_until"])
                        if row is not None and row["cooldown_until"] is not None
                        else None
                    )
                    manual_block = bool(int(row["manual_block"] or 0)) if row is not None else False
                    remaining = 0.0
                    if cooldown_until is not None and cooldown_until > now_ts:
                        remaining = float(cooldown_until - now_ts)
                    out.update(
                        {
                            "active": bool(manual_block or remaining > 0.0),
                            "manual_block": bool(manual_block),
                            "cooldown_until": float(cooldown_until) if cooldown_until is not None else None,
                            "remaining_sec": float(max(0.0, remaining)),
                            "last_trigger_reason": str(row["last_trigger_reason"] or "").strip() if row is not None else None,
                            "last_trigger_ts": (
                                float(row["last_trigger_ts"]) if row is not None and row["last_trigger_ts"] is not None else None
                            ),
                        }
                    )
            except Exception as exc:
                out["error"] = str(exc)
            return out

        def _ct_write_state(
            *,
            now_ts: float,
            cooldown_until: Optional[float] = None,
            manual_block: Optional[bool] = None,
            trigger_reason: Optional[str] = None,
        ) -> None:
            if not ct_enabled:
                return
            try:
                with sqlite3.connect(str(deps.db_path), timeout=30.0) as conn:
                    conn.row_factory = sqlite3.Row
                    _ct_ensure_table(conn)
                    row = conn.execute(
                        """
                        SELECT cooldown_until, manual_block, last_trigger_reason, last_trigger_ts
                        FROM countertrend_guard_state_v1
                        WHERE id = 1
                        """
                    ).fetchone()
                    prev_cooldown = (
                        float(row["cooldown_until"])
                        if row is not None and row["cooldown_until"] is not None
                        else None
                    )
                    prev_manual = bool(int(row["manual_block"] or 0)) if row is not None else False
                    prev_reason = str(row["last_trigger_reason"] or "").strip() if row is not None else None
                    prev_ts = float(row["last_trigger_ts"]) if row is not None and row["last_trigger_ts"] is not None else None
                    next_cooldown = prev_cooldown if cooldown_until is None else float(cooldown_until)
                    next_manual = prev_manual if manual_block is None else bool(manual_block)
                    next_reason = prev_reason if trigger_reason is None else str(trigger_reason)
                    next_ts = prev_ts if trigger_reason is None else float(now_ts)
                    conn.execute(
                        """
                        INSERT INTO countertrend_guard_state_v1 (
                            id, cooldown_until, manual_block, last_trigger_reason, last_trigger_ts, updated_at
                        ) VALUES (1, ?, ?, ?, ?, ?)
                        ON CONFLICT(id) DO UPDATE SET
                            cooldown_until = excluded.cooldown_until,
                            manual_block = excluded.manual_block,
                            last_trigger_reason = excluded.last_trigger_reason,
                            last_trigger_ts = excluded.last_trigger_ts,
                            updated_at = excluded.updated_at
                        """,
                        (
                            next_cooldown,
                            1 if next_manual else 0,
                            next_reason,
                            next_ts,
                            float(now_ts),
                        ),
                    )
                    conn.commit()
            except Exception as exc:
                print(f"Warning: failed to persist countertrend guard state: {exc}")

        def _ct_trade_is_countertrend(row: sqlite3.Row) -> bool:
            if row is None:
                return False
            ctx: Dict[str, Any] = {}
            try:
                raw_ctx = row["context_snapshot"]
                if isinstance(raw_ctx, dict):
                    ctx = raw_ctx
                elif raw_ctx:
                    parsed = json.loads(raw_ctx)
                    if isinstance(parsed, dict):
                        ctx = parsed
            except Exception:
                ctx = {}
            risk_obj = ctx.get("risk") if isinstance(ctx.get("risk"), dict) else {}
            if bool(ctx.get("countertrend_40")) or bool(risk_obj.get("countertrend_40")):
                return True
            trend_raw = ctx.get("trend_score")
            if trend_raw is None:
                km = ctx.get("key_metrics") if isinstance(ctx.get("key_metrics"), dict) else {}
                ts = km.get("trend_state") if isinstance(km.get("trend_state"), dict) else {}
                trend_raw = ts.get("trend_score")
            return _ct_countertrend(row["direction"], trend_raw)

        def _ct_recent_stats(now_ts: float) -> Dict[str, Any]:
            out = {"sl_count": 0, "loss_r": 0.0}
            if not ct_enabled:
                return out
            cutoff_hours = max(ct_kill_sl_window_hours, ct_kill_loss_window_hours)
            cutoff_ts = float(now_ts - (cutoff_hours * 3600.0))
            sl_cutoff_ts = float(now_ts - (ct_kill_sl_window_hours * 3600.0))
            loss_cutoff_ts = float(now_ts - (ct_kill_loss_window_hours * 3600.0))
            try:
                with sqlite3.connect(str(deps.db_path), timeout=30.0) as conn:
                    conn.row_factory = sqlite3.Row
                    rows = conn.execute(
                        """
                        SELECT exit_time, exit_reason, realized_pnl, risk_pct_used, equity_at_entry, direction, context_snapshot
                        FROM trades
                        WHERE state = 'CLOSED' AND exit_time IS NOT NULL AND exit_time >= ?
                        ORDER BY exit_time DESC
                        """,
                        (cutoff_ts,),
                    ).fetchall()
                sl_count = 0
                loss_r = 0.0
                for row in rows:
                    if not _ct_trade_is_countertrend(row):
                        continue
                    try:
                        exit_ts = float(row["exit_time"])
                    except Exception:
                        continue
                    reason = str(row["exit_reason"] or "").upper().strip()
                    if exit_ts >= sl_cutoff_ts and reason in {"SL", "STOP_LOSS", "STOP"}:
                        sl_count += 1
                    if exit_ts >= loss_cutoff_ts:
                        try:
                            pnl_usd = float(row["realized_pnl"] or 0.0)
                        except Exception:
                            pnl_usd = 0.0
                        pnl_r = 0.0
                        try:
                            denom = float(row["equity_at_entry"]) * (float(row["risk_pct_used"]) / 100.0)
                            if denom > 0:
                                pnl_r = pnl_usd / denom
                        except Exception:
                            pnl_r = 0.0
                        loss_r += float(pnl_r)
                out["sl_count"] = int(sl_count)
                out["loss_r"] = float(loss_r)
            except Exception as exc:
                out["error"] = str(exc)
            return out

        def _ct_symbol_expectancy(symbol_raw: Any, now_ts: float) -> Tuple[bool, int, Optional[float]]:
            symbol_norm = str(symbol_raw or "").upper().strip()
            if not symbol_norm:
                return False, 0, None
            cutoff_ts = float(now_ts - (ct_expectancy_lookback_hours * 3600.0))
            samples: list[float] = []
            try:
                with sqlite3.connect(str(deps.db_path), timeout=30.0) as conn:
                    conn.row_factory = sqlite3.Row
                    rows = conn.execute(
                        """
                        SELECT realized_pnl, risk_pct_used, equity_at_entry
                        FROM trades
                        WHERE state = 'CLOSED' AND symbol = ? AND exit_time IS NOT NULL AND exit_time >= ?
                        ORDER BY exit_time DESC
                        """,
                        (symbol_norm, cutoff_ts),
                    ).fetchall()
                for row in rows:
                    try:
                        pnl_usd = float(row["realized_pnl"] or 0.0)
                        denom = float(row["equity_at_entry"]) * (float(row["risk_pct_used"]) / 100.0)
                        if denom > 0:
                            samples.append(float(pnl_usd / denom))
                    except Exception:
                        continue
            except Exception:
                return False, 0, None
            n = len(samples)
            if n <= 0:
                return False, 0, None
            expectancy = float(sum(samples) / float(n))
            ok = bool(n >= ct_expectancy_min_samples and expectancy > 0.0)
            return ok, int(n), float(expectancy)

        fallback_equity = await _resolve_starting_equity(config)
        safety_mgr: Optional[SafetyManager] = None
        if bool(safety_cfg.get("enabled", True)):
            try:
                safety_mgr = SafetyManager(deps.db_path, starting_equity=fallback_equity)
            except Exception as e:
                print(f"Warning: failed to initialize SafetyManager for risk sizing: {e}")
                safety_mgr = None

        # Dynamic risk sizing (Option A): risk% is computed per-candidate from conviction
        # using the configured 0.5% - 2.5% band (not a fixed CLI risk%).
        risk_config = build_live_risk_config(
            config,
            risk_config_cls=RiskConfig,
        )

        lighter_equity = 0.0
        lighter_source = "disabled"
        if VENUE_LIGHTER in active_venues:
            lighter_equity, lighter_source = await get_equity_for_venue(
                db=db,
                executor=executor,
                venue=VENUE_LIGHTER,
                fallback=fallback_equity,
                dry_run=exec_config.dry_run,
            )
            if lighter_source.startswith("fallback"):
                print("Warning: using fallback equity for Lighter sizing.")

        # Unified Hyperliquid equity is used for both normal perps and HIP3 symbols.
        hl_equity = 0.0
        hl_source = "disabled"
        if VENUE_HYPERLIQUID in active_venues:
            hl_equity, hl_source = await get_equity_for_venue(
                db=db,
                executor=executor,
                venue=VENUE_HYPERLIQUID,
                fallback=fallback_equity,
                dry_run=exec_config.dry_run,
            )
            if hl_source.startswith("fallback"):
                print("Warning: using fallback equity for Hyperliquid sizing.")

        # -----------------------------------------------------------------
        # Net exposure warning (soft gate, per venue)
        # -----------------------------------------------------------------
        # Per-boss policy, warning caps remain venue-local.
        snapshot = None
        net_exposure_warn_mult = _cfg_float(exposure_cfg, "net_exposure_warn_mult", 2.0)
        if net_exposure_warn_mult <= 0:
            print(
                "Warning: config.exposure.net_exposure_warn_mult<=0 is unsafe; "
                "falling back to 2.0"
            )
            net_exposure_warn_mult = 2.0
        hl_warn_threshold = None
        lighter_warn_threshold = None

        # Hard net-exposure cap (per Degen): cap |HL net notional| to equity * mult.
        max_net_exposure_mult = _cfg_float(exposure_cfg, "max_net_exposure_mult", 2.0)
        if max_net_exposure_mult <= 0:
            print(
                "Warning: config.exposure.max_net_exposure_mult<=0 disables exposure caps; "
                "falling back to 2.0"
            )
            max_net_exposure_mult = 2.0
        hl_net_notional = None
        hl_net_cap = None
        lighter_net_notional = None
        try:
            snapshot = await _run_db_call(db.get_latest_monitor_snapshot)
            if snapshot:
                hl_net = float(snapshot.get("hl_net_notional") or 0.0)
                if hl_net == 0.0:
                    # Backward compatibility with old split-wallet snapshots.
                    hl_net = float(snapshot.get("hl_wallet_net_notional") or 0.0)
                lt_net = float(snapshot.get("lighter_net_notional") or 0.0)

                hl_net_notional = hl_net
                lighter_net_notional = lt_net
                # Use merged Hyperliquid equity for both Hyperliquid views.
                try:
                    hl_eq_for_cap = float(snapshot.get("hl_equity") or 0.0)
                    if hl_eq_for_cap <= 0:
                        hl_eq_for_cap = float(snapshot.get("hl_wallet_equity") or 0.0)
                except Exception:
                    hl_eq_for_cap = 0.0
                if hl_eq_for_cap > 0 and max_net_exposure_mult > 0:
                    hl_net_cap = hl_eq_for_cap * max_net_exposure_mult

                if net_exposure_warn_mult > 0:
                    if hl_eq_for_cap > 0:
                        hl_warn_threshold = hl_eq_for_cap * net_exposure_warn_mult
                    try:
                        lighter_eq_for_warn = float(snapshot.get("lighter_equity") or 0.0)
                    except Exception:
                        lighter_eq_for_warn = 0.0
                    if lighter_eq_for_warn > 0:
                        lighter_warn_threshold = lighter_eq_for_warn * net_exposure_warn_mult
        except Exception as exc:
            snapshot = None
            print(f"Warning: monitor snapshot unavailable for exposure gates: {exc}")

        if hl_warn_threshold is not None and hl_net_notional is not None and abs(hl_net_notional) >= hl_warn_threshold:
            print(
                f"Warning: Hyperliquid perps net exposure is high (net={hl_net_notional:.0f} vs "
                f"threshold={hl_warn_threshold:.0f}; mult={net_exposure_warn_mult:.2f}x)."
            )
        if (
            lighter_warn_threshold is not None
            and lighter_net_notional is not None
            and abs(lighter_net_notional) >= lighter_warn_threshold
        ):
            print(
                f"Warning: Lighter net exposure is high (net={lighter_net_notional:.0f} vs "
                f"threshold={lighter_warn_threshold:.0f}; mult={net_exposure_warn_mult:.2f}x)."
            )

        # Risk manager instances use the fetched venue equity.
        # (They can be recreated each cycle; they are lightweight.)
        risk_mgr_lighter = (
            DynamicRiskManager(config=risk_config, equity=lighter_equity, safety_manager=safety_mgr)
            if VENUE_LIGHTER in active_venues
            else None
        )
        hl_risk_equity = hl_equity if VENUE_HYPERLIQUID in active_venues else 0.0
        risk_mgr_hl = (
            DynamicRiskManager(config=risk_config, equity=hl_risk_equity, safety_manager=safety_mgr)
            if VENUE_HYPERLIQUID in active_venues
            else None
        )
        _sync_risk_manager_state_from_safety(risk_mgr_lighter, safety_mgr)
        _sync_risk_manager_state_from_safety(risk_mgr_hl, safety_mgr)

        projected_hl_net_notional = float(hl_net_notional) if hl_net_notional is not None else None
        projected_lighter_net_notional = (
            float(lighter_net_notional) if lighter_net_notional is not None else None
        )
        try:
            projected_total_exposure = float(executor.get_total_exposure() or 0.0)
        except Exception as exc:
            fallback_total = 0.0
            if isinstance(snapshot, dict):
                # EVClaw uses unified Hyperliquid account mode.
                # Do not include hl_wallet_* in gross fallback because it can overlap hl_*.
                for key in (
                    "hl_long_notional",
                    "hl_short_notional",
                    "lighter_long_notional",
                    "lighter_short_notional",
                ):
                    try:
                        fallback_total += max(0.0, float(snapshot.get(key) or 0.0))
                    except Exception:
                        continue
            projected_total_exposure = float(fallback_total)
            print(
                "Warning: executor.get_total_exposure failed; "
                f"using monitor snapshot fallback gross={projected_total_exposure:.2f}: {exc}"
            )
        approved_keys: Set[Tuple[str, str, str]] = set()
        countertrend_picks_this_cycle = 0
        ct_now_ts = time.time()
        ct_guard_state: Dict[str, Any] = {"enabled": bool(ct_enabled), "active": False}
        if ct_enabled:
            _ct_write_state(now_ts=ct_now_ts, manual_block=ct_manual_block_cfg)
            ct_guard_state = _ct_read_state(ct_now_ts)
            if not bool(ct_guard_state.get("active")):
                ct_stats = _ct_recent_stats(ct_now_ts)
                trigger_reason = None
                if int(ct_stats.get("sl_count", 0)) >= int(ct_kill_sl_count):
                    trigger_reason = (
                        f"countertrend_sl_cluster sl_count={int(ct_stats.get('sl_count', 0))}"
                        f"/{int(ct_kill_sl_count)} window_h={ct_kill_sl_window_hours:.1f}"
                    )
                elif float(ct_stats.get("loss_r", 0.0)) <= float(ct_kill_loss_r):
                    trigger_reason = (
                        f"countertrend_loss_cluster loss_r={float(ct_stats.get('loss_r', 0.0)):.3f}"
                        f"<= {float(ct_kill_loss_r):.3f} window_h={ct_kill_loss_window_hours:.1f}"
                    )
                if trigger_reason:
                    cooldown_until = float(ct_now_ts + (ct_cooldown_hours * 3600.0))
                    _ct_write_state(
                        now_ts=ct_now_ts,
                        cooldown_until=cooldown_until,
                        manual_block=ct_manual_block_cfg,
                        trigger_reason=trigger_reason,
                    )
                    ct_guard_state = _ct_read_state(ct_now_ts)
                    ct_guard_state["triggered_this_cycle"] = trigger_reason
                ct_guard_state["recent_sl_count"] = int(ct_stats.get("sl_count", 0))
                ct_guard_state["recent_loss_r"] = round(float(ct_stats.get("loss_r", 0.0)), 4)
            summary["countertrend_guard"] = ct_guard_state

        for candidate in kept:
            symbol = candidate["symbol"]
            direction = candidate["direction"]
            clamp_reason = None

            # Optional: temporarily pause HIP3 entries while focusing on normal perps.
            if _is_hip3(symbol) and not HIP3_TRADING_ENABLED:
                _block_candidate(
                    db=db,
                    summary=summary,
                    seq=seq,
                    candidate=candidate,
                    reason="hip3_trading_temporarily_disabled",
                    venues=active_venues,
                )
                continue

            # Policy backstop: XYZ symbols must carry HIP3_MAIN signal metadata.
            if _is_hip3(symbol):
                sig_snap = candidate.get("signals_snapshot")
                if not isinstance(sig_snap, dict) or not isinstance(sig_snap.get("hip3_main"), dict):
                    _block_candidate(
                        db=db,
                        summary=summary,
                        seq=seq,
                        candidate=candidate,
                        reason="hip3_main_required_for_xyz",
                        venues=active_venues,
                    )
                    continue

            conviction = float(candidate.get("conviction") or 0.0)
            if conviction < 0.0 or conviction > 1.0:
                _block_candidate(
                    db=db,
                    summary=summary,
                    seq=seq,
                    candidate=candidate,
                    reason="conviction_out_of_range",
                    venues=active_venues,
                )
                continue

            # Hard/soft exposure checks that require candidate_venues are enforced later.

            # Validate symbol exists in cycle data
            symbol_data = find_symbol_data(symbols, symbol)
            if not symbol_data:
                _block_candidate(
                    db=db,
                    summary=summary,
                    seq=seq,
                    candidate=candidate,
                    reason="symbol_not_in_cycle",
                    venues=active_venues,
                )
                continue

            price, best_bid, best_ask = derive_prices_from_symbol_data(symbol_data)
            if best_bid <= 0 or best_ask <= 0:
                # HIP3-triggered cycles may not include bid/ask in the cycle artifact.
                # Best-effort fetch from the venue adapter to avoid blocking execution.
                try:
                    if VENUE_HYPERLIQUID in active_venues:
                        bb, ba = await executor.hyperliquid.get_best_bid_ask(symbol)
                        if bb and ba and float(bb) > 0 and float(ba) > 0:
                            best_bid, best_ask = float(bb), float(ba)
                            if price <= 0:
                                price = (best_bid + best_ask) / 2.0
                except Exception:
                    print(f"Warning: best_bid_ask fallback failed for {symbol}")

            if best_bid <= 0 or best_ask <= 0:
                _block_candidate(
                    db=db,
                    summary=summary,
                    seq=seq,
                    candidate=candidate,
                    reason="missing_bid_ask",
                    venues=active_venues,
                )
                continue

            # Decide venues for this symbol (mirror + defaults).
            candidate_venues = venues_for_symbol(
                symbol,
                enabled_venues=active_venues,
                default_perps=getattr(exec_config, "default_venue_perps", VENUE_HYPERLIQUID),
                default_hip3=getattr(exec_config, "default_venue_hip3", VENUE_HYPERLIQUID_WALLET),
                mirror_wallet=bool(getattr(exec_config, "hl_mirror_wallet", False)),
                perps_venues=getattr(exec_config, "perps_venues", None),
            )

            if not candidate_venues:
                reason = "default_venue_disabled"
                _block_candidate(
                    db=db,
                    summary=summary,
                    seq=seq,
                    candidate=candidate,
                    reason=reason,
                    venues=active_venues,
                )
                continue

            # Selected venues availability check (per-venue).
            availability = await check_symbol_on_venues(symbol, executor, candidate_venues)
            available: List[str] = []
            if VENUE_HYPERLIQUID in candidate_venues and float(availability.get("hl_mid") or 0.0) > 0:
                available.append(VENUE_HYPERLIQUID)
            if VENUE_LIGHTER in candidate_venues and float(availability.get("lighter_mid") or 0.0) > 0:
                available.append(VENUE_LIGHTER)

            # Dry-run fallback: allow non-HIP3 even if venue mids are missing.
            if not available and exec_config.dry_run and not _is_hip3(symbol):
                available = list(candidate_venues)

            if not available:
                _block_candidate(
                    db=db,
                    summary=summary,
                    seq=seq,
                    candidate=candidate,
                    reason=(
                        f"symbol_not_on_any_enabled_venue hl={availability.get('hl_mid')} "
                        f"lt={availability.get('lighter_mid')}"
                    ),
                    venues=active_venues,
                )
                continue

            candidate_venues = available

            # Enforce router rules (HIP3 wallet-only).
            validated: List[str] = []
            errors: List[str] = []
            for v in list(candidate_venues):
                try:
                    executor.router.validate(v, symbol)
                    validated.append(v)
                except Exception as exc:
                    errors.append(f"{v}:{exc}")
            candidate_venues = validated
            if not candidate_venues:
                _block_candidate(
                    db=db,
                    summary=summary,
                    seq=seq,
                    candidate=candidate,
                    reason=f"venue_validation_failed {'; '.join(errors)}",
                    venues=active_venues,
                )
                continue

            # Soft net-exposure warning gate (per venue): when a venue is already skewed,
            # same-direction adds require strong signal for that venue only.
            d = str(direction or "").upper()
            strong = bool(candidate.get("must_trade"))
            if not strong:
                ss = candidate.get("strong_signals") or []
                try:
                    strong = bool(ss)
                except Exception:
                    strong = False
            if not strong:
                strong = conviction >= _cfg_float(exposure_cfg, "net_exposure_warn_min_conviction", 0.8)

            if not strong:
                blocked_warn_venues: List[str] = []
                for v in list(candidate_venues):
                    venue_key = str(v or "").lower()
                    venue_net: Optional[float] = None
                    venue_thr: Optional[float] = None
                    venue_label = venue_key
                    if venue_key == VENUE_HYPERLIQUID:
                        venue_net = projected_hl_net_notional
                        venue_thr = hl_warn_threshold
                        venue_label = "hl"
                    elif venue_key == VENUE_LIGHTER:
                        venue_net = projected_lighter_net_notional
                        venue_thr = lighter_warn_threshold
                        venue_label = "lighter"
                    if venue_net is None or venue_thr is None:
                        continue
                    aligned = (venue_net > 0 and d == "LONG") or (venue_net < 0 and d == "SHORT")
                    if aligned and abs(venue_net) >= venue_thr:
                        reason = (
                            f"net_exposure_warning_requires_strong_signal_{venue_label} "
                            f"(net={venue_net:.0f} thr={venue_thr:.0f} dir={d})"
                        )
                        _block_candidate(
                            db=db,
                            summary=summary,
                            seq=seq,
                            candidate=candidate,
                            reason=reason,
                            venues=[venue_key],
                        )
                        blocked_warn_venues.append(venue_key)

                if blocked_warn_venues:
                    candidate_venues = [v for v in candidate_venues if v not in blocked_warn_venues]
                    if not candidate_venues:
                        continue

            # Trend/countertrend policy guard.
            countertrend_40 = False
            countertrend_non_dead_confirms = 0
            countertrend_expectancy_r: Optional[float] = None
            countertrend_expectancy_samples = 0
            trend_score: Optional[float] = None
            try:
                km = (candidate.get("context_snapshot") or {}).get("key_metrics") if isinstance(candidate.get("context_snapshot"), dict) else {}
                km = km or {}
                ts = km.get("trend_state") or {}
                trend_score = float(ts.get("trend_score")) if ts.get("trend_score") is not None else None
                regime = str(ts.get("regime") or "").upper()

                sigs = [str(s).upper().strip() for s in (candidate.get("signals") or []) if isinstance(s, str) and str(s).strip()]
                dead_only = bool(sigs) and all(s.startswith("DEAD_CAPITAL") for s in sigs)

                countertrend_40 = _ct_countertrend(direction, trend_score)
                if countertrend_40 and dead_only:
                    _block_candidate(
                        db=db,
                        summary=summary,
                        seq=seq,
                        candidate=candidate,
                        reason=(
                            f"countertrend_dead_capital_only_blocked "
                            f"(trend_score={trend_score:.0f} regime={regime or 'unknown'})"
                        ),
                        venues=candidate_venues,
                        block_count=len(candidate_venues),
                    )
                    continue

                if ct_enabled and countertrend_40:
                    if bool(ct_guard_state.get("active")):
                        _block_candidate(
                            db=db,
                            summary=summary,
                            seq=seq,
                            candidate=candidate,
                            reason=(
                                f"countertrend_guard_active "
                                f"(remaining_s={int(float(ct_guard_state.get('remaining_sec', 0.0) or 0.0))} "
                                f"manual={1 if ct_guard_state.get('manual_block') else 0})"
                            ),
                            venues=candidate_venues,
                            block_count=len(candidate_venues),
                        )
                        continue
                    countertrend_non_dead_confirms = _ct_non_dead_confirms(candidate, direction)
                    if countertrend_non_dead_confirms < ct_min_non_dead_confirmations:
                        _block_candidate(
                            db=db,
                            summary=summary,
                            seq=seq,
                            candidate=candidate,
                            reason=(
                                f"countertrend_insufficient_non_dead_confirmations "
                                f"({countertrend_non_dead_confirms}<{ct_min_non_dead_confirmations})"
                            ),
                            venues=candidate_venues,
                            block_count=len(candidate_venues),
                        )
                        continue
                    if _ct_dead_only(candidate):
                        _block_candidate(
                            db=db,
                            summary=summary,
                            seq=seq,
                            candidate=candidate,
                            reason="countertrend_dead_capital_only_blocked",
                            venues=candidate_venues,
                            block_count=len(candidate_venues),
                        )
                        continue
                    if ct_require_positive_expectancy:
                        exp_ok, countertrend_expectancy_samples, countertrend_expectancy_r = _ct_symbol_expectancy(symbol, ct_now_ts)
                        if not exp_ok:
                            _block_candidate(
                                db=db,
                                summary=summary,
                                seq=seq,
                                candidate=candidate,
                                reason=(
                                    f"countertrend_expectancy_non_positive "
                                    f"(samples={countertrend_expectancy_samples} "
                                    f"expectancy_r={countertrend_expectancy_r if countertrend_expectancy_r is not None else 'NA'})"
                                ),
                                venues=candidate_venues,
                                block_count=len(candidate_venues),
                            )
                            continue
                    if ct_max_picks > 0 and countertrend_picks_this_cycle >= ct_max_picks:
                        _block_candidate(
                            db=db,
                            summary=summary,
                            seq=seq,
                            candidate=candidate,
                            reason=f"countertrend_cycle_cap_reached ({ct_max_picks})",
                            venues=candidate_venues,
                            block_count=len(candidate_venues),
                        )
                        continue
                    countertrend_picks_this_cycle += 1
                    risk_tag = dict(candidate.get("risk") or {})
                    risk_tag["countertrend_40"] = True
                    if trend_score is not None:
                        risk_tag["countertrend_trend_score"] = round(float(trend_score), 3)
                    risk_tag["countertrend_non_dead_confirms"] = int(countertrend_non_dead_confirms)
                    if countertrend_expectancy_r is not None:
                        risk_tag["countertrend_expectancy_r"] = round(float(countertrend_expectancy_r), 6)
                    risk_tag["countertrend_expectancy_samples"] = int(countertrend_expectancy_samples)
                    candidate["risk"] = risk_tag
            except Exception as exc:
                print(f"Warning: countertrend gate failed open for {symbol}: {exc}")

            # Re-entry cooldowns (time-based): block re-entry after SL / DECAY_EXIT / EXIT / TP.
            # - SL cooldown: default 60m
            # - EXIT cooldown (review / decay): from skill.yaml exit_decider.reentry_cooldown_minutes (default 60m)
            # - TP cooldown: default 15m
            cfg_root = config.get("config", {}) if isinstance(config, dict) else {}
            exit_cfg = cfg_root.get("exit_decider", {}) if isinstance(cfg_root, dict) else {}
            sl_cooldown_minutes = _safe_float(
                exit_cfg.get("sl_reentry_cooldown_minutes", 60.0),
                60.0,
            )
            exit_cooldown_minutes = _safe_float(
                exit_cfg.get("reentry_cooldown_minutes", 60.0),
                60.0,
            )
            tp_cooldown_minutes = _safe_float(
                exit_cfg.get("tp_reentry_cooldown_minutes", 15.0),
                15.0,
            )

            # Apply in order: SL (most strict), DECAY_EXIT, EXIT (review/other closes), then TP.
            candidate_venues = await _apply_exit_cooldown(
                db=db,
                summary=summary,
                seq=seq,
                candidate=candidate,
                symbol=symbol,
                direction=direction,
                candidate_venues=candidate_venues,
                label="sl",
                reasons=["SL", "STOP_LOSS", "STOP"],
                cooldown_minutes=sl_cooldown_minutes,
            )
            if not candidate_venues:
                continue

            decay_exit_cooldown_minutes = float(
                env_str(
                    "EVCLAW_DECAY_EXIT_REENTRY_COOLDOWN_MINUTES",
                    "60",
                )
                or 0.0
            )
            # Decay exits can have multiple concrete exit_reason values.
            candidate_venues = await _apply_exit_cooldown(
                db=db,
                summary=summary,
                seq=seq,
                candidate=candidate,
                symbol=symbol,
                direction=direction,
                candidate_venues=candidate_venues,
                label="decay_exit",
                reasons=["DECAY_EXIT", "DECAY_EXIT_SIGNAL_FLIP", "DECAY_EXIT_TREND_FLIP"],
                cooldown_minutes=decay_exit_cooldown_minutes,
            )
            if not candidate_venues:
                continue

            # General (non-SL/non-TP) exits can also have many concrete exit_reason values.
            # Cooldown is same-direction only (enforced in DB query via direction).
            candidate_venues = await _apply_exit_cooldown(
                db=db,
                summary=summary,
                seq=seq,
                candidate=candidate,
                symbol=symbol,
                direction=direction,
                candidate_venues=candidate_venues,
                label="exit",
                reasons=[
                    "EXIT",
                    "HOURLY_REVIEW_DEADLY_LOSER",
                    "HOURLY_REVIEW_DEAD_FLAT",
                    "HOURLY_REVIEW_EXPOSURE_REDUCE",
                    "HOURLY_REVIEW_NO_PROGRESS",
                    "POSITION_REVIEW_NO_PROGRESS",
                    "POSITION_REVIEW_VP_CONTRARY",
                    "SIGNAL_FLIP",
                    "EXPOSURE_DRIFT",
                    "REBALANCE",
                    "CLOSE_ALL",
                    "FUNDING_ARB_ABORT_SINGLE_LEG",
                ],
                cooldown_minutes=exit_cooldown_minutes,
            )
            if not candidate_venues:
                continue

            candidate_venues = await _apply_exit_cooldown(
                db=db,
                summary=summary,
                seq=seq,
                candidate=candidate,
                symbol=symbol,
                direction=direction,
                candidate_venues=candidate_venues,
                label="tp",
                reasons=["TP"],
                cooldown_minutes=tp_cooldown_minutes,
            )
            if not candidate_venues:
                continue

            # ATR(14) 1h: primary Binance Futures, fallback Hyperliquid (cached 1h).
            atr_service = get_atr_service(str(getattr(db, "db_path", "") or ""))
            atr_result = await atr_service.get_atr(symbol, price=price)
            atr = atr_result.atr if atr_result else None

            if atr_result:
                try:
                    candidate["risk"] = dict(candidate.get("risk") or {})
                    candidate["risk"]["atr_source"] = atr_result.source
                    candidate["risk"]["atr_ts"] = atr_result.computed_at
                    candidate["risk"]["atr_interval"] = atr_result.interval
                    candidate["risk"]["atr_period"] = atr_result.period
                except Exception:
                    pass

            if not atr or atr <= 0:
                _block_candidate(
                    db=db,
                    summary=summary,
                    seq=seq,
                    candidate=candidate,
                    reason="atr_missing_for_risk",
                    venues=active_venues,
                )
                continue

            # Option A: dynamic risk% from conviction (0.5% - 2.5%)
            # NOTE: CLI risk_pct_* is no longer the sizing driver; it's only kept for backwards compat.
            risk_pct_candidate_lighter = (
                float(risk_mgr_lighter.calculate_risk_budget(conviction, symbol))
                if (risk_mgr_lighter and VENUE_LIGHTER in active_venues)
                else float(risk_pct_lighter)
            )
            risk_pct_candidate_hl = (
                float(risk_mgr_hl.calculate_risk_budget(conviction, symbol))
                if (risk_mgr_hl and VENUE_HYPERLIQUID in active_venues)
                else float(risk_pct_hyperliquid)
            )

            # Apply sizing influence from Global Context + Learning Context.
            # We implement this as a risk_pct multiplier (then clamped), which keeps the
            # sizing semantics consistent with the risk engine.
            size_mult = 1.0
            size_notes: List[str] = []

            def _sf(x, default=0.0) -> float:
                try:
                    return float(x)
                except Exception:
                    return float(default)

            # Learning context multiplier (from context learning engine).
            ctx_adj = candidate.get("context_adjustment")
            if isinstance(ctx_adj, dict):
                ctx_adj_val = _sf(ctx_adj.get("value"), 1.0)
            else:
                ctx_adj_val = _sf(ctx_adj, 1.0)
            if ctx_adj_val and ctx_adj_val != 1.0:
                # Keep bounded so learning can't blow up size.
                ctx_adj_val = max(0.6, min(1.3, ctx_adj_val))
                size_mult *= ctx_adj_val
                size_notes.append(f"ctx_adj={ctx_adj_val:.2f}")

            # HIP3 REST boosters: confidence/size booster only (never standalone trigger).
            if _is_hip3(symbol):
                try:
                    r = candidate.get("risk") or {}
                    hip3_booster_mult = float(r.get("hip3_booster_size_mult") or 1.0)
                except Exception:
                    hip3_booster_mult = 1.0
                hip3_booster_mult = max(0.70, min(1.40, hip3_booster_mult))
                if hip3_booster_mult != 1.0:
                    size_mult *= hip3_booster_mult
                    size_notes.append(f"hip3_booster_mult={hip3_booster_mult:.2f}")

            # Global context: net exposure bias.
            net_exp = 0.0
            try:
                g = candidate.get("global") or {}
                ne = (g.get("net_exposure_usd") or {}) if isinstance(g, dict) else {}
                net_exp = _sf(ne.get("value"), 0.0)
            except Exception:
                net_exp = 0.0

            exp_thr = _cfg_float(exposure_cfg, "net_exposure_bias_threshold_usd", 15000.0)
            exp_penalty = _cfg_float(exposure_cfg, "net_exposure_penalty", 0.85)
            exp_bonus = _cfg_float(exposure_cfg, "net_exposure_bonus", 1.05)
            if exp_penalty <= 0:
                exp_penalty = 0.85
            if exp_bonus <= 0:
                exp_bonus = 1.05
            if abs(net_exp) >= exp_thr:
                if net_exp < 0:  # already net short
                    if direction == "SHORT":
                        size_mult *= exp_penalty
                        size_notes.append(f"net_short_penalize_short={exp_penalty:.2f}")
                    else:
                        size_mult *= exp_bonus
                        size_notes.append(f"net_short_bonus_long={exp_bonus:.2f}")
                else:  # already net long
                    if direction == "LONG":
                        size_mult *= exp_penalty
                        size_notes.append(f"net_long_penalize_long={exp_penalty:.2f}")
                    else:
                        size_mult *= exp_bonus
                        size_notes.append(f"net_long_bonus_short={exp_bonus:.2f}")

            # Global context: Deribit IV regime.
            try:
                g = candidate.get("global") or {}
                btc_ratio = _sf(((g.get("btc_iv_ratio") or {}) if isinstance(g, dict) else {}).get("value"), 0.0)
                eth_ratio = _sf(((g.get("eth_iv_ratio") or {}) if isinstance(g, dict) else {}).get("value"), 0.0)
                btc_iv = _sf(((g.get("btc_iv_24h") or {}) if isinstance(g, dict) else {}).get("value"), 0.0)
                btc_rv = _sf(((g.get("btc_realized_vol") or {}) if isinstance(g, dict) else {}).get("value"), 0.0)
                eth_iv = _sf(((g.get("eth_iv_24h") or {}) if isinstance(g, dict) else {}).get("value"), 0.0)
                eth_rv = _sf(((g.get("eth_realized_vol") or {}) if isinstance(g, dict) else {}).get("value"), 0.0)
            except Exception:
                btc_ratio = eth_ratio = btc_iv = btc_rv = eth_iv = eth_rv = 0.0

            iv_ratio = max(btc_ratio or 0.0, eth_ratio or 0.0)
            # Fear/backwardation: reduce size.
            if iv_ratio >= 1.25:
                size_mult *= 0.85
                size_notes.append(f"iv_ratio_high={iv_ratio:.2f}*0.85")
            # Calm: slightly increase size (only if IV is not screaming above RV).
            elif 0 < iv_ratio <= 1.05 and ((btc_iv and btc_rv and btc_iv <= btc_rv) or (eth_iv and eth_rv and eth_iv <= eth_rv)):
                size_mult *= 1.05
                size_notes.append(f"iv_ratio_calm={iv_ratio:.2f}*1.05")

            # Apply multiplier to risk pct (then clamp).
            if size_mult != 1.0:
                risk_pct_candidate_lighter, _ = clamp_risk_pct(risk_pct_candidate_lighter * size_mult)
                risk_pct_candidate_hl, _ = clamp_risk_pct(risk_pct_candidate_hl * size_mult)
                try:
                    candidate["risk"] = dict(candidate.get("risk") or {})
                    candidate["risk"]["size_multiplier"] = round(size_mult, 4)
                    if size_notes:
                        candidate["risk"]["size_multiplier_notes"] = ";".join(size_notes)
                except Exception:
                    pass
            effective_size_mult = float(size_mult)

            # Allow explicit per-candidate risk override (rare / manual / AGI)
            risk_override = candidate.get("risk") if isinstance(candidate.get("risk"), dict) else {}
            # AGI-specific SL/TP ATR multiplier overrides
            sl_atr_mult_override = None
            tp_atr_mult_override = None
            if risk_override:
                override_pct = risk_override.get("risk_pct")
                if override_pct is not None:
                    risk_pct_candidate_lighter, _ = clamp_risk_pct(override_pct)
                    risk_pct_candidate_hl, _ = clamp_risk_pct(override_pct)
                # AGI trader can override SL/TP ATR multipliers
                sl_atr_mult_override = risk_override.get("sl_atr_mult")
                tp_atr_mult_override = risk_override.get("tp_atr_mult")

            if countertrend_40:
                risk_pct_candidate_lighter, _ = clamp_risk_pct(
                    risk_pct_candidate_lighter * ct_risk_multiplier,
                    min_pct=0.05,
                    max_pct=ct_risk_cap_pct,
                )
                risk_pct_candidate_hl, _ = clamp_risk_pct(
                    risk_pct_candidate_hl * ct_risk_multiplier,
                    min_pct=0.05,
                    max_pct=ct_risk_cap_pct,
                )
                effective_size_mult *= float(ct_risk_multiplier)
                try:
                    candidate["risk"] = dict(candidate.get("risk") or {})
                    candidate["risk"]["countertrend_risk_mult"] = round(float(ct_risk_multiplier), 4)
                    candidate["risk"]["countertrend_risk_cap_pct"] = round(float(ct_risk_cap_pct), 4)
                except Exception:
                    pass

            # Opt2 (LLM) size multiple: scale risk_pct (then clamp). Range [0.5, 2.0].
            llm_mult = None
            try:
                llm_mult = float(candidate.get("llm_size_mult") or (candidate.get("execution") or {}).get("size_mult") or 0.0)
            except Exception:
                llm_mult = None
            if llm_mult is not None and llm_mult > 0:
                if countertrend_40 and llm_mult > 1.0:
                    llm_mult = 1.0
                llm_mult = max(0.5, min(2.0, float(llm_mult)))
                if llm_mult != 1.0:
                    risk_pct_candidate_lighter, _ = clamp_risk_pct(risk_pct_candidate_lighter * llm_mult)
                    risk_pct_candidate_hl, _ = clamp_risk_pct(risk_pct_candidate_hl * llm_mult)
                    effective_size_mult *= float(llm_mult)
                    try:
                        candidate["risk"] = dict(candidate.get("risk") or {})
                        candidate["risk"]["llm_size_mult"] = round(llm_mult, 4)
                    except Exception:
                        pass

            if countertrend_40:
                risk_pct_candidate_lighter, _ = clamp_risk_pct(
                    risk_pct_candidate_lighter,
                    min_pct=0.05,
                    max_pct=ct_risk_cap_pct,
                )
                risk_pct_candidate_hl, _ = clamp_risk_pct(
                    risk_pct_candidate_hl,
                    min_pct=0.05,
                    max_pct=ct_risk_cap_pct,
                )

            hl_sizing_equity = hl_equity

            # Enforce risk sizing; ignore candidate size_usd unless explicitly forced.
            force_size = bool(candidate.get("force_size_usd"))
            size_override = float(candidate.get("size_usd") or 0.0)
            size_usd_lighter = 0.0
            size_usd_hl = 0.0
            if force_size and size_override > 0:
                if VENUE_LIGHTER in active_venues:
                    size_usd_lighter = size_override
                if VENUE_HYPERLIQUID in active_venues:
                    size_usd_hl = size_override
            else:
                # Use AGI override SL multiplier if provided, otherwise use config default
                sl_mult_for_sizing = float(sl_atr_mult_override or exec_config.sl_atr_multiplier)
                if VENUE_LIGHTER in active_venues:
                    size_usd_lighter = compute_risk_size_usd(
                        equity=lighter_equity,
                        risk_pct=risk_pct_candidate_lighter,
                        atr=atr,
                        sl_multiplier=sl_mult_for_sizing,
                        price=price,
                    )
                if VENUE_HYPERLIQUID in active_venues:
                    size_usd_hl = compute_risk_size_usd(
                        equity=hl_sizing_equity,
                        risk_pct=risk_pct_candidate_hl,
                        atr=atr,
                        sl_multiplier=sl_mult_for_sizing,
                        price=price,
                    )

                    # Hard cap per-position notional as % of equity.
                    pct_cap = _normalize_pct_cap(getattr(risk_config, 'max_position_pct', 25.0), 25.0)
                    size_usd_hl, clamp_reason = _apply_position_pct_cap(
                        size_usd=float(size_usd_hl or 0.0),
                        equity_usd=float(hl_sizing_equity or 0.0),
                        pct_cap=float(pct_cap or 0.0),
                        reason_tag="max_position_pct_cap",
                        clamp_reason=clamp_reason,
                    )

            try:
                risk_meta = dict(candidate.get("risk") or {})
                risk_meta["size_multiplier"] = round(float(size_mult), 4)
                risk_meta["size_multiplier_effective"] = round(float(effective_size_mult), 4)
                if llm_mult is not None and llm_mult > 0:
                    risk_meta["llm_size_mult"] = round(float(llm_mult), 4)
                risk_meta["countertrend_40"] = bool(countertrend_40)
                if countertrend_40:
                    if trend_score is not None:
                        risk_meta["countertrend_trend_score"] = round(float(trend_score), 3)
                    risk_meta["countertrend_non_dead_confirms"] = int(countertrend_non_dead_confirms)
                    risk_meta["countertrend_expectancy_samples"] = int(countertrend_expectancy_samples)
                    if countertrend_expectancy_r is not None:
                        risk_meta["countertrend_expectancy_r"] = round(float(countertrend_expectancy_r), 6)
                risk_meta["risk_pct_used_by_venue"] = {
                    VENUE_LIGHTER: float(risk_pct_candidate_lighter),
                    VENUE_HYPERLIQUID: float(risk_pct_candidate_hl),
                }
                risk_meta["equity_at_entry_by_venue"] = {
                    VENUE_LIGHTER: float(lighter_equity or 0.0),
                    VENUE_HYPERLIQUID: float(hl_sizing_equity or 0.0),
                }
                candidate["risk"] = risk_meta
            except Exception:
                pass

            # Hard net-exposure cap (HL-only): if this trade would INCREASE |HL net| beyond cap,
            # block HL venue for this candidate (but still allow Lighter if present).
            if (
                hl_net_cap is not None
                and projected_hl_net_notional is not None
                and VENUE_HYPERLIQUID in candidate_venues
                and not _is_hip3(symbol)
            ):
                d = str(direction or '').upper()
                delta = float(size_usd_hl or 0.0) * (1.0 if d == 'LONG' else -1.0)
                proposed = float(projected_hl_net_notional) + delta
                # Allow trades that reduce absolute net even if cap is exceeded.
                if abs(proposed) > float(hl_net_cap) and abs(proposed) >= abs(float(projected_hl_net_notional)):
                    reason = (
                        f"net_exposure_cap (hl_net={projected_hl_net_notional:.0f} -> {proposed:.0f} "
                        f"cap={hl_net_cap:.0f} mult={max_net_exposure_mult:.2f})"
                    )
                    _block_candidate(
                        db=db,
                        summary=summary,
                        seq=seq,
                        candidate=candidate,
                        reason=reason,
                        venues=[VENUE_HYPERLIQUID],
                        size_overrides={VENUE_HYPERLIQUID: float(size_usd_hl or 0.0)},
                    )
                    # Remove HL venue for this candidate and continue evaluation.
                    candidate_venues = [v for v in candidate_venues if v != VENUE_HYPERLIQUID]
                    if not candidate_venues:
                        continue

            if (
                (VENUE_LIGHTER in candidate_venues and size_usd_lighter <= 0)
                or (VENUE_HYPERLIQUID in candidate_venues and size_usd_hl <= 0)
            ):
                # Sizing debug: record why size went to zero.
                try:
                    debug = {
                        "symbol": symbol,
                        "direction": direction,
                        "venues": list(candidate_venues),
                        "price": price,
                        "atr": atr,
                        "sl_mult": sl_mult_for_sizing if 'sl_mult_for_sizing' in locals() else None,
                        "risk_pct_hl": risk_pct_candidate_hl,
                        "risk_pct_lighter": risk_pct_candidate_lighter,
                        "equity_hl": hl_equity,
                        "size_usd_hl": size_usd_hl,
                        "size_usd_lighter": size_usd_lighter,
                        "size_mult": (candidate.get("risk") or {}).get("size_multiplier") if isinstance(candidate.get("risk"), dict) else None,
                        "size_mult_notes": (candidate.get("risk") or {}).get("size_multiplier_notes") if isinstance(candidate.get("risk"), dict) else None,
                    }
                    append_jsonl(
                        str(Path(EVCLAW_DOCS_DIR) / "llm_decisions.jsonl"),
                        {
                            "ts_utc": _utc_now(),
                            "kind": "SIZING_DEBUG",
                            "seq": int(seq),
                            "debug": debug,
                        },
                    )
                    candidate["risk"] = dict(candidate.get("risk") or {})
                    candidate["risk"]["sizing_debug"] = debug
                except Exception:
                    pass

                _block_candidate(
                    db=db,
                    summary=summary,
                    seq=seq,
                    candidate=candidate,
                    reason="risk_size_invalid",
                    venues=candidate_venues,
                    size_overrides={
                        VENUE_LIGHTER: float(size_usd_lighter or 0.0),
                        VENUE_HYPERLIQUID: float(size_usd_hl or 0.0),
                    },
                )
                continue

            sanity_cap = float(exec_config.max_position_per_symbol_usd or 0.0)
            size_by_venue = {
                VENUE_LIGHTER: float(size_usd_lighter or 0.0),
                VENUE_HYPERLIQUID: float(size_usd_hl or 0.0),
            }
            size_by_venue, clamp_notes = _apply_sanity_cap_for_venues(
                size_by_venue=size_by_venue,
                enabled_venues=active_venues,
                sanity_cap=sanity_cap,
            )
            size_usd_lighter = float(size_by_venue.get(VENUE_LIGHTER) or 0.0)
            size_usd_hl = float(size_by_venue.get(VENUE_HYPERLIQUID) or 0.0)
            if clamp_notes:
                clamp_reason = _append_reason_note(clamp_reason, "; ".join(clamp_notes))

            remaining = exec_config.max_total_exposure_usd - projected_total_exposure
            requested_total, size_overrides = _build_requested_size_overrides(
                candidate_venues=candidate_venues,
                size_by_venue=size_by_venue,
            )

            if requested_total > remaining:
                _block_candidate(
                    db=db,
                    summary=summary,
                    seq=seq,
                    candidate=candidate,
                    reason=(
                        f"exposure_cap (need {requested_total:.2f}, remaining {remaining:.2f}; "
                        f"projected={projected_total_exposure:.2f})"
                    ),
                    size_overrides=size_overrides,
                    venues=candidate_venues,
                )
                continue

            # Duplicate position check (venue-aware)
            # We support Hyperliquid perps + HIP3 wallet holding the same symbol/direction in parallel (mirroring).
            # Only block a venue if that SAME venue already has the same-direction position.
            existing = await executor.get_all_positions()
            same_dir_venues = set()
            opposite_pos = None
            dust_notional = float(getattr(exec_config, "dust_notional_usd", 0.0) or 0.0)
            for pos in existing.values():
                if pos.venue and pos.venue not in active_venues:
                    continue
                if (pos.state or "").upper() == "DUST":
                    continue
                notional = (pos.size or 0.0) * (pos.entry_price or 0.0)
                if dust_notional > 0 and notional < dust_notional:
                    continue
                if pos.symbol.upper() != symbol:
                    continue

                pos_dir = (pos.direction or "").upper()
                pos_venue = (pos.venue or "").lower()
                if pos_dir == direction and pos_venue in candidate_venues:
                    same_dir_venues.add(pos_venue)
                if pos_dir in ("LONG", "SHORT") and pos_dir != direction:
                    # If opposite exists anywhere, we may need flip-handling (HIP3).
                    opposite_pos = pos

            if _is_hip3(symbol) and opposite_pos is not None:
                hip3_main = None
                try:
                    sig_snap = candidate.get("signals_snapshot") or {}
                    if isinstance(sig_snap, dict):
                        hip3_main = sig_snap.get("hip3_main")
                except Exception:
                    hip3_main = None
                venue = opposite_pos.venue or VENUE_HYPERLIQUID
                await _maybe_emit_hip3_flip_review(
                    db=db,
                    symbol=symbol,
                    venue=venue,
                    trade_id=getattr(opposite_pos, "trade_id", None),
                    existing_dir=(opposite_pos.direction or "").upper(),
                    new_dir=direction,
                    hip3_main=hip3_main,
                )
                _block_candidate(
                    db=db,
                    summary=summary,
                    seq=seq,
                    candidate=candidate,
                    reason="hip3_main_flip_review",
                    venues=candidate_venues,
                )
                continue

            if same_dir_venues:
                # Block only the duplicated venues; allow remaining venues to proceed.
                _block_candidate(
                    db=db,
                    summary=summary,
                    seq=seq,
                    candidate=candidate,
                    reason="duplicate_position_exists",
                    venues=sorted(list(same_dir_venues)),
                    block_count=len(same_dir_venues),
                )
                candidate_venues = [v for v in candidate_venues if v not in same_dir_venues]
                if not candidate_venues:
                    continue

            # In-cycle duplicate guard: prevent duplicate proposals before guardian/execution runs.
            in_cycle_duplicates = {
                v
                for v in candidate_venues
                if (symbol.upper(), str(direction or "").upper(), str(v or "").lower()) in approved_keys
            }
            if in_cycle_duplicates:
                _block_candidate(
                    db=db,
                    summary=summary,
                    seq=seq,
                    candidate=candidate,
                    reason="duplicate_proposal_in_cycle",
                    venues=sorted(list(in_cycle_duplicates)),
                    block_count=len(in_cycle_duplicates),
                )
                candidate_venues = [v for v in candidate_venues if v not in in_cycle_duplicates]
                if not candidate_venues:
                    continue

            # Backfill deterministic execution routing metadata for every kept proposal.
            # This guarantees CLI can route limit/chase correctly even when LLM gate metadata is absent.
            effective_order_type = _ensure_deterministic_execution_metadata(
                candidate,
                resolve_order_type_fn=resolve_order_type,
                conviction_config=conviction_cfg,
            )
            if effective_order_type == "reject":
                _block_candidate(
                    db=db,
                    summary=summary,
                    seq=seq,
                    candidate=candidate,
                    reason="conviction_below_min",
                    venues=candidate_venues,
                    size_overrides=size_overrides,
                )
                continue

            # Always record proposals; OpenClaw agent handles execution.
            proposal_ids = insert_proposals(
                db,
                seq,
                candidate,
                "PROPOSED",
                clamp_reason,
                size_overrides=size_overrides,
                venues=candidate_venues,
            )
            successful_venues = [v for v in candidate_venues if v in proposal_ids]
            failed_venues = [v for v in candidate_venues if v not in proposal_ids]
            if failed_venues:
                summary["counts"]["failed"] = summary["counts"].get("failed", 0) + len(failed_venues)
                print(
                    f"Proposal metadata persistence failed: {symbol} {direction} "
                    f"venues={','.join(sorted(str(v) for v in failed_venues))}"
                )
            if not successful_venues:
                continue
            summary["counts"]["proposed"] += 1
            accepted_total = sum(float(size_overrides.get(v) or 0.0) for v in successful_venues)
            projected_total_exposure += accepted_total
            dir_sign = 1.0 if str(direction or "").upper() == "LONG" else -1.0
            if VENUE_HYPERLIQUID in successful_venues and projected_hl_net_notional is not None:
                projected_hl_net_notional += float(size_usd_hl or 0.0) * dir_sign
            if VENUE_LIGHTER in successful_venues and projected_lighter_net_notional is not None:
                projected_lighter_net_notional += float(size_usd_lighter or 0.0) * dir_sign
            for v in successful_venues:
                approved_keys.add((symbol.upper(), str(direction or "").upper(), str(v or "").lower()))
            continue

        # Determine cycle status based on proposals
        if summary["counts"]["proposed"] > 0:
            summary["status"] = "PROPOSED"
        elif summary["counts"]["blocked"] > 0:
            summary["status"] = "BLOCKED"
        else:
            summary["status"] = "NO_CANDIDATES"
        return 0, summary
    finally:
        if own_executor:
            await executor.close()
            # Close aiohttp session used by ATRService singleton in one-shot invocations.
            # (Long-running `--loop/--continuous` runs won't hit this path until shutdown.)
            try:
                await get_atr_service().close()
            except Exception:
                pass
        # deps object is intentionally not "closed"; caller controls lifecycle for db/tracker.
