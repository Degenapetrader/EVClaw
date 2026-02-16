#!/usr/bin/env python3
"""Cycle orchestration helpers extracted from live_agent."""

from __future__ import annotations

import os
import sqlite3
import time
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Dict, Optional, Tuple

import yaml
from env_utils import EVCLAW_DOCS_DIR, env_str


_SKILL_FILE = Path(__file__).with_name("skill.yaml")


def _load_skill_hip3_cfg() -> Dict[str, Any]:
    try:
        with _SKILL_FILE.open("r", encoding="utf-8") as f:
            raw = yaml.safe_load(f) or {}
        if not isinstance(raw, dict):
            return {}
        cfg = raw.get("config") or {}
        if not isinstance(cfg, dict):
            return {}
        hip3 = cfg.get("hip3") or {}
        return hip3 if isinstance(hip3, dict) else {}
    except Exception:
        return {}


def _load_skill_exposure_cfg() -> Dict[str, Any]:
    try:
        with _SKILL_FILE.open("r", encoding="utf-8") as f:
            raw = yaml.safe_load(f) or {}
        if not isinstance(raw, dict):
            return {}
        cfg = raw.get("config") or {}
        if not isinstance(cfg, dict):
            return {}
        exposure = cfg.get("exposure") or {}
        return exposure if isinstance(exposure, dict) else {}
    except Exception:
        return {}


def _load_skill_guardian_cfg() -> Dict[str, Any]:
    try:
        with _SKILL_FILE.open("r", encoding="utf-8") as f:
            raw = yaml.safe_load(f) or {}
        if not isinstance(raw, dict):
            return {}
        cfg = raw.get("config") or {}
        if not isinstance(cfg, dict):
            return {}
        guardian = cfg.get("guardian") or {}
        return guardian if isinstance(guardian, dict) else {}
    except Exception:
        return {}


def _skill_float(cfg: Dict[str, Any], key: str, default: float) -> float:
    try:
        value = cfg.get(key, default)
        return float(default if value is None else value)
    except Exception:
        return float(default)


def _skill_bool(cfg: Dict[str, Any], key: str, default: bool) -> bool:
    raw = cfg.get(key, default)
    if raw is None:
        return bool(default)
    if isinstance(raw, bool):
        return raw
    return str(raw).strip().lower() in ("1", "true", "yes", "y", "on")


_HIP3_CFG = _load_skill_hip3_cfg()
_EXPOSURE_CFG = _load_skill_exposure_cfg()
_GUARDIAN_CFG = _load_skill_guardian_cfg()
MAX_NET_EXPOSURE_MULT = _skill_float(_EXPOSURE_CFG, "max_net_exposure_mult", 2.0)
AUTO_GUARDIAN_ENABLED = _skill_bool(_GUARDIAN_CFG, "auto_guardian", True)


async def generate_cycle_candidates_file(
    *,
    seq: int,
    cycle_file: str,
    context_json_file: str,
    output_file: str,
    db: Optional[Any],
    db_path_override: Optional[str],
    dry_run: bool,
    use_llm: bool,
    llm_model: Optional[str],
    summary: Dict[str, Any],
    api: Any,
) -> bool:
    load_context_file = api.load_context_file
    load_cycle_file = api.load_cycle_file
    resolve_db_path = api.resolve_db_path
    ai_trader_db_cls = api.ai_trader_db_cls
    build_candidates_from_context = api.build_candidates_from_context
    max_candidates = api.MAX_CANDIDATES
    get_learning_engine = api.get_learning_engine
    apply_learning_overlay = api.apply_learning_overlay
    runtime_conviction_config = api.runtime_conviction_config
    annotate_conviction_fields = api.annotate_conviction_fields
    run_db_call = api.run_db_call
    compact_open_positions_for_gate = api.compact_open_positions_for_gate
    enrich_candidates_atr_pct = api.enrich_candidates_atr_pct
    run_entry_gate = api.run_entry_gate
    append_jsonl = api.append_jsonl
    utc_now = api.utc_now
    atomic_write_json = api.atomic_write_json

    context_payload = load_context_file(context_json_file)
    if not context_payload:
        summary["status"] = "FAILED"
        summary["reason"] = "invalid_context_json"
        return False

    try:
        cycle_data = load_cycle_file(cycle_file)
    except Exception:
        summary["status"] = "FAILED"
        summary["reason"] = "cycle_file_unreadable"
        return False

    symbols = cycle_data.get("symbols", {}) or {}
    if not isinstance(symbols, dict) or not symbols:
        summary["status"] = "FAILED"
        summary["reason"] = "cycle_symbols_missing"
        return False

    resolved_db_path = str(getattr(db, "db_path", "") or db_path_override or resolve_db_path(dry_run))
    local_db = db or ai_trader_db_cls(resolved_db_path)

    if use_llm:
        try:
            from llm_decider import LLMDecider

            decider = LLMDecider(db_path=resolved_db_path, model=llm_model)
            candidates = await decider.build_candidates(
                context_payload=context_payload,
                cycle_symbols=symbols,
                max_candidates=max_candidates,
            )
        except Exception as exc:
            candidates = build_candidates_from_context(
                seq=int(seq),
                context_payload=context_payload,
                cycle_symbols=symbols,
                db=local_db,
            )
            print(f"Warning: llm_decider failed; falling back to deterministic candidate build: {exc}")
    else:
        candidates = build_candidates_from_context(
            seq=int(seq),
            context_payload=context_payload,
            cycle_symbols=symbols,
            db=local_db,
        )

    if candidates:
        learning_engine = get_learning_engine(resolved_db_path)
        if learning_engine:
            candidates = [apply_learning_overlay(c, learning_engine) for c in candidates]
        conviction_cfg = runtime_conviction_config(local_db)
        annotate_conviction_fields(candidates, conviction_config=conviction_cfg)

    llm_gate = None
    try:
        global_compact = str(context_payload.get("global_context_compact") or "")
        exposure_ctx: Dict[str, Any] = {
            "notes": "Net cap is symmetric (+/-). Per operator policy, net cap blocks SAME-DIRECTION adds only.",
        }
        # HIP3 balance policy context (used by dedicated HIP3 entry gate).
        hip3_target_abs_pct = max(0.0, _skill_float(_HIP3_CFG, "target_net_pct", 20.0))
        hip3_soft_band_pct = max(0.0, _skill_float(_HIP3_CFG, "soft_band_pct", 15.0))
        hip3_hard_band_pct = max(0.0, _skill_float(_HIP3_CFG, "hard_band_pct", 30.0))
        try:
            snap = (
                await run_db_call(local_db.get_latest_monitor_snapshot)
                if hasattr(local_db, "get_latest_monitor_snapshot")
                else None
            )
        except Exception:
            snap = None
        max_mult = float(MAX_NET_EXPOSURE_MULT if MAX_NET_EXPOSURE_MULT > 0 else 2.0)
        if isinstance(snap, dict) and snap:
            try:
                hl_eq = float(snap.get("hl_equity") or 0.0)
            except Exception:
                hl_eq = 0.0
            try:
                hl_net = float(snap.get("hl_net_notional") or 0.0)
            except Exception:
                hl_net = 0.0
            cap_abs = hl_eq * max_mult if hl_eq > 0 and max_mult > 0 else 0.0
            try:
                hl_wallet_eq = float(snap.get("hl_wallet_equity") or 0.0)
            except Exception:
                hl_wallet_eq = 0.0
            try:
                hl_wallet_net = float(snap.get("hl_wallet_net_notional") or 0.0)
            except Exception:
                hl_wallet_net = 0.0
            if hl_wallet_eq > 0:
                hip3_current_net_pct = (hl_wallet_net / hl_wallet_eq) * 100.0
            else:
                hip3_current_net_pct = None
            hip3_band_state = "unknown"
            if hip3_current_net_pct is not None:
                abs_pct = abs(float(hip3_current_net_pct))
                if abs_pct >= hip3_hard_band_pct:
                    hip3_band_state = "hard_breach"
                elif abs_pct >= hip3_soft_band_pct:
                    hip3_band_state = "soft_breach"
                else:
                    hip3_band_state = "within_band"
            exposure_ctx.update(
                {
                    "hl_equity_usd": hl_eq,
                    "hl_net_exposure_usd": hl_net,
                    "hl_net_cap_abs_usd": cap_abs,
                    "hl_net_cap_long_usd": cap_abs,
                    "hl_net_cap_short_usd": -cap_abs,
                    "max_net_exposure_mult": max_mult,
                    "hl_wallet_equity_usd": hl_wallet_eq,
                    "hl_wallet_net_exposure_usd": hl_wallet_net,
                    "hip3_balance": {
                        "target_net_abs_pct": hip3_target_abs_pct,
                        "target_sign_policy": "llm_decides_from_context",
                        "soft_band_pct": hip3_soft_band_pct,
                        "hard_band_pct": hip3_hard_band_pct,
                        "current_net_pct": hip3_current_net_pct,
                        "band_state": hip3_band_state,
                        "priority": "balance_first_volume_second",
                    },
                }
            )
        # HIP3 volume progress (daily notional target vs 14d baseline).
        try:
            with sqlite3.connect(resolved_db_path, timeout=10.0) as conn:
                today_notional = conn.execute(
                    """
                    SELECT COALESCE(SUM(ABS(COALESCE(notional_usd, 0))), 0)
                    FROM trades
                    WHERE UPPER(symbol) LIKE 'XYZ:%'
                      AND entry_time >= strftime('%s','now','start of day')
                      AND entry_time < strftime('%s','now','start of day','+1 day')
                    """
                ).fetchone()[0]
                rows = conn.execute(
                    """
                    SELECT COALESCE(SUM(ABS(COALESCE(notional_usd, 0))), 0) AS day_notional
                    FROM trades
                    WHERE UPPER(symbol) LIKE 'XYZ:%'
                      AND entry_time >= strftime('%s','now','start of day','-14 day')
                      AND entry_time < strftime('%s','now','start of day')
                    GROUP BY date(entry_time,'unixepoch')
                    """
                ).fetchall()
            day_vals = [float(r[0] or 0.0) for r in (rows or [])]
            baseline_daily = (sum(day_vals) / len(day_vals)) if day_vals else 0.0
            target_daily = baseline_daily * 1.25 if baseline_daily > 0 else 0.0
            progress = (float(today_notional) / target_daily) if target_daily > 0 else None
            exposure_ctx["hip3_volume"] = {
                "today_notional_usd": float(today_notional or 0.0),
                "baseline_14d_daily_usd": float(baseline_daily),
                "target_daily_usd": float(target_daily),
                "target_uplift_pct": 25.0,
                "progress_ratio": float(progress) if progress is not None else None,
            }
        except Exception:
            pass
        exposure_ctx["open_positions"] = compact_open_positions_for_gate(local_db)

        try:
            await enrich_candidates_atr_pct(candidates, db_path=resolved_db_path)
        except Exception:
            pass

        def _is_hip3_candidate(item: Dict[str, Any]) -> bool:
            sym = str((item or {}).get("symbol") or "").upper().strip()
            return sym.startswith("XYZ:")

        def _deterministic_pick(item: Dict[str, Any], *, reason: str, gate_mode: str) -> Dict[str, Any]:
            return {
                "symbol": str(item.get("symbol") or "").upper().strip(),
                "direction": str(item.get("direction") or "").upper().strip(),
                "reason": reason,
                "gate_mode": gate_mode,
            }

        hip3_candidates = [c for c in candidates if _is_hip3_candidate(c)]
        normal_candidates = [c for c in candidates if not _is_hip3_candidate(c)]

        subgate_payloads: Dict[str, Any] = {}
        merged_picks: list[Dict[str, Any]] = []
        merged_rejects: list[Dict[str, Any]] = []
        any_gate_enabled = False
        any_gate_applied = False

        for gate_mode, subset in (("normal", normal_candidates), ("hip3", hip3_candidates)):
            if not subset:
                continue
            decision = await run_entry_gate(
                seq=int(seq),
                global_context_compact=global_compact,
                exposure_context=exposure_ctx,
                candidates=subset,
                mode=gate_mode,
            )
            mode_enabled = bool(getattr(decision, "enabled", False))
            mode_error = str(getattr(decision, "error", "") or "").strip() or None
            mode_session_id = str(getattr(decision, "session_id", "") or "").strip() or None
            if gate_mode == "hip3":
                mode_agent_id = (
                    env_str("EVCLAW_HIP3_LLM_GATE_AGENT_ID", "")
                    or env_str("EVCLAW_LLM_GATE_AGENT_ID", "evclaw-entry-gate")
                    or "evclaw-hip3-entry-gate"
                ).strip() or "evclaw-hip3-entry-gate"
                mode_model = (
                    env_str("EVCLAW_HIP3_LLM_GATE_MODEL", "")
                    or "openai-codex/gpt-5.2"
                ).strip() or None
            else:
                mode_agent_id = (
                    env_str("EVCLAW_LLM_GATE_AGENT_ID", "evclaw-entry-gate")
                    or "evclaw-entry-gate"
                ).strip() or "evclaw-entry-gate"
                mode_model = (
                    env_str("EVCLAW_LLM_GATE_MODEL", "")
                    or "openai-codex/gpt-5.2"
                ).strip() or None

            subgate_payloads[gate_mode] = {
                "enabled": mode_enabled,
                "error": mode_error,
                "session_id": mode_session_id,
                "agent_id": mode_agent_id,
                "model": mode_model,
                "candidate_count": len(subset),
                "raw_text": getattr(decision, "raw_text", None),
            }
            any_gate_enabled = any_gate_enabled or mode_enabled

            if mode_enabled and not mode_error:
                any_gate_applied = True
                for p in list(getattr(decision, "picks", []) or []):
                    row = dict(p)
                    row["gate_mode"] = gate_mode
                    if mode_session_id:
                        row["session_id"] = mode_session_id
                    if mode_agent_id:
                        row["agent_id"] = mode_agent_id
                    if mode_model:
                        row["model"] = mode_model
                    merged_picks.append(row)
                for r in list(getattr(decision, "rejects", []) or []):
                    row = dict(r)
                    row["gate_mode"] = gate_mode
                    if mode_session_id:
                        row["session_id"] = mode_session_id
                    if mode_agent_id:
                        row["agent_id"] = mode_agent_id
                    if mode_model:
                        row["model"] = mode_model
                    merged_rejects.append(row)
            else:
                # Gate disabled/error fallback: keep deterministic flow for this subset.
                fallback_reason = (
                    f"{gate_mode}_gate_bypassed:{mode_error}" if mode_error else f"{gate_mode}_gate_disabled"
                )
                for c in subset:
                    merged_picks.append(_deterministic_pick(c, reason=fallback_reason, gate_mode=gate_mode))

        if any_gate_enabled:
            session_ids = [
                str(v.get("session_id") or "").strip()
                for v in subgate_payloads.values()
                if isinstance(v, dict) and str(v.get("session_id") or "").strip()
            ]
            llm_gate = {
                "enabled": True,
                "agent_id": "multi_gate" if len(subgate_payloads) > 1 else (
                    next(iter(subgate_payloads.values())).get("agent_id") if subgate_payloads else None
                ),
                "model": "multi_gate" if len(subgate_payloads) > 1 else (
                    next(iter(subgate_payloads.values())).get("model") if subgate_payloads else None
                ),
                "session_id": ",".join(session_ids) if session_ids else f"hl_entry_gate_{int(seq)}",
                "picks": merged_picks,
                "rejects": merged_rejects,
                "exposure": exposure_ctx,
                "subgates": subgate_payloads,
                "partial_bypass": bool(any_gate_enabled and not any_gate_applied),
            }
    except Exception as exc:
        llm_gate = None
        print(f"Warning: entry gate invocation failed; continuing with deterministic flow: {exc}")

    if llm_gate and (llm_gate.get("enabled") or llm_gate.get("error")):
        try:
            append_jsonl(
                str(Path(EVCLAW_DOCS_DIR) / "llm_decisions.jsonl"),
                {
                    "ts_utc": utc_now(),
                    "kind": "ENTRY_GATE",
                    "seq": int(seq),
                    "global_context": str(context_payload.get("global_context_compact") or ""),
                    "exposure": (llm_gate or {}).get("exposure") if isinstance(llm_gate, dict) else None,
                    "candidates": [
                        {
                            "symbol": c.get("symbol"),
                            "direction": c.get("direction"),
                            "conviction": c.get("conviction"),
                            "pipeline_conviction": c.get("pipeline_conviction"),
                            "brain_conviction": c.get("brain_conviction"),
                            "blended_conviction": c.get("blended_conviction"),
                            "signals": c.get("signals"),
                            "rank": c.get("rank"),
                        }
                        for c in (candidates or [])
                    ],
                    "llm_gate": llm_gate,
                },
            )
            try:
                p = Path(EVCLAW_DOCS_DIR) / "openclawdiary.md"
                p.parent.mkdir(parents=True, exist_ok=True)
                kept = [
                    f"{k.get('symbol')}:{k.get('direction')}"
                    for k in (llm_gate.get("picks") or [])
                    if isinstance(k, dict)
                ]
                with p.open("a", encoding="utf-8") as f:
                    f.write(f"[{utc_now()}] LLM_GATE seq={int(seq)} picks={kept}\n")
            except Exception:
                pass
        except Exception:
            pass

    payload = {
        "schema_version": 1,
        "cycle_seq": int(seq),
        "context_file": context_json_file,
        "generated_at": utc_now(),
        "candidates": candidates,
        "llm_gate": llm_gate,
        "notes": "auto-selected from JSON context",
        "errors": [],
    }
    atomic_write_json(output_file, payload)
    return True


def annotate_cycle_llm_gate_status(
    *,
    proc_summary: Dict[str, Any],
    output_file: str,
    gate_enabled: bool,
    api: Any,
) -> None:
    load_candidates_file = api.load_candidates_file

    llm_gate_payload = None
    try:
        payload_for_summary = load_candidates_file(output_file)
        if isinstance(payload_for_summary, dict):
            llm_gate_payload = payload_for_summary.get("llm_gate")
    except Exception:
        llm_gate_payload = None
    llm_gate_enabled = bool(
        isinstance(llm_gate_payload, dict) and llm_gate_payload.get("enabled") is True
    )
    llm_gate_error = None
    if isinstance(llm_gate_payload, dict) and llm_gate_payload.get("error"):
        llm_gate_error = str(llm_gate_payload.get("error"))
    llm_gate_bypassed = bool(gate_enabled and not llm_gate_enabled)
    proc_summary["llm_gate_config_enabled"] = bool(gate_enabled)
    proc_summary["llm_gate_enabled"] = bool(llm_gate_enabled)
    proc_summary["llm_gate_bypassed"] = bool(llm_gate_bypassed)
    proc_summary["llm_gate_bypass_reason"] = (
        llm_gate_error if llm_gate_bypassed else None
    ) or ("llm_gate_missing_or_disabled" if llm_gate_bypassed else None)


async def run_cycle_gate_step(
    *,
    seq: int,
    cycle_file: str,
    output_file: str,
    dry_run: bool,
    risk_pct_lighter: float,
    risk_pct_hyperliquid: float,
    db_path_override: Optional[str],
    shared_deps: Optional[Any],
    shared_executor: Optional[Any],
    gate_enabled: bool,
    api: Any,
) -> Tuple[int, Dict[str, Any]]:
    process_candidates = api.process_candidates

    rc, proc_summary = await process_candidates(
        seq=int(seq),
        cycle_file=cycle_file,
        candidates_file=output_file,
        dry_run=dry_run,
        risk_pct_lighter=risk_pct_lighter,
        risk_pct_hyperliquid=risk_pct_hyperliquid,
        db_path_override=db_path_override,
        deps=shared_deps,
        executor=shared_executor,
    )

    annotate_cycle_llm_gate_status(
        proc_summary=proc_summary,
        output_file=output_file,
        gate_enabled=gate_enabled,
        api=api,
    )

    auto_guardian = AUTO_GUARDIAN_ENABLED
    if auto_guardian and not dry_run:
        try:
            from argparse import Namespace
            from cli import cmd_guardian

            guardian_rc = await cmd_guardian(
                Namespace(cycle_file=str(cycle_file), seq=int(seq), dry_run=bool(dry_run)),
                preloaded_db=shared_deps.db if shared_deps is not None else None,
                preloaded_executor=shared_executor,
            )
            if int(guardian_rc or 0) != 0:
                proc_summary["auto_guardian"] = False
                proc_summary["auto_guardian_error"] = f"guardian_rc={guardian_rc}"
                proc_summary["status"] = "FAILED"
                proc_summary["reason"] = "auto_guardian_failed"
                rc = 1
            else:
                proc_summary["auto_guardian"] = True
        except Exception as exc:
            proc_summary["auto_guardian"] = False
            proc_summary["auto_guardian_error"] = str(exc)
            proc_summary["status"] = "FAILED"
            proc_summary["reason"] = "auto_guardian_failed"
            rc = 1

    return rc, proc_summary


async def run_cycle_impl(
    seq: int,
    cycle_file: str,
    context_json_file: str,
    dry_run: bool,
    risk_pct_lighter: float,
    risk_pct_hyperliquid: float,
    candidates_file: Optional[str] = None,
    db: Optional[Any] = None,
    reuse_existing: bool = True,
    db_path_override: Optional[str] = None,
    context_file: Optional[str] = None,
    use_llm: bool = False,
    llm_model: Optional[str] = None,
    api: Optional[Any] = None,
) -> Tuple[int, Dict[str, Any]]:
    if api is None:
        raise RuntimeError("run_cycle_impl requires api")

    load_context_file = api.load_context_file
    resolve_context_generated_ts = api.resolve_context_generated_ts
    default_context_max_age_seconds = api.DEFAULT_CONTEXT_MAX_AGE_SECONDS
    default_candidates_file = api.default_candidates_file
    entry_gate_enabled = api.entry_gate_enabled
    mark_busy = api.mark_busy
    mark_idle = api.mark_idle
    should_reuse_candidates_file = api.should_reuse_candidates_file
    run_db_call = api.run_db_call
    build_deps = api.build_deps
    build_executor_with_learning = api.build_executor_with_learning
    get_atr_service = api.get_atr_service

    summary: Dict[str, Any] = {"seq": int(seq), "status": "UNKNOWN", "reason": None}

    if not cycle_file or not Path(cycle_file).exists():
        summary["status"] = "FAILED"
        summary["reason"] = "cycle_file_missing"
        return 1, summary

    if not context_json_file or not Path(context_json_file).exists():
        summary["status"] = "FAILED"
        summary["reason"] = "context_json_missing"
        return 1, summary
    context_path = Path(context_json_file)
    context_payload = load_context_file(context_json_file)
    if not isinstance(context_payload, dict) or not context_payload:
        summary["status"] = "FAILED"
        summary["reason"] = "invalid_context_json"
        return 1, summary
    context_generated_ts = resolve_context_generated_ts(context_payload, context_path)
    now_ts = time.time()
    context_age_sec: Optional[float] = None
    if context_generated_ts is not None:
        context_age_sec = max(0.0, now_ts - float(context_generated_ts))
    summary["context_age_sec"] = round(float(context_age_sec), 3) if context_age_sec is not None else None
    summary["context_max_age_sec"] = float(default_context_max_age_seconds)
    if context_age_sec is not None and context_age_sec > float(default_context_max_age_seconds):
        summary["status"] = "FAILED"
        summary["reason"] = "context_stale"
        return 1, summary

    output_file = candidates_file or default_candidates_file(int(seq))
    gate_enabled = entry_gate_enabled()

    mark_busy(int(seq))
    shared_deps: Optional[Any] = None
    shared_executor: Optional[Any] = None
    try:
        use_existing = reuse_existing and should_reuse_candidates_file(
            output_file,
            gate_enabled=gate_enabled,
        )

        if not use_existing:
            generated = await generate_cycle_candidates_file(
                seq=int(seq),
                cycle_file=cycle_file,
                context_json_file=context_json_file,
                output_file=output_file,
                db=db,
                db_path_override=db_path_override,
                dry_run=dry_run,
                use_llm=use_llm,
                llm_model=llm_model,
                summary=summary,
                api=api,
            )
            if not generated:
                return 1, summary

        if db:
            await run_db_call(db.update_cycle_candidates_file, int(seq), output_file)

        try:
            shared_deps = build_deps(dry_run=dry_run, db_path_override=db_path_override)
            shared_executor = await build_executor_with_learning(
                shared_deps.exec_config,
                shared_deps.tracker,
                db_path=shared_deps.db_path,
            )
        except Exception as exc:
            shared_deps = None
            shared_executor = None
            print(f"Warning: shared executor init failed; using isolated candidate executor: {exc}")

        rc, proc_summary = await run_cycle_gate_step(
            seq=int(seq),
            cycle_file=cycle_file,
            output_file=output_file,
            dry_run=dry_run,
            risk_pct_lighter=risk_pct_lighter,
            risk_pct_hyperliquid=risk_pct_hyperliquid,
            db_path_override=db_path_override,
            shared_deps=shared_deps,
            shared_executor=shared_executor,
            gate_enabled=gate_enabled,
            api=api,
        )

        proc_summary["cycle_file"] = cycle_file
        proc_summary["context_file"] = context_file
        proc_summary["context_json_file"] = context_json_file
        proc_summary["candidates_file"] = output_file
        return rc, proc_summary
    finally:
        if shared_executor is not None:
            try:
                await shared_executor.close()
            except Exception:
                pass
            try:
                await get_atr_service().close()
            except Exception:
                pass
        mark_idle(int(seq))
