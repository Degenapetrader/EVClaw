#!/usr/bin/env python3
"""Candidate payload/gate helpers extracted from live_agent."""

from __future__ import annotations

from typing import Any, Callable, Dict, List, Optional, Tuple


def build_process_summary(seq: int) -> Dict[str, Any]:
    return {
        "seq": int(seq),
        "status": "UNKNOWN",
        "reason": None,
        "counts": {
            "valid": 0,
            "invalid": 0,
            "dropped": 0,
            "blocked": 0,
            "proposed": 0,
            "approved": 0,
            "executed": 0,
            "failed": 0,
            "sl_cooldown_blocked": 0,
        },
    }


def load_and_validate_candidates_payload(
    *,
    candidates_file: str,
    db: Any,
    summary: Dict[str, Any],
    seq: int,
    venues: List[str],
    load_candidates_file_fn: Callable[[str], Any],
    validate_candidates_payload_fn: Callable[[Any], Dict[str, Any]],
    block_candidate_fn: Callable[..., None],
) -> Tuple[Optional[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]:
    payload = load_candidates_file_fn(candidates_file)
    if not payload:
        print("Invalid or missing candidates JSON; recording BLOCKED placeholders.")
        placeholder = {
            "symbol": "INVALID_CANDIDATES",
            "direction": "LONG",
            "size_usd": 0.0,
            "conviction": 0.0,
            "reason_short": "invalid candidates file",
            "signals": [],
        }
        block_candidate_fn(
            db=db,
            summary=summary,
            seq=seq,
            candidate=placeholder,
            reason="invalid_candidates_json",
            venues=venues,
        )
        summary["status"] = "FAILED"
        summary["reason"] = "invalid_candidates_json"
        return None, [], []

    validation = validate_candidates_payload_fn(payload)
    valid_candidates = validation.get("valid", [])
    invalid_candidates = validation.get("invalid", [])
    summary["counts"]["valid"] = len(valid_candidates)
    summary["counts"]["invalid"] = len(invalid_candidates)
    return payload, valid_candidates, invalid_candidates


def apply_llm_gate_selection(
    *,
    payload: Dict[str, Any],
    db: Any,
    summary: Dict[str, Any],
    seq: int,
    active_venues: List[str],
    valid_candidates: List[Dict[str, Any]],
    invalid_candidates: List[Dict[str, Any]],
    safe_float_fn: Callable[[Any, float], float],
    block_candidate_fn: Callable[..., None],
    record_invalid_candidates_fn: Callable[..., None],
    resolve_order_type_fn: Callable[..., str],
    conviction_config: Optional[Any] = None,
) -> Tuple[List[Dict[str, Any]], bool]:
    llm_gate = payload.get("llm_gate") if isinstance(payload, dict) else None
    if not (isinstance(llm_gate, dict) and llm_gate.get("enabled") is True):
        return valid_candidates, False
    gate_summary = summary.setdefault("llm_gate", {})
    if isinstance(gate_summary, dict):
        gate_summary.update(
            {
                "enabled": True,
                "status": "EVALUATING",
                "applied": False,
                "fallback_mode": None,
                "fallback_reason": None,
            }
        )
    llm_agent_id = str(llm_gate.get("agent_id") or "").strip() or None
    llm_model = str(llm_gate.get("model") or "").strip() or None
    llm_session_id = str(llm_gate.get("session_id") or "").strip() or None

    picks = llm_gate.get("picks")
    rejects = llm_gate.get("rejects")
    if picks is None and "keep" in llm_gate:
        picks = llm_gate.get("keep")
    if rejects is None and "veto" in llm_gate:
        rejects = llm_gate.get("veto")
    if not isinstance(picks, list) or not isinstance(rejects, list):
        if isinstance(gate_summary, dict):
            gate_summary.update(
                {
                    "status": "INVALID_SCHEMA",
                    "fallback_mode": "deterministic_policy",
                    "fallback_reason": "picks_rejects_not_lists",
                    "pick_count": len(valid_candidates or []),
                    "reject_count": 0,
                }
            )
        return valid_candidates, False

    pick_map: Dict[Tuple[str, str], Dict[str, Any]] = {}
    reject_map: Dict[Tuple[str, str], Dict[str, Any]] = {}

    def _key(d: Dict[str, Any]) -> Tuple[str, str]:
        return (
            str(d.get("symbol") or "").upper().strip(),
            str(d.get("direction") or "").upper().strip(),
        )

    schema_ok = True
    for it in picks:
        if not isinstance(it, dict):
            schema_ok = False
            break
        k = _key(it)
        if not k[0] or k[1] not in {"LONG", "SHORT"}:
            schema_ok = False
            break
        if not str(it.get("reason") or "").strip():
            schema_ok = False
            break
        pick_map[k] = it

    if schema_ok:
        for it in rejects:
            if not isinstance(it, dict):
                schema_ok = False
                break
            k = _key(it)
            if not k[0] or k[1] not in {"LONG", "SHORT"}:
                schema_ok = False
                break
            reason = str(it.get("reason") or it.get("note") or "").strip()
            if not reason:
                schema_ok = False
                break
            reject_map[k] = {
                "reason": reason,
                "agent_id": str(it.get("agent_id") or "").strip() or None,
                "model": str(it.get("model") or "").strip() or None,
                "session_id": str(it.get("session_id") or "").strip() or None,
            }

    cand_keys = {
        (str(c.get("symbol") or "").upper(), str(c.get("direction") or "").upper())
        for c in (valid_candidates or [])
        if isinstance(c, dict)
    }
    decided_keys = set(pick_map.keys()) | set(reject_map.keys())
    if not schema_ok or (cand_keys and decided_keys != cand_keys):
        reason = "invalid_item_schema" if not schema_ok else "coverage_mismatch"
        if isinstance(gate_summary, dict):
            gate_summary.update(
                {
                    "status": "INVALID_SCHEMA" if not schema_ok else "INVALID_COVERAGE",
                    "fallback_mode": "deterministic_policy",
                    "fallback_reason": reason,
                    "candidate_count": len(cand_keys),
                    "decided_count": len(decided_keys),
                    "pick_count": len(pick_map),
                    "reject_count": len(reject_map),
                }
            )
        return valid_candidates, False

    kept_after_gate: List[Dict[str, Any]] = []
    rejected_after_gate: List[Dict[str, Any]] = []

    for cand in valid_candidates:
        k = (str(cand.get("symbol") or "").upper(), str(cand.get("direction") or "").upper())
        if k in pick_map:
            ov = pick_map.get(k) or {}
            pick_reason = str(ov.get("reason") or "").strip()
            llm_agent_id_row = str(ov.get("agent_id") or llm_agent_id or "").strip() or None
            llm_model_row = str(ov.get("model") or llm_model or "").strip() or None
            llm_session_id_row = str(ov.get("session_id") or llm_session_id or "").strip() or None
            blended_conv = safe_float_fn(cand.get("blended_conviction", cand.get("conviction")), 0.0)
            gate_decision_id = None
            try:
                if db is not None and hasattr(db, "insert_gate_decision"):
                    gate_decision_id = db.insert_gate_decision(
                        cycle_seq=int(seq),
                        symbol=k[0],
                        direction=k[1],
                        decision="PICK",
                        reason=pick_reason or None,
                        candidate_rank=int(cand.get("rank")) if cand.get("rank") is not None else None,
                        conviction=safe_float_fn(cand.get("conviction"), 0.0),
                        blended_conviction=safe_float_fn(
                            cand.get("blended_conviction", cand.get("conviction")), 0.0
                        ),
                        pipeline_conviction=safe_float_fn(
                            cand.get("pipeline_conviction", cand.get("conviction")), 0.0
                        ),
                        brain_conviction=safe_float_fn(cand.get("brain_conviction"), 0.0),
                        llm_agent_id=llm_agent_id_row,
                        llm_model=llm_model_row,
                        llm_session_id=llm_session_id_row,
                    )
            except Exception:
                gate_decision_id = None

            if gate_decision_id is not None:
                try:
                    cand["gate_decision_id"] = int(gate_decision_id)
                except Exception:
                    cand["gate_decision_id"] = gate_decision_id
            if llm_session_id_row:
                cand["gate_session_id"] = llm_session_id_row
            if pick_reason:
                cand["gate_decision_reason"] = pick_reason
            gate_mode = str(ov.get("gate_mode") or "").strip().lower()
            if gate_mode not in {"hip3", "normal"}:
                gate_mode = "hip3" if k[0].startswith("XYZ:") else "normal"
            cand["gate_mode"] = gate_mode
            cand["entry_gate_mode"] = gate_mode

            ot = resolve_order_type_fn(blended_conv, config=conviction_config)
            routing = summary.setdefault("conviction_routing", {"chase_limit": 0, "limit": 0, "reject": 0})
            routing[str(ot)] = int(routing.get(str(ot), 0)) + 1
            if ot == "reject":
                block_candidate_fn(
                    db=db,
                    summary=summary,
                    seq=seq,
                    candidate=cand,
                    reason=f"blended_conviction_below_min:{blended_conv:.4f}",
                    venues=active_venues,
                )
                continue

            try:
                sm = float(ov.get("size_mult") or 1.0)
            except Exception:
                sm = 1.0
            sm = max(0.5, min(2.0, sm))
            cand["llm_size_mult"] = sm
            cand["llm_pick_reason"] = pick_reason

            try:
                cand["execution"] = dict(cand.get("execution") or {})
                cand["execution"].update(
                    {
                        "source": "llm_gate",
                        "order_type": ot,
                        "pick_reason": cand.get("llm_pick_reason"),
                        "size_mult": sm,
                        "conviction_source": "blended",
                        "blended_conviction": blended_conv,
                        "gate_mode": gate_mode,
                    }
                )
                if ot == "limit":
                    cand["execution"].update(
                        {
                            "limit_style": "sr_limit",
                            "limit_fallback": "atr_1x",
                        }
                    )
            except Exception:
                pass

            kept_after_gate.append(cand)
        else:
            rejected_after_gate.append(cand)

    for cand in rejected_after_gate:
        k = (str(cand.get("symbol") or "").upper(), str(cand.get("direction") or "").upper())
        rej_meta = reject_map.get(k) or {}
        reason = str(rej_meta.get("reason") or "llm_gate_reject")
        llm_agent_id_row = str(rej_meta.get("agent_id") or llm_agent_id or "").strip() or None
        llm_model_row = str(rej_meta.get("model") or llm_model or "").strip() or None
        llm_session_id_row = str(rej_meta.get("session_id") or llm_session_id or "").strip() or None
        gate_decision_id = None
        try:
            if db is not None and hasattr(db, "insert_gate_decision"):
                gate_decision_id = db.insert_gate_decision(
                    cycle_seq=int(seq),
                    symbol=k[0],
                    direction=k[1],
                    decision="REJECT",
                    reason=reason,
                    candidate_rank=int(cand.get("rank")) if cand.get("rank") is not None else None,
                    conviction=safe_float_fn(cand.get("conviction"), 0.0),
                    blended_conviction=safe_float_fn(
                        cand.get("blended_conviction", cand.get("conviction")), 0.0
                    ),
                    pipeline_conviction=safe_float_fn(
                        cand.get("pipeline_conviction", cand.get("conviction")), 0.0
                    ),
                    brain_conviction=safe_float_fn(cand.get("brain_conviction"), 0.0),
                    llm_agent_id=llm_agent_id_row,
                    llm_model=llm_model_row,
                    llm_session_id=llm_session_id_row,
                )
        except Exception:
            gate_decision_id = None

        if gate_decision_id is not None:
            try:
                cand["gate_decision_id"] = int(gate_decision_id)
            except Exception:
                cand["gate_decision_id"] = gate_decision_id
        if llm_session_id_row:
            cand["gate_session_id"] = llm_session_id_row
        if reason:
            cand["gate_decision_reason"] = reason
        block_candidate_fn(
            db=db,
            summary=summary,
            seq=seq,
            candidate=cand,
            reason=f"llm_gate_reject: {reason}",
            venues=active_venues,
        )

    valid_candidates = kept_after_gate
    summary["counts"]["valid"] = len(valid_candidates)
    if isinstance(gate_summary, dict):
        gate_summary.update(
            {
                "status": "APPLIED",
                "applied": True,
                "pick_count": len(pick_map),
                "reject_count": len(reject_map),
                "kept_count": len(valid_candidates),
                "rejected_count": len(rejected_after_gate),
            }
        )
    if valid_candidates:
        return valid_candidates, False

    record_invalid_candidates_fn(
        db=db,
        summary=summary,
        seq=seq,
        invalid_candidates=invalid_candidates,
        venues=active_venues,
    )
    print("LLM gate rejected all candidates.")
    summary["status"] = "NO_CANDIDATES"
    summary["reason"] = "llm_gate_reject_all"
    if isinstance(gate_summary, dict):
        gate_summary["status"] = "REJECT_ALL"
    return valid_candidates, True


def adaptive_candidate_limit(
    candidates: List[Dict[str, Any]],
    *,
    min_k: int,
    max_k: int,
    score_gate: float,
    conviction_gate: float,
    max_candidates: int,
    safe_float_fn: Callable[[Any, float], float],
) -> int:
    strong_count = len(
        [c for c in candidates if isinstance(c, dict) and (c.get("must_trade") or bool(c.get("strong_signals")))]
    )
    max_cap = max(1, int(max_k), int(strong_count))
    min_cap = max(1, min(int(min_k), max_cap))
    quality_count = 0
    for cand in candidates:
        if not isinstance(cand, dict):
            continue
        if cand.get("must_trade") or cand.get("strong_signals"):
            quality_count += 1
            continue
        score = 0.0
        try:
            ctx = cand.get("context_snapshot") or {}
            if isinstance(ctx, dict):
                score = safe_float_fn(ctx.get("score"), 0.0)
            if score <= 0:
                score = safe_float_fn(cand.get("score"), 0.0)
        except Exception:
            score = 0.0
        conv = safe_float_fn(cand.get("blended_conviction", cand.get("conviction")), 0.0)
        if score >= float(score_gate) or conv >= float(conviction_gate):
            quality_count += 1

    target = max(min_cap, int(strong_count), int(quality_count))
    return max(1, min(target, max_cap))


def enforce_candidate_limit_and_audit(
    *,
    db: Any,
    summary: Dict[str, Any],
    seq: int,
    active_venues: List[str],
    valid_candidates: List[Dict[str, Any]],
    invalid_candidates: List[Dict[str, Any]],
    adaptive_candidate_limit_fn: Callable[[List[Dict[str, Any]]], int],
    enforce_max_candidates_fn: Callable[[List[Dict[str, Any]], int], Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]],
    block_candidate_fn: Callable[..., None],
    record_invalid_candidates_fn: Callable[..., None],
) -> List[Dict[str, Any]]:
    limit = adaptive_candidate_limit_fn(valid_candidates)
    kept, dropped = enforce_max_candidates_fn(valid_candidates, limit)
    summary["counts"]["dropped"] = len(dropped)
    summary["counts"]["blocked"] += len(dropped)
    if dropped:
        for cand in dropped:
            block_candidate_fn(
                db=db,
                summary=summary,
                seq=seq,
                candidate=cand,
                reason="candidate_limit_exceeded",
                venues=active_venues,
            )

    record_invalid_candidates_fn(
        db=db,
        summary=summary,
        seq=seq,
        invalid_candidates=invalid_candidates,
        venues=active_venues,
    )
    return kept
