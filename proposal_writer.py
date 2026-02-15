from __future__ import annotations

import logging
import time
from typing import Any, Dict, List, Optional

from ai_trader_db import AITraderDB
from conviction_model import resolve_order_type_runtime
from exchanges import VENUE_HYPERLIQUID, VENUE_LIGHTER
from proposal_status import (
    PROPOSAL_STATUS_APPROVED,
    PROPOSAL_STATUS_BLOCKED,
    PROPOSAL_STATUS_FAILED,
    PROPOSAL_STATUS_PROPOSED,
    normalize_proposal_status,
)

LOG = logging.getLogger(__name__)


def _safe_float(value: Any, default: Optional[float] = None) -> Optional[float]:
    try:
        out = float(value)
    except Exception:
        return default
    if out != out:
        return default
    return out


def _normalize_execution_metadata(
    *,
    db: Optional[AITraderDB],
    candidate: Dict[str, Any],
    status_norm: str,
    conviction: Any,
) -> tuple[Optional[Dict[str, Any]], str]:
    """Normalize execution metadata for proposal persistence."""
    raw_execution = candidate.get("execution")
    execution = dict(raw_execution) if isinstance(raw_execution, dict) else None
    meta_order_type = ""
    if isinstance(execution, dict):
        meta_order_type = str(execution.get("order_type") or "").strip().lower()
    allowed = {"limit", "chase_limit", "reject"}

    blended_conv = candidate.get("blended_conviction")
    route_conv = blended_conv if blended_conv is not None else conviction
    conv_val = _safe_float(route_conv, None)
    deterministic_order_type = "reject"
    if conv_val is not None:
        resolved = str(resolve_order_type_runtime(conv_val, db=db) or "reject").strip().lower()
        deterministic_order_type = resolved if resolved in allowed else "reject"

    if status_norm != PROPOSAL_STATUS_PROPOSED:
        if execution is None:
            return None, "missing_non_proposed"
        if meta_order_type in allowed:
            return execution, "valid_non_proposed"
        return execution, "invalid_non_proposed"

    # PROPOSED rows must persist deterministic execution.order_type.
    normalized = execution or {}
    if meta_order_type in allowed:
        validation_status = "valid"
        normalized_order_type = meta_order_type
    else:
        validation_status = "normalized_missing_order_type" if not meta_order_type else "normalized_invalid_order_type"
        normalized_order_type = deterministic_order_type
        normalized["source"] = str(normalized.get("source") or "").strip() or "proposal_writer_deterministic_policy"

    normalized["order_type"] = normalized_order_type
    if "conviction_source" not in normalized:
        normalized["conviction_source"] = "blended" if blended_conv is not None else "proposal_conviction"
    if conv_val is not None:
        normalized["blended_conviction"] = float(conv_val)
    if normalized_order_type == "limit":
        normalized.setdefault("limit_style", "sr_limit")
        normalized.setdefault("limit_fallback", "atr_1x")
    return normalized, validation_status


def insert_proposals(
    db: AITraderDB,
    seq: int,
    candidate: Dict[str, Any],
    status: str,
    reason: Optional[str],
    size_overrides: Optional[Dict[str, float]] = None,
    venues: Optional[List[str]] = None,
) -> Dict[str, int]:
    """Insert proposals for selected venues; return proposal IDs per venue."""
    symbol = str(candidate.get("symbol") or "UNKNOWN").upper()
    direction = str(candidate.get("direction") or "LONG").upper()
    try:
        size_usd = float(candidate.get("size_usd") or 0.0)
    except Exception:
        size_usd = 0.0
    conviction = candidate.get("conviction")
    reason_short = candidate.get("reason_short") or ""
    signals = candidate.get("signals") or []
    sl = candidate.get("sl")
    tp = candidate.get("tp")
    gate_decision_id = candidate.get("gate_decision_id")
    try:
        gate_decision_id = int(gate_decision_id) if gate_decision_id is not None else None
    except Exception:
        gate_decision_id = None
    gate_session_id = str(candidate.get("gate_session_id") or "").strip() or None
    gate_decision_reason = str(candidate.get("gate_decision_reason") or "").strip() or None
    status_norm = normalize_proposal_status(status, default=str(status or "").upper()) or str(status or "").upper()
    normalized_execution, execution_validation_status = _normalize_execution_metadata(
        db=db,
        candidate=candidate,
        status_norm=status_norm,
        conviction=conviction,
    )
    gate_mode = str(
        candidate.get("entry_gate_mode")
        or candidate.get("gate_mode")
        or ((normalized_execution or {}).get("gate_mode") if isinstance(normalized_execution, dict) else "")
        or ""
    ).strip().lower()
    if gate_mode not in {"hip3", "normal"}:
        gate_mode = "hip3" if symbol.startswith("XYZ:") else "normal"

    strategy_segment = "hip3" if symbol.startswith("XYZ:") else "perp"
    context_snapshot = candidate.get("context_snapshot")
    context_snapshot = dict(context_snapshot) if isinstance(context_snapshot, dict) else context_snapshot
    risk_meta = candidate.get("risk")
    risk_meta = dict(risk_meta) if isinstance(risk_meta, dict) else {}
    if gate_mode and "entry_gate_mode" not in risk_meta:
        risk_meta["entry_gate_mode"] = gate_mode
    if gate_mode and "gate_mode" not in risk_meta:
        risk_meta["gate_mode"] = gate_mode
    if isinstance(context_snapshot, dict):
        context_snapshot.setdefault("entry_gate_mode", gate_mode)
        context_snapshot.setdefault("strategy_segment", strategy_segment)
        context_risk = context_snapshot.get("risk")
        context_risk = dict(context_risk) if isinstance(context_risk, dict) else {}
        for key, value in risk_meta.items():
            context_risk.setdefault(key, value)
        if context_risk:
            context_snapshot["risk"] = context_risk

    meta = {
        "strong_signals": candidate.get("strong_signals"),
        "must_trade": candidate.get("must_trade"),
        "constraint_reason": candidate.get("constraint_reason"),
        "signals_snapshot": candidate.get("signals_snapshot"),
        "context_snapshot": context_snapshot,
        "symbol_learning_dossier": candidate.get("symbol_learning_dossier"),
        "playbook_id": candidate.get("playbook_id"),
        "memory_ids": candidate.get("memory_ids"),
        "execution": normalized_execution,
        "execution_validation_status": execution_validation_status,
        "risk": risk_meta or candidate.get("risk"),
        "entry_gate_mode": gate_mode,
        "strategy_segment": strategy_segment,
        "rank": candidate.get("rank"),
        "force_size_usd": candidate.get("force_size_usd"),
        "gate_decision_id": gate_decision_id,
        "gate_session_id": gate_session_id,
        "gate_decision_reason": gate_decision_reason,
        # Audit helpers for approvals/rejections
        "approve_reason": reason_short or None,
        "reject_reason": reason
        if status_norm in {PROPOSAL_STATUS_BLOCKED, PROPOSAL_STATUS_FAILED}
        else None,
    }

    proposal_ids: Dict[str, int] = {}
    target_venues = venues or [VENUE_LIGHTER, VENUE_HYPERLIQUID]
    for venue in target_venues:
        venue_size = size_usd
        if size_overrides and venue in size_overrides:
            venue_size = float(size_overrides[venue])
        proposal_id = db.insert_proposal(
            cycle_seq=seq,
            symbol=symbol,
            venue=venue,
            direction=direction,
            size_usd=venue_size,
            sl=sl,
            tp=tp,
            conviction=conviction,
            reason_short=reason_short,
            signals=signals,
        )
        if gate_decision_id is not None and hasattr(db, "link_gate_decision_to_proposal"):
            try:
                linked = bool(
                    db.link_gate_decision_to_proposal(
                        gate_decision_id=int(gate_decision_id),
                        proposal_id=int(proposal_id),
                        venue=str(venue or "").lower() or None,
                    )
                )
                if not linked:
                    LOG.warning(
                        "Gate decision proposal link returned false: gate_decision_id=%s proposal_id=%s venue=%s",
                        gate_decision_id,
                        proposal_id,
                        venue,
                    )
            except Exception as exc:
                LOG.warning(
                    "Gate decision proposal link failed: gate_decision_id=%s proposal_id=%s venue=%s err=%s",
                    gate_decision_id,
                    proposal_id,
                    venue,
                    exc,
                )
        # Persist approve/reject rationale in a query-friendly way.
        status_reason = reason
        if status_norm in {PROPOSAL_STATUS_PROPOSED, PROPOSAL_STATUS_APPROVED}:
            parts = []
            if reason_short:
                parts.append(str(reason_short))
            if reason:
                parts.append(str(reason))
            status_reason = " | ".join([p for p in parts if p]) or None
        db.update_proposal_status(proposal_id, status, status_reason)
        metadata_error = None
        try:
            persisted = bool(db.insert_proposal_metadata(proposal_id, meta))
            if not persisted:
                metadata_error = "insert_proposal_metadata_returned_false"
        except Exception as exc:
            metadata_error = f"{type(exc).__name__}: {exc}"

        if metadata_error is not None:
            fail_reason = f"proposal_metadata_write_failed ({metadata_error})"
            LOG.error(
                "Proposal metadata write failed: proposal_id=%s seq=%s symbol=%s venue=%s status=%s error=%s",
                proposal_id,
                seq,
                symbol,
                venue,
                status_norm,
                metadata_error,
            )
            if status_norm in {PROPOSAL_STATUS_PROPOSED, PROPOSAL_STATUS_APPROVED}:
                try:
                    db.update_proposal_status(proposal_id, PROPOSAL_STATUS_FAILED, fail_reason)
                except Exception:
                    pass
                continue

        proposal_ids[venue] = proposal_id

    return proposal_ids




def record_execution_block(
    db: AITraderDB,
    proposal_id: int,
    reason: str,
    venue: Optional[str] = None,
) -> None:
    ts = time.time()
    db.insert_execution_result(
        proposal_id=proposal_id,
        started_at=ts,
        finished_at=ts,
        success=False,
        error=reason,
        exchange=venue,
        entry_price=None,
        size=None,
        sl_price=None,
        tp_price=None,
    )
    db.update_proposal_status(proposal_id, "BLOCKED", reason)
