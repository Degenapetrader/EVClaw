#!/usr/bin/env python3
"""Tests for proposal metadata execution normalization."""

import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from ai_trader_db import AITraderDB
from conviction_model import resolve_order_type
from proposal_writer import insert_proposals


def _build_candidate(*, conviction: float, execution=None):
    return {
        "symbol": "ETH",
        "direction": "LONG",
        "size_usd": 1000.0,
        "conviction": conviction,
        "reason_short": "test",
        "signals": ["cvd"],
        "execution": execution,
    }


def test_insert_proposals_normalizes_missing_execution_for_proposed() -> None:
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db = AITraderDB(f.name)

    candidate = _build_candidate(conviction=0.55, execution=None)
    proposal_ids = insert_proposals(
        db,
        seq=1,
        candidate=candidate,
        status="PROPOSED",
        reason=None,
        venues=["hyperliquid"],
    )
    proposal_id = proposal_ids["hyperliquid"]
    meta = db.get_proposal_metadata(proposal_id)

    expected_order_type = resolve_order_type(0.55)
    assert meta["execution"]["order_type"] == expected_order_type
    assert meta["execution"]["source"] == "proposal_writer_deterministic_policy"
    assert meta["execution_validation_status"] == "normalized_missing_order_type"


def test_insert_proposals_normalizes_invalid_execution_order_type_for_proposed() -> None:
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db = AITraderDB(f.name)

    candidate = _build_candidate(
        conviction=0.88,
        execution={"order_type": "ioc", "source": "llm_gate"},
    )
    proposal_ids = insert_proposals(
        db,
        seq=2,
        candidate=candidate,
        status="PROPOSED",
        reason=None,
        venues=["hyperliquid"],
    )
    proposal_id = proposal_ids["hyperliquid"]
    meta = db.get_proposal_metadata(proposal_id)

    assert meta["execution"]["order_type"] == resolve_order_type(0.88)
    assert meta["execution"]["source"] == "llm_gate"
    assert meta["execution_validation_status"] == "normalized_invalid_order_type"


def test_insert_proposals_allows_missing_execution_for_blocked() -> None:
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db = AITraderDB(f.name)

    candidate = _build_candidate(conviction=0.1, execution=None)
    proposal_ids = insert_proposals(
        db,
        seq=3,
        candidate=candidate,
        status="BLOCKED",
        reason="conviction_below_min",
        venues=["hyperliquid"],
    )
    proposal_id = proposal_ids["hyperliquid"]
    meta = db.get_proposal_metadata(proposal_id)

    assert meta["execution"] is None
    assert meta["execution_validation_status"] == "missing_non_proposed"


def test_insert_proposals_marks_failed_when_metadata_write_fails_for_proposed(monkeypatch) -> None:
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db = AITraderDB(f.name)

    candidate = _build_candidate(conviction=0.66, execution={"order_type": "limit"})

    def _boom(_proposal_id, _meta):
        raise RuntimeError("disk_full")

    monkeypatch.setattr(db, "insert_proposal_metadata", _boom)

    proposal_ids = insert_proposals(
        db,
        seq=4,
        candidate=candidate,
        status="PROPOSED",
        reason=None,
        venues=["hyperliquid"],
    )

    assert proposal_ids == {}
    rows = db.get_proposals_for_cycle(4)
    assert len(rows) == 1
    assert rows[0]["status"] == "FAILED"
    assert "proposal_metadata_write_failed" in str(rows[0]["status_reason"] or "")


def test_insert_proposals_uses_runtime_conviction_config_for_fallback_order_type() -> None:
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db = AITraderDB(f.name)

    db.insert_conviction_config_snapshot(
        params={"chase_threshold": 0.85, "limit_min": 0.40},
        source="test_runtime",
        activate=True,
    )

    candidate = _build_candidate(conviction=0.75, execution=None)
    proposal_ids = insert_proposals(
        db,
        seq=5,
        candidate=candidate,
        status="PROPOSED",
        reason=None,
        venues=["hyperliquid"],
    )
    proposal_id = proposal_ids["hyperliquid"]
    meta = db.get_proposal_metadata(proposal_id)

    assert meta["execution"]["order_type"] == "limit"


def test_insert_proposals_persists_gate_mode_into_context_and_risk() -> None:
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db = AITraderDB(f.name)

    candidate = _build_candidate(conviction=0.82, execution={"order_type": "chase_limit"})
    candidate["symbol"] = "XYZ:NVDA"
    candidate["gate_mode"] = "hip3"
    candidate["risk"] = {"hip3_driver": "flow"}
    candidate["context_snapshot"] = {"key_metrics": {"price": 500.0}}

    proposal_ids = insert_proposals(
        db,
        seq=6,
        candidate=candidate,
        status="PROPOSED",
        reason=None,
        venues=["hip3"],
    )
    proposal_id = proposal_ids["hip3"]
    meta = db.get_proposal_metadata(proposal_id)

    assert meta["entry_gate_mode"] == "hip3"
    assert meta["strategy_segment"] == "hip3"
    assert meta["risk"]["entry_gate_mode"] == "hip3"
    assert meta["risk"]["gate_mode"] == "hip3"
    assert meta["context_snapshot"]["entry_gate_mode"] == "hip3"
    assert meta["context_snapshot"]["strategy_segment"] == "hip3"
    assert isinstance(meta["context_snapshot"].get("risk"), dict)


def test_insert_proposals_links_gate_decision_to_proposal_row() -> None:
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db = AITraderDB(f.name)

    gate_decision_id = db.insert_gate_decision(
        cycle_seq=7001,
        symbol="ETH",
        direction="LONG",
        decision="PICK",
        reason="test link",
        candidate_rank=1,
        conviction=0.73,
    )
    candidate = _build_candidate(conviction=0.73, execution={"order_type": "chase_limit"})
    candidate["gate_decision_id"] = gate_decision_id

    proposal_ids = insert_proposals(
        db,
        seq=7001,
        candidate=candidate,
        status="PROPOSED",
        reason=None,
        venues=["hyperliquid"],
    )
    proposal_id = proposal_ids["hyperliquid"]

    gate_rows = db.get_gate_decisions_for_cycle(7001)
    assert len(gate_rows) == 1
    gate_row = gate_rows[0]
    assert int(gate_row["id"]) == int(gate_decision_id)
    assert int(gate_row["proposal_id"] or 0) == int(proposal_id)
    assert str(gate_row["venue"] or "").lower() == "hyperliquid"
