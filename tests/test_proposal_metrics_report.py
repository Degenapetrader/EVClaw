#!/usr/bin/env python3
"""Tests for proposal metrics report queries."""

from __future__ import annotations

import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from ai_trader_db import AITraderDB
from proposal_metrics_report import collect_proposal_metrics


def test_collect_proposal_metrics_includes_status_and_gate_counts() -> None:
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    db = AITraderDB(db_path)

    p1 = db.insert_proposal(
        cycle_seq=5001,
        symbol="ETH",
        venue="hyperliquid",
        direction="LONG",
        size_usd=300.0,
        conviction=0.7,
        reason_short="proposal-1",
        signals=["CVD:LONG z=2.0"],
    )
    p2 = db.insert_proposal(
        cycle_seq=5001,
        symbol="BTC",
        venue="hyperliquid",
        direction="SHORT",
        size_usd=350.0,
        conviction=0.65,
        reason_short="proposal-2",
        signals=["FADE:SHORT z=2.1"],
    )
    p3 = db.insert_proposal(
        cycle_seq=5001,
        symbol="SOL",
        venue="hyperliquid",
        direction="LONG",
        size_usd=280.0,
        conviction=0.6,
        reason_short="proposal-3",
        signals=["WHALE:LONG str=0.7"],
    )
    assert p1 > 0 and p2 > 0 and p3 > 0

    assert db.update_proposal_status(p2, "BLOCKED", "Sector limit reached") is True
    assert db.update_proposal_status(p3, "EXECUTED", "executed") is True

    gate_pick_id = db.insert_gate_decision(
        cycle_seq=5001,
        symbol="ETH",
        direction="LONG",
        decision="PICK",
        reason="good setup",
        candidate_rank=1,
        conviction=0.72,
    )
    db.insert_gate_decision(
        cycle_seq=5001,
        symbol="BTC",
        direction="SHORT",
        decision="REJECT",
        reason="sector concentration",
        candidate_rank=2,
        conviction=0.58,
    )

    trade_id = db.log_trade_entry(
        symbol="ETH",
        direction="LONG",
        entry_price=3000.0,
        size=1.0,
        venue="hyperliquid",
        context_snapshot={"entry_gate": {"gate_decision_id": gate_pick_id}},
    )
    db.log_trade_exit(trade_id, exit_price=3030.0, exit_reason="TP", total_fees=0.0)
    assert db.finalize_gate_outcome_from_trade(trade_id) is True

    out = collect_proposal_metrics(db_path=db_path, window_hours=24, top_n=5)
    counts = out.get("proposal_status_counts") or {}
    gate_counts = out.get("gate_decision_counts") or {}
    pick_outcomes = out.get("gate_pick_outcomes") or {}
    pick_rate = out.get("gate_pick_rate_window") or {}

    assert int(counts.get("PROPOSED", 0)) == 1
    assert int(counts.get("BLOCKED", 0)) == 1
    assert int(counts.get("EXECUTED", 0)) == 1
    assert int(gate_counts.get("PICK", 0)) == 1
    assert int(gate_counts.get("REJECT", 0)) == 1
    assert int(pick_outcomes.get("picks_total", 0)) == 1
    assert int(pick_outcomes.get("picks_with_outcome", 0)) == 1
    assert float(pick_outcomes.get("pick_win_rate_pct", 0.0)) == 100.0
    assert int(pick_rate.get("candidates_decided", 0)) == 2
    assert float(pick_rate.get("pick_rate_pct", 0.0)) == 50.0

    block_rows = out.get("top_block_reasons") or []
    assert any(str(r.get("reason") or "") == "Sector limit reached" for r in block_rows)
