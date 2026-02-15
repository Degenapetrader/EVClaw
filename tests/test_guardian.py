#!/usr/bin/env python3
"""Tests for guardian dry-run behavior."""

import json
import sqlite3
import sys
from argparse import Namespace
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import asyncio

import cli
from ai_trader_db import AITraderDB


def test_guardian_dry_run_updates_status(tmp_path, monkeypatch) -> None:
    """Dry-run guardian should write proposal status and execution rows without live exchange calls."""
    db_path = tmp_path / "test.db"
    db = AITraderDB(str(db_path))

    db.insert_proposal(
        cycle_seq=77,
        symbol="ETH",
        venue="lighter",
        direction="LONG",
        size_usd=20.0,
        sl=None,
        tp=None,
        conviction=0.6,
        reason_short="test proposal",
        signals=["cvd"],
    )

    cycle_data = {
        "sequence": 77,
        "symbols": {
            "ETH": {
                "price": 2000.0,
                "best_bid": 1999.0,
                "best_ask": 2001.0,
                "atr_pct": 2.0,
            }
        },
    }
    cycle_path = tmp_path / "cycle.json"
    cycle_path.write_text(json.dumps(cycle_data))

    def _mock_load_config():
        return {
            "config": {
                "db_path": str(db_path),
                "executor": {
                    "db_path": str(db_path),
                    "use_db_positions": True,
                    "write_positions_yaml": False,
                    "enable_trade_journal": False,
                    "enable_trade_tracker": False,
                    "use_fill_reconciler": False,
                    "dry_run": True,
                },
                "risk": {"max_sector_concentration": 3},
            }
        }

    monkeypatch.setattr(cli, "load_config", _mock_load_config)

    args = Namespace(cycle_file=str(cycle_path), seq=77, dry_run=True)
    result = asyncio.run(cli.cmd_guardian(args))
    assert result == 0

    proposals = db.get_proposals_for_cycle(77)
    assert proposals[0]["status"] in ("FAILED", "PROPOSED")
    if proposals[0]["status"] == "FAILED":
        assert proposals[0]["status_reason"] == "dry_run"

    conn = sqlite3.connect(str(db_path))
    count = conn.execute("SELECT COUNT(*) FROM proposal_executions").fetchone()[0]
    conn.close()
    assert count == 1


def test_guardian_blocks_xyz_without_hip3_main_metadata(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "test.db"
    db = AITraderDB(str(db_path))

    db.insert_proposal(
        cycle_seq=78,
        symbol="XYZ:ABC",
        venue="hip3",
        direction="LONG",
        size_usd=50.0,
        sl=None,
        tp=None,
        conviction=0.7,
        reason_short="HIP3_MAIN",
        signals=["cvd"],
    )

    cycle_data = {
        "sequence": 78,
        "symbols": {
            "XYZ:ABC": {
                "price": 10.0,
                "best_bid": 9.95,
                "best_ask": 10.05,
                "atr_pct": 2.0,
            }
        },
    }
    cycle_path = tmp_path / "cycle.json"
    cycle_path.write_text(json.dumps(cycle_data))

    def _mock_load_config():
        return {
            "config": {
                "db_path": str(db_path),
                "executor": {
                    "db_path": str(db_path),
                    "use_db_positions": True,
                    "write_positions_yaml": False,
                    "enable_trade_journal": False,
                    "enable_trade_tracker": False,
                    "use_fill_reconciler": False,
                    "dry_run": True,
                },
                "risk": {"max_sector_concentration": 3},
            }
        }

    monkeypatch.setattr(cli, "load_config", _mock_load_config)

    args = Namespace(cycle_file=str(cycle_path), seq=78, dry_run=True)
    result = asyncio.run(cli.cmd_guardian(args))
    assert result == 0

    proposals = db.get_proposals_for_cycle(78)
    assert len(proposals) == 1
    assert proposals[0]["status"] == "BLOCKED"
    assert proposals[0]["status_reason"] == "hip3_main_required_for_xyz"


def test_guardian_allows_xyz_with_hip3_main_metadata(tmp_path, monkeypatch) -> None:
    db_path = tmp_path / "test.db"
    db = AITraderDB(str(db_path))

    proposal_id = db.insert_proposal(
        cycle_seq=79,
        symbol="XYZ:ABC",
        venue="hip3",
        direction="LONG",
        size_usd=50.0,
        sl=None,
        tp=None,
        conviction=0.7,
        reason_short="HIP3_MAIN",
        signals=["hip3_main"],
    )
    db.insert_proposal_metadata(
        proposal_id,
        {"signals_snapshot": {"hip3_main": {"direction": "LONG", "z_score": 2.9}}},
    )

    cycle_data = {
        "sequence": 79,
        "symbols": {
            "XYZ:ABC": {
                "price": 10.0,
                "best_bid": 9.95,
                "best_ask": 10.05,
                "atr_pct": 2.0,
            }
        },
    }
    cycle_path = tmp_path / "cycle.json"
    cycle_path.write_text(json.dumps(cycle_data))

    def _mock_load_config():
        return {
            "config": {
                "db_path": str(db_path),
                "executor": {
                    "db_path": str(db_path),
                    "use_db_positions": True,
                    "write_positions_yaml": False,
                    "enable_trade_journal": False,
                    "enable_trade_tracker": False,
                    "use_fill_reconciler": False,
                    "dry_run": True,
                },
                "risk": {"max_sector_concentration": 3},
            }
        }

    monkeypatch.setattr(cli, "load_config", _mock_load_config)

    args = Namespace(cycle_file=str(cycle_path), seq=79, dry_run=True)
    result = asyncio.run(cli.cmd_guardian(args))
    assert result == 0

    proposals = db.get_proposals_for_cycle(79)
    assert len(proposals) == 1
    assert proposals[0]["status"] in ("FAILED", "PROPOSED")
    if proposals[0]["status"] == "FAILED":
        assert proposals[0]["status_reason"] == "dry_run"
