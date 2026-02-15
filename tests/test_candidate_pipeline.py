#!/usr/bin/env python3
"""Tests for candidate pipeline selection + gating."""

import sqlite3
import sys
import tempfile
import time
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import candidate_pipeline
from ai_trader_db import AITraderDB
from candidate_pipeline import _build_signals_list, build_candidates_from_context
from learning_dossier_aggregator import update_from_trade_close


class DummyDB:
    def __init__(self, open_trades=None):
        self._open_trades = open_trades or []

    def get_open_trades(self):
        return self._open_trades


class DummyContextLearning:
    def get_context_adjustment(self, _opp, _direction):
        return 1.0, {}


def _opp(symbol: str, direction: str, score: float, strong: bool = False):
    return {
        "symbol": symbol,
        "direction": direction,
        "score": score,
        "key_metrics": {"pct_24h": 0, "atr_pct": 1},
        "perp_signals": {},
        "strong_signals": ["whale"] if strong else [],
    }


def test_build_candidates_includes_must_trade_symbol() -> None:
    context_payload = {
        "top_opportunities": [
            _opp("AAA", "LONG", 1, strong=True),
            _opp("BBB", "SHORT", 100, strong=False),
        ]
    }
    cycle_symbols = {"AAA": {"symbol": "AAA"}, "BBB": {"symbol": "BBB"}}
    db = DummyDB()

    candidates = build_candidates_from_context(
        seq=1,
        context_payload=context_payload,
        cycle_symbols=cycle_symbols,
        db=db,
        context_learning=DummyContextLearning(),
    )

    symbols = {c["symbol"] for c in candidates}
    assert "AAA" in symbols
    must_candidate = next(c for c in candidates if c["symbol"] == "AAA")
    assert must_candidate["must_trade"] is True


def test_build_candidates_biases_against_aligned_net_exposure() -> None:
    context_payload = {
        "global_context": {"exposure": {"current": {"net": 20000}}},
        "top_opportunities": [
            _opp("LONGY", "LONG", 50, strong=False),
            _opp("SHORTY", "SHORT", 50, strong=False),
        ],
    }
    cycle_symbols = {"LONGY": {"symbol": "LONGY"}, "SHORTY": {"symbol": "SHORTY"}}
    db = DummyDB()

    candidates = build_candidates_from_context(
        seq=2,
        context_payload=context_payload,
        cycle_symbols=cycle_symbols,
        db=db,
        context_learning=DummyContextLearning(),
    )

    long_cand = next(c for c in candidates if c["symbol"] == "LONGY")
    short_cand = next(c for c in candidates if c["symbol"] == "SHORTY")
    assert short_cand["conviction"] > long_cand["conviction"]


def test_build_candidates_skips_open_trade() -> None:
    open_trade = SimpleNamespace(symbol="AAA", direction="LONG")
    context_payload = {
        "top_opportunities": [
            _opp("AAA", "LONG", 60, strong=False),
            _opp("BBB", "LONG", 55, strong=False),
        ]
    }
    cycle_symbols = {"AAA": {"symbol": "AAA"}, "BBB": {"symbol": "BBB"}}
    db = DummyDB(open_trades=[open_trade])

    candidates = build_candidates_from_context(
        seq=3,
        context_payload=context_payload,
        cycle_symbols=cycle_symbols,
        db=db,
        context_learning=DummyContextLearning(),
    )

    symbols = {c["symbol"] for c in candidates}
    assert "AAA" not in symbols
    assert "BBB" in symbols


def test_build_candidates_blocks_xyz_without_hip3_main() -> None:
    context_payload = {
        "top_opportunities": [
            {
                "symbol": "XYZ:ABC",
                "direction": "LONG",
                "score": 88,
                "key_metrics": {"pct_24h": 0, "atr_pct": 1},
                "perp_signals": {"cvd": {"direction": "LONG", "z_score": 2.1}},
            }
        ]
    }
    cycle_symbols = {"XYZ:ABC": {"symbol": "XYZ:ABC"}}

    candidates = build_candidates_from_context(
        seq=31,
        context_payload=context_payload,
        cycle_symbols=cycle_symbols,
        db=DummyDB(),
        context_learning=DummyContextLearning(),
    )

    assert candidates == []


def test_build_candidates_allows_xyz_with_hip3_main() -> None:
    context_payload = {
        "top_opportunities": [
            {
                "symbol": "XYZ:ABC",
                "direction": "LONG",
                "score": 88,
                "key_metrics": {"pct_24h": 0, "atr_pct": 1},
                "perp_signals": {
                    "hip3_main": {
                        "direction": "LONG",
                        "z_score": 2.7,
                        "driver_type": "flow",
                        "size_mult_hint": 1.2,
                    }
                },
            }
        ]
    }
    cycle_symbols = {"XYZ:ABC": {"symbol": "XYZ:ABC"}}

    candidates = build_candidates_from_context(
        seq=32,
        context_payload=context_payload,
        cycle_symbols=cycle_symbols,
        db=DummyDB(),
        context_learning=DummyContextLearning(),
    )

    assert len(candidates) == 1
    assert candidates[0]["symbol"] == "XYZ:ABC"
    assert isinstance(candidates[0].get("signals_snapshot"), dict)
    assert "hip3_main" in candidates[0]["signals_snapshot"]
    risk = candidates[0].get("risk") or {}
    assert float(risk.get("sl_atr_mult")) == candidate_pipeline.HIP3_FLOW_SL_ATR_MULT
    assert float(risk.get("tp_atr_mult")) == candidate_pipeline.HIP3_FLOW_TP_ATR_MULT
    assert abs(float(risk.get("hip3_booster_size_mult")) - 1.2) < 1e-9


def test_build_candidates_attaches_symbol_learning_dossier() -> None:
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    db = AITraderDB(db_path)
    trade_id = db.log_trade_entry(
        symbol="AAA",
        direction="LONG",
        entry_price=100.0,
        size=1.0,
        venue="lighter",
        signals_snapshot={"cvd": {"direction": "LONG", "z_score": 2.3}},
        signals_agreed=["CVD:LONG z=2.3"],
    )
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            UPDATE trades
            SET exit_time = ?, exit_price = ?, exit_reason = ?, realized_pnl = ?, realized_pnl_pct = ?, state = 'CLOSED'
            WHERE id = ?
            """,
            (time.time(), 101.5, "TP", 1.5, 1.5, trade_id),
        )
        conn.commit()
    update_from_trade_close(db_path, trade_id)

    context_payload = {"top_opportunities": [_opp("AAA", "LONG", 80, strong=True)]}
    cycle_symbols = {"AAA": {"symbol": "AAA"}}

    candidates = build_candidates_from_context(
        seq=4,
        context_payload=context_payload,
        cycle_symbols=cycle_symbols,
        db=db,
        context_learning=DummyContextLearning(),
    )

    assert candidates
    cand = candidates[0]
    assert isinstance(cand.get("symbol_learning_dossier"), str)
    assert "AAA" in cand["symbol_learning_dossier"]


def test_cvd_signal_fallback_preserves_sign() -> None:
    opp = {
        "signals": {
            "cvd": {
                "direction": "SHORT",
                "z_smart": -2.4,
                "z_dumb": 1.1,
            }
        }
    }
    signals = _build_signals_list(opp, expected_direction="SHORT")
    assert any("CVD:SHORT z=-2.4" in s for s in signals)


def test_whale_signal_string_zscore_is_parsed_safely() -> None:
    opp = {
        "perp_signals": {
            "whale": {
                "direction": "LONG",
                "z_score": "2.4",
            }
        }
    }
    signals = _build_signals_list(opp, expected_direction="LONG")
    assert any("WHALE:LONG str=1.2" in s for s in signals)


def test_build_candidates_propagates_constraints_strong_reason() -> None:
    context_payload = {
        "top_opportunities": [
            {
                "symbol": "ETH",
                "direction": "LONG",
                "score": 90,
                "key_metrics": {"pct_24h": 0, "atr_pct": 1},
                "perp_signals": {
                    "whale": {
                        "direction": "LONG",
                        "banner_trigger": True,
                        "z_score": 3.0,
                    }
                },
            }
        ]
    }
    cycle_symbols = {"ETH": {"symbol": "ETH"}}

    candidates = build_candidates_from_context(
        seq=10,
        context_payload=context_payload,
        cycle_symbols=cycle_symbols,
        db=DummyDB(),
        context_learning=DummyContextLearning(),
    )

    assert len(candidates) == 1
    cand = candidates[0]
    assert cand["must_trade"] is True
    assert cand["constraint_reason"] == "whale_strong"
    assert cand["strong_signals"] == ["whale"]
    assert cand["reason_short"] == "whale LONG"


def test_build_candidates_uses_local_case_insensitive_symbol_lookup() -> None:
    context_payload = {"top_opportunities": [_opp("ETH", "LONG", 80, strong=False)]}
    cycle_symbols = {"eth": {"symbol": "eth"}}

    candidates = build_candidates_from_context(
        seq=11,
        context_payload=context_payload,
        cycle_symbols=cycle_symbols,
        db=DummyDB(),
        context_learning=DummyContextLearning(),
    )

    assert len(candidates) == 1
    assert candidates[0]["symbol"] == "ETH"


def test_context_learning_singleton_initialized_once(monkeypatch) -> None:
    calls = {"count": 0}

    class _CountingContextLearning:
        def __init__(self):
            calls["count"] += 1

        def get_context_adjustment(self, _opp, _direction):
            return 1.0, {}

    monkeypatch.setattr(candidate_pipeline, "_CONTEXT_LEARNING_SINGLETON", None)
    monkeypatch.setattr(candidate_pipeline, "_CONTEXT_LEARNING_INIT_FAILED", False)
    monkeypatch.setattr(candidate_pipeline, "ContextLearningEngine", _CountingContextLearning)

    context_payload = {"top_opportunities": [_opp("ETH", "LONG", 70, strong=False)]}
    cycle_symbols = {"ETH": {"symbol": "ETH"}}
    db = DummyDB()

    out1 = build_candidates_from_context(
        seq=12,
        context_payload=context_payload,
        cycle_symbols=cycle_symbols,
        db=db,
        context_learning=None,
    )
    out2 = build_candidates_from_context(
        seq=13,
        context_payload=context_payload,
        cycle_symbols=cycle_symbols,
        db=db,
        context_learning=None,
    )

    assert out1 and out2
    assert calls["count"] == 1


def test_build_candidates_handles_zero_score_divisor(monkeypatch) -> None:
    monkeypatch.setattr(candidate_pipeline, "CANDIDATE_SCORE_DIVISOR", 0.0)
    context_payload = {"top_opportunities": [_opp("ETH", "LONG", 80, strong=False)]}
    cycle_symbols = {"ETH": {"symbol": "ETH"}}

    candidates = build_candidates_from_context(
        seq=14,
        context_payload=context_payload,
        cycle_symbols=cycle_symbols,
        db=DummyDB(),
        context_learning=DummyContextLearning(),
    )

    assert len(candidates) == 1
    conviction = float(candidates[0].get("conviction") or 0.0)
    assert conviction >= candidate_pipeline.CANDIDATE_MIN_CONVICTION


def test_build_candidates_adaptive_topk_uses_quality_count(monkeypatch) -> None:
    monkeypatch.setattr(candidate_pipeline, "CANDIDATE_TOPK_MIN", 2)
    monkeypatch.setattr(candidate_pipeline, "CANDIDATE_TOPK_MAX", 5)
    monkeypatch.setattr(candidate_pipeline, "CANDIDATE_TOPK_SCORE_GATE", 70.0)

    context_payload = {
        "top_opportunities": [
            _opp("A1", "LONG", 95, strong=False),
            _opp("A2", "SHORT", 82, strong=False),
            _opp("A3", "LONG", 74, strong=False),
            _opp("A4", "SHORT", 65, strong=False),
            _opp("A5", "LONG", 55, strong=False),
            _opp("A6", "SHORT", 45, strong=False),
        ]
    }
    cycle_symbols = {f"A{i}": {"symbol": f"A{i}"} for i in range(1, 7)}

    candidates = build_candidates_from_context(
        seq=15,
        context_payload=context_payload,
        cycle_symbols=cycle_symbols,
        db=DummyDB(),
        context_learning=DummyContextLearning(),
    )
    assert len(candidates) == 3


def test_build_candidates_adaptive_topk_honors_min_when_weak(monkeypatch) -> None:
    monkeypatch.setattr(candidate_pipeline, "CANDIDATE_TOPK_MIN", 2)
    monkeypatch.setattr(candidate_pipeline, "CANDIDATE_TOPK_MAX", 5)
    monkeypatch.setattr(candidate_pipeline, "CANDIDATE_TOPK_SCORE_GATE", 70.0)

    context_payload = {
        "top_opportunities": [
            _opp("B1", "LONG", 61, strong=False),
            _opp("B2", "SHORT", 59, strong=False),
            _opp("B3", "LONG", 51, strong=False),
            _opp("B4", "SHORT", 43, strong=False),
        ]
    }
    cycle_symbols = {f"B{i}": {"symbol": f"B{i}"} for i in range(1, 5)}

    candidates = build_candidates_from_context(
        seq=16,
        context_payload=context_payload,
        cycle_symbols=cycle_symbols,
        db=DummyDB(),
        context_learning=DummyContextLearning(),
    )
    assert len(candidates) == 2


def test_build_candidates_adaptive_topk_keeps_strong_floor(monkeypatch) -> None:
    monkeypatch.setattr(candidate_pipeline, "CANDIDATE_TOPK_MIN", 1)
    monkeypatch.setattr(candidate_pipeline, "CANDIDATE_TOPK_MAX", 2)
    monkeypatch.setattr(candidate_pipeline, "CANDIDATE_TOPK_SCORE_GATE", 90.0)

    context_payload = {
        "top_opportunities": [
            _opp("C1", "LONG", 40, strong=True),
            _opp("C2", "SHORT", 35, strong=True),
            _opp("C3", "LONG", 30, strong=True),
            _opp("C4", "SHORT", 28, strong=False),
        ]
    }
    cycle_symbols = {f"C{i}": {"symbol": f"C{i}"} for i in range(1, 5)}

    candidates = build_candidates_from_context(
        seq=17,
        context_payload=context_payload,
        cycle_symbols=cycle_symbols,
        db=DummyDB(),
        context_learning=DummyContextLearning(),
    )
    assert len(candidates) == 3
