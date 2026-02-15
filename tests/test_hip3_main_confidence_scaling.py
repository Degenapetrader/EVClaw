from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from hip3_main import compute_hip3_main


def test_hip3_main_confidence_scaling_deadcap_and_ofm_conf() -> None:
    payload = {
        "flow": {
            "skipped": False,
            "direction": "LONG",
            "z_signed": 10.0,
            "dynamic_threshold": 5.0,
            "spread_bps": 12.3,
            "mispricing_bps": 4.5,
        },
        "ofm_pred": {
            "direction": "LONG",
            "confidence": 0.8,
        },
    }
    dead = {"signal": "LONG", "z_score": 3.5}

    sig, meta = compute_hip3_main(payload, dead_capital=dead, threshold_mult=1.2, require_ofm=True)
    assert sig is not None

    # require_ofm=True uses strict flow+ofm gate. Base z is max(flow_z, ofm_conf*3)*1.05.
    assert abs(sig["z_score_effective"] - 10.5) < 1e-9
    assert sig.get("driver_type") == "both"
    assert meta.get("dead_aligned") is True
    assert abs(meta.get("ofm_conf", 0.0) - 0.8) < 1e-9


def test_hip3_main_blocks_when_ofm_misaligned() -> None:
    payload = {
        "flow": {
            "skipped": False,
            "direction": "LONG",
            "z_signed": 10.0,
            "dynamic_threshold": 5.0,
        },
        "ofm_pred": {
            "direction": "SHORT",
            "confidence": 1.0,
        },
    }

    sig, meta = compute_hip3_main(payload, threshold_mult=1.2, require_ofm=True)
    assert sig is None
    assert meta.get("blocked_reason") == "flow_ofm_direction_conflict"


def test_hip3_main_or_mode_allows_flow_only() -> None:
    payload = {
        "flow": {
            "skipped": False,
            "direction": "LONG",
            "z_score": 2.6,
            "dynamic_threshold": 2.0,
        },
        "ofm_pred": {
            "direction": "NEUTRAL",
            "confidence": 0.2,
        },
    }

    sig, meta = compute_hip3_main(payload, threshold_mult=1.0, require_ofm=False)
    assert sig is not None
    assert sig.get("driver_type") == "flow"
    assert sig.get("direction") == "LONG"
    assert bool(meta.get("flow_pass")) is True
    assert bool(meta.get("ofm_pass")) is False


def test_hip3_main_or_mode_allows_ofm_only() -> None:
    payload = {
        "flow": {
            "skipped": False,
            "direction": "NEUTRAL",
            "z_score": 0.2,
            "dynamic_threshold": 2.0,
        },
        "ofm_pred": {
            "direction": "SHORT",
            "confidence": 0.72,
        },
    }

    sig, meta = compute_hip3_main(payload, threshold_mult=1.0, require_ofm=False)
    assert sig is not None
    assert sig.get("driver_type") == "ofm"
    assert sig.get("direction") == "SHORT"
    assert bool(meta.get("flow_pass")) is False
    assert bool(meta.get("ofm_pass")) is True


def test_hip3_main_blocks_when_market_hours_paused() -> None:
    payload = {
        "paused": True,
        "entry_allowed": False,
        "market_session": "post_market",
        "pause_reason": "post_market",
        "flow": {
            "direction": "LONG",
            "z_score": 3.2,
            "dynamic_threshold": 2.0,
        },
        "ofm_pred": {
            "direction": "LONG",
            "confidence": 0.9,
        },
    }

    sig, meta = compute_hip3_main(payload, threshold_mult=1.0, require_ofm=False)
    assert sig is None
    assert meta.get("blocked_reason") == "market_hours_paused"
    assert meta.get("market_session") == "post_market"
