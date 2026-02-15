#!/usr/bin/env python3
"""Tests for shared conviction model helpers."""

import os
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from conviction_model import ConvictionConfig, compute_blended_conviction, resolve_order_type


def test_resolve_order_type_thresholds() -> None:
    cfg = ConvictionConfig(chase_threshold=0.70, limit_min=0.20)
    assert resolve_order_type(0.70, config=cfg) == "chase_limit"
    assert resolve_order_type(0.95, config=cfg) == "chase_limit"
    assert resolve_order_type(0.6999, config=cfg) == "limit"
    assert resolve_order_type(0.2001, config=cfg) == "limit"
    assert resolve_order_type(0.20, config=cfg) == "reject"
    assert resolve_order_type(0.01, config=cfg) == "reject"


def test_compute_blended_conviction_uses_pipeline_weight() -> None:
    out = compute_blended_conviction(0.10, 0.80, blend_pipeline=0.20)
    assert out == pytest.approx(0.66, rel=1e-9)


def test_conviction_config_ignores_legacy_brain_env_keys() -> None:
    old_conv_weights = os.environ.get("EVCLAW_CONVICTION_WEIGHTS")
    old_conv_blend = os.environ.get("EVCLAW_CONVICTION_BLEND_PIPELINE")
    old_weights = os.environ.get("EVCLAW_BRAIN_CONVICTION_WEIGHTS")
    old_blend = os.environ.get("EVCLAW_BRAIN_BLEND_PIPELINE")
    old_thr = os.environ.get("EVCLAW_BRAIN_CONVICTION_THRESHOLD")
    try:
        os.environ.pop("EVCLAW_CONVICTION_WEIGHTS", None)
        os.environ.pop("EVCLAW_CONVICTION_BLEND_PIPELINE", None)
        os.environ["EVCLAW_BRAIN_CONVICTION_WEIGHTS"] = '{"cvd": 0.55}'
        os.environ["EVCLAW_BRAIN_BLEND_PIPELINE"] = "0.33"
        os.environ["EVCLAW_BRAIN_CONVICTION_THRESHOLD"] = "0.77"
        cfg = ConvictionConfig.load()
        assert cfg.weights["cvd"] == 0.10
        assert cfg.blend_pipeline == 0.20
        assert cfg.chase_threshold == 0.70
    finally:
        if old_conv_weights is None:
            os.environ.pop("EVCLAW_CONVICTION_WEIGHTS", None)
        else:
            os.environ["EVCLAW_CONVICTION_WEIGHTS"] = old_conv_weights
        if old_conv_blend is None:
            os.environ.pop("EVCLAW_CONVICTION_BLEND_PIPELINE", None)
        else:
            os.environ["EVCLAW_CONVICTION_BLEND_PIPELINE"] = old_conv_blend
        if old_weights is None:
            os.environ.pop("EVCLAW_BRAIN_CONVICTION_WEIGHTS", None)
        else:
            os.environ["EVCLAW_BRAIN_CONVICTION_WEIGHTS"] = old_weights
        if old_blend is None:
            os.environ.pop("EVCLAW_BRAIN_BLEND_PIPELINE", None)
        else:
            os.environ["EVCLAW_BRAIN_BLEND_PIPELINE"] = old_blend
        if old_thr is None:
            os.environ.pop("EVCLAW_BRAIN_CONVICTION_THRESHOLD", None)
        else:
            os.environ["EVCLAW_BRAIN_CONVICTION_THRESHOLD"] = old_thr
