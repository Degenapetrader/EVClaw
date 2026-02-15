#!/usr/bin/env python3
"""Context-builder env override behavior tests."""

import importlib
import sys
import copy
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


def test_cb_env_override_is_ignored_in_hardcoded_mode(monkeypatch) -> None:
    import context_builder_v2 as cb

    with monkeypatch.context() as m:
        m.setenv("EVCLAW_CB_MODE", "balanced")
        m.setenv("EVCLAW_CB_MIN_Z_SCORE", "2.0")
        reloaded = importlib.reload(cb)
        assert abs(float(reloaded.CB_MIN_Z_SCORE) - 1.5) < 1e-9

    # Reload once more with restored environment so constants don't leak to other tests.
    importlib.reload(cb)


def test_cb_scale_value_zero_factor_is_noop() -> None:
    from context_builder_v2 import _cb_scale_value

    assert _cb_scale_value(10.0, 0.0, inverse=False) == 10.0
    assert _cb_scale_value(10.0, 0.0, inverse=True) == 10.0


def test_augment_dead_capital_learning_idempotent_for_same_object_only() -> None:
    from context_builder_v2 import ContextBuilderV2

    class _DummyDB:
        def __init__(self):
            self.calls = 0

        def update_dead_capital_imbalance_stats(self, **_kwargs):
            self.calls += 1
            return {
                "z": 0.0,
                "z_abs": 0.0,
                "override_streak": 0,
                "override_trigger": False,
                "n": 1,
                "mean": 0.0,
                "std": 1.0,
            }

    builder = ContextBuilderV2.__new__(ContextBuilderV2)
    builder.db = _DummyDB()

    sse_data = {
        "BTC": {
            "perp_signals": {
                "dead_capital": {
                    "locked_long_pct": 10.0,
                    "locked_short_pct": 20.0,
                }
            }
        }
    }
    builder.augment_dead_capital_learning(sse_data)
    builder.augment_dead_capital_learning(sse_data)
    assert builder.db.calls == 1

    sse_data_new = copy.deepcopy(sse_data)
    builder.augment_dead_capital_learning(sse_data_new)
    assert builder.db.calls == 2


def test_extract_entry_signals_parses_array_and_dict_payloads() -> None:
    from context_builder_v2 import ContextBuilderV2

    builder = ContextBuilderV2.__new__(ContextBuilderV2)
    assert builder._extract_entry_signals('["CVD","OFM"]') == ["CVD", "OFM"]
    assert builder._extract_entry_signals({"WHALE": True, "FADE": False, "CVD": 1}) == ["WHALE", "CVD"]
    assert builder._extract_entry_signals(None) == []


def test_load_active_positions_falls_back_to_yaml_on_db_error(monkeypatch) -> None:
    from context_builder_v2 import ContextBuilderV2

    class _DummyLog:
        def warning(self, *_args, **_kwargs):
            pass

        def error(self, *_args, **_kwargs):
            pass

    class _DummyDB:
        def get_open_trades(self):
            raise RuntimeError("db unavailable")

    builder = ContextBuilderV2.__new__(ContextBuilderV2)
    builder.db = _DummyDB()
    builder.log = _DummyLog()
    sentinel = [{"symbol": "BTC"}]
    called = {"fallback": 0}

    def _fallback():
        called["fallback"] += 1
        return sentinel

    monkeypatch.setattr(builder, "_load_positions_from_yaml", _fallback)
    got = builder.load_active_positions()
    assert called["fallback"] == 1
    assert got == sentinel
