#!/usr/bin/env python3
"""Context learning adjustment regressions."""

import tempfile
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from context_learning import ContextLearningEngine, FeatureStats


def test_context_adjustment_uses_geometric_mean() -> None:
    with tempfile.TemporaryDirectory() as td:
        eng = ContextLearningEngine(memory_dir=Path(td))

        # Force 5 mild penalties (~0.8 each) with enough samples.
        stats = FeatureStats(trades=20, wins=6, total_pnl=-10.0, avg_pnl=-0.5, win_rate=0.30)
        eng._stats = {
            "f1": {"a": stats},
            "f2": {"a": stats},
            "f3": {"a": stats},
            "f4": {"a": stats},
            "f5": {"a": stats},
        }
        eng.extract_conditions = lambda _ctx, _dir: {k: "a" for k in eng._stats.keys()}  # type: ignore[assignment]

        adj, _ = eng.get_context_adjustment({}, "LONG")
        single_feature_adj = eng.get_condition_adjustment("f1", "a")

        # Old multiplicative logic collapsed to ~0.328 then clamped to 0.5.
        # Geometric-mean logic should stay equal to a single-feature penalty
        # when all feature adjustments are the same.
        assert abs(adj - single_feature_adj) < 1e-9
        assert 0.8 < adj < 1.0
