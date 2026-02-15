#!/usr/bin/env python3
"""Tests for merging worker outputs into context builder data."""

import json
import os
import sys
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from context_builder_v2 import ContextBuilderV2


class TestContextBuilderWorkerMerge(unittest.TestCase):
    def test_merge_worker_outputs(self) -> None:
        with TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            signals_dir = tmp_path / "signals"
            memory_dir = tmp_path / "memory"
            signals_dir.mkdir()
            memory_dir.mkdir()

            (signals_dir / "trend_state.json").write_text(
                json.dumps({
                    "generated_at": "2026-01-01T00:00:00+00:00",
                    "symbols": {
                        "BTC": {
                            "trend_score": 55.0,
                            "regime": "LONG_ONLY",
                            "direction": "LONG",
                            "signal": {"direction": "LONG", "z_score": 2.5},
                        }
                    },
                })
            )

            (signals_dir / "volume_profile.json").write_text(
                json.dumps({
                    "generated_at": "2026-01-01T00:00:00+00:00",
                    "symbols": {
                        "BTC": {
                            "timeframes": {
                                "session_8h": {
                                    "poc": 100.0,
                                    "vah": 110.0,
                                    "val": 90.0,
                                    "distance_to_poc": 0.1,
                                    "signal": {"direction": "LONG", "z_score": 2.0},
                                }
                            }
                        }
                    },
                })
            )

            (signals_dir / "sr_levels.json").write_text(
                json.dumps({
                    "generated_at": "2026-01-01T00:00:00+00:00",
                    "symbols": {
                        "BTC": {
                            "price": 100.0,
                            "nearest": {
                                "support": {"price": 95.0, "strength_z": 2.3},
                                "resistance": {"price": 105.0, "strength_z": 1.8},
                            },
                            "levels": [{"price": 95.0}],
                            "signal": {"direction": "LONG", "z_score": 2.1},
                        }
                    },
                })
            )

            sse_data = {"BTC": {"price": 100.0}}

            builder = ContextBuilderV2(
                db_path="",
                memory_dir=memory_dir,
                signals_dir=signals_dir,
            )

            builder.merge_worker_signals(sse_data)

            self.assertIn("trend_state", sse_data["BTC"])
            self.assertIn("volume_profile", sse_data["BTC"])
            self.assertIn("sr_levels", sse_data["BTC"])
            self.assertNotIn("levels", sse_data["BTC"]["sr_levels"])


if __name__ == "__main__":
    unittest.main()
