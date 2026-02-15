#!/usr/bin/env python3
"""Regression tests for executor persistence locking."""

import logging
import threading
import time
from pathlib import Path
import sys

import yaml

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import executor as executor_module
from executor import PositionPersistence


def test_append_trade_uses_shared_lock_to_prevent_lost_updates(monkeypatch, tmp_path) -> None:
    persistence = PositionPersistence(
        log=logging.getLogger("test.persistence"),
        memory_dir=tmp_path,
        enable_positions=False,
        enable_journal=True,
    )

    # Force overlap in the read phase; without a shared lock this reliably causes
    # both writers to read the same pre-append snapshot and lose one update.
    original_safe_load = executor_module.yaml.safe_load

    def slow_safe_load(stream):
        time.sleep(0.05)
        return original_safe_load(stream)

    monkeypatch.setattr(executor_module.yaml, "safe_load", slow_safe_load)

    def _append(tag: str) -> None:
        ok = persistence.append_trade({"event": "entry", "tag": tag})
        assert ok

    t1 = threading.Thread(target=_append, args=("A",), daemon=True)
    t2 = threading.Thread(target=_append, args=("B",), daemon=True)
    t1.start()
    t2.start()
    t1.join()
    t2.join()

    with open(tmp_path / "trade_journal.yaml", "r") as f:
        data = yaml.safe_load(f) or {}

    tags = {str(t.get("tag")) for t in data.get("trades", [])}
    assert tags == {"A", "B"}
    assert len(data.get("trades", [])) == 2
