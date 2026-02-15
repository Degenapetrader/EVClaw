#!/usr/bin/env python3
"""Tests for global context IV health reporting."""

from __future__ import annotations

import time
from pathlib import Path

import global_context


def test_iv_health_degrades_after_consecutive_failures(tmp_path: Path) -> None:
    provider = global_context.GlobalContextProvider(db_path=str(tmp_path / "test.db"))
    provider.iv_alert_consec_fail = 3

    now = time.time()
    provider._mark_iv_failure("BTC", "network_error", now)
    provider._mark_iv_failure("BTC", "network_error", now + 1)
    provider._mark_iv_failure("BTC", "network_error", now + 2)

    health = provider._build_iv_health()
    assert health["btc"]["status"] == "degraded"
    assert int(health["btc"]["consecutive_failures"]) == 3
    assert health["degraded"] is True


def test_iv_health_resets_on_success(tmp_path: Path) -> None:
    provider = global_context.GlobalContextProvider(db_path=str(tmp_path / "test.db"))
    now = time.time()

    provider._mark_iv_failure("ETH", "timeout", now)
    provider._mark_iv_success("ETH", now + 5)
    health = provider._build_iv_health()

    assert health["eth"]["status"] == "ok"
    assert int(health["eth"]["consecutive_failures"]) == 0
    assert float(health["eth"]["last_ok_ts"]) > now
