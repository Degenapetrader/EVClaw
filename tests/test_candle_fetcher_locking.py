#!/usr/bin/env python3
"""Candle fetcher cache-lock robustness tests."""

import asyncio
import importlib
import os
import sys
import time
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


def test_stale_cache_lock_is_recovered(monkeypatch, tmp_path) -> None:
    import candle_fetcher as cf

    with monkeypatch.context() as m:
        mod = importlib.reload(cf)
        m.setattr(mod, "CANDLE_CACHE_DIR", str(tmp_path))
        m.setattr(mod, "CANDLE_CACHE_TTL_SEC", 180)
        m.setattr(mod, "CANDLE_CACHE_LOCK_STALE_SEC", 1)

        symbols = ["BTC"]
        hours_back = 24
        lock_path = Path(mod._lock_path(hours_back, mod._symbols_hash(symbols)))
        lock_path.parent.mkdir(parents=True, exist_ok=True)
        lock_path.write_text("old")
        old_ts = time.time() - 300
        os.utime(lock_path, (old_ts, old_ts))

        async def _fake_fetch(**_kwargs):
            return "none", []

        m.setattr(mod, "fetch_candles", _fake_fetch)
        started = time.monotonic()
        result = asyncio.run(mod.fetch_1h_candles_for_symbols(symbols, hours_back, concurrency=1, timeout_sec=2))
        elapsed = time.monotonic() - started
        assert elapsed < 2.0
        assert "BTC" in result

    importlib.reload(cf)


def test_leader_lock_released_even_on_worker_failure(monkeypatch, tmp_path) -> None:
    import candle_fetcher as cf

    with monkeypatch.context() as m:
        mod = importlib.reload(cf)
        m.setattr(mod, "CANDLE_CACHE_DIR", str(tmp_path))
        m.setattr(mod, "CANDLE_CACHE_TTL_SEC", 180)

        symbols = ["ETH"]
        hours_back = 8
        lock_path = Path(mod._lock_path(hours_back, mod._symbols_hash(symbols)))

        async def _boom(**_kwargs):
            raise RuntimeError("boom")

        m.setattr(mod, "fetch_candles", _boom)
        with pytest.raises(RuntimeError):
            asyncio.run(mod.fetch_1h_candles_for_symbols(symbols, hours_back, concurrency=1, timeout_sec=2))
        assert not lock_path.exists()

    importlib.reload(cf)


def test_cache_prunes_stale_artifacts(monkeypatch, tmp_path) -> None:
    import candle_fetcher as cf

    with monkeypatch.context() as m:
        mod = importlib.reload(cf)
        m.setattr(mod, "CANDLE_CACHE_DIR", str(tmp_path))
        m.setattr(mod, "CANDLE_CACHE_TTL_SEC", 180)
        m.setattr(mod, "CANDLE_CACHE_RETENTION_SEC", 1)

        old_file = tmp_path / "evclaw_candles_1h_24_oldhash.json"
        old_file.write_text("{}")
        old_ts = time.time() - 300
        os.utime(old_file, (old_ts, old_ts))

        mod._write_cached_1h(
            24,
            ["BTC"],
            {"BTC": {"source": "none", "candles": []}},
        )

        assert not old_file.exists()
        current_file = Path(mod._cache_path(24, mod._symbols_hash(["BTC"])))
        assert current_file.exists()

    importlib.reload(cf)
