#!/usr/bin/env python3
"""Tests for dual-venue universe caching and intersection behavior."""

import asyncio
import json
import os
import sys
import time
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import AsyncMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from universe_cache import load_dual_venue_symbols


class TestUniverseCache(unittest.TestCase):
    def test_cache_hit_uses_existing_file(self) -> None:
        with TemporaryDirectory() as tmp:
            cache_path = os.path.join(tmp, "symbol_universe_dual_venue.json")
            payload = {
                "generated_at": "2026-01-01T00:00:00+00:00",
                "ttl_days": 7,
                "hl_symbols_count": 2,
                "lighter_symbols_count": 2,
                "dual_symbols": ["BTC", "ETH"],
            }
            with open(cache_path, "w") as f:
                json.dump(payload, f)
            now = time.time()
            os.utime(cache_path, (now, now))

            with patch("universe_cache.fetch_hl_symbols", new=AsyncMock(side_effect=AssertionError("fetch_hl_symbols called"))):
                with patch("universe_cache.fetch_lighter_symbols", new=AsyncMock(side_effect=AssertionError("fetch_lighter_symbols called"))):
                    symbols = asyncio.run(load_dual_venue_symbols(cache_path=cache_path, ttl_days=7))

            self.assertEqual(symbols, ["BTC", "ETH"])

    def test_cache_refresh_builds_intersection(self) -> None:
        with TemporaryDirectory() as tmp:
            cache_path = os.path.join(tmp, "symbol_universe_dual_venue.json")
            payload = {
                "generated_at": "2026-01-01T00:00:00+00:00",
                "ttl_days": 7,
                "hl_symbols_count": 0,
                "lighter_symbols_count": 0,
                "dual_symbols": [],
            }
            with open(cache_path, "w") as f:
                json.dump(payload, f)
            old = time.time() - 8 * 86400
            os.utime(cache_path, (old, old))

            with patch("universe_cache.fetch_hl_symbols", new=AsyncMock(return_value=["BTC", "ETH", "xyz:NVDA", "SOL"])):
                with patch("universe_cache.fetch_lighter_symbols", new=AsyncMock(return_value=["BTC-PERP", "ETH", "XRP"])):
                    symbols = asyncio.run(load_dual_venue_symbols(cache_path=cache_path, ttl_days=7))

            self.assertEqual(symbols, ["BTC", "ETH"])

            with open(cache_path, "r") as f:
                saved = json.load(f)
            self.assertEqual(saved.get("dual_symbols"), ["BTC", "ETH"])


if __name__ == "__main__":
    unittest.main()
