#!/usr/bin/env python3
"""Tests for fill reconciler rate limit backoff."""

import asyncio
import tempfile
import time
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from ai_trader_db import AITraderDB
from fill_reconciler import FillReconciler


class SequencedAdapter:
    def __init__(self, responses):
        self._responses = list(responses)
        self.calls = 0

    async def get_account_trades(self, limit=100):
        self.calls += 1
        if not self._responses:
            return []
        resp = self._responses.pop(0)
        if isinstance(resp, Exception):
            raise resp
        return resp

    async def get_all_positions(self):
        return {}


def test_backoff_on_rate_limit_and_reset() -> None:
    """Backoff doubles on 429 and resets after success."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    AITraderDB(db_path)

    adapter = SequencedAdapter([
        Exception("429 Too Many Requests"),
        Exception("Too Many Requests"),
        [],
    ])

    reconciler = FillReconciler(
        db_path=db_path,
        exchange_adapter=adapter,
        venue="lighter",
    )

    async def run() -> None:
        # First call hits rate limit -> backoff set
        page, _ = await reconciler._fetch_fills_page(limit=1, cursor=None)
        assert page == []
        assert adapter.calls == 1
        assert reconciler._backoff_seconds == reconciler._backoff_base_seconds
        assert reconciler._backoff_until > time.time()

        # Backoff active: no extra request
        page, _ = await reconciler._fetch_fills_page(limit=1, cursor=None)
        assert page == []
        assert adapter.calls == 1

        # Force backoff expiry and hit another rate limit
        reconciler._backoff_until = time.time() - 1
        page, _ = await reconciler._fetch_fills_page(limit=1, cursor=None)
        assert page == []
        assert adapter.calls == 2
        assert reconciler._backoff_seconds == min(
            reconciler._backoff_base_seconds * 2,
            reconciler._backoff_max_seconds,
        )

        # Force expiry and succeed -> backoff reset
        reconciler._backoff_until = time.time() - 1
        page, _ = await reconciler._fetch_fills_page(limit=1, cursor=None)
        assert page == []
        assert adapter.calls == 3
        assert reconciler._backoff_seconds == 0.0
        assert reconciler._backoff_until == 0.0

    asyncio.run(run())
