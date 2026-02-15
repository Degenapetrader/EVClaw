import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from exchanges.lighter_adapter import LighterAdapter


class DummyLog:
    def __init__(self):
        self.lines = []

    def info(self, msg):
        self.lines.append(("info", msg))

    def warning(self, msg):
        self.lines.append(("warning", msg))

    def error(self, msg):
        self.lines.append(("error", msg))


class RateLimitExc(Exception):
    pass


def test_lighter_client_call_retries_on_429(monkeypatch):
    log = DummyLog()
    adapter = LighterAdapter(log=log, dry_run=False)

    calls = {"n": 0}

    async def fake_sleep(_):
        return None

    async def fn():
        calls["n"] += 1
        if calls["n"] == 1:
            raise RateLimitExc("(429) Too Many Requests")
        return "ok"

    monkeypatch.setattr(asyncio, "sleep", fake_sleep)

    result = asyncio.run(adapter._client_call(fn))
    assert result == "ok"
    assert calls["n"] == 2


def test_lighter_client_call_does_not_retry_non_429(monkeypatch):
    log = DummyLog()
    adapter = LighterAdapter(log=log, dry_run=False)

    async def fake_sleep(_):
        return None

    async def fn():
        raise RateLimitExc("boom")

    monkeypatch.setattr(asyncio, "sleep", fake_sleep)

    try:
        asyncio.run(adapter._client_call(fn))
        assert False, "expected exception"
    except RateLimitExc as e:
        assert "boom" in str(e)
