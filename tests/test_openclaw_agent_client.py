#!/usr/bin/env python3
"""Tests for OpenClaw CLI helper parsing behavior."""

import asyncio
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import openclaw_agent_client
from openclaw_agent_client import openclaw_agent_turn, safe_json_loads


def test_safe_json_loads_strips_markdown_fences() -> None:
    raw = "```json\n{\"picks\":[],\"rejects\":[]}\n```"
    parsed = safe_json_loads(raw)
    assert isinstance(parsed, dict)
    assert parsed.get("picks") == []
    assert parsed.get("rejects") == []


def test_safe_json_loads_extracts_outer_object_from_logs() -> None:
    raw = "INFO preface\n{\"picks\":[],\"rejects\":[]}\nWARN suffix"
    parsed = safe_json_loads(raw)
    assert isinstance(parsed, dict)
    assert parsed.get("picks") == []


def test_openclaw_agent_turn_prefers_json_payload_text() -> None:
    class DummyProc:
        def __init__(self, out_b: bytes):
            self._out = out_b
            self.returncode = 0

        async def communicate(self):
            return self._out, b""

        async def wait(self):
            return 0

    payload = {
        "status": "ok",
        "result": {
            "payloads": [
                {"text": "reasoning text"},
                {"text": "{\"picks\":[],\"rejects\":[]}"},
            ]
        },
    }
    out_b = json.dumps(payload).encode("utf-8")

    async def fake_create_subprocess_exec(*_args, **_kwargs):
        return DummyProc(out_b)

    original_create = openclaw_agent_client.asyncio.create_subprocess_exec
    original_model_flag = openclaw_agent_client._MODEL_FLAG_SUPPORTED
    openclaw_agent_client.asyncio.create_subprocess_exec = fake_create_subprocess_exec
    openclaw_agent_client._MODEL_FLAG_SUPPORTED = None
    try:
        meta, assistant_text = asyncio.run(
            openclaw_agent_turn(
                message="m",
                session_id="s",
                openclaw_cmd="openclaw",
                timeout_sec=10,
            )
        )
        assert isinstance(meta, dict)
        assert meta.get("error_kind") == "none"
        assert assistant_text == "{\"picks\":[],\"rejects\":[]}"
    finally:
        openclaw_agent_client.asyncio.create_subprocess_exec = original_create
        openclaw_agent_client._MODEL_FLAG_SUPPORTED = original_model_flag


def test_openclaw_agent_turn_timeout_kills_process() -> None:
    class DummyProc:
        def __init__(self):
            self.returncode = None
            self.killed = False

        async def communicate(self):
            await asyncio.sleep(60)
            return b"", b""

        def kill(self):
            self.killed = True

        async def wait(self):
            return 0

    proc = DummyProc()

    async def fake_create_subprocess_exec(*_args, **_kwargs):
        return proc

    calls = {"n": 0}

    async def fake_wait_for(coro, timeout=None):
        if calls["n"] == 0:
            calls["n"] += 1
            try:
                coro.close()
            except Exception:
                pass
            raise asyncio.TimeoutError
        calls["n"] += 1
        return await coro

    original_create = openclaw_agent_client.asyncio.create_subprocess_exec
    original_wait_for = openclaw_agent_client.asyncio.wait_for
    openclaw_agent_client.asyncio.create_subprocess_exec = fake_create_subprocess_exec
    openclaw_agent_client.asyncio.wait_for = fake_wait_for
    try:
        meta, assistant_text = asyncio.run(
            openclaw_agent_turn(
                message="m",
                session_id="s",
                openclaw_cmd="openclaw",
                timeout_sec=2,
            )
        )
        assert isinstance(meta, dict)
        assert meta.get("error_kind") == "timeout"
        assert assistant_text == ""
        assert proc.killed is True
    finally:
        openclaw_agent_client.asyncio.create_subprocess_exec = original_create
        openclaw_agent_client.asyncio.wait_for = original_wait_for
