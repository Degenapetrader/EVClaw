#!/usr/bin/env python3
"""OpenClaw CLI agent call helpers.

We intentionally avoid direct model provider SDK calls. This module shells out to
`openclaw agent --json` and parses the result.

Design goals:
- async-friendly (used by live_agent.py)
- strict parsing + bounded timeouts
- returns raw assistant text payload(s)
"""

from __future__ import annotations

import asyncio
import json
import re
from typing import Any, Dict, List, Optional, Tuple


_MODEL_FLAG_SUPPORTED: Optional[bool] = None
_COMMUNICATE_TIMEOUT_GRACE_SEC = 2.0


def _extract_json_object(text: str) -> Optional[str]:
    start = text.find("{")
    if start < 0:
        return None
    depth = 0
    in_str = False
    escape = False
    for idx in range(start, len(text)):
        ch = text[idx]
        if in_str:
            if escape:
                escape = False
            elif ch == "\\":
                escape = True
            elif ch == "\"":
                in_str = False
            continue
        if ch == "\"":
            in_str = True
            continue
        if ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                return text[start : idx + 1]
    return None


async def openclaw_agent_turn(
    *,
    message: str,
    session_id: str,
    agent_id: Optional[str] = None,
    model: Optional[str] = None,
    thinking: Optional[str] = None,
    timeout_sec: float = 60.0,
    openclaw_cmd: str = "openclaw",
) -> Tuple[Optional[Dict[str, Any]], str]:
    """Run `openclaw agent` and return (meta, assistant_text).

    meta shape:
    {
      "error_kind": "none|timeout|subprocess_error|invalid_json|empty_output",
      "timed_out": bool,
      "return_code": Optional[int],
      "stderr": str,
      "payload": Optional[dict],
    }
    """

    base_cmd: List[str] = [openclaw_cmd, "agent", "--session-id", session_id, "--message", message, "--json"]
    if agent_id:
        base_cmd.extend(["--agent", agent_id])
    if thinking:
        base_cmd.extend(["--thinking", thinking])
    if timeout_sec and timeout_sec > 0:
        timeout_cli = max(1, int(float(timeout_sec)))
        base_cmd.extend(["--timeout", str(timeout_cli)])

    cmd: List[str] = list(base_cmd)
    global _MODEL_FLAG_SUPPORTED
    use_model_flag = bool(model) and (_MODEL_FLAG_SUPPORTED is not False)
    if use_model_flag and model:
        cmd.extend(["--model", str(model)])

    async def _run_once(argv: List[str]) -> Tuple[Optional[bytes], Optional[bytes], Optional[int], bool]:
        proc = await asyncio.create_subprocess_exec(
            *argv,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            out_b, err_b = await asyncio.wait_for(
                proc.communicate(),
                timeout=max(1.0, float(timeout_sec) + _COMMUNICATE_TIMEOUT_GRACE_SEC),
            )
            return out_b, err_b, proc.returncode, False
        except asyncio.TimeoutError:
            try:
                proc.kill()
            except Exception:
                pass
            try:
                await asyncio.wait_for(proc.wait(), timeout=3.0)
            except Exception:
                pass
            return None, None, None, True

    try:
        out_b, err_b, rc, timed_out = await _run_once(cmd)
        if timed_out:
            return (
                {
                    "error_kind": "timeout",
                    "timed_out": True,
                    "return_code": None,
                    "stderr": "",
                    "payload": None,
                },
                "",
            )

        # Graceful fallback for older OpenClaw CLIs that do not support --model.
        if use_model_flag and model and rc not in (0, None):
            err_txt = (err_b or b"").decode("utf-8", errors="replace")
            if "unknown option '--model'" in err_txt:
                _MODEL_FLAG_SUPPORTED = False
                out_b, err_b, rc, timed_out = await _run_once(base_cmd)
                if timed_out:
                    return (
                        {
                            "error_kind": "timeout",
                            "timed_out": True,
                            "return_code": None,
                            "stderr": "",
                            "payload": None,
                        },
                        "",
                    )
            else:
                _MODEL_FLAG_SUPPORTED = True
        elif use_model_flag and model and rc in (0, None):
            _MODEL_FLAG_SUPPORTED = True

        out = (out_b or b"").decode("utf-8", errors="replace").strip()
        err_txt = (err_b or b"").decode("utf-8", errors="replace")
        if not out:
            return (
                {
                    "error_kind": "empty_output",
                    "timed_out": False,
                    "return_code": rc,
                    "stderr": err_txt,
                    "payload": None,
                },
                "",
            )

        try:
            payload = json.loads(out)
        except Exception:
            # Some failures may emit logs + JSON. Try to salvage outermost object.
            obj_txt = _extract_json_object(out)
            if obj_txt:
                try:
                    payload = json.loads(obj_txt)
                except Exception:
                    return (
                        {
                            "error_kind": "invalid_json",
                            "timed_out": False,
                            "return_code": rc,
                            "stderr": err_txt,
                            "payload": None,
                        },
                        "",
                    )
            else:
                return (
                    {
                        "error_kind": "invalid_json",
                        "timed_out": False,
                        "return_code": rc,
                        "stderr": err_txt,
                        "payload": None,
                    },
                    "",
                )

        # Expected:
        # { status: ok, result: { payloads: [ {text: "..."}, ... ] } }
        assistant_text = ""
        try:
            result = payload.get("result") or {}
            payloads = result.get("payloads") or []
            if isinstance(payloads, list) and payloads:
                first_text = ""
                for part in payloads:
                    if not isinstance(part, dict):
                        continue
                    txt = part.get("text")
                    if not isinstance(txt, str):
                        continue
                    txt = txt.strip()
                    if not txt:
                        continue
                    if not first_text:
                        first_text = txt
                    if safe_json_loads(txt) is not None:
                        assistant_text = txt
                        break
                if not assistant_text:
                    assistant_text = first_text
        except Exception:
            assistant_text = ""

        return (
            {
                "error_kind": "none",
                "timed_out": False,
                "return_code": rc,
                "stderr": err_txt,
                "payload": payload,
            },
            assistant_text,
        )
    except Exception as exc:
        return (
            {
                "error_kind": "subprocess_error",
                "timed_out": False,
                "return_code": None,
                "stderr": str(exc),
                "payload": None,
            },
            "",
        )


def safe_json_loads(text: str) -> Optional[Any]:
    if text is None:
        return None
    raw = str(text).strip()
    if not raw:
        return None
    candidates: List[str] = [raw]
    no_fence = re.sub(r"^\s*```(?:json)?\s*|\s*```\s*$", "", raw, flags=re.IGNORECASE | re.DOTALL).strip()
    if no_fence and no_fence not in candidates:
        candidates.append(no_fence)
    obj_txt = _extract_json_object(raw)
    if obj_txt and obj_txt not in candidates:
        candidates.append(obj_txt)
    if obj_txt:
        obj_no_fence = re.sub(
            r"^\s*```(?:json)?\s*|\s*```\s*$",
            "",
            obj_txt,
            flags=re.IGNORECASE | re.DOTALL,
        ).strip()
        if obj_no_fence and obj_no_fence not in candidates:
            candidates.append(obj_no_fence)
    for cand in candidates:
        try:
            return json.loads(cand)
        except Exception:
            continue
    try:
        match = re.search(r"```(?:json)?\s*(\{[\s\S]*\})\s*```", raw, flags=re.IGNORECASE)
        if match:
            return json.loads(match.group(1))
    except Exception:
        pass
    return None
