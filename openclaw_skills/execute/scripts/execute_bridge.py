#!/usr/bin/env python3
"""Bridge helper for flexible /execute manual command forms.

Supported:
1) Plan mode (existing):
   /execute <PLAN_ID> <chase|limit> [ttl]
2) Ad-hoc mode (new):
   /execute <long|short> <SYMBOL> <chase|limit> [size_usd] [ttl] [confirm]
   /execute <SYMBOL> <long|short> <chase|limit> [size_usd] [ttl] [confirm]

Ad-hoc mode keeps auditability by generating a manual plan first, then executing it.
"""

from __future__ import annotations

import json
import os
import re
import shlex
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

SCRIPT_PATH = Path(__file__).resolve()
EVCLAW_ROOT = Path(os.getenv("EVCLAW_ROOT") or SCRIPT_PATH.parents[3])
DB_PATH = str(Path(os.getenv("EVCLAW_DB_PATH") or (EVCLAW_ROOT / "ai_trader.db")))
RUNTIME_DIR = str(Path(os.getenv("EVCLAW_RUNTIME_DIR") or (EVCLAW_ROOT / "state")))
GENERATE_PLANS = EVCLAW_ROOT / "openclaw_skills" / "trade" / "scripts" / "generate_plans.py"
EXECUTE_PLAN = EVCLAW_ROOT / "openclaw_skills" / "trade" / "scripts" / "execute_plan.py"

PLAN_ID_RE = re.compile(r"^[A-Za-z0-9:_]+-\d{2,}$")
MODE_SET = {"chase", "limit"}
DIRECTION_SET = {"long", "short"}
CONFIRM_SET = {"confirm", "yes", "y", "go", "ok"}
TTL_SUFFIX_RE = re.compile(r"^\d+(\.\d+)?[mhd]$", re.IGNORECASE)


def _emit(obj: Dict[str, Any]) -> int:
    print(json.dumps(obj, indent=2, sort_keys=True))
    return 0


def _run_json(argv: List[str]) -> Dict[str, Any]:
    try:
        proc = subprocess.run(argv, capture_output=True, text=True, check=False)
    except Exception as exc:
        return {"ok": False, "error": f"subprocess_failed: {exc}", "argv": argv}

    out = (proc.stdout or "").strip()
    err = (proc.stderr or "").strip()
    if not out:
        return {
            "ok": False,
            "error": "empty_stdout",
            "stderr": err,
            "returncode": proc.returncode,
            "argv": argv,
        }
    try:
        return json.loads(out)
    except Exception:
        return {
            "ok": False,
            "error": "invalid_json_stdout",
            "stdout": out[:4000],
            "stderr": err[:2000],
            "returncode": proc.returncode,
            "argv": argv,
        }


def _parse_float(s: str) -> Optional[float]:
    try:
        x = float(s)
    except Exception:
        return None
    if x <= 0:
        return None
    return float(x)


def _tokenize(raw_args: List[str]) -> List[str]:
    if len(raw_args) == 1 and " " in raw_args[0]:
        try:
            return shlex.split(raw_args[0])
        except Exception:
            return raw_args
    return raw_args


def _looks_ttl(tok: str) -> bool:
    return bool(TTL_SUFFIX_RE.match(tok.strip()))


def _is_plan_mode(tokens: List[str]) -> bool:
    if len(tokens) < 2:
        return False
    if not PLAN_ID_RE.match(tokens[0].strip()):
        return False
    return tokens[1].strip().lower() in MODE_SET


def _exec_plan(display_id: str, mode: str, ttl: Optional[str]) -> Dict[str, Any]:
    argv = [sys.executable, str(EXECUTE_PLAN), str(display_id).upper(), mode]
    if ttl:
        argv.append(ttl)
    argv.extend(["--db", DB_PATH])
    payload = _run_json(argv)
    return {
        "ok": bool(payload.get("ok")),
        "mode": "plan",
        "display_id": str(display_id).upper(),
        "execute": payload,
    }


def _parse_adhoc(tokens: List[str]) -> Dict[str, Any]:
    direction: Optional[str] = None
    symbol: Optional[str] = None
    mode: Optional[str] = None
    size_usd: Optional[float] = None
    ttl: Optional[str] = None
    confirmed = False

    for raw in tokens:
        tok = str(raw or "").strip()
        if not tok:
            continue
        low = tok.lower()

        if low in CONFIRM_SET:
            confirmed = True
            continue
        if mode is None and low in MODE_SET:
            mode = low
            continue
        if direction is None and low in DIRECTION_SET:
            direction = low.upper()
            continue
        if ttl is None and _looks_ttl(tok):
            ttl = tok
            continue

        num = _parse_float(tok)
        if num is not None:
            if size_usd is None:
                size_usd = num
            elif ttl is None:
                # Numeric fallback: treat second numeric as TTL in minutes.
                ttl = tok
            continue

        if symbol is None:
            symbol = tok.upper()
            continue

    return {
        "direction": direction,
        "symbol": symbol,
        "mode": mode,
        "size_usd": size_usd,
        "ttl": ttl,
        "confirmed": confirmed,
    }


def _generate_plan(*, symbol: str, direction: str, size_usd: float) -> Dict[str, Any]:
    argv = [
        sys.executable,
        str(GENERATE_PLANS),
        symbol,
        "--direction",
        direction.lower(),
        "--size-usd",
        str(size_usd),
        "--db",
        DB_PATH,
        "--runtime",
        RUNTIME_DIR,
    ]
    return _run_json(argv)


def _pick_display_id(payload: Dict[str, Any], symbol: str) -> Optional[str]:
    sym = str(symbol or "").upper()
    stored = payload.get("stored")
    if isinstance(stored, list):
        for row in stored:
            if not isinstance(row, dict):
                continue
            if str(row.get("symbol") or "").upper() == sym and row.get("display_id"):
                return str(row.get("display_id")).upper()
        for row in stored:
            if isinstance(row, dict) and row.get("display_id"):
                return str(row.get("display_id")).upper()
    plans = payload.get("plans")
    if isinstance(plans, list):
        for row in plans:
            if isinstance(row, dict) and row.get("display_id"):
                return str(row.get("display_id")).upper()
    return None


def main() -> int:
    if not GENERATE_PLANS.exists() or not EXECUTE_PLAN.exists():
        return _emit(
            {
                "ok": False,
                "error": "missing_scripts",
                "generate_plans": str(GENERATE_PLANS),
                "execute_plan": str(EXECUTE_PLAN),
            }
        )

    tokens = _tokenize(sys.argv[1:])
    if not tokens:
        return _emit(
            {
                "ok": False,
                "error": "missing_arguments",
                "usage": [
                    "/execute <PLAN_ID> <chase|limit> [ttl]",
                    "/execute <long|short> <SYMBOL> <chase|limit> [size_usd] [ttl] [confirm]",
                ],
            }
        )

    # Existing plan-id path: execute immediately to preserve behavior.
    if _is_plan_mode(tokens):
        display_id = tokens[0]
        mode = tokens[1].lower()
        ttl = None
        for extra in tokens[2:]:
            if str(extra).strip().lower() in CONFIRM_SET:
                continue
            ttl = str(extra).strip()
            break
        return _emit(_exec_plan(display_id, mode, ttl))

    parsed = _parse_adhoc(tokens)
    direction = parsed.get("direction")
    symbol = parsed.get("symbol")
    mode = parsed.get("mode")
    size_usd = parsed.get("size_usd")
    ttl = parsed.get("ttl")
    confirmed = bool(parsed.get("confirmed"))

    if not direction or not symbol or not mode:
        return _emit(
            {
                "ok": False,
                "error": "missing_required_fields",
                "parsed": parsed,
                "hint": "Need direction, symbol, and mode. Example: /execute long ETH chase 300",
            }
        )

    if size_usd is None:
        return _emit(
            {
                "ok": False,
                "needs": "size_usd",
                "parsed": parsed,
                "prompt": f"What size in USD for {direction} {symbol} ({mode})?",
                "example": f"/execute {direction.lower()} {symbol} {mode} 300",
            }
        )

    normalized = f"/execute {direction.lower()} {symbol} {mode} {size_usd:g}"
    if ttl:
        normalized += f" {ttl}"
    confirm_cmd = normalized + " confirm"

    if not confirmed:
        return _emit(
            {
                "ok": False,
                "needs": "confirm",
                "parsed": parsed,
                "preview": {
                    "direction": direction,
                    "symbol": symbol,
                    "mode": mode,
                    "size_usd": size_usd,
                    "ttl": ttl,
                    "db_path": DB_PATH,
                },
                "confirm_command": confirm_cmd,
            }
        )

    gen = _generate_plan(symbol=symbol, direction=direction, size_usd=float(size_usd))
    if not bool(gen.get("ok")):
        return _emit(
            {
                "ok": False,
                "error": "plan_generation_failed",
                "parsed": parsed,
                "plan_generation": gen,
            }
        )

    display_id = _pick_display_id(gen, symbol)
    if not display_id:
        return _emit(
            {
                "ok": False,
                "error": "display_id_not_found",
                "parsed": parsed,
                "plan_generation": {"ok": gen.get("ok"), "stored": gen.get("stored")},
            }
        )

    exec_payload = _exec_plan(display_id, mode, ttl)
    return _emit(
        {
            "ok": bool(exec_payload.get("ok")),
            "mode": "adhoc",
            "parsed": parsed,
            "generated_plan_id": display_id,
            "plan_generation": {"ok": gen.get("ok"), "stored": gen.get("stored")},
            "execution": exec_payload.get("execute"),
        }
    )


if __name__ == "__main__":
    raise SystemExit(main())

