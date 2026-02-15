from __future__ import annotations

import json
import os
import subprocess
from typing import Any, Dict


_DEFAULT_CMD = "openclaw"
_ALLOWED_BASENAMES = {"openclaw", "clawdbot"}
_TRUSTED_ABS_DIRS = (
    "/usr/bin",
    "/usr/local/bin",
    "/bin",
    f"{os.path.expanduser('~')}/.local/bin",
)


def _resolve_dispatch_cmd() -> str:
    """Resolve dispatch binary from env with strict validation."""
    raw = (os.getenv("OPENCLAW_CMD") or _DEFAULT_CMD).strip()
    if not raw:
        return _DEFAULT_CMD
    # Reject tokenized/injected strings (`cmd arg`, `cmd;...`).
    if any(ch.isspace() for ch in raw) or ";" in raw:
        return _DEFAULT_CMD

    base = os.path.basename(raw)
    if base not in _ALLOWED_BASENAMES:
        return _DEFAULT_CMD

    # Bare command name is allowed; shell lookup decides path.
    if os.path.sep not in raw:
        return base

    # Absolute paths are allowed only from trusted locations.
    if not os.path.isabs(raw):
        return _DEFAULT_CMD
    real = os.path.realpath(raw)
    if not any(real.startswith(prefix + os.path.sep) or real == prefix for prefix in _TRUSTED_ABS_DIRS):
        return _DEFAULT_CMD
    if not (os.path.isfile(real) and os.access(real, os.X_OK)):
        return _DEFAULT_CMD
    return real


DISPATCH_CMD = _resolve_dispatch_cmd()


def send_system_event(payload: Dict[str, Any]) -> None:
    """Best-effort system event emission."""
    msg = json.dumps(payload, separators=(",", ":"), sort_keys=True)
    cmd = [DISPATCH_CMD, "system", "event", "--text", msg, "--mode", "now"]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
        if result.returncode != 0:
            print(f"System event failed: {result.stderr.strip()}")
    except Exception as exc:
        print(f"System event error: {exc}")
