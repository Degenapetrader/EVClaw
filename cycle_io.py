from __future__ import annotations

import json
from typing import Any, Dict, Optional


def load_candidates_file(path: str) -> Optional[Dict[str, Any]]:
    try:
        with open(path, "r") as f:
            return json.load(f)
    except Exception:
        return None


def load_context_file(path: str) -> Optional[Dict[str, Any]]:
    try:
        with open(path, "r") as f:
            payload = json.load(f)
        return payload if isinstance(payload, dict) else None
    except Exception:
        return None


def load_cycle_file(path: str) -> Dict[str, Any]:
    try:
        with open(path, "r") as f:
            payload = json.load(f)
        return payload if isinstance(payload, dict) else {}
    except Exception:
        return {}
