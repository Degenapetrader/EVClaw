#!/usr/bin/env python3
"""Shared helpers for loading latest runtime context artifacts."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Optional


def load_latest_context_path(
    runtime_dir: Path,
    *,
    pattern: str = "evclaw_context_*.json",
) -> Optional[Path]:
    """Return newest context json path by mtime, or None when unavailable."""
    try:
        paths = sorted(runtime_dir.glob(pattern), key=lambda p: p.stat().st_mtime, reverse=True)
        return paths[0] if paths else None
    except Exception:
        return None


def load_context_payload(path: Optional[Path]) -> Dict[str, Any]:
    """Best-effort JSON payload loader used by context-consuming workers."""
    if path is None or not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text())
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def load_latest_context_payload(
    runtime_dir: Path,
    *,
    pattern: str = "evclaw_context_*.json",
) -> Dict[str, Any]:
    """Return newest context payload from runtime dir (or empty dict)."""
    return load_context_payload(load_latest_context_path(runtime_dir, pattern=pattern))
