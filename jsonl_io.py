#!/usr/bin/env python3
"""Shared JSONL append helpers."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict


def append_jsonl(path: str, record: Dict[str, Any]) -> None:
    """Best-effort JSONL append with parent directory creation."""
    try:
        p = Path(path)
        p.parent.mkdir(parents=True, exist_ok=True)
        with p.open("a", encoding="utf-8") as f:
            f.write(json.dumps(record, separators=(",", ":"), ensure_ascii=False) + "\n")
    except Exception:
        return

