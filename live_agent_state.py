#!/usr/bin/env python3
"""
Shared state helpers for hl-trader live agent mode.

Tracks main-agent busy state, last notified cycle, and pending cycle payloads.
Uses a simple file lock to avoid concurrent writes from cycle_trigger and the
live agent runner.
"""

from __future__ import annotations

import json
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Dict, Optional

import fcntl
from env_utils import EVCLAW_RUNTIME_DIR

RUNTIME_DIR = Path(EVCLAW_RUNTIME_DIR)
STATE_FILE = Path(EVCLAW_RUNTIME_DIR) / "evclaw_main_agent_state.json"
PENDING_FILE = Path(EVCLAW_RUNTIME_DIR) / "evclaw_main_agent_pending.json"
LOCK_FILE = Path(EVCLAW_RUNTIME_DIR) / "evclaw_main_agent.lock"
STALE_SECONDS = 1200  # 20 minutes


def _default_state() -> Dict[str, Any]:
    return {
        "busy": False,
        "in_progress_seq": None,
        "updated_at": 0.0,
        "last_notified_seq": -1,
        "last_notified_at": 0.0,
        "pending_seq": None,
        "pending_at": 0.0,
    }


@contextmanager
def _state_lock():
    LOCK_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(LOCK_FILE, "a+") as lock_fp:
        fcntl.flock(lock_fp, fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(lock_fp, fcntl.LOCK_UN)


def _load_state_unlocked() -> Dict[str, Any]:
    state = _default_state()
    if STATE_FILE.exists():
        try:
            raw = json.loads(STATE_FILE.read_text())
            if isinstance(raw, dict):
                state.update(raw)
        except Exception:
            pass
    return _normalize_state(state)


def _save_state_unlocked(state: Dict[str, Any]) -> None:
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = STATE_FILE.with_suffix(".tmp")
    tmp_path.write_text(json.dumps(state, sort_keys=True))
    tmp_path.replace(STATE_FILE)


def _normalize_state(state: Dict[str, Any]) -> Dict[str, Any]:
    now = time.time()
    updated_at = float(state.get("updated_at") or 0.0)
    if state.get("busy") and updated_at > 0 and now - updated_at > STALE_SECONDS:
        state["busy"] = False
        state["in_progress_seq"] = None
    return state


def load_state() -> Dict[str, Any]:
    """Load state with locking and staleness normalization."""
    with _state_lock():
        return _load_state_unlocked()


def save_state(state: Dict[str, Any]) -> None:
    """Save state with locking."""
    with _state_lock():
        _save_state_unlocked(state)


def mark_busy(seq: int) -> Dict[str, Any]:
    """Mark the main agent as busy for a given sequence."""
    with _state_lock():
        state = _load_state_unlocked()
        state["busy"] = True
        state["in_progress_seq"] = int(seq)
        state["updated_at"] = time.time()
        _save_state_unlocked(state)
        return state


def mark_idle(seq: Optional[int] = None) -> Dict[str, Any]:
    """Clear busy flag (optionally only if seq matches)."""
    with _state_lock():
        state = _load_state_unlocked()
        if seq is None or state.get("in_progress_seq") == seq:
            state["busy"] = False
            state["in_progress_seq"] = None
        state["updated_at"] = time.time()
        _save_state_unlocked(state)
        return state


def set_last_notified(seq: int) -> Dict[str, Any]:
    """Update last notified sequence."""
    with _state_lock():
        state = _load_state_unlocked()
        state["last_notified_seq"] = int(seq)
        state["last_notified_at"] = time.time()
        _save_state_unlocked(state)
        return state




def load_pending() -> Optional[Dict[str, Any]]:
    """Load pending payload without clearing it."""
    with _state_lock():
        if not PENDING_FILE.exists():
            return None
        try:
            payload = json.loads(PENDING_FILE.read_text())
            return payload if isinstance(payload, dict) else None
        except Exception:
            return None


def clear_pending() -> None:
    """Remove pending payload file and clear state reference."""
    with _state_lock():
        state = _load_state_unlocked()
        if PENDING_FILE.exists():
            try:
                PENDING_FILE.unlink()
            except Exception:
                pass
        state["pending_seq"] = None
        state["pending_at"] = 0.0
        _save_state_unlocked(state)
