#!/usr/bin/env python3
"""Central proposal status constants and transition rules."""

from __future__ import annotations

from typing import Optional, Set


PROPOSAL_STATUS_PROPOSED = "PROPOSED"
PROPOSAL_STATUS_APPROVED = "APPROVED"
PROPOSAL_STATUS_BLOCKED = "BLOCKED"
PROPOSAL_STATUS_FAILED = "FAILED"
PROPOSAL_STATUS_EXECUTED = "EXECUTED"
PROPOSAL_STATUS_CANCELLED = "CANCELLED"

VALID_PROPOSAL_STATUSES: Set[str] = {
    PROPOSAL_STATUS_PROPOSED,
    PROPOSAL_STATUS_APPROVED,
    PROPOSAL_STATUS_BLOCKED,
    PROPOSAL_STATUS_FAILED,
    PROPOSAL_STATUS_EXECUTED,
    PROPOSAL_STATUS_CANCELLED,
}

# These should never transition back into active states.
TERMINAL_PROPOSAL_STATUSES: Set[str] = {
    PROPOSAL_STATUS_BLOCKED,
    PROPOSAL_STATUS_FAILED,
    PROPOSAL_STATUS_EXECUTED,
    PROPOSAL_STATUS_CANCELLED,
}

_ALLOWED_TRANSITIONS = {
    PROPOSAL_STATUS_PROPOSED: {
        PROPOSAL_STATUS_PROPOSED,
        PROPOSAL_STATUS_APPROVED,
        PROPOSAL_STATUS_BLOCKED,
        PROPOSAL_STATUS_FAILED,
        PROPOSAL_STATUS_EXECUTED,
        PROPOSAL_STATUS_CANCELLED,
    },
    PROPOSAL_STATUS_APPROVED: {
        PROPOSAL_STATUS_APPROVED,
        PROPOSAL_STATUS_EXECUTED,
        PROPOSAL_STATUS_FAILED,
        PROPOSAL_STATUS_BLOCKED,
        PROPOSAL_STATUS_CANCELLED,
    },
    PROPOSAL_STATUS_BLOCKED: {PROPOSAL_STATUS_BLOCKED},
    PROPOSAL_STATUS_FAILED: {PROPOSAL_STATUS_FAILED},
    PROPOSAL_STATUS_EXECUTED: {PROPOSAL_STATUS_EXECUTED},
    PROPOSAL_STATUS_CANCELLED: {PROPOSAL_STATUS_CANCELLED},
}


def normalize_proposal_status(value: object, default: Optional[str] = None) -> Optional[str]:
    if value is None:
        return default
    status = str(value).strip().upper()
    if not status:
        return default
    if status not in VALID_PROPOSAL_STATUSES:
        return default
    return status


def can_transition_proposal_status(current_status: object, new_status: object) -> bool:
    """Return True when transition is allowed by proposal state contract."""
    nxt = normalize_proposal_status(new_status, default=None)
    if nxt is None:
        return False
    cur = normalize_proposal_status(current_status, default=None)
    if cur is None:
        # Unknown/legacy state in DB: allow normalizing forward once.
        return True
    if cur == nxt:
        return True
    return nxt in _ALLOWED_TRANSITIONS.get(cur, set())
