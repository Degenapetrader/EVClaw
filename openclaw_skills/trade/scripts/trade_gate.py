#!/usr/bin/env python3
"""LLM gate for /trade manual plans.

This mirrors EVClaw's entry gate style:
- Provide (global_context_compact + exposure + compact candidates)
- Candidate list is restricted to the symbol(s) the user asked for
- Output is a human-friendly manual plan schema (NOT picks/rejects)

No direct provider calls; uses OpenClaw agent (openclaw_agent_client).
"""

from __future__ import annotations

import json
import logging
import os
import secrets
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# Keep stdout clean JSON (silence other loggers).
logging.basicConfig(level=logging.CRITICAL)
logging.getLogger().setLevel(logging.CRITICAL)

SCRIPT_PATH = Path(__file__).resolve()
EVCLAW_ROOT = Path(os.getenv("EVCLAW_ROOT") or SCRIPT_PATH.parents[3])
EVCLAW_ROOT_STR = str(EVCLAW_ROOT)
if EVCLAW_ROOT_STR not in sys.path:
    sys.path.insert(0, EVCLAW_ROOT_STR)

from openclaw_agent_client import openclaw_agent_turn, safe_json_loads  # type: ignore


def _utc_now() -> float:
    return time.time()


def _iso(ts: float) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _truthy(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return str(raw).strip().lower() in {"1", "true", "yes", "y", "on"}


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return int(str(raw).strip())
    except Exception:
        return default


def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return float(str(raw).strip())
    except Exception:
        return default


def _build_session_id(prefix: str, *, suffix: Optional[str] = None) -> str:
    stamp_ms = int(_utc_now() * 1000)
    token = secrets.token_hex(4)
    if suffix:
        return f"{prefix}_{suffix}_{stamp_ms}_{token}"
    return f"{prefix}_{stamp_ms}_{token}"


@dataclass
class TradeGateConfig:
    enabled: bool
    agent_id: Optional[str]
    thinking: Optional[str]
    timeout_sec: float
    openclaw_cmd: str

    @classmethod
    def load(cls) -> "TradeGateConfig":
        enabled = _truthy("EVCLAW_MANUAL_TRADE_GATE_ENABLED", True)
        # Default to OpenClaw "default" agent.
        agent_id = (os.getenv("EVCLAW_MANUAL_TRADE_GATE_AGENT_ID") or "default").strip() or None
        thinking = (os.getenv("EVCLAW_MANUAL_TRADE_GATE_THINKING") or "medium").strip() or None
        timeout_sec = max(5.0, _env_float("EVCLAW_MANUAL_TRADE_GATE_TIMEOUT_SEC", 25.0))
        openclaw_cmd = (os.getenv("OPENCLAW_CMD") or os.getenv("CLAWDBOT_CMD") or "openclaw").strip()
        return cls(
            enabled=enabled,
            agent_id=agent_id,
            thinking=thinking,
            timeout_sec=timeout_sec,
            openclaw_cmd=openclaw_cmd,
        )


def build_trade_gate_prompt(
    *,
    seq: Optional[int],
    global_context_compact: str,
    exposure: Dict[str, Any],
    open_positions: List[Dict[str, Any]],
    candidates_compact: List[Dict[str, Any]],
    plan_metas: List[Dict[str, Any]],
) -> str:
    """Return a strict JSON prompt for the trade-gate agent."""

    cancel_default_min = _env_int("EVCLAW_MANUAL_TRADE_CANCEL_AFTER_MINUTES", 60)

    rules = [
        "Return ONLY valid JSON (no markdown, no commentary).",
        "Output schema: {plans:[...]}.",
        "You MUST produce exactly one plan per input candidate.",
        "You MUST ONLY use the provided candidates list (no new symbols).",
        "Direction must be LONG or SHORT.",
        "LEARNED OVERRIDE: If a candidate includes a learning/veto note (dossier) that conflicts with the candidate direction (e.g. hard-veto longs), you MUST flip direction or set confidence_pct to 0.",
        "IMPORTANT: Do NOT mention the word 'dossier' or internal learning keys in the why bullets. Explain the flip in plain trading English (e.g. 'recent performance suggests this side is dangerous; prefer the other side').",
        "Confidence must be an integer percent 0-100. If dossier conflicts with direction, cap confidence at 15 max.",
        "Size must be in USD notional. Use plan_meta.recommended_size_usd unless user_size_usd is provided.",
        "SL/TP must be absolute prices; ensure correct ordering for LONG (SL<entry<TP) and SHORT (SL>entry>TP).",
        "Always include two options: FAST chase + RESTING limit.",
        f"Default cancel_after_minutes for RESTING is {int(cancel_default_min)} unless otherwise specified.",
        "RESTING limit must include limit_price and cancel_after_minutes.",
        "Do NOT include funding rate or 24h price change in why bullets.",
        "Do NOT include a raw pipeline score in why bullets.",
        "Ground your why bullets in the candidate fields (signals, trend_score, sr, atr_pct, context_adjustment) and SSE snapshot context. If learning suggests a veto/flip, describe it in plain English without saying 'dossier'.",
    ]

    payload = {
        "task": "manual_trade_gate",
        "seq": int(seq) if seq is not None else None,
        "global_context_compact": str(global_context_compact or ""),
        "exposure": exposure or {},
        "open_positions": open_positions or [],
        "plan_metas": plan_metas or [],
        "candidates": candidates_compact or [],
        "policy": {"rules": rules},
        "output_schema": {
            "plans": [
                {
                    "display_id": "ETH-01",
                    "symbol": "ETH",
                    "venue": "hyperliquid",
                    "direction_idea": "LONG",
                    "confidence_pct": 55,
                    "size_usd": 5000,
                    "sltp": {"sl_price": 100.0, "tp_price": 110.0},
                    "options": {
                        "chase": {"label": "FAST", "note": "..."},
                        "limit": {
                            "label": "RESTING",
                            "limit_price": 99.0,
                            "cancel_after_minutes": 60,
                            "note": "...",
                        },
                    },
                    "why": ["..."],
                }
            ]
        },
    }

    return "Return ONLY valid JSON.\n" + json.dumps(payload, separators=(",", ":"), ensure_ascii=False)


def _normalize_plan(plan: Dict[str, Any], *, meta: Dict[str, Any]) -> Dict[str, Any]:
    # enforce display_id/symbol/venue
    out: Dict[str, Any] = dict(plan or {})

    display_id = str(meta.get("display_id") or "").strip().upper()
    symbol = str(meta.get("symbol") or "").strip().upper()
    venue = str(meta.get("venue") or "hyperliquid").strip().lower() or "hyperliquid"

    out["display_id"] = display_id
    out["symbol"] = symbol
    out["venue"] = venue

    # direction
    direction = str(out.get("direction_idea") or "").strip().upper()
    if direction not in {"LONG", "SHORT"}:
        # fallback to meta suggestion
        direction = str(meta.get("direction_hint") or "LONG").strip().upper()
        if direction not in {"LONG", "SHORT"}:
            direction = "LONG"
    out["direction_idea"] = direction

    # confidence percent
    # Confidence is always mapped from our conviction model (meta.confidence_pct).
    # LLM may explain it, but should not invent a different numeric value.
    try:
        cp = int(meta.get("confidence_pct") or 50)
    except Exception:
        cp = 50
    out["confidence_pct"] = max(0, min(100, int(cp)))

    # size
    user_size = meta.get("user_size_usd")
    rec_size = meta.get("recommended_size_usd")
    try:
        rec_size = float(rec_size) if rec_size is not None else None
    except Exception:
        rec_size = None

    if user_size is not None:
        try:
            size_usd = float(user_size)
        except Exception:
            size_usd = 0.0
    else:
        try:
            size_usd = float(out.get("size_usd") or 0.0)
        except Exception:
            size_usd = 0.0
        if size_usd <= 0 and rec_size is not None:
            size_usd = float(rec_size)

    if size_usd <= 0 and rec_size is not None:
        size_usd = float(rec_size)
    out["size_usd"] = float(max(0.0, size_usd))

    # sltp
    sltp = out.get("sltp")
    if not isinstance(sltp, dict):
        sltp = {}
    try:
        sl = float(sltp.get("sl_price") or 0.0)
    except Exception:
        sl = 0.0
    try:
        tp = float(sltp.get("tp_price") or 0.0)
    except Exception:
        tp = 0.0
    out["sltp"] = {"sl_price": sl, "tp_price": tp}

    # options
    options = out.get("options")
    if not isinstance(options, dict):
        options = {}
    chase = options.get("chase")
    if not isinstance(chase, dict):
        chase = {}
    chase.setdefault("label", "FAST")
    chase.setdefault("note", "FAST chase_limit entry")

    limit = options.get("limit")
    if not isinstance(limit, dict):
        limit = {}
    limit.setdefault("label", "RESTING")
    try:
        limit_price = float(limit.get("limit_price") or 0.0)
    except Exception:
        limit_price = 0.0
    limit["limit_price"] = limit_price

    cancel_default = int(meta.get("cancel_after_minutes") or _env_int("EVCLAW_MANUAL_TRADE_CANCEL_AFTER_MINUTES", 60))
    try:
        cancel_after = int(float(limit.get("cancel_after_minutes") or cancel_default))
    except Exception:
        cancel_after = cancel_default
    cancel_after = max(1, min(24 * 60, cancel_after))
    limit["cancel_after_minutes"] = cancel_after

    options["chase"] = chase
    options["limit"] = limit
    out["options"] = options

    why = out.get("why")
    if not isinstance(why, list):
        why = []
    why = [str(x) for x in why if str(x).strip()][:3]
    out["why"] = why

    return out


async def run_trade_gate(
    *,
    seq: Optional[int],
    global_context_compact: str,
    exposure: Dict[str, Any],
    open_positions: List[Dict[str, Any]],
    candidates_compact: List[Dict[str, Any]],
    plan_metas: List[Dict[str, Any]],
) -> Dict[str, Any]:
    cfg = TradeGateConfig.load()
    if not cfg.enabled:
        return {"enabled": False, "plans": [], "error": "disabled"}

    session_id = _build_session_id("hl_trade_gate")
    prompt = build_trade_gate_prompt(
        seq=seq,
        global_context_compact=global_context_compact,
        exposure=exposure,
        open_positions=open_positions,
        candidates_compact=candidates_compact,
        plan_metas=plan_metas,
    )

    meta, assistant_text = await openclaw_agent_turn(
        message=prompt,
        session_id=session_id,
        agent_id=cfg.agent_id,
        thinking=cfg.thinking,
        timeout_sec=cfg.timeout_sec,
        openclaw_cmd=cfg.openclaw_cmd,
    )

    if not assistant_text:
        err_kind = None
        stderr = None
        if isinstance(meta, dict):
            err_kind = str(meta.get("error_kind") or "") or None
            stderr = str(meta.get("stderr") or "") or None
        return {
            "enabled": True,
            "plans": [],
            "error": f"empty_reply:{err_kind or 'unknown'}",
            "session_id": session_id,
            "meta": meta,
            "stderr": (stderr[:400] if isinstance(stderr, str) and stderr else None),
            "raw": assistant_text,
        }

    decision = safe_json_loads(assistant_text)
    if not isinstance(decision, dict):
        # One retry with a minimal schema reminder.
        retry_prompt = (
            "Your previous answer was not valid JSON.\n"
            "Return ONLY valid JSON with this exact schema:\n"
            '{"plans":[{"display_id":"...","symbol":"...","venue":"hyperliquid","direction_idea":"LONG|SHORT","confidence_pct":55,"size_usd":1234,"sltp":{"sl_price":0,"tp_price":0},"options":{"chase":{"label":"FAST","note":"..."},"limit":{"label":"RESTING","limit_price":0,"cancel_after_minutes":60,"note":"..."}},"why":["..."]}]}\n'
            + json.dumps(
                {
                    "candidates": [
                        {
                            "symbol": str(c.get("symbol") or ""),
                            "direction": str(c.get("direction") or ""),
                        }
                        for c in (candidates_compact or [])
                        if isinstance(c, dict)
                    ],
                    "plan_metas": [
                        {
                            "symbol": str(m.get("symbol") or ""),
                            "display_id": str(m.get("display_id") or ""),
                        }
                        for m in (plan_metas or [])
                        if isinstance(m, dict)
                    ],
                },
                separators=(",", ":"),
                ensure_ascii=False,
            )
        )
        retry_meta, retry_text = await openclaw_agent_turn(
            message=retry_prompt,
            session_id=f"{session_id}_retry1",
            agent_id=cfg.agent_id,
            thinking="minimal",
            timeout_sec=min(10.0, float(cfg.timeout_sec)),
            openclaw_cmd=cfg.openclaw_cmd,
        )
        if retry_text:
            decision = safe_json_loads(retry_text)
            assistant_text = retry_text
            meta = retry_meta

    if not isinstance(decision, dict):
        return {
            "enabled": True,
            "plans": [],
            "error": "invalid_json",
            "session_id": session_id,
            "meta": meta,
            "raw": assistant_text,
        }

    plans = decision.get("plans")
    if not isinstance(plans, list):
        return {"enabled": True, "plans": [], "error": "missing_plans", "session_id": session_id, "raw": assistant_text}

    meta_by_symbol = {str(m.get("symbol") or "").upper(): dict(m) for m in (plan_metas or []) if isinstance(m, dict)}

    norm_plans: List[Dict[str, Any]] = []
    for it in plans:
        if not isinstance(it, dict):
            continue
        sym = str(it.get("symbol") or "").upper()
        m = meta_by_symbol.get(sym)
        if not m:
            # try display_id match
            did = str(it.get("display_id") or "").upper()
            m = next((x for x in plan_metas if str((x or {}).get("display_id") or "").upper() == did), None)
            m = dict(m) if isinstance(m, dict) else None
        if not isinstance(m, dict):
            continue
        norm_plans.append(_normalize_plan(it, meta=m))

    # Fill missing symbols with basic fallback plans
    have = {str(p.get("symbol") or "").upper() for p in norm_plans}
    for m in plan_metas or []:
        if not isinstance(m, dict):
            continue
        sym = str(m.get("symbol") or "").upper()
        if not sym or sym in have:
            continue
        fallback = {
            "display_id": str(m.get("display_id") or "").upper(),
            "symbol": sym,
            "venue": str(m.get("venue") or "hyperliquid").lower(),
            "direction_idea": str(m.get("direction_hint") or "LONG").upper(),
            "confidence_pct": int(m.get("confidence_pct") or 50),
            "size_usd": float(m.get("recommended_size_usd") or 0.0),
            "sltp": {"sl_price": 0.0, "tp_price": 0.0},
            "options": {
                "chase": {"label": "FAST", "note": "fallback"},
                "limit": {
                    "label": "RESTING",
                    "limit_price": 0.0,
                    "cancel_after_minutes": int(m.get("cancel_after_minutes") or 60),
                    "note": "fallback",
                },
            },
            "why": ["fallback_plan"],
        }
        norm_plans.append(_normalize_plan(fallback, meta=dict(m)))

    return {
        "enabled": True,
        "plans": norm_plans,
        "session_id": session_id,
        "raw": assistant_text,
        "meta": meta,
    }
