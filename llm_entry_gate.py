#!/usr/bin/env python3
"""LLM entry-gate for EVClaw using OpenClaw.

This is Opt2: after deterministic candidate generation, ask a stateless agent to
rank/veto a small candidate set.

No direct model-provider API calls. We use `openclaw agent`.

Safety:
- Agent can only select from provided candidates.
- Keep list is capped.
- Any failure -> fallback to current deterministic flow.
"""

from __future__ import annotations

import json
import logging
import os
import secrets
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import sqlite3
from pathlib import Path

from openclaw_agent_client import openclaw_agent_turn, safe_json_loads
from env_utils import env_float as _compat_env_float, env_int as _compat_env_int, env_str as _compat_env_str
from conviction_model import (
    compute_blended_conviction,
    compute_brain_conviction_no_floor,
)

_MAX_DOSSIER_CHARS = 480
_SKILL_DIR = Path(__file__).parent
_DB_PATH = str(_SKILL_DIR / "ai_trader.db")
_LOG = logging.getLogger(__name__)

SECTOR_MAP = {
    "BTC": "BTC", "ETH": "L1", "SOL": "L1", "AVAX": "L1", "SUI": "L1",
    "SEI": "L1", "TIA": "L1", "NEAR": "L1", "ADA": "L1",
    "LINK": "Oracle", "PYTH": "Oracle",
    "UNI": "DeFi", "AAVE": "DeFi", "CRV": "DeFi", "PENDLE": "DeFi",
    "LDO": "DeFi", "ENA": "DeFi", "RESOLV": "DeFi",
    "DOGE": "Meme", "WIF": "Meme", "FARTCOIN": "Meme", "SPX": "Meme",
    "MON": "Meme", "TRUMP": "Meme",
    "JUP": "Solana-DeFi",
    "HYPE": "Exchange", "ASTER": "Exchange", "DYDX": "Exchange", "BNB": "Exchange",
    "XRP": "Payment", "LTC": "Payment", "BCH": "Payment",
    "ZEC": "Privacy",
    "ZK": "L2", "POL": "L2", "MNT": "L2", "ZRO": "L2",
    "ICP": "Infra", "LIT": "Infra", "XPL": "Infra",
    "SKR": "AI", "AVNT": "AI",
    "ZORA": "NFT",
}


def _env_int(name: str, default: int) -> int:
    return _compat_env_int(name, default)


def _env_float(name: str, default: float) -> float:
    return _compat_env_float(name, default)


def _truthy(name: str, default: bool = False) -> bool:
    raw = _compat_env_str(name, None)
    if raw is None:
        return default
    return str(raw).strip().lower() in {"1", "true", "yes", "y", "on"}


def _normalize_agent_id(value: Optional[str]) -> Optional[str]:
    raw = str(value or "").strip()
    if not raw or raw.lower() in {"default", "openclaw-default"}:
        return None
    return raw


def _build_entry_gate_session_id(seq: int, *, mode: str = "normal") -> str:
    stamp_ms = int(time.time() * 1000)
    suffix = secrets.token_hex(4)
    prefix = "hl_hip3_entry_gate" if str(mode or "").strip().lower() == "hip3" else "hl_entry_gate"
    return f"{prefix}_{int(seq)}_{stamp_ms}_{suffix}"


def entry_gate_enabled_env(default: bool = False, *, mode: str = "normal") -> bool:
    """Shared env toggle parser for entry gate callers."""
    if str(mode or "").strip().lower() == "hip3":
        return True
    return True


def _cand_key(d: Dict[str, Any]) -> Tuple[str, str]:
    return (
        str(d.get("symbol") or "").upper().strip(),
        str(d.get("direction") or "").upper().strip(),
    )


def _get_val(obj: Any) -> Any:
    if isinstance(obj, dict) and "value" in obj:
        return obj.get("value")
    return obj


def _safe_conviction(value: Any, default: float = 0.0) -> float:
    try:
        out = float(value)
    except Exception:
        out = float(default)
    return max(0.0, min(1.0, out))


def _extract_sr(km: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Extract compact SR summary from key-metrics payload."""
    try:
        sr_levels = km.get("sr_levels") or {}
        nearest = (sr_levels.get("nearest") or {}) if isinstance(sr_levels, dict) else {}
        sup = (nearest.get("support") or {}) if isinstance(nearest, dict) else {}
        res = (nearest.get("resistance") or {}) if isinstance(nearest, dict) else {}
        return {
            "price": sr_levels.get("price") or km.get("price"),
            "support": sup.get("price"),
            "resistance": res.get("price"),
        }
    except Exception:
        return None


def _enrich_sector_exposure(result: Dict[str, Any], symbol: str, direction: str) -> None:
    """Add sector_exposure to entry candidate (best-effort)."""
    try:
        base_sym = symbol.split(":")[-1].upper() if ":" in symbol else symbol.upper()
        sector = SECTOR_MAP.get(base_sym)
        if sector:
            with sqlite3.connect(_DB_PATH) as conn:
                rows = conn.execute(
                    "SELECT symbol, direction FROM trades WHERE exit_time IS NULL"
                ).fetchall()
                same_dir = 0
                total_sect = 0
                for r in rows:
                    s = str(r[0] or "")
                    bs = s.split(":")[-1].upper() if ":" in s else s.upper()
                    if SECTOR_MAP.get(bs) == sector:
                        total_sect += 1
                        if str(r[1] or "").upper() == direction.upper():
                            same_dir += 1
                result["sector_exposure"] = {
                    "sector": sector,
                    "same_dir_count": same_dir,
                    "total_sector_positions": total_sect,
                }
    except Exception as exc:
        _LOG.warning("entry-gate sector_exposure enrichment failed for %s: %s", symbol, exc)


def _compact_candidate(c: Dict[str, Any]) -> Dict[str, Any]:
    """Trim candidate to a small summary for the gate prompt."""

    sym = str(c.get("symbol") or "").upper()
    direction = str(c.get("direction") or "").upper()

    trend = _get_val(c.get("trend_score"))
    atr_pct = _get_val(c.get("atr_pct"))
    z24 = _get_val(c.get("pct_24h_zscore"))

    ctx_adj = c.get("context_adjustment") or {}
    ctx_adj_val = _get_val(ctx_adj) if not isinstance(ctx_adj, dict) else _get_val(ctx_adj.get("value"))

    dossier = str(c.get("symbol_learning_dossier") or "").strip()
    if len(dossier) > _MAX_DOSSIER_CHARS:
        tail_len = max(0, _MAX_DOSSIER_CHARS - 3)
        dossier = "..." + dossier[-tail_len:]

    km: Dict[str, Any] = {}
    if isinstance(c.get("context_snapshot"), dict):
        km_raw = (c.get("context_snapshot") or {}).get("key_metrics") or {}
        if isinstance(km_raw, dict):
            km = km_raw

    pipeline_conv = _safe_conviction(
        c.get("pipeline_conviction", c.get("conviction")),
        0.0,
    )
    brain_raw = c.get("brain_conviction")
    signals_snapshot = c.get("signals_snapshot")
    has_signals_snapshot = isinstance(signals_snapshot, dict) and bool(signals_snapshot)
    if brain_raw is None:
        if not isinstance(signals_snapshot, dict):
            signals_snapshot = {}
        brain_conv = compute_brain_conviction_no_floor(
            signals_snapshot=signals_snapshot,
            key_metrics=km,
            direction=direction,
        )
    else:
        brain_conv = _safe_conviction(brain_raw, 0.0)
    blended_raw = c.get("blended_conviction")
    if blended_raw is None:
        if brain_raw is None and not has_signals_snapshot:
            blended_conv = pipeline_conv
        else:
            blended_conv = compute_blended_conviction(pipeline_conv, brain_conv)
    else:
        blended_conv = _safe_conviction(blended_raw, 0.0)

    # SR compact (if present)
    sr = _extract_sr(km)
    hip3_driver = None
    hip3_flow_pass = None
    hip3_ofm_pass = None
    hip3_booster_score = None
    hip3_booster_size_mult = None
    try:
        sig_snap = c.get("signals_snapshot") if isinstance(c.get("signals_snapshot"), dict) else {}
        hip3 = sig_snap.get("hip3_main") if isinstance(sig_snap, dict) else None
        if isinstance(hip3, dict) and hip3:
            hip3_driver = str(hip3.get("driver_type") or "").strip().lower() or None
            hip3_flow_pass = bool(hip3.get("flow_pass")) if "flow_pass" in hip3 else None
            hip3_ofm_pass = bool(hip3.get("ofm_pass")) if "ofm_pass" in hip3 else None
            comps = hip3.get("components") if isinstance(hip3.get("components"), dict) else {}
            if isinstance(comps, dict):
                try:
                    hip3_booster_score = float(comps.get("rest_booster_score"))
                except Exception:
                    hip3_booster_score = None
                try:
                    hip3_booster_size_mult = float(comps.get("rest_booster_size_mult"))
                except Exception:
                    hip3_booster_size_mult = None
    except Exception:
        pass

    result = {
        "symbol": sym,
        "direction": direction,
        "conviction": blended_conv,
        "pipeline_conviction": pipeline_conv,
        "brain_conviction": brain_conv,
        "blended_conviction": blended_conv,
        "signals": c.get("signals") or [],
        "strong_signals": c.get("strong_signals") or [],
        "must_trade": bool(c.get("must_trade") or False),
        "constraint_reason": c.get("constraint_reason"),
        "trend_score": trend,
        "atr_pct": atr_pct,
        "pct_24h_zscore": z24,
        "context_adjustment": ctx_adj_val,
        "symbol_learning_dossier": dossier or None,
        "price": c.get("price") or km.get("price"),
        "funding_rate": _get_val(c.get("funding_rate")) or km.get("funding_rate"),
        "volume_24h_usd": _get_val(c.get("volume_24h_usd")) or km.get("volume_24h"),
        "trend_regime": ((km.get("trend_state") or {}).get("regime") if isinstance(km.get("trend_state"), dict) else None),
        "sr": sr,
        "rank": c.get("rank"),
        "is_hip3": bool(sym.startswith("XYZ:")),
        "hip3_driver": hip3_driver,
        "hip3_flow_pass": hip3_flow_pass,
        "hip3_ofm_pass": hip3_ofm_pass,
        "hip3_booster_score": hip3_booster_score,
        "hip3_booster_size_mult": hip3_booster_size_mult,
    }

    # Enrich with sector exposure (best-effort)
    _enrich_sector_exposure(result, sym, direction)
    return result


def build_entry_gate_prompt(
    *,
    seq: int,
    global_context_compact: str,
    exposure_context: Optional[Dict[str, Any]],
    candidates: List[Dict[str, Any]],
    max_pick: int,
    mode: str = "normal",
) -> str:
    """Build Opt2 prompt.

    New contract (pick-mode): the agent must provide a reason for BOTH picks and rejects.

    Return schema:
      {
        "picks":[{"symbol","direction","size_mult"?,"reason"}],
        "rejects":[{"symbol","direction","reason"}]
      }

    Requirements:
    - At most max_pick in picks.
    - Every provided candidate must appear EXACTLY ONCE in either picks or rejects.
    """

    exposure = exposure_context or {}
    open_positions = exposure.get("open_positions") if isinstance(exposure, dict) else []
    if not isinstance(open_positions, list):
        open_positions = []

    mode_norm = str(mode or "").strip().lower()
    rules = [
        f"Pick at most {max_pick} candidates to TRADE (picks).",
        "Every candidate you do NOT pick must be returned in rejects with a clear reason.",
        "You MUST ONLY choose from the provided candidates list.",
        "Do NOT invent new symbols or directions.",
        "Return ONLY valid JSON (no markdown, no commentary).",
        "For EVERY pick and EVERY reject you MUST provide a non-empty reason.",
        "Use open_positions to avoid concentration/redundant exposure in the same direction.",
        "Check sector_exposure: reject if same_dir_count >= 6 in same sector (too concentrated).",
        "Do not decide order_type. Runtime applies deterministic order_type from conviction thresholds.",
        "Optional: size_mult is a float in [0.5, 2.0] (default 1.0).",
    ]
    if mode_norm == "hip3":
        rules.extend(
            [
                "HIP3 mode: FLOW/OFM are primary drivers. REST boosters may adjust size/confidence only.",
                "Prioritize book balance: keep HIP3 net exposure near configured target without over-skew.",
                "If soft/hard skew bands are provided, avoid picks that worsen drift beyond hard band.",
                "When uncertain between equal setups, prefer the side that improves target net exposure balance.",
            ]
        )
    else:
        rules.append(
            "Priority: if any HIP3_MAIN / XYZ candidates are present (symbol starts with 'XYZ:' or signals mention HIP3_MAIN), prefer picking those over non-HIP3 candidates when risk/exposure allows."
        )

    policy = {
        "task": "entry_gate_pick_mode_hip3" if mode_norm == "hip3" else "entry_gate_pick_mode",
        "rules": rules,
    }

    compact = [_compact_candidate(c) for c in (candidates or [])]

    payload = {
        "gate_mode": mode_norm or "normal",
        "seq": int(seq),
        "global_context_compact": str(global_context_compact or ""),
        "exposure": exposure,
        "open_positions": open_positions,
        "candidates": compact,
        "max_pick": int(max_pick),
        "policy": policy,
    }

    return "Return ONLY valid JSON.\n" + json.dumps(
        payload, separators=(",", ":"), ensure_ascii=False
    )


@dataclass
class EntryGateConfig:
    enabled: bool
    agent_id: Optional[str]
    model: Optional[str]
    thinking: Optional[str]
    timeout_sec: float
    max_pick: int
    openclaw_cmd: str
    retry_on_parse_fail: bool

    @classmethod
    def load(cls, *, mode: str = "normal") -> "EntryGateConfig":
        mode_norm = str(mode or "").strip().lower()
        hip3_mode = mode_norm == "hip3"
        env_prefix = "EVCLAW_HIP3_LLM_GATE_" if hip3_mode else "EVCLAW_LLM_GATE_"
        if hip3_mode:
            enabled_default = True
            agent_id = _normalize_agent_id(
                _compat_env_str(
                    "EVCLAW_HIP3_LLM_GATE_AGENT_ID",
                    _compat_env_str("EVCLAW_LLM_GATE_AGENT_ID", "default"),
                )
            )
            model = (
                _compat_env_str("EVCLAW_HIP3_LLM_GATE_MODEL", "openai-codex/gpt-5.2")
                or "openai-codex/gpt-5.2"
            ).strip() or None
            thinking = (
                _compat_env_str("EVCLAW_HIP3_LLM_GATE_THINKING", "medium")
                or "medium"
            ).strip() or None
        else:
            enabled_default = entry_gate_enabled_env(False, mode=mode_norm)
            agent_id = _normalize_agent_id(
                _compat_env_str("EVCLAW_LLM_GATE_AGENT_ID", "default")
            )
            model = (
                _compat_env_str(f"{env_prefix}MODEL", "openai-codex/gpt-5.2")
                or "openai-codex/gpt-5.2"
            ).strip() or None
            thinking = (
                _compat_env_str(f"{env_prefix}THINKING", "medium")
                or "medium"
            ).strip() or None
        timeout_name = f"{env_prefix}TIMEOUT_SEC"
        max_keep_name = f"{env_prefix}MAX_KEEP"
        retry_name = f"{env_prefix}RETRY_ON_PARSE_FAIL"
        return cls(
            enabled=enabled_default,
            agent_id=agent_id,
            model=model,
            thinking=thinking,
            timeout_sec=max(
                1.0, _env_float(timeout_name, _env_float("EVCLAW_LLM_GATE_TIMEOUT_SEC", 120.0))
            ),
            max_pick=max(0, _env_int(max_keep_name, _env_int("EVCLAW_LLM_GATE_MAX_KEEP", 4))),
            openclaw_cmd=(os.getenv("OPENCLAW_CMD") or "openclaw").strip(),
            retry_on_parse_fail=_truthy(
                retry_name, _truthy("EVCLAW_LLM_GATE_RETRY_ON_PARSE_FAIL", True)
            ),
        )


_ENTRY_GATE_CFG_KEYS_COMMON = (
    "OPENCLAW_CMD",
)
_ENTRY_GATE_CFG_KEYS_NORMAL = (
    "EVCLAW_LLM_GATE_AGENT_ID",
    "EVCLAW_LLM_GATE_MODEL",
    "EVCLAW_LLM_GATE_THINKING",
    "EVCLAW_LLM_GATE_TIMEOUT_SEC",
    "EVCLAW_LLM_GATE_MAX_KEEP",
    "EVCLAW_LLM_GATE_RETRY_ON_PARSE_FAIL",
)
_ENTRY_GATE_CFG_KEYS_HIP3 = (
    "EVCLAW_HIP3_LLM_GATE_AGENT_ID",
    "EVCLAW_HIP3_LLM_GATE_MODEL",
    "EVCLAW_HIP3_LLM_GATE_THINKING",
    "EVCLAW_HIP3_LLM_GATE_TIMEOUT_SEC",
    "EVCLAW_HIP3_LLM_GATE_MAX_KEEP",
    "EVCLAW_HIP3_LLM_GATE_RETRY_ON_PARSE_FAIL",
)
_ENTRY_GATE_CFG_CACHE: Dict[str, Tuple[Tuple[Optional[str], ...], EntryGateConfig]] = {}


def _entry_gate_cfg_fingerprint(*, mode: str = "normal") -> Tuple[Optional[str], ...]:
    mode_norm = str(mode or "").strip().lower()
    mode_keys = _ENTRY_GATE_CFG_KEYS_HIP3 if mode_norm == "hip3" else _ENTRY_GATE_CFG_KEYS_NORMAL
    all_keys = tuple(mode_keys) + tuple(_ENTRY_GATE_CFG_KEYS_COMMON)
    return tuple(_compat_env_str(k, None) for k in all_keys)


def _get_entry_gate_config(*, mode: str = "normal") -> EntryGateConfig:
    global _ENTRY_GATE_CFG_CACHE
    mode_norm = str(mode or "").strip().lower() or "normal"
    fp = _entry_gate_cfg_fingerprint(mode=mode_norm)
    cached = _ENTRY_GATE_CFG_CACHE.get(mode_norm)
    if cached and cached[0] == fp:
        return cached[1]
    cfg = EntryGateConfig.load(mode=mode_norm)
    _ENTRY_GATE_CFG_CACHE[mode_norm] = (fp, cfg)
    return cfg


@dataclass
class EntryGateDecision:
    enabled: bool
    picks: List[Dict[str, Any]]
    rejects: List[Dict[str, Any]]
    error: Optional[str] = None
    error_kind: Optional[str] = None
    stderr_excerpt: Optional[str] = None
    session_id: Optional[str] = None
    raw_text: Optional[str] = None


def _normalize_gate_decision(
    *,
    candidates: List[Dict[str, Any]],
    picks_raw: Any,
    rejects_raw: Any,
    max_pick: int,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """Normalize partially-valid gate output without disabling the whole gate."""
    cand_keys: List[Tuple[str, str]] = []
    cand_key_set = set()
    for c in (candidates or []):
        if not isinstance(c, dict):
            continue
        key = _cand_key(c)
        if not key[0] or key[1] not in {"LONG", "SHORT"}:
            continue
        cand_keys.append(key)
        cand_key_set.add(key)

    picks: List[Dict[str, Any]] = []
    rejects: List[Dict[str, Any]] = []
    seen = set()

    picks_list = picks_raw if isinstance(picks_raw, list) else []
    rejects_list = rejects_raw if isinstance(rejects_raw, list) else []

    for it in picks_list:
        if not isinstance(it, dict):
            continue
        key = _cand_key(it)
        if key not in cand_key_set or key in seen:
            continue
        reason = str(it.get("reason") or "").strip()
        if not reason:
            continue
        row: Dict[str, Any] = {
            "symbol": key[0],
            "direction": key[1],
            "reason": reason,
        }
        size_mult = it.get("size_mult")
        if size_mult is not None:
            try:
                row["size_mult"] = max(0.5, min(2.0, float(size_mult)))
            except Exception:
                pass
        picks.append(row)
        seen.add(key)

    if max_pick > 0 and len(picks) > int(max_pick):
        picks = picks[: int(max_pick)]
        seen = {(str(p["symbol"]).upper(), str(p["direction"]).upper()) for p in picks}

    for it in rejects_list:
        if not isinstance(it, dict):
            continue
        key = _cand_key(it)
        if key not in cand_key_set or key in seen:
            continue
        reason = str(it.get("reason") or it.get("note") or "").strip() or "rejected_by_gate"
        rejects.append({"symbol": key[0], "direction": key[1], "reason": reason})
        seen.add(key)

    # Fill any missing candidates as rejected so coverage remains total.
    for key in cand_keys:
        if key in seen:
            continue
        rejects.append({"symbol": key[0], "direction": key[1], "reason": "rejected_by_gate_default"})
        seen.add(key)

    return picks, rejects


async def run_entry_gate(
    *,
    seq: int,
    global_context_compact: str,
    exposure_context: Optional[Dict[str, Any]] = None,
    candidates: List[Dict[str, Any]],
    mode: str = "normal",
) -> EntryGateDecision:
    mode_norm = str(mode or "").strip().lower() or "normal"
    cfg = _get_entry_gate_config(mode=mode_norm)
    if not cfg.enabled:
        return EntryGateDecision(enabled=False, picks=[], rejects=[])

    if not candidates:
        return EntryGateDecision(enabled=True, picks=[], rejects=[])

    # Stateless per-cycle session with collision-resistant suffix.
    session_id = _build_entry_gate_session_id(int(seq), mode=mode_norm)
    prompt = build_entry_gate_prompt(
        seq=int(seq),
        global_context_compact=str(global_context_compact or ""),
        exposure_context=exposure_context or {},
        candidates=candidates,
        max_pick=cfg.max_pick,
        mode=mode_norm,
    )

    meta, assistant_text = await openclaw_agent_turn(
        message=prompt,
        session_id=session_id,
        agent_id=cfg.agent_id,
        model=cfg.model,
        thinking=cfg.thinking,
        timeout_sec=cfg.timeout_sec,
        openclaw_cmd=cfg.openclaw_cmd,
    )

    error_kind = None
    stderr_excerpt = None
    if isinstance(meta, dict):
        error_kind = str(meta.get("error_kind") or "none")
        stderr_raw = str(meta.get("stderr") or "")
        stderr_excerpt = stderr_raw[:300] if stderr_raw else None
    if not assistant_text:
        err = "empty_reply"
        if error_kind and error_kind != "none":
            err = f"empty_reply:{error_kind}"
        _LOG.warning(
            "entry-gate empty reply seq=%s err=%s kind=%s stderr=%r",
            seq,
            err,
            error_kind,
            stderr_excerpt,
        )
        return EntryGateDecision(
            enabled=True,
            picks=[],
            rejects=[],
            error=err,
            error_kind=error_kind,
            stderr_excerpt=stderr_excerpt,
            session_id=session_id,
            raw_text=assistant_text,
        )

    decision = safe_json_loads(assistant_text)
    if not isinstance(decision, dict) and cfg.retry_on_parse_fail:
        retry_compact = [
            {
                "symbol": str(c.get("symbol") or "").upper(),
                "direction": str(c.get("direction") or "").upper(),
            }
            for c in (candidates or [])
            if isinstance(c, dict)
        ]
        retry_prompt = (
            "Your previous answer was not valid JSON.\n"
            "Return ONLY valid JSON with this exact schema:\n"
            '{"picks":[{"symbol":"...","direction":"LONG|SHORT","size_mult":1.0,"reason":"..."}],"rejects":[{"symbol":"...","direction":"LONG|SHORT","reason":"..."}]}\n'
            "Every candidate must appear exactly once in picks or rejects.\n"
            + json.dumps(
                {
                    "seq": int(seq),
                    "max_pick": int(cfg.max_pick),
                    "candidates": retry_compact,
                },
                separators=(",", ":"),
                ensure_ascii=False,
            )
        )
        retry_meta, retry_text = await openclaw_agent_turn(
            message=retry_prompt,
            session_id=f"{session_id}_retry1",
            agent_id=cfg.agent_id,
            model=cfg.model,
            thinking="minimal",
            timeout_sec=min(8.0, float(cfg.timeout_sec)),
            openclaw_cmd=cfg.openclaw_cmd,
        )
        if retry_text:
            retry_decision = safe_json_loads(retry_text)
            if isinstance(retry_decision, dict):
                decision = retry_decision
                assistant_text = retry_text
            else:
                if isinstance(retry_meta, dict):
                    error_kind = str(retry_meta.get("error_kind") or error_kind or "none")
        elif isinstance(retry_meta, dict):
            error_kind = str(retry_meta.get("error_kind") or error_kind or "none")
    if not isinstance(decision, dict):
        err = "invalid_json"
        if error_kind and error_kind != "none":
            err = f"invalid_json:{error_kind}"
        _LOG.warning(
            "entry-gate invalid json seq=%s err=%s kind=%s stderr=%r raw=%r",
            seq,
            err,
            error_kind,
            stderr_excerpt,
            (assistant_text or "")[:160],
        )
        return EntryGateDecision(
            enabled=True,
            picks=[],
            rejects=[],
            error=err,
            error_kind=error_kind,
            stderr_excerpt=stderr_excerpt,
            session_id=session_id,
            raw_text=assistant_text,
        )

    picks = decision.get("picks")
    rejects = decision.get("rejects")
    picks_norm, rejects_norm = _normalize_gate_decision(
        candidates=candidates,
        picks_raw=picks,
        rejects_raw=rejects,
        max_pick=int(cfg.max_pick),
    )

    return EntryGateDecision(
        enabled=True,
        picks=picks_norm,
        rejects=rejects_norm,
        error_kind=error_kind,
        stderr_excerpt=stderr_excerpt,
        session_id=session_id,
        raw_text=assistant_text,
    )
