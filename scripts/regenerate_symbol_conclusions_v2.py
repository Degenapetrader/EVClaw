#!/usr/bin/env python3
"""Regenerate symbol conclusions from reflections (dry-run/apply).

This tool is maintainer/advanced-user oriented and is intentionally separate
from bootstrap/import flow. Standard users usually import release seeds.

Model routing:
- If --model is omitted (default), OpenClaw agent routing decides model.
- If --model is provided, that model is requested explicitly.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sqlite3
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

# Ensure repo root is importable when run as: python3 scripts/<tool>.py
REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ai_trader_db import AITraderDB
from learning_reflector_worker import (
    DEFAULT_AGENT_ID,
    DEFAULT_DB,
    DEFAULT_THINKING,
    _fetch_new_symbol_lessons,
    build_symbol_conclusion_prompt,
)
from openclaw_agent_client import openclaw_agent_turn, safe_json_loads

DEFAULT_REGEN_MODEL = (os.getenv("EVCLAW_LEARNING_REFLECTOR_MODEL") or "").strip()


@dataclass
class SymbolCandidate:
    symbol: str
    lesson_count: int


@dataclass
class SymbolResult:
    symbol: str
    lesson_count: int
    closed_trades: int
    batches: int
    changed: bool
    old_long_adj: float
    old_short_adj: float
    new_long_adj: float
    new_short_adj: float
    error: Optional[str] = None


def _safe_float(x: Any, default: float = 0.0) -> float:
    try:
        if x is None:
            return float(default)
        return float(x)
    except Exception:
        return float(default)


def _cap(s: str, n: int) -> str:
    txt = str(s or "")
    if len(txt) <= n:
        return txt
    return txt[: max(0, n - 1)] + "…"


def _safe_json_dict(raw: Any) -> Dict[str, Any]:
    if isinstance(raw, dict):
        return raw
    if raw is None:
        return {}
    try:
        parsed = json.loads(str(raw))
        return parsed if isinstance(parsed, dict) else {}
    except Exception:
        return {}


def _extract_adjustments(raw_json: Any) -> Dict[str, float]:
    obj = _safe_json_dict(raw_json)
    return {
        "long": float(_safe_float(obj.get("long_adjustment"), 0.0)),
        "short": float(_safe_float(obj.get("short_adjustment"), 0.0)),
    }


def _regen_retry_hint(prompt: str, bad_output: str) -> str:
    trimmed = str(bad_output or "").strip()
    if len(trimmed) > 2000:
        trimmed = trimmed[:2000] + "...<truncated>"
    return (
        prompt
        + "\n\nIMPORTANT: Return ONLY one valid JSON object. "
        + "No prose, no markdown, no code fences.\n"
        + "Your previous response was invalid JSON/object:\n"
        + trimmed
    )


async def _call_agent_json_retry(
    *,
    agent_id: Optional[str],
    model: Optional[str],
    thinking: str,
    session_id: str,
    message: str,
    timeout_sec: float,
    max_retries: int,
    retry_sleep_sec: float,
    openclaw_cmd: str,
) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    prompt = str(message or "")
    retries = max(0, int(max_retries))
    for attempt in range(retries + 1):
        meta, txt = await openclaw_agent_turn(
            agent_id=agent_id,
            model=(str(model).strip() if model else None),
            thinking=thinking,
            message=prompt,
            session_id=str(session_id),
            timeout_sec=timeout_sec,
            openclaw_cmd=str(openclaw_cmd or "openclaw"),
        )
        parsed = safe_json_loads(txt)
        if isinstance(parsed, dict):
            return meta or {}, parsed
        if attempt >= retries:
            err_kind = str((meta or {}).get("error_kind") or "none")
            raise ValueError(f"agent did not return JSON object (error_kind={err_kind})")
        prompt = _regen_retry_hint(message, txt)
        if retry_sleep_sec > 0:
            await asyncio.sleep(retry_sleep_sec)
    raise RuntimeError("unreachable")


def _normalize_conclusion(raw: Dict[str, Any]) -> Tuple[str, float, str]:
    data = dict(raw or {})
    text = _cap(str(data.get("conclusion_text") or "").strip(), 1200)
    conf = _safe_float(data.get("confidence"), 0.0)
    conf = max(0.0, min(1.0, float(conf)))

    j = data.get("conclusion_json")
    if isinstance(j, (dict, list)):
        j_obj: Any = j
    else:
        parsed = safe_json_loads(str(j or ""))
        if isinstance(parsed, (dict, list)):
            j_obj = parsed
        elif j is not None and str(j).strip():
            j_obj = {"raw": _cap(str(j), 8000)}
        else:
            j_obj = {}
    j_txt = json.dumps(j_obj, separators=(",", ":"), ensure_ascii=False)
    return text, conf, j_txt


def _fetch_symbol_closed_trades(db_path: str, symbol: str) -> int:
    conn = sqlite3.connect(db_path, timeout=30.0)
    try:
        row = conn.execute(
            "SELECT COUNT(*) FROM trades WHERE symbol = ? AND exit_time IS NOT NULL",
            (str(symbol or "").strip().upper(),),
        ).fetchone()
        return int((row[0] if row else 0) or 0)
    except Exception:
        return 0
    finally:
        conn.close()


def _load_symbol_candidates(
    db_path: str,
    *,
    symbols_filter: Sequence[str],
    min_lessons: int,
    max_symbols: int,
) -> List[SymbolCandidate]:
    conn = sqlite3.connect(db_path, timeout=30.0)
    conn.row_factory = sqlite3.Row
    try:
        clauses = ["t.symbol IS NOT NULL", "TRIM(t.symbol) <> ''"]
        params: List[Any] = []
        if symbols_filter:
            placeholders = ",".join(["?"] * len(symbols_filter))
            clauses.append(f"UPPER(t.symbol) IN ({placeholders})")
            params.extend([str(s).strip().upper() for s in symbols_filter])

        where_sql = " AND ".join(clauses)
        rows = conn.execute(
            f"""
            SELECT
                UPPER(t.symbol) AS symbol,
                COUNT(*) AS lesson_count
            FROM reflections_v2 r
            JOIN trades t ON t.id = r.trade_id
            WHERE {where_sql}
            GROUP BY UPPER(t.symbol)
            HAVING COUNT(*) >= ?
            ORDER BY lesson_count DESC, symbol ASC
            """,
            (*params, int(min_lessons)),
        ).fetchall()

        out = [
            SymbolCandidate(
                symbol=str(row["symbol"] or "").strip().upper(),
                lesson_count=int(row["lesson_count"] or 0),
            )
            for row in rows
            if str(row["symbol"] or "").strip()
        ]
        if max_symbols > 0:
            out = out[: int(max_symbols)]
        return out
    finally:
        conn.close()


async def _regenerate_symbol(
    *,
    db: AITraderDB,
    db_path: str,
    symbol: str,
    lesson_count: int,
    agent_id: Optional[str],
    model: Optional[str],
    thinking: str,
    timeout_sec: float,
    max_retries: int,
    retry_sleep_sec: float,
    openclaw_cmd: str,
    batch_size: int,
    sleep_sec: float,
    apply_changes: bool,
) -> SymbolResult:
    existing = db.get_symbol_conclusion_row(symbol) or {}
    old_adj = _extract_adjustments(existing.get("conclusion_json"))
    closed_trades = int(_fetch_symbol_closed_trades(db_path, symbol))

    previous: Dict[str, Any] = dict(existing) if isinstance(existing, dict) else {}
    last_seen = int(previous.get("last_reflection_id_seen") or 0)
    batches = 0
    final_text = str(existing.get("conclusion_text") or "")
    final_json = str(existing.get("conclusion_json") or "")
    final_conf = _safe_float(existing.get("confidence"), 0.0)

    while True:
        new_lessons = _fetch_new_symbol_lessons(
            db_path,
            symbol=symbol,
            last_seen_id=last_seen,
            limit=int(batch_size),
        )
        if not new_lessons:
            break

        prompt = build_symbol_conclusion_prompt(
            symbol=symbol,
            previous=previous,
            new_lessons=new_lessons,
        )
        _meta, raw_conclusion = await _call_agent_json_retry(
            agent_id=agent_id,
            model=model,
            thinking=thinking,
            session_id=f"regen_concl_{symbol}_{int(time.time() * 1000)}_{batches}",
            message=prompt,
            timeout_sec=timeout_sec,
            max_retries=max_retries,
            retry_sleep_sec=retry_sleep_sec,
            openclaw_cmd=openclaw_cmd,
        )

        final_text, final_conf, final_json = _normalize_conclusion(raw_conclusion)

        last_seen = int(new_lessons[-1].get("reflection_id") or last_seen)
        previous = {
            "conclusion_text": final_text,
            "conclusion_json": final_json,
            "confidence": float(final_conf),
            "last_reflection_id_seen": int(last_seen),
        }
        batches += 1
        if sleep_sec > 0:
            await asyncio.sleep(sleep_sec)

    if batches > 0 and apply_changes:
        db.upsert_symbol_conclusion(
            symbol=symbol,
            conclusion_text=final_text,
            conclusion_json=final_json,
            confidence=float(final_conf),
            last_reflection_id_seen=int(last_seen),
        )

    new_adj = _extract_adjustments(final_json)
    changed = (
        str(final_text).strip() != str(existing.get("conclusion_text") or "").strip()
        or str(final_json).strip() != str(existing.get("conclusion_json") or "").strip()
        or abs(float(final_conf) - _safe_float(existing.get("confidence"), 0.0)) > 1e-9
    )

    return SymbolResult(
        symbol=symbol,
        lesson_count=int(lesson_count),
        closed_trades=int(closed_trades),
        batches=int(batches),
        changed=bool(changed),
        old_long_adj=float(old_adj["long"]),
        old_short_adj=float(old_adj["short"]),
        new_long_adj=float(new_adj["long"]),
        new_short_adj=float(new_adj["short"]),
        error=None,
    )


def _print_results(results: List[SymbolResult], *, apply_changes: bool) -> None:
    total = len(results)
    failed = [r for r in results if r.error]
    processed = [r for r in results if not r.error]
    changed = [r for r in processed if r.changed]
    nonzero = [r for r in processed if abs(r.new_long_adj) > 1e-9 or abs(r.new_short_adj) > 1e-9]

    print("Regenerate symbol conclusions v2")
    print(f"- mode: {'apply' if apply_changes else 'dry-run'}")
    print(f"- symbols_total: {total}")
    print(f"- symbols_processed: {len(processed)}")
    print(f"- symbols_changed: {len(changed)}")
    print(f"- symbols_nonzero_adjustment: {len(nonzero)}")
    print(f"- symbols_failed: {len(failed)}")
    print("")

    preview = processed[:20]
    for r in preview:
        print(
            f"{r.symbol}: lessons={r.lesson_count} closed={r.closed_trades} batches={r.batches} "
            f"changed={int(r.changed)} "
            f"adj {r.old_long_adj:+.3f}/{r.old_short_adj:+.3f} -> {r.new_long_adj:+.3f}/{r.new_short_adj:+.3f}"
        )
    if total > len(preview):
        print(f"... ({total - len(preview)} more symbols)")

    if failed:
        print("")
        print("Failures:")
        for r in failed[:20]:
            print(f"- {r.symbol}: {r.error}")


async def main_async() -> int:
    ap = argparse.ArgumentParser(
        description="Regenerate symbol_conclusions_v1/v2 rows from reflections using EVClaw reflector flow"
    )
    ap.add_argument("--db", default=os.getenv("EVCLAW_DB_PATH", DEFAULT_DB))
    ap.add_argument("--agent-id", default=os.getenv("EVCLAW_LEARNING_REFLECTOR_AGENT_ID", DEFAULT_AGENT_ID))
    ap.add_argument("--model", default=DEFAULT_REGEN_MODEL)
    ap.add_argument("--thinking", default=os.getenv("EVCLAW_LEARNING_REFLECTOR_THINKING", DEFAULT_THINKING))
    ap.add_argument("--timeout-sec", type=float, default=float(os.getenv("EVCLAW_LEARNING_REFLECTOR_TIMEOUT_SEC", "120")))
    ap.add_argument("--max-retries", type=int, default=int(os.getenv("EVCLAW_CONCLUSION_REGEN_MAX_RETRIES", "2")))
    ap.add_argument("--retry-sleep-sec", type=float, default=float(os.getenv("EVCLAW_CONCLUSION_REGEN_RETRY_SLEEP_SEC", "0.25")))
    ap.add_argument("--openclaw-cmd", default=(os.getenv("OPENCLAW_CMD") or "openclaw"))
    ap.add_argument("--batch-size", type=int, default=50)
    ap.add_argument("--min-lessons", type=int, default=3)
    ap.add_argument("--max-symbols", type=int, default=0)
    ap.add_argument("--sleep-sec", type=float, default=0.0)
    ap.add_argument("--symbol", action="append", default=[], help="optional symbol filter; repeatable")
    mode = ap.add_mutually_exclusive_group(required=False)
    mode.add_argument("--dry-run", action="store_true")
    mode.add_argument("--apply", action="store_true")
    args = ap.parse_args()

    db_path = str(args.db or "").strip()
    if not db_path:
        print("ERROR: missing --db path")
        return 2

    apply_changes = bool(args.apply)
    if not args.apply and not args.dry_run:
        apply_changes = False

    symbols_filter = [str(s or "").strip().upper() for s in args.symbol if str(s or "").strip()]
    candidates = _load_symbol_candidates(
        db_path,
        symbols_filter=symbols_filter,
        min_lessons=max(1, int(args.min_lessons)),
        max_symbols=max(0, int(args.max_symbols)),
    )
    if not candidates:
        print("No eligible symbols found (check --symbol / --min-lessons).")
        return 0

    db = AITraderDB(db_path)
    timeout_sec = max(1.0, float(args.timeout_sec))
    max_retries = max(0, int(args.max_retries))
    retry_sleep_sec = max(0.0, float(args.retry_sleep_sec))
    batch_size = max(1, int(args.batch_size))
    sleep_sec = max(0.0, float(args.sleep_sec))
    model_raw = str(args.model or "").strip()
    model = model_raw if model_raw else None
    openclaw_cmd = str(args.openclaw_cmd or "").strip() or "openclaw"

    results: List[SymbolResult] = []
    for c in candidates:
        try:
            res = await _regenerate_symbol(
                db=db,
                db_path=db_path,
                symbol=c.symbol,
                lesson_count=c.lesson_count,
                agent_id=(str(args.agent_id).strip() or None),
                model=model,
                thinking=str(args.thinking),
                timeout_sec=timeout_sec,
                max_retries=max_retries,
                retry_sleep_sec=retry_sleep_sec,
                openclaw_cmd=openclaw_cmd,
                batch_size=batch_size,
                sleep_sec=sleep_sec,
                apply_changes=apply_changes,
            )
            results.append(res)
        except Exception as exc:
            results.append(
                SymbolResult(
                    symbol=c.symbol,
                    lesson_count=c.lesson_count,
                    closed_trades=0,
                    batches=0,
                    changed=False,
                    old_long_adj=0.0,
                    old_short_adj=0.0,
                    new_long_adj=0.0,
                    new_short_adj=0.0,
                    error=str(exc),
                )
            )

    _print_results(results, apply_changes=apply_changes)
    return 0


def main() -> int:
    return asyncio.run(main_async())


if __name__ == "__main__":
    raise SystemExit(main())
