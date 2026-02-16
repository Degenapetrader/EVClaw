#!/usr/bin/env python3
"""Dedicated HIP3 exit decider runner.

Wraps llm_exit_decider with HIP3-scoped defaults:
- symbol prefixes: XYZ:
- venue scope: hyperliquid (canonical)
- separate agent/model env namespace
"""

from __future__ import annotations

import argparse
import asyncio
import os
from pathlib import Path
from env_utils import EVCLAW_MEMORY_DIR
from env_utils import env_str

from llm_exit_decider import DEFAULT_DB, run_loop


def _normalize_agent_id(value: str | None) -> str | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    if raw.lower() in {"default", "openclaw-default"}:
        return "default"
    return raw


def _hip3_runtime_overrides() -> dict:
    hip3_agent_id = _normalize_agent_id(
        (env_str("EVCLAW_HIP3_EXIT_DECIDER_AGENT_ID", "evclaw-hip3-exit-decider") or "").strip()
        or (env_str("EVCLAW_EXIT_DECIDER_AGENT_ID", "evclaw-exit-decider") or "").strip()
        or "evclaw-hip3-exit-decider"
    )
    hip3_model_raw = (env_str("EVCLAW_HIP3_EXIT_DECIDER_MODEL", "") or "").strip()
    if hip3_model_raw.lower() in {"default", "openclaw-default"}:
        hip3_model_raw = ""
    hip3_model = hip3_model_raw or None
    hip3_thinking = (
        env_str("EVCLAW_HIP3_EXIT_DECIDER_THINKING", "")
        or "medium"
    ).strip() or "medium"

    return {
        "symbol_prefixes": ("XYZ:",),
        "allowed_venues": ("hyperliquid",),
        "agent_id": hip3_agent_id,
        "model": hip3_model,
        "thinking": hip3_thinking,
        "state_path": str(Path(EVCLAW_MEMORY_DIR) / "hip3_exit_decider_state.json"),
        "lock_path": str(Path(EVCLAW_MEMORY_DIR) / "hip3_exit_decider.lock"),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="HIP3-only exit decider via OpenClaw agent")
    parser.add_argument("--db-path", default=DEFAULT_DB)
    args = parser.parse_args()

    try:
        return asyncio.run(
            run_loop(
                db_path=str(args.db_path),
                runtime_overrides=_hip3_runtime_overrides(),
            )
        )
    except KeyboardInterrupt:
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
