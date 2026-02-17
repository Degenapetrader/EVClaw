#!/usr/bin/env python3
"""
Cycle Trigger for HL-Trader.

Monitors SSE for watcher cycle completion (snapshot events), saves the cycle,
builds a filtered context, and records cycle metadata for DB polling.

Architecture:
    1. Connects to symbol_watcher SSE server (tracker.evplus.ai:8443)
    2. Detects when msg_type == 'snapshot' arrives (end of watcher cycle)
    3. Saves full cycle data to <runtime_dir>/evclaw_cycle_{seq}.json
    4. Builds filtered context to <runtime_dir>/evclaw_context_{seq}.txt
       and JSON context to <runtime_dir>/evclaw_context_{seq}.json
    5. Records cycle metadata in DB (live_agent polls unprocessed cycles)

Usage:
    # Run in tmux session:
    tmux new-session -d -s cycle_trigger 'python3 cycle_trigger.py'
    
    # Or run directly:
    python3 cycle_trigger.py [--dry-run] [--verbose]

Options:
    --dry-run    Don't actually notify, just log
    --verbose    Enable debug logging

Log: <runtime_dir>/cycle_trigger.log
"""

import argparse
import asyncio
from collections import Counter, deque
from dataclasses import dataclass
import statistics
import json
import logging
import os
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import yaml
from dotenv import load_dotenv

# Load skill .env for HIP3/Main+runtime thresholds (cycle_trigger does not otherwise import env_utils).
load_dotenv(Path(__file__).resolve().parent / ".env")

# Import the SSE consumer from same directory
sys.path.insert(0, str(Path(__file__).parent))
from sse_consumer import TrackerSSEClient, SSEMessage
from hip3_main import compute_hip3_main
from ai_trader_db import AITraderDB
from env_utils import EVCLAW_DB_PATH, EVCLAW_RUNTIME_DIR, env_int, env_str
from live_agent_state import load_state, set_last_notified
from logging_utils import setup_logging

# Configuration
SKILL_DIR = Path(__file__).parent
CONFIG_FILE = SKILL_DIR / "skill.yaml"

RUNTIME_DIR = Path(EVCLAW_RUNTIME_DIR)
LOG_FILE = str(RUNTIME_DIR / "cycle_trigger.log")
DATA_DIR = str(RUNTIME_DIR)
RUNTIME_DIR.mkdir(parents=True, exist_ok=True)
CONTEXT_BUILDER = str(SKILL_DIR / "context_builder_v2.py")

def _load_skill_hip3_cfg() -> Dict[str, Any]:
    if not CONFIG_FILE.exists():
        return {}
    try:
        with CONFIG_FILE.open("r", encoding="utf-8") as f:
            raw = yaml.safe_load(f) or {}
        if not isinstance(raw, dict):
            return {}
        cfg = raw.get("config") or {}
        if not isinstance(cfg, dict):
            return {}
        hip3 = cfg.get("hip3") or {}
        return hip3 if isinstance(hip3, dict) else {}
    except Exception:
        return {}


def _skill_float(cfg: Dict[str, Any], key: str, default: float) -> float:
    try:
        value = cfg.get(key, default)
        return float(default if value is None else value)
    except Exception:
        return float(default)


def _skill_bool(cfg: Dict[str, Any], key: str, default: bool) -> bool:
    value = cfg.get(key, default)
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        raw = value.strip().lower()
        if raw in {"1", "true", "yes", "y", "on"}:
            return True
        if raw in {"0", "false", "no", "n", "off"}:
            return False
    return bool(default)


_HIP3_CFG = _load_skill_hip3_cfg()


TMP_MIRROR_DIR = Path("/tmp")

# Synthetic snapshot fallback (CycleTrigger):
# If snapshots stop arriving but deltas keep flowing, we emit a synthetic snapshot
# cycle based on the latest merged symbols state.
SYNTHETIC_SNAPSHOT_ENABLED = True
# Default stale threshold slightly above the nominal 10min cadence. In practice
# snapshot delivery can drift (proxy reconnects, upstream hiccups). A threshold
# of exactly 600s causes frequent "synthetic snapshot" emits right before the
# real snapshot arrives.
SYNTHETIC_SNAPSHOT_STALE_SEC = 750.0
SYNTHETIC_SNAPSHOT_ACTIVITY_SEC = 180.0
SYNTHETIC_SNAPSHOT_POLL_SEC = 5.0
SYNTHETIC_SNAPSHOT_MIN_INTERVAL_SEC = 300.0

# /tmp mirroring: keep "latest" symlinks for ops simplicity.
TMP_MIRROR_KEEP_LATEST = True


def _tmp_unlink_best_effort(path: Path) -> None:
    try:
        if path.exists() or path.is_symlink():
            path.unlink()
    except Exception:
        pass


def prune_broken_tmp_symlinks(prefixes: Tuple[str, ...], log: Optional[logging.Logger] = None) -> None:
    """Remove broken /tmp symlinks created by mirroring runtime artifacts.

    Old runtime files get cleaned up from the runtime directory; their mirrored /tmp
    symlinks then become broken. This makes naive glob+mtime tooling crash or lie.
    """
    try:
        TMP_MIRROR_DIR.mkdir(parents=True, exist_ok=True)
        removed = 0
        for prefix in prefixes:
            for p in TMP_MIRROR_DIR.glob(f"{prefix}*"):
                try:
                    if not p.is_symlink():
                        continue
                    target = os.readlink(p)
                    if not os.path.exists(target):
                        p.unlink()
                        removed += 1
                except Exception:
                    continue
        if removed and log:
            log.debug(f"Pruned {removed} broken /tmp symlinks")
    except Exception as e:
        if log:
            try:
                log.debug(f"/tmp prune failed: {e}")
            except Exception:
                pass


def prune_tmp_regular_mirror_files(prefixes: Tuple[str, ...], log: Optional[logging.Logger] = None) -> None:
    """Remove legacy regular files in /tmp that should be symlink mirrors."""
    try:
        TMP_MIRROR_DIR.mkdir(parents=True, exist_ok=True)
        removed = 0
        for prefix in prefixes:
            for p in TMP_MIRROR_DIR.glob(f"{prefix}*"):
                try:
                    if p.is_symlink() or (not p.exists()) or (not p.is_file()):
                        continue
                    p.unlink()
                    removed += 1
                except Exception:
                    continue
        if removed and log:
            log.info(f"Pruned {removed} legacy /tmp regular mirror files")
    except Exception as e:
        if log:
            try:
                log.debug(f"/tmp regular prune failed: {e}")
            except Exception:
                pass


def mirror_runtime_file_to_tmp(src_path: str, log: logging.Logger) -> None:
    """Best-effort: mirror runtime artifacts into /tmp via symlink.

    Reason: ops prompts and tooling often look in /tmp for the latest cycle/context.
    We keep canonical artifacts in the runtime directory, but make them discoverable.

    HARDENING:
    - Keep "*_latest" symlinks for cycle/context artifacts.
    - Allow pruning broken symlinks elsewhere.
    """
    try:
        TMP_MIRROR_DIR.mkdir(parents=True, exist_ok=True)
        src = Path(src_path)
        dst = TMP_MIRROR_DIR / src.name
        _tmp_unlink_best_effort(dst)
        os.symlink(str(src), str(dst))

        if TMP_MIRROR_KEEP_LATEST:
            name = src.name
            latest = None
            if name.startswith("evclaw_cycle_") and name.endswith(".json"):
                latest = TMP_MIRROR_DIR / "evclaw_cycle_latest.json"
            elif name.startswith("evclaw_context_") and name.endswith(".txt"):
                latest = TMP_MIRROR_DIR / "evclaw_context_latest.txt"
            elif name.startswith("evclaw_context_") and name.endswith(".json"):
                latest = TMP_MIRROR_DIR / "evclaw_context_latest.json"
            elif name.startswith("evclaw_candidates_") and name.endswith(".json"):
                latest = TMP_MIRROR_DIR / "evclaw_candidates_latest.json"

            if latest is not None:
                _tmp_unlink_best_effort(latest)
                os.symlink(str(src), str(latest))
    except Exception as e:
        # Never fail the cycle on mirror issues
        try:
            log.debug(f"/tmp mirror failed for {src_path}: {e}")
        except Exception:
            pass


def _context_artifacts_ready(context_file: str, context_json_file: str) -> bool:
    """Return True when both context artifacts exist and are non-empty."""
    for raw_path in (context_file, context_json_file):
        path = Path(str(raw_path or ""))
        if not path.exists():
            return False
        try:
            if path.stat().st_size <= 0:
                return False
        except Exception:
            return False
    return True


# HIP3 interrupt configuration (threshold gating handled by hip3_main)
# Context builder timeout (seconds)
CONTEXT_BUILDER_TIMEOUT_SEC = 60.0
CONTEXT_BUILDER_KILL_TIMEOUT_SEC = 5.0
HIP3_INTERRUPTS_ENABLED = _skill_bool(_HIP3_CFG, "interrupts_enabled", True)
HIP3_TRIGGER_GLOBAL_COOLDOWN_SEC = _skill_float(_HIP3_CFG, "trigger_global_cooldown_sec", 4.0)
HIP3_TRIGGER_SYMBOL_COOLDOWN_SEC = _skill_float(_HIP3_CFG, "trigger_symbol_cooldown_sec", 45.0)
HIP3_TRIGGER_TTL_SEC = _skill_float(_HIP3_CFG, "trigger_ttl_sec", 600.0)
HIP3_CACHE_TTL_SEC = 21600.0
HIP3_CACHE_MAX = env_int(
    "EVCLAW_HIP3_CACHE_MAX",
    2000,
)
HIP3_FLOW_SL_ATR_MULT = _skill_float(_HIP3_CFG, "flow_sl_atr_mult", 1.4)
HIP3_FLOW_TP_ATR_MULT = _skill_float(_HIP3_CFG, "flow_tp_atr_mult", 2.0)
HIP3_OFM_SL_ATR_MULT = _skill_float(_HIP3_CFG, "ofm_sl_atr_mult", 1.8)
HIP3_OFM_TP_ATR_MULT = _skill_float(_HIP3_CFG, "ofm_tp_atr_mult", 2.8)

# HIP3 flow vs OFM mismatch stats (diagnostics)
HIP3_FLOW_OFM_STATS_ENABLED = True
HIP3_FLOW_OFM_STATS_WINDOW_SEC = 3600.0
HIP3_FLOW_OFM_STATS_WRITE_MIN_INTERVAL_SEC = 30.0
HIP3_FLOW_OFM_STATS_FILE = str(RUNTIME_DIR / "hip3_flow_ofm_stats.json")

# HIP3 dynamic window state (snapshot cadence estimator)
HIP3_WINDOW_STATE_ENABLED = True
HIP3_WINDOW_STATE_FILE = str(RUNTIME_DIR / "hip3_window_state.json")
HIP3_WINDOW_MAX_INTERVAL_SAMPLES = 30
HIP3_WINDOW_OUTLIER_K = 6.0
HIP3_WINDOW_EWMA_ALPHA = 0.20
HIP3_WINDOW_INIT_SEC = 420.0


@dataclass(frozen=True)
class CycleConfig:
    """Runtime knobs for cycle trigger behavior."""

    synthetic_snapshot_enabled: bool = SYNTHETIC_SNAPSHOT_ENABLED
    synthetic_snapshot_stale_sec: float = SYNTHETIC_SNAPSHOT_STALE_SEC
    synthetic_snapshot_activity_sec: float = SYNTHETIC_SNAPSHOT_ACTIVITY_SEC
    synthetic_snapshot_poll_sec: float = SYNTHETIC_SNAPSHOT_POLL_SEC
    synthetic_snapshot_min_interval_sec: float = SYNTHETIC_SNAPSHOT_MIN_INTERVAL_SEC
    context_builder_timeout_sec: float = CONTEXT_BUILDER_TIMEOUT_SEC
    context_builder_kill_timeout_sec: float = CONTEXT_BUILDER_KILL_TIMEOUT_SEC
    hip3_interrupts_enabled: bool = HIP3_INTERRUPTS_ENABLED
    hip3_trigger_global_cooldown_sec: float = HIP3_TRIGGER_GLOBAL_COOLDOWN_SEC
    hip3_trigger_symbol_cooldown_sec: float = HIP3_TRIGGER_SYMBOL_COOLDOWN_SEC
    hip3_trigger_ttl_sec: float = HIP3_TRIGGER_TTL_SEC
    hip3_cache_ttl_sec: float = HIP3_CACHE_TTL_SEC
    hip3_cache_max: int = HIP3_CACHE_MAX
    hip3_flow_ofm_stats_enabled: bool = HIP3_FLOW_OFM_STATS_ENABLED
    hip3_flow_ofm_stats_window_sec: float = HIP3_FLOW_OFM_STATS_WINDOW_SEC
    hip3_flow_ofm_stats_write_min_interval_sec: float = HIP3_FLOW_OFM_STATS_WRITE_MIN_INTERVAL_SEC
    hip3_window_state_enabled: bool = HIP3_WINDOW_STATE_ENABLED
    hip3_window_max_interval_samples: int = HIP3_WINDOW_MAX_INTERVAL_SAMPLES
    hip3_window_outlier_k: float = HIP3_WINDOW_OUTLIER_K
    hip3_window_ewma_alpha: float = HIP3_WINDOW_EWMA_ALPHA
    hip3_window_init_sec: float = HIP3_WINDOW_INIT_SEC

    @classmethod
    def load(cls) -> "CycleConfig":
        return cls(
            synthetic_snapshot_enabled=SYNTHETIC_SNAPSHOT_ENABLED,
            synthetic_snapshot_stale_sec=SYNTHETIC_SNAPSHOT_STALE_SEC,
            synthetic_snapshot_activity_sec=SYNTHETIC_SNAPSHOT_ACTIVITY_SEC,
            synthetic_snapshot_poll_sec=SYNTHETIC_SNAPSHOT_POLL_SEC,
            synthetic_snapshot_min_interval_sec=SYNTHETIC_SNAPSHOT_MIN_INTERVAL_SEC,
            context_builder_timeout_sec=CONTEXT_BUILDER_TIMEOUT_SEC,
            context_builder_kill_timeout_sec=CONTEXT_BUILDER_KILL_TIMEOUT_SEC,
            hip3_interrupts_enabled=HIP3_INTERRUPTS_ENABLED,
            hip3_trigger_global_cooldown_sec=HIP3_TRIGGER_GLOBAL_COOLDOWN_SEC,
            hip3_trigger_symbol_cooldown_sec=HIP3_TRIGGER_SYMBOL_COOLDOWN_SEC,
            hip3_trigger_ttl_sec=HIP3_TRIGGER_TTL_SEC,
            hip3_cache_ttl_sec=HIP3_CACHE_TTL_SEC,
            hip3_cache_max=env_int("EVCLAW_HIP3_CACHE_MAX", 2000),
            hip3_flow_ofm_stats_enabled=HIP3_FLOW_OFM_STATS_ENABLED,
            hip3_flow_ofm_stats_window_sec=HIP3_FLOW_OFM_STATS_WINDOW_SEC,
            hip3_flow_ofm_stats_write_min_interval_sec=HIP3_FLOW_OFM_STATS_WRITE_MIN_INTERVAL_SEC,
            hip3_window_state_enabled=HIP3_WINDOW_STATE_ENABLED,
            hip3_window_max_interval_samples=HIP3_WINDOW_MAX_INTERVAL_SAMPLES,
            hip3_window_outlier_k=HIP3_WINDOW_OUTLIER_K,
            hip3_window_ewma_alpha=HIP3_WINDOW_EWMA_ALPHA,
            hip3_window_init_sec=HIP3_WINDOW_INIT_SEC,
        )


def load_skill_config() -> Dict[str, Any]:
    """Load skill.yaml configuration (minimal reader; avoids importing full cli stack)."""
    if CONFIG_FILE.exists():
        with open(CONFIG_FILE, "r") as f:
            return yaml.safe_load(f) or {}
    return {}


def resolve_db_path(config: Dict[str, Any]) -> str:
    """Resolve DB path with env override, then skill.yaml, then default."""
    env_path = str(env_str("EVCLAW_DB_PATH", EVCLAW_DB_PATH) or EVCLAW_DB_PATH).strip()
    if env_path:
        return env_path
    try:
        cfg_path = str((config.get("config", {}) or {}).get("db_path", "") or "").strip()
    except Exception:
        cfg_path = ""
    if cfg_path:
        return cfg_path
    return EVCLAW_DB_PATH


class CycleTrigger:
    """
    Monitors SSE for watcher cycles and records cycle metadata for DB polling.

    Flow:
        1. Connect to SSE server
        2. Wait for snapshot event (indicates cycle complete)
        3. Save cycle data to JSON file
        4. Build filtered context
        5. Record cycle in DB (live_agent polls unprocessed cycles)
        6. Track sequence to avoid duplicate triggers
    """
    
    def __init__(self, dry_run: bool = False, verbose: bool = False):
        self.dry_run = dry_run
        self.verbose = verbose
        self.log = setup_logging("cycle_trigger", log_file=LOG_FILE, verbose=verbose)
        self._skill_cfg: Dict[str, Any] = load_skill_config()
        self._db_path: str = resolve_db_path(self._skill_cfg)
        self._db: Optional[AITraderDB] = None
        self._cfg: CycleConfig = CycleConfig.load()

        # State tracking
        self._last_triggered_seq: int = -1
        self._last_reserved_seq: int = -1
        self._trigger_count: int = 0
        self._start_time: float = time.time()
        self._last_snapshot_time: Optional[float] = None
        self._last_data_time: Optional[float] = None
        self._last_tracker_sse_seq: Optional[int] = None
        self._seq_lock = asyncio.Lock()

        # Synthetic snapshot fallback state
        self._synthetic_task: Optional[asyncio.Task] = None
        self._synthetic_last_emit_ts: float = 0.0
        self._synthetic_inflight: bool = False

        self._last_hip3_global_ts: float = 0.0
        self._hip3_symbol_last_ts: Dict[str, float] = {}
        self._hip3_symbol_last_dir: Dict[str, Optional[str]] = {}

        # HIP3 flow vs OFM mismatch stats (rolling window)
        self._hip3_flow_ofm_events = deque()
        self._hip3_flow_ofm_last_generated_at: Dict[str, str] = {}
        self._hip3_flow_ofm_last_write_ts: float = 0.0

        # HIP3 dynamic window estimator (based on snapshot cadence)
        self._hip3_snapshot_intervals = deque(maxlen=int(max(5, self._cfg.hip3_window_max_interval_samples)))
        self._hip3_interval_est_sec: Optional[float] = None
        self._hip3_window_last_write_ts: float = 0.0
        self._hip3_window_last_sse_seq: Optional[int] = None
        self._hip3_window_last_cycle_seq: Optional[int] = None

        # SSE client
        self._client: Optional[TrackerSSEClient] = None
        self._running = False
        
        # Current cycle data (latest merged state from tracker updates).
        self._current_symbols: Dict[str, Any] = {}
        
    async def start(self) -> None:
        """Start monitoring SSE for cycle completions."""
        self.log.info("=" * 60)
        self.log.info("Cycle Trigger Starting")
        self.log.info(f"  Dry run: {self.dry_run}")
        self.log.info("  Notify: disabled (live_agent polls DB)")
        self.log.info(f"  Log file: {LOG_FILE}")
        self.log.info("=" * 60)
        
        self._running = True

        # Refresh config at startup so env/skill edits are picked up on restart.
        self._skill_cfg = load_skill_config()
        self._db_path = resolve_db_path(self._skill_cfg)
        try:
            self._db = AITraderDB(self._db_path)
        except Exception as exc:
            self._db = None
            self.log.warning(f"DB init failed at start (will retry lazily): {exc}")
        self._cfg = CycleConfig.load()
        cfg = self._skill_cfg.get("config", {})
        sse_host = str(
            env_str("EVCLAW_SSE_HOST", str(cfg.get("sse_host", "tracker.evplus.ai")))
            or "tracker.evplus.ai"
        ).strip()
        sse_port = env_int(
            "EVCLAW_SSE_PORT",
            int(env_str("EVCLAW_SSE_PORT", str(int(cfg.get("sse_port", 8443) or 8443))) or 8443),
        )
        sse_endpoint = str(
            env_str(
                "EVCLAW_SSE_ENDPOINT",
                str(cfg.get("sse_endpoint", "/sse/tracker")),
            ) or "/sse/tracker"
        ).strip()
        wallet_address = str(
            env_str("HYPERLIQUID_ADDRESS", env_str("EVCLAW_WALLET_ADDRESS", ""))
        ).strip()

        # Create custom SSE client that gives us access to raw messages
        client_kwargs: Dict[str, Any] = dict(
            on_snapshot=self._handle_snapshot,
            on_hip3_snapshot=self._handle_hip3_snapshot,
            on_data=self._handle_data_update,
            verbose=self.verbose,
            host=sse_host,
            port=sse_port,
            endpoint=sse_endpoint,
        )
        # Let TrackerSSEClient default wallet apply when not explicitly configured.
        if wallet_address:
            client_kwargs["wallet_address"] = wallet_address
        self._client = CycleAwareSSEClient(**client_kwargs)
        
        # Run watchdog concurrently with SSE connect loop.
        if SYNTHETIC_SNAPSHOT_ENABLED:
            self._synthetic_task = asyncio.create_task(self._synthetic_snapshot_watchdog())

        try:
            await self._client.connect()
        except asyncio.CancelledError:
            self.log.info("Cycle trigger cancelled")
        except Exception as e:
            self.log.error(f"Fatal error: {e}")
            raise
        finally:
            self._running = False
            if self._synthetic_task:
                self._synthetic_task.cancel()
                try:
                    await self._synthetic_task
                except Exception:
                    pass
                self._synthetic_task = None
    
    async def stop(self) -> None:
        """Stop monitoring."""
        self._running = False
        if self._synthetic_task:
            self._synthetic_task.cancel()
            try:
                await self._synthetic_task
            except Exception:
                pass
            self._synthetic_task = None
        if self._client:
            await self._client.disconnect()
        self.log.info("Cycle trigger stopped")

    def _handle_data_update(self, symbols: Dict[str, Any]) -> None:
        """Handle any data update (snapshot or delta)."""
        self._current_symbols = symbols.copy()
        self._last_data_time = time.time()
        # Best-effort: capture tracker SSE sequence for debug / synthetic snapshots
        try:
            if self._client is not None:
                self._last_tracker_sse_seq = int(getattr(self._client, "last_tracker_seq", None) or getattr(self._client, "_last_sequence", -1))
        except Exception:
            pass

    async def _reserve_cycle_seq(self) -> int:
        """Reserve a unique, monotonic cycle sequence (seconds since epoch)."""
        async with self._seq_lock:
            floor = max(self._last_triggered_seq, self._last_reserved_seq)
            cycle_seq = int(time.time())
            if cycle_seq <= floor:
                cycle_seq = floor + 1
            self._last_reserved_seq = cycle_seq
            return cycle_seq

    async def _synthetic_snapshot_watchdog(self) -> None:
        """Emit synthetic snapshots when full snapshots stop arriving.

        Problem: cycle_trigger only fires on tracker-data snapshots. If symbol_watcher
        gets stuck in a long cycle (heavy wallet fetch) we can go "dark" for long
        periods even though deltas / partial updates are still flowing.

        Policy: If we have recent data updates (deltas flowing) but haven't observed
        a snapshot for SYNTHETIC_SNAPSHOT_STALE_SEC, trigger a synthetic snapshot
        cycle from the latest merged symbols state.
        """
        while self._running:
            try:
                await asyncio.sleep(max(1.0, float(self._cfg.synthetic_snapshot_poll_sec)))

                if not self._cfg.synthetic_snapshot_enabled:
                    continue

                now = time.time()
                last_data = self._last_data_time
                last_snap = self._last_snapshot_time

                if not last_data or not self._current_symbols:
                    continue

                # Only act if deltas are still flowing.
                if (now - last_data) > float(self._cfg.synthetic_snapshot_activity_sec):
                    continue

                # If we never saw a snapshot, treat it as stale immediately after the
                # stale threshold since start.
                if last_snap and (now - last_snap) <= float(self._cfg.synthetic_snapshot_stale_sec):
                    continue
                if not last_snap and (now - self._start_time) <= float(self._cfg.synthetic_snapshot_stale_sec):
                    continue

                # Avoid spamming synthetic cycles.
                if (now - float(self._synthetic_last_emit_ts)) < float(self._cfg.synthetic_snapshot_min_interval_sec):
                    continue

                if self._synthetic_inflight:
                    continue

                self._synthetic_inflight = True
                try:
                    sse_seq = int(self._last_tracker_sse_seq or -1)
                    self.log.warning(
                        "SYNTHETIC_SNAPSHOT emit reason=no_recent_snapshot stale_sec=%.1f activity_age_sec=%.1f sse_seq=%s",
                        (now - last_snap) if last_snap else float('inf'),
                        (now - last_data),
                        sse_seq,
                    )
                    await self._handle_snapshot(sse_seq, self._current_symbols)
                    self._synthetic_last_emit_ts = time.time()
                finally:
                    self._synthetic_inflight = False

            except asyncio.CancelledError:
                break
            except Exception as e:
                try:
                    self.log.warning(f"Synthetic snapshot watchdog error: {e}")
                except Exception:
                    pass

    async def _emit_cycle(
        self,
        *,
        cycle_seq: int,
        sse_seq: int,
        symbols: Dict[str, Any],
        timestamp_utc: str,
        source: str,
        candidates_file: Optional[str] = None,
        hip3_symbol: Optional[str] = None,
        hip3_now_ts: Optional[float] = None,
    ) -> bool:
        """Emit a cycle through shared save/build/persist/notify flow."""
        data_file = await self._save_cycle_data(cycle_seq, symbols, sse_seq=sse_seq)
        context_file, context_json_file = await self._build_context(cycle_seq, data_file)
        if not _context_artifacts_ready(context_file, context_json_file):
            if source == "hip3":
                self.log.error(
                    "HIP3 context artifacts missing/empty after build; suppress trigger "
                    "seq=%s symbol=%s context=%s context_json=%s",
                    cycle_seq,
                    hip3_symbol or "",
                    context_file,
                    context_json_file,
                )
                if hip3_now_ts is not None and hip3_symbol:
                    self._last_hip3_global_ts = hip3_now_ts
                    self._hip3_symbol_last_ts[hip3_symbol] = hip3_now_ts
            else:
                self.log.error(
                    "Context artifacts missing/empty after build; suppress cycle "
                    "seq=%s cycle_file=%s context=%s context_json=%s",
                    cycle_seq,
                    data_file,
                    context_file,
                    context_json_file,
                )
            return False

        try:
            db = self._get_db()
            db.insert_cycle_run(
                cycle_seq,
                cycle_file=data_file,
                context_file=context_file,
                candidates_file=candidates_file,
                timestamp=time.time(),
            )
        except Exception as e:
            self.log.warning(f"Failed to record {source} cycle run in DB: {e}")

        payload = {
            "seq": cycle_seq,
            "sse_seq": sse_seq,
            "cycle_file": data_file,
            "context_file": context_file,
            "context_json_file": context_json_file,
            "ts_utc": timestamp_utc,
        }
        await self._notify_main_agent(payload)

        self._last_triggered_seq = max(self._last_triggered_seq, cycle_seq)
        self._trigger_count += 1
        if source == "snapshot":
            self._last_snapshot_time = time.time()
        elif source == "hip3" and hip3_now_ts is not None and hip3_symbol:
            self._last_hip3_global_ts = hip3_now_ts
            self._hip3_symbol_last_ts[hip3_symbol] = hip3_now_ts
        return True

    def _get_db(self) -> AITraderDB:
        if self._db is None or str(getattr(self._db, "db_path", "")) != str(self._db_path):
            self._db = AITraderDB(self._db_path)
        return self._db

    async def _handle_snapshot(self, sequence: int, symbols: Dict[str, Any]) -> None:
        """Handle snapshot event - this is the cycle trigger point."""
        sse_seq = int(sequence)
        cycle_seq = await self._reserve_cycle_seq()
        timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
        symbol_count = len(symbols)
        self.log.info(f"🔔 Snapshot received: cycle_seq={cycle_seq} (sse_seq={sse_seq}), {symbol_count} symbols")

        prev_snapshot_ts = self._last_snapshot_time
        ok = await self._emit_cycle(
            cycle_seq=cycle_seq,
            sse_seq=sse_seq,
            symbols=symbols,
            timestamp_utc=timestamp,
            source="snapshot",
        )
        if not ok:
            return

        # After a successful snapshot cycle emit, update HIP3 window estimator.
        try:
            now_ts = float(self._last_snapshot_time or time.time())
            self._update_and_write_hip3_window_state(
                now_ts=now_ts,
                snapshot_prev_ts=prev_snapshot_ts,
                snapshot_sse_seq=sse_seq,
                snapshot_cycle_seq=cycle_seq,
            )
        except Exception:
            pass

        uptime = time.time() - self._start_time
        self.log.info(f"📊 Stats: {self._trigger_count} triggers in {uptime/3600:.1f}h")

    def _parse_generated_at(self, value: Optional[str]) -> Optional[float]:
        if not value:
            return None
        try:
            ts = str(value).strip()
            if ts.endswith("Z"):
                ts = ts[:-1] + "+00:00"
            return datetime.fromisoformat(ts).timestamp()
        except Exception:
            return None

    def _prune_hip3_cache(self, now: float) -> None:
        if self._cfg.hip3_cache_ttl_sec > 0:
            cutoff = now - self._cfg.hip3_cache_ttl_sec
            stale = [k for k, ts in self._hip3_symbol_last_ts.items() if ts < cutoff]
            for symbol in stale:
                self._hip3_symbol_last_ts.pop(symbol, None)
                self._hip3_symbol_last_dir.pop(symbol, None)
        if self._cfg.hip3_cache_max > 0 and len(self._hip3_symbol_last_ts) > self._cfg.hip3_cache_max:
            excess = len(self._hip3_symbol_last_ts) - self._cfg.hip3_cache_max
            for symbol, _ts in sorted(self._hip3_symbol_last_ts.items(), key=lambda kv: kv[1])[:excess]:
                self._hip3_symbol_last_ts.pop(symbol, None)
                self._hip3_symbol_last_dir.pop(symbol, None)

    def _build_hip3_candidates(
        self,
        *,
        hip3_state: Dict[str, Any],
        now: float,
    ) -> list[tuple[int, float, str, str, float, Dict[str, Any]]]:
        candidates: list[tuple[int, float, str, str, float, Dict[str, Any]]] = []
        for symbol, payload in hip3_state.items():
            if not symbol or not str(symbol).lower().startswith("xyz:"):
                continue
            if not isinstance(payload, dict):
                continue
            hip3_payload = payload.get("hip3_predator")
            if hip3_payload is None and payload:
                hip3_payload = payload
            if not isinstance(hip3_payload, dict):
                continue

            gen_ts = hip3_payload.get("generated_at")
            gen_epoch = self._parse_generated_at(gen_ts)
            hip3_main, _meta = compute_hip3_main(hip3_payload)
            if not hip3_main:
                if self._hip3_symbol_last_dir.get(symbol):
                    self._hip3_symbol_last_dir[symbol] = None
                continue
            direction = str(hip3_main.get("direction") or "").upper()
            try:
                z_score = float(hip3_main.get("z_score") or 0.0)
            except Exception:
                z_score = 0.0
            if direction not in ("LONG", "SHORT") or z_score <= 0:
                if self._hip3_symbol_last_dir.get(symbol):
                    self._hip3_symbol_last_dir[symbol] = None
                continue

            if gen_epoch is None:
                if self._hip3_symbol_last_dir.get(symbol):
                    self._hip3_symbol_last_dir[symbol] = None
                self.log.info(
                    "HIP3_TRIGGERED symbol=%s dir=%s z=%.3f action=suppressed reason=missing_generated_at",
                    symbol,
                    direction,
                    z_score,
                )
                continue
            age_sec = now - gen_epoch
            if age_sec > self._cfg.hip3_trigger_ttl_sec:
                if self._hip3_symbol_last_dir.get(symbol):
                    self._hip3_symbol_last_dir[symbol] = None
                self.log.info(
                    "HIP3_TRIGGERED symbol=%s dir=%s z=%.3f age_sec=%.1f action=suppressed reason=stale",
                    symbol,
                    direction,
                    z_score,
                    age_sec,
                )
                continue

            prev_dir = self._hip3_symbol_last_dir.get(symbol)
            flip_detected = prev_dir in ("LONG", "SHORT") and prev_dir != direction
            if prev_dir in ("LONG", "SHORT") and prev_dir == direction:
                continue

            if not flip_detected:
                last_symbol_ts = self._hip3_symbol_last_ts.get(symbol, 0.0)
                if (now - last_symbol_ts) < self._cfg.hip3_trigger_symbol_cooldown_sec:
                    self.log.info(
                        "HIP3_TRIGGERED symbol=%s dir=%s z=%.3f age_sec=%.1f action=suppressed reason=symbol_cooldown",
                        symbol,
                        direction,
                        z_score,
                        age_sec,
                    )
                    continue
            else:
                self.log.info(
                    "HIP3_TRIGGERED symbol=%s dir=%s z=%.3f age_sec=%.1f action=candidate reason=direction_flip",
                    symbol,
                    direction,
                    z_score,
                    age_sec,
                )

            candidates.append(
                (
                    1 if flip_detected else 0,
                    z_score,
                    symbol,
                    direction,
                    age_sec,
                    hip3_main,
                )
            )
        return candidates

    def _classify_hip3_flow_ofm(self, hip3_payload: Dict[str, Any]) -> Dict[str, Any]:
        """Classify HIP3 FLOW vs OFM pass/fail under OR-driver semantics."""
        _sig, meta = compute_hip3_main(hip3_payload, require_ofm=False)
        flow_pass = bool(meta.get("flow_pass"))
        ofm_pass = bool(meta.get("ofm_pass"))
        flow_dir = str(meta.get("flow_direction") or "")
        ofm_dir = str(meta.get("ofm_direction") or "")
        conflict = bool(meta.get("conflict"))
        blocked_reason = str(meta.get("blocked_reason") or "")

        if conflict:
            label = "flow_ofm_conflict"
        elif flow_pass and ofm_pass:
            label = "flow_pass_ofm_pass"
        elif flow_pass and (not ofm_pass):
            label = "flow_pass_ofm_fail"
        elif (not flow_pass) and ofm_pass:
            label = "ofm_pass_flow_fail"
        else:
            label = "both_fail"

        return {
            "label": label,
            "flow_pass": flow_pass,
            "ofm_pass": ofm_pass,
            "conflict": conflict,
            "flow_dir": flow_dir,
            "ofm_dir": ofm_dir,
            "blocked_reason": blocked_reason,
            "flow_z_signed": meta.get("flow_z_signed"),
            "flow_threshold": meta.get("flow_threshold"),
            "threshold_mult": meta.get("threshold_mult"),
        }

    def _record_hip3_flow_ofm_stats(self, *, hip3_state: Dict[str, Any], now: float) -> None:
        """Record rolling HIP3 flow/ofm mismatch stats (XYZ only).

        We dedup by per-symbol `generated_at` so we count *new* HIP3 predator updates,
        not repeated deltas carrying the same payload.
        """
        if not self._cfg.hip3_flow_ofm_stats_enabled:
            return

        window_sec = float(max(60.0, self._cfg.hip3_flow_ofm_stats_window_sec))
        cutoff = now - window_sec
        while self._hip3_flow_ofm_events and self._hip3_flow_ofm_events[0][0] < cutoff:
            self._hip3_flow_ofm_events.popleft()

        added = 0
        for symbol, payload in (hip3_state or {}).items():
            if not symbol or not str(symbol).lower().startswith("xyz:"):
                continue
            if not isinstance(payload, dict):
                continue
            hip3_payload = payload.get("hip3_predator")
            if hip3_payload is None and payload:
                hip3_payload = payload
            if not isinstance(hip3_payload, dict):
                continue

            sym_key = str(symbol).upper()
            gen_ts = str(hip3_payload.get("generated_at") or "")
            if gen_ts:
                prev = self._hip3_flow_ofm_last_generated_at.get(sym_key)
                if prev == gen_ts:
                    continue
                self._hip3_flow_ofm_last_generated_at[sym_key] = gen_ts

            cls = self._classify_hip3_flow_ofm(hip3_payload)
            self._hip3_flow_ofm_events.append(
                (
                    now,
                    sym_key,
                    gen_ts,
                    str(cls.get("label") or ""),
                    str(cls.get("blocked_reason") or ""),
                    str(cls.get("flow_dir") or ""),
                    str(cls.get("ofm_dir") or ""),
                    cls.get("flow_z_signed"),
                    cls.get("flow_threshold"),
                    cls.get("threshold_mult"),
                )
            )
            added += 1

        if added:
            self._maybe_write_hip3_flow_ofm_stats(now=now)

    def _render_hip3_flow_ofm_stats(self, *, now: float) -> Dict[str, Any]:
        matrix = Counter()
        reasons = Counter()
        per_symbol: Dict[str, Dict[str, Any]] = {}

        for (
            _ts,
            sym,
            _gen_ts,
            label,
            blocked_reason,
            _flow_dir,
            _ofm_dir,
            _z_signed,
            _thr,
            _mult,
        ) in list(self._hip3_flow_ofm_events):
            if label:
                matrix[label] += 1
            if blocked_reason:
                reasons[blocked_reason] += 1

            slot = per_symbol.get(sym)
            if slot is None:
                slot = {"matrix": Counter(), "blocked_reasons": Counter(), "events": 0}
                per_symbol[sym] = slot
            slot["events"] += 1
            if label:
                slot["matrix"][label] += 1
            if blocked_reason:
                slot["blocked_reasons"][blocked_reason] += 1

        # Convert Counters to plain dicts for JSON
        per_symbol_out: Dict[str, Any] = {}
        for sym, slot in per_symbol.items():
            per_symbol_out[sym] = {
                "events": int(slot.get("events") or 0),
                "matrix": dict(slot["matrix"]),
                "blocked_reasons": dict(slot["blocked_reasons"]),
            }

        return {
            "schema_version": 1,
            "generated_at": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC"),
            "window_sec": float(self._cfg.hip3_flow_ofm_stats_window_sec),
            "events": int(len(self._hip3_flow_ofm_events)),
            "matrix": dict(matrix),
            "blocked_reasons": dict(reasons),
            "per_symbol": per_symbol_out,
        }

    def _maybe_write_hip3_flow_ofm_stats(self, *, now: float) -> None:
        if not self._cfg.hip3_flow_ofm_stats_enabled:
            return
        min_interval = float(max(1.0, self._cfg.hip3_flow_ofm_stats_write_min_interval_sec))
        if (now - float(self._hip3_flow_ofm_last_write_ts or 0.0)) < min_interval:
            return

        try:
            payload = self._render_hip3_flow_ofm_stats(now=now)
            tmp_path = f"{HIP3_FLOW_OFM_STATS_FILE}.tmp"
            Path(tmp_path).write_text(
                json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=True),
                encoding="utf-8",
            )
            os.replace(tmp_path, HIP3_FLOW_OFM_STATS_FILE)
            self._hip3_flow_ofm_last_write_ts = now
        except Exception as exc:
            # Never fail cycle_trigger for a diagnostics write.
            try:
                self.log.debug(f"HIP3 stats write failed: {exc}")
            except Exception:
                pass

    def _median(self, values: list[float]) -> float:
        try:
            if not values:
                return 0.0
            return float(statistics.median(values))
        except Exception:
            return 0.0

    def _update_and_write_hip3_window_state(
        self,
        *,
        now_ts: float,
        snapshot_prev_ts: Optional[float],
        snapshot_sse_seq: int,
        snapshot_cycle_seq: int,
    ) -> None:
        """Estimate the snapshot cadence (robustly) and write hip3_window_state.json.

        This is designed for 5-7 minute snapshot environments:
        - Uses median + MAD outlier rejection (no hard clamps).
        - Uses EWMA smoothing so the window adapts slowly.
        """
        if not self._cfg.hip3_window_state_enabled:
            return

        # Update interval sample set
        if snapshot_prev_ts is not None:
            interval = float(now_ts - snapshot_prev_ts)
            if interval > 0:
                self._hip3_snapshot_intervals.append(interval)

        intervals = [float(x) for x in list(self._hip3_snapshot_intervals) if isinstance(x, (int, float)) and x > 0]
        if not intervals:
            # First snapshot after restart (or no stable history yet): publish a sane default so
            # the HIP3 worker can begin operating immediately.
            est = float(self._hip3_interval_est_sec or self._cfg.hip3_window_init_sec or HIP3_WINDOW_INIT_SEC)
            if est <= 0:
                return
            self._hip3_interval_est_sec = est
            payload = {
                "schema_version": 1,
                "updated_at_ts": float(now_ts),
                "updated_at_utc": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC"),
                "snapshot_end_ts": float(now_ts),
                "snapshot_end_utc": datetime.utcfromtimestamp(float(now_ts)).strftime("%Y-%m-%d %H:%M:%S UTC"),
                "snapshot_sse_seq": int(snapshot_sse_seq),
                "snapshot_cycle_seq": int(snapshot_cycle_seq),
                "snapshot_interval_est_sec": float(est),
                "window_sec": float(est),
                "sample_count": 0,
                "sample_count_filtered": 0,
                "ewma_alpha": float(self._cfg.hip3_window_ewma_alpha),
                "outlier_k": float(self._cfg.hip3_window_outlier_k),
                "note": "bootstrap_default",
            }
            try:
                tmp_path = f"{HIP3_WINDOW_STATE_FILE}.tmp"
                Path(tmp_path).write_text(
                    json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=True),
                    encoding="utf-8",
                )
                os.replace(tmp_path, HIP3_WINDOW_STATE_FILE)
                self._hip3_window_last_write_ts = float(now_ts)
                self._hip3_window_last_sse_seq = int(snapshot_sse_seq)
                self._hip3_window_last_cycle_seq = int(snapshot_cycle_seq)
            except Exception:
                pass
            return

        k = float(self._cfg.hip3_window_outlier_k)
        alpha = float(self._cfg.hip3_window_ewma_alpha)
        alpha = max(0.0, min(1.0, alpha))

        med = self._median(intervals)
        abs_dev = [abs(x - med) for x in intervals]
        mad = self._median(abs_dev)

        if mad > 0 and k > 0:
            filtered = [x for x in intervals if abs(x - med) <= (k * mad)]
        else:
            filtered = intervals
        if not filtered:
            filtered = intervals

        med_f = self._median(filtered)
        if self._hip3_interval_est_sec is None or self._hip3_interval_est_sec <= 0:
            est = float(med_f)
        else:
            est = float((1.0 - alpha) * self._hip3_interval_est_sec + alpha * med_f)
        if est <= 0:
            return

        self._hip3_interval_est_sec = est
        window_sec = float(est)

        payload = {
            "schema_version": 1,
            "updated_at_ts": float(now_ts),
            "updated_at_utc": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC"),
            "snapshot_end_ts": float(now_ts),
            "snapshot_end_utc": datetime.utcfromtimestamp(float(now_ts)).strftime("%Y-%m-%d %H:%M:%S UTC"),
            "snapshot_sse_seq": int(snapshot_sse_seq),
            "snapshot_cycle_seq": int(snapshot_cycle_seq),
            "snapshot_interval_med_sec": float(med),
            "snapshot_interval_mad_sec": float(mad),
            "snapshot_interval_med_filtered_sec": float(med_f),
            "snapshot_interval_est_sec": float(est),
            "window_sec": float(window_sec),
            "sample_count": int(len(intervals)),
            "sample_count_filtered": int(len(filtered)),
            "ewma_alpha": float(alpha),
            "outlier_k": float(k),
        }

        try:
            tmp_path = f"{HIP3_WINDOW_STATE_FILE}.tmp"
            Path(tmp_path).write_text(
                json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=True),
                encoding="utf-8",
            )
            os.replace(tmp_path, HIP3_WINDOW_STATE_FILE)
            self._hip3_window_last_write_ts = float(now_ts)
            self._hip3_window_last_sse_seq = int(snapshot_sse_seq)
            self._hip3_window_last_cycle_seq = int(snapshot_cycle_seq)
        except Exception as exc:
            try:
                self.log.debug(f"HIP3 window-state write failed: {exc}")
            except Exception:
                pass

    def _write_hip3_candidates_file(
        self,
        *,
        cycle_seq: int,
        symbol: str,
        direction: str,
        z_score: float,
        hip3_main: Dict[str, Any],
    ) -> Optional[str]:
        candidates_file = str(RUNTIME_DIR / f"evclaw_candidates_{cycle_seq}.json")
        try:
            driver_type = str(hip3_main.get("driver_type") or "").lower()
            sl_mult = float(HIP3_OFM_SL_ATR_MULT)
            tp_mult = float(HIP3_OFM_TP_ATR_MULT)
            if driver_type == "flow":
                sl_mult = float(HIP3_FLOW_SL_ATR_MULT)
                tp_mult = float(HIP3_FLOW_TP_ATR_MULT)
            elif driver_type == "both":
                sl_mult = float((HIP3_FLOW_SL_ATR_MULT + HIP3_OFM_SL_ATR_MULT) / 2.0)
                tp_mult = float((HIP3_FLOW_TP_ATR_MULT + HIP3_OFM_TP_ATR_MULT) / 2.0)
            booster_mult = 1.0
            try:
                booster_mult = float(hip3_main.get("size_mult_hint") or 1.0)
            except Exception:
                booster_mult = 1.0
            booster_mult = max(0.70, min(1.40, booster_mult))

            cand = {
                "symbol": str(symbol).upper(),
                "direction": str(direction).upper(),
                "conviction": float(max(0.6, min(1.0, z_score / 3.0))),
                "reason_short": "HIP3_MAIN",
                "signals": [f"HIP3_MAIN:{direction.upper()} z={z_score:.3f}"],
                "signals_snapshot": {"hip3_main": hip3_main},
                "risk": {
                    "sl_atr_mult": float(sl_mult),
                    "tp_atr_mult": float(tp_mult),
                    "hip3_driver": driver_type or "unknown",
                    "hip3_booster_size_mult": float(booster_mult),
                },
                "rank": 1,
            }
            payload = {
                "schema_version": 1,
                "cycle_seq": int(cycle_seq),
                "generated_at": datetime.utcnow().isoformat(),
                "candidates": [cand],
                "notes": "auto-generated from HIP3 trigger",
                "errors": [],
            }
            Path(candidates_file).write_text(
                json.dumps(payload, separators=(",", ":"), ensure_ascii=True),
                encoding="utf-8",
            )
            return candidates_file
        except Exception as e:
            self.log.warning(f"Failed to write HIP3 candidates file: {e}")
            return None


    async def _handle_hip3_snapshot(self, sequence: int, hip3_state: Dict[str, Any]) -> None:
        # Allows temporarily disabling HIP3-triggered cycle interrupts (focus normal perps).
        if not self._cfg.hip3_interrupts_enabled:
            return
        if not hip3_state:
            return
        now = time.time()
        self._prune_hip3_cache(now)

        # Always record diagnostics, even if we suppress actual trading interrupts.
        self._record_hip3_flow_ofm_stats(hip3_state=hip3_state, now=now)

        if not self._current_symbols:
            self.log.debug("HIP3_TRIGGERED action=suppressed reason=no_symbols_loaded")
            return

        candidates = self._build_hip3_candidates(hip3_state=hip3_state, now=now)

        if not candidates:
            return

        candidates.sort(key=lambda item: (item[0], item[1]), reverse=True)
        flip_priority, z_score, symbol, direction, age_sec, hip3_main = candidates[0]
        flip_detected = bool(flip_priority)

        if (now - self._last_hip3_global_ts) < self._cfg.hip3_trigger_global_cooldown_sec:
            self.log.info(
                "HIP3_TRIGGERED symbol=%s dir=%s z=%.3f age_sec=%.1f action=suppressed reason=global_cooldown",
                symbol,
                direction,
                z_score,
                age_sec,
                )
            return

        cycle_seq = await self._reserve_cycle_seq()
        timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")

        # For HIP3-triggered cycles, pre-generate a minimal candidates file so the
        # live agent will actually attempt the HIP3 symbol (HIP3 symbols do not always
        # rank into top_opportunities).
        candidates_file = self._write_hip3_candidates_file(
            cycle_seq=cycle_seq,
            symbol=symbol,
            direction=direction,
            z_score=z_score,
            hip3_main=hip3_main,
        )

        ok = await self._emit_cycle(
            cycle_seq=cycle_seq,
            sse_seq=int(sequence),
            symbols=self._current_symbols,
            timestamp_utc=timestamp,
            source="hip3",
            candidates_file=candidates_file,
            hip3_symbol=symbol,
            hip3_now_ts=now,
        )
        if not ok:
            return
        self._hip3_symbol_last_dir[symbol] = direction

        self.log.info(
            "HIP3_TRIGGERED symbol=%s dir=%s z=%.3f age_sec=%.1f action=emit reason=%s seq=%s",
            symbol,
            direction,
            z_score,
            age_sec,
            "direction_flip" if flip_detected else "signal",
            cycle_seq,
        )
    
    async def _save_cycle_data(self, sequence: int, symbols: Dict[str, Any], *, sse_seq: Optional[int] = None) -> str:
        """Save full cycle data to JSON file.

        Args:
            sequence: Our monotonic cycle seq (used for filenames + DB)
            symbols: Full symbol data dict
            sse_seq: Raw SSE seq (debug only; may reset)

        Returns:
            Path to saved file
        """
        Path(DATA_DIR).mkdir(parents=True, exist_ok=True)
        data_file = f"{DATA_DIR}/evclaw_cycle_{sequence}.json"

        cycle_data = {
            "sequence": int(sequence),
            "sse_sequence": int(sse_seq) if sse_seq is not None else None,
            "timestamp": datetime.utcnow().isoformat(),
            "symbol_count": len(symbols),
            "symbols": symbols,
        }
        
        try:
            with open(data_file, 'w') as f:
                json.dump(cycle_data, f, separators=(",", ":"), ensure_ascii=True, default=str)

            file_size = os.path.getsize(data_file)
            self.log.info(f"💾 Saved cycle data: {data_file} ({file_size/1024:.1f} KB)")
            mirror_runtime_file_to_tmp(data_file, self.log)

            # Cleanup old cycle files (keep last N). IMPORTANT: if this is too low,
            # the proposal executor may miss cycles and be unable to execute.
            await self._cleanup_old_files(sequence)
            
        except Exception as e:
            self.log.error(f"Failed to save cycle data: {e}")
        
        return data_file
    
    async def _cleanup_old_files(self, current_seq: int) -> None:
        """Remove old cycle/context/candidate files, keeping last N per class."""
        try:
            import re

            keep_n = 20
            keep_n = max(5, keep_n)  # never go below 5

            prefixes = (
                "evclaw_cycle_",
                "evclaw_context_",
                "evclaw_candidates_",
            )
            classes = (
                ("evclaw_cycle_", ".json"),
                ("evclaw_context_", ".txt"),
                ("evclaw_context_", ".json"),
                ("evclaw_candidates_", ".json"),
            )

            for prefix, suffix in classes:
                pattern = f"{prefix}*{suffix}"
                seq_re = re.compile(rf"^{re.escape(prefix)}(\d+){re.escape(suffix)}$")

                def get_sequence(path: Path) -> int:
                    match = seq_re.match(path.name)
                    return int(match.group(1)) if match else 0

                files = sorted(Path(DATA_DIR).glob(pattern), key=get_sequence)
                for old_file in files[:-keep_n]:
                    try:
                        old_file.unlink()
                        self.log.debug(f"Cleaned up old file: {old_file}")
                        _tmp_unlink_best_effort(TMP_MIRROR_DIR / old_file.name)
                    except Exception as e:
                        self.log.warning(f"Failed to delete {old_file}: {e}")

            # Best-effort prune of already-broken symlinks (legacy buildup)
            prune_broken_tmp_symlinks(prefixes=prefixes, log=self.log)
            # Best-effort prune of legacy regular files in /tmp from old copy-based mirroring.
            prune_tmp_regular_mirror_files(prefixes=prefixes, log=self.log)
        except Exception as e:
            self.log.warning(f"Cleanup error: {e}")
    
    async def _build_context(self, sequence: int, data_file: str) -> Tuple[str, str]:
        """Run context builder for the cycle and return context file paths."""
        context_file = f"{DATA_DIR}/evclaw_context_{sequence}.txt"
        context_json_file = f"{DATA_DIR}/evclaw_context_{sequence}.json"
        temp_suffix = f".tmp.{os.getpid()}.{int(time.time() * 1000)}"
        tmp_context_file = f"{context_file}{temp_suffix}"
        tmp_context_json_file = f"{context_json_file}{temp_suffix}"
        cmd = [
            sys.executable,
            CONTEXT_BUILDER,
            "--cycle-file",
            data_file,
            "--output",
            tmp_context_file,
            "--json-output",
            tmp_context_json_file,
        ]

        if self.dry_run:
            self.log.info(f"🔸 DRY RUN - Would build context: {' '.join(cmd)}")
            return context_file, context_json_file

        try:
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout, stderr = await asyncio.wait_for(
                proc.communicate(),
                timeout=self._cfg.context_builder_timeout_sec,
            )
            if proc.returncode != 0:
                self.log.error(
                    f"Context builder failed (rc={proc.returncode}): {stderr.decode().strip()}"
                )
            else:
                if _context_artifacts_ready(tmp_context_file, tmp_context_json_file):
                    os.replace(tmp_context_file, context_file)
                    os.replace(tmp_context_json_file, context_json_file)
                else:
                    self.log.error(
                        "Context builder completed but temp artifacts missing/empty "
                        "(seq=%s tmp_context=%s tmp_context_json=%s)",
                        sequence,
                        tmp_context_file,
                        tmp_context_json_file,
                    )
                self.log.info(f"🧾 Context built: {context_file}")
                self.log.info(f"🧾 JSON context built: {context_json_file}")
                mirror_runtime_file_to_tmp(context_file, self.log)
                mirror_runtime_file_to_tmp(context_json_file, self.log)
                if stdout:
                    self.log.debug(stdout.decode().strip())
        except asyncio.TimeoutError:
            self.log.error(
                f"Context builder timed out after {self._cfg.context_builder_timeout_sec:.1f}s"
            )
            pid = getattr(proc, "pid", None)
            try:
                proc.kill()
            except Exception as kill_exc:
                self.log.warning(
                    "Context builder kill failed after timeout (seq=%s pid=%s): %s",
                    sequence,
                    pid,
                    kill_exc,
                )
            try:
                await asyncio.wait_for(
                    proc.communicate(),
                    timeout=max(0.5, self._cfg.context_builder_kill_timeout_sec),
                )
            except asyncio.TimeoutError:
                self.log.warning(
                    "Context builder cleanup wait timed out (seq=%s pid=%s kill_timeout=%.1fs)",
                    sequence,
                    pid,
                    max(0.5, self._cfg.context_builder_kill_timeout_sec),
                )
            except Exception as cleanup_exc:
                self.log.warning(
                    "Context builder cleanup error (seq=%s pid=%s): %s",
                    sequence,
                    pid,
                    cleanup_exc,
                )
        except Exception as e:
            self.log.error(f"Context builder error: {e}")
        finally:
            _tmp_unlink_best_effort(Path(tmp_context_file))
            _tmp_unlink_best_effort(Path(tmp_context_json_file))

        return context_file, context_json_file

    async def _notify_main_agent(self, payload: Dict[str, Any]) -> None:
        """Record latest notification marker; live-agent polls DB directly."""
        seq = int(payload.get("seq", -1))
        set_last_notified(seq)
        self.log.info(f"📣 Cycle seq={seq} ready (event disabled — live-agent polls DB)")


class CycleAwareSSEClient(TrackerSSEClient):
    """
    Extended SSE client that exposes snapshot events.
    
    Overrides _handle_tracker_data to call on_snapshot callback
    when a snapshot message is received.
    """
    
    def __init__(
        self, 
        on_snapshot=None,
        on_hip3_snapshot=None,
        on_data=None,
        verbose: bool = False,
        **kwargs
    ):
        super().__init__(on_data=on_data, **kwargs)
        self._on_snapshot = on_snapshot
        self._on_hip3_snapshot = on_hip3_snapshot
        self.log.setLevel(logging.DEBUG if verbose else logging.INFO)
    
    async def _handle_tracker_data(self, msg: SSEMessage) -> None:
        """Handle tracker-data, triggering snapshot callback when appropriate."""
        # Call parent implementation first
        await super()._handle_tracker_data(msg)

        # Expose last tracker seq for watchdog / synthetic snapshots.
        try:
            self.last_tracker_seq = int(msg.sequence)
        except Exception:
            pass
        
        # If this was a snapshot, trigger the callback
        if msg.msg_type == 'snapshot' and self._on_snapshot:
            try:
                # Call the async snapshot handler
                await self._on_snapshot(msg.sequence, self._symbols)
            except Exception as e:
                self.log.error(f"on_snapshot callback error: {e}")

    async def _handle_hip3_data(self, msg: SSEMessage) -> None:
        """Handle hip3-data and normalize to current full HIP3 snapshot."""
        await super()._handle_hip3_data(msg)
        if self._on_hip3_snapshot:
            try:
                # Normalize to the full merged HIP3 state so downstream logic
                # is snapshot-based regardless of upstream frame type.
                merged_hip3: Dict[str, Any] = {}
                for symbol, symbol_data in self._symbols.items():
                    if not isinstance(symbol_data, dict):
                        continue
                    hip3_payload = symbol_data.get("hip3_predator")
                    if isinstance(hip3_payload, dict) and hip3_payload:
                        merged_hip3[symbol] = symbol_data
                await self._on_hip3_snapshot(msg.sequence, merged_hip3)
            except Exception as e:
                self.log.error(f"on_hip3_snapshot callback error: {e}")


async def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Monitor SSE for watcher cycles and notify main agent"
    )
    parser.add_argument(
        "--dry-run", 
        action="store_true",
        help="Don't actually notify, just log"
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true", 
        help="Enable debug logging"
    )
    
    args = parser.parse_args()
    
    trigger = CycleTrigger(dry_run=args.dry_run, verbose=args.verbose)
    
    try:
        await trigger.start()
    except KeyboardInterrupt:
        print("\nShutting down...")
    finally:
        await trigger.stop()


if __name__ == "__main__":
    asyncio.run(main())
