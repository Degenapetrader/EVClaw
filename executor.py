#!/usr/bin/env python3
"""
Executor for HyperLighter Trading Skill.

Handles order execution for both exchanges:
- Lighter (crypto perps)
- Hyperliquid (HIP3 stocks)

Features:
- Token bucket rate limiting (40 req/60s for Lighter)
- Exchange adapter layer for unified interface
- Position tracking with atomic persistence
- SL/TP management with ATR-based or fallback pricing
- Crash recovery via state machine

Phase 3 of HyperLighter Trading Skill.
"""

import asyncio
from contextlib import contextmanager
import fcntl
import logging
from logging_utils import get_logger
import os
import time
import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

import yaml

# Env helpers
from mode_controller import get_param
# SQLite is the SINGLE source of truth for tracking. Do not remove DB wiring.
from ai_trader_db import AITraderDB

from exchanges import (
    ExchangeAdapter, ExchangeRouter, ExchangeRoutingError,
    HyperliquidAdapter, LighterAdapter, Position, RouterConfig,
    VENUE_HYPERLIQUID, VENUE_HYPERLIQUID_WALLET, VENUE_LIGHTER, VALID_VENUES,
)
from venues import normalize_venue


# ============================================================================
# Configuration
# ============================================================================

# IMPORTANT: get_param() can return 0/None when running outside the full skill.yaml
# context (e.g. unit tests). Provide sane fallbacks so dry-run tests don't fail.
def _param_float(name: str, default: float, *, min_value: Optional[float] = None) -> float:
    raw = get_param("executor", name)
    try:
        val = float(raw)
    except Exception:
        val = float(default)
    if min_value is not None and val < float(min_value):
        return float(default)
    return val


def _param_int(name: str, default: int, *, min_value: Optional[int] = None) -> int:
    raw = get_param("executor", name)
    try:
        val = int(raw)
    except Exception:
        val = int(default)
    if min_value is not None and val < int(min_value):
        return int(default)
    return val


DEFAULT_BASE_POSITION_SIZE_USD = _param_float("default_base_position_size_usd", 1000.0, min_value=0.01)
DEFAULT_MAX_POSITION_PER_SYMBOL_USD = _param_float("default_max_position_per_symbol_usd", 0.0, min_value=0.0)  # 0 = disabled (no per-symbol cap)
DEFAULT_MAX_TOTAL_EXPOSURE_USD = _param_float("default_max_total_exposure_usd", 100_000_000.0, min_value=0.01)  # 100M - aligned with skill.yaml
DEFAULT_SL_ATR_MULTIPLIER = _param_float("default_sl_atr_multiplier", 2.0, min_value=0.01)   # ATR × 2.0 for stop loss
DEFAULT_TP_ATR_MULTIPLIER = _param_float("default_tp_atr_multiplier", 3.0, min_value=0.01)   # ATR × 3.0 for take profit
DEFAULT_SL_FALLBACK_PCT = _param_float("default_sl_fallback_pct", 2.0, min_value=0.01)     # 2% SL if ATR unavailable
DEFAULT_SLTP_DELAY_SECONDS = _param_float("default_sltp_delay_seconds", 1.5, min_value=0.0)
DEFAULT_RATE_LIMIT_TOKENS = 40

# SL/TP placement reliability
SLTP_MAX_RETRIES = 2
SLTP_RETRY_BASE_DELAY_SECONDS = 1.0
SLTP_RETRY_MAX_DELAY_SECONDS = 10.0
DEFAULT_RATE_LIMIT_WINDOW_SECONDS = 60

# Dust handling (Lighter min size guard)
DUST_STATE = "DUST"
DUST_MISMATCH_PCT = 100.0
DEFAULT_DUST_NOTIONAL_USD = _param_float("default_dust_notional_usd", 10.0, min_value=0.0)

# Adapter call timeout (seconds)
def _adapter_timeout_seconds() -> float:
    return 10.0


ADAPTER_TIMEOUT_SEC = _adapter_timeout_seconds()


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return str(raw).strip().lower() in ("1", "true", "yes", "y", "on")


# Cancel open orders before reduce-only close (disable to keep SL/TP in place).
CANCEL_FIRST_CLOSE = _env_bool("EVCLAW_CANCEL_FIRST_CLOSE", True)

# Chase Limit execution parameters
CHASE_TICK_OFFSET = _param_int("chase_tick_offset", 2, min_value=1)          # Place limit ±2 ticks from mid price
CHASE_POLL_INTERVAL = _param_float("chase_poll_interval", 5.0, min_value=0.1)      # Check every 5 seconds
CHASE_TIMEOUT = _param_float("chase_timeout", 300.0, min_value=1.0)           # Total timeout: 300 seconds (5 min)
CHASE_RETRIGGER_TICKS = _param_int("chase_retrigger_ticks", 2, min_value=1)      # Retrigger when price moves ≥2 ticks from current limit

# Memory paths
DEFAULT_MEMORY_DIR = Path(__file__).parent / "memory"



# ============================================================================
# Rate Limiter
# ============================================================================

class TokenBucket:
    """Token bucket rate limiter.

    Implements a refilling token bucket for rate limiting API requests.
    Default: 40 tokens per 60 seconds (Lighter rate limit).
    """

    def __init__(
        self,
        tokens: int = DEFAULT_RATE_LIMIT_TOKENS,
        refill_seconds: int = DEFAULT_RATE_LIMIT_WINDOW_SECONDS
    ):
        self.tokens = float(tokens)
        self.max_tokens = float(tokens)
        self.refill_rate = tokens / refill_seconds  # tokens per second
        self.last_refill = time.monotonic()
        self._lock = asyncio.Lock()
        self.log = get_logger("rate_limiter")

    def _refill(self) -> None:
        """Refill tokens based on elapsed time."""
        now = time.monotonic()
        elapsed = now - self.last_refill
        self.tokens = min(self.max_tokens, self.tokens + elapsed * self.refill_rate)
        self.last_refill = now

    async def acquire(self, timeout: float = 30.0) -> bool:
        """Acquire a token, waiting up to timeout seconds.

        Returns True if token acquired, False if timeout.
        """
        start = time.monotonic()

        while True:
            async with self._lock:
                self._refill()

                if self.tokens >= 1.0:
                    self.tokens -= 1.0
                    return True

                # Calculate wait time
                wait_time = (1.0 - self.tokens) / self.refill_rate

                if time.monotonic() - start + wait_time > timeout:
                    self.log.warning(f"Rate limit timeout ({timeout}s)")
                    return False

            # Wait outside the lock to avoid convoying other waiters.
            await asyncio.sleep(min(wait_time, 0.5))

# ============================================================================
# Data Classes
# ============================================================================

@dataclass
class ExecutionConfig:
    """Configuration for executor.

    Chase Limit execution replaces market orders.  All entries and exits
    use a limit order that chases the mid price at ±2 ticks, monitored
    every 5 s with a 300 s timeout.  If not filled, the signal is abandoned.

    ATR-based hard SL/TP are placed as exchange trigger orders after fill.
    They act as a safety net; the trading brain can also close early via
    signal decay or manual decision.
    """
    lighter_enabled: bool = True
    hl_enabled: bool = True
    # Legacy compatibility fields kept so old configs / callers remain valid.
    hl_wallet_enabled: bool = False
    hl_mirror_wallet: bool = False
    default_venue_perps: str = VENUE_HYPERLIQUID
    # Legacy alias (same venue after split cleanup).
    default_venue_hip3: str = VENUE_HYPERLIQUID_WALLET
    perps_venues: List[str] = field(default_factory=list)
    base_position_size_usd: float = DEFAULT_BASE_POSITION_SIZE_USD
    max_position_per_symbol_usd: float = DEFAULT_MAX_POSITION_PER_SYMBOL_USD
    max_total_exposure_usd: float = DEFAULT_MAX_TOTAL_EXPOSURE_USD
    enable_sltp_backstop: bool = True  # Hard SL/TP enabled by default
    # When AGI provides SL/TP ATR multipliers, keep them as the seed but still
    # let adaptive SLTP scale by learned symbol/regime factors.
    adaptive_on_override: bool = True
    sl_atr_multiplier: float = DEFAULT_SL_ATR_MULTIPLIER
    tp_atr_multiplier: float = DEFAULT_TP_ATR_MULTIPLIER
    sl_fallback_pct: float = DEFAULT_SL_FALLBACK_PCT
    sltp_delay_seconds: float = DEFAULT_SLTP_DELAY_SECONDS
    rate_limit_tokens: int = DEFAULT_RATE_LIMIT_TOKENS
    rate_limit_window_seconds: int = DEFAULT_RATE_LIMIT_WINDOW_SECONDS
    # Chase Limit parameters
    chase_tick_offset: int = CHASE_TICK_OFFSET
    chase_poll_interval: float = CHASE_POLL_INTERVAL
    chase_timeout: float = CHASE_TIMEOUT
    chase_retrigger_ticks: int = CHASE_RETRIGGER_TICKS
    # Chase limit TIF:
    # - "Gtc" (default): can cross / may fill taker depending on limit placement
    # - "Alo": add-liquidity-only (post-only). Exchange rejects immediate matches.
    chase_entry_tif: str = "Gtc"
    chase_exit_tif: str = "Gtc"

    # Optional fallback: after spending N seconds trying post-only, switch TIF.
    # (Useful when maker-only fill performance is poor or exits become urgent.)
    chase_entry_fallback_after_seconds: float = 0.0
    chase_entry_fallback_tif: str = "Gtc"
    chase_exit_fallback_after_seconds: float = 0.0
    chase_exit_fallback_tif: str = "Gtc"
    dry_run: bool = False
    memory_dir: Path = DEFAULT_MEMORY_DIR
    router_overrides: Dict[str, str] = field(default_factory=dict)
    # Tracking (SQLite is source of truth)
    db_path: Optional[str] = None
    use_db_positions: bool = True
    write_positions_yaml: bool = True  # optional snapshot only
    enable_trade_journal: bool = False
    enable_trade_tracker: bool = False
    use_fill_reconciler: bool = False
    dust_notional_usd: float = DEFAULT_DUST_NOTIONAL_USD
    # Hard floor for new entries (prevents junk micro-positions)
    min_position_notional_usd: float = 0.0
    # Entry underfill guard: if chase-limit times out and only fills a tiny fraction,
    # treat the entry as failed and auto-unwind instead of marking EXECUTED.
    #
    # Example: requested ~$4k, but only ~$200 fills (5%) due to cancel churn.
    min_entry_fill_ratio: float = 0.8
    min_entry_fill_notional_usd: float = 250.0
    # SR-anchored limit entries (v0)
    sr_limit_enabled: bool = False
    sr_limit_max_pending: int = 2
    sr_limit_max_notional_pct: float = 25.0
    sr_limit_timeout_minutes: int = 30
    sr_limit_min_distance_atr: float = 0.3
    sr_limit_max_distance_atr: float = 1.0
    sr_limit_min_strength_z: float = 0.5

    # Conviction gating (v0.1):
    # - If conviction >= this threshold, prefer Chase (fast fill) and skip SR-limit.
    sr_limit_chase_conviction_threshold: float = 0.6
    # If SR-limit is not viable and conviction is below this, skip instead of chasing.
    sr_limit_fallback_chase_min_conviction: float = 0.45
    # Heuristic realism cap: max ATR move expected per hour for SR-limit reachability.
    sr_limit_reachable_atr_per_hour: float = 2.0

    # v0.2 (Hyperliquid only): place SL stop at SR-limit placement time.
    sr_limit_prefill_sl_hl: bool = True


@dataclass
class ExecutionResult:
    """Result of executing a trade decision."""
    success: bool
    symbol: str
    direction: str = ""
    entry_price: float = 0.0
    size: float = 0.0
    sl_price: float = 0.0
    tp_price: float = 0.0
    error: str = ""
    exchange: str = ""


@dataclass
class ExecutionDecision:
    """Decision payload for executor."""
    symbol: str
    direction: str  # LONG, SHORT, NO_TRADE
    size_multiplier: float = 1.0
    signals_agreeing: List[str] = field(default_factory=list)
    conviction: Optional[float] = None
    reason: str = ""
    size_usd: Optional[float] = None
    sl_price: Optional[float] = None
    tp_price: Optional[float] = None
    signals_snapshot: Optional[Dict[str, Any]] = None
    context_snapshot: Optional[Dict[str, Any]] = None
    playbook_id: Optional[Any] = None
    memory_ids: Optional[List[Any]] = None
    execution: Optional[Dict[str, Any]] = None
    risk: Optional[Dict[str, Any]] = None

    @property
    def should_trade(self) -> bool:
        """Whether the decision should be executed."""
        return self.direction in ("LONG", "SHORT")



# ============================================================================
# Persistence
# ============================================================================

class PositionPersistence:
    """Handles atomic persistence of positions and optional trade journal."""

    def __init__(
        self,
        log: logging.Logger,
        memory_dir: Optional[Path] = None,
        enable_positions: bool = True,
        enable_journal: bool = True,
    ):
        self.log = log
        self.memory_dir = Path(memory_dir) if memory_dir else DEFAULT_MEMORY_DIR
        self.positions_file = self.memory_dir / 'positions.yaml'
        self.journal_file = self.memory_dir / 'trade_journal.yaml'
        self.positions_lock_file = self.memory_dir / 'positions.lock'
        self.journal_lock_file = self.memory_dir / 'trade_journal.lock'
        self.enable_positions = enable_positions
        self.enable_journal = enable_journal

        # Ensure memory directory exists
        self.memory_dir.mkdir(parents=True, exist_ok=True)

    @contextmanager
    def _with_lock(self, lock_path: Path, *, shared: bool = False):
        """Acquire a process-shared file lock for critical persistence sections."""
        lock_path.parent.mkdir(parents=True, exist_ok=True)
        with open(lock_path, 'a+') as lock_f:
            mode = fcntl.LOCK_SH if shared else fcntl.LOCK_EX
            fcntl.flock(lock_f.fileno(), mode)
            try:
                yield
            finally:
                fcntl.flock(lock_f.fileno(), fcntl.LOCK_UN)

    def save_positions_atomic(self, positions: Dict[str, Position]) -> bool:
        """Save positions with atomic write (fcntl + rename)."""
        if not self.enable_positions:
            return True
        try:
            with self._with_lock(self.positions_lock_file, shared=False):
                temp_path = self.positions_file.with_suffix('.tmp')
                data = {key: pos.to_dict() for key, pos in positions.items()}

                with open(temp_path, 'w') as f:
                    yaml.dump(data, f, default_flow_style=False)
                    f.flush()
                    os.fsync(f.fileno())

                os.replace(temp_path, self.positions_file)
            return True
        except Exception as e:
            self.log.error(f"Failed to save positions: {e}")
            return False

    def load_positions(self) -> Dict[str, Position]:
        """Load positions from file."""
        if not self.enable_positions:
            return {}
        try:
            if not self.positions_file.exists():
                return {}

            with self._with_lock(self.positions_lock_file, shared=True):
                with open(self.positions_file, 'r') as f:
                    data = yaml.safe_load(f) or {}

            positions: Dict[str, Position] = {}
            for key, pos_data in data.items():
                pos = Position.from_dict(pos_data)
                if not pos.symbol:
                    if ":" in str(key):
                        _, sym = str(key).split(":", 1)
                        pos.symbol = sym
                    else:
                        pos.symbol = str(key)
                if not pos.venue and ":" in str(key):
                    venue, _ = str(key).split(":", 1)
                    pos.venue = venue
                composite_key = f"{pos.venue}:{pos.symbol}" if pos.venue else str(key)
                positions[composite_key] = pos
            return positions
        except Exception as e:
            self.log.error(f"Failed to load positions: {e}")
            return {}

    def append_trade(self, trade: Dict[str, Any]) -> bool:
        """Append trade to journal."""
        if not self.enable_journal:
            return True
        try:
            trade['timestamp'] = datetime.now(timezone.utc).isoformat()

            with self._with_lock(self.journal_lock_file, shared=False):
                # Load existing journal (handles both list and dict format)
                journal_data = {'trades': [], 'last_updated': None}
                if self.journal_file.exists():
                    with open(self.journal_file, 'r') as f:
                        loaded = yaml.safe_load(f)
                        if isinstance(loaded, list):
                            journal_data['trades'] = loaded
                        elif isinstance(loaded, dict):
                            journal_data = loaded
                            if 'trades' not in journal_data:
                                journal_data['trades'] = []

                # Append new trade
                journal_data['trades'].append(trade)
                journal_data['last_updated'] = datetime.now(timezone.utc).isoformat()

                # Keep last 1000 trades (rolling window)
                if len(journal_data['trades']) > 1000:
                    journal_data['trades'] = journal_data['trades'][-1000:]

                # Save atomically
                temp_path = self.journal_file.with_suffix('.tmp')
                with open(temp_path, 'w') as f:
                    yaml.dump(journal_data, f, default_flow_style=False)
                    f.flush()
                    os.fsync(f.fileno())

                os.replace(temp_path, self.journal_file)
            return True
        except Exception as e:
            self.log.error(f"Failed to append trade: {e}")
            return False


# ============================================================================
# Main Executor
# ============================================================================

class Executor:
    """
    Main executor for trading decisions.

    Routes trades to appropriate exchange (Lighter or HL) based on symbol.
    Manages positions, SL/TP, and trade journaling.
    """

    def __init__(
        self,
        config: Optional[ExecutionConfig] = None,
        on_pnl: Optional[Callable[[float], None]] = None,
        on_trade_close: Optional[Callable[[Dict[str, Any]], None]] = None,  # Phase 4: Learning callback
        adaptive_sltp: Optional[Any] = None,  # AdaptiveSLTPManager for learned SL/TP
        trade_tracker: Optional[Any] = None,  # TradeTracker for outcome recording
    ):
        self.config = config or ExecutionConfig()
        self.on_pnl = on_pnl  # Callback to update circuit breaker
        self.on_trade_close = on_trade_close  # Callback for learning engine
        self.adaptive_sltp = adaptive_sltp  # Adaptive SL/TP manager
        self.trade_tracker = trade_tracker  # Trade outcome tracker
        self.log = get_logger("executor")
        self._lock_owner = f"executor:{os.getpid()}"  # cross-process symbol lock owner

        # Rate limiter for Lighter
        self.rate_limiter = TokenBucket(
            tokens=self.config.rate_limit_tokens,
            refill_seconds=self.config.rate_limit_window_seconds,
        )

        # Exchange adapters + router
        self.lighter = LighterAdapter(self.log, dry_run=self.config.dry_run)
        self.hyperliquid = HyperliquidAdapter(self.log, dry_run=self.config.dry_run, account_mode="wallet")
        # Keep a compatibility alias for code paths that still reference
        # `executor.hip3_wallet`; it now points to the same Hyperliquid adapter.
        self.hip3_wallet = self.hyperliquid
        self.router = ExchangeRouter(
            config=RouterConfig(
                lighter_enabled=self.config.lighter_enabled,
                hyperliquid_enabled=self.config.hl_enabled,
                hip3_wallet_enabled=self.config.hl_enabled,
                mirror_hl_wallet=self.config.hl_mirror_wallet,
                overrides=self.config.router_overrides,
            ),
            lighter=self.lighter,
            hyperliquid=self.hyperliquid,
            hip3_wallet=self.hip3_wallet,
            log=self.log,
        )

        # Database (single source of truth for tracking)
        self.db = AITraderDB(str(self.config.db_path)) if self.config.db_path else None

        # Persistence (positions snapshot + optional journal)
        self.persistence = PositionPersistence(
            self.log,
            memory_dir=self.config.memory_dir,
            enable_positions=self.config.write_positions_yaml,
            enable_journal=self.config.enable_trade_journal,
        )

        # Local position state (keyed by "venue:symbol" to avoid collisions)
        self._positions: Dict[str, Position] = {}
        self._initialized = False

        # SR-anchored pending limit manager (v0)
        from pending_limit import PendingLimitManager
        pending_cfg = {
            'sr_limit_enabled': getattr(self.config, 'sr_limit_enabled', False),
            'sr_limit_max_pending': getattr(self.config, 'sr_limit_max_pending', 2),
            'sr_limit_max_notional_pct': getattr(self.config, 'sr_limit_max_notional_pct', 25),
            'sr_limit_timeout_minutes': getattr(self.config, 'sr_limit_timeout_minutes', 30),
            'sr_limit_min_distance_atr': getattr(self.config, 'sr_limit_min_distance_atr', 0.3),
            'sr_limit_max_distance_atr': getattr(self.config, 'sr_limit_max_distance_atr', 1.0),
            'sr_limit_min_strength_z': getattr(self.config, 'sr_limit_min_strength_z', 0.5),
            'sr_limit_chase_conviction_threshold': getattr(self.config, 'sr_limit_chase_conviction_threshold', 0.6),
            'sr_limit_fallback_chase_min_conviction': getattr(self.config, 'sr_limit_fallback_chase_min_conviction', 0.45),
            'sr_limit_reachable_atr_per_hour': getattr(self.config, 'sr_limit_reachable_atr_per_hour', 2.0),
            'sr_limit_prefill_sl_hl': getattr(self.config, 'sr_limit_prefill_sl_hl', True),
        }
        self.pending_mgr = PendingLimitManager(pending_cfg, self.log)

    @classmethod
    async def create(
        cls,
        config: Optional[ExecutionConfig] = None,
        on_pnl: Optional[Callable[[float], None]] = None,
        on_trade_close: Optional[Callable[[Dict[str, Any]], None]] = None,
        adaptive_sltp: Optional[Any] = None,
        trade_tracker: Optional[Any] = None,
    ) -> "Executor":
        """Construct and initialize an executor in one call.

        Reduces the risk of forgetting `await initialize()` in async call sites.
        """
        instance = cls(
            config=config,
            on_pnl=on_pnl,
            on_trade_close=on_trade_close,
            adaptive_sltp=adaptive_sltp,
            trade_tracker=trade_tracker,
        )
        ok = await instance.initialize()
        if not ok:
            raise RuntimeError("Executor initialization failed")
        return instance

    @staticmethod
    def _position_key(symbol: str, venue: str) -> str:
        """Build composite key for positions."""
        return f"{venue}:{symbol}"

    @staticmethod
    def _is_builder_symbol(symbol: str) -> bool:
        s = str(symbol or "").strip()
        if ":" not in s:
            return False
        pfx = s.split(":", 1)[0].strip().lower()
        return pfx in HyperliquidAdapter.BUILDER_DEXES

    def _coerce_venue_for_symbol(self, symbol: str, venue: Optional[str]) -> Optional[str]:
        """Self-heal legacy venue mismatches for builder symbols.

        Builder-dex symbols (xyz:*, km:*, ...) are traded through the unified
        Hyperliquid venue. Old rows/proposals may still carry split venue labels.
        """
        if venue is None:
            return None
        v = normalize_venue(venue)
        if self._is_builder_symbol(symbol):
            if v != VENUE_HYPERLIQUID:
                self.log.warning(
                    f"Venue auto-remap for builder symbol {symbol}: {v or '<none>'} -> {VENUE_HYPERLIQUID}"
                )
            return VENUE_HYPERLIQUID
        return v

    def _find_positions_by_symbol(self, symbol: str) -> List[Tuple[str, Position]]:
        """Find all positions matching a symbol across venues."""
        matches: List[Tuple[str, Position]] = []
        for key, pos in self._positions.items():
            if pos.symbol == symbol:
                matches.append((key, pos))
        return matches

    def _resolve_position(self, symbol: str, venue: Optional[str] = None) -> Tuple[Optional[str], Optional[Position]]:
        """Resolve a position key+object for symbol/venue."""
        if venue:
            key = self._position_key(symbol, venue)
            return key, self._positions.get(key)

        matches = self._find_positions_by_symbol(symbol)
        if not matches:
            return None, None
        if len(matches) > 1:
            self.log.error(
                f"Ambiguous position lookup for {symbol}: {len(matches)} venues found. "
                "Provide explicit venue."
            )
            return None, None
        return matches[0]

    async def initialize(self) -> bool:
        """Initialize executor and exchanges."""
        self.log.info("Initializing executor...")

        # Load positions (prefer DB as source of truth)
        if self.db and self.config.use_db_positions:
            self._positions = self._load_positions_from_db()
            if self._positions:
                self.log.info(f"Recovered {len(self._positions)} positions from database")
            elif self.persistence.enable_positions and self.persistence.positions_file.exists():
                self.log.warning("No open trades in DB; positions.yaml exists but will be ignored")
        else:
            self._positions = self.persistence.load_positions()
            if self._positions:
                self.log.info(f"Recovered {len(self._positions)} positions from disk")

        # Initialize exchanges (count only enabled venues)
        lighter_ok = False
        hl_ok = False
        if self.config.hl_wallet_enabled:
            self.log.info(
                "hl_wallet_enabled is deprecated; using unified Hyperliquid venue for all symbols."
            )
            self.config.hl_wallet_enabled = False
            self.router.config.hip3_wallet_enabled = False

        if self.config.lighter_enabled:
            lighter_ok = await self.lighter.initialize()
            if not lighter_ok:
                self.log.error("Lighter enabled but failed to initialize; continuing without Lighter for this run")

        if self.config.hl_enabled:
            hl_ok = await self.hyperliquid.initialize()
            if not hl_ok:
                self.log.error("Hyperliquid enabled but failed to initialize; continuing without Hyperliquid for this run")

        # Loud account-mode logs for audit safety.
        try:
            if hl_ok:
                addr = getattr(self.hyperliquid, "_address", None)
                self.log.info(f"[HL_INIT] unified Hyperliquid account address={addr}")
        except Exception:
            pass

        self._initialized = bool(
            (self.config.lighter_enabled and lighter_ok)
            or (self.config.hl_enabled and hl_ok)
        )
        if self._initialized:
            await self._reconcile_positions_from_exchange()
        return self._initialized

    async def close(self) -> None:
        """Close exchange adapters and release resources."""
        # Close Lighter adapter (if ever initialized)
        try:
            if getattr(self, "lighter", None) is not None:
                await self.lighter.close()
        except Exception as exc:
            try:
                self.log.warning(f"Lighter close failed: {exc}")
            except Exception:
                pass

        # Close Hyperliquid adapter (if ever initialized)
        try:
            if getattr(self, "hyperliquid", None) is not None:
                await self.hyperliquid.close()
        except Exception as exc:
            try:
                self.log.warning(f"Hyperliquid close failed: {exc}")
            except Exception:
                pass

    def _load_positions_from_db(self) -> Dict[str, Position]:
        """Build local positions from open trades in the DB."""
        if not self.db:
            return {}
        positions: Dict[str, Position] = {}
        position_is_legacy: Dict[str, bool] = {}
        try:
            open_trades = self.db.get_open_trades()
        except Exception as e:
            self.log.error(f"Failed to load open trades from DB: {e}")
            return {}

        for trade in open_trades:
            opened_at = datetime.fromtimestamp(trade.entry_time, tz=timezone.utc).isoformat()
            signals = trade.get_signals_agreed_list()
            effective_venue = self._coerce_venue_for_symbol(trade.symbol, trade.venue) or trade.venue
            key = self._position_key(trade.symbol, effective_venue)
            trade_is_legacy = normalize_venue(trade.venue) != normalize_venue(effective_venue)

            # There can only be ONE net position per (symbol, venue). If we have multiple
            # open trades for the same key, it is a tracking bug. Keep the newest
            # (open_trades is ordered DESC by entry_time) and close the older duplicates.
            if key in positions:
                existing_legacy = bool(position_is_legacy.get(key, False))
                # Prefer canonical venue rows over legacy venue rows when both exist.
                if existing_legacy and not trade_is_legacy:
                    stale_id = positions[key].trade_id
                    if stale_id:
                        self.log.warning(
                            f"🚨 Legacy builder venue row detected for {key}. "
                            f"Replacing stale trade_id={stale_id} with canonical trade_id={trade.id}."
                        )
                        try:
                            self.db.mark_trade_closed_externally(
                                int(stale_id),
                                exit_reason="RECONCILE_LEGACY_BUILDER_VENUE",
                            )
                        except Exception as exc:
                            self.log.warning(f"Failed to close legacy trade {stale_id} externally: {exc}")
                    # Replace with canonical row below.
                else:
                    self.log.warning(
                        f"🚨 Duplicate open trade rows detected for {key}. "
                        f"Keeping newest; closing stale trade_id={trade.id} externally."
                    )
                    try:
                        self.db.mark_trade_closed_externally(
                            trade.id,
                            exit_reason="RECONCILE_DUPLICATE_OPEN_TRADE",
                        )
                    except Exception as exc:
                        self.log.warning(f"Failed to close duplicate trade {trade.id} externally: {exc}")
                    continue

            positions[key] = Position(
                symbol=trade.symbol,
                direction=trade.direction,
                size=trade.size,
                entry_price=trade.entry_price,
                unrealized_pnl=0.0,
                sl_price=trade.sl_price or 0.0,
                tp_price=trade.tp_price or 0.0,
                sl_order_id=trade.sl_order_id,
                tp_order_id=trade.tp_order_id,
                opened_at=opened_at,
                state=getattr(trade, "state", "ACTIVE") or "ACTIVE",
                signals_agreeing=signals,
                venue=effective_venue,
                trade_id=trade.id,
            )
            position_is_legacy[key] = trade_is_legacy

        # Snapshot to positions.yaml if enabled
        if positions:
            self.persistence.save_positions_atomic(positions)
        return positions

    async def _reconcile_positions_from_exchange(self) -> None:
        """Reconcile exchange positions against local tracking."""
        now_iso = datetime.now(timezone.utc).isoformat()
        updated = False

        venues: List[Tuple[str, ExchangeAdapter, bool]] = [
            (VENUE_LIGHTER, self.lighter, self.config.lighter_enabled),
            (VENUE_HYPERLIQUID, self.hyperliquid, self.config.hl_enabled),
        ]

        for venue, adapter, enabled in venues:
            if not enabled or not getattr(adapter, "_initialized", False):
                continue

            try:
                exchange_positions = await asyncio.wait_for(
                    adapter.get_all_positions(),
                    timeout=ADAPTER_TIMEOUT_SEC,
                )
                self.log.info(f"✅ Reconciled {len(exchange_positions)} positions from {venue}")
            except asyncio.TimeoutError:
                self.log.error(
                    f"🚨 RECONCILIATION TIMEOUT for {venue} after {ADAPTER_TIMEOUT_SEC:.2f}s"
                )
                continue
            except Exception as e:
                self.log.error(
                    f"🚨 RECONCILIATION FAILURE for {venue}: {e}\n"
                    f"   ⚠️  Positions on {venue} will NOT be tracked until next successful reconciliation!\n"
                    f"   This could lead to untracked positions if orders were executed outside this system."
                )
                continue

            for symbol, exchange_pos in exchange_positions.items():
                if (
                    venue == VENUE_HYPERLIQUID
                    and self._is_builder_symbol(symbol)
                ):
                    # Builder symbols are first-class on unified Hyperliquid venue.
                    # Never force-close open builder trades here.
                    # Only clean stale in-memory legacy keys (if any).
                    for legacy_venue in ("hip3", "hyperliquid_wallet", "hl_wallet", "wallet"):
                        legacy_key = self._position_key(symbol, legacy_venue)
                        if legacy_key in self._positions:
                            del self._positions[legacy_key]
                            updated = True

                if exchange_pos.size <= 0 or exchange_pos.direction == "FLAT":
                    continue

                key = self._position_key(symbol, venue)
                tracked = self._positions.get(key)
                exch_dir = (exchange_pos.direction or "").upper()

                if tracked and tracked.direction != "FLAT":
                    # -----------------------------------------------------------------
                    # DIRECTION + DUPLICATE SELF-HEAL
                    # -----------------------------------------------------------------
                    # Net-position venues MUST have at most one open trade row per
                    # (symbol, venue). If we ever end up with duplicates (e.g. after a
                    # signal flip / reconciliation lag), fill matching and decay logic
                    # become ambiguous.
                    if self.db:
                        try:
                            open_trades_key = self.db.get_open_trades_for_key(symbol=symbol, venue=venue)
                            if open_trades_key:
                                # Prefer trade with direction matching exchange truth.
                                chosen = None
                                if exch_dir in ("LONG", "SHORT"):
                                    for t in open_trades_key:
                                        if (t.direction or "").upper() == exch_dir:
                                            chosen = t
                                            break
                                if chosen is None:
                                    chosen = open_trades_key[0]

                                # Close any other open trades for this key.
                                for t in open_trades_key:
                                    if t.id != chosen.id:
                                        self.db.mark_trade_closed_externally(
                                            t.id,
                                            exit_reason="RECONCILE_DUPLICATE_OPEN_TRADE",
                                        )

                                # Ensure tracked trade_id points at the chosen row.
                                if tracked.trade_id != chosen.id:
                                    tracked.trade_id = chosen.id
                                    updated = True

                                # Ensure DB direction matches exchange direction (source of truth).
                                if exch_dir in ("LONG", "SHORT") and (chosen.direction or "").upper() != exch_dir:
                                    self.db.update_trade_direction(chosen.id, exch_dir)

                        except Exception as exc:
                            self.log.warning(f"Reconcile duplicate/direction self-heal failed for {venue}:{symbol}: {exc}")

                    # Ensure tracked direction matches exchange direction.
                    if exch_dir in ("LONG", "SHORT") and tracked.direction != exch_dir:
                        self.log.warning(
                            f"⚠️  Reconcile direction mismatch on {venue} for {symbol}: "
                            f"tracked={tracked.direction} exchange={exch_dir}. Updating tracked direction."
                        )
                        tracked.direction = exch_dir
                        updated = True

                    if not tracked.venue:
                        tracked.venue = venue
                        updated = True

                    # Sync size/entry from exchange to prevent mismatches.
                    size_diff = abs(tracked.size - exchange_pos.size)
                    size_updated = False
                    entry_updated = False
                    if size_diff > 0:
                        diff_pct = (size_diff / tracked.size * 100) if tracked.size > 0 else 0
                        if diff_pct > 5.0:
                            self.log.warning(
                                f"⚠️  Position size mismatch on {venue} for {symbol}: "
                                f"tracked={tracked.size:.6f}, exchange={exchange_pos.size:.6f} "
                                f"({diff_pct:.1f}%). Updating to exchange size."
                            )
                        else:
                            self.log.debug(
                                f"Position size sync on {venue} for {symbol}: "
                                f"{tracked.size:.6f} -> {exchange_pos.size:.6f}"
                            )
                        tracked.size = exchange_pos.size
                        size_updated = True
                        updated = True
                    if exchange_pos.entry_price > 0 and tracked.entry_price != exchange_pos.entry_price:
                        tracked.entry_price = exchange_pos.entry_price
                        entry_updated = True
                        updated = True
                    if not tracked.venue:
                        tracked.venue = venue
                        updated = True

                    # Sync unrealized PnL for downstream risk/portfolio logic.
                    # (Not persisted to trades table; kept on the in-memory position state.)
                    try:
                        exch_unrl = float(exchange_pos.unrealized_pnl or 0.0)
                    except Exception:
                        exch_unrl = 0.0
                    try:
                        if abs(float(getattr(tracked, 'unrealized_pnl', 0.0) or 0.0) - exch_unrl) > 0.01:
                            tracked.unrealized_pnl = exch_unrl
                            updated = True
                    except Exception:
                        pass

                    if self.db and tracked.trade_id and (size_updated or entry_updated):
                        self.db.update_trade_position(
                            tracked.trade_id,
                            size=tracked.size if size_updated else None,
                            entry_price=tracked.entry_price if entry_updated else None,
                        )
                    continue

                # Add missing position to tracking (e.g., IMX)
                # Use NEEDS_PROTECTION state - SL/TP will be placed by _place_protection_for_unprotected()
                exchange_pos.state = "NEEDS_PROTECTION"
                exchange_pos.opened_at = exchange_pos.opened_at or now_iso
                exchange_pos.venue = venue
                if self.db:
                    # IMPORTANT: net-position venues must have at most one open
                    # trade row per (symbol, venue). Prefer reusing any existing open
                    # trade row (even if direction mismatched) to avoid creating
                    # duplicate open trades.
                    existing_any = self.db.find_open_trade(symbol, venue=venue)
                    if existing_any:
                        exchange_pos.trade_id = existing_any.id
                        # Make DB direction match exchange truth.
                        exch_dir = (exchange_pos.direction or "").upper()
                        if exch_dir in ("LONG", "SHORT") and (existing_any.direction or "").upper() != exch_dir:
                            self.db.update_trade_direction(existing_any.id, exch_dir)

                        # If existing trade has no SL, mark for protection
                        if not existing_any.sl_order_id:
                            self.db.update_trade_state(existing_any.id, "NEEDS_PROTECTION")

                        # Close any other duplicates (defensive).
                        try:
                            open_trades_key = self.db.get_open_trades_for_key(symbol=symbol, venue=venue)
                            for t in open_trades_key:
                                if t.id != existing_any.id:
                                    self.db.mark_trade_closed_externally(
                                        t.id,
                                        exit_reason="RECONCILE_DUPLICATE_OPEN_TRADE",
                                    )
                        except Exception:
                            pass
                    else:
                        around_ts = time.time()
                        try:
                            opened_raw = getattr(exchange_pos, "opened_at", None)
                            if isinstance(opened_raw, (int, float)):
                                around_ts = float(opened_raw)
                            elif isinstance(opened_raw, str) and opened_raw.strip():
                                iso = opened_raw.strip()
                                if iso.endswith("Z"):
                                    iso = iso[:-1] + "+00:00"
                                around_ts = datetime.fromisoformat(iso).timestamp()
                        except Exception:
                            around_ts = time.time()

                        reconcile_meta: Dict[str, Any] = {}
                        if self.db:
                            try:
                                pending_meta = self.db.find_recent_filled_pending_order(
                                    symbol=symbol,
                                    venue=venue,
                                    around_ts=around_ts,
                                    window_sec=600.0,
                                ) or {}
                            except Exception:
                                pending_meta = {}

                            try:
                                proposal_meta = self.db.find_recent_executed_proposal(
                                    symbol=symbol,
                                    venue=venue,
                                    around_ts=around_ts,
                                    window_sec=600.0,
                                ) or {}
                            except Exception:
                                proposal_meta = {}

                            # Prefer pending-order context, then backfill missing fields
                            # from proposal metadata.
                            if pending_meta:
                                reconcile_meta.update(pending_meta)
                            if proposal_meta:
                                if not reconcile_meta:
                                    reconcile_meta.update(proposal_meta)
                                else:
                                    for k in (
                                        "signals_agreed",
                                        "signals_snapshot",
                                        "context_snapshot",
                                        "risk",
                                        "conviction",
                                        "reason",
                                        "execution_order_type",
                                        "execution_source",
                                        "execution_limit_style",
                                    ):
                                        if not reconcile_meta.get(k) and proposal_meta.get(k):
                                            reconcile_meta[k] = proposal_meta[k]
                                    if reconcile_meta.get("source") and proposal_meta.get("source"):
                                        reconcile_meta["source"] = f"{reconcile_meta['source']}+{proposal_meta['source']}"

                        signals_agreed = reconcile_meta.get("signals_agreed") if isinstance(reconcile_meta.get("signals_agreed"), list) else []
                        signals_agreed = [str(s) for s in signals_agreed if str(s).strip()]
                        signals_snapshot = reconcile_meta.get("signals_snapshot")
                        if not isinstance(signals_snapshot, dict):
                            signals_snapshot = None
                        context_snapshot = reconcile_meta.get("context_snapshot")
                        if not isinstance(context_snapshot, dict):
                            context_snapshot = None
                        reconcile_risk = reconcile_meta.get("risk")
                        if not isinstance(reconcile_risk, dict):
                            reconcile_risk = None

                        reconcile_source = str(reconcile_meta.get("source") or "reconcile_untracked")
                        instrumentation_quality = "medium" if (signals_snapshot or signals_agreed or context_snapshot) else "low"

                        if signals_snapshot is None:
                            signals_snapshot = {
                                "reconciled": True,
                                "source": reconcile_source,
                                "signals_agreed": list(signals_agreed),
                                "instrumentation_quality": instrumentation_quality,
                                "detected_at": time.time(),
                            }

                        if context_snapshot is None:
                            context_snapshot = {
                                "reconciled_untracked": True,
                                "source": reconcile_source,
                                "instrumentation_quality": instrumentation_quality,
                                "detected_at": time.time(),
                                "symbol": symbol,
                                "venue": venue,
                            }
                        if reconcile_risk:
                            context_snapshot_risk = context_snapshot.get("risk")
                            context_snapshot_risk = (
                                dict(context_snapshot_risk) if isinstance(context_snapshot_risk, dict) else {}
                            )
                            for key, value in reconcile_risk.items():
                                context_snapshot_risk.setdefault(key, value)
                            if context_snapshot_risk:
                                context_snapshot["risk"] = context_snapshot_risk
                        if not str(context_snapshot.get("strategy_segment") or "").strip():
                            context_snapshot["strategy_segment"] = (
                                "hip3" if str(symbol or "").upper().startswith("XYZ:") else "perp"
                            )
                        if not str(context_snapshot.get("entry_gate_mode") or "").strip():
                            gate_mode = None
                            risk_obj = context_snapshot.get("risk")
                            if isinstance(risk_obj, dict):
                                gate_mode = str(
                                    risk_obj.get("entry_gate_mode")
                                    or risk_obj.get("gate_mode")
                                    or ""
                                ).strip().lower()
                            if not gate_mode:
                                gate_mode = "hip3" if str(symbol or "").upper().startswith("XYZ:") else "normal"
                            context_snapshot["entry_gate_mode"] = gate_mode
                        execution_order_type = str(reconcile_meta.get("execution_order_type") or "").strip().lower()
                        if execution_order_type in {"limit", "chase_limit", "sr_limit"}:
                            if not str(context_snapshot.get("order_type") or "").strip():
                                context_snapshot["order_type"] = execution_order_type
                            if not str(context_snapshot.get("execution_route") or "").strip():
                                context_snapshot["execution_route"] = "reconcile_from_proposal_execution"
                            execution_source = str(reconcile_meta.get("execution_source") or "").strip()
                            if execution_source and not str(context_snapshot.get("execution_source") or "").strip():
                                context_snapshot["execution_source"] = execution_source
                            limit_style = str(reconcile_meta.get("execution_limit_style") or "").strip()
                            if limit_style and not str(context_snapshot.get("limit_style") or "").strip():
                                context_snapshot["limit_style"] = limit_style

                        confidence = None
                        try:
                            cv = reconcile_meta.get("conviction")
                            confidence = str(float(cv)) if cv is not None else None
                        except Exception:
                            confidence = None

                        ai_reasoning = str(reconcile_meta.get("reason") or "RECONCILED_UNTRACKED")

                        exchange_pos.trade_id = self.db.log_trade_entry(
                            symbol=symbol,
                            direction=exchange_pos.direction,
                            entry_price=exchange_pos.entry_price,
                            size=exchange_pos.size,
                            venue=venue,
                            state="NEEDS_PROTECTION",
                            signals_snapshot=signals_snapshot,
                            signals_agreed=signals_agreed,
                            context_snapshot=context_snapshot,
                            ai_reasoning=ai_reasoning,
                            confidence=confidence,
                        )
                self._positions[key] = exchange_pos
                updated = True
                self.log.warning(
                    f"🚨 Untracked position detected on {venue}: {symbol} "
                    f"{exchange_pos.direction} size={exchange_pos.size:.6f} @ ${exchange_pos.entry_price:.4f}. "
                    f"Added to positions.yaml for tracking."
                )

        if updated:
            self.persistence.save_positions_atomic(self._positions)

        # Place SL/TP for any unprotected positions
        await self._place_protection_for_unprotected()

    async def _place_protection_for_unprotected(self) -> None:
        """Place SL/TP orders for trades missing protection.

        Finds all trades in DB where:
        - state = 'NEEDS_PROTECTION', OR
        - state = 'ACTIVE' AND sl_order_id IS NULL

        For each, calculates SL/TP and places orders using existing methods.
        Updates DB state to ACTIVE and sets sl_order_id/tp_order_id on success.
        """
        if not self.db:
            return

        try:
            unprotected = self.db.get_unprotected_trades()
        except Exception as e:
            self.log.error(f"Failed to get unprotected trades: {e}")
            return

        if not unprotected:
            return

        self.log.info(f"🛡️ Found {len(unprotected)} trades needing protection - placing SL/TP orders...")

        # Import ATR service for calculating SL/TP
        try:
            from atr_service import get_atr_service
            atr_service = get_atr_service()
        except Exception:
            atr_service = None

        for trade in unprotected:
            symbol = trade.symbol
            raw_venue = trade.venue
            venue = normalize_venue(raw_venue)
            if not venue:
                venue = str(raw_venue or "")
            direction = trade.direction
            entry_price = trade.entry_price
            size = trade.size
            trade_id = trade.id

            self.log.info(f"🛡️ Placing protection for trade {trade_id}: {symbol} {direction} on {venue}")

            # Get the adapter for this venue
            adapter = None
            if venue == VENUE_LIGHTER and self.config.lighter_enabled:
                adapter = self.lighter
            elif venue == VENUE_HYPERLIQUID and self.config.hl_enabled:
                adapter = self.hyperliquid
            elif venue == VENUE_HYPERLIQUID_WALLET and self.config.hl_enabled:
                adapter = self.hyperliquid

            if not adapter or not getattr(adapter, "_initialized", False):
                self.log.warning(
                    f"Adapter for {venue} not available (raw_venue={raw_venue}), "
                    f"skipping protection for trade {trade_id}"
                )
                continue

            # Get ATR for SL/TP calculation
            atr = None
            if atr_service:
                try:
                    atr_result = await atr_service.get_atr(symbol, price=entry_price)
                    atr = atr_result.atr if atr_result else None
                except Exception:
                    pass

            # Calculate SL/TP (will use 2% fallback if no ATR)
            sl_price, tp_price, sltp_metadata = self._calculate_sl_tp(
                direction=direction,
                entry_price=entry_price,
                atr=atr,
                symbol=symbol,
                venue=venue,
            )

            # Place SL order
            sl_side = "SELL" if direction == "LONG" else "BUY"
            sl_ok, sl_oid = await self._place_stop_with_retries(
                adapter=adapter,
                symbol=symbol,
                side=sl_side,
                size=size,
                trigger_price=sl_price,
                order_type="sl",
            )

            # Place TP order
            tp_side = sl_side  # Same side as SL (both are exit orders)
            tp_ok, tp_oid = await self._place_stop_with_retries(
                adapter=adapter,
                symbol=symbol,
                side=tp_side,
                size=size,
                trigger_price=tp_price,
                order_type="tp",
            )

            # Update DB with SL/TP info
            if sl_ok or tp_ok:
                self.db.update_trade_sltp(
                    trade_id=trade_id,
                    sl_price=sl_price if sl_ok else None,
                    tp_price=tp_price if tp_ok else None,
                    sl_order_id=sl_oid if sl_ok else None,
                    tp_order_id=tp_oid if tp_ok else None,
                    protection_snapshot=sltp_metadata,
                )

            # Update state to ACTIVE if SL was placed successfully
            if sl_ok:
                self.db.update_trade_state(trade_id, "ACTIVE")
                self.log.info(
                    f"✅ Protection placed for trade {trade_id} ({symbol}): "
                    f"SL=${sl_price:.4f} (oid={sl_oid}), TP=${tp_price:.4f} (oid={tp_oid})"
                )

                # Also update the in-memory position
                key = self._position_key(symbol, venue)
                if key in self._positions:
                    self._positions[key].sl_price = sl_price
                    self._positions[key].tp_price = tp_price
                    self._positions[key].sl_order_id = sl_oid
                    self._positions[key].tp_order_id = tp_oid
                    self._positions[key].state = "ACTIVE"
                    self.persistence.save_positions_atomic(self._positions)
            else:
                self.log.error(
                    f"❌ Failed to place SL for trade {trade_id} ({symbol}) - "
                    f"position remains unprotected! Will retry on next reconciliation."
                )

    def _get_adapter(self, symbol: str) -> Optional[ExchangeAdapter]:
        """Get appropriate adapter for symbol."""
        try:
            return self.router.select(symbol)
        except ExchangeRoutingError as exc:
            self.log.warning(str(exc))
            return None

    def _calculate_position_size(self, decision: ExecutionDecision) -> float:
        """Calculate position size in USD."""
        size_usd = float(decision.size_usd or 0.0)

        # Enforce a hard minimum notional for NEW entries.
        # This prevents accumulating lots of junk micro-positions.
        min_notional = float(getattr(self.config, 'min_position_notional_usd', 0.0) or 0.0)
        if min_notional > 0 and size_usd < min_notional:
            self.log.warning(
                f"Skipping entry: size_usd=${size_usd:.2f} below min_position_notional_usd=${min_notional:.2f}"
            )
            # Return a negative sentinel so execute() can surface the real reason.
            return -1.0

        # Optional per-symbol cap. Set <=0 to disable.
        cap = float(self.config.max_position_per_symbol_usd or 0.0)
        if cap > 0:
            size_usd = min(size_usd, cap)

        # Check total exposure limit (<=0 disables)
        total_exposure = sum(
            p.size * p.entry_price
            for p in self._positions.values()
            if p.direction != 'FLAT'
            and p.state != DUST_STATE
            and (p.size * p.entry_price) >= self.config.dust_notional_usd
        )
        try:
            max_total = float(getattr(self.config, 'max_total_exposure_usd', 0.0) or 0.0)
        except Exception:
            max_total = 0.0
        if max_total > 0:
            remaining = max_total - total_exposure
            if size_usd > remaining:
                self.log.warning(f"Capping size to {remaining:.2f} USD (exposure limit)")
                size_usd = max(0, remaining)

        return size_usd

    @staticmethod
    def _extract_sizing_tracking_from_decision(
        decision: ExecutionDecision,
        *,
        venue: Optional[str] = None,
    ) -> Tuple[Optional[float], Optional[float], Optional[float]]:
        """Extract risk/equity/size tracking fields from decision metadata."""
        risk_meta = decision.risk if isinstance(decision.risk, dict) else {}
        venue_key = str(venue or "").lower()

        def _maybe_float(value: Any) -> Optional[float]:
            try:
                out = float(value)
            except Exception:
                return None
            if out != out:
                return None
            return out

        risk_pct_used = _maybe_float(risk_meta.get("risk_pct_used"))
        equity_at_entry = _maybe_float(risk_meta.get("equity_at_entry"))
        size_multiplier = _maybe_float(
            risk_meta.get("size_multiplier_effective", risk_meta.get("size_multiplier"))
        )

        if (risk_pct_used is None or equity_at_entry is None) and venue_key:
            risk_by_venue = risk_meta.get("risk_pct_used_by_venue")
            equity_by_venue = risk_meta.get("equity_at_entry_by_venue")
            if isinstance(risk_by_venue, dict):
                if risk_pct_used is None:
                    risk_pct_used = _maybe_float(risk_by_venue.get(venue_key))
            if isinstance(equity_by_venue, dict):
                if equity_at_entry is None:
                    equity_at_entry = _maybe_float(equity_by_venue.get(venue_key))

        if size_multiplier is None:
            size_multiplier = _maybe_float(decision.size_multiplier)
        if size_multiplier is None:
            size_multiplier = 1.0
        return risk_pct_used, equity_at_entry, size_multiplier

    def _build_learning_context_snapshot(
        self,
        *,
        decision: ExecutionDecision,
        symbol: str,
        direction: str,
        venue: Optional[str],
        order_type: str,
        execution_route: str,
    ) -> Dict[str, Any]:
        """Build enriched context snapshot for trade learning features."""
        context: Dict[str, Any] = (
            dict(decision.context_snapshot)
            if isinstance(decision.context_snapshot, dict)
            else {}
        )
        risk_meta: Dict[str, Any] = (
            dict(decision.risk)
            if isinstance(decision.risk, dict)
            else {}
        )
        signals_snapshot: Dict[str, Any] = (
            dict(decision.signals_snapshot)
            if isinstance(decision.signals_snapshot, dict)
            else {}
        )

        def _safe_text(value: Any) -> Optional[str]:
            txt = str(value or "").strip()
            return txt if txt else None

        def _safe_float(value: Any) -> Optional[float]:
            try:
                if value is None:
                    return None
                return float(value)
            except Exception:
                return None

        def _safe_bool(value: Any) -> Optional[bool]:
            if value is None:
                return None
            if isinstance(value, bool):
                return value
            if isinstance(value, (int, float)):
                if value == 1:
                    return True
                if value == 0:
                    return False
                return None
            txt = str(value).strip().lower()
            if txt in {"1", "true", "yes", "y", "on"}:
                return True
            if txt in {"0", "false", "no", "n", "off"}:
                return False
            return None

        context["order_type"] = str(order_type or "chase_limit")
        context["execution_route"] = str(execution_route or "executor_chase")
        context.setdefault("symbol", str(symbol or "").upper())
        context.setdefault("direction", str(direction or "").upper())
        if venue:
            context.setdefault("venue", str(venue).lower())

        context_risk = context.get("risk")
        context_risk = dict(context_risk) if isinstance(context_risk, dict) else {}
        for key, value in risk_meta.items():
            context_risk.setdefault(key, value)
        if context_risk:
            context["risk"] = context_risk

        strategy_segment = _safe_text(context.get("strategy_segment"))
        if strategy_segment is None:
            strategy_segment = "hip3" if str(symbol or "").upper().startswith("XYZ:") else "perp"
        context["strategy_segment"] = strategy_segment

        gate_mode = _safe_text(context.get("entry_gate_mode"))
        if gate_mode is None:
            gate_mode = _safe_text(context.get("gate_mode"))
        entry_gate_obj = context.get("entry_gate")
        entry_gate_obj = dict(entry_gate_obj) if isinstance(entry_gate_obj, dict) else {}
        if gate_mode is None:
            gate_mode = _safe_text(entry_gate_obj.get("gate_mode"))
        if gate_mode is None:
            gate_mode = _safe_text(risk_meta.get("entry_gate_mode"))
        if gate_mode is None:
            gate_mode = _safe_text(risk_meta.get("gate_mode"))
        if gate_mode is None:
            gate_mode = "hip3" if strategy_segment == "hip3" else "normal"
        context["entry_gate_mode"] = gate_mode
        if gate_mode and "gate_mode" not in entry_gate_obj:
            entry_gate_obj["gate_mode"] = gate_mode
        if entry_gate_obj:
            context["entry_gate"] = entry_gate_obj

        if strategy_segment == "hip3" or str(symbol or "").upper().startswith("XYZ:"):
            hip3_main = signals_snapshot.get("hip3_main")
            hip3_main = dict(hip3_main) if isinstance(hip3_main, dict) else {}
            hip3_components = hip3_main.get("components")
            hip3_components = dict(hip3_components) if isinstance(hip3_components, dict) else {}

            hip3_driver = _safe_text(context.get("hip3_driver"))
            if hip3_driver is None:
                hip3_driver = _safe_text(context_risk.get("hip3_driver"))
            if hip3_driver is None:
                hip3_driver = _safe_text(hip3_main.get("driver_type"))
            if hip3_driver:
                context["hip3_driver"] = hip3_driver.lower()

            flow_pass = _safe_bool(context.get("hip3_flow_pass"))
            if flow_pass is None:
                flow_pass = _safe_bool(hip3_main.get("flow_pass"))
            if flow_pass is not None:
                context["hip3_flow_pass"] = bool(flow_pass)

            ofm_pass = _safe_bool(context.get("hip3_ofm_pass"))
            if ofm_pass is None:
                ofm_pass = _safe_bool(hip3_main.get("ofm_pass"))
            if ofm_pass is not None:
                context["hip3_ofm_pass"] = bool(ofm_pass)

            booster_score = _safe_float(context.get("hip3_booster_score"))
            if booster_score is None:
                booster_score = _safe_float(hip3_components.get("rest_booster_score"))
            if booster_score is not None:
                context["hip3_booster_score"] = float(booster_score)

            booster_size_mult = _safe_float(context.get("hip3_booster_size_mult"))
            if booster_size_mult is None:
                booster_size_mult = _safe_float(context_risk.get("hip3_booster_size_mult"))
            if booster_size_mult is None:
                booster_size_mult = _safe_float(hip3_components.get("rest_booster_size_mult"))
            if booster_size_mult is not None:
                context["hip3_booster_size_mult"] = float(booster_size_mult)

        return context

    def _calculate_sl_tp(
        self,
        direction: str,
        entry_price: float,
        atr: Optional[float] = None,
        symbol: Optional[str] = None,
        volatility_regime: str = 'NORMAL',
        venue: Optional[str] = None,
        sl_mult_override: Optional[float] = None,
        tp_mult_override: Optional[float] = None,
    ) -> Tuple[float, float, Dict]:
        """Calculate SL and TP prices.

        Args:
            direction: LONG or SHORT
            entry_price: Entry price
            atr: ATR value (optional, uses fallback % if None)
            symbol: Trading symbol (for adaptive lookups)
            volatility_regime: LOW/NORMAL/HIGH (for adaptive lookups)
            venue: Exchange venue (reserved, currently unused)
            sl_mult_override: AGI-provided SL ATR multiplier override
            tp_mult_override: AGI-provided TP ATR multiplier override

        Returns:
            (sl_price, tp_price, metadata_dict)

        Fallback chain:
            1. config defaults
            2. adaptive_sltp (per-symbol+regime, most specific)
            3. AGI override as seed (if provided), optionally scaled by adaptive regime
        """
        # Start with config defaults
        sl_mult = self.config.sl_atr_multiplier
        tp_mult = self.config.tp_atr_multiplier
        adaptive_used = False
        agi_override_used = False
        adaptive_on_override = False

        adaptive_sl = None
        adaptive_tp = None
        if self.adaptive_sltp and symbol:
            try:
                adaptive_sl, adaptive_tp = self.adaptive_sltp.get_multipliers(symbol, volatility_regime)
            except Exception as e:
                self.log.warning(f"Adaptive SLTP query failed for {symbol}: {e}")
                adaptive_sl, adaptive_tp = None, None

        has_adaptive_data = (
            adaptive_sl is not None
            and adaptive_tp is not None
            and (
                adaptive_sl != self.config.sl_atr_multiplier
                or adaptive_tp != self.config.tp_atr_multiplier
            )
        )
        if has_adaptive_data:
            sl_mult = float(adaptive_sl)
            tp_mult = float(adaptive_tp)
            adaptive_used = True
            self.log.debug(
                f"Using adaptive SL/TP for {symbol}/{volatility_regime}: SL={sl_mult}, TP={tp_mult}"
            )

        # AGI override is a seed. If adaptive data exists and config allows it,
        # apply adaptive scale factors to preserve learned symbol/regime behavior.
        if sl_mult_override is not None and tp_mult_override is not None:
            agi_override_used = True
            sl_seed = float(sl_mult_override)
            tp_seed = float(tp_mult_override)
            if self.config.adaptive_on_override and has_adaptive_data:
                sl_scale = float(adaptive_sl) / float(self.config.sl_atr_multiplier or 1.0)
                tp_scale = float(adaptive_tp) / float(self.config.tp_atr_multiplier or 1.0)
                sl_mult = sl_seed * sl_scale
                tp_mult = tp_seed * tp_scale
                adaptive_on_override = True
                adaptive_used = True
                self.log.debug(
                    f"Using AGI override + adaptive scale for {symbol}: "
                    f"seed=({sl_seed:.4f},{tp_seed:.4f}) scale=({sl_scale:.4f},{tp_scale:.4f}) "
                    f"final=({sl_mult:.4f},{tp_mult:.4f})"
                )
            else:
                sl_mult = sl_seed
                tp_mult = tp_seed
                self.log.debug(
                    f"Using AGI override SL/TP for {symbol}: SL={sl_mult}, TP={tp_mult}"
                )

        used_fallback = False
        if atr is not None and atr > 0:
            sl_distance = atr * sl_mult
            tp_distance = atr * tp_mult
        else:
            # Fallback to fixed percentage
            used_fallback = True
            sl_distance = entry_price * (self.config.sl_fallback_pct / 100)
            tp_distance = sl_distance * (tp_mult / sl_mult)

        if direction == 'LONG':
            sl_price = entry_price - sl_distance
            tp_price = entry_price + tp_distance
        else:  # SHORT
            sl_price = entry_price + sl_distance
            tp_price = entry_price - tp_distance

        # Build metadata for learning
        metadata = {
            'atr': atr,
            'atr_pct': (atr / entry_price * 100) if (atr and entry_price > 0) else None,
            'volatility_regime': volatility_regime,
            'sl_mult': sl_mult,
            'tp_mult': tp_mult,
            'sl_distance': sl_distance,
            'tp_distance': tp_distance,
            'rr_ratio': round(tp_distance / sl_distance, 2) if sl_distance > 0 else None,
            'used_fallback': used_fallback,
            'sl_fallback_pct': self.config.sl_fallback_pct if used_fallback else None,
            'adaptive_used': adaptive_used,
            'agi_override_used': agi_override_used,
            'adaptive_on_override': adaptive_on_override,
            'adaptive_override_enabled': self.config.adaptive_on_override,
        }

        return sl_price, tp_price, metadata

    def _get_adapter_by_venue(self, venue: str) -> ExchangeAdapter:
        """Get adapter by explicit venue name. Raises ValueError for unknown venue."""
        venue = normalize_venue(venue)
        if venue == VENUE_HYPERLIQUID:
            return self.hyperliquid
        elif venue == VENUE_LIGHTER:
            return self.lighter
        else:
            raise ValueError(f"Unknown venue: {venue}")

    def _infer_venue_from_adapter(self, adapter: Optional[ExchangeAdapter]) -> str:
        """Infer canonical venue key from adapter instance."""
        if adapter is None:
            return ""
        if adapter is self.lighter:
            return VENUE_LIGHTER
        if adapter is self.hyperliquid:
            return VENUE_HYPERLIQUID
        if isinstance(adapter, LighterAdapter):
            return VENUE_LIGHTER
        if isinstance(adapter, HyperliquidAdapter):
            return VENUE_HYPERLIQUID
        return ""

    @staticmethod
    def _round_to_tick(price: float, tick_size: float) -> float:
        """Round *price* to the nearest tick."""
        if tick_size <= 0:
            return price
        return round(round(price / tick_size) * tick_size, 12)

    def _get_lighter_min_base_amount(
        self,
        adapter: ExchangeAdapter,
        symbol: str,
    ) -> Optional[float]:
        if not isinstance(adapter, LighterAdapter):
            return None
        try:
            min_size = adapter.get_min_base_amount(symbol)
        except Exception:
            return None
        if min_size is None:
            return None
        try:
            min_size = float(min_size)
        except (TypeError, ValueError):
            return None
        if min_size <= 0:
            return None
        return min_size

    def _mark_position_dust(
        self,
        key: Optional[str],
        position: Position,
        min_size: Optional[float],
        actual_size: float,
        reason: str,
    ) -> None:
        position.state = DUST_STATE
        position.sl_price = 0.0
        position.tp_price = 0.0
        if actual_size > 0:
            position.size = actual_size

        venue = getattr(position, 'venue', '') or 'unknown'
        if min_size is not None:
            self.log.warning(
                f"⚠️  DUST position on {venue}: {position.symbol} size={position.size:.6f} "
                f"< min {min_size:.6f}. {reason}"
            )
        else:
            self.log.warning(
                f"⚠️  DUST position on {venue}: {position.symbol} size={position.size:.6f}. {reason}"
            )

        payload = {
            'event': 'dust',
            'symbol': position.symbol,
            'size': position.size,
            'reason': reason,
            'venue': position.venue,
            'trade_id': position.trade_id,
        }
        if min_size is not None:
            payload['min_size'] = min_size
        self.persistence.append_trade(payload)

        if key and key not in self._positions:
            self._positions[key] = position

        self.persistence.save_positions_atomic(self._positions)
        if self.db and position.trade_id:
            self.db.update_trade_state(position.trade_id, DUST_STATE)

    async def _attempt_dust_close(
        self,
        adapter: ExchangeAdapter,
        position: Position,
        symbol: str,
        best_bid: float,
        best_ask: float,
        min_size: Optional[float],
        reason: str,
    ) -> bool:
        """Best-effort dust close when the venue supports it."""
        supports_close = True
        if isinstance(adapter, LighterAdapter) and min_size is not None and position.size < min_size:
            supports_close = False

        if not supports_close:
            self.log.critical(
                f"🚫 DUST UNMANAGEABLE on {position.venue}: {symbol} size={position.size:.6f} "
                f"< min {min_size:.6f}. Manual flatten required."
            )
            self.persistence.append_trade({
                'event': 'dust_unclosable',
                'symbol': symbol,
                'size': position.size,
                'min_size': min_size,
                'reason': reason,
                'venue': position.venue,
                'trade_id': position.trade_id,
                'action': 'manual_flatten_required',
            })
            if self.db and position.trade_id:
                self.db.update_trade_state(position.trade_id, DUST_STATE)
            return False

        try:
            close_ok = await self.close_position(
                symbol=symbol,
                reason="dust_close",
                best_bid=best_bid,
                best_ask=best_ask,
                venue=position.venue,
            )
        except Exception as exc:
            close_ok = False
            self.log.error(f"Dust close exception for {symbol} on {position.venue}: {exc}")

        if close_ok:
            self.persistence.append_trade({
                'event': 'dust_close',
                'symbol': symbol,
                'size': position.size,
                'reason': reason,
                'venue': position.venue,
                'trade_id': position.trade_id,
            })
            return True

        # If dust close fails (often due to fill mismatch / cancel churn), attempt an
        # automatic force-flatten of the residual position instead of punting to manual.
        force_ok = False
        try:
            force_ok = await self._force_flatten_residual(
                adapter=adapter,
                symbol=symbol,
                venue=position.venue,
                max_attempts=6,
            )
        except Exception as exc:
            self.log.error(f"Force-flatten exception for {symbol} on {position.venue}: {exc}")
            force_ok = False

        if force_ok:
            # Best-effort: mark the trade closed externally (PnL unknown; fill_reconciler
            # may still reconcile if enabled).
            trade_id = position.trade_id
            if self.db and trade_id is None:
                linked = self.db.find_open_trade(symbol, venue=position.venue)
                if linked:
                    trade_id = linked.id
                    position.trade_id = trade_id

            if self.db and trade_id:
                try:
                    self.db.mark_trade_closed_externally(
                        int(trade_id),
                        exit_reason="DUST_CLOSE",
                    )
                except Exception:
                    pass

            # Remove local position snapshot now that exchange is flat.
            try:
                key = self._position_key(symbol, position.venue)
                if key in self._positions:
                    del self._positions[key]
                self.persistence.save_positions_atomic(self._positions)
            except Exception:
                pass

            self.persistence.append_trade({
                'event': 'dust_close_forced',
                'symbol': symbol,
                'size': position.size,
                'reason': reason,
                'venue': position.venue,
                'trade_id': position.trade_id,
                'action': 'auto_flatten',
            })
            return True

        self.log.critical(
            f"🚫 DUST close failed on {position.venue} for {symbol}; manual flatten required."
        )
        self.persistence.append_trade({
            'event': 'dust_close_failed',
            'symbol': symbol,
            'size': position.size,
            'reason': reason,
            'venue': position.venue,
            'trade_id': position.trade_id,
            'action': 'manual_flatten_required',
        })
        return False

    async def _force_flatten_residual(
        self,
        adapter: ExchangeAdapter,
        symbol: str,
        venue: str,
        max_attempts: int = 6,
    ) -> bool:
        """Force-flatten a residual position using reduce-only limit orders.

        Used as an automatic recovery path when dust-close / verification fails and
        leaves an untracked residual position on the exchange.

        Returns True iff exchange position is FLAT at end.
        """
        # Always cancel first to clear stale chase orders / SLTP.
        for attempt in range(1, max_attempts + 1):
            try:
                if hasattr(adapter, "cancel_all_orders"):
                    await adapter.cancel_all_orders(symbol)
            except Exception:
                pass

            try:
                exch_pos = await adapter.get_position(symbol)
            except Exception:
                exch_pos = None

            if not exch_pos or (exch_pos.direction or '').upper() == 'FLAT' or float(getattr(exch_pos, 'size', 0.0) or 0.0) <= 0:
                return True

            # Determine closing side and an aggressive-but-limit price.
            try:
                bid, ask = await adapter.get_best_bid_ask(symbol)
            except Exception:
                bid, ask = None, None

            if not bid or not ask:
                await asyncio.sleep(0.25)
                continue

            close_side = 'buy' if (exch_pos.direction or '').upper() == 'SHORT' else 'sell'
            px = float(ask) if close_side == 'buy' else float(bid)
            qty = float(getattr(exch_pos, 'size', 0.0) or 0.0)
            if qty <= 0:
                return True

            if isinstance(adapter, LighterAdapter):
                await self.rate_limiter.acquire()

            self.log.warning(
                f"⚠️  Force-flatten attempt {attempt}/{max_attempts} for {symbol} on {venue}: "
                f"{exch_pos.direction} size={qty:.6f} -> {close_side.upper()} @ ${px:.6f} reduce-only"
            )

            ok = False
            oid = None
            try:
                ok, oid = await adapter.place_limit_order(
                    symbol=symbol,
                    side=close_side,
                    size=qty,
                    price=px,
                    reduce_only=True,
                    tif='Gtc',
                )
            except Exception:
                ok, oid = False, None

            if not ok or not oid:
                await asyncio.sleep(0.5)
                continue

            # Short settle wait; then cancel and retry if still not flat.
            await asyncio.sleep(1.0)

        # Final verification
        try:
            if hasattr(adapter, "cancel_all_orders"):
                await adapter.cancel_all_orders(symbol)
        except Exception:
            pass

        try:
            exch_pos = await adapter.get_position(symbol)
        except Exception:
            exch_pos = None

        return (not exch_pos) or (exch_pos.direction or '').upper() == 'FLAT' or float(getattr(exch_pos, 'size', 0.0) or 0.0) <= 0

    async def _place_stop_with_retries(
        self,
        adapter: ExchangeAdapter,
        symbol: str,
        side: str,
        size: float,
        trigger_price: float,
        order_type: str,
    ) -> Tuple[bool, Optional[str]]:
        """Place SL/TP with retries + best-effort verification.

        We retry because SL/TP placement is safety-critical.
        Verification uses adapter.check_order_status when possible.
        """
        last_err = ""
        cleaned_up = False  # Track if we already cleaned up stale orders
        
        for attempt in range(1, SLTP_MAX_RETRIES + 1):
            try:
                if isinstance(adapter, LighterAdapter):
                    await self.rate_limiter.acquire()

                # Hyperliquid TP should be a reduce-only LIMIT order (not trigger TP).
                # This makes TP appear as a normal limit order and matches boss preference.
                if isinstance(adapter, HyperliquidAdapter) and order_type == "tp":
                    ok, oid = await adapter.place_limit_order(
                        symbol=symbol,
                        side=side,
                        size=size,
                        price=trigger_price,
                        reduce_only=True,
                    )
                else:
                    ok, oid = await adapter.place_stop_order(
                        symbol=symbol,
                        side=side,
                        size=size,
                        trigger_price=trigger_price,
                        order_type=order_type,
                    )

                if ok and oid:
                    # For stop orders (SL/TP), skip immediate verification.
                    # They're resting orders that won't execute until triggered, and may not
                    # appear in active orders immediately due to API propagation delay.
                    # Verification was designed for Chase Limit fills, not stop orders.
                    # Trust the exchange API response for stop order placement.
                    return True, oid

                last_err = last_err or "place_stop_order returned failure"
            except Exception as e:
                last_err = str(e)

            # Check if error is due to maximum pending orders (Lighter specific)
            if 'maximum pending order' in last_err.lower() and not cleaned_up and isinstance(adapter, LighterAdapter):
                self.log.warning(
                    f"Detected 'maximum pending order count' error for {symbol}. "
                    f"Attempting to clean up stale orders..."
                )
                cleaned_up = True
                try:
                    # Get market and query pending orders
                    market = adapter._lookup_market(symbol)
                    if market:
                        auth = await adapter._auth_token()
                        if auth and adapter._order_api:
                            orders = await adapter._order_api.account_active_orders(
                                market_id=market.market_id,
                                account_index=adapter._account_index,
                                auth=auth,
                            )
                            
                            if orders.orders:
                                self.log.info(
                                    f"Found {len(orders.orders)} pending orders for {symbol}. "
                                    f"Canceling to free up order slots..."
                                )
                                cancel_count = 0
                                for order in orders.orders:
                                    order_id = f"{market.market_id}:{order.client_order_index}"
                                    success = await adapter.cancel_order(symbol, order_id)
                                    if success:
                                        cancel_count += 1
                                    await asyncio.sleep(0.2)  # Rate limit
                                
                                self.log.info(f"Canceled {cancel_count}/{len(orders.orders)} orders for {symbol}")
                                
                                # Brief pause before retrying placement
                                await asyncio.sleep(1.0)
                                continue  # Retry placement immediately without incrementing attempt
                except Exception as cleanup_err:
                    self.log.error(f"Order cleanup failed for {symbol}: {cleanup_err}")

            delay = min(
                SLTP_RETRY_BASE_DELAY_SECONDS * (2 ** (attempt - 1)),
                SLTP_RETRY_MAX_DELAY_SECONDS,
            )
            self.log.warning(
                f"{order_type.upper()} placement retry {attempt}/{SLTP_MAX_RETRIES} failed for {symbol} "
                f"(trigger=${trigger_price:.4f}): {last_err}. Sleeping {delay:.1f}s"
            )
            await asyncio.sleep(delay)

        self.log.error(f"{order_type.upper()} placement failed after {SLTP_MAX_RETRIES} retries for {symbol}: {last_err}")
        return False, None

    # ------------------------------------------------------------------
    # SR Limit Fill Handler (v0)
    # ------------------------------------------------------------------

    async def handle_sr_limit_fill(self, p, db_path: str = "") -> bool:
        """Handle a filled SR limit order: log trade + place SL/TP.

        CANONICAL location for trade entry handling.
        Reuses: _calculate_sl_tp(), _place_stop_with_retries(), db.log_trade_entry().

        Args:
            p: PendingLimit with filled_size, filled_price, venue, direction set.
            db_path: DB path for ATR service (optional).

        Returns:
            True if SL/TP placed successfully, False otherwise.
        """
        from atr_service import get_atr_service

        symbol = p.symbol
        direction = p.direction
        filled_size = p.filled_size
        filled_price = p.filled_price
        venue = p.venue

        # Get correct adapter based on venue
        try:
            adapter = self._get_adapter_by_venue(venue)
            exchange_name = venue
        except Exception:
            adapter = None
            exchange_name = venue

        if not adapter:
            self.log.error(f"[{symbol}] No adapter for venue {venue}")
            return False

        # =====================================================================
        # DUPLICATE TRADE / POSITION CHECK (MANDATORY)
        # =====================================================================
        existing_trade = self.db.find_open_trade_key(symbol, venue, direction)
        if existing_trade:
            self.log.warning(
                f"[{symbol}] SR limit fill: open trade already exists "
                f"(id={existing_trade.id}, {direction} @ {existing_trade.entry_price:.4f}). "
                f"Skipping duplicate trade creation."
            )
            return False

        # Also check exchange position
        try:
            exch_pos = await adapter.get_position(symbol)
            if exch_pos and abs(exch_pos.size) > 0:
                pos_dir = 'LONG' if exch_pos.size > 0 else 'SHORT'
                if pos_dir == direction:
                    self.log.warning(
                        f"[{symbol}] SR limit fill: exchange position already exists "
                        f"({pos_dir} size={abs(exch_pos.size):.4f}). Skipping duplicate."
                    )
                    return False
        except Exception as e:
            self.log.warning(f"[{symbol}] Position check failed: {e}")

        # =====================================================================
        # ATR RETRIEVAL (same pattern as live_agent.py)
        # =====================================================================
        atr = None
        try:
            atr_service = get_atr_service(db_path)
            atr_result = await atr_service.get_atr(symbol, price=filled_price)
            atr = atr_result.atr if atr_result else None
        except Exception as e:
            self.log.warning(f"[{symbol}] ATR fetch failed: {e}")

        # =====================================================================
        # SL/TP CALCULATION (reuse existing _calculate_sl_tp)
        # =====================================================================
        volatility_regime = 'NORMAL'
        if atr and filled_price > 0:
            atr_pct = (atr / filled_price) * 100
            if atr_pct < 0.5:
                volatility_regime = 'LOW'
            elif atr_pct > 2.0:
                volatility_regime = 'HIGH'

        sl_price, tp_price, sltp_metadata = self._calculate_sl_tp(
            direction=direction,
            entry_price=filled_price,
            atr=atr,
            symbol=symbol,
            volatility_regime=volatility_regime,
            venue=venue,
        )

        # =====================================================================
        # PLACE SL/TP (reuse existing _place_stop_with_retries)
        # =====================================================================
        sl_side = 'sell' if direction == 'LONG' else 'buy'

        # v0.2 HL: if SL was prefilled at SR placement time, reuse it.
        sl_ok = True
        sl_oid = getattr(p, 'sl_order_id', None)
        sl_price_prefill = getattr(p, 'sl_price', None)
        if sl_oid and sl_price_prefill:
            try:
                sl_price = float(sl_price_prefill)
            except Exception:
                sl_oid = None

        if not sl_oid:
            sl_ok, sl_oid = await self._place_stop_with_retries(
                adapter=adapter,
                symbol=symbol,
                side=sl_side,
                size=filled_size,
                trigger_price=sl_price,
                order_type='sl',
            )

        # Always place TP after fill (reduce-only TP limit on HL).
        tp_ok, tp_oid = await self._place_stop_with_retries(
            adapter=adapter,
            symbol=symbol,
            side=sl_side,  # Same side as SL (exit side)
            size=filled_size,
            trigger_price=tp_price,
            order_type='tp',
        )

        if not sl_ok:
            self.log.error(f"[{symbol}] SR limit fill: SL placement failed")
        if not tp_ok:
            self.log.error(f"[{symbol}] SR limit fill: TP placement failed")

        # =====================================================================
        # LOG TRADE ENTRY (reuse existing db.log_trade_entry)
        # =====================================================================
        state = 'ACTIVE' if (sl_ok and tp_ok) else ('SL_ONLY' if sl_ok else 'SLTP_FAILED')
        trade_id = None
        raw_signals_snapshot = getattr(p, "signals_snapshot", None)
        raw_signals_agreed = getattr(p, "signals_agreed", None)
        raw_context_snapshot = getattr(p, "context_snapshot", None)
        parsed_signals_snapshot: Optional[Dict[str, Any]] = None
        parsed_signals_agreed: List[str] = []
        parsed_context_snapshot: Dict[str, Any] = {}

        if raw_signals_snapshot:
            try:
                tmp = json.loads(raw_signals_snapshot) if isinstance(raw_signals_snapshot, str) else raw_signals_snapshot
                if isinstance(tmp, dict):
                    parsed_signals_snapshot = tmp
            except Exception as exc:
                self.log.warning(f"[{symbol}] Invalid pending-order signals_snapshot JSON; ignoring: {exc}")

        if raw_signals_agreed:
            try:
                tmp = json.loads(raw_signals_agreed) if isinstance(raw_signals_agreed, str) else raw_signals_agreed
                if isinstance(tmp, list):
                    parsed_signals_agreed = [str(x) for x in tmp if str(x).strip()]
            except Exception as exc:
                self.log.warning(f"[{symbol}] Invalid pending-order signals_agreed JSON; ignoring: {exc}")

        if raw_context_snapshot:
            try:
                tmp = json.loads(raw_context_snapshot) if isinstance(raw_context_snapshot, str) else raw_context_snapshot
                if isinstance(tmp, dict):
                    parsed_context_snapshot = tmp
            except Exception as exc:
                self.log.warning(f"[{symbol}] Invalid pending-order context_snapshot JSON; ignoring: {exc}")

        risk_pct_used = None
        equity_at_entry = None
        size_multiplier_used = 1.0
        try:
            pending_risk = parsed_context_snapshot.get("risk") if isinstance(parsed_context_snapshot, dict) else None
            if isinstance(pending_risk, dict):
                risk_pct_used = float(pending_risk.get("risk_pct_used")) if pending_risk.get("risk_pct_used") is not None else None
                equity_at_entry = float(pending_risk.get("equity_at_entry")) if pending_risk.get("equity_at_entry") is not None else None
                size_multiplier_used = float(
                    pending_risk.get("size_multiplier_effective", pending_risk.get("size_multiplier", 1.0))
                )
        except Exception:
            risk_pct_used = None
            equity_at_entry = None
            size_multiplier_used = 1.0

        confidence_value = None
        try:
            raw_confidence = getattr(p, "conviction", None)
            if raw_confidence is None and isinstance(parsed_context_snapshot, dict):
                raw_confidence = parsed_context_snapshot.get("conviction")
            if raw_confidence is not None:
                confidence_value = str(float(raw_confidence))
        except Exception:
            confidence_value = None

        sr_context_snapshot: Dict[str, Any] = {}
        if isinstance(parsed_context_snapshot, dict) and parsed_context_snapshot:
            sr_context_snapshot.update(parsed_context_snapshot)
        sr_context_snapshot["sr_level"] = p.sr_level
        sr_context_snapshot["order_type"] = "sr_limit"
        sr_context_snapshot["execution_route"] = "pending_sr_limit_fill"

        try:
            trade_id = self.db.log_trade_entry(
                symbol=symbol,
                direction=direction,
                entry_price=filled_price,
                size=filled_size,
                venue=venue,
                sl_price=sl_price if sl_ok else None,
                tp_price=tp_price if tp_ok else None,
                sl_order_id=str(sl_oid) if sl_oid else None,
                tp_order_id=str(tp_oid) if tp_oid else None,
                state=state,
                signals_snapshot=parsed_signals_snapshot,
                signals_agreed=parsed_signals_agreed,
                ai_reasoning=(getattr(p, 'reason', None) or 'SR limit fill'),
                confidence=confidence_value,
                size_multiplier=size_multiplier_used,
                context_snapshot=sr_context_snapshot,
                protection_snapshot=sltp_metadata,
                risk_pct_used=risk_pct_used,
                equity_at_entry=equity_at_entry,
                strategy=(
                    str(sr_context_snapshot.get('strategy'))
                    if isinstance(sr_context_snapshot, dict) and sr_context_snapshot.get('strategy')
                    else None
                ),
                pair_id=(
                    str(sr_context_snapshot.get('manual_plan_id') or sr_context_snapshot.get('pair_id'))
                    if isinstance(sr_context_snapshot, dict)
                    and (sr_context_snapshot.get('manual_plan_id') or sr_context_snapshot.get('pair_id'))
                    else None
                ),
            )
            self.log.info(f"[{symbol}] SR limit fill logged: trade_id={trade_id} @ {filled_price:.6g}")
        except Exception as e:
            self.log.error(f"[{symbol}] Failed to log SR limit trade: {e}")
            return False

        # --- Journal entry ---
        self.persistence.append_trade({
            'event': 'entry',
            'symbol': symbol,
            'direction': direction,
            'size': filled_size,
            'price': filled_price,
            'order_type': 'sr_limit',
            'exchange': exchange_name,
            'venue': venue,
            'sr_level': p.sr_level,
            'trade_id': trade_id,
        })

        return sl_ok and tp_ok

    # ------------------------------------------------------------------
    # Chase Limit Execution
    # ------------------------------------------------------------------

    async def _chase_limit_fill(
        self,
        adapter: ExchangeAdapter,
        symbol: str,
        side: str,
        size: float,
        reduce_only: bool = False,
        fallback_bid: float = 0.0,
        fallback_ask: float = 0.0,
    ) -> Tuple[bool, float, float]:
        """Execute an order using the Chase Limit algorithm.

        Places a GTC limit at ±N ticks from mid, monitors periodically,
        and re-places when price drifts ≥ M ticks from current limit.

        Args:
            adapter: Exchange adapter to use
            symbol: Trading symbol
            side: 'buy' or 'sell'
            size: Order size in base units
            reduce_only: Whether this is a closing order
            fallback_bid: Fallback bid price if adapter.get_mid_price() unavailable
            fallback_ask: Fallback ask price if adapter.get_mid_price() unavailable

        Returns:
            (success, filled_size, avg_fill_price)
        """
        cfg = self.config
        try:
            tick_size = await asyncio.wait_for(
                adapter.get_tick_size(symbol),
                timeout=ADAPTER_TIMEOUT_SEC,
            )
        except asyncio.TimeoutError:
            self.log.error(f"Chase Limit: timeout getting tick_size for {symbol}")
            return False, 0.0, 0.0
        if tick_size <= 0:
            self.log.error(f"Chase Limit: invalid tick_size {tick_size} for {symbol}")
            return False, 0.0, 0.0

        is_buy = side.lower() == 'buy'
        total_filled = 0.0
        weighted_fill = 0.0
        remaining = size
        start_time = time.monotonic()
        current_order_id: Optional[str] = None
        # Track fills on the CURRENT order_id (Lighter can report canceled orders with
        # remaining_size=0 even when only partially filled; do not infer fills from
        # remaining_size alone).
        current_order_size = float(remaining)
        current_order_filled = 0.0
        latest_fallback_bid = float(fallback_bid or 0.0)
        latest_fallback_ask = float(fallback_ask or 0.0)

        async def _fallback_mid(refresh_quotes: bool = True) -> float:
            """Best-effort fallback mid from freshest available bid/ask."""
            nonlocal latest_fallback_bid, latest_fallback_ask

            if refresh_quotes and hasattr(adapter, "get_best_bid_ask"):
                try:
                    bid, ask = await asyncio.wait_for(
                        adapter.get_best_bid_ask(symbol),
                        timeout=ADAPTER_TIMEOUT_SEC,
                    )
                    bid_f = float(bid or 0.0)
                    ask_f = float(ask or 0.0)
                    if bid_f > 0:
                        latest_fallback_bid = bid_f
                    if ask_f > 0:
                        latest_fallback_ask = ask_f
                except asyncio.TimeoutError:
                    self.log.error(f"Chase Limit: timeout get_best_bid_ask for {symbol}")
                except Exception:
                    pass

            if latest_fallback_bid > 0 and latest_fallback_ask > 0:
                return (latest_fallback_bid + latest_fallback_ask) / 2.0
            if latest_fallback_bid > 0:
                return latest_fallback_bid
            if latest_fallback_ask > 0:
                return latest_fallback_ask
            return 0.0

        async def _limit_price(mid: float, tif_norm: str) -> float:
            """Compute desired limit price.

            For post-only (Alo) on Hyperliquid, clamp the price so it will NOT
            immediately match (prevents `badAloPxRejected`).
            """
            offset = cfg.chase_tick_offset * tick_size

            # Pricing: entries are passive (buy below / sell above mid).
            # Exits are slightly aggressive to actually get out (buy above / sell below mid).
            if reduce_only:
                if is_buy:
                    px = self._round_to_tick(mid + offset, tick_size)
                else:
                    px = self._round_to_tick(mid - offset, tick_size)
            else:
                if is_buy:
                    px = self._round_to_tick(mid - offset, tick_size)
                else:
                    px = self._round_to_tick(mid + offset, tick_size)

            tif_norm = str(tif_norm or "Gtc").strip().title()

            if tif_norm == "Alo" and isinstance(adapter, HyperliquidAdapter):
                try:
                    bid, ask = await asyncio.wait_for(
                        adapter.get_best_bid_ask(symbol),
                        timeout=ADAPTER_TIMEOUT_SEC,
                    )
                except asyncio.TimeoutError:
                    self.log.error(f"Chase Limit: timeout get_best_bid_ask for {symbol}")
                    bid, ask = 0.0, 0.0
                except Exception:
                    bid, ask = 0.0, 0.0

                # Clamp conservatively so the order will NOT immediately match.
                # NOTE: there can be a race between fetching l2Book and order placement,
                # so we keep a small buffer (in ticks) to reduce `badAloPxRejected`.
                buffer_ticks = max(3, int(cfg.chase_tick_offset or 0))
                buffer = buffer_ticks * tick_size

                if is_buy:
                    if bid > 0:
                        target = bid - buffer
                    elif ask > 0:
                        target = ask - buffer
                    else:
                        target = 0.0

                    if target > 0:
                        px = min(px, self._round_to_tick(target, tick_size))

                else:
                    if ask > 0:
                        target = ask + buffer
                    elif bid > 0:
                        target = bid + buffer
                    else:
                        target = 0.0

                    if target > 0:
                        px = max(px, self._round_to_tick(target, tick_size))

            return px

        def _effective_tif(elapsed_seconds: float) -> str:
            """Return the TIF to use at the given elapsed time."""
            primary = cfg.chase_exit_tif if reduce_only else cfg.chase_entry_tif
            primary_norm = str(primary or "Gtc").strip().title()

            fb_after = float(cfg.chase_exit_fallback_after_seconds or 0.0) if reduce_only else float(cfg.chase_entry_fallback_after_seconds or 0.0)
            fb_tif = cfg.chase_exit_fallback_tif if reduce_only else cfg.chase_entry_fallback_tif
            fb_norm = str(fb_tif or "Gtc").strip().title()

            if primary_norm == "Alo" and fb_after > 0 and elapsed_seconds >= fb_after:
                return fb_norm
            return primary_norm

        async def _place_limit_with_retry(size_qty: float) -> Tuple[bool, Optional[str], float]:
            """Place limit order with retries for post-only immediate-match rejects."""

            for attempt in range(1, 6):
                elapsed = time.monotonic() - start_time
                tif_norm = _effective_tif(elapsed)

                try:
                    mid2 = await asyncio.wait_for(
                        adapter.get_mid_price(symbol),
                        timeout=ADAPTER_TIMEOUT_SEC,
                    )
                except asyncio.TimeoutError:
                    self.log.error(f"Chase Limit: timeout get_mid_price for {symbol}")
                    mid2 = 0.0
                if mid2 <= 0:
                    mid2 = await _fallback_mid(refresh_quotes=True)
                if mid2 <= 0:
                    return False, None, 0.0

                limit2 = await _limit_price(mid2, tif_norm)
                if limit2 <= 0:
                    return False, None, 0.0

                self.log.info(
                    f"Chase Limit: {side.upper()} {size_qty:.6f} {symbol} "
                    f"limit ${limit2:.6f} (mid ${mid2:.6f}, tick {tick_size})"
                )

                if isinstance(adapter, LighterAdapter):
                    await self.rate_limiter.acquire()

                try:
                    ok2, oid2 = await asyncio.wait_for(
                        adapter.place_limit_order(
                            symbol=symbol,
                            side=side,
                            size=size_qty,
                            price=limit2,
                            reduce_only=reduce_only,
                            tif=tif_norm,
                        ),
                        timeout=ADAPTER_TIMEOUT_SEC,
                    )
                except asyncio.TimeoutError:
                    self.log.error(f"Chase Limit: timeout placing limit order for {symbol}")
                    ok2, oid2 = False, None
                if ok2 and oid2:
                    return True, oid2, limit2

                # Special handling for Hyperliquid post-only immediate match rejection.
                if tif_norm == "Alo" and isinstance(adapter, HyperliquidAdapter):
                    err = str(getattr(adapter, "last_order_error", "") or "")
                    low = err.lower()
                    if ("post only" in low and "immediately matched" in low) or ("badalopxrejected" in low):
                        backoff = min(0.25 * attempt, 1.5)
                        self.log.warning(
                            f"Chase Limit: post-only rejected for {symbol} (attempt {attempt}/5); "
                            f"recomputing price and retrying in {backoff:.2f}s"
                        )
                        await asyncio.sleep(backoff)
                        continue

                return False, None, limit2

            return False, None, 0.0

        # --- Initial order ---
        ok, order_id, limit_px = await _place_limit_with_retry(remaining)
        if not ok or order_id is None:
            err_hint = str(getattr(adapter, "last_order_error", "") or "").strip()

            # Self-heal for reduce-only close mismatches:
            # - stale direction can submit wrong side ("reduce-only would increase position")
            # - stale size can submit invalid qty
            if reduce_only:
                err_low = err_hint.lower()
                needs_side_fix = ("reduce-only" in err_low and "increase" in err_low) or ("reduce only" in err_low and "increase" in err_low)
                needs_size_fix = "invalid size" in err_low
                needs_mapping_fix = ("asset" in err_low and "range" in err_low) or ("stale builder asset id" in err_low)

                # For any close failure with reduce-only, trust exchange truth for side/size.
                if needs_side_fix or needs_size_fix or needs_mapping_fix or (not err_hint):
                    try:
                        exch_pos = await adapter.get_position(symbol)
                    except Exception:
                        exch_pos = None

                    if exch_pos and (exch_pos.direction or "").upper() in ("LONG", "SHORT") and float(exch_pos.size or 0.0) > 0:
                        exch_dir = (exch_pos.direction or "").upper()
                        retry_side = "sell" if exch_dir == "LONG" else "buy"
                        retry_size = float(exch_pos.size)

                        # If exchange reports the same size but venue rejects as invalid,
                        # shave one quantum (based on observed decimal places) to avoid
                        # edge-case precision/min-increment rejects.
                        if needs_size_fix and abs(retry_size - float(size)) <= 0:
                            s_txt = f"{float(size):.8f}".rstrip("0").rstrip(".")
                            decs = len(s_txt.split(".", 1)[1]) if "." in s_txt else 0
                            step = 10 ** (-max(0, decs)) if decs > 0 else 1.0
                            retry_size = max(0.0, float(size) - step)

                        if retry_side != side or abs(retry_size - float(size)) > 0:
                            retry_tif = _effective_tif(time.monotonic() - start_time)
                            self.log.warning(
                                f"Chase Limit self-heal retry for {symbol}: side {side}->{retry_side}, size {float(size):.6f}->{retry_size:.6f}"
                            )
                            if isinstance(adapter, LighterAdapter):
                                await self.rate_limiter.acquire()
                            try:
                                ok, order_id = await asyncio.wait_for(
                                    adapter.place_limit_order(
                                        symbol=symbol,
                                        side=retry_side,
                                        size=retry_size,
                                        price=limit_px,
                                        reduce_only=True,
                                        tif=retry_tif,
                                    ),
                                    timeout=ADAPTER_TIMEOUT_SEC,
                                )
                                if ok and order_id:
                                    side = retry_side
                                    size = retry_size
                                    remaining = retry_size
                            except Exception:
                                ok, order_id = False, None
                            err_hint = str(getattr(adapter, "last_order_error", "") or err_hint).strip()

            if not ok or order_id is None:
                if err_hint:
                    self.log.error(f"Chase Limit: initial limit order failed for {symbol} | reason={err_hint}")
                else:
                    self.log.error(f"Chase Limit: initial limit order failed for {symbol}")
                return False, 0.0, 0.0

        current_order_id = order_id
        current_limit_px = limit_px
        current_order_size = float(remaining)
        current_order_filled = 0.0

        # --- Monitoring loop ---
        while True:
            elapsed = time.monotonic() - start_time

            # Timeout check
            if elapsed >= cfg.chase_timeout:
                self.log.warning(
                    f"Chase Limit: timeout ({cfg.chase_timeout}s) for {symbol}, "
                    f"filled {total_filled:.6f}/{size:.6f}"
                )
                # Cancel remaining order
                if current_order_id:
                    if isinstance(adapter, LighterAdapter):
                        await self.rate_limiter.acquire()
                    try:
                        await asyncio.wait_for(
                            adapter.cancel_order(symbol, current_order_id),
                            timeout=ADAPTER_TIMEOUT_SEC,
                        )
                    except asyncio.TimeoutError:
                        self.log.error(
                            f"Chase Limit: timeout cancel_order for {symbol} order {current_order_id}"
                        )
                # Accept partial fills if any
                if total_filled > 0:
                    avg_px = weighted_fill / total_filled
                    self.log.info(
                        f"Chase Limit: partial fill accepted {total_filled:.6f} @ ${avg_px:.4f}"
                    )
                    return True, total_filled, avg_px
                return False, 0.0, 0.0

            await asyncio.sleep(cfg.chase_poll_interval)

            # Check order status
            if isinstance(adapter, LighterAdapter):
                await self.rate_limiter.acquire()

            try:
                status = await asyncio.wait_for(
                    adapter.check_order_status(symbol, current_order_id),
                    timeout=ADAPTER_TIMEOUT_SEC,
                )
            except asyncio.TimeoutError:
                self.log.error(
                    f"Chase Limit: timeout check_order_status for {symbol} order {current_order_id}"
                )
                status = {}
            order_status = status.get('status', 'unknown')
            filled_total_for_order = float(status.get('filled_size', 0.0) or 0.0)
            avg_price = float(status.get('avg_price', 0.0) or 0.0)

            # ---- Accumulate incremental fills for the current order_id ----
            # Never infer fills from remaining_size alone on Lighter.
            filled_total_for_order = max(0.0, min(filled_total_for_order, current_order_size))
            newly_filled_on_this_order = max(0.0, filled_total_for_order - current_order_filled)
            if newly_filled_on_this_order > 0:
                fill_px = avg_price if avg_price > 0 else current_limit_px
                total_filled += newly_filled_on_this_order
                weighted_fill += newly_filled_on_this_order * fill_px
                current_order_filled = filled_total_for_order
                remaining = max(0.0, size - total_filled)
                self.log.info(
                    f"Chase Limit: partial fill +{newly_filled_on_this_order:.6f}, "
                    f"total {total_filled:.6f}, remaining {remaining:.6f}"
                )

            if order_status == 'filled' or remaining <= 0:
                avg_final = weighted_fill / total_filled if total_filled > 0 else current_limit_px
                self.log.info(
                    f"Chase Limit: FILLED {total_filled:.6f} {symbol} @ ${avg_final:.4f}"
                )
                return True, total_filled, avg_final

            if order_status == 'canceled':
                self.log.warning(f"Chase Limit: order unexpectedly canceled for {symbol}")
                if total_filled > 0:
                    avg_px = weighted_fill / total_filled
                    return True, total_filled, avg_px
                return False, 0.0, 0.0

            # --- Chase logic: check if price moved enough to re-place ---
            try:
                new_mid = await asyncio.wait_for(
                    adapter.get_mid_price(symbol),
                    timeout=ADAPTER_TIMEOUT_SEC,
                )
            except asyncio.TimeoutError:
                self.log.error(f"Chase Limit: timeout get_mid_price for {symbol}")
                new_mid = 0.0
            if new_mid <= 0:
                new_mid = await _fallback_mid(refresh_quotes=True)
                if new_mid <= 0:
                    continue  # Can't get price, retry next cycle

            ticks_from_limit = abs(new_mid - current_limit_px) / tick_size

            if ticks_from_limit >= cfg.chase_retrigger_ticks:
                # Only chase if the newly computed (tick-rounded) limit would actually change.
                proposed_limit = await _limit_price(new_mid, _effective_tif(elapsed))
                if abs(proposed_limit - current_limit_px) < (tick_size / 2):
                    continue

                self.log.info(
                    f"Chase Limit: price moved {ticks_from_limit:.1f} ticks "
                    f"(mid ${new_mid:.6f} vs limit ${current_limit_px:.6f}), chasing..."
                )

                # Cancel current order
                if isinstance(adapter, LighterAdapter):
                    await self.rate_limiter.acquire()

                try:
                    cancel_ok = await asyncio.wait_for(
                        adapter.cancel_order(symbol, current_order_id),
                        timeout=ADAPTER_TIMEOUT_SEC,
                    )
                except asyncio.TimeoutError:
                    self.log.error(
                        f"Chase Limit: timeout cancel_order for {symbol} order {current_order_id}"
                    )
                    cancel_ok = False

                if not cancel_ok:
                    # Cancel failed — order may have been filled already.
                    # Check order status to detect fill before continuing.
                    if isinstance(adapter, LighterAdapter):
                        await self.rate_limiter.acquire()

                    try:
                        fail_status = await asyncio.wait_for(
                            adapter.check_order_status(symbol, current_order_id),
                            timeout=ADAPTER_TIMEOUT_SEC,
                        )
                    except asyncio.TimeoutError:
                        self.log.error(
                            f"Chase Limit: timeout check_order_status for {symbol} order {current_order_id}"
                        )
                        fail_status = {}
                    fail_order_status = fail_status.get('status', 'unknown')
                    fail_avg = fail_status.get('avg_price', 0.0)
                    fail_filled_total = float(fail_status.get('filled_size', 0.0) or 0.0)

                    if fail_order_status == 'filled':
                        # Order was filled before we could cancel.
                        # Use reported cumulative fill delta for THIS order_id only.
                        # Fallback to remaining-on-order if the venue omits filled_size.
                        fail_filled_total = max(0.0, min(fail_filled_total, current_order_size))
                        fill_delta = max(0.0, fail_filled_total - current_order_filled)
                        if fill_delta <= 0:
                            fill_delta = max(0.0, current_order_size - current_order_filled)
                        if fill_delta > 0:
                            fill_px = fail_avg if fail_avg > 0 else current_limit_px
                            total_filled += fill_delta
                            weighted_fill += fill_delta * fill_px
                            current_order_filled += fill_delta
                        avg_final = weighted_fill / total_filled if total_filled > 0 else current_limit_px
                        self.log.info(
                            f"Chase Limit: FILLED (detected after cancel fail) "
                            f"{total_filled:.6f} {symbol} @ ${avg_final:.4f}"
                        )
                        return True, total_filled, avg_final

                    if fail_order_status == 'canceled':
                        self.log.warning(
                            f"Chase Limit: order canceled externally for {symbol}"
                        )
                        if total_filled > 0:
                            avg_px = weighted_fill / total_filled
                            return True, total_filled, avg_px
                        return False, 0.0, 0.0

                    # Order still active or unknown — continue monitoring
                    self.log.warning(
                        f"Chase Limit: cancel failed for {symbol} "
                        f"(status={fail_order_status}), continuing to monitor"
                    )
                    continue

                # Race condition protection: verify cancel has *settled* before we place a replacement.
                # We have observed exact 2x / 100% mismatches when a "canceled" order later fills
                # while a replacement is also live. To reduce that, we:
                #   1) poll order status a few times after cancel
                #   2) accumulate any late fills
                #   3) only proceed to replacement when status is stable and not open/resting
                post_status = 'unknown'
                post_avg = 0.0
                stable_cancel_polls = 0
                for _ in range(4):
                    await asyncio.sleep(0.25)
                    if isinstance(adapter, LighterAdapter):
                        await self.rate_limiter.acquire()
                    try:
                        post_cancel_status = await asyncio.wait_for(
                            adapter.check_order_status(symbol, current_order_id),
                            timeout=ADAPTER_TIMEOUT_SEC,
                        )
                    except asyncio.TimeoutError:
                        self.log.error(
                            f"Chase Limit: timeout check_order_status for {symbol} order {current_order_id}"
                        )
                        post_cancel_status = {}

                    post_status = str(post_cancel_status.get('status', 'unknown') or 'unknown')
                    post_filled_total = float(post_cancel_status.get('filled_size', 0.0) or 0.0)
                    post_avg = float(post_cancel_status.get('avg_price', 0.0) or 0.0)

                    # Accumulate any fills that occurred on the cancelled order using filled_size.
                    post_filled_total = max(0.0, min(post_filled_total, current_order_size))
                    final_delta = max(0.0, post_filled_total - current_order_filled)
                    if final_delta > 0:
                        fill_px = post_avg if post_avg > 0 else current_limit_px
                        total_filled += final_delta
                        weighted_fill += final_delta * fill_px
                        current_order_filled = post_filled_total
                        remaining = max(0.0, size - total_filled)
                        self.log.info(
                            f"Chase Limit: fill during cancel +{final_delta:.6f}, "
                            f"total {total_filled:.6f}, remaining {remaining:.6f}"
                        )

                    if post_status in ('open', 'resting', 'partially_filled'):
                        stable_cancel_polls = 0
                        continue

                    if post_status == 'filled' or remaining <= 0:
                        avg_final = weighted_fill / total_filled if total_filled > 0 else current_limit_px
                        self.log.info(
                            f"Chase Limit: FILLED during cancel {total_filled:.6f} @ ${avg_final:.4f}"
                        )
                        return True, total_filled, avg_final

                    if post_status in ('canceled', 'cancelled'):
                        stable_cancel_polls += 1
                        if stable_cancel_polls >= 2:
                            break

                # Critical safety: do NOT place a replacement order unless the cancel is actually effective.
                if post_status in ('open', 'resting', 'partially_filled'):
                    self.log.warning(
                        f"Chase Limit: cancel not settled (status={post_status}) for {symbol}; continuing to monitor (no replacement)"
                    )
                    continue

                # Place replacement order for remaining qty (remaining on THIS order)
                remaining_on_order = max(0.0, current_order_size - current_order_filled)
                if remaining_on_order > 0:
                    new_limit = proposed_limit
                    self.log.info(
                        f"Chase Limit: re-placing {remaining_on_order:.6f} @ ${new_limit:.6f} "
                        f"(new mid ${new_mid:.6f})"
                    )

                    if isinstance(adapter, LighterAdapter):
                        await self.rate_limiter.acquire()

                    placed = False
                    for attempt in range(1, 6):
                        elapsed = time.monotonic() - start_time
                        tif_norm = _effective_tif(elapsed)
                        if isinstance(adapter, LighterAdapter):
                            await self.rate_limiter.acquire()

                        try:
                            ok, new_order_id = await asyncio.wait_for(
                                adapter.place_limit_order(
                                    symbol=symbol,
                                    side=side,
                                    size=remaining_on_order,
                                    price=new_limit,
                                    reduce_only=reduce_only,
                                    tif=tif_norm,
                                ),
                                timeout=ADAPTER_TIMEOUT_SEC,
                            )
                        except asyncio.TimeoutError:
                            self.log.error(
                                f"Chase Limit: timeout placing limit order for {symbol}"
                            )
                            ok, new_order_id = False, None
                        if ok and new_order_id:
                            current_order_id = new_order_id
                            current_limit_px = new_limit
                            current_order_size = float(remaining_on_order)
                            current_order_filled = 0.0
                            placed = True
                            break

                        # Post-only reject: recompute limit and retry.
                        if tif_norm == "Alo" and isinstance(adapter, HyperliquidAdapter):
                            err = str(getattr(adapter, "last_order_error", "") or "")
                            low = err.lower()
                            if ("post only" in low and "immediately matched" in low) or ("badalopxrejected" in low):
                                backoff = min(0.25 * attempt, 1.5)
                                self.log.warning(
                                    f"Chase Limit: post-only replacement rejected for {symbol} (attempt {attempt}/5); "
                                    f"retrying in {backoff:.2f}s"
                                )
                                await asyncio.sleep(backoff)
                                # Refresh mid and recompute the limit more conservatively.
                                try:
                                    new_mid2 = await asyncio.wait_for(
                                        adapter.get_mid_price(symbol),
                                        timeout=ADAPTER_TIMEOUT_SEC,
                                    )
                                except asyncio.TimeoutError:
                                    self.log.error(
                                        f"Chase Limit: timeout get_mid_price for {symbol}"
                                    )
                                    new_mid2 = 0.0
                                except Exception:
                                    new_mid2 = 0.0
                                if new_mid2 > 0:
                                    new_limit = await _limit_price(new_mid2, tif_norm)
                                continue

                        break

                    if not placed:
                        self.log.error(
                            f"Chase Limit: replacement order failed for {symbol}"
                        )
                        if total_filled > 0:
                            avg_px = weighted_fill / total_filled
                            return True, total_filled, avg_px
                        return False, 0.0, 0.0

    async def execute(
        self,
        decision: ExecutionDecision,
        best_bid: float = 0.0,
        best_ask: float = 0.0,
        atr: Optional[float] = None,
        venue: Optional[str] = None,
    ) -> ExecutionResult:
        """Execute a trading decision.

        Args:
            decision: ExecutionDecision from trading pipeline
            best_bid: Current best bid price
            best_ask: Current best ask price
            atr: ATR for SL/TP calculation (optional)
            venue: Explicit venue (required for execution-by-call)

        Returns:
            ExecutionResult with success/failure details
        """
        symbol = decision.symbol
        direction = decision.direction
        if venue is not None:
            venue = self._coerce_venue_for_symbol(symbol, venue)

        self.log.info(f"Executing {direction} for {symbol} on venue={venue}")

        # Validate decision
        if not decision.should_trade:
            return ExecutionResult(
                success=False,
                symbol=symbol,
                error="Decision should_trade is False",
            )

        # Get adapter — explicit venue or fallback to router
        if venue is not None:
            try:
                self.router.validate(venue, symbol)
                adapter = self._get_adapter_by_venue(venue)
            except (ExchangeRoutingError, ValueError) as exc:
                return ExecutionResult(
                    success=False,
                    symbol=symbol,
                    error=str(exc),
                )
        else:
            adapter = self._get_adapter(symbol)

        if adapter is None:
            return ExecutionResult(
                success=False,
                symbol=symbol,
                error=f"No adapter available for {symbol}",
            )

        exchange_name = getattr(adapter, "name", adapter.__class__.__name__)
        if venue in VALID_VENUES:
            venue_name = venue
        else:
            venue_name = self._infer_venue_from_adapter(adapter) or VENUE_HYPERLIQUID

        # Calculate position size
        if decision.size_usd is None:
            return ExecutionResult(
                success=False,
                symbol=symbol,
                error="Missing size_usd in decision",
            )
        try:
            requested_size_usd = float(decision.size_usd)
        except Exception:
            return ExecutionResult(
                success=False,
                symbol=symbol,
                error=f"Invalid size_usd value: {decision.size_usd!r}",
            )
        if requested_size_usd <= 0:
            return ExecutionResult(
                success=False,
                symbol=symbol,
                error=f"Invalid size_usd: {requested_size_usd}",
            )

        size_usd = self._calculate_position_size(decision)
        if size_usd < 0:
            return ExecutionResult(
                success=False,
                symbol=symbol,
                error="size_usd below min_position_notional_usd",
            )
        if size_usd == 0:
            return ExecutionResult(
                success=False,
                symbol=symbol,
                error="Position size is zero (exposure limit)",
            )

        # Estimate size in base units
        price = best_ask if direction == 'LONG' else best_bid
        if price <= 0:
            return ExecutionResult(
                success=False,
                symbol=symbol,
                error="Invalid price (bid/ask is 0)",
            )
        size = size_usd / price

        # =====================================================================
        # PRE-TRADE SYMBOL VALIDATION (fixes phantom trade bug - cycle 23579 ADA)
        # =====================================================================
        # Validate symbol is tradeable BEFORE attempting execution
        try:
            mid_price = await asyncio.wait_for(
                adapter.get_mid_price(symbol),
                timeout=ADAPTER_TIMEOUT_SEC,
            )
        except asyncio.TimeoutError:
            self.log.error(
                f"❌ SYMBOL VALIDATION TIMEOUT: get_mid_price timeout for {symbol} on {exchange_name}"
            )
            mid_price = 0.0
        except Exception as e:
            self.log.error(
                f"❌ SYMBOL VALIDATION ERROR: get_mid_price failed for {symbol} on {exchange_name}: {e}"
            )
            mid_price = 0.0
        if mid_price <= 0:
            # Dry-run adapters may not implement market data; fall back to the provided bid/ask.
            if self.config.dry_run and price > 0:
                mid_price = float(price)
            else:
                self.log.error(
                    f"❌ SYMBOL VALIDATION FAILED: {symbol} mid price is ${mid_price:.4f} "
                    f"(symbol may not exist on {exchange_name} or has zero liquidity). "
                    f"Rejecting trade to prevent phantom journal entry."
                )
                return ExecutionResult(
                    success=False,
                    symbol=symbol,
                    error=f"Invalid symbol or zero liquidity (mid=${mid_price:.4f})",
                    exchange=exchange_name,
                )
        self.log.debug(f"✅ Symbol validation passed: {symbol} mid=${mid_price:.4f}")

        # =====================================================================
        # DUPLICATE POSITION CHECK (fixes phantom trade bug - cycle 23340)
        # =====================================================================
        key = self._position_key(symbol, venue_name)
        existing = self._positions.get(key)
        if existing and existing.direction != 'FLAT':
            # Position already exists
            if existing.direction == direction:
                # Same direction: attempting to pyramid/average
                self.log.warning(
                    f"🚫 DUPLICATE POSITION REJECTED: {symbol} {direction} already exists "
                    f"(size={existing.size:.4f} @ ${existing.entry_price:.4f}). "
                    f"Position averaging not supported - SKIPPING new trade."
                )
                return ExecutionResult(
                    success=False,
                    symbol=symbol,
                    error=f"Position already exists ({direction} {existing.size:.4f}); "
                          f"averaging/pyramiding not supported",
                )
            else:
                # Opposite direction: signal flip (close existing, then enter new)
                self.log.info(
                    f"🔄 POSITION FLIP DETECTED: {symbol} {existing.direction} → {direction}. "
                    f"Closing existing position first..."
                )
                close_success = await self.close_position(
                    symbol=symbol,
                    reason="signal_flip",
                    best_bid=best_bid,
                    best_ask=best_ask,
                    venue=venue_name,
                )
                if not close_success:
                    return ExecutionResult(
                        success=False,
                        symbol=symbol,
                        error=f"Failed to close existing {existing.direction} position before flip",
                    )
                self.log.info(f"✅ Existing {symbol} {existing.direction} closed. Proceeding with {direction} entry.")

        # Create NEW position entry (only after duplicate check passed)
        position = Position(
            symbol=symbol,
            direction=direction,
            size=0,  # Will update after fill
            entry_price=0,  # Will update after fill
            state="ENTERING",
            opened_at=datetime.now(timezone.utc).isoformat(),
            signals_agreeing=decision.signals_agreeing,  # Phase 4: For learning
            venue=venue_name,
        )
        self._positions[key] = position

        # Execute via Chase Limit algorithm (replaces market orders)
        side = 'buy' if direction == 'LONG' else 'sell'
        success, filled_size, filled_price = await self._chase_limit_fill(
            adapter=adapter,
            symbol=symbol,
            side=side,
            size=size,
            reduce_only=False,
            fallback_bid=best_bid,
            fallback_ask=best_ask,
        )

        if not success:
            # Cleanup failed entry
            del self._positions[key]
            self.persistence.save_positions_atomic(self._positions)
            return ExecutionResult(
                success=False,
                symbol=symbol,
                error="Chase-limit entry failed",
                exchange=exchange_name,
            )

        # Update position with fill (reported by chase limit)
        position.size = filled_size
        position.entry_price = filled_price

        # =====================================================================
        # POSITION STATE VALIDATION (safety check after fill)
        # =====================================================================
        if key not in self._positions:
            self.log.critical(
                f"🚨 CRITICAL: Position {symbol} disappeared from tracking after fill! "
                f"This should NEVER happen. Proceeding with SL/TP placement anyway."
            )
            # Re-add position to tracking (emergency recovery)
            self._positions[key] = position

        if self._positions[key].size != filled_size:
            self.log.warning(
                f"⚠️  Position size mismatch for {symbol}: "
                f"tracked={self._positions[key].size:.4f}, fill={filled_size:.4f}. "
                f"Using fill size for SL/TP placement."
            )
            self._positions[key].size = filled_size
            self._positions[key].entry_price = filled_price

        reported_fill_size = filled_size
        reported_fill_price = filled_price
        verification_status = "skipped"
        verification_error = None

        # =====================================================================
        # POST-EXECUTION POSITION VERIFICATION (fixes phantom trade bug - cycle 23579 ADA)
        # =====================================================================
        # VERIFY position actually exists on exchange after "fill"
        if not getattr(adapter, "dry_run", False):
            await asyncio.sleep(0.5)  # Brief delay for exchange sync
            try:
                actual_position = await adapter.get_position(symbol)

                if not actual_position or actual_position.size == 0:
                    self.log.critical(
                        f"🚨 PHANTOM TRADE DETECTED: {symbol} logged as filled "
                        f"(size={reported_fill_size:.4f} @ ${reported_fill_price:.4f}) "
                        f"but NO position exists on {exchange_name}! Removing from tracking and logging correction."
                    )

                    # Remove phantom position from tracking
                    if key in self._positions:
                        del self._positions[key]

                    # Log correction to journal
                    self.persistence.append_trade({
                        'event': 'phantom_detected',
                        'symbol': symbol,
                        'reason': 'Position verification failed - no position on exchange',
                        'reported_fill_price': reported_fill_price,
                        'reported_fill_size': reported_fill_size,
                        'exchange': exchange_name,
                    })

                    self.persistence.save_positions_atomic(self._positions)

                    return ExecutionResult(
                        success=False,
                        symbol=symbol,
                        error="Position verification failed (phantom trade)",
                        exchange=exchange_name,
                    )

                verification_status = "verified"

                # Sync size/entry to exchange source of truth (fixes size mismatches)
                actual_size = actual_position.size
                actual_entry = actual_position.entry_price or filled_price
                size_diff_pct = (
                    abs(actual_size - filled_size) / filled_size * 100
                    if filled_size > 0 else 0
                )

                if size_diff_pct > 5.0:
                    self.log.warning(
                        f"⚠️  Position size mismatch for {symbol}: "
                        f"chase_limit reported {filled_size:.4f}, exchange shows {actual_size:.4f} "
                        f"({size_diff_pct:.1f}% difference). Using exchange value as source of truth."
                    )
                elif size_diff_pct > 0.0:
                    self.log.debug(
                        f"Position size sync for {symbol}: "
                        f"chase_limit {filled_size:.6f} -> exchange {actual_size:.6f}"
                    )

                if actual_size > 0:
                    filled_size = actual_size
                    filled_price = actual_entry
                    position.size = actual_size
                    position.entry_price = actual_entry
            except Exception as e:
                verification_status = "error"
                verification_error = str(e)
                self.log.error(
                    f"⚠️  Failed to verify position {symbol} on exchange: {e}. "
                    f"Proceeding with caution - position may not exist!"
                )
        else:
            verification_status = "dry_run"

        # -----------------------------------------------------------------
        # Entry underfill guard (Chase Limit can accept partial fills).
        #
        # If we only get a tiny partial fill vs requested entry size, do NOT treat
        # the trade as a normal EXECUTED entry. Instead, journal + log it as
        # UNDERFILLED and immediately unwind (reduce-only chase-limit close).
        # This prevents mirror legs from drifting and avoids “$4k proposal → $200 fill”.
        # -----------------------------------------------------------------
        requested_entry_size = float(size or 0.0)
        min_fill_ratio = float(getattr(self.config, 'min_entry_fill_ratio', 0.0) or 0.0)
        min_fill_notional = float(getattr(self.config, 'min_entry_fill_notional_usd', 0.0) or 0.0)
        if requested_entry_size > 0 and filled_size > 0 and filled_price > 0 and min_fill_ratio > 0:
            fill_ratio = filled_size / requested_entry_size
            requested_notional = requested_entry_size * float(filled_price)
            actual_notional = float(filled_size) * float(filled_price)

            if fill_ratio < min_fill_ratio and requested_notional >= min_fill_notional:
                underfill_reason = (
                    f"ENTRY_UNDERFILLED: fill_ratio={fill_ratio:.3f} "
                    f"filled_notional=${actual_notional:.2f} requested_notional≈${requested_notional:.2f}"
                )
                self.log.critical(f"[{symbol}] {underfill_reason}")

                # Persist the underfilled position (so we can unwind cleanly).
                position.state = 'UNDERFILLED'
                self._positions[key] = position
                try:
                    self.persistence.save_positions_atomic(self._positions)
                except Exception:
                    pass

                # Log a minimal trade record (no SL/TP) so fill_reconciler can
                # reconcile the subsequent unwind.
                if self.db:
                    try:
                        underfilled_context = self._build_learning_context_snapshot(
                            decision=decision,
                            symbol=symbol,
                            direction=direction,
                            venue=venue_name,
                            order_type="chase_limit",
                            execution_route="executor_chase_underfilled",
                        )
                        risk_pct_used, equity_at_entry, size_multiplier_used = self._extract_sizing_tracking_from_decision(
                            decision,
                            venue=venue_name,
                        )
                        trade_id = self.db.log_trade_entry(
                            symbol=symbol,
                            direction=direction,
                            entry_price=filled_price,
                            size=filled_size,
                            venue=venue_name,
                            sl_price=None,
                            tp_price=None,
                            state='UNDERFILLED',
                            signals_snapshot=decision.signals_snapshot,
                            signals_agreed=list(decision.signals_agreeing or []),
                            ai_reasoning=str(decision.reason or ''),
                            confidence=str(decision.conviction) if decision.conviction is not None else None,
                            size_multiplier=size_multiplier_used,
                            context_snapshot=underfilled_context,
                            protection_snapshot=None,
                            risk_pct_used=risk_pct_used,
                            equity_at_entry=equity_at_entry,
                            strategy=(
                                str(decision.context_snapshot.get('strategy'))
                                if isinstance(decision.context_snapshot, dict) and decision.context_snapshot.get('strategy')
                                else None
                            ),
                            pair_id=(
                                str(decision.context_snapshot.get('manual_plan_id') or decision.context_snapshot.get('pair_id'))
                                if isinstance(decision.context_snapshot, dict)
                                and (decision.context_snapshot.get('manual_plan_id') or decision.context_snapshot.get('pair_id'))
                                else None
                            ),
                        )
                        position.trade_id = trade_id
                        self._positions[key].trade_id = trade_id
                    except Exception as e:
                        self.log.error(f"[{symbol}] Failed to log UNDERFILLED trade entry: {e}")

                self.persistence.append_trade({
                    'event': 'entry_underfilled',
                    'symbol': symbol,
                    'direction': direction,
                    'requested_size': requested_entry_size,
                    'filled_size': filled_size,
                    'price': filled_price,
                    'fill_ratio': fill_ratio,
                    'requested_notional_usd': requested_notional,
                    'actual_notional_usd': actual_notional,
                    'venue': venue_name,
                    'exchange': exchange_name,
                    'trade_id': position.trade_id,
                    'reason': underfill_reason,
                })

                # Attempt immediate unwind (no need to “pair”; just fail + move on).
                bb, ba = float(best_bid or 0.0), float(best_ask or 0.0)
                if (bb <= 0 or ba <= 0) and hasattr(adapter, 'get_best_bid_ask'):
                    try:
                        bb2, ba2 = await adapter.get_best_bid_ask(symbol)
                        if bb2 and ba2:
                            bb, ba = float(bb2), float(ba2)
                    except Exception:
                        pass

                unwind_ok = False
                if bb > 0 and ba > 0:
                    try:
                        unwind_ok = await self.close_position(
                            symbol=symbol,
                            reason='entry_underfill',
                            best_bid=bb,
                            best_ask=ba,
                            venue=venue_name,
                        )
                    except Exception:
                        unwind_ok = False

                self.persistence.append_trade({
                    'event': 'entry_underfilled_unwind',
                    'symbol': symbol,
                    'venue': venue_name,
                    'trade_id': position.trade_id,
                    'ok': bool(unwind_ok),
                })

                return ExecutionResult(
                    success=False,
                    symbol=symbol,
                    direction=direction,
                    entry_price=filled_price,
                    size=filled_size,
                    sl_price=0.0,
                    tp_price=0.0,
                    exchange=exchange_name,
                    error=underfill_reason,
                )

        sltp_size = filled_size
        dust_min_size = self._get_lighter_min_base_amount(adapter, symbol)
        if isinstance(adapter, LighterAdapter):
            normalized_size, min_base = adapter.normalize_base_size(symbol, filled_size, round_down=True)
            if min_base is not None:
                dust_min_size = min_base
            if normalized_size > 0:
                sltp_size = normalized_size
                if abs(sltp_size - filled_size) > 0:
                    self.log.debug(
                        f"Lighter size normalized for SL/TP: {filled_size:.6f} -> {sltp_size:.6f} {symbol}"
                    )

        is_dust = False
        dust_reason = ""
        dust_mismatch_pct = None
        effective_size = sltp_size if sltp_size > 0 else filled_size
        if reported_fill_size > 0:
            dust_mismatch_pct = abs(effective_size - reported_fill_size) / reported_fill_size * 100

        if isinstance(adapter, LighterAdapter) and sltp_size <= 0:
            is_dust = True
            dust_reason = "size rounded to zero"
        if dust_min_size is not None and effective_size > 0 and effective_size < dust_min_size:
            is_dust = True
            dust_reason = "size below min base amount"
        if dust_mismatch_pct is not None and dust_mismatch_pct >= DUST_MISMATCH_PCT:
            is_dust = True
            dust_reason = f"extreme fill mismatch ({dust_mismatch_pct:.1f}%)"

        # Calculate volatility regime from ATR
        volatility_regime = 'NORMAL'
        if atr is not None and filled_price > 0:
            atr_pct = (atr / filled_price) * 100
            if atr_pct < 0.5:
                volatility_regime = 'LOW'
            elif atr_pct > 2.0:
                volatility_regime = 'HIGH'

        # Calculate SL/TP BEFORE setting state and saving (crash safety)
        sl_price = 0.0
        tp_price = 0.0
        if self.config.enable_sltp_backstop and not is_dust:
            use_override = (
                decision.sl_price is not None
                and decision.tp_price is not None
                and decision.sl_price > 0
                and decision.tp_price > 0
            )
            if use_override:
                sl_price = float(decision.sl_price)
                tp_price = float(decision.tp_price)
                if direction == "LONG" and not (sl_price < filled_price < tp_price):
                    self.log.warning(
                        f"Override SL/TP invalid for LONG {symbol} (entry={filled_price:.4f}, "
                        f"sl={sl_price:.4f}, tp={tp_price:.4f}); using ATR fallback"
                    )
                    use_override = False
                elif direction == "SHORT" and not (sl_price > filled_price > tp_price):
                    self.log.warning(
                        f"Override SL/TP invalid for SHORT {symbol} (entry={filled_price:.4f}, "
                        f"sl={sl_price:.4f}, tp={tp_price:.4f}); using ATR fallback"
                    )
                    use_override = False

            if not use_override:
                # Extract AGI multiplier overrides from decision.risk
                agi_sl_mult = None
                agi_tp_mult = None
                if decision.risk and isinstance(decision.risk, dict):
                    agi_sl_mult = decision.risk.get('sl_atr_mult')
                    agi_tp_mult = decision.risk.get('tp_atr_mult')

                # Calculate SL/TP with adaptive params (or AGI override)
                sl_price, tp_price, sltp_metadata = self._calculate_sl_tp(
                    direction, filled_price, atr,
                    symbol=symbol, volatility_regime=volatility_regime,
                    venue=venue_name,
                    sl_mult_override=agi_sl_mult,
                    tp_mult_override=agi_tp_mult,
                )
            else:
                # Build metadata for override case
                sltp_metadata = {
                    'atr': atr,
                    'atr_pct': (atr / filled_price * 100) if (atr and filled_price > 0) else None,
                    'volatility_regime': volatility_regime,
                    'sl_mult': None,  # Override - no multiplier used
                    'tp_mult': None,
                    'sl_distance': abs(filled_price - sl_price),
                    'tp_distance': abs(tp_price - filled_price),
                    'rr_ratio': round(abs(tp_price - filled_price) / abs(filled_price - sl_price), 2) if abs(filled_price - sl_price) > 0 else None,
                    'used_fallback': False,
                    'used_override': True,
                    'adaptive_used': False,
                }
            position.sl_price = sl_price
            position.tp_price = tp_price
        else:
            position.sl_price = 0.0
            position.tp_price = 0.0
            sltp_metadata = None

        # NOW set state and save (with valid SL/TP targets)
        if is_dust:
            entry_state = DUST_STATE
        else:
            entry_state = "PLACING_SLTP" if self.config.enable_sltp_backstop else "ACTIVE"
        position.state = entry_state

        # Log entry to SQLite (source of truth) after verification
        if self.db:
            # Ensure winners teach too: if signals_agreeing is empty but we have a
            # signals_snapshot, derive a minimal signals_agreed list from the
            # snapshot (signals whose direction matches the trade direction).
            signals_agreed = list(decision.signals_agreeing or [])
            if not signals_agreed and isinstance(decision.signals_snapshot, dict) and decision.signals_snapshot:
                try:
                    norm_map = {"BULLISH": "LONG", "BEARISH": "SHORT", "LONG": "LONG", "SHORT": "SHORT"}
                    for sig, data in decision.signals_snapshot.items():
                        if not isinstance(data, dict):
                            continue
                        raw_dir = str(data.get("direction") or data.get("signal") or "").upper()
                        if norm_map.get(raw_dir, "") == str(direction).upper():
                            signals_agreed.append(str(sig))
                    # Fallback: keep at least the keys, so we know what existed.
                    if not signals_agreed:
                        signals_agreed = [str(k) for k in decision.signals_snapshot.keys()]
                except Exception:
                    pass

            try:
                chase_context = self._build_learning_context_snapshot(
                    decision=decision,
                    symbol=symbol,
                    direction=direction,
                    venue=venue_name,
                    order_type="chase_limit",
                    execution_route="executor_chase",
                )
                risk_pct_used, equity_at_entry, size_multiplier_used = self._extract_sizing_tracking_from_decision(
                    decision,
                    venue=venue_name,
                )
                trade_id = self.db.log_trade_entry(
                    symbol=symbol,
                    direction=direction,
                    entry_price=filled_price,
                    size=filled_size,
                    venue=venue_name,
                    sl_price=sl_price if self.config.enable_sltp_backstop else None,
                    tp_price=tp_price if self.config.enable_sltp_backstop else None,
                    state=entry_state,
                    signals_snapshot=decision.signals_snapshot,
                    signals_agreed=signals_agreed,
                    ai_reasoning=decision.reason,
                    confidence=str(decision.conviction) if decision.conviction is not None else None,
                    size_multiplier=size_multiplier_used,
                    context_snapshot=chase_context,
                    protection_snapshot=sltp_metadata,
                    risk_pct_used=risk_pct_used,
                    equity_at_entry=equity_at_entry,
                    strategy=(
                        str(decision.context_snapshot.get('strategy'))
                        if isinstance(decision.context_snapshot, dict) and decision.context_snapshot.get('strategy')
                        else None
                    ),
                    pair_id=(
                        str(decision.context_snapshot.get('manual_plan_id') or decision.context_snapshot.get('pair_id'))
                        if isinstance(decision.context_snapshot, dict)
                        and (decision.context_snapshot.get('manual_plan_id') or decision.context_snapshot.get('pair_id'))
                        else None
                    ),
                )
                position.trade_id = trade_id
                self._positions[key].trade_id = trade_id
            except Exception as e:
                self.log.error(f"Failed to log trade entry to DB for {symbol}: {e}")

        # Log entry to journal AFTER verification attempt
        entry_payload = {
            'event': 'entry',
            'symbol': symbol,
            'direction': direction,
            'size': filled_size,
            'price': filled_price,
            'order_type': 'chase_limit',
            'exchange': exchange_name,
            'signals_agreeing': decision.signals_agreeing,  # Phase 4: For learning
            'verification_status': verification_status,
            'verified': verification_status == "verified",
            'venue': venue_name,
            'trade_id': position.trade_id,
        }
        if is_dust:
            entry_payload['dust'] = True
            entry_payload['dust_min_size'] = dust_min_size
            if dust_reason:
                entry_payload['dust_reason'] = dust_reason
            if dust_mismatch_pct is not None:
                entry_payload['dust_mismatch_pct'] = dust_mismatch_pct
        if verification_error:
            entry_payload['verification_error'] = verification_error
        self.persistence.append_trade(entry_payload)
        self.persistence.save_positions_atomic(self._positions)

        if is_dust:
            reason = dust_reason or "dust detected"
            self._mark_position_dust(
                key=key,
                position=position,
                min_size=dust_min_size,
                actual_size=filled_size,
                reason=reason,
            )
            dust_close_ok = False
            dust_close_attempted = False
            if not self.config.dry_run:
                # Ensure we have prices for the dust-close attempt.
                bb, ba = best_bid, best_ask
                if (bb <= 0 or ba <= 0) and getattr(adapter, '_initialized', False):
                    try:
                        bb2, ba2 = await adapter.get_best_bid_ask(symbol)
                        if bb2 and ba2:
                            bb, ba = float(bb2), float(ba2)
                    except Exception:
                        pass

                dust_close_attempted = True
                dust_close_ok = await self._attempt_dust_close(
                    adapter=adapter,
                    position=position,
                    symbol=symbol,
                    best_bid=bb,
                    best_ask=ba,
                    min_size=dust_min_size,
                    reason=reason,
                )
            error_tag = "DUST_UNCLOSABLE" if dust_close_attempted and not dust_close_ok else "DUST"
            return ExecutionResult(
                success=False,
                symbol=symbol,
                direction=direction,
                entry_price=filled_price,
                size=filled_size,
                sl_price=0.0,
                tp_price=0.0,
                exchange=exchange_name,
                error=f"{error_tag}: {reason}",
            )

        # =====================================================================
        # SL/TP PROTECTION VALIDATION
        # =====================================================================
        if sl_price <= 0.0 or tp_price <= 0.0:
            self.log.critical(
                f"🚨 SAFETY ALERT: {symbol} position has SL={sl_price:.4f} or TP={tp_price:.4f} "
                f"(backstop={'enabled' if self.config.enable_sltp_backstop else 'DISABLED'}). "
                f"Position will be {'WITHOUT PROTECTION' if not self.config.enable_sltp_backstop else 'unprotected until placement completes'}!"
            )

        if not self.config.enable_sltp_backstop:
            position.state = "ACTIVE"
            self.persistence.save_positions_atomic(self._positions)
            if self.db and position.trade_id:
                self.db.update_trade_state(position.trade_id, "ACTIVE")
            self.log.warning(
                f"⚠️  SL/TP backstop DISABLED; position {symbol} active WITHOUT protection orders"
            )
            return ExecutionResult(
                success=True,
                symbol=symbol,
                direction=direction,
                entry_price=filled_price,
                size=filled_size,
                sl_price=0.0,
                tp_price=0.0,
                exchange=exchange_name,
            )

        sl_side = 'sell' if direction == 'LONG' else 'buy'

        # Place SL (retry + verify). SL is mandatory for backstop mode.
        sl_success, sl_order_id = await self._place_stop_with_retries(
            adapter=adapter,
            symbol=symbol,
            side=sl_side,
            size=sltp_size,
            trigger_price=sl_price,
            order_type='sl',
        )

        if not sl_success or not sl_order_id:
            # Per boss instruction: do NOT auto-close. We retry a small number of times,
            # then return an error so the operator (me/the brain) can handle it.
            position.state = "SLTP_FAILED"
            self.persistence.save_positions_atomic(self._positions)
            if self.db and position.trade_id:
                self.db.update_trade_state(position.trade_id, "SLTP_FAILED")
                try:
                    self.db.open_sltp_incident(
                        trade_id=position.trade_id,
                        symbol=symbol,
                        venue=venue_name,
                        leg="SL",
                        attempts=SLTP_MAX_RETRIES,
                        last_error="sl_placement_failed_after_retries",
                    )
                except Exception:
                    pass
            self.log.critical(
                f"SL placement failed after {SLTP_MAX_RETRIES} retries for {symbol}; "
                f"POSITION IS OPEN — manual intervention required"
            )
            return ExecutionResult(
                success=False,
                symbol=symbol,
                direction=direction,
                entry_price=filled_price,
                size=filled_size,
                sl_price=sl_price,
                tp_price=tp_price,
                exchange=exchange_name,
                error="SL placement failed after retries; POSITION IS OPEN (manual fix required)",
            )

        position.sl_order_id = sl_order_id
        self.persistence.save_positions_atomic(self._positions)
        self.log.info(f"SL placed at ${sl_price:.2f}")
        if self.db and position.trade_id:
            self.db.update_trade_sltp(
                trade_id=position.trade_id,
                sl_price=sl_price,
                sl_order_id=sl_order_id,
            )
            try:
                self.db.resolve_sltp_incident(
                    trade_id=position.trade_id,
                    leg="SL",
                    resolution_note="sl_order_placed",
                )
            except Exception:
                pass

        # Wait before TP (rate limit throttle)
        await asyncio.sleep(self.config.sltp_delay_seconds)

        # Place TP (retry + verify). TP is also treated as mandatory for backstop mode.
        tp_success, tp_order_id = await self._place_stop_with_retries(
            adapter=adapter,
            symbol=symbol,
            side=sl_side,  # Same side as SL (exit side)
            size=sltp_size,
            trigger_price=tp_price,
            order_type='tp',
        )

        if not tp_success or not tp_order_id:
            # Per boss instruction: do NOT auto-close. SL exists; TP is missing.
            position.state = "SL_ONLY"
            self.persistence.save_positions_atomic(self._positions)
            if self.db and position.trade_id:
                self.db.update_trade_state(position.trade_id, "SL_ONLY")
                try:
                    self.db.open_sltp_incident(
                        trade_id=position.trade_id,
                        symbol=symbol,
                        venue=venue_name,
                        leg="TP",
                        attempts=SLTP_MAX_RETRIES,
                        last_error="tp_placement_failed_after_retries",
                    )
                except Exception:
                    pass
            self.log.critical(
                f"TP placement failed after {SLTP_MAX_RETRIES} retries for {symbol}; "
                f"POSITION IS OPEN WITH SL ONLY — manual intervention required"
            )
            return ExecutionResult(
                success=False,
                symbol=symbol,
                direction=direction,
                entry_price=filled_price,
                size=filled_size,
                sl_price=sl_price,
                tp_price=tp_price,
                exchange=exchange_name,
                error="TP placement failed after retries; POSITION OPEN (SL only; manual TP/management required)",
            )

        position.tp_order_id = tp_order_id
        self.log.info(f"TP placed at ${tp_price:.2f}")
        if self.db and position.trade_id:
            self.db.update_trade_sltp(
                trade_id=position.trade_id,
                tp_price=tp_price,
                tp_order_id=tp_order_id,
            )
            try:
                self.db.resolve_sltp_incident(
                    trade_id=position.trade_id,
                    leg="TP",
                    resolution_note="tp_order_placed",
                )
            except Exception:
                pass

        # Update state
        position.state = "ACTIVE"
        self.persistence.save_positions_atomic(self._positions)
        if self.db and position.trade_id:
            self.db.update_trade_state(position.trade_id, "ACTIVE")

        return ExecutionResult(
            success=True,
            symbol=symbol,
            direction=direction,
            entry_price=filled_price,
            size=filled_size,
            sl_price=sl_price,
            tp_price=tp_price,
            exchange=exchange_name,
        )

    async def close_position(
        self,
        symbol: str,
        reason: str = "manual",
        best_bid: float = 0.0,
        best_ask: float = 0.0,
        venue: Optional[str] = None,
    ) -> bool:
        """Close an open position.

        Args:
            symbol: Symbol to close
            reason: Reason for closing (manual, sl, tp, signal_flip)
            best_bid: Current best bid
            best_ask: Current best ask
            venue: Explicit venue (required for execution-by-call)

        Returns:
            True if closed successfully
        """
        if venue is not None:
            venue = self._coerce_venue_for_symbol(symbol, venue)
        key, position = self._resolve_position(symbol, venue)
        if position is None or position.direction == 'FLAT':
            self.log.warning(f"No open position for {symbol}")
            return False

        # Get adapter — explicit venue, then tracked position venue, else router fallback.
        if venue is not None:
            try:
                self.router.validate(venue, symbol)
                adapter = self._get_adapter_by_venue(venue)
            except (ExchangeRoutingError, ValueError):
                self.log.error(f"Invalid venue {venue} for {symbol}")
                return False
        elif position.venue:
            try:
                pos_venue = self._coerce_venue_for_symbol(symbol, position.venue) or position.venue
                if pos_venue != position.venue:
                    position.venue = pos_venue
                adapter = self._get_adapter_by_venue(pos_venue)
            except ValueError:
                adapter = self._get_adapter(symbol)
        else:
            adapter = self._get_adapter(symbol)

        if adapter is None:
            return False

        # Cross-process per-symbol guard: prevents overlapping close/flip/entry.
        lock_venue = (
            normalize_venue(venue or position.venue or '')
            or self._infer_venue_from_adapter(adapter)
            or VENUE_HYPERLIQUID
        )
        got_lock = True
        if self.db:
            got_lock = self.db.acquire_symbol_lock(
                symbol=symbol,
                venue=lock_venue,
                owner=self._lock_owner,
                lock_type='CLOSE',
                reason=str(reason or ''),
                ttl_seconds=180.0,
            )
        if not got_lock:
            self.log.warning(f"SYMBOL_BUSY: {symbol} on {lock_venue} (close blocked; another action in-flight)")
            return False

        dust_min_size = self._get_lighter_min_base_amount(adapter, symbol)
        if dust_min_size is not None and position.size > 0 and position.size < dust_min_size:
            self._mark_position_dust(
                key=key,
                position=position,
                min_size=dust_min_size,
                actual_size=position.size,
                reason="close skipped (size below min base amount)",
            )
            try:
                if self.db:
                    self.db.release_symbol_lock(symbol, lock_venue, owner=self._lock_owner)
            except Exception:
                pass
            return True

        position.state = "EXITING"

        # Hardening: cancel-first on close/flip.
        # We cancel *all* open orders for this symbol (including stale chase orders)
        # before placing the reduce-only close.
        try:
            if CANCEL_FIRST_CLOSE and hasattr(adapter, "cancel_all_orders"):
                await adapter.cancel_all_orders(symbol)
        except Exception:
            pass

        # Persist close intent so fill_reconciler can apply the correct exit_reason later.
        try:
            if self.db:
                tid = position.trade_id
                if tid is None:
                    linked = self.db.find_open_trade(symbol, venue=position.venue)
                    if linked:
                        tid = linked.id
                        position.trade_id = tid
                if tid:
                    self.db.set_pending_exit(int(tid), reason=str(reason or ""), detail="")
        except Exception:
            pass

        def _clear_pending_exit_intent() -> None:
            try:
                if self.db and position.trade_id:
                    self.db.clear_pending_exit(int(position.trade_id))
            except Exception:
                pass

        # -----------------------------------------------------------------
        # Source of truth before close: exchange position direction/size.
        # Tracking can be stale (e.g. signal flips + reconciliation lag), and a
        # reduce-only order in the wrong direction will be rejected.
        # -----------------------------------------------------------------
        try:
            exch_pos = await adapter.get_position(symbol)
        except Exception:
            exch_pos = None

        if exch_pos and (exch_pos.direction or '').upper() != 'FLAT' and float(exch_pos.size or 0) > 0:
            exch_dir = (exch_pos.direction or '').upper()
            if exch_dir in ('LONG', 'SHORT') and exch_dir != position.direction:
                self.log.warning(
                    f"⚠️  Close preflight: tracked direction {position.direction} != exchange {exch_dir} "
                    f"for {symbol}. Using exchange direction/size for reduce-only close."
                )
                position.direction = exch_dir
                position.size = float(exch_pos.size)
                if float(exch_pos.entry_price or 0) > 0:
                    position.entry_price = float(exch_pos.entry_price)

        # Close via Chase Limit algorithm (replaces market orders)
        requested_close_size = float(position.size or 0.0)
        side = 'sell' if position.direction == 'LONG' else 'buy'
        try:
            success, filled_size, filled_price = await self._chase_limit_fill(
                adapter=adapter,
                symbol=symbol,
                side=side,
                size=position.size,
                reduce_only=True,
                fallback_bid=best_bid,
                fallback_ask=best_ask,
            )
        except Exception as e:
            self.log.error(f"Close execution failed for {symbol}: {e}")
            position.state = "ACTIVE"
            self.persistence.save_positions_atomic(self._positions)
            try:
                if self.db and position.trade_id:
                    self.db.update_trade_state(position.trade_id, "ACTIVE")
            except Exception:
                pass
            _clear_pending_exit_intent()
            try:
                if self.db:
                    self.db.release_symbol_lock(symbol, lock_venue, owner=self._lock_owner)
            except Exception:
                pass
            return False

        if success:
            # -----------------------------------------------------------------
            # VERIFY CLOSE AGAINST EXCHANGE (critical)
            # -----------------------------------------------------------------
            # Some venues/APIs can transiently report an order as "filled" even when
            # it was only partially filled (e.g. remaining size becomes 0 on cancel).
            # Never mark a position closed unless the exchange position is FLAT.
            try:
                await asyncio.sleep(0.5)
                exch_pos = await adapter.get_position(symbol)
            except Exception:
                exch_pos = None

            if exch_pos and exch_pos.direction != 'FLAT' and exch_pos.size and exch_pos.size > 0:
                # Treat as still-open unless it's effectively dust.
                residual_notional = exch_pos.size * (
                    exch_pos.entry_price or filled_price or position.entry_price or 0.0
                )
                is_effective_dust = residual_notional > 0 and residual_notional < self.config.dust_notional_usd

                if not is_effective_dust:
                    # For dust-close flows, don't spam SL/TP restoration. The caller will
                    # force-flatten or otherwise handle the residual.
                    if str(reason) == 'dust_close':
                        self.log.critical(
                            f"🚨 Close verification failed for {symbol} during dust_close: exchange still shows "
                            f"{exch_pos.direction} size={exch_pos.size:.6f} (notional≈${residual_notional:.2f})."
                        )
                        try:
                            if self.db:
                                self.db.release_symbol_lock(symbol, lock_venue, owner=self._lock_owner)
                        except Exception:
                            pass
                        _clear_pending_exit_intent()
                        return False

                    self.log.critical(
                        f"🚨 Close verification failed for {symbol}: exchange still shows "
                        f"{exch_pos.direction} size={exch_pos.size:.6f} (notional≈${residual_notional:.2f}). "
                        f"NOT marking closed; attempting to restore SL/TP."
                    )

                    # Update local position to exchange truth and restore protection.
                    position.direction = exch_pos.direction
                    position.size = float(exch_pos.size)
                    if float(exch_pos.entry_price or 0) > 0:
                        position.entry_price = float(exch_pos.entry_price)
                    position.unrealized_pnl = float(exch_pos.unrealized_pnl or 0.0)
                    position.state = "ACTIVE"
                    position.sl_order_id = None
                    position.tp_order_id = None
                    self.persistence.save_positions_atomic(self._positions)

                    # Keep DB trade record consistent with exchange truth (size/order_ids)
                    try:
                        if self.db and position.trade_id:
                            notional = float(position.size or 0.0) * float(position.entry_price or 0.0)
                            self.db.update_trade_size_entry(
                                trade_id=int(position.trade_id),
                                new_size=float(position.size or 0.0),
                                new_entry=float(position.entry_price or 0.0),
                                notional_usd=float(notional or 0.0),
                            )
                            # Clear + then restore SL/TP order ids (we overwrite any stale ids).
                            self.db.set_trade_sltp(
                                trade_id=int(position.trade_id),
                                sl_price=float(position.sl_price or 0.0) if (position.sl_price or 0.0) > 0 else None,
                                tp_price=float(position.tp_price or 0.0) if (position.tp_price or 0.0) > 0 else None,
                                sl_order_id=None,
                                tp_order_id=None,
                            )
                    except Exception:
                        pass

                    sltp_side = 'sell' if position.direction == 'LONG' else 'buy'

                    # Best-effort restore SL/TP using stored prices.
                    if position.sl_price and position.sl_price > 0:
                        if isinstance(adapter, LighterAdapter):
                            await self.rate_limiter.acquire()
                        sl_ok, sl_oid = await self._place_stop_with_retries(
                            adapter=adapter,
                            symbol=symbol,
                            side=sltp_side,
                            size=position.size,
                            trigger_price=position.sl_price,
                            order_type='sl',
                        )
                        if sl_ok and sl_oid:
                            position.sl_order_id = sl_oid
                            try:
                                if self.db and position.trade_id:
                                    self.db.set_trade_sltp(
                                        trade_id=int(position.trade_id),
                                        sl_order_id=str(sl_oid),
                                    )
                            except Exception:
                                pass

                    await asyncio.sleep(self.config.sltp_delay_seconds)

                    if position.tp_price and position.tp_price > 0:
                        if isinstance(adapter, LighterAdapter):
                            await self.rate_limiter.acquire()
                        tp_ok, tp_oid = await self._place_stop_with_retries(
                            adapter=adapter,
                            symbol=symbol,
                            side=sltp_side,
                            size=position.size,
                            trigger_price=position.tp_price,
                            order_type='tp',
                        )
                        if tp_ok and tp_oid:
                            position.tp_order_id = tp_oid
                            try:
                                if self.db and position.trade_id:
                                    self.db.set_trade_sltp(
                                        trade_id=int(position.trade_id),
                                        tp_order_id=str(tp_oid),
                                    )
                            except Exception:
                                pass

                    self.persistence.save_positions_atomic(self._positions)
                    try:
                        if self.db:
                            self.db.release_symbol_lock(symbol, lock_venue, owner=self._lock_owner)
                    except Exception:
                        pass
                    _clear_pending_exit_intent()
                    return False

            # Cancel any remaining SL/TP now that the exchange position is confirmed closed.
            # (Best-effort: ignore errors like already-cancelled/filled.)
            if position.sl_order_id:
                try:
                    if isinstance(adapter, LighterAdapter):
                        await self.rate_limiter.acquire()
                    await adapter.cancel_order(symbol, position.sl_order_id)
                except Exception:
                    pass
            if position.tp_order_id:
                try:
                    if isinstance(adapter, LighterAdapter):
                        await self.rate_limiter.acquire()
                    await adapter.cancel_order(symbol, position.tp_order_id)
                except Exception:
                    pass

            effective_close_size = requested_close_size if requested_close_size > 0 else float(filled_size or 0.0)
            if effective_close_size <= 0:
                effective_close_size = float(filled_size or 0.0)
            if (
                effective_close_size > 0
                and float(filled_size or 0.0) > 0
                and abs(float(filled_size) - effective_close_size) / effective_close_size > 0.02
            ):
                self.log.warning(
                    f"[{symbol}] Close fill size mismatch: reported={filled_size:.6f} "
                    f"requested={effective_close_size:.6f}. Using requested size for PnL."
                )

            # Calculate PnL
            if position.direction == 'LONG':
                pnl = (filled_price - position.entry_price) * effective_close_size
            else:
                pnl = (position.entry_price - filled_price) * effective_close_size

            # Log exit to journal
            self.persistence.append_trade({
                'event': 'exit',
                'symbol': symbol,
                'direction': position.direction,
                'size': effective_close_size,
                'reported_fill_size': filled_size,
                'entry_price': position.entry_price,
                'exit_price': filled_price,
                'pnl': pnl,
                'reason': reason,
                'venue': position.venue,
                'trade_id': position.trade_id,
            })

            # Update circuit breaker (skip if fill_reconciler owns PnL)
            if self.on_pnl and not self.config.use_fill_reconciler:
                self.on_pnl(pnl)

            # Log exit to SQLite (unless fill_reconciler will do fee-accurate close)
            trade_id = position.trade_id
            if self.db and trade_id is None:
                linked = self.db.find_open_trade(symbol, venue=position.venue)
                if linked:
                    trade_id = linked.id
                    position.trade_id = trade_id

            # When fill_reconciler is enabled, prefer fee-accurate close via fills.
            # Only force-close in DB for emergency rollback paths.
            force_db_close = (
                self.config.use_fill_reconciler
                and str(reason).upper() in {"PARTIAL_EXECUTION_ROLLBACK"}
            )
            if self.db and trade_id and (not self.config.use_fill_reconciler or force_db_close):
                self.db.log_trade_exit(
                    trade_id=trade_id,
                    exit_price=filled_price,
                    exit_reason=reason,
                    total_fees=0.0,
                )
                try:
                    self.db.update_trade_state(trade_id, "CLOSED")
                except Exception:
                    pass

            # Phase 4: Notify learning engine (skip if fill_reconciler owns close)
            if self.on_trade_close and not self.config.use_fill_reconciler:
                self.on_trade_close({
                    'trade_id': trade_id,
                    'symbol': symbol,
                    'direction': position.direction,
                    'pnl': pnl,
                    'signals_agreeing': position.signals_agreeing,
                    'entry_time': position.opened_at,
                    'exit_time': datetime.now(timezone.utc).isoformat(),
                    'exit_reason': reason,
                })

            # Remove position
            position.state = "COMPLETED"
            if key:
                del self._positions[key]
            self.persistence.save_positions_atomic(self._positions)

            self.log.info(f"Closed {symbol}: PnL ${pnl:.2f}")
            try:
                if self.db:
                    self.db.release_symbol_lock(symbol, lock_venue, owner=self._lock_owner)
            except Exception:
                pass
            return True
        else:
            # Last-gasp verification: reduce-only closes can be reported as failures
            # if the position flips to FLAT between cancel/re-place attempts.
            try:
                exch_pos2 = await adapter.get_position(symbol)
            except Exception:
                exch_pos2 = None

            if not exch_pos2 or (exch_pos2.direction or '').upper() == 'FLAT' or float(getattr(exch_pos2, 'size', 0.0) or 0.0) <= 0:
                self.log.warning(
                    f"Close reported failure for {symbol}, but exchange is FLAT. Treating as closed (fill_reconciler will reconcile)."
                )
                fallback_exit_price = float(filled_price or 0.0)
                if fallback_exit_price <= 0:
                    if position.direction == 'LONG':
                        fallback_exit_price = float(best_bid or 0.0)
                    else:
                        fallback_exit_price = float(best_ask or 0.0)
                if fallback_exit_price <= 0:
                    fallback_exit_price = float(position.entry_price or 0.0)

                effective_close_size = requested_close_size if requested_close_size > 0 else float(filled_size or 0.0)
                if effective_close_size <= 0:
                    effective_close_size = float(position.size or 0.0)

                if position.direction == 'LONG':
                    pnl = (fallback_exit_price - position.entry_price) * effective_close_size
                else:
                    pnl = (position.entry_price - fallback_exit_price) * effective_close_size

                self.persistence.append_trade({
                    'event': 'exit',
                    'symbol': symbol,
                    'direction': position.direction,
                    'size': effective_close_size,
                    'reported_fill_size': filled_size,
                    'entry_price': position.entry_price,
                    'exit_price': fallback_exit_price,
                    'pnl': pnl,
                    'reason': reason,
                    'venue': position.venue,
                    'trade_id': position.trade_id,
                    'verification_flat_after_failed_close': True,
                })

                if self.on_pnl and not self.config.use_fill_reconciler:
                    self.on_pnl(pnl)

                trade_id = position.trade_id
                if self.db and trade_id is None:
                    linked = self.db.find_open_trade(symbol, venue=position.venue)
                    if linked:
                        trade_id = linked.id
                        position.trade_id = trade_id

                force_db_close = (
                    self.config.use_fill_reconciler
                    and str(reason).upper() in {"PARTIAL_EXECUTION_ROLLBACK"}
                )
                if self.db and trade_id and (not self.config.use_fill_reconciler or force_db_close):
                    self.db.log_trade_exit(
                        trade_id=trade_id,
                        exit_price=fallback_exit_price,
                        exit_reason=reason,
                        total_fees=0.0,
                    )
                    try:
                        self.db.update_trade_state(trade_id, "CLOSED")
                    except Exception:
                        pass

                if self.on_trade_close and not self.config.use_fill_reconciler:
                    self.on_trade_close({
                        'trade_id': trade_id,
                        'symbol': symbol,
                        'direction': position.direction,
                        'pnl': pnl,
                        'signals_agreeing': position.signals_agreeing,
                        'entry_time': position.opened_at,
                        'exit_time': datetime.now(timezone.utc).isoformat(),
                        'exit_reason': reason,
                    })

                position.state = "COMPLETED"
                if key:
                    del self._positions[key]
                self.persistence.save_positions_atomic(self._positions)
                try:
                    if self.db:
                        self.db.release_symbol_lock(symbol, lock_venue, owner=self._lock_owner)
                except Exception:
                    pass
                return True

            if exch_pos2 and (exch_pos2.direction or '').upper() in ('LONG', 'SHORT') and float(getattr(exch_pos2, 'size', 0.0) or 0.0) > 0:
                position.direction = (exch_pos2.direction or position.direction).upper()
                position.size = float(getattr(exch_pos2, 'size', position.size) or position.size)
                if float(getattr(exch_pos2, 'entry_price', 0.0) or 0.0) > 0:
                    position.entry_price = float(exch_pos2.entry_price)
                position.unrealized_pnl = float(getattr(exch_pos2, 'unrealized_pnl', position.unrealized_pnl) or 0.0)
            position.state = "ACTIVE"
            self.persistence.save_positions_atomic(self._positions)
            try:
                if self.db and position.trade_id:
                    self.db.update_trade_state(position.trade_id, "ACTIVE")
            except Exception:
                pass
            self.log.error(f"Failed to close {symbol}")
            try:
                if self.db:
                    self.db.release_symbol_lock(symbol, lock_venue, owner=self._lock_owner)
            except Exception:
                pass
            _clear_pending_exit_intent()
            return False

    async def get_position(self, symbol: str, venue: Optional[str] = None) -> Optional[Position]:
        """Get local position state (optionally by venue)."""
        _, position = self._resolve_position(symbol, venue)
        return position

    async def get_position_live(self, symbol: str, venue: Optional[str] = None) -> Optional[Position]:
        """Get live position state from exchange.

        Returns:
            Position when exchange reports an open position, else None.

        Raises:
            ValueError/RuntimeError/TimeoutError on fetch/routing failures.
        """
        venue_name = self._coerce_venue_for_symbol(symbol, venue) if venue else None

        if venue_name:
            try:
                self.router.validate(venue_name, symbol)
                adapter = self._get_adapter_by_venue(venue_name)
            except (ExchangeRoutingError, ValueError) as exc:
                raise ValueError(f"Invalid venue {venue_name} for {symbol}: {exc}") from exc
        else:
            adapter = self._get_adapter(symbol)

        if adapter is None or not getattr(adapter, "_initialized", False):
            raise RuntimeError(f"Adapter unavailable for {symbol} venue={venue_name or 'auto'}")

        try:
            pos = await asyncio.wait_for(adapter.get_position(symbol), timeout=ADAPTER_TIMEOUT_SEC)
        except asyncio.TimeoutError as exc:
            raise TimeoutError(f"get_position timeout for {symbol} venue={venue_name or 'auto'}") from exc
        except Exception as exc:
            raise RuntimeError(f"get_position failed for {symbol} venue={venue_name or 'auto'}: {exc}") from exc

        if pos and not str(getattr(pos, "venue", "") or "").strip():
            pos.venue = venue_name or self._infer_venue_from_adapter(adapter)
        return pos

    async def get_live_positions_by_venue(self, venue: str) -> Dict[str, Position]:
        """Fetch live exchange positions for a venue keyed as ``venue:symbol``."""
        venue_name = normalize_venue(venue)
        try:
            adapter = self._get_adapter_by_venue(venue_name)
        except ValueError as exc:
            raise ValueError(f"Unknown venue for live positions: {venue}") from exc

        if adapter is None or not getattr(adapter, "_initialized", False):
            raise RuntimeError(f"Adapter not initialized for venue={venue_name}")

        try:
            raw_positions = await asyncio.wait_for(adapter.get_all_positions(), timeout=ADAPTER_TIMEOUT_SEC)
        except asyncio.TimeoutError as exc:
            raise TimeoutError(f"get_all_positions timeout for venue={venue_name}") from exc
        except Exception as exc:
            raise RuntimeError(f"get_all_positions failed for venue={venue_name}: {exc}") from exc

        out: Dict[str, Position] = {}
        for symbol, pos in (raw_positions or {}).items():
            if not pos:
                continue
            if not str(getattr(pos, "venue", "") or "").strip():
                pos.venue = venue_name
            key = self._position_key(str(symbol or "").upper(), venue_name)
            out[key] = pos
        return out

    async def get_position_truth(self, symbol: str, venue: Optional[str] = None) -> Dict[str, Any]:
        """Return local+exchange position truth with confirmation metadata."""
        tracked = await self.get_position(symbol, venue=venue)
        tracked_dir = str(getattr(tracked, "direction", "") or "").upper()
        tracked_size = float(getattr(tracked, "size", 0.0) or 0.0)

        exchange: Optional[Position] = None
        exchange_dir = ""
        exchange_size = 0.0
        exchange_confirmed = False
        confirm_error: Optional[str] = None
        try:
            exchange = await self.get_position_live(symbol, venue=venue)
            exchange_confirmed = True
            exchange_dir = str(getattr(exchange, "direction", "") or "").upper() if exchange else ""
            exchange_size = float(getattr(exchange, "size", 0.0) or 0.0) if exchange else 0.0
        except Exception as exc:
            confirm_error = str(exc)

        return {
            "tracked_position": tracked,
            "tracked_direction": tracked_dir,
            "tracked_size": tracked_size,
            "exchange_position": exchange,
            "exchange_direction": exchange_dir,
            "exchange_size": exchange_size,
            "exchange_confirmed": exchange_confirmed,
            "confirm_error": confirm_error,
        }

    async def get_all_positions(self) -> Dict[str, Position]:
        """Get all local positions (keyed by venue:symbol)."""
        return dict(self._positions)

    def get_total_exposure(self) -> float:
        """Total notional exposure across tracked positions (ignores dust)."""
        total = 0.0
        try:
            dust = float(getattr(self.config, "dust_notional_usd", 0.0) or 0.0)
        except Exception:
            dust = 0.0

        for p in self._positions.values():
            if not p or (p.direction or '').upper() == 'FLAT':
                continue
            if (p.state or '').upper() == 'DUST':
                continue
            try:
                notional = abs(float(p.size or 0.0) * float(p.entry_price or 0.0))
            except Exception:
                continue
            if dust > 0 and notional < dust:
                continue
            total += notional

        return total

# ============================================================================
# Integration
# ============================================================================



if __name__ == "__main__":
    # Quick test
    import asyncio

    async def test():
        config = ExecutionConfig(dry_run=True)
        executor = Executor(config=config)
        await executor.initialize()

        print(f"Executor initialized, dry_run={config.dry_run}")
        print("Rate limiter initialized")

    asyncio.run(test())
