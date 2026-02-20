#!/usr/bin/env python3
"""
AI Trader Database Module for Captain EVP.

SQLite database with WAL mode for concurrent access from:
- Fill reconciler (60s polling)
- Safety manager (per-trade updates)
- Learning engine (async reflections)
- Telegram interface (on-demand queries)

Phase 1 of Captain EVP AI Trading Agent.
"""

import json
from logging_utils import get_logger
import os
import sqlite3
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

from proposal_status import can_transition_proposal_status, normalize_proposal_status
from env_utils import EVCLAW_DB_PATH
from venues import normalize_venue



# =============================================================================
# Data Classes
# =============================================================================

@dataclass
class TradeRecord:
    """A trade record from the database."""
    id: int
    symbol: str
    direction: str
    venue: str
    state: str
    entry_time: float
    entry_price: float
    size: float
    notional_usd: float
    exit_time: Optional[float] = None
    exit_price: Optional[float] = None
    exit_reason: Optional[str] = None
    realized_pnl: Optional[float] = None
    realized_pnl_pct: Optional[float] = None
    total_fees: float = 0.0
    sl_price: Optional[float] = None
    tp_price: Optional[float] = None
    sl_order_id: Optional[str] = None
    tp_order_id: Optional[str] = None
    signals_snapshot: Optional[str] = None
    signals_agreed: Optional[str] = None
    ai_reasoning: Optional[str] = None
    confidence: Optional[str] = None
    size_multiplier: Optional[float] = None
    context_snapshot: Optional[str] = None
    protection_snapshot: Optional[str] = None
    safety_tier: Optional[int] = None



    def get_signals_agreed_list(self) -> List[str]:
        """Parse signals_agreed JSON array."""
        if self.signals_agreed:
            try:
                return json.loads(self.signals_agreed)
            except json.JSONDecodeError:
                return []
        return []


@dataclass
class SafetyState:
    """Safety state from the database."""
    current_tier: int = 1
    daily_pnl: float = 0.0
    daily_pnl_reset_at: Optional[str] = None
    consecutive_losses: int = 0
    cooldown_until: Optional[str] = None
    max_drawdown_pct: float = 0.0
    peak_equity: float = 10000.0
    current_equity: float = 10000.0
    paused: bool = False


# =============================================================================
# Database Class
# =============================================================================

class AITraderDB:
    """
    SQLite database for Captain EVP AI Trading Agent.

    Features:
    - WAL mode for concurrent access
    - Atomic writes with proper locking
    - Fill idempotency via UNIQUE constraint
    - Trade-fill relationship integrity
    """

    def __init__(
        self,
        db_path: str = EVCLAW_DB_PATH,
        starting_equity: float = 10000.0,
    ):
        self.db_path = Path(db_path)
        self.starting_equity = starting_equity
        self.log = get_logger("ai_trader_db")
        self._local = threading.local()
        self._conn_lock = threading.Lock()
        self._conn_by_tid: Dict[int, sqlite3.Connection] = {}
        self._conn_pid: int = int(os.getpid())
        self._init_db()

    def _open_connection(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self.db_path), timeout=30.0)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=30000")
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA synchronous=NORMAL")
        return conn

    def _get_connection(self) -> sqlite3.Connection:
        """Get a per-thread database connection."""
        tid = int(threading.get_ident())
        current_pid = int(os.getpid())
        with self._conn_lock:
            # After fork, inherited sqlite handles are unsafe in the child.
            if current_pid != int(self._conn_pid):
                for conn in self._conn_by_tid.values():
                    try:
                        conn.close()
                    except Exception:
                        pass
                self._conn_by_tid.clear()
                self._local.conn = None
                self._conn_pid = current_pid
            self._cleanup_stale_connections_locked()
            conn = self._conn_by_tid.get(tid)
            if conn is None:
                conn = self._open_connection()
                self._conn_by_tid[tid] = conn
                self._local.conn = conn
            return conn

    def _cleanup_stale_connections_locked(self) -> None:
        alive = {int(t.ident) for t in threading.enumerate() if t.ident is not None}
        stale_tids = [tid for tid in self._conn_by_tid.keys() if tid not in alive]
        for tid in stale_tids:
            conn = self._conn_by_tid.pop(tid, None)
            if conn is None:
                continue
            try:
                conn.close()
            except Exception:
                pass

    def _init_db(self) -> None:
        """Initialize database with WAL mode and all tables."""
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

        with self._get_connection() as conn:
            # Enable WAL mode for concurrent access
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA busy_timeout=30000")
            conn.execute("PRAGMA foreign_keys=ON")
            conn.execute("PRAGMA synchronous=NORMAL")

            # Create trades table (v2: includes venue)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS trades (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol TEXT NOT NULL,
                    direction TEXT NOT NULL,
                    venue TEXT NOT NULL DEFAULT 'lighter',
                    state TEXT NOT NULL DEFAULT 'ACTIVE',
                    entry_time REAL NOT NULL,
                    entry_price REAL NOT NULL,
                    size REAL NOT NULL,
                    notional_usd REAL NOT NULL,
                    exit_time REAL,
                    exit_price REAL,
                    exit_reason TEXT,
                    realized_pnl REAL,
                    realized_pnl_pct REAL,
                    realized_pnl_partial_usd REAL NOT NULL DEFAULT 0.0,
                    exit_fees_partial_usd REAL NOT NULL DEFAULT 0.0,
                    total_fees REAL DEFAULT 0.0,
                    sl_price REAL,
                    tp_price REAL,
                    sl_order_id TEXT,
                    tp_order_id TEXT,
                    signals_snapshot TEXT,
                    signals_agreed TEXT,
                    ai_reasoning TEXT,
                    confidence TEXT,
                    size_multiplier REAL,
                    context_snapshot TEXT,
                    protection_snapshot TEXT,
                    safety_tier INTEGER,
                    created_at REAL DEFAULT (strftime('%s', 'now')),
                    updated_at REAL DEFAULT (strftime('%s', 'now'))
                )
            """)

            # Create indexes for trades
            conn.execute("CREATE INDEX IF NOT EXISTS idx_trades_symbol ON trades (symbol, exit_time)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_trades_time ON trades (entry_time)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_trades_confidence ON trades (confidence, exit_time)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_trades_symbol_entry ON trades (symbol, entry_time DESC)")

            # -----------------------------------------------------------------
            # Decay tracking (flags + decisions)
            # -----------------------------------------------------------------
            conn.execute("""
                CREATE TABLE IF NOT EXISTS decay_flags (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts REAL NOT NULL,
                    symbol TEXT NOT NULL,
                    venue TEXT NOT NULL,
                    trade_id INTEGER,
                    db_direction TEXT,
                    live_direction TEXT,
                    reason TEXT NOT NULL,
                    detail TEXT,
                    context_path TEXT,
                    context_age_seconds REAL,
                    context_generated_at REAL,
                    context_direction TEXT,
                    signal_flip_only INTEGER DEFAULT 0,
                    notify_only INTEGER DEFAULT 0,
                    created_at REAL DEFAULT (strftime('%s', 'now'))
                )
            """)
            conn.execute("CREATE INDEX IF NOT EXISTS idx_decay_flags_ts ON decay_flags (ts DESC)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_decay_flags_symbol ON decay_flags (symbol, venue, ts DESC)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_decay_flags_trade ON decay_flags (trade_id, ts DESC)")

            conn.execute("""
                CREATE TABLE IF NOT EXISTS decay_decisions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts REAL NOT NULL,
                    symbol TEXT NOT NULL,
                    venue TEXT NOT NULL,
                    trade_id INTEGER,
                    source_plan_id INTEGER,
                    action TEXT NOT NULL,          -- CLOSE | HOLD
                    reason TEXT,
                    detail TEXT,
                    decided_by TEXT,
                    source TEXT NOT NULL DEFAULT 'manual',
                    created_at REAL DEFAULT (strftime('%s', 'now'))
                )
            """)
            conn.execute("CREATE INDEX IF NOT EXISTS idx_decay_decisions_ts ON decay_decisions (ts DESC)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_decay_decisions_symbol ON decay_decisions (symbol, venue, ts DESC)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_decay_decisions_trade ON decay_decisions (trade_id, ts DESC)")

            # Create fills table (v2: multi-venue)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS fills (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    trade_id INTEGER,
                    venue TEXT NOT NULL DEFAULT 'lighter',
                    exchange_trade_id TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    fill_time REAL NOT NULL,
                    fill_price REAL NOT NULL,
                    fill_size REAL NOT NULL,
                    fill_type TEXT NOT NULL,
                    side TEXT NOT NULL,
                    fee REAL DEFAULT 0.0,
                    fee_maker REAL DEFAULT 0.0,
                    position_sign_changed INTEGER DEFAULT 0,
                    raw_json TEXT,
                    created_at REAL DEFAULT (strftime('%s', 'now')),
                    UNIQUE(venue, exchange_trade_id),
                    FOREIGN KEY (trade_id) REFERENCES trades(id)
                )
            """)

            # Create trade_reflections table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS trade_reflections (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    trade_id INTEGER NOT NULL UNIQUE,
                    reflection TEXT,
                    lessons TEXT,
                    outcome_category TEXT,
                    signal_contributions TEXT,
                    created_at REAL DEFAULT (strftime('%s', 'now')),
                    FOREIGN KEY (trade_id) REFERENCES trades(id)
                )
            """)

            # Create reflections_v2 (used by hourly reflection sweeps / agent notes)
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS reflections_v2 (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    trade_id INTEGER NOT NULL,
                    reflection_json TEXT,
                    lesson_text TEXT,
                    confidence TEXT,
                    created_at REAL DEFAULT (strftime('%s', 'now'))
                )
                """
            )
            # Dedupe only when bootstrapping the unique index.
            idx_exists = conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='index' AND name='idx_reflections_v2_trade' LIMIT 1"
            ).fetchone() is not None
            if not idx_exists:
                conn.execute(
                    """
                    DELETE FROM reflections_v2
                    WHERE rowid NOT IN (
                        SELECT MAX(rowid) FROM reflections_v2 GROUP BY trade_id
                    )
                    """
                )
                conn.execute(
                    "CREATE UNIQUE INDEX IF NOT EXISTS idx_reflections_v2_trade ON reflections_v2(trade_id)"
                )

            # Reflection task queue (trade_id keyed) — processed by learning reflector worker.
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS reflection_tasks_v1 (
                    trade_id INTEGER PRIMARY KEY,
                    symbol TEXT,
                    status TEXT NOT NULL DEFAULT 'PENDING',
                    attempts INTEGER NOT NULL DEFAULT 0,
                    last_error TEXT,
                    locked_by TEXT,
                    locked_at REAL,
                    created_at REAL DEFAULT (strftime('%s', 'now')),
                    updated_at REAL DEFAULT (strftime('%s', 'now'))
                )
                """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_reflection_tasks_v1_status_created ON reflection_tasks_v1(status, created_at)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_reflection_tasks_v1_symbol ON reflection_tasks_v1(symbol)"
            )

            # Per-symbol rolling conclusion (A→B) derived from reflections.
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS symbol_conclusions_v1 (
                    symbol TEXT PRIMARY KEY,
                    conclusion_text TEXT,
                    conclusion_json TEXT,
                    confidence REAL,
                    last_reflection_id_seen INTEGER DEFAULT 0,
                    updated_at REAL DEFAULT (strftime('%s', 'now'))
                )
                """
            )

            # Create signal_combos table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS signal_combos (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    combo_key TEXT NOT NULL UNIQUE,
                    trades INTEGER DEFAULT 0,
                    wins INTEGER DEFAULT 0,
                    total_pnl REAL DEFAULT 0.0,
                    avg_pnl REAL DEFAULT 0.0,
                    win_rate REAL DEFAULT 0.0,
                    last_trade_at REAL,
                    updated_at REAL DEFAULT (strftime('%s', 'now'))
                )
            """)

            # Create safety_state table (singleton)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS safety_state (
                    id INTEGER PRIMARY KEY CHECK (id = 1),
                    current_tier INTEGER DEFAULT 1,
                    daily_pnl REAL DEFAULT 0.0,
                    daily_pnl_reset_at TEXT,
                    consecutive_losses INTEGER DEFAULT 0,
                    cooldown_until TEXT,
                    max_drawdown_pct REAL DEFAULT 0.0,
                    peak_equity REAL DEFAULT 10000.0,
                    current_equity REAL DEFAULT 10000.0,
                    paused INTEGER DEFAULT 0,
                    updated_at REAL DEFAULT (strftime('%s', 'now'))
                )
            """)

            # Initialize safety_state singleton
            conn.execute(
                "INSERT OR IGNORE INTO safety_state (id, peak_equity, current_equity) VALUES (1, ?, ?)",
                (self.starting_equity, self.starting_equity)
            )

            conn.commit()

            # Run migrations for existing databases (before fills indexes,
            # since v1 fills table lacks venue/exchange_trade_id columns)
            self._migrate_v2(conn)
            self._migrate_v3(conn)
            self._migrate_v4(conn)
            self._migrate_v5(conn)
            self._migrate_v6(conn)
            self._migrate_v7(conn)
            self._migrate_v8(conn)
            self._migrate_v9(conn)
            self._migrate_v10(conn)
            self._migrate_v11(conn)
            self._migrate_v12(conn)
            self._migrate_v13(conn)
            self._migrate_v14(conn)
            self._migrate_v15(conn)
            self._migrate_v16(conn)
            self._migrate_v17(conn)
            self._migrate_v18(conn)
            self._migrate_v19(conn)
            self._migrate_v20(conn)
            self._migrate_v21(conn)
            self._migrate_v22(conn)
            self._migrate_v23(conn)
            self._migrate_v24(conn)
            self._migrate_v25(conn)
            self._migrate_v26(conn)
            self._migrate_v27(conn)
            self._migrate_v28(conn)
            self._migrate_v29(conn)
            self._migrate_v30(conn)
            self._migrate_v31(conn)
            self._migrate_v32(conn)
            self._migrate_v33(conn)
            self._migrate_v34(conn)
            self._migrate_v35(conn)
            self._migrate_v36(conn)

            # Create fills indexes (after migration so columns exist)
            conn.execute("CREATE INDEX IF NOT EXISTS idx_fills_trade ON fills (trade_id)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_fills_venue_eid ON fills (venue, exchange_trade_id)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_fills_symbol_time ON fills (symbol, fill_time)")
            conn.commit()

            # Sanitize legacy/non-JSON cycle summaries so json_extract/json_valid queries don't break.
            try:
                rows = conn.execute(
                    "SELECT seq, processed_summary FROM cycle_runs WHERE processed_summary IS NOT NULL"
                ).fetchall()
                for r in rows:
                    seq = int(r[0])
                    s = r[1]
                    norm = self._normalize_cycle_summary_json(s)
                    if norm is not None and norm != s:
                        conn.execute(
                            "UPDATE cycle_runs SET processed_summary=? WHERE seq=?",
                            (norm, seq),
                        )
                conn.commit()
            except Exception:
                pass

            self.log.info(f"Database initialized at {self.db_path} with WAL mode")

    def _migrate_v2(self, conn: sqlite3.Connection) -> None:
        """Migrate to multi-venue schema (v2). Idempotent via PRAGMA user_version."""
        version = conn.execute("PRAGMA user_version").fetchone()[0]
        if version >= 2:
            return  # Already migrated

        # Check if old fills table exists with lighter_trade_id column
        cursor = conn.execute("PRAGMA table_info(fills)")
        columns = {row[1] for row in cursor.fetchall()}

        if 'lighter_trade_id' not in columns:
            # New DB or already migrated — just set version
            conn.execute("PRAGMA user_version = 2")
            conn.commit()
            return

        self.log.info("Migrating fills table to multi-venue schema (v2)...")

        # Use a regular transaction (not EXCLUSIVE — we already hold the connection)
        try:
            # Clean up orphan from a prior failed migration attempt
            conn.execute("DROP TABLE IF EXISTS fills_new")

            conn.execute("""
                CREATE TABLE fills_new (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    trade_id INTEGER,
                    venue TEXT NOT NULL DEFAULT 'lighter',
                    exchange_trade_id TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    fill_time REAL NOT NULL,
                    fill_price REAL NOT NULL,
                    fill_size REAL NOT NULL,
                    fill_type TEXT NOT NULL,
                    side TEXT NOT NULL,
                    fee REAL DEFAULT 0.0,
                    fee_maker REAL DEFAULT 0.0,
                    position_sign_changed INTEGER DEFAULT 0,
                    raw_json TEXT,
                    created_at REAL DEFAULT (strftime('%s', 'now')),
                    UNIQUE(venue, exchange_trade_id),
                    FOREIGN KEY (trade_id) REFERENCES trades(id)
                )
            """)

            conn.execute("""
                INSERT INTO fills_new (id, trade_id, venue, exchange_trade_id,
                    symbol, fill_time, fill_price, fill_size, fill_type, side,
                    fee, fee_maker, position_sign_changed, raw_json, created_at)
                SELECT id, trade_id, 'lighter', CAST(lighter_trade_id AS TEXT),
                    symbol, fill_time, fill_price, fill_size, fill_type, side,
                    fee, fee_maker, position_sign_changed, raw_json, created_at
                FROM fills
            """)

            conn.execute("DROP TABLE fills")
            conn.execute("ALTER TABLE fills_new RENAME TO fills")

            # Recreate indexes
            conn.execute("CREATE INDEX idx_fills_trade ON fills (trade_id)")
            conn.execute("CREATE INDEX idx_fills_venue_eid ON fills (venue, exchange_trade_id)")
            conn.execute("CREATE INDEX idx_fills_symbol_time ON fills (symbol, fill_time)")

            # Add venue column to trades table if missing
            trades_cursor = conn.execute("PRAGMA table_info(trades)")
            trades_columns = {row[1] for row in trades_cursor.fetchall()}
            if 'venue' not in trades_columns:
                conn.execute("ALTER TABLE trades ADD COLUMN venue TEXT NOT NULL DEFAULT 'lighter'")
                conn.execute("CREATE INDEX IF NOT EXISTS idx_trades_venue ON trades (venue)")

            conn.execute("PRAGMA user_version = 2")
            conn.commit()
            self.log.info("Migration to v2 complete")
        except Exception:
            conn.rollback()
            raise

    def _migrate_v3(self, conn: sqlite3.Connection) -> None:
        """Add trade state column for tracking (v3)."""
        version = conn.execute("PRAGMA user_version").fetchone()[0]
        if version >= 3:
            return

        trades_cursor = conn.execute("PRAGMA table_info(trades)")
        trades_columns = {row[1] for row in trades_cursor.fetchall()}
        if 'state' not in trades_columns:
            conn.execute("ALTER TABLE trades ADD COLUMN state TEXT NOT NULL DEFAULT 'ACTIVE'")

        conn.execute("PRAGMA user_version = 3")
        conn.commit()

    def _migrate_v4(self, conn: sqlite3.Connection) -> None:
        """Add cycle/proposal tracking tables (v4)."""
        version = conn.execute("PRAGMA user_version").fetchone()[0]
        if version >= 4:
            return

        conn.execute("""
            CREATE TABLE IF NOT EXISTS cycle_runs (
                seq INTEGER PRIMARY KEY,
                timestamp REAL NOT NULL,
                cycle_file TEXT,
                context_file TEXT,
                candidates_file TEXT,
                claimed_at REAL,
                claimed_by TEXT,
                processed_at REAL,
                processed_by TEXT,
                processed_status TEXT,
                processed_summary TEXT,
                processed_error TEXT
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS trade_proposals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                cycle_seq INTEGER NOT NULL,
                symbol TEXT NOT NULL,
                venue TEXT NOT NULL,
                direction TEXT NOT NULL,
                size_usd REAL NOT NULL,
                sl REAL,
                tp REAL,
                conviction REAL,
                reason_short TEXT,
                signals_json TEXT,
                status TEXT NOT NULL DEFAULT 'PROPOSED',
                status_reason TEXT,
                created_at REAL DEFAULT (strftime('%s', 'now')),
                updated_at REAL DEFAULT (strftime('%s', 'now'))
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS proposal_executions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                proposal_id INTEGER NOT NULL,
                started_at REAL,
                finished_at REAL,
                success INTEGER,
                error TEXT,
                exchange TEXT,
                entry_price REAL,
                size REAL,
                sl_price REAL,
                tp_price REAL,
                FOREIGN KEY (proposal_id) REFERENCES trade_proposals(id)
            )
        """)

        conn.execute("CREATE INDEX IF NOT EXISTS idx_cycle_runs_seq ON cycle_runs (seq)")
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_cycle_runs_processed "
            "ON cycle_runs (processed_at, claimed_at, seq)"
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_trade_proposals_cycle ON trade_proposals (cycle_seq)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_trade_proposals_status ON trade_proposals (status)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_proposal_execs_proposal ON proposal_executions (proposal_id)")

        conn.execute("PRAGMA user_version = 4")
        conn.commit()

    def _migrate_v5(self, conn: sqlite3.Connection) -> None:
        """Add monitoring snapshot tables into ai_trader.db (v5)."""
        version = conn.execute("PRAGMA user_version").fetchone()[0]
        if version >= 5:
            return

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS monitor_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts REAL NOT NULL,
                ts_iso TEXT NOT NULL,
                hl_equity REAL,
                hl_balance REAL,
                hl_unrealized REAL,
                lighter_equity REAL,
                lighter_balance REAL,
                lighter_unrealized REAL,
                total_equity REAL,
                sltp_failed_count INTEGER,
                placing_sltp_count INTEGER
            )
            """
        )

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS monitor_positions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                snapshot_id INTEGER NOT NULL,
                source TEXT NOT NULL,
                symbol TEXT NOT NULL,
                direction TEXT,
                size REAL,
                entry_price REAL,
                unrealized_pnl REAL,
                state TEXT,
                sl_price REAL,
                tp_price REAL,
                sl_order_id TEXT,
                tp_order_id TEXT,
                opened_at TEXT,
                signals_agreeing TEXT,
                venue TEXT,
                FOREIGN KEY (snapshot_id) REFERENCES monitor_snapshots(id)
            )
            """
        )

        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_monitor_positions_snapshot "
            "ON monitor_positions (snapshot_id)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_monitor_positions_symbol "
            "ON monitor_positions (symbol)"
        )

        conn.execute("PRAGMA user_version = 5")
        conn.commit()

    def _migrate_v6(self, conn: sqlite3.Connection) -> None:
        """Add fill reconciliation state table for paging (v6)."""
        version = conn.execute("PRAGMA user_version").fetchone()[0]
        if version >= 6:
            return

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS fill_reconcile_state (
                venue TEXT PRIMARY KEY,
                last_fill_time REAL,
                last_exchange_trade_id TEXT,
                updated_at REAL DEFAULT (strftime('%s', 'now'))
            )
            """
        )

        conn.execute("PRAGMA user_version = 6")
        conn.commit()

    def _migrate_v7(self, conn: sqlite3.Connection) -> None:
        """Add cycle queue processing metadata (v7)."""
        version = conn.execute("PRAGMA user_version").fetchone()[0]
        if version >= 7:
            return

        columns = {row[1] for row in conn.execute("PRAGMA table_info(cycle_runs)").fetchall()}
        if "context_file" not in columns:
            conn.execute("ALTER TABLE cycle_runs ADD COLUMN context_file TEXT")
        if "candidates_file" not in columns:
            conn.execute("ALTER TABLE cycle_runs ADD COLUMN candidates_file TEXT")
        if "claimed_at" not in columns:
            conn.execute("ALTER TABLE cycle_runs ADD COLUMN claimed_at REAL")
        if "claimed_by" not in columns:
            conn.execute("ALTER TABLE cycle_runs ADD COLUMN claimed_by TEXT")
        if "processed_at" not in columns:
            conn.execute("ALTER TABLE cycle_runs ADD COLUMN processed_at REAL")
        if "processed_by" not in columns:
            conn.execute("ALTER TABLE cycle_runs ADD COLUMN processed_by TEXT")
        if "processed_status" not in columns:
            conn.execute("ALTER TABLE cycle_runs ADD COLUMN processed_status TEXT")
        if "processed_summary" not in columns:
            conn.execute("ALTER TABLE cycle_runs ADD COLUMN processed_summary TEXT")
        if "processed_error" not in columns:
            conn.execute("ALTER TABLE cycle_runs ADD COLUMN processed_error TEXT")

        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_cycle_runs_processed "
            "ON cycle_runs (processed_at, claimed_at, seq)"
        )

        conn.execute("PRAGMA user_version = 7")
        conn.commit()

    def _migrate_v8(self, conn: sqlite3.Connection) -> None:
        """Add protection_snapshot column for SL/TP learning (v8).

        Note: some historical DBs may have user_version bumped without the column
        actually being present (e.g. interrupted migration). We handle that in v9.
        """
        version = conn.execute("PRAGMA user_version").fetchone()[0]
        if version >= 8:
            return

        columns = {row[1] for row in conn.execute("PRAGMA table_info(trades)").fetchall()}
        if "protection_snapshot" not in columns:
            conn.execute("ALTER TABLE trades ADD COLUMN protection_snapshot TEXT")

        conn.execute("PRAGMA user_version = 8")
        conn.commit()

    def _migrate_v9(self, conn: sqlite3.Connection) -> None:
        """Repair/ensure protection_snapshot column exists (v9)."""
        version = conn.execute("PRAGMA user_version").fetchone()[0]
        if version >= 9:
            return

        columns = {row[1] for row in conn.execute("PRAGMA table_info(trades)").fetchall()}
        if "protection_snapshot" not in columns:
            conn.execute("ALTER TABLE trades ADD COLUMN protection_snapshot TEXT")

        conn.execute("PRAGMA user_version = 9")
        conn.commit()

    def _migrate_v10(self, conn: sqlite3.Connection) -> None:
        """Add AGI trader tables and columns (v10).

        New columns on trades:
        - risk_pct_used: actual risk % used for this trade
        - equity_at_entry: account equity when trade was opened
        - agi_model_version: version of AGI model that made decision

        New tables:
        - position_actions: tracks partial closes, SL/TP modifications
        - symbol_policy: per-symbol learned adjustments
        - agi_decisions: full AGI decision audit trail
        """
        version = conn.execute("PRAGMA user_version").fetchone()[0]
        if version >= 10:
            return

        self.log.info("Migrating database to v10 (AGI trader schema)...")

        # Add new columns to trades table
        columns = {row[1] for row in conn.execute("PRAGMA table_info(trades)").fetchall()}
        if "risk_pct_used" not in columns:
            conn.execute("ALTER TABLE trades ADD COLUMN risk_pct_used REAL")
        if "equity_at_entry" not in columns:
            conn.execute("ALTER TABLE trades ADD COLUMN equity_at_entry REAL")
        if "agi_model_version" not in columns:
            conn.execute("ALTER TABLE trades ADD COLUMN agi_model_version TEXT")

        # Create position_actions table
        conn.execute("""
            CREATE TABLE IF NOT EXISTS position_actions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                trade_id INTEGER NOT NULL,
                action_time REAL NOT NULL,
                action_type TEXT NOT NULL,
                old_value REAL,
                new_value REAL,
                reason TEXT,
                detail TEXT,
                created_at REAL DEFAULT (strftime('%s', 'now')),
                FOREIGN KEY (trade_id) REFERENCES trades(id)
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_position_actions_trade ON position_actions (trade_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_position_actions_time ON position_actions (action_time DESC)")

        # Create symbol_policy table
        conn.execute("""
            CREATE TABLE IF NOT EXISTS symbol_policy (
                symbol TEXT PRIMARY KEY,
                sl_mult_adjustment REAL DEFAULT 1.0,
                tp_mult_adjustment REAL DEFAULT 1.0,
                size_adjustment REAL DEFAULT 1.0,
                stop_out_rate REAL DEFAULT 0.0,
                win_rate REAL DEFAULT 0.5,
                samples INTEGER DEFAULT 0,
                last_updated REAL,
                notes TEXT
            )
        """)

        # Create agi_decisions table
        conn.execute("""
            CREATE TABLE IF NOT EXISTS agi_decisions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                cycle_seq INTEGER,
                timestamp REAL NOT NULL,
                decision_type TEXT NOT NULL,
                symbol TEXT NOT NULL,
                action TEXT NOT NULL,
                direction TEXT,
                risk_pct REAL,
                sl_atr_mult REAL,
                tp_atr_mult REAL,
                conviction_score REAL,
                reasoning TEXT,
                raw_llm_response TEXT,
                execution_result TEXT,
                execution_error TEXT,
                created_at REAL DEFAULT (strftime('%s', 'now'))
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_agi_decisions_cycle ON agi_decisions (cycle_seq)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_agi_decisions_symbol ON agi_decisions (symbol, timestamp DESC)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_agi_decisions_time ON agi_decisions (timestamp DESC)")

        conn.execute("PRAGMA user_version = 10")
        conn.commit()
        self.log.info("Migration to v10 complete")

    def _migrate_v11(self, conn: sqlite3.Connection) -> None:
        """Add dead_capital rolling stats for internal z-score learning (v11)."""
        version = conn.execute("PRAGMA user_version").fetchone()[0]
        if version >= 11:
            return

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS dead_capital_stats (
                symbol TEXT NOT NULL,
                venue TEXT NOT NULL,
                n INTEGER NOT NULL DEFAULT 0,
                mean REAL NOT NULL DEFAULT 0.0,
                var REAL NOT NULL DEFAULT 1.0,
                last_ts REAL,
                last_x REAL,
                last_z REAL,
                override_streak INTEGER NOT NULL DEFAULT 0,
                updated_at REAL DEFAULT (strftime('%s', 'now')),
                PRIMARY KEY(symbol, venue)
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_dead_cap_stats_updated ON dead_capital_stats (updated_at DESC)"
        )

        conn.execute("PRAGMA user_version = 11")
        conn.commit()

    def _migrate_v12(self, conn: sqlite3.Connection) -> None:
        """Add pending_orders table for SR-anchored limit workflow (v12)."""
        version = conn.execute("PRAGMA user_version").fetchone()[0]
        if version >= 12:
            return

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS pending_orders (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                venue TEXT NOT NULL,
                direction TEXT NOT NULL,
                limit_price REAL NOT NULL,
                intended_size REAL NOT NULL,
                exchange_order_id TEXT UNIQUE,
                sr_level REAL NOT NULL,
                placed_at REAL NOT NULL,
                expires_at REAL NOT NULL,
                entry_direction TEXT NOT NULL,
                state TEXT DEFAULT 'PENDING',
                cancel_reason TEXT,
                filled_size REAL DEFAULT 0.0,
                filled_price REAL DEFAULT 0.0,
                created_at REAL DEFAULT (strftime('%s','now')),
                updated_at REAL DEFAULT (strftime('%s','now'))
            )
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_pending_state ON pending_orders(state)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_pending_symbol_venue_state ON pending_orders(symbol, venue, state)")

        conn.execute("PRAGMA user_version = 12")
        conn.commit()

    def _migrate_v13(self, conn: sqlite3.Connection) -> None:
        """Add learning/protection columns to pending_orders (v13)."""
        version = conn.execute("PRAGMA user_version").fetchone()[0]
        if version >= 13:
            return

        for ddl in (
            "ALTER TABLE pending_orders ADD COLUMN signals_snapshot TEXT",
            "ALTER TABLE pending_orders ADD COLUMN signals_agreed TEXT",
            "ALTER TABLE pending_orders ADD COLUMN context_snapshot TEXT",
            "ALTER TABLE pending_orders ADD COLUMN conviction REAL",
            "ALTER TABLE pending_orders ADD COLUMN reason TEXT",
            "ALTER TABLE pending_orders ADD COLUMN sl_order_id TEXT",
            "ALTER TABLE pending_orders ADD COLUMN sl_price REAL",
        ):
            try:
                conn.execute(ddl)
            except Exception:
                pass

        conn.execute("PRAGMA user_version = 13")
        conn.commit()

    def _migrate_v14(self, conn: sqlite3.Connection) -> None:
        """Add AGI decision tracking columns (v14).

        Newer tooling (save_agi_decision.py) inserts richer context into
        agi_decisions. Fresh DBs created from scratch need these columns.

        Idempotent via PRAGMA user_version and try/except around ALTER TABLE.
        """
        version = conn.execute("PRAGMA user_version").fetchone()[0]
        if version >= 14:
            return

        for ddl in (
            "ALTER TABLE agi_decisions ADD COLUMN context_snapshot TEXT",
            "ALTER TABLE agi_decisions ADD COLUMN trend_score REAL",
            "ALTER TABLE agi_decisions ADD COLUMN atr_pct REAL",
            "ALTER TABLE agi_decisions ADD COLUMN signals_agreed TEXT",
            "ALTER TABLE agi_decisions ADD COLUMN strong_signals TEXT",
            "ALTER TABLE agi_decisions ADD COLUMN existing_position INTEGER DEFAULT 0",
            "ALTER TABLE agi_decisions ADD COLUMN trend_aligned INTEGER",
            "ALTER TABLE agi_decisions ADD COLUMN size_usd REAL",
            "ALTER TABLE agi_decisions ADD COLUMN executed_size_usd REAL",
            "ALTER TABLE agi_decisions ADD COLUMN trade_id INTEGER",
            "ALTER TABLE agi_decisions ADD COLUMN size_adjustment_reason TEXT",
        ):
            try:
                conn.execute(ddl)
            except Exception:
                pass

        conn.execute("PRAGMA user_version = 14")
        conn.commit()

    def _migrate_v15(self, conn: sqlite3.Connection) -> None:
        """Add proposal_metadata table (v15).

        live_agent.py stores extra execution context per proposal.
        """
        version = conn.execute("PRAGMA user_version").fetchone()[0]
        if version >= 15:
            return

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS proposal_metadata (
                proposal_id INTEGER PRIMARY KEY,
                meta_json TEXT,
                created_at REAL DEFAULT (strftime('%s', 'now')),
                updated_at REAL DEFAULT (strftime('%s', 'now'))
            )
            """
        )
        conn.execute("PRAGMA user_version = 15")
        conn.commit()

    def _migrate_v16(self, conn: sqlite3.Connection) -> None:
        """Add proposal_status_history table (v16).

        Tracks all proposal status transitions (APPROVED/BLOCKED/FAILED/EXECUTED)
        with reasons so we can audit approve+reject rationale end-to-end.
        """
        version = conn.execute("PRAGMA user_version").fetchone()[0]
        if version >= 16:
            return

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS proposal_status_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                proposal_id INTEGER NOT NULL,
                status TEXT NOT NULL,
                reason TEXT,
                created_at REAL DEFAULT (strftime('%s', 'now')),
                FOREIGN KEY (proposal_id) REFERENCES trade_proposals(id)
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_proposal_status_hist_pid_ts ON proposal_status_history (proposal_id, created_at)"
        )
        conn.execute("PRAGMA user_version = 16")
        conn.commit()

    def _migrate_v17(self, conn: sqlite3.Connection) -> None:
        """Hardening schema: pending exit reasons + cross-process symbol locks (v17)."""
        version = conn.execute("PRAGMA user_version").fetchone()[0]
        if version >= 17:
            return

        # 1) Persist close intent even when fee-accurate close is finalized later by fill_reconciler.
        trade_cols = {row[1] for row in conn.execute("PRAGMA table_info(trades)").fetchall()}
        if "pending_exit_reason" not in trade_cols:
            conn.execute("ALTER TABLE trades ADD COLUMN pending_exit_reason TEXT")
        if "pending_exit_detail" not in trade_cols:
            conn.execute("ALTER TABLE trades ADD COLUMN pending_exit_detail TEXT")
        if "pending_exit_set_at" not in trade_cols:
            conn.execute("ALTER TABLE trades ADD COLUMN pending_exit_set_at REAL")

        # 2) Cross-process symbol busy guard (guardian/live_agent/executor).
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS symbol_locks (
                symbol TEXT NOT NULL,
                venue TEXT NOT NULL,
                owner TEXT NOT NULL,
                lock_type TEXT,
                reason TEXT,
                acquired_at REAL NOT NULL,
                expires_at REAL NOT NULL,
                PRIMARY KEY (symbol, venue)
            )
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_symbol_locks_expires ON symbol_locks (expires_at)")

        conn.execute("PRAGMA user_version = 17")
        conn.commit()

    def _migrate_v18(self, conn: sqlite3.Connection) -> None:
        """Add orphan_orders table for failed cancel tracking (v18)."""
        version = conn.execute("PRAGMA user_version").fetchone()[0]
        if version >= 18:
            return

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS orphan_orders (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                venue TEXT NOT NULL,
                order_id TEXT NOT NULL,
                order_type TEXT NOT NULL,
                reason TEXT,
                meta_json TEXT,
                created_at REAL DEFAULT (strftime('%s', 'now'))
            )
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_orphan_orders_symbol_venue ON orphan_orders (symbol, venue)")
        conn.execute("PRAGMA user_version = 18")
        conn.commit()

    def _migrate_v19(self, conn: sqlite3.Connection) -> None:
        """Hardening: prevent duplicate open trade rows per (symbol, venue) (v19).

        Net-position venues should only have one open trade row per (symbol, venue).
        This migration:
          1) closes stale duplicates externally (keeps newest by entry_time)
          2) creates a partial UNIQUE index for open rows
        """
        version = conn.execute("PRAGMA user_version").fetchone()[0]
        if version >= 19:
            return

        # 1) Close duplicates (keep newest)
        dups = conn.execute(
            """
            SELECT symbol, venue
            FROM trades
            WHERE exit_time IS NULL
            GROUP BY symbol, venue
            HAVING COUNT(*) > 1
            """
        ).fetchall()

        for sym, ven in dups:
            rows = conn.execute(
                """
                SELECT id
                FROM trades
                WHERE symbol = ? AND venue = ? AND exit_time IS NULL
                ORDER BY entry_time DESC
                """,
                (sym, ven),
            ).fetchall()
            ids = [int(r[0]) for r in rows]
            keep = ids[0] if ids else None
            for tid in ids[1:]:
                conn.execute(
                    """
                    UPDATE trades SET
                        exit_time = strftime('%s','now'),
                        exit_price = NULL,
                        exit_reason = 'RECONCILE_DUPLICATE_OPEN_TRADE',
                        realized_pnl = NULL,
                        realized_pnl_pct = NULL,
                        total_fees = COALESCE(total_fees, 0.0),
                        state = 'CLOSED_EXTERNALLY',
                        pending_exit_reason = NULL,
                        pending_exit_detail = NULL,
                        pending_exit_set_at = NULL,
                        updated_at = strftime('%s','now')
                    WHERE id = ? AND exit_time IS NULL
                    """,
                    (tid,),
                )

        # 2) Enforce uniqueness going forward
        conn.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS uniq_open_trade_symbol_venue
            ON trades(symbol, venue)
            WHERE exit_time IS NULL
            """
        )

        conn.execute("PRAGMA user_version = 19")
        conn.commit()

    def _migrate_v20(self, conn: sqlite3.Connection) -> None:
        """Ensure MAE/MFE columns exist on trades (v20)."""
        version = conn.execute("PRAGMA user_version").fetchone()[0]
        if version >= 20:
            return

        cols = {row[1] for row in conn.execute("PRAGMA table_info(trades)").fetchall()}
        for name, ddl in (
            ("mae_pct", "ALTER TABLE trades ADD COLUMN mae_pct REAL"),
            ("mfe_pct", "ALTER TABLE trades ADD COLUMN mfe_pct REAL"),
            ("mae_price", "ALTER TABLE trades ADD COLUMN mae_price REAL"),
            ("mfe_price", "ALTER TABLE trades ADD COLUMN mfe_price REAL"),
        ):
            if name not in cols:
                try:
                    conn.execute(ddl)
                except Exception:
                    pass

        conn.execute("PRAGMA user_version = 20")
        conn.commit()

    def _migrate_v21(self, conn: sqlite3.Connection) -> None:
        """Add symbol dossier learning tables (v21)."""
        version = conn.execute("PRAGMA user_version").fetchone()[0]
        if version >= 21:
            return

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS symbol_learning_state (
                symbol TEXT PRIMARY KEY,
                updated_at REAL DEFAULT (strftime('%s', 'now')),
                n_closed INTEGER DEFAULT 0,
                win_rate REAL DEFAULT 0.0,
                expectancy REAL DEFAULT 0.0,
                avg_pnl_pct REAL DEFAULT 0.0,
                n_closed_30d INTEGER DEFAULT 0,
                win_rate_30d REAL DEFAULT 0.0,
                expectancy_30d REAL DEFAULT 0.0,
                avg_pnl_pct_30d REAL DEFAULT 0.0,
                avg_mae_pct REAL DEFAULT 0.0,
                p90_mae_pct REAL DEFAULT 0.0,
                avg_mfe_pct REAL DEFAULT 0.0,
                p90_mfe_pct REAL DEFAULT 0.0,
                sltp_fallback_rate REAL DEFAULT 0.0,
                typical_sl_mult REAL,
                typical_tp_mult REAL,
                signal_rank_json TEXT,
                combo_rank_json TEXT,
                notes_summary TEXT,
                last_reflection_id_seen INTEGER DEFAULT 0
            )
            """
        )

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS signal_symbol_stats (
                symbol TEXT NOT NULL,
                signal TEXT NOT NULL,
                direction TEXT NOT NULL,
                n INTEGER DEFAULT 0,
                wins INTEGER DEFAULT 0,
                win_rate REAL DEFAULT 0.0,
                avg_pnl_pct REAL DEFAULT 0.0,
                expectancy REAL DEFAULT 0.0,
                last_updated REAL,
                PRIMARY KEY (symbol, signal, direction)
            )
            """
        )

        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_symbol_learning_updated ON symbol_learning_state (updated_at DESC)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_signal_symbol_last_updated ON signal_symbol_stats (symbol, last_updated DESC)"
        )

        # Repair partial tables from interrupted deployments.
        sym_cols = {row[1] for row in conn.execute("PRAGMA table_info(symbol_learning_state)").fetchall()}
        for name, ddl in (
            ("updated_at", "ALTER TABLE symbol_learning_state ADD COLUMN updated_at REAL DEFAULT (strftime('%s', 'now'))"),
            ("n_closed", "ALTER TABLE symbol_learning_state ADD COLUMN n_closed INTEGER DEFAULT 0"),
            ("win_rate", "ALTER TABLE symbol_learning_state ADD COLUMN win_rate REAL DEFAULT 0.0"),
            ("expectancy", "ALTER TABLE symbol_learning_state ADD COLUMN expectancy REAL DEFAULT 0.0"),
            ("avg_pnl_pct", "ALTER TABLE symbol_learning_state ADD COLUMN avg_pnl_pct REAL DEFAULT 0.0"),
            ("n_closed_30d", "ALTER TABLE symbol_learning_state ADD COLUMN n_closed_30d INTEGER DEFAULT 0"),
            ("win_rate_30d", "ALTER TABLE symbol_learning_state ADD COLUMN win_rate_30d REAL DEFAULT 0.0"),
            ("expectancy_30d", "ALTER TABLE symbol_learning_state ADD COLUMN expectancy_30d REAL DEFAULT 0.0"),
            ("avg_pnl_pct_30d", "ALTER TABLE symbol_learning_state ADD COLUMN avg_pnl_pct_30d REAL DEFAULT 0.0"),
            ("avg_mae_pct", "ALTER TABLE symbol_learning_state ADD COLUMN avg_mae_pct REAL DEFAULT 0.0"),
            ("p90_mae_pct", "ALTER TABLE symbol_learning_state ADD COLUMN p90_mae_pct REAL DEFAULT 0.0"),
            ("avg_mfe_pct", "ALTER TABLE symbol_learning_state ADD COLUMN avg_mfe_pct REAL DEFAULT 0.0"),
            ("p90_mfe_pct", "ALTER TABLE symbol_learning_state ADD COLUMN p90_mfe_pct REAL DEFAULT 0.0"),
            ("sltp_fallback_rate", "ALTER TABLE symbol_learning_state ADD COLUMN sltp_fallback_rate REAL DEFAULT 0.0"),
            ("typical_sl_mult", "ALTER TABLE symbol_learning_state ADD COLUMN typical_sl_mult REAL"),
            ("typical_tp_mult", "ALTER TABLE symbol_learning_state ADD COLUMN typical_tp_mult REAL"),
            ("signal_rank_json", "ALTER TABLE symbol_learning_state ADD COLUMN signal_rank_json TEXT"),
            ("combo_rank_json", "ALTER TABLE symbol_learning_state ADD COLUMN combo_rank_json TEXT"),
            ("notes_summary", "ALTER TABLE symbol_learning_state ADD COLUMN notes_summary TEXT"),
            ("last_reflection_id_seen", "ALTER TABLE symbol_learning_state ADD COLUMN last_reflection_id_seen INTEGER DEFAULT 0"),
        ):
            if name not in sym_cols:
                try:
                    conn.execute(ddl)
                except Exception:
                    pass

        sig_cols = {row[1] for row in conn.execute("PRAGMA table_info(signal_symbol_stats)").fetchall()}
        for name, ddl in (
            ("n", "ALTER TABLE signal_symbol_stats ADD COLUMN n INTEGER DEFAULT 0"),
            ("wins", "ALTER TABLE signal_symbol_stats ADD COLUMN wins INTEGER DEFAULT 0"),
            ("win_rate", "ALTER TABLE signal_symbol_stats ADD COLUMN win_rate REAL DEFAULT 0.0"),
            ("avg_pnl_pct", "ALTER TABLE signal_symbol_stats ADD COLUMN avg_pnl_pct REAL DEFAULT 0.0"),
            ("expectancy", "ALTER TABLE signal_symbol_stats ADD COLUMN expectancy REAL DEFAULT 0.0"),
            ("last_updated", "ALTER TABLE signal_symbol_stats ADD COLUMN last_updated REAL"),
        ):
            if name not in sig_cols:
                try:
                    conn.execute(ddl)
                except Exception:
                    pass

        conn.execute("PRAGMA user_version = 21")
        conn.commit()

    def _migrate_v22(self, conn: sqlite3.Connection) -> None:
        """Add partial-exit realized PnL tracking columns to trades (v22)."""
        version = conn.execute("PRAGMA user_version").fetchone()[0]
        if version >= 22:
            return

        cols = {row[1] for row in conn.execute("PRAGMA table_info(trades)").fetchall()}
        for name, ddl in (
            ("realized_pnl_partial_usd", "ALTER TABLE trades ADD COLUMN realized_pnl_partial_usd REAL NOT NULL DEFAULT 0.0"),
            ("exit_fees_partial_usd", "ALTER TABLE trades ADD COLUMN exit_fees_partial_usd REAL NOT NULL DEFAULT 0.0"),
        ):
            if name not in cols:
                try:
                    conn.execute(ddl)
                except Exception:
                    pass

        conn.execute("PRAGMA user_version = 22")
        conn.commit()

    def _migrate_v23(self, conn: sqlite3.Connection) -> None:
        """Add structured source_plan_id for robust decider dedup (v23)."""
        version = conn.execute("PRAGMA user_version").fetchone()[0]
        if version >= 23:
            return

        cols = {row[1] for row in conn.execute("PRAGMA table_info(decay_decisions)").fetchall()}
        if "source_plan_id" not in cols:
            try:
                conn.execute("ALTER TABLE decay_decisions ADD COLUMN source_plan_id INTEGER")
            except Exception:
                pass

        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_decay_decisions_source_plan ON decay_decisions (source, source_plan_id, ts DESC)"
        )
        conn.execute("PRAGMA user_version = 23")
        conn.commit()

    def _migrate_v24(self, conn: sqlite3.Connection) -> None:
        """Add adaptive conviction config/run/feature tables (v24)."""
        version = conn.execute("PRAGMA user_version").fetchone()[0]
        if version >= 24:
            return

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS conviction_config_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at REAL DEFAULT (strftime('%s', 'now')),
                effective_from REAL NOT NULL,
                source TEXT NOT NULL DEFAULT 'manual',
                notes TEXT,
                params_json TEXT NOT NULL,
                is_active INTEGER NOT NULL DEFAULT 0
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_conv_cfg_effective ON conviction_config_history (effective_from DESC)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_conv_cfg_active ON conviction_config_history (is_active, effective_from DESC)"
        )

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS adaptation_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                started_at REAL NOT NULL,
                finished_at REAL,
                status TEXT NOT NULL,
                window_start REAL,
                window_end REAL,
                fingerprint TEXT NOT NULL,
                result_json TEXT,
                error_text TEXT,
                created_at REAL DEFAULT (strftime('%s', 'now'))
            )
            """
        )
        conn.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS uq_adaptation_runs_fingerprint ON adaptation_runs (fingerprint)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_adaptation_runs_window ON adaptation_runs (window_end DESC, status)"
        )

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS trade_features (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                trade_id INTEGER NOT NULL,
                symbol TEXT NOT NULL,
                direction TEXT,
                venue TEXT,
                closed_at REAL NOT NULL,
                conviction REAL,
                order_type TEXT,
                order_type_source TEXT,
                risk_pct_used REAL,
                equity_at_entry REAL,
                mae_pct REAL,
                mfe_pct REAL,
                pnl_usd REAL,
                pnl_r REAL,
                exit_reason TEXT,
                created_at REAL DEFAULT (strftime('%s', 'now'))
            )
            """
        )
        conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS uq_trade_features_trade_id ON trade_features (trade_id)")
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_trade_features_window ON trade_features (closed_at DESC, symbol)"
        )

        conn.execute("PRAGMA user_version = 24")
        conn.commit()

    def _migrate_v25(self, conn: sqlite3.Connection) -> None:
        """Add normalized LLM entry-gate decision journal table (v25)."""
        version = conn.execute("PRAGMA user_version").fetchone()[0]
        if version >= 25:
            return

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS gate_decisions_v1 (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                cycle_seq INTEGER NOT NULL,
                symbol TEXT NOT NULL,
                direction TEXT NOT NULL,
                decision TEXT NOT NULL,
                reason TEXT,
                candidate_rank INTEGER,
                conviction REAL,
                blended_conviction REAL,
                pipeline_conviction REAL,
                brain_conviction REAL,
                llm_agent_id TEXT,
                llm_model TEXT,
                llm_session_id TEXT,
                created_at REAL DEFAULT (strftime('%s', 'now'))
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_gate_decisions_cycle ON gate_decisions_v1 (cycle_seq, created_at)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_gate_decisions_symbol ON gate_decisions_v1 (symbol, created_at)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_gate_decisions_decision ON gate_decisions_v1 (decision, created_at)"
        )
        conn.execute("PRAGMA user_version = 25")
        conn.commit()

    def _migrate_v26(self, conn: sqlite3.Connection) -> None:
        """Add gate decision linkage + outcome columns (v26)."""
        version = conn.execute("PRAGMA user_version").fetchone()[0]
        if version >= 26:
            return

        cols = {row[1] for row in conn.execute("PRAGMA table_info(gate_decisions_v1)").fetchall()}
        for name, ddl in (
            ("proposal_id", "ALTER TABLE gate_decisions_v1 ADD COLUMN proposal_id INTEGER"),
            ("venue", "ALTER TABLE gate_decisions_v1 ADD COLUMN venue TEXT"),
            ("trade_id", "ALTER TABLE gate_decisions_v1 ADD COLUMN trade_id INTEGER"),
            ("outcome_status", "ALTER TABLE gate_decisions_v1 ADD COLUMN outcome_status TEXT"),
            ("outcome_pnl", "ALTER TABLE gate_decisions_v1 ADD COLUMN outcome_pnl REAL"),
            ("outcome_pnl_pct", "ALTER TABLE gate_decisions_v1 ADD COLUMN outcome_pnl_pct REAL"),
            ("outcome_closed_at", "ALTER TABLE gate_decisions_v1 ADD COLUMN outcome_closed_at REAL"),
            ("updated_at", "ALTER TABLE gate_decisions_v1 ADD COLUMN updated_at REAL DEFAULT (strftime('%s', 'now'))"),
        ):
            if name not in cols:
                try:
                    conn.execute(ddl)
                except Exception:
                    pass

        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_gate_decisions_trade ON gate_decisions_v1 (trade_id)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_gate_decisions_proposal ON gate_decisions_v1 (proposal_id)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_gate_decisions_outcome ON gate_decisions_v1 (decision, outcome_status, created_at)"
        )

        conn.execute("PRAGMA user_version = 26")
        conn.commit()

    def _migrate_v27(self, conn: sqlite3.Connection) -> None:
        """Add normalized execution attempt telemetry table (v27)."""
        version = conn.execute("PRAGMA user_version").fetchone()[0]
        if version >= 27:
            return

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS execution_attempts_v1 (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                proposal_id INTEGER,
                trade_id INTEGER,
                symbol TEXT,
                venue TEXT,
                action TEXT NOT NULL DEFAULT 'entry',
                expected_price REAL,
                filled_price REAL,
                slippage_pct REAL,
                requested_size REAL,
                filled_size REAL,
                elapsed_ms REAL,
                fallback_used INTEGER DEFAULT 0,
                success INTEGER NOT NULL DEFAULT 0,
                error_code TEXT,
                error_text TEXT,
                created_at REAL DEFAULT (strftime('%s', 'now')),
                FOREIGN KEY (proposal_id) REFERENCES trade_proposals(id),
                FOREIGN KEY (trade_id) REFERENCES trades(id)
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_exec_attempts_proposal ON execution_attempts_v1 (proposal_id, created_at)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_exec_attempts_symbol ON execution_attempts_v1 (symbol, venue, created_at)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_exec_attempts_success ON execution_attempts_v1 (success, created_at)"
        )
        conn.execute("PRAGMA user_version = 27")
        conn.commit()

    def _migrate_v28(self, conn: sqlite3.Connection) -> None:
        """Add SL/TP incident lifecycle table (v28)."""
        version = conn.execute("PRAGMA user_version").fetchone()[0]
        if version >= 28:
            return

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS sltp_incidents_v1 (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                trade_id INTEGER,
                symbol TEXT NOT NULL,
                venue TEXT,
                leg TEXT NOT NULL,
                attempts INTEGER DEFAULT 0,
                last_error TEXT,
                state TEXT NOT NULL DEFAULT 'OPEN',
                opened_at REAL DEFAULT (strftime('%s', 'now')),
                resolved_at REAL,
                resolution_note TEXT,
                created_at REAL DEFAULT (strftime('%s', 'now')),
                updated_at REAL DEFAULT (strftime('%s', 'now')),
                FOREIGN KEY (trade_id) REFERENCES trades(id)
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_sltp_incidents_trade_leg ON sltp_incidents_v1 (trade_id, leg, state)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_sltp_incidents_state ON sltp_incidents_v1 (state, opened_at)"
        )
        conn.execute("PRAGMA user_version = 28")
        conn.commit()

    def _migrate_v29(self, conn: sqlite3.Connection) -> None:
        """Add builder-specific diagnostics columns to execution attempts (v29)."""
        version = conn.execute("PRAGMA user_version").fetchone()[0]
        if version >= 29:
            return

        cols = {
            row[1] for row in conn.execute("PRAGMA table_info(execution_attempts_v1)").fetchall()
        }
        if "is_builder_symbol" not in cols:
            conn.execute(
                "ALTER TABLE execution_attempts_v1 ADD COLUMN is_builder_symbol INTEGER DEFAULT 0"
            )
        if "builder_error_category" not in cols:
            conn.execute(
                "ALTER TABLE execution_attempts_v1 ADD COLUMN builder_error_category TEXT"
            )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_exec_attempts_builder ON execution_attempts_v1 (is_builder_symbol, builder_error_category, created_at)"
        )
        conn.execute("PRAGMA user_version = 29")
        conn.commit()

    def _migrate_v30(self, conn: sqlite3.Connection) -> None:
        """Add exit decision outcome/regret tracking table (v30)."""
        version = conn.execute("PRAGMA user_version").fetchone()[0]
        if version >= 30:
            return

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS exit_decision_outcomes_v1 (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                decision_rowid INTEGER NOT NULL,
                source_plan_id INTEGER,
                trade_id INTEGER NOT NULL,
                symbol TEXT NOT NULL,
                venue TEXT NOT NULL,
                action TEXT NOT NULL,
                outcome_kind TEXT NOT NULL,
                horizon_sec INTEGER NOT NULL,
                decision_ts REAL NOT NULL,
                evaluated_ts REAL NOT NULL,
                decision_mark REAL,
                horizon_mark REAL,
                mark_move_pct REAL,
                meta_json TEXT,
                created_at REAL DEFAULT (strftime('%s', 'now')),
                UNIQUE(decision_rowid, horizon_sec)
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_exit_outcomes_trade ON exit_decision_outcomes_v1 (trade_id, evaluated_ts DESC)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_exit_outcomes_kind ON exit_decision_outcomes_v1 (outcome_kind, evaluated_ts DESC)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_exit_outcomes_symbol ON exit_decision_outcomes_v1 (symbol, venue, evaluated_ts DESC)"
        )
        conn.execute("PRAGMA user_version = 30")
        conn.commit()

    def _migrate_v31(self, conn: sqlite3.Connection) -> None:
        """Add order_type_source to trade_features (v31)."""
        version = conn.execute("PRAGMA user_version").fetchone()[0]
        if version >= 31:
            return

        cols = {row[1] for row in conn.execute("PRAGMA table_info(trade_features)").fetchall()}
        if "order_type_source" not in cols:
            conn.execute("ALTER TABLE trade_features ADD COLUMN order_type_source TEXT")

        conn.execute("PRAGMA user_version = 31")
        conn.commit()

    def _migrate_v32(self, conn: sqlite3.Connection) -> None:
        """Add HIP3 segmentation/features to trade_features (v32)."""
        version = conn.execute("PRAGMA user_version").fetchone()[0]
        if version >= 32:
            return

        cols = {row[1] for row in conn.execute("PRAGMA table_info(trade_features)").fetchall()}
        if "strategy_segment" not in cols:
            conn.execute("ALTER TABLE trade_features ADD COLUMN strategy_segment TEXT")
        if "entry_gate_mode" not in cols:
            conn.execute("ALTER TABLE trade_features ADD COLUMN entry_gate_mode TEXT")
        if "hip3_driver" not in cols:
            conn.execute("ALTER TABLE trade_features ADD COLUMN hip3_driver TEXT")
        if "hip3_flow_pass" not in cols:
            conn.execute("ALTER TABLE trade_features ADD COLUMN hip3_flow_pass INTEGER")
        if "hip3_ofm_pass" not in cols:
            conn.execute("ALTER TABLE trade_features ADD COLUMN hip3_ofm_pass INTEGER")
        if "hip3_booster_score" not in cols:
            conn.execute("ALTER TABLE trade_features ADD COLUMN hip3_booster_score REAL")
        if "hip3_booster_size_mult" not in cols:
            conn.execute("ALTER TABLE trade_features ADD COLUMN hip3_booster_size_mult REAL")

        conn.execute("PRAGMA user_version = 32")
        conn.commit()

    def _migrate_v33(self, conn: sqlite3.Connection) -> None:
        """Add manual trade plan tables (v33).

        These are used by OpenClaw /trade + /execute advisor flow.
        """
        version = conn.execute("PRAGMA user_version").fetchone()[0]
        if version >= 33:
            return

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS manual_plan_counters (
                symbol TEXT PRIMARY KEY,
                next_seq INTEGER NOT NULL
            )
            """
        )

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS manual_trade_plans (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                display_id TEXT NOT NULL UNIQUE,
                symbol TEXT NOT NULL,
                seq INTEGER NOT NULL,
                status TEXT NOT NULL DEFAULT 'DRAFT',
                created_at REAL NOT NULL,
                expires_at REAL NOT NULL,
                plan_json TEXT,
                executed_at REAL,
                executed_option TEXT,
                executed_trade_id INTEGER,
                error TEXT
            )
            """
        )

        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_manual_trade_plans_symbol_created ON manual_trade_plans(symbol, created_at)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_manual_trade_plans_expires ON manual_trade_plans(expires_at)"
        )

        conn.execute("PRAGMA user_version = 33")
        conn.commit()

    def _migrate_v34(self, conn: sqlite3.Connection) -> None:
        """Add strategy/pair_id columns to trades for attribution (v34).

        Required by manual trade advisor flow and by core trade logging.
        """
        version = conn.execute("PRAGMA user_version").fetchone()[0]
        if version >= 34:
            return

        cols = {row[1] for row in conn.execute("PRAGMA table_info(trades)").fetchall()}
        if "strategy" not in cols:
            conn.execute("ALTER TABLE trades ADD COLUMN strategy TEXT NOT NULL DEFAULT 'core'")
        if "pair_id" not in cols:
            conn.execute("ALTER TABLE trades ADD COLUMN pair_id TEXT")

        conn.execute("PRAGMA user_version = 34")
        conn.commit()

    def _migrate_v35(self, conn: sqlite3.Connection) -> None:
        """Repair gate_decisions_v1 linkage schema/backfills (v35).

        Historical DBs can have gate_decisions_v1 without updated_at, which breaks
        proposal/trade linkage updates and silently drops observability.
        """
        version = conn.execute("PRAGMA user_version").fetchone()[0]
        if version >= 35:
            return

        gate_cols = {row[1] for row in conn.execute("PRAGMA table_info(gate_decisions_v1)").fetchall()}
        if "updated_at" not in gate_cols:
            try:
                # Use a simple nullable column to stay compatible across SQLite builds.
                conn.execute("ALTER TABLE gate_decisions_v1 ADD COLUMN updated_at REAL")
            except Exception:
                pass

        # Backfill proposal linkage from proposal metadata gate_decision_id.
        try:
            conn.execute(
                """
                WITH proposal_map AS (
                    SELECT
                        CAST(json_extract(pm.meta_json, '$.gate_decision_id') AS INTEGER) AS gate_decision_id,
                        MAX(p.id) AS proposal_id
                    FROM proposal_metadata pm
                    JOIN trade_proposals p ON p.id = pm.proposal_id
                    WHERE json_extract(pm.meta_json, '$.gate_decision_id') IS NOT NULL
                    GROUP BY CAST(json_extract(pm.meta_json, '$.gate_decision_id') AS INTEGER)
                )
                UPDATE gate_decisions_v1
                SET
                    proposal_id = COALESCE(
                        proposal_id,
                        (
                            SELECT m.proposal_id
                            FROM proposal_map m
                            WHERE m.gate_decision_id = gate_decisions_v1.id
                        )
                    )
                WHERE proposal_id IS NULL
                  AND id IN (SELECT gate_decision_id FROM proposal_map)
                """
            )
            conn.execute(
                """
                UPDATE gate_decisions_v1
                SET venue = COALESCE(
                    NULLIF(venue, ''),
                    (SELECT LOWER(p.venue) FROM trade_proposals p WHERE p.id = gate_decisions_v1.proposal_id)
                )
                WHERE proposal_id IS NOT NULL
                  AND (venue IS NULL OR venue = '')
                """
            )
        except Exception:
            pass

        # Backfill trade/proposal linkage from trade context entry_gate metadata.
        try:
            conn.execute(
                """
                WITH trade_map AS (
                    SELECT
                        CAST(json_extract(t.context_snapshot, '$.entry_gate.gate_decision_id') AS INTEGER) AS gate_decision_id,
                        MAX(t.id) AS trade_id
                    FROM trades t
                    WHERE json_extract(t.context_snapshot, '$.entry_gate.gate_decision_id') IS NOT NULL
                    GROUP BY CAST(json_extract(t.context_snapshot, '$.entry_gate.gate_decision_id') AS INTEGER)
                )
                UPDATE gate_decisions_v1
                SET
                    trade_id = COALESCE(
                        trade_id,
                        (
                            SELECT m.trade_id
                            FROM trade_map m
                            WHERE m.gate_decision_id = gate_decisions_v1.id
                        )
                    )
                WHERE trade_id IS NULL
                  AND id IN (SELECT gate_decision_id FROM trade_map)
                """
            )
            conn.execute(
                """
                UPDATE gate_decisions_v1
                SET
                    proposal_id = COALESCE(
                        proposal_id,
                        (
                            SELECT CAST(json_extract(t.context_snapshot, '$.entry_gate.proposal_id') AS INTEGER)
                            FROM trades t
                            WHERE t.id = gate_decisions_v1.trade_id
                        )
                    ),
                    venue = COALESCE(
                        NULLIF(venue, ''),
                        (
                            SELECT LOWER(COALESCE(json_extract(t.context_snapshot, '$.entry_gate.venue'), t.venue))
                            FROM trades t
                            WHERE t.id = gate_decisions_v1.trade_id
                        )
                    )
                WHERE trade_id IS NOT NULL
                  AND (proposal_id IS NULL OR venue IS NULL OR venue = '')
                """
            )
        except Exception:
            pass

        try:
            conn.execute(
                """
                UPDATE gate_decisions_v1
                SET updated_at = COALESCE(updated_at, created_at, strftime('%s','now'))
                WHERE updated_at IS NULL
                """
            )
        except Exception:
            pass

        conn.execute("PRAGMA user_version = 35")
        conn.commit()

    def _migrate_v36(self, conn: sqlite3.Connection) -> None:
        """Ensure symbol_policy table exists for all DB histories (v36)."""
        version = conn.execute("PRAGMA user_version").fetchone()[0]
        if version >= 36:
            return

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS symbol_policy (
                symbol TEXT PRIMARY KEY,
                sl_mult_adjustment REAL DEFAULT 1.0,
                tp_mult_adjustment REAL DEFAULT 1.0,
                size_adjustment REAL DEFAULT 1.0,
                stop_out_rate REAL DEFAULT 0.0,
                win_rate REAL DEFAULT 0.5,
                samples INTEGER DEFAULT 0,
                last_updated REAL,
                notes TEXT
            )
            """
        )

        conn.execute("PRAGMA user_version = 36")
        conn.commit()

    # =========================================================================
    # Trade Methods
    # =========================================================================

    def log_trade_entry(
        self,
        symbol: str,
        direction: str,
        entry_price: float,
        size: float,
        venue: str = "lighter",
        state: str = "ACTIVE",
        sl_price: Optional[float] = None,
        tp_price: Optional[float] = None,
        sl_order_id: Optional[str] = None,
        tp_order_id: Optional[str] = None,
        signals_snapshot: Optional[Dict] = None,
        signals_agreed: Optional[List[str]] = None,
        ai_reasoning: Optional[str] = None,
        confidence: Optional[str] = None,
        size_multiplier: Optional[float] = None,
        context_snapshot: Optional[Dict] = None,
        protection_snapshot: Optional[Dict] = None,
        safety_tier: Optional[int] = None,
        # AGI trader fields (v10)
        risk_pct_used: Optional[float] = None,
        equity_at_entry: Optional[float] = None,
        agi_model_version: Optional[str] = None,
        # Tagging / attribution
        strategy: Optional[str] = None,
        pair_id: Optional[str] = None,
    ) -> int:
        """
        Log a new trade entry.

        Returns:
            trade_id
        """
        notional_usd = entry_price * size
        venue_norm = normalize_venue(str(venue or "").strip().lower()) or str(venue or "").strip().lower() or "lighter"

        with self._get_connection() as conn:
            try:
                cursor = conn.execute("""
                    INSERT INTO trades (
                        symbol, direction, venue, entry_time, entry_price, size, notional_usd,
                        sl_price, tp_price, sl_order_id, tp_order_id, state,
                        signals_snapshot, signals_agreed, ai_reasoning, confidence,
                        size_multiplier, context_snapshot, protection_snapshot, safety_tier,
                        risk_pct_used, equity_at_entry, agi_model_version,
                        strategy, pair_id
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    symbol.upper(),
                    direction.upper(),
                    venue_norm,
                    time.time(),
                    entry_price,
                    size,
                    notional_usd,
                    sl_price,
                    tp_price,
                    sl_order_id,
                    tp_order_id,
                    state,
                    json.dumps(signals_snapshot) if signals_snapshot is not None else None,
                    # Always store a JSON list ("[]") rather than NULL so we have consistent
                    # attribution fields for performance analysis.
                    json.dumps(list(signals_agreed or [])),
                    ai_reasoning,
                    confidence,
                    size_multiplier,
                    json.dumps(context_snapshot) if context_snapshot is not None else None,
                    json.dumps(protection_snapshot) if protection_snapshot is not None else None,
                    safety_tier,
                    risk_pct_used,
                    equity_at_entry,
                    agi_model_version,
                    str(strategy or "core"),
                    (str(pair_id) if pair_id is not None else None),
                ))
                trade_id = int(cursor.lastrowid)
                conn.commit()
            except sqlite3.IntegrityError:
                # Duplicate open trade row (symbol, venue) — return existing instead of creating churn.
                conn.rollback()
                row = conn.execute(
                    """
                    SELECT id FROM trades
                    WHERE symbol = ? AND venue = ? AND exit_time IS NULL
                    ORDER BY entry_time DESC
                    LIMIT 1
                    """,
                    (symbol.upper(), venue_norm),
                ).fetchone()
                if row:
                    trade_id = int(row[0])
                    self.log.warning(
                        f"Duplicate open trade prevented for {symbol} on {venue_norm}; "
                        f"reusing trade_id={trade_id} without overwriting entry fields"
                    )
                else:
                    raise

        # Best-effort linkage so gate decisions can be joined to proposal sizing
        # and realized trades immediately after entry.
        gate_decision_id: Optional[int] = None
        proposal_id: Optional[int] = None
        gate_venue: Optional[str] = None
        try:
            ctx_obj: Any = context_snapshot
            if isinstance(ctx_obj, str):
                ctx_obj = json.loads(ctx_obj)
            if isinstance(ctx_obj, dict):
                entry_gate = ctx_obj.get("entry_gate")
                entry_gate = dict(entry_gate) if isinstance(entry_gate, dict) else {}
                gate_raw = entry_gate.get("gate_decision_id") or entry_gate.get("decision_id")
                proposal_raw = entry_gate.get("proposal_id")
                venue_raw = entry_gate.get("venue") or venue_norm
                if gate_raw is not None:
                    try:
                        gate_decision_id = int(gate_raw)
                    except Exception:
                        gate_decision_id = None
                if proposal_raw is not None:
                    try:
                        proposal_id = int(proposal_raw)
                    except Exception:
                        proposal_id = None
                gate_venue = normalize_venue(str(venue_raw or "").lower()) or None
        except Exception:
            gate_decision_id = None
            proposal_id = None
            gate_venue = None

        if gate_decision_id is not None:
            if proposal_id is not None:
                try:
                    self.link_gate_decision_to_proposal(
                        gate_decision_id=int(gate_decision_id),
                        proposal_id=int(proposal_id),
                        venue=gate_venue,
                    )
                except Exception as exc:
                    self.log.warning(
                        "gate proposal link failed after trade insert: gate_decision_id=%s proposal_id=%s trade_id=%s err=%s",
                        gate_decision_id,
                        proposal_id,
                        trade_id,
                        exc,
                    )
            try:
                self.link_gate_decision_to_trade(
                    gate_decision_id=int(gate_decision_id),
                    trade_id=int(trade_id),
                )
            except Exception as exc:
                self.log.warning(
                    "gate trade link failed after trade insert: gate_decision_id=%s trade_id=%s err=%s",
                    gate_decision_id,
                    trade_id,
                    exc,
                )

        self.log.info(f"Trade entry logged: {symbol} {direction} on {venue} @ ${entry_price:.2f} (ID: {trade_id})")
        return int(trade_id)

    def update_trade_position(
        self,
        trade_id: int,
        size: Optional[float] = None,
        entry_price: Optional[float] = None,
    ) -> bool:
        """Update size/entry_price for an open trade and recompute notional."""
        with self._get_connection() as conn:
            cursor = conn.execute(
                "SELECT size, entry_price FROM trades WHERE id = ? AND exit_time IS NULL",
                (trade_id,)
            )
            row = cursor.fetchone()
            if not row:
                return False

            new_size = size if size is not None else row['size']
            new_entry = entry_price if entry_price is not None else row['entry_price']
            notional_usd = new_size * new_entry

            cursor = conn.execute("""
                UPDATE trades SET
                    size = ?,
                    entry_price = ?,
                    notional_usd = ?,
                    updated_at = strftime('%s', 'now')
                WHERE id = ? AND exit_time IS NULL
            """, (new_size, new_entry, notional_usd, trade_id))
            conn.commit()
            return cursor.rowcount > 0

    def update_trade_state(self, trade_id: int, state: str) -> bool:
        """Update tracking state for an open trade."""
        with self._get_connection() as conn:
            cursor = conn.execute("""
                UPDATE trades SET
                    state = ?,
                    updated_at = strftime('%s', 'now')
                WHERE id = ? AND exit_time IS NULL
            """, (state, trade_id))
            conn.commit()
            return cursor.rowcount > 0

    def update_trade_sltp(
        self,
        trade_id: int,
        sl_price: Optional[float] = None,
        tp_price: Optional[float] = None,
        sl_order_id: Optional[str] = None,
        tp_order_id: Optional[str] = None,
        protection_snapshot: Optional[dict] = None,
    ) -> bool:
        """Update SL/TP prices and order IDs for a trade."""
        with self._get_connection() as conn:
            ps_json = None
            if protection_snapshot is not None:
                try:
                    ps_json = json.dumps(protection_snapshot)
                except Exception:
                    ps_json = None

            cursor = conn.execute("""
                UPDATE trades SET
                    sl_price = COALESCE(?, sl_price),
                    tp_price = COALESCE(?, tp_price),
                    sl_order_id = COALESCE(?, sl_order_id),
                    tp_order_id = COALESCE(?, tp_order_id),
                    protection_snapshot = COALESCE(?, protection_snapshot),
                    updated_at = strftime('%s', 'now')
                WHERE id = ? AND exit_time IS NULL
            """, (sl_price, tp_price, sl_order_id, tp_order_id, ps_json, trade_id))
            conn.commit()
            return cursor.rowcount > 0


    def set_trade_sltp(
        self,
        trade_id: int,
        sl_price: Optional[float] = None,
        tp_price: Optional[float] = None,
        sl_order_id: Optional[str] = None,
        tp_order_id: Optional[str] = None,
        protection_snapshot: Optional[dict] = None,
    ) -> bool:
        """Set SL/TP prices and order IDs for a trade (OVERWRITE, allows clearing).

        NOTE: update_trade_sltp() uses COALESCE to avoid overwriting existing values.
        This helper is for recovery flows (e.g., restore SL/TP after partial close timeout)
        where we must replace stale order ids in the DB.
        """
        with self._get_connection() as conn:
            ps_json = None
            if protection_snapshot is not None:
                try:
                    ps_json = json.dumps(protection_snapshot)
                except Exception:
                    ps_json = None

            cur = conn.execute(
                """
                UPDATE trades SET
                    sl_price = COALESCE(?, sl_price),
                    tp_price = COALESCE(?, tp_price),
                    sl_order_id = ?,
                    tp_order_id = ?,
                    protection_snapshot = COALESCE(?, protection_snapshot),
                    updated_at = strftime('%s', 'now')
                WHERE id = ? AND exit_time IS NULL
                """,
                (sl_price, tp_price, sl_order_id, tp_order_id, ps_json, trade_id),
            )
            conn.commit()
            return cur.rowcount > 0

    def update_trade_mae_mfe(
        self,
        trade_id: int,
        mae_pct: float,
        mfe_pct: float,
        mae_price: Optional[float] = None,
        mfe_price: Optional[float] = None,
    ) -> bool:
        """Persist MAE/MFE metrics for a trade with non-negative guards."""
        try:
            mae = max(0.0, float(mae_pct))
            mfe = max(0.0, float(mfe_pct))
        except Exception:
            return False

        try:
            mae_px = float(mae_price) if mae_price is not None else None
        except Exception:
            mae_px = None
        try:
            mfe_px = float(mfe_price) if mfe_price is not None else None
        except Exception:
            mfe_px = None

        with self._get_connection() as conn:
            cur = conn.execute(
                """
                UPDATE trades
                SET mae_pct = ?, mfe_pct = ?, mae_price = ?, mfe_price = ?,
                    updated_at = strftime('%s', 'now')
                WHERE id = ?
                """,
                (mae, mfe, mae_px, mfe_px, int(trade_id)),
            )
            conn.commit()
            return cur.rowcount > 0

    def set_pending_exit(
        self,
        trade_id: int,
        reason: Optional[str],
        detail: str = "",
        ts: Optional[float] = None,
    ) -> bool:
        """Persist the *intent* to close so reconciler can apply it later."""
        ts_val = float(ts if ts is not None else time.time())
        with self._get_connection() as conn:
            cur = conn.execute(
                """
                UPDATE trades
                SET pending_exit_reason = ?, pending_exit_detail = ?, pending_exit_set_at = ?,
                    updated_at = strftime('%s', 'now')
                WHERE id = ? AND exit_time IS NULL
                """,
                (
                    (str(reason).upper() if reason else None),
                    (str(detail or "") or ""),
                    ts_val,
                    int(trade_id),
                ),
            )
            conn.commit()
            return cur.rowcount > 0

    def clear_pending_exit(self, trade_id: int) -> bool:
        """Clear pending exit intent for an open trade."""
        with self._get_connection() as conn:
            cur = conn.execute(
                """
                UPDATE trades
                SET pending_exit_reason = NULL,
                    pending_exit_detail = NULL,
                    pending_exit_set_at = NULL,
                    updated_at = strftime('%s', 'now')
                WHERE id = ? AND exit_time IS NULL
                """,
                (int(trade_id),),
            )
            conn.commit()
            return cur.rowcount > 0


    def acquire_symbol_lock(
        self,
        symbol: str,
        venue: str,
        owner: str,
        lock_type: str = "",
        reason: str = "",
        ttl_seconds: float = 180.0,
    ) -> bool:
        """Cross-process guard: one in-flight action per (symbol, venue)."""
        now = time.time()
        exp = now + float(ttl_seconds or 0)
        sym = str(symbol or "").upper()
        ven = normalize_venue(venue or "")
        if not sym or not ven:
            return False

        with self._get_connection() as conn:
            conn.execute("BEGIN IMMEDIATE")
            # Clear expired locks
            conn.execute("DELETE FROM symbol_locks WHERE expires_at < ?", (now,))

            # Re-entrant acquire: if we already hold it, extend TTL.
            row = conn.execute(
                "SELECT owner FROM symbol_locks WHERE symbol=? AND venue=?",
                (sym, ven),
            ).fetchone()
            if row and str(row[0] or "") == str(owner):
                conn.execute(
                    "UPDATE symbol_locks SET expires_at=?, lock_type=?, reason=? WHERE symbol=? AND venue=?",
                    (exp, lock_type, reason, sym, ven),
                )
                conn.commit()
                return True

            try:
                conn.execute(
                    """
                    INSERT INTO symbol_locks (symbol, venue, owner, lock_type, reason, acquired_at, expires_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (sym, ven, owner, lock_type, reason, now, exp),
                )
                conn.commit()
                return True
            except sqlite3.IntegrityError:
                return False

    def release_symbol_lock(self, symbol: str, venue: str, owner: Optional[str] = None) -> bool:
        sym = str(symbol or "").upper()
        ven = normalize_venue(venue or "")
        if not sym or not ven:
            return False

        with self._get_connection() as conn:
            if owner:
                cur = conn.execute(
                    "DELETE FROM symbol_locks WHERE symbol=? AND venue=? AND owner=?",
                    (sym, ven, str(owner)),
                )
            else:
                cur = conn.execute(
                    "DELETE FROM symbol_locks WHERE symbol=? AND venue=?",
                    (sym, ven),
                )
            conn.commit()
            return cur.rowcount > 0

    def get_symbol_lock(self, symbol: str, venue: str) -> Optional[Dict[str, Any]]:
        sym = str(symbol or "").upper()
        ven = normalize_venue(venue or "")
        if not sym or not ven:
            return None

        with self._get_connection() as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                "SELECT * FROM symbol_locks WHERE symbol=? AND venue=? AND expires_at >= ?",
                (sym, ven, time.time()),
            ).fetchone()
            return dict(row) if row else None

    def log_trade_exit(
        self,
        trade_id: int,
        exit_price: float,
        exit_reason: str,
        total_fees: float = 0.0,
    ) -> Optional[float]:
        """
        Log trade exit and calculate PnL.

        Returns:
            Net PnL (after fees) or None if trade not found/already closed
        """
        with self._get_connection() as conn:
            # Get trade info
            cursor = conn.execute(
                "SELECT direction, entry_price, size FROM trades WHERE id = ? AND exit_time IS NULL",
                (trade_id,)
            )
            row = cursor.fetchone()

            if not row:
                self.log.warning(f"Trade {trade_id} not found or already closed")
                return None

            direction, entry_price, size = row['direction'], row['entry_price'], row['size']

            # Calculate PnL with price guards
            if entry_price <= 0 or exit_price <= 0:
                self.log.warning(f"Invalid prices for trade {trade_id}")
                gross_pnl = 0.0
                gross_pct = 0.0
            elif direction == 'LONG':
                gross_pnl = (exit_price - entry_price) * size
                gross_pct = ((exit_price / entry_price) - 1) * 100
            else:  # SHORT
                gross_pnl = (entry_price - exit_price) * size
                gross_pct = ((entry_price - exit_price) / entry_price) * 100

            net_pnl = gross_pnl - total_fees
            notional = float(entry_price or 0.0) * float(size or 0.0)
            net_pct = (net_pnl / notional * 100.0) if notional > 0 else 0.0

            # Update trade
            cursor = conn.execute("""
                UPDATE trades SET
                    exit_time = ?,
                    exit_price = ?,
                    exit_reason = ?,
                    realized_pnl = ?,
                    realized_pnl_pct = ?,
                    total_fees = ?,
                    pending_exit_reason = NULL,
                    pending_exit_detail = NULL,
                    pending_exit_set_at = NULL,
                    updated_at = strftime('%s', 'now')
                WHERE id = ? AND exit_time IS NULL
            """, (
                time.time(),
                exit_price,
                exit_reason.upper(),
                net_pnl,
                net_pct,
                total_fees,
                trade_id,
            ))

            if cursor.rowcount == 0:
                return None

            conn.commit()

        # Best-effort: if this was a decay-driven exit, persist a decision record.
        try:
            if str(exit_reason or "").upper() in {"DECAY_EXIT", "MAX_HOLD"}:
                # Pull venue/symbol for auditing
                trade = self.get_trade(trade_id)
                if trade:
                    self.record_decay_decision(
                        symbol=trade.symbol,
                        venue=trade.venue,
                        trade_id=trade_id,
                        action="CLOSE",
                        reason=str(exit_reason).upper(),
                        detail="",
                        decided_by=None,
                        source="trade_exit",
                        ts=time.time(),
                    )
        except Exception:
            pass

        self.log.info(
            f"Trade {trade_id} closed: {exit_reason} @ ${exit_price:.2f}, "
            f"net=${net_pnl:+.2f} ({net_pct:+.2f}%), gross_pct={gross_pct:+.2f}%"
        )
        return net_pnl

    # ---------------------------------------------------------------------
    # Decay tracking
    # ---------------------------------------------------------------------

    def record_decay_flag(
        self,
        *,
        symbol: str,
        venue: str,
        trade_id: Optional[int],
        db_direction: Optional[str],
        live_direction: Optional[str],
        reason: str,
        detail: str = "",
        context_path: Optional[str] = None,
        context_age_seconds: Optional[float] = None,
        context_generated_at: Optional[float] = None,
        context_direction: Optional[str] = None,
        signal_flip_only: bool = False,
        notify_only: bool = False,
        ts: Optional[float] = None,
    ) -> int:
        """Insert a decay flag event (audit trail). Returns inserted row id."""
        ts_val = float(ts or time.time())
        with self._get_connection() as conn:
            cur = conn.execute(
                """
                INSERT INTO decay_flags (
                    ts, symbol, venue, trade_id,
                    db_direction, live_direction,
                    reason, detail,
                    context_path, context_age_seconds, context_generated_at, context_direction,
                    signal_flip_only, notify_only
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    ts_val,
                    symbol.upper(),
                    normalize_venue(venue or ""),
                    trade_id,
                    (db_direction or None),
                    (live_direction or None),
                    str(reason or "").upper(),
                    str(detail or ""),
                    context_path,
                    context_age_seconds,
                    context_generated_at,
                    (context_direction or None),
                    1 if signal_flip_only else 0,
                    1 if notify_only else 0,
                ),
            )
            conn.commit()
            return int(cur.lastrowid)

    def record_decay_decision(
        self,
        *,
        symbol: str,
        venue: str,
        trade_id: Optional[int],
        source_plan_id: Optional[int] = None,
        action: str,
        reason: Optional[str] = None,
        detail: str = "",
        decided_by: Optional[str] = None,
        source: str = "manual",
        ts: Optional[float] = None,
        dedupe_seconds: Optional[float] = None,
    ) -> int:
        """Insert a decay decision (CLOSE/HOLD).

        If `dedupe_seconds` is provided, avoids spamming identical decisions by
        returning the most recent matching decision id when the last matching
        row is within the dedupe window.
        """
        ts_val = float(ts or time.time())
        sym = symbol.upper()
        ven = normalize_venue(venue or "")
        act = str(action or "").upper()
        rsn = str(reason).upper() if reason else None
        det = str(detail or "")
        spid = int(source_plan_id) if source_plan_id is not None else None

        with self._get_connection() as conn:
            if dedupe_seconds and float(dedupe_seconds) > 0:
                cutoff = ts_val - float(dedupe_seconds)
                row = conn.execute(
                    """
                    SELECT id
                    FROM decay_decisions
                    WHERE symbol = ? AND venue = ?
                      AND action = ?
                      AND source = ?
                      AND COALESCE(source_plan_id, -1) = COALESCE(?, -1)
                      AND COALESCE(reason,'') = COALESCE(?, '')
                      AND COALESCE(detail,'') = COALESCE(?, '')
                      AND ts >= ?
                    ORDER BY ts DESC
                    LIMIT 1
                    """,
                    (sym, ven, act, source, spid, rsn, det, cutoff),
                ).fetchone()
                if row:
                    return int(row[0])

            cur = conn.execute(
                """
                INSERT INTO decay_decisions (
                    ts, symbol, venue, trade_id, source_plan_id,
                    action, reason, detail, decided_by, source
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    ts_val,
                    sym,
                    ven,
                    trade_id,
                    spid,
                    act,
                    rsn,
                    det,
                    decided_by,
                    source,
                ),
            )
            conn.commit()
            return int(cur.lastrowid)

    def record_exit_decision_outcome(
        self,
        *,
        decision_rowid: int,
        source_plan_id: Optional[int],
        trade_id: int,
        symbol: str,
        venue: str,
        action: str,
        outcome_kind: str,
        horizon_sec: int,
        decision_ts: float,
        evaluated_ts: Optional[float] = None,
        decision_mark: Optional[float] = None,
        horizon_mark: Optional[float] = None,
        mark_move_pct: Optional[float] = None,
        meta_json: Optional[str] = None,
    ) -> int:
        """Upsert an exit decision outcome/regret row keyed by (decision_rowid, horizon_sec)."""
        ev_ts = float(evaluated_ts if evaluated_ts is not None else time.time())
        d_ts = float(decision_ts)
        meta = str(meta_json or "")
        with self._get_connection() as conn:
            conn.execute(
                """
                INSERT INTO exit_decision_outcomes_v1 (
                    decision_rowid, source_plan_id, trade_id, symbol, venue, action,
                    outcome_kind, horizon_sec, decision_ts, evaluated_ts,
                    decision_mark, horizon_mark, mark_move_pct, meta_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(decision_rowid, horizon_sec) DO UPDATE SET
                    source_plan_id = excluded.source_plan_id,
                    trade_id = excluded.trade_id,
                    symbol = excluded.symbol,
                    venue = excluded.venue,
                    action = excluded.action,
                    outcome_kind = excluded.outcome_kind,
                    decision_ts = excluded.decision_ts,
                    evaluated_ts = excluded.evaluated_ts,
                    decision_mark = excluded.decision_mark,
                    horizon_mark = excluded.horizon_mark,
                    mark_move_pct = excluded.mark_move_pct,
                    meta_json = excluded.meta_json
                """,
                (
                    int(decision_rowid),
                    (int(source_plan_id) if source_plan_id is not None else None),
                    int(trade_id),
                    str(symbol or "").upper(),
                    normalize_venue(venue or ""),
                    str(action or "").upper(),
                    str(outcome_kind or "").upper(),
                    int(horizon_sec),
                    d_ts,
                    ev_ts,
                    (float(decision_mark) if decision_mark is not None else None),
                    (float(horizon_mark) if horizon_mark is not None else None),
                    (float(mark_move_pct) if mark_move_pct is not None else None),
                    meta,
                ),
            )
            row = conn.execute(
                """
                SELECT id
                FROM exit_decision_outcomes_v1
                WHERE decision_rowid = ? AND horizon_sec = ?
                LIMIT 1
                """,
                (int(decision_rowid), int(horizon_sec)),
            ).fetchone()
            conn.commit()
            return int(row[0]) if row else 0

    def fetch_unevaluated_exit_decisions(
        self,
        *,
        horizon_sec: int,
        as_of_ts: Optional[float] = None,
        limit: int = 200,
    ) -> List[Dict[str, Any]]:
        """Return decider decisions old enough for outcome evaluation and not yet scored."""
        horizon_i = max(1, int(horizon_sec))
        eligible_ts = float(as_of_ts if as_of_ts is not None else time.time()) - float(horizon_i)
        with self._get_connection() as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                """
                SELECT
                    d.id AS decision_rowid,
                    d.ts AS decision_ts,
                    d.source_plan_id,
                    d.trade_id,
                    d.symbol,
                    d.venue,
                    d.action,
                    d.reason,
                    d.detail,
                    d.source,
                    t.direction AS trade_direction,
                    t.entry_price AS trade_entry_price,
                    t.exit_time AS trade_exit_time,
                    t.exit_reason AS trade_exit_reason,
                    t.exit_price AS trade_exit_price
                FROM decay_decisions d
                LEFT JOIN trades t ON t.id = d.trade_id
                WHERE d.source IN ('hl_exit_decider', 'hl_exit_decider_fail')
                  AND d.action IN ('HOLD', 'CLOSE')
                  AND d.source_plan_id IS NOT NULL
                  AND d.trade_id IS NOT NULL
                  AND d.ts <= ?
                  AND NOT EXISTS (
                    SELECT 1
                    FROM exit_decision_outcomes_v1 o
                    WHERE o.decision_rowid = d.id
                      AND o.horizon_sec = ?
                  )
                ORDER BY d.ts ASC, d.id ASC
                LIMIT ?
                """,
                (eligible_ts, horizon_i, max(1, int(limit))),
            ).fetchall()
            return [dict(r) for r in rows]

    def get_latest_decay_flag(
        self,
        *,
        symbol: str,
        venue: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        """Fetch latest decay flag for symbol (optionally by venue)."""
        with self._get_connection() as conn:
            if venue:
                row = conn.execute(
                    """
                    SELECT * FROM decay_flags
                    WHERE symbol = ? AND venue = ?
                    ORDER BY ts DESC
                    LIMIT 1
                    """,
                    (symbol.upper(), venue.lower()),
                ).fetchone()
            else:
                row = conn.execute(
                    """
                    SELECT * FROM decay_flags
                    WHERE symbol = ?
                    ORDER BY ts DESC
                    LIMIT 1
                    """,
                    (symbol.upper(),),
                ).fetchone()
            return dict(row) if row else None

    def has_recent_decay_flag(self, *, trade_id: int, since_ts: float) -> bool:
        """Return True when a decay flag exists for the trade at/after `since_ts`."""
        with self._get_connection() as conn:
            row = conn.execute(
                """
                SELECT 1
                FROM decay_flags
                WHERE trade_id = ?
                  AND ts >= ?
                LIMIT 1
                """,
                (int(trade_id), float(since_ts)),
            ).fetchone()
            return bool(row)

    def get_trade(self, trade_id: int) -> Optional[TradeRecord]:
        """Get a trade by ID."""
        with self._get_connection() as conn:
            cursor = conn.execute("SELECT * FROM trades WHERE id = ?", (trade_id,))
            row = cursor.fetchone()

            if not row:
                return None

            return self._row_to_trade_record(row)

    def get_open_trades(self, symbol: Optional[str] = None) -> List[TradeRecord]:
        """Get all open trades, optionally filtered by symbol."""
        with self._get_connection() as conn:
            if symbol:
                cursor = conn.execute(
                    "SELECT * FROM trades WHERE exit_time IS NULL AND symbol = ? ORDER BY entry_time DESC",
                    (symbol.upper(),)
                )
            else:
                cursor = conn.execute(
                    "SELECT * FROM trades WHERE exit_time IS NULL ORDER BY entry_time DESC"
                )

            return [self._row_to_trade_record(row) for row in cursor.fetchall()]

    def find_open_trade(self, symbol: str, venue: Optional[str] = None) -> Optional[TradeRecord]:
        """Find open trade for a symbol (optionally scoped to venue)."""
        with self._get_connection() as conn:
            if venue:
                cursor = conn.execute("""
                    SELECT * FROM trades
                    WHERE symbol = ? AND venue = ? AND exit_time IS NULL
                    ORDER BY entry_time DESC
                    LIMIT 1
                """, (symbol.upper(), venue))
            else:
                cursor = conn.execute("""
                    SELECT * FROM trades
                    WHERE symbol = ? AND exit_time IS NULL
                    ORDER BY entry_time DESC
                    LIMIT 1
                """, (symbol.upper(),))
            row = cursor.fetchone()

            return self._row_to_trade_record(row) if row else None

    def get_open_trades_for_key(self, *, symbol: str, venue: str) -> List[TradeRecord]:
        """Get *all* open trades for (symbol, venue), newest first.

        There should be at most one open trade per (symbol, venue) because both
        venues are net-position markets. If multiple rows exist, it indicates a
        tracking bug; callers may self-heal by closing duplicates.
        """
        with self._get_connection() as conn:
            rows = conn.execute(
                """
                SELECT * FROM trades
                WHERE symbol = ? AND venue = ? AND exit_time IS NULL
                ORDER BY entry_time DESC
                """,
                (symbol.upper(), venue),
            ).fetchall()
            return [self._row_to_trade_record(r) for r in rows]

    def get_unprotected_trades(self) -> List[TradeRecord]:
        """Get open trades missing SL/TP protection.

        Returns open trades that are not fully protected yet.

        We include:
        - state='NEEDS_PROTECTION'
        - state in ('SLTP_FAILED','SL_ONLY')
        - any open trade missing SL or TP order id (regardless of state)

        Used by reconciliation/initialization to (re)place SL/TP on discovered
        or previously failed positions.
        """
        with self._get_connection() as conn:
            rows = conn.execute(
                """
                SELECT * FROM trades
                WHERE exit_time IS NULL
                  AND (
                    state = 'NEEDS_PROTECTION'
                    OR state IN ('SLTP_FAILED', 'SL_ONLY')
                    OR sl_order_id IS NULL
                    OR tp_order_id IS NULL
                  )
                ORDER BY entry_time ASC
                """,
            ).fetchall()
            return [self._row_to_trade_record(r) for r in rows]

    def update_trade_direction(self, trade_id: int, direction: str) -> bool:
        """Update direction for an open trade (used for reconciliation self-heal)."""
        direction = str(direction or "").upper()
        if direction not in ("LONG", "SHORT"):
            return False
        with self._get_connection() as conn:
            cur = conn.execute(
                """
                UPDATE trades SET
                    direction = ?,
                    updated_at = strftime('%s', 'now')
                WHERE id = ? AND exit_time IS NULL
                """,
                (direction, int(trade_id)),
            )
            conn.commit()
            return cur.rowcount > 0

    def mark_trade_closed_externally(
        self,
        trade_id: int,
        *,
        exit_reason: str = "EXTERNAL",
        exit_price: Optional[float] = None,
        exit_time: Optional[float] = None,
    ) -> bool:
        """Close an open trade without fills (DB self-heal).

        Used when reconciliation detects duplicate/stale open trade rows.
        We set realized_pnl fields to NULL (unknown) and state to CLOSED_EXTERNALLY.
        """
        exit_reason = str(exit_reason or "EXTERNAL").upper()
        ts = float(exit_time or time.time())
        with self._get_connection() as conn:
            cur = conn.execute(
                """
                UPDATE trades SET
                    exit_time = ?,
                    exit_price = ?,
                    exit_reason = ?,
                    realized_pnl = NULL,
                    realized_pnl_pct = NULL,
                    total_fees = COALESCE(total_fees, 0.0),
                    state = 'CLOSED_EXTERNALLY',
                    pending_exit_reason = NULL,
                    pending_exit_detail = NULL,
                    pending_exit_set_at = NULL,
                    updated_at = strftime('%s', 'now')
                WHERE id = ? AND exit_time IS NULL
                """,
                (ts, exit_price, exit_reason, int(trade_id)),
            )
            conn.commit()
            return cur.rowcount > 0

    def find_open_trade_key(
        self,
        symbol: str,
        venue: str,
        direction: str,
    ) -> Optional[TradeRecord]:
        """Find an open trade by (symbol, venue, direction).

        Used to prevent duplicate trade inserts during reconciliation.
        """
        with self._get_connection() as conn:
            row = conn.execute(
                """
                SELECT * FROM trades
                WHERE symbol = ?
                  AND venue = ?
                  AND direction = ?
                  AND exit_time IS NULL
                ORDER BY entry_time DESC
                LIMIT 1
                """,
                (symbol.upper(), venue, direction.upper()),
            ).fetchone()
            return self._row_to_trade_record(row) if row else None

    # =========================================================================
    # Pending Order Methods (SR-anchored limits v0)
    # =========================================================================

    def insert_pending_order(self, p) -> int:
        """Insert a pending limit order (idempotent on exchange_order_id)."""
        with self._get_connection() as conn:
            cur = conn.execute(
                """
                INSERT INTO pending_orders
                (symbol, venue, direction, limit_price, intended_size, exchange_order_id,
                 sr_level, placed_at, expires_at, entry_direction, state,
                 signals_snapshot, signals_agreed, context_snapshot, conviction, reason,
                 sl_order_id, sl_price)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(exchange_order_id) DO UPDATE SET
                    symbol = excluded.symbol,
                    venue = excluded.venue,
                    direction = excluded.direction,
                    limit_price = excluded.limit_price,
                    intended_size = excluded.intended_size,
                    sr_level = excluded.sr_level,
                    placed_at = excluded.placed_at,
                    expires_at = excluded.expires_at,
                    entry_direction = excluded.entry_direction,
                    state = excluded.state,
                    signals_snapshot = COALESCE(excluded.signals_snapshot, pending_orders.signals_snapshot),
                    signals_agreed = COALESCE(excluded.signals_agreed, pending_orders.signals_agreed),
                    context_snapshot = COALESCE(excluded.context_snapshot, pending_orders.context_snapshot),
                    conviction = COALESCE(excluded.conviction, pending_orders.conviction),
                    reason = COALESCE(excluded.reason, pending_orders.reason),
                    sl_order_id = COALESCE(excluded.sl_order_id, pending_orders.sl_order_id),
                    sl_price = COALESCE(excluded.sl_price, pending_orders.sl_price),
                    updated_at = strftime('%s', 'now')
                """,
                (
                    p.symbol,
                    p.venue,
                    p.direction,
                    p.limit_price,
                    p.intended_size,
                    p.exchange_order_id,
                    p.sr_level,
                    p.placed_at,
                    p.expires_at,
                    p.entry_direction,
                    p.state,
                    getattr(p, "signals_snapshot", None),
                    getattr(p, "signals_agreed", None),
                    getattr(p, "context_snapshot", None),
                    getattr(p, "conviction", None),
                    getattr(p, "reason", None),
                    getattr(p, "sl_order_id", None),
                    getattr(p, "sl_price", None),
                ),
            )
            conn.commit()
            row = conn.execute(
                "SELECT id FROM pending_orders WHERE exchange_order_id = ? ORDER BY id DESC LIMIT 1",
                (getattr(p, "exchange_order_id", None),),
            ).fetchone()
            if row and row[0] is not None:
                return int(row[0])
            return int(cur.lastrowid or 0)

    def update_pending_state(
        self,
        order_id: str,
        state: str,
        reason: Optional[str],
        filled_size: float = 0.0,
        filled_price: float = 0.0,
    ):
        """Update pending order state."""
        with self._get_connection() as conn:
            conn.execute(
                """
                UPDATE pending_orders
                SET state = ?, cancel_reason = ?, filled_size = ?, filled_price = ?,
                    updated_at = strftime('%s', 'now')
                WHERE exchange_order_id = ?
                """,
                (state, reason, filled_size, filled_price, order_id),
            )
            conn.commit()

    @staticmethod
    def _parse_json_field(raw: Any, default: Any = None) -> Any:
        if raw is None:
            return default
        if isinstance(raw, (dict, list)):
            return raw
        try:
            return json.loads(str(raw))
        except Exception:
            return default

    def find_recent_filled_pending_order(
        self,
        symbol: str,
        venue: str,
        around_ts: Optional[float],
        window_sec: float = 600.0,
        *,
        allow_out_of_window_nearest: bool = False,
    ) -> Optional[Dict[str, Any]]:
        """Return nearest FILLED pending order metadata for symbol/venue."""
        sym = str(symbol or "").strip().upper()
        ven = normalize_venue(venue or "")
        if not sym or not ven:
            return None

        try:
            ts = float(around_ts if around_ts is not None else time.time())
        except Exception:
            ts = time.time()
        try:
            win = max(1.0, float(window_sec or 600.0))
        except Exception:
            win = 600.0

        with self._get_connection() as conn:
            row = conn.execute(
                """
                SELECT *
                FROM pending_orders
                WHERE UPPER(symbol) = ?
                  AND LOWER(venue) = ?
                  AND UPPER(COALESCE(state,'')) = 'FILLED'
                  AND ABS(COALESCE(updated_at, placed_at, 0) - ?) <= ?
                ORDER BY ABS(COALESCE(updated_at, placed_at, 0) - ?) ASC
                LIMIT 1
                """,
                (sym, ven, ts, win, ts),
            ).fetchone()
            if not row and allow_out_of_window_nearest:
                row = conn.execute(
                    """
                    SELECT *
                    FROM pending_orders
                    WHERE UPPER(symbol) = ?
                      AND LOWER(venue) = ?
                      AND UPPER(COALESCE(state,'')) = 'FILLED'
                    ORDER BY ABS(COALESCE(updated_at, placed_at, 0) - ?) ASC
                    LIMIT 1
                    """,
                    (sym, ven, ts),
                ).fetchone()

        if not row:
            return None
        payload = dict(row)
        signals_agreed = self._parse_json_field(payload.get("signals_agreed"), default=[])
        if not isinstance(signals_agreed, list):
            signals_agreed = []
        return {
            "source": "pending_orders_filled",
            "matched_at": payload.get("updated_at") or payload.get("placed_at"),
            "signals_agreed": [str(x) for x in signals_agreed],
            "signals_snapshot": self._parse_json_field(payload.get("signals_snapshot"), default=None),
            "context_snapshot": self._parse_json_field(payload.get("context_snapshot"), default=None),
            "conviction": payload.get("conviction"),
            "reason": payload.get("reason"),
            "order_id": payload.get("exchange_order_id"),
        }

    def find_recent_executed_proposal(
        self,
        symbol: str,
        venue: str,
        around_ts: Optional[float],
        window_sec: float = 600.0,
        *,
        allow_out_of_window_nearest: bool = False,
    ) -> Optional[Dict[str, Any]]:
        """Return nearest EXECUTED/FAILED proposal metadata for symbol/venue."""
        sym = str(symbol or "").strip().upper()
        ven = normalize_venue(venue or "")
        if not sym or not ven:
            return None

        try:
            ts = float(around_ts if around_ts is not None else time.time())
        except Exception:
            ts = time.time()
        try:
            win = max(1.0, float(window_sec or 600.0))
        except Exception:
            win = 600.0

        with self._get_connection() as conn:
            row = conn.execute(
                """
                SELECT p.*, pm.meta_json AS proposal_meta_json
                FROM trade_proposals p
                LEFT JOIN proposal_metadata pm ON pm.proposal_id = p.id
                WHERE UPPER(p.symbol) = ?
                  AND LOWER(p.venue) = ?
                  AND UPPER(COALESCE(p.status,'')) IN ('EXECUTED', 'FAILED')
                  AND ABS(COALESCE(p.updated_at, p.created_at, 0) - ?) <= ?
                ORDER BY
                  CASE WHEN UPPER(COALESCE(p.status,'')) = 'EXECUTED' THEN 0 ELSE 1 END ASC,
                  ABS(COALESCE(p.updated_at, p.created_at, 0) - ?) ASC
                LIMIT 1
                """,
                (sym, ven, ts, win, ts),
            ).fetchone()
            if not row and allow_out_of_window_nearest:
                row = conn.execute(
                    """
                    SELECT p.*, pm.meta_json AS proposal_meta_json
                    FROM trade_proposals p
                    LEFT JOIN proposal_metadata pm ON pm.proposal_id = p.id
                    WHERE UPPER(p.symbol) = ?
                      AND LOWER(p.venue) = ?
                      AND UPPER(COALESCE(p.status,'')) IN ('EXECUTED', 'FAILED')
                    ORDER BY
                      CASE WHEN UPPER(COALESCE(p.status,'')) = 'EXECUTED' THEN 0 ELSE 1 END ASC,
                      ABS(COALESCE(p.updated_at, p.created_at, 0) - ?) ASC
                    LIMIT 1
                    """,
                    (sym, ven, ts),
                ).fetchone()

        if not row:
            return None

        payload = dict(row)
        signals_json = self._parse_json_field(payload.get("signals_json"), default=[])
        if not isinstance(signals_json, list):
            signals_json = []
        proposal_meta = self._parse_json_field(payload.get("proposal_meta_json"), default={})
        if not isinstance(proposal_meta, dict):
            proposal_meta = {}
        signals_snapshot = proposal_meta.get("signals_snapshot")
        if not isinstance(signals_snapshot, dict):
            signals_snapshot = None
        context_snapshot = proposal_meta.get("context_snapshot")
        if not isinstance(context_snapshot, dict):
            context_snapshot = None
        risk_meta = proposal_meta.get("risk")
        if not isinstance(risk_meta, dict):
            risk_meta = None
        execution_meta = proposal_meta.get("execution")
        if not isinstance(execution_meta, dict):
            execution_meta = {}
        execution_order_type = str(execution_meta.get("order_type") or "").strip().lower()
        if execution_order_type not in {"limit", "chase_limit", "sr_limit"}:
            execution_order_type = None
        execution_source = str(execution_meta.get("source") or "").strip() or None
        execution_limit_style = str(execution_meta.get("limit_style") or "").strip() or None

        return {
            "source": f"trade_proposals_{str(payload.get('status') or '').lower()}",
            "proposal_id": payload.get("id"),
            "matched_at": payload.get("updated_at") or payload.get("created_at"),
            "signals_agreed": [str(x) for x in signals_json],
            "signals_snapshot": signals_snapshot,
            "context_snapshot": context_snapshot,
            "risk": risk_meta,
            "conviction": payload.get("conviction"),
            "reason": payload.get("reason_short"),
            "proposal_status": payload.get("status"),
            "execution_order_type": execution_order_type,
            "execution_source": execution_source,
            "execution_limit_style": execution_limit_style,
        }

    def get_pending_orders(self, state: Optional[str] = None) -> list:
        """Get pending orders, optionally filtered by state."""
        with self._get_connection() as conn:
            if state:
                rows = conn.execute(
                    "SELECT * FROM pending_orders WHERE state = ?", (state,)
                ).fetchall()
            else:
                rows = conn.execute("SELECT * FROM pending_orders").fetchall()
            return [dict(r) for r in rows]

    def log_orphan_order(
        self,
        *,
        symbol: str,
        venue: str,
        order_id: str,
        order_type: str,
        reason: Optional[str] = None,
        meta: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Log an orphaned order for manual cleanup."""
        with self._get_connection() as conn:
            conn.execute(
                """
                INSERT INTO orphan_orders (symbol, venue, order_id, order_type, reason, meta_json)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    symbol,
                    venue,
                    order_id,
                    order_type,
                    reason,
                    json.dumps(meta) if meta is not None else None,
                ),
            )
            conn.commit()


    def get_recent_closed_trades(self, limit: int = 10) -> List[TradeRecord]:
        """Get recent closed trades across all symbols."""
        with self._get_connection() as conn:
            cursor = conn.execute("""
                SELECT * FROM trades
                WHERE exit_time IS NOT NULL
                ORDER BY exit_time DESC
                LIMIT ?
            """, (limit,))

            return [self._row_to_trade_record(row) for row in cursor.fetchall()]


    def get_last_exit_by_reasons(
        self,
        *,
        symbol: str,
        venue: str,
        direction: str,
        reasons: List[str],
    ) -> Optional[Dict[str, Any]]:
        """Return the most recent exit for (symbol, venue, direction) matching any reason.

        Args:
            reasons: List of exit_reason strings to match (case-insensitive).

        Returns:
            {"trade_id": int, "exit_time": float, "exit_reason": str} or None
        """
        rs = [str(r or "").upper() for r in (reasons or []) if str(r or "").strip()]
        if not rs:
            return None

        placeholders = ",".join(["?"] * len(rs))
        with self._get_connection() as conn:
            row = conn.execute(
                f"""
                SELECT id, exit_time, exit_reason
                FROM trades
                WHERE symbol = ?
                  AND venue = ?
                  AND direction = ?
                  AND exit_time IS NOT NULL
                  AND UPPER(COALESCE(exit_reason,'')) IN ({placeholders})
                ORDER BY exit_time DESC
                LIMIT 1
                """,
                (symbol.upper(), venue, direction.upper(), *rs),
            ).fetchone()
            if not row:
                return None
            return {
                "trade_id": int(row["id"]),
                "exit_time": float(row["exit_time"]),
                "exit_reason": str(row["exit_reason"] or "").upper(),
            }

    def _row_to_trade_record(self, row: sqlite3.Row) -> TradeRecord:
        """Convert database row to TradeRecord."""
        return TradeRecord(
            id=row['id'],
            symbol=row['symbol'],
            direction=row['direction'],
            venue=row['venue'] if 'venue' in row.keys() else "lighter",
            state=row['state'] if 'state' in row.keys() else "ACTIVE",
            entry_time=row['entry_time'],
            entry_price=row['entry_price'],
            size=row['size'],
            notional_usd=row['notional_usd'],
            exit_time=row['exit_time'],
            exit_price=row['exit_price'],
            exit_reason=row['exit_reason'],
            realized_pnl=row['realized_pnl'],
            realized_pnl_pct=row['realized_pnl_pct'],
            total_fees=row['total_fees'] or 0.0,
            sl_price=row['sl_price'],
            tp_price=row['tp_price'],
            sl_order_id=row['sl_order_id'],
            tp_order_id=row['tp_order_id'],
            signals_snapshot=row['signals_snapshot'],
            signals_agreed=row['signals_agreed'],
            ai_reasoning=row['ai_reasoning'],
            confidence=row['confidence'],
            size_multiplier=row['size_multiplier'],
            context_snapshot=row['context_snapshot'],
            protection_snapshot=row['protection_snapshot'] if 'protection_snapshot' in row.keys() else None,
            safety_tier=row['safety_tier'],
        )

    # =========================================================================
    # Fill Methods
    # =========================================================================

    def log_fill(
        self,
        trade_id: Optional[int],
        venue: str,
        exchange_trade_id: str,
        symbol: str,
        fill_time: float,
        fill_price: float,
        fill_size: float,
        fill_type: str,
        side: str,
        fee: float = 0.0,
        fee_maker: float = 0.0,
        position_sign_changed: bool = False,
        raw_json: Optional[str] = None,
    ) -> int:
        """
        Log a fill from the exchange.

        Returns:
            fill_id, or -1 if duplicate (already exists)
        """
        try:
            with self._get_connection() as conn:
                cursor = conn.execute("""
                    INSERT INTO fills (
                        trade_id, venue, exchange_trade_id, symbol, fill_time, fill_price,
                        fill_size, fill_type, side, fee, fee_maker,
                        position_sign_changed, raw_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    trade_id,
                    venue,
                    str(exchange_trade_id),
                    symbol.upper(),
                    fill_time,
                    fill_price,
                    fill_size,
                    fill_type.upper(),
                    side.upper(),
                    fee,
                    fee_maker,
                    1 if position_sign_changed else 0,
                    raw_json,
                ))
                fill_id = cursor.lastrowid
                conn.commit()

            self.log.debug(f"Fill logged: {venue}/{symbol} {fill_type} @ ${fill_price:.2f} (ID: {fill_id})")
            return fill_id

        except sqlite3.IntegrityError:
            # Duplicate fill (UNIQUE constraint on venue + exchange_trade_id)
            return -1



    # =========================================================================
    # Cycle / Proposal Methods
    # =========================================================================

    def insert_cycle_run(
        self,
        seq: int,
        cycle_file: Optional[str] = None,
        context_file: Optional[str] = None,
        candidates_file: Optional[str] = None,
        timestamp: Optional[float] = None,
    ) -> None:
        """Insert or update a cycle run record."""
        ts = timestamp if timestamp is not None else time.time()
        with self._get_connection() as conn:
            conn.execute(
                """
                INSERT INTO cycle_runs (seq, timestamp, cycle_file, context_file, candidates_file)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(seq) DO UPDATE SET
                    timestamp = excluded.timestamp,
                    cycle_file = COALESCE(excluded.cycle_file, cycle_runs.cycle_file),
                    context_file = COALESCE(excluded.context_file, cycle_runs.context_file),
                    candidates_file = COALESCE(excluded.candidates_file, cycle_runs.candidates_file)
                """,
                (seq, ts, cycle_file, context_file, candidates_file),
            )
            conn.commit()

    def update_cycle_candidates_file(self, seq: int, candidates_file: str) -> bool:
        """Update candidates file path for a cycle run."""
        with self._get_connection() as conn:
            cursor = conn.execute(
                "UPDATE cycle_runs SET candidates_file = ? WHERE seq = ?",
                (candidates_file, int(seq)),
            )
            conn.commit()
            return cursor.rowcount > 0

    def claim_next_cycle(
        self,
        processor_id: str,
        stale_after_seconds: float = 600.0,
    ) -> Optional[Dict[str, Any]]:
        """Claim the newest unprocessed cycle for processing."""
        now = time.time()
        stale_before = now - float(stale_after_seconds)

        with self._get_connection() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                """
                SELECT *
                FROM cycle_runs
                WHERE processed_at IS NULL
                  AND (claimed_at IS NULL OR claimed_at < ?)
                ORDER BY seq DESC
                LIMIT 1
                """,
                (stale_before,),
            ).fetchone()

            if not row:
                conn.commit()
                return None

            cursor = conn.execute(
                """
                UPDATE cycle_runs
                SET claimed_at = ?, claimed_by = ?
                WHERE seq = ?
                  AND processed_at IS NULL
                  AND (claimed_at IS NULL OR claimed_at < ?)
                """,
                (now, processor_id, int(row["seq"]), stale_before),
            )
            if cursor.rowcount == 0:
                conn.commit()
                return None

            conn.commit()

        result = dict(row)
        result["claimed_at"] = now
        result["claimed_by"] = processor_id
        return result

    def _normalize_cycle_summary_json(self, summary: Optional[str]) -> Optional[str]:
        """Ensure processed_summary is always valid JSON.

        We historically stored plain strings in `cycle_runs.processed_summary`.
        This breaks JSON queries (json_extract/json_valid). To make the field
        reliably parseable, we wrap non-JSON summaries as {"text": <summary>}.
        """
        if summary is None:
            return None
        raw = str(summary)
        if not raw.strip():
            return None
        try:
            json.loads(raw)
            return raw
        except Exception:
            return json.dumps({"text": raw}, separators=(",", ":"), sort_keys=True)

    def mark_cycle_processed(
        self,
        seq: int,
        status: str,
        summary: Optional[str] = None,
        processed_by: Optional[str] = None,
        error: Optional[str] = None,
    ) -> bool:
        """Mark a cycle run as processed with status/summary."""
        summary = self._normalize_cycle_summary_json(summary)
        now = time.time()
        with self._get_connection() as conn:
            cursor = conn.execute(
                """
                UPDATE cycle_runs
                SET processed_at = COALESCE(processed_at, ?),
                    processed_by = COALESCE(?, processed_by),
                    processed_status = COALESCE(?, processed_status),
                    processed_summary = COALESCE(?, processed_summary),
                    processed_error = COALESCE(?, processed_error)
                WHERE seq = ?
                """,
                (
                    now,
                    processed_by,
                    status,
                    summary,
                    error,
                    int(seq),
                ),
            )
            conn.commit()
            return cursor.rowcount > 0

    def mark_cycle_retryable_failure(
        self,
        seq: int,
        status: str,
        summary: Optional[str] = None,
        processed_by: Optional[str] = None,
        error: Optional[str] = None,
        *,
        stale_after_seconds: float = 600.0,
        retry_delay_seconds: float = 30.0,
    ) -> bool:
        """Record a failed attempt without permanently consuming the cycle.

        Keeps `processed_at` NULL so `claim_next_cycle()` can re-claim later.
        The claim is pushed forward by `retry_delay_seconds` to avoid hot loops.
        """
        summary = self._normalize_cycle_summary_json(summary)
        now = time.time()
        stale = max(0.0, float(stale_after_seconds or 0.0))
        retry_delay = max(0.0, float(retry_delay_seconds or 0.0))
        # claim_next condition is: claimed_at < (now - stale_after_seconds)
        # Shift claimed_at so reclaim becomes possible after retry_delay.
        next_claim_at = now - stale + retry_delay

        with self._get_connection() as conn:
            cursor = conn.execute(
                """
                UPDATE cycle_runs
                SET claimed_at = ?,
                    claimed_by = COALESCE(?, claimed_by),
                    processed_status = COALESCE(?, processed_status),
                    processed_summary = COALESCE(?, processed_summary),
                    processed_error = COALESCE(?, processed_error)
                WHERE seq = ?
                  AND processed_at IS NULL
                """,
                (
                    next_claim_at,
                    processed_by,
                    status,
                    summary,
                    error,
                    int(seq),
                ),
            )
            conn.commit()
            return cursor.rowcount > 0

    def insert_proposal(
        self,
        cycle_seq: int,
        symbol: str,
        venue: str,
        direction: str,
        size_usd: float,
        sl: Optional[float] = None,
        tp: Optional[float] = None,
        conviction: Optional[float] = None,
        reason_short: Optional[str] = None,
        signals: Optional[List[str]] = None,
    ) -> int:
        """Insert a trade proposal. Returns proposal ID."""
        with self._get_connection() as conn:
            cursor = conn.execute(
                """
                INSERT INTO trade_proposals (
                    cycle_seq, symbol, venue, direction, size_usd,
                    sl, tp, conviction, reason_short, signals_json, status
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    int(cycle_seq),
                    symbol.upper(),
                    venue,
                    direction.upper(),
                    float(size_usd),
                    float(sl) if sl is not None else None,
                    float(tp) if tp is not None else None,
                    float(conviction) if conviction is not None else None,
                    reason_short,
                    json.dumps(signals) if signals else None,
                    "PROPOSED",
                ),
            )
            proposal_id = cursor.lastrowid
            conn.commit()
            return int(proposal_id)

    def get_proposals_for_cycle(self, seq: int) -> List[Dict[str, Any]]:
        """Fetch proposals for a given cycle."""
        with self._get_connection() as conn:
            cursor = conn.execute(
                """
                SELECT * FROM trade_proposals
                WHERE cycle_seq = ?
                ORDER BY id ASC
                """,
                (int(seq),),
            )
            rows = cursor.fetchall()

        proposals: List[Dict[str, Any]] = []
        for row in rows:
            item = dict(row)
            signals = item.get("signals_json")
            if signals:
                try:
                    item["signals"] = json.loads(signals)
                except json.JSONDecodeError:
                    item["signals"] = []
            else:
                item["signals"] = []
            proposals.append(item)
        return proposals

    def update_proposal_status(self, proposal_id: int, status: str, reason: Optional[str] = None) -> bool:
        """Update proposal status and optional reason.

        Also appends an audit row into proposal_status_history (v16+) so we can
        reconstruct all approve/reject/execute transitions over time.
        """
        status_norm = normalize_proposal_status(status, default=None)
        if status_norm is None:
            self.log.warning(
                "Rejected proposal status update with invalid status: proposal_id=%s status=%s",
                proposal_id,
                status,
            )
            return False

        with self._get_connection() as conn:
            current_row = conn.execute(
                "SELECT status FROM trade_proposals WHERE id = ?",
                (int(proposal_id),),
            ).fetchone()
            current_status = current_row["status"] if current_row else None
            if current_row and not can_transition_proposal_status(current_status, status_norm):
                self.log.warning(
                    "Rejected proposal status transition: proposal_id=%s from=%s to=%s",
                    proposal_id,
                    current_status,
                    status_norm,
                )
                return False

            cursor = conn.execute(
                """
                UPDATE trade_proposals SET
                    status = ?,
                    status_reason = ?,
                    updated_at = strftime('%s', 'now')
                WHERE id = ?
                """,
                (status_norm, reason, int(proposal_id)),
            )

            # Best-effort history insert (older DBs may not have v16 yet).
            try:
                conn.execute(
                    """
                    INSERT INTO proposal_status_history (proposal_id, status, reason, created_at)
                    VALUES (?, ?, ?, strftime('%s','now'))
                    """,
                    (int(proposal_id), status_norm, reason),
                )
            except Exception:
                pass

            conn.commit()
            return cursor.rowcount > 0

    def insert_gate_decision(
        self,
        *,
        cycle_seq: int,
        symbol: str,
        direction: str,
        decision: str,
        reason: Optional[str] = None,
        candidate_rank: Optional[int] = None,
        conviction: Optional[float] = None,
        blended_conviction: Optional[float] = None,
        pipeline_conviction: Optional[float] = None,
        brain_conviction: Optional[float] = None,
        llm_agent_id: Optional[str] = None,
        llm_model: Optional[str] = None,
        llm_session_id: Optional[str] = None,
    ) -> int:
        """Insert a normalized entry-gate decision row. Returns decision ID."""
        with self._get_connection() as conn:
            cursor = conn.execute(
                """
                INSERT INTO gate_decisions_v1 (
                    cycle_seq, symbol, direction, decision, reason, candidate_rank,
                    conviction, blended_conviction, pipeline_conviction, brain_conviction,
                    llm_agent_id, llm_model, llm_session_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    int(cycle_seq),
                    str(symbol or "").upper(),
                    str(direction or "").upper(),
                    str(decision or "").upper(),
                    reason,
                    int(candidate_rank) if candidate_rank is not None else None,
                    float(conviction) if conviction is not None else None,
                    float(blended_conviction) if blended_conviction is not None else None,
                    float(pipeline_conviction) if pipeline_conviction is not None else None,
                    float(brain_conviction) if brain_conviction is not None else None,
                    str(llm_agent_id) if llm_agent_id else None,
                    str(llm_model) if llm_model else None,
                    str(llm_session_id) if llm_session_id else None,
                ),
            )
            conn.commit()
            return int(cursor.lastrowid)

    def get_gate_decisions_for_cycle(self, cycle_seq: int) -> List[Dict[str, Any]]:
        with self._get_connection() as conn:
            rows = conn.execute(
                """
                SELECT * FROM gate_decisions_v1
                WHERE cycle_seq = ?
                ORDER BY id ASC
                """,
                (int(cycle_seq),),
            ).fetchall()
        return [dict(r) for r in rows]

    def link_gate_decision_to_proposal(
        self,
        *,
        gate_decision_id: int,
        proposal_id: int,
        venue: Optional[str] = None,
    ) -> bool:
        """Best-effort link from gate decision to proposal row."""
        with self._get_connection() as conn:
            cursor = conn.execute(
                """
                UPDATE gate_decisions_v1
                SET
                    proposal_id = CASE
                        WHEN proposal_id IS NULL THEN ?
                        ELSE proposal_id
                    END,
                    venue = CASE
                        WHEN (venue IS NULL OR venue = '') THEN COALESCE(?, venue)
                        ELSE venue
                    END
                WHERE id = ?
                """,
                (
                    int(proposal_id),
                    normalize_venue(venue or "") or None,
                    int(gate_decision_id),
                ),
            )
            conn.commit()
            return cursor.rowcount > 0

    def link_gate_decision_to_trade(
        self,
        *,
        gate_decision_id: int,
        trade_id: int,
    ) -> bool:
        """Best-effort link from gate decision to trade row."""
        with self._get_connection() as conn:
            cursor = conn.execute(
                """
                UPDATE gate_decisions_v1
                SET
                    trade_id = CASE
                        WHEN trade_id IS NULL THEN ?
                        ELSE trade_id
                    END
                WHERE id = ?
                """,
                (
                    int(trade_id),
                    int(gate_decision_id),
                ),
            )
            conn.commit()
            return cursor.rowcount > 0

    def finalize_gate_outcome_from_trade(self, trade_id: int) -> bool:
        """Write realized outcome from a closed trade onto linked gate decision."""
        with self._get_connection() as conn:
            trade = conn.execute(
                """
                SELECT id, symbol, direction, venue, exit_time, realized_pnl, realized_pnl_pct, context_snapshot
                FROM trades
                WHERE id = ?
                LIMIT 1
                """,
                (int(trade_id),),
            ).fetchone()
            if not trade:
                return False

            exit_time = trade["exit_time"]
            if exit_time is None:
                return False

            gate_decision_id = None
            proposal_id = None
            ctx_venue = None
            context_raw = trade["context_snapshot"]
            if context_raw:
                try:
                    ctx_obj = json.loads(context_raw) if isinstance(context_raw, str) else context_raw
                except Exception:
                    ctx_obj = None
                if isinstance(ctx_obj, dict):
                    eg = ctx_obj.get("entry_gate")
                    if isinstance(eg, dict):
                        gate_decision_id = eg.get("gate_decision_id") or eg.get("decision_id")
                        proposal_id = eg.get("proposal_id")
                        ctx_venue = eg.get("venue")

            if gate_decision_id is not None:
                try:
                    gate_decision_id = int(gate_decision_id)
                except Exception:
                    gate_decision_id = None
            if proposal_id is not None:
                try:
                    proposal_id = int(proposal_id)
                except Exception:
                    proposal_id = None

            if gate_decision_id is None:
                fallback = conn.execute(
                    """
                    SELECT id
                    FROM gate_decisions_v1
                    WHERE UPPER(COALESCE(decision, '')) = 'PICK'
                      AND symbol = ?
                      AND direction = ?
                      AND trade_id IS NULL
                    ORDER BY created_at DESC, id DESC
                    LIMIT 1
                    """,
                    (
                        str(trade["symbol"] or "").upper(),
                        str(trade["direction"] or "").upper(),
                    ),
                ).fetchone()
                if not fallback:
                    return False
                gate_decision_id = int(fallback["id"])

            pnl_raw = trade["realized_pnl"]
            pnl_pct_raw = trade["realized_pnl_pct"]
            pnl = float(pnl_raw) if pnl_raw is not None else None
            pnl_pct = float(pnl_pct_raw) if pnl_pct_raw is not None else None

            if pnl is None:
                outcome_status = "UNKNOWN"
            elif pnl > 0:
                outcome_status = "WIN"
            elif pnl < 0:
                outcome_status = "LOSS"
            else:
                outcome_status = "FLAT"

            resolved_venue = normalize_venue(ctx_venue or trade["venue"] or "") or None

            cursor = conn.execute(
                """
                UPDATE gate_decisions_v1
                SET
                    trade_id = CASE
                        WHEN trade_id IS NULL THEN ?
                        ELSE trade_id
                    END,
                    proposal_id = CASE
                        WHEN ? IS NOT NULL THEN ?
                        ELSE proposal_id
                    END,
                    venue = CASE
                        WHEN ? IS NOT NULL THEN ?
                        ELSE venue
                    END,
                    outcome_status = ?,
                    outcome_pnl = ?,
                    outcome_pnl_pct = ?,
                    outcome_closed_at = ?
                WHERE id = ?
                """,
                (
                    int(trade_id),
                    proposal_id,
                    proposal_id,
                    resolved_venue,
                    resolved_venue,
                    outcome_status,
                    pnl,
                    pnl_pct,
                    float(exit_time),
                    int(gate_decision_id),
                ),
            )
            conn.commit()
            return cursor.rowcount > 0

    def insert_proposal_metadata(self, proposal_id: int, meta: Dict[str, Any]) -> bool:
        """Upsert JSON metadata for a proposal."""
        if proposal_id is None:
            return False
        try:
            meta_json = json.dumps(meta or {}, separators=(",", ":"), sort_keys=True)
        except Exception:
            meta_json = "{}"

        with self._get_connection() as conn:
            conn.execute(
                """
                INSERT INTO proposal_metadata (proposal_id, meta_json, created_at, updated_at)
                VALUES (?, ?, strftime('%s','now'), strftime('%s','now'))
                ON CONFLICT(proposal_id) DO UPDATE SET
                    meta_json = excluded.meta_json,
                    updated_at = strftime('%s','now')
                """,
                (int(proposal_id), meta_json),
            )
            conn.commit()
        return True

    def get_proposal_metadata(self, proposal_id: int) -> Dict[str, Any]:
        """Fetch proposal metadata JSON. Returns {} if missing."""
        if proposal_id is None:
            return {}
        with self._get_connection() as conn:
            row = conn.execute(
                "SELECT meta_json FROM proposal_metadata WHERE proposal_id = ?",
                (int(proposal_id),),
            ).fetchone()
        if not row:
            return {}
        raw = row[0]
        if not raw:
            return {}
        try:
            parsed = json.loads(raw)
            return parsed if isinstance(parsed, dict) else {"value": parsed}
        except Exception:
            return {}

    def insert_execution_result(
        self,
        proposal_id: int,
        started_at: Optional[float],
        finished_at: Optional[float],
        success: bool,
        error: Optional[str] = None,
        exchange: Optional[str] = None,
        entry_price: Optional[float] = None,
        size: Optional[float] = None,
        sl_price: Optional[float] = None,
        tp_price: Optional[float] = None,
    ) -> int:
        """Insert execution result for a proposal. Returns execution ID."""
        with self._get_connection() as conn:
            cursor = conn.execute(
                """
                INSERT INTO proposal_executions (
                    proposal_id, started_at, finished_at, success, error,
                    exchange, entry_price, size, sl_price, tp_price
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    int(proposal_id),
                    started_at,
                    finished_at,
                    1 if success else 0,
                    error,
                    exchange,
                    entry_price,
                    size,
                    sl_price,
                    tp_price,
                ),
            )
            exec_id = cursor.lastrowid
            # Add normalized attempt telemetry alongside legacy proposal_executions.
            symbol = None
            venue = None
            try:
                proposal_row = conn.execute(
                    "SELECT symbol, venue FROM trade_proposals WHERE id = ?",
                    (int(proposal_id),),
                ).fetchone()
                if proposal_row:
                    symbol = proposal_row["symbol"]
                    venue = proposal_row["venue"]
            except Exception:
                symbol = None
                venue = None

            elapsed_ms = None
            if started_at is not None and finished_at is not None:
                try:
                    elapsed_ms = max(0.0, (float(finished_at) - float(started_at)) * 1000.0)
                except Exception:
                    elapsed_ms = None

            error_text_norm = (str(error).strip() if error is not None else "")
            error_code = error_text_norm.lower().replace(" ", "_") if error_text_norm else None
            if error_code and len(error_code) > 128:
                error_code = error_code[:128]

            fallback_used = 0
            if error_text_norm:
                low = error_text_norm.lower()
                if "fallback" in low:
                    fallback_used = 1

            is_builder_symbol = 1 if (symbol and ":" in str(symbol)) else 0
            builder_error_category = None
            if is_builder_symbol and error_text_norm:
                low = error_text_norm.lower()
                if "lot" in low or "size" in low:
                    builder_error_category = "size_or_lot"
                elif "asset" in low or "mapping" in low or "range" in low:
                    builder_error_category = "asset_mapping"
                elif "bid/ask" in low or "book" in low or "mid" in low:
                    builder_error_category = "market_data"
                elif "underfill" in low:
                    builder_error_category = "underfill"
                elif "verification failed" in low or "phantom" in low:
                    builder_error_category = "verification"
                elif "chase-limit entry failed" in low or "timeout" in low:
                    builder_error_category = "chase_timeout"
                else:
                    builder_error_category = "other"

            slippage_pct = None
            if entry_price is not None and entry_price not in (0, 0.0):
                # expected_price is currently unavailable at this persistence layer.
                slippage_pct = None

            conn.execute(
                """
                INSERT INTO execution_attempts_v1 (
                    proposal_id, trade_id, symbol, venue, action,
                    expected_price, filled_price, slippage_pct,
                    requested_size, filled_size, elapsed_ms, fallback_used,
                    success, error_code, error_text, is_builder_symbol, builder_error_category
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    int(proposal_id),
                    None,
                    symbol,
                    venue,
                    "entry",
                    None,
                    entry_price,
                    slippage_pct,
                    None,
                    size,
                    elapsed_ms,
                    int(fallback_used),
                    1 if success else 0,
                    error_code,
                    error_text_norm or None,
                    int(is_builder_symbol),
                    builder_error_category,
                ),
            )
            conn.commit()
            return int(exec_id)

    def open_sltp_incident(
        self,
        *,
        trade_id: Optional[int],
        symbol: str,
        venue: Optional[str],
        leg: str,
        attempts: int = 0,
        last_error: Optional[str] = None,
    ) -> int:
        """Open or refresh an unresolved SL/TP incident; return incident ID."""
        leg_norm = str(leg or "").strip().upper()
        if leg_norm not in ("SL", "TP"):
            raise ValueError(f"invalid SLTP leg: {leg!r}")

        with self._get_connection() as conn:
            row = None
            if trade_id is not None:
                row = conn.execute(
                    """
                    SELECT id FROM sltp_incidents_v1
                    WHERE trade_id = ? AND leg = ? AND state = 'OPEN'
                    ORDER BY id DESC
                    LIMIT 1
                    """,
                    (int(trade_id), leg_norm),
                ).fetchone()
            if row:
                conn.execute(
                    """
                    UPDATE sltp_incidents_v1
                    SET attempts = ?, last_error = ?, updated_at = strftime('%s', 'now')
                    WHERE id = ?
                    """,
                    (int(attempts), last_error, int(row["id"])),
                )
                conn.commit()
                return int(row["id"])

            cur = conn.execute(
                """
                INSERT INTO sltp_incidents_v1 (
                    trade_id, symbol, venue, leg, attempts, last_error, state, opened_at
                ) VALUES (?, ?, ?, ?, ?, ?, 'OPEN', strftime('%s', 'now'))
                """,
                (
                    int(trade_id) if trade_id is not None else None,
                    str(symbol or "").upper(),
                    normalize_venue(venue or "") or None,
                    leg_norm,
                    int(attempts),
                    last_error,
                ),
            )
            conn.commit()
            return int(cur.lastrowid)

    def resolve_sltp_incident(
        self,
        *,
        trade_id: Optional[int],
        leg: str,
        resolution_note: Optional[str] = None,
    ) -> int:
        """Resolve open SL/TP incidents for trade+leg. Returns number of rows updated."""
        if trade_id is None:
            return 0
        leg_norm = str(leg or "").strip().upper()
        if leg_norm not in ("SL", "TP"):
            return 0
        with self._get_connection() as conn:
            cur = conn.execute(
                """
                UPDATE sltp_incidents_v1
                SET state = 'RESOLVED',
                    resolved_at = strftime('%s', 'now'),
                    resolution_note = COALESCE(?, resolution_note),
                    updated_at = strftime('%s', 'now')
                WHERE trade_id = ? AND leg = ? AND state = 'OPEN'
                """,
                (resolution_note, int(trade_id), leg_norm),
            )
            conn.commit()
            return int(cur.rowcount or 0)

    # =========================================================================
    # Monitoring Snapshot Methods
    # =========================================================================

    def get_latest_monitor_snapshot(self) -> Optional[Dict[str, Any]]:
        """Fetch the most recent monitor snapshot."""
        with self._get_connection() as conn:
            row = conn.execute(
                "SELECT * FROM monitor_snapshots ORDER BY ts DESC LIMIT 1"
            ).fetchone()
        return dict(row) if row else None

    def get_latest_equity(self, venue: str) -> Optional[float]:
        """Return latest equity for a venue ('hyperliquid', 'hip3', or 'lighter')."""
        snapshot = self.get_latest_monitor_snapshot()
        if not snapshot:
            return None
        venue_key = str(venue or "").lower()
        if venue_key in ("hyperliquid", "hl"):
            return snapshot.get("hl_equity")
        if venue_key in ("hip3", "hl_wallet", "wallet", "hyperliquid_wallet"):
            return snapshot.get("hl_wallet_equity") or snapshot.get("hip3_equity")
        if venue_key in ("lighter", "lt"):
            return snapshot.get("lighter_equity")
        return None

    # =========================================================================
    # Safety State Methods
    # =========================================================================

    def get_safety_state(self) -> SafetyState:
        """Get current safety state."""
        with self._get_connection() as conn:
            cursor = conn.execute("SELECT * FROM safety_state WHERE id = 1")
            row = cursor.fetchone()
            if row is None:
                conn.execute(
                    "INSERT OR IGNORE INTO safety_state (id, peak_equity, current_equity) VALUES (1, ?, ?)",
                    (self.starting_equity, self.starting_equity),
                )
                conn.commit()
                row = conn.execute("SELECT * FROM safety_state WHERE id = 1").fetchone()
            if row is None:
                return SafetyState(
                    peak_equity=float(self.starting_equity),
                    current_equity=float(self.starting_equity),
                )

            return SafetyState(
                current_tier=row['current_tier'],
                daily_pnl=row['daily_pnl'],
                daily_pnl_reset_at=row['daily_pnl_reset_at'],
                consecutive_losses=row['consecutive_losses'],
                cooldown_until=row['cooldown_until'],
                max_drawdown_pct=row['max_drawdown_pct'],
                peak_equity=row['peak_equity'],
                current_equity=row['current_equity'],
                paused=bool(row['paused']),
            )

    def update_safety_state(
        self,
        current_tier: Optional[int] = None,
        daily_pnl: Optional[float] = None,
        daily_pnl_reset_at: Optional[str] = None,
        consecutive_losses: Optional[int] = None,
        cooldown_until: Optional[str] = None,
        max_drawdown_pct: Optional[float] = None,
        peak_equity: Optional[float] = None,
        current_equity: Optional[float] = None,
        paused: Optional[bool] = None,
    ) -> None:
        """Update safety state fields."""
        updates = []
        params = []

        if current_tier is not None:
            updates.append("current_tier = ?")
            params.append(current_tier)
        if daily_pnl is not None:
            updates.append("daily_pnl = ?")
            params.append(daily_pnl)
        if daily_pnl_reset_at is not None:
            updates.append("daily_pnl_reset_at = ?")
            params.append(daily_pnl_reset_at)
        if consecutive_losses is not None:
            updates.append("consecutive_losses = ?")
            params.append(consecutive_losses)
        if cooldown_until is not None:
            updates.append("cooldown_until = ?")
            params.append(cooldown_until)
        if max_drawdown_pct is not None:
            updates.append("max_drawdown_pct = ?")
            params.append(max_drawdown_pct)
        if peak_equity is not None:
            updates.append("peak_equity = ?")
            params.append(peak_equity)
        if current_equity is not None:
            updates.append("current_equity = ?")
            params.append(current_equity)
        if paused is not None:
            updates.append("paused = ?")
            params.append(1 if paused else 0)

        if not updates:
            return

        updates.append("updated_at = strftime('%s', 'now')")

        with self._get_connection() as conn:
            conn.execute(
                f"UPDATE safety_state SET {', '.join(updates)} WHERE id = 1",
                params
            )
            conn.commit()

    # =========================================================================
    # AGI Trader Methods (v10)
    # =========================================================================






    def update_symbol_policy(
        self,
        symbol: str,
        sl_mult_adjustment: Optional[float] = None,
        tp_mult_adjustment: Optional[float] = None,
        size_adjustment: Optional[float] = None,
        stop_out_rate: Optional[float] = None,
        win_rate: Optional[float] = None,
        samples: Optional[int] = None,
        notes: Optional[str] = None,
    ) -> bool:
        """Update or insert symbol policy adjustments."""
        with self._get_connection() as conn:
            symbol_u = symbol.upper()
            now = time.time()
            conn.execute("BEGIN IMMEDIATE")

            exists = conn.execute(
                "SELECT 1 FROM symbol_policy WHERE symbol = ?",
                (symbol_u,),
            ).fetchone() is not None

            if exists:
                updates = []
                params = []
                if sl_mult_adjustment is not None:
                    updates.append("sl_mult_adjustment = ?")
                    params.append(float(sl_mult_adjustment))
                if tp_mult_adjustment is not None:
                    updates.append("tp_mult_adjustment = ?")
                    params.append(float(tp_mult_adjustment))
                if size_adjustment is not None:
                    updates.append("size_adjustment = ?")
                    params.append(float(size_adjustment))
                if stop_out_rate is not None:
                    updates.append("stop_out_rate = ?")
                    params.append(float(stop_out_rate))
                if win_rate is not None:
                    updates.append("win_rate = ?")
                    params.append(float(win_rate))
                if samples is not None:
                    updates.append("samples = ?")
                    params.append(int(samples))
                if notes is not None:
                    updates.append("notes = ?")
                    params.append(notes)

                if updates:
                    updates.append("last_updated = ?")
                    params.append(now)
                    params.append(symbol_u)
                    conn.execute(
                        f"UPDATE symbol_policy SET {', '.join(updates)} WHERE symbol = ?",
                        params,
                    )
            else:
                conn.execute(
                    """
                    INSERT INTO symbol_policy (
                        symbol, sl_mult_adjustment, tp_mult_adjustment, size_adjustment,
                        stop_out_rate, win_rate, samples, last_updated, notes
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        symbol_u,
                        1.0 if sl_mult_adjustment is None else float(sl_mult_adjustment),
                        1.0 if tp_mult_adjustment is None else float(tp_mult_adjustment),
                        1.0 if size_adjustment is None else float(size_adjustment),
                        0.0 if stop_out_rate is None else float(stop_out_rate),
                        0.5 if win_rate is None else float(win_rate),
                        0 if samples is None else int(samples),
                        now,
                        notes,
                    ),
                )

            conn.commit()
            return True


    def update_dead_capital_imbalance_stats(
        self,
        symbol: str,
        venue: str,
        ts: float,
        imbalance: float,
        *,
        half_life_days: float = 3.0,
        min_samples: int = 300,
        override_z: float = 3.5,
        max_gap_seconds: float = 20 * 60,
        clamp_z: float = 8.0,
        eps_std: float = 1e-6,
    ) -> Dict[str, Any]:
        """Update rolling (EWMA) stats for dead_capital imbalance.

        We treat `imbalance = locked_short_pct - locked_long_pct` as the learned variable.
        This lets us compute a *real* internal z-score over time.

        Returns a dict with z, n, mean, std, override_streak, override_trigger.
        """
        import math

        symbol_u = str(symbol).upper()
        venue_u = str(venue)

        # Half-life -> time constant
        half_life_seconds = max(60.0, float(half_life_days) * 86400.0)
        tau = half_life_seconds / math.log(2.0)

        with self._get_connection() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                "SELECT n, mean, var, last_ts, override_streak FROM dead_capital_stats WHERE symbol=? AND venue=?",
                (symbol_u, venue_u),
            ).fetchone()

            if row is None:
                # Bootstrap with a sane variance; z not trustworthy yet.
                conn.execute(
                    """
                    INSERT OR REPLACE INTO dead_capital_stats (
                        symbol, venue, n, mean, var, last_ts, last_x, last_z, override_streak, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (symbol_u, venue_u, 1, float(imbalance), 1.0, float(ts), float(imbalance), 0.0, 0, time.time()),
                )
                conn.commit()
                return {
                    'symbol': symbol_u,
                    'venue': venue_u,
                    'n': 1,
                    'mean': float(imbalance),
                    'std': 1.0,
                    'z': 0.0,
                    'z_abs': 0.0,
                    'override_streak': 0,
                    'override_trigger': False,
                }

            n = int(row[0] or 0)
            mean = float(row[1] or 0.0)
            var = float(row[2] or 0.0)
            last_ts = row[3]
            streak = int(row[4] or 0)

            dt = None
            try:
                dt = float(ts) - float(last_ts) if last_ts is not None else None
            except Exception:
                dt = None
            if dt is None or dt <= 0:
                dt = 600.0  # assume ~10min

            gap_ok = dt <= float(max_gap_seconds)
            if not gap_ok:
                streak = 0  # break streak across long gaps

            alpha = 1.0 - math.exp(-dt / tau)
            alpha = max(0.0001, min(0.5, alpha))  # keep stable

            std = math.sqrt(var) if var > 0 else 0.0
            warmed = (n >= int(min_samples)) and (std > float(eps_std))

            z = 0.0
            if warmed:
                z = (float(imbalance) - mean) / std
                z = max(-float(clamp_z), min(float(clamp_z), z))

            override_hit = bool(warmed and gap_ok and abs(z) >= float(override_z))
            if override_hit:
                streak = streak + 1
            else:
                streak = 0
            override_trigger = bool(streak >= 2)

            # Update EWMA mean/variance after computing z (so z uses prior distribution)
            mean_prev = mean
            mean_new = mean + alpha * (float(imbalance) - mean)
            # EW variance update (stable):
            diff = float(imbalance) - mean_prev
            var_new = (1.0 - alpha) * var + alpha * (diff ** 2)
            var_new = max(var_new, 0.0)

            n_new = n + 1

            conn.execute(
                """
                UPDATE dead_capital_stats
                SET n=?, mean=?, var=?, last_ts=?, last_x=?, last_z=?, override_streak=?, updated_at=?
                WHERE symbol=? AND venue=?
                """,
                (
                    n_new,
                    mean_new,
                    var_new,
                    float(ts),
                    float(imbalance),
                    float(z),
                    int(streak),
                    time.time(),
                    symbol_u,
                    venue_u,
                ),
            )
            conn.commit()

            std_new = math.sqrt(var_new) if var_new > 0 else 0.0
            return {
                'symbol': symbol_u,
                'venue': venue_u,
                'n': n_new,
                'mean': mean_new,
                'std': std_new,
                'z': float(z),
                'z_abs': float(abs(z)),
                'override_streak': int(streak),
                'override_trigger': bool(override_trigger),
            }


    def insert_conviction_config_snapshot(
        self,
        *,
        params: Dict[str, Any],
        source: str = "manual",
        notes: Optional[str] = None,
        effective_from: Optional[float] = None,
        activate: bool = True,
    ) -> int:
        """Insert conviction config snapshot and optionally mark it active."""
        eff = float(effective_from if effective_from is not None else time.time())
        payload = json.dumps(params or {}, separators=(",", ":"), sort_keys=True)

        with self._get_connection() as conn:
            conn.execute("BEGIN IMMEDIATE")
            if activate:
                conn.execute("UPDATE conviction_config_history SET is_active = 0 WHERE is_active = 1")
            cur = conn.execute(
                """
                INSERT INTO conviction_config_history (
                    effective_from, source, notes, params_json, is_active
                ) VALUES (?, ?, ?, ?, ?)
                """,
                (eff, str(source or "manual"), notes, payload, 1 if activate else 0),
            )
            conn.commit()
            return int(cur.lastrowid)

    def get_active_conviction_config(self) -> Optional[Dict[str, Any]]:
        """Get active conviction config snapshot, or the most recent snapshot."""
        with self._get_connection() as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                """
                SELECT *
                FROM conviction_config_history
                WHERE is_active = 1
                ORDER BY effective_from DESC, id DESC
                LIMIT 1
                """
            ).fetchone()
            if row is None:
                row = conn.execute(
                    """
                    SELECT *
                    FROM conviction_config_history
                    ORDER BY effective_from DESC, id DESC
                    LIMIT 1
                    """
                ).fetchone()
            if row is None:
                return None
            out = dict(row)
            try:
                out["params"] = json.loads(out.get("params_json") or "{}")
            except Exception:
                out["params"] = {}
            return out

    def create_adaptation_run(
        self,
        *,
        window_start: Optional[float],
        window_end: Optional[float],
        fingerprint: str,
        status: str = "running",
    ) -> Optional[int]:
        """Create adaptation run row unless fingerprint already exists."""
        fp = str(fingerprint or "").strip()
        if not fp:
            return None
        now = time.time()
        with self._get_connection() as conn:
            conn.execute("BEGIN IMMEDIATE")
            exists = conn.execute(
                "SELECT id FROM adaptation_runs WHERE fingerprint = ? LIMIT 1",
                (fp,),
            ).fetchone()
            if exists:
                conn.rollback()
                return None
            cur = conn.execute(
                """
                INSERT INTO adaptation_runs (
                    started_at, status, window_start, window_end, fingerprint
                ) VALUES (?, ?, ?, ?, ?)
                """,
                (
                    now,
                    str(status or "running"),
                    float(window_start) if window_start is not None else None,
                    float(window_end) if window_end is not None else None,
                    fp,
                ),
            )
            conn.commit()
            return int(cur.lastrowid)

    def finish_adaptation_run(
        self,
        run_id: int,
        *,
        status: str,
        result: Optional[Dict[str, Any]] = None,
        error_text: Optional[str] = None,
    ) -> bool:
        """Finalize adaptation run status/result payload."""
        rid = int(run_id)
        with self._get_connection() as conn:
            cur = conn.execute(
                """
                UPDATE adaptation_runs
                SET finished_at = ?,
                    status = ?,
                    result_json = ?,
                    error_text = ?
                WHERE id = ?
                """,
                (
                    time.time(),
                    str(status or "unknown"),
                    json.dumps(result, separators=(",", ":"), sort_keys=True) if result is not None else None,
                    error_text,
                    rid,
                ),
            )
            conn.commit()
            return cur.rowcount > 0

    def get_latest_adaptation_fingerprint(self) -> Optional[str]:
        with self._get_connection() as conn:
            row = conn.execute(
                """
                SELECT fingerprint
                FROM adaptation_runs
                ORDER BY COALESCE(window_end, started_at) DESC, id DESC
                LIMIT 1
                """
            ).fetchone()
            return str(row[0]) if row and row[0] is not None else None

    def get_latest_adaptation_started_at(self) -> Optional[float]:
        with self._get_connection() as conn:
            row = conn.execute(
                "SELECT MAX(started_at) FROM adaptation_runs"
            ).fetchone()
            if not row or row[0] is None:
                return None
            try:
                return float(row[0])
            except Exception:
                return None

    def insert_trade_features(
        self,
        *,
        trade_id: int,
        symbol: str,
        direction: Optional[str],
        venue: Optional[str],
        closed_at: float,
        conviction: Optional[float] = None,
        order_type: Optional[str] = None,
        order_type_source: Optional[str] = None,
        strategy_segment: Optional[str] = None,
        entry_gate_mode: Optional[str] = None,
        hip3_driver: Optional[str] = None,
        hip3_flow_pass: Optional[bool] = None,
        hip3_ofm_pass: Optional[bool] = None,
        hip3_booster_score: Optional[float] = None,
        hip3_booster_size_mult: Optional[float] = None,
        risk_pct_used: Optional[float] = None,
        equity_at_entry: Optional[float] = None,
        mae_pct: Optional[float] = None,
        mfe_pct: Optional[float] = None,
        pnl_usd: Optional[float] = None,
        pnl_r: Optional[float] = None,
        exit_reason: Optional[str] = None,
    ) -> int:
        """Insert or update per-trade feature row for adaptive learner."""
        with self._get_connection() as conn:
            conn.execute("BEGIN IMMEDIATE")
            conn.execute(
                """
                INSERT INTO trade_features (
                    trade_id, symbol, direction, venue, closed_at, conviction, order_type, order_type_source,
                    strategy_segment, entry_gate_mode, hip3_driver, hip3_flow_pass, hip3_ofm_pass,
                    hip3_booster_score, hip3_booster_size_mult,
                    risk_pct_used, equity_at_entry, mae_pct, mfe_pct, pnl_usd, pnl_r, exit_reason
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(trade_id) DO UPDATE SET
                    symbol = excluded.symbol,
                    direction = excluded.direction,
                    venue = excluded.venue,
                    closed_at = excluded.closed_at,
                    conviction = excluded.conviction,
                    order_type = excluded.order_type,
                    order_type_source = excluded.order_type_source,
                    strategy_segment = excluded.strategy_segment,
                    entry_gate_mode = excluded.entry_gate_mode,
                    hip3_driver = excluded.hip3_driver,
                    hip3_flow_pass = excluded.hip3_flow_pass,
                    hip3_ofm_pass = excluded.hip3_ofm_pass,
                    hip3_booster_score = excluded.hip3_booster_score,
                    hip3_booster_size_mult = excluded.hip3_booster_size_mult,
                    risk_pct_used = excluded.risk_pct_used,
                    equity_at_entry = excluded.equity_at_entry,
                    mae_pct = excluded.mae_pct,
                    mfe_pct = excluded.mfe_pct,
                    pnl_usd = excluded.pnl_usd,
                    pnl_r = excluded.pnl_r,
                    exit_reason = excluded.exit_reason
                """,
                (
                    int(trade_id),
                    str(symbol or "").upper(),
                    str(direction or "").upper() if direction else None,
                    normalize_venue(venue or "") if venue else None,
                    float(closed_at),
                    float(conviction) if conviction is not None else None,
                    str(order_type) if order_type is not None else None,
                    str(order_type_source) if order_type_source is not None else None,
                    str(strategy_segment) if strategy_segment is not None else None,
                    str(entry_gate_mode) if entry_gate_mode is not None else None,
                    str(hip3_driver) if hip3_driver is not None else None,
                    int(bool(hip3_flow_pass)) if hip3_flow_pass is not None else None,
                    int(bool(hip3_ofm_pass)) if hip3_ofm_pass is not None else None,
                    float(hip3_booster_score) if hip3_booster_score is not None else None,
                    float(hip3_booster_size_mult) if hip3_booster_size_mult is not None else None,
                    float(risk_pct_used) if risk_pct_used is not None else None,
                    float(equity_at_entry) if equity_at_entry is not None else None,
                    float(mae_pct) if mae_pct is not None else None,
                    float(mfe_pct) if mfe_pct is not None else None,
                    float(pnl_usd) if pnl_usd is not None else None,
                    float(pnl_r) if pnl_r is not None else None,
                    str(exit_reason) if exit_reason is not None else None,
                ),
            )
            row = conn.execute(
                "SELECT id FROM trade_features WHERE trade_id = ?",
                (int(trade_id),),
            ).fetchone()
            conn.commit()
            return int(row[0]) if row else -1

    # =========================================================================
    # Reflections (v2) + per-symbol conclusions
    # =========================================================================

    def upsert_reflection_v2(
        self,
        *,
        trade_id: int,
        lesson_text: str,
        reflection_json: Optional[str] = None,
        confidence: Optional[float] = None,
        created_at: Optional[float] = None,
    ) -> bool:
        """Insert or update reflections_v2 row for trade_id.

        Important: we keep the same row/id for a trade_id (UPDATE on conflict),
        so downstream "last_reflection_id_seen" tracking remains stable.
        """
        tid = int(trade_id)
        now = float(created_at if created_at is not None else time.time())
        with self._get_connection() as conn:
            conn.execute("BEGIN IMMEDIATE")
            conn.execute(
                """
                INSERT INTO reflections_v2 (trade_id, reflection_json, lesson_text, confidence, created_at)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(trade_id) DO UPDATE SET
                    reflection_json = excluded.reflection_json,
                    lesson_text = excluded.lesson_text,
                    confidence = excluded.confidence,
                    created_at = COALESCE(reflections_v2.created_at, excluded.created_at)
                """,
                (
                    tid,
                    str(reflection_json) if reflection_json is not None else None,
                    str(lesson_text or "").strip(),
                    str(confidence) if confidence is not None else None,
                    now,
                ),
            )
            conn.commit()
            return True

    def enqueue_reflection_task(
        self,
        *,
        trade_id: int,
        symbol: Optional[str] = None,
    ) -> bool:
        """Enqueue trade for reflection processing (idempotent).

        Returns True if a new task was inserted; False if it already existed.
        """
        tid = int(trade_id)
        sym = str(symbol or "").strip().upper() or None
        now = time.time()
        with self._get_connection() as conn:
            conn.execute("BEGIN IMMEDIATE")
            cur = conn.execute(
                """
                INSERT OR IGNORE INTO reflection_tasks_v1 (
                    trade_id, symbol, status, attempts, created_at, updated_at
                ) VALUES (?, ?, 'PENDING', 0, ?, ?)
                """,
                (tid, sym, now, now),
            )
            conn.commit()
            return bool(cur.rowcount)

    def claim_reflection_task(
        self,
        *,
        worker_id: str,
        max_attempts: int = 5,
        include_error: bool = True,
        stale_running_sec: float = 600.0,
        error_backoff_sec: float = 30.0,
    ) -> Optional[int]:
        """Atomically claim the next reflection task.

        Priority order:
        1) oldest PENDING
        2) stale RUNNING (worker likely crashed)
        3) eligible ERROR retries (respecting backoff)
        """
        wid = str(worker_id or "").strip() or "worker"
        now = time.time()
        stale_cutoff = now - max(0.0, float(stale_running_sec or 0.0))
        retry_cutoff = now - max(0.0, float(error_backoff_sec or 0.0))
        max_attempts_i = int(max_attempts)
        with self._get_connection() as conn:
            conn.execute("BEGIN IMMEDIATE")
            if include_error:
                row = conn.execute(
                    """
                    SELECT trade_id
                    FROM reflection_tasks_v1
                    WHERE attempts < ?
                      AND (
                        status = 'PENDING'
                        OR (status = 'RUNNING' AND locked_at IS NOT NULL AND locked_at <= ?)
                        OR (status = 'ERROR' AND updated_at <= ?)
                      )
                    ORDER BY
                      CASE status
                        WHEN 'PENDING' THEN 0
                        WHEN 'RUNNING' THEN 1
                        ELSE 2
                      END,
                      created_at ASC
                    LIMIT 1
                    """,
                    (max_attempts_i, stale_cutoff, retry_cutoff),
                ).fetchone()
            else:
                row = conn.execute(
                    """
                    SELECT trade_id
                    FROM reflection_tasks_v1
                    WHERE attempts < ?
                      AND (
                        status = 'PENDING'
                        OR (status = 'RUNNING' AND locked_at IS NOT NULL AND locked_at <= ?)
                      )
                    ORDER BY
                      CASE status
                        WHEN 'PENDING' THEN 0
                        ELSE 1
                      END,
                      created_at ASC
                    LIMIT 1
                    """,
                    (max_attempts_i, stale_cutoff),
                ).fetchone()
            if not row:
                conn.rollback()
                return None
            tid = int(row[0])
            if include_error:
                cur = conn.execute(
                    """
                    UPDATE reflection_tasks_v1
                    SET status='RUNNING', locked_by=?, locked_at=?, updated_at=?, attempts=attempts+1
                    WHERE trade_id=?
                      AND attempts < ?
                      AND (
                        status = 'PENDING'
                        OR (status = 'RUNNING' AND locked_at IS NOT NULL AND locked_at <= ?)
                        OR (status = 'ERROR' AND updated_at <= ?)
                      )
                    """,
                    (wid, now, now, tid, max_attempts_i, stale_cutoff, retry_cutoff),
                )
            else:
                cur = conn.execute(
                    """
                    UPDATE reflection_tasks_v1
                    SET status='RUNNING', locked_by=?, locked_at=?, updated_at=?, attempts=attempts+1
                    WHERE trade_id=?
                      AND attempts < ?
                      AND (
                        status = 'PENDING'
                        OR (status = 'RUNNING' AND locked_at IS NOT NULL AND locked_at <= ?)
                      )
                    """,
                    (wid, now, now, tid, max_attempts_i, stale_cutoff),
                )
            if cur.rowcount != 1:
                conn.rollback()
                return None
            conn.commit()
            return tid

    def mark_reflection_task_done(self, *, trade_id: int) -> bool:
        tid = int(trade_id)
        now = time.time()
        with self._get_connection() as conn:
            cur = conn.execute(
                """
                UPDATE reflection_tasks_v1
                SET status='DONE', last_error=NULL, locked_by=NULL, locked_at=NULL, updated_at=?
                WHERE trade_id=?
                """,
                (now, tid),
            )
            conn.commit()
            return bool(cur.rowcount)

    def mark_reflection_task_error(self, *, trade_id: int, error: str) -> bool:
        tid = int(trade_id)
        now = time.time()
        with self._get_connection() as conn:
            cur = conn.execute(
                """
                UPDATE reflection_tasks_v1
                SET status='ERROR', last_error=?, locked_by=NULL, locked_at=NULL, updated_at=?
                WHERE trade_id=?
                """,
                (str(error or "")[:2000], now, tid),
            )
            conn.commit()
            return bool(cur.rowcount)

    def get_symbol_conclusion_row(self, symbol: str) -> Optional[Dict[str, Any]]:
        sym = str(symbol or "").strip().upper()
        if not sym:
            return None
        with self._get_connection() as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                "SELECT * FROM symbol_conclusions_v1 WHERE symbol = ? LIMIT 1",
                (sym,),
            ).fetchone()
            return dict(row) if row else None

    def upsert_symbol_conclusion(
        self,
        *,
        symbol: str,
        conclusion_text: str,
        conclusion_json: Optional[str] = None,
        confidence: Optional[float] = None,
        last_reflection_id_seen: int = 0,
    ) -> bool:
        sym = str(symbol or "").strip().upper()
        if not sym:
            return False
        now = time.time()
        with self._get_connection() as conn:
            conn.execute("BEGIN IMMEDIATE")
            conn.execute(
                """
                INSERT INTO symbol_conclusions_v1 (
                    symbol, conclusion_text, conclusion_json, confidence, last_reflection_id_seen, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(symbol) DO UPDATE SET
                    conclusion_text = excluded.conclusion_text,
                    conclusion_json = excluded.conclusion_json,
                    confidence = excluded.confidence,
                    last_reflection_id_seen = excluded.last_reflection_id_seen,
                    updated_at = excluded.updated_at
                """,
                (
                    sym,
                    str(conclusion_text or "").strip(),
                    str(conclusion_json) if conclusion_json is not None else None,
                    float(confidence) if confidence is not None else None,
                    int(last_reflection_id_seen or 0),
                    now,
                ),
            )
            conn.commit()
            return True

    def get_trade_feature_source(self, trade_id: int) -> Optional[Dict[str, Any]]:
        """Fetch normalized feature fields from trades for adaptive logging."""
        tid = int(trade_id)
        with self._get_connection() as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                """
                SELECT
                    id,
                    symbol,
                    direction,
                    venue,
                    entry_time,
                    exit_time,
                    exit_reason,
                    confidence,
                    context_snapshot,
                    signals_snapshot,
                    risk_pct_used,
                    equity_at_entry,
                    mae_pct,
                    mfe_pct,
                    realized_pnl
                FROM trades
                WHERE id = ?
                LIMIT 1
                """,
                (tid,),
            ).fetchone()
            if row is None:
                return None
            item = dict(row)

        confidence_value: Optional[float] = None
        raw_conf = item.get("confidence")
        try:
            if raw_conf is None:
                confidence_value = None
            elif isinstance(raw_conf, (int, float)):
                confidence_value = float(raw_conf)
            else:
                txt = str(raw_conf).strip()
                confidence_value = float(txt)
        except Exception:
            confidence_value = None

        context_order_type: Optional[str] = None
        raw_context = item.get("context_snapshot")
        try:
            ctx_obj = json.loads(raw_context) if isinstance(raw_context, str) else raw_context
        except Exception:
            ctx_obj = None
        if isinstance(ctx_obj, dict):
            raw_ot = str(ctx_obj.get("order_type") or "").strip().lower()
            if raw_ot in {"limit", "chase_limit", "sr_limit"}:
                context_order_type = raw_ot
        raw_signals = item.get("signals_snapshot")
        try:
            signals_obj = json.loads(raw_signals) if isinstance(raw_signals, str) else raw_signals
        except Exception:
            signals_obj = None

        order_type: Optional[str] = context_order_type
        order_type_source = "context_snapshot" if context_order_type else None
        if confidence_value is not None:
            try:
                from conviction_model import resolve_order_type_runtime

                if order_type is None:
                    order_type = resolve_order_type_runtime(float(confidence_value), db=self)
                    order_type_source = "confidence_policy"
                if order_type == "reject":
                    order_type = None
                    if order_type_source == "confidence_policy":
                        order_type_source = None
            except Exception:
                if order_type is None:
                    order_type = None
                    order_type_source = None

        def _safe_text(value: Any) -> Optional[str]:
            out = str(value or "").strip()
            return out if out else None

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
                if value == 0:
                    return False
                if value == 1:
                    return True
                return None
            txt = str(value).strip().lower()
            if txt in {"1", "true", "yes", "y", "on"}:
                return True
            if txt in {"0", "false", "no", "n", "off"}:
                return False
            return None

        symbol_norm = str(item.get("symbol") or "").upper()
        is_hip3 = symbol_norm.startswith("XYZ:")
        venue_norm = normalize_venue(item.get("venue") or "") or None

        risk_obj = ctx_obj.get("risk") if isinstance(ctx_obj, dict) and isinstance(ctx_obj.get("risk"), dict) else {}
        gate_obj = ctx_obj.get("entry_gate") if isinstance(ctx_obj, dict) and isinstance(ctx_obj.get("entry_gate"), dict) else {}
        hip3_main = (
            signals_obj.get("hip3_main")
            if isinstance(signals_obj, dict) and isinstance(signals_obj.get("hip3_main"), dict)
            else {}
        )
        hip3_components = hip3_main.get("components") if isinstance(hip3_main.get("components"), dict) else {}

        strategy_segment = _safe_text(
            (ctx_obj or {}).get("strategy_segment") if isinstance(ctx_obj, dict) else None
        )
        if strategy_segment is None:
            strategy_segment = "hip3" if is_hip3 else "perp"

        entry_gate_mode = _safe_text(
            (ctx_obj or {}).get("entry_gate_mode") if isinstance(ctx_obj, dict) else None
        )
        if entry_gate_mode is None:
            entry_gate_mode = _safe_text((ctx_obj or {}).get("gate_mode") if isinstance(ctx_obj, dict) else None)
        if entry_gate_mode is None:
            entry_gate_mode = _safe_text(gate_obj.get("gate_mode"))
        if entry_gate_mode is None:
            entry_gate_mode = "hip3" if is_hip3 else "normal"

        hip3_driver = _safe_text((ctx_obj or {}).get("hip3_driver") if isinstance(ctx_obj, dict) else None)
        if hip3_driver is None:
            hip3_driver = _safe_text(risk_obj.get("hip3_driver"))
        if hip3_driver is None:
            hip3_driver = _safe_text(hip3_main.get("driver_type"))
        if hip3_driver is not None:
            hip3_driver = hip3_driver.lower()

        hip3_flow_pass = _safe_bool((ctx_obj or {}).get("hip3_flow_pass") if isinstance(ctx_obj, dict) else None)
        if hip3_flow_pass is None:
            hip3_flow_pass = _safe_bool(hip3_main.get("flow_pass"))

        hip3_ofm_pass = _safe_bool((ctx_obj or {}).get("hip3_ofm_pass") if isinstance(ctx_obj, dict) else None)
        if hip3_ofm_pass is None:
            hip3_ofm_pass = _safe_bool(hip3_main.get("ofm_pass"))

        hip3_booster_score = _safe_float(
            (ctx_obj or {}).get("hip3_booster_score") if isinstance(ctx_obj, dict) else None
        )
        if hip3_booster_score is None:
            hip3_booster_score = _safe_float(hip3_components.get("rest_booster_score"))

        hip3_booster_size_mult = _safe_float(
            (ctx_obj or {}).get("hip3_booster_size_mult") if isinstance(ctx_obj, dict) else None
        )
        if hip3_booster_size_mult is None:
            hip3_booster_size_mult = _safe_float(risk_obj.get("hip3_booster_size_mult"))
        if hip3_booster_size_mult is None:
            hip3_booster_size_mult = _safe_float(hip3_components.get("rest_booster_size_mult"))

        pnl_usd = item.get("realized_pnl")
        risk_pct_used = item.get("risk_pct_used")
        equity_at_entry = item.get("equity_at_entry")
        pnl_r: Optional[float] = None
        try:
            # risk_pct_used is stored as percent points (e.g. 1.0 == 1%),
            # so convert to fractional risk before computing R-multiple.
            denom = float(equity_at_entry) * (float(risk_pct_used) / 100.0)
            if denom > 0 and pnl_usd is not None:
                pnl_r = float(pnl_usd) / denom
        except Exception:
            pnl_r = None

        return {
            "trade_id": tid,
            "symbol": symbol_norm,
            "direction": str(item.get("direction") or "").upper() or None,
            "venue": venue_norm,
            "closed_at": float(item.get("exit_time") or time.time()),
            "conviction": float(confidence_value) if confidence_value is not None else None,
            "order_type": order_type,
            "order_type_source": order_type_source,
            "strategy_segment": strategy_segment,
            "entry_gate_mode": entry_gate_mode,
            "hip3_driver": hip3_driver,
            "hip3_flow_pass": hip3_flow_pass,
            "hip3_ofm_pass": hip3_ofm_pass,
            "hip3_booster_score": hip3_booster_score,
            "hip3_booster_size_mult": hip3_booster_size_mult,
            "risk_pct_used": float(risk_pct_used) if risk_pct_used is not None else None,
            "equity_at_entry": float(equity_at_entry) if equity_at_entry is not None else None,
            "mae_pct": float(item.get("mae_pct")) if item.get("mae_pct") is not None else None,
            "mfe_pct": float(item.get("mfe_pct")) if item.get("mfe_pct") is not None else None,
            "pnl_usd": float(pnl_usd) if pnl_usd is not None else None,
            "pnl_r": float(pnl_r) if pnl_r is not None else None,
            "exit_reason": str(item.get("exit_reason") or "").upper() or None,
        }

    def get_trade_features_window(
        self,
        *,
        window_start: float,
        window_end: float,
        limit: int = 5000,
    ) -> List[Dict[str, Any]]:
        """Fetch feature rows in a closed-at time window."""
        with self._get_connection() as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                """
                SELECT *
                FROM trade_features
                WHERE closed_at >= ? AND closed_at <= ?
                ORDER BY closed_at DESC, id DESC
                LIMIT ?
                """,
                (float(window_start), float(window_end), int(max(1, limit))),
            ).fetchall()
            return [dict(r) for r in rows]


# =============================================================================
# Test
# =============================================================================

if __name__ == "__main__":
    import tempfile

    print("AI Trader DB Test")
    print("=" * 60)

    # Use temp file for testing
    with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as f:
        db = AITraderDB(f.name)

    # Test trade logging
    trade_id = db.log_trade_entry(
        symbol='BTC',
        direction='LONG',
        entry_price=95000.0,
        size=0.001,
        sl_price=94000.0,
        tp_price=97000.0,
        signals_agreed=['cvd', 'whale', 'fade'],
        ai_reasoning='Strong CVD + whale confirmation',
        confidence='HIGH',
        size_multiplier=0.75,
        safety_tier=1,
    )
    print(f"Created trade: {trade_id}")

    # Test fill logging
    fill_id = db.log_fill(
        trade_id=trade_id,
        venue='lighter',
        exchange_trade_id='123456789',
        symbol='BTC',
        fill_time=time.time(),
        fill_price=95000.0,
        fill_size=0.001,
        fill_type='ENTRY',
        side='BUY',
        fee=0.095,
    )
    print(f"Created fill: {fill_id}")

    # Test idempotency
    fill_id2 = db.log_fill(
        trade_id=trade_id,
        venue='lighter',
        exchange_trade_id='123456789',  # Same venue+ID
        symbol='BTC',
        fill_time=time.time(),
        fill_price=95000.0,
        fill_size=0.001,
        fill_type='ENTRY',
        side='BUY',
    )
    print(f"Duplicate fill (should be -1): {fill_id2}")

    # Test trade exit
    pnl = db.log_trade_exit(
        trade_id=trade_id,
        exit_price=96000.0,
        exit_reason='TP',
        total_fees=0.19,
    )
    print(f"Trade closed, PnL: ${pnl:.2f}")

    # Test safety state
    state = db.get_safety_state()
    print(f"\nSafety state: tier={state.current_tier}, equity=${state.current_equity:.2f}")

    db.update_safety_state(current_tier=2, daily_pnl=-150.0)
    state = db.get_safety_state()
    print(f"Updated safety: tier={state.current_tier}, daily_pnl=${state.daily_pnl:.2f}")

    print("\n" + "=" * 60)
    print("All tests passed!")
