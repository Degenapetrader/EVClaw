#!/usr/bin/env python3
"""
Captain EVP Learning Engine

Extends EnhancedLearningEngine with mistake tracking and pattern avoidance.
Learns from losses to improve future decisions.

Key Features:
- Mistake classification (WRONG_DIRECTION, BAD_TIMING, etc.)
- Pattern tracking with win rates
- Adaptive signal/symbol multipliers
- Temporary pattern avoidance for losing combos
"""

import json
import threading
from logging_utils import get_logger
import sqlite3
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

from mode_controller import get_param
from enhanced_learning_engine import EnhancedLearningEngine
from context_learning import ContextLearningEngine



# =============================================================================
# Data Classes
# =============================================================================

@dataclass(frozen=True)
class LearningConfig:
    avoid_win_rate_threshold: float = float(get_param("learning_engine", "avoid_win_rate_threshold"))
    avoid_min_trades: int = int(get_param("learning_engine", "avoid_min_trades"))
    avoid_duration_hours: float = float(get_param("learning_engine", "avoid_duration_hours"))
    loss_adjustment_mult: float = 0.98
    win_adjustment_mult: float = 1.02


LEARNING_CONFIG = LearningConfig()


@dataclass
class Mistake:
    """A trading mistake with analysis."""

    trade_id: int
    symbol: str
    direction: str
    mistake_type: str
    pnl: float
    pnl_pct: float

    # Context
    signals_at_entry: Dict[str, str]  # signal -> direction
    signals_at_exit: Dict[str, str]
    entry_price: float
    exit_price: float
    hold_hours: float

    # Analysis
    lesson: str
    avoidable: bool
    timestamp: float = field(default_factory=time.time)

    def to_dict(self) -> Dict[str, Any]:
        return {
            'trade_id': self.trade_id,
            'symbol': self.symbol,
            'direction': self.direction,
            'mistake_type': self.mistake_type,
            'pnl': self.pnl,
            'pnl_pct': self.pnl_pct,
            'signals_at_entry': self.signals_at_entry,
            'signals_at_exit': self.signals_at_exit,
            'entry_price': self.entry_price,
            'exit_price': self.exit_price,
            'hold_hours': self.hold_hours,
            'lesson': self.lesson,
            'avoidable': self.avoidable,
            'timestamp': self.timestamp,
        }


@dataclass
class PatternStats:
    """Statistics for a signal pattern + direction combo."""

    pattern_key: str  # e.g., "cvd+fade+whale:LONG"
    trades: int = 0
    wins: int = 0
    total_pnl: float = 0.0
    avg_pnl: float = 0.0
    win_rate: float = 0.0
    avg_hold_hours: float = 0.0
    best_trade: float = 0.0
    worst_trade: float = 0.0
    symbols: Set[str] = field(default_factory=set)
    last_trade_time: float = 0.0

    # Avoidance tracking
    avoid_until: Optional[float] = None
    avoid_reason: str = ""

    @property
    def is_avoided(self) -> bool:
        """Check if pattern is currently avoided."""
        if not self.avoid_until:
            return False
        return time.time() < self.avoid_until


# =============================================================================
# Learning Engine
# =============================================================================

class LearningEngine(EnhancedLearningEngine):
    """
    Extended learning engine with mistake tracking and pattern avoidance.

    This EXTENDS EnhancedLearningEngine to add:
    - Mistake classification
    - Pattern avoidance for losing combos
    - Adaptive signal/symbol adjustments
    """

    # Avoidance thresholds
    AVOID_WIN_RATE_THRESHOLD = LEARNING_CONFIG.avoid_win_rate_threshold
    AVOID_MIN_TRADES = LEARNING_CONFIG.avoid_min_trades
    AVOID_DURATION_HOURS = LEARNING_CONFIG.avoid_duration_hours

    # Adjustment thresholds
    # Unified policy:
    # - win: pnl > 0
    # - loss: pnl < 0
    # - break-even: pnl == 0 (neutral, no reward/penalty)
    LOSS_ADJUSTMENT_MULT = LEARNING_CONFIG.loss_adjustment_mult
    WIN_ADJUSTMENT_MULT = LEARNING_CONFIG.win_adjustment_mult

    def __init__(
        self,
        db_path: str,
        memory_dir: Optional[Path] = None,
        on_mistake: Optional[Callable[[Mistake], None]] = None,
    ):
        # Initialize parent engine.
        super().__init__(db_path=db_path)

        self.db_path = db_path
        self.memory_dir = Path(memory_dir) if memory_dir else Path(__file__).parent / "memory"
        self.memory_dir.mkdir(parents=True, exist_ok=True)
        self.on_mistake = on_mistake
        self.log = get_logger("learning")

        # Memory files
        self._mistakes_file = self.memory_dir / "mistakes.json"
        self._patterns_file = self.memory_dir / "patterns.json"
        self._adjustments_file = self.memory_dir / "adjustments.json"

        # In-memory caches
        self._mistakes: List[Mistake] = []
        self._patterns: Dict[str, PatternStats] = {}
        self._signal_adjustments: Dict[str, float] = {}  # signal -> multiplier
        self._symbol_adjustments: Dict[str, float] = {}  # symbol -> multiplier
        self._pattern_recorded_trade_ids: Dict[int, float] = {}
        self._pattern_recorded_trade_ids_max = 5000
        self._state_lock = threading.RLock()

        # Context learning engine (tracks win rates by context conditions)
        self._context_learning: Optional['ContextLearningEngine'] = None
        try:
            self._context_learning = ContextLearningEngine(memory_dir=self.memory_dir)
            self.log.info("Context learning engine initialized")
        except Exception as e:
            self.log.warning(f"Failed to initialize context learning: {e}")

        # DB-backed state is source of truth. File state is legacy fallback only.
        self._ensure_state_table()
        self._load_state()

    @contextmanager
    def _db_conn(self):
        conn = sqlite3.connect(self.db_path, timeout=30.0)
        try:
            conn.execute("PRAGMA busy_timeout=30000")
            yield conn
        finally:
            conn.close()

    def _ensure_state_table(self) -> None:
        try:
            with self._db_conn() as conn:
                conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS learning_state_kv (
                        key TEXT PRIMARY KEY,
                        value_json TEXT NOT NULL,
                        updated_at REAL NOT NULL
                    )
                    """
                )
                conn.commit()
        except Exception as e:
            self.log.warning(f"Failed to ensure learning_state_kv table: {e}")

    def _load_state_from_db(self) -> bool:
        loaded_any = False
        try:
            with self._db_conn() as conn:
                conn.row_factory = sqlite3.Row
                rows = conn.execute(
                    """
                    SELECT key, value_json
                    FROM learning_state_kv
                    WHERE key IN ('mistakes', 'patterns', 'adjustments')
                    """
                ).fetchall()
        except Exception as e:
            self.log.warning(f"Failed to load learning state from DB: {e}")
            return False

        row_map = {str(r["key"]): str(r["value_json"] or "") for r in rows}

        if "mistakes" in row_map:
            try:
                data = json.loads(row_map["mistakes"])
                if isinstance(data, list):
                    self._mistakes = [self._dict_to_mistake(m) for m in data[-100:]]
                    loaded_any = True
            except Exception as e:
                self.log.warning(f"Failed to parse mistakes from DB state: {e}")

        if "patterns" in row_map:
            try:
                data = json.loads(row_map["patterns"])
                if isinstance(data, dict):
                    self._patterns = {}
                    for key, p in data.items():
                        if not isinstance(p, dict):
                            continue
                        self._patterns[key] = PatternStats(
                            pattern_key=key,
                            trades=p.get('trades', 0),
                            wins=p.get('wins', 0),
                            total_pnl=p.get('total_pnl', 0),
                            avg_pnl=p.get('avg_pnl', 0),
                            win_rate=p.get('win_rate', 0),
                            avg_hold_hours=p.get('avg_hold_hours', 0),
                            best_trade=p.get('best_trade', 0),
                            worst_trade=p.get('worst_trade', 0),
                            symbols=set(p.get('symbols', [])),
                            last_trade_time=p.get('last_trade_time', 0),
                            avoid_until=p.get('avoid_until'),
                            avoid_reason=p.get('avoid_reason', ''),
                        )
                    loaded_any = True
            except Exception as e:
                self.log.warning(f"Failed to parse patterns from DB state: {e}")

        if "adjustments" in row_map:
            try:
                data = json.loads(row_map["adjustments"])
                if isinstance(data, dict):
                    self._signal_adjustments = data.get('signals', {}) if isinstance(data.get('signals', {}), dict) else {}
                    self._symbol_adjustments = data.get('symbols', {}) if isinstance(data.get('symbols', {}), dict) else {}
                    loaded_any = True
            except Exception as e:
                self.log.warning(f"Failed to parse adjustments from DB state: {e}")

        return loaded_any

    def _load_state(self):
        """Load state from DB; fallback to legacy files and migrate once."""
        with self._state_lock:
            if self._load_state_from_db():
                return

            # Legacy file fallback (one-time migration path).
            loaded_from_files = False

            if self._mistakes_file.exists():
                try:
                    data = json.loads(self._mistakes_file.read_text())
                    self._mistakes = [self._dict_to_mistake(m) for m in data[-100:]]
                    loaded_from_files = True
                except Exception as e:
                    self.log.warning(f"Failed to load legacy mistakes file: {e}")

            if self._patterns_file.exists():
                try:
                    data = json.loads(self._patterns_file.read_text())
                    if isinstance(data, dict):
                        for key, p in data.items():
                            if not isinstance(p, dict):
                                continue
                            self._patterns[key] = PatternStats(
                                pattern_key=key,
                                trades=p.get('trades', 0),
                                wins=p.get('wins', 0),
                                total_pnl=p.get('total_pnl', 0),
                                avg_pnl=p.get('avg_pnl', 0),
                                win_rate=p.get('win_rate', 0),
                                avg_hold_hours=p.get('avg_hold_hours', 0),
                                best_trade=p.get('best_trade', 0),
                                worst_trade=p.get('worst_trade', 0),
                                symbols=set(p.get('symbols', [])),
                                last_trade_time=p.get('last_trade_time', 0),
                                avoid_until=p.get('avoid_until'),
                                avoid_reason=p.get('avoid_reason', ''),
                            )
                        loaded_from_files = True
                except Exception as e:
                    self.log.warning(f"Failed to load legacy patterns file: {e}")

            if self._adjustments_file.exists():
                try:
                    data = json.loads(self._adjustments_file.read_text())
                    if isinstance(data, dict):
                        self._signal_adjustments = data.get('signals', {}) if isinstance(data.get('signals', {}), dict) else {}
                        self._symbol_adjustments = data.get('symbols', {}) if isinstance(data.get('symbols', {}), dict) else {}
                        loaded_from_files = True
                except Exception as e:
                    self.log.warning(f"Failed to load legacy adjustments file: {e}")

            if loaded_from_files:
                self.save_state()

    def save_state(self):
        """Persist state to DB (DB-first source of truth)."""
        with self._state_lock:
            self._ensure_state_table()
            mistakes = [m.to_dict() for m in self._mistakes[-100:]]
            patterns = {}
            for key, p in self._patterns.items():
                patterns[key] = {
                    'trades': p.trades,
                    'wins': p.wins,
                    'total_pnl': p.total_pnl,
                    'avg_pnl': p.avg_pnl,
                    'win_rate': p.win_rate,
                    'avg_hold_hours': p.avg_hold_hours,
                    'best_trade': p.best_trade,
                    'worst_trade': p.worst_trade,
                    'symbols': list(p.symbols),
                    'last_trade_time': p.last_trade_time,
                    'avoid_until': p.avoid_until,
                    'avoid_reason': p.avoid_reason,
                }
            adjustments = {
                'signals': self._signal_adjustments,
                'symbols': self._symbol_adjustments,
            }

            try:
                now_ts = time.time()
                with self._db_conn() as conn:
                    conn.execute(
                        """
                        INSERT INTO learning_state_kv(key, value_json, updated_at)
                        VALUES(?, ?, ?)
                        ON CONFLICT(key) DO UPDATE SET
                            value_json=excluded.value_json,
                            updated_at=excluded.updated_at
                        """,
                        ("mistakes", json.dumps(mistakes, separators=(",", ":"), ensure_ascii=False), now_ts),
                    )
                    conn.execute(
                        """
                        INSERT INTO learning_state_kv(key, value_json, updated_at)
                        VALUES(?, ?, ?)
                        ON CONFLICT(key) DO UPDATE SET
                            value_json=excluded.value_json,
                            updated_at=excluded.updated_at
                        """,
                        ("patterns", json.dumps(patterns, separators=(",", ":"), ensure_ascii=False), now_ts),
                    )
                    conn.execute(
                        """
                        INSERT INTO learning_state_kv(key, value_json, updated_at)
                        VALUES(?, ?, ?)
                        ON CONFLICT(key) DO UPDATE SET
                            value_json=excluded.value_json,
                            updated_at=excluded.updated_at
                        """,
                        ("adjustments", json.dumps(adjustments, separators=(",", ":"), ensure_ascii=False), now_ts),
                    )
                    conn.commit()
            except Exception as e:
                self.log.error(f"Failed to save learning state: {e}")

    def _dict_to_mistake(self, d: Dict) -> Mistake:
        """Convert dict to Mistake."""
        return Mistake(
            trade_id=d['trade_id'],
            symbol=d['symbol'],
            direction=d['direction'],
            mistake_type=d['mistake_type'],
            pnl=d['pnl'],
            pnl_pct=d['pnl_pct'],
            signals_at_entry=d.get('signals_at_entry', {}),
            signals_at_exit=d.get('signals_at_exit', {}),
            entry_price=d.get('entry_price', 0),
            exit_price=d.get('exit_price', 0),
            hold_hours=d.get('hold_hours', 0),
            lesson=d.get('lesson', ''),
            avoidable=d.get('avoidable', False),
            timestamp=d.get('timestamp', 0),
        )

    # =========================================================================
    # Mistake Analysis
    # =========================================================================

    @staticmethod
    def _is_win_pnl(pnl: Any) -> bool:
        try:
            return float(pnl) > 0.0
        except Exception:
            return False

    @staticmethod
    def _is_loss_pnl(pnl: Any) -> bool:
        try:
            return float(pnl) < 0.0
        except Exception:
            return False

    def analyze_trade(
        self,
        trade_id: int,
        persist: bool = True,
        trade: Optional[Dict] = None,
    ) -> Optional[Mistake]:
        """
        Analyze a closed trade to identify mistakes.

        Returns Mistake if this was a losing trade, None otherwise.
        """
        # Get trade from database
        trade = trade or self._get_trade_from_db(trade_id)
        if not trade:
            return None

        # Only analyze losing trades
        pnl = trade.get('realized_pnl', 0) or 0
        if not self._is_loss_pnl(pnl):
            return None

        # Extract data
        symbol = trade.get('symbol', '')
        direction = trade.get('direction', '')
        entry_price = trade.get('entry_price', 0)
        exit_price = trade.get('exit_price', 0)
        pnl_pct = trade.get('realized_pnl_pct', 0)

        # Parse signals
        entry_signals = self._parse_signals_snapshot(trade.get('signals_snapshot'))
        exit_signals = {}  # Would need exit snapshot from fill_reconciler

        # Calculate hold time
        entry_time = trade.get('entry_time', 0)
        exit_time = trade.get('exit_time', 0)
        hold_hours = (exit_time - entry_time) / 3600 if exit_time and entry_time else 0

        # Classify mistake
        mistake_type = self._classify_mistake(trade, entry_signals, exit_signals)

        # Extract lesson
        lesson = self._extract_lesson(mistake_type, trade)

        # Determine if avoidable
        avoidable = mistake_type in ('IGNORED_VETO', 'CHASED_MOVE', 'FOUGHT_TREND', 'SIZE_TOO_BIG')

        mistake = Mistake(
            trade_id=trade_id,
            symbol=symbol,
            direction=direction,
            mistake_type=mistake_type,
            pnl=pnl,
            pnl_pct=pnl_pct,
            signals_at_entry=entry_signals,
            signals_at_exit=exit_signals,
            entry_price=entry_price,
            exit_price=exit_price,
            hold_hours=hold_hours,
            lesson=lesson,
            avoidable=avoidable,
        )

        # Store
        with self._state_lock:
            self._mistakes.append(mistake)
        self.log.info(f"Mistake logged: {symbol} {mistake_type} (${pnl:.2f})")

        # Callback
        if self.on_mistake:
            self.on_mistake(mistake)

        # Update adjustments
        self._update_adjustments_from_mistake(mistake)

        # Save
        if persist:
            self.save_state()

        return mistake

    def _classify_mistake(
        self,
        trade: Dict,
        entry_signals: Dict[str, str],
        exit_signals: Dict[str, str]
    ) -> str:
        """Classify the type of mistake made."""
        direction = trade.get('direction', '')
        exit_reason = trade.get('exit_reason', '')
        pnl_pct = trade.get('realized_pnl_pct', 0) or 0

        # Check for veto that was ignored
        # (Would need to check if whale/cvd were opposite at entry)
        whale_at_entry = entry_signals.get('whale', 'NEUTRAL')
        if whale_at_entry != 'NEUTRAL' and whale_at_entry != direction:
            return 'IGNORED_VETO'

        cvd_at_entry = entry_signals.get('cvd', 'NEUTRAL')
        if cvd_at_entry != 'NEUTRAL' and cvd_at_entry != direction:
            # Check if we had strong opposing CVD
            return 'FOUGHT_TREND'

        # Check for early exit
        if exit_reason == 'DECAY' and abs(pnl_pct) < 1:
            return 'EXITED_TOO_EARLY'

        # Check for SL hit
        if exit_reason in ('SL', 'LIKELY_SL'):
            # If quick SL, likely wrong direction or bad timing
            try:
                exit_ts = float(trade.get('exit_time') or 0.0)
                entry_ts = float(trade.get('entry_time') or 0.0)
                hold_hours = ((exit_ts - entry_ts) / 3600.0) if (exit_ts > 0 and entry_ts > 0) else 0.0
            except Exception:
                hold_hours = 0.0
            if hold_hours < 0.5:
                return 'BAD_TIMING'
            return 'WRONG_DIRECTION'

        # Check for size issues
        size_mult = trade.get('size_multiplier', 1.0) or 1.0
        if size_mult > 1.5 and pnl_pct < -3:
            return 'SIZE_TOO_BIG'

        # Default
        return 'WRONG_DIRECTION'

    def _extract_lesson(self, mistake_type: str, trade: Dict) -> str:
        """Extract a lesson from the mistake."""
        symbol = trade.get('symbol', '')
        direction = trade.get('direction', '')

        lessons = {
            'WRONG_DIRECTION': f"Signals for {symbol} {direction} were misleading - consider more confirmation",
            'BAD_TIMING': f"Entry timing for {symbol} was poor - wait for better price action",
            'SIZE_TOO_BIG': f"Position in {symbol} was too large for the setup",
            'IGNORED_VETO': f"Ignored veto signal in {symbol} - respect WHALE/CVD vetoes",
            'CHASED_MOVE': f"Chased extended move in {symbol} - wait for pullbacks",
            'FOUGHT_TREND': f"Faded momentum in {symbol} - don't fight strong trends",
            'HELD_TOO_LONG': f"Overstayed {symbol} position - tighten exit criteria",
            'EXITED_TOO_EARLY': f"Exited {symbol} too early - give trades room to work",
        }

        return lessons.get(mistake_type, f"Review {symbol} {direction} trade for patterns")

    def _parse_signals_snapshot(self, snapshot: Optional[str]) -> Dict[str, str]:
        """Parse signals snapshot JSON to dict of signal -> direction."""
        if not snapshot:
            return {}
        try:
            data = json.loads(snapshot)
            result = {}
            # IMPORTANT: Keep this list in sync with the signals we actually trade on.
            # HIP3: `hip3_main` must be included so the learning engine
            # can attribute wins/losses to HIP3 predator signals.
            for sig_name in [
                'cvd',
                'fade',
                'liq_pnl',
                'whale',
                'dead_capital',
                'ofm',
                'hip3_main',
            ]:
                sig_data = data.get(sig_name, {})
                if isinstance(sig_data, dict):
                    result[sig_name] = sig_data.get('direction', 'NEUTRAL')
            return result
        except Exception as exc:
            self.log.warning(f"Failed to parse signals_snapshot JSON: {exc}")
            return {}

    # =========================================================================
    # Pattern Tracking
    # =========================================================================

    def _record_pattern_outcome(
        self,
        *,
        pattern_key: str,
        pnl: float,
        symbol: Optional[str] = None,
        hold_hours: Optional[float] = None,
        persist: bool = False,
    ) -> PatternStats:
        """Record one pattern outcome update and optional persistence."""
        with self._state_lock:
            stats = self._patterns.get(pattern_key)
            if stats is None:
                stats = PatternStats(pattern_key=pattern_key)
                self._patterns[pattern_key] = stats

            stats.trades += 1
            stats.total_pnl += pnl
            stats.avg_pnl = stats.total_pnl / stats.trades
            if self._is_win_pnl(pnl):
                stats.wins += 1
            stats.win_rate = stats.wins / stats.trades if stats.trades > 0 else 0.0
            stats.best_trade = max(stats.best_trade, pnl)
            stats.worst_trade = min(stats.worst_trade, pnl)
            if symbol:
                stats.symbols.add(str(symbol))
            if hold_hours is not None and hold_hours > 0:
                total_hours = stats.avg_hold_hours * (stats.trades - 1) + float(hold_hours)
                stats.avg_hold_hours = total_hours / stats.trades
            stats.last_trade_time = time.time()

            if stats.trades >= self.AVOID_MIN_TRADES and stats.win_rate < self.AVOID_WIN_RATE_THRESHOLD:
                if not stats.is_avoided:
                    stats.avoid_until = time.time() + (self.AVOID_DURATION_HOURS * 3600)
                    stats.avoid_reason = (
                        f"Win rate {stats.win_rate:.0%} < {self.AVOID_WIN_RATE_THRESHOLD:.0%}"
                    )
                    self.log.warning(f"Avoiding pattern {pattern_key}: {stats.avoid_reason}")

            if persist:
                self.save_state()
            return stats

    def _is_pattern_recorded_for_trade(self, trade_id: Optional[int]) -> bool:
        """Check if this trade already updated pattern stats in this process."""
        if trade_id is None:
            return False
        try:
            tid = int(trade_id)
        except Exception:
            return False
        with self._state_lock:
            return tid in self._pattern_recorded_trade_ids

    def _mark_pattern_recorded_for_trade(self, trade_id: Optional[int]) -> None:
        """Remember trade IDs that already contributed to pattern outcomes."""
        if trade_id is None:
            return
        try:
            tid = int(trade_id)
        except Exception:
            return
        with self._state_lock:
            self._pattern_recorded_trade_ids[tid] = time.time()
            if len(self._pattern_recorded_trade_ids) > int(self._pattern_recorded_trade_ids_max):
                # Prune oldest marker to keep memory bounded.
                oldest_tid, _ = min(
                    self._pattern_recorded_trade_ids.items(),
                    key=lambda kv: float(kv[1] or 0.0),
                )
                self._pattern_recorded_trade_ids.pop(oldest_tid, None)

    def update_pattern_stats(
        self,
        trade_id: int,
        persist: bool = True,
        trade: Optional[Dict] = None,
    ) -> Optional[str]:
        """
        Update pattern statistics after a trade closes.

        Returns the pattern key if updated.
        """
        trade = trade or self._get_trade_from_db(trade_id)
        if not trade:
            return None

        # Get signals that agreed
        signals_agreed = trade.get('signals_agreed', '[]')
        if isinstance(signals_agreed, str):
            try:
                signals_agreed = json.loads(signals_agreed)
            except Exception as exc:
                self.log.debug(f"Failed to parse signals_agreed for trade {trade_id}: {exc}")
                signals_agreed = []

        if not signals_agreed:
            return None

        # Build pattern key
        direction = trade.get('direction', 'UNKNOWN')
        sorted_signals = sorted(s.lower() for s in signals_agreed)
        pattern_key = '+'.join(sorted_signals) + ':' + direction

        entry_time = trade.get('entry_time', 0)
        exit_time = trade.get('exit_time', 0)
        hold_hours = None
        if entry_time and exit_time:
            try:
                hold_hours = (float(exit_time) - float(entry_time)) / 3600.0
            except Exception:
                hold_hours = None

        self._record_pattern_outcome(
            pattern_key=pattern_key,
            pnl=float(trade.get('realized_pnl', 0) or 0.0),
            symbol=trade.get('symbol', ''),
            hold_hours=hold_hours,
            persist=persist,
        )
        self._mark_pattern_recorded_for_trade(trade_id)
        return pattern_key

    def should_avoid_pattern(self, signals: List[str], direction: str) -> bool:
        """
        Check if a signal pattern should be avoided.

        Args:
            signals: List of signal names
            direction: Trade direction

        Returns:
            True if pattern should be avoided
        """
        sorted_signals = sorted(s.lower() for s in signals)
        pattern_key = '+'.join(sorted_signals) + ':' + direction

        stats = self._patterns.get(pattern_key)
        if not stats:
            return False

        return stats.is_avoided

    # =========================================================================
    # Adaptive Adjustments
    # =========================================================================

    def get_signal_adjustment(self, signal_name: str, direction: Optional[str] = None) -> float:
        """
        Get adjustment multiplier for a signal.
        
        Args:
            signal_name: Signal name (e.g., 'dead_capital')
            direction: Trade direction ('LONG' or 'SHORT'). If provided, uses directional key.

        Returns 1.0 if no adjustment, <1.0 if signal has been performing poorly.
        """
        # Backwards-compat: allow non-directional keys (e.g. {'cvd': 0.8}).
        try:
            plain = self._signal_adjustments.get(str(signal_name or "").lower())
            if plain is None:
                plain = self._signal_adjustments.get(str(signal_name or ""))
            if plain is not None:
                return float(plain)
        except Exception:
            pass

        if direction:
            sig_key = f"{signal_name.lower()}:{direction.upper()}"
            return float(self._signal_adjustments.get(sig_key, 1.0))
        # Fallback: check both directions and return the lower (more conservative)
        long_adj = float(self._signal_adjustments.get(f"{signal_name.lower()}:LONG", 1.0))
        short_adj = float(self._signal_adjustments.get(f"{signal_name.lower()}:SHORT", 1.0))
        return min(long_adj, short_adj)

    def get_symbol_adjustment(self, symbol: str) -> float:
        """
        Get adjustment multiplier for a symbol.

        Returns 1.0 if no adjustment, <1.0 if symbol trades have been losing.
        """
        return self._symbol_adjustments.get(symbol.upper(), 1.0)



    def _update_adjustments_from_mistake(self, mistake: Mistake):
        """Update adjustments based on a mistake (losing trade)."""
        with self._state_lock:
            # Update symbol adjustment
            symbol = mistake.symbol.upper()
            current = self._symbol_adjustments.get(symbol, 1.0)

            # Symmetric update policy: 2% penalty on loss.
            new_mult = max(0.3, current * self.LOSS_ADJUSTMENT_MULT)
            self._symbol_adjustments[symbol] = new_mult

            self.log.info(f"Symbol {symbol} adjustment (loss): {current:.2f} -> {new_mult:.2f}")

            # Update signal adjustments based on entry signals (directional: signal:LONG or signal:SHORT)
            for signal, sig_direction in mistake.signals_at_entry.items():
                if sig_direction == mistake.direction:
                    # This signal agreed with our losing direction - penalize for THIS direction only
                    sig_key = f"{signal.lower()}:{mistake.direction.upper()}"
                    current = self._signal_adjustments.get(sig_key, 1.0)
                    new_mult = max(0.5, current * self.LOSS_ADJUSTMENT_MULT)
                    self._signal_adjustments[sig_key] = new_mult
                    self.log.info(f"Signal {sig_key} adjustment (loss): {current:.2f} -> {new_mult:.2f}")

    def _update_adjustments_from_win(
        self,
        symbol: str,
        signals_at_entry: Dict[str, str],
        direction: str,
        persist: bool = True,
    ):
        """Update adjustments based on a winning trade (TP or profitable exit)."""
        with self._state_lock:
            # Symmetric update policy: 2% reward on win.
            symbol = symbol.upper()
            current = self._symbol_adjustments.get(symbol, 1.0)
            new_mult = min(1.5, current * self.WIN_ADJUSTMENT_MULT)
            self._symbol_adjustments[symbol] = new_mult
            self.log.info(f"Symbol {symbol} adjustment (win): {current:.2f} -> {new_mult:.2f}")

            # Update signal adjustments for signals that agreed with winning direction (directional)
            for signal, sig_dir in signals_at_entry.items():
                if sig_dir == direction:
                    sig_key = f"{signal.lower()}:{direction.upper()}"
                    current = self._signal_adjustments.get(sig_key, 1.0)
                    new_mult = min(1.5, current * self.WIN_ADJUSTMENT_MULT)
                    self._signal_adjustments[sig_key] = new_mult
                    self.log.info(f"Signal {sig_key} adjustment (win): {current:.2f} -> {new_mult:.2f}")

        if persist:
            self.save_state()


    def decay_adjustments(self):
        """Decay adjustments toward 1.0 (bidirectional). Call daily."""
        decay_rate = 0.02  # 2% per day toward 1.0

        with self._state_lock:
            for key in list(self._signal_adjustments.keys()):
                current = self._signal_adjustments[key]
                if current < 1.0:
                    # Penalty decaying up toward 1.0
                    new_val = min(1.0, current + decay_rate)
                elif current > 1.0:
                    # Reward decaying down toward 1.0
                    new_val = max(1.0, current - decay_rate)
                else:
                    new_val = 1.0

                if abs(new_val - 1.0) < 0.001:
                    del self._signal_adjustments[key]
                else:
                    self._signal_adjustments[key] = new_val

            for key in list(self._symbol_adjustments.keys()):
                current = self._symbol_adjustments[key]
                if current < 1.0:
                    new_val = min(1.0, current + decay_rate)
                elif current > 1.0:
                    new_val = max(1.0, current - decay_rate)
                else:
                    new_val = 1.0

                if abs(new_val - 1.0) < 0.001:
                    del self._symbol_adjustments[key]
                else:
                    self._symbol_adjustments[key] = new_val

            self.save_state()
        self.log.info(f"Decay applied: {len(self._signal_adjustments)} signal adj, {len(self._symbol_adjustments)} symbol adj remaining")

    def daily_decay_check(self):
        """Check if we should run daily decay (call from live_agent on each cycle)."""
        today = datetime.now(timezone.utc).strftime('%Y-%m-%d')
        last_decay = getattr(self, '_last_decay_date', None)
        if last_decay != today:
            self._last_decay_date = today
            self.decay_adjustments()
            return True
        return False

    # =========================================================================
    # Brain Decision Recording
    # =========================================================================

    async def process_closed_trade(self, trade_id: int) -> None:
        """
        Full processing for a closed trade:
        1. Update combo stats (from parent)
        2. Analyze trade for mistakes OR reward wins (updates adjustments)
        3. Update pattern stats (sets avoidance if needed)
        4. Update context feature stats

        Call this from fill_reconciler when a trade is closed.
        """
        # Parent handles combo stats.
        await super().process_closed_trade(trade_id)

        # Fetch trade to check win/loss
        trade = self._get_trade_from_db(trade_id)
        state_changed = False
        if trade:
            pnl = trade.get('realized_pnl', 0) or 0
            direction = trade.get('direction', '')
            symbol = trade.get('symbol', '')
            
            if self._is_win_pnl(pnl):
                # Winning trade - apply reward
                entry_signals = self._parse_signals_snapshot(trade.get('signals_snapshot'))
                self._update_adjustments_from_win(symbol, entry_signals, direction, persist=False)
                state_changed = True
                self.log.info(f"Win reward applied: {symbol} ${pnl:.2f}")
            elif self._is_loss_pnl(pnl):
                # Losing trade - analyze_trade handles penalties
                mistake = self.analyze_trade(
                    trade_id,
                    persist=False,
                    trade=trade,
                )  # Classifies mistakes, updates adjustments
                state_changed = bool(mistake) or state_changed
            else:
                self.log.debug(f"Break-even trade treated as neutral: {symbol} ${pnl:.2f}")

            # Update context feature stats (NEW)
            if self._context_learning is not None:
                context_snapshot_str = trade.get('context_snapshot')
                if context_snapshot_str:
                    try:
                        context_snapshot = json.loads(context_snapshot_str)
                        conditions = self._context_learning.update_stats(
                            context_snapshot, direction, pnl
                        )
                        self.log.info(
                            f"Context stats updated for {symbol}: {conditions}"
                        )
                    except Exception as e:
                        self.log.warning(f"Failed to update context stats: {e}")

        pattern_key = self.update_pattern_stats(
            trade_id,
            persist=False,
            trade=trade,
        )  # Updates patterns.json, sets avoid_until
        if pattern_key:
            state_changed = True

        if state_changed:
            self.save_state()

        self.log.debug(f"Full learning pipeline complete for trade {trade_id}")

    # =========================================================================
    # Database Access
    # =========================================================================

    def _get_trade_from_db(self, trade_id: int) -> Optional[Dict]:
        """Get trade from database."""
        try:
            with self._db_conn() as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.execute(
                    "SELECT * FROM trades WHERE id = ?",
                    (trade_id,)
                )
                row = cursor.fetchone()
                if row:
                    return dict(row)
        except Exception as e:
            self.log.error(f"Failed to get trade {trade_id}: {e}")
        return None

    # =========================================================================
    # Reporting
    # =========================================================================


# =============================================================================
# Module exports
# =============================================================================

__all__ = [
    'Mistake',
    'PatternStats',
    'LearningEngine',
]
