#!/usr/bin/env python3
"""
Per-Symbol R/R Learning with MAE/MFE Analysis

Learns optimal SL/TP multipliers for each symbol based on:
1. Historical trade outcomes (win/loss, SL hit vs TP hit)
2. MAE/MFE (Maximum Adverse/Favorable Excursion) analysis
3. Bayesian shrinkage toward category priors for low-sample symbols

Updates after every N trades (default: 5) per symbol.
"""

import argparse
import asyncio
import concurrent.futures
import logging
import sqlite3
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Tuple
from statistics import mean, quantiles

from mode_controller import get_param
from ai_trader_db import AITraderDB
from mae_mfe import compute_mae_mfe as canonical_compute_mae_mfe, fetch_candles as canonical_fetch_candles
from symbol_rr_defaults import CATEGORY_DEFAULTS, classify_symbol_category, defaults_for_category

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(name)s] %(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# Constants
# ─────────────────────────────────────────────────────────────────────────────

MIN_SAMPLES_FOR_SYMBOL_POLICY = get_param("symbol_rr_learning", "min_samples_for_symbol_policy")  # Minimum trades before symbol gets own policy
MIN_SAMPLES_FOR_CONFIDENCE = get_param("symbol_rr_learning", "min_samples_for_confidence")    # High confidence threshold
LOOKBACK_DAYS = get_param("symbol_rr_learning", "lookback_days")                # How far back to look for trades

# OI fetch limits (reduce blocking time)
BINANCE_OI_TIMEOUT_SEC = 5.0
BINANCE_OI_MAX_WORKERS = 8
HL_OI_TIMEOUT_SEC = 5.0
BINANCE_EXCHANGE_INFO_TIMEOUT_SEC = 5.0
OI_FETCH_TOTAL_BUDGET_SEC = 8.0
OI_FETCH_RETRY_COOLDOWN_SEC = 60.0

# Default multipliers (ATR-based)
DEFAULT_SL_MULT = get_param("symbol_rr_learning", "default_sl_mult")
DEFAULT_TP_MULT = get_param("symbol_rr_learning", "default_tp_mult")

# Bounds for optimization
SL_MIN = get_param("symbol_rr_learning", "sl_min")
SL_MAX = get_param("symbol_rr_learning", "sl_max")
TP_MIN = get_param("symbol_rr_learning", "tp_min")
TP_MAX = get_param("symbol_rr_learning", "tp_max")

# Bayesian shrinkage parameters
PRIOR_STRENGTH = get_param("symbol_rr_learning", "prior_strength")  # Equivalent sample size of prior

def compute_shrinkage_weight(n_samples: int, prior_strength: int = PRIOR_STRENGTH) -> float:
    """Weight for observed data (0 to 1). prior_strength = equivalent sample size of prior."""
    return n_samples / (n_samples + prior_strength)

def shrink_toward_prior(observed: float, prior: float, n_samples: int) -> float:
    """Apply Bayesian shrinkage: blend observed toward prior based on sample size."""
    w = compute_shrinkage_weight(n_samples)
    return w * observed + (1 - w) * prior

# Cache for OI data (refreshed periodically)
_oi_cache: Dict[str, float] = {}
_oi_cache_time: float = 0
_oi_cache_lock = threading.Lock()
_oi_next_retry_ts: float = 0
OI_CACHE_TTL = get_param("symbol_rr_learning", "oi_cache_ttl")  # 1 hour

# OI weighting: 75% Binance, 25% Hyperliquid
BINANCE_OI_WEIGHT = get_param("symbol_rr_learning", "binance_oi_weight")
HL_OI_WEIGHT = get_param("symbol_rr_learning", "hl_oi_weight")


# ─────────────────────────────────────────────────────────────────────────────
# Data Classes
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class TradeRecord:
    """Single trade with MAE/MFE data."""
    id: int
    symbol: str
    direction: str
    entry_price: float
    exit_price: Optional[float]
    sl_price: Optional[float]
    tp_price: Optional[float]
    realized_pnl: Optional[float]
    realized_pnl_pct: Optional[float]
    exit_reason: Optional[str]
    mae_pct: Optional[float] = None
    mfe_pct: Optional[float] = None
    entry_time: Optional[float] = None
    exit_time: Optional[float] = None

    @property
    def is_winner(self) -> bool:
        return self.realized_pnl is not None and self.realized_pnl > 0

    @property
    def hit_sl(self) -> bool:
        return self.exit_reason == 'SL'

    @property
    def hit_tp(self) -> bool:
        return self.exit_reason == 'TP'

    @property
    def sl_distance_pct(self) -> Optional[float]:
        """Distance to SL as percentage of entry price."""
        if not self.sl_price or not self.entry_price:
            return None
        return abs(self.sl_price - self.entry_price) / self.entry_price * 100

    @property
    def tp_distance_pct(self) -> Optional[float]:
        """Distance to TP as percentage of entry price."""
        if not self.tp_price or not self.entry_price:
            return None
        return abs(self.tp_price - self.entry_price) / self.entry_price * 100


@dataclass
class SymbolStats:
    """Aggregated statistics for a symbol."""
    symbol: str
    trades: int = 0
    wins: int = 0
    losses: int = 0
    sl_hits: int = 0
    tp_hits: int = 0
    other_exits: int = 0
    total_pnl: float = 0.0
    avg_pnl_pct: float = 0.0
    win_rate: float = 0.0

    # MAE/MFE stats
    avg_mae_pct: Optional[float] = None
    avg_mfe_pct: Optional[float] = None
    max_mae_pct: Optional[float] = None
    max_mfe_pct: Optional[float] = None
    p90_mae_pct: Optional[float] = None  # 90th percentile MAE for tail risk

    # Current SL/TP distances used
    avg_sl_distance_pct: Optional[float] = None
    avg_tp_distance_pct: Optional[float] = None

    # Computed optimal multipliers
    optimal_sl_mult: Optional[float] = None
    optimal_tp_mult: Optional[float] = None
    expectancy: float = 0.0
    confidence: str = 'low'  # low, medium, high


@dataclass
class SymbolPolicy:
    """Policy for a single symbol."""
    symbol: str
    sl_mult: float
    tp_mult: float
    samples: int
    win_rate: float
    expectancy: float
    avg_mae_pct: Optional[float]
    avg_mfe_pct: Optional[float]
    confidence: str
    last_updated: datetime
    notes: str = ""


# ─────────────────────────────────────────────────────────────────────────────
# MAE/MFE helpers (canonical module)
# ─────────────────────────────────────────────────────────────────────────────

async def _compute_trade_mae_mfe(trade: TradeRecord) -> Tuple[Optional[float], Optional[float]]:
    """Compute MAE/MFE for a trade using canonical mae_mfe helpers."""
    if not trade.entry_time or not trade.exit_time:
        return None, None

    try:
        candles = await canonical_fetch_candles(
            trade.symbol,
            int(trade.entry_time * 1000),
            int(trade.exit_time * 1000),
        )
        if not candles:
            return None, None
        mae_pct, mfe_pct, _, _ = canonical_compute_mae_mfe(
            direction=trade.direction,
            entry_price=trade.entry_price,
            candles=candles,
        )
        return mae_pct, mfe_pct
    except Exception as e:
        logger.warning(f"Failed to compute MAE/MFE for {trade.symbol}: {e}")
        return None, None


# ─────────────────────────────────────────────────────────────────────────────
# Symbol R/R Learner
# ─────────────────────────────────────────────────────────────────────────────

class SymbolRRLearner:
    """
    Per-symbol risk/reward learning engine.

    Analyzes historical trades and MAE/MFE to optimize SL/TP multipliers.
    """

    def __init__(self, db_path: str = "ai_trader.db"):
        self.db_path = db_path
        self._conn: Optional[sqlite3.Connection] = None
        self._conn_lock = threading.Lock()

    def _ensure_symbol_policy_table(self, conn: sqlite3.Connection) -> None:
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
        conn.commit()

    def _get_connection(self) -> sqlite3.Connection:
        with self._conn_lock:
            conn = self._conn
            if conn is not None:
                try:
                    conn.execute("SELECT 1")
                    return conn
                except Exception:
                    try:
                        conn.close()
                    except Exception:
                        pass
                    self._conn = None

            conn = sqlite3.connect(self.db_path, timeout=30.0, check_same_thread=False)
            conn.execute("PRAGMA busy_timeout=30000")
            conn.row_factory = sqlite3.Row
            self._ensure_symbol_policy_table(conn)
            self._conn = conn
            return conn

    def close(self) -> None:
        with self._conn_lock:
            conn = self._conn
            self._conn = None
        if conn is None:
            return
        try:
            conn.close()
        except Exception:
            pass

    def __del__(self) -> None:
        try:
            self.close()
        except Exception:
            pass

    def _fetch_oi_data(self) -> Dict[str, float]:
        """Fetch OI data from Binance (75%) + Hyperliquid (25%), cached."""
        global _oi_cache, _oi_cache_time, _oi_next_retry_ts
        now = datetime.now(timezone.utc).timestamp()
        with _oi_cache_lock:
            if _oi_cache and (now - _oi_cache_time) < OI_CACHE_TTL:
                return dict(_oi_cache)
            # Cooldown failed refreshes to avoid blocking entry path repeatedly.
            if now < float(_oi_next_retry_ts or 0.0):
                return dict(_oi_cache) if _oi_cache else {}

        deadline = time.monotonic() + max(0.1, float(OI_FETCH_TOTAL_BUDGET_SEC))

        def _remaining_budget() -> float:
            return max(0.0, deadline - time.monotonic())

        def _request_timeout(limit_sec: float) -> float:
            return max(0.2, min(float(limit_sec), _remaining_budget()))

        import requests
        hl_oi: Dict[str, float] = {}
        binance_oi: Dict[str, float] = {}

        # 1. Fetch Hyperliquid OI
        if _remaining_budget() > 0:
            try:
                resp = requests.post(
                    "https://api.hyperliquid.xyz/info",
                    json={"type": "metaAndAssetCtxs"},
                    timeout=_request_timeout(HL_OI_TIMEOUT_SEC),
                )
                if resp.status_code == 200:
                    data = resp.json()
                    if len(data) >= 2:
                        universe = data[0].get('universe', [])
                        asset_ctxs = data[1]
                        for i, asset in enumerate(universe):
                            symbol = asset.get('name', '')
                            if i < len(asset_ctxs):
                                ctx = asset_ctxs[i]
                                oi = float(ctx.get('openInterest', 0))
                                mark = float(ctx.get('markPx', 0))
                                hl_oi[symbol] = oi * mark
            except Exception as e:
                logger.warning(f"Failed to fetch Hyperliquid OI: {e}")
        else:
            logger.warning("Skipping Hyperliquid OI refresh: fetch budget exhausted")

        # 2. Fetch Binance Futures OI (top symbols only to avoid rate limits)
        if _remaining_budget() > 0:
            try:
                # Get all USDT perpetual symbols
                info_resp = requests.get(
                    "https://fapi.binance.com/fapi/v1/exchangeInfo",
                    timeout=_request_timeout(BINANCE_EXCHANGE_INFO_TIMEOUT_SEC),
                )
                if info_resp.status_code == 200:
                    symbols_info = info_resp.json().get('symbols', [])
                    usdt_perps = [
                        s['symbol']
                        for s in symbols_info
                        if s['symbol'].endswith('USDT') and s['contractType'] == 'PERPETUAL'
                    ]

                    # Fetch OI for symbols we trade on HL (intersection)
                    hl_symbols = set(hl_oi.keys())
                    binance_targets = []
                    for binance_sym in usdt_perps[:100]:  # Limit to avoid rate limits
                        base = binance_sym.replace('USDT', '')
                        if base in hl_symbols:
                            binance_targets.append(binance_sym)

                    def _fetch_binance_oi(sym: str) -> Optional[Tuple[str, float]]:
                        base_sym = sym.replace('USDT', '')
                        if _remaining_budget() <= 0:
                            return None
                        try:
                            oi_resp = requests.get(
                                f"https://fapi.binance.com/fapi/v1/openInterest?symbol={sym}",
                                timeout=_request_timeout(BINANCE_OI_TIMEOUT_SEC),
                            )
                            if _remaining_budget() <= 0:
                                return None
                            price_resp = requests.get(
                                f"https://fapi.binance.com/fapi/v1/ticker/price?symbol={sym}",
                                timeout=_request_timeout(BINANCE_OI_TIMEOUT_SEC),
                            )
                            if oi_resp.status_code == 200 and price_resp.status_code == 200:
                                oi = float(oi_resp.json()['openInterest'])
                                price = float(price_resp.json()['price'])
                                return base_sym, oi * price
                        except Exception:
                            return None
                        return None

                    if binance_targets and _remaining_budget() > 0:
                        max_workers = min(BINANCE_OI_MAX_WORKERS, len(binance_targets))
                        ex = concurrent.futures.ThreadPoolExecutor(max_workers=max_workers)
                        futures = []
                        try:
                            futures = [ex.submit(_fetch_binance_oi, sym) for sym in binance_targets]
                            remaining = _remaining_budget()
                            if remaining > 0:
                                for fut in concurrent.futures.as_completed(futures, timeout=remaining):
                                    res = fut.result()
                                    if res:
                                        base, oi_usd = res
                                        binance_oi[base] = oi_usd
                            else:
                                logger.warning("Binance OI skipped: budget exhausted before futures collection")
                        except concurrent.futures.TimeoutError:
                            logger.warning("Binance OI fetch exceeded total budget; using partial results")
                        finally:
                            for fut in futures:
                                if not fut.done():
                                    fut.cancel()
                            ex.shutdown(wait=False, cancel_futures=True)

                    logger.info(f"Fetched Binance OI for {len(binance_oi)} symbols")
            except Exception as e:
                logger.warning(f"Failed to fetch Binance OI: {e}")
        else:
            logger.warning("Skipping Binance OI refresh: fetch budget exhausted")

        # 3. Combine: 75% Binance + 25% HL (use HL only if Binance unavailable)
        combined: Dict[str, float] = {}
        all_symbols = set(hl_oi.keys()) | set(binance_oi.keys())

        for sym in all_symbols:
            hl = hl_oi.get(sym, 0)
            bn = binance_oi.get(sym, 0)

            if bn > 0 and hl > 0:
                combined[sym] = BINANCE_OI_WEIGHT * bn + HL_OI_WEIGHT * hl
            elif bn > 0:
                combined[sym] = bn
            else:
                combined[sym] = hl

        with _oi_cache_lock:
            now2 = datetime.now(timezone.utc).timestamp()
            if _oi_cache and (now2 - _oi_cache_time) < OI_CACHE_TTL:
                return dict(_oi_cache)
            if not combined and _oi_cache:
                logger.warning("OI refresh produced empty dataset; keeping previous cached OI snapshot")
                _oi_next_retry_ts = now2 + max(1.0, float(OI_FETCH_RETRY_COOLDOWN_SEC))
                return dict(_oi_cache)
            if not combined and not _oi_cache:
                logger.warning("OI refresh produced empty dataset and no cache is available")
                _oi_next_retry_ts = now2 + max(1.0, float(OI_FETCH_RETRY_COOLDOWN_SEC))
                return {}
            _oi_cache = combined
            _oi_cache_time = now2
            _oi_next_retry_ts = 0.0
            logger.info(
                f"Refreshed OI cache: {len(combined)} symbols (Binance: {len(binance_oi)}, HL: {len(hl_oi)})"
            )
            return dict(_oi_cache)

    def _get_category(self, symbol: str) -> str:
        """Get category for symbol. BTC/ETH hardcoded as MAJOR, rest by OI."""
        oi_data = self._fetch_oi_data()
        oi_usd = oi_data.get(symbol, 0)
        return classify_symbol_category(symbol, float(oi_usd or 0.0))

    def load_trades(self, symbol: Optional[str] = None,
                    lookback_days: int = LOOKBACK_DAYS) -> List[TradeRecord]:
        """Load closed trades from database."""
        conn = self._get_connection()
        cutoff = datetime.now(timezone.utc) - timedelta(days=lookback_days)
        cutoff_ts = cutoff.timestamp()

        if symbol:
            query = """
                SELECT * FROM trades
                WHERE exit_time IS NOT NULL
                AND symbol = ?
                AND exit_time >= ?
                ORDER BY exit_time DESC
            """
            rows = conn.execute(query, (symbol, cutoff_ts)).fetchall()
        else:
            query = """
                SELECT * FROM trades
                WHERE exit_time IS NOT NULL
                AND exit_time >= ?
                ORDER BY exit_time DESC
            """
            rows = conn.execute(query, (cutoff_ts,)).fetchall()

        trades = []
        for row in rows:
            trades.append(TradeRecord(
                id=row['id'],
                symbol=row['symbol'],
                direction=row['direction'],
                entry_price=row['entry_price'],
                exit_price=row['exit_price'],
                sl_price=row['sl_price'],
                tp_price=row['tp_price'],
                realized_pnl=row['realized_pnl'],
                realized_pnl_pct=row['realized_pnl_pct'],
                exit_reason=row['exit_reason'],
                mae_pct=row['mae_pct'],
                mfe_pct=row['mfe_pct'],
                entry_time=row['entry_time'],
                exit_time=row['exit_time'],
            ))
        return trades

    def compute_symbol_stats(self, symbol: str, trades: List[TradeRecord]) -> SymbolStats:
        """Compute aggregated statistics for a symbol."""
        stats = SymbolStats(symbol=symbol)

        if not trades:
            return stats

        stats.trades = len(trades)
        stats.wins = sum(1 for t in trades if t.is_winner)
        stats.losses = stats.trades - stats.wins
        stats.sl_hits = sum(1 for t in trades if t.hit_sl)
        stats.tp_hits = sum(1 for t in trades if t.hit_tp)
        stats.other_exits = stats.trades - stats.sl_hits - stats.tp_hits

        pnls = [t.realized_pnl for t in trades if t.realized_pnl is not None]
        pnl_pcts = [t.realized_pnl_pct for t in trades if t.realized_pnl_pct is not None]

        if pnls:
            stats.total_pnl = sum(pnls)
        if pnl_pcts:
            stats.avg_pnl_pct = mean(pnl_pcts)

        stats.win_rate = stats.wins / stats.trades if stats.trades > 0 else 0.0

        # MAE/MFE stats (use 90th percentile for MAE to capture tail risk)
        maes = [t.mae_pct for t in trades if t.mae_pct is not None]
        mfes = [t.mfe_pct for t in trades if t.mfe_pct is not None]

        if maes:
            stats.avg_mae_pct = mean(maes)
            stats.max_mae_pct = max(maes)
            # Add p90 for tail risk (use quantiles if enough data)
            if len(maes) >= 4:
                stats.p90_mae_pct = quantiles(maes, n=10)[-1]  # 90th percentile
            else:
                stats.p90_mae_pct = stats.max_mae_pct  # Fall back to max
        if mfes:
            stats.avg_mfe_pct = mean(mfes)
            stats.max_mfe_pct = max(mfes)

        # SL/TP distances
        sl_dists = [t.sl_distance_pct for t in trades if t.sl_distance_pct is not None]
        tp_dists = [t.tp_distance_pct for t in trades if t.tp_distance_pct is not None]

        if sl_dists:
            stats.avg_sl_distance_pct = mean(sl_dists)
        if tp_dists:
            stats.avg_tp_distance_pct = mean(tp_dists)

        # Expectancy = (win_rate * avg_win) - (loss_rate * avg_loss)
        winning_pnls = [t.realized_pnl_pct for t in trades if t.is_winner and t.realized_pnl_pct is not None]
        losing_pnls = [abs(t.realized_pnl_pct) for t in trades if (not t.is_winner) and t.realized_pnl_pct is not None]

        avg_win = mean(winning_pnls) if winning_pnls else 0
        avg_loss = mean(losing_pnls) if losing_pnls else 0

        stats.expectancy = (stats.win_rate * avg_win) - ((1 - stats.win_rate) * avg_loss)

        # Confidence level
        if stats.trades >= MIN_SAMPLES_FOR_CONFIDENCE:
            stats.confidence = 'high'
        elif stats.trades >= MIN_SAMPLES_FOR_SYMBOL_POLICY:
            stats.confidence = 'medium'
        else:
            stats.confidence = 'low'

        return stats

    def optimize_sl_tp(self, stats: SymbolStats) -> Tuple[float, float]:
        """
        Compute optimal SL/TP multipliers based on MAE/MFE and trade outcomes.

        Uses Bayesian shrinkage toward category priors based on sample size.
        Uses 90th percentile MAE (not mean) to capture tail risk.

        Logic:
        - If p90 MAE < current SL distance: can tighten SL (less risk)
        - If avg MFE > current TP distance: can widen TP (more reward)
        - If SL hit rate high but trades would have worked: widen SL
        - If TP rarely hit but MFE shows we get there: keep or tighten TP
        """
        category = self._get_category(stats.symbol)
        defaults = defaults_for_category(category, CATEGORY_DEFAULTS)

        prior_sl = defaults['sl']
        prior_tp = defaults['tp']

        # Need minimum data for optimization
        if stats.trades < MIN_SAMPLES_FOR_SYMBOL_POLICY:
            return prior_sl, prior_tp

        # Start with priors
        sl_mult = prior_sl
        tp_mult = prior_tp

        # ─────────────────────────────────────────────────────────────────────
        # SL Optimization based on MAE (use p90 for tail risk)
        # ─────────────────────────────────────────────────────────────────────
        mae_for_comparison = stats.p90_mae_pct or stats.avg_mae_pct  # Prefer p90

        if mae_for_comparison is not None and stats.avg_sl_distance_pct is not None:
            sl_hit_rate = stats.sl_hits / stats.trades if stats.trades > 0 else 0

            if sl_hit_rate > 0.35:
                # High stop rate - consider widening SL
                sl_mult = min(sl_mult * 1.15, SL_MAX)
            elif sl_hit_rate < 0.15 and mae_for_comparison < stats.avg_sl_distance_pct * 0.8:
                # Low stop rate and p90 MAE well within SL - can tighten (use 0.8 threshold, more conservative)
                sl_mult = max(sl_mult * 0.9, SL_MIN)

        # ─────────────────────────────────────────────────────────────────────
        # TP Optimization based on MFE
        # ─────────────────────────────────────────────────────────────────────
        if stats.avg_mfe_pct is not None and stats.avg_tp_distance_pct is not None:
            tp_hit_rate = stats.tp_hits / stats.trades if stats.trades > 0 else 0

            if tp_hit_rate > 0.5:
                # Hitting TP often - maybe we can aim higher
                if stats.max_mfe_pct and stats.max_mfe_pct > stats.avg_tp_distance_pct * 1.3:
                    tp_mult = min(tp_mult * 1.1, TP_MAX)
            elif tp_hit_rate < 0.2:
                # Rarely hitting TP
                if stats.avg_mfe_pct < stats.avg_tp_distance_pct * 0.7:
                    # MFE not even close to TP - tighten
                    tp_mult = max(tp_mult * 0.85, TP_MIN)

        # ─────────────────────────────────────────────────────────────────────
        # Win rate adjustments (diagnose WHY win rate is low)
        # ─────────────────────────────────────────────────────────────────────
        if stats.win_rate < 0.3:
            sl_hit_rate = stats.sl_hits / stats.trades if stats.trades > 0 else 0
            if sl_hit_rate > 0.4:
                # Stops ARE the problem - widen SL
                sl_mult = min(sl_mult * 1.1, SL_MAX)
            # Don't tighten TP on low win rate - that reduces upside on the few winners
        elif stats.win_rate > 0.6:
            # High win rate - current params working, minor tweaks only
            pass

        # ─────────────────────────────────────────────────────────────────────
        # Apply Bayesian shrinkage toward category priors
        # ─────────────────────────────────────────────────────────────────────
        sl_mult = shrink_toward_prior(sl_mult, prior_sl, stats.trades)
        tp_mult = shrink_toward_prior(tp_mult, prior_tp, stats.trades)

        # ─────────────────────────────────────────────────────────────────────
        # R:R ratio enforcement (prevent TP < SL scenarios)
        # ─────────────────────────────────────────────────────────────────────
        MIN_RR_RATIO = 1.0
        MAX_RR_RATIO = 2.5

        rr_ratio = tp_mult / sl_mult if sl_mult > 0 else 1.5
        if rr_ratio < MIN_RR_RATIO:
            tp_mult = sl_mult * MIN_RR_RATIO
        elif rr_ratio > MAX_RR_RATIO:
            tp_mult = sl_mult * MAX_RR_RATIO

        # Round to 2 decimals
        return round(sl_mult, 2), round(tp_mult, 2)

    def update_symbol_policy(self, symbol: str, stats: SymbolStats,
                             sl_mult: float, tp_mult: float) -> SymbolPolicy:
        """Update or insert symbol policy in database."""
        conn = self._get_connection()
        now = datetime.now(timezone.utc)

        # Build notes
        notes_parts = []
        if stats.avg_mae_pct:
            notes_parts.append(f"avgMAE={stats.avg_mae_pct:.1f}%")
        if stats.avg_mfe_pct:
            notes_parts.append(f"avgMFE={stats.avg_mfe_pct:.1f}%")
        notes_parts.append(f"SL_hit={stats.sl_hits}/{stats.trades}")
        notes_parts.append(f"TP_hit={stats.tp_hits}/{stats.trades}")
        notes = " | ".join(notes_parts)

        conn.execute("""
            INSERT INTO symbol_policy (symbol, sl_mult_adjustment, tp_mult_adjustment,
                                      size_adjustment, win_rate, samples, stop_out_rate,
                                      last_updated, notes)
            VALUES (?, ?, ?, 1.0, ?, ?, ?, ?, ?)
            ON CONFLICT(symbol) DO UPDATE SET
                sl_mult_adjustment = excluded.sl_mult_adjustment,
                tp_mult_adjustment = excluded.tp_mult_adjustment,
                win_rate = excluded.win_rate,
                samples = excluded.samples,
                stop_out_rate = excluded.stop_out_rate,
                last_updated = excluded.last_updated,
                notes = excluded.notes
        """, (
            symbol, sl_mult, tp_mult, stats.win_rate, stats.trades,
            stats.sl_hits / stats.trades if stats.trades > 0 else 0,
            now.timestamp(), notes
        ))
        conn.commit()

        policy = SymbolPolicy(
            symbol=symbol,
            sl_mult=sl_mult,
            tp_mult=tp_mult,
            samples=stats.trades,
            win_rate=stats.win_rate,
            expectancy=stats.expectancy,
            avg_mae_pct=stats.avg_mae_pct,
            avg_mfe_pct=stats.avg_mfe_pct,
            confidence=stats.confidence,
            last_updated=now,
            notes=notes
        )

        logger.info(f"Updated policy for {symbol}: SL={sl_mult}x TP={tp_mult}x "
                   f"(n={stats.trades}, WR={stats.win_rate:.1%}, exp={stats.expectancy:.2f})")

        return policy

    def get_policy_for_symbol(self, symbol: str) -> Tuple[float, float, str]:
        """
        Get SL/TP multipliers for a symbol.

        Returns: (sl_mult, tp_mult, source) where source is 'symbol', 'category', or 'default'
        """
        conn = self._get_connection()
        row = conn.execute("""
            SELECT sl_mult_adjustment, tp_mult_adjustment, samples
            FROM symbol_policy
            WHERE symbol = ? AND samples >= ?
        """, (symbol, MIN_SAMPLES_FOR_SYMBOL_POLICY)).fetchone()

        if row:
            return row['sl_mult_adjustment'], row['tp_mult_adjustment'], 'symbol'

        # Fall back to category
        category = self._get_category(symbol)
        defaults = defaults_for_category(category, CATEGORY_DEFAULTS)
        return defaults['sl'], defaults['tp'], f'category:{category}'

    async def backfill_mae_mfe(self, limit: int = 100):
        """Backfill MAE/MFE for trades that don't have it."""
        trades = self.load_trades()
        missing = [t for t in trades if t.mae_pct is None and t.entry_time and t.exit_time]
        db = AITraderDB(self.db_path)

        logger.info(f"Backfilling MAE/MFE for {min(limit, len(missing))} trades...")

        for trade in missing[:limit]:
            mae, mfe = await _compute_trade_mae_mfe(trade)
            if mae is not None and mfe is not None:
                ok = db.update_trade_mae_mfe(
                    trade_id=int(trade.id),
                    mae_pct=float(mae),
                    mfe_pct=float(mfe),
                )
                if not ok:
                    logger.error(
                        f"Failed to persist MAE/MFE for trade #{trade.id}: "
                        f"MAE={mae:.2f}% MFE={mfe:.2f}%"
                    )
                else:
                    logger.debug(f"  {trade.symbol} #{trade.id}: MAE={mae:.2f}% MFE={mfe:.2f}%")
                await asyncio.sleep(0.1)  # Rate limit

    def learn_all_symbols(self, min_trades: int = MIN_SAMPLES_FOR_SYMBOL_POLICY) -> Dict[str, SymbolPolicy]:
        """Learn and update policies for all symbols with enough data."""
        conn = self._get_connection()
        # Get symbols with enough trades
        rows = conn.execute("""
            SELECT symbol, COUNT(*) as cnt
            FROM trades
            WHERE exit_time IS NOT NULL
            GROUP BY symbol
            HAVING cnt >= ?
            ORDER BY cnt DESC
        """, (min_trades,)).fetchall()

        policies = {}
        for row in rows:
            symbol = row['symbol']
            trades = self.load_trades(symbol)
            stats = self.compute_symbol_stats(symbol, trades)
            sl_mult, tp_mult = self.optimize_sl_tp(stats)
            policy = self.update_symbol_policy(symbol, stats, sl_mult, tp_mult)
            policies[symbol] = policy

        logger.info(f"Updated policies for {len(policies)} symbols")
        return policies

    def get_all_policies(self) -> List[Dict]:
        """Get all symbol policies for display."""
        conn = self._get_connection()
        rows = conn.execute("""
            SELECT symbol, sl_mult_adjustment as sl, tp_mult_adjustment as tp,
                   samples, win_rate, stop_out_rate, notes,
                   datetime(last_updated, 'unixepoch') as updated
            FROM symbol_policy
            WHERE samples > 0
            ORDER BY samples DESC
        """).fetchall()
        return [dict(row) for row in rows]


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────

# ─────────────────────────────────────────────────────────────────────────────
# Hybrid Adapter (wraps both SymbolRRLearner and AdaptiveSLTPManager)
# ─────────────────────────────────────────────────────────────────────────────

class HybridSLTPAdapter:
    """
    Hybrid adapter that combines SymbolRRLearner with existing AdaptiveSLTPManager.

    Fallback chain:
    1. SymbolRRLearner (if symbol has ≥15 trades with MAE/MFE data)
    2. AdaptiveSLTPManager (regime-aware, if available)
    3. Category defaults

    Usage:
        hybrid = HybridSLTPAdapter(db_path='ai_trader.db', fallback_manager=adaptive_sltp)
        executor = Executor(..., adaptive_sltp=hybrid)
    """

    def __init__(self, db_path: str = "ai_trader.db", fallback_manager=None):
        self.rr_learner = SymbolRRLearner(db_path)
        self.fallback = fallback_manager

    def get_multipliers(self, symbol: str, regime: str = 'NORMAL') -> Tuple[float, float]:
        """
        Get SL/TP multipliers with fallback chain.

        Returns: (sl_mult, tp_mult)
        """
        # Try symbol-specific RR learning first
        sl, tp, source = self.rr_learner.get_policy_for_symbol(symbol)

        if source == 'symbol':
            # Have enough data for this symbol
            logger.debug(f"Using RR learner for {symbol}: SL={sl}, TP={tp}")
            return sl, tp

        # Fall back to regime-aware adaptive_sltp if available
        if self.fallback is not None:
            try:
                sl, tp = self.fallback.get_multipliers(symbol, regime)
                logger.debug(f"Using adaptive_sltp fallback for {symbol}/{regime}: SL={sl}, TP={tp}")
                return sl, tp
            except Exception as e:
                logger.warning(f"Fallback get_multipliers failed: {e}")

        # Final fallback: category defaults from RR learner
        logger.debug(f"Using category defaults for {symbol}: SL={sl}, TP={tp}")
        return sl, tp


def update_symbol_policy_for_symbol(
    db_path: str,
    symbol: str,
    min_trades: int = MIN_SAMPLES_FOR_SYMBOL_POLICY,
) -> bool:
    """Refresh symbol RR policy from current closed-trade history.

    Returns True when a policy row was updated, False when there is still
    insufficient symbol history.
    """
    sym = str(symbol or "").strip().upper()
    if not sym:
        return False

    learner = SymbolRRLearner(db_path)
    try:
        trades = learner.load_trades(sym)
        if len(trades) < int(min_trades):
            return False
        stats = learner.compute_symbol_stats(sym, trades)
        sl_mult, tp_mult = learner.optimize_sl_tp(stats)
        learner.update_symbol_policy(sym, stats, sl_mult, tp_mult)
        return True
    finally:
        try:
            learner.close()
        except Exception:
            pass



def main():
    parser = argparse.ArgumentParser(description='Per-Symbol R/R Learning')
    parser.add_argument('action', choices=['learn', 'show', 'backfill', 'symbol'],
                        help='Action to perform')
    parser.add_argument('--symbol', type=str, help='Symbol for single-symbol actions')
    parser.add_argument('--limit', type=int, default=100, help='Limit for backfill')
    parser.add_argument('--db-path', type=str, default='ai_trader.db')

    args = parser.parse_args()

    learner = SymbolRRLearner(args.db_path)

    if args.action == 'learn':
        policies = learner.learn_all_symbols()
        print(f"\nUpdated {len(policies)} symbol policies")

    elif args.action == 'show':
        policies = learner.get_all_policies()
        if not policies:
            print("No symbol policies yet (need 5+ trades per symbol)")
            return

        print("\n" + "=" * 80)
        print("SYMBOL POLICIES")
        print("=" * 80)
        print(f"{'Symbol':<10} {'SL':<6} {'TP':<6} {'Samples':<8} {'WinRate':<8} {'StopRate':<10} Notes")
        print("-" * 80)
        for p in policies:
            print(f"{p['symbol']:<10} {p['sl']:<6.2f} {p['tp']:<6.2f} {p['samples']:<8} "
                  f"{p['win_rate']*100:<7.1f}% {p['stop_out_rate']*100:<9.1f}% {p['notes'][:30]}")
        print("=" * 80)

    elif args.action == 'backfill':
        asyncio.run(learner.backfill_mae_mfe(args.limit))
        print(f"Backfill complete")

    elif args.action == 'symbol':
        if not args.symbol:
            print("--symbol required for 'symbol' action")
            return

        sl, tp, source = learner.get_policy_for_symbol(args.symbol)
        trades = learner.load_trades(args.symbol)
        stats = learner.compute_symbol_stats(args.symbol, trades)

        print(f"\n{args.symbol} Policy:")
        print(f"  SL mult: {sl}x (source: {source})")
        print(f"  TP mult: {tp}x")
        print(f"  Trades: {stats.trades}")
        print(f"  Win rate: {stats.win_rate:.1%}")
        print(f"  SL hits: {stats.sl_hits}, TP hits: {stats.tp_hits}")
        if stats.avg_mae_pct:
            print(f"  Avg MAE: {stats.avg_mae_pct:.2f}%")
        if stats.avg_mfe_pct:
            print(f"  Avg MFE: {stats.avg_mfe_pct:.2f}%")


if __name__ == '__main__':
    main()
