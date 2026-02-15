#!/usr/bin/env python3
"""
AGI Context Builder - Simplified context for AGI trading decisions.

Outputs only relevant, actionable data at top-level.
Removes nested structures that waste tokens and get ignored.
"""

import json
import logging
import sqlite3
import time
from collections import defaultdict
from typing import Any, Dict, List, Optional

# Rolling cache for pct_24h z-score calculation
_pct_24h_history: Dict[str, List[float]] = defaultdict(list)
_pct_24h_last_seen: Dict[str, float] = {}
_HISTORY_WINDOW = 7 * 4  # 7 days of 6-hour snapshots (28 data points)
_HISTORY_STALE_SECONDS = 14 * 24 * 3600  # prune symbols not updated for 14d
_HISTORY_MAX_SYMBOLS = 4000
_history_update_count = 0
_LOG = logging.getLogger(__name__)

# AGI Decision Instruction
AGI_INSTRUCTION = """
You are an AGI trader. You MUST consider ALL fields below before deciding.
- Does trend_score align with the proposed direction?
- Is pct_24h_zscore showing unusual momentum or mean-reversion setup?
- Is volatility (atr_pct) suitable for this trade size?
- What is the global market stress (IV ratios, realized vs implied)?
- How does this trade affect portfolio exposure?
- What does context_adjustment say about historical performance in this context?

You are not a bot following rules. You see the full picture and make your own call.

## ORDER TYPE SELECTION
You have TWO order types available:
1. **CHASE LIMIT** (fast fill) - Use when:
   - High conviction (>0.6)
   - Trend-aligned setup
   - Time-sensitive entry (signal decay risk)
   - Urgency high

2. **LIMIT ORDER** (patient fill) - Use when:
   - Lower conviction (<0.6)
   - Counter-trend or unclear direction
   - Price at unfavorable level (wait for better entry)
   - Patience rewarded by context

Your output should specify: order_type='chase_limit' or order_type='limit'

## DECAY EXIT THRESHOLDS
Only close positions on STRONG trend flips:
- LONG positions: close when trend_score <= -40 (strong bearish reversal)
- SHORT positions: close when trend_score >= +40 (strong bullish reversal)
Do NOT close on weak flips (e.g., +12, -20). Hold through noise.
"""


def _calculate_zscore(values: List[float], current: float) -> float:
    """Calculate z-score of current value vs historical values."""
    n = len(values)
    if n < 5:
        return 0.0

    mean = sum(values) / n
    # Use sample variance (N-1) to avoid inflating z-scores for small windows.
    variance = sum((x - mean) ** 2 for x in values) / max(1, (n - 1))
    std = variance ** 0.5

    if std < 0.001:
        return 0.0

    return (current - mean) / std


def update_pct_24h_history(symbol: str, pct_24h: float) -> None:
    """Update rolling history for a symbol."""
    global _history_update_count
    sym = str(symbol or "").upper().strip()
    if not sym:
        return

    history = _pct_24h_history[sym]
    history.append(pct_24h)
    _pct_24h_last_seen[sym] = time.time()
    
    # Keep only last N values
    if len(history) > _HISTORY_WINDOW:
        _pct_24h_history[sym] = history[-_HISTORY_WINDOW:]

    _history_update_count += 1
    # Periodic pruning keeps key count bounded for long-lived processes.
    if _history_update_count % 256 == 0:
        _prune_pct_24h_history()


def get_pct_24h_zscore(symbol: str, current_pct_24h: float) -> float:
    """Get z-score of current pct_24h vs rolling history."""
    history = _pct_24h_history.get(symbol, [])
    return _calculate_zscore(history, current_pct_24h)


def _prune_pct_24h_history(now_ts: Optional[float] = None) -> None:
    """Bound rolling history map growth by age and max symbol count."""
    now = float(now_ts if now_ts is not None else time.time())
    stale_cutoff = now - float(_HISTORY_STALE_SECONDS)

    stale_symbols = [
        sym
        for sym, seen_ts in list(_pct_24h_last_seen.items())
        if float(seen_ts or 0.0) <= stale_cutoff
    ]
    for sym in stale_symbols:
        _pct_24h_last_seen.pop(sym, None)
        _pct_24h_history.pop(sym, None)

    if len(_pct_24h_history) <= _HISTORY_MAX_SYMBOLS:
        return

    # Hard cap safeguard: drop oldest symbols first.
    ordered = sorted(
        _pct_24h_last_seen.items(),
        key=lambda kv: float(kv[1] or 0.0),
    )
    overflow = len(_pct_24h_history) - _HISTORY_MAX_SYMBOLS
    for sym, _ in ordered[: max(0, overflow)]:
        _pct_24h_last_seen.pop(sym, None)
        _pct_24h_history.pop(sym, None)


def build_global_context(global_ctx: Optional[Dict]) -> Dict[str, Any]:
    """Build simplified global context."""
    if not global_ctx:
        return {}
    
    result = {}
    
    # BTC IV
    btc_iv = global_ctx.get('btc_iv') or {}
    if btc_iv:
        result['btc_iv_24h'] = {
            'value': btc_iv.get('iv_24h'),
            'meaning': 'BTC implied vol 24h. What options market expects.'
        }
        result['btc_iv_weekly'] = {
            'value': btc_iv.get('iv_weekly'),
            'meaning': 'BTC implied vol weekly.'
        }
        result['btc_iv_ratio'] = {
            'value': btc_iv.get('iv_ratio'),
            'meaning': '24h/weekly. >1=short-term fear elevated vs longer term.'
        }
        result['btc_realized_vol'] = {
            'value': btc_iv.get('realized_vol'),
            'meaning': 'BTC actual vol. Compare to IV - if IV>RV, market pricing more risk.'
        }
    
    # ETH IV
    eth_iv = global_ctx.get('eth_iv') or {}
    if eth_iv:
        result['eth_iv_24h'] = {
            'value': eth_iv.get('iv_24h'),
            'meaning': 'ETH implied vol 24h.'
        }
        result['eth_iv_weekly'] = {
            'value': eth_iv.get('iv_weekly'),
            'meaning': 'ETH implied vol weekly.'
        }
        result['eth_iv_ratio'] = {
            'value': eth_iv.get('iv_ratio'),
            'meaning': 'ETH 24h/weekly ratio.'
        }
        result['eth_realized_vol'] = {
            'value': eth_iv.get('realized_vol'),
            'meaning': 'ETH actual vol.'
        }
    
    # Exposure
    exposure = global_ctx.get('exposure') or {}
    current_exp = exposure.get('current') or {}
    if current_exp:
        result['net_exposure_usd'] = {
            'value': current_exp.get('net'),
            'meaning': 'Portfolio net exposure. Negative=short-biased, positive=long-biased.'
        }
    
    drift = exposure.get('drift')
    if drift is not None:
        result['exposure_drift'] = {
            'value': round(drift, 2),
            'meaning': 'How far current exposure drifted from recent average.'
        }
    
    return result


def build_agi_candidate(
    opp: Dict[str, Any],
    rank: int,
    signals_list: List[str],
    strong_signals: List[str],
    must_trade: bool,
    constraint_reason: Optional[str],
    conviction: float,
    global_context: Optional[Dict] = None,
    context_adjustment: Optional[float] = None,
    context_breakdown: Optional[Dict] = None,
) -> Dict[str, Any]:
    """
    Build AGI-style candidate with only relevant top-level data.
    
    No nested structures. Every field has meaning.
    """
    symbol = str(opp.get('symbol') or '').upper()
    direction = str(opp.get('direction') or '').upper()
    key_metrics = opp.get('key_metrics') or {}
    
    # Extract trend_score
    trend_state = key_metrics.get('trend_state') or {}
    trend_score = trend_state.get('trend_score', 0)
    
    # Extract volatility metrics
    pct_24h = key_metrics.get('pct_24h', 0)
    atr_pct = key_metrics.get('atr_pct', 0)
    
    # Update history and calculate z-score
    if pct_24h:
        update_pct_24h_history(symbol, pct_24h)
    pct_24h_zscore = get_pct_24h_zscore(symbol, pct_24h)
    
    # Determine suggested order type based on conviction
    if conviction >= 0.6:
        order_type_hint = 'chase_limit'
        order_type_reason = 'High conviction (>=0.6) - use chase limit for fast fill'
    else:
        order_type_hint = 'limit'
        order_type_reason = 'Lower conviction (<0.6) - use limit order, be patient'
    
    # Build reason_short from strong signals
    if strong_signals:
        reason_short = f"{'+'.join(strong_signals)} {direction}"
    else:
        reason_short = f"{direction} signal alignment"
    
    # Build candidate
    candidate = {
        # Instruction for AGI
        '_instruction': AGI_INSTRUCTION,
        
        # === SYMBOL CONTEXT ===
        'symbol': symbol,
        'direction': direction,
        'conviction': round(conviction, 4),
        'reason_short': reason_short,
        'signals': signals_list,
        'strong_signals': strong_signals,
        'must_trade': must_trade,
        'constraint_reason': constraint_reason,
        'rank': rank,
        
        # === ORDER TYPE GUIDANCE ===
        'order_type_hint': order_type_hint,
        'order_type_reason': order_type_reason,
        
        # === TREND ===
        'trend_score': {
            'value': trend_score,
            'meaning': 'Composite trend [-100,+100]. >=+40=strong bullish, <=-40=strong bearish. Values in between are weak/noise. Based on EMA/ADX/VWAP/Supertrend/MTF agreement.'
        },
        
        # === VOLATILITY ===
        'pct_24h_zscore': {
            'value': round(pct_24h_zscore, 2),
            'meaning': 'How unusual is today\'s move vs last 7 days. High=outlier, near 0=typical.'
        },
        'atr_pct': {
            'value': round(atr_pct, 2) if atr_pct else 0,
            'meaning': 'Average true range as % of price. Higher=more volatile.'
        },
        
        # === CONTEXT LEARNING ===
        'context_adjustment': {
            'value': round(context_adjustment, 3) if context_adjustment else 1.0,
            'meaning': 'Multiplier from historical context performance. >1=favorable context, <1=unfavorable. Apply to sizing.',
            'breakdown': context_breakdown or {},
        },
        
        # === GLOBAL CONTEXT ===
        'global': build_global_context(global_context),
    }
    
    return candidate




# For loading historical pct_24h from database on startup
def load_pct_24h_history_from_trades(db_path: str, days: int = 7) -> None:
    """Load recent pct_24h history from trades table."""
    cutoff = time.time() - (days * 24 * 3600)
    try:
        with sqlite3.connect(db_path) as con:
            cur = con.cursor()
            cur.execute(
                """
                SELECT symbol, context_snapshot
                FROM trades
                WHERE entry_time > ?
                AND context_snapshot IS NOT NULL
                ORDER BY entry_time ASC
                """,
                (cutoff,),
            )
            rows = cur.fetchall()
    except Exception as exc:
        _LOG.debug("Failed to load pct_24h history from %s: %s", db_path, exc)
        return

    for symbol, ctx_json in rows:
        try:
            ctx = json.loads(ctx_json)
            km = ctx.get("key_metrics", {})
            pct_24h = km.get("pct_24h")
            if pct_24h is not None:
                update_pct_24h_history(str(symbol), float(pct_24h))
        except Exception as exc:
            _LOG.debug("Skipping malformed context_snapshot for %s: %s", symbol, exc)
