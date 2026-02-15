# Symbol R/R Learning System - Comprehensive Research Report

**Date:** 2026-01-30  
**Analyst:** Claude (Subagent)  
**Files Reviewed:**
- `${EVCLAW_ROOT}/symbol_rr_learning.py` (new per-symbol learner)
- `${EVCLAW_ROOT}/sltp_learning.py` (existing bucket learner)
- `${EVCLAW_ROOT}/adaptive_sltp.py` (existing per-symbol+regime learner)
- `${EVCLAW_ROOT}/executor.py` (integration point)

---

## Executive Summary

The new `symbol_rr_learning.py` introduces per-symbol SL/TP optimization using MAE/MFE analysis. The approach is **conceptually sound** but has **critical data quality bugs** that must be fixed before production use. The system is well-architected and provides a `get_multipliers(symbol, regime)` interface compatible with the existing `adaptive_sltp` integration in `executor.py`.

### Key Findings

| Area | Rating | Summary |
|------|--------|---------|
| MAE/MFE Computation | 🔴 CRITICAL BUG | Negative values in DB indicate computation/storage errors |
| Optimization Logic | 🟡 NEEDS WORK | Thresholds are reasonable but logic has gaps |
| Integration | 🟢 GOOD | Clean interface, drop-in compatible |
| Architecture | 🟢 GOOD | Well-structured, Bayesian-shrinkage approach |

---

## 1. MAE/MFE Computation Analysis

### 1.1 Current Approach

The `MAEMFECalculator` fetches 1-minute candles from Hyperliquid for the trade duration:

```python
async def compute_for_trade(self, trade: TradeRecord) -> Tuple[Optional[float], Optional[float]]:
    candles = await self._fetch_candles(trade.symbol, entry_ms, exit_ms)
    max_high = max([c['h'] for c in candles])
    min_low = min([c['l'] for c in candles])
    
    if trade.direction == 'LONG':
        mae_pct = (entry - min_low) / entry * 100   # Drawdown
        mfe_pct = (max_high - entry) / entry * 100  # Profit potential
    else:  # SHORT
        mae_pct = (max_high - entry) / entry * 100  # Drawdown
        mfe_pct = (entry - min_low) / entry * 100   # Profit potential
```

**The logic is mathematically correct.** However...

### 1.2 🔴 CRITICAL BUG: Negative MAE/MFE Values

Database analysis reveals **29 trades with impossible negative values**:

```sql
SELECT COUNT(*) as total, 
       SUM(CASE WHEN mae_pct < 0 THEN 1 ELSE 0 END) as negative_mae,  -- 19 trades
       SUM(CASE WHEN mfe_pct < 0 THEN 1 ELSE 0 END) as negative_mfe   -- 10 trades
FROM trades WHERE exit_time IS NOT NULL AND mae_pct IS NOT NULL
```

**Examples of corrupted data:**
| Symbol | Direction | Entry Price | MAE% | MFE% | Issue |
|--------|-----------|-------------|------|------|-------|
| S | LONG | 0.058407 | +15.8% | **-15.1%** | MFE can't be negative for LONG |
| BCH | SHORT | 538.136 | **-2.9%** | +3.6% | MAE can't be negative for SHORT |
| DOT | LONG | 1.6419 | +9.3% | **-8.6%** | MFE can't be negative for LONG |

**Root Causes (ranked by likelihood):**

1. **Direction mismatch during backfill** - The `backfill_mae_mfe()` function creates a minimal `TradeRecord` without loading the actual `direction` from DB. If default/wrong direction is used, calculations invert.

2. **Empty candle data** - API returns empty array → `max([])` raises exception, but if caught silently, could produce garbage.

3. **Timestamp precision issues** - Entry/exit times as float seconds, converted to milliseconds. If precision is lost, wrong candles fetched.

4. **Hyperliquid API symbol format** - Some symbols may need transformation (e.g., "PERP" suffix).

### 1.3 Edge Cases Not Handled

```python
# Missing guards:
if not candles:
    return None, None  # ✅ Handled

# But these aren't:
if len(candles) == 1:
    # Single candle - MAE/MFE might be understated
    pass

if exit_time - entry_time < 60:
    # Sub-minute trade - might miss actual extremes
    pass

if max_high == min_low == entry_price:
    # No price movement detected (API issue?)
    pass
```

### 1.4 Recommendations for MAE/MFE

```python
# 1. Validate before storing
def validate_mae_mfe(direction: str, mae: float, mfe: float) -> bool:
    """MAE and MFE should always be non-negative."""
    if mae < 0 or mfe < 0:
        logger.error(f"Invalid MAE/MFE: direction={direction}, mae={mae}, mfe={mfe}")
        return False
    return True

# 2. Fix backfill to load actual direction
async def backfill_mae_mfe(self, limit: int = 100):
    trades = self.load_trades()  # This DOES load direction correctly
    # ... rest is fine

# 3. Add floor to calculations
mae_pct = max(0, (entry - min_low) / entry * 100)  # Floor at 0
mfe_pct = max(0, (max_high - entry) / entry * 100)
```

---

## 2. Optimization Logic Analysis

### 2.1 Current Thresholds

| Condition | Action | Assessment |
|-----------|--------|------------|
| SL hit rate > 35% | Widen SL by 15% | ✅ Reasonable |
| SL hit rate < 15% AND avg MAE < 60% of SL | Tighten SL by 10% | 🟡 Conservative |
| TP hit rate > 50% AND max MFE > 130% TP | Widen TP by 10% | 🟡 Needs MFE validation |
| TP hit rate < 20% AND avg MFE < 70% TP | Tighten TP by 15% | ✅ Reasonable |
| Win rate < 30% | Widen SL +10%, Tighten TP -10% | 🔴 Counterproductive |

### 2.2 Statistical Issues

**Problem 1: Win rate adjustment is backwards**

```python
if stats.win_rate < 0.3:
    # Low win rate - widen SL to give trades more room
    sl_mult = min(sl_mult * 1.1, SL_MAX)
    # And tighten TP to lock in what we can
    tp_mult = max(tp_mult * 0.9, TP_MIN)
```

**This is wrong!** If win rate is low despite MAE not triggering stops:
- Widening SL doesn't help if stops aren't the problem
- Tightening TP reduces winning potential on the few winners

**Better logic:**
```python
if stats.win_rate < 0.3:
    # First diagnose WHY win rate is low
    if stats.sl_hits / stats.trades > 0.4:
        # Stops are the problem - widen SL
        sl_mult *= 1.1
    elif stats.avg_mfe_pct and stats.avg_mfe_pct > stats.avg_tp_distance_pct:
        # We're reaching TP but closing as losers - timing issue, not SL/TP
        pass
    else:
        # Trades just aren't working - reduce size instead
        # Don't touch SL/TP
        pass
```

**Problem 2: No Bayesian shrinkage actually applied**

The docstring mentions "Bayesian shrinkage toward category priors" but the code just returns category defaults when samples < 5. True Bayesian shrinkage would be:

```python
def optimize_sl_tp(self, stats: SymbolStats) -> Tuple[float, float]:
    category = self._get_category(stats.symbol)
    prior = CATEGORY_DEFAULTS[category]
    
    # Weight toward prior for low samples (Bayesian shrinkage)
    if stats.trades < MIN_SAMPLES_FOR_CONFIDENCE:
        alpha = stats.trades / MIN_SAMPLES_FOR_CONFIDENCE  # 0 to 1
        sl_mult = (1 - alpha) * prior['sl'] + alpha * computed_sl
        tp_mult = (1 - alpha) * prior['tp'] + alpha * computed_tp
```

**Problem 3: No confidence intervals**

With only 5-15 samples, the variance in MAE/MFE is high. Current logic treats `avg_mae_pct` as ground truth. Should use confidence intervals:

```python
from statistics import stdev
import math

if maes:
    avg_mae = mean(maes)
    if len(maes) >= 3:
        std_mae = stdev(maes)
        ci_mae = 1.96 * std_mae / math.sqrt(len(maes))  # 95% CI
        # Use upper bound for conservative SL sizing
        conservative_mae = avg_mae + ci_mae
```

### 2.3 Missing Features

1. **No regime awareness** - Volatility regime (LOW/NORMAL/HIGH) is accepted in `get_multipliers()` but ignored

2. **No time-decay** - Old trades weighted equally to recent ones. Should exponentially decay:
   ```python
   age_days = (now - trade.exit_time) / 86400
   weight = math.exp(-age_days / 30)  # Half-life of 30 days
   ```

3. **No cross-symbol learning** - Correlated symbols (ETH/SOL, meme coins) should share information

4. **No max drawdown protection** - If `max_mae_pct` exceeds emergency threshold, should flag for review

---

## 3. Integration Analysis

### 3.1 Current Executor Fallback Chain

```python
def _calculate_sl_tp(...):
    # Priority order:
    # 1. AGI explicit override (highest)
    # 2. adaptive_sltp (per-symbol+regime)
    # 3. sltp_learner (per-venue+category)
    # 4. config defaults (lowest)
```

### 3.2 How `symbol_rr_learning` Fits

The new learner has a compatible interface:

```python
# symbol_rr_learning.py
def get_multipliers(self, symbol: str, regime: str = 'NORMAL') -> Tuple[float, float]:
    """AdaptiveSLTPManager-compatible interface."""
    sl_mult, tp_mult, _ = self.get_policy_for_symbol(symbol)
    return sl_mult, tp_mult
```

**Integration options:**

**Option A: Replace `adaptive_sltp`**
```python
# live_agent.py
from symbol_rr_learning import SymbolRRLearner
rr_learner = SymbolRRLearner('ai_trader.db')
executor = Executor(config=exec_config, adaptive_sltp=rr_learner, ...)
```

**Option B: Hybrid approach (recommended)**
```python
# Use symbol_rr_learning for MAE/MFE-backed decisions
# Fall back to adaptive_sltp for regime awareness
def get_multipliers(self, symbol: str, regime: str) -> Tuple[float, float]:
    # Check if we have high-confidence MAE/MFE data
    sl, tp, source = self.rr_learner.get_policy_for_symbol(symbol)
    if source == 'symbol':  # Has actual data
        return sl, tp
    
    # Fall back to regime-aware adaptive_sltp
    return self.adaptive_sltp.get_multipliers(symbol, regime)
```

### 3.3 `symbol_policy` Table Schema Analysis

Current schema:
```sql
CREATE TABLE symbol_policy (
    symbol TEXT PRIMARY KEY,
    sl_mult_adjustment REAL DEFAULT 1.0,  -- Absolute multiplier, not adjustment!
    tp_mult_adjustment REAL DEFAULT 1.0,  -- Naming is misleading
    size_adjustment REAL DEFAULT 1.0,
    stop_out_rate REAL DEFAULT 0.0,
    win_rate REAL DEFAULT 0.5,
    samples INTEGER DEFAULT 0,
    last_updated REAL,
    notes TEXT
);
```

**Issues:**
1. Column names say "adjustment" but store absolute multipliers
2. No regime column (can't store per-regime policies)
3. No confidence metric stored
4. No MAE/MFE columns for debugging

**Recommended schema update:**
```sql
ALTER TABLE symbol_policy ADD COLUMN avg_mae_pct REAL;
ALTER TABLE symbol_policy ADD COLUMN avg_mfe_pct REAL;
ALTER TABLE symbol_policy ADD COLUMN confidence TEXT DEFAULT 'low';
ALTER TABLE symbol_policy ADD COLUMN regime TEXT DEFAULT 'ALL';
-- Or create composite primary key (symbol, regime)
```

---

## 4. Comparison with Existing Systems

| Feature | `symbol_rr_learning` | `adaptive_sltp` | `sltp_learning` |
|---------|---------------------|-----------------|-----------------|
| Granularity | Per-symbol | Per-symbol+regime | Per-venue+category |
| Learning Signal | MAE/MFE + outcomes | Hit rates only | Expectancy grid search |
| Min Samples | 5 | 10 | 50 |
| Persistence | SQLite | JSON file | JSON file |
| Update Trigger | Every 5 trades | On query | Manual recompute |
| Regime Aware | ❌ (ignored) | ✅ | ❌ |
| Bayesian Prior | Mentioned, not implemented | ❌ | ✅ (baseline comparison) |

**Recommendation:** Merge strengths into unified system:
- Use MAE/MFE analysis from `symbol_rr_learning`
- Add regime awareness from `adaptive_sltp`
- Keep bucket-level fallback from `sltp_learning`

---

## 5. Bugs and Issues Summary

### 🔴 Critical (Must Fix)

1. **Negative MAE/MFE values** - Corrupted data in DB affecting 29 trades. Need to:
   - Add validation before storage
   - Clean existing bad data
   - Investigate root cause (likely direction mismatch in backfill)

2. **`on_trade_closed()` creates new event loop** - Will fail if called from async context:
   ```python
   loop = asyncio.new_event_loop()  # 💥 Fails in async
   mae, mfe = loop.run_until_complete(...)
   ```
   Fix: Accept running loop or make sync version

### 🟡 Medium (Should Fix)

3. **No R:R ratio enforcement** - Unlike `adaptive_sltp` which enforces min/max R:R, this system can produce TP < SL

4. **Win rate adjustment logic is backwards** - Widening SL on low win rate can worsen drawdown

5. **Missing Bayesian shrinkage** - Claimed in docstring but not implemented

6. **`_get_category()` is duplicated** - Same logic exists in 3 files (executor, sltp_learning, symbol_rr_learning)

### 🟢 Minor (Nice to Have)

7. **No regime support** - `regime` parameter accepted but ignored

8. **No time decay** - All trades weighted equally regardless of age

9. **Hardcoded Hyperliquid API** - Can't use for Lighter trades (needs adapter pattern)

---

## 6. Recommendations

### Immediate Actions (This Week)

1. **Fix MAE/MFE validation**
   ```python
   def update_trade_in_db(self, trade_id, mae_pct, mfe_pct, ...):
       if mae_pct < 0 or mfe_pct < 0:
           logger.error(f"Rejecting invalid MAE/MFE for #{trade_id}")
           return
       # ... proceed with update
   ```

2. **Clean corrupted data**
   ```sql
   UPDATE trades SET mae_pct = NULL, mfe_pct = NULL 
   WHERE mae_pct < 0 OR mfe_pct < 0;
   ```

3. **Re-run backfill** after fixing

### Short-term (Next 2 Weeks)

4. **Add R:R constraints** to match `adaptive_sltp`:
   ```python
   MIN_RR_RATIO = 1.0
   MAX_RR_RATIO = 2.5
   
   # After optimization
   if tp_mult / sl_mult < MIN_RR_RATIO:
       tp_mult = sl_mult * MIN_RR_RATIO
   if tp_mult / sl_mult > MAX_RR_RATIO:
       tp_mult = sl_mult * MAX_RR_RATIO
   ```

5. **Implement proper Bayesian shrinkage**

6. **Extract `_get_category()` to shared module**

### Long-term (Future)

7. **Unify learning systems** into single `SLTPLearningEngine` with:
   - Symbol-level policies (MAE/MFE-based)
   - Regime awareness
   - Bucket-level fallbacks
   - Proper Bayesian priors

8. **Add cross-symbol correlation** for related assets

9. **Implement time-decay weighting**

---

## 7. Appendix: Production Data Summary

### Current symbol_policy Snapshot (Top 10 by Samples)

| Symbol | SL Mult | TP Mult | Samples | Win Rate | Notes |
|--------|---------|---------|---------|----------|-------|
| HYPE | 2.48 | 2.68 | 17 | 11.8% | avgMAE=4.4%, avgMFE=3.5% |
| SOL | 1.75 | 2.75 | 13 | 46.2% | avgMAE=2.1%, avgMFE=3.6% |
| BCH | 2.25 | 3.50 | 12 | 41.7% | avgMAE=1.5%, avgMFE=2.8% |
| SUI | 2.25 | 3.50 | 12 | 33.3% | avgMAE=3.7%, avgMFE=2.4% |
| BTC | 1.50 | 2.50 | 11 | 63.6% | avgMAE=0.9%, avgMFE=2.9% |
| ETH | 1.35 | 2.75 | 11 | 81.8% | avgMAE=1.4%, avgMFE=5.2% |

**Notable patterns:**
- ETH has excellent metrics (low MAE, high MFE, 81.8% WR) - current tight SL (1.35) justified
- HYPE has poor WR (11.8%) with SL=2.48 - needs investigation
- DOT has 70% stop rate with WR=10% - widening SL to 2.21 hasn't helped

### Database Stats

- Total closed trades: 612
- Trades with MAE/MFE: 193 (31.5%)
- Corrupted MAE/MFE records: 29 (15% of computed!)
- Symbols with policies: 50+

---

*Report generated for hl-trader AGI trading system review.*
