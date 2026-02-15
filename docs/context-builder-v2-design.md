# Context Builder V2 - Design Document

## Problem Statement

The current context builder only uses 6 `perp_signals` fields (~327 tokens per symbol), wasting the rich data available from the SSE tracker. Meanwhile, raw symbol data is 103k tokens per symbol - far too much for any practical context window.

**Goal:** Build a context that uses ~50% of context window (100k tokens) efficiently by:
1. Selecting top 10-20 opportunities (not all 210 symbols)
2. Including ALL relevant data for selected symbols (not just perp_signals)
3. Compressing data to maximize information per token
4. Maintaining rolling memory between 15-minute cycles

## Data Analysis

### Field Sizes (Per Symbol - BTC Example)

| Field | Tokens | Useful? | Include? |
|-------|--------|---------|----------|
| zones | 56,492 | Low (raw zone data) | ❌ Skip |
| liquidation_heatmap | 18,490 | Medium (useful signals buried) | ⚠️ Extract summary only |
| orderbook_heatmap_all | 8,277 | Low (too granular) | ❌ Skip |
| orderbook_heatmap | 5,915 | Medium (useful for spread) | ⚠️ Extract key metrics |
| by_size | 3,180 | Medium (position distribution) | ⚠️ Compact summary |
| by_label | 3,112 | Medium (trader cohorts) | ⚠️ Compact summary |
| by_cohort | 2,616 | Medium (smart/dumb money) | ⚠️ Compact summary |
| fragile_wallets | 1,578 | Medium (liquidation risk) | ⚠️ Count + notional only |
| position_alerts | 1,245 | High (actionable alerts) | ✅ Extract key alerts |
| ofm | 640 | High (order flow) | ✅ Include |
| perp_signals | 327 | High (core signals) | ✅ Include |
| cvd | 303 | High (cumulative delta) | ✅ Include |
| cohort_delta | 192 | High (smart vs dumb) | ✅ Include |
| summary | 105 | High (pre-summarized) | ✅ Include |
| flow_score | 91 | High (composite score) | ✅ Include |
| smart_dumb_cvd | 86 | High (divergence) | ✅ Include |
| evscore | 59 | High (composite sentiment) | ✅ Include |
| evflow | 53 | High (flow alignment) | ✅ Include |
| sentiment | 47 | High (QWSI/bias) | ✅ Include |
| spread | 41 | Medium (price spread) | ✅ Include |
| kalman_pairs | 33 | Medium (pair correlations) | ✅ Include |
| price_change | 28 | High (24h change) | ✅ Include |
| funding | 27 | High (funding rate) | ✅ Include |
| atr | 24 | High (volatility) | ✅ Include |
| signals | 23 | High (follow/fade) | ✅ Include |

### Token Budget

**Target:** 100k tokens (50% of 200k context)

### Actual Measurements (Real SSE Data)

| N Symbols | Chars | Tokens | Per Symbol |
|-----------|-------|--------|------------|
| 10 | 11,228 | ~2,807 | ~281 |
| 15 | 16,655 | ~4,163 | ~278 |
| 20 | 22,012 | ~5,503 | ~275 |
| 30 | 32,644 | ~8,161 | ~272 |

**Result:** ~267-281 tokens per symbol with FULL rich data (not 3k as originally estimated!)

### Revised Budget (Conservative)

| Component | Tokens | Notes |
|-----------|--------|-------|
| System prompt + instructions | ~5,000 | Fixed overhead |
| Safety state + constraints | ~100 | Current portfolio state |
| Market overview | ~100 | BTC/ETH/SOL regime |
| **Top opportunities (30 symbols)** | ~9,000 | ~300 per symbol |
| Rolling memory (10 cycles) | ~3,000 | Last 10 cycles of context |
| Trade history + stats | ~2,000 | Recent trades, win rates |
| Signal performance | ~1,000 | Which combos work |
| Active positions | ~500 | Current holdings |
| AI reflections | ~500 | Learnings |
| **Buffer** | ~78,800 | Massive headroom! |
| **TOTAL** | ~100,000 | |

**Key Finding:** With ~270 tokens/symbol, we can include **ALL 210 symbols** in ~57k tokens if needed!

Recommended: **30-50 symbols** for focused decision making while staying well under budget.

## Opportunity Scoring

### Composite Score Formula

```python
opportunity_score = (
    signal_strength * 0.30 +      # Combined z-scores
    flow_alignment * 0.20 +       # evscore + evflow alignment
    smart_money_bias * 0.20 +     # cohort_delta smart_vs_dumb
    liquidity_edge * 0.15 +       # position imbalance, fragile wallets
    volatility_opportunity * 0.15 # ATR suitable for trading
)
```

### Signal Strength Calculation

```python
signal_strength = (
    max(abs(cvd_z_smart), abs(cvd_z_dumb)) * 0.25 +
    abs(fade_z_score) * 0.20 +
    abs(liq_pnl_z_score) * 0.20 +
    whale_strength * 0.15 +
    dead_cap_strength * 0.10 +
    abs(ofm_z_score) * 0.10
)
```

### Selection Criteria

1. **Minimum threshold:** At least one z-score >= 1.5 OR evscore confidence >= 60
2. **Direction clarity:** Not all signals conflicting
3. **Liquidity:** Volume > minimum threshold
4. **Not recently traded:** Avoid symbols we just exited (unless reversal)

## Compressed Symbol Format

Instead of raw JSON, use a dense text format:

```
## ETH ($2,650 | ATR 1.8% | Vol $2.1B/24h)

**Signals:** CVD=LONG(z3.1) FADE=LONG(z2.4) LIQ=SHORT(z1.2) WHALE=LONG(0.8) OFM=LONG(z2.0)
**Consensus:** 4 LONG | 1 SHORT | Direction: STRONG_LONG

**Scores:** EVScore=72(HIGH) Flow=68(STRONG_LONG) Sentiment=+0.34
**Smart Money:** Smart +$2.3M vs Dumb -$1.1M | Divergence z=2.8

**Position Structure:**
- Longs: 12,450 ($180M) | Shorts: 8,200 ($95M) | Net: 65% LONG
- Hot zone: 8.3% positions | Danger: 2.1% | Fragile: 45 wallets ($12M)
- Avg leverage: 18.5x | Margin cushion: 4.2%

**Funding:** -0.012% (shorts pay) | Spread: 0.8bps | Direction: LONGS_PAY

**Liquidation Edge:**
- Nearest long liq: -3.2% ($8.5M) | Nearest short liq: +2.1% ($4.2M)
- Long pain: -$1.2M | Short pain: -$0.8M | Asymmetry: HUNT_SHORTS

**Recent Flow (5m):**
- Smart CVD: +$1.2M (z=2.1) | Dumb CVD: -$0.3M (z=-0.8)
- New longs: 234 ($4.5M) | Closed shorts: 156 ($2.1M)
```

**Estimated tokens per symbol: ~3,000** (vs 103k raw)

## Memory System

### Directory Structure

```
${EVCLAW_MEMORY_DIR}/
├── cycle_state.json          # Current cycle metadata
├── rolling_context.jsonl     # Last N cycles' opportunities + decisions
├── active_positions.json     # Current open positions
├── signal_performance.json   # Win rates by signal combo
├── symbol_history.json       # Per-symbol recent trades + notes
└── reflections.md            # AI's learnings in natural language
```

### cycle_state.json

```json
{
  "cycle_id": "2026-01-26T08:15:00Z",
  "previous_cycle": "2026-01-26T08:00:00Z",
  "symbols_analyzed": 210,
  "opportunities_found": 15,
  "decisions_made": 3,
  "trades_opened": 1,
  "trades_closed": 0,
  "cycle_pnl": 0.0,
  "notes": "Strong CVD divergence on ETH, entered long"
}
```

### rolling_context.jsonl

Append-only log of recent cycles (keep last 10 cycles = ~2.5 hours):

```jsonl
{"cycle":"2026-01-26T08:00:00Z","top_opps":["ETH","SOL","ARB"],"decisions":[{"symbol":"ETH","action":"ENTER_LONG","reason":"CVD+FADE confluence"}],"market_state":"trending_up"}
{"cycle":"2026-01-26T08:15:00Z","top_opps":["ETH","DOGE","PEPE"],"decisions":[{"symbol":"ETH","action":"HOLD","reason":"Position still valid"}],"market_state":"trending_up"}
```

### What to Save After Each Cycle

1. **Top opportunities** with scores (even if not traded)
2. **Decisions made** with reasoning
3. **Position updates** (entries, exits, PnL)
4. **Market regime** observations
5. **Signal patterns** noticed
6. **AI notes** (free-form observations)

### What to Load at Cycle Start

1. **Last 3 cycles** from rolling_context.jsonl (~30k tokens)
2. **Active positions** with entry context
3. **Signal performance** stats
4. **Recent symbol history** for top candidates
5. **Reflections** (last 5-10 entries)

## Implementation Plan

### Phase 1: Opportunity Scorer

```python
class OpportunityScorer:
    def score_symbol(self, symbol_data: Dict) -> float
    def rank_opportunities(self, all_data: Dict) -> List[Tuple[str, float, Dict]]
    def select_top_n(self, all_data: Dict, n: int = 15) -> List[str]
```

### Phase 2: Compressed Formatter

```python
class CompactFormatter:
    def format_symbol(self, symbol: str, data: Dict) -> str  # ~3k tokens
    def format_market_overview(self, btc: Dict, eth: Dict, sol: Dict) -> str
    def format_safety_state(self, safety: Dict) -> str
    def format_memory_context(self, memory: Dict) -> str
```

### Phase 3: Memory Manager

```python
class CycleMemory:
    def __init__(self, memory_dir: Path)
    def load_context(self) -> Dict  # Load previous cycles
    def save_cycle(self, cycle_data: Dict)  # Save after decisions
    def get_symbol_history(self, symbol: str) -> List[Dict]
    def update_reflections(self, reflection: str)
```

### Phase 4: Context Builder V2

```python
class ContextBuilderV2:
    def __init__(self, db_path: str, memory_dir: Path)
    def build_full_context(self, sse_data: Dict, safety_state: Dict) -> str
    def estimate_tokens(self) -> int
```

## Token Optimization Techniques

1. **Abbreviations:**
   - `LONG` → `L`, `SHORT` → `S`, `NEUTRAL` → `N`
   - `confidence` → `conf`, `strength` → `str`
   - Use `+`/`-` for positive/negative

2. **Number formatting:**
   - `$12,345,678` → `$12.3M`
   - `0.00012345` → `0.012%`
   - Percentages to 1 decimal

3. **Skip nulls/zeros:**
   - Don't include fields with no signal
   - Skip `NEUTRAL` signals unless relevant

4. **Structured tables:**
   - Use tables for multi-symbol comparisons
   - Dense but readable format

## Example Full Context (~100k tokens)

```markdown
# Captain EVP Trading Context
Generated: 2026-01-26 08:15:00 UTC | Cycle: #1234

## Safety Status
Tier: 1/5 (NORMAL) | Daily P&L: +$45.20 | Streak: 2W | Exposure: $1,200/$10,000

## Market Regime
BTC: $87,500 (ranging, vol=1.2%) | ETH: $2,650 (trending up, vol=1.8%) | SOL: $185 (volatile, vol=3.1%)
Funding: Neutral across majors | Fear/Greed: 62 (Greed)

## Active Positions (1)
ETH LONG: Entry $2,620 (1h ago) | Size: $500 | P&L: +$5.70 (+1.1%)
- Entry signals: CVD(z3.1) + FADE(z2.4) | Current: CVD(z2.8) + FADE(z2.1)
- TP: $2,720 (+2.7%) | SL: $2,580 (-1.5%)

## Previous Cycles (3)
[08:00] Top: ETH, SOL, ARB | Entered: ETH LONG | Market: trending
[07:45] Top: ETH, DOGE, WIF | Skipped: waiting for confirmation | Market: choppy
[07:30] Top: SOL, ETH, PEPE | Closed: SOL SHORT +$12 | Market: reversal

## Top Opportunities (15)

### #1 ETH ($2,650) — Score: 85/100 ⭐ HOLDING
[... compressed format as above ...]

### #2 SOL ($185.20) — Score: 72/100
[... compressed format ...]

### #3 ARB ($0.82) — Score: 68/100
[... compressed format ...]

[... symbols 4-15 ...]

## Signal Performance (Last 7 Days)
| Combo | Trades | Win% | Avg P&L |
|-------|--------|------|---------|
| CVD+FADE | 12 | 75% | +$8.50 |
| FADE+LIQ | 8 | 62% | +$5.20 |
| WHALE+OFM | 5 | 40% | -$2.10 |

## AI Reflections
- 2026-01-26: CVD divergence on ETH consistently profitable this week
- 2026-01-25: Avoid WHALE signals on memecoins - too noisy
- 2026-01-24: FADE works best when funding is neutral
```

## Success Metrics

1. **Context usage:** ~50% of window (100k tokens)
2. **Information density:** 15-20 symbols with full data vs 6 signals before
3. **Memory continuity:** Track patterns across 10+ cycles
4. **Decision quality:** AI has all relevant data to make informed decisions
