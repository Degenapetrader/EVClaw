# Signal System Design - Volume Profile, Support/Resistance, Trend Filter

## Scope and goals
- Build three background workers that pre-compute signals and write JSON snapshots.
- Keep outputs machine-readable for the trading brain and consistent with existing signal schema.
- In this repo, worker inputs come from tracker APIs; local tracker file fallback is disabled in OSS EVClaw.

`EVCLAW_TRACKER_BASE_URL` (`https://tracker.evplus.ai`) is the default tracker source.

## Evidence-based context (existing code)

### Local node + tracker outputs
- The tracker uses the private HL node at `https://node2.evplus/info` as its primary API and publishes per-symbol SSE snapshots at `/sse/tracker` (symbol_watcher.py:92-97).
- Market data is sourced from `metaAndAssetCtxs` and stored as funding, day volume, and prices (symbol_watcher.py:875-899).
- Per-symbol JSON includes funding, 24h volume, and ATR (atr/atr_pct) fields that can be reused by new workers (symbol_watcher.py:2751-2805).
- Existing ATR calculator already uses Binance Futures klines as primary with HL fallback (atr_calculator.py:92-108, 158-229).

### Orderbook heatmap (L4)
- The orderbook heatmap buckets orders by percent distance from mid and outputs `orderbook_heatmap` / `orderbook_heatmap_all` with bids/asks buckets (orderbook_heatmap.py:416-604).
- Heatmap payload includes per-bucket `price_bucket`, `total_value`, and `top_wallets`, and overall bid/ask depth plus imbalance (orderbook_heatmap.py:478-603).
- Heatmap updates are posted to the tracker `/orderbook/update` endpoint (orderbook_heatmap.py:635-661), then cached and broadcast via SSE (symbol_watcher.py:3393-3448).
- The tracker also reads heatmap data from symbol JSON files and merges it into SSE payloads (symbol_watcher.py:3906-3925).

### Signal schema expectations (trading brain)
- The trading brain expects `opp.signals` as a dict of `{signal_name: {direction, z_score, ...}}` (trading_brain.py:169-172).
- Conviction is computed using each signal's `direction` and `z_score` (trading_brain.py:197-245).
- The signal parser defines standard z-score thresholds (2.0 standard, 3.0 strong) and uses normalized direction values (signal_parser.py:16-35, 19-21).

## Candle data sources and formats (Binance primary, HL fallback)

### Binance klines (primary)
- Existing code uses Binance Futures klines as the primary candle source and falls back to HL if Binance fails (atr_calculator.py:92-108, 158-229).
- Binance kline format is a list of arrays: `[open_time, open, high, low, close, volume, ...]` which is parsed into high/low/close (atr_calculator.py:171-182).

### HL candleSnapshot (fallback)
#### Request shape
Existing code requests candles using:
```
{"type": "candleSnapshot", "req": {"coin": "BTC", "interval": "1h", "startTime": <ms>, "endTime": <ms>}}
```
(download_candles.py:41-48)

#### Response shape used in code
`candleSnapshot` returns a list of candle dicts with fields used as:
- `t` (timestamp ms), `o` (open), `h` (high), `l` (low), `c` (close), `v` (volume)
(download_candles.py:83-94)

### Local node support (verified)
One tracker utility explicitly states that the local node does not support `candleSnapshot` and uses the public API instead (historical_bootstrap.py:81-82).

## Design: Volume Profile Worker (`volume_profile_worker.py`)

### Inputs
- Candle data for each symbol and timeframe (session 8h, daily 24h, weekly 7d).
- Primary: Binance klines (Futures) for candles; parse `high/low/close/volume` from array format (atr_calculator.py:171-182).
- Fallback: HL `candleSnapshot` with fields `t,o,h,l,c,v` (download_candles.py:83-94).
- Optional: ATR data from tracker JSON for adaptive bin sizing (symbol_watcher.py:2796-2805).

### Core logic
1. Fetch candles for each timeframe window.
2. Build a volume histogram across price bins:
   - For each candle, distribute `v` across price bins intersecting `[l, h]`.
   - Use uniform distribution unless a more advanced volume-at-price model is added later.
3. Compute:
   - POC: price bin with maximum volume.
   - VAH/VAL: bounds containing 70% of total volume around POC.
   - HVN/LVN: local maxima/minima in smoothed volume histogram.
4. Classify profiles:
   - Developing: current in-progress session/day/week.
   - Fixed: last completed session/day/week (freeze once window ends).
5. Provide directional bias and z-score:
   - Compute `distance_to_poc` and normalize with rolling standard deviation to comply with z-score standardization (signal_parser.py:19-21).

### Proposed parameters (defaults to validate)
- Bin size: `max(tick_size, price * 0.001)` (0.10% of price) or `max(tick_size, atr_abs * 0.1)` if ATR available.
- Value area: 70% total volume.
- HVN/LVN detection: local extrema after 3-bin moving average smoothing.
- Update cadence: every 5 minutes (per task requirement).

### Output JSON schema
File: `${EVCLAW_SIGNALS_DIR}/volume_profile.json`
```
{
  "generated_at": "<iso8601>",
  "symbols": {
    "BTC": {
      "timeframes": {
        "session_8h": {
          "status": "developing",
          "start_ms": 0,
          "end_ms": 0,
          "poc": 0.0,
          "vah": 0.0,
          "val": 0.0,
          "hvn": [{"price": 0.0, "volume": 0.0}],
          "lvn": [{"price": 0.0, "volume": 0.0}],
          "total_volume": 0.0,
          "distance_to_poc": 0.0,
          "distance_to_poc_z": 0.0,
          "signal": {"direction": "NEUTRAL", "z_score": 0.0}
        },
        "daily_24h": { "...": "same fields" },
        "weekly_7d": { "...": "same fields" }
      }
    }
  }
}
```

## Design: Support/Resistance Worker (`sr_levels_worker.py`)

### Inputs
- Rolling highs/lows from candles:
  - 1h, 4h, 24h, 7d windows (from candle data).
- Primary candles from Binance klines; fallback to HL `candleSnapshot`.
- Session highs/lows (8h window).
- Round numbers (symbol-specific step, e.g., 0.5% of price).
- Volume-weighted candle extremes:
  - High/low of candles with volume > 2x rolling average volume.
- Volume profile levels (POC/VAH/VAL) from `volume_profile.json`.
- Optional: Orderbook heatmap buckets for large resting liquidity (orderbook_heatmap.py:478-603).

### Strength scoring (raw -> z-score)
Strength is a composite of:
```
raw = (touch_count * timeframe_weight)
    + (volume_weight)
    + (flip_bonus)
    - (decay_by_recency)
```
Then standardize with z-score over a rolling window per symbol (signal_parser.py:19-21).

### Flip zone detection
- Track crossings: if price breaks a level and later retests from the other side, mark `flip=True` and boost strength.
- Persist flip count and last flip timestamp to increase confidence.

### Output JSON schema
File: `${EVCLAW_SIGNALS_DIR}/sr_levels.json`
```
{
  "generated_at": "<iso8601>",
  "symbols": {
    "BTC": {
      "price": 0.0,
      "nearest": {
        "support": {"price": 0.0, "strength_z": 0.0, "sources": ["1h_high", "vp_val"]},
        "resistance": {"price": 0.0, "strength_z": 0.0, "sources": ["24h_high", "round"]}
      },
      "levels": [
        {
          "price": 0.0,
          "type": "support",
          "strength_raw": 0.0,
          "strength_z": 0.0,
          "touch_count": 0,
          "timeframe": "24h",
          "last_touch_ms": 0,
          "flip": false,
          "flip_count": 0,
          "sources": ["rolling_low", "vp_val"]
        }
      ],
      "signal": {"direction": "NEUTRAL", "z_score": 0.0}
    }
  }
}
```

Signal direction suggestion:
- LONG if price is near strong support and above POC.
- SHORT if price is near strong resistance and below POC.
- NEUTRAL otherwise.

## Design: Trend Filter Worker (`trend_filter_worker.py`)

### Inputs
- 1h and 4h candles (Binance klines primary, HL `candleSnapshot` fallback).
- Price + ATR from tracker if available (symbol_watcher.py:2796-2805).

### Components (each yields -1 to +1, then weighted)
1. EMA cross (9/21 on 1h and 4h).
2. Supertrend (ATR length 10, multiplier 3.0).
3. ADX + DI (14-period; trend if ADX >= 25).
4. Price vs VWAP (session VWAP or rolling 24h VWAP).
5. MTF alignment (1h and 4h component agreement).

### Composite score
```
score = 100 * (
  0.25 * ema_score +
  0.20 * supertrend_score +
  0.20 * adx_score +
  0.20 * vwap_score +
  0.15 * mtf_score
)
```
Clamp to [-100, 100].

### Regime rules
- >= 60: LONG only
- 20 to 59: LONG preferred
- -19 to 19: choppy / no edge
- -59 to -20: SHORT preferred
- <= -60: SHORT only

### Output JSON schema
File: `${EVCLAW_SIGNALS_DIR}/trend_state.json`
```
{
  "generated_at": "<iso8601>",
  "symbols": {
    "BTC": {
      "trend_score": 0,
      "regime": "CHOPPY",
      "direction": "NEUTRAL",
      "components": {
        "ema": {"score": 0.0, "ema_1h": [0, 0], "ema_4h": [0, 0]},
        "supertrend": {"score": 0.0, "trend": "NEUTRAL", "atr": 0.0},
        "adx": {"score": 0.0, "adx": 0.0, "di_plus": 0.0, "di_minus": 0.0},
        "vwap": {"score": 0.0, "vwap": 0.0, "price": 0.0},
        "mtf": {"score": 0.0}
      },
      "signal": {"direction": "NEUTRAL", "z_score": 0.0}
    }
  }
}
```

Update cadence: every 1 minute (per task requirement).

## Parameter defaults (proposed starting points)

| Component | Parameter | Default | Notes |
|-----------|-----------|---------|------|
| Volume Profile | Bin size | 0.10% of price or 0.1*ATR | Validate vs tick size |
| Volume Profile | Value area | 70% | Standard VAH/VAL |
| S/R | Volume extreme filter | volume > 2x avg | Use rolling avg per timeframe |
| Trend | EMA periods | 9/21 | On 1h and 4h |
| Trend | ADX period | 14 | Trend if >= 25 |
| Trend | Supertrend | ATR 10, mult 3.0 | Tune per symbol if needed |

## Trading brain integration (what is needed)

### Requirements from existing brain code
- `TradingBrain` consumes `opp.signals` with `direction` and `z_score` and applies conviction weights (trading_brain.py:169-245).
- `ContextBuilderV2` passes `signals` into `ScoredOpportunity` as `{direction, z_score, ...}` (context_builder_v2.py:146-229).

### Recommended integration (minimal risk)
1. Add a small loader module in `hl-trader` that reads the three JSON files and merges them into the per-symbol data used by `ContextBuilderV2`.
2. Inject new signal entries into `signal_scores["signals"]` for:
   - `trend_filter`
   - `volume_profile`
   - `sr_levels`
3. Add conviction weights in `TradingBrain.BrainConfig.conviction_weights` for these signals.

Rationale: This keeps tracker/SSE unchanged and aligns with the existing `signals` schema already used in scoring and conviction.

### Alternative (bigger change)
Embed these signals into tracker output (`symbol_watcher` SSE) so they behave like other `perp_signals`. This increases coupling and requires tracker changes and restarts.

Recommendation: Use the minimal-risk loader approach first because it isolates changes to `hl-trader` and avoids modifying the tracker service.

## Open questions / verification checklist

1. Confirm preferred Binance interval strategy (direct 1h/4h/1d vs aggregate from 1h for 8h/7d).
2. Confirm canonical tick size source (likely from HL `meta` endpoint) for VP bin sizing.
3. Decide whether to include orderbook heatmap buckets as additional S/R sources.
