# Signal Types Reference

EVClaw processes signals from the tracker and workers for trading decisions.

## Signal Types

| Signal | Description | Z-Score Field | Source |
|--------|-------------|---------------|--------|
| CVD | Cumulative Volume Delta | z_smart, z_dumb | Tracker |
| FADE | Smart money fade signal | z_score | Tracker |
| LIQ_PNL | Liquidation PnL flow | z_score | Tracker |
| WHALE | Large trader activity | strength (0-1) | Tracker |
| DEAD_CAPITAL | Trapped capital detection | strength (0-1) | Tracker |
| OFM | Order Flow Momentum | z_score | Tracker |
| HIP3_MAIN | FLOW divergence gate + OFM alignment (xyz: only) | z_score_effective | hip3_predator_worker |

## HIP3 Predator Signals

For HIP3 symbols (`xyz:`), the worker provides FLOW + OFM components combined into **HIP3_MAIN**:

- **FLOW gate**: HL vs Massive spread divergence (z_signed vs dynamic_threshold)
- **OFM confirm**: Predator OFM must align with FLOW direction

HIP3_MAIN is PRIMARY for HIP3 symbols and can trigger trades independently.

HIP3 signals stream over SSE as event `hip3-data` and are merged into context in real time.

### HIP3 Data Source

EVClaw does not run tracker-side HIP3 workers locally. EVClaw reads HIP3 data from tracker endpoints:
- `https://tracker.evplus.ai/api/hip3/predator-state`
- SSE `hip3-data` events

## Conviction Weights

Default weights (configurable in `skill.yaml` under `config.brain.conviction_weights`):

| Signal | Weight |
|--------|--------|
| dead_capital | 0.35 |
| whale | 0.26 |
| ofm | 0.22 |
| cvd | 0.10 |
| fade | 0.07 |
| hip3_main | 0.65 |

## Veto Conditions

| Condition | Effect |
|-----------|--------|
| WHALE opposite direction | VETO trade |
| CVD opposite with strong z-score | VETO trade |

## Context Features

Context features tracked for learning:

| Feature | Conditions | Meaning |
|---------|------------|---------|
| `trend_alignment` | aligned / counter / neutral | Does trend_score agree with trade direction? |
| `vol_regime` | high / mid / low | ATR-based volatility bucket |
| `funding_alignment` | aligned / counter / neutral | Does funding favor our direction? |
| `smart_money` | aligned / counter / neutral | Does smart money divergence favor us? |
| `signal_strength` | strong / moderate / weak | Max z-score bucket |

## Context Adjustment

When enough data exists (≥10 trades per condition), the learning engine provides a multiplier:
- Win rate > 45% → boost sizing (up to 1.5x)
- Win rate < 45% → reduce sizing (down to 0.5x)
