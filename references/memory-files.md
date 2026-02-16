# Memory Files Reference

Persistent state is stored in `memory/` directory.

## File Overview

| File | Format | Purpose |
|------|--------|---------|
| `signal_weights.yaml` | YAML | Per-signal confidence multipliers |
| `trade_journal.yaml` | YAML | Trade history (rolling 1000) |
| `circuit_breaker.yaml` | YAML | Daily loss, loss streak state |
| `positions.yaml` | YAML | Active positions |
| `symbol_blacklist.yaml` | YAML | Blacklisted symbols |
| `context_feature_stats.json` | JSON | Win rates by context condition |
| `mistakes.json` | JSON | Classified trading mistakes |
| `patterns.json` | JSON | Signal pattern performance |
| `adjustments.json` | JSON | Signal/symbol adjustments |

## signal_weights.yaml

Per-signal weight multipliers adjusted by learning engine:

```yaml
cvd: 1.0
fade: 1.0
liq_pnl: 1.0
whale: 1.0
dead_capital: 1.0
ofm: 1.0
hip3_main: 1.0
# Symbol-specific overrides
ETH:
  cvd: 1.2
  whale: 0.8
```

## trade_journal.yaml

Rolling trade history (default: last 1000 trades):

```yaml
trades:
  - trade_id: "abc123"
    symbol: "ETH"
    direction: "LONG"
    entry_price: 3500.0
    exit_price: 3550.0
    size_usd: 500.0
    pnl_usd: 7.14
    conviction: 0.72
    signals:
      cvd: 2.5
      whale: 0.8
    entry_time: "2026-01-25T12:00:00Z"
    exit_time: "2026-01-25T14:30:00Z"
```

## circuit_breaker.yaml

Risk management state:

```yaml
daily_pnl: -150.0
daily_trades: 12
loss_streak: 2
last_reset: "2026-01-25T00:00:00Z"
triggered: false
```

## positions.yaml

Active position tracking:

```yaml
positions:
  ETH:
    symbol: "ETH"
    direction: "LONG"
    size: 0.5
    entry_price: 3500.0
    unrealized_pnl: 25.0
    opened_at: "2026-01-25T12:00:00Z"
```

## context_feature_stats.json

Win rates by context conditions for learning:

```json
{
  "trend_alignment": {
    "aligned": {"trades": 50, "wins": 30, "win_rate": 0.60},
    "counter": {"trades": 30, "wins": 12, "win_rate": 0.40},
    "neutral": {"trades": 20, "wins": 10, "win_rate": 0.50}
  },
  "vol_regime": {
    "high": {"trades": 25, "wins": 10, "win_rate": 0.40},
    "mid": {"trades": 50, "wins": 28, "win_rate": 0.56},
    "low": {"trades": 25, "wins": 14, "win_rate": 0.56}
  }
}
```

## symbol_blacklist.yaml

Symbols to avoid trading:

```yaml
blacklist:
  - "SCAM"
  - "RUGPULL"
reasons:
  SCAM: "Flagged as scam token"
  RUGPULL: "Historical rug pull"
```

## mistakes.json

Classified trading mistakes for learning:

```json
[
  {
    "trade_id": "abc123",
    "mistake_type": "counter_trend",
    "severity": "medium",
    "notes": "Entered against strong trend"
  }
]
```
