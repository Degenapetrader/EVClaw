# Configuration Reference

Configuration is loaded from `skill.yaml` in the EVClaw root directory.

## Key Configuration Sections

### Mode Controller

Primary trading profile switch:

```yaml
mode_controller:
  mode: balanced  # conservative | balanced | aggressive
  sliders:
    risk_appetite: 50      # 0-100
    trade_frequency: 50    # 0-100
    aggression: 50         # 0-100
    signal_weighting: 50   # 0-100
    learning_speed: 50     # 0-100
```

- `conservative`: fewer trades, stricter filters, smaller risk/sizing
- `balanced`: default baseline
- `aggressive`: more trades, looser filters, higher risk/sizing

### Brain Settings

```yaml
brain:
  base_confidence_threshold: 0.55
  high_conviction_threshold: 0.75
  conviction_chase_threshold: 0.70
  degen_mode: true
  degen_multiplier: 1.5
  max_candidates: 6
  candidate_min_conviction: 0.1
```

### Risk Settings

```yaml
risk:
  starting_equity: 10000.0
  min_risk_pct: 0.5
  max_risk_pct: 2.5
  base_risk_pct: 1.0
  equity_floor_pct: 80.0
  daily_drawdown_limit_pct: 10.0
  max_concurrent_positions: 100
  no_hard_stops: false
  min_hold_hours: 2.0
```

### Executor Settings

```yaml
executor:
  base_position_size_usd: 500.0
  min_position_notional_usd: 20.0
  max_position_per_symbol_usd: 0.0
  enable_sltp_backstop: true
  sl_atr_multiplier: 2.0
  tp_atr_multiplier: 3.0
  chase_timeout: 300
  sr_limit_enabled: true
  sr_limit_max_notional_pct: 500.0
```

### HIP3 Settings

```yaml
hip3:
  main_flow_z_threshold: 2.0
  main_ofm_conf_threshold: 0.65
  interrupts_enabled: true
  target_net_pct: 20
  soft_band_pct: 15
  hard_band_pct: 30
```

### Learning Settings

```yaml
learning:
  enabled: true
  min_trades_for_adjustment: 30
  min_symbol_trades: 10
  weight_bounds: [0.5, 2.0]
  significance_threshold: 0.1
  learning_rate: 0.02
  auto_update_interval_trades: 50
  symbol_specific_learning: true
  decay_halflife_days: 30
```

### Exit Decider Settings

```yaml
exit_decider:
  sl_reentry_cooldown_minutes: 60
  reentry_cooldown_minutes: 60
  tp_reentry_cooldown_minutes: 15
  backoff_after_hold_sec: 3600
  backoff_after_close_failed_sec: 1800
```

## Exchange Configuration

```yaml
exchanges:
  router:
    overrides: {}  # Force specific symbols to specific exchanges
  lighter:
    type: crypto_perps
    enabled: true
  hyperliquid:
    type: hip3_stocks
    enabled: true
```

## Symbol Routing Overrides

Force specific symbols to route to specific exchanges:

```yaml
exchanges:
  router:
    overrides:
      BTC: hyperliquid  # Force BTC to HL instead of Lighter
      ETH: hyperliquid
```
