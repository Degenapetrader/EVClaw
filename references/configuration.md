# Configuration Reference

Configuration is loaded from `skill.yaml` in the EVClaw root directory.

## Key Configuration Sections

### Mode Controller

Primary trading profile switch:

```yaml
config:
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
config:
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
config:
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
config:
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
config:
  hip3:
    main_flow_z_threshold: 2.0
    main_ofm_conf_threshold: 0.65
    interrupts_enabled: true
    target_net_pct: 20
    soft_band_pct: 15
    hard_band_pct: 30
```

### Global Pause

Use this as a true kill switch for new entries.

```yaml
config:
  global_pause:
    enabled: false
    reason: manual_pause
```

When `enabled: true`, EVClaw blocks all new candidates regardless of direction, trend classification, or signal type.

### Countertrend Guard Notes

- `countertrend_guard.manual_block` is no longer rewritten from `skill.yaml` every cycle.
- Runtime DB state is treated as source of truth after initialization.

### DEAD_CAPITAL Reversal Guard

Protects against SHORTing broad bullish continuation from DEAD_CAPITAL-only pressure.

```yaml
config:
  dead_capital_reversal_guard:
    enabled: true
    short_block_trend_min: 20
    short_block_pct24h_min: 5.0
    short_block_pct24h_z_min: 1.5
    require_dead_capital_only: true
    max_non_dead_confirms: 0
```

### Entry Gate Bypass Guard

Controls risk when LLM gate output is bypassed and deterministic fallback is used.

```yaml
config:
  entry_gate_bypass_guard:
    enabled: true
    size_mult_cap: 0.5
    window_minutes: 60
    max_entries_per_window: 3
    hard_block_unreachable_after_minutes: 30
    count_disabled_as_bypass: false
```

Behavior:
- Caps bypass sizing multiplier.
- Limits number of bypass entries per rolling window.
- Can hard-block new bypass entries after prolonged gate unreachability.

### Learning Settings

```yaml
config:
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

Bypass-aware learning:
- Trades tagged as `entry_gate_execution_type: bypass` are excluded from adaptive signal/symbol updates and pattern updates.

### Exit Decider Settings

```yaml
config:
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

## External Endpoint Requirements (HIP3)

- Primary HIP3 stream: `https://tracker.evplus.ai:8443/sse/tracker?key=<wallet>`
- EVClaw default stream profile: `https://tracker.evplus.ai:8443/sse/tracker?key=<wallet>&profile=evclaw-lite`
- Fallback profile: set `EVCLAW_SSE_PROFILE=full`
- HIP3 REST state: `https://tracker.evplus.ai/api/hip3/predator-state?key=<wallet>`
- HIP3 symbols REST: `https://tracker.evplus.ai/api/hip3-symbols?key=<wallet>`
- `?key=<wallet>` is required for REST calls; missing key can cause HIP3 context gaps.

## Symbol Routing Overrides

Force specific symbols to route to specific exchanges:

```yaml
exchanges:
  router:
    overrides:
      BTC: hyperliquid  # Force BTC to HL instead of Lighter
      ETH: hyperliquid
```
