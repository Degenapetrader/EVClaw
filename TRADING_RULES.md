# TRADING_RULES.md — EVClaw (Current Operating Rules)

This document is the **current** trading policy for EVClaw.
It is meant to match the code + runtime behavior in the current repo.

If this doc and the code disagree, **the code wins** — then update this doc.

---

## 0) Operating Mode (Reality)

**Architecture:**
Signals → ContextBuilderV2 → TradingBrain (me) → DynamicRiskManager → Executor → Exchange

**State source of truth:**
- **SQLite (`ai_trader.db`) is canonical** for trades/positions/fills.
- `memory/positions.yaml` is snapshot-only (currently disabled by config).

**Venues:**
- Designed for dual-venue (Lighter + Hyperliquid), but current config is:
  - `lighter_enabled: false`
  - `hl_enabled: true`

**Hyperliquid account (single unified wallet):**
- **Wallet account:** uses `HYPERLIQUID_ADDRESS` / `HYPERLIQUID_API`
  - **HIP3 (`xyz:*`) trades:** routed with dex context and a unified signer
  - **Perps and HIP3:** both use the same configured wallet address
  - If `HYPERLIQUID_API` is missing, execution is blocked (safety)

---

## 1) Non‑Negotiable Invariants (Hard Rules)

1. **Max new positions per cycle:** bounded by candidate count + risk gates for the cycle.
2. **No pyramiding / averaging:** if a position already exists in the same direction, **skip**.
3. **Signal flip entry:** if opposite direction entry is attempted:
   - close existing position first, then enter new one.
4. **Protection backstop:** if `enable_sltp_backstop=true`, place **exchange-level SL + TP** after fills.
5. **Fill reconciler must run** when `use_fill_reconciler=true` (it is true in current config).
6. **No phantom journaling:** verify symbol/position exists on venue before journaling (executor preflight).

---

## 2) Signals: Hierarchy + Interpretation

### Active signals (from tracker SSE)
- `dead_capital` (primary)
- `whale` (primary)
- `cvd` (secondary)
- `ofm` (secondary)
- `fade` (secondary)

**Deprecated / excluded from scoring:** `liq_pnl` (kept for monitoring only).

### HIP3 signals (xyz:* only)
- `hip3_main` (primary for HIP3): FLOW divergence gate (HL vs Massive) + OFM alignment confirmation

**Tracking + learning (important):**
- These are stored into `ai_trader.db` under `trades.signals_snapshot` / `trades.context_snapshot`
- Learning engine updates pattern/signal adjustments for `hip3_main` just like other signals
- HIP3 Predator signals stream over SSE as `hip3-data` and are merged into context in real time

### Current weights (used by OpportunityScorer)
- Primary:
  - `DEAD_CAPITAL` = **0.32**
  - `WHALE` = **0.28**
- Secondary:
  - `CVD` = **0.20** (reduced in scorer implementation)
  - `OFM` = **0.12** (reduced in scorer implementation)
  - `FADE` = **0.08**

### DEAD_CAPITAL gating (important)
If `dead_capital` includes `banner_trigger` and/or `override_trigger` and **both are false**, we treat it as **neutral** for direction consensus.

### Direction labels
Context direction is one of:
- `LONG`
- `SHORT`
- `MIXED` (signals conflict / not decisive)

`MIXED` is **not** a flip signal and should not trigger forced exits by itself.

---

## 3) Entry Rules (What can open a trade)

### Minimum requirements
1. **At least one primary signal** must be active (DEAD_CAPITAL or WHALE).
2. Secondary signals **cannot** trigger trades alone; they can only raise conviction.

### Veto logic (risk reduction / block)
- If strong opposing evidence exists (e.g. CVD is violently opposite), either:
  - block the trade, or
  - reduce conviction / size.

(Exact veto thresholds are enforced in code paths; this section is policy intent.)

### Practical note: min notional gate
Even if a trade is selected, executor will **skip** entry if computed size is below:
- `executor.min_position_notional_usd` (currently **$500**)

---

## 4) Risk + Position Sizing (How big)

### Risk % bounds (hard)
- `min_risk_pct = 0.5%`
- `max_risk_pct = 2.5%`

Conviction scales the risk budget within those bounds (DynamicRiskManager).

### Context Learning Adjustment (v0.6.0)
When context_snapshot and learning_engine are provided, risk is further adjusted by:
- `context_adjustment` multiplier (0.5x to 1.5x) based on historical win rates by context condition.

Context features tracked:
- `trend_alignment` (aligned / counter / neutral)
- `vol_regime` (high / mid / low)
- `funding_alignment` (aligned / counter / neutral)
- `smart_money` (aligned / counter / neutral)
- `signal_strength` (strong / moderate / weak)

Stats are persisted to `memory/context_feature_stats.json`.

### ATR is required for sizing (with fallback)
Sizing is ATR-based (approx):
- position size is chosen so that **SL distance × size ≈ risk budget**.

If ATR is missing, we convert fixed SL fallback into an ATR-equivalent:
- `atr_equiv = (price × sl_fallback_pct) / sl_atr_multiplier`

This prevents cycles from being blocked just because ATR data is missing.

---

## 5) ATR Policy (Updated)

**ATR definition:** ATR(14) on 1h candles.

**Sources (in priority order):**
1. **Binance USDT-M Futures** klines (`/fapi/v1/klines`) — primary
2. **Hyperliquid** `info` → `candleSnapshot` — fallback

**Cache:**
- 1 hour TTL in SQLite table `atr_cache`.

**Why:** tracker SSE ATR currently reports `available=false`, so we compute our own.

---

## 6) Execution Mechanics (How we enter/exit)

### Order Type Selection (v0.6.0)
AGI trader selects order type based on conviction and context:

| Order Type | When to Use |
|------------|-------------|
| **CHASE LIMIT** | High conviction (≥0.6), trend-aligned, time-sensitive |
| **LIMIT ORDER** | Lower conviction (<0.6), counter-trend, patient fill preferred |

AGI context includes `order_type_hint` and `order_type_reason` fields.

### Entries/Exits use Chase-Limit (default)
Executor uses a chase-limit algo:
- place limit at ~±2 ticks from mid
- poll every ~5s
- re-price if mid moves ≥2 ticks
- total timeout ~300s

### Safety preflight
Before executing, executor validates:
- symbol has a real mid price on the venue
- duplicate position rules
- venue routing rules

---

## 7) SL/TP Backstop (Hard Protection)

When enabled (`enable_sltp_backstop=true`):
- **SL:** stop order at `entry ± (ATR × sl_mult)`
- **TP:** limit reduce-only at `entry ± (ATR × tp_mult)`

Default multipliers (current config):
- `sl_atr_multiplier = 2.0`
- `tp_atr_multiplier = 3.0`

**Adaptive overrides (in priority order):**
1. AGI explicit override (`decision.risk.sl_atr_mult` / `tp_atr_mult`)
2. `adaptive_sltp` per-symbol adjustments (if learned)
3. config defaults

If ATR is unavailable, use fixed % fallback:
- SL distance = `entry × sl_fallback_pct` (current `2.0%`)

---

## 8) Exits: Decay + Signal Flip (Decision Points)

### Decay concepts
- `min_hold_hours` default **2h** (don’t whipsaw-close too fast)
- `max_hold_hours` default **24h** (prevents zombie positions)
- `stale_signal_max_minutes` default **5m** (context staleness)

### Signal-flip definition (strict)
A strict signal flip is ONLY when:
- live position is `LONG` or `SHORT`, and
- context direction is explicitly `LONG` or `SHORT`, and
- they disagree (`live != context`).

If context is `MIXED`, it is **not** an explicit flip.

### How it is handled
- In production, flip detection should typically be **notify/evaluate/act**, not blind auto-close.
- The code supports both:
  - `--notify-only` (flag + alert)
  - live closes (when explicitly run without notify-only)

---

## 9) Emergency Rails

1. **Catastrophic loss per position:** `emergency_loss_pct = 10%`
2. **Portfolio daily loss rail:** `emergency_portfolio_loss_pct = 15%`
3. **SL/TP missing after entry:** treat as incident → self-heal (repair SL/TP) + log.

---

## 10) What a “Good” Decision Payload Looks Like

Executor accepts an `ExecutionDecision` and optional risk overrides:

```json
{
  "symbol": "ETH",
  "direction": "LONG",
  "reason": "dead_capital+whale alignment",
  "conviction": 0.82,
  "size_usd": null,
  "risk": {
    "risk_pct": 1.5,
    "sl_atr_mult": 2.0,
    "tp_atr_mult": 3.0,
    "atr_source": "binance"
  }
}
```

Hard validation guardrails (skill.yaml):
- `risk_pct ∈ [0.5, 2.5]`
- `sl_mult ∈ [1.0, 3.0]`
- `tp_mult ∈ [1.5, 5.0]`
- `tp/sl ≤ 2.5`

---

## 11) Changelog Notes (Why this doc was updated)

Recent operational updates:
- **ATR moved to computed ATR(14) 1h** with **Binance primary / Hyperliquid fallback** and **1h cache**.
- Execution is **chase-limit-first** with symbol/position verification before journaling.
- Decay worker now uses `symbol_directions` for **all open positions** (not just top opps).
