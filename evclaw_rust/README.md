# EVCLAW

EVCLAW is a standalone Rust directional trader for Hyperliquid.

It keeps only:
- `DEAD_CAP` signal logic
- `WHALE` signal logic
- a Hyperliquid execution core aligned with the tester order lifecycle

## Behavior

- Hyperliquid native symbol universe
- Shared live account
- Single combined strategy per symbol:
  `0.5 * DEAD_CAP_strength * side + 0.5 * WHALE_strength * side`
- Base size `$30`, floored to `$12` when a non-zero signal is smaller
- No partial resize while the current sign stays the same
- Exchange-native ATR stop loss / take profit
- 1-hour minimum hold before any signal-driven exit
- Full exit if either `DEAD_CAP` or `WHALE` turns opposite after hold
- Same-cycle reversal when the combined sign is opposite after exit
- Tester-style Hyperliquid chase-limit execution
- Startup adoption of unmanaged live positions with SL/TP rebuild
- SQLite trade journal plus JSON runtime state

## Inputs

- Wallet labels from `EVCLAW_SCORED_WALLETS_PATH`
- Hyperliquid `/info` data for wallet/account scans and live positions
- `metaAndAssetCtxs` / `allMids` / `openOrders` / `orderStatus`
- Binance or Hyperliquid candles for ATR

## Run

```bash
cd /root/clawd/skills/EVClaw/evclaw_rust
cargo run --release -- --dry-run --once
```

```bash
cd /root/clawd/skills/EVClaw/evclaw_rust
cargo run --release -- --dry-run
```

## State

Persistent runtime state is stored under `EVCLAW_STATE_DIR`, and the trade journal under
`EVCLAW_JOURNAL_PATH`, defaults:

```text
/root/clawd/skills/EVClaw/evclaw_rust/state-live/state.json
/root/clawd/skills/EVClaw/evclaw_rust/state-live/trades.db
```

## Managed From EVClaw

When this bot is imported into the Openclaw EVClaw repo, manage it with:

```bash
cd /root/clawd/skills/EVClaw
./scripts/evclaw_rust_status.sh
./scripts/evclaw_rust_start.sh
./scripts/evclaw_rust_restart.sh
./scripts/evclaw_rust_logs.sh
```
