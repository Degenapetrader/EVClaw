# ACPTRADE

EVCLAW is a standalone Rust directional trader for Hyperliquid.

The intended live path is now:
- Virtuals ACP v2 agent wallet for competition identity
- unified Hyperliquid account on that wallet
- approved Hyperliquid API wallet for direct execution

ACP job trading is no longer the preferred execution path for this bot.
The bot should run in direct Hyperliquid mode with its original order lifecycle.

It keeps only:
- `DEAD_CAP` signal logic
- `WHALE` signal logic
- a Hyperliquid execution core aligned with the tester order lifecycle

## Behavior

- Hyperliquid native symbol universe
- Shared live account
- Single combined strategy per symbol:
  `0.5 * DEAD_CAP_strength * side + 0.5 * WHALE_strength * side`
- Direct Hyperliquid chase-limit execution
- Exchange-native Hyperliquid SL/TP orders
- No partial resize while the current sign stays the same
- ATR stop loss / take profit
- Config-driven minimum hold before signal-driven exit
- Signal-driven exits from current runtime rules
- Same-cycle reversal when the combined sign is opposite after exit
- Startup adoption of unmanaged live positions with SL/TP rebuild
- SQLite trade journal plus JSON runtime state

## Inputs

- Wallet labels from `EVCLAW_SCORED_WALLETS_PATH`
- Hyperliquid `/info` data for wallet/account scans and live positions
- `metaAndAssetCtxs` / `allMids` / `openOrders` / `orderStatus`
- Binance or Hyperliquid candles for ATR

## Run

```bash
cd /root/clawd/skills/EVClaw/acptrade
cargo run --release -- --dry-run --once
```

For live direct-HL mode, set in `.env`:

```text
EVCLAW_ACP_MODE=false
EVCLAW_ADDRESS=<master competition wallet>
EVCLAW_AGENT_PRIVATE_KEY=<hl api wallet key>
EVCLAW_VAULT_ADDRESS=
```

## State

Persistent runtime state is stored under `EVCLAW_STATE_DIR`, and the trade journal under
`EVCLAW_JOURNAL_PATH`, defaults:

```text
/root/clawd/skills/EVClaw/acptrade/state-live/state.json
/root/clawd/skills/EVClaw/acptrade/state-live/trades.db
```
