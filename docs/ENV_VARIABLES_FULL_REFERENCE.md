# EVClaw Environment Variables (OSS Baseline)

This reference documents the active EVClaw runtime environment variables.
Legacy aliases are intentionally removed in this clean-slate OSS package.

## Core Runtime

| Key | Default | Required | Purpose |
|---|---|---|---|
| `HYPERLIQUID_ADDRESS` | `""` | Yes | Trading wallet address. |
| `HYPERLIQUID_API` | `""` | Yes | Trading API/private key used by adapters. |
| `HYPERLIQUID_PRIVATE_NODE` | `https://node2.evplus/info` | No | Private node info endpoint. |
| `HYPERLIQUID_PROXIES` | `""` | No | Optional comma-separated proxy list for public requests. |
| `EVCLAW_DB_PATH` | `./ai_trader.db` | No | SQLite DB location. |
| `EVCLAW_RUNTIME_DIR` | `./state` | No | Runtime artifacts directory. |
| `EVCLAW_DOCS_DIR` | `./docs` | No | Docs output directory. |
| `EVCLAW_MEMORY_DIR` | `./memory` | No | Runtime memory directory. |
| `EVCLAW_SIGNALS_DIR` | `./signals` | No | Worker signal output directory. |

## Tracker / SSE (EVPlus)

| Key | Default | Required | Purpose |
|---|---|---|---|
| `EVCLAW_TRACKER_BASE_URL` | `https://tracker.evplus.ai` | No | Base tracker API URL. |
| `EVCLAW_SSE_HOST` | `tracker.evplus.ai` | No | SSE host. |
| `EVCLAW_SSE_PORT` | `8443` | No | SSE port. |
| `EVCLAW_SSE_ENDPOINT` | `/sse/tracker` | No | SSE endpoint path. |
| `EVCLAW_TRACKER_HIP3_PREDATOR_URL` | `${EVCLAW_TRACKER_BASE_URL}/api/hip3/predator-state` | No | HIP3 predator state URL. |
| `EVCLAW_TRACKER_HIP3_SYMBOLS_URL` | `${EVCLAW_TRACKER_BASE_URL}/api/hip3-symbols` | No | HIP3 symbols URL. |
| `EVCLAW_TRACKER_SYMBOL_URL_TEMPLATE` | `${EVCLAW_TRACKER_BASE_URL}/api/symbols/{symbol_upper}.json` | No | Per-symbol tracker metadata URL template. |

## Venue Routing

| Key | Default | Required | Purpose |
|---|---|---|---|
| `EVCLAW_ENABLED_VENUES` | `hyperliquid,hip3` | No | Enabled execution venues. |
| `EVCLAW_DEFAULT_VENUE_PERPS` | `hyperliquid` | No | Default venue for perp symbols. |
| `EVCLAW_DEFAULT_VENUE_HIP3` | `hip3` | No | Default venue for HIP3 symbols. |

## LLM Controls

| Key | Default | Required | Purpose |
|---|---|---|---|
| `EVCLAW_LLM_GATE_MODEL` | `""` | No | Entry gate model override. |
| `EVCLAW_LLM_GATE_THINKING` | `medium` | No | Entry gate reasoning level. |
| `EVCLAW_LLM_GATE_TIMEOUT_SEC` | `120` | No | Entry gate timeout. |
| `EVCLAW_LLM_GATE_MAX_KEEP` | `4` | No | Entry gate max selected candidates. |
| `EVCLAW_HIP3_LLM_GATE_MODEL` | `""` | No | HIP3 entry gate model override. |
| `EVCLAW_EXIT_DECIDER_MODEL` | `openai-codex/gpt-5.2` | No | Exit decider model. |
| `EVCLAW_EXIT_DECIDER_THINKING` | `medium` | No | Exit decider reasoning level. |
| `EVCLAW_HIP3_EXIT_DECIDER_MODEL` | `""` | No | HIP3 exit decider model. |

## Optional Integrations

| Key | Default | Required | Purpose |
|---|---|---|---|
| `LIGHTER_BASE_URL` | `https://mainnet.zklighter.elliot.ai` | No | Lighter API base URL. |
| `LIGHTER_ACCOUNT_INDEX` | `""` | No | Lighter account index. |
| `LIGHTER_API_KEY_PRIVATE_KEY` | `""` | No | Lighter API private key. |
| `LIGHTER_API_KEY_INDEX` | `""` | No | Lighter API key index. |
| `MASSIVE_API_KEY` | `""` | No | Massive API key for HIP3/candle pathways. |
