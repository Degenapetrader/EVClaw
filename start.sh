#!/usr/bin/env bash
set -euo pipefail

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$DIR"

if [[ ! -f .env ]]; then
  echo ".env is missing. Run ./bootstrap.sh first." >&2
  exit 1
fi

# shellcheck disable=SC1091
source .env

if [[ -n "${HYPERLIQUID_API:-}" ]]; then
  echo "Unsupported legacy env var: HYPERLIQUID_API" >&2
  echo "Use HYPERLIQUID_AGENT_PRIVATE_KEY (delegated agent signer key for HYPERLIQUID_ADDRESS)." >&2
  echo "Do not use your main wallet private key." >&2
  exit 1
fi

MISSING_REQUIRED_ENV=()
if [[ -z "${HYPERLIQUID_ADDRESS:-}" ]]; then
  MISSING_REQUIRED_ENV+=("HYPERLIQUID_ADDRESS")
fi
if [[ -z "${HYPERLIQUID_AGENT_PRIVATE_KEY:-}" ]]; then
  MISSING_REQUIRED_ENV+=("HYPERLIQUID_AGENT_PRIVATE_KEY")
fi

if ((${#MISSING_REQUIRED_ENV[@]} > 0)); then
  echo "Missing required env vars: ${MISSING_REQUIRED_ENV[*]}" >&2
  echo "Set HYPERLIQUID_ADDRESS (main wallet address) and HYPERLIQUID_AGENT_PRIVATE_KEY (delegated agent signer key)." >&2
  echo "Edit .env, then run ./start.sh again." >&2
  exit 1
fi

if [[ ! -x ./restart.sh ]]; then
  echo "restart.sh is missing or not executable." >&2
  exit 1
fi

exec ./restart.sh
