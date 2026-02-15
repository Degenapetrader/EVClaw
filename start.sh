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

MISSING_REQUIRED_ENV=()
if [[ -z "${HYPERLIQUID_ADDRESS:-}" ]]; then
  MISSING_REQUIRED_ENV+=("HYPERLIQUID_ADDRESS")
fi
if [[ -z "${HYPERLIQUID_API:-}" ]]; then
  MISSING_REQUIRED_ENV+=("HYPERLIQUID_API")
fi

if ((${#MISSING_REQUIRED_ENV[@]} > 0)); then
  echo "Missing required env vars: ${MISSING_REQUIRED_ENV[*]}" >&2
  echo "Edit .env, then run ./start.sh again." >&2
  exit 1
fi

if [[ ! -x ./restart.sh ]]; then
  echo "restart.sh is missing or not executable." >&2
  exit 1
fi

exec ./restart.sh
