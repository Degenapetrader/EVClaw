#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BOT_ROOT="${EVCLAW_RUST_ROOT:-$ROOT/evclaw_rust}"
ENV_FILE="$BOT_ROOT/.env"
SESSION_NAME="${EVCLAW_RUST_SESSION_NAME:-evclaw-rust}"

if [[ ! -d "$BOT_ROOT" ]]; then
  echo "Missing Rust bot directory: $BOT_ROOT" >&2
  exit 1
fi

if [[ ! -f "$ENV_FILE" ]]; then
  echo "Missing Rust bot env file: $ENV_FILE" >&2
  exit 1
fi

read_env() {
  local key="$1"
  awk -F= -v key="$key" '$1 == key {sub(/^[[:space:]]+/, "", $2); print $2}' "$ENV_FILE" | tail -n 1
}

ADDRESS="$(read_env EVCLAW_ADDRESS)"
PRIVATE_URL="$(read_env EVCLAW_PRIVATE_INFO_URL)"
PUBLIC_URL="$(read_env EVCLAW_PUBLIC_INFO_URL)"
LOG_DIR="$BOT_ROOT/logs"
LATEST_LOG="$(ls -1t "$LOG_DIR"/*.log 2>/dev/null | head -n 1 || true)"

echo "bot_root=$BOT_ROOT"
echo "session_name=$SESSION_NAME"
if command -v tmux >/dev/null 2>&1 && tmux has-session -t "$SESSION_NAME" 2>/dev/null; then
  echo "session_status=running"
else
  echo "session_status=stopped"
fi
echo "address=${ADDRESS:-unknown}"
echo "latest_log=${LATEST_LOG:-none}"

if [[ -z "${ADDRESS:-}" ]]; then
  exit 0
fi

query_info() {
  local url="$1"
  local payload="$2"
  [[ -z "$url" ]] && return 1
  curl -fsS "$url" -H 'Content-Type: application/json' -d "$payload"
}

STATE_PAYLOAD="{\"type\":\"clearinghouseState\",\"user\":\"$ADDRESS\"}"
ORDERS_PAYLOAD="{\"type\":\"openOrders\",\"user\":\"$ADDRESS\"}"

STATE_JSON="$(query_info "$PRIVATE_URL" "$STATE_PAYLOAD" || query_info "$PUBLIC_URL" "$STATE_PAYLOAD" || true)"
ORDERS_JSON="$(query_info "$PRIVATE_URL" "$ORDERS_PAYLOAD" || query_info "$PUBLIC_URL" "$ORDERS_PAYLOAD" || true)"

if [[ -n "$STATE_JSON" ]] && command -v jq >/dev/null 2>&1; then
  echo "$STATE_JSON" | jq -r '"account_value=" + (.marginSummary.accountValue // "unknown") + "\nwithdrawable=" + (.withdrawable // "unknown") + "\npositions=" + ((.assetPositions | length | tostring) // "0")'
fi

if [[ -n "$ORDERS_JSON" ]] && command -v jq >/dev/null 2>&1; then
  echo "$ORDERS_JSON" | jq -r '"open_orders=" + (length | tostring) + "\nreduce_only_orders=" + (map(select(.reduceOnly == true)) | length | tostring)'
fi
