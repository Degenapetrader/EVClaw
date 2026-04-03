#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BOT_ROOT="${EVCLAW_RUST_ROOT:-$ROOT/evclaw_rust}"
LOG_DIR="$BOT_ROOT/logs"

if [[ ! -d "$LOG_DIR" ]]; then
  echo "Missing log directory: $LOG_DIR" >&2
  exit 1
fi

LATEST_LOG="$(ls -1t "$LOG_DIR"/*.log 2>/dev/null | head -n 1 || true)"
if [[ -z "$LATEST_LOG" ]]; then
  echo "No Rust bot logs found in $LOG_DIR" >&2
  exit 1
fi

exec tail -n 200 -f "$LATEST_LOG"
