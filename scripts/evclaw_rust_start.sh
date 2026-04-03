#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BOT_ROOT="${EVCLAW_RUST_ROOT:-$ROOT/evclaw_rust}"
SESSION_NAME="${EVCLAW_RUST_SESSION_NAME:-evclaw-rust}"
LOG_DIR="$BOT_ROOT/logs"

if [[ ! -d "$BOT_ROOT" ]]; then
  echo "Missing Rust bot directory: $BOT_ROOT" >&2
  exit 1
fi

if [[ ! -f "$BOT_ROOT/.env" ]]; then
  echo "Missing Rust bot env file: $BOT_ROOT/.env" >&2
  echo "Copy $BOT_ROOT/.env.example first." >&2
  exit 1
fi

if ! command -v cargo >/dev/null 2>&1; then
  echo "cargo is required to build evclaw_rust." >&2
  exit 1
fi

if ! command -v tmux >/dev/null 2>&1; then
  echo "tmux is required to run evclaw_rust." >&2
  exit 1
fi

if tmux has-session -t "$SESSION_NAME" 2>/dev/null; then
  echo "Session already running: $SESSION_NAME" >&2
  exit 1
fi

mkdir -p "$LOG_DIR" "$BOT_ROOT/state-live" "$BOT_ROOT/state-live-backups"

echo "Building evclaw_rust..."
cargo build --release --manifest-path "$BOT_ROOT/Cargo.toml"

LOG_FILE="$LOG_DIR/live-$(date -u +%Y%m%dT%H%M%SZ).log"
CMD="cd '$BOT_ROOT' && ./target/release/evclaw >> '$LOG_FILE' 2>&1"
tmux new-session -d -s "$SESSION_NAME" "bash -lc \"$CMD\""

echo "Started evclaw_rust in tmux session $SESSION_NAME"
echo "Log: $LOG_FILE"
