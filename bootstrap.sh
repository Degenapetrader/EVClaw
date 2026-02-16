#!/usr/bin/env bash
set -euo pipefail

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$DIR"

if ! command -v python3 >/dev/null 2>&1; then
  echo "python3 is required" >&2
  exit 1
fi

if ! python3 - <<'PY'
import sys
raise SystemExit(0 if sys.version_info >= (3, 10) else 1)
PY
then
  echo "Python 3.10+ is required" >&2
  exit 1
fi

if ! python3 -m pip --version >/dev/null 2>&1; then
  echo "pip for python3 is required (try: python3 -m ensurepip --upgrade)" >&2
  exit 1
fi

if ! command -v tmux >/dev/null 2>&1; then
  echo "tmux is required (runtime uses tmux sessions via ./start.sh)" >&2
  echo "Install example: apt-get install -y tmux" >&2
  exit 1
fi

# Create venv if it does not exist.
if [[ ! -d .venv ]]; then
  echo "Creating virtual environment..."
  python3 -m venv .venv
fi

# Resolve python: venv > system.
EVCLAW_PYTHON="$DIR/.venv/bin/python3"
if [[ ! -x "$EVCLAW_PYTHON" ]]; then
  EVCLAW_PYTHON="python3"
fi
export EVCLAW_PYTHON

echo "Installing dependencies (using $EVCLAW_PYTHON)..."
"$EVCLAW_PYTHON" -m pip install -r requirements.txt

if [[ ! -f .env ]]; then
  cp .env.example .env
  echo "Created .env from template."
  echo "Edit .env with your wallet + provider credentials before running."
fi

ROOT_ESCAPED="$(printf '%s\n' "$DIR" | sed 's/[&|]/\\&/g')"
if grep -q '^EVCLAW_ROOT=' .env; then
  sed -i "s|^EVCLAW_ROOT=.*|EVCLAW_ROOT=$ROOT_ESCAPED|" .env
else
  printf "\nEVCLAW_ROOT=%s\n" "$DIR" >> .env
fi
export EVCLAW_ROOT="$DIR"

# shellcheck disable=SC1091
source .env

warn_if_missing_runtime_env() {
  local missing=()
  if [[ -z "${HYPERLIQUID_ADDRESS:-}" ]]; then
    missing+=("HYPERLIQUID_ADDRESS")
  fi
  if [[ -z "${HYPERLIQUID_AGENT_PRIVATE_KEY:-}" ]]; then
    missing+=("HYPERLIQUID_AGENT_PRIVATE_KEY")
  fi

  if ((${#missing[@]} > 0)); then
    echo "Warning: missing runtime env vars in .env: ${missing[*]}"
    echo "Bootstrap will continue. Set them before running ./start.sh"
  fi

  if [[ -n "${HYPERLIQUID_API:-}" ]]; then
    echo "Warning: legacy env var HYPERLIQUID_API is set and is unsupported." >&2
    echo "Use HYPERLIQUID_AGENT_PRIVATE_KEY (delegated agent signer key for HYPERLIQUID_ADDRESS)." >&2
    echo "Do not use your main wallet private key here." >&2
  fi
}

ensure_openclaw_cli() {
  if ! command -v openclaw >/dev/null 2>&1; then
    echo "openclaw is required but was not found in PATH." >&2
    echo "Install and configure OpenClaw first, then re-run ./bootstrap.sh" >&2
    return 1
  fi
}

ensure_openclaw_skill_link() {
  local skills_dir="${EVCLAW_OPENCLAW_SKILLS_DIR:-${OPENCLAW_SKILLS_DIR:-$HOME/.openclaw/skills}}"
  local link_path="$skills_dir/EVClaw"
  local repo_real
  repo_real="$(readlink -f "$DIR")"

  mkdir -p "$skills_dir"

  if [[ -L "$link_path" ]]; then
    local link_real
    link_real="$(readlink -f "$link_path" || true)"
    if [[ "$link_real" == "$repo_real" ]]; then
      echo "OpenClaw skill link already configured: $link_path"
      return 0
    fi
    rm -f "$link_path"
  fi

  if [[ -e "$link_path" ]]; then
    local existing_real
    existing_real="$(readlink -f "$link_path" || true)"
    if [[ "$existing_real" == "$repo_real" ]]; then
      echo "OpenClaw skill directory already points to this repo: $link_path"
      return 0
    fi
    echo "OpenClaw skill path already exists and points elsewhere: $link_path" >&2
    echo "Set EVCLAW_OPENCLAW_SKILLS_DIR to a different path if needed." >&2
    return 0
  fi

  ln -s "$DIR" "$link_path"
  echo "Linked EVClaw into OpenClaw skills: $link_path -> $DIR"
}

install_openclaw_helper_skills() {
  local enabled="${EVCLAW_INSTALL_EXTRA_SKILLS:-1}"
  if [[ "$enabled" != "1" && "${enabled,,}" != "true" && "${enabled,,}" != "yes" ]]; then
    echo "Skipping helper skill install (EVCLAW_INSTALL_EXTRA_SKILLS=$enabled)"
    return 0
  fi

  local skills_dir="${EVCLAW_OPENCLAW_SKILLS_DIR:-${OPENCLAW_SKILLS_DIR:-$HOME/.openclaw/skills}}"
  local bundle_dir="$DIR/openclaw_skills"
  local skills_csv="${EVCLAW_EXTRA_SKILLS:-trade,execute,best3,stats,hedge}"

  if [[ ! -d "$bundle_dir" ]]; then
    echo "Helper skill bundle not found: $bundle_dir" >&2
    return 1
  fi

  mkdir -p "$skills_dir"
  skills_dir="$(cd "$skills_dir" && pwd)"
  bundle_dir="$(cd "$bundle_dir" && pwd)"
  IFS=',' read -r -a requested <<< "$skills_csv"
  for raw in "${requested[@]}"; do
    local name="${raw//[[:space:]]/}"
    [[ -z "$name" ]] && continue

    # Strict name allowlist to prevent path traversal and unsafe rm targets.
    if [[ ! "$name" =~ ^[A-Za-z0-9_-]+$ ]]; then
      echo "Unsafe helper skill name (skip): $name" >&2
      continue
    fi

    local src="$bundle_dir/$name"
    local dst="$skills_dir/$name"

    if [[ ! -d "$src" ]]; then
      echo "Helper skill missing in bundle (skip): $name" >&2
      continue
    fi

    # Safety guard: never allow dst outside skills_dir.
    case "$dst" in
      "$skills_dir"/*) ;;
      *)
        echo "Refusing unsafe helper skill target path (skip): $dst" >&2
        continue
        ;;
    esac

    if [[ -e "$dst" || -L "$dst" ]]; then
      rm -rf -- "$dst"
    fi
    ln -s -- "$src" "$dst"
    echo "Installed OpenClaw helper skill: $dst -> $src"
  done
}

ensure_openclaw_cli
ensure_openclaw_skill_link
install_openclaw_helper_skills
warn_if_missing_runtime_env

mkdir -p state memory signals docs

"$EVCLAW_PYTHON" - <<'PY'
from ai_trader_db import AITraderDB
from pathlib import Path

db = AITraderDB()
print(f"DB initialized: {Path(db.db_path).resolve()}")
PY

"$EVCLAW_PYTHON" - <<'PY'
import yaml
with open("skill.yaml", "r", encoding="utf-8") as f:
    data = yaml.safe_load(f)
if not isinstance(data, dict):
    raise SystemExit("skill.yaml parse failed")
print("skill.yaml parsed successfully")
PY

if [[ "${EVCLAW_INSTALL_OPENCLAW_CRONS:-1}" == "1" || "${EVCLAW_INSTALL_OPENCLAW_CRONS:-}" == "true" ]]; then
  if [[ -x "./scripts/install_openclaw_crons.sh" ]]; then
    ./scripts/install_openclaw_crons.sh || true
  else
    echo "cron installer script missing or not executable; skipping"
  fi
fi

echo "Run ./start.sh to start all services"
