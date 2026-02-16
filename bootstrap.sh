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

if ! command -v git >/dev/null 2>&1; then
  echo "git is required" >&2
  exit 1
fi

if ! python3 -m venv --help >/dev/null 2>&1; then
  echo "python3 venv module is required (try: apt-get install -y python3-venv)" >&2
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

echo "Installing core dependencies (using $EVCLAW_PYTHON)..."
"$EVCLAW_PYTHON" -m pip install -r requirements.txt

if [[ "${EVCLAW_INSTALL_LIGHTER_DEPS:-0}" == "1" || "${EVCLAW_INSTALL_LIGHTER_DEPS:-}" == "true" || "${EVCLAW_INSTALL_LIGHTER_DEPS:-}" == "yes" ]]; then
  if [[ -f requirements-lighter.txt ]]; then
    echo "Installing optional Lighter dependencies..."
    "$EVCLAW_PYTHON" -m pip install -r requirements-lighter.txt
  else
    echo "Optional Lighter dependency file not found: requirements-lighter.txt" >&2
  fi
fi

verify_core_python_deps() {
  "$EVCLAW_PYTHON" - <<'PY'
import importlib
import sys

checks = [
    ("aiohttp", "aiohttp"),
    ("aiohttp_sse_client", "aiohttp-sse-client"),
    ("dotenv", "python-dotenv"),
    ("requests", "requests"),
    ("yaml", "pyyaml"),
    ("eth_account", "eth-account"),
    ("hyperliquid.info", "hyperliquid-python-sdk"),
]

missing = []
for module_name, package_name in checks:
    try:
        importlib.import_module(module_name)
    except Exception as exc:
        missing.append((module_name, package_name, str(exc)))

if missing:
    print("Dependency check failed after requirements install.", file=sys.stderr)
    for module_name, package_name, err in missing:
        print(
            f"  - module '{module_name}' (package '{package_name}') missing: {err}",
            file=sys.stderr,
        )
    print("", file=sys.stderr)
    print("Fix:", file=sys.stderr)
    print("  ./.venv/bin/python3 -m pip install -r requirements.txt", file=sys.stderr)
    raise SystemExit(1)

print("Core dependencies verified.")
PY
}

verify_core_python_deps

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

ensure_evclaw_agent_env_defaults() {
  local pairs=(
    "EVCLAW_LLM_GATE_AGENT_ID=evclaw-entry-gate"
    "EVCLAW_HIP3_LLM_GATE_AGENT_ID=evclaw-hip3-entry-gate"
    "EVCLAW_EXIT_DECIDER_AGENT_ID=evclaw-exit-decider"
    "EVCLAW_HIP3_EXIT_DECIDER_AGENT_ID=evclaw-hip3-exit-decider"
    "EVCLAW_LEARNING_REFLECTOR_AGENT_ID=evclaw-learning-reflector"
  )
  local changed=0
  local key val cur

  for kv in "${pairs[@]}"; do
    key="${kv%%=*}"
    val="${kv#*=}"
    cur="${!key:-}"
    if grep -q "^${key}=" .env; then
      if [[ -z "$cur" || "$cur" == "default" || "$cur" == "openclaw-default" ]]; then
        sed -i "s|^${key}=.*|${key}=${val}|" .env
        export "${key}=${val}"
        changed=1
      fi
    else
      printf "%s=%s\n" "$key" "$val" >> .env
      export "${key}=${val}"
      changed=1
    fi
  done

  if [[ "$changed" == "1" ]]; then
    echo "Set EVClaw isolated agent IDs in .env."
  fi
}

ensure_openclaw_isolated_agents() {
  local enabled="${EVCLAW_INSTALL_ISOLATED_AGENTS:-1}"
  if [[ "$enabled" != "1" && "${enabled,,}" != "true" && "${enabled,,}" != "yes" ]]; then
    echo "Skipping isolated agent provisioning (EVCLAW_INSTALL_ISOLATED_AGENTS=$enabled)"
    return 0
  fi

  local agents_json
  if ! agents_json="$(openclaw agents list --json 2>/dev/null)"; then
    echo "Warning: unable to list OpenClaw agents; skipping isolated agent provisioning." >&2
    return 0
  fi

  local specs=(
    "${EVCLAW_LLM_GATE_AGENT_ID:-evclaw-entry-gate}|${EVCLAW_LLM_GATE_MODEL:-}"
    "${EVCLAW_HIP3_LLM_GATE_AGENT_ID:-evclaw-hip3-entry-gate}|${EVCLAW_HIP3_LLM_GATE_MODEL:-}"
    "${EVCLAW_EXIT_DECIDER_AGENT_ID:-evclaw-exit-decider}|${EVCLAW_EXIT_DECIDER_MODEL:-}"
    "${EVCLAW_HIP3_EXIT_DECIDER_AGENT_ID:-evclaw-hip3-exit-decider}|${EVCLAW_HIP3_EXIT_DECIDER_MODEL:-}"
    "${EVCLAW_LEARNING_REFLECTOR_AGENT_ID:-evclaw-learning-reflector}|"
  )

  local spec agent_id model workspace
  for spec in "${specs[@]}"; do
    agent_id="${spec%%|*}"
    model="${spec#*|}"
    agent_id="$(printf '%s' "$agent_id" | xargs)"
    model="$(printf '%s' "$model" | xargs)"
    [[ -z "$agent_id" ]] && continue

    if printf '%s' "$agents_json" | python3 - "$agent_id" <<'PY'
import json
import sys

target = str(sys.argv[1]).strip()
try:
    data = json.load(sys.stdin)
except Exception:
    raise SystemExit(2)

if isinstance(data, list):
    for row in data:
        if isinstance(row, dict) and str(row.get("id") or "").strip() == target:
            raise SystemExit(0)

raise SystemExit(1)
PY
    then
      echo "OpenClaw isolated agent exists: $agent_id"
      continue
    fi

    workspace="$HOME/.openclaw/workspace-$agent_id"
    if [[ -n "$model" ]]; then
      if openclaw agents add "$agent_id" --workspace "$workspace" --non-interactive --model "$model" >/dev/null 2>&1; then
        echo "Created OpenClaw isolated agent: $agent_id (model=$model)"
      else
        echo "Warning: failed to create OpenClaw agent $agent_id" >&2
      fi
    else
      if openclaw agents add "$agent_id" --workspace "$workspace" --non-interactive >/dev/null 2>&1; then
        echo "Created OpenClaw isolated agent: $agent_id"
      else
        echo "Warning: failed to create OpenClaw agent $agent_id" >&2
      fi
    fi

    agents_json="$(openclaw agents list --json 2>/dev/null || printf '[]')"
  done
}

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

remind_builder_approval() {
  echo "Reminder: approve builder fee for your wallet before first run:"
  echo "  https://atsetup.evplus.ai/"
  echo "If not approved, tracker/node2 auth can reject with 401/403."
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
ensure_evclaw_agent_env_defaults
ensure_openclaw_isolated_agents
ensure_openclaw_skill_link
install_openclaw_helper_skills
warn_if_missing_runtime_env
remind_builder_approval

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
