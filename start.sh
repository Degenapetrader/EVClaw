#!/usr/bin/env bash
set -euo pipefail

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$DIR"

# Resolve python once for preflight checks (venv preferred).
EVCLAW_PYTHON="$DIR/.venv/bin/python3"
if [[ ! -x "$EVCLAW_PYTHON" ]]; then
  EVCLAW_PYTHON="python3"
fi

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

normalize_private_info_url() {
  local raw="${1:-}"
  local base="${raw%/}"
  if [[ -z "$base" ]]; then
    base="https://node2.evplus.ai/evclaw/info"
  fi
  if [[ "$base" != */info ]]; then
    base="${base}/info"
  fi
  printf '%s' "$base"
}

append_wallet_key_if_needed() {
  local url="${1:-}"
  if [[ "$url" == *"node2.evplus.ai"*"/evclaw/info"* ]]; then
    if [[ "$url" != *"key="* ]]; then
      local sep='?'
      if [[ "$url" == *"?"* ]]; then
        sep='&'
      fi
      url="${url}${sep}key=${HYPERLIQUID_ADDRESS}"
    fi
  fi
  printf '%s' "$url"
}

preflight_node2_meta() {
  local skip="${EVCLAW_SKIP_NODE2_PREFLIGHT:-0}"
  if [[ "$skip" == "1" || "${skip,,}" == "true" || "${skip,,}" == "yes" ]]; then
    echo "Skipping node2 preflight (EVCLAW_SKIP_NODE2_PREFLIGHT=$skip)"
    return 0
  fi

  local raw="${HYPERLIQUID_PRIVATE_NODE:-https://node2.evplus.ai/evclaw/info}"
  local info_url
  info_url="$(normalize_private_info_url "$raw")"
  local url_with_key
  url_with_key="$(append_wallet_key_if_needed "$info_url")"

  local result status body
  result="$("$EVCLAW_PYTHON" - "$url_with_key" <<'PY'
import json
import sys
import urllib.error
import urllib.request

url = sys.argv[1]
payload = json.dumps({"type": "meta"}).encode("utf-8")
req = urllib.request.Request(
    url,
    data=payload,
    headers={"Content-Type": "application/json"},
    method="POST",
)

try:
    with urllib.request.urlopen(req, timeout=8) as resp:
        body = resp.read().decode("utf-8", errors="replace")
        print(f"{resp.status}\t{body[:240]}")
except urllib.error.HTTPError as exc:
    body = exc.read().decode("utf-8", errors="replace")
    print(f"{exc.code}\t{body[:240]}")
except Exception as exc:
    print(f"000\t{exc}")
PY
)"

  status="${result%%$'\t'*}"
  body="${result#*$'\t'}"

  if [[ "$status" == "200" ]]; then
    echo "node2 preflight passed: POST ${url_with_key} {\"type\":\"meta\"} -> 200"
    return 0
  fi

  echo "node2 preflight failed (status=${status})" >&2
  echo "URL tested: ${url_with_key}" >&2
  echo "Response: ${body}" >&2
  if [[ "$status" == "401" ]]; then
    echo "Fix: ensure key is in query (?key=\$HYPERLIQUID_ADDRESS) and wallet format is 0x + 40 hex chars." >&2
  elif [[ "$status" == "403" ]]; then
    echo "Fix: approve builder fee at https://atsetup.evplus.ai/ then retry." >&2
  elif [[ "$status" == "422" ]]; then
    echo "Fix: use POST /evclaw/info with JSON object body; do not use /evclaw/meta or wallet-in-body auth." >&2
  else
    echo "Fix: verify node2 reachability and HYPERLIQUID_PRIVATE_NODE value." >&2
  fi
  return 1
}

preflight_node2_meta

if [[ ! -x ./restart.sh ]]; then
  echo "restart.sh is missing or not executable." >&2
  exit 1
fi

exec ./restart.sh
