"""Environment helpers for EVClaw (loads .env + typed accessors)."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Iterable, Optional

from dotenv import load_dotenv


# Load .env early for any module importing env_utils.
load_dotenv(Path(__file__).parent / ".env")


_TRUE = {"1", "true", "yes", "y", "on"}
_FALSE = {"0", "false", "no", "n", "off"}


def _env_name_candidates(name: str) -> list[str]:
    if not name:
        return []
    return [name]


def _env_lookup(name: str) -> Optional[str]:
    return os.getenv(name)


def env_present(name: str) -> bool:
    value = _env_lookup(name)
    return value is not None and str(value).strip() != ""


def env_str(name: str, default: Optional[str] = None) -> Optional[str]:
    if not env_present(name):
        return default
    return str(_env_lookup(name) or "").strip()


def env_int(name: str, default: int) -> int:
    if not env_present(name):
        return default
    try:
        return int(str(_env_lookup(name) or "").strip())
    except (TypeError, ValueError):
        return default


def env_float(name: str, default: float) -> float:
    if not env_present(name):
        return default
    try:
        return float(str(_env_lookup(name) or "").strip())
    except (TypeError, ValueError):
        return default


def _join_tracker_api_url(base_url: Optional[str], path: str) -> str:
    base = (str(base_url or "").strip().rstrip("/"))
    if not base:
        return ""
    if not path:
        return base
    if not path.startswith("/"):
        path = "/" + path
    return f"{base}{path}"


def env_bool(name: str, default: bool) -> bool:
    if not env_present(name):
        return default
    value = str(_env_lookup(name) or "").strip().lower()
    if value in _TRUE:
        return True
    if value in _FALSE:
        return False
    return default


def env_json(name: str, default: Any) -> Any:
    if not env_present(name):
        return default
    raw = str(_env_lookup(name) or "").strip()
    if not raw:
        return default
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return default


_DEFAULT_ROOT = Path(__file__).resolve().parent
_ROOT_RAW = env_str("EVCLAW_ROOT", str(_DEFAULT_ROOT)) or str(_DEFAULT_ROOT)
_ROOT_PATH = Path(_ROOT_RAW).expanduser()
if not _ROOT_PATH.is_absolute():
    _ROOT_PATH = (_DEFAULT_ROOT / _ROOT_PATH).resolve()

EVCLAW_ROOT = str(_ROOT_PATH)
EVCLAW_RUNTIME_DIR = env_str("EVCLAW_RUNTIME_DIR", str(Path(EVCLAW_ROOT) / "state"))
EVCLAW_DOCS_DIR = env_str("EVCLAW_DOCS_DIR", str(Path(EVCLAW_ROOT) / "docs"))
EVCLAW_MEMORY_DIR = env_str("EVCLAW_MEMORY_DIR", str(Path(EVCLAW_ROOT) / "memory"))
EVCLAW_DB_PATH = env_str("EVCLAW_DB_PATH", str(Path(EVCLAW_ROOT) / "ai_trader.db"))
EVCLAW_SIGNALS_DIR = env_str("EVCLAW_SIGNALS_DIR", str(Path(EVCLAW_ROOT) / "signals"))
EVCLAW_TRACKER_BASE_URL = env_str("EVCLAW_TRACKER_BASE_URL", "https://tracker.evplus.ai")
EVCLAW_TRACKER_HIP3_PREDATOR_URL = env_str(
    "EVCLAW_TRACKER_HIP3_PREDATOR_URL",
    _join_tracker_api_url(EVCLAW_TRACKER_BASE_URL, "/api/hip3/predator-state"),
)
EVCLAW_TRACKER_HIP3_SYMBOLS_URL = env_str(
    "EVCLAW_TRACKER_HIP3_SYMBOLS_URL",
    _join_tracker_api_url(EVCLAW_TRACKER_BASE_URL, "/api/hip3-symbols"),
)
EVCLAW_TRACKER_SYMBOL_URL_TEMPLATE = env_str(
    "EVCLAW_TRACKER_SYMBOL_URL_TEMPLATE",
    _join_tracker_api_url(
        EVCLAW_TRACKER_BASE_URL, "/api/symbols/{symbol_upper}.json"
    ),
)


def _ensure_runtime_paths() -> None:
    """Create EVClaw runtime and docs directories if needed."""
    try:
        Path(EVCLAW_RUNTIME_DIR).mkdir(parents=True, exist_ok=True)
    except Exception:
        pass
    try:
        Path(EVCLAW_DOCS_DIR).mkdir(parents=True, exist_ok=True)
    except Exception:
        pass
    try:
        Path(EVCLAW_MEMORY_DIR).mkdir(parents=True, exist_ok=True)
    except Exception:
        pass
    try:
        Path(EVCLAW_SIGNALS_DIR).mkdir(parents=True, exist_ok=True)
    except Exception:
        pass


_ensure_runtime_paths()


def env_list(name: str, default: Iterable[str]) -> list:
    if not env_present(name):
        return list(default)
    raw = str(os.getenv(name)).strip()
    if not raw:
        return list(default)
    if raw.startswith("["):
        parsed = env_json(name, list(default))
        return list(parsed) if isinstance(parsed, list) else list(default)
    parts = [p.strip() for p in raw.split(",") if p.strip()]
    return parts if parts else list(default)
