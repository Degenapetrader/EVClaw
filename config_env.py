"""Apply env overrides to skill.yaml config."""

from __future__ import annotations

from copy import deepcopy
import os
from typing import Any, Dict, Tuple

from env_utils import (
    env_present,
    env_str,
    env_int,
    env_float,
    env_bool,
    env_list,
    env_json,
)
from venues import (
    normalize_venue,
    parse_enabled_venues,
    VENUE_HYPERLIQUID,
    VENUE_HYPERLIQUID_WALLET,
    VENUE_LIGHTER,
)


PathKey = Tuple[str, ...]

# Keep env overrides focused on connectivity and runtime plumbing.
# Strategy/tuning params now come from skill.yaml by default.
ALLOWED_ENV_OVERRIDES = {
    "EVCLAW_SSE_HOST",
    "EVCLAW_SSE_PORT",
    "EVCLAW_SSE_ENDPOINT",
    "EVCLAW_DB_PATH",
    "EVCLAW_HL_ONLY",
    "EVCLAW_ENABLED_VENUES",
    "EVCLAW_DEFAULT_VENUE_PERPS",
    "EVCLAW_PERPS_VENUES",
    "EVCLAW_LEARNING_ENABLED",
    "EVCLAW_LEARNING_MIN_TRADES_FOR_ADJUSTMENT",
    "EVCLAW_LEARNING_MIN_SYMBOL_TRADES",
    "EVCLAW_LEARNING_SIGNIFICANCE_THRESHOLD",
    "EVCLAW_LEARNING_RATE",
    "EVCLAW_LEARNING_AUTO_UPDATE_INTERVAL_TRADES",
    "EVCLAW_LEARNING_DECAY_HALFLIFE_DAYS",
    "HYPERLIQUID_ADDRESS",
    "EVCLAW_WALLET_ADDRESS",
}

_WARNED_IGNORED_ENV_OVERRIDES = False


def _warn_ignored_env_overrides_once(names: set[str]) -> None:
    global _WARNED_IGNORED_ENV_OVERRIDES
    if _WARNED_IGNORED_ENV_OVERRIDES or not names:
        return
    sorted_names = sorted(names)
    preview = ", ".join(sorted_names[:12])
    extra = len(sorted_names) - 12
    if extra > 0:
        preview = f"{preview}, +{extra} more"
    print(
        "Config warning: ignoring non-whitelisted EVCLAW env overrides "
        "(YAML-first mode). "
        f"Ignored keys: {preview}"
    )
    _WARNED_IGNORED_ENV_OVERRIDES = True


def _get_path(cfg: Dict[str, Any], path: PathKey, default: Any = None) -> Any:
    cur: Any = cfg
    for key in path:
        if not isinstance(cur, dict) or key not in cur:
            return default
        cur = cur[key]
    return cur


def _set_path(cfg: Dict[str, Any], path: PathKey, value: Any) -> None:
    cur: Any = cfg
    for key in path[:-1]:
        if key not in cur or not isinstance(cur[key], dict):
            cur[key] = {}
        cur = cur[key]
    cur[path[-1]] = value


def apply_env_overrides(config: Dict[str, Any]) -> Dict[str, Any]:
    cfg = deepcopy(config) if config else {}
    allow_legacy_param_env = False
    ignored_env_overrides: set[str] = set()

    def override(path: PathKey, env_name: str, kind: str = "str") -> None:
        if not env_present(env_name):
            return
        if (not allow_legacy_param_env) and (env_name not in ALLOWED_ENV_OVERRIDES):
            ignored_env_overrides.add(env_name)
            return
        default = _get_path(cfg, path)
        if kind == "int":
            value = env_int(env_name, default if isinstance(default, int) else 0)
        elif kind == "float":
            value = env_float(env_name, float(default) if default is not None else 0.0)
        elif kind == "bool":
            value = env_bool(env_name, bool(default) if default is not None else False)
        elif kind == "list":
            value = env_list(env_name, default if isinstance(default, list) else [])
        elif kind == "json":
            value = env_json(env_name, default if default is not None else {})
        else:
            value = env_str(env_name, default if default is not None else "")
        _set_path(cfg, path, value)

    # Top-level config
    override(("config", "sse_host"), "EVCLAW_SSE_HOST")
    override(("config", "sse_port"), "EVCLAW_SSE_PORT", kind="int")
    override(("config", "sse_endpoint"), "EVCLAW_SSE_ENDPOINT")
    wallet_env_name = "HYPERLIQUID_ADDRESS"
    if not env_present(wallet_env_name):
        wallet_env_name = "EVCLAW_WALLET_ADDRESS"
    override(("config", "wallet_address"), wallet_env_name)
    override(("config", "db_path"), "EVCLAW_DB_PATH")

    # Risk: YAML-first trading config (no direct env alias overrides).

    # Executor/trading tuning params are YAML-first and no longer accept direct
    # legacy EVCLAW_EXECUTOR_* env aliases.

    # Learning
    override(("config", "learning", "enabled"), "EVCLAW_LEARNING_ENABLED", kind="bool")
    override(("config", "learning", "min_trades_for_adjustment"), "EVCLAW_LEARNING_MIN_TRADES_FOR_ADJUSTMENT", kind="int")
    override(("config", "learning", "min_symbol_trades"), "EVCLAW_LEARNING_MIN_SYMBOL_TRADES", kind="int")
    override(("config", "learning", "significance_threshold"), "EVCLAW_LEARNING_SIGNIFICANCE_THRESHOLD", kind="float")
    override(("config", "learning", "learning_rate"), "EVCLAW_LEARNING_RATE", kind="float")
    override(("config", "learning", "auto_update_interval_trades"), "EVCLAW_LEARNING_AUTO_UPDATE_INTERVAL_TRADES", kind="int")
    override(("config", "learning", "decay_halflife_days"), "EVCLAW_LEARNING_DECAY_HALFLIFE_DAYS", kind="float")

    _warn_ignored_env_overrides_once(ignored_env_overrides)

    # Venue config (unified Hyperliquid venue).
    exec_cfg = _get_path(cfg, ("config", "executor"), {}) or {}
    lighter_enabled = bool(exec_cfg.get("lighter_enabled", True))
    hl_enabled = bool(exec_cfg.get("hl_enabled", True))
    default_perps = normalize_venue(exec_cfg.get("default_venue_perps", VENUE_HYPERLIQUID))
    default_hip3 = normalize_venue(exec_cfg.get("default_venue_hip3", VENUE_HYPERLIQUID))
    perps_venues = exec_cfg.get("perps_venues", []) or []
    if isinstance(perps_venues, list):
        perps_venues = [normalize_venue(v) for v in perps_venues if normalize_venue(v)]
    else:
        perps_venues = []

    source = "yaml"

    new_enabled_raw = env_str("EVCLAW_ENABLED_VENUES", "").strip()
    new_default_perps_raw = env_str("EVCLAW_DEFAULT_VENUE_PERPS", "").strip()
    new_perps_venues_raw = env_str("EVCLAW_PERPS_VENUES", "").strip()
    new_legacy_wallet_flag = env_present("EVCLAW_MIRROR_PERPS_TO_HL_WALLET")

    # Kept for compatibility; per-venue wallet mirroring is no longer separate.
    # A legacy flag in the environment only affects observability.
    new_any = bool(
        new_enabled_raw
        or new_default_perps_raw
        or new_perps_venues_raw
        or new_legacy_wallet_flag
    )

    if new_any:
        source = "new_env"
        if new_enabled_raw:
            enabled = parse_enabled_venues(new_enabled_raw)
            if enabled:
                lighter_enabled = VENUE_LIGHTER in enabled
                hl_enabled = VENUE_HYPERLIQUID in enabled
        if new_default_perps_raw:
            default_perps = normalize_venue(new_default_perps_raw)
        if new_perps_venues_raw:
            perps_venues = parse_enabled_venues(new_perps_venues_raw)
        if new_legacy_wallet_flag:
            print(
                "Config note: EVCLAW_MIRROR_PERPS_TO_HL_WALLET is deprecated "
                "because Hyperliquid no longer uses a split wallet venue."
            )
    if not new_any and not env_present("HYPERLIQUID_AGENT_PRIVATE_KEY"):
        # Use YAML defaults only.
        pass

    # Defaults if invalid or missing.
    default_perps = normalize_venue(default_perps)
    if default_perps not in (VENUE_HYPERLIQUID, VENUE_LIGHTER):
        if hl_enabled:
            default_perps = VENUE_HYPERLIQUID
        elif lighter_enabled:
            default_perps = VENUE_LIGHTER
        else:
            default_perps = VENUE_HYPERLIQUID

    default_hip3 = VENUE_HYPERLIQUID_WALLET

    _set_path(cfg, ("config", "executor", "lighter_enabled"), lighter_enabled)
    _set_path(cfg, ("config", "executor", "hl_enabled"), hl_enabled)
    _set_path(cfg, ("config", "executor", "hl_wallet_enabled"), False)
    _set_path(cfg, ("config", "executor", "hl_mirror_wallet"), False)
    _set_path(cfg, ("config", "executor", "default_venue_perps"), default_perps)
    _set_path(cfg, ("config", "executor", "default_venue_hip3"), default_hip3)
    _set_path(cfg, ("config", "executor", "perps_venues"), perps_venues)

    # One-line startup summary (once per process).
    try:
        if os.getenv("_EVCLAW_CONFIG_LOGGED") != "1":
            enabled_list = []
            if hl_enabled:
                enabled_list.append(VENUE_HYPERLIQUID)
            if lighter_enabled:
                enabled_list.append(VENUE_LIGHTER)
            perps_label = ",".join(perps_venues) if perps_venues else ""
            print(
                f"Config: enabled_venues={','.join(enabled_list) or 'none'} "
                f"default_perps={default_perps} perps_venues={perps_label or 'auto'} "
                f"mirror_perps=0 (source={source})"
            )
            os.environ["_EVCLAW_CONFIG_LOGGED"] = "1"
    except Exception:
        pass

    return cfg
