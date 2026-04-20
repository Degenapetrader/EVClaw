use std::collections::HashSet;
use std::env;
use std::path::PathBuf;

use anyhow::{anyhow, Context, Result};
use dotenvy::from_path;

#[derive(Debug, Clone)]
pub struct RuntimeOverrides {
    pub dry_run_flag: bool,
    pub symbols: Option<Vec<String>>,
}

#[derive(Debug, Clone)]
pub struct Config {
    pub enabled: bool,
    pub dry_run: bool,
    pub acp_mode: bool,
    pub loop_interval_secs: u64,
    pub min_hold_hours: f64,
    pub reentry_cooldown_secs: u64,
    pub sl_atr_multiplier: f64,
    pub tp_atr_multiplier: f64,
    pub fallback_exit_pct: f64,
    pub base_notional_usd: f64,
    pub min_trade_notional_usd: f64,
    pub tp_min_quote_usd: f64,
    pub max_open_positions: usize,
    pub public_info_url: String,
    pub private_info_url: Option<String>,
    pub binance_klines_url: String,
    pub log_level: String,
    pub state_dir: PathBuf,
    pub journal_path: PathBuf,
    pub scored_wallets_path: PathBuf,
    pub request_timeout_secs: u64,
    pub max_wallet_concurrency: usize,
    pub wallet_batch_size: usize,
    pub dead_cap_inactive_min_volume_usd: f64,
    pub dead_cap_max_extra_wallets: usize,
    pub atr_period: usize,
    pub atr_interval: String,
    pub chase_limit_timeout_secs: u64,
    pub chase_check_interval_ms: u64,
    pub chase_replace_ticks: f64,
    pub sltp_reconcile_every_cycles: u64,
    pub sltp_throttle_delay_ms: u64,
    pub exit_verify_retries: u32,
    pub exit_verify_delay_ms: u64,
    pub rate_limit_backoff_ms: u64,
    pub hyperliquid_proxies: Vec<String>,
    pub acp_pending_timeout_secs: u64,
    pub acp_max_new_entries_per_cycle: usize,
    pub acp_entry_submit_delay_ms: u64,
    pub acp_leverage: f64,
    pub acp_limit_offset_bps: f64,
    pub acp_sltp_resubmit_cooldown_secs: u64,
    pub acp_sltp_refresh_secs: u64,
    pub symbols: Option<HashSet<String>>,
    pub hl_address: Option<String>,
    pub hl_agent_private_key: Option<String>,
    pub hl_vault_address: Option<String>,
    pub dgclaw_api_key: Option<String>,
    pub dgclaw_trader_base_url: String,
    pub dgclaw_job_script_path: PathBuf,
}

impl Config {
    pub fn load(overrides: RuntimeOverrides) -> Result<Self> {
        let env_path = resolve_env_path()?;
        from_path(&env_path).with_context(|| {
            format!("failed to load env file at {}", env_path.to_string_lossy())
        })?;

        let enabled = env_bool("EVCLAW_ENABLED", true);
        let dry_run = overrides.dry_run_flag || env_bool("EVCLAW_DRY_RUN", true);
        let acp_mode = env_bool("EVCLAW_ACP_MODE", false);

        let symbols = if let Some(cli_symbols) = overrides.symbols {
            normalize_symbol_set(cli_symbols)
        } else {
            normalize_symbol_set(env_csv("EVCLAW_SYMBOLS", ""))
        };

        let hl_address = env_opt_string("EVCLAW_ADDRESS");
        let hl_agent_private_key = env_opt_string("EVCLAW_AGENT_PRIVATE_KEY");
        if !dry_run && !acp_mode {
            if hl_address.is_none() {
                return Err(anyhow!(
                    "EVCLAW_ADDRESS is required when EVCLAW_DRY_RUN=false"
                ));
            }
            if hl_agent_private_key.is_none() {
                return Err(anyhow!(
                    "EVCLAW_AGENT_PRIVATE_KEY is required when EVCLAW_DRY_RUN=false"
                ));
            }
        }
        let dgclaw_api_key = env_opt_string("DGCLAW_API_KEY");
        if acp_mode && dgclaw_api_key.is_none() {
            return Err(anyhow!(
                "DGCLAW_API_KEY is required when EVCLAW_ACP_MODE=true"
            ));
        }

        let base_notional_usd = env_f64("EVCLAW_BASE_NOTIONAL_USD", 30.0).max(1.0);
        let min_trade_notional_usd = env_f64(
            "EVCLAW_MIN_TRADE_NOTIONAL_USD",
            env_f64("EVCLAW_MIN_ORDER_NOTIONAL_USD", 12.0),
        )
        .max(1.0);
        let tp_min_quote_usd = env_f64("EVCLAW_TP_MIN_QUOTE_USD", 10.0).max(1.0);
        let max_open_positions = env_u64("EVCLAW_MAX_OPEN_POSITIONS", 30).max(1) as usize;

        Ok(Self {
            enabled,
            dry_run,
            acp_mode,
            loop_interval_secs: env_u64("EVCLAW_LOOP_INTERVAL_SECS", 60).max(5),
            min_hold_hours: env_f64("EVCLAW_MIN_HOLD_HOURS", 4.0).max(0.0),
            reentry_cooldown_secs: env_u64("EVCLAW_REENTRY_COOLDOWN_SECS", 3_600).max(0),
            sl_atr_multiplier: env_f64("EVCLAW_SL_ATR_MULTIPLIER", 1.5).max(0.1),
            tp_atr_multiplier: env_f64("EVCLAW_TP_ATR_MULTIPLIER", 1.0).max(0.1),
            fallback_exit_pct: env_f64("EVCLAW_FALLBACK_EXIT_PCT", 0.025).max(0.001),
            base_notional_usd,
            min_trade_notional_usd,
            tp_min_quote_usd,
            max_open_positions,
            public_info_url: env_string(
                "EVCLAW_PUBLIC_INFO_URL",
                "https://api.hyperliquid.xyz/info",
            ),
            private_info_url: env_opt_string("EVCLAW_PRIVATE_INFO_URL"),
            binance_klines_url: env_string(
                "EVCLAW_BINANCE_KLINES_URL",
                "https://fapi.binance.com/fapi/v1/klines",
            ),
            log_level: env_string("EVCLAW_LOG_LEVEL", "info"),
            state_dir: PathBuf::from(env_string(
                "EVCLAW_STATE_DIR",
                "/root/clawd/skills/EVClaw/acptrade/state-live",
            )),
            journal_path: PathBuf::from(env_string(
                "EVCLAW_JOURNAL_PATH",
                "/root/clawd/skills/EVClaw/acptrade/state-live/trades.db",
            )),
            scored_wallets_path: PathBuf::from(env_string(
                "EVCLAW_SCORED_WALLETS_PATH",
                "/root/clawd/skills/EVClaw/acptrade/scored_wallets.json",
            )),
            request_timeout_secs: env_u64("EVCLAW_REQUEST_TIMEOUT_SECS", 10).max(1),
            max_wallet_concurrency: env_u64("EVCLAW_MAX_WALLET_CONCURRENCY", 50).max(1) as usize,
            wallet_batch_size: env_u64("EVCLAW_WALLET_BATCH_SIZE", 5_000).max(10) as usize,
            dead_cap_inactive_min_volume_usd: env_f64(
                "EVCLAW_DEAD_CAP_INACTIVE_MIN_VOLUME_USD",
                50_000_000.0,
            )
            .max(0.0),
            dead_cap_max_extra_wallets: env_u64("EVCLAW_DEAD_CAP_MAX_EXTRA_WALLETS", 2_000).max(0)
                as usize,
            atr_period: env_u64("EVCLAW_ATR_PERIOD", 14).max(2) as usize,
            atr_interval: env_string("EVCLAW_ATR_INTERVAL", "1h"),
            chase_limit_timeout_secs: env_u64("EVCLAW_CHASE_LIMIT_TIMEOUT_SECS", 15).max(5),
            chase_check_interval_ms: env_u64("EVCLAW_CHASE_CHECK_INTERVAL_MS", 1_000).max(100),
            chase_replace_ticks: env_f64("EVCLAW_CHASE_REPLACE_TICKS", 2.0).max(0.1),
            sltp_reconcile_every_cycles: env_u64("EVCLAW_SLTP_RECONCILE_EVERY_CYCLES", 5).max(1),
            sltp_throttle_delay_ms: env_u64("EVCLAW_SLTP_THROTTLE_DELAY_MS", 1_500),
            exit_verify_retries: env_u64("EVCLAW_EXIT_VERIFY_RETRIES", 3).max(1) as u32,
            exit_verify_delay_ms: env_u64("EVCLAW_EXIT_VERIFY_DELAY_MS", 500),
            rate_limit_backoff_ms: env_u64("EVCLAW_RATE_LIMIT_BACKOFF_MS", 5_000),
            hyperliquid_proxies: env_csv(
                "HYPERLIQUID_PROXIES",
                &env::var("PROXY_FETCHING").unwrap_or_default(),
            ),
            acp_pending_timeout_secs: env_u64("EVCLAW_ACP_PENDING_TIMEOUT_SECS", 1_800).max(60),
            acp_max_new_entries_per_cycle: env_u64("EVCLAW_ACP_MAX_NEW_ENTRIES_PER_CYCLE", 1).max(1)
                as usize,
            acp_entry_submit_delay_ms: env_u64("EVCLAW_ACP_ENTRY_SUBMIT_DELAY_MS", 5_000),
            acp_leverage: env_f64("EVCLAW_ACP_LEVERAGE", 10.0).max(1.0),
            acp_limit_offset_bps: env_f64("EVCLAW_ACP_LIMIT_OFFSET_BPS", 10.0).max(0.0),
            acp_sltp_resubmit_cooldown_secs: env_u64("EVCLAW_ACP_SLTP_RESUBMIT_COOLDOWN_SECS", 900)
                .max(30),
            acp_sltp_refresh_secs: env_u64("EVCLAW_ACP_SLTP_REFRESH_SECS", 21_600).max(300),
            symbols,
            hl_address,
            hl_agent_private_key,
            hl_vault_address: env_opt_string("EVCLAW_VAULT_ADDRESS"),
            dgclaw_api_key,
            dgclaw_trader_base_url: env_string(
                "EVCLAW_DGCLAW_TRADER_BASE_URL",
                "https://dgclaw-trader.virtuals.io",
            ),
            dgclaw_job_script_path: PathBuf::from(env_string(
                "EVCLAW_DGCLAW_JOB_SCRIPT_PATH",
                "/root/clawd/skills/EVClaw/scripts/dgclaw_acp_job.js",
            )),
        })
    }
}

fn resolve_env_path() -> Result<PathBuf> {
    if let Ok(path) = env::var("EVCLAW_ENV_PATH") {
        let candidate = PathBuf::from(path);
        if candidate.exists() {
            return Ok(candidate);
        }
    }

    let cwd = env::current_dir().context("failed to resolve current_dir")?;
    let candidates = [
        cwd.join(".env"),
        cwd.join("../.env"),
        PathBuf::from("/root/clawd/skills/EVClaw/acptrade/.env"),
    ];
    candidates
        .into_iter()
        .find(|path| path.exists())
        .ok_or_else(|| anyhow!("no EVCLAW .env file found"))
}

fn env_string(key: &str, default: &str) -> String {
    env::var(key)
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
        .unwrap_or_else(|| default.to_string())
}

fn env_opt_string(key: &str) -> Option<String> {
    env::var(key)
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
}

fn env_bool(key: &str, default: bool) -> bool {
    match env::var(key) {
        Ok(v) => matches!(
            v.trim().to_ascii_lowercase().as_str(),
            "1" | "true" | "yes" | "on"
        ),
        Err(_) => default,
    }
}

fn env_u64(key: &str, default: u64) -> u64 {
    env::var(key)
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .unwrap_or(default)
}

fn env_f64(key: &str, default: f64) -> f64 {
    env::var(key)
        .ok()
        .and_then(|v| v.trim().parse::<f64>().ok())
        .unwrap_or(default)
}

fn env_csv(key: &str, default: &str) -> Vec<String> {
    env_string(key, default)
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(ToOwned::to_owned)
        .collect()
}

fn normalize_symbol_set(values: Vec<String>) -> Option<HashSet<String>> {
    let set: HashSet<String> = values
        .into_iter()
        .map(|s| normalize_symbol(&s))
        .filter(|s| !s.is_empty())
        .collect();
    if set.is_empty() {
        None
    } else {
        Some(set)
    }
}

fn normalize_symbol(value: &str) -> String {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return String::new();
    }
    if let Some(rest) = trimmed.strip_prefix('k') {
        return format!("k{}", rest.to_ascii_uppercase());
    }
    if let Some(rest) = trimmed.strip_prefix('K') {
        let candidate = format!("k{}", rest.to_ascii_uppercase());
        if is_known_k_symbol(&candidate) {
            return candidate;
        }
    }
    trimmed.to_ascii_uppercase()
}

fn is_known_k_symbol(symbol: &str) -> bool {
    matches!(
        symbol,
        "kPEPE" | "kBONK" | "kSHIB" | "kFLOKI" | "kLUNC" | "kTOSHI" | "kXEC" | "kSATS"
    )
}
