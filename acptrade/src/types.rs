use std::collections::HashMap;
use std::fmt;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Ord, PartialOrd)]
pub enum Direction {
    Long,
    Short,
    Neutral,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TradeStreakOutcome {
    Win,
    Loss,
    Neutral,
}

impl TradeStreakOutcome {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Win => "WIN",
            Self::Loss => "LOSS",
            Self::Neutral => "NEUTRAL",
        }
    }

    pub fn from_db_str(value: &str) -> Self {
        match value.trim().to_ascii_uppercase().as_str() {
            "WIN" => Self::Win,
            "LOSS" => Self::Loss,
            _ => Self::Neutral,
        }
    }
}

impl Direction {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Long => "LONG",
            Self::Short => "SHORT",
            Self::Neutral => "NEUTRAL",
        }
    }

    pub fn sign(self) -> f64 {
        match self {
            Self::Long => 1.0,
            Self::Short => -1.0,
            Self::Neutral => 0.0,
        }
    }

    pub fn from_signed_qty(qty: f64) -> Self {
        if qty > 0.0 {
            Self::Long
        } else if qty < 0.0 {
            Self::Short
        } else {
            Self::Neutral
        }
    }

    pub fn from_score(score: f64) -> Self {
        Self::from_signed_qty(score)
    }

    pub fn from_db_str(value: &str) -> Self {
        match value.trim().to_ascii_uppercase().as_str() {
            "LONG" => Self::Long,
            "SHORT" => Self::Short,
            _ => Self::Neutral,
        }
    }

    pub fn opposes(self, other: Self) -> bool {
        matches!(
            (self, other),
            (Self::Long, Self::Short) | (Self::Short, Self::Long)
        )
    }

    pub fn is_actionable(self) -> bool {
        self != Self::Neutral
    }
}

impl fmt::Display for Direction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalletPosition {
    pub wallet: String,
    pub symbol: String,
    pub side: Direction,
    pub position_value: f64,
    pub entry_price: f64,
    pub unrealized_pnl: f64,
    pub margin_used: f64,
    pub leverage: f64,
    pub funding_since_open: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccountSummary {
    pub wallet: String,
    pub account_value: f64,
    pub margin_used: f64,
    pub available_margin: f64,
    pub is_locked: bool,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct MarketMeta {
    pub exchange_symbol: String,
    pub mark_px: f64,
    pub oi_usd: f64,
    pub sz_decimals: u32,
    pub price_decimals: u32,
    pub min_size: f64,
    pub tick_size: f64,
    pub max_leverage: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccountDataSnapshot {
    pub market_meta: HashMap<String, MarketMeta>,
    pub positions_by_symbol: HashMap<String, Vec<WalletPosition>>,
    pub account_summaries: HashMap<String, AccountSummary>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SignalComponent {
    pub direction: Direction,
    pub strength: f64,
    pub reason: String,
}

impl SignalComponent {
    pub fn neutral(reason: impl Into<String>) -> Self {
        Self {
            direction: Direction::Neutral,
            strength: 0.0,
            reason: reason.into(),
        }
    }

    pub fn signed_strength(&self, weight: f64) -> f64 {
        self.direction.sign() * self.strength * weight
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CombinedSignal {
    pub symbol: String,
    pub exchange_symbol: String,
    pub dead_cap: SignalComponent,
    pub whale: SignalComponent,
    pub net_score: f64,
    pub target_direction: Direction,
    pub raw_notional_usd: f64,
    pub order_notional_usd: f64,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OiBucketPolicy {
    #[serde(default = "default_bucket_label")]
    pub bucket_label: String,
    #[serde(default)]
    pub oi_usd_at_entry: f64,
    #[serde(default = "default_size_multiplier")]
    pub size_multiplier: f64,
    #[serde(default = "default_sl_atr_multiplier")]
    pub sl_atr_multiplier: f64,
    #[serde(default = "default_tp_atr_multiplier")]
    pub tp_atr_multiplier: f64,
    #[serde(default = "default_min_hold_hours")]
    pub min_hold_hours: f64,
}

impl Default for OiBucketPolicy {
    fn default() -> Self {
        Self {
            bucket_label: default_bucket_label(),
            oi_usd_at_entry: 0.0,
            size_multiplier: default_size_multiplier(),
            sl_atr_multiplier: default_sl_atr_multiplier(),
            tp_atr_multiplier: default_tp_atr_multiplier(),
            min_hold_hours: default_min_hold_hours(),
        }
    }
}

impl OiBucketPolicy {
    pub fn is_initialized(&self) -> bool {
        !self.bucket_label.is_empty()
            && self.size_multiplier > 0.0
            && self.sl_atr_multiplier > 0.0
            && self.tp_atr_multiplier > 0.0
            && self.min_hold_hours > 0.0
    }
}

fn default_bucket_label() -> String {
    "legacy".to_string()
}

fn default_size_multiplier() -> f64 {
    1.0
}

fn default_sl_atr_multiplier() -> f64 {
    1.5
}

fn default_tp_atr_multiplier() -> f64 {
    1.0
}

fn default_min_hold_hours() -> f64 {
    6.0
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeadCapSnapshot {
    pub symbol: String,
    pub signal: Direction,
    pub strength: f64,
    pub threshold: f64,
    pub locked_long_pct: f64,
    pub locked_short_pct: f64,
    pub effective_long_pct: f64,
    pub effective_short_pct: f64,
    pub bad_long_pct: f64,
    pub bad_short_pct: f64,
    pub smart_long_pct: f64,
    pub smart_short_pct: f64,
    pub observed_pct: f64,
    pub locked_wallet_count: usize,
    pub dominant_top_share: f64,
    pub persistence_streak: u32,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReentryBlock {
    pub symbol: String,
    pub blocked_direction: Direction,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SymbolCooldown {
    pub symbol: String,
    pub blocked_until_ms: u64,
    #[serde(default)]
    pub reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LivePosition {
    pub symbol: String,
    pub exchange_symbol: String,
    pub qty: f64,
    pub direction: Direction,
    pub entry_price: f64,
    pub unrealized_pnl: f64,
    pub notional_value: f64,
}

impl LivePosition {
    pub fn is_flat(&self) -> bool {
        !self.direction.is_actionable() || self.qty <= 0.0
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OpenOrder {
    pub order_id: String,
    pub order_type: String,
    pub reduce_only: bool,
    pub price: f64,
    pub size: f64,
    pub symbol: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderResult {
    pub success: bool,
    pub order_id: Option<String>,
    pub filled_size: f64,
    pub filled_price: f64,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserFill {
    pub fill_id: String,
    pub symbol: String,
    pub exchange_symbol: String,
    pub side: Direction,
    pub dir_text: Option<String>,
    pub price: f64,
    pub size: f64,
    pub ts_ms: u64,
    pub oid: Option<u64>,
    pub tid: Option<u64>,
    pub start_position: Option<f64>,
    pub closed_pnl: Option<f64>,
    pub fee_usd: Option<f64>,
    pub builder_fee_usd: Option<f64>,
    pub crossed: bool,
    pub twap: bool,
}

#[derive(Debug, Clone)]
pub struct AtrData {
    pub atr: f64,
    pub atr_pct: f64,
    pub volatility_tier: String,
    pub min_mult: f64,
    pub max_mult: f64,
}

#[derive(Debug, Clone)]
pub struct AtrCheckpointData {
    pub atr: f64,
    pub high_4h_ago: f64,
    pub low_4h_ago: f64,
    pub high_8h_ago: f64,
    pub low_8h_ago: f64,
    pub high_12h_ago: f64,
    pub low_12h_ago: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrackedPosition {
    pub symbol: String,
    pub exchange_symbol: String,
    pub direction: Direction,
    pub qty: f64,
    pub entry_price: f64,
    pub entry_notional_usd: f64,
    pub stop_price: f64,
    pub take_profit_price: f64,
    pub opened_at_ms: u64,
    pub entry_signal: CombinedSignal,
    #[serde(default)]
    pub bucket_policy: OiBucketPolicy,
    #[serde(default)]
    pub trade_id: Option<i64>,
    #[serde(default = "default_entry_source")]
    pub entry_source: String,
    #[serde(default)]
    pub acp_sltp_last_submit_ms: u64,
    #[serde(default)]
    pub acp_sltp_last_stop_price: f64,
    #[serde(default)]
    pub acp_sltp_last_take_profit_price: f64,
    #[serde(default)]
    pub pressure_exit_last_check_ms: u64,
}

fn default_entry_source() -> String {
    "signal".to_string()
}
