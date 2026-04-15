pub mod acp_executor;
pub mod atr;
pub mod config;
pub mod hyperliquid;
pub mod journal;
pub mod labels;
pub mod runtime;
pub mod signals;
pub mod state;
pub mod types;

use std::time::{SystemTime, UNIX_EPOCH};

pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}
