use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use log::warn;
use serde::{Deserialize, Serialize};

use crate::types::{DeadCapSnapshot, Direction, ReentryBlock, SymbolCooldown, TrackedPosition};

#[derive(Debug, Default, Serialize, Deserialize)]
struct PersistedState {
    #[serde(default)]
    positions: Vec<TrackedPosition>,
    #[serde(default)]
    reentry_blocks: Vec<ReentryBlock>,
    #[serde(default)]
    dead_cap_snapshots: Vec<DeadCapSnapshot>,
    #[serde(default)]
    symbol_cooldowns: Vec<SymbolCooldown>,
    #[serde(default)]
    loss_streaks: HashMap<String, u32>,
    #[serde(default)]
    loss_streak_start_after_trade_id: i64,
}

#[derive(Debug, Default)]
pub struct RuntimeState {
    pub positions: HashMap<String, TrackedPosition>,
    pub reentry_blocks: HashMap<String, Direction>,
    pub dead_cap_snapshots: HashMap<String, DeadCapSnapshot>,
    pub symbol_cooldowns: HashMap<String, SymbolCooldown>,
    pub loss_streaks: HashMap<String, u32>,
    pub loss_streak_start_after_trade_id: i64,
}

pub struct StateStore {
    path: PathBuf,
}

impl StateStore {
    pub fn new(path: PathBuf) -> Self {
        Self { path }
    }

    pub fn load(&self) -> Result<RuntimeState> {
        if !self.path.exists() {
            return Ok(RuntimeState::default());
        }
        let raw = fs::read_to_string(&self.path)
            .with_context(|| format!("failed to read state file {}", self.path.display()))?;
        let state: PersistedState = match serde_json::from_str(&raw) {
            Ok(state) => state,
            Err(err) => {
                warn!(
                    "state parse failed for {}: {}; starting fresh",
                    self.path.display(),
                    err
                );
                return Ok(RuntimeState::default());
            }
        };

        let mut out = RuntimeState::default();
        for position in state.positions {
            out.positions.insert(position.symbol.clone(), position);
        }
        for block in state.reentry_blocks {
            out.reentry_blocks
                .insert(block.symbol, block.blocked_direction);
        }
        for snapshot in state.dead_cap_snapshots {
            out.dead_cap_snapshots
                .insert(snapshot.symbol.clone(), snapshot);
        }
        for cooldown in state.symbol_cooldowns {
            out.symbol_cooldowns
                .insert(cooldown.symbol.clone(), cooldown);
        }
        out.loss_streaks = state.loss_streaks;
        out.loss_streak_start_after_trade_id = state.loss_streak_start_after_trade_id;
        Ok(out)
    }

    pub fn save(&self, state: &RuntimeState) -> Result<()> {
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("failed to create state dir {}", parent.display()))?;
        }

        let persisted = PersistedState {
            positions: state.positions.values().cloned().collect(),
            reentry_blocks: state
                .reentry_blocks
                .iter()
                .map(|(symbol, blocked_direction)| ReentryBlock {
                    symbol: symbol.clone(),
                    blocked_direction: *blocked_direction,
                })
                .collect(),
            dead_cap_snapshots: state.dead_cap_snapshots.values().cloned().collect(),
            symbol_cooldowns: state.symbol_cooldowns.values().cloned().collect(),
            loss_streaks: state.loss_streaks.clone(),
            loss_streak_start_after_trade_id: state.loss_streak_start_after_trade_id,
        };
        let raw = serde_json::to_string_pretty(&persisted)?;
        write_atomic(&self.path, raw.as_bytes())
    }
}

fn write_atomic(path: &Path, contents: &[u8]) -> Result<()> {
    let tmp_path = path.with_extension("tmp");
    fs::write(&tmp_path, contents)
        .with_context(|| format!("failed to write temp state file {}", tmp_path.display()))?;
    fs::rename(&tmp_path, path)
        .with_context(|| format!("failed to replace state file {}", path.display()))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::PersistedState;

    #[test]
    fn old_state_without_dead_cap_snapshots_loads_cleanly() {
        let raw = r#"
        {
          "positions": [],
          "reentry_blocks": []
        }
        "#;
        let parsed: PersistedState = serde_json::from_str(raw).expect("old state should parse");
        assert!(parsed.positions.is_empty());
        assert!(parsed.reentry_blocks.is_empty());
        assert!(parsed.dead_cap_snapshots.is_empty());
        assert!(parsed.symbol_cooldowns.is_empty());
        assert!(parsed.loss_streaks.is_empty());
        assert_eq!(parsed.loss_streak_start_after_trade_id, 0);
    }
}
