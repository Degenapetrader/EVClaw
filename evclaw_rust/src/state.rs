use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use log::warn;
use serde::{Deserialize, Serialize};

use crate::types::{Direction, ReentryBlock, TrackedPosition};

#[derive(Debug, Default, Serialize, Deserialize)]
struct PersistedState {
    positions: Vec<TrackedPosition>,
    reentry_blocks: Vec<ReentryBlock>,
}

#[derive(Debug, Default)]
pub struct RuntimeState {
    pub positions: HashMap<String, TrackedPosition>,
    pub reentry_blocks: HashMap<String, Direction>,
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
