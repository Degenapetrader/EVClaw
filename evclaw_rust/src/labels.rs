use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::PathBuf;
use std::time::UNIX_EPOCH;

use anyhow::{Context, Result};
use serde_json::Value;

pub struct WalletLabelStore {
    path: PathBuf,
    labels: HashMap<String, String>,
    last_mtime: f64,
}

impl WalletLabelStore {
    pub fn new(path: PathBuf) -> Self {
        Self {
            path,
            labels: HashMap::new(),
            last_mtime: 0.0,
        }
    }

    pub fn reload(&mut self) -> Result<()> {
        let raw = fs::read_to_string(&self.path)
            .with_context(|| format!("failed to read wallet labels {}", self.path.display()))?;
        let json: Value = serde_json::from_str(&raw)
            .with_context(|| format!("failed to parse wallet labels {}", self.path.display()))?;
        let Some(obj) = json.as_object() else {
            self.labels.clear();
            return Ok(());
        };

        self.labels.clear();
        for (wallet, info) in obj {
            let label = info
                .get("label")
                .and_then(Value::as_str)
                .unwrap_or("NO_DATA");
            self.labels
                .insert(wallet.to_ascii_lowercase(), label.to_string());
        }
        self.last_mtime = self
            .path
            .metadata()
            .and_then(|meta| meta.modified())
            .ok()
            .and_then(|ts| ts.duration_since(UNIX_EPOCH).ok())
            .map(|ts| ts.as_secs_f64())
            .unwrap_or(0.0);
        Ok(())
    }

    pub fn reload_if_changed(&mut self) -> Result<bool> {
        let current_mtime = self
            .path
            .metadata()
            .and_then(|meta| meta.modified())
            .ok()
            .and_then(|ts| ts.duration_since(UNIX_EPOCH).ok())
            .map(|ts| ts.as_secs_f64())
            .unwrap_or(self.last_mtime);
        if self.labels.is_empty() || (current_mtime - self.last_mtime).abs() > f64::EPSILON {
            self.reload()?;
            return Ok(true);
        }
        Ok(false)
    }

    pub fn tracked_wallets(&self) -> HashSet<String> {
        self.labels
            .iter()
            .filter_map(|(wallet, label)| {
                if matches!(label.as_str(), "INACTIVE" | "NO_DATA" | "MM_IGNORED") {
                    None
                } else {
                    Some(wallet.clone())
                }
            })
            .collect()
    }

    pub fn label_for(&self, wallet: &str) -> &str {
        self.labels
            .get(&wallet.to_ascii_lowercase())
            .map(|s| s.as_str())
            .unwrap_or("NO_DATA")
    }
}
