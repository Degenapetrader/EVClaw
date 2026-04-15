use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::PathBuf;
use std::time::UNIX_EPOCH;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalletMeta {
    pub label: String,
    pub score: f64,
    pub efficiency_bps: f64,
    pub volume: f64,
    pub volume_weight: f64,
    pub exposure: f64,
    pub hft_blacklisted: bool,
    pub source: Option<String>,
}

impl WalletMeta {
    fn from_value(info: &Value) -> Self {
        Self {
            label: info
                .get("label")
                .and_then(Value::as_str)
                .unwrap_or("NO_DATA")
                .to_string(),
            score: value_as_f64(info.get("score")).unwrap_or(0.0),
            efficiency_bps: value_as_f64(info.get("efficiency_bps")).unwrap_or(0.0),
            volume: value_as_f64(info.get("volume")).unwrap_or(0.0),
            volume_weight: value_as_f64(info.get("volume_weight")).unwrap_or(1.0),
            exposure: value_as_f64(info.get("exposure")).unwrap_or(0.0),
            hft_blacklisted: info
                .get("hft_blacklisted")
                .and_then(Value::as_bool)
                .unwrap_or(false),
            source: info
                .get("source")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned),
        }
    }

    fn default_for(label: &str) -> Self {
        Self {
            label: label.to_string(),
            score: 0.0,
            efficiency_bps: 0.0,
            volume: 0.0,
            volume_weight: 1.0,
            exposure: 0.0,
            hft_blacklisted: false,
            source: None,
        }
    }
}

pub struct WalletLabelStore {
    path: PathBuf,
    labels: HashMap<String, WalletMeta>,
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
            self.labels
                .insert(wallet.to_ascii_lowercase(), WalletMeta::from_value(info));
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
            .filter_map(|(wallet, meta)| {
                if matches!(meta.label.as_str(), "INACTIVE" | "NO_DATA" | "MM_IGNORED") {
                    None
                } else {
                    Some(wallet.clone())
                }
            })
            .collect()
    }

    pub fn dead_cap_wallets(
        &self,
        inactive_min_volume_usd: f64,
        max_extra_wallets: usize,
    ) -> HashSet<String> {
        let mut wallets = self.tracked_wallets();
        if max_extra_wallets == 0 || inactive_min_volume_usd <= 0.0 {
            return wallets;
        }

        let mut extras = self
            .labels
            .iter()
            .filter(|(_, meta)| {
                meta.label == "INACTIVE"
                    && !meta.hft_blacklisted
                    && meta.volume >= inactive_min_volume_usd
            })
            .map(|(wallet, meta)| (wallet.clone(), meta.volume))
            .collect::<Vec<_>>();
        extras.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        extras.truncate(max_extra_wallets);
        wallets.extend(extras.into_iter().map(|(wallet, _)| wallet));
        wallets
    }

    pub fn label_for(&self, wallet: &str) -> &str {
        self.labels
            .get(&wallet.to_ascii_lowercase())
            .map(|s| s.label.as_str())
            .unwrap_or("NO_DATA")
    }

    pub fn meta_for(&self, wallet: &str) -> WalletMeta {
        self.labels
            .get(&wallet.to_ascii_lowercase())
            .cloned()
            .unwrap_or_else(|| WalletMeta::default_for("NO_DATA"))
    }
}

fn value_as_f64(value: Option<&Value>) -> Option<f64> {
    match value {
        Some(Value::Number(number)) => number.as_f64(),
        Some(Value::String(text)) => text.parse::<f64>().ok(),
        _ => None,
    }
}
