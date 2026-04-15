use serde::{Deserialize, Serialize};

use crate::labels::WalletLabelStore;
use crate::types::{Direction, WalletPosition};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WhaleDetail {
    pub wallet: String,
    pub label: String,
    pub weight: f64,
    pub side: Direction,
    pub notional: f64,
    pub funding_since_open: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WhaleEvaluation {
    pub signal: Direction,
    pub score: f64,
    pub strength: f64,
    pub whale_count: usize,
    pub whales: Vec<WhaleDetail>,
    pub threshold: f64,
    pub reason: String,
}

pub struct WhaleSignal;

impl WhaleSignal {
    const MIN_NOTIONAL: f64 = 250_000.0;
    const MAX_FUNDING_SINCE_OPEN: f64 = 100.0;
    const OI_TIERS: &'static [(f64, f64)] = &[
        (1_000_000_000.0, 0.0025),
        (500_000_000.0, 0.005),
        (250_000_000.0, 0.015),
        (100_000_000.0, 0.03),
        (1_000_000.0, 0.05),
        (0.0, 0.05),
    ];

    pub fn evaluate(
        &self,
        _symbol: &str,
        positions: &[WalletPosition],
        oi_usd: f64,
        labels: &WalletLabelStore,
    ) -> WhaleEvaluation {
        let threshold = whale_threshold(oi_usd);
        let mut whales = Vec::new();
        let mut score = 0.0;

        for position in positions {
            if position.position_value < threshold {
                continue;
            }
            if position.position_value < Self::MIN_NOTIONAL {
                continue;
            }
            if position.funding_since_open > Self::MAX_FUNDING_SINCE_OPEN {
                continue;
            }

            let label = labels.label_for(&position.wallet);
            let weight = label_weight(label);
            let direction_sign = if position.side == Direction::Long {
                1.0
            } else {
                -1.0
            };
            score += weight * direction_sign;

            whales.push(WhaleDetail {
                wallet: position.wallet.clone(),
                label: label.to_string(),
                weight,
                side: position.side,
                notional: position.position_value,
                funding_since_open: position.funding_since_open,
            });
        }

        let signal = if score > 0.0 {
            Direction::Long
        } else if score < 0.0 {
            Direction::Short
        } else {
            Direction::Neutral
        };
        let strength = (score.abs() / 10.0).clamp(0.0, 1.0);

        let reason = if whales.is_empty() {
            "no new whales".to_string()
        } else {
            let mut whale_summary = whales
                .iter()
                .take(3)
                .map(|whale| {
                    format!(
                        "{}:{}:${:.1}M",
                        whale.label,
                        whale.side.as_str().chars().next().unwrap_or('N'),
                        whale.notional / 1_000_000.0
                    )
                })
                .collect::<Vec<_>>()
                .join(", ");
            if whales.len() > 3 {
                whale_summary.push_str(&format!(" +{} more", whales.len() - 3));
            }
            format!(
                "{} whales, score={:+.1} [{}]",
                whales.len(),
                score,
                whale_summary
            )
        };

        WhaleEvaluation {
            signal,
            score,
            strength,
            whale_count: whales.len(),
            whales,
            threshold,
            reason,
        }
    }
}

fn whale_threshold(oi_usd: f64) -> f64 {
    for (oi_min, pct) in WhaleSignal::OI_TIERS {
        if oi_usd >= *oi_min {
            let threshold = oi_usd * pct;
            return threshold.max(WhaleSignal::MIN_NOTIONAL);
        }
    }
    WhaleSignal::MIN_NOTIONAL
}

fn label_weight(label: &str) -> f64 {
    match label {
        "LEGEND" => 5.0,
        "ELITE" => 4.0,
        "SKILLED" => 3.0,
        "SOLID" => 2.0,
        "GRINDER" => 1.0,
        "SURVIVOR" => 0.5,
        "WEAK" => -1.0,
        "BLEEDING" => -2.0,
        "LOSER" => -3.0,
        "REKT" => -4.0,
        "WRECKED" => -5.0,
        _ => 0.0,
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::PathBuf;

    use super::WhaleSignal;
    use crate::labels::WalletLabelStore;
    use crate::types::{Direction, WalletPosition};

    #[test]
    fn whale_copies_good_wallet_longs() {
        let path = PathBuf::from("/tmp/evclaw_whale_test.json");
        fs::write(
            &path,
            r#"{"0x1":{"label":"LEGEND"},"0x2":{"label":"REKT"}}"#,
        )
        .unwrap();
        let mut labels = WalletLabelStore::new(path.clone());
        labels.reload().unwrap();

        let signal = WhaleSignal;
        let positions = vec![WalletPosition {
            wallet: "0x1".to_string(),
            symbol: "ETH".to_string(),
            side: Direction::Long,
            position_value: 4_000_000.0,
            entry_price: 0.0,
            unrealized_pnl: 0.0,
            margin_used: 0.0,
            leverage: 1.0,
            funding_since_open: 0.0,
        }];
        let evaluation = signal.evaluate("ETH", &positions, 100_000_000.0, &labels);
        assert_eq!(evaluation.signal, Direction::Long);
        let _ = fs::remove_file(path);
    }

    #[test]
    fn whale_counts_zero_weight_wallets() {
        let path = PathBuf::from("/tmp/evclaw_whale_zero_test.json");
        fs::write(&path, r#"{"0x1":{"label":"NO_DATA"}}"#).unwrap();
        let mut labels = WalletLabelStore::new(path.clone());
        labels.reload().unwrap();

        let signal = WhaleSignal;
        let positions = vec![WalletPosition {
            wallet: "0x1".to_string(),
            symbol: "ETH".to_string(),
            side: Direction::Long,
            position_value: 6_000_000.0,
            entry_price: 0.0,
            unrealized_pnl: 0.0,
            margin_used: 0.0,
            leverage: 1.0,
            funding_since_open: 0.0,
        }];
        let evaluation = signal.evaluate("ETH", &positions, 100_000_000.0, &labels);
        assert_eq!(evaluation.signal, Direction::Neutral);
        assert_eq!(evaluation.whale_count, 1);
        let _ = fs::remove_file(path);
    }
}
