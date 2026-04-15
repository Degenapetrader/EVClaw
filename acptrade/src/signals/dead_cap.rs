use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::labels::{WalletLabelStore, WalletMeta};
use crate::types::{AccountSummary, Direction, WalletPosition};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeadCapEvaluation {
    pub signal: Direction,
    pub strength: f64,
    pub locked_long_pct: f64,
    pub locked_short_pct: f64,
    pub effective_long_pct: f64,
    pub effective_short_pct: f64,
    pub bad_long_pct: f64,
    pub bad_short_pct: f64,
    pub smart_long_pct: f64,
    pub smart_short_pct: f64,
    pub observed_pct: f64,
    pub locked_long: f64,
    pub locked_short: f64,
    pub effective_long: f64,
    pub effective_short: f64,
    pub locked_long_count: usize,
    pub locked_short_count: usize,
    pub threshold: f64,
    pub locked_wallet_count: usize,
    pub dominant_top_share: f64,
    pub persistence_streak: u32,
    pub reason: String,
}

#[derive(Debug, Clone, Default)]
struct DeadCapState {
    last_signal: Option<Direction>,
    last_effective_long_pct: f64,
    last_effective_short_pct: f64,
    streak: u32,
}

pub struct DeadCapitalSignal {
    history: HashMap<String, DeadCapState>,
}

impl DeadCapitalSignal {
    const MIN_OI: f64 = 10_000_000.0;
    const MIN_TRIGGER_STRENGTH: f64 = 0.75;
    const THRESHOLD_HARDENING_MULTIPLIER: f64 = 2.5;
    const THRESHOLDS: &'static [(f64, f64)] = &[
        (1_000_000_000.0, 2.5),
        (500_000_000.0, 4.0),
        (100_000_000.0, 6.0),
        (10_000_000.0, 6.0),
    ];

    pub fn evaluate(
        &mut self,
        symbol: &str,
        positions: &[WalletPosition],
        account_summaries: &HashMap<String, AccountSummary>,
        oi_usd: f64,
        labels: &WalletLabelStore,
    ) -> DeadCapEvaluation {
        if oi_usd < Self::MIN_OI {
            self.reset(symbol);
            return DeadCapEvaluation {
                signal: Direction::Neutral,
                strength: 0.0,
                locked_long_pct: 0.0,
                locked_short_pct: 0.0,
                effective_long_pct: 0.0,
                effective_short_pct: 0.0,
                bad_long_pct: 0.0,
                bad_short_pct: 0.0,
                smart_long_pct: 0.0,
                smart_short_pct: 0.0,
                observed_pct: 0.0,
                locked_long: 0.0,
                locked_short: 0.0,
                effective_long: 0.0,
                effective_short: 0.0,
                locked_long_count: 0,
                locked_short_count: 0,
                threshold: 0.0,
                locked_wallet_count: 0,
                dominant_top_share: 0.0,
                persistence_streak: 0,
                reason: format!("OI < $10M (${:.2}M)", oi_usd / 1_000_000.0),
            };
        }

        let mut observed_notional = 0.0;
        let mut locked_wallet_count = 0usize;
        let mut locked_long = 0.0;
        let mut locked_short = 0.0;
        let mut locked_long_count = 0usize;
        let mut locked_short_count = 0usize;

        let mut bad_long = 0.0;
        let mut bad_short = 0.0;
        let mut smart_long = 0.0;
        let mut smart_short = 0.0;
        let mut neutral_long = 0.0;
        let mut neutral_short = 0.0;
        let mut weighted_long_contribs = Vec::new();
        let mut weighted_short_contribs = Vec::new();

        for position in positions {
            observed_notional += position.position_value.abs();
            let Some(account) = account_summaries.get(&position.wallet) else {
                continue;
            };
            if !account.is_locked {
                continue;
            }

            locked_wallet_count += 1;
            let raw_notional = position.position_value.abs();
            match position.side {
                Direction::Long => {
                    locked_long += raw_notional;
                    locked_long_count += 1;
                }
                Direction::Short => {
                    locked_short += raw_notional;
                    locked_short_count += 1;
                }
                Direction::Neutral => continue,
            }

            let meta = labels.meta_for(&position.wallet);
            let volume_weight = meta.volume_weight.clamp(0.4, 1.0);
            let stress = stress_multiplier(position);
            let weighted = raw_notional * volume_weight * stress;
            let bucket_weight = bucket_weight(&meta);
            let contribution = weighted * bucket_weight.abs();

            match bucket_for(&meta) {
                WalletBucket::Bad => match position.side {
                    Direction::Long => bad_long += contribution,
                    Direction::Short => bad_short += contribution,
                    Direction::Neutral => {}
                },
                WalletBucket::Smart => match position.side {
                    Direction::Long => smart_long += contribution,
                    Direction::Short => smart_short += contribution,
                    Direction::Neutral => {}
                },
                WalletBucket::Neutral => match position.side {
                    Direction::Long => neutral_long += contribution,
                    Direction::Short => neutral_short += contribution,
                    Direction::Neutral => {}
                },
                WalletBucket::Ignored => continue,
            }

            match position.side {
                Direction::Long => weighted_long_contribs.push(contribution),
                Direction::Short => weighted_short_contribs.push(contribution),
                Direction::Neutral => {}
            }
        }

        let threshold = threshold_for_oi(oi_usd);
        let observed_pct = pct(observed_notional, oi_usd);
        let coverage_gate = coverage_target_pct(oi_usd);
        if observed_pct < coverage_gate * 0.5 {
            self.reset(symbol);
            return DeadCapEvaluation {
                signal: Direction::Neutral,
                strength: 0.0,
                locked_long_pct: round_two(pct(locked_long, oi_usd)),
                locked_short_pct: round_two(pct(locked_short, oi_usd)),
                effective_long_pct: 0.0,
                effective_short_pct: 0.0,
                bad_long_pct: round_two(pct(bad_long, oi_usd)),
                bad_short_pct: round_two(pct(bad_short, oi_usd)),
                smart_long_pct: round_two(pct(smart_long, oi_usd)),
                smart_short_pct: round_two(pct(smart_short, oi_usd)),
                observed_pct: round_two(observed_pct),
                locked_long,
                locked_short,
                effective_long: 0.0,
                effective_short: 0.0,
                locked_long_count,
                locked_short_count,
                threshold,
                locked_wallet_count,
                dominant_top_share: 0.0,
                persistence_streak: 0,
                reason: format!(
                    "coverage low obs:{:.1}% < gate:{:.1}%",
                    observed_pct,
                    coverage_gate * 0.5
                ),
            };
        }

        let coverage_factor = (observed_pct / coverage_gate).clamp(0.5, 1.0);
        let effective_long = (bad_long + neutral_long * 0.35 - smart_long * 0.5).max(0.0);
        let effective_short = (bad_short + neutral_short * 0.35 - smart_short * 0.5).max(0.0);
        let effective_long_pct = pct(effective_long, oi_usd) * coverage_factor;
        let effective_short_pct = pct(effective_short, oi_usd) * coverage_factor;

        let dominant_side = if effective_long_pct > effective_short_pct {
            Direction::Short
        } else if effective_short_pct > effective_long_pct {
            Direction::Long
        } else {
            Direction::Neutral
        };
        let dominant_pct = match dominant_side {
            Direction::Short => effective_long_pct,
            Direction::Long => effective_short_pct,
            Direction::Neutral => 0.0,
        };
        let dominant_count = match dominant_side {
            Direction::Short => locked_long_count,
            Direction::Long => locked_short_count,
            Direction::Neutral => 0,
        };
        let dominant_top_share = match dominant_side {
            Direction::Short => top_share(&weighted_long_contribs),
            Direction::Long => top_share(&weighted_short_contribs),
            Direction::Neutral => 0.0,
        };

        if dominant_side.is_actionable() && dominant_count < min_breadth_wallets(oi_usd) {
            self.reset(symbol);
            return self.neutral_evaluation(
                oi_usd,
                threshold,
                locked_long,
                locked_short,
                bad_long,
                bad_short,
                smart_long,
                smart_short,
                effective_long,
                effective_short,
                locked_long_count,
                locked_short_count,
                locked_wallet_count,
                observed_pct,
                dominant_top_share,
                format!(
                    "breadth low count:{} < min:{}",
                    dominant_count,
                    min_breadth_wallets(oi_usd)
                ),
            );
        }

        let concentration_factor = concentration_factor(dominant_top_share);
        let (streak, persistence_factor) = self.persistence_adjustment(
            symbol,
            dominant_side,
            effective_long_pct,
            effective_short_pct,
        );
        let base_strength = signal_strength(dominant_pct, threshold);
        let final_strength =
            (base_strength * concentration_factor * persistence_factor).clamp(0.0, 1.0);

        if dominant_side.is_actionable()
            && dominant_pct >= threshold
            && final_strength >= Self::MIN_TRIGGER_STRENGTH
        {
            DeadCapEvaluation {
                signal: dominant_side,
                strength: final_strength,
                locked_long_pct: round_two(pct(locked_long, oi_usd)),
                locked_short_pct: round_two(pct(locked_short, oi_usd)),
                effective_long_pct: round_two(effective_long_pct),
                effective_short_pct: round_two(effective_short_pct),
                bad_long_pct: round_two(pct(bad_long, oi_usd)),
                bad_short_pct: round_two(pct(bad_short, oi_usd)),
                smart_long_pct: round_two(pct(smart_long, oi_usd)),
                smart_short_pct: round_two(pct(smart_short, oi_usd)),
                observed_pct: round_two(observed_pct),
                locked_long,
                locked_short,
                effective_long,
                effective_short,
                locked_long_count,
                locked_short_count,
                threshold,
                locked_wallet_count,
                dominant_top_share: round_two(dominant_top_share * 100.0),
                persistence_streak: streak,
                reason: format!(
                    "effL:{:.1}% effS:{:.1}% badL:{:.1}% badS:{:.1}% smartL:{:.1}% smartS:{:.1}% cov:{:.1}% top:{:.0}% streak:{} -> {}",
                    effective_long_pct,
                    effective_short_pct,
                    pct(bad_long, oi_usd),
                    pct(bad_short, oi_usd),
                    pct(smart_long, oi_usd),
                    pct(smart_short, oi_usd),
                    observed_pct,
                    dominant_top_share * 100.0,
                    streak,
                    dominant_side
                ),
            }
        } else {
            self.neutral_evaluation(
                oi_usd,
                threshold,
                locked_long,
                locked_short,
                bad_long,
                bad_short,
                smart_long,
                smart_short,
                effective_long,
                effective_short,
                locked_long_count,
                locked_short_count,
                locked_wallet_count,
                observed_pct,
                dominant_top_share,
                format!(
                    "effL:{:.1}% effS:{:.1}% < thr:{:.1}% or str:{:.2} < {:.2}",
                    effective_long_pct,
                    effective_short_pct,
                    threshold,
                    final_strength,
                    Self::MIN_TRIGGER_STRENGTH
                ),
            )
        }
    }

    fn persistence_adjustment(
        &mut self,
        symbol: &str,
        signal: Direction,
        effective_long_pct: f64,
        effective_short_pct: f64,
    ) -> (u32, f64) {
        let state = self.history.entry(symbol.to_string()).or_default();
        let current_dom_pct = match signal {
            Direction::Short => effective_long_pct,
            Direction::Long => effective_short_pct,
            Direction::Neutral => 0.0,
        };
        let prev_dom_pct = match signal {
            Direction::Short => state.last_effective_long_pct,
            Direction::Long => state.last_effective_short_pct,
            Direction::Neutral => 0.0,
        };

        let streak = if signal.is_actionable() && state.last_signal == Some(signal) {
            state.streak.saturating_add(1)
        } else if signal.is_actionable() {
            1
        } else {
            0
        };
        let acceleration = current_dom_pct - prev_dom_pct;
        let persistence_factor =
            (0.85 + (streak.min(3) as f64 * 0.1) + if acceleration > 0.5 { 0.1 } else { 0.0 })
                .clamp(0.6, 1.15);

        state.last_signal = signal.is_actionable().then_some(signal);
        state.last_effective_long_pct = effective_long_pct;
        state.last_effective_short_pct = effective_short_pct;
        state.streak = streak;
        (streak, persistence_factor)
    }

    fn neutral_evaluation(
        &self,
        oi_usd: f64,
        threshold: f64,
        locked_long: f64,
        locked_short: f64,
        bad_long: f64,
        bad_short: f64,
        smart_long: f64,
        smart_short: f64,
        effective_long: f64,
        effective_short: f64,
        locked_long_count: usize,
        locked_short_count: usize,
        locked_wallet_count: usize,
        observed_pct: f64,
        dominant_top_share: f64,
        reason: String,
    ) -> DeadCapEvaluation {
        DeadCapEvaluation {
            signal: Direction::Neutral,
            strength: 0.0,
            locked_long_pct: round_two(pct(locked_long, oi_usd)),
            locked_short_pct: round_two(pct(locked_short, oi_usd)),
            effective_long_pct: round_two(pct(effective_long, oi_usd)),
            effective_short_pct: round_two(pct(effective_short, oi_usd)),
            bad_long_pct: round_two(pct(bad_long, oi_usd)),
            bad_short_pct: round_two(pct(bad_short, oi_usd)),
            smart_long_pct: round_two(pct(smart_long, oi_usd)),
            smart_short_pct: round_two(pct(smart_short, oi_usd)),
            observed_pct: round_two(observed_pct),
            locked_long,
            locked_short,
            effective_long,
            effective_short,
            locked_long_count,
            locked_short_count,
            threshold,
            locked_wallet_count,
            dominant_top_share: round_two(dominant_top_share * 100.0),
            persistence_streak: 0,
            reason,
        }
    }

    fn reset(&mut self, symbol: &str) {
        self.history.remove(symbol);
    }
}

impl Default for DeadCapitalSignal {
    fn default() -> Self {
        Self {
            history: HashMap::new(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WalletBucket {
    Bad,
    Smart,
    Neutral,
    Ignored,
}

fn bucket_for(meta: &WalletMeta) -> WalletBucket {
    if meta.hft_blacklisted || meta.label == "HFT_BOT" || meta.label == "MM_IGNORED" {
        return WalletBucket::Ignored;
    }
    match meta.label.as_str() {
        "WRECKED" | "REKT" | "LOSER" | "BLEEDING" | "WEAK" => WalletBucket::Bad,
        "LEGEND" | "ELITE" | "SKILLED" | "SOLID" | "GRINDER" | "SURVIVOR" => WalletBucket::Smart,
        _ => WalletBucket::Neutral,
    }
}

fn bucket_weight(meta: &WalletMeta) -> f64 {
    match meta.label.as_str() {
        "WRECKED" => 2.0,
        "REKT" => 1.8,
        "LOSER" => 1.6,
        "BLEEDING" => 1.4,
        "WEAK" => 1.2,
        "SURVIVOR" => 0.4,
        "GRINDER" => 0.55,
        "SOLID" => 0.7,
        "SKILLED" => 0.9,
        "ELITE" => 1.05,
        "LEGEND" => 1.2,
        _ if meta.score < 0.0 => 1.0,
        _ if meta.score > 0.0 => 0.7,
        _ => 0.6,
    }
}

fn stress_multiplier(position: &WalletPosition) -> f64 {
    let pnl_pct = if position.position_value > 0.0 {
        position.unrealized_pnl / position.position_value
    } else {
        0.0
    };
    let roe = if position.margin_used > 0.0 {
        position.unrealized_pnl / position.margin_used
    } else {
        0.0
    };

    let mut mult: f64 = 1.0;
    if pnl_pct < -0.03 {
        mult += 0.25;
    } else if pnl_pct < -0.01 {
        mult += 0.15;
    } else if pnl_pct < 0.0 {
        mult += 0.05;
    }

    if roe < -0.5 {
        mult += 0.25;
    } else if roe < -0.25 {
        mult += 0.15;
    } else if roe < 0.0 {
        mult += 0.05;
    }

    if position.funding_since_open > 100.0 {
        mult += 0.15;
    } else if position.funding_since_open > 25.0 {
        mult += 0.05;
    }

    mult.clamp(0.75, 1.6)
}

fn threshold_for_oi(oi_usd: f64) -> f64 {
    for (oi_min, threshold) in DeadCapitalSignal::THRESHOLDS {
        if oi_usd >= *oi_min {
            return *threshold * DeadCapitalSignal::THRESHOLD_HARDENING_MULTIPLIER;
        }
    }
    6.0 * DeadCapitalSignal::THRESHOLD_HARDENING_MULTIPLIER
}

fn coverage_target_pct(oi_usd: f64) -> f64 {
    if oi_usd >= 1_000_000_000.0 {
        6.0
    } else if oi_usd >= 500_000_000.0 {
        8.0
    } else {
        10.0
    }
}

fn min_breadth_wallets(oi_usd: f64) -> usize {
    if oi_usd >= 1_000_000_000.0 {
        3
    } else {
        2
    }
}

fn concentration_factor(top_share: f64) -> f64 {
    if top_share >= 0.70 {
        0.6
    } else if top_share >= 0.55 {
        0.75
    } else if top_share >= 0.40 {
        0.9
    } else {
        1.0
    }
}

fn top_share(values: &[f64]) -> f64 {
    let total: f64 = values.iter().sum();
    if total <= 0.0 {
        return 0.0;
    }
    let top = values
        .iter()
        .copied()
        .fold(0.0_f64, |acc, value| acc.max(value));
    (top / total).clamp(0.0, 1.0)
}

fn pct(value: f64, denom: f64) -> f64 {
    if denom > 0.0 {
        (value / denom) * 100.0
    } else {
        0.0
    }
}

fn round_two(value: f64) -> f64 {
    (value * 100.0).round() / 100.0
}

fn signal_strength(locked_pct: f64, threshold: f64) -> f64 {
    if threshold <= 0.0 {
        return 0.0;
    }
    (locked_pct / (threshold * 2.0)).clamp(0.0, 1.0)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::fs;
    use std::path::PathBuf;

    use super::DeadCapitalSignal;
    use crate::labels::WalletLabelStore;
    use crate::types::{AccountSummary, Direction, WalletPosition};

    #[test]
    fn dead_cap_fades_bad_locked_longs() {
        let wallet_file = PathBuf::from("/tmp/evclaw_dead_cap_wallets.json");
        fs::write(
            &wallet_file,
            r#"{"0x1":{"label":"REKT","score":-80,"volume_weight":1.0},"0x2":{"label":"LOSER","score":-45,"volume_weight":1.0}}"#,
        )
        .unwrap();
        let mut labels = WalletLabelStore::new(wallet_file.clone());
        labels.reload().unwrap();

        let mut signal = DeadCapitalSignal::default();
        let positions = vec![
            WalletPosition {
                wallet: "0x1".to_string(),
                symbol: "BTC".to_string(),
                side: Direction::Long,
                position_value: 12_000_000.0,
                entry_price: 100_000.0,
                unrealized_pnl: -1_000_000.0,
                margin_used: 4_000_000.0,
                leverage: 3.0,
                funding_since_open: 50.0,
            },
            WalletPosition {
                wallet: "0x2".to_string(),
                symbol: "BTC".to_string(),
                side: Direction::Long,
                position_value: 8_000_000.0,
                entry_price: 100_000.0,
                unrealized_pnl: -600_000.0,
                margin_used: 2_800_000.0,
                leverage: 3.0,
                funding_since_open: 30.0,
            },
        ];
        let mut accounts = HashMap::new();
        accounts.insert(
            "0x1".to_string(),
            AccountSummary {
                wallet: "0x1".to_string(),
                account_value: 1.0,
                margin_used: 1.0,
                available_margin: 0.0,
                is_locked: true,
            },
        );
        accounts.insert(
            "0x2".to_string(),
            AccountSummary {
                wallet: "0x2".to_string(),
                account_value: 1.0,
                margin_used: 1.0,
                available_margin: 0.0,
                is_locked: true,
            },
        );

        let evaluation = signal.evaluate("BTC", &positions, &accounts, 100_000_000.0, &labels);
        assert_eq!(evaluation.signal, Direction::Short);
        assert_eq!(evaluation.locked_long_count, 2);
        let _ = fs::remove_file(wallet_file);
    }

}
