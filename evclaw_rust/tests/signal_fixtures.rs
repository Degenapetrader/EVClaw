use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use evclaw::labels::WalletLabelStore;
use evclaw::signals::dead_cap::DeadCapitalSignal;
use evclaw::signals::whale::WhaleSignal;
use evclaw::types::{AccountSummary, Direction, WalletPosition};

#[test]
fn dead_cap_low_oi_fixture() {
    let result = DeadCapitalSignal.evaluate("BTC", &[], &HashMap::new(), 500_000.0);
    assert_eq!(result.signal, Direction::Neutral);
    assert_eq!(result.reason, "OI < $10M ($0.50M)");
    assert_eq!(result.threshold, 0.0);
    assert_eq!(result.locked_wallet_count, 0);
}

#[test]
fn dead_cap_locked_long_fixture() {
    let positions = vec![
        wallet_position("0x1", "BTC", Direction::Long, 10_000_000.0, 0.0),
        wallet_position("0x2", "BTC", Direction::Short, 1_000_000.0, 0.0),
    ];
    let mut accounts = HashMap::new();
    accounts.insert("0x1".to_string(), locked_account("0x1"));
    accounts.insert("0x2".to_string(), unlocked_account("0x2"));

    let result = DeadCapitalSignal.evaluate("BTC", &positions, &accounts, 100_000_000.0);
    assert_eq!(result.signal, Direction::Short);
    assert_close(result.strength, 10.0 / 12.0);
    assert_close(result.locked_long_pct, 10.0);
    assert_close(result.locked_short_pct, 0.0);
    assert_close(result.threshold, 6.0);
    assert_eq!(result.locked_long_count, 1);
    assert_eq!(result.locked_short_count, 0);
    assert_eq!(result.locked_wallet_count, 1);
    assert_eq!(result.reason, "locked L:10.0% > thr:6.0% → SHORT");
}

#[test]
fn dead_cap_locked_short_fixture() {
    let positions = vec![
        wallet_position("0x9", "ETH", Direction::Short, 5_000_000.0, 0.0),
        wallet_position("0x8", "ETH", Direction::Long, 1_000_000.0, 0.0),
    ];
    let mut accounts = HashMap::new();
    accounts.insert("0x9".to_string(), locked_account("0x9"));
    accounts.insert("0x8".to_string(), unlocked_account("0x8"));

    let result = DeadCapitalSignal.evaluate("ETH", &positions, &accounts, 50_000_000.0);
    assert_eq!(result.signal, Direction::Long);
    assert_close(result.strength, 10.0 / 12.0);
    assert_close(result.locked_long_pct, 0.0);
    assert_close(result.locked_short_pct, 10.0);
    assert_close(result.threshold, 6.0);
    assert_eq!(result.reason, "locked S:10.0% > thr:6.0% → LONG");
}

#[test]
fn dead_cap_strength_gate_fixture() {
    let positions = vec![wallet_position(
        "0x1",
        "BTC",
        Direction::Long,
        8_000_000.0,
        0.0,
    )];
    let mut accounts = HashMap::new();
    accounts.insert("0x1".to_string(), locked_account("0x1"));

    let result = DeadCapitalSignal.evaluate("BTC", &positions, &accounts, 100_000_000.0);
    assert_eq!(result.signal, Direction::Neutral);
    assert_eq!(result.strength, 0.0);
    assert_eq!(result.reason, "L:8.0% S:0.0% < min str:0.75");
}

#[test]
fn dead_cap_threshold_tiers_fixture() {
    let cases = [
        (10_000_000.0, 6.0),
        (100_000_000.0, 6.0),
        (500_000_000.0, 4.0),
        (1_000_000_000.0, 2.5),
    ];

    for (oi, threshold) in cases {
        let result = DeadCapitalSignal.evaluate("SOL", &[], &HashMap::new(), oi);
        assert_eq!(result.signal, Direction::Neutral);
        assert_close(result.threshold, threshold);
        assert_eq!(
            result.reason,
            format!("L:0.0% S:0.0% < thr:{threshold:.1}%")
        );
    }
}

#[test]
fn whale_good_bad_mix_fixture() {
    let wallet_file = temp_file(
        "whale-mix",
        r#"{"0x1":{"label":"LEGEND"},"0x2":{"label":"REKT"},"0x3":{"label":"SOLID"}}"#,
    );
    let labels = label_store(&wallet_file);
    let positions = vec![
        wallet_position("0x1", "BTC", Direction::Long, 5_000_000.0, 0.0),
        wallet_position("0x2", "BTC", Direction::Short, 3_500_000.0, 25.0),
        wallet_position("0x3", "BTC", Direction::Long, 1_000_000.0, 0.0),
    ];

    let result = WhaleSignal.evaluate("BTC", &positions, 100_000_000.0, &labels);
    assert_eq!(result.signal, Direction::Long);
    assert_close(result.score, 9.0);
    assert_close(result.strength, 0.9);
    assert_close(result.threshold, 3_000_000.0);
    assert_eq!(result.whale_count, 2);
    assert_eq!(
        result.reason,
        "2 whales, score=+9.0 [LEGEND:L:$5.0M, REKT:S:$3.5M]"
    );
    assert_eq!(result.whales[0].label, "LEGEND");
    assert_eq!(result.whales[1].label, "REKT");

    let _ = fs::remove_file(wallet_file);
}

#[test]
fn whale_no_new_whales_fixture() {
    let wallet_file = temp_file("whale-empty", r#"{"0x1":{"label":"LEGEND"}}"#);
    let labels = label_store(&wallet_file);
    let positions = vec![wallet_position(
        "0x1",
        "ETH",
        Direction::Long,
        4_000_000.0,
        101.0,
    )];

    let result = WhaleSignal.evaluate("ETH", &positions, 100_000_000.0, &labels);
    assert_eq!(result.signal, Direction::Neutral);
    assert_close(result.threshold, 3_000_000.0);
    assert_eq!(result.whale_count, 0);
    assert_eq!(result.reason, "no new whales");

    let _ = fs::remove_file(wallet_file);
}

#[test]
fn whale_zero_weight_fixture() {
    let wallet_file = temp_file("whale-zero", r#"{"0x1":{"label":"NO_DATA"}}"#);
    let labels = label_store(&wallet_file);
    let positions = vec![wallet_position(
        "0x1",
        "ETH",
        Direction::Long,
        6_000_000.0,
        0.0,
    )];

    let result = WhaleSignal.evaluate("ETH", &positions, 100_000_000.0, &labels);
    assert_eq!(result.signal, Direction::Neutral);
    assert_close(result.score, 0.0);
    assert_close(result.threshold, 3_000_000.0);
    assert_eq!(result.whale_count, 1);
    assert_eq!(result.reason, "1 whales, score=+0.0 [NO_DATA:L:$6.0M]");

    let _ = fs::remove_file(wallet_file);
}

#[test]
fn whale_threshold_tiers_fixture() {
    let wallet_file = temp_file("whale-threshold", r#"{}"#);
    let labels = label_store(&wallet_file);
    let cases = [
        (0.0, 250_000.0),
        (1_000_000.0, 250_000.0),
        (100_000_000.0, 3_000_000.0),
        (250_000_000.0, 3_750_000.0),
        (500_000_000.0, 2_500_000.0),
        (1_000_000_000.0, 2_500_000.0),
    ];

    for (oi, threshold) in cases {
        let result = WhaleSignal.evaluate("SOL", &[], oi, &labels);
        assert_eq!(result.signal, Direction::Neutral);
        assert_close(result.threshold, threshold);
        assert_eq!(result.reason, "no new whales");
    }

    let _ = fs::remove_file(wallet_file);
}

fn wallet_position(
    wallet: &str,
    symbol: &str,
    side: Direction,
    position_value: f64,
    funding_since_open: f64,
) -> WalletPosition {
    WalletPosition {
        wallet: wallet.to_string(),
        symbol: symbol.to_string(),
        side,
        position_value,
        funding_since_open,
    }
}

fn locked_account(wallet: &str) -> AccountSummary {
    AccountSummary {
        wallet: wallet.to_string(),
        account_value: 1.0,
        margin_used: 1.0,
        available_margin: 0.0,
        is_locked: true,
    }
}

fn unlocked_account(wallet: &str) -> AccountSummary {
    AccountSummary {
        wallet: wallet.to_string(),
        account_value: 10.0,
        margin_used: 1.0,
        available_margin: 9.0,
        is_locked: false,
    }
}

fn label_store(path: &Path) -> WalletLabelStore {
    let mut labels = WalletLabelStore::new(path.to_path_buf());
    labels.reload().expect("failed to load wallet labels");
    labels
}

fn temp_file(prefix: &str, contents: &str) -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock went backwards")
        .as_nanos();
    let path = std::env::temp_dir().join(format!("evclaw-{prefix}-{nonce}.json"));
    fs::write(&path, contents).expect("failed to write temp file");
    path
}

fn assert_close(left: f64, right: f64) {
    let delta = (left - right).abs();
    assert!(
        delta <= 1e-9,
        "left={} right={} delta={}",
        left,
        right,
        delta
    );
}
