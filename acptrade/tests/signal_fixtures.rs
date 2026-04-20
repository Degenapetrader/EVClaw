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
    let wallet_file = temp_file("dead-cap-low-oi", r#"{}"#);
    let labels = label_store(&wallet_file);
    let mut signal = DeadCapitalSignal::default();
    let result = signal.evaluate("BTC", &[], &HashMap::new(), 500_000.0, &labels);
    assert_eq!(result.signal, Direction::Neutral);
    assert_eq!(result.reason, "OI < $1M ($0.50M)");
    assert_eq!(result.threshold, 0.0);
    assert_eq!(result.locked_wallet_count, 0);
    let _ = fs::remove_file(wallet_file);
}

#[test]
fn dead_cap_locked_long_fixture() {
    let wallet_file = temp_file(
        "dead-cap-locked-long",
        r#"{"0x1":{"label":"REKT","score":-80,"volume_weight":1.0},"0x3":{"label":"LOSER","score":-45,"volume_weight":1.0}}"#,
    );
    let labels = label_store(&wallet_file);
    let mut signal = DeadCapitalSignal::default();
    let positions = vec![
        wallet_position("0x1", "BTC", Direction::Long, 40_000_000.0, 0.0),
        wallet_position("0x3", "BTC", Direction::Long, 20_000_000.0, 0.0),
        wallet_position("0x2", "BTC", Direction::Short, 1_000_000.0, 0.0),
    ];
    let mut accounts = HashMap::new();
    accounts.insert("0x1".to_string(), locked_account("0x1"));
    accounts.insert("0x3".to_string(), locked_account("0x3"));
    accounts.insert("0x2".to_string(), unlocked_account("0x2"));

    let result = signal.evaluate("BTC", &positions, &accounts, 100_000_000.0, &labels);
    assert_eq!(result.signal, Direction::Short);
    assert!(result.strength >= 0.75);
    assert_close(result.locked_long_pct, 60.0);
    assert_close(result.locked_short_pct, 0.0);
    assert_close(result.threshold, 9.0);
    assert_eq!(result.locked_long_count, 2);
    assert_eq!(result.locked_short_count, 0);
    assert_eq!(result.locked_wallet_count, 2);
    assert!(result.reason.contains("-> SHORT"));
    let _ = fs::remove_file(wallet_file);
}

#[test]
fn dead_cap_locked_short_fixture() {
    let wallet_file = temp_file(
        "dead-cap-locked-short",
        r#"{"0x9":{"label":"REKT","score":-80,"volume_weight":1.0},"0x7":{"label":"LOSER","score":-45,"volume_weight":1.0}}"#,
    );
    let labels = label_store(&wallet_file);
    let mut signal = DeadCapitalSignal::default();
    let positions = vec![
        wallet_position("0x9", "ETH", Direction::Short, 40_000_000.0, 0.0),
        wallet_position("0x7", "ETH", Direction::Short, 20_000_000.0, 0.0),
        wallet_position("0x8", "ETH", Direction::Long, 1_000_000.0, 0.0),
    ];
    let mut accounts = HashMap::new();
    accounts.insert("0x9".to_string(), locked_account("0x9"));
    accounts.insert("0x7".to_string(), locked_account("0x7"));
    accounts.insert("0x8".to_string(), unlocked_account("0x8"));

    let result = signal.evaluate("ETH", &positions, &accounts, 100_000_000.0, &labels);
    assert_eq!(result.signal, Direction::Long);
    assert!(result.strength >= 0.75);
    assert_close(result.locked_long_pct, 0.0);
    assert_close(result.locked_short_pct, 60.0);
    assert_close(result.threshold, 9.0);
    assert!(result.reason.contains("-> LONG"));
    let _ = fs::remove_file(wallet_file);
}

#[test]
fn dead_cap_strength_gate_fixture() {
    let wallet_file = temp_file(
        "dead-cap-strength-gate",
        r#"{"0x1":{"label":"REKT","score":-80,"volume_weight":1.0}}"#,
    );
    let labels = label_store(&wallet_file);
    let mut signal = DeadCapitalSignal::default();
    let positions = vec![wallet_position(
        "0x1",
        "BTC",
        Direction::Long,
        5_000_000.0,
        0.0,
    )];
    let mut accounts = HashMap::new();
    accounts.insert("0x1".to_string(), locked_account("0x1"));

    let result = signal.evaluate("BTC", &positions, &accounts, 100_000_000.0, &labels);
    assert_eq!(result.signal, Direction::Neutral);
    assert_eq!(result.strength, 0.0);
    assert!(!result.reason.is_empty());
    let _ = fs::remove_file(wallet_file);
}

#[test]
fn dead_cap_threshold_tiers_fixture() {
    let cases = [
        (10_000_000.0, 12.0),
        (100_000_000.0, 9.0),
        (500_000_000.0, 6.0),
        (1_000_000_000.0, 4.0),
    ];
    let wallet_file = temp_file("dead-cap-thresholds", r#"{}"#);
    let labels = label_store(&wallet_file);

    for (oi, threshold) in cases {
        let mut signal = DeadCapitalSignal::default();
        let result = signal.evaluate("SOL", &[], &HashMap::new(), oi, &labels);
        assert_eq!(result.signal, Direction::Neutral);
        assert_close(result.threshold, threshold);
        assert!(result.reason.contains("coverage low") || result.reason.contains("effL"));
    }
    let _ = fs::remove_file(wallet_file);
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
        entry_price: 0.0,
        unrealized_pnl: 0.0,
        margin_used: 0.0,
        leverage: 1.0,
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
