use evclaw::runtime::{combined_target, should_release_reentry_block};
use evclaw::types::{Direction, OiBucketPolicy, SignalComponent};

#[test]
fn combined_target_uses_dead_cap_only() {
    let dead = SignalComponent {
        direction: Direction::Long,
        strength: 0.8,
        reason: "dead".to_string(),
    };
    let whale = SignalComponent {
        direction: Direction::Short,
        strength: 0.4,
        reason: "whale".to_string(),
    };
    let bucket_policy = OiBucketPolicy {
        bucket_label: ">=1B".to_string(),
        oi_usd_at_entry: 1_000_000_000.0,
        size_multiplier: 3.0,
        sl_atr_multiplier: 2.0,
        tp_atr_multiplier: 3.0,
        min_hold_hours: 5.0,
    };

    let (score, direction, raw_notional, order_notional) =
        combined_target(&dead, &whale, 30.0, 12.0, &bucket_policy);
    assert!((score - 0.8).abs() < 1e-9);
    assert_eq!(direction, Direction::Long);
    assert!((raw_notional - 72.0).abs() < 1e-9);
    assert!((order_notional - 72.0).abs() < 1e-9);
}

#[test]
fn combined_target_ignores_whale_conflict() {
    let dead = SignalComponent {
        direction: Direction::Long,
        strength: 0.7,
        reason: String::new(),
    };
    let whale = SignalComponent {
        direction: Direction::Short,
        strength: 0.7,
        reason: String::new(),
    };
    let bucket_policy = OiBucketPolicy {
        bucket_label: ">=100M".to_string(),
        oi_usd_at_entry: 100_000_000.0,
        size_multiplier: 2.0,
        sl_atr_multiplier: 2.5,
        tp_atr_multiplier: 4.0,
        min_hold_hours: 6.0,
    };

    let (score, direction, raw_notional, order_notional) =
        combined_target(&dead, &whale, 30.0, 12.0, &bucket_policy);
    assert!((score - 0.7).abs() < 1e-9);
    assert_eq!(direction, Direction::Long);
    assert!((raw_notional - 42.0).abs() < 1e-9);
    assert!((order_notional - 42.0).abs() < 1e-9);
}

#[test]
fn combined_target_scales_floor_by_oi_bucket() {
    let dead = SignalComponent {
        direction: Direction::Short,
        strength: 0.1,
        reason: String::new(),
    };
    let whale = SignalComponent {
        direction: Direction::Neutral,
        strength: 0.0,
        reason: String::new(),
    };
    let bucket_policy = OiBucketPolicy {
        bucket_label: ">=10M".to_string(),
        oi_usd_at_entry: 10_000_000.0,
        size_multiplier: 0.6,
        sl_atr_multiplier: 3.5,
        tp_atr_multiplier: 6.0,
        min_hold_hours: 8.0,
    };

    let (_, direction, raw_notional, order_notional) =
        combined_target(&dead, &whale, 30.0, 12.0, &bucket_policy);
    assert_eq!(direction, Direction::Short);
    assert!((raw_notional - 1.8).abs() < 1e-9);
    assert!((order_notional - 7.2).abs() < 1e-9);
}

#[test]
fn reentry_block_releases_only_after_neutral_or_opposite() {
    assert!(!should_release_reentry_block(
        Direction::Long,
        Direction::Long
    ));
    assert!(should_release_reentry_block(
        Direction::Long,
        Direction::Neutral
    ));
    assert!(should_release_reentry_block(
        Direction::Long,
        Direction::Short
    ));
}
