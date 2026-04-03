use evclaw::runtime::{combined_target, should_release_reentry_block};
use evclaw::types::{Direction, SignalComponent};

#[test]
fn combined_target_uses_native_strength_with_half_weights() {
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

    let (score, direction, raw_notional, order_notional) =
        combined_target(&dead, &whale, 30.0, 12.0);
    assert!((score - 0.2).abs() < 1e-9);
    assert_eq!(direction, Direction::Long);
    assert!((raw_notional - 6.0).abs() < 1e-9);
    assert!((order_notional - 12.0).abs() < 1e-9);
}

#[test]
fn combined_target_flats_when_components_cancel() {
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

    let (score, direction, raw_notional, order_notional) =
        combined_target(&dead, &whale, 30.0, 12.0);
    assert!(score.abs() < 1e-9);
    assert_eq!(direction, Direction::Neutral);
    assert_eq!(raw_notional, 0.0);
    assert_eq!(order_notional, 0.0);
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
