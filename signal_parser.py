#!/usr/bin/env python3
"""
Signal Parser for Tracker Data.

Extracts and normalizes perp_signals from symbol data received via SSE.
Handles the various z-score field names across different signal types.

Phase 1 of HyperLighter Trading Skill.
"""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

# Signal types available in perp_signals
# NOTE: liq_pnl removed upstream (2026-01) and deprecated for decisions.
SIGNAL_TYPES = ['cvd', 'fade', 'whale', 'dead_capital', 'ofm']

# Z-score thresholds
Z_THRESHOLD_STANDARD = 2.0  # Standard signal
Z_THRESHOLD_STRONG = 3.0   # Strong signal

# Direction mappings
DIRECTION_MAP = {
    # CVD uses NEUTRAL/BULLISH/BEARISH
    'BULLISH': 'LONG',
    'BEARISH': 'SHORT',
    'NEUTRAL': 'NEUTRAL',
    # Other signals use LONG/SHORT
    'LONG': 'LONG',
    'SHORT': 'SHORT',
    # Default
    None: 'NEUTRAL',
    '': 'NEUTRAL',
}


@dataclass
class ParsedSignal:
    """
    Normalized signal data extracted from tracker perp_signals.

    Attributes:
        symbol: Trading symbol (e.g., 'BTC', 'ETH')
        signal_type: Signal name (cvd, fade, liq_pnl, whale, dead_capital, ofm)
        direction: Normalized direction (LONG, SHORT, NEUTRAL)
        z_score: Primary z-score for this signal (may be 0 if not available)
        z_smart: Smart money z-score (CVD only)
        z_dumb: Dumb money z-score (CVD only)
        confidence: Derived confidence level (LOW, MEDIUM, HIGH)
        strength: Signal strength (0.0-1.0) for whale/dead_capital
        raw_data: Original signal data for debugging
        generated_at: Timestamp when signal was generated
    """
    symbol: str
    signal_type: str
    direction: str
    z_score: float = 0.0
    z_smart: float = 0.0
    z_dumb: float = 0.0
    confidence: str = "LOW"
    strength: float = 0.0
    raw_data: Dict[str, Any] = field(default_factory=dict)
    generated_at: Optional[str] = None

    @property
    def is_actionable(self) -> bool:
        """Check if signal meets minimum threshold for trading consideration."""
        return abs(self.z_score) >= Z_THRESHOLD_STANDARD or self.strength >= 1.0

    @property
    def is_strong(self) -> bool:
        """Check if signal is strong (higher confidence)."""
        return abs(self.z_score) >= Z_THRESHOLD_STRONG


def normalize_direction(raw_direction: Optional[str]) -> str:
    """Convert various direction formats to standard LONG/SHORT/NEUTRAL."""
    if raw_direction is None:
        return 'NEUTRAL'
    return DIRECTION_MAP.get(raw_direction.upper(), 'NEUTRAL')


def derive_confidence(z_score: float, raw_confidence: Optional[str] = None) -> str:
    """
    Derive confidence level from z-score.

    Priority:
    1. Use raw confidence if provided and valid
    2. Fall back to z-score based calculation
    """
    # If raw confidence is provided, validate and use it
    if raw_confidence and raw_confidence.upper() in ('LOW', 'MEDIUM', 'HIGH'):
        return raw_confidence.upper()

    # Derive from z-score
    abs_z = abs(z_score)
    if abs_z >= Z_THRESHOLD_STRONG:
        return 'HIGH'
    elif abs_z >= Z_THRESHOLD_STANDARD:
        return 'MEDIUM'
    else:
        return 'LOW'


def extract_z_score(signal_data: Dict[str, Any], signal_type: str) -> tuple:
    """
    Extract z-score(s) from signal data.

    Returns:
        (primary_z, z_smart, z_dumb)

    Different signals have different z-score fields:
    - cvd: z_smart, z_dumb (use max abs as primary)
    - fade, liq_pnl, ofm: z_score
    - whale, dead_capital: strength (no z_score)
    """
    z_score = 0.0
    z_smart = 0.0
    z_dumb = 0.0

    if signal_type == 'cvd':
        z_smart = float(signal_data.get('z_smart', 0) or 0)
        z_dumb = float(signal_data.get('z_dumb', 0) or 0)
        # Use the stronger signal as primary z-score
        z_score = z_smart if abs(z_smart) >= abs(z_dumb) else z_dumb

    elif signal_type in ('fade', 'ofm'):
        z_score = float(signal_data.get('z_score', 0) or 0)

    elif signal_type in ('whale', 'dead_capital'):
        # These use 'strength' instead of z-score (legacy).
        strength = float(signal_data.get('strength', 0) or 0)
        if strength > 0:
            z_score = strength * 2.0
        elif signal_type == 'dead_capital':
            # New watcher format: banner_trigger indicates a STRONG state,
            # but we still derive a usable z-equivalent even when banner_trigger
            # is false (e.g. due to cooldown suppression).
            banner_trigger = bool(signal_data.get('banner_trigger', False))
            direction = normalize_direction(signal_data.get('signal'))
            try:
                locked_long = float(signal_data.get('locked_long_pct', 0) or 0)
            except (TypeError, ValueError):
                locked_long = 0.0
            try:
                locked_short = float(signal_data.get('locked_short_pct', 0) or 0)
            except (TypeError, ValueError):
                locked_short = 0.0
            try:
                threshold = float(signal_data.get('threshold', 0) or 0)
            except (TypeError, ValueError):
                threshold = 0.0
            try:
                banner_threshold = float(signal_data.get('banner_threshold', 0) or 0)
            except (TypeError, ValueError):
                banner_threshold = 0.0

            # Use microstructure-consistent trapped-side magnitude.
            # LONG signal => trapped shorts dominate (locked_short_pct).
            # SHORT signal => trapped longs dominate (locked_long_pct).
            locked_mag = locked_short if direction == 'LONG' else locked_long if direction == 'SHORT' else 0.0

            # Compute a "z-equivalent" scale calibrated to our internal thresholds.
            if threshold > 0 and banner_threshold > threshold:
                if locked_mag <= threshold:
                    z_score = (locked_mag / threshold) * 2.0
                else:
                    slope = 1.0 / (banner_threshold - threshold)
                    z_score = 2.0 + (locked_mag - threshold) * slope
            elif threshold > 0:
                z_score = (locked_mag / threshold) * 2.0
            else:
                z_score = locked_mag

            if banner_trigger:
                # Strong signal state.
                z_score = max(z_score, 3.0)
            else:
                # No banner: allow contribution, but cap below strong.
                z_score = min(z_score, 2.8)
        else:
            z_score = 0.0

    return z_score, z_smart, z_dumb


def parse_signal(
    symbol: str,
    signal_type: str,
    signal_data: Dict[str, Any],
    generated_at: Optional[str] = None
) -> ParsedSignal:
    """
    Parse a single signal into normalized ParsedSignal.

    Args:
        symbol: Trading symbol
        signal_type: Signal name (cvd, fade, etc.)
        signal_data: Raw signal data from perp_signals
        generated_at: Symbol generation timestamp

    Returns:
        ParsedSignal with normalized values
    """
    if not signal_data or not isinstance(signal_data, dict):
        return ParsedSignal(
            symbol=symbol,
            signal_type=signal_type,
            direction='NEUTRAL',
            generated_at=generated_at,
            raw_data={}
        )

    # Extract direction
    raw_direction = signal_data.get('signal')
    direction = normalize_direction(raw_direction)

    # Extract z-scores
    z_score, z_smart, z_dumb = extract_z_score(signal_data, signal_type)

    # Extract strength (for whale/dead_capital)
    strength = float(signal_data.get('strength', 0) or 0)
    if signal_type in ('whale', 'dead_capital') and strength <= 0 and z_score > 0:
        # Backfill legacy strength scale from z-equivalent
        strength = z_score / 2.0

    # DEAD_CAPITAL policy: banner_trigger is a STRONG boost, not a hard gate.
    # (We still allow dead_capital contribution when banner_trigger is false,
    # e.g. due to cooldown suppression.)

    # Derive confidence
    raw_confidence = signal_data.get('confidence')
    confidence = derive_confidence(z_score, raw_confidence)

    return ParsedSignal(
        symbol=symbol,
        signal_type=signal_type,
        direction=direction,
        z_score=z_score,
        z_smart=z_smart,
        z_dumb=z_dumb,
        confidence=confidence,
        strength=strength,
        raw_data=signal_data,
        generated_at=generated_at
    )


def parse_symbol_signals(symbol_data: Dict[str, Any]) -> List[ParsedSignal]:
    """
    Parse all signals for a single symbol.

    Args:
        symbol_data: Full symbol data from tracker (contains perp_signals)

    Returns:
        List of ParsedSignal objects, sorted by z_score (strongest first)
    """
    if not symbol_data or not isinstance(symbol_data, dict):
        return []

    symbol = symbol_data.get('symbol', 'UNKNOWN')
    generated_at = symbol_data.get('generated_at')
    perp_signals = symbol_data.get('perp_signals')

    if not perp_signals or not isinstance(perp_signals, dict):
        return []

    signals = []
    for signal_type in SIGNAL_TYPES:
        signal_data = perp_signals.get(signal_type)
        if signal_data:
            parsed = parse_signal(symbol, signal_type, signal_data, generated_at)
            signals.append(parsed)

    # Sort by absolute z-score (strongest first)
    signals.sort(key=lambda s: abs(s.z_score), reverse=True)

    return signals


def parse_all_signals(symbols_data: Dict[str, Dict]) -> Dict[str, List[ParsedSignal]]:
    """
    Parse signals for all symbols.

    Args:
        symbols_data: Full data from SSE (all 207 symbols)

    Returns:
        Dict mapping symbol to list of parsed signals
    """
    result = {}
    for symbol, symbol_data in symbols_data.items():
        signals = parse_symbol_signals(symbol_data)
        if signals:
            result[symbol] = signals
    return result


def get_actionable_signals(
    symbols_data: Dict[str, Dict],
    min_z_score: float = Z_THRESHOLD_STANDARD,
    direction: Optional[str] = None
) -> List[ParsedSignal]:
    """
    Get all actionable signals across all symbols.

    Args:
        symbols_data: Full data from SSE
        min_z_score: Minimum z-score threshold (default: 2.0)
        direction: Filter by direction (LONG, SHORT, or None for all)

    Returns:
        List of ParsedSignal meeting criteria, sorted by z_score
    """
    all_signals = []

    for symbol, symbol_data in symbols_data.items():
        signals = parse_symbol_signals(symbol_data)
        for sig in signals:
            # Check z-score threshold
            if abs(sig.z_score) < min_z_score:
                continue

            # Check direction filter
            if direction and sig.direction != direction:
                continue

            # Skip neutral signals
            if sig.direction == 'NEUTRAL':
                continue

            all_signals.append(sig)

    # Sort by z-score (strongest first)
    all_signals.sort(key=lambda s: abs(s.z_score), reverse=True)

    return all_signals


def format_signal(signal: ParsedSignal) -> str:
    """Format signal for display."""
    z_display = f"z={signal.z_score:.2f}"
    if signal.signal_type == 'cvd' and (signal.z_smart or signal.z_dumb):
        z_display = f"z_smart={signal.z_smart:.2f}, z_dumb={signal.z_dumb:.2f}"

    strength_str = f", strength={signal.strength:.1f}" if signal.strength else ""

    return (
        f"{signal.symbol} {signal.signal_type.upper()}: "
        f"{signal.direction} ({signal.confidence}) - {z_display}{strength_str}"
    )


# Example usage
if __name__ == "__main__":
    # Sample data matching SSE cache structure
    sample_data = {
        "BTC": {
            "symbol": "BTC",
            "generated_at": "2026-01-25T15:42:51Z",
            "perp_signals": {
                "cvd": {
                    "signal": "NEUTRAL",
                    "confidence": "LOW",
                    "z_smart": 0.26,
                    "z_dumb": -0.37,
                    "smart_cvd": -5598.31,
                    "dumb_cvd": 273.89
                },
                "fade": {
                    "signal": "NEUTRAL",
                    "z_score": 0.3,
                    "divergence": -0.001
                },
                "whale": {
                    "signal": "SHORT",
                    "strength": 1.0,
                    "score": -32.5
                },
                "dead_capital": {
                    "signal": "SHORT",
                    "strength": 1.0,
                    "locked_long_pct": 38.36
                }
            }
        },
        "SOL": {
            "symbol": "SOL",
            "generated_at": "2026-01-25T15:42:51Z",
            "perp_signals": {
                "cvd": {
                    "signal": "BEARISH",
                    "z_smart": -2.5,
                    "z_dumb": 1.2
                },
                "fade": {
                    "signal": "SHORT",
                    "z_score": 2.8
                }
            }
        }
    }

    print("=== All Parsed Signals ===")
    all_parsed = parse_all_signals(sample_data)
    for symbol, signals in all_parsed.items():
        print(f"\n{symbol}:")
        for sig in signals:
            print(f"  {format_signal(sig)}")

    print("\n=== Actionable Signals (z >= 2.0) ===")
    actionable = get_actionable_signals(sample_data)
    for sig in actionable:
        print(f"  {format_signal(sig)}")
