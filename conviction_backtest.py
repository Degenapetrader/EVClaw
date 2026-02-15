#!/usr/bin/env python3
"""
Conviction Backtest — Compare Pipeline vs Brain conviction paths across all historical trades.

Outputs:
  1. Per-trade CSV with both conviction scores + would-trade flags
  2. Summary comparison table: actual vs brain-only vs combined filter
"""

from __future__ import annotations
import sqlite3
import json
import os
import sys
import csv
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime, timezone

import yaml

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
DB_PATH = os.path.join(os.path.dirname(__file__), "ai_trader.db")
CSV_OUT = os.path.join(os.path.dirname(__file__), "conviction_backtest_results.csv")


def _load_skill_brain_cfg() -> Dict[str, Any]:
    try:
        cfg_path = Path(__file__).with_name("skill.yaml")
        with cfg_path.open("r", encoding="utf-8") as f:
            raw = yaml.safe_load(f) or {}
        if not isinstance(raw, dict):
            return {}
        cfg = raw.get("config") or {}
        if not isinstance(cfg, dict):
            return {}
        brain = cfg.get("brain") or {}
        return brain if isinstance(brain, dict) else {}
    except Exception:
        return {}


def _skill_float(cfg: Dict[str, Any], key: str, default: float) -> float:
    try:
        value = cfg.get(key, default)
        return float(default if value is None else value)
    except Exception:
        return float(default)


_BRAIN_CFG = _load_skill_brain_cfg()

# Pipeline defaults
SCORE_DIVISOR = _skill_float(_BRAIN_CFG, "candidate_score_divisor", 100.0)
MIN_CONVICTION = _skill_float(_BRAIN_CFG, "candidate_min_conviction", 0.1)
BIAS_PENALTY = 0.6
BIAS_BONUS = 1.05
BIAS_THRESHOLD = 5000.0

# Brain defaults
BRAIN_CONVICTION_WEIGHTS = {
    "cvd": 0.20,
    "dead_capital": 0.32,
    "fade": 0.08,
    "ofm": 0.12,
    "hip3_main": 0.70,
    "whale": 0.28,
}
BRAIN_Z_DENOM = 3.0
BRAIN_STRENGTH_Z_MULT = 2.0
BRAIN_BASE_THRESHOLD = 0.55
BRAIN_HIGH_THRESHOLD = 0.75
BRAIN_SMART_ADJUST_MULT = 0.1
BRAIN_SMART_DIV_Z_DENOM = 3.0
BRAIN_SMART_DIV_Z_MULT = 0.5
BRAIN_COHORT_ALIGN_BONUS = 0.3
BRAIN_COHORT_COUNTER_PENALTY = 0.2
BRAIN_LIQ_EDGE_THRESHOLD = 0.5
BRAIN_LIQ_EDGE_BONUS = 0.05
BRAIN_VOL_AVOID_PENALTY = 0.15
BRAIN_AGREE_BONUS_COUNT_1 = 4
BRAIN_AGREE_BONUS_1 = 0.10
BRAIN_AGREE_BONUS_COUNT_2 = 5
BRAIN_AGREE_BONUS_2 = 0.05
BRAIN_STRONG_WHALE_BONUS = 0.20
BRAIN_STRONG_OTHER_BONUS = 0.10
BRAIN_STRONG_DEAD_MIN_CONVICTION = 0.90
BRAIN_STRONG_Z_THRESHOLD = 1.5

# ATR volatility bands
BRAIN_ATR_AVOID_MAX = 0.3
BRAIN_ATR_SCALP_MAX = 0.8
BRAIN_ATR_SWING_MAX = 3.0
BRAIN_ATR_VOLATILE_MAX = 6.0


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _sf(v: Any, d: float = 0.0) -> float:
    try:
        return float(v)
    except (TypeError, ValueError):
        return d


def _normalize_dir(v: Any) -> str:
    d = str(v or "").upper()
    return d if d in ("LONG", "SHORT", "NEUTRAL") else "NEUTRAL"


# ---------------------------------------------------------------------------
# Brain conviction calculator (standalone, no imports from trading_brain)
# ---------------------------------------------------------------------------
def calc_brain_conviction(
    signals: Dict[str, Dict],
    metrics: Dict[str, Any],
    direction: str,
) -> Tuple[float, float, str, bool]:
    """
    Returns (base_conviction, adjusted_conviction, notes, would_trade).
    """
    direction = _normalize_dir(direction)
    notes_parts: List[str] = []

    # --- Step 1: base conviction (weighted signal strengths) ---
    total_weight = 0.0
    weighted_strength = 0.0

    for sig_name, w in BRAIN_CONVICTION_WEIGHTS.items():
        sig_data = signals.get(sig_name)
        if not sig_data or not isinstance(sig_data, dict):
            continue

        sig_dir = _normalize_dir(sig_data.get("direction"))

        # Derive z-score
        z = _sf(sig_data.get("z_score"))
        if sig_name == "cvd":
            z_smart = abs(_sf(sig_data.get("z_smart")))
            z_dumb = abs(_sf(sig_data.get("z_dumb")))
            z = max(abs(z), z_smart, z_dumb)
        if sig_name in ("whale", "dead_capital") and z == 0:
            strength_raw = _sf(sig_data.get("strength"))
            z = strength_raw * BRAIN_STRENGTH_Z_MULT

        strength = min(1.0, abs(z) / max(1e-9, BRAIN_Z_DENOM))
        total_weight += w

        if sig_dir == direction:
            weighted_strength += w * strength
        elif sig_dir != "NEUTRAL":
            weighted_strength -= w * strength * 0.5

    if total_weight == 0:
        return 0.0, 0.0, "no_signals", False

    base = weighted_strength / total_weight

    # Agreement bonus
    if base > 0:
        agree = sum(1 for s in signals.values()
                    if isinstance(s, dict) and _normalize_dir(s.get("direction")) == direction)
        if agree >= BRAIN_AGREE_BONUS_COUNT_1:
            base += BRAIN_AGREE_BONUS_1
        if agree >= BRAIN_AGREE_BONUS_COUNT_2:
            base += BRAIN_AGREE_BONUS_2
        notes_parts.append(f"agree={agree}")

    base = max(0.0, min(1.0, base))

    # --- Step 2: adjustments ---
    adj = base

    # Smart money alignment
    smart_cvd = _sf(metrics.get("smart_cvd"))
    div_z = _sf(metrics.get("divergence_z"))
    smart_score = 0.0
    if direction == "LONG":
        if smart_cvd > 0 and div_z > 0:
            smart_score = min(1.0, div_z / BRAIN_SMART_DIV_Z_DENOM) * BRAIN_SMART_DIV_Z_MULT
        elif smart_cvd < 0:
            smart_score = -min(1.0, abs(div_z) / BRAIN_SMART_DIV_Z_DENOM) * BRAIN_SMART_DIV_Z_MULT
    elif direction == "SHORT":
        if smart_cvd < 0 and div_z < 0:
            smart_score = min(1.0, abs(div_z) / BRAIN_SMART_DIV_Z_DENOM) * BRAIN_SMART_DIV_Z_MULT
        elif smart_cvd > 0:
            smart_score = -min(1.0, div_z / BRAIN_SMART_DIV_Z_DENOM) * BRAIN_SMART_DIV_Z_MULT
    adj += smart_score * BRAIN_SMART_ADJUST_MULT
    if smart_score != 0:
        notes_parts.append(f"smart={smart_score:+.3f}")

    # Cohort signal
    cohort = str(metrics.get("cohort_signal") or "").upper()
    if direction == "LONG" and "ACCUMULATING" in cohort:
        adj += BRAIN_COHORT_ALIGN_BONUS
        notes_parts.append("cohort_align+")
    elif direction == "SHORT" and "DISTRIBUTING" in cohort:
        adj += BRAIN_COHORT_ALIGN_BONUS
        notes_parts.append("cohort_align+")
    elif "ACCUMULATING" in cohort or "DISTRIBUTING" in cohort:
        adj -= BRAIN_COHORT_COUNTER_PENALTY
        notes_parts.append("cohort_counter-")

    # Liquidation edge
    fragile_count = int(_sf(metrics.get("fragile_count")))
    fragile_notional = _sf(metrics.get("fragile_notional"))
    if fragile_count > 10 and fragile_notional > 500000:
        adj += BRAIN_LIQ_EDGE_BONUS
        notes_parts.append("liq_edge+")

    # Volatility fit
    atr_pct = _sf(metrics.get("atr_pct"))
    if atr_pct > 0:
        if atr_pct < BRAIN_ATR_AVOID_MAX or atr_pct > BRAIN_ATR_VOLATILE_MAX:
            adj -= BRAIN_VOL_AVOID_PENALTY
            notes_parts.append(f"vol_avoid(atr={atr_pct:.2f})")

    # Strong signal boosts
    strong_signals = []
    for sn, sd in signals.items():
        if not isinstance(sd, dict):
            continue
        sd_dir = _normalize_dir(sd.get("direction"))
        if sd_dir != direction:
            continue
        if sn in ("whale", "dead_capital"):
            s_val = _sf(sd.get("strength"))
            if s_val >= BRAIN_STRONG_Z_THRESHOLD:
                strong_signals.append(sn)
        else:
            z_val = abs(_sf(sd.get("z_score")))
            if sn == "cvd":
                z_val = max(z_val, abs(_sf(sd.get("z_smart"))), abs(_sf(sd.get("z_dumb"))))
            if z_val >= BRAIN_STRONG_Z_THRESHOLD * BRAIN_Z_DENOM / BRAIN_STRENGTH_Z_MULT:
                strong_signals.append(sn)

    strong_dead = "dead_capital" in strong_signals
    strong_whale = "whale" in strong_signals
    strong_other = any(s not in ("dead_capital", "whale") for s in strong_signals)

    if strong_whale:
        adj += BRAIN_STRONG_WHALE_BONUS
        notes_parts.append("strong_whale+")
    elif strong_other:
        adj += BRAIN_STRONG_OTHER_BONUS
        notes_parts.append("strong_other+")

    if strong_dead:
        adj = max(adj, BRAIN_HIGH_THRESHOLD, BRAIN_STRONG_DEAD_MIN_CONVICTION)
        notes_parts.append("strong_dead_floor")

    adj = max(0.0, min(1.0, adj))

    # Would brain trade?
    would_trade = adj >= BRAIN_BASE_THRESHOLD
    if strong_dead:
        would_trade = True  # strong dead always trades

    return base, adj, "; ".join(notes_parts), would_trade


# ---------------------------------------------------------------------------
# Load trades
# ---------------------------------------------------------------------------
def load_trades(db_path: str) -> List[Dict[str, Any]]:
    db = sqlite3.connect(db_path)
    db.row_factory = sqlite3.Row
    rows = db.execute("""
        SELECT
            id, symbol, direction, venue, state,
            entry_time, entry_price, size, notional_usd,
            exit_time, exit_price, exit_reason,
            realized_pnl, realized_pnl_pct, total_fees,
            sl_price, tp_price,
            signals_snapshot, context_snapshot,
            confidence, size_multiplier, ai_reasoning,
            strategy, mae_pct, mfe_pct
        FROM trades
        WHERE signals_snapshot IS NOT NULL
          AND context_snapshot IS NOT NULL
          AND realized_pnl IS NOT NULL
          AND exit_reason NOT IN ('EXTERNAL', 'RECONCILE', 'MANUAL')
          AND direction IN ('LONG', 'SHORT')
        ORDER BY entry_time ASC
    """).fetchall()
    db.close()

    trades = []
    for r in rows:
        try:
            sigs = json.loads(r["signals_snapshot"]) if r["signals_snapshot"] else {}
            ctx = json.loads(r["context_snapshot"]) if r["context_snapshot"] else {}
        except (json.JSONDecodeError, TypeError):
            continue
        if not isinstance(sigs, dict) or not isinstance(ctx, dict):
            continue

        trades.append({
            "id": r["id"],
            "symbol": r["symbol"],
            "direction": r["direction"],
            "venue": r["venue"],
            "state": r["state"],
            "entry_time": r["entry_time"],
            "entry_price": _sf(r["entry_price"]),
            "exit_price": _sf(r["exit_price"]),
            "size": _sf(r["size"]),
            "notional_usd": _sf(r["notional_usd"]),
            "exit_reason": r["exit_reason"] or "",
            "pnl": _sf(r["realized_pnl"]),
            "pnl_pct": _sf(r["realized_pnl_pct"]),
            "fees": _sf(r["total_fees"]),
            "pipeline_conviction": _sf(r["confidence"]),
            "raw_score": _sf(ctx.get("score")),
            "signals": sigs,
            "key_metrics": ctx.get("key_metrics") or {},
            "strategy": r["strategy"] or "perp",
            "mae_pct": _sf(r["mae_pct"]),
            "mfe_pct": _sf(r["mfe_pct"]),
            "sl_price": _sf(r["sl_price"]),
            "tp_price": _sf(r["tp_price"]),
        })

    return trades


# ---------------------------------------------------------------------------
# Metrics calculator
# ---------------------------------------------------------------------------
@dataclass
class BucketMetrics:
    label: str
    trades: List[Dict[str, Any]] = field(default_factory=list)

    @property
    def n(self) -> int:
        return len(self.trades)

    @property
    def winners(self) -> List[Dict]:
        return [t for t in self.trades if t["pnl"] > 0]

    @property
    def losers(self) -> List[Dict]:
        return [t for t in self.trades if t["pnl"] <= 0]

    @property
    def longs(self) -> List[Dict]:
        return [t for t in self.trades if t["direction"] == "LONG"]

    @property
    def shorts(self) -> List[Dict]:
        return [t for t in self.trades if t["direction"] == "SHORT"]

    def _pnl_list(self, subset=None) -> List[float]:
        return [t["pnl"] for t in (subset or self.trades)]

    def calc(self) -> Dict[str, Any]:
        if not self.trades:
            return {"label": self.label, "total_trades": 0}

        pnls = self._pnl_list()
        win_pnls = self._pnl_list(self.winners)
        lose_pnls = self._pnl_list(self.losers)
        long_pnls = self._pnl_list(self.longs)
        short_pnls = self._pnl_list(self.shorts)
        fees = sum(t["fees"] for t in self.trades)

        avg_win = sum(win_pnls) / len(win_pnls) if win_pnls else 0
        avg_loss = sum(lose_pnls) / len(lose_pnls) if lose_pnls else 0

        # Win rate
        win_rate = len(self.winners) / self.n if self.n else 0

        # Profit factor
        gross_profit = sum(win_pnls) if win_pnls else 0
        gross_loss = abs(sum(lose_pnls)) if lose_pnls else 0
        pf = gross_profit / gross_loss if gross_loss > 0 else float("inf") if gross_profit > 0 else 0

        # Expectancy
        expectancy = sum(pnls) / self.n if self.n else 0

        # Max drawdown (sequential)
        peak = 0.0
        dd = 0.0
        max_dd = 0.0
        cum = 0.0
        for p in pnls:
            cum += p
            if cum > peak:
                peak = cum
            dd = peak - cum
            if dd > max_dd:
                max_dd = dd

        # Avg MAE/MFE
        maes = [t["mae_pct"] for t in self.trades if t["mae_pct"]]
        mfes = [t["mfe_pct"] for t in self.trades if t["mfe_pct"]]

        # Conviction stats
        convictions = [t.get("brain_adj_conviction", 0) for t in self.trades]
        pipeline_convs = [t.get("pipeline_conviction", 0) for t in self.trades]

        # By exit reason
        exit_reasons: Dict[str, int] = {}
        for t in self.trades:
            er = t.get("exit_reason", "UNKNOWN")
            exit_reasons[er] = exit_reasons.get(er, 0) + 1

        # By venue
        venues: Dict[str, int] = {}
        for t in self.trades:
            v = t.get("venue", "unknown")
            venues[v] = venues.get(v, 0) + 1

        # Consecutive losses
        max_consec_loss = 0
        cur_consec = 0
        for p in pnls:
            if p <= 0:
                cur_consec += 1
                max_consec_loss = max(max_consec_loss, cur_consec)
            else:
                cur_consec = 0

        # Avg hold time (rough, from entry/exit timestamps)
        hold_times = []
        for t in self.trades:
            et = _sf(t.get("entry_time"))
            xt = _sf(t.get("exit_time") if t.get("exit_time") else 0)
            if et > 0 and xt > et:
                hold_times.append((xt - et) / 3600.0)

        return {
            "label": self.label,
            "total_trades": self.n,
            "winners": len(self.winners),
            "losers": len(self.losers),
            "win_rate": win_rate,
            "net_pnl": sum(pnls),
            "gross_profit": gross_profit,
            "gross_loss": -gross_loss,
            "total_fees": fees,
            "profit_factor": pf,
            "expectancy": expectancy,
            "avg_win": avg_win,
            "avg_loss": avg_loss,
            "best_trade": max(pnls) if pnls else 0,
            "worst_trade": min(pnls) if pnls else 0,
            "max_drawdown": max_dd,
            "max_consec_losses": max_consec_loss,
            "longs": len(self.longs),
            "shorts": len(self.shorts),
            "long_pnl": sum(long_pnls),
            "short_pnl": sum(short_pnls),
            "long_win_rate": sum(1 for p in long_pnls if p > 0) / len(long_pnls) if long_pnls else 0,
            "short_win_rate": sum(1 for p in short_pnls if p > 0) / len(short_pnls) if short_pnls else 0,
            "avg_mae_pct": sum(maes) / len(maes) if maes else 0,
            "avg_mfe_pct": sum(mfes) / len(mfes) if mfes else 0,
            "avg_conviction_brain": sum(convictions) / len(convictions) if convictions else 0,
            "avg_conviction_pipeline": sum(pipeline_convs) / len(pipeline_convs) if pipeline_convs else 0,
            "avg_hold_hours": sum(hold_times) / len(hold_times) if hold_times else 0,
            "exit_reasons": exit_reasons,
            "venues": venues,
        }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    print(f"Loading trades from {DB_PATH}...")
    trades = load_trades(DB_PATH)
    print(f"Loaded {len(trades)} trades with signals + PnL data.")
    if not trades:
        print("No trades to analyze.")
        return

    # Calculate brain conviction for each trade
    for t in trades:
        base, adj, notes, would_trade = calc_brain_conviction(
            t["signals"], t["key_metrics"], t["direction"]
        )
        t["brain_base_conviction"] = base
        t["brain_adj_conviction"] = adj
        t["brain_notes"] = notes
        t["brain_would_trade"] = would_trade

    # --- Build buckets ---
    # Bucket 1: ALL trades (actual — what AGI Flow took)
    actual = BucketMetrics(label="ACTUAL (all trades taken)")
    actual.trades = trades

    # Bucket 2: Brain-only filter (brain conviction >= 0.55)
    brain_only = BucketMetrics(label="BRAIN FILTER (conviction >= 0.55)")
    brain_only.trades = [t for t in trades if t["brain_would_trade"]]

    # Bucket 3: Brain-rejected (trades the brain would NOT have taken)
    brain_rejected = BucketMetrics(label="BRAIN REJECTED (conviction < 0.55)")
    brain_rejected.trades = [t for t in trades if not t["brain_would_trade"]]

    # Bucket 4: High conviction brain (>= 0.75)
    brain_high = BucketMetrics(label="BRAIN HIGH CONVICTION (>= 0.75)")
    brain_high.trades = [t for t in trades if t["brain_adj_conviction"] >= BRAIN_HIGH_THRESHOLD]

    # Bucket 5: Pipeline high conviction (>= 0.60)
    pipeline_high = BucketMetrics(label="PIPELINE HIGH (confidence >= 0.60)")
    pipeline_high.trades = [t for t in trades if t["pipeline_conviction"] >= 0.60]

    # Bucket 6: Pipeline low conviction (< 0.40)
    pipeline_low = BucketMetrics(label="PIPELINE LOW (confidence < 0.40)")
    pipeline_low.trades = [t for t in trades if t["pipeline_conviction"] < 0.40]

    # Bucket 7: Both agree (brain would trade AND pipeline >= 0.50)
    both_agree = BucketMetrics(label="BOTH AGREE (brain>=0.55 & pipeline>=0.50)")
    both_agree.trades = [t for t in trades if t["brain_would_trade"] and t["pipeline_conviction"] >= 0.50]

    buckets = [actual, brain_only, brain_rejected, brain_high, pipeline_high, pipeline_low, both_agree]

    # --- Print results ---
    print("\n" + "=" * 120)
    print("CONVICTION BACKTEST RESULTS")
    print("=" * 120)

    metrics_list = [b.calc() for b in buckets]

    # Summary table
    header_fields = [
        ("total_trades", "Trades", "d"),
        ("winners", "Winners", "d"),
        ("losers", "Losers", "d"),
        ("win_rate", "Win Rate", ".1%"),
        ("net_pnl", "Net PnL $", ",.2f"),
        ("total_fees", "Fees $", ",.2f"),
        ("profit_factor", "PF", ".2f"),
        ("expectancy", "Expect $", ",.2f"),
        ("best_trade", "Best $", ",.2f"),
        ("worst_trade", "Worst $", ",.2f"),
        ("max_drawdown", "Max DD $", ",.2f"),
        ("max_consec_losses", "Max Consec L", "d"),
        ("longs", "Longs", "d"),
        ("shorts", "Shorts", "d"),
        ("long_pnl", "Long PnL $", ",.2f"),
        ("short_pnl", "Short PnL $", ",.2f"),
        ("long_win_rate", "Long WR", ".1%"),
        ("short_win_rate", "Short WR", ".1%"),
        ("avg_win", "Avg Win $", ",.2f"),
        ("avg_loss", "Avg Loss $", ",.2f"),
        ("avg_mae_pct", "Avg MAE %", ".2f"),
        ("avg_mfe_pct", "Avg MFE %", ".2f"),
        ("avg_conviction_brain", "Avg Brain Conv", ".3f"),
        ("avg_conviction_pipeline", "Avg Pipe Conv", ".3f"),
        ("avg_hold_hours", "Avg Hold (h)", ".1f"),
    ]

    # Print each bucket
    for m in metrics_list:
        print(f"\n{'─' * 80}")
        print(f"  {m['label']}")
        print(f"{'─' * 80}")
        if m["total_trades"] == 0:
            print("  (no trades)")
            continue
        for key, label, fmt in header_fields:
            val = m.get(key, 0)
            if isinstance(val, float) and val == float("inf"):
                print(f"  {label:20s}: INF")
            else:
                print(f"  {label:20s}: {val:{fmt}}")

        # Exit reasons
        if m.get("exit_reasons"):
            reasons = sorted(m["exit_reasons"].items(), key=lambda x: -x[1])
            reason_str = ", ".join(f"{k}={v}" for k, v in reasons)
            print(f"  {'Exit Reasons':20s}: {reason_str}")

        # Venues
        if m.get("venues"):
            venue_str = ", ".join(f"{k}={v}" for k, v in sorted(m["venues"].items(), key=lambda x: -x[1]))
            print(f"  {'Venues':20s}: {venue_str}")

    # --- Key insight: what would brain have filtered out? ---
    print(f"\n{'=' * 120}")
    print("KEY INSIGHTS")
    print(f"{'=' * 120}")

    a = metrics_list[0]  # actual
    bf = metrics_list[1]  # brain filter
    br = metrics_list[2]  # brain rejected

    if a["total_trades"] > 0:
        print(f"\n  Brain would have KEPT {bf['total_trades']}/{a['total_trades']} trades ({bf['total_trades']/a['total_trades']:.1%})")
        print(f"  Brain would have REJECTED {br['total_trades']}/{a['total_trades']} trades ({br['total_trades']/a['total_trades']:.1%})")
        print()
        print(f"  Actual net PnL:             ${a['net_pnl']:>+10,.2f}")
        print(f"  Brain-filtered net PnL:     ${bf['net_pnl']:>+10,.2f}")
        print(f"  Rejected trades' PnL:       ${br['net_pnl']:>+10,.2f}  ← this is what brain would have avoided")
        print()
        if br["total_trades"] > 0:
            if br["net_pnl"] < 0:
                print(f"  ✅ Brain filter would have SAVED ${abs(br['net_pnl']):,.2f} by rejecting {br['total_trades']} bad trades")
            else:
                print(f"  ⚠️  Brain filter would have MISSED ${br['net_pnl']:,.2f} profit from {br['total_trades']} trades")
        print()
        print(f"  Actual win rate:            {a['win_rate']:.1%}")
        print(f"  Brain-filtered win rate:    {bf['win_rate']:.1%}")
        print(f"  Rejected trades win rate:   {br['win_rate']:.1%}")

    # --- Conviction correlation with PnL ---
    print(f"\n{'=' * 120}")
    print("CONVICTION vs PnL CORRELATION")
    print(f"{'=' * 120}")

    # Bin by brain conviction ranges
    bins = [
        (0.0, 0.2, "0.00-0.20"),
        (0.2, 0.4, "0.20-0.40"),
        (0.4, 0.55, "0.40-0.55"),
        (0.55, 0.70, "0.55-0.70"),
        (0.70, 0.85, "0.70-0.85"),
        (0.85, 1.01, "0.85-1.00"),
    ]
    print(f"\n  {'Brain Conv Range':18s} {'Trades':>7s} {'Win Rate':>9s} {'Net PnL':>12s} {'Avg PnL':>10s} {'Avg Conviction':>15s}")
    print(f"  {'─'*18} {'─'*7} {'─'*9} {'─'*12} {'─'*10} {'─'*15}")
    for lo, hi, label in bins:
        subset = [t for t in trades if lo <= t["brain_adj_conviction"] < hi]
        if not subset:
            print(f"  {label:18s} {'0':>7s} {'—':>9s} {'—':>12s} {'—':>10s} {'—':>15s}")
            continue
        n = len(subset)
        wins = sum(1 for t in subset if t["pnl"] > 0)
        net = sum(t["pnl"] for t in subset)
        avg_pnl = net / n
        avg_conv = sum(t["brain_adj_conviction"] for t in subset) / n
        print(f"  {label:18s} {n:>7d} {wins/n:>8.1%} {net:>12,.2f} {avg_pnl:>10,.2f} {avg_conv:>15.3f}")

    # Same for pipeline conviction
    print(f"\n  {'Pipeline Conv Range':18s} {'Trades':>7s} {'Win Rate':>9s} {'Net PnL':>12s} {'Avg PnL':>10s}")
    print(f"  {'─'*18} {'─'*7} {'─'*9} {'─'*12} {'─'*10}")
    pbins = [
        (0.0, 0.30, "0.00-0.30"),
        (0.30, 0.45, "0.30-0.45"),
        (0.45, 0.55, "0.45-0.55"),
        (0.55, 0.70, "0.55-0.70"),
        (0.70, 1.01, "0.70-1.00"),
    ]
    for lo, hi, label in pbins:
        subset = [t for t in trades if lo <= t["pipeline_conviction"] < hi]
        if not subset:
            print(f"  {label:18s} {'0':>7s} {'—':>9s} {'—':>12s} {'—':>10s}")
            continue
        n = len(subset)
        wins = sum(1 for t in subset if t["pnl"] > 0)
        net = sum(t["pnl"] for t in subset)
        avg_pnl = net / n
        print(f"  {label:18s} {n:>7d} {wins/n:>8.1%} {net:>12,.2f} {avg_pnl:>10,.2f}")

    # --- Write CSV ---
    print(f"\n\nWriting per-trade CSV to {CSV_OUT}...")
    with open(CSV_OUT, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "trade_id", "symbol", "direction", "venue", "state",
            "entry_price", "exit_price", "pnl", "pnl_pct", "fees",
            "exit_reason", "notional_usd",
            "pipeline_conviction", "raw_score",
            "brain_base_conviction", "brain_adj_conviction", "brain_would_trade",
            "brain_notes",
            "mae_pct", "mfe_pct", "strategy",
        ])
        for t in trades:
            writer.writerow([
                t["id"], t["symbol"], t["direction"], t["venue"], t["state"],
                f'{t["entry_price"]:.6f}', f'{t["exit_price"]:.6f}',
                f'{t["pnl"]:.4f}', f'{t["pnl_pct"]:.4f}', f'{t["fees"]:.4f}',
                t["exit_reason"], f'{t["notional_usd"]:.2f}',
                f'{t["pipeline_conviction"]:.6f}', f'{t["raw_score"]:.4f}',
                f'{t["brain_base_conviction"]:.6f}', f'{t["brain_adj_conviction"]:.6f}',
                t["brain_would_trade"],
                t["brain_notes"],
                f'{t["mae_pct"]:.4f}', f'{t["mfe_pct"]:.4f}', t["strategy"],
            ])
    print(f"Done. {len(trades)} rows written.")


if __name__ == "__main__":
    main()
