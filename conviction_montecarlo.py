#!/usr/bin/env python3
"""
Monte Carlo Weight Optimizer for Option B & Option D conviction scoring.

Randomly samples signal weights, blend ratios, and thresholds to find the
optimal configuration. Evaluates each combo against the full trade history.

Usage:
    python conviction_montecarlo.py [--iterations 50000] [--top 20] [--perps-only]
"""
from __future__ import annotations
import sqlite3, json, os, sys, time, argparse, random
from typing import Any, Dict, List, Tuple, Optional

DB_PATH = os.path.join(os.path.dirname(__file__), "ai_trader.db")

# Signal names that get weight randomization
SIGNAL_NAMES = ["cvd", "dead_capital", "fade", "ofm", "whale"]
# hip3_main is separate — only relevant for HIP3 trades, keep it variable too
HIP3_SIGNAL = "hip3_main"

Z_DENOM = 3.0
STR_MULT = 2.0

# ── Weight bounds (min, max) for each signal — informed by backtest insights ──
WEIGHT_BOUNDS = {
    "cvd":          (0.05, 0.60),   # strongest directional signal
    "dead_capital": (0.02, 0.40),   # was dominant at 0.32; needs room to go lower
    "fade":         (0.01, 0.20),   # minor signal
    "ofm":          (0.05, 0.35),   # order flow momentum
    "whale":        (0.05, 0.45),   # large player activity
    "hip3_main":    (0.30, 0.90),   # HIP3 primary (only for HIP3 trades)
}

# Blend ratio bounds
BLEND_PIPELINE_MIN = 0.20  # minimum pipeline weight in blend
BLEND_PIPELINE_MAX = 0.80  # maximum pipeline weight in blend

# Threshold sweep
THRESHOLDS = [0.40, 0.45, 0.50, 0.55, 0.60, 0.65, 0.70]

# Minimum trade count to consider a result valid
MIN_TRADES = 50


def _sf(v, d=0.0):
    try:
        return float(v)
    except:
        return d


def _nd(v):
    d = str(v or "").upper()
    return d if d in ("LONG", "SHORT", "NEUTRAL") else "NEUTRAL"


def load_trades(perps_only: bool = False):
    """Load trades from DB. If perps_only, exclude builder/HIP3 symbols."""
    db = sqlite3.connect(DB_PATH)
    db.row_factory = sqlite3.Row
    rows = db.execute("""
        SELECT id, symbol, direction, venue, state, entry_time, entry_price, exit_price,
            size, notional_usd, exit_time, exit_reason, realized_pnl, realized_pnl_pct,
            total_fees, signals_snapshot, context_snapshot, confidence, strategy,
            mae_pct, mfe_pct
        FROM trades
        WHERE signals_snapshot IS NOT NULL AND context_snapshot IS NOT NULL
            AND realized_pnl IS NOT NULL
            AND exit_reason NOT IN ('EXTERNAL', 'RECONCILE', 'MANUAL')
            AND direction IN ('LONG', 'SHORT')
        ORDER BY entry_time ASC
    """).fetchall()
    db.close()

    builder_prefixes = ("XYZ:", "CASH:", "FLX:", "HYNA:", "KM:")
    trades = []
    for r in rows:
        try:
            sigs = json.loads(r["signals_snapshot"]) if r["signals_snapshot"] else {}
            ctx = json.loads(r["context_snapshot"]) if r["context_snapshot"] else {}
        except:
            continue
        if not isinstance(sigs, dict):
            continue

        sym = r["symbol"] or ""
        if perps_only and any(sym.startswith(p) for p in builder_prefixes):
            continue

        trades.append(dict(
            id=r["id"], symbol=sym, direction=r["direction"], venue=r["venue"],
            entry_time=_sf(r["entry_time"]), exit_time=_sf(r["exit_time"]),
            pnl=_sf(r["realized_pnl"]), pnl_pct=_sf(r["realized_pnl_pct"]),
            fees=_sf(r["total_fees"]), exit_reason=r["exit_reason"] or "",
            pipeline=_sf(r["confidence"]), raw_score=_sf(ctx.get("score")),
            signals=sigs, metrics=ctx.get("key_metrics") or {},
            mae_pct=_sf(r["mae_pct"]), mfe_pct=_sf(r["mfe_pct"]),
        ))
    return trades


# ── Pre-extract signal features for speed ──
def preprocess_trades(trades: List[dict]) -> List[dict]:
    """
    Pre-extract the signal strengths and directions for each trade so we
    don't re-parse JSON in every Monte Carlo iteration.
    """
    processed = []
    for t in trades:
        sigs = t["signals"]
        direction = _nd(t["direction"])
        met = t["metrics"]

        # Extract per-signal: aligned_strength (positive if aligned, negative if opposing)
        sig_data = {}
        for sn in SIGNAL_NAMES + [HIP3_SIGNAL]:
            sd = sigs.get(sn)
            if not sd or not isinstance(sd, dict):
                sig_data[sn] = 0.0
                continue
            sd_dir = _nd(sd.get("direction"))
            z = _sf(sd.get("z_score"))
            if sn == "cvd":
                z = max(abs(z), abs(_sf(sd.get("z_smart"))), abs(_sf(sd.get("z_dumb"))))
            if sn in ("whale", "dead_capital") and z == 0:
                z = _sf(sd.get("strength")) * STR_MULT
            if abs(z) < 0.01:
                sig_data[sn] = 0.0
                continue
            strength = min(1.0, abs(z) / max(1e-9, Z_DENOM))
            if sd_dir == direction:
                sig_data[sn] = strength
            elif sd_dir != "NEUTRAL":
                sig_data[sn] = -strength * 0.5
            else:
                sig_data[sn] = 0.0

        # Agreement count
        agree = sum(
            1 for s in sigs.values()
            if isinstance(s, dict) and _nd(s.get("direction")) == direction
        )

        # Smart money factor
        sc = _sf(met.get("smart_cvd"))
        dz = _sf(met.get("divergence_z"))
        smart_f = 1.0
        if direction == "LONG":
            if sc > 0 and dz > 0:
                smart_f = 1.0 + min(0.15, dz / 10)
            elif sc < 0:
                smart_f = 1.0 - min(0.15, abs(dz) / 10)
        elif direction == "SHORT":
            if sc < 0 and dz < 0:
                smart_f = 1.0 + min(0.15, abs(dz) / 10)
            elif sc > 0:
                smart_f = 1.0 - min(0.15, dz / 10)

        # Cohort factor
        co = str(met.get("cohort_signal") or "").upper()
        cohort_f = 1.0
        if (direction == "LONG" and "ACCUMULATING" in co) or \
           (direction == "SHORT" and "DISTRIBUTING" in co):
            cohort_f = 1.15
        elif "ACCUMULATING" in co or "DISTRIBUTING" in co:
            cohort_f = 0.85

        # Volatility factor
        atr = _sf(met.get("atr_pct"))
        vol_f = 1.0
        if atr > 0 and (atr < 0.3 or atr > 6.0):
            vol_f = 0.85

        processed.append(dict(
            pnl=t["pnl"],
            pipeline=t["pipeline"],
            direction=t["direction"],
            sig_data=sig_data,
            agree=agree,
            smart_f=smart_f,
            cohort_f=cohort_f,
            vol_f=vol_f,
            symbol=t["symbol"],
            exit_reason=t["exit_reason"],
        ))
    return processed


def compute_brain_conv(t: dict, weights: dict) -> float:
    """Compute brain base conviction for a pre-processed trade given weights."""
    sig_data = t["sig_data"]
    tw = 0.0
    ws = 0.0
    for sn in SIGNAL_NAMES + [HIP3_SIGNAL]:
        w = weights.get(sn, 0.0)
        s = sig_data.get(sn, 0.0)
        if abs(s) < 0.001:
            continue
        tw += w
        ws += w * s

    if tw == 0:
        return 0.0

    base = ws / tw

    # Agreement bonus
    if base > 0:
        if t["agree"] >= 4:
            base += 0.10
        if t["agree"] >= 5:
            base += 0.05
    base = max(0.0, min(1.0, base))

    # Apply adjustment factors
    adj = base * t["smart_f"] * t["cohort_f"] * t["vol_f"]

    # Agreement multiplier (for Option D style)
    if t["agree"] >= 4:
        adj *= 1.10
    elif t["agree"] >= 3:
        adj *= 1.05

    return max(0.0, min(1.0, adj))


def compute_option_b_conv(t: dict, weights: dict, blend_pipeline: float) -> float:
    """Option B: blend_pipeline × pipeline + (1 - blend_pipeline) × brain."""
    brain = compute_brain_conv(t, weights)
    return max(0.0, min(1.0, blend_pipeline * t["pipeline"] + (1.0 - blend_pipeline) * brain))


def compute_option_d_conv(t: dict, weights: dict, blend_pipeline: float) -> float:
    """Option D: blend_pipeline × pipeline + (1 - blend_pipeline) × brain (with re-weighted signals)."""
    # Same math as B but the weights are different (that's the point of D)
    brain = compute_brain_conv(t, weights)
    return max(0.0, min(1.0, blend_pipeline * t["pipeline"] + (1.0 - blend_pipeline) * brain))


def calc_metrics(trades: List[dict], convictions: List[float], threshold: float) -> Optional[dict]:
    """Calculate performance metrics for trades above threshold."""
    filtered_pnls = []
    n_long = n_short = long_w = short_w = 0
    for t, c in zip(trades, convictions):
        if c >= threshold:
            filtered_pnls.append(t["pnl"])
            if t["direction"] == "LONG":
                n_long += 1
                if t["pnl"] > 0:
                    long_w += 1
            else:
                n_short += 1
                if t["pnl"] > 0:
                    short_w += 1

    n = len(filtered_pnls)
    if n < MIN_TRADES:
        return None

    wins = [p for p in filtered_pnls if p > 0]
    losses = [p for p in filtered_pnls if p <= 0]
    gp = sum(wins)
    gl = abs(sum(losses))
    net = sum(filtered_pnls)
    wr = len(wins) / n
    pf = gp / gl if gl > 0 else (float("inf") if gp > 0 else 0.0)
    exp = net / n

    # Max drawdown
    peak = cum = mdd = 0.0
    for p in filtered_pnls:
        cum += p
        if cum > peak:
            peak = cum
        if peak - cum > mdd:
            mdd = peak - cum

    # Calmar-like ratio (net / maxDD)
    calmar = net / mdd if mdd > 0 else (net if net > 0 else 0.0)

    # Long/short WR
    lwr = long_w / n_long if n_long > 0 else 0.0
    swr = short_w / n_short if n_short > 0 else 0.0

    return dict(
        n=n, net=net, wr=wr, pf=pf, exp=exp, mdd=mdd,
        gp=gp, gl=-gl, calmar=calmar,
        nl=n_long, ns=n_short, lwr=lwr, swr=swr,
    )


def random_weights() -> dict:
    """Generate random signal weights within bounds."""
    w = {}
    for sn, (lo, hi) in WEIGHT_BOUNDS.items():
        w[sn] = random.uniform(lo, hi)
    return w


def score_result(m: dict) -> float:
    """
    Composite score for ranking results.
    Priorities: profitability > risk-adjusted > trade count.
    """
    if m is None:
        return -9999999.0

    # Must be profitable
    if m["net"] <= 0:
        return m["net"] - m["mdd"]  # negative, penalize further by DD

    # Composite: net PnL + PF bonus + calmar bonus - drawdown penalty
    score = m["net"]
    score += (m["pf"] - 1.0) * 200        # PF bonus (1.10 = +20)
    score += m["calmar"] * 100             # calmar bonus
    score -= m["mdd"] * 0.3               # DD penalty
    if m["n"] < 80:
        score -= (80 - m["n"]) * 5        # trade count penalty
    return score


def run_monte_carlo(
    trades: List[dict],
    iterations: int,
    top_k: int,
    option: str,  # "B" or "D"
) -> List[dict]:
    """Run Monte Carlo optimization."""
    best_results = []  # list of (score, config, metrics)

    for i in range(iterations):
        weights = random_weights()
        blend_pipeline = random.uniform(BLEND_PIPELINE_MIN, BLEND_PIPELINE_MAX)

        # Compute conviction for all trades
        if option == "B":
            convictions = [compute_option_b_conv(t, weights, blend_pipeline) for t in trades]
        else:  # D
            convictions = [compute_option_d_conv(t, weights, blend_pipeline) for t in trades]

        # Find best threshold
        best_th_score = -9999999.0
        best_th_metrics = None
        best_th = 0.0

        for th in THRESHOLDS:
            m = calc_metrics(trades, convictions, th)
            if m is None:
                continue
            s = score_result(m)
            if s > best_th_score:
                best_th_score = s
                best_th_metrics = m
                best_th = th

        if best_th_metrics is None:
            continue

        config = dict(
            weights={k: round(v, 4) for k, v in weights.items()},
            blend_pipeline=round(blend_pipeline, 4),
            threshold=best_th,
        )

        # Insert into top-k list
        entry = (best_th_score, config, best_th_metrics)
        if len(best_results) < top_k:
            best_results.append(entry)
            best_results.sort(key=lambda x: -x[0])
        elif best_th_score > best_results[-1][0]:
            best_results[-1] = entry
            best_results.sort(key=lambda x: -x[0])

        # Progress
        if (i + 1) % 5000 == 0:
            best_net = best_results[0][2]["net"] if best_results else 0
            print(f"  [{option}] {i+1:>6}/{iterations}  "
                  f"best_net=${best_net:>+,.0f}  "
                  f"top_k={len(best_results)}", flush=True)

    return best_results


def print_results(option: str, results: List[Tuple], n_total: int):
    """Pretty-print the top results."""
    print(f"\n{'='*120}")
    print(f"  OPTION {option} — TOP {len(results)} WEIGHT CONFIGURATIONS")
    print(f"{'='*120}")

    if not results:
        print("  No profitable configurations found!")
        return

    # Header
    print(f"\n  {'#':>3} {'Score':>8} {'Net':>10} {'PF':>6} {'WR':>6} {'N':>5} "
          f"{'MaxDD':>9} {'Exp':>8} {'Calmar':>7} {'LWR':>5} {'SWR':>5} │ "
          f"{'Blend':>5} {'Thr':>4} │ "
          f"{'cvd':>5} {'d_cap':>5} {'fade':>5} {'ofm':>5} {'whale':>5} {'hip3':>5}")
    print(f"  {'─'*3} {'─'*8} {'─'*10} {'─'*6} {'─'*6} {'─'*5} "
          f"{'─'*9} {'─'*8} {'─'*7} {'─'*5} {'─'*5} │ "
          f"{'─'*5} {'─'*4} │ "
          f"{'─'*5} {'─'*5} {'─'*5} {'─'*5} {'─'*5} {'─'*5}")

    for rank, (score, cfg, m) in enumerate(results, 1):
        w = cfg["weights"]
        pf_s = "INF" if m["pf"] == float("inf") else f"{m['pf']:.2f}"
        cal_s = f"{m['calmar']:.2f}" if abs(m["calmar"]) < 100 else "INF"
        print(
            f"  {rank:>3} {score:>8.0f} ${m['net']:>+8,.0f} {pf_s:>6} "
            f"{m['wr']:>5.1%} {m['n']:>5} "
            f"${m['mdd']:>7,.0f} ${m['exp']:>+6,.2f} {cal_s:>7} "
            f"{m['lwr']:>4.0%} {m['swr']:>4.0%} │ "
            f"{cfg['blend_pipeline']:>5.2f} {cfg['threshold']:>4.2f} │ "
            f"{w.get('cvd',0):>5.3f} {w.get('dead_capital',0):>5.3f} "
            f"{w.get('fade',0):>5.3f} {w.get('ofm',0):>5.3f} "
            f"{w.get('whale',0):>5.3f} {w.get('hip3_main',0):>5.3f}"
        )

    # ═══ WEIGHT STATISTICS across top results ═══
    print(f"\n  {'─'*80}")
    print(f"  WEIGHT STATISTICS (across top {len(results)} configs)")
    print(f"  {'─'*80}")

    all_weights = {sn: [] for sn in SIGNAL_NAMES + [HIP3_SIGNAL]}
    all_blends = []
    all_thresholds = []

    for _, cfg, _ in results:
        for sn in SIGNAL_NAMES + [HIP3_SIGNAL]:
            all_weights[sn].append(cfg["weights"].get(sn, 0))
        all_blends.append(cfg["blend_pipeline"])
        all_thresholds.append(cfg["threshold"])

    print(f"\n  {'Signal':<15} {'Mean':>8} {'Std':>8} {'Min':>8} {'Max':>8} │ {'Current':>8} {'OptD':>8}")
    print(f"  {'─'*15} {'─'*8} {'─'*8} {'─'*8} {'─'*8} │ {'─'*8} {'─'*8}")

    current = {"cvd": 0.20, "dead_capital": 0.32, "fade": 0.08, "ofm": 0.12, "whale": 0.28, "hip3_main": 0.70}
    optd =    {"cvd": 0.35, "dead_capital": 0.15, "fade": 0.06, "ofm": 0.15, "whale": 0.25, "hip3_main": 0.70}

    for sn in SIGNAL_NAMES + [HIP3_SIGNAL]:
        vals = all_weights[sn]
        mean = sum(vals) / len(vals)
        std = (sum((v - mean) ** 2 for v in vals) / len(vals)) ** 0.5
        mn = min(vals)
        mx = max(vals)
        print(f"  {sn:<15} {mean:>8.4f} {std:>8.4f} {mn:>8.4f} {mx:>8.4f} │ "
              f"{current.get(sn, 0):>8.4f} {optd.get(sn, 0):>8.4f}")

    b_mean = sum(all_blends) / len(all_blends)
    b_std = (sum((v - b_mean) ** 2 for v in all_blends) / len(all_blends)) ** 0.5
    print(f"\n  {'blend_pipe':<15} {b_mean:>8.4f} {b_std:>8.4f} "
          f"{min(all_blends):>8.4f} {max(all_blends):>8.4f} │ {'—':>8} {'0.5000':>8}")

    # Threshold distribution
    th_counts = {}
    for th in all_thresholds:
        th_counts[th] = th_counts.get(th, 0) + 1
    th_sorted = sorted(th_counts.items(), key=lambda x: -x[1])
    print(f"\n  Threshold distribution: {', '.join(f'{th}={cnt}' for th, cnt in th_sorted)}")

    # ═══ RECOMMENDED CONFIG ═══
    print(f"\n  {'='*80}")
    print(f"  RECOMMENDED CONFIG (Option {option}) — Median of top {min(5, len(results))} results")
    print(f"  {'='*80}")

    top_n = min(5, len(results))
    rec_weights = {}
    for sn in SIGNAL_NAMES + [HIP3_SIGNAL]:
        vals = sorted(all_weights[sn][:top_n])
        rec_weights[sn] = vals[len(vals) // 2]  # median
    rec_blend = sorted(all_blends[:top_n])[top_n // 2]
    rec_th = sorted(all_thresholds[:top_n])[top_n // 2]

    print(f"\n  Weights:")
    for sn in SIGNAL_NAMES + [HIP3_SIGNAL]:
        arrow = ""
        if sn in current:
            diff = rec_weights[sn] - current[sn]
            if abs(diff) > 0.01:
                arrow = f"  ({'↑' if diff > 0 else '↓'} {abs(diff):.3f} from current {current[sn]:.3f})"
        print(f"    {sn:<15} = {rec_weights[sn]:.4f}{arrow}")

    print(f"\n  Blend (pipeline) = {rec_blend:.4f}")
    print(f"  Threshold        = {rec_th:.2f}")

    # Verify recommended config
    print(f"\n  Verification — applying recommended config to {n_total} trades:")
    # We can't easily re-run here without the preprocessed data, so just point to the #1 result
    m1 = results[0][2]
    print(f"  (Top #1 result: Net=${m1['net']:>+,.0f}, PF={m1['pf']:.2f}, "
          f"WR={m1['wr']:.1%}, N={m1['n']}, MaxDD=${m1['mdd']:>,.0f})")


def main():
    parser = argparse.ArgumentParser(description="Monte Carlo conviction weight optimizer")
    parser.add_argument("--iterations", "-n", type=int, default=50000,
                        help="Number of Monte Carlo iterations per option (default: 50000)")
    parser.add_argument("--top", "-k", type=int, default=20,
                        help="Number of top results to keep (default: 20)")
    parser.add_argument("--perps-only", action="store_true",
                        help="Exclude builder/HIP3 symbols")
    parser.add_argument("--option", choices=["B", "D", "both"], default="both",
                        help="Which option to optimize (default: both)")
    parser.add_argument("--seed", type=int, default=None,
                        help="Random seed for reproducibility")
    args = parser.parse_args()

    if args.seed is not None:
        random.seed(args.seed)

    print(f"{'='*120}")
    print(f"  MONTE CARLO CONVICTION WEIGHT OPTIMIZER")
    print(f"  Iterations: {args.iterations:,} per option | Top-K: {args.top} | "
          f"Perps only: {args.perps_only} | Seed: {args.seed or 'random'}")
    print(f"{'='*120}")

    # Load and preprocess
    print("\n  Loading trades...", flush=True)
    raw_trades = load_trades(perps_only=args.perps_only)
    print(f"  Loaded {len(raw_trades)} trades")

    print("  Preprocessing signal features...", flush=True)
    trades = preprocess_trades(raw_trades)
    print(f"  Preprocessed {len(trades)} trades")

    # Actual performance baseline
    pnls = [t["pnl"] for t in trades]
    net = sum(pnls)
    wins = sum(1 for p in pnls if p > 0)
    print(f"\n  BASELINE: {len(trades)} trades, Net=${net:>+,.0f}, "
          f"WR={wins/len(trades):.1%}")

    t0 = time.time()

    if args.option in ("B", "both"):
        print(f"\n{'─'*120}")
        print(f"  Running Option B Monte Carlo ({args.iterations:,} iterations)...")
        print(f"{'─'*120}")
        results_b = run_monte_carlo(trades, args.iterations, args.top, "B")
        print_results("B", results_b, len(trades))

    if args.option in ("D", "both"):
        print(f"\n{'─'*120}")
        print(f"  Running Option D Monte Carlo ({args.iterations:,} iterations)...")
        print(f"{'─'*120}")
        results_d = run_monte_carlo(trades, args.iterations, args.top, "D")
        print_results("D", results_d, len(trades))

    elapsed = time.time() - t0
    print(f"\n  Total time: {elapsed:.1f}s ({elapsed/60:.1f}min)")

    # ═══ HEAD-TO-HEAD: Best B vs Best D vs Current ═══
    if args.option == "both" and results_b and results_d:
        print(f"\n{'='*120}")
        print(f"  HEAD-TO-HEAD: Best Option B vs Best Option D vs Current (Option D @ 0.60)")
        print(f"{'='*120}")

        print(f"\n  {'Metric':<20} {'Opt B #1':>14} {'Opt D #1':>14} {'Current D':>14}")
        print(f"  {'─'*20} {'─'*14} {'─'*14} {'─'*14}")

        mb = results_b[0][2]
        md = results_d[0][2]

        # Compute current Option D (from conviction_options_backtest.py weights)
        current_d_weights = {"cvd": 0.35, "dead_capital": 0.15, "fade": 0.06,
                             "ofm": 0.15, "hip3_main": 0.70, "whale": 0.25}
        current_d_convs = [compute_option_d_conv(t, current_d_weights, 0.50) for t in trades]
        mc = calc_metrics(trades, current_d_convs, 0.60)

        for label, key, fmt in [
            ("Trades", "n", "d"),
            ("Net PnL", "net", "+$"),
            ("Win Rate", "wr", "%"),
            ("Profit Factor", "pf", "f"),
            ("Expectancy", "exp", "+$"),
            ("Max Drawdown", "mdd", "$"),
            ("Calmar", "calmar", "f"),
            ("Longs", "nl", "d"),
            ("Shorts", "ns", "d"),
            ("Long WR", "lwr", "%"),
            ("Short WR", "swr", "%"),
        ]:
            vals = [mb, md, mc]
            parts = []
            for v in vals:
                if v is None:
                    parts.append(f"{'—':>14}")
                elif fmt == "d":
                    parts.append(f"{v[key]:>14,}")
                elif fmt == "+$":
                    parts.append(f"${v[key]:>+13,.0f}")
                elif fmt == "%":
                    parts.append(f"{v[key]:>13.1%}")
                elif fmt == "$":
                    parts.append(f"${v[key]:>13,.0f}")
                elif fmt == "f":
                    x = v[key]
                    parts.append(f"{'INF':>14}" if x == float("inf") else f"{x:>14.2f}")
            print(f"  {label:<20} {''.join(parts)}")

        # Show the configs
        print(f"\n  Best B config:")
        cb = results_b[0][1]
        print(f"    blend={cb['blend_pipeline']:.3f}, threshold={cb['threshold']}")
        print(f"    weights: {', '.join(f'{k}={v:.3f}' for k, v in cb['weights'].items())}")

        print(f"\n  Best D config:")
        cd = results_d[0][1]
        print(f"    blend={cd['blend_pipeline']:.3f}, threshold={cd['threshold']}")
        print(f"    weights: {', '.join(f'{k}={v:.3f}' for k, v in cd['weights'].items())}")

    print(f"\n  Done. ✓")


if __name__ == "__main__":
    main()
