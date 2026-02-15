#!/usr/bin/env python3
"""
Conviction Redesign Backtest — Compare 4 options against actual performance.

Option A: Enhanced Pipeline (pipeline × brain adjustment multipliers, no dead_floor)
Option B: Blended (0.6×pipeline + 0.4×brain_no_floor)
Option C: Signal Quality Gate (pipeline + quality filter that can reject)
Option D: Tiered (re-weighted raw score with reduced dead_cap, boosted cvd/smart)

Each option produces a conviction score. We filter at threshold and compare metrics.
"""
from __future__ import annotations
import sqlite3, json, os
from typing import Any, Dict, List, Tuple

DB_PATH = os.path.join(os.path.dirname(__file__), "ai_trader.db")

# Current weights
BRAIN_WEIGHTS = {"cvd":0.20,"dead_capital":0.32,"fade":0.08,"ofm":0.12,"hip3_main":0.70,"whale":0.28}
# Option D re-weighted
OPTION_D_WEIGHTS = {"cvd":0.35,"dead_capital":0.15,"fade":0.06,"ofm":0.15,"hip3_main":0.70,"whale":0.25}

Z_DENOM=3.0; STR_MULT=2.0

def _sf(v,d=0.0):
    try: return float(v)
    except: return d

def _nd(v):
    d=str(v or"").upper()
    return d if d in("LONG","SHORT","NEUTRAL") else "NEUTRAL"

# ── Brain base conviction (NO floor, NO strong boosts) ──
def brain_base_no_floor(sigs, met, direction, weights=None):
    """Returns (base_conviction, smart_money_factor, cohort_factor, vol_factor, agree_count, signal_count, has_primary)"""
    weights = weights or BRAIN_WEIGHTS
    direction = _nd(direction)
    tw = ws = 0.0
    sig_count = 0
    primary_present = False
    primaries = {"cvd","whale","ofm","hip3_main"}

    for sn, w in weights.items():
        sd = sigs.get(sn)
        if not sd or not isinstance(sd, dict): continue
        sd_dir = _nd(sd.get("direction"))
        z = _sf(sd.get("z_score"))
        if sn == "cvd": z = max(abs(z), abs(_sf(sd.get("z_smart"))), abs(_sf(sd.get("z_dumb"))))
        if sn in ("whale","dead_capital") and z == 0: z = _sf(sd.get("strength")) * STR_MULT
        if abs(z) < 0.01: continue  # skip zero signals
        strength = min(1.0, abs(z) / max(1e-9, Z_DENOM))
        tw += w; sig_count += 1
        if sd_dir == direction:
            ws += w * strength
            if sn in primaries: primary_present = True
        elif sd_dir != "NEUTRAL":
            ws -= w * strength * 0.5

    if tw == 0: return 0, 1.0, 1.0, 1.0, 0, 0, False
    base = ws / tw

    # Agreement count
    agree = sum(1 for s in sigs.values() if isinstance(s,dict) and _nd(s.get("direction")) == direction)
    if base > 0:
        if agree >= 4: base += 0.10
        if agree >= 5: base += 0.05
    base = max(0, min(1, base))

    # Smart money factor
    sc = _sf(met.get("smart_cvd")); dz = _sf(met.get("divergence_z"))
    smart_f = 1.0
    if direction == "LONG":
        if sc > 0 and dz > 0: smart_f = 1.0 + min(0.15, dz / 10)
        elif sc < 0: smart_f = 1.0 - min(0.15, abs(dz) / 10)
    elif direction == "SHORT":
        if sc < 0 and dz < 0: smart_f = 1.0 + min(0.15, abs(dz) / 10)
        elif sc > 0: smart_f = 1.0 - min(0.15, dz / 10)

    # Cohort factor
    co = str(met.get("cohort_signal") or "").upper()
    cohort_f = 1.0
    if (direction=="LONG" and "ACCUMULATING" in co) or (direction=="SHORT" and "DISTRIBUTING" in co):
        cohort_f = 1.15
    elif "ACCUMULATING" in co or "DISTRIBUTING" in co:
        cohort_f = 0.85

    # Volatility factor
    atr = _sf(met.get("atr_pct"))
    vol_f = 1.0
    if atr > 0 and (atr < 0.3 or atr > 6.0): vol_f = 0.85

    return base, smart_f, cohort_f, vol_f, agree, sig_count, primary_present


def option_a(pipeline_conv, sigs, met, direction):
    """Enhanced Pipeline: pipeline × brain multipliers, no dead_floor."""
    _, smart_f, cohort_f, vol_f, agree, sig_count, _ = brain_base_no_floor(sigs, met, direction)
    # Agreement factor
    agree_f = 1.0
    if agree >= 4: agree_f = 1.10
    elif agree >= 3: agree_f = 1.05
    elif agree <= 1 and sig_count >= 3: agree_f = 0.90

    conv = pipeline_conv * smart_f * cohort_f * vol_f * agree_f
    return max(0, min(1, conv))


def option_b(pipeline_conv, sigs, met, direction):
    """Blended: 0.6×pipeline + 0.4×brain_no_floor."""
    base, smart_f, cohort_f, vol_f, agree, _, _ = brain_base_no_floor(sigs, met, direction)
    # Apply adjustments to brain base
    adj = base * smart_f * cohort_f * vol_f
    adj = max(0, min(1, adj))
    conv = 0.6 * pipeline_conv + 0.4 * adj
    return max(0, min(1, conv))


def option_c(pipeline_conv, sigs, met, direction):
    """Signal Quality Gate: pipeline score but rejected if quality too low."""
    base, smart_f, cohort_f, vol_f, agree, sig_count, has_primary = brain_base_no_floor(sigs, met, direction)

    # Quality score (0-1)
    quality = 0.0

    # Signal count: 2+ aligned signals = good
    if agree >= 3: quality += 0.35
    elif agree >= 2: quality += 0.20
    elif agree >= 1: quality += 0.10

    # Primary signal present
    if has_primary: quality += 0.25

    # Smart money alignment
    if smart_f > 1.05: quality += 0.20
    elif smart_f > 0.95: quality += 0.10
    # opposing smart money = 0

    # Cohort alignment
    if cohort_f > 1.0: quality += 0.10
    elif cohort_f < 1.0: quality -= 0.05

    # Volatility ok
    if vol_f >= 1.0: quality += 0.10

    quality = max(0, min(1, quality))

    # Gate: must pass quality threshold to keep pipeline conviction
    # If quality < 0.30, reject (return 0). Otherwise scale pipeline by quality.
    if quality < 0.30:
        return 0.0
    return pipeline_conv * (0.7 + 0.3 * quality)  # scale 0.70-1.00 based on quality


def option_d(pipeline_conv, sigs, met, direction, raw_score=0.0):
    """Tiered: re-weighted conviction with reduced dead_cap, boosted cvd/smart."""
    base, smart_f, cohort_f, vol_f, agree, sig_count, has_primary = brain_base_no_floor(
        sigs, met, direction, weights=OPTION_D_WEIGHTS
    )
    # Apply adjustments
    adj = base * smart_f * cohort_f * vol_f
    if agree >= 4: adj *= 1.10
    elif agree >= 3: adj *= 1.05
    adj = max(0, min(1, adj))

    # Blend with pipeline (50/50) since we reweighted the brain
    conv = 0.5 * pipeline_conv + 0.5 * adj
    return max(0, min(1, conv))


def load_trades():
    db = sqlite3.connect(DB_PATH); db.row_factory = sqlite3.Row
    rows = db.execute("""SELECT id,symbol,direction,venue,state,entry_time,entry_price,exit_price,
        size,notional_usd,exit_time,exit_reason,realized_pnl,realized_pnl_pct,total_fees,
        signals_snapshot,context_snapshot,confidence,strategy,mae_pct,mfe_pct
        FROM trades WHERE signals_snapshot IS NOT NULL AND context_snapshot IS NOT NULL
        AND realized_pnl IS NOT NULL AND exit_reason NOT IN('EXTERNAL','RECONCILE','MANUAL')
        AND direction IN('LONG','SHORT') ORDER BY entry_time ASC""").fetchall()
    db.close()
    trades = []
    for r in rows:
        try:
            sigs = json.loads(r["signals_snapshot"]) if r["signals_snapshot"] else {}
            ctx = json.loads(r["context_snapshot"]) if r["context_snapshot"] else {}
        except: continue
        if not isinstance(sigs, dict): continue
        trades.append(dict(
            id=r["id"], symbol=r["symbol"], direction=r["direction"], venue=r["venue"],
            entry_time=_sf(r["entry_time"]), exit_time=_sf(r["exit_time"]),
            pnl=_sf(r["realized_pnl"]), pnl_pct=_sf(r["realized_pnl_pct"]),
            fees=_sf(r["total_fees"]), exit_reason=r["exit_reason"] or "",
            pipeline=_sf(r["confidence"]), raw_score=_sf(ctx.get("score")),
            signals=sigs, metrics=ctx.get("key_metrics") or {},
            mae_pct=_sf(r["mae_pct"]), mfe_pct=_sf(r["mfe_pct"]),
        ))
    return trades


def calc_stats(label, ts):
    if not ts: return dict(label=label, n=0)
    n = len(ts); pnls = [t["pnl"] for t in ts]
    wins = [t for t in ts if t["pnl"] > 0]; losses = [t for t in ts if t["pnl"] <= 0]
    longs = [t for t in ts if t["direction"] == "LONG"]; shorts = [t for t in ts if t["direction"] == "SHORT"]
    gp = sum(t["pnl"] for t in wins); gl = abs(sum(t["pnl"] for t in losses))
    fees = sum(t["fees"] for t in ts)
    peak = cum = mdd = 0
    for p in pnls:
        cum += p
        if cum > peak: peak = cum
        if peak - cum > mdd: mdd = peak - cum
    mcl = cl = 0
    for p in pnls:
        if p <= 0: cl += 1; mcl = max(mcl, cl)
        else: cl = 0
    holds = [(t["exit_time"]-t["entry_time"])/3600 for t in ts if t["exit_time"] > t["entry_time"] > 0]
    lw = sum(1 for t in longs if t["pnl"] > 0); sw = sum(1 for t in shorts if t["pnl"] > 0)
    maes = [t["mae_pct"] for t in ts if t["mae_pct"]]; mfes = [t["mfe_pct"] for t in ts if t["mfe_pct"]]
    return dict(label=label, n=n, w=len(wins), l=len(losses),
        wr=len(wins)/n, net=sum(pnls), gp=gp, gl=-gl, fees=fees,
        pf=gp/gl if gl else (float("inf") if gp else 0),
        exp=sum(pnls)/n,
        aw=gp/len(wins) if wins else 0, al=-gl/len(losses) if losses else 0,
        best=max(pnls), worst=min(pnls), mdd=mdd, mcl=mcl,
        nl=len(longs), ns=len(shorts),
        lpnl=sum(t["pnl"] for t in longs), spnl=sum(t["pnl"] for t in shorts),
        lwr=lw/len(longs) if longs else 0, swr=sw/len(shorts) if shorts else 0,
        amae=sum(maes)/len(maes) if maes else 0, amfe=sum(mfes)/len(mfes) if mfes else 0,
        ahold=sum(holds)/len(holds) if holds else 0)


def print_comparison(all_stats):
    labels = [s["label"] for s in all_stats]
    # Header
    w = 14
    print(f"\n  {'Metric':<22}", end="")
    for l in labels: print(f" {l:>{w}}", end="")
    print()
    print(f"  {'─'*22}", end="")
    for _ in labels: print(f" {'─'*w}", end="")
    print()

    def row(name, key, fmt="d", prefix=""):
        print(f"  {name:<22}", end="")
        for s in all_stats:
            if s["n"] == 0: print(f" {'—':>{w}}", end=""); continue
            v = s.get(key, 0)
            if v == float("inf"): print(f" {'INF':>{w}}", end="")
            elif fmt == "d": print(f" {prefix}{v:>{w-len(prefix)}d}", end="")
            elif fmt == ".1%": print(f" {v:>{w}.1%}", end="")
            elif fmt == "+$": print(f" ${v:>+{w-1},.0f}", end="")
            elif fmt == ".2f": print(f" {v:>{w}.2f}", end="")
            elif fmt == "$": print(f" ${v:>{w-1},.0f}", end="")
            else: print(f" {v:>{w}}", end="")
        print()

    row("Trades", "n")
    row("Winners", "w")
    row("Losers", "l")
    row("Win Rate", "wr", ".1%")
    row("Net PnL", "net", "+$")
    row("Gross Profit", "gp", "+$")
    row("Gross Loss", "gl", "+$")
    row("Fees", "fees", "$")
    row("Profit Factor", "pf", ".2f")
    row("Expectancy/trade", "exp", "+$")
    row("Avg Win", "aw", "+$")
    row("Avg Loss", "al", "+$")

    # W/L ratio
    print(f"  {'W/L Ratio':<22}", end="")
    for s in all_stats:
        if s["n"] == 0 or not s.get("al"): print(f" {'—':>{w}}", end="")
        else: print(f" {abs(s['aw']/s['al']):>{w}.2f}", end="")
    print()

    row("Best Trade", "best", "+$")
    row("Worst Trade", "worst", "+$")
    row("Max Drawdown", "mdd", "$")
    row("Max Consec Losses", "mcl")
    row("Longs", "nl")
    row("Shorts", "ns")
    row("Long PnL", "lpnl", "+$")
    row("Short PnL", "spnl", "+$")
    row("Long WR", "lwr", ".1%")
    row("Short WR", "swr", ".1%")
    row("Avg MAE %", "amae", ".2f")
    row("Avg MFE %", "amfe", ".2f")
    row("Avg Hold (h)", "ahold", ".2f")


def main():
    trades = load_trades()
    print(f"Loaded {len(trades)} trades\n")

    # Calculate all options for each trade
    for t in trades:
        s, m, d = t["signals"], t["metrics"], t["direction"]
        t["opt_a"] = option_a(t["pipeline"], s, m, d)
        t["opt_b"] = option_b(t["pipeline"], s, m, d)
        t["opt_c"] = option_c(t["pipeline"], s, m, d)
        t["opt_d"] = option_d(t["pipeline"], s, m, d, t["raw_score"])

    # ═══ FIND OPTIMAL THRESHOLDS ═══
    print("=" * 120)
    print("  THRESHOLD SWEEP — Finding best threshold for each option")
    print("=" * 120)

    thresholds = [0.25, 0.30, 0.35, 0.40, 0.45, 0.50, 0.55, 0.60]
    options = [
        ("Pipeline", "pipeline"),
        ("Opt A", "opt_a"),
        ("Opt B", "opt_b"),
        ("Opt C", "opt_c"),
        ("Opt D", "opt_d"),
    ]

    print(f"\n  {'Option':<10}", end="")
    for th in thresholds: print(f"  {'≥'+str(th):>10}", end="")
    print()

    best_th = {}
    for oname, okey in options:
        print(f"\n  {oname}")
        # Trades kept
        print(f"  {'  Trades':<10}", end="")
        for th in thresholds:
            kept = [t for t in trades if t[okey] >= th]
            print(f"  {len(kept):>10}", end="")
        print()
        # Net PnL
        print(f"  {'  Net PnL':<10}", end="")
        best_pnl = -999999; best_t = 0
        for th in thresholds:
            kept = [t for t in trades if t[okey] >= th]
            net = sum(t["pnl"] for t in kept) if kept else 0
            print(f"  ${net:>+9,.0f}", end="")
            if net > best_pnl: best_pnl = net; best_t = th
        print()
        # Win Rate
        print(f"  {'  WR':<10}", end="")
        for th in thresholds:
            kept = [t for t in trades if t[okey] >= th]
            wr = sum(1 for t in kept if t["pnl"] > 0) / len(kept) if kept else 0
            print(f"  {wr:>9.1%}", end="")
        print()
        # PF
        print(f"  {'  PF':<10}", end="")
        for th in thresholds:
            kept = [t for t in trades if t[okey] >= th]
            gp = sum(t["pnl"] for t in kept if t["pnl"] > 0)
            gl = abs(sum(t["pnl"] for t in kept if t["pnl"] <= 0))
            pf = gp/gl if gl else 0
            print(f"  {pf:>10.2f}", end="")
        print()
        # Max DD
        print(f"  {'  MaxDD':<10}", end="")
        for th in thresholds:
            kept = [t for t in trades if t[okey] >= th]
            peak = cum = mdd = 0
            for t in kept:
                cum += t["pnl"]
                if cum > peak: peak = cum
                if peak-cum > mdd: mdd = peak-cum
            print(f"  ${mdd:>9,.0f}", end="")
        print()

        best_th[oname] = best_t

    # ═══ BEST THRESHOLD COMPARISON ═══
    print(f"\n\n{'=' * 120}")
    print("  HEAD-TO-HEAD — Each option at its BEST threshold")
    print("=" * 120)
    print(f"  Best thresholds: {', '.join(f'{k}≥{v}' for k,v in best_th.items())}")

    results = []
    # Actual (no filter)
    results.append(calc_stats("ACTUAL", trades))
    for oname, okey in options:
        th = best_th[oname]
        kept = [t for t in trades if t[okey] >= th]
        results.append(calc_stats(f"{oname}≥{th}", kept))

    print_comparison(results)

    # ═══ SAME THRESHOLD COMPARISON (0.45 for all) ═══
    print(f"\n\n{'=' * 120}")
    print("  FAIR COMPARISON — All options at threshold ≥ 0.45")
    print("=" * 120)

    results2 = [calc_stats("ACTUAL", trades)]
    for oname, okey in options:
        kept = [t for t in trades if t[okey] >= 0.45]
        results2.append(calc_stats(f"{oname}≥.45", kept))
    print_comparison(results2)

    # ═══ SAME THRESHOLD 0.40 ═══
    print(f"\n\n{'=' * 120}")
    print("  FAIR COMPARISON — All options at threshold ≥ 0.40")
    print("=" * 120)

    results3 = [calc_stats("ACTUAL", trades)]
    for oname, okey in options:
        kept = [t for t in trades if t[okey] >= 0.40]
        results3.append(calc_stats(f"{oname}≥.40", kept))
    print_comparison(results3)

    # ═══ CONVICTION SPREAD (how well does each option discriminate?) ═══
    print(f"\n\n{'=' * 120}")
    print("  CONVICTION SPREAD — Distribution quality")
    print("=" * 120)

    for oname, okey in options:
        vals = [t[okey] for t in trades]
        vals_nz = [v for v in vals if v > 0.01]
        if not vals_nz:
            print(f"\n  {oname}: all zero")
            continue
        avg = sum(vals_nz)/len(vals_nz)
        mn = min(vals_nz); mx = max(vals_nz)
        std = (sum((v-avg)**2 for v in vals_nz)/len(vals_nz))**0.5
        # Quartiles
        sv = sorted(vals_nz)
        q1 = sv[len(sv)//4]; q2 = sv[len(sv)//2]; q3 = sv[3*len(sv)//4]
        # How many unique 0.1 bins have trades?
        bins_used = len(set(int(v*10) for v in vals_nz))
        print(f"\n  {oname}:")
        print(f"    N={len(vals_nz)} (zeros={len(vals)-len(vals_nz)})  Range: {mn:.3f}–{mx:.3f}  Std: {std:.3f}")
        print(f"    Q1={q1:.3f}  Median={q2:.3f}  Q3={q3:.3f}  Mean={avg:.3f}")
        print(f"    Bins used (of 10): {bins_used}  ← higher = better discrimination")

    # ═══ REJECTED TRADE ANALYSIS ═══
    print(f"\n\n{'=' * 120}")
    print("  FILTER QUALITY — What does each option reject? (at threshold 0.45)")
    print("=" * 120)

    for oname, okey in options:
        rejected = [t for t in trades if t[okey] < 0.45]
        kept = [t for t in trades if t[okey] >= 0.45]
        if not rejected:
            print(f"\n  {oname}: rejects 0 trades")
            continue
        r_wins = [t for t in rejected if t["pnl"] > 0]
        r_losses = [t for t in rejected if t["pnl"] <= 0]
        saved = abs(sum(t["pnl"] for t in r_losses))
        missed = sum(t["pnl"] for t in r_wins)
        net_benefit = saved - missed
        print(f"\n  {oname} (rejects {len(rejected)}/{len(trades)}):")
        print(f"    💡 Saved from {len(r_losses)} losers: ${saved:>+,.0f}")
        print(f"    ⚠️  Missed {len(r_wins)} winners: ${missed:>+,.0f}")
        print(f"    Net benefit: ${net_benefit:>+,.0f}")
        print(f"    Rejected WR: {len(r_wins)/len(rejected):.1%} vs Kept WR: {sum(1 for t in kept if t['pnl']>0)/len(kept):.1%}" if kept else "")
        print(f"    Rejected PF: {sum(t['pnl'] for t in r_wins)/saved:.2f}" if saved else "")


if __name__ == "__main__":
    main()
