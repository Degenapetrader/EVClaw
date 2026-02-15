#!/usr/bin/env python3
"""
Conviction Backtest v2 — Two recalculations with full per-trade detail.

Recalc 1: Brain-only conviction (weighted signals + adjustments)
Recalc 2: Combined (pipeline × brain averaged) conviction

Displays everything to stdout — no CSV dependency.
"""

from __future__ import annotations
import sqlite3
import json
import os
import sys
from typing import Any, Dict, List, Tuple
from datetime import datetime, timezone

DB_PATH = os.path.join(os.path.dirname(__file__), "ai_trader.db")

# ── Pipeline defaults ──
SCORE_DIVISOR = 100.0
BIAS_PENALTY = 0.6
BIAS_BONUS = 1.05

# ── Brain defaults ──
BRAIN_WEIGHTS = {
    "cvd": 0.20, "dead_capital": 0.32, "fade": 0.08,
    "ofm": 0.12, "hip3_main": 0.70, "whale": 0.28,
}
Z_DENOM = 3.0
STRENGTH_MULT = 2.0
BASE_THRESH = 0.55
HIGH_THRESH = 0.75
SMART_ADJUST = 0.1
SMART_DIV_DENOM = 3.0
SMART_DIV_MULT = 0.5
COHORT_ALIGN = 0.3
COHORT_COUNTER = 0.2
LIQ_THRESH = 0.5
LIQ_BONUS = 0.05
VOL_PENALTY = 0.15
AGREE_COUNT1 = 4; AGREE_BONUS1 = 0.10
AGREE_COUNT2 = 5; AGREE_BONUS2 = 0.05
STRONG_WHALE = 0.20
STRONG_OTHER = 0.10
STRONG_DEAD_FLOOR = 0.90
STRONG_Z = 1.5
ATR_AVOID_LO = 0.3; ATR_AVOID_HI = 6.0

def _sf(v, d=0.0):
    try: return float(v)
    except: return d

def _nd(v):
    d = str(v or "").upper()
    return d if d in ("LONG","SHORT","NEUTRAL") else "NEUTRAL"


def calc_brain(signals: Dict, metrics: Dict, direction: str) -> Tuple[float, float, str, bool]:
    direction = _nd(direction)
    notes = []
    tw = 0.0; ws = 0.0

    for sn, w in BRAIN_WEIGHTS.items():
        sd = signals.get(sn)
        if not sd or not isinstance(sd, dict): continue
        sd_dir = _nd(sd.get("direction"))
        z = _sf(sd.get("z_score"))
        if sn == "cvd":
            z = max(abs(z), abs(_sf(sd.get("z_smart"))), abs(_sf(sd.get("z_dumb"))))
        if sn in ("whale","dead_capital") and z == 0:
            z = _sf(sd.get("strength")) * STRENGTH_MULT
        strength = min(1.0, abs(z) / max(1e-9, Z_DENOM))
        tw += w
        if sd_dir == direction: ws += w * strength
        elif sd_dir != "NEUTRAL": ws -= w * strength * 0.5

    if tw == 0: return 0, 0, "no_signals", False
    base = ws / tw

    if base > 0:
        ac = sum(1 for s in signals.values() if isinstance(s,dict) and _nd(s.get("direction")) == direction)
        if ac >= AGREE_COUNT1: base += AGREE_BONUS1
        if ac >= AGREE_COUNT2: base += AGREE_BONUS2
        notes.append(f"agree={ac}")
    base = max(0, min(1, base))

    adj = base
    # Smart money
    sc = _sf(metrics.get("smart_cvd")); dz = _sf(metrics.get("divergence_z"))
    sm = 0
    if direction == "LONG":
        if sc > 0 and dz > 0: sm = min(1, dz/SMART_DIV_DENOM)*SMART_DIV_MULT
        elif sc < 0: sm = -min(1, abs(dz)/SMART_DIV_DENOM)*SMART_DIV_MULT
    elif direction == "SHORT":
        if sc < 0 and dz < 0: sm = min(1, abs(dz)/SMART_DIV_DENOM)*SMART_DIV_MULT
        elif sc > 0: sm = -min(1, dz/SMART_DIV_DENOM)*SMART_DIV_MULT
    adj += sm * SMART_ADJUST
    if sm: notes.append(f"smart={sm:+.2f}")

    # Cohort
    co = str(metrics.get("cohort_signal") or "").upper()
    if (direction=="LONG" and "ACCUMULATING" in co) or (direction=="SHORT" and "DISTRIBUTING" in co):
        adj += COHORT_ALIGN; notes.append("cohort+")
    elif "ACCUMULATING" in co or "DISTRIBUTING" in co:
        adj -= COHORT_COUNTER; notes.append("cohort-")

    # Liq edge
    if int(_sf(metrics.get("fragile_count"))) > 10 and _sf(metrics.get("fragile_notional")) > 500000:
        adj += LIQ_BONUS; notes.append("liq+")

    # Vol
    atr = _sf(metrics.get("atr_pct"))
    if atr > 0 and (atr < ATR_AVOID_LO or atr > ATR_AVOID_HI):
        adj -= VOL_PENALTY; notes.append(f"vol_avoid")

    # Strong signals
    strong = []
    for sn, sd in signals.items():
        if not isinstance(sd, dict): continue
        if _nd(sd.get("direction")) != direction: continue
        if sn in ("whale","dead_capital"):
            if _sf(sd.get("strength")) >= STRONG_Z: strong.append(sn)
        else:
            zv = abs(_sf(sd.get("z_score")))
            if sn == "cvd": zv = max(zv, abs(_sf(sd.get("z_smart"))), abs(_sf(sd.get("z_dumb"))))
            if zv >= STRONG_Z * Z_DENOM / STRENGTH_MULT: strong.append(sn)

    if "whale" in strong: adj += STRONG_WHALE; notes.append("strong_whale")
    elif any(s not in ("dead_capital","whale") for s in strong): adj += STRONG_OTHER; notes.append("strong_other")
    if "dead_capital" in strong:
        adj = max(adj, HIGH_THRESH, STRONG_DEAD_FLOOR); notes.append("dead_floor")

    adj = max(0, min(1, adj))
    would = adj >= BASE_THRESH or "dead_capital" in strong
    return base, adj, " ".join(notes), would


def calc_combined(pipeline_conv: float, brain_adj: float) -> Tuple[float, bool]:
    """Combined = average of pipeline and brain, must pass 0.50 threshold."""
    combined = (pipeline_conv + brain_adj) / 2.0
    return combined, combined >= 0.50


def load_trades():
    db = sqlite3.connect(DB_PATH)
    db.row_factory = sqlite3.Row
    rows = db.execute("""
        SELECT id, symbol, direction, venue, state,
               entry_time, entry_price, exit_price, size, notional_usd,
               exit_time, exit_reason, realized_pnl, realized_pnl_pct, total_fees,
               sl_price, tp_price, signals_snapshot, context_snapshot,
               confidence, strategy, mae_pct, mfe_pct
        FROM trades
        WHERE signals_snapshot IS NOT NULL AND context_snapshot IS NOT NULL
          AND realized_pnl IS NOT NULL
          AND exit_reason NOT IN ('EXTERNAL','RECONCILE','MANUAL')
          AND direction IN ('LONG','SHORT')
        ORDER BY entry_time ASC
    """).fetchall()
    db.close()
    trades = []
    for r in rows:
        try:
            sigs = json.loads(r["signals_snapshot"]) if r["signals_snapshot"] else {}
            ctx = json.loads(r["context_snapshot"]) if r["context_snapshot"] else {}
        except: continue
        if not isinstance(sigs, dict): continue
        trades.append(dict(
            id=r["id"], symbol=r["symbol"], direction=r["direction"],
            venue=r["venue"], state=r["state"],
            entry_time=_sf(r["entry_time"]), exit_time=_sf(r["exit_time"]),
            entry_price=_sf(r["entry_price"]), exit_price=_sf(r["exit_price"]),
            notional=_sf(r["notional_usd"]), pnl=_sf(r["realized_pnl"]),
            pnl_pct=_sf(r["realized_pnl_pct"]), fees=_sf(r["total_fees"]),
            exit_reason=r["exit_reason"] or "", strategy=r["strategy"] or "perp",
            pipeline_conv=_sf(r["confidence"]), raw_score=_sf(ctx.get("score")),
            signals=sigs, metrics=ctx.get("key_metrics") or {},
            mae_pct=_sf(r["mae_pct"]), mfe_pct=_sf(r["mfe_pct"]),
        ))
    return trades


def bucket_stats(label, trades):
    if not trades:
        return {"label": label, "n": 0}
    pnls = [t["pnl"] for t in trades]
    wins = [t for t in trades if t["pnl"] > 0]
    losses = [t for t in trades if t["pnl"] <= 0]
    longs = [t for t in trades if t["direction"] == "LONG"]
    shorts = [t for t in trades if t["direction"] == "SHORT"]
    gp = sum(t["pnl"] for t in wins)
    gl = abs(sum(t["pnl"] for t in losses))
    fees = sum(t["fees"] for t in trades)

    # Max DD
    peak = cum = dd = mdd = 0
    for p in pnls:
        cum += p
        if cum > peak: peak = cum
        dd = peak - cum
        if dd > mdd: mdd = dd

    # Consec losses
    mcl = cl = 0
    for p in pnls:
        if p <= 0: cl += 1; mcl = max(mcl, cl)
        else: cl = 0

    # Hold times
    holds = []
    for t in trades:
        if t["exit_time"] > t["entry_time"] > 0:
            holds.append((t["exit_time"] - t["entry_time"]) / 3600)

    # Exit breakdown
    exits = {}
    for t in trades:
        exits[t["exit_reason"]] = exits.get(t["exit_reason"], 0) + 1

    # By symbol top winners/losers
    sym_pnl = {}
    for t in trades:
        sym_pnl.setdefault(t["symbol"], []).append(t["pnl"])
    sym_net = {s: sum(ps) for s, ps in sym_pnl.items()}
    top_win_syms = sorted(sym_net.items(), key=lambda x: -x[1])[:5]
    top_lose_syms = sorted(sym_net.items(), key=lambda x: x[1])[:5]

    long_wins = [t for t in longs if t["pnl"] > 0]
    short_wins = [t for t in shorts if t["pnl"] > 0]

    return {
        "label": label, "n": len(trades),
        "winners": len(wins), "losers": len(losses),
        "wr": len(wins)/len(trades), "net": sum(pnls),
        "gp": gp, "gl": -gl, "fees": fees,
        "pf": gp/gl if gl else float("inf"),
        "expect": sum(pnls)/len(trades),
        "avg_win": gp/len(wins) if wins else 0,
        "avg_loss": -gl/len(losses) if losses else 0,
        "best": max(pnls), "worst": min(pnls),
        "mdd": mdd, "mcl": mcl,
        "longs": len(longs), "shorts": len(shorts),
        "long_pnl": sum(t["pnl"] for t in longs),
        "short_pnl": sum(t["pnl"] for t in shorts),
        "long_wr": len(long_wins)/len(longs) if longs else 0,
        "short_wr": len(short_wins)/len(shorts) if shorts else 0,
        "long_wins": len(long_wins), "short_wins": len(short_wins),
        "avg_mae": sum(t["mae_pct"] for t in trades if t["mae_pct"])/max(1,sum(1 for t in trades if t["mae_pct"])),
        "avg_mfe": sum(t["mfe_pct"] for t in trades if t["mfe_pct"])/max(1,sum(1 for t in trades if t["mfe_pct"])),
        "avg_hold_h": sum(holds)/len(holds) if holds else 0,
        "exits": exits,
        "top_win_syms": top_win_syms,
        "top_lose_syms": top_lose_syms,
        "avg_brain": sum(t.get("brain_adj",0) for t in trades)/len(trades),
        "avg_pipe": sum(t.get("pipeline_conv",0) for t in trades)/len(trades),
        "avg_combined": sum(t.get("combined_conv",0) for t in trades)/len(trades),
    }


def print_stats(s):
    if s["n"] == 0:
        print(f"  (no trades)")
        return
    print(f"  Trades:           {s['n']:>6d}   (W:{s['winners']} / L:{s['losers']})")
    print(f"  Win Rate:         {s['wr']:>6.1%}   (Long: {s['long_wr']:.1%} [{s['long_wins']}/{s['longs']}] | Short: {s['short_wr']:.1%} [{s['short_wins']}/{s['shorts']}])")
    print(f"  Net PnL:       ${s['net']:>+10,.2f}   (Long: ${s['long_pnl']:>+,.2f} | Short: ${s['short_pnl']:>+,.2f})")
    print(f"  Gross P/L:     ${s['gp']:>+10,.2f} / ${s['gl']:>+,.2f}   Fees: ${s['fees']:>,.2f}")
    print(f"  Profit Factor:    {s['pf']:>6.2f}   Expectancy: ${s['expect']:>+,.2f}/trade")
    print(f"  Avg Win/Loss:  ${s['avg_win']:>+10,.2f} / ${s['avg_loss']:>+,.2f}   (ratio: {abs(s['avg_win']/s['avg_loss']) if s['avg_loss'] else 0:.2f})")
    print(f"  Best/Worst:    ${s['best']:>+10,.2f} / ${s['worst']:>+,.2f}")
    print(f"  Max Drawdown:  ${s['mdd']:>10,.2f}   Max Consec Losses: {s['mcl']}")
    print(f"  L/S Ratio:        {s['longs']}L / {s['shorts']}S ({s['longs']/s['n']:.0%} / {s['shorts']/s['n']:.0%})")
    print(f"  Avg MAE/MFE:      {s['avg_mae']:.2f}% / {s['avg_mfe']:.2f}%")
    print(f"  Avg Hold:         {s['avg_hold_h']:.1f}h")
    print(f"  Avg Conviction:   brain={s['avg_brain']:.3f}  pipeline={s['avg_pipe']:.3f}  combined={s['avg_combined']:.3f}")
    exits = sorted(s["exits"].items(), key=lambda x: -x[1])
    print(f"  Exit Reasons:     {', '.join(f'{k}={v}' for k,v in exits)}")
    print(f"  Top Win Symbols:  {', '.join(f'{s}=${p:+,.0f}' for s,p in s['top_win_syms'])}")
    print(f"  Top Lose Symbols: {', '.join(f'{s}=${p:+,.0f}' for s,p in s['top_lose_syms'])}")


def main():
    trades = load_trades()
    print(f"Loaded {len(trades)} trades\n")

    # ── Recalculate both conviction paths for every trade ──
    for t in trades:
        base, adj, notes, would = calc_brain(t["signals"], t["metrics"], t["direction"])
        t["brain_base"] = base
        t["brain_adj"] = adj
        t["brain_notes"] = notes
        t["brain_would"] = would

        comb, comb_would = calc_combined(t["pipeline_conv"], adj)
        t["combined_conv"] = comb
        t["combined_would"] = comb_would

    # ════════════════════════════════════════════════════════════════
    # PART 1: PER-TRADE TABLE (all trades with both convictions)
    # ════════════════════════════════════════════════════════════════
    print("=" * 160)
    print("PER-TRADE CONVICTION COMPARISON")
    print("=" * 160)
    hdr = f"{'ID':>5} {'Symbol':>8} {'Dir':>5} {'PnL':>10} {'Exit':>14} {'Pipe':>6} {'Brain':>6} {'Comb':>6} {'B.Trd':>5} {'C.Trd':>5} {'Notes'}"
    print(hdr)
    print("─" * 160)
    for t in trades:
        wt = "Y" if t["brain_would"] else "N"
        ct = "Y" if t["combined_would"] else "N"
        pnl_str = f"${t['pnl']:>+8,.2f}"
        # Color coding via markers
        marker = "✅" if t["pnl"] > 0 else "❌"
        # Highlight trades where brain disagrees with pipeline
        flag = ""
        if not t["brain_would"] and t["pnl"] < 0:
            flag = " 💡saved"  # brain would have saved us
        elif not t["brain_would"] and t["pnl"] > 0:
            flag = " ⚠️miss"  # brain would have missed profit
        print(f"{t['id']:>5} {t['symbol']:>8} {t['direction']:>5} {pnl_str} {t['exit_reason']:>14} {t['pipeline_conv']:>6.3f} {t['brain_adj']:>6.3f} {t['combined_conv']:>6.3f} {wt:>5} {ct:>5} {t['brain_notes']}{flag}")

    # ════════════════════════════════════════════════════════════════
    # PART 2: RECALC 1 — BRAIN ONLY
    # ════════════════════════════════════════════════════════════════
    brain_kept = [t for t in trades if t["brain_would"]]
    brain_rejected = [t for t in trades if not t["brain_would"]]

    print(f"\n\n{'=' * 120}")
    print("RECALC 1: BRAIN-ONLY CONVICTION")
    print(f"Filter: brain_adj_conviction >= {BASE_THRESH} (or strong dead_capital)")
    print(f"{'=' * 120}")

    print(f"\n── A) ACTUAL (baseline — all {len(trades)} trades) ──")
    print_stats(bucket_stats("actual", trades))

    print(f"\n── B) BRAIN WOULD TRADE ({len(brain_kept)} trades) ──")
    print_stats(bucket_stats("brain_kept", brain_kept))

    print(f"\n── C) BRAIN WOULD REJECT ({len(brain_rejected)} trades) ──")
    print_stats(bucket_stats("brain_rejected", brain_rejected))

    # ════════════════════════════════════════════════════════════════
    # PART 3: RECALC 2 — COMBINED (pipeline + brain averaged)
    # ════════════════════════════════════════════════════════════════
    comb_kept = [t for t in trades if t["combined_would"]]
    comb_rejected = [t for t in trades if not t["combined_would"]]

    print(f"\n\n{'=' * 120}")
    print("RECALC 2: COMBINED CONVICTION (pipeline + brain averaged)")
    print(f"Filter: (pipeline_conv + brain_adj) / 2 >= 0.50")
    print(f"{'=' * 120}")

    print(f"\n── A) COMBINED WOULD TRADE ({len(comb_kept)} trades) ──")
    print_stats(bucket_stats("combined_kept", comb_kept))

    print(f"\n── B) COMBINED WOULD REJECT ({len(comb_rejected)} trades) ──")
    print_stats(bucket_stats("combined_rejected", comb_rejected))

    # ════════════════════════════════════════════════════════════════
    # PART 4: CONVICTION DISTRIBUTION
    # ════════════════════════════════════════════════════════════════
    print(f"\n\n{'=' * 120}")
    print("CONVICTION DISTRIBUTION")
    print(f"{'=' * 120}")

    bins = [(0,0.1),(0.1,0.2),(0.2,0.3),(0.3,0.4),(0.4,0.5),(0.5,0.6),(0.6,0.7),(0.7,0.8),(0.8,0.9),(0.9,1.01)]
    print(f"\n  {'Range':>10} │ {'Brain':>6} {'WR':>6} {'Net PnL':>10} │ {'Pipeline':>8} {'WR':>6} {'Net PnL':>10} │ {'Combined':>8} {'WR':>6} {'Net PnL':>10}")
    print(f"  {'─'*10} │ {'─'*6} {'─'*6} {'─'*10} │ {'─'*8} {'─'*6} {'─'*10} │ {'─'*8} {'─'*6} {'─'*10}")
    for lo, hi in bins:
        label = f"{lo:.1f}-{hi:.1f}" if hi <= 1 else f"{lo:.1f}-1.0"
        bt = [t for t in trades if lo <= t["brain_adj"] < hi]
        pt = [t for t in trades if lo <= t["pipeline_conv"] < hi]
        ct = [t for t in trades if lo <= t["combined_conv"] < hi]

        bn = len(bt); bwr = f"{sum(1 for t in bt if t['pnl']>0)/bn:.0%}" if bn else "—"; bpnl = f"${sum(t['pnl'] for t in bt):>+,.0f}" if bn else "—"
        pn = len(pt); pwr = f"{sum(1 for t in pt if t['pnl']>0)/pn:.0%}" if pn else "—"; ppnl = f"${sum(t['pnl'] for t in pt):>+,.0f}" if pn else "—"
        cn = len(ct); cwr = f"{sum(1 for t in ct if t['pnl']>0)/cn:.0%}" if cn else "—"; cpnl = f"${sum(t['pnl'] for t in ct):>+,.0f}" if cn else "—"
        print(f"  {label:>10} │ {bn:>6} {bwr:>6} {bpnl:>10} │ {pn:>8} {pwr:>6} {ppnl:>10} │ {cn:>8} {cwr:>6} {cpnl:>10}")

    # ════════════════════════════════════════════════════════════════
    # PART 5: HEAD-TO-HEAD COMPARISON
    # ════════════════════════════════════════════════════════════════
    a = bucket_stats("actual", trades)
    b = bucket_stats("brain", brain_kept)
    c = bucket_stats("combined", comb_kept)

    print(f"\n\n{'=' * 120}")
    print("HEAD-TO-HEAD COMPARISON")
    print(f"{'=' * 120}")
    print(f"\n  {'Metric':<25} {'ACTUAL':>15} {'BRAIN ONLY':>15} {'COMBINED':>15}")
    print(f"  {'─'*25} {'─'*15} {'─'*15} {'─'*15}")
    rows = [
        ("Trades", f"{a['n']}", f"{b['n']}", f"{c['n']}"),
        ("Win Rate", f"{a['wr']:.1%}", f"{b['wr']:.1%}" if b['n'] else "—", f"{c['wr']:.1%}" if c['n'] else "—"),
        ("Net PnL", f"${a['net']:>+,.2f}", f"${b['net']:>+,.2f}" if b['n'] else "—", f"${c['net']:>+,.2f}" if c['n'] else "—"),
        ("Profit Factor", f"{a['pf']:.2f}", f"{b['pf']:.2f}" if b['n'] else "—", f"{c['pf']:.2f}" if c['n'] else "—"),
        ("Expectancy", f"${a['expect']:>+,.2f}", f"${b['expect']:>+,.2f}" if b['n'] else "—", f"${c['expect']:>+,.2f}" if c['n'] else "—"),
        ("Max Drawdown", f"${a['mdd']:>,.2f}", f"${b['mdd']:>,.2f}" if b['n'] else "—", f"${c['mdd']:>,.2f}" if c['n'] else "—"),
        ("Max Consec Losses", f"{a['mcl']}", f"{b['mcl']}" if b['n'] else "—", f"{c['mcl']}" if c['n'] else "—"),
        ("Avg Win", f"${a['avg_win']:>+,.2f}", f"${b['avg_win']:>+,.2f}" if b['n'] else "—", f"${c['avg_win']:>+,.2f}" if c['n'] else "—"),
        ("Avg Loss", f"${a['avg_loss']:>+,.2f}", f"${b['avg_loss']:>+,.2f}" if b['n'] else "—", f"${c['avg_loss']:>+,.2f}" if c['n'] else "—"),
        ("W/L Ratio", f"{abs(a['avg_win']/a['avg_loss']) if a['avg_loss'] else 0:.2f}", f"{abs(b['avg_win']/b['avg_loss']) if b.get('avg_loss') else 0:.2f}" if b['n'] else "—", f"{abs(c['avg_win']/c['avg_loss']) if c.get('avg_loss') else 0:.2f}" if c['n'] else "—"),
        ("Long/Short", f"{a['longs']}L/{a['shorts']}S", f"{b['longs']}L/{b['shorts']}S" if b['n'] else "—", f"{c['longs']}L/{c['shorts']}S" if c['n'] else "—"),
        ("Long WR", f"{a['long_wr']:.1%}", f"{b['long_wr']:.1%}" if b['n'] else "—", f"{c['long_wr']:.1%}" if c['n'] else "—"),
        ("Short WR", f"{a['short_wr']:.1%}", f"{b['short_wr']:.1%}" if b['n'] else "—", f"{c['short_wr']:.1%}" if c['n'] else "—"),
        ("Long PnL", f"${a['long_pnl']:>+,.2f}", f"${b['long_pnl']:>+,.2f}" if b['n'] else "—", f"${c['long_pnl']:>+,.2f}" if c['n'] else "—"),
        ("Short PnL", f"${a['short_pnl']:>+,.2f}", f"${b['short_pnl']:>+,.2f}" if b['n'] else "—", f"${c['short_pnl']:>+,.2f}" if c['n'] else "—"),
        ("Avg MAE %", f"{a['avg_mae']:.2f}%", f"{b['avg_mae']:.2f}%" if b['n'] else "—", f"{c['avg_mae']:.2f}%" if c['n'] else "—"),
        ("Avg MFE %", f"{a['avg_mfe']:.2f}%", f"{b['avg_mfe']:.2f}%" if b['n'] else "—", f"{c['avg_mfe']:.2f}%" if c['n'] else "—"),
        ("Fees", f"${a['fees']:>,.2f}", f"${b['fees']:>,.2f}" if b['n'] else "—", f"${c['fees']:>,.2f}" if c['n'] else "—"),
    ]
    for label, va, vb, vc in rows:
        print(f"  {label:<25} {va:>15} {vb:>15} {vc:>15}")

    # ════════════════════════════════════════════════════════════════
    # PART 6: SAVED / MISSED ANALYSIS
    # ════════════════════════════════════════════════════════════════
    print(f"\n\n{'=' * 120}")
    print("FILTER IMPACT ANALYSIS")
    print(f"{'=' * 120}")

    # Brain
    brain_saved = [t for t in brain_rejected if t["pnl"] < 0]
    brain_missed = [t for t in brain_rejected if t["pnl"] > 0]
    print(f"\n  BRAIN FILTER:")
    print(f"    Rejected {len(brain_rejected)} trades total")
    print(f"    💡 Saved from {len(brain_saved)} losers:  ${abs(sum(t['pnl'] for t in brain_saved)):>+,.2f}")
    print(f"    ⚠️  Missed {len(brain_missed)} winners: ${sum(t['pnl'] for t in brain_missed):>+,.2f}")
    brain_net_benefit = abs(sum(t["pnl"] for t in brain_saved)) - sum(t["pnl"] for t in brain_missed)
    print(f"    Net filter benefit: ${brain_net_benefit:>+,.2f}")

    # Combined
    comb_saved_t = [t for t in comb_rejected if t["pnl"] < 0]
    comb_missed_t = [t for t in comb_rejected if t["pnl"] > 0]
    print(f"\n  COMBINED FILTER:")
    print(f"    Rejected {len(comb_rejected)} trades total")
    print(f"    💡 Saved from {len(comb_saved_t)} losers:  ${abs(sum(t['pnl'] for t in comb_saved_t)):>+,.2f}")
    print(f"    ⚠️  Missed {len(comb_missed_t)} winners: ${sum(t['pnl'] for t in comb_missed_t):>+,.2f}")
    comb_net_benefit = abs(sum(t["pnl"] for t in comb_saved_t)) - sum(t["pnl"] for t in comb_missed_t)
    print(f"    Net filter benefit: ${comb_net_benefit:>+,.2f}")

    # ════════════════════════════════════════════════════════════════
    # PART 7: EXIT REASON BREAKDOWN PER BUCKET
    # ════════════════════════════════════════════════════════════════
    print(f"\n\n{'=' * 120}")
    print("EXIT REASON PnL BREAKDOWN (Actual)")
    print(f"{'=' * 120}")
    exit_buckets = {}
    for t in trades:
        exit_buckets.setdefault(t["exit_reason"], []).append(t)
    print(f"\n  {'Exit Reason':<30} {'Trades':>7} {'WR':>6} {'Net PnL':>12} {'Avg PnL':>10} {'Avg Brain':>10} {'Avg Pipe':>10}")
    print(f"  {'─'*30} {'─'*7} {'─'*6} {'─'*12} {'─'*10} {'─'*10} {'─'*10}")
    for er in sorted(exit_buckets.keys(), key=lambda k: sum(t["pnl"] for t in exit_buckets[k])):
        ts = exit_buckets[er]
        n = len(ts)
        wr = sum(1 for t in ts if t["pnl"] > 0) / n
        net = sum(t["pnl"] for t in ts)
        avg = net / n
        ab = sum(t["brain_adj"] for t in ts) / n
        ap = sum(t["pipeline_conv"] for t in ts) / n
        print(f"  {er:<30} {n:>7} {wr:>5.0%} ${net:>+11,.2f} ${avg:>+9,.2f} {ab:>10.3f} {ap:>10.3f}")


if __name__ == "__main__":
    main()
