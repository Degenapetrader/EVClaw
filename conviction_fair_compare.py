#!/usr/bin/env python3
"""
Fair Conviction Comparison — All options at ≥0.1 threshold (same as pipeline).
Compare basket-by-basket how well each discriminates winners from losers.
"""
from __future__ import annotations
import sqlite3, json, os
from typing import Any, Dict, List

DB_PATH = os.path.join(os.path.dirname(__file__), "ai_trader.db")

BRAIN_WEIGHTS = {"cvd":0.20,"dead_capital":0.32,"fade":0.08,"ofm":0.12,"hip3_main":0.70,"whale":0.28}
OPTD_WEIGHTS = {"cvd":0.35,"dead_capital":0.15,"fade":0.06,"ofm":0.15,"hip3_main":0.70,"whale":0.25}
Z_DENOM=3.0; STR_MULT=2.0

def _sf(v,d=0.0):
    try: return float(v)
    except: return d
def _nd(v):
    d=str(v or"").upper()
    return d if d in("LONG","SHORT","NEUTRAL") else "NEUTRAL"

def brain_base(sigs, met, direction, weights=None):
    weights = weights or BRAIN_WEIGHTS
    direction = _nd(direction); tw=ws=0.0; sig_count=0; primary=False
    primaries={"cvd","whale","ofm","hip3_main"}
    for sn,w in weights.items():
        sd=sigs.get(sn)
        if not sd or not isinstance(sd,dict): continue
        sd_dir=_nd(sd.get("direction"))
        z=_sf(sd.get("z_score"))
        if sn=="cvd": z=max(abs(z),abs(_sf(sd.get("z_smart"))),abs(_sf(sd.get("z_dumb"))))
        if sn in("whale","dead_capital") and z==0: z=_sf(sd.get("strength"))*STR_MULT
        if abs(z)<0.01: continue
        s=min(1.0,abs(z)/max(1e-9,Z_DENOM)); tw+=w; sig_count+=1
        if sd_dir==direction:
            ws+=w*s
            if sn in primaries: primary=True
        elif sd_dir!="NEUTRAL": ws-=w*s*0.5
    if tw==0: return 0,1.0,1.0,1.0,0,0,False
    base=ws/tw
    agree=sum(1 for s in sigs.values() if isinstance(s,dict) and _nd(s.get("direction"))==direction)
    if base>0:
        if agree>=4: base+=0.10
        if agree>=5: base+=0.05
    base=max(0,min(1,base))
    sc=_sf(met.get("smart_cvd")); dz=_sf(met.get("divergence_z")); smart_f=1.0
    if direction=="LONG":
        if sc>0 and dz>0: smart_f=1.0+min(0.15,dz/10)
        elif sc<0: smart_f=1.0-min(0.15,abs(dz)/10)
    elif direction=="SHORT":
        if sc<0 and dz<0: smart_f=1.0+min(0.15,abs(dz)/10)
        elif sc>0: smart_f=1.0-min(0.15,dz/10)
    co=str(met.get("cohort_signal")or"").upper(); cohort_f=1.0
    if(direction=="LONG" and "ACCUMULATING" in co)or(direction=="SHORT" and "DISTRIBUTING" in co): cohort_f=1.15
    elif "ACCUMULATING" in co or "DISTRIBUTING" in co: cohort_f=0.85
    atr=_sf(met.get("atr_pct")); vol_f=1.0
    if atr>0 and(atr<0.3 or atr>6.0): vol_f=0.85
    return base,smart_f,cohort_f,vol_f,agree,sig_count,primary

def opt_a(pipe,sigs,met,d):
    _,sm,co,vo,ag,sc,_=brain_base(sigs,met,d)
    af=1.0
    if ag>=4: af=1.10
    elif ag>=3: af=1.05
    elif ag<=1 and sc>=3: af=0.90
    return max(0,min(1,pipe*sm*co*vo*af))

def opt_b(pipe,sigs,met,d):
    base,sm,co,vo,_,_,_=brain_base(sigs,met,d)
    adj=max(0,min(1,base*sm*co*vo))
    return max(0,min(1, 0.6*pipe + 0.4*adj))

def opt_c(pipe,sigs,met,d):
    _,sm,co,vo,ag,sc,hp=brain_base(sigs,met,d)
    q=0.0
    if ag>=3: q+=0.35
    elif ag>=2: q+=0.20
    elif ag>=1: q+=0.10
    if hp: q+=0.25
    if sm>1.05: q+=0.20
    elif sm>0.95: q+=0.10
    if co>1.0: q+=0.10
    elif co<1.0: q-=0.05
    if vo>=1.0: q+=0.10
    q=max(0,min(1,q))
    if q<0.30: return 0.0
    return pipe*(0.7+0.3*q)

def opt_d(pipe,sigs,met,d):
    base,sm,co,vo,ag,_,_=brain_base(sigs,met,d,OPTD_WEIGHTS)
    adj=base*sm*co*vo
    if ag>=4: adj*=1.10
    elif ag>=3: adj*=1.05
    adj=max(0,min(1,adj))
    return max(0,min(1, 0.5*pipe + 0.5*adj))

def load_trades():
    db=sqlite3.connect(DB_PATH); db.row_factory=sqlite3.Row
    rows=db.execute("""SELECT id,symbol,direction,venue,entry_time,exit_time,exit_price,
        realized_pnl,realized_pnl_pct,total_fees,exit_reason,
        signals_snapshot,context_snapshot,confidence,mae_pct,mfe_pct
        FROM trades WHERE signals_snapshot IS NOT NULL AND context_snapshot IS NOT NULL
        AND realized_pnl IS NOT NULL AND exit_reason NOT IN('EXTERNAL','RECONCILE','MANUAL')
        AND direction IN('LONG','SHORT')
        AND symbol NOT LIKE 'XYZ:%' AND symbol NOT LIKE 'CASH:%'
        AND symbol NOT LIKE 'FLX:%' AND symbol NOT LIKE 'HYNA:%'
        AND symbol NOT LIKE 'KM:%'
        ORDER BY entry_time ASC""").fetchall()
    db.close()
    trades=[]
    for r in rows:
        try:
            sigs=json.loads(r["signals_snapshot"]) if r["signals_snapshot"] else {}
            ctx=json.loads(r["context_snapshot"]) if r["context_snapshot"] else {}
        except: continue
        if not isinstance(sigs,dict): continue
        t=dict(id=r["id"],symbol=r["symbol"],direction=r["direction"],venue=r["venue"],
            entry_time=_sf(r["entry_time"]),exit_time=_sf(r["exit_time"]),
            pnl=_sf(r["realized_pnl"]),fees=_sf(r["total_fees"]),
            exit_reason=r["exit_reason"]or"",
            pipeline=_sf(r["confidence"]),signals=sigs,metrics=ctx.get("key_metrics")or{},
            mae_pct=_sf(r["mae_pct"]),mfe_pct=_sf(r["mfe_pct"]))
        s,m,d=t["signals"],t["metrics"],t["direction"]
        t["opt_a"]=opt_a(t["pipeline"],s,m,d)
        t["opt_b"]=opt_b(t["pipeline"],s,m,d)
        t["opt_c"]=opt_c(t["pipeline"],s,m,d)
        t["opt_d"]=opt_d(t["pipeline"],s,m,d)
        trades.append(t)
    return trades

def basket_stats(ts):
    if not ts: return None
    n=len(ts); pnls=[t["pnl"] for t in ts]
    w=sum(1 for p in pnls if p>0); l=n-w
    net=sum(pnls); gp=sum(p for p in pnls if p>0); gl=abs(sum(p for p in pnls if p<=0))
    fees=sum(t["fees"] for t in ts)
    longs=[t for t in ts if t["direction"]=="LONG"]; shorts=[t for t in ts if t["direction"]=="SHORT"]
    lw=sum(1 for t in longs if t["pnl"]>0); sw=sum(1 for t in shorts if t["pnl"]>0)
    peak=cum=mdd=0
    for p in pnls:
        cum+=p
        if cum>peak: peak=cum
        if peak-cum>mdd: mdd=peak-cum
    mcl=cl=0
    for p in pnls:
        if p<=0: cl+=1; mcl=max(mcl,cl)
        else: cl=0
    return dict(n=n,w=w,l=l,wr=w/n,net=net,gp=gp,gl=-gl,fees=fees,
        pf=gp/gl if gl else(float("inf") if gp else 0),exp=net/n,
        aw=gp/w if w else 0,al=-gl/l if l else 0,
        best=max(pnls),worst=min(pnls),mdd=mdd,mcl=mcl,
        nl=len(longs),ns=len(shorts),
        lpnl=sum(t["pnl"] for t in longs),spnl=sum(t["pnl"] for t in shorts),
        lwr=lw/len(longs) if longs else 0,swr=sw/len(shorts) if shorts else 0)

def print_basket_grid(label, key, trades, bins):
    print(f"\n  {label}")
    print(f"  {'Basket':>10} {'N':>5} {'W':>4} {'L':>4} {'WR':>6} {'Net PnL':>11} {'PF':>6} {'Exp':>9} {'AvgW':>8} {'AvgL':>8} {'W/L':>5} {'MaxDD':>8} {'MCL':>4} {'L':>3} {'S':>3} {'LWR':>5} {'SWR':>5}")
    print(f"  {'─'*10} {'─'*5} {'─'*4} {'─'*4} {'─'*6} {'─'*11} {'─'*6} {'─'*9} {'─'*8} {'─'*8} {'─'*5} {'─'*8} {'─'*4} {'─'*3} {'─'*3} {'─'*5} {'─'*5}")
    for lo,hi,lbl in bins:
        sub=[t for t in trades if lo<=t[key]<hi]
        if not sub:
            print(f"  {lbl:>10} {'0':>5}")
            continue
        s=basket_stats(sub)
        pf_s="INF" if s['pf']==float('inf') else f"{s['pf']:.2f}"
        wlr=abs(s['aw']/s['al']) if s['al'] else 0
        print(f"  {lbl:>10} {s['n']:>5} {s['w']:>4} {s['l']:>4} {s['wr']:>5.1%} ${s['net']:>+9,.0f} {pf_s:>6} ${s['exp']:>+7,.2f} ${s['aw']:>+6,.0f} ${s['al']:>+6,.0f} {wlr:>5.2f} ${s['mdd']:>6,.0f} {s['mcl']:>4} {s['nl']:>3} {s['ns']:>3} {s['lwr']:>4.0%} {s['swr']:>4.0%}")

    # Totals
    all_s = basket_stats([t for t in trades if t[key] >= 0.1])
    if all_s:
        pf_s="INF" if all_s['pf']==float('inf') else f"{all_s['pf']:.2f}"
        wlr=abs(all_s['aw']/all_s['al']) if all_s['al'] else 0
        print(f"  {'─'*10} {'─'*5} {'─'*4} {'─'*4} {'─'*6} {'─'*11} {'─'*6} {'─'*9} {'─'*8} {'─'*8} {'─'*5} {'─'*8} {'─'*4} {'─'*3} {'─'*3} {'─'*5} {'─'*5}")
        print(f"  {'ALL≥0.1':>10} {all_s['n']:>5} {all_s['w']:>4} {all_s['l']:>4} {all_s['wr']:>5.1%} ${all_s['net']:>+9,.0f} {pf_s:>6} ${all_s['exp']:>+7,.2f} ${all_s['aw']:>+6,.0f} ${all_s['al']:>+6,.0f} {wlr:>5.2f} ${all_s['mdd']:>6,.0f} {all_s['mcl']:>4} {all_s['nl']:>3} {all_s['ns']:>3} {all_s['lwr']:>4.0%} {all_s['swr']:>4.0%}")

def main():
    trades = load_trades()
    # Filter to ≥0.1 pipeline (same as actual system)
    trades_01 = [t for t in trades if t["pipeline"] >= 0.1]
    print(f"Loaded {len(trades)} trades, {len(trades_01)} with pipeline ≥ 0.1\n")

    bins = [
        (0.0, 0.1, "0.0–0.1"),
        (0.1, 0.2, "0.1–0.2"),
        (0.2, 0.3, "0.2–0.3"),
        (0.3, 0.4, "0.3–0.4"),
        (0.4, 0.5, "0.4–0.5"),
        (0.5, 0.6, "0.5–0.6"),
        (0.6, 0.7, "0.6–0.7"),
        (0.7, 0.8, "0.7–0.8"),
        (0.8, 0.9, "0.8–0.9"),
        (0.9, 1.01, "0.9–1.0"),
    ]

    options = [
        ("PIPELINE (current)", "pipeline"),
        ("OPTION A (enhanced pipeline)", "opt_a"),
        ("OPTION B (blended 60/40)", "opt_b"),
        ("OPTION C (quality gate)", "opt_c"),
        ("OPTION D (tiered re-weighted)", "opt_d"),
    ]

    print("=" * 150)
    print("  FAIR COMPARISON — All trades with pipeline ≥ 0.1 (system entry threshold)")
    print("  Each option re-scores the SAME trades, then we compare baskets")
    print("=" * 150)

    for oname, okey in options:
        print_basket_grid(oname, okey, trades, bins)

    # ═══ DISCRIMINATION SCORE ═══
    # How well does each option separate winners from losers?
    print(f"\n\n{'='*150}")
    print("  DISCRIMINATION QUALITY — Does higher conviction = better trades?")
    print("="*150)

    for oname, okey in options:
        # Split into bottom half and top half by conviction
        scored = sorted(trades_01, key=lambda t: t[okey])
        mid = len(scored)//2
        bottom = scored[:mid]; top = scored[mid:]
        b_wr = sum(1 for t in bottom if t["pnl"]>0)/len(bottom) if bottom else 0
        t_wr = sum(1 for t in top if t["pnl"]>0)/len(top) if top else 0
        b_net = sum(t["pnl"] for t in bottom)
        t_net = sum(t["pnl"] for t in top)
        b_pf_gp = sum(t["pnl"] for t in bottom if t["pnl"]>0)
        b_pf_gl = abs(sum(t["pnl"] for t in bottom if t["pnl"]<=0))
        t_pf_gp = sum(t["pnl"] for t in top if t["pnl"]>0)
        t_pf_gl = abs(sum(t["pnl"] for t in top if t["pnl"]<=0))
        b_pf = b_pf_gp/b_pf_gl if b_pf_gl else 0
        t_pf = t_pf_gp/t_pf_gl if t_pf_gl else 0

        # Correlation: conviction vs pnl
        convs = [t[okey] for t in trades_01]
        pnls = [t["pnl"] for t in trades_01]
        n = len(convs)
        if n > 2:
            mc = sum(convs)/n; mp = sum(pnls)/n
            cov = sum((c-mc)*(p-mp) for c,p in zip(convs,pnls))/(n-1)
            sc = (sum((c-mc)**2 for c in convs)/(n-1))**0.5
            sp = (sum((p-mp)**2 for p in pnls)/(n-1))**0.5
            corr = cov/(sc*sp) if sc*sp else 0
        else: corr = 0

        # Spread: WR difference between top quartile and bottom quartile
        q1 = scored[:n//4]; q4 = scored[3*n//4:]
        q1_wr = sum(1 for t in q1 if t["pnl"]>0)/len(q1) if q1 else 0
        q4_wr = sum(1 for t in q4 if t["pnl"]>0)/len(q4) if q4 else 0
        q1_net = sum(t["pnl"] for t in q1)
        q4_net = sum(t["pnl"] for t in q4)

        print(f"\n  {oname}")
        print(f"    Bottom half: {len(bottom)} trades, WR={b_wr:.1%}, Net=${b_net:>+,.0f}, PF={b_pf:.2f}")
        print(f"    Top half:    {len(top)} trades, WR={t_wr:.1%}, Net=${t_net:>+,.0f}, PF={t_pf:.2f}")
        print(f"    WR spread (top-bottom): {t_wr-b_wr:>+.1%}")
        print(f"    PnL spread (top-bottom): ${t_net-b_net:>+,.0f}")
        print(f"    Q1 (bottom 25%): WR={q1_wr:.1%}, Net=${q1_net:>+,.0f}")
        print(f"    Q4 (top 25%):    WR={q4_wr:.1%}, Net=${q4_net:>+,.0f}")
        print(f"    Q4-Q1 WR spread: {q4_wr-q1_wr:>+.1%}  ← HIGHER = BETTER DISCRIMINATION")
        print(f"    Q4-Q1 PnL spread: ${q4_net-q1_net:>+,.0f}")
        print(f"    Conviction-PnL correlation: {corr:>+.4f}  ← positive = higher conv → higher pnl")

    # ═══ CUMULATIVE BOTTOM-UP ═══
    print(f"\n\n{'='*150}")
    print("  CUMULATIVE — If you traded everything above X, what happens?")
    print("="*150)
    cum_thresholds = [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8]
    print(f"\n  {'':>10}", end="")
    for oname,_ in options:
        short = oname.split("(")[0].strip()[:12]
        print(f" {short:>24}", end="")
    print()
    print(f"  {'Threshold':>10}", end="")
    for _ in options:
        print(f" {'N':>5} {'WR':>5} {'Net':>7} {'PF':>5}", end="")
    print()
    print(f"  {'─'*10}", end="")
    for _ in options:
        print(f" {'─'*5} {'─'*5} {'─'*7} {'─'*5}", end="")
    print()

    for th in cum_thresholds:
        print(f"  {'≥'+str(th):>10}", end="")
        for _,okey in options:
            sub=[t for t in trades if t[okey]>=th]
            n=len(sub)
            if n==0: print(f" {'0':>5} {'—':>5} {'—':>7} {'—':>5}", end=""); continue
            wr=sum(1 for t in sub if t["pnl"]>0)/n
            net=sum(t["pnl"] for t in sub)
            gp=sum(t["pnl"] for t in sub if t["pnl"]>0)
            gl=abs(sum(t["pnl"] for t in sub if t["pnl"]<=0))
            pf=gp/gl if gl else 0
            print(f" {n:>5} {wr:>4.0%} ${net:>+5,.0f} {pf:>5.2f}", end="")
        print()

if __name__ == "__main__":
    main()
