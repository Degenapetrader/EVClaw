#!/usr/bin/env python3
"""Conviction Baskets — Full metrics for each conviction range bin."""
from __future__ import annotations
import sqlite3, json, os, sys
from typing import Any, Dict, List

DB_PATH = os.path.join(os.path.dirname(__file__), "ai_trader.db")

# Brain config
BRAIN_WEIGHTS = {"cvd":0.20,"dead_capital":0.32,"fade":0.08,"ofm":0.12,"hip3_main":0.70,"whale":0.28}
Z_DENOM=3.0; STR_MULT=2.0; BASE_THRESH=0.55; HIGH_THRESH=0.75
SMART_ADJ=0.1; SMART_DIV_D=3.0; SMART_DIV_M=0.5; COHORT_A=0.3; COHORT_C=0.2
LIQ_T=0.5; LIQ_B=0.05; VOL_P=0.15; AG_C1=4; AG_B1=0.10; AG_C2=5; AG_B2=0.05
S_WHALE=0.20; S_OTHER=0.10; S_DEAD=0.90; S_Z=1.5

def _sf(v,d=0.0):
    try: return float(v)
    except: return d

def _nd(v):
    d=str(v or"").upper()
    return d if d in("LONG","SHORT","NEUTRAL") else "NEUTRAL"

def calc_brain(sigs,met,direction):
    direction=_nd(direction); tw=ws=0.0
    for sn,w in BRAIN_WEIGHTS.items():
        sd=sigs.get(sn)
        if not sd or not isinstance(sd,dict): continue
        sd_dir=_nd(sd.get("direction"))
        z=_sf(sd.get("z_score"))
        if sn=="cvd": z=max(abs(z),abs(_sf(sd.get("z_smart"))),abs(_sf(sd.get("z_dumb"))))
        if sn in("whale","dead_capital") and z==0: z=_sf(sd.get("strength"))*STR_MULT
        s=min(1.0,abs(z)/max(1e-9,Z_DENOM)); tw+=w
        if sd_dir==direction: ws+=w*s
        elif sd_dir!="NEUTRAL": ws-=w*s*0.5
    if tw==0: return 0,0,False
    base=ws/tw
    if base>0:
        ac=sum(1 for s in sigs.values() if isinstance(s,dict) and _nd(s.get("direction"))==direction)
        if ac>=AG_C1: base+=AG_B1
        if ac>=AG_C2: base+=AG_B2
    base=max(0,min(1,base)); adj=base
    sc=_sf(met.get("smart_cvd")); dz=_sf(met.get("divergence_z")); sm=0
    if direction=="LONG":
        if sc>0 and dz>0: sm=min(1,dz/SMART_DIV_D)*SMART_DIV_M
        elif sc<0: sm=-min(1,abs(dz)/SMART_DIV_D)*SMART_DIV_M
    elif direction=="SHORT":
        if sc<0 and dz<0: sm=min(1,abs(dz)/SMART_DIV_D)*SMART_DIV_M
        elif sc>0: sm=-min(1,dz/SMART_DIV_D)*SMART_DIV_M
    adj+=sm*SMART_ADJ
    co=str(met.get("cohort_signal")or"").upper()
    if(direction=="LONG" and "ACCUMULATING" in co) or (direction=="SHORT" and "DISTRIBUTING" in co): adj+=COHORT_A
    elif "ACCUMULATING" in co or "DISTRIBUTING" in co: adj-=COHORT_C
    if int(_sf(met.get("fragile_count")))>10 and _sf(met.get("fragile_notional"))>500000: adj+=LIQ_B
    atr=_sf(met.get("atr_pct"))
    if atr>0 and(atr<0.3 or atr>6.0): adj-=VOL_P
    strong=[]
    for sn,sd in sigs.items():
        if not isinstance(sd,dict): continue
        if _nd(sd.get("direction"))!=direction: continue
        if sn in("whale","dead_capital"):
            if _sf(sd.get("strength"))>=S_Z: strong.append(sn)
        else:
            zv=abs(_sf(sd.get("z_score")))
            if sn=="cvd": zv=max(zv,abs(_sf(sd.get("z_smart"))),abs(_sf(sd.get("z_dumb"))))
            if zv>=S_Z*Z_DENOM/STR_MULT: strong.append(sn)
    if "whale" in strong: adj+=S_WHALE
    elif any(s not in("dead_capital","whale") for s in strong): adj+=S_OTHER
    if "dead_capital" in strong: adj=max(adj,HIGH_THRESH,S_DEAD)
    adj=max(0,min(1,adj))
    would=adj>=BASE_THRESH or "dead_capital" in strong
    return base,adj,would

def load_trades():
    db=sqlite3.connect(DB_PATH); db.row_factory=sqlite3.Row
    rows=db.execute("""SELECT id,symbol,direction,venue,state,entry_time,entry_price,exit_price,
        size,notional_usd,exit_time,exit_reason,realized_pnl,realized_pnl_pct,total_fees,
        sl_price,tp_price,signals_snapshot,context_snapshot,confidence,strategy,mae_pct,mfe_pct
        FROM trades WHERE signals_snapshot IS NOT NULL AND context_snapshot IS NOT NULL
        AND realized_pnl IS NOT NULL AND exit_reason NOT IN('EXTERNAL','RECONCILE','MANUAL')
        AND direction IN('LONG','SHORT') ORDER BY entry_time ASC""").fetchall()
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
            entry_price=_sf(r["entry_price"]),exit_price=_sf(r["exit_price"]),
            notional=_sf(r["notional_usd"]),pnl=_sf(r["realized_pnl"]),
            pnl_pct=_sf(r["realized_pnl_pct"]),fees=_sf(r["total_fees"]),
            exit_reason=r["exit_reason"]or"",strategy=r["strategy"]or"perp",
            pipeline_conv=_sf(r["confidence"]),raw_score=_sf(ctx.get("score")),
            signals=sigs,metrics=ctx.get("key_metrics")or{},
            mae_pct=_sf(r["mae_pct"]),mfe_pct=_sf(r["mfe_pct"]))
        _,adj,would=calc_brain(t["signals"],t["metrics"],t["direction"])
        t["brain"]=adj; t["brain_would"]=would
        t["combined"]=(t["pipeline_conv"]+adj)/2
        trades.append(t)
    return trades

def stats(ts):
    if not ts: return None
    n=len(ts); pnls=[t["pnl"] for t in ts]
    wins=[t for t in ts if t["pnl"]>0]; losses=[t for t in ts if t["pnl"]<=0]
    longs=[t for t in ts if t["direction"]=="LONG"]; shorts=[t for t in ts if t["direction"]=="SHORT"]
    gp=sum(t["pnl"] for t in wins); gl=abs(sum(t["pnl"] for t in losses))
    fees=sum(t["fees"] for t in ts)
    peak=cum=mdd=0
    for p in pnls:
        cum+=p
        if cum>peak: peak=cum
        if peak-cum>mdd: mdd=peak-cum
    mcl=cl=0
    for p in pnls:
        if p<=0: cl+=1; mcl=max(mcl,cl)
        else: cl=0
    holds=[(t["exit_time"]-t["entry_time"])/3600 for t in ts if t["exit_time"]>t["entry_time"]>0]
    maes=[t["mae_pct"] for t in ts if t["mae_pct"]]; mfes=[t["mfe_pct"] for t in ts if t["mfe_pct"]]
    lw=sum(1 for t in longs if t["pnl"]>0); sw=sum(1 for t in shorts if t["pnl"]>0)
    exits={}
    for t in ts: exits[t["exit_reason"]]=exits.get(t["exit_reason"],0)+1
    sym_pnl={}
    for t in ts: sym_pnl.setdefault(t["symbol"],[]).append(t["pnl"])
    sym_net=sorted(((s,sum(ps)) for s,ps in sym_pnl.items()),key=lambda x:x[1])
    return dict(n=n,w=len(wins),l=len(losses),wr=len(wins)/n,
        net=sum(pnls),gp=gp,gl=-gl,fees=fees,
        pf=gp/gl if gl else(float("inf") if gp else 0),
        exp=sum(pnls)/n,
        aw=gp/len(wins) if wins else 0,al=-gl/len(losses) if losses else 0,
        best=max(pnls),worst=min(pnls),mdd=mdd,mcl=mcl,
        nl=len(longs),ns=len(shorts),
        lpnl=sum(t["pnl"] for t in longs),spnl=sum(t["pnl"] for t in shorts),
        lwr=lw/len(longs) if longs else 0,swr=sw/len(shorts) if shorts else 0,
        lw=lw,sw=sw,
        amae=sum(maes)/len(maes) if maes else 0,amfe=sum(mfes)/len(mfes) if mfes else 0,
        ahold=sum(holds)/len(holds) if holds else 0,
        exits=exits,
        top_win=sym_net[-3:] if len(sym_net)>=3 else sym_net,
        top_lose=sym_net[:3])

def print_full(label,s):
    if not s:
        print(f"\n{'─'*90}\n  {label}\n{'─'*90}\n  (no trades)\n")
        return
    print(f"\n{'─'*90}")
    print(f"  {label}")
    print(f"{'─'*90}")
    wlr=abs(s['aw']/s['al']) if s['al'] else 0
    print(f"  Trades:        {s['n']:>5}   (W:{s['w']} L:{s['l']})")
    print(f"  Win Rate:      {s['wr']:>5.1%}   (Long: {s['lwr']:.1%} [{s['lw']}/{s['nl']}] | Short: {s['swr']:.1%} [{s['sw']}/{s['ns']}])")
    print(f"  Net PnL:    ${s['net']:>+10,.2f}   (Long: ${s['lpnl']:>+,.2f} | Short: ${s['spnl']:>+,.2f})")
    print(f"  Gross P/L:  ${s['gp']:>+10,.2f} / ${s['gl']:>+,.2f}   Fees: ${s['fees']:>,.2f}")
    pf_str = "INF" if s['pf']==float('inf') else f"{s['pf']:.2f}"
    print(f"  Profit Factor: {pf_str:>5}   Expectancy: ${s['exp']:>+,.2f}/trade")
    print(f"  Avg Win/Loss: ${s['aw']:>+8,.2f} / ${s['al']:>+,.2f}   (W/L ratio: {wlr:.2f})")
    print(f"  Best/Worst:   ${s['best']:>+8,.2f} / ${s['worst']:>+,.2f}")
    print(f"  Max DD:       ${s['mdd']:>8,.2f}   Max Consec L: {s['mcl']}")
    print(f"  L/S Split:     {s['nl']}L / {s['ns']}S")
    print(f"  Avg MAE/MFE:   {s['amae']:.2f}% / {s['amfe']:.2f}%   Avg Hold: {s['ahold']:.1f}h")
    exits=sorted(s["exits"].items(),key=lambda x:-x[1])[:8]
    print(f"  Exit Reasons:  {', '.join(f'{k}={v}' for k,v in exits)}")
    tw=[f"{s}=${p:+,.0f}" for s,p in reversed(s["top_win"])]
    tl=[f"{s}=${p:+,.0f}" for s,p in s["top_lose"]]
    print(f"  Top Winners:   {', '.join(tw)}")
    print(f"  Top Losers:    {', '.join(tl)}")

def main():
    trades=load_trades()
    print(f"Loaded {len(trades)} trades\n")

    # Define bins
    brain_bins=[(0,0.20,"0.00–0.20"),(0.20,0.40,"0.20–0.40"),(0.40,0.55,"0.40–0.55"),
                (0.55,0.75,"0.55–0.75"),(0.75,0.90,"0.75–0.90"),(0.90,1.01,"0.90–1.00")]
    pipe_bins=[(0,0.20,"0.00–0.20"),(0.20,0.35,"0.20–0.35"),(0.35,0.50,"0.35–0.50"),
               (0.50,0.65,"0.50–0.65"),(0.65,0.80,"0.65–0.80"),(0.80,1.01,"0.80–1.00")]
    comb_bins=[(0,0.25,"0.00–0.25"),(0.25,0.40,"0.25–0.40"),(0.40,0.55,"0.40–0.55"),
               (0.55,0.65,"0.55–0.65"),(0.65,0.75,"0.65–0.75"),(0.75,1.01,"0.75–1.00")]

    # ═══ BRAIN BASKETS ═══
    print("="*90)
    print("  BRAIN CONVICTION BASKETS")
    print("="*90)
    for lo,hi,label in brain_bins:
        subset=[t for t in trades if lo<=t["brain"]<hi]
        print_full(f"Brain {label}  ({len(subset)} trades)",stats(subset))

    # ═══ PIPELINE BASKETS ═══
    print("\n\n"+"="*90)
    print("  PIPELINE CONVICTION BASKETS")
    print("="*90)
    for lo,hi,label in pipe_bins:
        subset=[t for t in trades if lo<=t["pipeline_conv"]<hi]
        print_full(f"Pipeline {label}  ({len(subset)} trades)",stats(subset))

    # ═══ COMBINED BASKETS ═══
    print("\n\n"+"="*90)
    print("  COMBINED CONVICTION BASKETS")
    print("="*90)
    for lo,hi,label in comb_bins:
        subset=[t for t in trades if lo<=t["combined"]<hi]
        print_full(f"Combined {label}  ({len(subset)} trades)",stats(subset))

    # ═══ SUMMARY GRID ═══
    print("\n\n"+"="*120)
    print("  SUMMARY GRID — All Baskets Side-by-Side")
    print("="*120)
    def row(label,bins_list,key,trades_list):
        print(f"\n  {label}:")
        print(f"  {'Bin':>12} {'N':>5} {'WR':>6} {'Net PnL':>11} {'PF':>6} {'Exp':>9} {'MaxDD':>9} {'AvgW':>8} {'AvgL':>8} {'W/L':>5} {'L':>4} {'S':>4} {'LWR':>5} {'SWR':>5}")
        print(f"  {'─'*12} {'─'*5} {'─'*6} {'─'*11} {'─'*6} {'─'*9} {'─'*9} {'─'*8} {'─'*8} {'─'*5} {'─'*4} {'─'*4} {'─'*5} {'─'*5}")
        for lo,hi,lbl in bins_list:
            sub=[t for t in trades_list if lo<=t[key]<hi]
            if not sub:
                print(f"  {lbl:>12} {'0':>5}")
                continue
            s=stats(sub)
            pf_s="INF" if s['pf']==float('inf') else f"{s['pf']:.2f}"
            wlr=abs(s['aw']/s['al']) if s['al'] else 0
            print(f"  {lbl:>12} {s['n']:>5} {s['wr']:>5.1%} ${s['net']:>+9,.0f} {pf_s:>6} ${s['exp']:>+7,.2f} ${s['mdd']:>7,.0f} ${s['aw']:>+6,.0f} ${s['al']:>+6,.0f} {wlr:>5.2f} {s['nl']:>4} {s['ns']:>4} {s['lwr']:>4.0%} {s['swr']:>4.0%}")

    row("BRAIN CONVICTION",brain_bins,"brain",trades)
    row("PIPELINE CONVICTION",pipe_bins,"pipeline_conv",trades)
    row("COMBINED CONVICTION",comb_bins,"combined",trades)

if __name__=="__main__":
    main()
