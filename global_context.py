#!/usr/bin/env python3
"""
Global Context Data for Captain EVP AI Trading Agent.

Provides market-wide reference data for all symbols:
1. IV Term Structure (24h vs Weekly) for BTC and ETH from Deribit
2. Realized Volatility for BTC and ETH
3. Net exposure tracking vs last N snapshots

This data is GLOBAL context - same for all symbols in a cycle.
"""

from logging_utils import get_logger
import time
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple
import requests
import os
from env_utils import EVCLAW_DB_PATH


@dataclass
class IVData:
    """IV term structure for a single asset."""
    symbol: str
    iv_24h: Optional[float]  # Short-term IV (1-3 days)
    iv_weekly: Optional[float]  # Weekly IV (4-12 days)
    iv_ratio: Optional[float]  # 24h / weekly (>1 = backwardation)
    realized_vol: Optional[float]  # Historical realized vol
    timestamp: float
    
    def to_dict(self) -> Dict:
        return {
            'symbol': self.symbol,
            'iv_24h': round(self.iv_24h, 1) if self.iv_24h else None,
            'iv_weekly': round(self.iv_weekly, 1) if self.iv_weekly else None,
            'iv_ratio': round(self.iv_ratio, 2) if self.iv_ratio else None,
            'realized_vol': round(self.realized_vol, 1) if self.realized_vol else None,
        }


@dataclass
class ExposureSnapshot:
    """Net exposure at a point in time."""
    timestamp: float
    long_notional: float
    short_notional: float
    net_notional: float  # long - short (positive = net long)
    
    def to_dict(self) -> Dict:
        return {
            'long': round(self.long_notional, 2),
            'short': round(self.short_notional, 2),
            'net': round(self.net_notional, 2),
        }


@dataclass
class GlobalContext:
    """Global market context for all symbols."""
    btc_iv: Optional[IVData]
    eth_iv: Optional[IVData]
    current_exposure: Optional[ExposureSnapshot]
    exposure_history: List[ExposureSnapshot]  # Last N snapshots
    exposure_drift: Optional[float]  # Current net vs avg of last N
    iv_health: Dict[str, Any]
    generated_at: str
    
    def to_dict(self) -> Dict:
        hist_avg = None
        if self.exposure_history:
            hist_avg = sum(e.net_notional for e in self.exposure_history) / len(self.exposure_history)
        
        return {
            'btc_iv': self.btc_iv.to_dict() if self.btc_iv else None,
            'eth_iv': self.eth_iv.to_dict() if self.eth_iv else None,
            'exposure': {
                'current': self.current_exposure.to_dict() if self.current_exposure else None,
                'history_avg': round(hist_avg, 2) if hist_avg else None,
                'drift': round(self.exposure_drift, 2) if self.exposure_drift else None,
                'history_count': len(self.exposure_history),
            },
            'iv_health': self.iv_health,
            'generated_at': self.generated_at,
        }
    
    def format_compact(self) -> str:
        """One-line compact format for context."""
        parts = []
        
        # IV section
        iv_parts = []
        if self.btc_iv and self.btc_iv.iv_24h and self.btc_iv.iv_weekly:
            iv_parts.append(f"BTC {self.btc_iv.iv_24h:.0f}/{self.btc_iv.iv_weekly:.0f} ({self.btc_iv.iv_ratio:.2f}x)")
        if self.eth_iv and self.eth_iv.iv_24h and self.eth_iv.iv_weekly:
            iv_parts.append(f"ETH {self.eth_iv.iv_24h:.0f}/{self.eth_iv.iv_weekly:.0f} ({self.eth_iv.iv_ratio:.2f}x)")
        if iv_parts:
            parts.append("IV: " + " | ".join(iv_parts))
        
        # RV section
        rv_parts = []
        if self.btc_iv and self.btc_iv.realized_vol:
            rv_parts.append(f"BTC {self.btc_iv.realized_vol:.0f}")
        if self.eth_iv and self.eth_iv.realized_vol:
            rv_parts.append(f"ETH {self.eth_iv.realized_vol:.0f}")
        if rv_parts:
            parts.append("RV: " + " ".join(rv_parts))
        
        # Exposure section
        if self.current_exposure:
            exp = self.current_exposure
            exp_str = f"Exp: {exp.long_notional/1000:.1f}K L / {exp.short_notional/1000:.1f}K S (net {exp.net_notional/1000:+.1f}K)"
            if self.exposure_drift is not None and self.exposure_history:
                hist_avg = sum(e.net_notional for e in self.exposure_history) / len(self.exposure_history)
                drift_dir = "→ LONG" if self.exposure_drift > 500 else "→ SHORT" if self.exposure_drift < -500 else "→ flat"
                exp_str += f" | 10-avg: {hist_avg/1000:+.1f}K {drift_dir}"
            parts.append(exp_str)
        
        return " | ".join(parts) if parts else "Global context unavailable"


class GlobalContextProvider:
    """
    Fetches and caches global context data.
    
    - Deribit IV data cached for 5 minutes
    - Exposure from monitor_snapshots table
    """
    
    DERIBIT_BASE = "https://www.deribit.com/api/v2/public"
    IV_CACHE_TTL = 300  # 5 minutes
    EXPOSURE_HISTORY_COUNT = 10
    
    def __init__(self, db_path: Optional[str] = None):
        self.log = get_logger("global_context")
        self.db_path = db_path or EVCLAW_DB_PATH
        self.iv_alert_consec_fail = 3
        self.iv_alert_cooldown_sec = 900.0
        
        # Cache
        self._iv_cache: Dict[str, IVData] = {}
        self._iv_cache_time: Dict[str, float] = {}
        self._iv_fail_counts: Dict[str, int] = {}
        self._iv_last_error: Dict[str, str] = {}
        self._iv_last_ok_ts: Dict[str, float] = {}
        self._iv_last_alert_ts: Dict[str, float] = {}
    
    def get_global_context(self) -> GlobalContext:
        """Get complete global context."""
        now = time.time()
        
        # Fetch IV data (cached)
        btc_iv = self._get_iv_data("BTC")
        eth_iv = self._get_iv_data("ETH")
        
        # Fetch exposure data
        current_exp, exp_history = self._get_exposure_data()
        
        # Calculate drift
        drift = None
        if current_exp and exp_history:
            hist_avg = sum(e.net_notional for e in exp_history) / len(exp_history)
            drift = current_exp.net_notional - hist_avg
        
        return GlobalContext(
            btc_iv=btc_iv,
            eth_iv=eth_iv,
            current_exposure=current_exp,
            exposure_history=exp_history,
            exposure_drift=drift,
            iv_health=self._build_iv_health(),
            generated_at=datetime.now(timezone.utc).isoformat(),
        )

    def _mark_iv_success(self, currency: str, ts: float) -> None:
        self._iv_fail_counts[currency] = 0
        self._iv_last_ok_ts[currency] = ts
        self._iv_last_error[currency] = ""

    def _mark_iv_failure(self, currency: str, err: str, now: float) -> None:
        count = int(self._iv_fail_counts.get(currency, 0) or 0) + 1
        self._iv_fail_counts[currency] = count
        self._iv_last_error[currency] = str(err or "")
        if count >= self.iv_alert_consec_fail:
            last_alert = float(self._iv_last_alert_ts.get(currency, 0.0) or 0.0)
            if (now - last_alert) >= self.iv_alert_cooldown_sec:
                self._iv_last_alert_ts[currency] = now
                self.log.warning(
                    "IV health degraded for %s: consecutive_failures=%s last_error=%s",
                    currency,
                    count,
                    self._iv_last_error[currency],
                )

    def _build_iv_health(self) -> Dict[str, Any]:
        assets = ("BTC", "ETH")
        health: Dict[str, Any] = {}
        degraded_any = False
        for asset in assets:
            failures = int(self._iv_fail_counts.get(asset, 0) or 0)
            last_ok_ts = self._iv_last_ok_ts.get(asset)
            status = "degraded" if failures >= self.iv_alert_consec_fail else "ok"
            if status == "degraded":
                degraded_any = True
            health[asset.lower()] = {
                "status": status,
                "consecutive_failures": failures,
                "last_ok_ts": float(last_ok_ts) if last_ok_ts else None,
                "last_error": self._iv_last_error.get(asset) or None,
            }
        health["degraded"] = degraded_any
        return health
    
    def _get_iv_data(self, currency: str) -> Optional[IVData]:
        """Get IV term structure for a currency."""
        now = time.time()
        
        # Check cache
        cached_at = float(self._iv_cache_time.get(currency, 0.0) or 0.0)
        if currency in self._iv_cache and (now - cached_at) < self.IV_CACHE_TTL:
            return self._iv_cache[currency]
        
        try:
            # Fetch options data from Deribit
            resp = requests.get(
                f"{self.DERIBIT_BASE}/get_book_summary_by_currency",
                params={"currency": currency, "kind": "option"},
                timeout=10
            )
            resp.raise_for_status()
            options = resp.json().get('result', [])
            
            if not options:
                self._mark_iv_failure(currency, "empty_options_result", now)
                return None
            
            # Parse expiries and calculate OI-weighted IV
            months = {'JAN':1,'FEB':2,'MAR':3,'APR':4,'MAY':5,'JUN':6,
                      'JUL':7,'AUG':8,'SEP':9,'OCT':10,'NOV':11,'DEC':12}
            
            short_term = []  # 0.5-3.5 days
            weekly = []  # 3.5-12 days
            
            now_dt = datetime.now(timezone.utc)
            
            for o in options:
                name = o.get('instrument_name', '')
                mark_iv = o.get('mark_iv')
                oi = o.get('open_interest', 0) or 0
                
                if not mark_iv or mark_iv <= 0:
                    continue
                
                parts = name.split('-')
                if len(parts) < 3:
                    continue
                
                try:
                    exp_str = parts[1]
                    day = int(exp_str[:-5])
                    month = months.get(exp_str[-5:-2], 0)
                    year = int("20" + exp_str[-2:])
                    if month == 0:
                        continue
                    
                    expiry_dt = datetime(year, month, day, 8, 0, tzinfo=timezone.utc)
                    days = (expiry_dt - now_dt).total_seconds() / 86400
                    
                    if 0.5 <= days < 3.5:
                        short_term.append({'iv': mark_iv, 'oi': oi})
                    elif 3.5 <= days < 12:
                        weekly.append({'iv': mark_iv, 'oi': oi})
                except Exception:                    continue
            
            # Calculate OI-weighted averages
            def weighted_iv(opts):
                if not opts:
                    return None
                total_oi = sum(o['oi'] for o in opts)
                if total_oi > 0:
                    return sum(o['iv'] * o['oi'] for o in opts) / total_oi
                return sum(o['iv'] for o in opts) / len(opts)
            
            iv_24h = weighted_iv(short_term)
            iv_weekly = weighted_iv(weekly)
            
            # Get realized vol from Deribit
            realized_vol = self._get_realized_vol(currency)
            
            iv_data = IVData(
                symbol=currency,
                iv_24h=iv_24h,
                iv_weekly=iv_weekly,
                iv_ratio=iv_24h / iv_weekly if iv_24h and iv_weekly else None,
                realized_vol=realized_vol,
                timestamp=now,
            )
            
            # Update cache
            self._iv_cache[currency] = iv_data
            self._iv_cache_time[currency] = now
            self._mark_iv_success(currency, now)
            
            return iv_data
            
        except Exception as e:
            self.log.warning(f"Failed to fetch IV for {currency}: {e}")
            self._mark_iv_failure(currency, str(e), now)
            # Return cached if available
            return self._iv_cache.get(currency)
    
    def _get_realized_vol(self, currency: str) -> Optional[float]:
        """Get realized volatility from Deribit historical vol endpoint."""
        try:
            resp = requests.get(
                f"{self.DERIBIT_BASE}/get_historical_volatility",
                params={"currency": currency},
                timeout=10
            )
            resp.raise_for_status()
            data = resp.json().get('result', [])
            
            if data and len(data) > 0:
                # Returns [[timestamp, vol], ...] - get latest
                return data[-1][1]
            return None
        except Exception as e:
            self.log.debug(f"Failed to fetch RV for {currency}: {e}")
            return None
    
    def _get_exposure_data(self) -> Tuple[Optional[ExposureSnapshot], List[ExposureSnapshot]]:
        """Get current and historical exposure from monitor_snapshots."""
        try:
            with sqlite3.connect(self.db_path) as con:
                cur = con.cursor()
                # Get last N+1 snapshots (current + history)
                cur.execute("""
                    SELECT ts, hl_long_notional, hl_short_notional
                    FROM monitor_snapshots
                    ORDER BY ts DESC
                    LIMIT ?
                """, (self.EXPOSURE_HISTORY_COUNT + 1,))
                rows = cur.fetchall()
            
            if not rows:
                return None, []
            
            snapshots = []
            for ts, long_n, short_n in rows:
                long_n = long_n or 0
                short_n = short_n or 0
                snapshots.append(ExposureSnapshot(
                    timestamp=ts,
                    long_notional=long_n,
                    short_notional=short_n,
                    net_notional=long_n - short_n,
                ))
            
            current = snapshots[0] if snapshots else None
            history = snapshots[1:] if len(snapshots) > 1 else []
            
            return current, history
            
        except Exception as e:
            self.log.warning(f"Failed to get exposure data: {e}")
            return None, []


# =============================================================================
# CLI for testing
# =============================================================================

if __name__ == '__main__':
    import json
    
    provider = GlobalContextProvider()
    ctx = provider.get_global_context()
    
    print("\n=== Global Context ===")
    print(json.dumps(ctx.to_dict(), indent=2))
    
    print("\n=== Compact Format ===")
    print(ctx.format_compact())
