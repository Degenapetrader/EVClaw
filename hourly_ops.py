#!/usr/bin/env python3
"""Deterministic hourly operations runner for EVClaw.

This script is intentionally non-LLM and deterministic:
- process/tmux self-heal
- cycle freshness + stuck claim unlock
- SL/TP incident reconciliation
- pending SR-limit zombie reconciliation
- unmatched live entry cleanup (aggressive mode)
- open position protection audit
- DB maintenance (check + midnight repair path)
- reflection queue backfill
- disk check

It writes:
- JSON report (machine-readable)
- short summary lines (ops/Telegram-friendly)
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import shutil
import sqlite3
import subprocess
import sys
import time
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple

from exchanges.hyperliquid_adapter import HyperliquidAdapter


ROOT_DIR = Path(__file__).resolve().parent
REPO_NAME = "evclaw"
DEFAULT_DB_PATH = ROOT_DIR / "ai_trader.db"
DEFAULT_INFO_URL = "https://node2.evplus.ai/evclaw/info"
DEFAULT_PUBLIC_INFO_URL = "https://api.hyperliquid.xyz/info"
DEFAULT_JSON_OUT = ROOT_DIR / "state" / "hourly_ops_report.json"
DEFAULT_SUMMARY_OUT = ROOT_DIR / "state" / "hourly_ops_summary.txt"
DEFAULT_CYCLE_LATEST = Path("/tmp/evclaw_cycle_latest.json")
DOTENV_PATH = ROOT_DIR / ".env"

SESSION_NAMES = [
    "evclaw-cycle-trigger",
    "evclaw-live-agent",
    "evclaw-live-monitor",
    "evclaw-fill-reconciler",
    "evclaw-exit-decider",
    "evclaw-hip3-exit-decider",
    "evclaw-exit-outcome",
    "evclaw-decay",
    "evclaw-review",
    "evclaw-learning-reflector",
]

OPEN_TRADE_VENUES = {"hyperliquid", "hip3", "hyperliquid_wallet"}
WALLET_VENUES = {"hyperliquid", "hip3", "hyperliquid_wallet"}

LOG = logging.getLogger("hourly_ops")


@dataclass
class AccountCtx:
    label: str
    user: str
    adapter_key: str


def _setup_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(level=level, format="%(asctime)s %(levelname)s %(message)s")


def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _normalize_info_url(raw: str) -> str:
    base = (raw or "").strip().rstrip("/")
    if not base:
        return DEFAULT_INFO_URL
    if base.endswith("/info"):
        return base
    return f"{base}/info"


def _append_private_key_if_needed(info_url: str, wallet_key: str) -> str:
    key = (wallet_key or "").strip()
    if not key:
        return info_url
    if "node2.evplus.ai" not in info_url:
        return info_url
    if "/evclaw/info" not in info_url:
        return info_url

    parsed = urllib.parse.urlsplit(info_url)
    params = dict(urllib.parse.parse_qsl(parsed.query, keep_blank_values=True))
    if params.get("key"):
        return info_url
    params["key"] = key
    query = urllib.parse.urlencode(params)
    return urllib.parse.urlunsplit((parsed.scheme, parsed.netloc, parsed.path, query, parsed.fragment))


def _load_dotenv(path: Path) -> Dict[str, str]:
    data: Dict[str, str] = {}
    if not path.exists():
        return data
    try:
        for line in path.read_text(encoding="utf-8").splitlines():
            s = line.strip()
            if not s or s.startswith("#") or "=" not in s:
                continue
            k, v = s.split("=", 1)
            data[k.strip()] = v.strip().strip('"').strip("'")
    except Exception:
        return data
    return data


def _env(name: str, dotenv: Dict[str, str], default: str = "") -> str:
    v = (os.getenv(name) or "").strip()
    if v:
        return v
    return (dotenv.get(name) or default).strip()


def _run(cmd: Sequence[str], cwd: Path = ROOT_DIR) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        list(cmd),
        cwd=str(cwd),
        text=True,
        capture_output=True,
        check=False,
    )


def _tmux_exists(session: str) -> bool:
    cp = _run(["tmux", "has-session", "-t", session])
    return cp.returncode == 0


def _post_info(
    info_url: str,
    payload: Dict[str, Any],
    timeout: float = 8.0,
    *,
    fallback_info_url: str = DEFAULT_PUBLIC_INFO_URL,
) -> Any:
    urls: List[str] = [info_url]
    if fallback_info_url and fallback_info_url != info_url:
        urls.append(fallback_info_url)

    last_exc: Optional[Exception] = None
    for target_url in urls:
        try:
            req = urllib.request.Request(
                target_url,
                data=json.dumps(payload).encode("utf-8"),
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                raw = resp.read().decode("utf-8")
            if not raw:
                return {}
            return json.loads(raw)
        except Exception as exc:
            last_exc = exc
            continue

    if last_exc is not None:
        raise last_exc
    return {}


def _safe_float(x: Any, default: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return default


def _symbol_to_coin(symbol: str) -> str:
    s = str(symbol or "").strip().upper()
    if ":" in s:
        return s.split(":", 1)[1].strip().upper()
    return s


def _current_disk_usage_pct(path: Path) -> float:
    usage = shutil.disk_usage(str(path))
    if usage.total <= 0:
        return 0.0
    return float(usage.used) * 100.0 / float(usage.total)


def _fetch_open_orders(
    info_url: str,
    user: str,
    *,
    fallback_info_url: str = DEFAULT_PUBLIC_INFO_URL,
) -> List[Dict[str, Any]]:
    out = _post_info(
        info_url,
        {"type": "openOrders", "user": user},
        fallback_info_url=fallback_info_url,
    )
    return out if isinstance(out, list) else []


def _fetch_clearinghouse_state(
    info_url: str,
    user: str,
    *,
    fallback_info_url: str = DEFAULT_PUBLIC_INFO_URL,
) -> Dict[str, Any]:
    out = _post_info(
        info_url,
        {"type": "clearinghouseState", "user": user},
        fallback_info_url=fallback_info_url,
    )
    return out if isinstance(out, dict) else {}


async def _init_adapters(dotenv: Dict[str, str], report: Dict[str, Any]) -> Dict[str, HyperliquidAdapter]:
    adapters: Dict[str, HyperliquidAdapter] = {}

    wallet = HyperliquidAdapter(LOG, dry_run=False, account_mode="wallet")
    if await wallet.initialize():
        adapters["wallet"] = wallet
    else:
        report["warnings"].append("hyperliquid_wallet_adapter_init_failed")

    return adapters


def _build_accounts(dotenv: Dict[str, str], adapters: Dict[str, HyperliquidAdapter]) -> List[AccountCtx]:
    accounts: List[AccountCtx] = []
    wallet_addr = _env("HYPERLIQUID_ADDRESS", dotenv)
    if wallet_addr and "wallet" in adapters:
        accounts.append(AccountCtx(label="wallet", user=wallet_addr, adapter_key="wallet"))

    return accounts


def _adapter_for_venue(
    venue: str,
    adapters: Dict[str, HyperliquidAdapter],
) -> Optional[HyperliquidAdapter]:
    _ = venue
    return adapters.get("wallet")


def _append_action(report: Dict[str, Any], msg: str) -> None:
    report["actions"].append(msg)


def _enqueue_orphan(
    conn: sqlite3.Connection,
    *,
    symbol: str,
    venue: str,
    order_id: str,
    reason: str,
    meta: Dict[str, Any],
) -> None:
    try:
        conn.execute(
            """
            INSERT INTO orphan_orders (symbol, venue, order_id, order_type, reason, meta_json)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                symbol,
                venue,
                order_id,
                "HOURLY_OPS",
                reason,
                json.dumps(meta, sort_keys=True),
            ),
        )
    except Exception:
        return


def _update_pending_state(
    conn: sqlite3.Connection,
    *,
    order_id: str,
    state: str,
    reason: str,
    filled_size: float = 0.0,
    filled_price: float = 0.0,
) -> None:
    conn.execute(
        """
        UPDATE pending_orders
        SET state = ?, cancel_reason = ?, filled_size = ?, filled_price = ?,
            updated_at = strftime('%s', 'now')
        WHERE exchange_order_id = ?
        """,
        (state, reason, float(filled_size), float(filled_price), str(order_id)),
    )


async def _reconcile_pending_orders(
    conn: sqlite3.Connection,
    adapters: Dict[str, HyperliquidAdapter],
    *,
    apply_fixes: bool,
    report: Dict[str, Any],
) -> Dict[str, Any]:
    out: Dict[str, Any] = {
        "pending_total": 0,
        "expired_total": 0,
        "filled_marked": 0,
        "cancelled_marked": 0,
        "expired_cancelled": 0,
        "cancel_failures": 0,
    }

    try:
        rows = conn.execute(
            """
            SELECT symbol, venue, exchange_order_id, expires_at
            FROM pending_orders
            WHERE state = 'PENDING' AND exchange_order_id IS NOT NULL
            """
        ).fetchall()
    except Exception as exc:
        report["warnings"].append(f"pending_orders_query_failed:{exc}")
        return out

    now_ts = time.time()
    out["pending_total"] = len(rows)

    for row in rows:
        symbol = str(row["symbol"] or "").strip()
        venue = str(row["venue"] or "").strip().lower()
        oid = str(row["exchange_order_id"] or "").strip()
        expires_at = _safe_float(row["expires_at"], 0.0)
        expired = expires_at > 0 and now_ts > expires_at
        if expired:
            out["expired_total"] += 1

        if not symbol or not oid:
            continue

        adapter = _adapter_for_venue(venue, adapters)
        if adapter is None:
            report["warnings"].append(f"pending_order_no_adapter:{venue}:{symbol}:{oid}")
            continue

        status = await adapter.check_order_status(symbol, oid)
        st = str(status.get("status") or "unknown").lower()
        filled_size = _safe_float(status.get("filled_size"), 0.0)
        avg_price = _safe_float(status.get("avg_price"), 0.0)

        if st == "filled":
            _update_pending_state(
                conn,
                order_id=oid,
                state="FILLED",
                reason="FILLED_HOURLY_RECONCILE",
                filled_size=filled_size,
                filled_price=avg_price,
            )
            out["filled_marked"] += 1
            continue

        if st in {"canceled", "cancelled", "rejected"}:
            _update_pending_state(
                conn,
                order_id=oid,
                state="CANCELLED",
                reason="CANCELLED_ON_EXCHANGE",
                filled_size=filled_size,
                filled_price=avg_price,
            )
            out["cancelled_marked"] += 1
            continue

        if expired and apply_fixes and st in {"open", "partially_filled", "unknown"}:
            ok_cancel = False
            try:
                ok_cancel = bool(await adapter.cancel_order(symbol, oid))
            except Exception:
                ok_cancel = False
            if ok_cancel:
                _update_pending_state(
                    conn,
                    order_id=oid,
                    state="CANCELLED",
                    reason="EXPIRED_TIMEOUT_CANCELLED",
                    filled_size=filled_size,
                    filled_price=avg_price,
                )
                out["expired_cancelled"] += 1
                _append_action(report, f"expired_pending_cancelled:{symbol}:{venue}:{oid}")
            else:
                out["cancel_failures"] += 1
                _enqueue_orphan(
                    conn,
                    symbol=symbol,
                    venue=venue,
                    order_id=oid,
                    reason="EXPIRED_CANCEL_FAILED",
                    meta={"status": st},
                )

    return out


def _known_order_ids(conn: sqlite3.Connection) -> Set[str]:
    out: Set[str] = set()
    try:
        for row in conn.execute(
            "SELECT exchange_order_id FROM pending_orders WHERE state='PENDING' AND exchange_order_id IS NOT NULL"
        ).fetchall():
            oid = str(row[0] or "").strip()
            if oid:
                out.add(oid)
    except Exception:
        pass

    try:
        for row in conn.execute(
            """
            SELECT sl_order_id, tp_order_id
            FROM trades
            WHERE exit_time IS NULL
            """
        ).fetchall():
            sl = str(row[0] or "").strip()
            tp = str(row[1] or "").strip()
            if sl:
                out.add(sl)
            if tp:
                out.add(tp)
    except Exception:
        pass
    return out


async def _cancel_unmatched_live_entries(
    conn: sqlite3.Connection,
    info_url: str,
    fallback_info_url: str,
    accounts: List[AccountCtx],
    adapters: Dict[str, HyperliquidAdapter],
    *,
    apply_fixes: bool,
    cancel_unmatched: bool,
    report: Dict[str, Any],
) -> Tuple[Dict[str, Any], Dict[str, List[Dict[str, Any]]], Dict[str, Dict[str, Any]]]:
    metrics: Dict[str, Any] = {
        "live_open_total": 0,
        "unmatched_total": 0,
        "cancelled": 0,
        "cancel_failed": 0,
    }

    open_orders_by_account: Dict[str, List[Dict[str, Any]]] = {}
    states_by_account: Dict[str, Dict[str, Any]] = {}
    known = _known_order_ids(conn)

    for acct in accounts:
        try:
            orders = _fetch_open_orders(
                info_url,
                acct.user,
                fallback_info_url=fallback_info_url,
            )
        except Exception as exc:
            report["warnings"].append(f"open_orders_fetch_failed:{acct.label}:{exc}")
            orders = []
        open_orders_by_account[acct.label] = orders
        metrics["live_open_total"] += len(orders)

        try:
            states_by_account[acct.label] = _fetch_clearinghouse_state(
                info_url,
                acct.user,
                fallback_info_url=fallback_info_url,
            )
        except Exception:
            states_by_account[acct.label] = {}

        adapter = adapters.get(acct.adapter_key)
        for order in orders:
            oid = str(order.get("oid") or "").strip()
            if not oid:
                continue
            if oid in known:
                continue
            if bool(order.get("reduceOnly")):
                continue
            metrics["unmatched_total"] += 1
            coin = str(order.get("coin") or "").strip()
            if not coin:
                continue

            if not (apply_fixes and cancel_unmatched and adapter):
                continue

            symbol_candidates = [coin]
            if ":" not in coin and acct.label == "wallet":
                symbol_candidates.extend([f"xyz:{coin}", f"cash:{coin}"])

            cancelled = False
            for sym in symbol_candidates:
                try:
                    if await adapter.cancel_order(sym, oid):
                        cancelled = True
                        _append_action(report, f"cancel_unmatched:{acct.label}:{sym}:{oid}")
                        break
                except Exception:
                    continue

            if cancelled:
                metrics["cancelled"] += 1
            else:
                metrics["cancel_failed"] += 1
                _enqueue_orphan(
                    conn,
                    symbol=coin,
                    venue="hyperliquid",
                    order_id=oid,
                    reason="UNMATCHED_CANCEL_FAILED",
                    meta={"account": acct.label},
                )

    return metrics, open_orders_by_account, states_by_account


def _extract_positions(state: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    for item in state.get("assetPositions", []) if isinstance(state, dict) else []:
        pos = item.get("position", item) if isinstance(item, dict) else {}
        coin = str(pos.get("coin") or "").strip().upper()
        if not coin:
            continue
        szi = _safe_float(pos.get("szi", pos.get("size", 0.0)), 0.0)
        if szi == 0:
            continue
        out[coin] = {
            "direction": "LONG" if szi > 0 else "SHORT",
            "size": abs(szi),
        }
    return out


def _is_builder_symbol(symbol: str) -> bool:
    s = str(symbol or "").strip()
    return ":" in s


def _normalize_order_status(raw: Any) -> str:
    s = str(raw or "").strip().lower()
    if s == "cancelled":
        s = "canceled"
    if s in {
        "open",
        "resting",
        "partially_filled",
        "filledorresting",
        "filled",
        "canceled",
        "rejected",
        "unknown",
    }:
        return s
    return "unknown"


def _is_openish_status(status: str) -> bool:
    return status in {"open", "resting", "partially_filled", "filledorresting"}


async def _audit_positions(
    conn: sqlite3.Connection,
    *,
    open_orders_by_account: Dict[str, List[Dict[str, Any]]],
    states_by_account: Dict[str, Dict[str, Any]],
    adapters: Dict[str, HyperliquidAdapter],
    report: Dict[str, Any],
) -> Dict[str, Any]:
    venue_list = sorted(OPEN_TRADE_VENUES)
    placeholders = ",".join("?" for _ in venue_list)
    db_rows = conn.execute(
        f"""
        SELECT id, symbol, direction, venue, sl_order_id, tp_order_id
        FROM trades
        WHERE exit_time IS NULL
          AND venue IN ({placeholders})
        """,
        venue_list,
    ).fetchall()

    db_by_coin: Dict[str, List[sqlite3.Row]] = {}
    for r in db_rows:
        coin = _symbol_to_coin(str(r["symbol"] or ""))
        if not coin:
            continue
        db_by_coin.setdefault(coin, []).append(r)

    exchange_positions: Dict[str, Dict[str, Any]] = {}
    for st in states_by_account.values():
        exchange_positions.update(_extract_positions(st))

    all_orders: List[Dict[str, Any]] = []
    for orders in open_orders_by_account.values():
        all_orders.extend(orders)

    orders_by_coin: Dict[str, List[Dict[str, Any]]] = {}
    for o in all_orders:
        coin = str(o.get("coin") or "").strip().upper()
        if not coin:
            continue
        orders_by_coin.setdefault(coin, []).append(o)

    missing_in_db: List[str] = []
    for coin, pos in sorted(exchange_positions.items()):
        if coin not in db_by_coin:
            missing_in_db.append(f"{coin}:{pos['direction']}")

    unprotected_builder: List[str] = []
    unprotected_perps: List[str] = []
    unknown_builder_protection: List[str] = []

    for r in db_rows:
        trade_id = int(r["id"])
        symbol = str(r["symbol"] or "").strip().upper()
        venue = str(r["venue"] or "").strip().lower()
        coin = _symbol_to_coin(symbol)
        sl = str(r["sl_order_id"] or "").strip()
        tp = str(r["tp_order_id"] or "").strip()

        if _is_builder_symbol(symbol):
            if not sl or not tp:
                unprotected_builder.append(
                    f"{symbol}:trade_id={trade_id}:reason=missing_sltp_oid"
                )
                continue

            adapter = _adapter_for_venue(venue, adapters)
            if adapter is None:
                unknown_builder_protection.append(
                    f"{symbol}:trade_id={trade_id}:reason=missing_adapter"
                )
                report["warnings"].append(f"builder_protection_missing_adapter:{symbol}:{venue}")
                continue

            sl_status = "unknown"
            tp_status = "unknown"
            try:
                sl_status = _normalize_order_status(
                    (await adapter.check_order_status(symbol, sl)).get("status")
                )
            except Exception as exc:
                report["warnings"].append(
                    f"builder_sl_order_status_failed:{symbol}:{trade_id}:{exc}"
                )

            try:
                tp_status = _normalize_order_status(
                    (await adapter.check_order_status(symbol, tp)).get("status")
                )
            except Exception as exc:
                report["warnings"].append(
                    f"builder_tp_order_status_failed:{symbol}:{trade_id}:{exc}"
                )

            if sl_status in {"canceled", "rejected"} or tp_status in {"canceled", "rejected"}:
                unprotected_builder.append(
                    f"{symbol}:trade_id={trade_id}:sl={sl_status}:tp={tp_status}"
                )
                continue

            if sl_status == "unknown" or tp_status == "unknown":
                # Keep unknown in a separate bucket to avoid inflating hard
                # unprotected counts from transient API visibility issues.
                unknown_builder_protection.append(
                    f"{symbol}:trade_id={trade_id}:sl={sl_status}:tp={tp_status}"
                )
                continue

            if _is_openish_status(sl_status) and (_is_openish_status(tp_status) or tp_status == "filled"):
                continue

            unprotected_builder.append(
                f"{symbol}:trade_id={trade_id}:sl={sl_status}:tp={tp_status}"
            )
            continue

        coin_orders = orders_by_coin.get(coin, [])
        open_oids = {str(o.get("oid")) for o in coin_orders if o.get("oid") is not None}
        reduce_only = sum(1 for o in coin_orders if bool(o.get("reduceOnly")))

        protected = bool(sl and tp and sl in open_oids and tp in open_oids)
        if not protected and reduce_only >= 2:
            protected = True
        if not protected:
            unprotected_perps.append(
                f"{symbol}:trade_id={trade_id}:open_reduce_only={reduce_only}"
            )

    unprotected = list(unprotected_perps) + list(unprotected_builder)

    total_equity = 0.0
    for st in states_by_account.values():
        ms = st.get("marginSummary", {}) if isinstance(st, dict) else {}
        total_equity += _safe_float(ms.get("accountValue"), 0.0)

    return {
        "exchange_open_positions": len(exchange_positions),
        "missing_in_db": missing_in_db,
        "unprotected_builder": unprotected_builder,
        "unprotected_perps": unprotected_perps,
        "unknown_builder_protection": unknown_builder_protection,
        "unprotected": unprotected,
        "equity_total": total_equity,
    }


def _cycle_freshness(path: Path) -> Optional[int]:
    if not path.exists():
        return None
    return int(time.time() - path.stat().st_mtime)


def _unlock_stuck_claims(conn: sqlite3.Connection, *, apply_fixes: bool) -> Dict[str, int]:
    out = {"stuck_total": 0, "unlocked": 0}
    now = time.time()
    rows = conn.execute(
        """
        SELECT seq, claimed_at
        FROM cycle_runs
        WHERE processed_at IS NULL
          AND claimed_at IS NOT NULL
        ORDER BY claimed_at ASC
        """
    ).fetchall()
    stale: List[int] = []
    for r in rows:
        seq = int(r["seq"])
        claimed_at = _safe_float(r["claimed_at"], 0.0)
        if claimed_at > 0 and (now - claimed_at) > 120.0:
            stale.append(seq)
    out["stuck_total"] = len(stale)
    if apply_fixes:
        for seq in stale:
            cur = conn.execute(
                """
                UPDATE cycle_runs
                SET claimed_at = NULL, claimed_by = NULL
                WHERE seq = ?
                  AND processed_at IS NULL
                """,
                (seq,),
            )
            out["unlocked"] += int(cur.rowcount or 0)
    return out


def _run_db_maintenance(db_path: Path, *, apply_fixes: bool) -> Dict[str, Any]:
    out: Dict[str, Any] = {
        "check_rc": 0,
        "check_stdout": "",
        "repair_rc": None,
        "backup_rc": None,
        "repair_attempted": False,
    }
    check = _run([sys.executable, "db_maintenance.py", "--db", str(db_path), "--check"])
    out["check_rc"] = int(check.returncode)
    out["check_stdout"] = (check.stdout or "").strip()

    utc_hour = datetime.now(timezone.utc).hour
    if out["check_rc"] != 0 and apply_fixes and utc_hour == 0:
        out["repair_attempted"] = True
        backup = _run(
            [sys.executable, "db_maintenance.py", "--db", str(db_path), "--backup", "--keep", "14"]
        )
        repair = _run(
            [sys.executable, "db_maintenance.py", "--db", str(db_path), "--repair", "--check"]
        )
        out["backup_rc"] = int(backup.returncode)
        out["repair_rc"] = int(repair.returncode)
    return out


def _enqueue_reflection_backfill(conn: sqlite3.Connection) -> Dict[str, Any]:
    out: Dict[str, Any] = {"inserted": 0, "queue_counts": {}}
    now = time.time()
    before = conn.total_changes
    conn.execute(
        """
        INSERT OR IGNORE INTO reflection_tasks_v1 (
            trade_id, symbol, status, attempts, created_at, updated_at
        )
        SELECT t.id, t.symbol, 'PENDING', 0, ?, ?
        FROM trades t
        WHERE t.exit_time IS NOT NULL
          AND t.exit_time >= ?
        """,
        (now, now, now - 7200.0),
    )
    out["inserted"] = max(0, int(conn.total_changes - before))
    rows = conn.execute(
        "SELECT status, COUNT(*) AS n FROM reflection_tasks_v1 GROUP BY status"
    ).fetchall()
    out["queue_counts"] = {str(r["status"]): int(r["n"]) for r in rows}
    return out


def _recent_closes(conn: sqlite3.Connection) -> Dict[str, Any]:
    cutoff = time.time() - 3600.0
    rows = conn.execute(
        """
        SELECT symbol, realized_pnl
        FROM trades
        WHERE exit_time IS NOT NULL AND exit_time >= ?
        ORDER BY exit_time DESC
        LIMIT 20
        """,
        (cutoff,),
    ).fetchall()
    top = []
    for r in rows[:3]:
        sym = str(r["symbol"] or "").upper()
        pnl = _safe_float(r["realized_pnl"], 0.0)
        top.append(f"{sym}:{pnl:+.2f}")
    return {"count": len(rows), "top": top}


def _health_level(report: Dict[str, Any]) -> str:
    crit = 0
    warn = len(report["warnings"])

    tmux_down = int(report["checks"].get("tmux", {}).get("down_after_heal", 0))
    if tmux_down > 0:
        crit += 1

    if report["checks"].get("db_maintenance", {}).get("check_rc", 0) != 0:
        warn += 1

    if int(report["checks"].get("pending_reconcile", {}).get("cancel_failures", 0)) > 0:
        warn += 1

    if int(report["checks"].get("unmatched_live_entries", {}).get("cancel_failed", 0)) > 0:
        warn += 1

    if crit > 0:
        return "CRIT"
    if warn > 0:
        return "WARN"
    return "OK"


def _write_outputs(report: Dict[str, Any], *, json_out: Path, summary_out: Path) -> List[str]:
    report["health"] = _health_level(report)

    tmux = report["checks"].get("tmux", {})
    cycle_age = report["checks"].get("cycle_freshness_sec")
    db_maint = report["checks"].get("db_maintenance", {})
    pending = report["checks"].get("pending_reconcile", {})
    unmatched = report["checks"].get("unmatched_live_entries", {})
    closes = report["checks"].get("closes_60m", {})
    audit = report["checks"].get("position_audit", {})
    refl = report["checks"].get("reflection_backfill", {})

    line1 = (
        f"{REPO_NAME} {report['health']} "
        f"tmux_down={int(tmux.get('down_after_heal', 0))} "
        f"cycle_age_s={cycle_age if cycle_age is not None else 'NOFILE'} "
        f"db_check_rc={int(db_maint.get('check_rc', 0))}"
    )
    top = closes.get("top") or []
    line2 = f"closes_60m={int(closes.get('count', 0))}" + (f" top={','.join(top)}" if top else "")
    line3 = (
        f"equity={_safe_float(audit.get('equity_total', 0.0)):.2f} "
        f"open_pos={int(audit.get('exchange_open_positions', 0))} "
        f"missing_in_db={len(audit.get('missing_in_db', []))} "
        f"unprot_builder={len(audit.get('unprotected_builder', []))} "
        f"unprot_perps={len(audit.get('unprotected_perps', []))} "
        f"unknown_builder={len(audit.get('unknown_builder_protection', []))}"
    )
    line4 = (
        f"pending_expired_cancelled={int(pending.get('expired_cancelled', 0))} "
        f"unmatched_cancelled={int(unmatched.get('cancelled', 0))} "
        f"reflection_enqueued={int(refl.get('inserted', 0))}"
    )
    lines = [line1, line2, line3, line4]

    json_out.parent.mkdir(parents=True, exist_ok=True)
    summary_out.parent.mkdir(parents=True, exist_ok=True)
    json_out.write_text(json.dumps(report, indent=2, sort_keys=True), encoding="utf-8")
    summary_out.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return lines


async def _run_hourly(args: argparse.Namespace) -> int:
    dotenv = _load_dotenv(DOTENV_PATH)
    raw_private = args.info_url or _env("HYPERLIQUID_PRIVATE_NODE", dotenv, DEFAULT_INFO_URL)
    wallet_key = _env("HYPERLIQUID_ADDRESS", dotenv)
    info_url = _append_private_key_if_needed(_normalize_info_url(raw_private), wallet_key)
    public_info_url = _normalize_info_url(_env("HYPERLIQUID_PUBLIC_URL", dotenv, DEFAULT_PUBLIC_INFO_URL))
    db_path = Path(args.db).expanduser()

    report: Dict[str, Any] = {
        "repo": REPO_NAME,
        "started_at": _now_utc_iso(),
        "settings": {
            "apply_fixes": bool(args.apply_fixes),
            "cancel_unmatched_live_entries": bool(args.cancel_unmatched_live_entries),
            "db_path": str(db_path),
            "info_url": info_url,
            "fallback_info_url": public_info_url,
        },
        "checks": {},
        "actions": [],
        "warnings": [],
        "errors": [],
    }

    if not db_path.exists():
        report["errors"].append(f"db_not_found:{db_path}")
        report["health"] = "CRIT"
        _write_outputs(report, json_out=Path(args.json_out), summary_out=Path(args.summary_out))
        return 2

    # 1) tmux self-heal
    missing = [s for s in SESSION_NAMES if not _tmux_exists(s)]
    restarted: List[str] = []
    if missing and args.apply_fixes:
        for s in missing:
            cp = _run(["bash", "restart.sh", s], cwd=ROOT_DIR)
            if cp.returncode == 0 and _tmux_exists(s):
                restarted.append(s)
            else:
                report["warnings"].append(f"restart_failed:{s}")
    down_after = [s for s in SESSION_NAMES if not _tmux_exists(s)]
    report["checks"]["tmux"] = {
        "required": SESSION_NAMES,
        "missing_before_heal": missing,
        "restarted": restarted,
        "down_after_heal": len(down_after),
        "down_sessions": down_after,
    }

    # 2) cycle freshness
    age = _cycle_freshness(DEFAULT_CYCLE_LATEST)
    report["checks"]["cycle_freshness_sec"] = age
    if age is None or age > 600:
        report["warnings"].append("cycle_stale_or_missing")

    conn = sqlite3.connect(str(db_path), timeout=30.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=30000")
    try:
        # 3) stuck cycle claims
        claim = _unlock_stuck_claims(conn, apply_fixes=bool(args.apply_fixes))
        conn.commit()
        report["checks"]["stuck_claims"] = claim
        if claim["unlocked"] > 0:
            _append_action(report, f"stuck_claims_unlocked:{claim['unlocked']}")

        # 4) sltp incident reconcile
        wallet = _env("HYPERLIQUID_ADDRESS", dotenv)
        cmd = [sys.executable, "sltp_incident_reconciler.py", "--db", str(db_path)]
        if wallet:
            cmd += ["--wallet", wallet]
        cp = _run(cmd, cwd=ROOT_DIR)
        report["checks"]["sltp_incident_reconcile"] = {
            "rc": int(cp.returncode),
            "stdout": (cp.stdout or "").strip()[-1200:],
        }
        if cp.returncode != 0:
            report["warnings"].append("sltp_incident_reconcile_nonzero")

        # 5) DB maintenance
        db_maint = _run_db_maintenance(db_path, apply_fixes=bool(args.apply_fixes))
        report["checks"]["db_maintenance"] = db_maint
        if int(db_maint.get("check_rc", 0)) != 0:
            report["warnings"].append("db_maintenance_check_failed")

        # 6) adapter-backed reconciliation/cancel tasks
        adapters = await _init_adapters(dotenv, report)
        try:
            accounts = _build_accounts(dotenv, adapters)
            report["checks"]["accounts"] = [a.__dict__ for a in accounts]

            pending = await _reconcile_pending_orders(
                conn,
                adapters,
                apply_fixes=bool(args.apply_fixes),
                report=report,
            )
            report["checks"]["pending_reconcile"] = pending

            unmatched, open_orders_by_account, states_by_account = await _cancel_unmatched_live_entries(
                conn,
                info_url,
                public_info_url,
                accounts,
                adapters,
                apply_fixes=bool(args.apply_fixes),
                cancel_unmatched=bool(args.cancel_unmatched_live_entries),
                report=report,
            )
            report["checks"]["unmatched_live_entries"] = unmatched

            audit = await _audit_positions(
                conn,
                open_orders_by_account=open_orders_by_account,
                states_by_account=states_by_account,
                adapters=adapters,
                report=report,
            )
            report["checks"]["position_audit"] = audit
            if audit["missing_in_db"]:
                report["warnings"].append(f"missing_positions_in_db:{len(audit['missing_in_db'])}")
            if audit["unprotected_builder"]:
                report["warnings"].append(
                    f"unprotected_builder_positions:{len(audit['unprotected_builder'])}"
                )
            if audit["unprotected_perps"]:
                report["warnings"].append(
                    f"unprotected_perps_positions:{len(audit['unprotected_perps'])}"
                )
            if audit["unknown_builder_protection"]:
                report["warnings"].append(
                    f"unknown_builder_protection:{len(audit['unknown_builder_protection'])}"
                )
        finally:
            for adapter in adapters.values():
                try:
                    await adapter.close()
                except Exception:
                    pass

        # 7) reflection queue backfill
        reflection = _enqueue_reflection_backfill(conn)
        report["checks"]["reflection_backfill"] = reflection

        # 8) recent closes summary
        report["checks"]["closes_60m"] = _recent_closes(conn)
        conn.commit()
    finally:
        conn.close()

    # 9) disk check
    disk_pct = _current_disk_usage_pct(Path("/root"))
    report["checks"]["disk_usage_pct"] = round(disk_pct, 2)
    if disk_pct > 85.0:
        report["warnings"].append(f"disk_high:{disk_pct:.2f}")

    report["finished_at"] = _now_utc_iso()
    lines = _write_outputs(report, json_out=Path(args.json_out), summary_out=Path(args.summary_out))
    for line in lines:
        print(line)
    return 0


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Deterministic hourly ops for EVClaw")
    ap.add_argument("--db", default=str(DEFAULT_DB_PATH), help="Path to ai_trader.db")
    ap.add_argument("--info-url", default="", help="Hyperliquid /info endpoint (default from env)")
    ap.add_argument("--json-out", default=str(DEFAULT_JSON_OUT), help="JSON report output path")
    ap.add_argument("--summary-out", default=str(DEFAULT_SUMMARY_OUT), help="summary output path")
    ap.add_argument("--no-apply-fixes", action="store_true", help="Detect only, no remediations")
    ap.add_argument(
        "--no-cancel-unmatched-live-entries",
        action="store_true",
        help="Do not cancel unmatched non-reduce-only live orders",
    )
    ap.add_argument("--verbose", action="store_true")
    args = ap.parse_args()
    args.apply_fixes = not bool(args.no_apply_fixes)
    args.cancel_unmatched_live_entries = not bool(args.no_cancel_unmatched_live_entries)
    return args


def main() -> int:
    args = parse_args()
    _setup_logging(args.verbose)
    try:
        return int(asyncio.run(_run_hourly(args)))
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
