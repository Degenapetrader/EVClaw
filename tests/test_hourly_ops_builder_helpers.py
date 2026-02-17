"""Unit guards for builder detection + status normalization in hourly_ops."""

import sqlite3

import pytest

from hourly_ops import (
    _append_position_audit_warnings,
    _audit_positions,
    _build_accounts,
    _fetch_clearinghouse_state,
    _fetch_open_orders,
    _is_builder_symbol,
    _is_openish_status,
    _needs_self_heal,
    _normalize_order_status,
    _parse_json_obj,
)


def _classify_builder_protection(sl_status, tp_status):
    sl = _normalize_order_status(sl_status)
    tp = _normalize_order_status(tp_status)
    if sl in {"canceled", "rejected"} or tp in {"canceled", "rejected"}:
        return "unprotected"
    if sl == "unknown" or tp == "unknown":
        return "unknown"
    if _is_openish_status(sl) and (_is_openish_status(tp) or tp == "filled"):
        return "protected"
    return "unprotected"


def test_is_builder_symbol_detection():
    assert _is_builder_symbol("XYZ:AMD")
    assert _is_builder_symbol(" xyz:msft ")
    assert not _is_builder_symbol("BTC")
    assert not _is_builder_symbol("")


def test_normalize_order_status_mapping():
    assert _normalize_order_status("cancelled") == "canceled"
    assert _normalize_order_status(" CANCELED ") == "canceled"
    assert _normalize_order_status("resting") == "resting"
    assert _normalize_order_status("PARTIALLY_FILLED") == "partially_filled"
    assert _normalize_order_status("weird_status") == "unknown"


def test_openish_status_set():
    for s in ("open", "resting", "partially_filled", "filledorresting"):
        assert _is_openish_status(s)
    for s in ("filled", "canceled", "rejected", "unknown"):
        assert not _is_openish_status(s)


def test_builder_protection_contract():
    assert _classify_builder_protection("open", "open") == "protected"
    assert _classify_builder_protection("resting", "filled") == "protected"
    assert _classify_builder_protection("unknown", "open") == "unknown"
    assert _classify_builder_protection("canceled", "open") == "unprotected"
    assert _classify_builder_protection("open", "rejected") == "unprotected"


def test_needs_self_heal_contract():
    assert _needs_self_heal({"missing_in_db": ["BTC:LONG"]}) is True
    assert _needs_self_heal({"unprotected_builder": ["XYZ:AMD"]}) is True
    assert _needs_self_heal({"unprotected_perps": ["ETH"]}) is True
    assert _needs_self_heal(
        {
            "missing_in_db": [],
            "unprotected_builder": [],
            "unprotected_perps": [],
        }
    ) is False


def test_append_position_audit_warnings():
    report = {"warnings": []}
    _append_position_audit_warnings(
        report,
        {
            "missing_in_db": ["BTC:LONG"],
            "unprotected_builder": ["XYZ:AMD:trade_id=1"],
            "unprotected_perps": ["ETH:trade_id=2"],
            "unknown_builder_protection": ["XYZ:NFLX:trade_id=3"],
        },
    )
    assert report["warnings"] == [
        "missing_positions_in_db:1",
        "unprotected_builder_positions:1",
        "unprotected_perps_positions:1",
        "unknown_builder_protection:1",
    ]


def test_parse_json_obj_resilient():
    assert _parse_json_obj('{"ok": true}') == {"ok": True}
    noisy = "log line\n{\"ok\": false, \"errors\": [\"x\"]}\n"
    assert _parse_json_obj(noisy) == {"ok": False, "errors": ["x"]}


def test_fetch_open_orders_builder_uses_frontend_with_dex(monkeypatch):
    calls = []

    def _fake_post(url, payload, timeout=8.0, fallback_info_url="https://api.hyperliquid.xyz/info"):
        calls.append((url, payload, fallback_info_url))
        return []

    monkeypatch.setattr("hourly_ops._post_info", _fake_post)
    rows = _fetch_open_orders(
        "https://node2.evplus.ai/evclaw/info",
        "0xabc",
        dex="xyz",
        use_frontend_orders=True,
        fallback_info_url="https://api.hyperliquid.xyz/info",
    )
    assert rows == []
    assert calls == [
        (
            "https://node2.evplus.ai/evclaw/info",
            {"type": "frontendOpenOrders", "user": "0xabc", "dex": "xyz"},
            "https://api.hyperliquid.xyz/info",
        )
    ]


def test_fetch_clearinghouse_state_builder_uses_dex(monkeypatch):
    calls = []

    def _fake_post(url, payload, timeout=8.0, fallback_info_url="https://api.hyperliquid.xyz/info"):
        calls.append((url, payload, fallback_info_url))
        return {"assetPositions": []}

    monkeypatch.setattr("hourly_ops._post_info", _fake_post)
    state = _fetch_clearinghouse_state(
        "https://node2.evplus.ai/evclaw/info",
        "0xabc",
        dex="xyz",
        fallback_info_url="https://api.hyperliquid.xyz/info",
    )
    assert state == {"assetPositions": []}
    assert calls == [
        (
            "https://node2.evplus.ai/evclaw/info",
            {"type": "clearinghouseState", "user": "0xabc", "dex": "xyz"},
            "https://api.hyperliquid.xyz/info",
        )
    ]


def test_build_accounts_includes_perps_and_builder():
    adapters = {"wallet": object()}
    accounts = _build_accounts({"HYPERLIQUID_ADDRESS": "0xwallet"}, adapters)
    labels = [a.label for a in accounts]
    assert labels == ["wallet_perps", "wallet_builder_xyz"]
    assert accounts[0].dex == ""
    assert accounts[0].use_frontend_orders is False
    assert accounts[1].dex == "xyz"
    assert accounts[1].use_frontend_orders is True


@pytest.mark.asyncio
async def test_audit_positions_reports_builder_coverage_gap():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        """
        CREATE TABLE trades (
          id INTEGER PRIMARY KEY,
          symbol TEXT,
          direction TEXT,
          venue TEXT,
          size REAL,
          sl_order_id TEXT,
          tp_order_id TEXT,
          exit_time REAL
        )
        """
    )
    conn.execute(
        """
        INSERT INTO trades (id, symbol, direction, venue, size, sl_order_id, tp_order_id, exit_time)
        VALUES (1, 'XYZ:BABA', 'SHORT', 'hyperliquid_wallet', 30.838, NULL, NULL, NULL)
        """
    )

    open_orders_by_account = {
        "wallet": [
            {"coin": "XYZ:BABA", "reduceOnly": True, "sz": "9.472", "oid": 123},
        ]
    }
    states_by_account = {
        "wallet": {
            "marginSummary": {"accountValue": "100.0"},
            "assetPositions": [{"position": {"coin": "XYZ:BABA", "szi": "-30.838"}}],
        }
    }

    audit = await _audit_positions(
        conn,
        open_orders_by_account=open_orders_by_account,
        states_by_account=states_by_account,
        adapters={},
        report={"warnings": []},
    )

    assert audit["exchange_open_positions"] == 1
    assert audit["exchange_open_positions_perps"] == 0
    assert audit["exchange_open_positions_builder"] == 1
    assert len(audit["coverage_gaps"]) == 1
    gap = audit["coverage_gaps"][0]
    assert gap["bucket"] == "builder"
    assert gap["symbol"] == "XYZ:BABA"
    assert gap["expected_size"] == pytest.approx(30.838)
    assert gap["protected_open_size"] == pytest.approx(9.472)
    assert gap["gap_size"] == pytest.approx(21.366)
