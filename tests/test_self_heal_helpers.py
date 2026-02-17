"""Unit checks for deterministic self-heal helper logic."""

import pytest

import self_heal


def test_normalize_status_mapping():
    assert self_heal._normalize_status("cancelled") == "canceled"
    assert self_heal._normalize_status(" OPEN ") == "open"
    assert self_heal._normalize_status("filledOrResting") == "filledorresting"
    assert self_heal._normalize_status("weird") == "unknown"


def test_default_sltp_long_short():
    sl, tp = self_heal._derive_default_sltp(100.0, "LONG")
    assert sl == pytest.approx(98.5)
    assert tp == pytest.approx(102.0)

    sl, tp = self_heal._derive_default_sltp(100.0, "SHORT")
    assert sl == pytest.approx(101.5)
    assert tp == pytest.approx(98.0)


def test_detect_incidents_missing_in_db():
    incidents = self_heal._detect_incidents(
        db_trades=[],
        live_positions={
            ("WLD", "hyperliquid"): {
                "symbol": "WLD",
                "coin": "WLD",
                "venue": "hyperliquid",
                "direction": "SHORT",
                "size": 100.0,
                "entry_price": 0.4,
            }
        },
        orders_by_key={},
        public_info_url="https://api.hyperliquid.xyz/info",
        user_by_venue={"hyperliquid": "0xvault", "hyperliquid_wallet": "0xwallet"},
    )
    assert len(incidents) == 1
    assert incidents[0]["type"] == "missing_in_db"
    assert incidents[0]["symbol"] == "WLD"
    assert incidents[0]["venue"] == "hyperliquid"


def test_detect_incidents_builder_partial_coverage(monkeypatch):
    def _fake_order_status(*, public_info_url, user, oid, cache):
        if str(oid) == "sl":
            return {"status": "filled", "orig_size": 9.472, "open_size": 0.0, "filled_size": 9.472}
        return {"status": "open", "orig_size": 9.472, "open_size": 9.472, "filled_size": 0.0}

    monkeypatch.setattr(self_heal, "_order_status", _fake_order_status)

    db_trades = [
        {
            "id": 1539,
            "symbol": "XYZ:BABA",
            "venue": "hyperliquid_wallet",
            "direction": "SHORT",
            "size": 30.838,
            "entry_price": 154.84,
            "sl_price": 158.88,
            "tp_price": 148.77,
            "sl_order_id": "sl",
            "tp_order_id": "tp",
            "state": "ACTIVE",
        }
    ]
    live_positions = {
        ("XYZ:BABA", "hyperliquid_wallet"): {
            "symbol": "XYZ:BABA",
            "coin": "BABA",
            "venue": "hyperliquid_wallet",
            "direction": "SHORT",
            "size": 30.838,
            "entry_price": 154.84,
        }
    }
    orders_by_key = {
        ("BABA", "hyperliquid_wallet"): [
            {"coin": "XYZ:BABA", "reduceOnly": True, "sz": "9.472", "oid": 320026453836}
        ]
    }

    incidents = self_heal._detect_incidents(
        db_trades=db_trades,
        live_positions=live_positions,
        orders_by_key=orders_by_key,
        public_info_url="https://api.hyperliquid.xyz/info",
        user_by_venue={"hyperliquid": "0xvault", "hyperliquid_wallet": "0xwallet"},
    )

    assert len(incidents) == 1
    inc = incidents[0]
    assert inc["type"] == "stale_oid"
    assert inc["symbol"] == "XYZ:BABA"
    assert inc["covered_size"] == 9.472
    assert inc["expected_size"] == 30.838
    assert inc["gap_size"] == 21.366
    assert inc["sl_status"] == "filled"
    assert inc["tp_status"] == "open"


def test_detect_incidents_dedupes_by_symbol_venue_key(monkeypatch):
    def _fake_order_status(*, public_info_url, user, oid, cache):
        return {"status": "canceled", "orig_size": 1.0, "open_size": 0.0, "filled_size": 1.0}

    monkeypatch.setattr(self_heal, "_order_status", _fake_order_status)

    # Newest row first (matches query ORDER BY entry_time DESC in runtime).
    db_trades = [
        {
            "id": 200,
            "symbol": "WLD",
            "venue": "hyperliquid",
            "direction": "SHORT",
            "size": 100.0,
            "entry_price": 0.4,
            "sl_price": 0.42,
            "tp_price": 0.38,
            "sl_order_id": "new_sl",
            "tp_order_id": "new_tp",
            "state": "ACTIVE",
        },
        {
            "id": 100,
            "symbol": "WLD",
            "venue": "hyperliquid",
            "direction": "SHORT",
            "size": 100.0,
            "entry_price": 0.4,
            "sl_price": 0.42,
            "tp_price": 0.38,
            "sl_order_id": "old_sl",
            "tp_order_id": "old_tp",
            "state": "ACTIVE",
        },
    ]
    live_positions = {
        ("WLD", "hyperliquid"): {
            "symbol": "WLD",
            "coin": "WLD",
            "venue": "hyperliquid",
            "direction": "SHORT",
            "size": 100.0,
            "entry_price": 0.4,
        }
    }
    orders_by_key = {("WLD", "hyperliquid"): []}

    incidents = self_heal._detect_incidents(
        db_trades=db_trades,
        live_positions=live_positions,
        orders_by_key=orders_by_key,
        public_info_url="https://api.hyperliquid.xyz/info",
        user_by_venue={"hyperliquid": "0xvault", "hyperliquid_wallet": "0xwallet"},
    )

    assert len(incidents) == 1
    # If dedupe regresses, this can flip to id=100 and break idempotent repair behavior.
    assert incidents[0]["trade_id"] == 200
    assert incidents[0]["type"] == "stale_oid"


def test_summarize_counts():
    summary = self_heal._summarize(
        [
            {"type": "missing_in_db"},
            {"type": "unprotected_perps"},
            {"type": "unprotected_builder"},
            {"type": "unprotected_builder"},
        ]
    )
    assert summary == {
        "missing_in_db": 1,
        "unprotected_perps": 1,
        "unprotected_builder": 2,
        "partial_coverage": 0,
        "stale_oid": 0,
        "total": 4,
    }


def test_detect_incidents_partial_coverage_type(monkeypatch):
    def _fake_order_status(*, public_info_url, user, oid, cache):
        return {"status": "open", "orig_size": 1.0, "open_size": 1.0, "filled_size": 0.0}

    monkeypatch.setattr(self_heal, "_order_status", _fake_order_status)
    incidents = self_heal._detect_incidents(
        db_trades=[
            {
                "id": 1,
                "symbol": "XYZ:AMD",
                "venue": "hyperliquid_wallet",
                "direction": "LONG",
                "size": 10.0,
                "entry_price": 100.0,
                "sl_price": 99.0,
                "tp_price": 101.0,
                "sl_order_id": "sl",
                "tp_order_id": "tp",
                "state": "ACTIVE",
            }
        ],
        live_positions={
            ("XYZ:AMD", "hyperliquid_wallet"): {
                "symbol": "XYZ:AMD",
                "coin": "AMD",
                "venue": "hyperliquid_wallet",
                "direction": "LONG",
                "size": 10.0,
                "entry_price": 100.0,
            }
        },
        orders_by_key={("AMD", "hyperliquid_wallet"): [{"coin": "XYZ:AMD", "reduceOnly": True, "sz": "3.0"}]},
        public_info_url="https://api.hyperliquid.xyz/info",
        user_by_venue={"hyperliquid": "0xwallet", "hyperliquid_wallet": "0xwallet", "hip3": "0xwallet"},
    )
    assert len(incidents) == 1
    assert incidents[0]["type"] == "partial_coverage"


def test_append_private_key_if_needed():
    base = "https://node2.evplus.ai/evclaw/info"
    with_key = self_heal._append_private_key_if_needed(base, "0xabc")
    assert with_key == "https://node2.evplus.ai/evclaw/info?key=0xabc"
    assert self_heal._append_private_key_if_needed(with_key, "0xdef") == with_key


def test_detect_incidents_hip3_venue_alias_to_builder(monkeypatch):
    def _fake_order_status(*, public_info_url, user, oid, cache):
        return {"status": "canceled", "orig_size": 1.0, "open_size": 0.0, "filled_size": 1.0}

    monkeypatch.setattr(self_heal, "_order_status", _fake_order_status)

    db_trades = [
        {
            "id": 11,
            "symbol": "XYZ:BABA",
            "venue": "hip3",
            "direction": "SHORT",
            "size": 30.838,
            "entry_price": 154.84,
            "sl_price": 158.88,
            "tp_price": 148.77,
            "sl_order_id": "sl",
            "tp_order_id": "tp",
            "state": "ACTIVE",
        }
    ]
    live_positions = {
        ("XYZ:BABA", "hyperliquid_wallet"): {
            "symbol": "XYZ:BABA",
            "coin": "BABA",
            "venue": "hyperliquid_wallet",
            "direction": "SHORT",
            "size": 30.838,
            "entry_price": 154.84,
        }
    }
    incidents = self_heal._detect_incidents(
        db_trades=db_trades,
        live_positions=live_positions,
        orders_by_key={("BABA", "hyperliquid_wallet"): []},
        public_info_url="https://api.hyperliquid.xyz/info",
        user_by_venue={"hyperliquid": "0xwallet", "hyperliquid_wallet": "0xwallet", "hip3": "0xwallet"},
    )
    assert len(incidents) == 1
    assert incidents[0]["type"] == "stale_oid"
