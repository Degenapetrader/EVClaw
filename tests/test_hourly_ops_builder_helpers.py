"""Unit guards for builder detection + status normalization in hourly_ops."""

from hourly_ops import (
    _append_position_audit_warnings,
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
