#!/usr/bin/env python3

import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from exchanges.hyperliquid_adapter import HyperliquidAdapter


def test_coin_key_canonicalizes_builder_prefix_case() -> None:
    assert HyperliquidAdapter._coin_key("XYZ:nvda") == "XYZ:NVDA"
    assert HyperliquidAdapter._coin_key("km:tsla") == "KM:TSLA"


def test_get_asset_id_accepts_uppercase_builder_prefix() -> None:
    a = HyperliquidAdapter(log=logging.getLogger("test"), dry_run=True)
    a._asset_map = {"XYZ:NVDA": 123456}
    assert a._get_asset_id("XYZ:NVDA") == 123456


def test_get_asset_id_refreshes_stale_builder_range() -> None:
    a = HyperliquidAdapter(log=logging.getLogger("test"), dry_run=True)
    a._asset_map = {"XYZ:MU": 20015}

    def _refresh() -> None:
        a._asset_map["XYZ:MU"] = 110015

    a._refresh_metadata_best_effort = _refresh  # type: ignore[method-assign]
    assert a._get_asset_id("XYZ:MU") == 110015


def test_builder_dex_offsets_handles_perpdex_sentinel_entry() -> None:
    raw = [
        None,
        {"name": "xyz"},
        {"name": "flx"},
        {"name": "vntl"},
    ]
    out = HyperliquidAdapter._builder_dex_offsets(raw)
    assert out["xyz"] == 110000
    assert out["flx"] == 120000
    assert out["vntl"] == 130000


def test_builder_dex_offsets_handles_missing_sentinel_entry() -> None:
    raw = [
        {"name": "xyz"},
        {"name": "flx"},
    ]
    out = HyperliquidAdapter._builder_dex_offsets(raw)
    assert out["xyz"] == 110000
    assert out["flx"] == 120000


def test_l2_coin_uses_lowercase_builder_prefix() -> None:
    assert HyperliquidAdapter._l2_coin("XYZ:pltr") == "xyz:PLTR"
    assert HyperliquidAdapter._l2_coin("CASH:silver") == "cash:SILVER"
    assert HyperliquidAdapter._l2_coin("BTC") == "BTC"
    assert HyperliquidAdapter._l2_coin("FOO:BAR") == "FOO:BAR"
