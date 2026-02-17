#!/usr/bin/env python3

import logging
import sys
from pathlib import Path

import pytest

# Ensure skill root is importable
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from exchanges.hyperliquid_adapter import HyperliquidAdapter


@pytest.mark.asyncio
async def test_get_all_positions_includes_hip3_from_dex_xyz() -> None:
    log = logging.getLogger("test")
    a = HyperliquidAdapter(log=log, dry_run=False, account_mode="wallet")

    # Minimal init state
    a._initialized = True
    a._address = "0xUSER"
    a._hip3_address = "0xUSER"

    # Pretend metadata includes ORCL as a hip3 coin so hip3_only filter passes.
    a._asset_map = {"xyz:ORCL": 123456}

    async def fake_post_public(payload):
        # Main perps positions (wallet, unified mode)
        if payload.get("user") == "0xUSER" and payload.get("dex") is None:
            return {"assetPositions": []}
        # HIP3 positions must be fetched with dex=xyz
        if payload.get("user") == "0xUSER" and payload.get("dex") == "xyz":
            return {
                "assetPositions": [
                    {"position": {"coin": "xyz:ORCL", "szi": -2.0, "entryPx": 100.0, "unrealizedPnl": 1.5}},
                ]
            }
        return {"assetPositions": []}

    a._post_public = fake_post_public  # type: ignore

    pos = await a.get_all_positions()
    assert "XYZ:ORCL" in pos
    assert pos["XYZ:ORCL"].direction == "SHORT"
    assert abs(pos["XYZ:ORCL"].size - 2.0) < 1e-9


@pytest.mark.asyncio
async def test_wallet_mode_includes_hip3_from_dex_xyz() -> None:
    log = logging.getLogger("test")
    a = HyperliquidAdapter(log=log, dry_run=False, account_mode="wallet")

    a._initialized = True
    a._address = "0xUSER"

    # Pretend metadata includes ORCL as a hip3 coin (affects formatting in wallet mode)
    a._asset_map = {"xyz:ORCL": 123456}

    async def fake_post_public(payload):
        if payload.get("user") != "0xUSER":
            return {"assetPositions": []}
        if payload.get("dex") == "xyz":
            return {
                "assetPositions": [
                    {"position": {"coin": "xyz:ORCL", "szi": -1.0, "entryPx": 99.0, "unrealizedPnl": -0.25}},
                ]
            }
        # non-dex (perps)
        return {"assetPositions": []}

    a._post_public = fake_post_public  # type: ignore

    pos = await a.get_all_positions()
    assert "XYZ:ORCL" in pos
    assert pos["XYZ:ORCL"].direction == "SHORT"


@pytest.mark.asyncio
async def test_get_account_trades_keeps_perp_symbol_plain_when_builder_basename_exists(monkeypatch) -> None:
    log = logging.getLogger("test")
    a = HyperliquidAdapter(log=log, dry_run=False, account_mode="wallet")

    a._initialized = True
    a._address = "0xUSER"
    a._hip3_address = "0xUSER"
    # Builder metadata includes ZEC basename, but this fill is a plain perp coin.
    a._asset_map = {"HYNA:ZEC": 140000}

    async def fake_post_public(payload):
        if payload.get("type") == "userFills" and payload.get("user") == "0xUSER":
            return [
                {
                    "tid": "1001",
                    "coin": "ZEC",
                    "side": "B",
                    "sz": 1.0,
                    "px": 200.0,
                    "fee": 0.0,
                    "time": 1700000000000,
                    "crossed": True,
                    "startPosition": -1.0,
                }
            ]
        return []

    a._post_public = fake_post_public  # type: ignore[method-assign]
    monkeypatch.setenv("EVCLAW_INCLUDE_WALLET_HIP3_FILLS", "0")

    fills = await a.get_account_trades(limit=10)
    assert len(fills) == 1
    assert fills[0]["symbol"] == "ZEC"


@pytest.mark.asyncio
async def test_get_account_trades_keeps_explicit_builder_prefix() -> None:
    log = logging.getLogger("test")
    a = HyperliquidAdapter(log=log, dry_run=False, account_mode="wallet")

    a._initialized = True
    a._address = "0xUSER"
    a._hip3_address = "0xUSER"

    async def fake_post_public(payload):
        if payload.get("type") == "userFills" and payload.get("user") == "0xUSER":
            return [
                {
                    "tid": "1002",
                    "coin": "hyna:ZEC",
                    "side": "B",
                    "sz": 1.0,
                    "px": 200.0,
                    "fee": 0.0,
                    "time": 1700000000001,
                    "crossed": True,
                    "startPosition": -1.0,
                }
            ]
        return []

    a._post_public = fake_post_public  # type: ignore[method-assign]

    fills = await a.get_account_trades(limit=10)
    assert len(fills) == 1
    assert fills[0]["symbol"] == "HYNA:ZEC"
