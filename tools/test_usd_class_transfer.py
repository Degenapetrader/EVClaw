#!/usr/bin/env python3
"""Test: transfer USD collateral between perp and spot on Hyperliquid.

Purpose (operator): verify we can move collateral (USDC) from perp margin to spot
(and back) using the delegated HYPERLIQUID_AGENT_PRIVATE_KEY signer key.

This uses the Hyperliquid SDK Exchange.usd_class_transfer() call.

Safety:
- Defaults to DRY RUN (prints balances + intended action)
- Requires --execute to actually submit the transfer.

Notes:
- In our deployment, perps run on the vault/subaccount (VAULT_ADDRESS). If
  VAULT_ADDRESS is set, the SDK appends "subaccount:<VAULT_ADDRESS>" to the
  amount string.
"""

from __future__ import annotations

import argparse
import os
import time
from typing import Any, Dict, Optional

import requests
from eth_account import Account
from hyperliquid.exchange import Exchange


def _info_url(base_url: str) -> str:
    base_url = (base_url or "https://api.hyperliquid.xyz").strip().rstrip("/")
    return base_url + "/info"


def _post_info(base_url: str, payload: Dict[str, Any]) -> Any:
    r = requests.post(_info_url(base_url), json=payload, timeout=15)
    r.raise_for_status()
    return r.json()


def _get_spot_usdc(base_url: str, address: str) -> float:
    j = _post_info(base_url, {"type": "spotClearinghouseState", "user": address})
    bals = j.get("balances") or []
    for b in bals:
        if str(b.get("coin") or "").upper() == "USDC":
            try:
                return float(b.get("total") or 0.0)
            except Exception:
                return 0.0
    return 0.0


def _get_perp_withdrawable(base_url: str, address: str) -> Optional[float]:
    j = _post_info(base_url, {"type": "clearinghouseState", "user": address})
    w = j.get("withdrawable")
    try:
        return float(w) if w is not None else None
    except Exception:
        return None


def main() -> int:
    ap = argparse.ArgumentParser(description="Test Hyperliquid usdClassTransfer (perp<->spot).")
    ap.add_argument("--amount", type=float, default=15.0, help="USD amount to transfer")
    ap.add_argument(
        "--to-spot",
        action="store_true",
        help="Transfer from perp -> spot (default). If omitted, transfer spot -> perp.",
    )
    ap.add_argument(
        "--execute",
        action="store_true",
        help="Actually submit the transfer. Without this flag: DRY RUN.",
    )
    ap.add_argument("--wait-seconds", type=float, default=3.0, help="Wait after transfer then re-check balances")

    args = ap.parse_args()

    base_url = os.getenv("HYPERLIQUID_PUBLIC_URL", "https://api.hyperliquid.xyz").strip().rstrip("/")
    priv = (os.getenv("HYPERLIQUID_AGENT_PRIVATE_KEY", "") or "").strip()
    addr = (os.getenv("HYPERLIQUID_ADDRESS", "") or "").strip()
    vault = (os.getenv("VAULT_ADDRESS", "") or "").strip() or None

    if not priv or not addr:
        raise SystemExit("Missing HYPERLIQUID_AGENT_PRIVATE_KEY or HYPERLIQUID_ADDRESS in env")

    # Pre balances (spot USDC + perp withdrawable)
    spot_before = _get_spot_usdc(base_url, addr)
    perp_withdrawable_before = _get_perp_withdrawable(base_url, addr)

    direction = "perp->spot" if args.to_spot else "spot->perp"
    print(f"Base URL: {base_url}")
    print(f"Address: {addr}")
    print(f"Vault: {vault or '(none)'}")
    print(f"Direction: {direction}")
    print(f"Amount: {args.amount}")
    print(f"Spot USDC before: {spot_before:.6f}")
    print(f"Perp withdrawable before: {perp_withdrawable_before}")

    if not args.execute:
        print("DRY RUN: not executing transfer (pass --execute to submit)")
        return 0

    wallet = Account.from_key(priv)
    ex = Exchange(wallet=wallet, base_url=base_url, vault_address=vault)

    # Exchange.usd_class_transfer(to_perp=True) means transfer to perp.
    to_perp = not args.to_spot
    print(f"Submitting usdClassTransfer amount={args.amount} to_perp={to_perp} ...")
    resp = ex.usd_class_transfer(amount=float(args.amount), to_perp=bool(to_perp))
    print("Response:", resp)

    time.sleep(float(args.wait_seconds))

    spot_after = _get_spot_usdc(base_url, addr)
    perp_withdrawable_after = _get_perp_withdrawable(base_url, addr)
    print(f"Spot USDC after: {spot_after:.6f} (delta={spot_after-spot_before:+.6f})")
    print(f"Perp withdrawable after: {perp_withdrawable_after}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
