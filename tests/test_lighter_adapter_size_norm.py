import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from exchanges.lighter_adapter import LighterAdapter, LighterMarket


def test_lighter_normalize_base_size_round_down():
    adapter = LighterAdapter(log=logging.getLogger("test"), dry_run=True)
    market = LighterMarket(
        market_id=1,
        symbol="EIGEN",
        size_decimals=1,
        price_decimals=2,
        min_base_amount=1.0,
        last_trade_price=1.0,
    )
    adapter._markets["EIGEN"] = market
    adapter._market_ids[1] = market

    normalized, min_base = adapter.normalize_base_size("EIGEN", 31.86, round_down=True)
    assert normalized == 31.8
    assert min_base == 1.0

    assert adapter._normalize_base_amount(318, market) == 31.8
