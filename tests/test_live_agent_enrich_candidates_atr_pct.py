import pytest


class _FakeATRResult:
    def __init__(self, *, atr=2.0, atr_pct=1.23, source="massive"):
        self.atr = atr
        self.atr_pct = atr_pct
        self.source = source
        self.computed_at = 1234567890.0
        self.interval = "1h"
        self.period = 14


class _FakeATRService:
    async def get_atr(self, symbol: str, price=None):
        # Keep it simple: always return a value.
        return _FakeATRResult(source="massive" if ":" in symbol else "binance")


@pytest.mark.asyncio
async def test_enrich_candidates_atr_pct_enriches_hip3_and_updates_context(monkeypatch):
    import live_agent

    monkeypatch.setattr(live_agent, "get_atr_service", lambda *_args, **_kw: _FakeATRService())

    candidates = [
        {
            "symbol": "XYZ:COIN",
            "atr_pct": {"value": 0.0},
            "context_snapshot": {"key_metrics": {"price": 150.0, "atr_pct": 0.0, "sr_levels": {}}},
        }
    ]

    await live_agent.enrich_candidates_atr_pct(candidates, db_path=":memory:")

    c = candidates[0]
    assert float(c["atr_pct"]["value"]) > 0
    km = c["context_snapshot"]["key_metrics"]
    assert float(km["atr_pct"]) > 0
    assert "atr_pct_source" in km
    assert "massive" in str(km["atr_pct_source"]).lower()


@pytest.mark.asyncio
async def test_enrich_candidates_atr_pct_still_enriches_perps(monkeypatch):
    import live_agent

    monkeypatch.setattr(live_agent, "get_atr_service", lambda *_args, **_kw: _FakeATRService())

    candidates = [
        {
            "symbol": "BTC",
            "atr_pct": {"value": 0.0},
            "context_snapshot": {"key_metrics": {"price": 50000.0, "atr_pct": 0.0}},
        }
    ]

    await live_agent.enrich_candidates_atr_pct(candidates, db_path=":memory:")

    c = candidates[0]
    assert float(c["atr_pct"]["value"]) > 0
    km = c["context_snapshot"]["key_metrics"]
    assert float(km["atr_pct"]) > 0
    assert "atr_pct_source" in km
    assert "binance" in str(km["atr_pct_source"]).lower()
