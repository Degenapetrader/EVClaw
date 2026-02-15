#!/usr/bin/env python3
"""Shared Binance cooldown synchronization tests."""

import importlib
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


def test_shared_binance_cooldown_roundtrip(monkeypatch, tmp_path) -> None:
    import candle_fetcher as cf

    cooldown_file = tmp_path / "binance_cooldown.txt"
    with monkeypatch.context() as m:
        mod = importlib.reload(cf)
        m.setattr(mod, "BINANCE_COOLDOWN_FILE", str(cooldown_file))
        mod._DISABLE_BINANCE_UNTIL_MS = 0
        mod._set_shared_binance_cooldown_until_ms(123456789)
        assert cooldown_file.exists()
        assert cooldown_file.read_text().strip() == "123456789"
        assert mod._read_shared_binance_cooldown_until_ms() == 123456789

    importlib.reload(cf)


def test_shared_binance_cooldown_adopts_larger_file_value(monkeypatch, tmp_path) -> None:
    import candle_fetcher as cf

    cooldown_file = tmp_path / "binance_cooldown.txt"
    with monkeypatch.context() as m:
        mod = importlib.reload(cf)
        m.setattr(mod, "BINANCE_COOLDOWN_FILE", str(cooldown_file))
        mod._DISABLE_BINANCE_UNTIL_MS = 100
        cooldown_file.write_text("250")
        assert mod._read_shared_binance_cooldown_until_ms() == 250
        assert int(mod._DISABLE_BINANCE_UNTIL_MS) == 250

    importlib.reload(cf)
