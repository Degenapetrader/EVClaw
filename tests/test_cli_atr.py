import unittest
import os
import sys

# Ensure project root is importable when running tests as scripts
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from cli import derive_atr_from_symbol_data


class TestDeriveAtrFromSymbolData(unittest.TestCase):
    def test_top_level_atr_pct(self):
        sd = {"atr_pct": 2.5}
        atr = derive_atr_from_symbol_data(sd, price=100.0)
        self.assertAlmostEqual(atr, 2.5)

    def test_nested_atr_pct(self):
        sd = {"atr": {"atr_pct": 3.0, "atr_abs": 9.99, "price": 100.0}}
        atr = derive_atr_from_symbol_data(sd, price=200.0)
        self.assertAlmostEqual(atr, 6.0)  # 3% of 200

    def test_nested_atr_abs_fallback(self):
        sd = {"atr": {"atr_abs": 1.234}}
        atr = derive_atr_from_symbol_data(sd, price=200.0)
        self.assertAlmostEqual(atr, 1.234)

    def test_missing(self):
        self.assertIsNone(derive_atr_from_symbol_data({}, price=100.0))
        self.assertIsNone(derive_atr_from_symbol_data(None, price=100.0))


if __name__ == "__main__":
    unittest.main()
