import os
import sqlite3
import tempfile
import unittest
import sys

# Ensure project root is importable when running tests as scripts
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from ai_trader_db import AITraderDB


class TestReconciliationNoDupTrade(unittest.TestCase):
    def test_find_open_trade_key_prevents_dup(self):
        with tempfile.TemporaryDirectory() as td:
            db_path = os.path.join(td, "test.db")
            db = AITraderDB(db_path)

            # Insert an open trade
            tid = db.log_trade_entry(
                symbol="CRV",
                direction="SHORT",
                entry_price=0.35,
                size=100.0,
                venue="lighter",
                state="ACTIVE",
            )
            self.assertIsInstance(tid, int)

            # Must be found by the new key lookup
            found = db.find_open_trade_key("CRV", venue="lighter", direction="SHORT")
            self.assertIsNotNone(found)
            self.assertEqual(found.id, tid)

            # Simulate the guard behavior: if found exists, we should NOT insert a new trade.
            found2 = db.find_open_trade_key("CRV", venue="lighter", direction="SHORT")
            self.assertEqual(found2.id, tid)

            # Ensure only one open trade row exists for this key.
            with sqlite3.connect(db_path) as conn:
                cur = conn.execute(
                    "SELECT COUNT(*) FROM trades WHERE symbol='CRV' AND venue='lighter' AND direction='SHORT' AND exit_time IS NULL"
                )
                self.assertEqual(cur.fetchone()[0], 1)


if __name__ == "__main__":
    unittest.main()
