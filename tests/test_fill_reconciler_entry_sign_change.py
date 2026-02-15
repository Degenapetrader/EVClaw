import os
import sys
import sqlite3
import tempfile
import unittest

# Ensure project root is importable
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from fill_reconciler import FillReconciler, Fill


class TestFillReconcilerEntrySignChange(unittest.TestCase):
    def test_entry_fill_with_sign_change_does_not_close_trade(self):
        with tempfile.TemporaryDirectory() as td:
            db_path = os.path.join(td, "test.db")
            conn = sqlite3.connect(db_path)
            conn.execute(
                """
                CREATE TABLE trades (
                  id INTEGER PRIMARY KEY,
                  symbol TEXT,
                  direction TEXT,
                  venue TEXT,
                  entry_time REAL,
                  entry_price REAL,
                  size REAL,
                  exit_time REAL,
                  exit_price REAL,
                  exit_reason TEXT,
                  state TEXT DEFAULT 'ACTIVE'
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE fills (
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  trade_id INTEGER,
                  venue TEXT,
                  exchange_trade_id TEXT,
                  symbol TEXT,
                  fill_time REAL,
                  fill_price REAL,
                  fill_size REAL,
                  fill_type TEXT,
                  side TEXT,
                  fee REAL,
                  fee_maker REAL,
                  position_sign_changed INTEGER,
                  raw_json TEXT
                )
                """
            )
            conn.execute(
                "INSERT INTO trades (id,symbol,direction,venue,entry_time,entry_price,size,exit_time,state) VALUES (1,'EIGEN','LONG','lighter',1000.0,0.33,10.0,NULL,'ACTIVE')"
            )
            conn.commit()
            conn.close()

            class DummyAdapter:
                async def get_account_trades(self, *args, **kwargs):
                    return []

            r = FillReconciler(db_path=db_path, exchange_adapter=DummyAdapter(), venue="lighter")

            # Entry-side fill but with position_sign_changed=True (0->open)
            fill = Fill(
                venue="lighter",
                exchange_trade_id="x1",
                symbol="EIGEN",
                fill_time=1000.5,
                fill_price=0.331,
                fill_size=10.0,
                side="BUY",
                fee=0.0,
                fee_maker=0.0,
                position_sign_changed=True,
                raw_json="{}",
            )

            # process fill
            import asyncio
            asyncio.run(r._process_fill(fill))

            conn = sqlite3.connect(db_path)
            row = conn.execute("SELECT exit_time, exit_reason, state FROM trades WHERE id=1").fetchone()
            conn.close()

            self.assertIsNone(row[0])
            self.assertIsNone(row[1])
            self.assertEqual(row[2], "ACTIVE")


if __name__ == "__main__":
    unittest.main()
