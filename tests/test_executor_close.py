#!/usr/bin/env python3
"""Regression test for Executor.close."""

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from executor import ExecutionConfig, Executor


def test_executor_close_does_not_crash() -> None:
    config = ExecutionConfig(dry_run=True, lighter_enabled=False, hl_enabled=False)
    executor = Executor(config=config)
    asyncio.run(executor.close())


if __name__ == "__main__":
    test_executor_close_does_not_crash()
    print("[PASS] executor close")
