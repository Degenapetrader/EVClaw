from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict

from ai_trader_db import AITraderDB
from trade_tracker import TradeTracker


@dataclass
class LiveAgentDeps:
    config: Dict[str, Any]
    exec_config: Any
    db: AITraderDB
    db_path: str
    tracker: TradeTracker
