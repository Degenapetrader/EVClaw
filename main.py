#!/usr/bin/env python3
"""
Analysis/report entrypoint for EVClaw.

This script is ANALYSIS/REPORT only. It does not execute trades.
"""

import argparse
import asyncio
import json
import logging
from logging_utils import get_logger
import os
import requests
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import yaml
from dotenv import load_dotenv

# Load environment variables from .env BEFORE any imports that use them
load_dotenv(Path(__file__).parent / '.env')

from context_builder_v2 import ContextBuilderV2, ScoredOpportunity
from risk_manager import DynamicRiskManager, RiskConfig
from safety_manager import SafetyManager
from trading_brain import BrainConfig, TradingBrain
from constants import BASE_CONFIDENCE_THRESHOLD
from sse_consumer import TrackerSSEClient, SSEMessage
from env_utils import env_int, env_str


SKILL_DIR = Path(__file__).parent



def load_config(path: Path) -> Dict[str, Any]:
    """Load YAML config from path."""
    if not path.exists():
        raise FileNotFoundError(f"Config not found: {path}")
    with open(path, "r") as f:
        return yaml.safe_load(f) or {}


def get_nested(config: Dict[str, Any], *keys: str, default: Any = None) -> Any:
    """Get nested config value with fallback."""
    current = config
    for key in keys:
        if not isinstance(current, dict) or key not in current:
            return default
        current = current[key]
    return current


def _normalize_hl_base_url(raw: str) -> str:
    base = (raw or "").strip()
    if not base:
        return "https://node2.evplus"
    if base.endswith("/info"):
        base = base[:-5]
    return base.rstrip("/")


def _resolve_hl_equity_address() -> Optional[str]:
    address = os.getenv("HYPERLIQUID_ADDRESS", "").strip()
    return address or None


def _fetch_hl_equity_sync(address: str, base_url: str, log: logging.Logger) -> float:
    payload = {"type": "clearinghouseState", "user": address}
    try:
        resp = requests.post(f"{base_url}/info", json=payload, timeout=5)
        resp.raise_for_status()
        state = resp.json()
    except Exception as exc:
        log.warning(f"Starting equity auto-fetch failed: {exc}")
        return 0.0

    margin = state.get("marginSummary", {}) if isinstance(state, dict) else {}
    return float(margin.get("accountValue", 0.0) or 0.0)


def _resolve_starting_equity(config: Dict[str, Any], log: logging.Logger) -> float:
    address = _resolve_hl_equity_address()
    base_url = _normalize_hl_base_url(os.getenv("HYPERLIQUID_PRIVATE_NODE", ""))

    if address:
        fetched = _fetch_hl_equity_sync(address, base_url, log)
    else:
        fetched = 0.0

    if fetched > 0:
        log.info(f"Starting equity mode=auto-fetch value={fetched:.2f}")
        return fetched

    fallback = float(
        (config.get("config", {}).get("risk", {}) or {}).get("starting_equity", 10000.0) or 0.0
    )
    if fallback <= 0:
        fallback = 10000.0
    if not address:
        log.warning(
            "Starting equity mode=fallback "
            f"value={fallback:.2f} (missing HYPERLIQUID_ADDRESS)"
        )
    else:
        log.warning(
            "Starting equity mode=fallback "
            f"value={fallback:.2f} (auto-fetch failed)"
        )
    return fallback


def save_cycle_file(sequence: int, symbols: Dict[str, Any], data_dir: Path) -> Path:
    """Save cycle data to a JSON file and return its path."""
    data_dir.mkdir(parents=True, exist_ok=True)
    path = data_dir / f"evclaw_cycle_{sequence}.json"
    payload = {
        "sequence": sequence,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "symbol_count": len(symbols),
        "symbols": symbols,
    }
    with open(path, "w") as f:
        json.dump(payload, f, indent=2, default=str)
    return path


class AnalysisEngine:
    """Builds opportunity reports without executing trades."""

    def __init__(self, config: Dict[str, Any]):
        self.log = get_logger("analysis")
        cfg = config.get("config", {})

        self.db_path = get_nested(cfg, "db_path", default=str(SKILL_DIR / "ai_trader.db"))

        context_cfg = cfg.get("context_builder", {})
        self.context_builder = ContextBuilderV2(
            db_path=self.db_path,
            memory_dir=get_nested(context_cfg, "memory_dir", default=SKILL_DIR / "memory"),
            max_opportunities=int(get_nested(context_cfg, "max_opportunities", default=30)),
            min_score=float(get_nested(context_cfg, "min_score", default=35.0)),
        )

        brain_cfg = cfg.get("brain", {})
        self.brain = TradingBrain(
            config=BrainConfig(
                base_confidence_threshold=float(
                    get_nested(brain_cfg, "base_confidence_threshold", default=BASE_CONFIDENCE_THRESHOLD)
                ),
                high_conviction_threshold=float(get_nested(brain_cfg, "high_conviction_threshold", default=0.75)),
                degen_mode=bool(get_nested(brain_cfg, "degen_mode", default=True)),
                degen_multiplier=float(get_nested(brain_cfg, "degen_multiplier", default=1.5)),
            )
        )

        safety_enabled = bool(get_nested(cfg, "safety", "enabled", default=True))
        starting_equity = float(get_nested(cfg, "risk", "starting_equity", default=10000.0))
        self.safety = SafetyManager(self.db_path, starting_equity=starting_equity) if safety_enabled else None

        risk_cfg = cfg.get("risk", {})
        self.risk = DynamicRiskManager(
            config=RiskConfig(
                min_risk_pct=float(get_nested(risk_cfg, "min_risk_pct", default=0.5)),
                max_risk_pct=float(get_nested(risk_cfg, "max_risk_pct", default=2.5)),
                base_risk_pct=float(get_nested(risk_cfg, "base_risk_pct", default=1.0)),
                equity_floor_pct=float(get_nested(risk_cfg, "equity_floor_pct", default=80.0)),
                daily_drawdown_limit_pct=float(get_nested(risk_cfg, "daily_drawdown_limit_pct", default=5.0)),
                max_concurrent_positions=int(get_nested(risk_cfg, "max_concurrent_positions", default=5)),
                max_sector_concentration=int(get_nested(risk_cfg, "max_sector_concentration", default=3)),
                no_hard_stops=bool(get_nested(risk_cfg, "no_hard_stops", default=True)),
                max_hold_hours=float(get_nested(risk_cfg, "max_hold_hours", default=24.0)),
                min_hold_hours=float(get_nested(risk_cfg, "min_hold_hours", default=2.0)),
                emergency_loss_pct=float(get_nested(risk_cfg, "emergency_loss_pct", default=10.0)),
                emergency_portfolio_loss_pct=float(get_nested(risk_cfg, "emergency_portfolio_loss_pct", default=15.0)),
                soft_stop_atr_mult=float(get_nested(risk_cfg, "soft_stop_atr_mult", default=2.0)),
                soft_stop_alert=bool(get_nested(risk_cfg, "soft_stop_alert", default=True)),
                stale_signal_max_minutes=float(get_nested(risk_cfg, "stale_signal_max_minutes", default=5.0)),
            ),
            equity=starting_equity,
            safety_manager=self.safety,
        )

        exec_cfg = cfg.get("executor", {})
        # Optional per-symbol cap. Set <=0 to disable.
        self.max_position_usd = float(exec_cfg.get("max_position_per_symbol_usd", 0.0))
        self.base_position_usd = float(exec_cfg.get("base_position_size_usd", 500.0))

    def evaluate(self, symbols: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], List[ScoredOpportunity]]:
        """Evaluate opportunities and build a decision list."""
        opportunities = self.context_builder.get_opportunities(symbols)
        decisions: List[Dict[str, Any]] = []

        for opp in opportunities:
            decision = self.brain.evaluate_opportunity(opp)
            atr_pct = float(opp.key_metrics.get("atr_pct", 0) or 0)
            risk_pct = self.risk.calculate_risk_budget(decision.conviction or 0.0, opp.symbol)
            size_usd = self.risk.position_size_usd(risk_pct, atr_pct)
            if size_usd <= 0:
                size_usd = self.base_position_usd
            if self.max_position_usd > 0:
                size_usd = min(size_usd, self.max_position_usd)

            decisions.append({
                "symbol": decision.symbol,
                "direction": decision.direction,
                "conviction": decision.conviction,
                "reason": decision.reasoning,
                "should_trade": decision.should_trade,
                "score": opp.score,
                "atr_pct": atr_pct,
                "size_usd": size_usd,
            })

        return decisions, opportunities

    def report(self, decisions: List[Dict[str, Any]], cycle_file: Path, sequence: Optional[int]) -> None:
        """Print a concise report and suggested execute commands."""
        actionable = [d for d in decisions if d.get("should_trade")]
        self.log.info(f"Cycle seq={sequence} opportunities={len(decisions)} actionable={len(actionable)}")

        top = decisions[:5]
        if top:
            print("Top opportunities:")
            for item in top:
                conv = item.get("conviction")
                conv_str = f"{conv:.2f}" if conv is not None else "n/a"
                print(
                    f"- {item['symbol']} score={item['score']:.1f} dir={item['direction']} "
                    f"conv={conv_str} atr_pct={item['atr_pct']:.2f}"
                )

        if actionable:
            print("\nSuggested executions:")
            for item in actionable:
                venue = "hip3" if str(item['symbol']).lower().startswith("xyz:") else "lighter"
                print(
                    "python3 cli.py execute "
                    f"--cycle-file {cycle_file} "
                    f"--symbol {item['symbol']} "
                    f"--direction {item['direction']} "
                    f"--size-usd {item['size_usd']:.2f} "
                    f"--venue {venue}"
                )
        else:
            print("\nNo actionable trades this cycle.")


class SnapshotSSEClient(TrackerSSEClient):
    """Tracker SSE client that exposes snapshot callbacks."""

    def __init__(self, on_snapshot=None, **kwargs):
        super().__init__(**kwargs)
        self._on_snapshot = on_snapshot

    async def _handle_tracker_data(self, msg: SSEMessage) -> None:
        await super()._handle_tracker_data(msg)
        if msg.msg_type == "snapshot" and self._on_snapshot:
            await self._on_snapshot(msg.sequence, self._symbols)


class AnalysisLoop:
    """Runs analysis on snapshot boundaries."""

    def __init__(self, config: Dict[str, Any]):
        self.log = get_logger("main")
        self.config = config
        self.engine = AnalysisEngine(config)
        self._cycle_lock = asyncio.Lock()

    async def handle_snapshot(self, sequence: int, symbols: Dict[str, Any]) -> None:
        if self._cycle_lock.locked():
            self.log.warning(f"Skipping snapshot {sequence}: previous cycle running")
            return

        async with self._cycle_lock:
            cycle_file = save_cycle_file(sequence, symbols, Path("/tmp"))
            decisions, _ = self.engine.evaluate(symbols)
            self.engine.report(decisions, cycle_file, sequence)


def report_from_cycle_file(config: Dict[str, Any], cycle_file: Path) -> None:
    with open(cycle_file, "r") as f:
        payload = json.load(f)
    sequence = payload.get("sequence")
    symbols = payload.get("symbols", {})

    engine = AnalysisEngine(config)
    decisions, _ = engine.evaluate(symbols)
    engine.report(decisions, cycle_file, sequence)


async def run_sse(config: Dict[str, Any]) -> None:
    cfg = config.get("config", {})
    host = env_str("EVCLAW_SSE_HOST", get_nested(cfg, "sse_host", default="tracker.evplus.ai"))
    host = str(host or "tracker.evplus.ai").strip()
    port = env_int(
        "EVCLAW_SSE_PORT",
        int(
            env_str("EVCLAW_SSE_PORT", str(int(get_nested(cfg, "sse_port", default=8443) or 8443)))
            or 8443
        ),
    )
    endpoint = str(
        env_str("EVCLAW_SSE_ENDPOINT", str(get_nested(cfg, "sse_endpoint", default="/sse/tracker")))
        or "/sse/tracker"
    ).strip()
    wallet_address = (
        os.getenv("HYPERLIQUID_ADDRESS", "").strip()
        or env_str("EVCLAW_WALLET_ADDRESS", "")
    )

    loop = AnalysisLoop(config=config)

    client_kwargs = {
        "host": host,
        "port": port,
        "on_snapshot": loop.handle_snapshot,
        "endpoint": endpoint,
    }
    if wallet_address:
        client_kwargs["wallet_address"] = wallet_address

    client = SnapshotSSEClient(**client_kwargs)

    try:
        await client.connect()
    finally:
        await client.disconnect()


def main() -> int:
    parser = argparse.ArgumentParser(description="EVClaw analysis/report")
    parser.add_argument("--config", default=str(SKILL_DIR / "skill.yaml"), help="Path to skill.yaml")
    parser.add_argument("--cycle-file", help="Analyze a saved cycle JSON and exit")
    args = parser.parse_args()

    config = load_config(Path(args.config))
    log = get_logger("main")
    starting_equity = _resolve_starting_equity(config, log)
    config.setdefault("config", {}).setdefault("risk", {})["starting_equity"] = starting_equity

    if args.cycle_file:
        report_from_cycle_file(config, Path(args.cycle_file))
        return 0

    try:
        asyncio.run(run_sse(config))
    except KeyboardInterrupt:
        print("Shutting down...")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
