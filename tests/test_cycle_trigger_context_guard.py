#!/usr/bin/env python3
"""Regression tests for cycle-trigger context artifact guard."""

import asyncio
import logging
import time
import sys
from pathlib import Path
from datetime import datetime, timezone

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import cycle_trigger


def test_context_artifacts_ready_truth_table(tmp_path) -> None:
    ctx = tmp_path / "ctx.txt"
    ctx_json = tmp_path / "ctx.json"

    assert cycle_trigger._context_artifacts_ready(str(ctx), str(ctx_json)) is False

    ctx.write_text("ok", encoding="utf-8")
    ctx_json.write_text("", encoding="utf-8")
    assert cycle_trigger._context_artifacts_ready(str(ctx), str(ctx_json)) is False

    ctx_json.write_text("{}", encoding="utf-8")
    assert cycle_trigger._context_artifacts_ready(str(ctx), str(ctx_json)) is True


def test_cycle_config_uses_skill_backed_hip3_trigger_defaults(monkeypatch) -> None:
    monkeypatch.setenv("EVCLAW_HIP3_TRIGGER_GLOBAL_COOLDOWN_SEC", "8")
    monkeypatch.setenv("HIP3_TRIGGER_GLOBAL_COOLDOWN_SEC", "99")
    monkeypatch.setenv("EVCLAW_HIP3_TRIGGER_SYMBOL_COOLDOWN_SEC", "55")
    monkeypatch.setenv("HIP3_TRIGGER_SYMBOL_COOLDOWN_SEC", "77")
    monkeypatch.setenv("EVCLAW_HIP3_TRIGGER_TTL_SEC", "700")
    monkeypatch.setenv("HIP3_TRIGGER_TTL_SEC", "900")

    cfg = cycle_trigger.CycleConfig.load()
    assert cfg.hip3_trigger_global_cooldown_sec == 4.0
    assert cfg.hip3_trigger_symbol_cooldown_sec == 45.0
    assert cfg.hip3_trigger_ttl_sec == 600.0


def test_handle_snapshot_suppresses_notify_when_context_missing(tmp_path) -> None:
    trigger = cycle_trigger.CycleTrigger(dry_run=True, verbose=False)
    notified = []

    cycle_file = tmp_path / "cycle.json"
    cycle_file.write_text("{}", encoding="utf-8")

    async def _reserve_cycle_seq():
        return 123

    async def _save_cycle_data(*_args, **_kwargs):
        return str(cycle_file)

    async def _build_context(*_args, **_kwargs):
        return str(tmp_path / "missing_context.txt"), str(tmp_path / "missing_context.json")

    async def _notify_main_agent(payload):
        notified.append(payload)

    trigger._reserve_cycle_seq = _reserve_cycle_seq
    trigger._save_cycle_data = _save_cycle_data
    trigger._build_context = _build_context
    trigger._notify_main_agent = _notify_main_agent

    asyncio.run(trigger._handle_snapshot(sequence=7, symbols={"ETH": {}}))

    assert notified == []
    assert trigger._trigger_count == 0
    assert trigger._last_triggered_seq == -1


def test_cycle_aware_client_handles_hip3_snapshot_callback() -> None:
    received = []

    async def _on_hip3_snapshot(sequence, hip3_snapshot):
        received.append((sequence, hip3_snapshot))

    client = cycle_trigger.CycleAwareSSEClient(
        on_snapshot=None,
        on_hip3_snapshot=_on_hip3_snapshot,
        verbose=False,
    )
    generated_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    msg = cycle_trigger.SSEMessage(
        event_type="hip3-data",
        msg_type="snapshot",
        sequence=42,
        data={"XYZ:AAA": {"hip3_predator": {"generated_at": generated_at}}},
        removed=[],
    )

    asyncio.run(client._handle_hip3_data(msg))

    assert len(received) == 1
    assert received[0][0] == 42
    assert "XYZ:AAA" in received[0][1]


def test_cycle_aware_client_normalizes_hip3_delta_to_full_snapshot() -> None:
    received = []

    async def _on_hip3_snapshot(sequence, hip3_snapshot):
        received.append((sequence, hip3_snapshot))

    client = cycle_trigger.CycleAwareSSEClient(
        on_snapshot=None,
        on_hip3_snapshot=_on_hip3_snapshot,
        verbose=False,
    )
    generated_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

    first = cycle_trigger.SSEMessage(
        event_type="hip3-data",
        msg_type="snapshot",
        sequence=100,
        data={"XYZ:AAA": {"hip3_predator": {"generated_at": generated_at}}},
        removed=[],
    )
    second = cycle_trigger.SSEMessage(
        event_type="hip3-data",
        msg_type="delta",
        sequence=101,
        data={"XYZ:BBB": {"hip3_predator": {"generated_at": generated_at}}},
        removed=[],
    )

    asyncio.run(client._handle_hip3_data(first))
    asyncio.run(client._handle_hip3_data(second))

    assert len(received) == 2
    assert set(received[1][1].keys()) == {"XYZ:AAA", "XYZ:BBB"}


def test_hip3_flip_emits_even_when_symbol_cooldown_active(monkeypatch, caplog) -> None:
    trigger = cycle_trigger.CycleTrigger(dry_run=False, verbose=False)
    symbol = "XYZ:ABC"
    now = time.time()
    trigger._current_symbols = {symbol: {"price": 1.0}}
    trigger._hip3_symbol_last_dir[symbol] = "LONG"
    trigger._hip3_symbol_last_ts[symbol] = now  # cooldown active

    monkeypatch.setattr(
        cycle_trigger,
        "compute_hip3_main",
        lambda _payload, **_kwargs: ({"direction": "SHORT", "z_score": 3.1}, {}),
    )

    emitted = []

    async def _reserve_cycle_seq():
        return 555

    async def _emit_cycle(**kwargs):
        emitted.append(kwargs)
        return True

    trigger._reserve_cycle_seq = _reserve_cycle_seq
    trigger._emit_cycle = _emit_cycle

    generated_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    hip3_state = {symbol: {"hip3_predator": {"generated_at": generated_at}}}

    with caplog.at_level(logging.INFO):
        asyncio.run(trigger._handle_hip3_snapshot(sequence=77, hip3_state=hip3_state))

    assert len(emitted) == 1
    assert emitted[0]["hip3_symbol"] == symbol
    assert emitted[0]["source"] == "hip3"
    assert "reason=direction_flip" in caplog.text


def test_hip3_global_cooldown_suppression_does_not_poison_last_dir(monkeypatch) -> None:
    trigger = cycle_trigger.CycleTrigger(dry_run=False, verbose=False)
    symbol = "XYZ:COOLDOWN"
    trigger._current_symbols = {symbol: {"price": 1.0}}
    trigger._hip3_symbol_last_dir[symbol] = "LONG"
    trigger._last_hip3_global_ts = time.time()  # force first attempt suppression

    monkeypatch.setattr(
        cycle_trigger,
        "compute_hip3_main",
        lambda _payload, **_kwargs: ({"direction": "SHORT", "z_score": 2.4}, {}),
    )

    emitted = []

    async def _reserve_cycle_seq():
        return 777

    async def _emit_cycle(**kwargs):
        emitted.append(kwargs)
        return True

    trigger._reserve_cycle_seq = _reserve_cycle_seq
    trigger._emit_cycle = _emit_cycle

    generated_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    hip3_state = {symbol: {"hip3_predator": {"generated_at": generated_at}}}

    asyncio.run(trigger._handle_hip3_snapshot(sequence=101, hip3_state=hip3_state))

    assert emitted == []
    assert trigger._hip3_symbol_last_dir.get(symbol) == "LONG"

    trigger._last_hip3_global_ts = 0.0
    asyncio.run(trigger._handle_hip3_snapshot(sequence=102, hip3_state=hip3_state))

    assert len(emitted) == 1
    assert emitted[0]["hip3_symbol"] == symbol
    assert trigger._hip3_symbol_last_dir.get(symbol) == "SHORT"


def test_hip3_flip_prioritized_over_non_flip(monkeypatch) -> None:
    trigger = cycle_trigger.CycleTrigger(dry_run=False, verbose=False)
    trigger._cfg = cycle_trigger.CycleConfig(
        **{
            **trigger._cfg.__dict__,
            "hip3_max_emits_per_snapshot": 1,
        }
    )
    flip_symbol = "XYZ:AAA"
    other_symbol = "XYZ:BBB"
    trigger._current_symbols = {flip_symbol: {}, other_symbol: {}}
    trigger._hip3_symbol_last_dir[flip_symbol] = "LONG"  # will flip to SHORT

    def _fake_main(payload, **_kwargs):
        tag = payload.get("tag")
        if tag == "flip":
            return {"direction": "SHORT", "z_score": 1.0}, {}
        return {"direction": "LONG", "z_score": 9.0}, {}

    monkeypatch.setattr(cycle_trigger, "compute_hip3_main", _fake_main)

    picked = []

    async def _reserve_cycle_seq():
        return 556

    async def _emit_cycle(**kwargs):
        picked.append(kwargs.get("hip3_symbol"))
        return True

    trigger._reserve_cycle_seq = _reserve_cycle_seq
    trigger._emit_cycle = _emit_cycle

    generated_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    hip3_state = {
        flip_symbol: {"hip3_predator": {"generated_at": generated_at, "tag": "flip"}},
        other_symbol: {"hip3_predator": {"generated_at": generated_at, "tag": "normal"}},
    }

    asyncio.run(trigger._handle_hip3_snapshot(sequence=78, hip3_state=hip3_state))
    assert picked == [flip_symbol]


def test_classify_hip3_flow_ofm_conflict_label() -> None:
    trigger = cycle_trigger.CycleTrigger(dry_run=True, verbose=False)
    cls = trigger._classify_hip3_flow_ofm(
        {
            "flow": {"direction": "LONG", "z_score": 2.6, "dynamic_threshold": 2.0},
            "ofm_pred": {"direction": "SHORT", "confidence": 0.9},
        }
    )
    assert cls["label"] == "flow_ofm_conflict"
    assert cls["flow_pass"] is True
    assert cls["ofm_pass"] is True
    assert cls["conflict"] is True


def test_build_context_timeout_logs_cleanup_timeout(monkeypatch, caplog) -> None:
    trigger = cycle_trigger.CycleTrigger(dry_run=False, verbose=False)
    trigger._cfg = cycle_trigger.CycleConfig(
        **{
            **trigger._cfg.__dict__,
            "context_builder_timeout_sec": 0.01,
            "context_builder_kill_timeout_sec": 0.01,
        }
    )

    class _Proc:
        pid = 4242
        returncode = None

        async def communicate(self):
            await asyncio.sleep(1.0)
            return b"", b""

        def kill(self):
            return None

    async def _fake_create_subprocess_exec(*_args, **_kwargs):
        return _Proc()

    monkeypatch.setattr(asyncio, "create_subprocess_exec", _fake_create_subprocess_exec)

    with caplog.at_level(logging.WARNING):
        asyncio.run(trigger._build_context(sequence=9, data_file="/tmp/nonexistent.json"))

    assert "cleanup wait timed out" in caplog.text


def test_build_context_atomic_rename_on_success(monkeypatch, tmp_path) -> None:
    trigger = cycle_trigger.CycleTrigger(dry_run=False, verbose=False)
    monkeypatch.setattr(cycle_trigger, "DATA_DIR", str(tmp_path))
    monkeypatch.setattr(cycle_trigger, "mirror_runtime_file_to_tmp", lambda *_args, **_kwargs: None)

    class _Proc:
        pid = 4243
        returncode = 0

        async def communicate(self):
            return b"ok", b""

    async def _fake_create_subprocess_exec(*_args, **kwargs):
        out_idx = kwargs.get("stdout")
        _ = out_idx  # keep signature parity
        argv = list(_args)
        out_path = Path(argv[argv.index("--output") + 1])
        out_json_path = Path(argv[argv.index("--json-output") + 1])
        out_path.write_text("context", encoding="utf-8")
        out_json_path.write_text("{}", encoding="utf-8")
        return _Proc()

    monkeypatch.setattr(asyncio, "create_subprocess_exec", _fake_create_subprocess_exec)

    ctx_txt, ctx_json = asyncio.run(trigger._build_context(sequence=11, data_file="/tmp/cycle.json"))
    final_txt = Path(ctx_txt)
    final_json = Path(ctx_json)

    assert final_txt.exists()
    assert final_json.exists()
    assert final_txt.suffix == ".txt"
    assert final_json.suffix == ".json"
    assert not list(tmp_path.glob("*.tmp.*"))


def test_save_cycle_data_uses_compact_json(monkeypatch, tmp_path) -> None:
    trigger = cycle_trigger.CycleTrigger(dry_run=False, verbose=False)
    monkeypatch.setattr(cycle_trigger, "DATA_DIR", str(tmp_path))
    monkeypatch.setattr(cycle_trigger, "mirror_runtime_file_to_tmp", lambda *_args, **_kwargs: None)

    async def _noop_cleanup(_seq):
        return None

    trigger._cleanup_old_files = _noop_cleanup
    path = asyncio.run(trigger._save_cycle_data(1001, {"ETH": {"v": 1}}, sse_seq=3))
    content = Path(path).read_text(encoding="utf-8")
    assert "\n" not in content
    assert content.startswith("{") and content.endswith("}")


def test_cleanup_old_files_prunes_all_artifact_classes(monkeypatch, tmp_path) -> None:
    runtime = tmp_path / "runtime"
    runtime.mkdir(parents=True, exist_ok=True)
    tmp_mirror = tmp_path / "tmpmirror"
    tmp_mirror.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(cycle_trigger, "DATA_DIR", str(runtime))
    monkeypatch.setattr(cycle_trigger, "TMP_MIRROR_DIR", tmp_mirror)
    for seq in range(1, 29):
        (runtime / f"evclaw_cycle_{seq}.json").write_text("{}", encoding="utf-8")
        (runtime / f"evclaw_context_{seq}.txt").write_text("x", encoding="utf-8")
        (runtime / f"evclaw_context_{seq}.json").write_text("{}", encoding="utf-8")
        (runtime / f"evclaw_candidates_{seq}.json").write_text("{}", encoding="utf-8")
        # Legacy regular /tmp mirror files from older copy-based flows.
        (tmp_mirror / f"evclaw_cycle_{seq}.json").write_text("{}", encoding="utf-8")
        (tmp_mirror / f"evclaw_context_{seq}.txt").write_text("x", encoding="utf-8")
        (tmp_mirror / f"evclaw_context_{seq}.json").write_text("{}", encoding="utf-8")
        (tmp_mirror / f"evclaw_candidates_{seq}.json").write_text("{}", encoding="utf-8")

    trigger = cycle_trigger.CycleTrigger(dry_run=False, verbose=False)
    trigger._cfg = cycle_trigger.CycleConfig(
        **{
            **trigger._cfg.__dict__,
            "cycle_files_keep_n": 20,
        }
    )
    asyncio.run(trigger._cleanup_old_files(current_seq=29))

    assert len(list(runtime.glob("evclaw_cycle_*.json"))) == 20
    assert len(list(runtime.glob("evclaw_context_*.txt"))) == 20
    assert len(list(runtime.glob("evclaw_context_*.json"))) == 20
    assert len(list(runtime.glob("evclaw_candidates_*.json"))) == 20

    # Legacy regular /tmp mirrors should be purged.
    assert len([p for p in tmp_mirror.iterdir() if p.is_file() and not p.is_symlink()]) == 0
