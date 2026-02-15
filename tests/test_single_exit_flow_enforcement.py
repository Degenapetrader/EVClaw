from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]


def _read(rel_path: str) -> str:
    return (REPO_ROOT / rel_path).read_text(encoding="utf-8")


def test_decay_worker_is_plan_only() -> None:
    src = _read("decay_worker.py")
    assert "--execute" not in src
    assert "executor.close_position(" not in src
    assert "DECAY_EXIT_EXEC" not in src
    assert "'event': 'DECAY_EXIT'" not in src
    assert 'source="decay_worker_notify"' in src


def test_position_review_worker_is_plan_only() -> None:
    src = _read("position_review_worker.py")
    assert "--execute-closes" not in src
    assert "EVCLAW_REVIEW_EXECUTE_CLOSES" not in src
    assert "EVCLAW_POSITION_REVIEW_ALLOW_EXECUTE_CLOSES" not in src
    assert "executor.close_position(" not in src
    assert "record_decay_decision(" in src
    assert 'source="position_review_worker"' in src
    assert '"event": "POSITION_REVIEW_PLAN"' not in src


def test_cli_close_execution_path_is_decider_only() -> None:
    decider_src = _read("llm_exit_decider.py")
    assert "_run_cli_close(" in decider_src
    assert "positions --close" in decider_src

    for path in REPO_ROOT.glob("*.py"):
        if path.name == "llm_exit_decider.py":
            continue
        src = path.read_text(encoding="utf-8")
        assert "_run_cli_close(" not in src, f"{path.name} should not define/call _run_cli_close"
        assert "positions --close" not in src, f"{path.name} should not execute cli positions --close"


def test_restart_script_uses_hybrid_fill_mode() -> None:
    src = _read("restart.sh")
    assert '[evclaw-fill-reconciler]="python3 run_fill_reconciler.py --mode hybrid 2>&1"' in src
