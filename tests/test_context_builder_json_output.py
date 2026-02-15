import json
import os
import subprocess
from pathlib import Path


def test_context_builder_v2_json_output(tmp_path: Path):
    repo_dir = Path(__file__).resolve().parents[1]
    script = repo_dir / "context_builder_v2.py"
    fixture = repo_dir / "tests" / "fixtures" / "cycle_sample.json"

    out_txt = tmp_path / "context.txt"
    out_json = tmp_path / "context.json"

    cmd = [
        "python3",
        str(script),
        "--cycle-file",
        str(fixture),
        "--output",
        str(out_txt),
        "--json-output",
        str(out_json),
    ]

    p = subprocess.run(cmd, cwd=str(repo_dir), capture_output=True, text=True)
    assert p.returncode == 0, f"stderr:\n{p.stderr}\nstdout:\n{p.stdout}"

    assert out_txt.exists() and out_txt.stat().st_size > 0
    assert out_json.exists() and out_json.stat().st_size > 0

    payload = json.loads(out_json.read_text())

    assert "generated_at" in payload
    assert payload["generated_at"].endswith("Z")
    assert isinstance(payload.get("generated_at_ts"), (int, float))
    assert float(payload["generated_at_ts"]) > 0
    assert payload["cycle_file"] == str(fixture)
    assert payload["symbol_count"] == 4

    assert "selected_opportunities" in payload and isinstance(
        payload["selected_opportunities"], list
    )

    # Each opportunity should be compact and not contain raw_data.
    if payload["selected_opportunities"]:
        o0 = payload["selected_opportunities"][0]
        assert "symbol" in o0
        assert "score" in o0
        assert "direction" in o0
        assert "signals" in o0
        assert "key_metrics" in o0
        assert "raw_data" not in o0
