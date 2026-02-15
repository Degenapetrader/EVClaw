#!/usr/bin/env python3
"""CLI tests for context_builder_v2."""

import json
import subprocess
import sys
from pathlib import Path


def test_context_builder_json_output(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[1]
    script = root / "context_builder_v2.py"
    fixture = Path(__file__).resolve().parent / "fixtures" / "cycle_sample.json"

    output_text = tmp_path / "context.txt"
    output_json = tmp_path / "context.json"

    result = subprocess.run(
        [
            sys.executable,
            str(script),
            "--cycle-file",
            str(fixture),
            "--output",
            str(output_text),
            "--json-output",
            str(output_json),
        ],
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert output_text.exists()
    assert output_json.exists()

    payload = json.loads(output_json.read_text())
    assert payload["cycle_file"] == str(fixture)
    assert payload["symbol_count"] == 4
    assert payload["generated_at"].endswith("Z")
    assert isinstance(payload["selected_opportunities"], list)

    for opp in payload["selected_opportunities"]:
        assert {
            "symbol",
            "score",
            "direction",
            "signals",
            "key_metrics",
        }.issubset(opp)
        assert "raw_data" not in opp
