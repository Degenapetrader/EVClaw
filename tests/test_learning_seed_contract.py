#!/usr/bin/env python3
"""Seed contract tests for learning import table list."""

import ast
from pathlib import Path


def _extract_learning_tables(script_path: Path) -> list[str]:
    tree = ast.parse(script_path.read_text(encoding="utf-8"), filename=str(script_path))

    for node in tree.body:
        value_node = None

        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id == "LEARNING_TABLES":
                    value_node = node.value
                    break
        elif isinstance(node, ast.AnnAssign):
            target = node.target
            if isinstance(target, ast.Name) and target.id == "LEARNING_TABLES":
                value_node = node.value

        if value_node is None:
            continue

        if isinstance(value_node, (ast.List, ast.Tuple)):
            tables: list[str] = []
            for elt in value_node.elts:
                if isinstance(elt, ast.Constant) and isinstance(elt.value, str):
                    tables.append(elt.value)
            return tables

    raise AssertionError("LEARNING_TABLES not found in script")


def test_import_learning_tables_include_symbol_policy_not_pattern_stats() -> None:
    root = Path(__file__).resolve().parents[1]
    script = root / "scripts" / "import_learning_seed.py"
    tables = _extract_learning_tables(script)

    assert "symbol_policy" in tables
    assert "pattern_stats" not in tables
