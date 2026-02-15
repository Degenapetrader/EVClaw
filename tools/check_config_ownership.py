#!/usr/bin/env python3
"""Guardrail checks for config ownership manifest (env/skill/hard-code/removed)."""

from __future__ import annotations

import csv
import re
from pathlib import Path
from typing import Dict, Iterable, List, Sequence, Tuple

ROOT = Path(__file__).resolve().parents[1]
MANIFEST = ROOT / "docs" / "ENV_VARIABLES_GROUPED_3SOURCE.csv"
SKILL_FILE = ROOT / "skill.yaml"
ENV_FILES = (ROOT / ".env", ROOT / ".env.min", ROOT / ".env.example")
RUNTIME_EXTS = {".py", ".sh"}
RUNTIME_EXCLUDE_DIRS = {
    ".git",
    "__pycache__",
    "archive",
    "backups",
    "docs",
    "scripts",
    "tests",
    "tmp",
    "tools",
}
ENV_LOOKUP_FUNCS = (
    "os.getenv",
    "env_present",
    "env_str",
    "env_int",
    "env_float",
    "env_bool",
    "env_list",
    "env_json",
)
EFFECTIVE_FROM_ALLOWED = {"hard-code", "skill.yaml", "removed", ".env", "code default", ""}


def load_manifest_rows(manifest: Path = MANIFEST) -> List[Dict[str, str]]:
    with manifest.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        return [{k: (v or "") for k, v in row.items()} for row in reader]


def _keys_by_effective_from(rows: Iterable[Dict[str, str]], effective_from: str) -> List[str]:
    keys: List[str] = []
    for row in rows:
        key = row.get("key", "").strip()
        source = row.get("effective_from", "").strip()
        if key and source == effective_from:
            keys.append(key)
    return keys


def iter_runtime_files(root: Path = ROOT) -> Iterable[Path]:
    for path in root.rglob("*"):
        if not path.is_file():
            continue
        if path.suffix not in RUNTIME_EXTS:
            continue
        if any(part in RUNTIME_EXCLUDE_DIRS for part in path.parts):
            continue
        yield path


def collect_hardcode_key_leaks(
    rows: Sequence[Dict[str, str]],
    *,
    env_files: Sequence[Path] = ENV_FILES,
    skill_file: Path = SKILL_FILE,
) -> List[str]:
    violations: List[str] = []
    hard_keys = _keys_by_effective_from(rows, "hard-code")

    for env_file in env_files:
        text = env_file.read_text(encoding="utf-8", errors="ignore") if env_file.exists() else ""
        for key in hard_keys:
            if re.search(rf"(?m)^\s*{re.escape(key)}\s*=", text):
                violations.append(f"{env_file.relative_to(ROOT)}: hard-code key present in env: {key}")

    skill_text = skill_file.read_text(encoding="utf-8", errors="ignore")
    for key in hard_keys:
        if re.search(rf"\b{re.escape(key)}\b", skill_text):
            violations.append(f"{skill_file.relative_to(ROOT)}: hard-code key present in skill.yaml: {key}")

    return violations


def collect_removed_env_lookup_violations(
    rows: Sequence[Dict[str, str]],
    *,
    runtime_files: Iterable[Path] | None = None,
) -> List[str]:
    violations: List[str] = []
    removed_keys = _keys_by_effective_from(rows, "removed")
    files = list(runtime_files) if runtime_files is not None else list(iter_runtime_files())

    funcs_pat = r"(?:%s)" % "|".join(re.escape(fn) for fn in ENV_LOOKUP_FUNCS)
    cache: Dict[Path, str] = {}
    for path in files:
        cache[path] = path.read_text(encoding="utf-8", errors="ignore")

    for key in removed_keys:
        key_pat = re.escape(key)
        lookup_pat = re.compile(funcs_pat + r"\(\s*[\"']" + key_pat + r"[\"']")
        for path, text in cache.items():
            if lookup_pat.search(text):
                violations.append(
                    f"{path.relative_to(ROOT)}: removed key still used via env lookup: {key}"
                )
                break
    return violations


def collect_invalid_effective_from_values(
    rows: Sequence[Dict[str, str]],
    *,
    allowed: Sequence[str] = tuple(EFFECTIVE_FROM_ALLOWED),
) -> List[str]:
    violations: List[str] = []
    allowed_set = set(allowed)
    for idx, row in enumerate(rows, start=2):
        value = row.get("effective_from", "").strip()
        if value not in allowed_set:
            key = row.get("key", "").strip()
            violations.append(f"manifest row {idx} invalid effective_from={value!r} key={key}")
    return violations


def collect_violations() -> List[str]:
    rows = load_manifest_rows()
    violations: List[str] = []
    violations.extend(collect_invalid_effective_from_values(rows))
    violations.extend(collect_hardcode_key_leaks(rows))
    violations.extend(collect_removed_env_lookup_violations(rows))
    return violations


def main() -> int:
    violations = collect_violations()
    if violations:
        print("[FAIL] Config ownership violations detected:")
        for item in violations:
            print(f" - {item}")
        return 1
    print("[PASS] Config ownership guardrail clean")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
