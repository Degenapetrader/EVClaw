#!/usr/bin/env python3
"""Safe cache cleanup helper for EVClaw user VPS environments.

Default behavior is dry-run (no deletions). Use --apply to execute.
"""

from __future__ import annotations

import argparse
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Iterable, List, Tuple


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _human_bytes(num: int) -> str:
    size = float(max(0, int(num)))
    units = ["B", "KB", "MB", "GB", "TB"]
    for unit in units:
        if size < 1024.0 or unit == units[-1]:
            return f"{size:.1f}{unit}"
        size /= 1024.0
    return f"{size:.1f}TB"


def _collect_pycache(root: Path) -> Tuple[List[Path], int]:
    dirs: List[Path] = []
    bytes_total = 0
    for d in root.rglob("__pycache__"):
        if not d.is_dir():
            continue
        dirs.append(d)
        for p in d.rglob("*"):
            if p.is_file():
                try:
                    bytes_total += int(p.stat().st_size)
                except Exception:
                    pass
    return dirs, bytes_total


def _collect_pyc(root: Path) -> Tuple[List[Path], int]:
    files: List[Path] = []
    bytes_total = 0
    for pattern in ("*.pyc", "*.pyo"):
        for p in root.rglob(pattern):
            if not p.is_file():
                continue
            files.append(p)
            try:
                bytes_total += int(p.stat().st_size)
            except Exception:
                pass
    return files, bytes_total


def _delete_paths(paths: Iterable[Path]) -> Tuple[int, int]:
    removed = 0
    failed = 0
    for path in paths:
        try:
            if path.is_dir():
                shutil.rmtree(path)
            elif path.exists() or path.is_symlink():
                path.unlink()
            removed += 1
        except Exception:
            failed += 1
    return removed, failed


def _run_cmd(argv: List[str]) -> Tuple[int, str]:
    try:
        cp = subprocess.run(argv, capture_output=True, text=True, check=False)
        out = (cp.stdout or "").strip()
        err = (cp.stderr or "").strip()
        msg = out if out else err
        return int(cp.returncode), msg
    except Exception as exc:
        return 127, str(exc)


def main() -> int:
    ap = argparse.ArgumentParser(description="Cleanup EVClaw runtime caches safely (dry-run by default).")
    ap.add_argument("--apply", action="store_true", help="Apply cleanup actions. Without this, no files are deleted.")
    ap.add_argument("--npm-cache", action="store_true", help="Run npm cache verify.")
    ap.add_argument(
        "--npm-cache-clean",
        action="store_true",
        help="Run npm cache clean --force (requires --apply and --npm-cache).",
    )
    args = ap.parse_args()

    root = _repo_root()
    pycache_dirs, pycache_bytes = _collect_pycache(root)
    pyc_files, pyc_bytes = _collect_pyc(root)
    target_count = len(pycache_dirs) + len(pyc_files)
    total_bytes = pycache_bytes + pyc_bytes

    print("EVClaw cleanup runtime artifacts")
    print(f"root={root}")
    print(f"mode={'APPLY' if args.apply else 'DRY_RUN'}")
    print(f"pycache_dirs={len(pycache_dirs)}")
    print(f"pyc_files={len(pyc_files)}")
    print(f"python_cache_bytes={_human_bytes(total_bytes)}")

    if args.apply:
        removed_dirs, failed_dirs = _delete_paths(pycache_dirs)
        removed_files, failed_files = _delete_paths(pyc_files)
        print(
            f"python_cleanup removed={removed_dirs + removed_files} "
            f"failed={failed_dirs + failed_files} targets={target_count}"
        )
    else:
        print("python_cleanup dry-run only; use --apply to delete.")

    if args.npm_cache:
        rc_verify, msg_verify = _run_cmd(["npm", "cache", "verify"])
        print(f"npm_cache_verify rc={rc_verify}")
        if msg_verify:
            print(msg_verify)

        if args.npm_cache_clean:
            if not args.apply:
                print("npm_cache_clean skipped: requires --apply.")
            else:
                rc_clean, msg_clean = _run_cmd(["npm", "cache", "clean", "--force"])
                print(f"npm_cache_clean rc={rc_clean}")
                if msg_clean:
                    print(msg_clean)
    elif args.npm_cache_clean:
        print("npm_cache_clean skipped: add --npm-cache.")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
