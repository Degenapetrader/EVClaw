#!/usr/bin/env python3
"""Deterministic EVClaw repository update checker.

This script compares local HEAD with origin/main and writes:
- JSON report (machine-readable)
- summary text (cron-friendly)
- markdown changelog block (chat-ready)

It never mutates the working tree and never applies updates.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

ROOT = Path(__file__).resolve().parents[1]
RUNTIME_DIR = ROOT / "state"
DEFAULT_JSON_OUT = RUNTIME_DIR / "repo_update_report.json"
DEFAULT_SUMMARY_OUT = RUNTIME_DIR / "repo_update_summary.txt"
DEFAULT_CHANGELOG_MD_OUT = RUNTIME_DIR / "repo_update_changelog.md"
DEFAULT_STATE_PATH = RUNTIME_DIR / "repo_update_state.json"


def _utc_iso(ts: float) -> str:
    return datetime.fromtimestamp(float(ts), tz=timezone.utc).isoformat()


def _run_git(args: List[str]) -> str:
    cmd = ["git", *args]
    proc = subprocess.run(
        cmd,
        cwd=str(ROOT),
        text=True,
        capture_output=True,
        check=False,
    )
    if proc.returncode != 0:
        err = (proc.stderr or proc.stdout or "").strip()
        raise RuntimeError(f"git {' '.join(args)} failed: {err}")
    return (proc.stdout or "").strip()


def _run_git_no_raise(args: List[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["git", *args],
        cwd=str(ROOT),
        text=True,
        capture_output=True,
        check=False,
    )


def _read_json(path: Path) -> Dict[str, Any]:
    try:
        raw = path.read_text(encoding="utf-8")
        obj = json.loads(raw)
        if isinstance(obj, dict):
            return obj
    except Exception:
        pass
    return {}


def _write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _write_summary(path: Path, lines: List[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _fmt_yes_no(v: bool) -> str:
    return "yes" if bool(v) else "no"


def _write_changelog_md(
    path: Path,
    *,
    status: str,
    remote_ref: str,
    local_head: str,
    remote_head: str,
    ahead: int,
    behind: int,
    notify_user: bool,
    changelog_lines: List[str],
    error: str = "",
) -> None:
    lines: List[str] = []
    lines.append("## EVClaw Repository Update")
    lines.append(f"- Status: `{status}`")
    if local_head:
        lines.append(f"- Local HEAD: `{local_head[:12]}`")
    if remote_head:
        lines.append(f"- Remote HEAD ({remote_ref}): `{remote_head[:12]}`")
    lines.append(f"- Ahead/Behind: `{int(ahead)}/{int(behind)}`")

    if status == "UPDATE_AVAILABLE":
        lines.append(f"- Notify user now: `{_fmt_yes_no(notify_user)}`")
        lines.append("")
        lines.append("### Changelog (HEAD..remote)")
        if changelog_lines:
            for raw in changelog_lines:
                txt = str(raw or "").strip()
                if not txt:
                    continue
                parts = txt.split(" ", 2)
                if len(parts) == 3:
                    h, d, s = parts
                    lines.append(f"- `{h}` {d} {s}")
                else:
                    lines.append(f"- {txt}")
        else:
            lines.append("- No changelog entries found.")
    elif status == "UP_TO_DATE":
        lines.append("- Repo is up to date.")
    else:
        lines.append("- Update check failed.")
        if error:
            lines.append("")
            lines.append("```text")
            lines.append(str(error))
            lines.append("```")

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    ap = argparse.ArgumentParser(description="Check EVClaw repo updates without applying changes.")
    ap.add_argument("--remote", default="origin", help="Git remote name (default: origin)")
    ap.add_argument("--branch", default="main", help="Git branch name on remote (default: main)")
    ap.add_argument("--changelog-limit", type=int, default=20, help="Max changelog lines in JSON")
    ap.add_argument("--remind-seconds", type=int, default=86400, help="Reminder interval when update is still pending")
    ap.add_argument("--json-out", default=str(DEFAULT_JSON_OUT), help="Output JSON path")
    ap.add_argument("--summary-out", default=str(DEFAULT_SUMMARY_OUT), help="Output summary path")
    ap.add_argument("--changelog-md-out", default=str(DEFAULT_CHANGELOG_MD_OUT), help="Output markdown changelog path")
    ap.add_argument("--state-path", default=str(DEFAULT_STATE_PATH), help="Persistent state path for notify throttling")
    args = ap.parse_args()

    now_ts = time.time()
    remote_ref = f"{args.remote}/{args.branch}"
    json_out = Path(args.json_out)
    summary_out = Path(args.summary_out)
    changelog_md_out = Path(args.changelog_md_out)
    state_path = Path(args.state_path)

    report: Dict[str, Any] = {
        "checked_at": _utc_iso(now_ts),
        "checked_at_ts": now_ts,
        "repo_path": str(ROOT),
        "remote": str(args.remote),
        "branch": str(args.branch),
        "remote_ref": remote_ref,
        "status": "CHECK_FAILED",
        "update_available": False,
        "notify_user": False,
        "remote_changed": False,
        "ahead": 0,
        "behind": 0,
        "local_head": "",
        "remote_head": "",
        "changelog": [],
        "changelog_total": 0,
        "changelog_md_path": str(changelog_md_out),
        "error": "",
    }

    summary_lines: List[str] = []
    changelog_md_lines: List[str] = []

    try:
        _run_git(["rev-parse", "--is-inside-work-tree"])
        local_head = _run_git(["rev-parse", "HEAD"])
        branch_name = _run_git(["rev-parse", "--abbrev-ref", "HEAD"])

        fetch_proc = _run_git_no_raise(["fetch", "--quiet", str(args.remote)])
        if fetch_proc.returncode != 0:
            err = (fetch_proc.stderr or fetch_proc.stdout or "").strip()
            raise RuntimeError(f"git fetch failed: {err}")

        remote_head = _run_git(["rev-parse", remote_ref])

        counts = _run_git(["rev-list", "--left-right", "--count", f"HEAD...{remote_ref}"])
        parts = counts.split()
        if len(parts) != 2:
            raise RuntimeError(f"unexpected rev-list output: {counts}")
        ahead = int(parts[0])
        behind = int(parts[1])

        update_available = behind > 0
        changelog_all: List[str] = []
        if update_available:
            raw = _run_git(
                [
                    "log",
                    "--no-decorate",
                    "--date=short",
                    "--pretty=format:%h %ad %s",
                    f"HEAD..{remote_ref}",
                ]
            )
            changelog_all = [ln.strip() for ln in raw.splitlines() if ln.strip()]

        state = _read_json(state_path)
        last_seen_remote = str(state.get("last_seen_remote") or "").strip()
        last_notified_ts = float(state.get("last_notified_ts") or 0.0)

        remote_changed = bool(remote_head and remote_head != last_seen_remote)
        notify_user = False
        if update_available:
            if remote_changed:
                notify_user = True
            elif (now_ts - last_notified_ts) >= max(0, int(args.remind_seconds)):
                notify_user = True

        state_out = dict(state)
        state_out["last_checked_ts"] = now_ts
        state_out["last_seen_remote"] = remote_head
        if notify_user:
            state_out["last_notified_ts"] = now_ts
            state_out["last_notified_remote"] = remote_head
        _write_json(state_path, state_out)

        changelog_limited = changelog_all[: max(0, int(args.changelog_limit))]
        report.update(
            {
                "status": "UPDATE_AVAILABLE" if update_available else "UP_TO_DATE",
                "update_available": bool(update_available),
                "notify_user": bool(notify_user),
                "remote_changed": bool(remote_changed),
                "ahead": int(ahead),
                "behind": int(behind),
                "local_head": local_head,
                "remote_head": remote_head,
                "branch_local": branch_name,
                "changelog": changelog_limited,
                "changelog_total": len(changelog_all),
                "error": "",
            }
        )

        summary_lines.append(f"repo_update_status={report['status']}")
        summary_lines.append(
            "local_head={local} remote_head={remote} ahead={ahead} behind={behind}".format(
                local=(local_head[:12] if local_head else ""),
                remote=(remote_head[:12] if remote_head else ""),
                ahead=int(ahead),
                behind=int(behind),
            )
        )
        summary_lines.append(
            "notify_user={notify} remote_changed={changed} remind_seconds={remind}".format(
                notify=("yes" if notify_user else "no"),
                changed=("yes" if remote_changed else "no"),
                remind=int(args.remind_seconds),
            )
        )
        summary_lines.append(f"changelog_md_path={str(changelog_md_out)}")
        if changelog_all:
            summary_lines.append(f"changelog_top1={changelog_all[0]}")
        else:
            summary_lines.append("changelog_top1=")

        changelog_md_lines = changelog_limited

    except Exception as exc:
        report["status"] = "CHECK_FAILED"
        report["error"] = str(exc)
        summary_lines = [
            "repo_update_status=CHECK_FAILED",
            f"error={str(exc)}",
            "notify_user=no remote_changed=no remind_seconds=0",
            f"changelog_md_path={str(changelog_md_out)}",
            "changelog_top1=",
        ]
        changelog_md_lines = []

    _write_json(json_out, report)
    _write_summary(summary_out, summary_lines)
    _write_changelog_md(
        changelog_md_out,
        status=str(report.get("status") or "CHECK_FAILED"),
        remote_ref=remote_ref,
        local_head=str(report.get("local_head") or ""),
        remote_head=str(report.get("remote_head") or ""),
        ahead=int(report.get("ahead") or 0),
        behind=int(report.get("behind") or 0),
        notify_user=bool(report.get("notify_user")),
        changelog_lines=changelog_md_lines,
        error=str(report.get("error") or ""),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
