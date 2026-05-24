#!/usr/bin/env python3
"""Migrate legacy task files to explicit status frontmatter."""

from __future__ import annotations

import argparse
import os
import shutil
import sys
import tempfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.models import TaskStatus  # noqa: E402
from src.queue_parser import QueueValidationError, TaskHeader, parse_task_header  # noqa: E402

VALID_FRONTMATTER_STATUSES = {"TODO", "DONE", "ERROR"}
LEGACY_FALLBACK_SUFFIXES = {
    ": missing Branch",
    ": missing Type",
    ": missing Complexity",
    ": missing Depends on",
}


@dataclass(frozen=True)
class MigrationResult:
    checked: int
    changed: int
    backups_dir: Path | None = None


def resolve_tasks_dir(repo: Path) -> Path:
    """Return the task directory for either a repo root or a tasks path."""
    candidate = repo.expanduser().resolve()
    if candidate.name == "tasks":
        return candidate
    tasks_dir = candidate / "tasks"
    if tasks_dir.is_dir():
        return tasks_dir
    return candidate


def has_frontmatter(content: str) -> bool:
    lines = content.splitlines()
    first = next((index for index, line in enumerate(lines) if line.strip()), None)
    if first is None or lines[first].rstrip() != "---":
        return False
    return any(line.rstrip() == "---" for line in lines[first + 1 :])


def legacy_status(content: str) -> str:
    """Return the legacy header status, defaulting to TODO like the parser."""
    in_task = False
    in_fields = False
    for raw_line in content.splitlines():
        line = raw_line.rstrip()
        if not in_task:
            if line.startswith("# "):
                in_task = True
            continue
        if not line.strip():
            continue
        if line.startswith("Branch:"):
            in_fields = True
            continue
        if line.startswith("- "):
            in_fields = True
            key, sep, value = line[2:].partition(":")
            if sep and key.strip().lower() == "status":
                status = value.strip().upper()
                if status in VALID_FRONTMATTER_STATUSES:
                    return status
                if status in {member.value for member in TaskStatus}:
                    raise ValueError(
                        f"legacy status {status!r} cannot be represented in frontmatter"
                    )
            continue
        if line.startswith("#") or in_fields:
            break
    return "TODO"


def current_status(content: str, fallback_status: str | None) -> str:
    if fallback_status is not None:
        return fallback_status
    if not has_frontmatter(content):
        return legacy_status(content)
    in_frontmatter = False
    for raw_line in content.splitlines():
        line = raw_line.rstrip()
        if not in_frontmatter:
            if line == "---":
                in_frontmatter = True
            continue
        if line == "---":
            break
        key, sep, value = line.partition(":")
        if sep and key.strip().lower() == "status":
            status = value.strip().split("#", 1)[0].strip().strip("'\"").upper()
            if status in VALID_FRONTMATTER_STATUSES:
                return status
    return "TODO"


def converted_content(content: str) -> str:
    status = legacy_status(content)
    return f"---\nstatus: {status}\n---\n\n{content}"


def task_files(tasks_dir: Path) -> list[Path]:
    return sorted(tasks_dir.glob("PR-*.md"))


def _is_legacy_validation_error(exc: QueueValidationError) -> bool:
    return bool(exc.issues) and all(
        any(issue.endswith(suffix) for suffix in LEGACY_FALLBACK_SUFFIXES)
        for issue in exc.issues
    )


def _legacy_issue_kinds(path: Path, issues: list[str]) -> tuple[str, ...]:
    prefix = f"{path}: "
    return tuple(issue.removeprefix(prefix) for issue in issues)


def _parse_or_legacy_issues(path: Path) -> TaskHeader | tuple[str, ...]:
    try:
        return parse_task_header(path)
    except QueueValidationError as exc:
        if _is_legacy_validation_error(exc):
            return _legacy_issue_kinds(path, exc.issues)
        raise


def _atomic_write(path: Path, content: str) -> None:
    fd, temp_name = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".tmp", dir=str(path.parent), text=True
    )
    temp_path = Path(temp_name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="") as handle:
            handle.write(content)
        os.replace(temp_path, path)
    except Exception:
        temp_path.unlink(missing_ok=True)
        raise


def backup_path_for(backups_dir: Path, tasks_dir: Path, task_path: Path) -> Path:
    relative = task_path.relative_to(tasks_dir)
    return backups_dir / relative


def create_backup(backups_dir: Path, tasks_dir: Path, task_path: Path) -> Path:
    backup_path = backup_path_for(backups_dir, tasks_dir, task_path)
    backup_path.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(task_path, backup_path)
    return backup_path


def default_backups_dir(tasks_dir: Path) -> Path:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return tasks_dir.parent / "artifacts" / "task-format-backups" / timestamp


def migrate_tasks(
    tasks_dir: Path,
    *,
    apply: bool = False,
    backups_dir: Path | None = None,
    stdout=sys.stdout,
) -> MigrationResult:
    tasks_dir = tasks_dir.resolve()
    files = task_files(tasks_dir)
    selected_backups_dir = backups_dir or (default_backups_dir(tasks_dir) if apply else None)
    should_backup_all = apply and any(
        not has_frontmatter(path.read_text(encoding="utf-8")) for path in files
    )
    changed = 0
    print(
        f"{'apply' if apply else 'dry-run'}: scanning {len(files)} task files in {tasks_dir}",
        file=stdout,
    )

    if should_backup_all:
        assert selected_backups_dir is not None
        for task_path in files:
            create_backup(selected_backups_dir, tasks_dir, task_path)

    for task_path in files:
        _parse_or_legacy_issues(task_path)
        before = task_path.read_text(encoding="utf-8")
        if has_frontmatter(before):
            continue

        status = legacy_status(before)
        after = converted_content(before)
        changed += 1
        print(
            f"{task_path.name}: {status} -> {status}",
            file=stdout,
        )
        if apply:
            _atomic_write(task_path, after)

    if apply and changed:
        print(f"backups: {selected_backups_dir}", file=stdout)
    print(f"checked={len(files)} changed={changed}", file=stdout)
    return MigrationResult(len(files), changed, selected_backups_dir)


def comparable_header(header: TaskHeader, fallback_status: str | None) -> tuple:
    return (
        header.pr_id,
        header.title,
        header.branch,
        header.task_type,
        header.complexity,
        tuple(header.depends_on),
        header.priority,
        header.coder,
        (header.frontmatter_status or fallback_status or "todo").lower(),
        header.blocked_reason,
    )


def comparable_task_state(path: Path, fallback_status: str | None) -> tuple:
    parsed = _parse_or_legacy_issues(path)
    if isinstance(parsed, tuple):
        status = current_status(path.read_text(encoding="utf-8"), fallback_status)
        return ("legacy-issues", parsed, status.lower())
    return ("header", comparable_header(parsed, fallback_status))


def latest_backups_dir(tasks_dir: Path) -> Path | None:
    root = tasks_dir.parent / "artifacts" / "task-format-backups"
    if not root.is_dir():
        return None
    candidates = [path for path in root.iterdir() if path.is_dir()]
    return max(candidates, default=None)


def verify_tasks(
    tasks_dir: Path,
    *,
    backups_dir: Path | None = None,
    stdout=sys.stdout,
) -> MigrationResult:
    tasks_dir = tasks_dir.resolve()
    selected_backups_dir = backups_dir or latest_backups_dir(tasks_dir)
    if selected_backups_dir is None:
        raise RuntimeError("no backup directory found; pass --backup-dir")

    files = task_files(tasks_dir)
    mismatches: list[str] = []
    compared = 0
    print(f"verify: scanning {len(files)} task files in {tasks_dir}", file=stdout)

    for task_path in files:
        backup_path = backup_path_for(selected_backups_dir, tasks_dir, task_path)
        if not backup_path.exists():
            mismatches.append(f"{task_path.name}: missing backup")
            continue

        before_content = backup_path.read_text(encoding="utf-8")
        before = comparable_task_state(backup_path, legacy_status(before_content))
        after = comparable_task_state(task_path, None)
        compared += 1
        if before != after:
            mismatches.append(f"{task_path.name}: parsed header differs from backup")

    if mismatches:
        for mismatch in mismatches:
            print(mismatch, file=stdout)
        raise RuntimeError(
            f"verify failed: {len(mismatches)} mismatched task files; "
            f"first: {mismatches[0]}"
        )

    print(f"verify ok: compared={compared} checked={len(files)}", file=stdout)
    return MigrationResult(len(files), 0, selected_backups_dir)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Migrate tasks/PR-*.md files from legacy status headers to frontmatter."
    )
    parser.add_argument(
        "--repo",
        type=Path,
        default=Path.cwd(),
        help="Repository root or tasks directory to scan.",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Write migrated files. Default is dry-run.",
    )
    parser.add_argument(
        "--verify",
        action="store_true",
        help="Compare migrated files against apply-created backups.",
    )
    parser.add_argument(
        "--backup-dir",
        type=Path,
        default=None,
        help="Backup directory to write or verify against.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    tasks_dir = resolve_tasks_dir(args.repo)
    try:
        if args.verify:
            verify_tasks(tasks_dir, backups_dir=args.backup_dir)
        else:
            migrate_tasks(tasks_dir, apply=args.apply, backups_dir=args.backup_dir)
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
