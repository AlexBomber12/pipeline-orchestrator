from __future__ import annotations

import subprocess
from pathlib import Path

from src.models import QueueTask
from src.models import TaskStatus
from src.queue_parser import TaskHeader
from src.task_status import (
    _load_task_header,
    derive_task_status,
    get_merged_branches,
)


def _header(branch: str) -> TaskHeader:
    return TaskHeader(
        pr_id="PR-085",
        title="Status derivation from git",
        branch=branch,
        task_type="feature",
        complexity="medium",
        depends_on=[],
        priority=2,
        coder="any",
    )


def test_derive_done_when_branch_merged() -> None:
    status = derive_task_status(
        _header("pr-085-status-from-git"),
        {"pr-085-status-from-git"},
        set(),
    )

    assert status == TaskStatus.DONE


def test_derive_doing_when_open_pr() -> None:
    status = derive_task_status(
        _header("pr-085-status-from-git"),
        set(),
        {"pr-085-status-from-git"},
    )

    assert status == TaskStatus.DOING


def test_derive_todo_when_neither() -> None:
    status = derive_task_status(
        _header("pr-085-status-from-git"),
        set(),
        set(),
    )

    assert status == TaskStatus.TODO


def test_get_merged_branches(monkeypatch) -> None:
    def fake_run(*args, **kwargs) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(
            args=args[0],
            returncode=0,
            stdout=(
                "  origin/HEAD -> origin/main\n"
                "  origin/main\n"
                "  origin/pr-084-task-header-parser\n"
                "  origin/pr-085-status-from-git\n"
            ),
            stderr="",
        )

    monkeypatch.setattr("src.task_status.subprocess.run", fake_run)

    assert get_merged_branches("/repo", "main") == {
        "pr-084-task-header-parser",
        "pr-085-status-from-git",
    }


def test_load_task_header_falls_back_for_legacy_task_files(
    tmp_path: Path,
) -> None:
    task_file = tmp_path / "tasks" / "PR-001.md"
    task_file.parent.mkdir()
    task_file.write_text(
        "# PR-001: Legacy task\n\n"
        "Branch: pr-001-legacy-task\n",
        encoding="utf-8",
    )
    task = QueueTask(
        pr_id="PR-001",
        title="Legacy task",
        status=TaskStatus.TODO,
        task_file="tasks/PR-001.md",
        depends_on=["PR-000"],
        branch="pr-001-legacy-task",
    )

    header = _load_task_header(task, str(tmp_path))

    assert header == TaskHeader(
        pr_id="PR-001",
        title="Legacy task",
        branch="pr-001-legacy-task",
        task_type="feature",
        complexity="medium",
        depends_on=["PR-000"],
        priority=3,
        coder="any",
    )
