from __future__ import annotations

import subprocess

from src.models import TaskStatus
from src.queue_parser import TaskHeader
from src.task_status import derive_task_status, get_merged_branches


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
