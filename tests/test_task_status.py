from __future__ import annotations

import subprocess
from pathlib import Path

from src.models import QueueTask
from src.models import TaskStatus
from src.queue_parser import TaskHeader
from src.task_status import (
    _load_task_header,
    derive_task_status,
    derive_queue_task_statuses,
    get_merged_pr_ids,
)


def _header(branch: str, pr_id: str = "PR-085") -> TaskHeader:
    return TaskHeader(
        pr_id=pr_id,
        title="Status derivation from git",
        branch=branch,
        task_type="feature",
        complexity="medium",
        depends_on=[],
        priority=2,
        coder="any",
    )


def test_derive_done_when_pr_id_is_in_merged_history() -> None:
    status = derive_task_status(
        _header("pr-085-status-from-git"),
        {"PR-085"},
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


def test_get_merged_pr_ids(monkeypatch) -> None:
    def fake_run(*args, **kwargs) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(
            args=args[0],
            returncode=0,
            stdout=(
                "PR-084: add task file header parser (#108)\n"
                "Merge pull request #97 from AlexBomber12/micro-20260418-rate-limit-on-failure\n"
                "PR-085: Status derivation from git (#109)\n"
            ),
            stderr="",
        )

    monkeypatch.setattr("src.task_status.subprocess.run", fake_run)

    assert get_merged_pr_ids("/repo", "main") == {
        "PR-084",
        "PR-085",
    }


def test_get_merged_pr_ids_accepts_full_queue_pr_id_grammar(
    monkeypatch,
) -> None:
    def fake_run(*args, **kwargs) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(
            args=args[0],
            returncode=0,
            stdout=(
                "PR-abc_1.2: queue parser parity\n"
                "Merge pull request #98 from AlexBomber12/pr-feature-x\n"
            ),
            stderr="",
        )

    monkeypatch.setattr("src.task_status.subprocess.run", fake_run)

    assert get_merged_pr_ids("/repo", "main") == {"PR-abc_1.2"}


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


def test_derive_queue_task_statuses_does_not_trust_stale_done_queue_status(
    monkeypatch,
) -> None:
    task = QueueTask(
        pr_id="PR-001",
        title="Queued task",
        status=TaskStatus.DONE,
        branch="pr-001-queued-task",
    )

    monkeypatch.setattr(
        "src.task_status.get_merged_pr_ids",
        lambda repo_path, base_branch: set(),
    )
    monkeypatch.setattr(
        "src.task_status._load_task_header",
        lambda current_task, repo_path: _header(
            "pr-001-queued-task",
            pr_id="PR-001",
        ),
    )

    derived = derive_queue_task_statuses(
        [task],
        "/repo",
        "main",
        set(),
    )

    assert derived[0].status == TaskStatus.TODO


def test_derive_queue_task_statuses_marks_done_from_merged_pr_history(
    monkeypatch,
) -> None:
    task = QueueTask(
        pr_id="PR-001",
        title="Completed task",
        status=TaskStatus.TODO,
        branch="pr-001-completed",
    )

    monkeypatch.setattr(
        "src.task_status.get_merged_pr_ids",
        lambda repo_path, base_branch: {"PR-001"},
    )
    monkeypatch.setattr(
        "src.task_status._load_task_header",
        lambda current_task, repo_path: _header(
            "pr-001-deleted-branch",
            pr_id="PR-001",
        ),
    )

    derived = derive_queue_task_statuses(
        [task],
        "/repo",
        "main",
        set(),
    )

    assert derived[0].status == TaskStatus.DONE
