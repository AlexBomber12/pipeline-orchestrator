from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
from src.daemon import recovery as recovery_module
from src.models import PRInfo, QueueTask, TaskStatus
from src.queue_parser import QueueValidationError
from src.task_status import MergedState

from tests.runner import _helpers as h


def _write_task_file(task_dir: Path, task: dict[str, Any]) -> None:
    path = task_dir / f"{task['pr_id']}.md"
    if task.get("legacy_unstructured"):
        path.write_text(f"# {task['pr_id']}: {task['title']}\n\nLegacy body\n")
        return
    depends_on = task.get("depends_on", [])
    depends_value = ", ".join(depends_on) if depends_on else "none"
    frontmatter = []
    if task.get("frontmatter_status"):
        frontmatter = [
            "---",
            f"status: {task['frontmatter_status']}",
            "---",
            "",
        ]
    path.write_text(
        "\n".join(
            frontmatter
            + [
                f"# {task['pr_id']}: {task['title']}",
                "",
                f"Branch: {task['branch']}",
                f"- Type: {task.get('type', 'refactor')}",
                f"- Complexity: {task.get('complexity', 'low')}",
                f"- Depends on: {depends_value}",
                f"- Priority: {task.get('priority', 3)}",
                f"- Coder: {task.get('coder', 'codex')}",
                "",
                "## Problem",
                "Fixture task body.",
                "",
            ]
        ),
        encoding="utf-8",
    )


def _populate_tasks(repo: Path, tasks: list[dict[str, Any]]) -> None:
    task_dir = repo / "tasks"
    task_dir.mkdir(parents=True, exist_ok=True)
    for task in tasks:
        _write_task_file(task_dir, task)


def _task_projection(task: QueueTask) -> dict[str, Any]:
    return {
        "pr_id": task.pr_id,
        "title": task.title,
        "status": task.status.value,
        "task_file": task.task_file,
        "depends_on": task.depends_on,
        "branch": task.branch,
        "priority": task.priority,
    }


def _runner_for_fixture(
    tmp_path: Path,
    before: dict[str, Any],
    monkeypatch: pytest.MonkeyPatch,
):
    runner = h._make_runner()
    repo = tmp_path / "repo"
    repo.mkdir(parents=True)
    runner.repo_path = str(repo)
    _populate_tasks(repo, before.get("tasks", []))
    runner._idle_open_prs = [
        PRInfo(**pr) for pr in before.get("open_prs", [])
    ]
    runner._crashed_task_pr_ids = set(before.get("crashed_task_pr_ids", []))
    if before.get("current_task_pr_id"):
        runner.state.current_task = QueueTask(
            pr_id=before["current_task_pr_id"],
            title="current",
            status=TaskStatus.DOING,
        )

    def fake_resolve(
        repo_path: str,
        base_branch: str,
        owner_repo: str,
        candidate_pr_ids,
        headers,
        *,
        log_event,
    ) -> MergedState:
        return MergedState(
            set(before.get("merged_pr_ids_via_git_log", [])),
            set(before.get("merged_branches_via_api", [])),
            True,
        )

    monkeypatch.setattr(recovery_module, "_resolve_merged_state", fake_resolve)
    return runner, repo


def test_helper_returns_none_on_missing_tasks_dir(tmp_path: Path) -> None:
    runner = h._make_runner()
    repo = tmp_path / "repo"
    repo.mkdir()
    runner.repo_path = str(repo)

    assert runner._parse_tasks_from_headers() is None


def test_helper_returns_empty_when_no_pr_files(tmp_path: Path) -> None:
    runner = h._make_runner()
    repo = tmp_path / "repo"
    (repo / "tasks").mkdir(parents=True)
    runner.repo_path = str(repo)

    assert runner._parse_tasks_from_headers() is None


def test_helper_skips_legacy_unstructured(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    before = {
        "tasks": [
            {
                "pr_id": "PR-001",
                "title": "Valid one",
                "branch": "pr-001-valid",
                "priority": 2,
            },
            {
                "pr_id": "PR-002",
                "title": "Legacy",
                "branch": "pr-002-legacy",
                "legacy_unstructured": True,
            },
            {
                "pr_id": "PR-003",
                "title": "Valid two",
                "branch": "pr-003-valid",
                "priority": 3,
            },
            {
                "pr_id": "PR-004",
                "title": "Valid three",
                "branch": "pr-004-valid",
                "priority": 4,
            },
        ]
    }
    runner, _repo = _runner_for_fixture(tmp_path, before, monkeypatch)

    tasks = runner._parse_tasks_from_headers()

    assert tasks is not None
    assert [task.pr_id for task in tasks] == ["PR-001", "PR-003", "PR-004"]


def test_helper_applies_merged_state_via_resolve(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    before = {
        "tasks": [
            {
                "pr_id": "PR-005",
                "title": "Merged by git",
                "branch": "pr-005-merged",
                "priority": 3,
            }
        ],
        "merged_pr_ids_via_git_log": ["PR-005"],
    }
    runner, _repo = _runner_for_fixture(tmp_path, before, monkeypatch)

    tasks = runner._parse_tasks_from_headers()

    assert tasks is not None
    assert tasks[0].status == TaskStatus.DONE


def test_helper_applies_crashed_set_override(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    before = {
        "tasks": [
            {
                "pr_id": "PR-007",
                "title": "Crashed",
                "branch": "pr-007-crashed",
                "priority": 3,
            }
        ],
        "crashed_task_pr_ids": ["PR-007"],
    }
    runner, _repo = _runner_for_fixture(tmp_path, before, monkeypatch)

    tasks = runner._parse_tasks_from_headers()

    assert tasks is not None
    assert tasks[0].status == TaskStatus.ERROR


def test_helper_orders_by_priority_then_pr_id(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    before = {
        "tasks": [
            {
                "pr_id": "PR-010",
                "title": "Later number",
                "branch": "pr-010",
                "priority": 2,
            },
            {
                "pr_id": "PR-ABC",
                "title": "Non numeric suffix",
                "branch": "pr-abc",
                "priority": 2,
            },
            {
                "pr_id": "PR-002",
                "title": "Earlier number",
                "branch": "pr-002",
                "priority": 2,
            },
            {
                "pr_id": "PR-001",
                "title": "Higher priority",
                "branch": "pr-001",
                "priority": 1,
            },
        ]
    }
    runner, _repo = _runner_for_fixture(tmp_path, before, monkeypatch)

    tasks = runner._parse_tasks_from_headers()

    assert tasks is not None
    assert [task.pr_id for task in tasks] == [
        "PR-001",
        "PR-002",
        "PR-010",
        "PR-ABC",
    ]


def test_helper_raises_non_legacy_header_errors(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    before = {
        "tasks": [
            {
                "pr_id": "PR-011",
                "title": "Invalid coder",
                "branch": "pr-011-invalid-coder",
                "coder": "invalid",
            }
        ]
    }
    runner, _repo = _runner_for_fixture(tmp_path, before, monkeypatch)

    with pytest.raises(QueueValidationError, match="invalid Coder"):
        runner._parse_tasks_from_headers()


def test_helper_raises_header_file_mismatch(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = h._make_runner()
    repo = tmp_path / "repo"
    task_dir = repo / "tasks"
    task_dir.mkdir(parents=True)
    runner.repo_path = str(repo)
    _write_task_file(
        task_dir,
        {
            "pr_id": "PR-013",
            "title": "Wrong file",
            "branch": "pr-013-wrong-file",
        },
    )
    (task_dir / "PR-012.md").write_text(
        (task_dir / "PR-013.md").read_text(encoding="utf-8"),
        encoding="utf-8",
    )
    (task_dir / "PR-013.md").unlink()

    monkeypatch.setattr(
        recovery_module,
        "_resolve_merged_state",
        lambda *args, **kwargs: MergedState(set(), set(), True),
    )

    with pytest.raises(QueueValidationError, match="does not match task file"):
        runner._parse_tasks_from_headers()


def test_helper_ignores_stopped_current_task(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    before = {
        "tasks": [
            {
                "pr_id": "PR-014",
                "title": "Stopped current task",
                "branch": "pr-014-stopped",
            }
        ],
        "current_task_pr_id": "PR-014",
    }
    runner, _repo = _runner_for_fixture(tmp_path, before, monkeypatch)
    runner._user_stopped_task_pr_ids = {"PR-014"}

    tasks = runner._parse_tasks_from_headers()

    assert tasks is not None
    assert tasks[0].status == TaskStatus.TODO


def test_helper_against_each_golden_fixture(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    recovery_golden_cases: list[tuple[str, dict, dict]],
) -> None:
    for scenario_name, before, expected in recovery_golden_cases:
        runner, _repo = _runner_for_fixture(
            tmp_path / scenario_name,
            before,
            monkeypatch,
        )

        tasks = runner._parse_tasks_from_headers()
        current_queue = [] if tasks is None else [_task_projection(task) for task in tasks]

        assert current_queue == expected["current_queue"], scenario_name
