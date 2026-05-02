"""PR-229a: tests for handle_idle decomposition helpers.

Verifies the two helpers extracted from ``handle_idle``:

- ``_check_legacy_queue_status``: regex screen for visible legacy
  ``## PR-*`` entries plus parse-error echo.
- ``_select_next_task_or_attach``: dispatch decision tree returning
  the next ``QueueTask`` to drive into CODING, or ``None`` when the
  handler must return without dispatching.
"""

from __future__ import annotations

import asyncio
from pathlib import Path

import pytest
from src.daemon.handlers import idle as idle_module
from src.models import QueueTask, TaskStatus

from tests.runner import _helpers as h


def _write_queue(tmp_path: Path, body: str) -> Path:
    tasks_dir = tmp_path / "tasks"
    tasks_dir.mkdir(exist_ok=True)
    queue_md = tasks_dir / "QUEUE.md"
    queue_md.write_text(body, encoding="utf-8")
    return queue_md


def test_check_legacy_queue_status_visible_legacy(tmp_path: Path) -> None:
    _write_queue(
        tmp_path,
        "## PR-999: Stray legacy task\n"
        "- Status: TODO\n"
        "- Tasks file: tasks/PR-999.md\n"
        "- Branch: pr-999-legacy\n",
    )
    runner = h._make_runner()
    runner.repo_path = str(tmp_path)
    runner._idle_dag_tasks = []  # no structured PRs => PR-999 is "legacy"

    visible, parse_error = runner._check_legacy_queue_status()

    assert visible is True
    assert parse_error is None


def test_check_legacy_queue_status_parse_failure(tmp_path: Path) -> None:
    # No visible legacy rows in QUEUE.md, but the caller flags a parse
    # failure via ``parse_error`` — the helper echoes it back since
    # there is nothing legacy to override the failure signal.
    _write_queue(tmp_path, "# Task Queue\n")
    runner = h._make_runner()
    runner.repo_path = str(tmp_path)
    runner._idle_dag_tasks = []

    visible, parse_error = runner._check_legacy_queue_status(
        parse_error="QUEUE.md malformed: bad header at line 3",
    )

    assert visible is False
    assert parse_error == "QUEUE.md malformed: bad header at line 3"


def test_check_legacy_queue_status_no_legacy_entries(tmp_path: Path) -> None:
    structured = QueueTask(
        pr_id="PR-100",
        title="Structured task",
        status=TaskStatus.TODO,
        task_file="tasks/PR-100.md",
        branch="pr-100-structured",
    )
    _write_queue(
        tmp_path,
        "## PR-100: Structured task\n"
        "- Status: TODO\n"
        "- Tasks file: tasks/PR-100.md\n"
        "- Branch: pr-100-structured\n",
    )
    runner = h._make_runner()
    runner.repo_path = str(tmp_path)
    # PR-100 is in the structured set, so it is NOT a legacy entry.
    runner._idle_dag_tasks = [structured]

    visible, parse_error = runner._check_legacy_queue_status()

    assert visible is False
    assert parse_error is None


def test_select_next_task_or_attach_picks_dag_task(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """DAG selection wins over legacy queue when a DAG task is available."""
    h._patch_subprocess(monkeypatch)
    _write_queue(tmp_path, "# Task Queue\n")

    dag_task = QueueTask(
        pr_id="PR-300",
        title="DAG-headed task",
        status=TaskStatus.TODO,
        task_file="tasks/PR-300.md",
        branch="pr-300-dag",
    )

    async def fake_dag_select(self) -> QueueTask | None:
        self._idle_dag_tasks = [dag_task]
        self._idle_dag_headers = []
        self._idle_dag_statuses = {dag_task.pr_id: TaskStatus.TODO}
        return dag_task

    monkeypatch.setattr(
        idle_module.IdleMixin,
        "_select_next_task_from_dag",
        fake_dag_select,
    )
    monkeypatch.setattr(idle_module, "parse_queue", lambda path, **kw: [])
    monkeypatch.setattr(
        idle_module,
        "derive_queue_task_statuses",
        lambda *args, **kwargs: [],
    )
    monkeypatch.setattr(idle_module, "get_next_task", lambda tasks: None)

    runner = h._make_runner()
    runner.repo_path = str(tmp_path)

    selected = asyncio.run(runner._select_next_task_or_attach([], []))

    assert selected is not None
    assert selected.pr_id == "PR-300"
    # DAG task in TODO is marked DOING for the shim before write.
    assert selected.status == TaskStatus.DOING
    assert runner.state.current_task == selected


def test_select_next_task_or_attach_falls_back_to_legacy(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """When no DAG task is available, legacy queue_task is selected."""
    h._patch_subprocess(monkeypatch)
    _write_queue(tmp_path, "# Task Queue\n")

    legacy_task = QueueTask(
        pr_id="PR-400",
        title="Legacy queue task",
        status=TaskStatus.TODO,
        task_file="tasks/PR-400.md",
        branch="pr-400-legacy",
    )
    (tmp_path / "tasks" / "PR-400.md").write_text("legacy body\n", encoding="utf-8")

    async def fake_dag_select(self) -> QueueTask | None:
        # No DAG selection; clear all DAG state.
        self._idle_dag_tasks = None
        self._idle_dag_headers = None
        self._idle_dag_statuses = None
        return None

    monkeypatch.setattr(
        idle_module.IdleMixin,
        "_select_next_task_from_dag",
        fake_dag_select,
    )
    monkeypatch.setattr(idle_module, "parse_queue", lambda path, **kw: [legacy_task])
    monkeypatch.setattr(
        idle_module,
        "derive_queue_task_statuses",
        lambda *args, **kwargs: [legacy_task],
    )
    monkeypatch.setattr(idle_module, "get_next_task", lambda tasks: legacy_task)

    runner = h._make_runner()
    runner.repo_path = str(tmp_path)

    selected = asyncio.run(runner._select_next_task_or_attach([], []))

    assert selected is not None
    assert selected.pr_id == "PR-400"
    assert runner.state.current_task == selected


def test_select_next_task_or_attach_returns_none_when_nothing_actionable(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """No DAG task, no legacy task, no PRs => returns None."""
    h._patch_subprocess(monkeypatch)
    _write_queue(tmp_path, "# Task Queue\n")

    async def fake_dag_select(self) -> QueueTask | None:
        self._idle_dag_tasks = None
        self._idle_dag_headers = None
        self._idle_dag_statuses = None
        return None

    monkeypatch.setattr(
        idle_module.IdleMixin,
        "_select_next_task_from_dag",
        fake_dag_select,
    )
    monkeypatch.setattr(idle_module, "parse_queue", lambda path, **kw: [])
    monkeypatch.setattr(
        idle_module,
        "derive_queue_task_statuses",
        lambda *args, **kwargs: [],
    )
    monkeypatch.setattr(idle_module, "get_next_task", lambda tasks: None)

    runner = h._make_runner()
    runner.repo_path = str(tmp_path)

    selected = asyncio.run(runner._select_next_task_or_attach([], []))

    assert selected is None
    # No PRs => current_pr is cleared (manual-work attach branch is skipped).
    assert runner.state.current_pr is None
