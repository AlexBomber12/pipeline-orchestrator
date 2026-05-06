"""Tests for the ``_select_next_task_or_attach`` dispatch helper.

PR-267 removed the legacy ``QUEUE.md`` parse fallback and the
``_check_legacy_queue_status`` regex screen — the snapshot is the
only source of truth now. The remaining cases verify the dispatch
decision tree returning the next ``QueueTask`` to drive into CODING,
or ``None`` when the handler must return without dispatching.
"""

from __future__ import annotations

import asyncio
from pathlib import Path

import pytest
from src.daemon.handlers import idle as idle_module
from src.models import QueueTask, TaskStatus

from tests.runner import _helpers as h


def test_select_next_task_or_attach_picks_dag_task(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """DAG selection produces the dispatched task."""
    h._patch_subprocess(monkeypatch)

    dag_task = QueueTask(
        pr_id="PR-300",
        title="DAG-headed task",
        status=TaskStatus.TODO,
        task_file="tasks/PR-300.md",
        branch="pr-300-dag",
    )

    async def fake_dag_select(self) -> QueueTask | None:
        self._idle_dag_tasks = [dag_task]
        self._idle_dag_statuses = {dag_task.pr_id: TaskStatus.TODO}
        return dag_task

    monkeypatch.setattr(
        idle_module.IdleMixin,
        "_select_next_task_from_dag",
        fake_dag_select,
    )

    runner = h._make_runner()
    runner.repo_path = str(tmp_path)

    selected = asyncio.run(runner._select_next_task_or_attach([], []))

    assert selected is not None
    assert selected.pr_id == "PR-300"
    # DAG task in TODO is marked DOING in the snapshot before dispatch.
    assert selected.status == TaskStatus.DOING
    assert runner.state.current_task == selected


def test_select_next_task_or_attach_returns_none_when_nothing_actionable(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """No DAG task and no PRs => returns None and clears current_pr."""
    h._patch_subprocess(monkeypatch)

    async def fake_dag_select(self) -> QueueTask | None:
        self._idle_dag_tasks = None
        self._idle_dag_statuses = None
        return None

    monkeypatch.setattr(
        idle_module.IdleMixin,
        "_select_next_task_from_dag",
        fake_dag_select,
    )

    runner = h._make_runner()
    runner.repo_path = str(tmp_path)

    selected = asyncio.run(runner._select_next_task_or_attach([], []))

    assert selected is None
    assert runner.state.current_pr is None


def test_select_next_task_or_attach_no_disk_write(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """A full IDLE selection cycle never writes ``tasks/QUEUE.md``."""
    h._patch_subprocess(monkeypatch)
    tasks_dir = tmp_path / "tasks"
    tasks_dir.mkdir()

    dag_task = QueueTask(
        pr_id="PR-300",
        title="DAG-headed task",
        status=TaskStatus.TODO,
        task_file="tasks/PR-300.md",
        branch="pr-300-dag",
    )

    async def fake_dag_select(self) -> QueueTask | None:
        self._idle_dag_tasks = [dag_task]
        self._idle_dag_statuses = {dag_task.pr_id: TaskStatus.TODO}
        return dag_task

    monkeypatch.setattr(
        idle_module.IdleMixin,
        "_select_next_task_from_dag",
        fake_dag_select,
    )

    runner = h._make_runner()
    runner.repo_path = str(tmp_path)

    asyncio.run(runner._select_next_task_or_attach([], []))

    assert not (tasks_dir / "QUEUE.md").exists()
