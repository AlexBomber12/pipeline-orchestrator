from __future__ import annotations

import pytest

from src.models import QueueTask, TaskStatus
from src.web.routes import repo_control


def _task(pr_id: str, status: TaskStatus) -> QueueTask:
    return QueueTask(pr_id=pr_id, title=pr_id, status=status)


async def _panel_context(
    monkeypatch: pytest.MonkeyPatch,
    tasks: list[QueueTask],
    merged_at_by_pr_id: dict[str, str | None],
) -> dict[str, object]:
    async def fake_task_view(
        task: QueueTask,
        repo_name: str,
        redis_client: object | None,
    ) -> dict[str, object]:
        view: dict[str, object] = task.model_dump(mode="json")
        if task.pr_id in merged_at_by_pr_id:
            view["merged_at"] = merged_at_by_pr_id[task.pr_id]
        return view

    monkeypatch.setattr(repo_control, "_task_view", fake_task_view)
    return await repo_control._build_tasks_panel_context(
        "example__alpha",
        tasks,
        redis_client=None,
        retry_cap=3,
    )


def _status_order(context: dict[str, object], status: str) -> list[str]:
    tasks_by_status = context["tasks_by_status"]
    assert isinstance(tasks_by_status, dict)
    views = tasks_by_status[status]
    assert isinstance(views, list)
    return [view["pr_id"] for view in views if isinstance(view, dict)]


@pytest.mark.asyncio
async def test_done_list_sorted_newest_first(monkeypatch: pytest.MonkeyPatch) -> None:
    tasks = [
        _task("PR-001", TaskStatus.DONE),
        _task("PR-002", TaskStatus.DONE),
        _task("PR-003", TaskStatus.DONE),
    ]
    context = await _panel_context(
        monkeypatch,
        tasks,
        {
            "PR-001": "2026-05-01T10:00:00+00:00",
            "PR-002": "2026-05-03T10:00:00+00:00",
            "PR-003": "2026-05-02T10:00:00+00:00",
        },
    )

    assert _status_order(context, "done") == ["PR-002", "PR-003", "PR-001"]


@pytest.mark.asyncio
async def test_done_list_with_missing_merged_at_falls_back(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tasks = [
        _task("PR-001", TaskStatus.DONE),
        _task("PR-002", TaskStatus.DONE),
    ]
    context = await _panel_context(
        monkeypatch,
        tasks,
        {"PR-001": "2026-05-01T10:00:00+00:00"},
    )

    assert _status_order(context, "done") == ["PR-001", "PR-002"]


@pytest.mark.asyncio
async def test_todo_list_order_unchanged(monkeypatch: pytest.MonkeyPatch) -> None:
    tasks = [
        _task("PR-001", TaskStatus.TODO),
        _task("PR-002", TaskStatus.TODO),
        _task("PR-003", TaskStatus.TODO),
    ]
    context = await _panel_context(
        monkeypatch,
        tasks,
        {
            "PR-001": "2026-05-01T10:00:00+00:00",
            "PR-002": "2026-05-03T10:00:00+00:00",
            "PR-003": "2026-05-02T10:00:00+00:00",
        },
    )

    assert _status_order(context, "todo") == ["PR-001", "PR-002", "PR-003"]


@pytest.mark.asyncio
async def test_doing_list_order_unchanged(monkeypatch: pytest.MonkeyPatch) -> None:
    tasks = [
        _task("PR-001", TaskStatus.DOING),
        _task("PR-002", TaskStatus.DOING),
        _task("PR-003", TaskStatus.DOING),
    ]
    context = await _panel_context(
        monkeypatch,
        tasks,
        {
            "PR-001": "2026-05-01T10:00:00+00:00",
            "PR-002": "2026-05-03T10:00:00+00:00",
            "PR-003": "2026-05-02T10:00:00+00:00",
        },
    )

    assert _status_order(context, "doing") == ["PR-001", "PR-002", "PR-003"]


@pytest.mark.asyncio
async def test_error_list_order_unchanged(monkeypatch: pytest.MonkeyPatch) -> None:
    tasks = [
        _task("PR-001", TaskStatus.ERROR),
        _task("PR-002", TaskStatus.ERROR),
        _task("PR-003", TaskStatus.ERROR),
    ]
    context = await _panel_context(
        monkeypatch,
        tasks,
        {
            "PR-001": "2026-05-01T10:00:00+00:00",
            "PR-002": "2026-05-03T10:00:00+00:00",
            "PR-003": "2026-05-02T10:00:00+00:00",
        },
    )

    assert _status_order(context, "error") == ["PR-001", "PR-002", "PR-003"]


@pytest.mark.asyncio
async def test_empty_done_list(monkeypatch: pytest.MonkeyPatch) -> None:
    context = await _panel_context(
        monkeypatch,
        [_task("PR-001", TaskStatus.TODO)],
        {},
    )

    assert _status_order(context, "done") == []
