"""PR-359: ``_mark_status_write_failed_task`` snapshot sync regression tests.

When a status commit to ``main`` is rejected (typically by branch
protection), the runner falls back to an in-memory ERROR marker via
``_mark_status_write_failed_task``. Without snapshot sync, the override
only lands at boot via ``_apply_recovery_decisions`` and operators see
no Retry button on the dashboard until docker restart. These tests pin
the inline sync to ``state.current_queue`` so the Retry button appears
on the next 5-second poll.
"""

from __future__ import annotations

import asyncio
from typing import Any

import pytest
from src.keyspace import status_write_failed_tasks
from src.models import QueueTask, TaskStatus

from tests.runner import _helpers as h


def _install_publish_state_spy(runner: Any) -> list[None]:
    calls: list[None] = []

    async def fake_publish() -> None:
        calls.append(None)

    runner.publish_state = fake_publish  # type: ignore[method-assign]
    return calls


def _queue_task(pr_id: str, status: TaskStatus = TaskStatus.TODO) -> QueueTask:
    return QueueTask(
        pr_id=pr_id,
        title=f"task {pr_id}",
        status=status,
        branch=f"{pr_id.lower()}-branch",
        task_file=f"tasks/{pr_id}.md",
    )


def test_marks_pr_id_in_memory_set() -> None:
    runner = h._make_runner()
    _install_publish_state_spy(runner)
    task = _queue_task("PR-330b")

    asyncio.run(runner._mark_status_write_failed_task(task))

    assert runner._status_write_failed_task_pr_ids == {"PR-330b"}


def test_persists_set_to_redis() -> None:
    runner = h._make_runner()
    _install_publish_state_spy(runner)
    task = _queue_task("PR-330b")

    asyncio.run(runner._mark_status_write_failed_task(task))

    assert runner.redis.store[status_write_failed_tasks(runner.name)] == (
        '["PR-330b"]'
    )


def test_logs_infra_warning() -> None:
    runner = h._make_runner()
    _install_publish_state_spy(runner)
    task = _queue_task("PR-330b")

    asyncio.run(runner._mark_status_write_failed_task(task))

    assert any(
        "using in-memory ERROR fallback for PR-330b" in entry["event"]
        for entry in runner.state.history
    )


def test_snapshot_with_no_matching_task_no_mutation() -> None:
    runner = h._make_runner()
    publish_calls = _install_publish_state_spy(runner)
    runner.state.current_queue = [_queue_task("PR-330a", TaskStatus.TODO)]
    snapshot_before = [t.model_copy() for t in runner.state.current_queue]

    asyncio.run(
        runner._mark_status_write_failed_task(_queue_task("PR-330b"))
    )

    assert runner.state.current_queue == snapshot_before
    assert len(publish_calls) == 1


def test_snapshot_with_matching_todo_task_flipped_to_error() -> None:
    runner = h._make_runner()
    publish_calls = _install_publish_state_spy(runner)
    runner.state.current_queue = [
        _queue_task("PR-330a", TaskStatus.TODO),
        _queue_task("PR-330b", TaskStatus.TODO),
    ]

    asyncio.run(
        runner._mark_status_write_failed_task(_queue_task("PR-330b"))
    )

    statuses = {t.pr_id: t.status for t in runner.state.current_queue}
    assert statuses == {
        "PR-330a": TaskStatus.TODO,
        "PR-330b": TaskStatus.ERROR,
    }
    assert len(publish_calls) == 1


def test_snapshot_with_matching_doing_task_flipped_to_error() -> None:
    runner = h._make_runner()
    publish_calls = _install_publish_state_spy(runner)
    runner.state.current_queue = [
        _queue_task("PR-330b", TaskStatus.DOING),
    ]

    asyncio.run(
        runner._mark_status_write_failed_task(_queue_task("PR-330b"))
    )

    assert runner.state.current_queue[0].status == TaskStatus.ERROR
    assert len(publish_calls) == 1


def test_snapshot_with_matching_done_task_discards_marker() -> None:
    runner = h._make_runner()
    publish_calls = _install_publish_state_spy(runner)
    done_task = _queue_task("PR-330b", TaskStatus.DONE)
    runner.state.current_queue = [done_task]

    asyncio.run(
        runner._mark_status_write_failed_task(_queue_task("PR-330b"))
    )

    assert "PR-330b" not in runner._status_write_failed_task_pr_ids
    assert runner.state.current_queue[0].status == TaskStatus.DONE
    assert status_write_failed_tasks(runner.name) not in runner.redis.store
    assert len(publish_calls) == 1


def test_snapshot_with_matching_error_task_no_redundant_write() -> None:
    runner = h._make_runner()
    publish_calls = _install_publish_state_spy(runner)
    error_task = _queue_task("PR-330b", TaskStatus.ERROR)
    runner.state.current_queue = [error_task]
    snapshot_before = [t.model_copy() for t in runner.state.current_queue]

    asyncio.run(
        runner._mark_status_write_failed_task(_queue_task("PR-330b"))
    )

    assert runner.state.current_queue == snapshot_before
    # Same object identity for the unchanged element confirms model_copy
    # was not executed on the no-op path.
    assert runner.state.current_queue[0] is error_task
    assert len(publish_calls) == 1


def test_empty_snapshot_skip_mutation() -> None:
    runner = h._make_runner()
    publish_calls = _install_publish_state_spy(runner)
    runner.state.current_queue = None

    asyncio.run(
        runner._mark_status_write_failed_task(_queue_task("PR-330b"))
    )

    assert runner.state.current_queue is None
    assert len(publish_calls) == 1


def test_current_queue_reassignment_restamps_snapshot_at() -> None:
    runner = h._make_runner()
    _install_publish_state_spy(runner)
    runner.state.current_queue = [_queue_task("PR-330b", TaskStatus.TODO)]
    prior_stamp = runner.state.current_queue_snapshot_at
    assert prior_stamp is not None

    asyncio.run(
        runner._mark_status_write_failed_task(_queue_task("PR-330b"))
    )

    assert runner.state.current_queue_snapshot_at is not None
    assert runner.state.current_queue_snapshot_at >= prior_stamp


def test_pr_id_missing_returns_silently() -> None:
    runner = h._make_runner()
    publish_calls = _install_publish_state_spy(runner)
    runner.state.current_queue = [_queue_task("PR-330a", TaskStatus.TODO)]

    asyncio.run(runner._mark_status_write_failed_task(object()))

    assert runner._status_write_failed_task_pr_ids == set()
    assert runner.state.history == []
    assert publish_calls == []


def test_persist_failure_blocks_snapshot_sync(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """If ``_persist_status_write_failed_task_pr_ids`` raises, the snapshot
    is NOT mutated. The persist call happens BEFORE snapshot sync so a
    raised exception aborts the method before reaching the snapshot
    loop. The in-memory marker is added before persist so it survives;
    Redis state loss is accepted in that scenario."""
    runner = h._make_runner()
    _install_publish_state_spy(runner)
    runner.state.current_queue = [_queue_task("PR-330b", TaskStatus.TODO)]
    snapshot_before = [t.model_copy() for t in runner.state.current_queue]

    async def raising_persist() -> None:
        raise RuntimeError("redis down")

    monkeypatch.setattr(
        runner,
        "_persist_status_write_failed_task_pr_ids",
        raising_persist,
    )

    with pytest.raises(RuntimeError, match="redis down"):
        asyncio.run(
            runner._mark_status_write_failed_task(_queue_task("PR-330b"))
        )

    # Marker is added to the in-memory set BEFORE persist, so it stays.
    assert runner._status_write_failed_task_pr_ids == {"PR-330b"}
    # Snapshot mutation lives AFTER persist, so it is skipped.
    assert runner.state.current_queue == snapshot_before
