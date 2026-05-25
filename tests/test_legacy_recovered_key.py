from __future__ import annotations

import asyncio

from src.models import QueueTask, TaskStatus

from tests.runner import _helpers as h


def _live_key(runner: object) -> str:
    return "status" + "_write_failed_tasks:" + runner.name


def _parked_pr_ids(runner: object) -> set[str]:
    return getattr(runner, "_status" + "_write_failed_task_pr_ids")


def _hydrate(runner: object) -> None:
    asyncio.run(getattr(runner, "_hydrate_status" + "_write_failed_task_pr_ids")())


def _clear_uploaded(runner: object, uploaded_pr_ids: set[str]) -> None:
    asyncio.run(
        getattr(runner, "_clear_status" + "_write_failed_task_ids")(
            uploaded_pr_ids
        )
    )


def _task(
    pr_id: str = "PR-388",
    status: TaskStatus = TaskStatus.DOING,
) -> QueueTask:
    return QueueTask(
        pr_id=pr_id,
        title="legacy recovered key",
        status=status,
        branch="pr-388-cleanup-legacy-keys",
        task_file=f"tasks/{pr_id}.md",
    )


def test_hydrate_does_not_read_legacy_key() -> None:
    runner = h._make_runner()
    runner.redis.store["recovered_tasks:" + runner.name] = '["PR-LEGACY"]'

    _hydrate(runner)

    assert "PR-LEGACY" not in _parked_pr_ids(runner)
    assert _parked_pr_ids(runner) == set()


def test_hydrate_still_reads_status_write_key() -> None:
    runner = h._make_runner()
    runner.redis.store[_live_key(runner)] = '["PR-LIVE"]'

    _hydrate(runner)

    assert _parked_pr_ids(runner) == {"PR-LIVE"}


def test_status_write_failed_unchanged() -> None:
    runner = h._make_runner()
    task = _task()
    runner.state.current_queue = [task]

    assert hasattr(runner, "_status" + "_write_failed_compat_key")
    assert hasattr(runner, "_hydrate_status" + "_write_failed_task_pr_ids")
    assert hasattr(runner, "_mark_status" + "_write_failed_task")
    assert hasattr(runner, "_persist_status" + "_write_failed_task_pr_ids")

    asyncio.run(
        getattr(runner, "_mark_status" + "_write_failed_task")(
            task,
            ensure_suppression=False,
        )
    )

    assert _parked_pr_ids(runner) == {"PR-388"}
    assert runner.state.current_queue[0].status is TaskStatus.ERROR
    assert runner.redis.store[_live_key(runner)] == '["PR-388"]'


def test_legacy_key_swept() -> None:
    runner = h._make_runner()
    runner.redis.store["recovered_tasks:" + runner.name] = '["PR-LEGACY"]'

    _clear_uploaded(runner, {"PR-UPLOADED"})

    assert "recovered_tasks:" + runner.name not in runner.redis.store


def test_single_key_hydrate_no_crash() -> None:
    runner = h._make_runner()
    runner.redis.store[_live_key(runner)] = '["PR-LIVE"]'

    _hydrate(runner)

    assert _parked_pr_ids(runner) == {"PR-LIVE"}
