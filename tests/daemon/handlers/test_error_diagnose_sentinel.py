"""Regression tests for diagnose_error exhaustion sentinels."""

from __future__ import annotations

import asyncio
from datetime import datetime
from typing import Any

import pytest
from src.config import FeatureFlags
from src.models import PipelineState, QueueTask, TaskStatus
from tests.runner import _helpers as h

_TTL_SECONDS = 7 * 24 * 3600


def _sentinel_key(runner: Any, task_id: str) -> str:
    return f"diagnose_exhausted:{runner.name}:{task_id}"


def _task(task_id: str) -> QueueTask:
    return QueueTask(pr_id=task_id, title=task_id, status=TaskStatus.DOING)


def _make_error_runner(task_id: str | None = "PR-001") -> Any:
    runner = h._make_runner()
    runner.state.state = PipelineState.ERROR
    runner.state.error_message = "Build failed after tests"
    runner.state.current_task = _task(task_id) if task_id is not None else None
    return runner


def _patch_cli_failure(monkeypatch: pytest.MonkeyPatch, runner: Any) -> None:
    async def _diagnose(*args: object, **kwargs: object) -> tuple[int, str, str]:
        return 1, "", "boom"

    monkeypatch.setattr(runner._registry.get("claude"), "diagnose_error", _diagnose)
    monkeypatch.setattr(runner._registry.get("codex"), "diagnose_error", _diagnose)


def test_first_threshold_sets_sentinel() -> None:
    runner = _make_error_runner()
    runner._error_diagnose_count = 3

    asyncio.run(runner.handle_error())

    key = _sentinel_key(runner, "PR-001")
    value = runner.redis.store[key]
    datetime.fromisoformat(value)
    assert runner.redis.ttls[key] == _TTL_SECONDS


def test_subsequent_cycles_skip_handler_when_sentinel_set() -> None:
    runner = _make_error_runner()
    runner._error_diagnose_count = 4
    key = _sentinel_key(runner, "PR-001")
    runner.redis.store[key] = "2026-05-20T00:00:00+00:00"
    before_history = list(runner.state.history)

    asyncio.run(runner.handle_error())

    assert runner._error_diagnose_count == 4
    assert runner.state.history == before_history
    assert not any(
        "[ERROR] diagnose_error: max attempts" in item["event"]
        for item in runner.state.history
    )


def test_no_sentinel_no_task_id() -> None:
    runner = _make_error_runner(task_id=None)
    runner._error_diagnose_count = 3

    asyncio.run(runner.handle_error())

    assert not [
        key for key, _value in runner.redis.writes if key.startswith("diagnose_exhausted:")
    ]


def test_sentinel_ttl_is_7_days() -> None:
    runner = _make_error_runner()
    key = _sentinel_key(runner, "PR-001")

    asyncio.run(runner._mark_diagnose_exhausted("PR-001"))

    ttl = asyncio.run(runner.redis.ttl(key))
    assert _TTL_SECONDS - 5 < ttl <= _TTL_SECONDS


def test_sentinel_does_not_block_other_tasks() -> None:
    runner = _make_error_runner(task_id="PR-002")
    runner.redis.store[_sentinel_key(runner, "PR-001")] = "2026-05-20T00:00:00+00:00"
    runner._error_diagnose_count = 3

    asyncio.run(runner.handle_error())

    assert runner._error_diagnose_count == 4
    assert _sentinel_key(runner, "PR-002") in runner.redis.store


def test_handle_error_below_threshold_does_not_set_sentinel(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    h._patch_subprocess(monkeypatch)
    runner = _make_error_runner()
    _patch_cli_failure(monkeypatch, runner)
    runner._error_diagnose_count = 1

    asyncio.run(runner.handle_error())

    assert runner._error_diagnose_count == 2
    assert _sentinel_key(runner, "PR-001") not in runner.redis.store


def test_clear_sentinel_helper_idempotent() -> None:
    runner = _make_error_runner()

    asyncio.run(runner._clear_diagnose_exhausted("PR-001"))

    assert runner.redis.deleted == [_sentinel_key(runner, "PR-001")]


def test_clear_sentinel_helper_deletes_redis_key_with_single_error_exit() -> None:
    runner = h._make_runner(feature_flags=FeatureFlags(use_single_error_exit=True))
    runner._error_diagnose_count = 3

    asyncio.run(runner._mark_diagnose_exhausted("PR-001"))
    assert _sentinel_key(runner, "PR-001") in runner.redis.store

    asyncio.run(runner._clear_diagnose_exhausted("PR-001"))

    assert _sentinel_key(runner, "PR-001") not in runner.redis.store
    assert asyncio.run(runner._suppression_record_for_task("PR-001")) is None


def test_single_error_exit_ignores_stale_sentinel_without_suppression() -> None:
    runner = h._make_runner(feature_flags=FeatureFlags(use_single_error_exit=True))
    key = _sentinel_key(runner, "PR-001")
    runner.redis.store[key] = "2026-05-20T00:00:00+00:00"

    assert asyncio.run(runner._is_diagnose_exhausted("PR-001")) is False
    assert key not in runner.redis.store


def test_clear_error_message_on_recovery_clears_sentinel() -> None:
    runner = _make_error_runner()
    key = _sentinel_key(runner, "PR-001")
    runner.redis.store[key] = "2026-05-20T00:00:00+00:00"

    asyncio.run(
        runner._clear_error_message_on_recovery(
            log_prefix="[ERROR]",
            reason="diagnose_error FIX retry",
        )
    )

    assert key not in runner.redis.store
    assert key in runner.redis.deleted


def test_handle_error_fix_resets_diagnose_counter(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    h._patch_subprocess(monkeypatch)
    runner = _make_error_runner()
    runner._error_diagnose_count = 2

    async def _diagnose(*args: object, **kwargs: object) -> tuple[int, str, str]:
        return 0, "FIX\nRoot cause found", ""

    monkeypatch.setattr(runner._registry.get("claude"), "diagnose_error", _diagnose)
    monkeypatch.setattr(runner._registry.get("codex"), "diagnose_error", _diagnose)

    asyncio.run(runner.handle_error())

    assert runner.state.state == PipelineState.IDLE
    assert runner._error_diagnose_count == 0


def test_clear_error_message_on_recovery_ignores_sentinel_delete_failure() -> None:
    class FailingDeleteRedis(h._FakeRedis):
        async def delete(self, key: str) -> int:
            raise RuntimeError(f"redis unavailable for {key}")

    runner = _make_error_runner()
    runner.redis = FailingDeleteRedis()

    asyncio.run(
        runner._clear_error_message_on_recovery(
            log_prefix="[ERROR]",
            reason="diagnose_error FIX retry",
        )
    )

    assert runner.state.error_message is None
    assert any(
        "cleared error_message (diagnose_error FIX retry)" in item["event"]
        for item in runner.state.history
    )


def test_sentinel_methods_ignore_empty_task_id() -> None:
    runner = _make_error_runner()

    assert asyncio.run(runner._is_diagnose_exhausted("")) is False
    asyncio.run(runner._mark_diagnose_exhausted(""))
    asyncio.run(runner._clear_diagnose_exhausted(""))

    assert runner.redis.writes == []
    assert runner.redis.deleted == []
