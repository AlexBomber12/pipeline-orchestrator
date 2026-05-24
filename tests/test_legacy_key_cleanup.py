from __future__ import annotations

from pathlib import Path

import pytest

from src.subsource_registry import SuppressionReason
from tests.runner._helpers import _make_runner


def _production_python_files() -> list[Path]:
    return [
        path
        for path in Path("src").rglob("*.py")
        if "test" not in path.parts
    ]


def test_no_production_references() -> None:
    forbidden = (
        "legacy_recovered_tasks",
        "status_write_failed_tasks",
        "recovered_tasks",
    )

    offenders = [
        (str(path), marker)
        for path in _production_python_files()
        for marker in forbidden
        if marker in path.read_text(encoding="utf-8")
    ]

    assert offenders == []


@pytest.mark.asyncio
async def test_stale_key_cleanup_idempotent() -> None:
    runner = _make_runner()
    runner.redis.store.update(
        {
            f"status_write_failed_tasks:{runner.name}": '["PR-001"]',
            f"recovered_tasks:{runner.name}": '["PR-002"]',
            f"legacy_recovered_tasks:{runner.name}": '["PR-003"]',
            "status_write_failed_tasks:other__repo": '["PR-101"]',
            "recovered_tasks:other__repo": '["PR-102"]',
            "legacy_recovered_tasks:other__repo": '["PR-103"]',
            f"pipeline:{runner.name}": "{}",
        }
    )

    await runner._cleanup_stale_legacy_key_markers()
    await runner._cleanup_stale_legacy_key_markers()

    assert "status_write_failed_tasks:other__repo" in runner.redis.store
    assert "recovered_tasks:other__repo" in runner.redis.store
    assert "legacy_recovered_tasks:other__repo" in runner.redis.store
    assert f"pipeline:{runner.name}" in runner.redis.store
    assert f"status_write_failed_tasks:{runner.name}" not in runner.redis.store
    assert f"recovered_tasks:{runner.name}" not in runner.redis.store
    assert f"legacy_recovered_tasks:{runner.name}" not in runner.redis.store

    for pr_id in ("PR-001", "PR-002", "PR-003"):
        record = await runner._suppression_record_for_task(pr_id)
        assert record is not None
        assert record.reason == SuppressionReason.CRASH
        assert record.detail["legacy_key"] is True


@pytest.mark.asyncio
async def test_cleanup_best_effort() -> None:
    runner = _make_runner()

    async def delete(key: str) -> int:
        del key
        raise RuntimeError("redis unavailable")

    runner.redis.delete = delete  # type: ignore[method-assign]

    await runner._cleanup_stale_legacy_key_markers()

    assert any(
        "failed to migrate/clean stale legacy Redis key" in entry["event"]
        for entry in runner.state.history
    )


@pytest.mark.asyncio
async def test_cleanup_preserves_key_when_migration_fails() -> None:
    runner = _make_runner()
    key = f"status_write_failed_tasks:{runner.name}"
    runner.redis.store[key] = '["PR-001"]'

    async def suppress(*_args: object, **_kwargs: object) -> None:
        raise RuntimeError("redis unavailable")

    runner._suppress_task = suppress  # type: ignore[method-assign]

    await runner._cleanup_stale_legacy_key_markers()

    assert runner.redis.store[key] == '["PR-001"]'
    assert any(
        "failed to migrate/clean stale legacy Redis key" in entry["event"]
        for entry in runner.state.history
    )


@pytest.mark.asyncio
async def test_cleanup_drops_malformed_legacy_payload() -> None:
    runner = _make_runner()
    key = f"status_write_failed_tasks:{runner.name}"
    runner.redis.store[key] = '{"PR-001": true}'

    await runner._cleanup_stale_legacy_key_markers()

    assert key not in runner.redis.store
    assert await runner._suppression_record_for_task("PR-001") is None


@pytest.mark.asyncio
async def test_cleanup_failure_does_not_block_startup(monkeypatch: pytest.MonkeyPatch) -> None:
    runner = _make_runner()

    async def ensure_repo_cloned() -> None:
        return None

    async def check_github_api_budget() -> bool:
        return True

    async def refresh_user_paused_from_redis() -> None:
        return None

    async def delete(key: str) -> int:
        del key
        raise RuntimeError("redis unavailable")

    async def recover_state() -> bool:
        return True

    async def publish_state() -> None:
        return None

    monkeypatch.setattr(runner, "ensure_repo_cloned", ensure_repo_cloned)
    monkeypatch.setattr(runner, "_check_github_api_budget", check_github_api_budget)
    monkeypatch.setattr(
        runner,
        "_refresh_user_paused_from_redis",
        refresh_user_paused_from_redis,
    )
    monkeypatch.setattr(runner, "recover_state", recover_state)
    monkeypatch.setattr(runner, "publish_state", publish_state)
    runner.redis.delete = delete  # type: ignore[method-assign]

    await runner._run_cycle_body()

    assert runner._recovered is True
