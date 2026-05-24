from __future__ import annotations

from pathlib import Path

import pytest

from src.cancellation import task_spec_content_hash
from src.subsource_registry import SuppressionReason
from tests.runner._helpers import _make_runner


def _production_python_files() -> list[Path]:
    return [
        path
        for path in Path("src").rglob("*.py")
        if "test" not in path.parts
    ]


def _write_task(runner, pr_id: str, status: str) -> None:
    tasks_dir = Path(runner.repo_path) / "tasks"
    tasks_dir.mkdir(parents=True, exist_ok=True)
    (tasks_dir / f"{pr_id}.md").write_text(
        "---\n"
        f"status: {status}\n"
        "---\n"
        f"# {pr_id}: Legacy task\n\n"
        f"Branch: {pr_id.lower()}\n"
        "- Type: feature\n"
        "- Complexity: low\n"
        "- Depends on: none\n"
        "- Priority: 1\n"
        "- Coder: any\n",
        encoding="utf-8",
    )


def _task_text(runner, pr_id: str) -> str:
    return (Path(runner.repo_path) / "tasks" / f"{pr_id}.md").read_text(
        encoding="utf-8"
    )


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
    for pr_id in ("PR-001", "PR-002", "PR-003"):
        _write_task(runner, pr_id, "ERROR")
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
        assert record.detail["source"] == "status_write_failed"
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
    _write_task(runner, "PR-001", "ERROR")

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
async def test_cleanup_preserves_existing_suppression_record() -> None:
    runner = _make_runner()
    key = f"recovered_tasks:{runner.name}"
    runner.redis.store[key] = '["PR-001"]'
    await runner._suppress_task(
        "PR-001",
        SuppressionReason.REVIEW_TIMEOUT,
        {"source": "review_timeout"},
    )

    await runner._cleanup_stale_legacy_key_markers()

    assert key not in runner.redis.store
    record = await runner._suppression_record_for_task("PR-001")
    assert record is not None
    assert record.reason == SuppressionReason.REVIEW_TIMEOUT
    assert record.detail["source"] == "review_timeout"


@pytest.mark.asyncio
async def test_cleanup_replaces_nonblocking_existing_suppression() -> None:
    runner = _make_runner()
    key = f"status_write_failed_tasks:{runner.name}"
    runner.redis.store[key] = '["PR-001"]'
    await runner._suppress_task(
        "PR-001",
        SuppressionReason.INFRA_FAILURE,
        {"source": "infra_failure"},
    )

    await runner._cleanup_stale_legacy_key_markers()

    assert key not in runner.redis.store
    record = await runner._suppression_record_for_task("PR-001")
    assert record is not None
    assert record.reason == SuppressionReason.CRASH
    assert record.detail["source"] == "status_write_failed"
    assert record.detail["legacy_key"] is True


@pytest.mark.asyncio
async def test_cleanup_drops_stale_todo_legacy_marker() -> None:
    runner = _make_runner()
    key = f"status_write_failed_tasks:{runner.name}"
    runner.redis.store[key] = '["PR-001"]'
    _write_task(runner, "PR-001", "TODO")

    await runner._cleanup_stale_legacy_key_markers()

    assert key not in runner.redis.store
    assert await runner._suppression_record_for_task("PR-001") is None


@pytest.mark.asyncio
async def test_cleanup_migrates_matching_hash_payload() -> None:
    runner = _make_runner()
    key = f"status_write_failed_tasks:{runner.name}"
    _write_task(runner, "PR-001", "TODO")
    task_hash = task_spec_content_hash(_task_text(runner, "PR-001"))
    runner.redis.store[key] = (
        f'[{{"pr_id": "PR-001", "task_spec_hash": "{task_hash}"}}]'
    )

    await runner._cleanup_stale_legacy_key_markers()

    assert key not in runner.redis.store
    record = await runner._suppression_record_for_task("PR-001")
    assert record is not None
    assert record.reason == SuppressionReason.CRASH


@pytest.mark.asyncio
async def test_cleanup_ignores_invalid_legacy_entries() -> None:
    runner = _make_runner()
    key = f"status_write_failed_tasks:{runner.name}"
    runner.redis.store[key] = '[123, {"task_spec_hash": "abc"}]'

    await runner._cleanup_stale_legacy_key_markers()

    assert key not in runner.redis.store


@pytest.mark.asyncio
async def test_cleanup_failure_retries_next_startup_cycle(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
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

    assert runner._recovered is False


@pytest.mark.asyncio
async def test_recovery_pending_upload_read_failure_is_best_effort(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = _make_runner()

    async def ensure_repo_cloned() -> None:
        return None

    async def check_github_api_budget() -> bool:
        return True

    async def refresh_user_paused_from_redis() -> None:
        return None

    async def recover_state() -> bool:
        return False

    async def publish_state() -> None:
        return None

    async def cleanup_legacy() -> bool:
        return True

    async def get(key: str) -> None:
        del key
        raise RuntimeError("redis unavailable")

    monkeypatch.setattr(runner, "ensure_repo_cloned", ensure_repo_cloned)
    monkeypatch.setattr(runner, "_check_github_api_budget", check_github_api_budget)
    monkeypatch.setattr(
        runner,
        "_refresh_user_paused_from_redis",
        refresh_user_paused_from_redis,
    )
    monkeypatch.setattr(runner, "recover_state", recover_state)
    monkeypatch.setattr(runner, "publish_state", publish_state)
    monkeypatch.setattr(runner, "_cleanup_stale_legacy_key_markers", cleanup_legacy)
    runner.redis.get = get  # type: ignore[method-assign]

    await runner._run_cycle_body()

    assert runner._recovered is False
