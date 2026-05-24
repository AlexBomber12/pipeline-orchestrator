from __future__ import annotations

from collections.abc import AsyncIterator
from pathlib import Path

import pytest

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
            "status_write_failed_tasks:demo": '["PR-001"]',
            "recovered_tasks:demo": '["PR-002"]',
            "legacy_recovered_tasks:demo": '["PR-003"]',
            "pipeline:demo": "{}",
        }
    )

    async def scan_iter(match: str | None = None) -> AsyncIterator[str]:
        prefix = "" if match is None else match.removesuffix("*")
        for key in list(runner.redis.store):
            if key.startswith(prefix):
                yield key

    runner.redis.scan_iter = scan_iter  # type: ignore[attr-defined]

    await runner._cleanup_stale_legacy_key_markers()
    await runner._cleanup_stale_legacy_key_markers()

    assert runner.redis.store == {"pipeline:demo": "{}"}


@pytest.mark.asyncio
async def test_cleanup_best_effort() -> None:
    runner = _make_runner()

    async def scan_iter(match: str | None = None) -> AsyncIterator[str]:
        del match
        raise RuntimeError("redis unavailable")
        yield ""

    runner.redis.scan_iter = scan_iter  # type: ignore[attr-defined]

    await runner._cleanup_stale_legacy_key_markers()

    assert any(
        "failed to clean stale legacy Redis keys" in entry["event"]
        for entry in runner.state.history
    )


@pytest.mark.asyncio
async def test_cleanup_failure_does_not_block_startup(monkeypatch: pytest.MonkeyPatch) -> None:
    runner = _make_runner()

    async def ensure_repo_cloned() -> None:
        return None

    async def check_github_api_budget() -> bool:
        return True

    async def refresh_user_paused_from_redis() -> None:
        return None

    async def scan_iter(match: str | None = None) -> AsyncIterator[str]:
        del match
        raise RuntimeError("redis unavailable")
        yield ""

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
    runner.redis.scan_iter = scan_iter  # type: ignore[attr-defined]

    await runner._run_cycle_body()

    assert runner._recovered is True
