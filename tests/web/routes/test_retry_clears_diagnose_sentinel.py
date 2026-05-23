"""Retry endpoint coverage for diagnose_error exhaustion sentinels."""

from __future__ import annotations

import asyncio
import subprocess
from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient
from src.models import QueueTask, TaskStatus
from src.web import app as web_app
from src.web.app import app
from src.web.routes import repo_control

from tests.web.test_retry_endpoint import (
    _aioredis,
    _RetryRedis,
    _snapshot,
    _write_config_and_task,
)


def _fake_git(monkeypatch: pytest.MonkeyPatch, repo_dir: Path) -> None:
    def fake_run(args: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(args, 0, "", "")

    monkeypatch.setattr(repo_control.subprocess, "run", fake_run)


def _seed_retryable_sentinel(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    include_sentinel: bool,
) -> tuple[_RetryRedis, str]:
    repo_dir = _write_config_and_task(tmp_path, monkeypatch, task_name="PR-001")
    _fake_git(monkeypatch, repo_dir)
    sentinel_key = "diagnose_exhausted:example__alpha:PR-001"
    store = {
        "pipeline:example__alpha": _snapshot(
            [
                QueueTask(
                    pr_id="PR-001",
                    title="Retry me",
                    status=TaskStatus.ERROR,
                    task_file="tasks/PR-001.md",
                )
            ]
        )
    }
    if include_sentinel:
        store[sentinel_key] = "2026-05-20T00:00:00+00:00"
    redis_client = _RetryRedis(store)
    monkeypatch.setattr(web_app, "aioredis", _aioredis(redis_client))
    return redis_client, sentinel_key


def test_retry_endpoint_clears_sentinel(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    redis_client, sentinel_key = _seed_retryable_sentinel(
        tmp_path,
        monkeypatch,
        include_sentinel=True,
    )

    with TestClient(app) as client:
        response = client.post("/repos/example__alpha/tasks/PR-001/retry")

    assert response.status_code == 200
    assert sentinel_key not in redis_client.store
    assert sentinel_key in redis_client.deleted


def test_retry_endpoint_clears_status_write_failed_marker(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    redis_client, _sentinel_key = _seed_retryable_sentinel(
        tmp_path,
        monkeypatch,
        include_sentinel=False,
    )
    marker_key = "status_write_failed_tasks:example__alpha"
    legacy_key = "recovered_tasks:example__alpha"
    redis_client.store[marker_key] = '["PR-001","PR-999"]'
    redis_client.store[legacy_key] = '["PR-001","PR-888"]'

    with TestClient(app) as client:
        response = client.post("/repos/example__alpha/tasks/PR-001/retry")

    assert response.status_code == 200
    assert redis_client.store[marker_key] == '["PR-999"]'
    assert redis_client.store[legacy_key] == '["PR-888"]'


def test_retry_endpoint_clears_legacy_recovered_task_marker(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    redis_client, _sentinel_key = _seed_retryable_sentinel(
        tmp_path,
        monkeypatch,
        include_sentinel=False,
    )
    legacy_key = "recovered_tasks:example__alpha"
    redis_client.store[legacy_key] = '["PR-001"]'

    with TestClient(app) as client:
        response = client.post("/repos/example__alpha/tasks/PR-001/retry")

    assert response.status_code == 200
    assert legacy_key not in redis_client.store


def test_status_write_failed_marker_helper_defensive_branches() -> None:
    marker_key = "status_write_failed_tasks:example__alpha"

    redis_client = _RetryRedis({marker_key: b'["PR-001"]'})  # type: ignore[dict-item]
    asyncio.run(
        repo_control._clear_status_write_failed_marker(
            redis_client,
            "example__alpha",
            "PR-001",
        )
    )
    assert marker_key not in redis_client.store

    for value in ("not-json", '{"task":"PR-001"}', '["PR-999"]'):
        redis_client = _RetryRedis({marker_key: value})
        asyncio.run(
            repo_control._clear_status_write_failed_marker(
                redis_client,
                "example__alpha",
                "PR-001",
            )
        )
        assert redis_client.store[marker_key] == value

    redis_client = _RetryRedis()
    asyncio.run(
        repo_control._clear_status_write_failed_marker(
            redis_client,
            "example__alpha",
            "PR-001",
        )
    )
    assert marker_key not in redis_client.store


def test_retry_tolerates_status_write_failed_marker_cleanup_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    redis_client, _sentinel_key = _seed_retryable_sentinel(
        tmp_path,
        monkeypatch,
        include_sentinel=False,
    )

    async def fail_cleanup(*args: object, **kwargs: object) -> None:
        raise RuntimeError("cleanup failed")

    monkeypatch.setattr(repo_control, "_clear_status_write_failed_marker", fail_cleanup)

    with TestClient(app) as client:
        response = client.post("/repos/example__alpha/tasks/PR-001/retry")

    assert response.status_code == 200
    assert redis_client.store["metrics:retry_count:example__alpha:PR-001"] == "1"


def test_retry_works_when_sentinel_absent(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    redis_client, sentinel_key = _seed_retryable_sentinel(
        tmp_path,
        monkeypatch,
        include_sentinel=False,
    )

    with TestClient(app) as client:
        response = client.post("/repos/example__alpha/tasks/PR-001/retry")

    assert response.status_code == 200
    assert sentinel_key not in redis_client.store
    assert sentinel_key in redis_client.deleted
    assert redis_client.store["metrics:retry_count:example__alpha:PR-001"] == "1"
