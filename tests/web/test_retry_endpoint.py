"""Contract tests for the durable per-task operator Retry endpoint."""

from __future__ import annotations

import asyncio
import re
from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient
from src.cancellation.storage import CancellationCause, cause_key
from src.daemon import retry_commands as daemon_retry
from src.keyspace import (
    pipeline_state as pipeline_state_key,
    retry_command,
    retry_command_dedupe,
    retry_command_pending,
)
from src.models import PipelineState, PRInfo, QueueTask, RepoState, TaskStatus
from src.retry_commands import (
    RetryCommandStatus,
    RetryExecutionState,
    load_latest_retry_command,
    new_retry_command,
)
from src.web import app as web_app
from src.web.app import app
from src.web.routes import repo_control

from tests.runner import _helpers as runner_helpers
from tests.runner._helpers import _FakeRedis


class _WebRedis(_FakeRedis):
    async def ping(self) -> bool:
        return True

    async def aclose(self) -> None:
        return None


class _GetFailureRedis(_WebRedis):
    async def get(self, key: str) -> str | None:
        if key.startswith("cancellation:"):
            raise RuntimeError("redis read failed")
        return await super().get(key)


class _TransactionFailureRedis(_WebRedis):
    async def transaction(self, *args: Any, **kwargs: Any) -> Any:
        raise RuntimeError("redis write failed")


def _aioredis(redis_client: _WebRedis) -> object:
    return type(
        "_Aioredis",
        (),
        {"from_url": staticmethod(lambda url, decode_responses=True: redis_client)},
    )()


def _task_text(*, status: str = "ERROR", body: str = "Retry body") -> str:
    blocked_reason = "blocked_reason: daemon\n" if status == "ERROR" else ""
    return (
        "---\n"
        f"status: {status}\n"
        f"{blocked_reason}"
        "---\n\n"
        "# PR-283: Retry me\n\n"
        "Branch: fix/pr-283\n"
        "- Type: bugfix\n"
        "- Complexity: low\n"
        "- Depends on: none\n"
        "- Priority: 2\n"
        "- Coder: codex\n\n"
        "## Problem\n"
        f"{body}\n"
    )


def _setup_retry(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    redis_client: _WebRedis | None = None,
    status: TaskStatus = TaskStatus.ERROR,
    pipeline_state: PipelineState = PipelineState.ERROR,
    retry_count: int = 0,
    current_pr: PRInfo | None = None,
) -> tuple[Path, _WebRedis]:
    (tmp_path / "config.yml").write_text(
        "repositories:\n"
        "  - url: https://github.com/example/alpha.git\n"
        "    branch: main\n"
        "daemon:\n"
        "  retry_button_cap: 3\n",
        encoding="utf-8",
    )
    repo = tmp_path / "repos" / "example__alpha"
    task_path = repo / "tasks" / "PR-283.md"
    task_path.parent.mkdir(parents=True, exist_ok=True)
    task_path.write_text(_task_text(status=status.value), encoding="utf-8")
    task = QueueTask(
        pr_id="PR-283",
        title="Retry me",
        status=status,
        task_file="tasks/PR-283.md",
        branch="fix/pr-283",
        priority=2,
    )
    state = RepoState(
        url="https://github.com/example/alpha.git",
        name="example__alpha",
        state=pipeline_state,
        current_task=(task if pipeline_state == PipelineState.ERROR else None),
        current_pr=current_pr,
        current_queue=[task],
        error_message="coder failed",
    )
    redis_client = redis_client or _WebRedis()
    redis_client.store["pipeline:example__alpha"] = state.model_dump_json()
    if retry_count:
        redis_client.store["metrics:retry_count:example__alpha:PR-283"] = str(
            retry_count
        )
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(web_app, "REPOS_DIR", str(tmp_path / "repos"))
    monkeypatch.setattr(web_app, "aioredis", _aioredis(redis_client))
    monkeypatch.setattr(repo_control, "write_audit_record", lambda *args: None)
    return task_path, redis_client


def _rendered_binding(client: TestClient) -> str:
    response = client.get("/repos/example__alpha/tasks")
    assert response.status_code == 200
    match = re.search(r'"retry_binding":"([0-9a-f]{64})"', response.text)
    assert match is not None, response.text
    return match.group(1)


def test_retry_only_enqueues_and_duplicate_delivery_is_idempotent(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    task_path, redis_client = _setup_retry(tmp_path, monkeypatch)
    original = task_path.read_text(encoding="utf-8")
    wake_calls: list[str] = []

    async def publish_wake(redis: Any, repo: str, event: str) -> None:
        assert redis is redis_client
        assert repo == "example__alpha"
        wake_calls.append(event)

    monkeypatch.setattr(web_app, "publish_wake", publish_wake)
    monkeypatch.setattr(
        repo_control.subprocess,
        "run",
        lambda *args, **kwargs: pytest.fail("Retry web handler must not run git"),
    )

    with TestClient(app) as client:
        binding = _rendered_binding(client)
        first = client.post(
            "/repos/example__alpha/tasks/PR-283/retry",
            data={"retry_binding": binding},
        )
        second = client.post(
            "/repos/example__alpha/tasks/PR-283/retry",
            data={"retry_binding": binding},
        )

    assert first.status_code == second.status_code == 202
    assert task_path.read_text(encoding="utf-8") == original
    assert "metrics:retry_count:example__alpha:PR-283" not in redis_client.store
    assert redis_client.zsets[retry_command_pending("example__alpha")]
    assert len(redis_client.zsets[retry_command_pending("example__alpha")]) == 1
    command = asyncio.run(
        load_latest_retry_command(redis_client, "example__alpha", "PR-283")
    )
    assert command is not None
    assert command.status == RetryCommandStatus.QUEUED
    assert command.request_binding == binding
    assert command.task_fingerprint == repo_control._task_retry_fingerprint(task_path)
    assert wake_calls == ["retry_command"]
    assert "Retry accepted" in first.text


def test_web_command_reaches_parked_daemon_and_dispatches_once(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    task_path, redis_client = _setup_retry(tmp_path, monkeypatch)
    with TestClient(app) as client:
        binding = _rendered_binding(client)
        response = client.post(
            "/repos/example__alpha/tasks/PR-283/retry",
            data={"retry_binding": binding},
        )
    assert response.status_code == 202

    runner = runner_helpers._make_runner()
    runner.name = "example__alpha"
    runner.owner_repo = "example/alpha"
    runner.redis = redis_client
    runner.repo_path = str(task_path.parents[1])
    runner.state = RepoState.model_validate_json(
        redis_client.store["pipeline:example__alpha"]
    )
    runner._retry_command_owner = "integration-runner"
    original = task_path.read_text(encoding="utf-8")
    status_commits: list[str] = []
    coder_calls: list[str] = []

    monkeypatch.setattr(runner, "_retry_worktree_dirty", lambda: (False, ""))
    monkeypatch.setattr(
        runner, "_origin_retry_task_text", lambda command: original
    )

    async def no_suppression(task_id: str) -> None:
        return None

    async def no_inhibitors(*args: Any) -> list[Any]:
        return []

    async def coder_available(*args: Any) -> tuple[str, object]:
        return "codex", object()

    async def commit_status(task: QueueTask, status: str, reason: str) -> bool:
        status_commits.append(status)
        return True

    monkeypatch.setattr(runner, "_suppression_record_for_task", no_suppression)
    monkeypatch.setattr(daemon_retry, "derive_active_inhibitors", no_inhibitors)
    monkeypatch.setattr(daemon_retry.gh_prs, "get_open_prs", lambda *args: [])
    monkeypatch.setattr(
        daemon_retry.gh_prs, "get_merged_prs", lambda *args, **kwargs: []
    )
    monkeypatch.setattr(runner, "_ensure_retry_coder_available", coder_available)
    monkeypatch.setattr(runner, "_commit_task_status_change", commit_status)

    async def ensure_repo_cloned() -> None:
        return None

    async def publish_state() -> None:
        await redis_client.set(
            pipeline_state_key(runner.name), runner.state.model_dump_json()
        )

    async def handle_coding() -> None:
        coder_calls.append("PR-283")
        runner.state.state = PipelineState.WATCH

    runner._recovered = True
    monkeypatch.setattr(runner, "ensure_repo_cloned", ensure_repo_cloned)
    monkeypatch.setattr(runner, "publish_state", publish_state)
    monkeypatch.setattr(runner, "handle_coding", handle_coding)
    asyncio.run(runner._run_cycle_body())
    command = asyncio.run(
        load_latest_retry_command(redis_client, "example__alpha", "PR-283")
    )
    assert command is not None
    assert command.status == RetryCommandStatus.APPLIED
    assert command.execution_state == RetryExecutionState.WATCHING
    assert command.selected_continuation == "coding"
    assert status_commits == ["TODO"]
    assert coder_calls == ["PR-283"]
    assert redis_client.store["metrics:retry_count:example__alpha:PR-283"] == "1"
    assert task_path.read_text(encoding="utf-8") == original
    published = RepoState.model_validate_json(
        redis_client.store[pipeline_state_key("example__alpha")]
    )
    assert published.state == PipelineState.WATCH
    assert published.current_task is not None
    assert published.current_task.status == TaskStatus.DOING


def test_retry_binds_failure_and_relevant_pr(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pr = PRInfo(
        number=531,
        branch="fix/pr-283",
        head_sha="abc123",
        pr_id="PR-283",
    )
    _, redis_client = _setup_retry(tmp_path, monkeypatch, current_pr=pr)
    redis_client.store[cause_key("example__alpha", "PR-283")] = CancellationCause(
        category="ERROR",
        created_at="2026-01-01T00:00:00+00:00",
        task_id="PR-283",
        repo_slug="example__alpha",
        payload={"subsource": "coder_crash"},
    ).to_redis()

    with TestClient(app) as client:
        binding = _rendered_binding(client)
        response = client.post(
            "/repos/example__alpha/tasks/PR-283/retry",
            data={"retry_binding": binding},
        )

    assert response.status_code == 202
    command = asyncio.run(
        load_latest_retry_command(redis_client, "example__alpha", "PR-283")
    )
    assert command is not None
    assert command.bound_pr_number == 531
    assert command.bound_pr_branch == "fix/pr-283"
    assert command.bound_pr_head_sha == "abc123"
    assert command.failure_subsource == "coder_crash"
    assert command.failure_created_at == "2026-01-01T00:00:00+00:00"
    assert cause_key("example__alpha", "PR-283") in redis_client.store


def test_retry_rejects_stale_or_invalid_binding(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    task_path, _redis = _setup_retry(tmp_path, monkeypatch)
    with TestClient(app) as client:
        binding = _rendered_binding(client)
        task_path.write_text(_task_text(body="changed"), encoding="utf-8")
        stale = client.post(
            "/repos/example__alpha/tasks/PR-283/retry",
            data={"retry_binding": binding},
        )
        invalid = client.post(
            "/repos/example__alpha/tasks/PR-283/retry",
            data={"retry_binding": "invalid"},
        )
        missing = client.post("/repos/example__alpha/tasks/PR-283/retry")

    assert stale.status_code == 409
    assert "stale" in stale.text
    assert invalid.status_code == 400
    assert missing.status_code == 422


@pytest.mark.parametrize(
    ("path", "expected"),
    [
        ("/repos/example__alpha/tasks/not-a-task/retry", 400),
        ("/repos/missing/tasks/PR-283/retry", 404),
        ("/repos/example__alpha/tasks/PR-999/retry", 404),
    ],
)
def test_retry_validates_target_before_enqueue(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    path: str,
    expected: int,
) -> None:
    _setup_retry(tmp_path, monkeypatch)
    with TestClient(app) as client:
        response = client.post(path, data={"retry_binding": "a" * 64})
    assert response.status_code == expected


def test_retry_rejects_non_error_and_retry_cap(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _setup_retry(
        tmp_path,
        monkeypatch,
        status=TaskStatus.DONE,
        pipeline_state=PipelineState.IDLE,
    )
    with TestClient(app) as client:
        not_error = client.post(
            "/repos/example__alpha/tasks/PR-283/retry",
            data={"retry_binding": "a" * 64},
        )
    assert not_error.status_code == 409

    _task_path, _redis = _setup_retry(tmp_path, monkeypatch, retry_count=3)
    with TestClient(app) as client:
        capped = client.post(
            "/repos/example__alpha/tasks/PR-283/retry",
            data={"retry_binding": "a" * 64},
        )
    assert capped.status_code == 409
    assert "Retry cap reached" in capped.text


def test_retry_reports_read_binding_and_command_store_failures(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _setup_retry(tmp_path, monkeypatch)
    original_reader = repo_control._read_task_frontmatter_status
    monkeypatch.setattr(
        repo_control,
        "_read_task_frontmatter_status",
        lambda path: (_ for _ in ()).throw(OSError("read failed")),
    )
    with TestClient(app) as client:
        read_failed = client.post(
            "/repos/example__alpha/tasks/PR-283/retry",
            data={"retry_binding": "a" * 64},
        )
    assert read_failed.status_code == 503

    class _DedupeReadFails(_WebRedis):
        async def get(self, key: str) -> str | None:
            if ":retry:dedupe:" in key:
                raise RuntimeError("command store down")
            return await super().get(key)

    monkeypatch.setattr(repo_control, "_read_task_frontmatter_status", original_reader)
    _setup_retry(tmp_path, monkeypatch, redis_client=_DedupeReadFails())
    with TestClient(app) as client:
        command_store_failed = client.post(
            "/repos/example__alpha/tasks/PR-283/retry",
            data={"retry_binding": "a" * 64},
        )
    assert command_store_failed.status_code == 503


def test_retry_rejects_cross_task_dedupe_and_unbindable_task(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    task_path, redis_client = _setup_retry(tmp_path, monkeypatch)
    existing = new_retry_command(
        repo_slug="example__alpha",
        task_id="PR-999",
        task_file="tasks/PR-999.md",
        task_branch="fix/pr-999",
        task_fingerprint="f" * 64,
        request_binding="a" * 64,
        failure_id="e" * 64,
        retry_cap=3,
    )
    redis_client.store[
        retry_command("example__alpha", existing.command_id)
    ] = existing.model_dump_json()
    redis_client.store[
        retry_command_dedupe("example__alpha", existing.request_binding)
    ] = existing.command_id
    with TestClient(app) as client:
        cross_task = client.post(
            "/repos/example__alpha/tasks/PR-283/retry",
            data={"retry_binding": "a" * 64},
        )
    assert cross_task.status_code == 409

    redis_client.store.clear()
    task = QueueTask(
        pr_id="PR-283",
        title="Retry me",
        status=TaskStatus.ERROR,
        task_file="tasks/PR-283.md",
        branch="fix/pr-283",
    )
    redis_client.store["pipeline:example__alpha"] = RepoState(
        url="https://github.com/example/alpha.git",
        name="example__alpha",
        state=PipelineState.ERROR,
        current_task=task,
        current_queue=[task],
    ).model_dump_json()

    async def no_binding(*args: Any, **kwargs: Any) -> None:
        return None

    monkeypatch.setattr(repo_control, "_retry_binding_context", no_binding)
    with TestClient(app) as client:
        unbindable = client.post(
            "/repos/example__alpha/tasks/PR-283/retry",
            data={"retry_binding": "a" * 64},
        )
    assert unbindable.status_code == 503
    assert task_path.exists()


def test_retry_store_failures_are_explicit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _, failing_read = _setup_retry(
        tmp_path,
        monkeypatch,
        redis_client=_GetFailureRedis(),
    )
    with TestClient(app) as client:
        response = client.post(
            "/repos/example__alpha/tasks/PR-283/retry",
            data={"retry_binding": "a" * 64},
        )
    assert response.status_code == 503
    assert not failing_read.zsets

    _setup_retry(
        tmp_path,
        monkeypatch,
        redis_client=_TransactionFailureRedis(),
    )
    with TestClient(app) as client:
        binding = _rendered_binding(client)
        response = client.post(
            "/repos/example__alpha/tasks/PR-283/retry",
            data={"retry_binding": binding},
        )
    assert response.status_code == 503
    assert "persist" in response.text


def test_retry_requires_redis_and_tolerates_wake_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _, redis_client = _setup_retry(tmp_path, monkeypatch)
    with TestClient(app) as client:
        binding = _rendered_binding(client)
        app.state.redis = None
        unavailable = client.post(
            "/repos/example__alpha/tasks/PR-283/retry",
            data={"retry_binding": binding},
        )
    assert unavailable.status_code == 503

    _setup_retry(tmp_path, monkeypatch, redis_client=redis_client)

    async def fail_wake(*args: Any) -> None:
        raise RuntimeError("pubsub unavailable")

    monkeypatch.setattr(web_app, "publish_wake", fail_wake)
    with TestClient(app) as client:
        binding = _rendered_binding(client)
        accepted = client.post(
            "/repos/example__alpha/tasks/PR-283/retry",
            data={"retry_binding": binding},
        )
    assert accepted.status_code == 202


def test_task_panel_renders_command_lifecycle_and_polling(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _, redis_client = _setup_retry(tmp_path, monkeypatch)
    with TestClient(app) as client:
        binding = _rendered_binding(client)
        client.post(
            "/repos/example__alpha/tasks/PR-283/retry",
            data={"retry_binding": binding},
        )
        queued = client.get("/repos/example__alpha/tasks")

    assert "Retry accepted" in queued.text
    assert 'hx-trigger="every 5s"' in queued.text
    command = asyncio.run(
        load_latest_retry_command(redis_client, "example__alpha", "PR-283")
    )
    assert command is not None
    raw_key = f"control:example__alpha:retry:command:{command.command_id}"
    for status, phrase, pending in [
        (RetryCommandStatus.PROCESSING, "Daemon acknowledged", True),
        (RetryCommandStatus.DEFERRED, "Retry deferred", True),
        (RetryCommandStatus.APPLIED, "Retry applied", True),
        (RetryCommandStatus.FAILED, "Retry failed", False),
    ]:
        command.status = status
        command.outcome_reason = f"{status.value} reason"
        if status == RetryCommandStatus.APPLIED:
            command.execution_state = RetryExecutionState.UNCERTAIN
        redis_client.store[raw_key] = command.model_dump_json()
        if not pending:
            redis_client.zsets[retry_command_pending("example__alpha")].clear()
        with TestClient(app) as client:
            rendered = client.get("/repos/example__alpha/tasks")
        assert phrase in rendered.text


def test_fingerprint_ignores_status_but_tracks_spec(tmp_path: Path) -> None:
    task_path = tmp_path / "PR-283.md"
    task_path.write_text(_task_text(), encoding="utf-8")
    error_fingerprint = repo_control._task_retry_fingerprint(task_path)
    task_path.write_text(_task_text(status="TODO"), encoding="utf-8")
    assert repo_control._task_retry_fingerprint(task_path) == error_fingerprint
    task_path.write_text(_task_text(status="TODO", body="changed"), encoding="utf-8")
    assert repo_control._task_retry_fingerprint(task_path) != error_fingerprint


@pytest.mark.asyncio
async def test_retry_count_decode_and_read_failures() -> None:
    redis_client = _WebRedis()
    assert repo_control._decode_retry_count(None) == 0
    assert repo_control._decode_retry_count(b"2") == 2
    assert repo_control._decode_retry_count("-1") == 0
    assert repo_control._decode_retry_count("bad") == 0
    assert await repo_control._get_retry_count(redis_client, "repo", "PR-1") == 0

    class _Broken(_WebRedis):
        async def get(self, key: str) -> str | None:
            raise RuntimeError("down")

    assert await repo_control._get_retry_count(_Broken(), "repo", "PR-1") == 0


@pytest.mark.asyncio
async def test_non_error_task_view_surfaces_active_retry_pipeline_state(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _task_path, redis_client = _setup_retry(tmp_path, monkeypatch)
    task = QueueTask(
        pr_id="PR-283",
        title="Retry me",
        status=TaskStatus.TODO,
        task_file="tasks/PR-283.md",
        branch="fix/pr-283",
    )
    command = new_retry_command(
        repo_slug="example__alpha",
        task_id=task.pr_id,
        task_file=str(task.task_file),
        task_branch=str(task.branch),
        task_fingerprint="f" * 64,
        request_binding="a" * 64,
        failure_id="e" * 64,
        retry_cap=3,
    )
    redis_client.store[
        retry_command("example__alpha", command.command_id)
    ] = command.model_dump_json()
    redis_client.store[
        f"control:example__alpha:retry:latest:{task.pr_id}"
    ] = command.command_id
    state = RepoState.model_validate_json(redis_client.store["pipeline:example__alpha"])
    state.state = PipelineState.CODING
    state.current_task = task
    redis_client.store["pipeline:example__alpha"] = state.model_dump_json()
    view = await repo_control._task_view(task, "example__alpha", redis_client)
    assert view["retry_pipeline_state"] == "CODING"
