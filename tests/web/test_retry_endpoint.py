"""Tests for the per-task operator retry endpoint."""

from __future__ import annotations

import subprocess
from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient
from src.cancellation.storage import cause_key, index_key
from src.models import PipelineState, QueueTask, RepoState, TaskStatus
from src.web import app as web_app
from src.web.app import app
from src.web.routes import repo_control


class _RetryRedis:
    def __init__(self, store: dict[str, str] | None = None) -> None:
        self.store = store or {}
        self.deleted: list[str] = []
        self.zremmed: list[tuple[str, tuple[str, ...]]] = []
        self.expiries: dict[str, int] = {}

    async def ping(self) -> bool:
        return True

    async def get(self, key: str) -> str | None:
        return self.store.get(key)

    def multi(self) -> None:
        return None

    def set(self, key: str, value: str, ex: int | None = None) -> None:
        self.store[key] = value
        if ex is not None:
            self.expiries[key] = ex

    async def delete(self, key: str) -> int:
        self.deleted.append(key)
        return 1 if self.store.pop(key, None) is not None else 0

    async def zrem(self, key: str, *members: str) -> int:
        self.zremmed.append((key, members))
        return 1

    async def transaction(self, callback: Any, *keys: str, value_from_callable: bool = False) -> Any:
        result = await callback(self)
        return result if value_from_callable else None

    async def aclose(self) -> None:
        return None


def _aioredis(redis_client: _RetryRedis) -> object:
    return type(
        "_Aioredis",
        (),
        {"from_url": staticmethod(lambda url, decode_responses=True: redis_client)},
    )()


def _write_config_and_task(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    task_name: str = "PR-283",
    status: str = "ERROR",
) -> Path:
    cfg = tmp_path / "config.yml"
    cfg.write_text(
        "repositories:\n"
        "  - url: https://github.com/example/alpha.git\n"
        "daemon:\n"
        "  retry_button_cap: 3\n",
        encoding="utf-8",
    )
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(web_app, "REPOS_DIR", str(tmp_path / "repos"))
    repo_dir = tmp_path / "repos" / "example__alpha"
    (repo_dir / "tasks").mkdir(parents=True)
    (repo_dir / "tasks" / f"{task_name}.md").write_text(
        f"---\nstatus: {status}\n---\n\n# {task_name}: Retry me\n\nBody\n",
        encoding="utf-8",
    )
    return repo_dir


def _snapshot(tasks: list[QueueTask]) -> str:
    state = RepoState(
        url="https://github.com/example/alpha.git",
        name="example__alpha",
        state=PipelineState.IDLE,
        current_queue=tasks,
    )
    return state.model_dump_json()


def test_retry_increments_counter_clears_cause_writes_queued(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo_dir = _write_config_and_task(tmp_path, monkeypatch)
    redis_client = _RetryRedis(
        {
            "pipeline:example__alpha": _snapshot(
                [
                    QueueTask(
                        pr_id="PR-283",
                        title="Retry me",
                        status=TaskStatus.ERROR,
                        task_file="tasks/PR-283.md",
                    )
                ]
            ),
            cause_key("example__alpha", "PR-283"): "{}",
        }
    )
    monkeypatch.setattr(web_app, "aioredis", _aioredis(redis_client))

    git_calls: list[list[str]] = []

    def fake_run(args: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        git_calls.append(args)
        return subprocess.CompletedProcess(args, 0, "", "")

    monkeypatch.setattr(repo_control.subprocess, "run", fake_run)

    with TestClient(app) as client:
        response = client.post("/repos/example__alpha/tasks/PR-283/retry")

    assert response.status_code == 200
    assert redis_client.store["metrics:retry_count:example__alpha:PR-283"] == "1"
    assert redis_client.expiries["metrics:retry_count:example__alpha:PR-283"] == 30 * 24 * 3600
    assert cause_key("example__alpha", "PR-283") not in redis_client.store
    assert redis_client.deleted == [cause_key("example__alpha", "PR-283")]
    assert redis_client.zremmed == [(index_key("example__alpha"), ("PR-283",))]
    assert "status: TODO" in (repo_dir / "tasks" / "PR-283.md").read_text(encoding="utf-8")
    assert ["git", "-C", str(repo_dir), "add", "tasks/PR-283.md"] in git_calls
    assert [
        "git",
        "-C",
        str(repo_dir),
        "commit",
        "-m",
        "[RETRY] PR-283 cleared by operator (attempt 1/3)",
        "-m",
        "[skip ci]",
    ] in git_calls
    assert ["git", "-C", str(repo_dir), "push", "origin", "HEAD:main"] in git_calls
    assert "TODO" in response.text
    assert "PR-283" in response.text


def test_retry_at_cap_returns_409(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo_dir = _write_config_and_task(tmp_path, monkeypatch)
    redis_client = _RetryRedis(
        {
            "pipeline:example__alpha": _snapshot(
                [QueueTask(pr_id="PR-283", title="Retry me", status=TaskStatus.ERROR)]
            ),
            "metrics:retry_count:example__alpha:PR-283": "3",
            cause_key("example__alpha", "PR-283"): "{}",
        }
    )
    monkeypatch.setattr(web_app, "aioredis", _aioredis(redis_client))

    git_called = False

    def fake_run(args: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        nonlocal git_called
        git_called = True
        return subprocess.CompletedProcess(args, 0, "", "")

    monkeypatch.setattr(repo_control.subprocess, "run", fake_run)

    with TestClient(app) as client:
        response = client.post("/repos/example__alpha/tasks/PR-283/retry")

    assert response.status_code == 409
    assert "Edit task spec or delete to proceed" in response.text
    assert redis_client.store["metrics:retry_count:example__alpha:PR-283"] == "3"
    assert cause_key("example__alpha", "PR-283") in redis_client.store
    assert "status: ERROR" in (repo_dir / "tasks" / "PR-283.md").read_text(encoding="utf-8")
    assert git_called is False


def test_retry_unknown_repo_returns_404(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _write_config_and_task(tmp_path, monkeypatch)
    monkeypatch.setattr(web_app, "aioredis", _aioredis(_RetryRedis()))

    with TestClient(app) as client:
        response = client.post("/repos/example__missing/tasks/PR-283/retry")

    assert response.status_code == 404
    assert "Repository not found" in response.text


def test_retry_unknown_task_returns_404(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _write_config_and_task(tmp_path, monkeypatch)
    monkeypatch.setattr(web_app, "aioredis", _aioredis(_RetryRedis()))

    with TestClient(app) as client:
        response = client.post("/repos/example__alpha/tasks/PR-999/retry")

    assert response.status_code == 404
    assert "Task file not found" in response.text


@pytest.mark.asyncio
async def test_retry_count_helpers_handle_bad_values() -> None:
    class _BoomRedis:
        async def get(self, key: str) -> str:
            raise RuntimeError("redis down")

    assert repo_control._decode_retry_count(b"2") == 2
    assert repo_control._decode_retry_count("bad") == 0
    assert await repo_control._get_retry_count(_BoomRedis(), "repo", "PR-1") == 0


@pytest.mark.asyncio
async def test_increment_retry_count_rejects_cap() -> None:
    redis_client = _RetryRedis({"metrics:retry_count:repo:PR-1": "2"})

    with pytest.raises(repo_control._RetryCapExceeded):
        await repo_control._increment_retry_count(redis_client, "repo", "PR-1", cap=2)


@pytest.mark.parametrize(
    ("content", "expected"),
    [
        ("# PR-1: No frontmatter\n", None),
        ("---\n---\n# PR-1: Missing status\n", None),
        ("---\ntitle: Test\nstatus: ERROR # retry\n---\n", TaskStatus.ERROR),
        ("---\nstatus: maybe\n---\n", None),
        ("---\ntitle: Test\n", None),
    ],
)
def test_read_task_frontmatter_status_variants(
    tmp_path: Path,
    content: str,
    expected: TaskStatus | None,
) -> None:
    task_path = tmp_path / "PR-1.md"
    task_path.write_text(content, encoding="utf-8")

    assert repo_control._read_task_frontmatter_status(task_path) == expected


def test_retry_invalid_pr_id_returns_400(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _write_config_and_task(tmp_path, monkeypatch)
    monkeypatch.setattr(web_app, "aioredis", _aioredis(_RetryRedis()))

    with TestClient(app) as client:
        response = client.post("/repos/example__alpha/tasks/not-a-pr/retry")

    assert response.status_code == 400
    assert "Invalid task identifier" in response.text


def test_retry_without_redis_returns_503(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _write_config_and_task(tmp_path, monkeypatch)
    monkeypatch.delattr(web_app.app.state, "redis", raising=False)

    client = TestClient(app)
    response = client.post("/repos/example__alpha/tasks/PR-283/retry")

    assert response.status_code == 503
    assert "Redis unavailable" in response.text


def test_retry_counter_failure_returns_503(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _write_config_and_task(tmp_path, monkeypatch)

    class _BoomTransactionRedis(_RetryRedis):
        async def transaction(self, callback: Any, *keys: str, value_from_callable: bool = False) -> Any:
            raise RuntimeError("boom")

    monkeypatch.setattr(web_app, "aioredis", _aioredis(_BoomTransactionRedis()))

    with TestClient(app) as client:
        response = client.post("/repos/example__alpha/tasks/PR-283/retry")

    assert response.status_code == 503
    assert "Failed to update retry counter" in response.text


def test_retry_cause_clear_failure_returns_503(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _write_config_and_task(tmp_path, monkeypatch)

    class _BoomDeleteRedis(_RetryRedis):
        async def delete(self, key: str) -> int:
            raise RuntimeError("boom")

    monkeypatch.setattr(web_app, "aioredis", _aioredis(_BoomDeleteRedis()))

    with TestClient(app) as client:
        response = client.post("/repos/example__alpha/tasks/PR-283/retry")

    assert response.status_code == 503
    assert "Failed to clear cancellation cause" in response.text
    assert "metrics:retry_count:example__alpha:PR-283" not in app.state.redis.store


def test_retry_frontmatter_write_failure_returns_503(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _write_config_and_task(tmp_path, monkeypatch)
    monkeypatch.setattr(web_app, "aioredis", _aioredis(_RetryRedis()))
    monkeypatch.setattr(
        repo_control,
        "write_frontmatter_status",
        lambda task_path, status: (_ for _ in ()).throw(OSError("boom")),
    )

    with TestClient(app) as client:
        response = client.post("/repos/example__alpha/tasks/PR-283/retry")

    assert response.status_code == 503
    assert "Failed to update task status" in response.text
    assert "metrics:retry_count:example__alpha:PR-283" not in app.state.redis.store


def test_retry_rejects_resolved_task_outside_repo(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _write_config_and_task(tmp_path, monkeypatch)
    monkeypatch.setattr(web_app, "aioredis", _aioredis(_RetryRedis()))
    outside = tmp_path / "outside.md"

    async def fake_resolve(name: str, pr_id: str) -> tuple[Path, str]:
        return outside, "outside.md"

    monkeypatch.setattr(repo_control, "_resolve_repo_task_path", fake_resolve)
    monkeypatch.setattr(repo_control, "write_frontmatter_status", lambda task_path, status: None)

    with TestClient(app) as client:
        response = client.post("/repos/example__alpha/tasks/PR-283/retry")

    assert response.status_code == 404
    assert "Task file not found" in response.text
    assert "metrics:retry_count:example__alpha:PR-283" not in app.state.redis.store


def test_retry_git_failure_returns_503(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _write_config_and_task(tmp_path, monkeypatch)
    monkeypatch.setattr(web_app, "aioredis", _aioredis(_RetryRedis()))

    def fake_run(args: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        raise subprocess.CalledProcessError(1, args)

    monkeypatch.setattr(repo_control.subprocess, "run", fake_run)

    with TestClient(app) as client:
        response = client.post("/repos/example__alpha/tasks/PR-283/retry")

    assert response.status_code == 503
    assert "Failed to commit retry change" in response.text
    assert "metrics:retry_count:example__alpha:PR-283" not in app.state.redis.store


def test_retry_commit_failure_returns_503_without_increment(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _write_config_and_task(tmp_path, monkeypatch)
    monkeypatch.setattr(web_app, "aioredis", _aioredis(_RetryRedis()))

    def fake_run(args: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        if args[3] == "commit":
            raise subprocess.CalledProcessError(1, args, stderr="fatal: bad revision")
        return subprocess.CompletedProcess(args, 0, "", "")

    monkeypatch.setattr(repo_control.subprocess, "run", fake_run)

    with TestClient(app) as client:
        response = client.post("/repos/example__alpha/tasks/PR-283/retry")

    assert response.status_code == 503
    assert "Failed to commit retry change" in response.text
    assert "metrics:retry_count:example__alpha:PR-283" not in app.state.redis.store


def test_retry_push_failure_can_retry_existing_local_commit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo_dir = _write_config_and_task(tmp_path, monkeypatch)
    redis_client = _RetryRedis()
    monkeypatch.setattr(web_app, "aioredis", _aioredis(redis_client))

    push_attempts = 0
    commit_attempts = 0
    git_calls: list[list[str]] = []

    def fake_run(args: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        nonlocal commit_attempts, push_attempts
        git_calls.append(args)
        if args[3] == "log":
            return subprocess.CompletedProcess(
                args,
                0,
                "[RETRY] PR-283 cleared by operator (attempt 1/3)\n",
                "",
            )
        if args[3] == "commit":
            commit_attempts += 1
            if commit_attempts == 2:
                raise subprocess.CalledProcessError(
                    1,
                    args,
                    output="On branch main\nnothing to commit, working tree clean\n",
                )
        if args[3] == "push":
            push_attempts += 1
            if push_attempts == 1:
                raise subprocess.CalledProcessError(1, args, stderr="rejected")
        return subprocess.CompletedProcess(args, 0, "", "")

    monkeypatch.setattr(repo_control.subprocess, "run", fake_run)

    with TestClient(app) as client:
        first = client.post("/repos/example__alpha/tasks/PR-283/retry")
        second = client.post("/repos/example__alpha/tasks/PR-283/retry")

    assert first.status_code == 503
    assert second.status_code == 200
    assert redis_client.store["metrics:retry_count:example__alpha:PR-283"] == "1"
    assert push_attempts == 2
    assert commit_attempts == 2
    assert ["git", "-C", str(repo_dir), "push", "origin", "HEAD:main"] in git_calls


def test_retry_rejects_task_not_in_error(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _write_config_and_task(tmp_path, monkeypatch, status="DONE")
    redis_client = _RetryRedis()
    monkeypatch.setattr(web_app, "aioredis", _aioredis(redis_client))

    git_called = False

    def fake_run(args: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        nonlocal git_called
        git_called = True
        return subprocess.CompletedProcess(args, 0, "", "")

    monkeypatch.setattr(repo_control.subprocess, "run", fake_run)

    with TestClient(app) as client:
        response = client.post("/repos/example__alpha/tasks/PR-283/retry")

    assert response.status_code == 409
    assert "Task is not in ERROR" in response.text
    assert "metrics:retry_count:example__alpha:PR-283" not in redis_client.store
    assert git_called is False


def test_retry_rejects_already_pushed_retry_commit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _write_config_and_task(tmp_path, monkeypatch, status="TODO")
    redis_client = _RetryRedis()
    monkeypatch.setattr(web_app, "aioredis", _aioredis(redis_client))

    def fake_run(args: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        if args[3] == "commit":
            raise subprocess.CalledProcessError(
                1,
                args,
                output="On branch main\nnothing to commit, working tree clean\n",
            )
        if args[3] == "log":
            return subprocess.CompletedProcess(
                args,
                0,
                "[RETRY] PR-283 cleared by operator (attempt 1/3)\n",
                "",
            )
        if args[3] == "push":
            return subprocess.CompletedProcess(args, 0, "Everything up-to-date\n", "")
        return subprocess.CompletedProcess(args, 0, "", "")

    monkeypatch.setattr(repo_control.subprocess, "run", fake_run)

    with TestClient(app) as client:
        response = client.post("/repos/example__alpha/tasks/PR-283/retry")

    assert response.status_code == 409
    assert "Task is not in ERROR" in response.text
    assert "metrics:retry_count:example__alpha:PR-283" not in redis_client.store


def test_retry_noop_commit_without_replay_permission_is_not_retryable(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo_dir = tmp_path / "repo"
    repo_dir.mkdir()

    def fake_run(args: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        if args[3] == "commit":
            raise subprocess.CalledProcessError(
                1,
                args,
                output="On branch main\nnothing to commit, working tree clean\n",
            )
        return subprocess.CompletedProcess(args, 0, "", "")

    monkeypatch.setattr(repo_control.subprocess, "run", fake_run)

    with pytest.raises(repo_control._TaskNotRetryable):
        repo_control._commit_and_push_retry_reset(
            repo_dir,
            Path("tasks/PR-283.md"),
            "[RETRY] PR-283 cleared by operator (attempt 1/3)",
            allow_existing_retry_commit=False,
        )


def test_retry_git_commands_run_in_thread(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo_dir = _write_config_and_task(tmp_path, monkeypatch)
    redis_client = _RetryRedis()
    monkeypatch.setattr(web_app, "aioredis", _aioredis(redis_client))

    to_thread_calls: list[tuple[Any, tuple[Any, ...], dict[str, Any]]] = []

    async def fake_to_thread(func: Any, *args: Any, **kwargs: Any) -> Any:
        to_thread_calls.append((func, args, kwargs))
        return func(*args, **kwargs)

    monkeypatch.setattr(repo_control.asyncio, "to_thread", fake_to_thread)
    monkeypatch.setattr(
        repo_control.subprocess,
        "run",
        lambda args, **kwargs: subprocess.CompletedProcess(args, 0, "", ""),
    )

    with TestClient(app) as client:
        response = client.post("/repos/example__alpha/tasks/PR-283/retry")

    assert response.status_code == 200
    assert to_thread_calls == [
        (
            repo_control._commit_and_push_retry_reset,
            (
                repo_dir,
                Path("tasks/PR-283.md"),
                "[RETRY] PR-283 cleared by operator (attempt 1/3)",
            ),
            {"allow_existing_retry_commit": False},
        )
    ]


def test_retry_post_push_counter_cap_returns_409(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _write_config_and_task(tmp_path, monkeypatch)
    monkeypatch.setattr(web_app, "aioredis", _aioredis(_RetryRedis()))
    monkeypatch.setattr(
        repo_control.subprocess,
        "run",
        lambda args, **kwargs: subprocess.CompletedProcess(args, 0, "", ""),
    )

    async def fake_increment(
        redis_client: Any,
        repo_slug: str,
        task_id: str,
        cap: int,
    ) -> int:
        raise repo_control._RetryCapExceeded(cap, cap)

    monkeypatch.setattr(repo_control, "_increment_retry_count", fake_increment)

    with TestClient(app) as client:
        response = client.post("/repos/example__alpha/tasks/PR-283/retry")

    assert response.status_code == 409
    assert "Edit task spec or delete to proceed" in response.text


def test_retry_post_push_counter_failure_returns_503(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _write_config_and_task(tmp_path, monkeypatch)
    monkeypatch.setattr(web_app, "aioredis", _aioredis(_RetryRedis()))
    monkeypatch.setattr(
        repo_control.subprocess,
        "run",
        lambda args, **kwargs: subprocess.CompletedProcess(args, 0, "", ""),
    )

    async def fake_increment(
        redis_client: Any,
        repo_slug: str,
        task_id: str,
        cap: int,
    ) -> int:
        raise RuntimeError("redis down")

    monkeypatch.setattr(repo_control, "_increment_retry_count", fake_increment)

    with TestClient(app) as client:
        response = client.post("/repos/example__alpha/tasks/PR-283/retry")

    assert response.status_code == 503
    assert "Failed to update retry counter" in response.text


def test_retry_success_without_snapshot_returns_single_todo_fragment(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo_dir = _write_config_and_task(tmp_path, monkeypatch)
    monkeypatch.setattr(web_app, "aioredis", _aioredis(_RetryRedis()))
    monkeypatch.setattr(
        repo_control.subprocess,
        "run",
        lambda args, **kwargs: subprocess.CompletedProcess(args, 0, "", ""),
    )

    with TestClient(app) as client:
        response = client.post("/repos/example__alpha/tasks/PR-283/retry")

    assert response.status_code == 200
    assert "1 total" in response.text
    assert "TODO" in response.text
    assert "PR-283" in response.text
    assert "status: TODO" in (repo_dir / "tasks" / "PR-283.md").read_text(encoding="utf-8")
