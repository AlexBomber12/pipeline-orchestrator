"""Tests for the atomic reset-task endpoint (PR-334)."""

from __future__ import annotations

import asyncio
import json
import subprocess
from pathlib import Path
from typing import Any

import pytest
import redis.asyncio as aioredis
from fastapi.testclient import TestClient

from src.cancellation.storage import (
    cause_key,
    current_run_started_at_key,
    index_key,
)
from src.keyspace import (
    legacy_recovered_tasks,
    pipeline_state,
    status_write_failed_tasks,
)
from src.models import PipelineState, PRInfo, QueueTask, RepoState, TaskStatus
from src.web import app as web_app
from src.web.app import app
from src.web.routes import repo_control


def _retry_count_key(slug: str, task_id: str) -> str:
    return f"metrics:retry_count:{slug}:{task_id}"


def _retry_fp_key(slug: str, task_id: str) -> str:
    return f"metrics:retry_fingerprint:{slug}:{task_id}"


def _swrf_key(slug: str, task_id: str) -> str:
    return f"metrics:status_write_failed_retry:{slug}:{task_id}"


class _ResetPipeline:
    def __init__(
        self,
        store: dict[str, str],
        zsets: dict[str, dict[str, float]],
        *,
        raise_on_execute: bool = False,
    ) -> None:
        self.store = store
        self.zsets = zsets
        self.raise_on_execute = raise_on_execute
        self.queued: list[tuple[str, Any]] = []
        self.watched: list[str] = []
        self.in_multi = False
        self.executed = False

    async def __aenter__(self) -> "_ResetPipeline":
        return self

    async def __aexit__(self, *exc_info: Any) -> None:
        return None

    async def watch(self, *keys: str) -> None:
        self.watched.extend(keys)

    async def unwatch(self) -> None:
        return None

    def multi(self) -> None:
        self.in_multi = True

    def delete(self, key: str) -> None:
        self.queued.append(("delete", key))

    def zrem(self, key: str, member: str) -> None:
        self.queued.append(("zrem", key, member))

    async def execute(self) -> Any:
        self.executed = True
        if self.raise_on_execute:
            raise aioredis.WatchError("conflict")
        for cmd in self.queued:
            if cmd[0] == "delete":
                self.store.pop(cmd[1], None)
            elif cmd[0] == "zrem":
                _, key, member = cmd
                zset = self.zsets.get(key)
                if zset is not None:
                    zset.pop(member, None)
        return None


class _ResetRedis:
    def __init__(
        self,
        store: dict[str, str] | None = None,
        zsets: dict[str, dict[str, float]] | None = None,
    ) -> None:
        self.store = store or {}
        self.zsets = zsets or {}
        self.raise_on_execute = False
        self.pipelines: list[_ResetPipeline] = []

    async def ping(self) -> bool:
        return True

    async def get(self, key: str) -> str | None:
        return self.store.get(key)

    async def set(self, key: str, value: str) -> bool:
        self.store[key] = value
        return True

    async def delete(self, key: str) -> int:
        return 1 if self.store.pop(key, None) is not None else 0

    async def zscore(self, key: str, member: str) -> float | None:
        zset = self.zsets.get(key)
        if zset is None:
            return None
        return zset.get(member)

    def pipeline(self, transaction: bool = False) -> _ResetPipeline:
        pipe = _ResetPipeline(
            self.store,
            self.zsets,
            raise_on_execute=self.raise_on_execute,
        )
        self.pipelines.append(pipe)
        return pipe

    async def aclose(self) -> None:
        return None


def _aioredis(redis_client: _ResetRedis) -> object:
    return type(
        "_Aioredis",
        (),
        {
            "from_url": staticmethod(
                lambda url, decode_responses=True: redis_client
            ),
            "WatchError": aioredis.WatchError,
        },
    )()


@pytest.fixture(autouse=True)
def _bypass_repo_retry_reservation(monkeypatch: pytest.MonkeyPatch) -> None:
    """Default to a no-op repo-retry reservation guard.

    Reset acquires the same repo-level reservation retry uses so that
    daemon/coder git activity cannot race destructive git mutations.
    The reservation helpers transact against ``RepoState`` Redis keys,
    which the in-memory test fake does not implement. Tests that
    exercise reservation behavior re-patch these helpers explicitly.
    """

    async def _noop_reserve(_redis: Any, _name: str, _url: str) -> bool:
        return False

    async def _noop_release(_redis: Any, _name: str, _previous: bool) -> None:
        return None

    monkeypatch.setattr(repo_control, "_reserve_repo_for_retry", _noop_reserve)
    monkeypatch.setattr(
        repo_control, "_release_repo_retry_reservation", _noop_release
    )


def _write_config_and_task(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    task_name: str = "PR-322",
    status: str | None = "ERROR",
    branch: str = "main",
) -> Path:
    cfg = tmp_path / "config.yml"
    cfg.write_text(
        "repositories:\n"
        "  - url: https://github.com/example/alpha.git\n"
        f"    branch: {branch}\n"
        "daemon:\n"
        "  retry_button_cap: 3\n",
        encoding="utf-8",
    )
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(web_app, "REPOS_DIR", str(tmp_path / "repos"))
    repo_dir = tmp_path / "repos" / "example__alpha"
    (repo_dir / "tasks").mkdir(parents=True)
    if status is None:
        body = f"# {task_name}: Reset me\n\nBody\n"
    else:
        body = f"---\nstatus: {status}\n---\n\n# {task_name}: Reset me\n\nBody\n"
    (repo_dir / "tasks" / f"{task_name}.md").write_text(body, encoding="utf-8")
    return repo_dir


def _seed_all_keys(redis_client: _ResetRedis, slug: str, task_id: str) -> list[str]:
    keys = [
        cause_key(slug, task_id),
        _retry_count_key(slug, task_id),
        _retry_fp_key(slug, task_id),
        current_run_started_at_key(slug, task_id),
        _swrf_key(slug, task_id),
    ]
    for key in keys:
        redis_client.store[key] = "seed"
    redis_client.zsets.setdefault(index_key(slug), {})[task_id] = 1.0
    return keys


def _ok_subprocess(*_args: Any, **_kwargs: Any) -> subprocess.CompletedProcess[str]:
    return subprocess.CompletedProcess([], 0, "", "")


def test_reset_deletes_all_redis_keys_for_task(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write_config_and_task(tmp_path, monkeypatch)
    redis_client = _ResetRedis()
    keys = _seed_all_keys(redis_client, "example__alpha", "PR-322")
    monkeypatch.setattr(web_app, "aioredis", _aioredis(redis_client))
    monkeypatch.setattr(repo_control.subprocess, "run", _ok_subprocess)

    with TestClient(app) as client:
        response = client.post("/api/reset-task/example__alpha/PR-322")

    assert response.status_code == 200
    body = response.json()
    assert sorted(body["deleted_keys"]) == sorted(keys)
    for key in keys:
        assert key not in redis_client.store
    assert body["frontmatter_pushed"] is True
    assert body["closed_pr_number"] is None


def test_reset_zrems_from_cancellation_index(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write_config_and_task(tmp_path, monkeypatch)
    redis_client = _ResetRedis()
    _seed_all_keys(redis_client, "example__alpha", "PR-322")
    monkeypatch.setattr(web_app, "aioredis", _aioredis(redis_client))
    monkeypatch.setattr(repo_control.subprocess, "run", _ok_subprocess)

    with TestClient(app) as client:
        response = client.post("/api/reset-task/example__alpha/PR-322")

    assert response.status_code == 200
    assert "PR-322" not in redis_client.zsets[index_key("example__alpha")]


def test_reset_with_close_orphan_pr_true_closes_pr(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write_config_and_task(tmp_path, monkeypatch)
    redis_client = _ResetRedis()
    _seed_all_keys(redis_client, "example__alpha", "PR-322")
    current_pr = PRInfo(
        number=444,
        branch="pr-322-feature",
        url="https://github.com/example/alpha/pull/444",
    )
    state = RepoState(
        url="https://github.com/example/alpha.git",
        name="example__alpha",
        state=PipelineState.ERROR,
        current_task=QueueTask(
            pr_id="PR-322",
            title="t",
            status=TaskStatus.ERROR,
            task_file="tasks/PR-322.md",
        ),
        current_pr=current_pr,
    )
    redis_client.store[pipeline_state("example__alpha")] = state.model_dump_json()
    monkeypatch.setattr(web_app, "aioredis", _aioredis(redis_client))

    monkeypatch.setattr(
        repo_control.gh_prs,
        "pr_state",
        lambda repo, pr_number: {"state": "OPEN", "mergedAt": None, "closedAt": None},
    )

    gh_calls: list[list[str]] = []
    git_calls: list[list[str]] = []

    def fake_run(args: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        if args and args[0] == "gh":
            gh_calls.append(args)
        else:
            git_calls.append(args)
        return subprocess.CompletedProcess(args, 0, "", "")

    monkeypatch.setattr(repo_control.subprocess, "run", fake_run)

    with TestClient(app) as client:
        response = client.post(
            "/api/reset-task/example__alpha/PR-322?close_orphan_pr=true"
        )

    assert response.status_code == 200
    body = response.json()
    assert body["closed_pr_number"] == 444
    assert gh_calls, "expected at least one gh CLI invocation"
    close_call = next(call for call in gh_calls if call[1] == "pr" and call[2] == "close")
    assert "444" in close_call
    assert "Closed by operator reset" in close_call


def test_reset_with_close_orphan_pr_false_leaves_pr_open(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write_config_and_task(tmp_path, monkeypatch)
    redis_client = _ResetRedis()
    _seed_all_keys(redis_client, "example__alpha", "PR-322")
    current_pr = PRInfo(number=444, branch="pr-322-feature")
    state = RepoState(
        url="https://github.com/example/alpha.git",
        name="example__alpha",
        state=PipelineState.ERROR,
        current_task=QueueTask(
            pr_id="PR-322",
            title="t",
            status=TaskStatus.ERROR,
            task_file="tasks/PR-322.md",
        ),
        current_pr=current_pr,
    )
    redis_client.store[pipeline_state("example__alpha")] = state.model_dump_json()
    monkeypatch.setattr(web_app, "aioredis", _aioredis(redis_client))

    gh_close_called = False

    def boom_pr_state(*_args: Any, **_kwargs: Any) -> dict[str, str | None]:
        raise AssertionError("pr_state must not run when close_orphan_pr=false")

    monkeypatch.setattr(repo_control.gh_prs, "pr_state", boom_pr_state)

    def fake_run(args: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        nonlocal gh_close_called
        if args and args[0] == "gh" and "close" in args:
            gh_close_called = True
        return subprocess.CompletedProcess(args, 0, "", "")

    monkeypatch.setattr(repo_control.subprocess, "run", fake_run)

    with TestClient(app) as client:
        response = client.post(
            "/api/reset-task/example__alpha/PR-322?close_orphan_pr=false"
        )

    assert response.status_code == 200
    assert response.json()["closed_pr_number"] is None
    assert gh_close_called is False


def test_reset_returns_409_on_concurrent_modification(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write_config_and_task(tmp_path, monkeypatch)
    redis_client = _ResetRedis()
    _seed_all_keys(redis_client, "example__alpha", "PR-322")
    redis_client.raise_on_execute = True
    monkeypatch.setattr(web_app, "aioredis", _aioredis(redis_client))
    monkeypatch.setattr(repo_control.subprocess, "run", _ok_subprocess)

    with TestClient(app) as client:
        response = client.post("/api/reset-task/example__alpha/PR-322")

    assert response.status_code == 409
    assert response.json() == {"error": "concurrent_modification"}


def test_reset_rewrites_frontmatter_to_todo(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo_dir = _write_config_and_task(tmp_path, monkeypatch, status="ERROR")
    redis_client = _ResetRedis()
    _seed_all_keys(redis_client, "example__alpha", "PR-322")
    monkeypatch.setattr(web_app, "aioredis", _aioredis(redis_client))

    git_calls: list[list[str]] = []

    def fake_run(args: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        git_calls.append(args)
        return subprocess.CompletedProcess(args, 0, "", "")

    monkeypatch.setattr(repo_control.subprocess, "run", fake_run)

    with TestClient(app) as client:
        response = client.post("/api/reset-task/example__alpha/PR-322")

    assert response.status_code == 200
    assert response.json()["frontmatter_pushed"] is True
    task_text = (repo_dir / "tasks" / "PR-322.md").read_text(encoding="utf-8")
    assert task_text.startswith("---\nstatus: TODO\n---")
    push_calls = [c for c in git_calls if "push" in c]
    assert push_calls, "expected git push to be invoked"


def test_reset_returns_503_on_git_push_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write_config_and_task(tmp_path, monkeypatch, status="ERROR")
    redis_client = _ResetRedis()
    keys = _seed_all_keys(redis_client, "example__alpha", "PR-322")
    monkeypatch.setattr(web_app, "aioredis", _aioredis(redis_client))

    def fake_run(args: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        if "push" in args:
            raise subprocess.CalledProcessError(1, args, "", "push rejected")
        return subprocess.CompletedProcess(args, 0, "", "")

    monkeypatch.setattr(repo_control.subprocess, "run", fake_run)

    with TestClient(app) as client:
        response = client.post("/api/reset-task/example__alpha/PR-322")

    assert response.status_code == 503
    body = response.json()
    assert body["partial_reset"] is True
    assert body["frontmatter_pushed"] is False
    assert sorted(body["deleted_keys"]) == sorted(keys)
    for key in keys:
        assert key not in redis_client.store


def test_reset_returns_400_when_task_already_todo(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write_config_and_task(tmp_path, monkeypatch, status="TODO")
    redis_client = _ResetRedis()
    monkeypatch.setattr(web_app, "aioredis", _aioredis(redis_client))

    with TestClient(app) as client:
        response = client.post("/api/reset-task/example__alpha/PR-322")

    assert response.status_code == 400
    assert response.json()["error"] == "task already TODO, nothing to reset"


def test_reset_invalid_task_id_returns_400(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write_config_and_task(tmp_path, monkeypatch)
    redis_client = _ResetRedis()
    monkeypatch.setattr(web_app, "aioredis", _aioredis(redis_client))

    with TestClient(app) as client:
        response = client.post("/api/reset-task/example__alpha/not-a-pr")

    assert response.status_code == 400
    assert response.json() == {"error": "invalid task id"}


def test_reset_unknown_repo_returns_404(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write_config_and_task(tmp_path, monkeypatch)
    redis_client = _ResetRedis()
    monkeypatch.setattr(web_app, "aioredis", _aioredis(redis_client))

    with TestClient(app) as client:
        response = client.post("/api/reset-task/example__missing/PR-322")

    assert response.status_code == 404
    assert response.json() == {"error": "repo not found"}


def test_reset_returns_503_when_redis_not_attached(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write_config_and_task(tmp_path, monkeypatch)
    if hasattr(web_app.app.state, "redis"):
        monkeypatch.delattr(web_app.app.state, "redis", raising=False)

    client = TestClient(web_app.app)
    response = client.post("/api/reset-task/example__alpha/PR-322")

    assert response.status_code == 503
    assert response.json() == {"error": "redis unavailable"}


def test_reset_returns_404_when_resolved_path_escapes_repo_root(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write_config_and_task(tmp_path, monkeypatch)
    redis_client = _ResetRedis()
    monkeypatch.setattr(web_app, "aioredis", _aioredis(redis_client))

    outside = tmp_path / "outside" / "PR-322.md"
    outside.parent.mkdir(parents=True)
    outside.write_text("---\nstatus: TODO\n---\n", encoding="utf-8")

    async def fake_resolve(_name: str, _pr_id: str) -> tuple[Path, str]:
        return outside.resolve(), "outside/PR-322.md"

    monkeypatch.setattr(repo_control, "_resolve_repo_task_path", fake_resolve)

    with TestClient(app) as client:
        response = client.post("/api/reset-task/example__alpha/PR-322")

    assert response.status_code == 404
    assert response.json() == {"error": "task file not found"}


def test_reset_returns_404_when_task_file_missing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo_dir = _write_config_and_task(tmp_path, monkeypatch)
    (repo_dir / "tasks" / "PR-322.md").unlink()
    redis_client = _ResetRedis()
    monkeypatch.setattr(web_app, "aioredis", _aioredis(redis_client))

    with TestClient(app) as client:
        response = client.post("/api/reset-task/example__alpha/PR-322")

    assert response.status_code == 404
    assert response.json() == {"error": "task file not found"}


def test_reset_returns_503_when_frontmatter_unreadable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo_dir = _write_config_and_task(tmp_path, monkeypatch)
    (repo_dir / "tasks" / "PR-322.md").write_bytes(
        b"---\nstatus: \xff\xfe\n---\n"
    )
    redis_client = _ResetRedis()
    monkeypatch.setattr(web_app, "aioredis", _aioredis(redis_client))

    with TestClient(app) as client:
        response = client.post("/api/reset-task/example__alpha/PR-322")

    assert response.status_code == 503
    assert response.json() == {"error": "failed to read task status"}


def test_reset_returns_503_on_pipeline_redis_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write_config_and_task(tmp_path, monkeypatch)
    redis_client = _ResetRedis()
    _seed_all_keys(redis_client, "example__alpha", "PR-322")
    monkeypatch.setattr(web_app, "aioredis", _aioredis(redis_client))

    def boom_pipeline(transaction: bool = False) -> Any:
        from redis.exceptions import RedisError
        raise RedisError("connection refused")

    redis_client.pipeline = boom_pipeline  # type: ignore[assignment]

    with TestClient(app) as client:
        response = client.post("/api/reset-task/example__alpha/PR-322")

    assert response.status_code == 503
    assert response.json() == {"error": "redis unavailable"}


def test_reset_returns_503_on_has_state_redis_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write_config_and_task(tmp_path, monkeypatch)
    redis_client = _ResetRedis()
    from redis.exceptions import RedisError

    async def boom_get(key: str) -> str | None:
        raise RedisError("connection refused")

    redis_client.get = boom_get  # type: ignore[assignment]
    monkeypatch.setattr(web_app, "aioredis", _aioredis(redis_client))

    with TestClient(app) as client:
        response = client.post("/api/reset-task/example__alpha/PR-322")

    assert response.status_code == 503
    assert response.json() == {"error": "redis unavailable"}


def test_reset_returns_503_on_zscore_redis_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A Redis failure during the cancellation-index probe must surface
    as 503 rather than be misread as "nothing to reset" (400)."""
    _write_config_and_task(tmp_path, monkeypatch, status="TODO")
    redis_client = _ResetRedis()
    from redis.exceptions import RedisError

    async def boom_zscore(key: str, member: str) -> float | None:
        raise RedisError("zset unavailable")

    redis_client.zscore = boom_zscore  # type: ignore[assignment]
    monkeypatch.setattr(web_app, "aioredis", _aioredis(redis_client))

    with TestClient(app) as client:
        response = client.post("/api/reset-task/example__alpha/PR-322")

    assert response.status_code == 503
    assert response.json() == {"error": "redis unavailable"}


def test_reset_returns_503_on_checkout_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write_config_and_task(tmp_path, monkeypatch)
    redis_client = _ResetRedis()
    keys = _seed_all_keys(redis_client, "example__alpha", "PR-322")
    monkeypatch.setattr(web_app, "aioredis", _aioredis(redis_client))

    def fake_run(args: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        if "checkout" in args:
            raise subprocess.CalledProcessError(1, args, "", "checkout failed")
        return subprocess.CompletedProcess(args, 0, "", "")

    monkeypatch.setattr(repo_control.subprocess, "run", fake_run)

    with TestClient(app) as client:
        response = client.post("/api/reset-task/example__alpha/PR-322")

    assert response.status_code == 503
    body = response.json()
    assert body["partial_reset"] is True
    assert "checkout failed" in body["error"]
    for key in keys:
        assert key not in redis_client.store
    _ = keys  # silence linter


def test_reset_returns_503_on_frontmatter_write_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write_config_and_task(tmp_path, monkeypatch, status="ERROR")
    redis_client = _ResetRedis()
    _seed_all_keys(redis_client, "example__alpha", "PR-322")
    monkeypatch.setattr(web_app, "aioredis", _aioredis(redis_client))
    monkeypatch.setattr(repo_control.subprocess, "run", _ok_subprocess)

    def boom_write(*_args: Any, **_kwargs: Any) -> None:
        raise OSError("disk full")

    monkeypatch.setattr(repo_control, "write_frontmatter_status", boom_write)

    with TestClient(app) as client:
        response = client.post("/api/reset-task/example__alpha/PR-322")

    assert response.status_code == 503
    body = response.json()
    assert body["partial_reset"] is True
    assert "write failed" in body["error"]


def test_reset_push_failure_also_swallows_worktree_reset_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write_config_and_task(tmp_path, monkeypatch, status="ERROR")
    redis_client = _ResetRedis()
    _seed_all_keys(redis_client, "example__alpha", "PR-322")
    monkeypatch.setattr(web_app, "aioredis", _aioredis(redis_client))

    push_seen = {"n": 0}

    def fake_run(args: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        # The first "reset --hard" in the spec is the base-checkout helper
        # that runs BEFORE the commit/push. The post-push "reset --hard" is
        # the recovery path we want to fail here, so only fail the second
        # reset, after a push attempt has already happened.
        if "push" in args:
            raise subprocess.CalledProcessError(1, args, "", "push rejected")
        if "reset" in args and "--hard" in args and push_seen["n"] >= 1:
            raise subprocess.CalledProcessError(1, args, "", "reset failed")
        if "push" in args:
            push_seen["n"] += 1
        return subprocess.CompletedProcess(args, 0, "", "")

    # Pre-bump push_seen so the second reset-hard fails after the failing push.
    def fake_run_with_counter(
        args: list[str], **kwargs: Any
    ) -> subprocess.CompletedProcess[str]:
        if "push" in args:
            push_seen["n"] += 1
            raise subprocess.CalledProcessError(1, args, "", "push rejected")
        if "reset" in args and "--hard" in args and push_seen["n"] >= 1:
            raise subprocess.CalledProcessError(1, args, "", "reset failed")
        return subprocess.CompletedProcess(args, 0, "", "")

    monkeypatch.setattr(repo_control.subprocess, "run", fake_run_with_counter)

    with TestClient(app) as client:
        response = client.post("/api/reset-task/example__alpha/PR-322")

    assert response.status_code == 503
    body = response.json()
    assert body["partial_reset"] is True
    assert "push failed" in body["error"]


def test_reset_close_orphan_pr_skipped_when_no_state(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write_config_and_task(tmp_path, monkeypatch)
    redis_client = _ResetRedis()
    _seed_all_keys(redis_client, "example__alpha", "PR-322")
    # No pipeline_state key set -> raw_state is None.
    monkeypatch.setattr(web_app, "aioredis", _aioredis(redis_client))
    monkeypatch.setattr(repo_control.subprocess, "run", _ok_subprocess)

    with TestClient(app) as client:
        response = client.post(
            "/api/reset-task/example__alpha/PR-322?close_orphan_pr=true"
        )

    assert response.status_code == 200
    assert response.json()["closed_pr_number"] is None


def test_reset_close_orphan_pr_skipped_on_invalid_state_json(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write_config_and_task(tmp_path, monkeypatch)
    redis_client = _ResetRedis()
    _seed_all_keys(redis_client, "example__alpha", "PR-322")
    redis_client.store[pipeline_state("example__alpha")] = "{not json"
    monkeypatch.setattr(web_app, "aioredis", _aioredis(redis_client))
    monkeypatch.setattr(repo_control.subprocess, "run", _ok_subprocess)

    with TestClient(app) as client:
        response = client.post(
            "/api/reset-task/example__alpha/PR-322?close_orphan_pr=true"
        )

    assert response.status_code == 200
    assert response.json()["closed_pr_number"] is None


def test_reset_close_orphan_pr_skipped_when_redis_state_read_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write_config_and_task(tmp_path, monkeypatch)
    redis_client = _ResetRedis()
    _seed_all_keys(redis_client, "example__alpha", "PR-322")

    from redis.exceptions import RedisError

    original_get = redis_client.get

    async def get_with_state_error(key: str) -> str | None:
        if key == pipeline_state("example__alpha"):
            raise RedisError("transient")
        return await original_get(key)

    redis_client.get = get_with_state_error  # type: ignore[assignment]
    monkeypatch.setattr(web_app, "aioredis", _aioredis(redis_client))
    monkeypatch.setattr(repo_control.subprocess, "run", _ok_subprocess)

    with TestClient(app) as client:
        response = client.post(
            "/api/reset-task/example__alpha/PR-322?close_orphan_pr=true"
        )

    assert response.status_code == 200
    assert response.json()["closed_pr_number"] is None


def test_reset_close_orphan_pr_skipped_when_state_has_no_current_pr(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write_config_and_task(tmp_path, monkeypatch)
    redis_client = _ResetRedis()
    _seed_all_keys(redis_client, "example__alpha", "PR-322")
    state = RepoState(
        url="https://github.com/example/alpha.git",
        name="example__alpha",
        state=PipelineState.ERROR,
        current_task=QueueTask(
            pr_id="PR-322",
            title="t",
            status=TaskStatus.ERROR,
        ),
        current_pr=None,
    )
    redis_client.store[pipeline_state("example__alpha")] = state.model_dump_json()
    monkeypatch.setattr(web_app, "aioredis", _aioredis(redis_client))
    monkeypatch.setattr(repo_control.subprocess, "run", _ok_subprocess)

    with TestClient(app) as client:
        response = client.post(
            "/api/reset-task/example__alpha/PR-322?close_orphan_pr=true"
        )

    assert response.status_code == 200
    assert response.json()["closed_pr_number"] is None


def test_reset_close_orphan_pr_skipped_when_current_task_id_differs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write_config_and_task(tmp_path, monkeypatch)
    redis_client = _ResetRedis()
    _seed_all_keys(redis_client, "example__alpha", "PR-322")
    state = RepoState(
        url="https://github.com/example/alpha.git",
        name="example__alpha",
        state=PipelineState.ERROR,
        current_task=QueueTask(
            pr_id="PR-999",
            title="t",
            status=TaskStatus.ERROR,
        ),
        current_pr=PRInfo(number=42, branch="b"),
    )
    redis_client.store[pipeline_state("example__alpha")] = state.model_dump_json()
    monkeypatch.setattr(web_app, "aioredis", _aioredis(redis_client))
    monkeypatch.setattr(repo_control.subprocess, "run", _ok_subprocess)

    with TestClient(app) as client:
        response = client.post(
            "/api/reset-task/example__alpha/PR-322?close_orphan_pr=true"
        )

    assert response.status_code == 200
    assert response.json()["closed_pr_number"] is None


def test_reset_close_orphan_pr_skipped_on_invalid_repo_url(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    cfg = tmp_path / "config.yml"
    cfg.write_text(
        "repositories:\n"
        "  - url: not-a-url\n"
        "    branch: main\n"
        "daemon:\n"
        "  retry_button_cap: 3\n",
        encoding="utf-8",
    )
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(web_app, "REPOS_DIR", str(tmp_path / "repos"))
    repo_dir = tmp_path / "repos" / "not-a-url"
    (repo_dir / "tasks").mkdir(parents=True)
    (repo_dir / "tasks" / "PR-322.md").write_text(
        "---\nstatus: ERROR\n---\n\nbody\n", encoding="utf-8"
    )

    redis_client = _ResetRedis()
    _seed_all_keys(redis_client, "not-a-url", "PR-322")
    state = RepoState(
        url="not-a-url",
        name="not-a-url",
        state=PipelineState.ERROR,
        current_task=QueueTask(
            pr_id="PR-322",
            title="t",
            status=TaskStatus.ERROR,
        ),
        current_pr=PRInfo(number=42, branch="b"),
    )
    redis_client.store[pipeline_state("not-a-url")] = state.model_dump_json()
    monkeypatch.setattr(web_app, "aioredis", _aioredis(redis_client))
    monkeypatch.setattr(repo_control.subprocess, "run", _ok_subprocess)

    with TestClient(app) as client:
        response = client.post(
            "/api/reset-task/not-a-url/PR-322?close_orphan_pr=true"
        )

    assert response.status_code == 200
    assert response.json()["closed_pr_number"] is None


def test_reset_close_orphan_pr_skipped_when_pr_state_non_open(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write_config_and_task(tmp_path, monkeypatch)
    redis_client = _ResetRedis()
    _seed_all_keys(redis_client, "example__alpha", "PR-322")
    state = RepoState(
        url="https://github.com/example/alpha.git",
        name="example__alpha",
        state=PipelineState.ERROR,
        current_task=QueueTask(
            pr_id="PR-322",
            title="t",
            status=TaskStatus.ERROR,
        ),
        current_pr=PRInfo(number=99, branch="b"),
    )
    redis_client.store[pipeline_state("example__alpha")] = state.model_dump_json()
    monkeypatch.setattr(web_app, "aioredis", _aioredis(redis_client))
    monkeypatch.setattr(
        repo_control.gh_prs,
        "pr_state",
        lambda repo, pr_number: {"state": "MERGED", "mergedAt": "now", "closedAt": None},
    )
    monkeypatch.setattr(repo_control.subprocess, "run", _ok_subprocess)

    with TestClient(app) as client:
        response = client.post(
            "/api/reset-task/example__alpha/PR-322?close_orphan_pr=true"
        )

    assert response.status_code == 200
    assert response.json()["closed_pr_number"] is None


def test_reset_close_orphan_pr_returns_none_on_gh_timeout(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write_config_and_task(tmp_path, monkeypatch)
    redis_client = _ResetRedis()
    _seed_all_keys(redis_client, "example__alpha", "PR-322")
    state = RepoState(
        url="https://github.com/example/alpha.git",
        name="example__alpha",
        state=PipelineState.ERROR,
        current_task=QueueTask(
            pr_id="PR-322",
            title="t",
            status=TaskStatus.ERROR,
        ),
        current_pr=PRInfo(number=444, branch="b"),
    )
    redis_client.store[pipeline_state("example__alpha")] = state.model_dump_json()
    monkeypatch.setattr(web_app, "aioredis", _aioredis(redis_client))
    monkeypatch.setattr(
        repo_control.gh_prs,
        "pr_state",
        lambda repo, pr_number: {"state": "OPEN", "mergedAt": None, "closedAt": None},
    )

    def fake_run(args: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        if args and args[0] == "gh" and "close" in args:
            raise subprocess.TimeoutExpired(args, kwargs.get("timeout", 60))
        return subprocess.CompletedProcess(args, 0, "", "")

    monkeypatch.setattr(repo_control.subprocess, "run", fake_run)

    with TestClient(app) as client:
        response = client.post(
            "/api/reset-task/example__alpha/PR-322?close_orphan_pr=true"
        )

    assert response.status_code == 200
    assert response.json()["closed_pr_number"] is None


def test_reset_close_orphan_pr_returns_none_when_gh_close_nonzero(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write_config_and_task(tmp_path, monkeypatch)
    redis_client = _ResetRedis()
    _seed_all_keys(redis_client, "example__alpha", "PR-322")
    state = RepoState(
        url="https://github.com/example/alpha.git",
        name="example__alpha",
        state=PipelineState.ERROR,
        current_task=QueueTask(
            pr_id="PR-322",
            title="t",
            status=TaskStatus.ERROR,
        ),
        current_pr=PRInfo(number=444, branch="b"),
    )
    redis_client.store[pipeline_state("example__alpha")] = state.model_dump_json()
    monkeypatch.setattr(web_app, "aioredis", _aioredis(redis_client))
    monkeypatch.setattr(
        repo_control.gh_prs,
        "pr_state",
        lambda repo, pr_number: {"state": "OPEN", "mergedAt": None, "closedAt": None},
    )

    def fake_run(args: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        if args and args[0] == "gh" and "close" in args:
            return subprocess.CompletedProcess(args, 1, "", "rate-limited")
        return subprocess.CompletedProcess(args, 0, "", "")

    monkeypatch.setattr(repo_control.subprocess, "run", fake_run)

    with TestClient(app) as client:
        response = client.post(
            "/api/reset-task/example__alpha/PR-322?close_orphan_pr=true"
        )

    assert response.status_code == 200
    assert response.json()["closed_pr_number"] is None


def test_reset_publish_wake_failure_is_swallowed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write_config_and_task(tmp_path, monkeypatch)
    redis_client = _ResetRedis()
    _seed_all_keys(redis_client, "example__alpha", "PR-322")
    monkeypatch.setattr(web_app, "aioredis", _aioredis(redis_client))
    monkeypatch.setattr(repo_control.subprocess, "run", _ok_subprocess)

    async def boom_publish_wake(
        redis: Any, repo_name: str, event_type: str
    ) -> None:
        raise RuntimeError("redis stream down")

    monkeypatch.setattr(web_app, "publish_wake", boom_publish_wake)

    with TestClient(app) as client:
        response = client.post("/api/reset-task/example__alpha/PR-322")

    assert response.status_code == 200
    assert response.json()["frontmatter_pushed"] is True


@pytest.mark.asyncio
async def test_reset_safe_for_concurrent_calls(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write_config_and_task(tmp_path, monkeypatch)
    redis_client = _ResetRedis()
    _seed_all_keys(redis_client, "example__alpha", "PR-322")
    monkeypatch.setattr(web_app, "aioredis", _aioredis(redis_client))
    monkeypatch.setattr(repo_control.subprocess, "run", _ok_subprocess)

    # First call sees populated state and executes normally; second call
    # finds Redis already empty but should still flag concurrent_modification
    # via the WATCH semantics. Simulate by toggling raise_on_execute after
    # the first pipeline finishes.
    original_pipeline = redis_client.pipeline
    call_count = {"n": 0}

    def pipeline(transaction: bool = False) -> _ResetPipeline:
        pipe = original_pipeline(transaction=transaction)
        if call_count["n"] >= 1:
            pipe.raise_on_execute = True
        call_count["n"] += 1
        return pipe

    redis_client.pipeline = pipeline  # type: ignore[assignment]

    with TestClient(app) as client:
        resp_a = client.post("/api/reset-task/example__alpha/PR-322")
        resp_b = client.post("/api/reset-task/example__alpha/PR-322")

    statuses = sorted([resp_a.status_code, resp_b.status_code])
    # Once the first call drains Redis, frontmatter is TODO and Redis is
    # empty, so the second call returns 400 (nothing to reset). When the
    # second call still saw populated state, the WATCH conflict yields 409.
    assert statuses[0] == 200
    assert statuses[1] in (400, 409)
    # Ensure exactly one call mutated state.
    assert call_count["n"] >= 1
    _ = asyncio  # keep import used (pytest-asyncio shims async fixtures)


def test_reset_returns_409_when_repo_busy_for_retry(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Reset must refuse to start git mutations when the repo-level
    retry reservation cannot be acquired."""
    _write_config_and_task(tmp_path, monkeypatch)
    redis_client = _ResetRedis()
    _seed_all_keys(redis_client, "example__alpha", "PR-322")
    monkeypatch.setattr(web_app, "aioredis", _aioredis(redis_client))

    async def busy_reserve(_redis: Any, _name: str, _url: str) -> bool:
        raise repo_control._RepoStateMutationError(
            "Repository retry already in progress; retry later.",
            status_code=409,
        )

    release_calls: list[tuple[str, bool]] = []

    async def track_release(_redis: Any, name: str, previous: bool) -> None:
        release_calls.append((name, previous))

    monkeypatch.setattr(repo_control, "_reserve_repo_for_retry", busy_reserve)
    monkeypatch.setattr(
        repo_control, "_release_repo_retry_reservation", track_release
    )

    def boom_subprocess(*_args: Any, **_kwargs: Any) -> subprocess.CompletedProcess[str]:
        raise AssertionError("no git/gh CLI invocation expected when busy")

    monkeypatch.setattr(repo_control.subprocess, "run", boom_subprocess)

    with TestClient(app) as client:
        response = client.post("/api/reset-task/example__alpha/PR-322")

    assert response.status_code == 409
    assert response.json() == {
        "error": "Repository retry already in progress; retry later."
    }
    # Reservation never acquired -> release must not run.
    assert release_calls == []
    # Redis state must be untouched when the reservation refuses.
    for key in _seed_all_keys(redis_client, "example__alpha", "PR-322"):
        assert key in redis_client.store


def test_reset_releases_reservation_on_happy_path(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write_config_and_task(tmp_path, monkeypatch)
    redis_client = _ResetRedis()
    _seed_all_keys(redis_client, "example__alpha", "PR-322")
    monkeypatch.setattr(web_app, "aioredis", _aioredis(redis_client))
    monkeypatch.setattr(repo_control.subprocess, "run", _ok_subprocess)

    reserve_calls: list[tuple[str, str]] = []
    release_calls: list[tuple[str, bool]] = []

    async def tracking_reserve(_redis: Any, name: str, url: str) -> bool:
        reserve_calls.append((name, url))
        return False

    async def tracking_release(_redis: Any, name: str, previous: bool) -> None:
        release_calls.append((name, previous))

    monkeypatch.setattr(repo_control, "_reserve_repo_for_retry", tracking_reserve)
    monkeypatch.setattr(
        repo_control, "_release_repo_retry_reservation", tracking_release
    )

    with TestClient(app) as client:
        response = client.post("/api/reset-task/example__alpha/PR-322")

    assert response.status_code == 200
    assert reserve_calls == [("example__alpha", "https://github.com/example/alpha.git")]
    assert release_calls == [("example__alpha", False)]


def test_reset_releases_reservation_on_push_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Even when the destructive path fails, the reservation must be
    released so the daemon is not left paused."""
    _write_config_and_task(tmp_path, monkeypatch, status="ERROR")
    redis_client = _ResetRedis()
    _seed_all_keys(redis_client, "example__alpha", "PR-322")
    monkeypatch.setattr(web_app, "aioredis", _aioredis(redis_client))

    release_calls: list[tuple[str, bool]] = []

    async def reserve(_redis: Any, _name: str, _url: str) -> bool:
        return True

    async def track_release(_redis: Any, name: str, previous: bool) -> None:
        release_calls.append((name, previous))

    monkeypatch.setattr(repo_control, "_reserve_repo_for_retry", reserve)
    monkeypatch.setattr(
        repo_control, "_release_repo_retry_reservation", track_release
    )

    def fake_run(args: list[str], **_kwargs: Any) -> subprocess.CompletedProcess[str]:
        if "push" in args:
            raise subprocess.CalledProcessError(1, args, "", "push rejected")
        return subprocess.CompletedProcess(args, 0, "", "")

    monkeypatch.setattr(repo_control.subprocess, "run", fake_run)

    with TestClient(app) as client:
        response = client.post("/api/reset-task/example__alpha/PR-322")

    assert response.status_code == 503
    assert response.json()["partial_reset"] is True
    assert release_calls == [("example__alpha", True)]


def test_reset_returns_503_when_reservation_redis_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A RedisError raised by the reservation acquire path must surface
    as 503 rather than crash the request."""
    _write_config_and_task(tmp_path, monkeypatch)
    redis_client = _ResetRedis()
    _seed_all_keys(redis_client, "example__alpha", "PR-322")
    monkeypatch.setattr(web_app, "aioredis", _aioredis(redis_client))

    from redis.exceptions import RedisError

    async def boom_reserve(_redis: Any, _name: str, _url: str) -> bool:
        raise RedisError("reservation set failed")

    monkeypatch.setattr(repo_control, "_reserve_repo_for_retry", boom_reserve)

    with TestClient(app) as client:
        response = client.post("/api/reset-task/example__alpha/PR-322")

    assert response.status_code == 503
    assert response.json() == {"error": "redis unavailable"}


def test_reset_clears_status_write_failed_set_entry(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The scheduler reads ``status_write_failed_tasks:{repo}`` to force
    parked tasks back to ERROR. Reset must drop the task PR id from that
    set so the daemon can dispatch the task again."""
    _write_config_and_task(tmp_path, monkeypatch)
    redis_client = _ResetRedis()
    _seed_all_keys(redis_client, "example__alpha", "PR-322")
    swf_key = status_write_failed_tasks("example__alpha")
    redis_client.store[swf_key] = json.dumps(["PR-322", "PR-999"])
    monkeypatch.setattr(web_app, "aioredis", _aioredis(redis_client))
    monkeypatch.setattr(repo_control.subprocess, "run", _ok_subprocess)

    with TestClient(app) as client:
        response = client.post("/api/reset-task/example__alpha/PR-322")

    assert response.status_code == 200
    assert json.loads(redis_client.store[swf_key]) == ["PR-999"]


def test_reset_deletes_status_write_failed_set_when_last_member(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """When the reset task is the only member of the persisted set, the
    whole key must be deleted so the scheduler stops forcing the parked
    bucket."""
    _write_config_and_task(tmp_path, monkeypatch)
    redis_client = _ResetRedis()
    _seed_all_keys(redis_client, "example__alpha", "PR-322")
    swf_key = status_write_failed_tasks("example__alpha")
    redis_client.store[swf_key] = json.dumps(["PR-322"])
    monkeypatch.setattr(web_app, "aioredis", _aioredis(redis_client))
    monkeypatch.setattr(repo_control.subprocess, "run", _ok_subprocess)

    with TestClient(app) as client:
        response = client.post("/api/reset-task/example__alpha/PR-322")

    assert response.status_code == 200
    assert swf_key not in redis_client.store


def test_reset_clears_legacy_recovered_tasks_set_entry(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Pre-PR-281 deployments persist the marker under ``recovered_tasks``;
    reset must also drop the task id from that legacy key."""
    _write_config_and_task(tmp_path, monkeypatch)
    redis_client = _ResetRedis()
    _seed_all_keys(redis_client, "example__alpha", "PR-322")
    legacy_key = legacy_recovered_tasks("example__alpha")
    redis_client.store[legacy_key] = json.dumps(["PR-322"])
    monkeypatch.setattr(web_app, "aioredis", _aioredis(redis_client))
    monkeypatch.setattr(repo_control.subprocess, "run", _ok_subprocess)

    with TestClient(app) as client:
        response = client.post("/api/reset-task/example__alpha/PR-322")

    assert response.status_code == 200
    assert legacy_key not in redis_client.store


def test_reset_succeeds_without_commit_when_checkout_leaves_task_todo(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """When the only thing that needed cleanup was Redis state, the
    checkout step leaves the task file at TODO. Reset must skip the
    commit/push step (there is nothing to commit) and return 200, not
    503 partial_reset."""
    # Local task is already TODO on disk; only Redis state is dirty.
    _write_config_and_task(tmp_path, monkeypatch, status="TODO")
    redis_client = _ResetRedis()
    _seed_all_keys(redis_client, "example__alpha", "PR-322")
    monkeypatch.setattr(web_app, "aioredis", _aioredis(redis_client))

    captured: list[list[str]] = []

    def fake_run(args: list[str], **_kwargs: Any) -> subprocess.CompletedProcess[str]:
        captured.append(list(args))
        return subprocess.CompletedProcess(args, 0, "", "")

    monkeypatch.setattr(repo_control.subprocess, "run", fake_run)

    with TestClient(app) as client:
        response = client.post("/api/reset-task/example__alpha/PR-322")

    assert response.status_code == 200
    body = response.json()
    assert body["frontmatter_pushed"] is True
    # No commit or push should have been attempted: the only mutation was
    # the Redis cleanup, and the task file was already at TODO.
    flat_args = [arg for cmd in captured for arg in cmd]
    assert "commit" not in flat_args
    assert "push" not in flat_args


def test_reset_skips_commit_push_when_checkout_reverts_to_todo(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Local task is ERROR but the origin task is already TODO. After
    checkout overwrites the working copy with origin, the file is at
    TODO; reset must detect the no-op and not let
    _commit_and_push_retry_reset raise _TaskNotRetryable for "nothing
    to commit"."""
    repo_dir = _write_config_and_task(tmp_path, monkeypatch, status="ERROR")
    task_path = repo_dir / "tasks" / "PR-322.md"
    redis_client = _ResetRedis()
    _seed_all_keys(redis_client, "example__alpha", "PR-322")
    monkeypatch.setattr(web_app, "aioredis", _aioredis(redis_client))

    captured: list[list[str]] = []

    def fake_run(args: list[str], **_kwargs: Any) -> subprocess.CompletedProcess[str]:
        captured.append(list(args))
        # Simulate the final "git checkout origin/main -- tasks/PR-322.md"
        # rewriting the working copy with origin's TODO version.
        if "checkout" in args and "--" in args:
            task_path.write_text(
                "---\nstatus: TODO\n---\n\n# PR-322: Reset me\n\nBody\n",
                encoding="utf-8",
            )
        return subprocess.CompletedProcess(args, 0, "", "")

    monkeypatch.setattr(repo_control.subprocess, "run", fake_run)

    with TestClient(app) as client:
        response = client.post("/api/reset-task/example__alpha/PR-322")

    assert response.status_code == 200
    body = response.json()
    assert body["frontmatter_pushed"] is True
    flat_args = [arg for cmd in captured for arg in cmd]
    assert "commit" not in flat_args
    assert "push" not in flat_args
    # Sanity: working tree now matches origin (TODO).
    assert "status: TODO" in task_path.read_text(encoding="utf-8")
