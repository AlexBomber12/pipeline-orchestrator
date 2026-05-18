"""Tests for the daemon-wide pause/resume/stop/drain-progress endpoints.

PR-339 adds four daemon-level endpoints that compose the per-repo
controls (``pause_repo``, ``stop_repo``, ``resume_repo``) into one
sweep across every configured repo. The tests in this module exercise
the success paths, the operator-visible payload shape, and the
defensive fallbacks (Redis offline, decode failure, empty config) that
make the dashboard's planned daemon-wide buttons safe to re-click.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient
from src.keyspace import control_stop, pipeline_state
from src.models import PipelineState, QueueTask, RepoState, TaskStatus
from src.web import app as web_app
from src.web.app import app
from src.web.routes import daemon_control as daemon_control_routes


class _FakeTransactionPipe:
    """Stand-in for ``redis.asyncio.Pipeline`` used inside WATCH/MULTI/EXEC.

    Models the surface the daemon-wide endpoints exercise: an immediate
    ``get`` while watching, ``multi`` to switch into buffered mode, then
    queued ``set``/``delete`` calls applied when the transaction commits.
    Errors injected on the owning ``_FakeRedis`` (``get_error``,
    ``set_error``, ``pipeline_execute_error``) surface at the same points
    they would in production so the swallow-and-continue paths get
    realistic coverage.
    """

    def __init__(self, redis: "_FakeRedis") -> None:
        self._redis = redis
        self._ops: list[tuple[str, tuple[Any, ...]]] = []
        self._in_multi = False

    async def get(self, key: str) -> str | None:
        if self._redis.get_error is not None:
            raise self._redis.get_error
        return self._redis.values.get(key)

    def multi(self) -> None:
        self._in_multi = True

    def set(self, key: str, value: str, ex: int | None = None) -> "_FakeTransactionPipe":
        self._ops.append(("set", (key, value, ex)))
        return self

    def delete(self, *keys: str) -> "_FakeTransactionPipe":
        self._ops.append(("delete", keys))
        return self

    async def execute(self) -> list[Any]:
        if self._redis.pipeline_execute_error is not None:
            raise self._redis.pipeline_execute_error
        results: list[Any] = []
        for op, args in self._ops:
            if op == "set":
                key, value, ex = args
                if self._redis.set_error is not None:
                    raise self._redis.set_error
                self._redis.values[key] = value
                if ex is not None:
                    self._redis.ttls[key] = ex
                results.append(True)
            elif op == "delete":
                deleted = 0
                for key in args:
                    if self._redis.values.pop(key, None) is not None:
                        self._redis.ttls.pop(key, None)
                        deleted += 1
                results.append(deleted)
        return results


class _FakeRedis:
    """In-memory Redis stub covering only the ops the endpoints touch."""

    def __init__(self) -> None:
        self.values: dict[str, str] = {}
        self.ttls: dict[str, int] = {}
        self.published: list[tuple[str, str]] = []
        self.get_error: Exception | None = None
        self.set_error: Exception | None = None
        self.publish_error: Exception | None = None
        self.pipeline_execute_error: Exception | None = None
        self.transaction_calls: list[tuple[str, ...]] = []

    async def ping(self) -> bool:
        return True

    async def get(self, key: str) -> str | None:
        if self.get_error is not None:
            raise self.get_error
        return self.values.get(key)

    async def set(self, key: str, value: str, ex: int | None = None) -> bool:
        if self.set_error is not None:
            raise self.set_error
        self.values[key] = value
        if ex is not None:
            self.ttls[key] = ex
        return True

    async def delete(self, key: str) -> int:
        return 1 if self.values.pop(key, None) is not None else 0

    async def publish(self, channel: str, message: str) -> int:
        if self.publish_error is not None:
            raise self.publish_error
        self.published.append((channel, message))
        return 1

    async def transaction(
        self,
        callback: Any,
        *keys: str,
        value_from_callable: bool = False,
    ) -> Any:
        self.transaction_calls.append(keys)
        pipe = _FakeTransactionPipe(self)
        result = await callback(pipe)
        if pipe._ops:
            await pipe.execute()
        return result if value_from_callable else None

    async def aclose(self) -> None:
        return None


def _aioredis(redis_client: _FakeRedis | None) -> object:
    return type(
        "_Aioredis",
        (),
        {"from_url": staticmethod(lambda url, decode_responses=True: redis_client)},
    )()


def _stub_redis(monkeypatch: pytest.MonkeyPatch, redis_client: _FakeRedis | None) -> None:
    monkeypatch.setattr(web_app, "aioredis", _aioredis(redis_client))


_REPO_URLS = [
    "https://github.com/example/alpha.git",
    "https://github.com/example/beta.git",
    "https://github.com/example/gamma.git",
]
_REPO_SLUGS = ["example__alpha", "example__beta", "example__gamma"]


def _write_config(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    repo_urls: list[str] | None = None,
) -> None:
    urls = _REPO_URLS if repo_urls is None else repo_urls
    lines = ["repositories:"]
    for url in urls:
        lines.extend(
            [
                f"  - url: {url}",
                "    branch: main",
            ]
        )
    if not urls:
        lines = ["repositories: []"]
    lines.extend(["daemon:", "  poll_interval_sec: 60"])
    (tmp_path / "config.yml").write_text("\n".join(lines) + "\n", encoding="utf-8")
    monkeypatch.chdir(tmp_path)


def _seed_state(
    redis_client: _FakeRedis,
    slug: str,
    *,
    url: str,
    state: PipelineState = PipelineState.IDLE,
    user_paused: bool = False,
    current_task: QueueTask | None = None,
) -> None:
    repo_state = RepoState(
        url=url,
        name=slug,
        state=state,
        user_paused=user_paused,
        current_task=current_task,
    )
    redis_client.values[pipeline_state(slug)] = repo_state.model_dump_json()


def _stored_state(redis_client: _FakeRedis, slug: str) -> RepoState:
    return RepoState.model_validate_json(redis_client.values[pipeline_state(slug)])


def test_daemon_pause_sets_user_paused_for_all_repos(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write_config(tmp_path, monkeypatch)
    redis_client = _FakeRedis()
    _stub_redis(monkeypatch, redis_client)
    for slug, url in zip(_REPO_SLUGS, _REPO_URLS):
        _seed_state(redis_client, slug, url=url)

    with TestClient(app) as client:
        response = client.post("/daemon/pause")

    assert response.status_code == 200
    body = response.json()
    assert body == {"affected": _REPO_SLUGS, "count": 3}
    for slug in _REPO_SLUGS:
        assert _stored_state(redis_client, slug).user_paused is True


def test_daemon_pause_publishes_wake_for_each(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write_config(tmp_path, monkeypatch)
    redis_client = _FakeRedis()
    _stub_redis(monkeypatch, redis_client)

    with TestClient(app) as client:
        client.post("/daemon/pause")

    wake_channels = [channel for channel, _ in redis_client.published]
    assert wake_channels == [
        f"orchestrator:wake:{slug}" for slug in _REPO_SLUGS
    ]


def test_daemon_pause_empty_config_returns_zero(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write_config(tmp_path, monkeypatch, repo_urls=[])
    redis_client = _FakeRedis()
    _stub_redis(monkeypatch, redis_client)

    with TestClient(app) as client:
        response = client.post("/daemon/pause")

    assert response.status_code == 200
    assert response.json() == {"affected": [], "count": 0}
    assert redis_client.published == []


def test_daemon_pause_without_redis_still_returns_affected(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write_config(tmp_path, monkeypatch)
    _stub_redis(monkeypatch, None)

    with TestClient(app) as client:
        response = client.post("/daemon/pause")

    assert response.status_code == 200
    assert response.json() == {"affected": _REPO_SLUGS, "count": 3}


def test_daemon_pause_swallows_publish_wake_errors(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write_config(tmp_path, monkeypatch)
    redis_client = _FakeRedis()
    redis_client.publish_error = RuntimeError("pubsub down")
    _stub_redis(monkeypatch, redis_client)

    with TestClient(app) as client:
        response = client.post("/daemon/pause")

    assert response.status_code == 200
    # State writes still landed even though publish_wake raised.
    assert all(
        _stored_state(redis_client, slug).user_paused is True
        for slug in _REPO_SLUGS
    )


def test_daemon_pause_swallows_state_write_errors(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write_config(tmp_path, monkeypatch)
    redis_client = _FakeRedis()
    redis_client.set_error = RuntimeError("redis down")
    _stub_redis(monkeypatch, redis_client)

    with TestClient(app) as client:
        response = client.post("/daemon/pause")

    # All repos accounted for even when writes fail; failure is logged.
    assert response.json() == {"affected": _REPO_SLUGS, "count": 3}


def test_daemon_pause_recovers_from_state_decode_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write_config(tmp_path, monkeypatch, repo_urls=[_REPO_URLS[0]])
    redis_client = _FakeRedis()
    redis_client.values[pipeline_state(_REPO_SLUGS[0])] = "{not valid json"
    _stub_redis(monkeypatch, redis_client)

    with TestClient(app) as client:
        response = client.post("/daemon/pause")

    assert response.status_code == 200
    rewritten = _stored_state(redis_client, _REPO_SLUGS[0])
    assert rewritten.user_paused is True
    assert rewritten.state == PipelineState.IDLE


def test_daemon_pause_skips_write_on_redis_get_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A transient ``pipe.get`` failure must not overwrite live repo state.

    Falling back to a synthesized default would erase ``current_task``,
    ``current_pr``, and rate-limit metadata on the active repo. The
    endpoint instead skips the write for that repo (the outer
    try/except logs and continues the sweep), leaving the existing
    payload intact for the next pause attempt once Redis recovers.
    """
    _write_config(tmp_path, monkeypatch, repo_urls=[_REPO_URLS[0]])
    redis_client = _FakeRedis()
    _seed_state(
        redis_client,
        _REPO_SLUGS[0],
        url=_REPO_URLS[0],
        state=PipelineState.CODING,
        user_paused=False,
        current_task=QueueTask(
            pr_id="PR-100",
            title="Active task",
            status=TaskStatus.DOING,
            task_file="tasks/PR-100.md",
            branch="pr-100",
        ),
    )
    seeded_payload = redis_client.values[pipeline_state(_REPO_SLUGS[0])]
    redis_client.get_error = RuntimeError("redis read down")
    _stub_redis(monkeypatch, redis_client)

    with TestClient(app) as client:
        response = client.post("/daemon/pause")

    assert response.status_code == 200
    # Sweep still reports the repo as affected; the outer handler logs
    # the failure and moves on rather than aborting the request.
    assert response.json() == {"affected": [_REPO_SLUGS[0]], "count": 1}
    # The seeded state is unchanged — no synthesized default clobbered
    # the live ``current_task`` / ``user_paused`` fields.
    assert (
        redis_client.values[pipeline_state(_REPO_SLUGS[0])] == seeded_payload
    )


def test_daemon_resume_clears_user_paused_for_all(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write_config(tmp_path, monkeypatch)
    redis_client = _FakeRedis()
    _stub_redis(monkeypatch, redis_client)
    for slug, url in zip(_REPO_SLUGS, _REPO_URLS):
        _seed_state(redis_client, slug, url=url, user_paused=True)
        redis_client.values[control_stop(slug)] = "1"
        redis_client.ttls[control_stop(slug)] = 30

    with TestClient(app) as client:
        response = client.post("/daemon/resume")

    assert response.status_code == 200
    assert response.json() == {"affected": _REPO_SLUGS, "count": 3}
    for slug in _REPO_SLUGS:
        assert _stored_state(redis_client, slug).user_paused is False
        # Resume also clears any stop flag so the previous daemon stop
        # cannot survive into the next dispatch.
        assert control_stop(slug) not in redis_client.values
    wake_channels = [channel for channel, _ in redis_client.published]
    assert wake_channels == [
        f"orchestrator:wake:{slug}" for slug in _REPO_SLUGS
    ]


def test_daemon_resume_swallows_pipeline_failures(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write_config(tmp_path, monkeypatch, repo_urls=[_REPO_URLS[0]])
    redis_client = _FakeRedis()
    redis_client.pipeline_execute_error = RuntimeError("pipe down")
    _stub_redis(monkeypatch, redis_client)
    _seed_state(redis_client, _REPO_SLUGS[0], url=_REPO_URLS[0], user_paused=True)

    with TestClient(app) as client:
        response = client.post("/daemon/resume")

    # Pipeline failure does not mask the affected list.
    assert response.json() == {"affected": [_REPO_SLUGS[0]], "count": 1}


def test_daemon_resume_without_redis_returns_affected(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write_config(tmp_path, monkeypatch)
    _stub_redis(monkeypatch, None)

    with TestClient(app) as client:
        response = client.post("/daemon/resume")

    assert response.json() == {"affected": _REPO_SLUGS, "count": 3}


def test_daemon_stop_writes_control_stop_with_60s_ttl(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write_config(tmp_path, monkeypatch)
    redis_client = _FakeRedis()
    _stub_redis(monkeypatch, redis_client)

    with TestClient(app) as client:
        response = client.post("/daemon/stop")

    assert response.status_code == 200
    assert response.json() == {"affected": _REPO_SLUGS, "count": 3}
    for slug in _REPO_SLUGS:
        assert redis_client.values[control_stop(slug)] == "1"
        assert redis_client.ttls[control_stop(slug)] == 60


def test_daemon_stop_swallows_set_errors(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write_config(tmp_path, monkeypatch)
    redis_client = _FakeRedis()
    # Allow the heartbeat middleware to write its key by only failing
    # the control_stop writes; ``set`` is shared so we wrap selectively.
    real_set = redis_client.set

    async def _failing_set(key: str, value: str, ex: int | None = None) -> bool:
        if key.startswith("control:") and key.endswith(":stop"):
            raise RuntimeError("redis stop down")
        return await real_set(key, value, ex=ex)

    redis_client.set = _failing_set  # type: ignore[assignment]
    _stub_redis(monkeypatch, redis_client)

    with TestClient(app) as client:
        response = client.post("/daemon/stop")

    assert response.json() == {"affected": _REPO_SLUGS, "count": 3}
    for slug in _REPO_SLUGS:
        assert control_stop(slug) not in redis_client.values


def test_daemon_stop_without_redis_returns_affected(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write_config(tmp_path, monkeypatch)
    _stub_redis(monkeypatch, None)

    with TestClient(app) as client:
        response = client.post("/daemon/stop")

    assert response.json() == {"affected": _REPO_SLUGS, "count": 3}


def test_drain_progress_returns_per_repo_state(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write_config(tmp_path, monkeypatch)
    redis_client = _FakeRedis()
    _stub_redis(monkeypatch, redis_client)
    for slug, url in zip(_REPO_SLUGS, _REPO_URLS):
        _seed_state(redis_client, slug, url=url)

    with TestClient(app) as client:
        response = client.get("/daemon/drain-progress")

    assert response.status_code == 200
    body = response.json()
    assert [entry["name"] for entry in body["repos"]] == _REPO_SLUGS
    for entry in body["repos"]:
        assert entry["state"] == PipelineState.IDLE.value
        assert entry["draining"] is False
        assert entry["current_task_id"] is None


def test_drain_progress_flags_draining_repos(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write_config(tmp_path, monkeypatch)
    redis_client = _FakeRedis()
    _stub_redis(monkeypatch, redis_client)
    # One repo paused while a task was mid-flight; the others idle.
    _seed_state(redis_client, _REPO_SLUGS[0], url=_REPO_URLS[0])
    _seed_state(
        redis_client,
        _REPO_SLUGS[1],
        url=_REPO_URLS[1],
        state=PipelineState.PAUSED,
        user_paused=True,
        current_task=QueueTask(
            pr_id="PR-339",
            title="Draining task",
            status=TaskStatus.DOING,
            task_file="tasks/PR-339.md",
            branch="pr-339-feature",
        ),
    )
    _seed_state(redis_client, _REPO_SLUGS[2], url=_REPO_URLS[2])

    with TestClient(app) as client:
        body = client.get("/daemon/drain-progress").json()

    by_name = {entry["name"]: entry for entry in body["repos"]}
    assert by_name[_REPO_SLUGS[1]]["draining"] is True
    assert by_name[_REPO_SLUGS[1]]["state"] == PipelineState.PAUSED.value
    assert by_name[_REPO_SLUGS[1]]["current_task_id"] == "PR-339"


def test_drain_progress_non_paused_repos_not_draining(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write_config(tmp_path, monkeypatch)
    redis_client = _FakeRedis()
    _stub_redis(monkeypatch, redis_client)
    # A CODING repo has a current task but is not paused, so it is
    # not draining; a PAUSED repo with no task is also not draining.
    _seed_state(
        redis_client,
        _REPO_SLUGS[0],
        url=_REPO_URLS[0],
        state=PipelineState.CODING,
        current_task=QueueTask(
            pr_id="PR-100",
            title="Active task",
            status=TaskStatus.DOING,
            task_file="tasks/PR-100.md",
            branch="pr-100",
        ),
    )
    _seed_state(
        redis_client,
        _REPO_SLUGS[1],
        url=_REPO_URLS[1],
        state=PipelineState.PAUSED,
        user_paused=True,
    )
    _seed_state(redis_client, _REPO_SLUGS[2], url=_REPO_URLS[2])

    with TestClient(app) as client:
        body = client.get("/daemon/drain-progress").json()

    by_name = {entry["name"]: entry for entry in body["repos"]}
    assert by_name[_REPO_SLUGS[0]]["draining"] is False
    assert by_name[_REPO_SLUGS[1]]["draining"] is False
    assert by_name[_REPO_SLUGS[2]]["draining"] is False


def test_drain_progress_rate_limit_pause_not_draining(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A PAUSED+current_task repo with ``user_paused=False`` is rate-limited.

    ``handle_paused`` uses ``user_paused`` to tell operator pauses apart
    from rate-limit pauses, and rate-limited repos can retain
    ``current_task`` while they wait out the window. The drain endpoint
    must mirror that distinction so deploy operators do not see
    rate-limit waits reported as a daemon-wide drain in progress.
    """
    _write_config(tmp_path, monkeypatch, repo_urls=[_REPO_URLS[0]])
    redis_client = _FakeRedis()
    _stub_redis(monkeypatch, redis_client)
    _seed_state(
        redis_client,
        _REPO_SLUGS[0],
        url=_REPO_URLS[0],
        state=PipelineState.PAUSED,
        user_paused=False,
        current_task=QueueTask(
            pr_id="PR-200",
            title="Rate-limited task",
            status=TaskStatus.DOING,
            task_file="tasks/PR-200.md",
            branch="pr-200",
        ),
    )

    with TestClient(app) as client:
        body = client.get("/daemon/drain-progress").json()

    entry = body["repos"][0]
    assert entry["state"] == PipelineState.PAUSED.value
    assert entry["current_task_id"] == "PR-200"
    assert entry["draining"] is False


def test_drain_progress_recovers_from_redis_get_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Drain-progress falls back to a default IDLE entry when ``get`` raises.

    The endpoint walks every configured repo sequentially; a transient
    Redis read failure for one repo must not abort the sweep or surface
    a 500 to the deploy operator polling this endpoint.
    """
    _write_config(tmp_path, monkeypatch, repo_urls=[_REPO_URLS[0]])
    redis_client = _FakeRedis()
    redis_client.get_error = RuntimeError("redis read down")
    _stub_redis(monkeypatch, redis_client)

    with TestClient(app) as client:
        body = client.get("/daemon/drain-progress").json()

    entry = body["repos"][0]
    assert entry["state"] == PipelineState.IDLE.value
    assert entry["draining"] is False


def test_drain_progress_recovers_from_state_decode_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Drain-progress falls back to a default entry on corrupt JSON."""
    _write_config(tmp_path, monkeypatch, repo_urls=[_REPO_URLS[0]])
    redis_client = _FakeRedis()
    redis_client.values[pipeline_state(_REPO_SLUGS[0])] = "{not valid json"
    _stub_redis(monkeypatch, redis_client)

    with TestClient(app) as client:
        body = client.get("/daemon/drain-progress").json()

    entry = body["repos"][0]
    assert entry["state"] == PipelineState.IDLE.value
    assert entry["draining"] is False


def test_drain_progress_returns_default_for_unseeded_repo(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Repos with no pipeline state in Redis report a default IDLE entry."""
    _write_config(tmp_path, monkeypatch, repo_urls=[_REPO_URLS[0]])
    redis_client = _FakeRedis()
    _stub_redis(monkeypatch, redis_client)

    with TestClient(app) as client:
        body = client.get("/daemon/drain-progress").json()

    entry = body["repos"][0]
    assert entry["state"] == PipelineState.IDLE.value
    assert entry["current_task_id"] is None
    assert entry["draining"] is False


def test_drain_progress_without_redis_returns_default_state(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write_config(tmp_path, monkeypatch)
    _stub_redis(monkeypatch, None)

    with TestClient(app) as client:
        body = client.get("/daemon/drain-progress").json()

    assert [entry["name"] for entry in body["repos"]] == _REPO_SLUGS
    assert all(entry["state"] == PipelineState.IDLE.value for entry in body["repos"])
    assert all(entry["draining"] is False for entry in body["repos"])
    assert all(entry["current_task_id"] is None for entry in body["repos"])


def test_daemon_pause_uses_cas_watch_on_state_key(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``/daemon/pause`` must WATCH the pipeline state key for every repo.

    Without the WATCH, a concurrent daemon ``publish_state`` write that
    lands between our read-modify-write can overwrite ``user_paused``
    (or our write can clobber fresher task metadata). Asserting on the
    recorded ``transaction`` watch keys verifies the CAS structure is in
    place; correctness of the retry-on-conflict semantics is provided by
    the underlying ``redis-py`` ``transaction()`` helper.
    """
    _write_config(tmp_path, monkeypatch)
    redis_client = _FakeRedis()
    _stub_redis(monkeypatch, redis_client)
    for slug, url in zip(_REPO_SLUGS, _REPO_URLS):
        _seed_state(redis_client, slug, url=url)

    with TestClient(app) as client:
        client.post("/daemon/pause")

    assert redis_client.transaction_calls == [
        (pipeline_state(slug),) for slug in _REPO_SLUGS
    ]


def test_daemon_resume_uses_cas_watch_on_state_and_stop_keys(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``/daemon/resume`` watches both the state key and the stop key.

    Resume clears the per-repo stop flag inside the same transaction
    that flips ``user_paused`` off, so both keys must participate in the
    WATCH set to keep the clear-and-resume side effects atomic with
    respect to concurrent daemon state writes.
    """
    _write_config(tmp_path, monkeypatch)
    redis_client = _FakeRedis()
    _stub_redis(monkeypatch, redis_client)
    for slug, url in zip(_REPO_SLUGS, _REPO_URLS):
        _seed_state(redis_client, slug, url=url, user_paused=True)

    with TestClient(app) as client:
        client.post("/daemon/resume")

    assert redis_client.transaction_calls == [
        (pipeline_state(slug), control_stop(slug)) for slug in _REPO_SLUGS
    ]


def test_daemon_endpoints_idempotent(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write_config(tmp_path, monkeypatch)
    redis_client = _FakeRedis()
    _stub_redis(monkeypatch, redis_client)
    for slug, url in zip(_REPO_SLUGS, _REPO_URLS):
        _seed_state(redis_client, slug, url=url)

    with TestClient(app) as client:
        first = client.post("/daemon/pause")
        second = client.post("/daemon/pause")

    assert first.json() == second.json()
    for slug in _REPO_SLUGS:
        assert _stored_state(redis_client, slug).user_paused is True


def test_config_path_helper_reads_app_setting(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(web_app, "CONFIG_PATH", "/custom/path/config.yml")
    assert daemon_control_routes._config_path() == "/custom/path/config.yml"
