"""Tests for src/web/app.py."""

from __future__ import annotations

import asyncio
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from fastapi.responses import HTMLResponse
from fastapi.testclient import TestClient
from src.config import load_config
from src.models import (
    CIStatus,
    PipelineState,
    PRInfo,
    QueueTask,
    RepoState,
    ReviewStatus,
    TaskStatus,
)
from src.web import app as web_app
from src.web.app import (
    _active_rate_limit_coder,
    _build_recent_graphql_burns_view,
    _build_resources_view,
    _claude_usage_chip,
    _find_repo_config_by_name,
    _format_duration_ms,
    _format_reset_unix,
    _get_repo_state_safe,
    _parse_iso8601,
    _recent_repo_metrics_payload,
    _resource_zone,
    app,
    get_all_repo_states,
    get_repo_state,
)


class _FakeRedis:
    def __init__(
        self,
        store: dict[str, str] | None = None,
        lists: dict[str, list[str]] | None = None,
    ) -> None:
        self.store = store or {}
        self.lists = lists or {}
        self.published: list[tuple[str, str]] = []

    async def ping(self) -> bool:
        return True

    async def get(self, key: str) -> str | None:
        return self.store.get(key)

    async def set(self, key: str, value: str, **_kwargs: object) -> None:
        self.store[key] = value

    async def delete(self, key: str) -> int:
        existed = key in self.store
        self.store.pop(key, None)
        return int(existed)

    async def publish(self, channel: str, message: str) -> int:
        self.published.append((channel, message))
        return 1

    async def transaction(
        self,
        func,
        *watches: str,
        value_from_callable: bool = False,
        **_kwargs: object,
    ):
        pipe = _FakePipeline(self)
        func_value = func(pipe)
        if asyncio.iscoroutine(func_value):
            func_value = await func_value
        exec_value = await pipe.execute()
        if value_from_callable:
            return func_value
        return exec_value

    async def _execute_pipeline(
        self,
        commands: list[tuple[str, tuple[object, ...], dict[str, object]]],
    ) -> list[object]:
        results: list[object] = []
        for command, args, kwargs in commands:
            if command == "set":
                await self.set(args[0], args[1], **kwargs)
                results.append(True)
            elif command == "delete":
                results.append(await self.delete(args[0]))
        return results

    async def lrange(self, key: str, start: int, stop: int) -> list[str]:
        values = self.lists.get(key, [])
        if stop < 0:
            stop = len(values) + stop
        return values[start : stop + 1]


class _FakePipeline:
    def __init__(self, redis: _FakeRedis) -> None:
        self.redis = redis
        self.commands: list[tuple[str, tuple[object, ...], dict[str, object]]] = []

    async def get(self, key: str) -> str | None:
        return await self.redis.get(key)

    def multi(self) -> None:
        return None

    def set(self, key: str, value: str, **kwargs: object) -> "_FakePipeline":
        self.commands.append(("set", (key, value), kwargs))
        return self

    def delete(self, key: str) -> "_FakePipeline":
        self.commands.append(("delete", (key,), {}))
        return self

    async def execute(self) -> list[object]:
        return await self.redis._execute_pipeline(self.commands)


class _BoomRedis:
    def __init__(self) -> None:
        self.calls: list[str] = []

    async def ping(self) -> bool:
        raise RuntimeError("redis is down")

    async def get(self, key: str) -> str | None:
        self.calls.append(key)
        raise RuntimeError("redis is down")


class _DeleteBoomRedis(_FakeRedis):
    async def _execute_pipeline(
        self,
        commands: list[tuple[str, tuple[object, ...], dict[str, object]]],
    ) -> list[object]:
        for command, args, _kwargs in commands:
            if command == "delete":
                raise RuntimeError(f"failed to delete {args[0]}")
        return await super()._execute_pipeline(commands)


class _StopSetBoomRedis(_FakeRedis):
    async def _execute_pipeline(
        self,
        commands: list[tuple[str, tuple[object, ...], dict[str, object]]],
    ) -> list[object]:
        for command, args, _kwargs in commands:
            if command == "set" and args[0] == "control:example__alpha:stop":
                raise RuntimeError("failed to set stop control")
        return await super()._execute_pipeline(commands)


class _StubAioredisClient:
    async def ping(self) -> bool:
        return True

    async def get(self, key: str) -> str | None:
        return None

    async def aclose(self) -> None:
        return None


class _StubAioredis:
    """Drop-in replacement for ``redis.asyncio`` used by the lifespan."""

    @staticmethod
    def from_url(url: str, decode_responses: bool = True) -> _StubAioredisClient:
        return _StubAioredisClient()


class _ClosingBoomAioredisClient(_StubAioredisClient):
    async def aclose(self) -> None:
        raise RuntimeError("close failed")


class _ClosingBoomAioredis:
    @staticmethod
    def from_url(
        url: str, decode_responses: bool = True
    ) -> _ClosingBoomAioredisClient:
        return _ClosingBoomAioredisClient()


def _stub_aioredis_with_state(fake: object) -> object:
    return type(
        "_StubAioredisWithMutableState",
        (),
        {"from_url": staticmethod(lambda url, decode_responses=True: fake)},
    )()


@pytest.fixture
def empty_config(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    cfg = tmp_path / "config.yml"
    cfg.write_text("repositories: []\n", encoding="utf-8")
    monkeypatch.chdir(tmp_path)
    return cfg


@pytest.fixture
def two_repo_config(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    cfg = tmp_path / "config.yml"
    cfg.write_text(
        "repositories:\n"
        "  - url: https://github.com/example/alpha.git\n"
        "  - url: https://github.com/example/beta\n",
        encoding="utf-8",
    )
    monkeypatch.chdir(tmp_path)
    return cfg


def test_get_all_repo_states_no_redis_returns_idle_defaults(
    two_repo_config: Path,
) -> None:
    states, warning = asyncio.run(get_all_repo_states(redis_client=None))

    assert warning is None
    assert [s.name for s in states] == ["example__alpha", "example__beta"]
    for state in states:
        assert state.state == PipelineState.PREFLIGHT
        assert state.current_task is None
        assert state.current_pr is None
        assert state.error_message == "Redis unavailable — state unknown"


def test_get_all_repo_states_uses_redis_when_present(
    two_repo_config: Path,
) -> None:
    now = datetime(2026, 4, 10, 12, 0, 0, tzinfo=timezone.utc)
    stored = RepoState(
        url="https://github.com/example/alpha.git",
        name="example__alpha",
        state=PipelineState.CODING,
        current_task=QueueTask(
            pr_id="PR-099",
            title="Sample",
            status=TaskStatus.DOING,
        ),
        current_pr=PRInfo(
            number=42,
            branch="pr-099-sample",
            ci_status=CIStatus.SUCCESS,
            review_status=ReviewStatus.EYES,
        ),
        last_updated=now,
    )
    fake = _FakeRedis({"pipeline:example__alpha": stored.model_dump_json()})

    states, warning = asyncio.run(get_all_repo_states(redis_client=fake))

    assert warning is None
    assert states[0].state == PipelineState.CODING
    assert states[0].current_task is not None
    assert states[0].current_task.pr_id == "PR-099"
    assert states[0].current_pr is not None
    assert states[0].current_pr.number == 42
    # second repo has no redis entry -> awaiting initialization
    assert states[1].state == PipelineState.PREFLIGHT
    assert states[1].error_message == "Waiting for daemon to initialize"


def test_get_all_repo_states_falls_back_when_redis_raises(
    two_repo_config: Path,
) -> None:
    boom = _BoomRedis()
    states, warning = asyncio.run(get_all_repo_states(redis_client=boom))

    assert warning == "Redis connection lost"
    assert len(states) == 2
    # Upfront ping detects the outage so no per-repo get() calls are made.
    assert boom.calls == []


def test_get_repo_state_safe_handles_redis_error() -> None:
    boom = _BoomRedis()

    state, warning = asyncio.run(
        _get_repo_state_safe(
            boom, "example__alpha", "https://github.com/example/alpha.git"
        )
    )

    assert warning == "Redis unavailable"
    assert state.state == PipelineState.PREFLIGHT
    assert state.error_message == "Redis unavailable — state unknown"


def test_get_all_repo_states_drops_to_synthetic_states_after_runtime_redis_error(
    two_repo_config: Path,
) -> None:
    class _PingThenBoomRedis:
        async def ping(self) -> bool:
            return True

        async def get(self, key: str) -> str | None:
            raise RuntimeError("redis is down")

    states, warning = asyncio.run(
        get_all_repo_states(redis_client=_PingThenBoomRedis())
    )

    assert warning == "Redis connection lost"
    assert [state.state for state in states] == [
        PipelineState.PREFLIGHT,
        PipelineState.PREFLIGHT,
    ]
    assert all(
        state.error_message == "Redis unavailable — state unknown"
        for state in states
    )


def test_index_route_returns_html(
    empty_config: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(web_app, "aioredis", _StubAioredis())

    with TestClient(app) as client:
        response = client.get("/")

    assert response.status_code == 200
    assert "text/html" in response.headers["content-type"]
    body = response.text
    assert "Pipeline Orchestrator" in body
    assert 'hx-get="/partials/repo-list"' in body
    assert 'id="status-bar"' in body


@pytest.mark.parametrize(
    "path",
    ["/", "/settings", "/repo/example__alpha"],
)
def test_theme_toggle_is_rendered_on_main_pages(
    two_repo_config: Path,
    monkeypatch: pytest.MonkeyPatch,
    path: str,
) -> None:
    monkeypatch.setattr(web_app, "aioredis", _StubAioredis())

    with TestClient(app) as client:
        response = client.get(path)

    assert response.status_code == 200
    assert 'id="theme-toggle"' in response.text
    assert 'meta name="color-scheme" content="dark light"' in response.text


def test_base_template_includes_theme_bootstrap_assets(
    empty_config: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(web_app, "aioredis", _StubAioredis())

    with TestClient(app) as client:
        response = client.get("/")

    body = response.text
    assert "html[data-theme=\"light\"]" in body
    assert "let storedTheme = null;" in body
    assert "localStorage.getItem(STORAGE_KEY)" in body
    assert "if (storedTheme === 'light' || storedTheme === 'dark')" in body
    assert "localStorage.setItem(STORAGE_KEY, theme)" in body
    assert "prefers-color-scheme: light" in body
    assert "document.documentElement.dataset.theme = theme;" in body
    assert "theme-icon-dark" in body
    assert "theme-icon-light" in body


def test_base_template_declares_dark_color_scheme(
    empty_config: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(web_app, "aioredis", _StubAioredis())

    with TestClient(app) as client:
        response = client.get("/")

    body = response.text
    root_block_start = body.find(":root {")
    assert root_block_start != -1
    root_block_end = body.find("}", root_block_start)
    assert "color-scheme: dark" in body[root_block_start:root_block_end]


def test_api_states_returns_json(
    two_repo_config: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(web_app, "aioredis", _StubAioredis())

    with TestClient(app) as client:
        response = client.get("/api/states")

    assert response.status_code == 200
    payload = response.json()
    assert isinstance(payload, list)
    assert {p["name"] for p in payload} == {"example__alpha", "example__beta"}
    for entry in payload:
        assert entry["state"] == "PREFLIGHT"


def test_api_repo_queue_returns_snapshot(
    two_repo_config: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    snapshot_written_at = datetime(2026, 4, 10, 12, 0, 0, tzinfo=timezone.utc)
    heartbeat = datetime(2026, 4, 10, 12, 5, 0, tzinfo=timezone.utc)
    tasks = [
        QueueTask(
            pr_id=f"PR-00{idx}",
            title=f"Task {idx}",
            status=TaskStatus.TODO,
            task_file=f"tasks/PR-00{idx}.md",
            branch=f"pr-00{idx}",
            priority=idx,
        )
        for idx in range(1, 4)
    ]
    stored = RepoState(
        url="https://github.com/example/alpha.git",
        name="example__alpha",
        state=PipelineState.IDLE,
        last_updated=heartbeat,
        current_queue=tasks,
        current_queue_snapshot_at=snapshot_written_at,
    )
    fake = _FakeRedis({"pipeline:example__alpha": stored.model_dump_json()})
    monkeypatch.setattr(web_app, "aioredis", _stub_aioredis_with_state(fake))

    with TestClient(app) as client:
        response = client.get("/api/repo/example__alpha/queue")

    assert response.status_code == 200
    payload = response.json()
    assert payload["repo"] == "example__alpha"
    # snapshot_at must reflect when the queue snapshot was written, not
    # the daemon heartbeat ``last_updated`` that rolls forward every cycle.
    assert payload["snapshot_at"] == snapshot_written_at.isoformat()
    assert payload["snapshot_at"] != heartbeat.isoformat()
    assert payload["source"] == "snapshot"
    assert [entry["pr_id"] for entry in payload["queue"]] == [
        "PR-001",
        "PR-002",
        "PR-003",
    ]
    assert payload["queue"][0]["priority"] == 1



def test_api_repo_queue_snapshot_without_snapshot_timestamp(
    two_repo_config: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Legacy state written before ``current_queue_snapshot_at`` existed
    # (or by a publisher that has not stamped it yet). The snapshot is
    # still served, but ``snapshot_at`` is null rather than a misleading
    # heartbeat timestamp.
    stored = RepoState(
        url="https://github.com/example/alpha.git",
        name="example__alpha",
        state=PipelineState.IDLE,
        current_queue=[
            QueueTask(
                pr_id="PR-001",
                title="Snapshot task",
                status=TaskStatus.TODO,
            )
        ],
    )
    assert stored.current_queue_snapshot_at is None
    fake = _FakeRedis({"pipeline:example__alpha": stored.model_dump_json()})
    monkeypatch.setattr(web_app, "aioredis", _stub_aioredis_with_state(fake))

    with TestClient(app) as client:
        response = client.get("/api/repo/example__alpha/queue")

    assert response.status_code == 200
    payload = response.json()
    assert payload["source"] == "snapshot"
    assert payload["snapshot_at"] is None
    assert payload["queue"][0]["pr_id"] == "PR-001"


def test_api_repo_queue_returns_503_when_snapshot_unavailable(
    two_repo_config: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake = _FakeRedis()
    monkeypatch.setattr(web_app, "REPOS_DIR", str(tmp_path / "repos"))
    monkeypatch.setattr(web_app, "aioredis", _stub_aioredis_with_state(fake))

    with TestClient(app) as client:
        response = client.get("/api/repo/example__alpha/queue")

    assert response.status_code == 503
    assert response.json()["error"] == "Queue not yet computed"


def test_api_repo_queue_503_when_redis_client_unavailable(
    two_repo_config: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``state.redis`` is None -> snapshot lookup short-circuits to 503."""
    fake = _FakeRedis()
    monkeypatch.setattr(web_app, "aioredis", _stub_aioredis_with_state(fake))

    with TestClient(app) as client:
        client.app.state.redis = None
        response = client.get("/api/repo/example__alpha/queue")

    assert response.status_code == 503
    assert response.json()["error"] == "Queue not yet computed"


def test_api_repo_queue_503_when_redis_get_raises(
    two_repo_config: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    boom = _BoomRedis()
    monkeypatch.setattr(web_app, "aioredis", _stub_aioredis_with_state(boom))

    with TestClient(app) as client:
        response = client.get("/api/repo/example__alpha/queue")

    assert response.status_code == 503
    assert response.json()["error"] == "Queue not yet computed"


def test_api_repo_queue_503_when_state_json_invalid(
    two_repo_config: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake = _FakeRedis({"pipeline:example__alpha": "{not valid json"})
    monkeypatch.setattr(web_app, "aioredis", _stub_aioredis_with_state(fake))

    with TestClient(app) as client:
        response = client.get("/api/repo/example__alpha/queue")

    assert response.status_code == 503
    assert response.json()["error"] == "Queue not yet computed"


def test_api_repo_queue_503_when_state_has_no_queue(
    two_repo_config: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stored = RepoState(
        url="https://github.com/example/alpha.git",
        name="example__alpha",
        state=PipelineState.IDLE,
    )
    assert stored.current_queue is None
    fake = _FakeRedis({"pipeline:example__alpha": stored.model_dump_json()})
    monkeypatch.setattr(web_app, "aioredis", _stub_aioredis_with_state(fake))

    with TestClient(app) as client:
        response = client.get("/api/repo/example__alpha/queue")

    assert response.status_code == 503
    assert response.json()["error"] == "Queue not yet computed"



def test_api_repo_queue_unknown_repo_returns_404(
    two_repo_config: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fake = _FakeRedis()
    monkeypatch.setattr(web_app, "aioredis", _stub_aioredis_with_state(fake))

    with TestClient(app) as client:
        response = client.get("/api/repo/example__missing/queue")

    assert response.status_code == 404
    assert response.json()["error"] == "Repository not found"


def _seed_disk_queue(repos_dir: Path) -> None:
    queue_dir = repos_dir / "example__alpha" / "tasks"
    queue_dir.mkdir(parents=True)
    (queue_dir / "QUEUE.md").write_text(
        "# Task Queue\n\n"
        "## PR-010: Disk fallback task\n"
        "- Status: TODO\n"
        "- Tasks file: tasks/PR-010.md\n",
        encoding="utf-8",
    )






def test_list_repo_tasks_uses_snapshot_when_available(
    two_repo_config: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repos_dir = tmp_path / "repos"
    queue_dir = repos_dir / "example__alpha" / "tasks"
    queue_dir.mkdir(parents=True)
    (queue_dir / "QUEUE.md").write_text(
        "# Task Queue\n\n"
        "## PR-001: Disk task that should not render\n"
        "- Status: TODO\n",
        encoding="utf-8",
    )
    stored = RepoState(
        url="https://github.com/example/alpha.git",
        name="example__alpha",
        state=PipelineState.IDLE,
        current_queue=[
            QueueTask(
                pr_id="PR-099",
                title="Snapshot task",
                status=TaskStatus.TODO,
                branch="pr-099-snapshot",
            )
        ],
    )
    fake = _FakeRedis({"pipeline:example__alpha": stored.model_dump_json()})
    monkeypatch.setattr(web_app, "REPOS_DIR", str(repos_dir))
    monkeypatch.setattr(web_app, "aioredis", _stub_aioredis_with_state(fake))

    with TestClient(app) as client:
        response = client.get("/repos/example__alpha/tasks")

    assert response.status_code == 200
    assert "Snapshot task" in response.text
    assert "Disk task that should not render" not in response.text



def test_partial_repo_list_returns_html_fragment(
    two_repo_config: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(web_app, "aioredis", _StubAioredis())

    with TestClient(app) as client:
        response = client.get("/partials/repo-list")

    assert response.status_code == 200
    body = response.text
    assert "<!DOCTYPE" not in body  # fragment, not full document
    assert "alpha" in body
    assert "beta" in body
    assert "initializing" in body


def test_partial_repo_list_empty_state(
    empty_config: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(web_app, "aioredis", _StubAioredis())

    with TestClient(app) as client:
        response = client.get("/partials/repo-list")

    assert response.status_code == 200
    assert "No repositories configured" in response.text


def test_pause_resume_stop_endpoints_update_repo_state(
    two_repo_config: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stored = RepoState(
        url="https://github.com/example/alpha.git",
        name="example__alpha",
        state=PipelineState.IDLE,
    )
    fake = _FakeRedis({"pipeline:example__alpha": stored.model_dump_json()})
    published: list[tuple[str, str, dict[str, object], object | None]] = []

    async def _fake_publish_repo_event(
        repo_name: str,
        event_type: str,
        payload: dict[str, object],
        redis_client: object | None = None,
    ) -> None:
        published.append((repo_name, event_type, payload, redis_client))

    monkeypatch.setattr(web_app, "aioredis", _stub_aioredis_with_state(fake))
    monkeypatch.setattr(web_app, "publish_repo_event", _fake_publish_repo_event)

    with TestClient(app) as client:
        pause = client.post("/repos/example__alpha/pause")
        assert pause.status_code == 200
        paused = RepoState.model_validate_json(fake.store["pipeline:example__alpha"])
        assert paused.user_paused is True
        assert paused.history[-1]["event"] == (
            "Pause requested. Finishing current PR cycle "
            "(may take several iterations)."
        )

        stop = client.post("/repos/example__alpha/stop")
        assert stop.status_code == 200
        stopped = RepoState.model_validate_json(fake.store["pipeline:example__alpha"])
        assert stopped.user_paused is True
        assert fake.store["control:example__alpha:stop"] == "1"
        assert stopped.history[-1]["event"] == (
            "Stop requested. Aborting run; working tree may be left dirty."
        )

        resume = client.post("/repos/example__alpha/resume")
        assert resume.status_code == 200
        resumed = RepoState.model_validate_json(fake.store["pipeline:example__alpha"])
        assert resumed.user_paused is False
        assert "control:example__alpha:stop" not in fake.store
        assert resumed.history[-1]["event"] == (
            "Resumed. Taking next task from queue."
        )

    assert [entry["event"] for entry in resumed.history] == [
        "Pause requested. Finishing current PR cycle (may take several iterations).",
        "Stop requested. Aborting run; working tree may be left dirty.",
        "Resumed. Taking next task from queue.",
    ]
    assert published == [
        (
            "example__alpha",
            "history_updated",
            {
                "state": "IDLE",
                "event": (
                    "Pause requested. Finishing current PR cycle "
                    "(may take several iterations)."
                ),
            },
            fake,
        ),
        (
            "example__alpha",
            "history_updated",
            {
                "state": "IDLE",
                "event": "Stop requested. Aborting run; working tree may be left dirty.",
            },
            fake,
        ),
        (
            "example__alpha",
            "history_updated",
            {
                "state": "IDLE",
                "event": "Resumed. Taking next task from queue.",
            },
            fake,
        ),
    ]
    # All three control endpoints wake the daemon via PR-183 pub/sub so an
    # adaptive-IDLE runner reacts within the next loop tick instead of
    # waiting up to ``idle_extended_poll_interval_sec``. Each endpoint
    # publishes its own event_type so subscribers can distinguish causes.
    channels = [channel for channel, _ in fake.published]
    assert channels == [
        "orchestrator:wake:example__alpha",
        "orchestrator:wake:example__alpha",
        "orchestrator:wake:example__alpha",
    ]
    payloads = [json.loads(message) for _, message in fake.published]
    assert [payload["event_type"] for payload in payloads] == [
        "pause",
        "stop",
        "resume",
    ]


def test_pause_endpoint_publishes_pause_wake_event(
    two_repo_config: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stored = RepoState(
        url="https://github.com/example/alpha.git",
        name="example__alpha",
        state=PipelineState.IDLE,
    )
    fake = _FakeRedis({"pipeline:example__alpha": stored.model_dump_json()})
    monkeypatch.setattr(web_app, "aioredis", _stub_aioredis_with_state(fake))

    with TestClient(app) as client:
        response = client.post("/repos/example__alpha/pause")

    assert response.status_code == 200
    assert len(fake.published) == 1
    channel, message = fake.published[0]
    assert channel == "orchestrator:wake:example__alpha"
    payload = json.loads(message)
    assert payload["event_type"] == "pause"
    assert payload["repo"] == "example__alpha"


def test_resume_endpoint_publishes_resume_wake_event(
    two_repo_config: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stored = RepoState(
        url="https://github.com/example/alpha.git",
        name="example__alpha",
        state=PipelineState.IDLE,
        user_paused=True,
    )
    fake = _FakeRedis({"pipeline:example__alpha": stored.model_dump_json()})
    monkeypatch.setattr(web_app, "aioredis", _stub_aioredis_with_state(fake))

    with TestClient(app) as client:
        response = client.post("/repos/example__alpha/resume")

    assert response.status_code == 200
    assert len(fake.published) == 1
    channel, message = fake.published[0]
    assert channel == "orchestrator:wake:example__alpha"
    payload = json.loads(message)
    assert payload["event_type"] == "resume"
    assert payload["repo"] == "example__alpha"


def test_stop_endpoint_publishes_stop_wake_event(
    two_repo_config: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stored = RepoState(
        url="https://github.com/example/alpha.git",
        name="example__alpha",
        state=PipelineState.IDLE,
    )
    fake = _FakeRedis({"pipeline:example__alpha": stored.model_dump_json()})
    monkeypatch.setattr(web_app, "aioredis", _stub_aioredis_with_state(fake))

    with TestClient(app) as client:
        response = client.post("/repos/example__alpha/stop")

    assert response.status_code == 200
    assert len(fake.published) == 1
    channel, message = fake.published[0]
    assert channel == "orchestrator:wake:example__alpha"
    payload = json.loads(message)
    assert payload["event_type"] == "stop"
    assert payload["repo"] == "example__alpha"


def test_pause_endpoint_succeeds_when_publish_wake_fails(
    two_repo_config: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Pause endpoint must persist the flag even if pub/sub fails."""

    class _PublishBoomRedis(_FakeRedis):
        async def publish(self, channel: str, message: str) -> int:
            raise RuntimeError("publish boom")

    stored = RepoState(
        url="https://github.com/example/alpha.git",
        name="example__alpha",
        state=PipelineState.IDLE,
    )
    fake = _PublishBoomRedis({"pipeline:example__alpha": stored.model_dump_json()})
    monkeypatch.setattr(web_app, "aioredis", _stub_aioredis_with_state(fake))

    with TestClient(app) as client:
        with caplog.at_level("WARNING", logger=web_app.logger.name):
            response = client.post("/repos/example__alpha/pause")

    assert response.status_code == 200
    paused = RepoState.model_validate_json(fake.store["pipeline:example__alpha"])
    assert paused.user_paused is True
    assert any(
        "publish_wake failed for example__alpha" in rec.getMessage()
        for rec in caplog.records
    )


def test_resume_endpoint_succeeds_when_publish_wake_fails(
    two_repo_config: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Resume endpoint must clear the pause flag even if pub/sub fails."""

    class _PublishBoomRedis(_FakeRedis):
        async def publish(self, channel: str, message: str) -> int:
            raise RuntimeError("publish boom")

    stored = RepoState(
        url="https://github.com/example/alpha.git",
        name="example__alpha",
        state=PipelineState.IDLE,
        user_paused=True,
    )
    fake = _PublishBoomRedis({"pipeline:example__alpha": stored.model_dump_json()})
    monkeypatch.setattr(web_app, "aioredis", _stub_aioredis_with_state(fake))

    with TestClient(app) as client:
        with caplog.at_level("WARNING", logger=web_app.logger.name):
            response = client.post("/repos/example__alpha/resume")

    assert response.status_code == 200
    resumed = RepoState.model_validate_json(fake.store["pipeline:example__alpha"])
    assert resumed.user_paused is False
    assert any(
        "publish_wake failed for example__alpha" in rec.getMessage()
        for rec in caplog.records
    )


def test_stop_endpoint_succeeds_when_publish_wake_fails(
    two_repo_config: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Wake-publish failures must not block the control-plane response."""

    class _PublishBoomRedis(_FakeRedis):
        async def publish(self, channel: str, message: str) -> int:
            raise RuntimeError("publish boom")

    stored = RepoState(
        url="https://github.com/example/alpha.git",
        name="example__alpha",
        state=PipelineState.IDLE,
    )
    fake = _PublishBoomRedis({"pipeline:example__alpha": stored.model_dump_json()})
    monkeypatch.setattr(web_app, "aioredis", _stub_aioredis_with_state(fake))

    with TestClient(app) as client:
        with caplog.at_level("WARNING", logger=web_app.logger.name):
            response = client.post("/repos/example__alpha/stop")

    assert response.status_code == 200
    stopped = RepoState.model_validate_json(fake.store["pipeline:example__alpha"])
    assert stopped.user_paused is True
    assert any(
        "publish_wake failed for example__alpha" in rec.getMessage()
        for rec in caplog.records
    )


def test_pause_endpoint_wake_message_resets_daemon_last_run(
    two_repo_config: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """End-to-end: Pause click publishes a wake message that the daemon's
    ``_apply_wake_message`` recognises and uses to reset its sleep timer.

    This proves the channel published by the control endpoint is the same
    channel the daemon subscribes to, so a daemon already in an extended
    IDLE poll wakes within the next ``_wait_or_wake`` tick instead of
    waiting up to ``idle_extended_poll_interval_sec`` (default 300s).
    """
    from src.daemon import main as daemon_main

    stored = RepoState(
        url="https://github.com/example/alpha.git",
        name="example__alpha",
        state=PipelineState.IDLE,
    )
    fake = _FakeRedis({"pipeline:example__alpha": stored.model_dump_json()})
    monkeypatch.setattr(web_app, "aioredis", _stub_aioredis_with_state(fake))

    with TestClient(app) as client:
        response = client.post("/repos/example__alpha/pause")

    assert response.status_code == 200
    assert len(fake.published) == 1
    channel, message = fake.published[0]
    payload = json.loads(message)
    assert payload["event_type"] == "pause"

    last_run = {"alpha-key": 100.0, "beta-key": 200.0}
    slug_to_key = {"example__alpha": "alpha-key", "example__beta": "beta-key"}
    daemon_main._apply_wake_message(
        {"channel": channel}, last_run, slug_to_key
    )

    assert last_run["alpha-key"] == 0.0
    assert last_run["beta-key"] == 200.0


def test_resume_endpoint_cancels_pending_pause_for_active_run(
    two_repo_config: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stored = RepoState(
        url="https://github.com/example/alpha.git",
        name="example__alpha",
        state=PipelineState.CODING,
        user_paused=True,
    )
    fake = _FakeRedis({"pipeline:example__alpha": stored.model_dump_json()})
    monkeypatch.setattr(web_app, "aioredis", _stub_aioredis_with_state(fake))

    with TestClient(app) as client:
        response = client.post("/repos/example__alpha/resume")

    assert response.status_code == 200
    resumed = RepoState.model_validate_json(fake.store["pipeline:example__alpha"])
    assert resumed.user_paused is False
    assert resumed.history[-1]["event"] == "Pause canceled. Continue current run."


def test_pause_then_resume_while_active_does_not_log_effective_paused_event(
    two_repo_config: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stored = RepoState(
        url="https://github.com/example/alpha.git",
        name="example__alpha",
        state=PipelineState.FIX,
        user_paused=False,
    )
    fake = _FakeRedis({"pipeline:example__alpha": stored.model_dump_json()})
    monkeypatch.setattr(web_app, "aioredis", _stub_aioredis_with_state(fake))

    with TestClient(app) as client:
        pause = client.post("/repos/example__alpha/pause")
        resume = client.post("/repos/example__alpha/resume")

    assert pause.status_code == 200
    assert resume.status_code == 200
    state = RepoState.model_validate_json(fake.store["pipeline:example__alpha"])
    assert [entry["event"] for entry in state.history] == [
        "Pause requested. Finishing current PR cycle (may take several iterations).",
        "Pause canceled. Continue current run.",
    ]
    assert "Paused. Press Play to resume." not in [
        entry["event"] for entry in state.history
    ]


def test_append_history_entry_caps_control_history_at_100() -> None:
    state = RepoState(
        url="https://github.com/example/alpha.git",
        name="example__alpha",
        history=[
            {
                "time": f"2026-04-10T12:{minute:02d}:00+00:00",
                "state": "IDLE",
                "event": f"event {minute}",
            }
            for minute in range(100)
        ],
    )

    web_app._append_history_entry(state, "new event")

    assert len(state.history) == 100
    assert state.history[0]["event"] == "event 1"
    assert state.history[-1]["event"] == "new event"


def test_pause_endpoint_is_idempotent(
    two_repo_config: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stored = RepoState(
        url="https://github.com/example/alpha.git",
        name="example__alpha",
        state=PipelineState.IDLE,
        user_paused=True,
    )
    fake = _FakeRedis({"pipeline:example__alpha": stored.model_dump_json()})
    monkeypatch.setattr(web_app, "aioredis", _stub_aioredis_with_state(fake))

    with TestClient(app) as client:
        first = client.post("/repos/example__alpha/pause")
        second = client.post("/repos/example__alpha/pause")

    assert first.status_code == 200
    assert second.status_code == 200
    paused = RepoState.model_validate_json(fake.store["pipeline:example__alpha"])
    assert paused.user_paused is True


def test_resume_endpoint_is_idempotent(
    two_repo_config: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stored = RepoState(
        url="https://github.com/example/alpha.git",
        name="example__alpha",
        state=PipelineState.IDLE,
        user_paused=False,
    )
    fake = _FakeRedis({"pipeline:example__alpha": stored.model_dump_json()})
    monkeypatch.setattr(web_app, "aioredis", _stub_aioredis_with_state(fake))

    with TestClient(app) as client:
        first = client.post("/repos/example__alpha/resume")
        second = client.post("/repos/example__alpha/resume")

    assert first.status_code == 200
    assert second.status_code == 200
    resumed = RepoState.model_validate_json(fake.store["pipeline:example__alpha"])
    assert resumed.user_paused is False


def test_pause_endpoint_does_not_persist_fallback_state_on_decode_failure(
    two_repo_config: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake = _FakeRedis({"pipeline:example__alpha": "{not-json"})
    monkeypatch.setattr(web_app, "aioredis", _stub_aioredis_with_state(fake))

    with TestClient(app) as client:
        response = client.post("/repos/example__alpha/pause")

    assert response.status_code == 503
    assert fake.store["pipeline:example__alpha"] == "{not-json"


def test_resume_endpoint_reports_stop_key_delete_failure(
    two_repo_config: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stored = RepoState(
        url="https://github.com/example/alpha.git",
        name="example__alpha",
        state=PipelineState.IDLE,
        user_paused=True,
    )
    fake = _DeleteBoomRedis(
        {
            "pipeline:example__alpha": stored.model_dump_json(),
            "control:example__alpha:stop": "1",
        }
    )
    monkeypatch.setattr(web_app, "aioredis", _stub_aioredis_with_state(fake))

    with TestClient(app) as client:
        response = client.post("/repos/example__alpha/resume")

    assert response.status_code == 503
    state = RepoState.model_validate_json(fake.store["pipeline:example__alpha"])
    assert state.user_paused is True
    assert fake.store["control:example__alpha:stop"] == "1"


def test_stop_endpoint_reports_atomic_stop_key_write_failure(
    two_repo_config: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stored = RepoState(
        url="https://github.com/example/alpha.git",
        name="example__alpha",
        state=PipelineState.CODING,
        user_paused=False,
    )
    fake = _StopSetBoomRedis({"pipeline:example__alpha": stored.model_dump_json()})
    monkeypatch.setattr(
        web_app,
        "aioredis",
        type(
            "_StubAioredisWithMutableState",
            (),
            {"from_url": staticmethod(lambda url, decode_responses=True: fake)},
        )(),
    )

    with TestClient(app) as client:
        response = client.post("/repos/example__alpha/stop")

    assert response.status_code == 503
    state = RepoState.model_validate_json(fake.store["pipeline:example__alpha"])
    assert state.user_paused is False
    assert "control:example__alpha:stop" not in fake.store


def test_recover_endpoint_queues_recovery_signal_when_state_is_hung(
    two_repo_config: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``POST /repos/{name}/recover`` writes a one-shot recover flag and
    publishes a wake event when the repo is HUNG (PR-247)."""
    stored = RepoState(
        url="https://github.com/example/alpha.git",
        name="example__alpha",
        state=PipelineState.HUNG,
        current_task=QueueTask(
            pr_id="PR-001", title="parked", status=TaskStatus.DOING
        ),
    )
    fake = _FakeRedis({"pipeline:example__alpha": stored.model_dump_json()})
    monkeypatch.setattr(web_app, "aioredis", _stub_aioredis_with_state(fake))

    with TestClient(app) as client:
        response = client.post("/repos/example__alpha/recover")

    assert response.status_code == 200
    assert response.json() == {"ok": True, "queued_state": "IDLE"}
    assert fake.store.get("control:example__alpha:recover") == "1"
    assert len(fake.published) == 1
    channel, message = fake.published[0]
    assert channel == "orchestrator:wake:example__alpha"
    payload = json.loads(message)
    assert payload["event_type"] == "recover"
    assert payload["repo"] == "example__alpha"
    # The endpoint must NOT mutate state directly — the daemon performs the
    # transition on its next tick after consuming the signal.
    persisted = RepoState.model_validate_json(
        fake.store["pipeline:example__alpha"]
    )
    assert persisted.state == PipelineState.HUNG
    assert persisted.current_task is not None


@pytest.mark.parametrize(
    "non_hung_state",
    [
        PipelineState.IDLE,
        PipelineState.CODING,
        PipelineState.WATCH,
        PipelineState.FIX,
        PipelineState.MERGE,
        PipelineState.ERROR,
        PipelineState.PAUSED,
    ],
)
def test_recover_endpoint_rejects_non_hung_state(
    two_repo_config: Path,
    monkeypatch: pytest.MonkeyPatch,
    non_hung_state: PipelineState,
) -> None:
    """Recover must be HUNG-specific — every other state returns 400 and
    leaves Redis untouched (no flag, no wake)."""
    stored = RepoState(
        url="https://github.com/example/alpha.git",
        name="example__alpha",
        state=non_hung_state,
    )
    fake = _FakeRedis({"pipeline:example__alpha": stored.model_dump_json()})
    monkeypatch.setattr(web_app, "aioredis", _stub_aioredis_with_state(fake))

    with TestClient(app) as client:
        response = client.post("/repos/example__alpha/recover")

    assert response.status_code == 400
    body = response.json()
    assert body["error"] == "recovery_only_from_hung"
    assert body["current_state"] == non_hung_state.value
    assert "control:example__alpha:recover" not in fake.store
    assert fake.published == []


def test_recover_endpoint_404_for_unknown_repo(
    two_repo_config: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake = _FakeRedis()
    monkeypatch.setattr(web_app, "aioredis", _stub_aioredis_with_state(fake))

    with TestClient(app) as client:
        response = client.post("/repos/unknown__repo/recover")

    assert response.status_code == 404


def test_recover_endpoint_503_when_pipeline_state_unreadable(
    two_repo_config: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A get() error after the initial ping must surface as 503 — never as
    a recovery transition for an unknown current state."""

    class _ReadFailRedis(_FakeRedis):
        async def get(self, key: str) -> str | None:
            raise RuntimeError("redis read failed")

    fake = _ReadFailRedis()
    monkeypatch.setattr(web_app, "aioredis", _stub_aioredis_with_state(fake))

    with TestClient(app) as client:
        response = client.post("/repos/example__alpha/recover")

    assert response.status_code == 503
    assert "control:example__alpha:recover" not in fake.store


def test_recover_endpoint_503_when_pipeline_state_unparseable(
    two_repo_config: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Garbage in the persisted state blob must produce 503, not a 400 with
    a bogus ``current_state`` value."""
    fake = _FakeRedis({"pipeline:example__alpha": "{not-json"})
    monkeypatch.setattr(web_app, "aioredis", _stub_aioredis_with_state(fake))

    with TestClient(app) as client:
        response = client.post("/repos/example__alpha/recover")

    assert response.status_code == 503
    assert "control:example__alpha:recover" not in fake.store


def test_recover_endpoint_uses_default_state_when_pipeline_state_absent(
    two_repo_config: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """No persisted state means the daemon has not initialized yet — there
    cannot be a HUNG repo to recover, so the endpoint returns 400 with the
    default state's value."""
    fake = _FakeRedis()
    monkeypatch.setattr(web_app, "aioredis", _stub_aioredis_with_state(fake))

    with TestClient(app) as client:
        response = client.post("/repos/example__alpha/recover")

    assert response.status_code == 400
    body = response.json()
    assert body["error"] == "recovery_only_from_hung"
    assert "control:example__alpha:recover" not in fake.store


def test_recover_endpoint_503_when_signal_write_fails(
    two_repo_config: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """If the recover-flag write fails we must surface 503 so the operator
    sees a failure — pretending the signal was queued would let them think
    the daemon will act and silently drop the request."""

    class _SetFailRedis(_FakeRedis):
        async def set(self, key: str, value: str, **kwargs: object) -> None:
            if key == "control:example__alpha:recover":
                raise RuntimeError("set failed")
            await super().set(key, value, **kwargs)

    stored = RepoState(
        url="https://github.com/example/alpha.git",
        name="example__alpha",
        state=PipelineState.HUNG,
    )
    fake = _SetFailRedis(
        {"pipeline:example__alpha": stored.model_dump_json()}
    )
    monkeypatch.setattr(web_app, "aioredis", _stub_aioredis_with_state(fake))

    with TestClient(app) as client:
        response = client.post("/repos/example__alpha/recover")

    assert response.status_code == 503
    assert fake.published == []


def test_recover_endpoint_succeeds_when_publish_wake_fails(
    two_repo_config: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A wake-publish failure must not block the recovery signal — the
    daemon will still consume the Redis flag on its next tick."""

    class _PublishBoomRedis(_FakeRedis):
        async def publish(self, channel: str, message: str) -> int:
            raise RuntimeError("publish boom")

    stored = RepoState(
        url="https://github.com/example/alpha.git",
        name="example__alpha",
        state=PipelineState.HUNG,
    )
    fake = _PublishBoomRedis(
        {"pipeline:example__alpha": stored.model_dump_json()}
    )
    monkeypatch.setattr(web_app, "aioredis", _stub_aioredis_with_state(fake))

    with TestClient(app) as client:
        with caplog.at_level("WARNING", logger=web_app.logger.name):
            response = client.post("/repos/example__alpha/recover")

    assert response.status_code == 200
    assert fake.store.get("control:example__alpha:recover") == "1"
    assert any(
        "publish_wake failed for example__alpha" in rec.getMessage()
        for rec in caplog.records
    )


def test_recover_endpoint_uses_atomic_check_and_set(
    two_repo_config: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The HUNG-state check and the recover-flag write must execute inside
    a single ``WATCH``/``MULTI`` transaction on the pipeline-state key.

    Without the atomic guard, a daemon cycle could move the repo out of
    HUNG between the check and the write — the endpoint would still return
    200 and leave a recover flag that a later, unrelated HUNG episode
    would consume within the 5-minute TTL, causing an unexpected
    auto-recovery. This test pins the contract that the SET is queued
    inside the same transaction that watches ``pipeline:{name}``.
    """
    transaction_calls: list[dict[str, object]] = []

    stored = RepoState(
        url="https://github.com/example/alpha.git",
        name="example__alpha",
        state=PipelineState.HUNG,
    )

    class _SpyRedis(_FakeRedis):
        async def transaction(
            self,
            func,
            *watches: str,
            value_from_callable: bool = False,
            **kwargs: object,
        ):
            pipe = _FakePipeline(self)
            func_value = func(pipe)
            if asyncio.iscoroutine(func_value):
                func_value = await func_value
            transaction_calls.append(
                {
                    "watches": watches,
                    "queued": list(pipe.commands),
                }
            )
            exec_value = await pipe.execute()
            if value_from_callable:
                return func_value
            return exec_value

    fake = _SpyRedis({"pipeline:example__alpha": stored.model_dump_json()})
    monkeypatch.setattr(web_app, "aioredis", _stub_aioredis_with_state(fake))

    with TestClient(app) as client:
        response = client.post("/repos/example__alpha/recover")

    assert response.status_code == 200
    assert len(transaction_calls) == 1
    call = transaction_calls[0]
    assert call["watches"] == ("pipeline:example__alpha",)
    queued = [
        (cmd, args) for cmd, args, _kw in call["queued"]
    ]
    assert ("set", ("control:example__alpha:recover", "1")) in queued


def test_recover_endpoint_aborts_when_state_changes_during_transaction(
    two_repo_config: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Race: a daemon cycle moves the repo out of HUNG between the
    endpoint's ``WATCH`` and the ``EXEC``.

    redis-py's ``Redis.transaction(...)`` retries the callback on
    ``WatchError``; on the retry the callback re-reads the state via the
    pipeline, finds it is no longer HUNG, and aborts with 400. No flag
    must be written and no wake event must be published.
    """
    hung = RepoState(
        url="https://github.com/example/alpha.git",
        name="example__alpha",
        state=PipelineState.HUNG,
    )
    idle = RepoState(
        url="https://github.com/example/alpha.git",
        name="example__alpha",
        state=PipelineState.IDLE,
    )

    class _RaceRedis(_FakeRedis):
        """Models redis-py's retry behavior on ``WatchError``.

        In real Redis, a watched key changing between ``WATCH`` and
        ``EXEC`` causes ``EXEC`` to fail; redis-py re-invokes the
        callback against the new value. We collapse both observations
        into the single callback invocation here by serving the
        post-race value the first time the callback reads the watched
        key. The endpoint must therefore observe the state change on
        its very first ``pipe.get`` and abort with 400.
        """

        async def transaction(
            self,
            func,
            *watches: str,
            value_from_callable: bool = False,
            **kwargs: object,
        ):
            for key in watches:
                if key in self.store:
                    self.store[key] = idle.model_dump_json()
            pipe = _FakePipeline(self)
            func_value = func(pipe)
            if asyncio.iscoroutine(func_value):
                func_value = await func_value
            exec_value = await pipe.execute()
            if value_from_callable:
                return func_value
            return exec_value

    fake = _RaceRedis({"pipeline:example__alpha": hung.model_dump_json()})
    monkeypatch.setattr(web_app, "aioredis", _stub_aioredis_with_state(fake))

    with TestClient(app) as client:
        response = client.post("/repos/example__alpha/recover")

    assert response.status_code == 400
    body = response.json()
    assert body["error"] == "recovery_only_from_hung"
    assert body["current_state"] == PipelineState.IDLE.value
    assert "control:example__alpha:recover" not in fake.store
    assert fake.published == []


@pytest.mark.parametrize(
    ("path", "expected_paused", "stop_key_present"),
    [
        ("/repos/example__alpha/pause", True, False),
        ("/repos/example__alpha/resume", False, False),
        ("/repos/example__alpha/stop", True, True),
    ],
)
def test_control_endpoints_work_without_existing_pipeline_state(
    two_repo_config: Path,
    monkeypatch: pytest.MonkeyPatch,
    path: str,
    expected_paused: bool,
    stop_key_present: bool,
) -> None:
    fake = _FakeRedis()
    monkeypatch.setattr(
        web_app,
        "aioredis",
        type(
            "_StubAioredisWithMutableState",
            (),
            {"from_url": staticmethod(lambda url, decode_responses=True: fake)},
        )(),
    )

    with TestClient(app) as client:
        response = client.post(path)

    assert response.status_code == 200
    state = RepoState.model_validate_json(fake.store["pipeline:example__alpha"])
    assert state.name == "example__alpha"
    assert state.url == "https://github.com/example/alpha.git"
    assert state.user_paused is expected_paused
    assert ("control:example__alpha:stop" in fake.store) is stop_key_present


def test_partial_repo_list_renders_queue_progress(
    two_repo_config: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    now = datetime(2026, 4, 10, 12, 0, 0, tzinfo=timezone.utc)
    stored = RepoState(
        url="https://github.com/example/alpha.git",
        name="example__alpha",
        state=PipelineState.CODING,
        current_task=QueueTask(
            pr_id="PR-005",
            title="Dashboard",
            status=TaskStatus.DOING,
        ),
        queue_done=4,
        queue_total=10,
        last_updated=now,
    )

    class _StubClientWithQueue:
        async def ping(self) -> bool:
            return True

        async def get(self, key: str) -> str | None:
            if key == "pipeline:example__alpha":
                return stored.model_dump_json()
            return None

        async def aclose(self) -> None:
            return None

    class _StubAioredisWithQueue:
        @staticmethod
        def from_url(
            url: str, decode_responses: bool = True
        ) -> _StubClientWithQueue:
            return _StubClientWithQueue()

    monkeypatch.setattr(web_app, "aioredis", _StubAioredisWithQueue())

    with TestClient(app) as client:
        response = client.get("/partials/repo-list")

    assert response.status_code == 200
    body = response.text
    assert "4 / 10 done" in body
    assert "PR-005" in body
    assert 'data-repo="example__alpha"' in body
    assert "data-progress-text" in body
    assert "data-progress-bar" in body
    assert "width: 40.0%" in body


def test_index_bootstraps_progress_sse_manager(
    two_repo_config: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(web_app, "aioredis", _StubAioredis())

    with TestClient(app) as client:
        response = client.get("/")

    assert response.status_code == 200
    body = response.text
    assert "window.repoProgressSSEManager" in body
    assert "new EventSource('/api/repos/' + encodeURIComponent(repoName) + '/events')" in body
    assert "const MAX_DELAY_MS = 10000;" in body
    # The SSE consumer must relay history_updated so pause/resume/stop —
    # which only mutate user_paused — refresh the repo-cards' play/pause
    # controls. Without it the cards stay stale until state.state moves.
    assert (
        "['state_change', 'event_log_append', "
        "'history_updated', 'pr_metrics_update']" in body
    )
    # The `every 30s` fallback covers repos beyond MAX_STREAMS (the SSE
    # manager only subscribes to the first slice) and any SSE outage.
    assert (
        'hx-trigger="repo:state_change from:body, '
        'repo:history_updated from:body, every 30s"' in body
    )
    assert (
        'hx-trigger="repo:state_change from:body, every 30s"' in body
    )
    # stream_repo_events replays up to HISTORY_REPLAY_LIMIT entries on
    # each (re)connect. Without dedup + a server-provided render-time
    # cutoff, every replayed state_change/history_updated frame would
    # refire the repo-list / stats fetches, causing a burst of redundant
    # requests on page load and reconnect. Replay suppression is keyed
    # to ``page_rendered_at`` (the server-side instant the HTML was
    # rendered) so client-clock skew cannot drop legitimate live events
    # AND events delivered during the page-render→SSE-subscribe gap
    # still trigger HTMX.
    assert "rememberSeen" in body
    assert "isReplayedFrame" in body
    assert "pageRenderedAt" in body
    assert "pageRenderedAtMs" in body
    assert "pageLoadedAt" not in body
    assert "REPLAY_SKEW_MS" not in body
    assert "liveSinceMs" not in body
    assert "replay_complete" not in body


def test_index_captures_page_rendered_at_before_reading_state(
    two_repo_config: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The replay-suppression cutoff must be sampled BEFORE the state
    snapshot read. Otherwise an event published in the gap between the
    snapshot read and cutoff sampling is not reflected in the rendered
    HTML yet still gets classified as ``replayed`` (timestamp <
    pageRenderedAt) and discarded — repo cards and stats would stay
    stale until the 30s fallback poll or a later event arrived.
    Capturing the cutoff first guarantees that any frame older than it
    is part of the snapshot the page was rendered from.
    """
    from src.web.routes import dashboard as dashboard_routes

    monkeypatch.setattr(web_app, "aioredis", _StubAioredis())

    call_order: list[str] = []

    real_iso = dashboard_routes._page_rendered_at_iso

    def spy_iso() -> str:
        call_order.append("page_rendered_at")
        return real_iso()

    real_get_states = web_app.get_all_repo_states

    async def spy_get_states(*args: object, **kwargs: object):
        call_order.append("get_all_repo_states")
        return await real_get_states(*args, **kwargs)

    monkeypatch.setattr(dashboard_routes, "_page_rendered_at_iso", spy_iso)
    monkeypatch.setattr(web_app, "get_all_repo_states", spy_get_states)

    with TestClient(app) as client:
        response = client.get("/")

    assert response.status_code == 200
    assert "page_rendered_at" in call_order
    assert "get_all_repo_states" in call_order
    assert call_order.index("page_rendered_at") < call_order.index(
        "get_all_repo_states"
    )


def test_partial_stats_renders_status_bar(
    two_repo_config: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(web_app, "aioredis", _StubAioredis())

    with TestClient(app) as client:
        response = client.get("/partials/stats")

    assert response.status_code == 200
    body = response.text
    assert "Repos" in body
    assert "Done today" in body
    assert "Active" in body
    assert "Alerts" in body


def test_partial_redis_banner_handles_missing_and_failing_redis(
    empty_config: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(web_app, "aioredis", _StubAioredis())

    with TestClient(app) as client:
        del client.app.state.redis
        missing = client.get("/partials/redis-banner")
        assert missing.status_code == 200
        assert "Redis not configured" in missing.text

        client.app.state.redis = _BoomRedis()
        failing = client.get("/partials/redis-banner")
        assert failing.status_code == 200
        assert "Redis connection lost" in failing.text


def test_lifespan_ignores_redis_close_errors(
    empty_config: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(web_app, "aioredis", _ClosingBoomAioredis())

    with TestClient(app) as client:
        response = client.get("/")

    assert response.status_code == 200


def test_get_repo_state_unknown_repo_returns_idle_default(
    empty_config: Path,
) -> None:
    state = asyncio.run(get_repo_state("example__ghost", redis_client=None))

    assert state.name == "example__ghost"
    assert state.url == ""
    assert state.state == PipelineState.IDLE
    assert state.current_task is None
    assert state.current_pr is None


def test_find_repo_config_by_name_returns_none_for_missing_repo(
    two_repo_config: Path,
) -> None:
    cfg = web_app.load_config(str(two_repo_config))

    assert _find_repo_config_by_name(cfg, "example__ghost") is None


def test_get_repo_state_unknown_repo_ignores_stale_redis_key(
    two_repo_config: Path,
) -> None:
    """Repos missing from config must not surface stale Redis payloads."""
    stale = RepoState(
        url="https://github.com/example/ghost.git",
        name="example__ghost",
        state=PipelineState.CODING,
        current_task=None,
        current_pr=None,
        last_updated=datetime(2026, 4, 10, 12, 0, 0, tzinfo=timezone.utc),
    )
    fake = _FakeRedis({"pipeline:example__ghost": stale.model_dump_json()})

    state = asyncio.run(get_repo_state("example__ghost", redis_client=fake))

    assert state.name == "example__ghost"
    assert state.url == ""
    assert state.state == PipelineState.IDLE


def test_get_repo_state_uses_redis_payload(two_repo_config: Path) -> None:
    now = datetime(2026, 4, 10, 12, 0, 0, tzinfo=timezone.utc)
    stored = RepoState(
        url="https://github.com/example/alpha.git",
        name="example__alpha",
        state=PipelineState.WATCH,
        current_task=QueueTask(
            pr_id="PR-007",
            title="Sample task",
            status=TaskStatus.DOING,
            task_file="tasks/PR-007.md",
        ),
        current_pr=PRInfo(
            number=17,
            branch="pr-007-sample",
            ci_status=CIStatus.PENDING,
            review_status=ReviewStatus.EYES,
            push_count=2,
            url="https://github.com/example/alpha/pull/17",
        ),
        last_updated=now,
        history=[
            {"time": "12:00:01", "state": "CODING", "event": "started coding"},
            {"time": "12:00:42", "state": "WATCH", "event": "watching CI"},
        ],
    )
    fake = _FakeRedis({"pipeline:example__alpha": stored.model_dump_json()})

    state = asyncio.run(get_repo_state("example__alpha", redis_client=fake))

    assert state.state == PipelineState.WATCH
    assert state.current_pr is not None
    assert state.current_pr.number == 17
    assert state.history[-1]["event"] == "watching CI"


def test_repo_detail_route_renders_full_page(
    two_repo_config: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(web_app, "aioredis", _StubAioredis())

    with TestClient(app) as client:
        response = client.get("/repo/example__alpha")

    assert response.status_code == 200
    assert "text/html" in response.headers["content-type"]
    body = response.text
    assert "<!DOCTYPE" in body
    assert "alpha" in body
    assert "All repositories" in body
    assert 'hx-get="/partials/repo/example__alpha"' in body
    # Pause/resume/stop publish history_updated; the summary fragment
    # owns the play/pause/stop controls (their visibility depends on
    # repo.user_paused), so it must refresh on history_updated too —
    # otherwise an IDLE repo's controls stay stale until something else
    # changes state.state. The `every 30s` fallback keeps the
    # PAUSED rate-limit countdown (server `utcnow()`-derived) ticking
    # during long pauses where no SSE event fires.
    assert (
        'hx-trigger="load, repo:state_change from:body, '
        'repo:history_updated from:body, every 30s"' in body
    )
    assert "Current Task" in body
    assert "Current PR" in body
    assert 'hx-post="/repos/example__alpha/coder"' in body
    assert 'name="coder"' in body
    assert "Any (bandit picks per-PR)" in body
    assert "Recent PRs" not in body
    assert "Event log" in body
    # Pause/resume/stop publish history_updated; the SSE consumer must
    # relay it so the event log refreshes for IDLE repos that never see
    # a daemon-side event_log_append. The `every 30s` fallback covers
    # the case where the SSE channel itself is unavailable (Redis outage
    # returns 503 from /api/repos/{name}/events, EventSource cannot
    # connect, etc.) so the log still catches up to state.history.
    assert "'history_updated'" in body
    assert (
        'hx-trigger="repo:event_log_append from:body, '
        'repo:history_updated from:body, every 30s"' in body
    )
    # stream_repo_events replays up to HISTORY_REPLAY_LIMIT entries on
    # each (re)connect. Without dedup + a server-provided render-time
    # cutoff, every replayed state_change/history_updated frame would
    # refire the repo-summary / event-log fetches, causing a burst of
    # redundant requests on page load and reconnect. Replay suppression
    # is keyed to ``page_rendered_at`` (the server-side instant the
    # HTML was rendered) so client-clock skew cannot drop legitimate
    # live events AND events delivered during the page-render→
    # SSE-subscribe gap still trigger HTMX.
    assert "rememberSeen" in body
    assert "isReplayedFrame" in body
    assert "pageRenderedAt" in body
    assert "pageRenderedAtMs" in body
    assert "pageLoadedAt" not in body
    assert "REPLAY_SKEW_MS" not in body
    assert "liveSinceMs" not in body
    assert "replay_complete" not in body


def test_repo_detail_captures_page_rendered_at_before_reading_state(
    two_repo_config: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The repo-detail cutoff must be sampled BEFORE the per-repo state
    read for the same reason as the dashboard route: an event published
    in the gap between snapshot read and cutoff sampling would not be
    in the rendered summary yet would be classified as replayed and
    suppressed, leaving the summary stale until the 30s fallback poll.
    """
    from src.web.routes import dashboard as dashboard_routes

    monkeypatch.setattr(web_app, "aioredis", _StubAioredis())

    call_order: list[str] = []

    real_iso = dashboard_routes._page_rendered_at_iso

    def spy_iso() -> str:
        call_order.append("page_rendered_at")
        return real_iso()

    real_ctx = dashboard_routes._repo_template_context

    async def spy_ctx(*args: object, **kwargs: object):
        call_order.append("repo_template_context")
        return await real_ctx(*args, **kwargs)

    monkeypatch.setattr(dashboard_routes, "_page_rendered_at_iso", spy_iso)
    monkeypatch.setattr(dashboard_routes, "_repo_template_context", spy_ctx)

    with TestClient(app) as client:
        response = client.get("/repo/example__alpha")

    assert response.status_code == 200
    assert "page_rendered_at" in call_order
    assert "repo_template_context" in call_order
    assert call_order.index("page_rendered_at") < call_order.index(
        "repo_template_context"
    )


def test_repo_summary_banner_keeps_fragment_wrapped(
    two_repo_config: Path,
) -> None:
    now = datetime(2026, 4, 20, 17, 0, 0, tzinfo=timezone.utc)
    stored = RepoState(
        url="https://github.com/example/alpha.git",
        name="example__alpha",
        state=PipelineState.IDLE,
        last_updated=now,
    )
    fake = _FakeRedis({"pipeline:example__alpha": stored.model_dump_json()})

    context = asyncio.run(
        web_app._repo_template_context(
            "example__alpha",
            fake,
            coder_update_message="Switching to Codex.",
        )
    )
    rendered = web_app.templates.get_template(
        "components/repo_summary.html"
    ).render(context)

    assert rendered.count("<section") == rendered.count("</section>")
    assert rendered.count("<div") == rendered.count("</div>")
    banner = (
        '<div class="mb-4 rounded-lg border border-accent/30 '
        'bg-accent/10 px-4 py-3 text-sm text-accent">'
    )
    assert banner in rendered
    assert "Switching to Codex." in rendered


def test_repo_summary_error_fragment_keeps_divs_balanced(
    two_repo_config: Path,
) -> None:
    now = datetime(2026, 4, 20, 17, 0, 0, tzinfo=timezone.utc)
    stored = RepoState(
        url="https://github.com/example/alpha.git",
        name="example__alpha",
        state=PipelineState.ERROR,
        error_message="boom",
        last_updated=now,
    )
    fake = _FakeRedis({"pipeline:example__alpha": stored.model_dump_json()})

    context = asyncio.run(web_app._repo_template_context("example__alpha", fake))
    rendered = web_app.templates.get_template(
        "components/repo_summary.html"
    ).render(context)

    assert rendered.count("<div") == rendered.count("</div>")


def test_repo_summary_renders_infra_failure_distinctly_from_pending(
    two_repo_config: Path,
) -> None:
    now = datetime(2026, 4, 28, 12, 0, 0, tzinfo=timezone.utc)
    stored = RepoState(
        url="https://github.com/example/alpha.git",
        name="example__alpha",
        state=PipelineState.WATCH,
        current_task=QueueTask(
            pr_id="PR-099",
            title="Sample",
            status=TaskStatus.DOING,
        ),
        current_pr=PRInfo(
            number=42,
            branch="pr-099-sample",
            ci_status=CIStatus.INFRA_FAILURE,
            review_status=ReviewStatus.PENDING,
        ),
        last_updated=now,
    )
    fake = _FakeRedis({"pipeline:example__alpha": stored.model_dump_json()})

    context = asyncio.run(web_app._repo_template_context("example__alpha", fake))
    rendered = web_app.templates.get_template(
        "components/repo_summary.html"
    ).render(context)

    assert "infra failure" in rendered
    assert "bg-fail" in rendered
    ci_block = rendered.split(">CI<", 1)[1].split("</dd>", 1)[0]
    assert "pending" not in ci_block
    assert "pulse-dot" not in ci_block


def test_repo_cards_render_infra_failure_distinctly_from_pending(
    two_repo_config: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    now = datetime(2026, 4, 28, 12, 0, 0, tzinfo=timezone.utc)
    stored = RepoState(
        url="https://github.com/example/alpha.git",
        name="example__alpha",
        state=PipelineState.WATCH,
        current_pr=PRInfo(
            number=42,
            branch="pr-099-sample",
            ci_status=CIStatus.INFRA_FAILURE,
            review_status=ReviewStatus.PENDING,
            url="https://github.com/example/alpha/pull/42",
        ),
        last_updated=now,
    )
    fake = _FakeRedis({"pipeline:example__alpha": stored.model_dump_json()})
    monkeypatch.setattr(web_app, "aioredis", _stub_aioredis_with_state(fake))

    with TestClient(app) as client:
        response = client.get("/partials/repo-list")

    assert response.status_code == 200
    body = response.text
    assert "CI infra failure" in body
    pr_row_marker = '<a href="https://github.com/example/alpha/pull/42"'
    assert pr_row_marker in body
    pr_row = body.split(pr_row_marker, 1)[1].split("</div>", 1)[0]
    assert "CI pending" not in pr_row
    assert "bg-fail" in pr_row
    assert "pulse-dot" not in pr_row


def _metrics_record(
    run_id: str,
    *,
    task_id: str,
    started_at: str,
    ended_at: str = "2026-04-18T10:05:00+00:00",
    duration_ms: int,
    fix_iterations: int,
    exit_reason: str,
    profile_id: str = "claude:opus:container",
) -> str:
    return json.dumps(
        {
            "run_id": run_id,
            "task_id": task_id,
            "repo_name": "example__alpha",
            "profile_id": profile_id,
            "task_type": "feature",
            "complexity": "medium",
            "started_at": started_at,
            "ended_at": ended_at,
            "duration_ms": duration_ms,
            "fix_iterations": fix_iterations,
            "tokens_in": 1200,
            "tokens_out": 800,
            "exit_reason": exit_reason,
            "operator_intervention": False,
        }
    )


def test_metrics_endpoint_returns_records(
    two_repo_config: Path,
) -> None:
    fake = _FakeRedis(
        store={
            "metrics:run:active-finished-step": _metrics_record(
                "active-finished-step",
                task_id="PR-083",
                started_at="2026-04-18T12:00:00+00:00",
                ended_at="2026-04-18T12:03:00+00:00",
                duration_ms=180000,
                fix_iterations=0,
                exit_reason="coding_complete",
            ),
            "metrics:run:older": _metrics_record(
                "older",
                task_id="PR-081",
                started_at="2026-04-18T09:00:00+00:00",
                ended_at="2026-04-18T11:05:00+00:00",
                duration_ms=61000,
                fix_iterations=1,
                exit_reason="rate_limit",
                profile_id="codex:gpt-5.4:container",
            ),
            "metrics:run:newer": _metrics_record(
                "newer",
                task_id="PR-082",
                started_at="2026-04-18T11:00:00+00:00",
                ended_at="2026-04-18T10:05:00+00:00",
                duration_ms=300000,
                fix_iterations=2,
                exit_reason="success_merged",
            ),
        },
        lists={
            "metrics:repo:example__alpha:PR": [
                "active-finished-step",
                "older",
                "newer",
            ]
        },
    )

    with TestClient(app) as client:
        client.app.state.redis = fake
        response = client.get("/repo/example__alpha/metrics")

    assert response.status_code == 200
    payload = response.json()
    assert [record["task_id"] for record in payload] == ["PR-081", "PR-082"]
    assert all(record["exit_reason"] != "coding_complete" for record in payload)
    assert payload[0]["coder"] == "codex"
    assert payload[0]["model"] == "gpt-5.4"
    assert payload[0]["stage"] == "coder"
    assert payload[0]["files_touched_count"] == 0
    assert payload[0]["languages_touched"] == []
    assert payload[0]["diff_lines_added"] == 0
    assert payload[0]["diff_lines_deleted"] == 0
    assert payload[0]["test_file_ratio"] == 0.0
    assert payload[0]["had_merge_conflict"] is False
    assert payload[0]["base_branch"] == ""
    assert payload[0]["duration_text"] == "1m 1s"
    assert payload[0]["exit_reason_label"] == "rate limit"
    assert payload[1]["coder"] == "claude"
    assert payload[1]["model"] == "opus"
    assert payload[1]["exit_reason_label"] == "merged"


def test_recent_repo_metrics_payload_returns_empty_without_redis() -> None:
    assert asyncio.run(_recent_repo_metrics_payload("example__alpha", None)) == []


def test_exit_reason_classes_treats_closed_unmerged_as_failure() -> None:
    assert (
        web_app._exit_reason_classes("closed_unmerged")
        == "bg-fail/15 text-fail border-fail/30"
    )
    assert web_app._exit_reason_label("closed_unmerged") == "closed without merge"
    assert (
        web_app._exit_reason_classes("error")
        == "bg-fail/15 text-fail border-fail/30"
    )
    assert (
        web_app._exit_reason_classes("something_else")
        == "bg-white/5 text-gray-300 border-white/10"
    )


def test_parse_iso8601_and_format_duration_cover_edge_cases() -> None:
    assert _parse_iso8601(None) is None
    assert _parse_iso8601("not-a-date") is None
    naive = _parse_iso8601("2026-04-19T12:00:00")
    assert naive is not None
    assert naive.tzinfo == timezone.utc

    assert _format_duration_ms(None) == "—"
    assert _format_duration_ms(0) == "<1s"
    assert _format_duration_ms(5_000) == "5s"
    assert _format_duration_ms(60_000) == "1m"
    assert _format_duration_ms(61_000) == "1m 1s"
    assert _format_duration_ms(3_600_000) == "1h"
    assert _format_duration_ms(3_660_000) == "1h 1m"


def test_active_rate_limit_coder_hides_any_without_active_pick() -> None:
    state = RepoState(
        url="https://github.com/example/alpha.git",
        name="example__alpha",
        current_task=QueueTask(
            pr_id="PR-141",
            title="Rate limit badge",
            status=TaskStatus.DOING,
        ),
    )

    assert _active_rate_limit_coder(state, "any") is None


def test_partial_repo_detail_returns_html_fragment(
    two_repo_config: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(web_app, "aioredis", _StubAioredis())

    with TestClient(app) as client:
        response = client.get("/partials/repo/example__alpha")

    assert response.status_code == 200
    body = response.text
    assert "<!DOCTYPE" not in body  # fragment, not full document
    assert "alpha" in body
    assert "PREFLIGHT" in body
    assert "Current Task" in body
    assert "Current PR" in body
    # Event log has been split out of /partials/repo/{name} — it now
    # self-polls via /partials/repo/{name}/events so the selected filter
    # tab survives across summary refreshes. See
    # test_partial_repo_events_* in test_observability.py for coverage.
    assert "Event log" not in body


def test_partial_repo_metrics_returns_html_fragment(
    two_repo_config: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(web_app, "aioredis", _StubAioredis())

    with TestClient(app) as client:
        response = client.get("/partials/repo/example__alpha/metrics")

    assert response.status_code == 200
    assert "<!DOCTYPE" not in response.text


def test_partial_repo_detail_skips_metrics_fetch(
    two_repo_config: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(web_app, "aioredis", _StubAioredis())

    async def _fail_metrics_fetch(
        name: str, redis_client: object | None
    ) -> list[dict[str, object]]:
        raise AssertionError("summary poll should not fetch metrics")

    monkeypatch.setattr(
        web_app,
        "_recent_repo_metrics_payload",
        _fail_metrics_fetch,
    )

    with TestClient(app) as client:
        response = client.get("/partials/repo/example__alpha")

    assert response.status_code == 200
    assert "Current PR" in response.text


def test_partial_repo_detail_renders_redis_payload(
    two_repo_config: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    now = datetime(2026, 4, 10, 12, 0, 0, tzinfo=timezone.utc)
    stored = RepoState(
        url="https://github.com/example/alpha.git",
        name="example__alpha",
        state=PipelineState.CODING,
        current_task=QueueTask(
            pr_id="PR-042",
            title="Wire it up",
            status=TaskStatus.DOING,
            task_file="tasks/PR-042.md",
        ),
        current_pr=PRInfo(
            number=99,
            branch="pr-042-wire-it-up",
            ci_status=CIStatus.SUCCESS,
            review_status=ReviewStatus.APPROVED,
            commits_count=4,
            push_count=3,
            url="https://github.com/example/alpha/pull/99",
        ),
        last_updated=now,
        history=[
            {"time": "11:59:00", "state": "PREFLIGHT", "event": "preflight ok"},
            {"time": "12:00:00", "state": "CODING", "event": "claude started"},
        ],
    )

    class _StubClientWithPayload:
        async def ping(self) -> bool:
            return True

        async def get(self, key: str) -> str | None:
            if key == "pipeline:example__alpha":
                return stored.model_dump_json()
            return None

        async def aclose(self) -> None:
            return None

    class _StubAioredisWithPayload:
        @staticmethod
        def from_url(
            url: str, decode_responses: bool = True
        ) -> _StubClientWithPayload:
            return _StubClientWithPayload()

    monkeypatch.setattr(web_app, "aioredis", _StubAioredisWithPayload())

    with TestClient(app) as client:
        response = client.get("/partials/repo/example__alpha")

    assert response.status_code == 200
    body = response.text
    assert "PR-042" in body
    assert "Wire it up" in body
    assert "#99" in body
    assert "https://github.com/example/alpha/pull/99" in body
    assert "pr-042-wire-it-up" in body
    assert "Commits" in body
    assert "Pushes" in body
    assert ">4</dd>" in body
    assert ">3</dd>" in body
    assert "CODING" in body
    # Event log lives on /partials/repo/{name}/events now, so history
    # entries are no longer rendered by the summary partial.
    assert "claude started" not in body


@pytest.mark.parametrize(
    ("state", "present", "absent"),
    [
        (
            RepoState(
                url="https://github.com/example/alpha.git",
                name="example__alpha",
                state=PipelineState.CODING,
                current_task=QueueTask(
                    pr_id="PR-141",
                    title="Rate limit badge",
                    status=TaskStatus.DOING,
                    task_file="tasks/PR-141.md",
                ),
                coder="claude",
                usage_session_percent=16,
                usage_weekly_percent=42,
                last_updated=datetime(
                    2026, 4, 10, 12, 0, 0, tzinfo=timezone.utc
                ),
            ),
            ("Claude Session 16%", "Weekly 42%"),
            ("Codex Session",),
        ),
        (
            RepoState(
                url="https://github.com/example/alpha.git",
                name="example__alpha",
                state=PipelineState.CODING,
                current_task=QueueTask(
                    pr_id="PR-141",
                    title="Rate limit badge",
                    status=TaskStatus.DOING,
                    task_file="tasks/PR-141.md",
                ),
                coder="codex",
                usage_session_percent=16,
                usage_weekly_percent=42,
                usage_api_degraded=True,
                last_updated=datetime(
                    2026, 4, 10, 12, 0, 0, tzinfo=timezone.utc
                ),
            ),
            ("Codex Session 16%", "Weekly 42%", "Usage API degraded"),
            ("Claude Session",),
        ),
        (
            RepoState(
                url="https://github.com/example/alpha.git",
                name="example__alpha",
                state=PipelineState.IDLE,
                current_task=None,
                coder="claude",
                usage_session_percent=16,
                usage_weekly_percent=42,
                last_updated=datetime(
                    2026, 4, 10, 12, 0, 0, tzinfo=timezone.utc
                ),
            ),
            (),
            ("Claude Session", "Session 16%", "Weekly 42%"),
        ),
    ],
)
def test_partial_repo_detail_rate_limit_badge_follows_active_coder(
    two_repo_config: Path,
    monkeypatch: pytest.MonkeyPatch,
    state: RepoState,
    present: tuple[str, ...],
    absent: tuple[str, ...],
) -> None:
    async def _fake_get_repo_state(
        name: str,
        redis_client: object | None = None,
        config_path: str = "config.yml",
    ) -> RepoState:
        assert name == "example__alpha"
        assert config_path
        return state

    monkeypatch.setattr(web_app, "get_repo_state", _fake_get_repo_state)

    with TestClient(app) as client:
        response = client.get("/partials/repo/example__alpha")

    assert response.status_code == 200
    body = response.text
    for needle in present:
        assert needle in body
    for needle in absent:
        assert needle not in body


def test_cli_log_route_returns_log(
    empty_config: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    empty_config.write_text(
        "repositories:\n  - url: https://github.com/x/testrepo\n",
        encoding="utf-8",
    )

    class _CliLogClient:
        async def get(self, key: str) -> str | None:
            if key == "cli_log:x__testrepo:latest":
                return "line1\nline2"
            return None

        async def aclose(self) -> None:
            return None

    class _CliLogAioredis:
        @staticmethod
        def from_url(url: str, decode_responses: bool = True) -> _CliLogClient:
            return _CliLogClient()

    monkeypatch.setattr(web_app, "aioredis", _CliLogAioredis())

    with TestClient(app) as client:
        response = client.get("/partials/repo/x__testrepo/cli-log")
    assert response.status_code == 200
    assert "line1" in response.text
    assert "line2" in response.text


def test_cli_log_route_handles_redis_errors_and_missing_logs(
    empty_config: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    empty_config.write_text(
        "repositories:\n  - url: https://github.com/x/testrepo\n",
        encoding="utf-8",
    )

    class _FailingCliLogClient:
        async def get(self, key: str) -> str | None:
            raise RuntimeError("redis error")

        async def aclose(self) -> None:
            return None

    class _FailingCliLogAioredis:
        @staticmethod
        def from_url(
            url: str, decode_responses: bool = True
        ) -> _FailingCliLogClient:
            return _FailingCliLogClient()

    monkeypatch.setattr(web_app, "aioredis", _FailingCliLogAioredis())

    with TestClient(app) as client:
        failed = client.get("/partials/repo/x__testrepo/cli-log")
        assert failed.status_code == 200
        assert "No CLI log available" in failed.text

        client.app.state.redis = _StubAioredisClient()
        missing = client.get("/partials/repo/x__testrepo/cli-log")
        assert missing.status_code == 200
        assert "No CLI log available" in missing.text


def test_cli_log_route_returns_empty(
    empty_config: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(web_app, "aioredis", _StubAioredis())

    with TestClient(app) as client:
        response = client.get("/partials/repo/x__testrepo/cli-log")
    assert response.status_code == 200
    assert "No CLI log available" in response.text


def test_event_log_has_data_ts(
    two_repo_config: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Event-log rows must carry ``data-ts`` so the base.html
    ``formatTimestamps`` script can rewrite the raw ISO value into a
    local "5 min ago" style label after the HTMX swap settles."""
    header_ts = datetime(2026, 4, 10, 12, 0, 0, tzinfo=timezone.utc)
    entry_ts = "2026-04-10T11:55:00+00:00"
    stored = RepoState(
        url="https://github.com/example/alpha.git",
        name="example__alpha",
        state=PipelineState.CODING,
        last_updated=header_ts,
        history=[
            {"time": entry_ts, "state": "CODING", "event": "hello"},
        ],
    )
    fake = _FakeRedis({"pipeline:example__alpha": stored.model_dump_json()})

    with TestClient(app) as client:
        client.app.state.redis = fake
        response = client.get("/repo/example__alpha")

    assert response.status_code == 200
    body = response.text
    assert f'data-ts="{entry_ts}"' in body


def test_repo_card_has_onclick(
    two_repo_config: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Repo cards on the dashboard must navigate to the detail page on
    click — except when the click originates inside an interactive
    element (``label``/``input``/``button``/``a``), which keeps the
    Upload tasks button and nested anchors working as before."""
    monkeypatch.setattr(web_app, "aioredis", _StubAioredis())

    with TestClient(app) as client:
        response = client.get("/partials/repo-list")

    assert response.status_code == 200
    body = response.text
    assert "window.location='/repo/example__alpha'" in body
    assert "window.location='/repo/example__beta'" in body
    assert "event.target.closest('form,label,input,button,a')" in body
    assert 'hx-target="#upload-feedback-example__alpha"' in body
    assert 'hx-indicator="#upload-indicator-example__alpha"' in body
    assert 'hx-disabled-elt="#upload-example__alpha"' in body
    assert 'id="upload-feedback-example__alpha"' in body
    assert 'id="upload-indicator-example__alpha"' in body
    assert "htmx-indicator" in body
    assert "animate-spin" in body
    assert "Uploading..." in body


def test_repo_card_escapes_dotted_repo_name_in_hx_selectors(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cfg = tmp_path / "config.yml"
    cfg.write_text(
        "repositories:\n"
        "  - url: https://github.com/example/my.repo.git\n",
        encoding="utf-8",
    )
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(web_app, "aioredis", _StubAioredis())

    with TestClient(app) as client:
        response = client.get("/partials/repo-list")

    assert response.status_code == 200
    body = response.text
    assert 'hx-target="#upload-feedback-example__my\\.repo"' in body
    assert 'hx-indicator="#upload-indicator-example__my\\.repo"' in body
    assert 'hx-disabled-elt="#upload-example__my\\.repo"' in body
    assert 'id="upload-feedback-example__my.repo"' in body
    assert 'id="upload-indicator-example__my.repo"' in body
    assert 'id="upload-example__my.repo"' in body


def test_repo_card_renders_pause_and_stop_controls_for_active_repo(
    two_repo_config: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    now = datetime(2026, 4, 10, 12, 0, 0, tzinfo=timezone.utc)
    stored = RepoState(
        url="https://github.com/example/alpha.git",
        name="example__alpha",
        state=PipelineState.CODING,
        user_paused=False,
        queue_done=0,
        queue_total=2,
        last_updated=now,
    )
    fake = _FakeRedis({"pipeline:example__alpha": stored.model_dump_json()})
    monkeypatch.setattr(web_app, "aioredis", _StubAioredis())

    with TestClient(app) as client:
        client.app.state.redis = fake
        response = client.get("/partials/repo-list")

    assert response.status_code == 200
    body = response.text
    assert 'hx-post="/repos/example__alpha/pause"' in body
    assert 'hx-post="/repos/example__alpha/stop"' in body
    assert 'hx-post="/repos/example__alpha/resume"' not in body
    assert 'aria-label="Pause daemon"' in body
    assert 'aria-label="Stop current run"' in body
    assert 'hx-confirm="Stop the current run? The working tree may be left dirty."' in body
    assert 'onclick="event.stopPropagation()"' in body
    assert 'h-8 w-8 md:h-7 md:w-7' in body


def test_repo_card_upload_button_renders_as_flat_icon(
    two_repo_config: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """PR-201: Upload tasks must render as a flat icon button matching
    Pause/Stop, with a ``title`` tooltip preserving discoverability and
    no bordered/text-label appearance."""
    monkeypatch.setattr(web_app, "aioredis", _StubAioredis())

    with TestClient(app) as client:
        response = client.get("/partials/repo-list")

    assert response.status_code == 200
    body = response.text
    assert 'for="upload-example__alpha"' in body
    assert 'aria-label="Upload tasks"' in body
    assert 'title="Upload tasks"' in body
    assert (
        'class="relative inline-flex items-center justify-center h-8 w-8 '
        "md:h-7 md:w-7 rounded-md text-gray-400 hover:text-white "
        'hover:bg-white/5 transition-colors cursor-pointer'
    ) in body
    assert "border border-white/10 px-3 text-xs font-medium" not in body
    assert ">\n                        Upload tasks\n                    </label>" not in body


@pytest.mark.parametrize(
    ("state", "user_paused", "queue_done", "queue_total", "present", "absent"),
    [
        (
            PipelineState.PAUSED,
            True,
            0,
            1,
            ('hx-post="/repos/example__alpha/resume"', 'aria-label="Resume daemon"'),
            (
                'hx-post="/repos/example__alpha/pause"',
                'hx-post="/repos/example__alpha/stop"',
            ),
        ),
        (
            PipelineState.IDLE,
            False,
            0,
            2,
            ('hx-post="/repos/example__alpha/resume"',),
            (
                'hx-post="/repos/example__alpha/pause"',
                'hx-post="/repos/example__alpha/stop"',
            ),
        ),
        (
            PipelineState.MERGE,
            False,
            1,
            1,
            (
                'hx-post="/repos/example__alpha/pause"',
                'hx-post="/repos/example__alpha/stop"',
            ),
            (
                'hx-post="/repos/example__alpha/resume"',
            ),
        ),
        (
            PipelineState.WATCH,
            False,
            1,
            1,
            (
                'hx-post="/repos/example__alpha/pause"',
                'hx-post="/repos/example__alpha/stop"',
            ),
            (
                'hx-post="/repos/example__alpha/resume"',
            ),
        ),
        (
            PipelineState.FIX,
            False,
            1,
            1,
            (
                'hx-post="/repos/example__alpha/pause"',
                'hx-post="/repos/example__alpha/stop"',
            ),
            ('hx-post="/repos/example__alpha/resume"',),
        ),
    ],
)
def test_repo_detail_renders_control_visibility_matrix(
    two_repo_config: Path,
    monkeypatch: pytest.MonkeyPatch,
    state: PipelineState,
    user_paused: bool,
    queue_done: int,
    queue_total: int,
    present: tuple[str, ...],
    absent: tuple[str, ...],
) -> None:
    now = datetime(2026, 4, 10, 12, 0, 0, tzinfo=timezone.utc)
    stored = RepoState(
        url="https://github.com/example/alpha.git",
        name="example__alpha",
        state=state,
        user_paused=user_paused,
        queue_done=queue_done,
        queue_total=queue_total,
        last_updated=now,
    )
    fake = _FakeRedis({"pipeline:example__alpha": stored.model_dump_json()})
    monkeypatch.setattr(web_app, "aioredis", _StubAioredis())

    with TestClient(app) as client:
        client.app.state.redis = fake
        response = client.get("/repo/example__alpha")

    assert response.status_code == 200
    body = response.text
    for needle in present:
        assert needle in body
    for needle in absent:
        assert needle not in body


def test_updated_header_has_data_ts(
    two_repo_config: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The repo detail ``updated <ts>`` header must expose ``data-ts``
    so the client-side formatter can replace the raw UTC string with a
    relative local-time label."""
    now = datetime(2026, 4, 10, 12, 0, 0, tzinfo=timezone.utc)
    stored = RepoState(
        url="https://github.com/example/alpha.git",
        name="example__alpha",
        state=PipelineState.CODING,
        last_updated=now,
    )
    fake = _FakeRedis({"pipeline:example__alpha": stored.model_dump_json()})

    with TestClient(app) as client:
        client.app.state.redis = fake
        response = client.get("/repo/example__alpha")

    assert response.status_code == 200
    body = response.text
    assert f'data-ts="{now.isoformat()}"' in body


@pytest.mark.parametrize(
    ("state", "should_pulse"),
    [
        (PipelineState.CODING, True),
        (PipelineState.WATCH, True),
        (PipelineState.FIX, True),
        (PipelineState.MERGE, True),
        (PipelineState.IDLE, False),
        (PipelineState.PREFLIGHT, False),
        (PipelineState.PAUSED, False),
        (PipelineState.HUNG, False),
        (PipelineState.ERROR, False),
    ],
)
def test_repo_detail_state_badge_pulses_only_for_active_states(
    two_repo_config: Path,
    monkeypatch: pytest.MonkeyPatch,
    state: PipelineState,
    should_pulse: bool,
) -> None:
    stored = RepoState(
        url="https://github.com/example/alpha.git",
        name="example__alpha",
        state=state,
        last_updated=datetime(2026, 4, 10, 12, 0, 0, tzinfo=timezone.utc),
    )
    async def _fake_get_repo_state(
        name: str,
        redis_client: object | None = None,
        config_path: str = "config.yml",
    ) -> RepoState:
        assert name == "example__alpha"
        assert config_path
        return stored

    monkeypatch.setattr(web_app, "get_repo_state", _fake_get_repo_state)

    with TestClient(app) as client:
        response = client.get("/partials/repo/example__alpha")

    assert response.status_code == 200
    body = response.text
    assert state.value in body
    assert body.count("pulse-badge") == (1 if should_pulse else 0)


def test_index_shows_warning_when_redis_key_missing(
    two_repo_config: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """When a Redis key is absent the card must show the initialization
    warning instead of a clean IDLE state."""
    monkeypatch.setattr(web_app, "aioredis", _StubAioredis())

    with TestClient(app) as client:
        response = client.get("/partials/repo-list")

    assert response.status_code == 200
    body = response.text
    assert "Waiting for daemon to initialize" in body
    assert "initializing" in body


def test_index_shows_error_on_decode_failure(
    two_repo_config: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """When a Redis payload cannot be decoded the card must show a decode
    error instead of silently falling back to IDLE."""

    class _BadPayloadClient:
        async def ping(self) -> bool:
            return True

        async def get(self, key: str) -> str | None:
            if key == "pipeline:example__alpha":
                return "{not valid json!!"
            return None

        async def aclose(self) -> None:
            return None

    class _BadPayloadAioredis:
        @staticmethod
        def from_url(
            url: str, decode_responses: bool = True
        ) -> _BadPayloadClient:
            return _BadPayloadClient()

    monkeypatch.setattr(web_app, "aioredis", _BadPayloadAioredis())

    with TestClient(app) as client:
        response = client.get("/partials/repo-list")

    assert response.status_code == 200
    body = response.text
    assert "State decode failed" in body
    assert "stale" in body


# ---------------------------------------------------------------------------
# Upload queue validation tests
# ---------------------------------------------------------------------------


class _UploadRedisClient:
    """Fake Redis that returns IDLE state for ``pipeline:{name}`` and
    accepts ``set`` calls for the upload staging manifest."""

    def __init__(self, name: str) -> None:
        now = datetime(2026, 4, 10, 12, 0, 0, tzinfo=timezone.utc)
        idle = RepoState(
            url=f"https://github.com/example/{name}.git",
            name=f"example__{name}",
            state=PipelineState.IDLE,
            last_updated=now,
        )
        self._store = {f"pipeline:example__{name}": idle.model_dump_json()}

    async def ping(self) -> bool:
        return True

    async def get(self, key: str) -> str | None:
        return self._store.get(key)

    async def set(self, key: str, value: str, **kw: object) -> None:
        self._store[key] = value

    async def aclose(self) -> None:
        return None


class _UploadAioredis:
    def __init__(self, name: str) -> None:
        self._name = name

    def from_url(self, url: str, decode_responses: bool = True) -> _UploadRedisClient:
        return _UploadRedisClient(self._name)


def test_upload_rejects_queue_md_with_duplicate_pr_id_payload_by_name(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    cfg = tmp_path / "config.yml"
    cfg.write_text(
        "repositories:\n  - url: https://github.com/example/alpha.git\n",
        encoding="utf-8",
    )
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(web_app, "aioredis", _UploadAioredis("alpha"))
    monkeypatch.setattr(web_app, "REPOS_DIR", str(tmp_path / "repos"))
    monkeypatch.setattr(web_app, "UPLOADS_DIR", str(tmp_path / "uploads"))
    (tmp_path / "repos" / "example__alpha").mkdir(parents=True)

    queue_content = (
        "## PR-001: First\n- Status: DONE\n- Branch: pr-001\n\n"
        "## PR-001: Duplicate\n- Status: TODO\n- Branch: pr-001-dup\n"
    )
    with TestClient(app) as client:
        response = client.post(
            "/repos/example__alpha/upload-tasks",
            files=[("files", ("QUEUE.md", queue_content.encode(), "text/markdown"))],
        )
    assert response.status_code == 422
    assert (
        "Invalid file name: &#39;QUEUE.md&#39;. Only AGENTS.md, "
        "CLAUDE.md, and PR-*.md allowed."
    ) in response.text


def test_upload_rejects_queue_md_with_cycle_payload_by_name(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    cfg = tmp_path / "config.yml"
    cfg.write_text(
        "repositories:\n  - url: https://github.com/example/alpha.git\n",
        encoding="utf-8",
    )
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(web_app, "aioredis", _UploadAioredis("alpha"))
    monkeypatch.setattr(web_app, "REPOS_DIR", str(tmp_path / "repos"))
    monkeypatch.setattr(web_app, "UPLOADS_DIR", str(tmp_path / "uploads"))
    (tmp_path / "repos" / "example__alpha").mkdir(parents=True)

    queue_content = (
        "## PR-A: First\n- Status: TODO\n- Branch: pr-a\n- Depends on: PR-B\n\n"
        "## PR-B: Second\n- Status: TODO\n- Branch: pr-b\n- Depends on: PR-A\n"
    )
    with TestClient(app) as client:
        response = client.post(
            "/repos/example__alpha/upload-tasks",
            files=[("files", ("QUEUE.md", queue_content.encode(), "text/markdown"))],
        )
    assert response.status_code == 422
    assert (
        "Invalid file name: &#39;QUEUE.md&#39;. Only AGENTS.md, "
        "CLAUDE.md, and PR-*.md allowed."
    ) in response.text


def test_pause_endpoint_returns_404_for_unknown_repo(
    two_repo_config: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake = _FakeRedis()
    monkeypatch.setattr(web_app, "aioredis", _stub_aioredis_with_state(fake))

    with TestClient(app) as client:
        response = client.post("/repos/example__missing/pause")

    assert response.status_code == 404
    assert response.text == "Repository not found"


def test_pause_endpoint_returns_503_when_redis_is_unavailable(
    two_repo_config: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake = _StubAioredisClient()
    monkeypatch.setattr(web_app, "aioredis", _stub_aioredis_with_state(fake))

    with TestClient(app) as client:
        client.app.state.redis = None
        response = client.post("/repos/example__alpha/pause")

    assert response.status_code == 503
    assert response.text == "Redis unavailable"


def test_post_repo_detail_coder_returns_503_before_config_write_when_redis_ping_fails(
    two_repo_config: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _PingBoomRedis(_FakeRedis):
        async def ping(self) -> bool:
            raise RuntimeError("redis is down")

    write_attempted = False

    def _fail_if_called(*args: object, **kwargs: object) -> None:
        nonlocal write_attempted
        write_attempted = True
        raise AssertionError("update_repository should not be called")

    monkeypatch.setattr(web_app, "update_repository", _fail_if_called)

    with TestClient(app) as client:
        client.app.state.redis = _PingBoomRedis()
        response = client.post(
            "/repos/example__alpha/coder",
            data={"coder": "codex"},
        )

    assert response.status_code == 503
    assert response.text == "Redis unavailable"
    assert write_attempted is False


def test_post_repo_detail_coder_defers_paused_repo_switch_message(
    two_repo_config: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    paused = RepoState(
        url="https://github.com/example/alpha.git",
        name="example__alpha",
        state=PipelineState.PAUSED,
        coder="claude",
        current_pr=PRInfo(number=17, branch="pr-017-sample"),
        last_updated=datetime(2026, 4, 20, 17, 0, 0, tzinfo=timezone.utc),
    )
    fake = _FakeRedis({"pipeline:example__alpha": paused.model_dump_json()})

    async def _fake_publish_repo_event(*args: object, **kwargs: object) -> None:
        return None

    async def _fake_repo_template_context(
        name: str,
        redis_client: object | None,
        *,
        coder_update_message: str | None = None,
        include_metrics: bool = False,
    ) -> dict[str, object]:
        return {"coder_update_message": coder_update_message or ""}

    def _fake_template_response(
        request: object,
        template_name: str,
        context: dict[str, object],
    ) -> HTMLResponse:
        return HTMLResponse(str(context["coder_update_message"]))

    monkeypatch.setattr(web_app, "publish_repo_event", _fake_publish_repo_event)
    monkeypatch.setattr(web_app, "update_repository", lambda *args, **kwargs: None)
    monkeypatch.setattr(web_app, "_repo_template_context", _fake_repo_template_context)
    monkeypatch.setattr(web_app.templates, "TemplateResponse", _fake_template_response)

    with TestClient(app) as client:
        client.app.state.redis = fake
        response = client.post(
            "/repos/example__alpha/coder",
            data={"coder": "codex"},
        )

    assert response.status_code == 200
    assert response.text == "Switching to Codex CLI - applies after current PR completes."
    stored = RepoState.model_validate_json(fake.store["pipeline:example__alpha"])
    assert stored.coder == "claude"


def test_post_repo_detail_coder_defers_hung_repo_switch_message(
    two_repo_config: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    hung = RepoState(
        url="https://github.com/example/alpha.git",
        name="example__alpha",
        state=PipelineState.HUNG,
        coder="claude",
        current_pr=PRInfo(number=17, branch="pr-017-sample"),
        last_updated=datetime(2026, 4, 20, 17, 0, 0, tzinfo=timezone.utc),
    )
    fake = _FakeRedis({"pipeline:example__alpha": hung.model_dump_json()})

    async def _fake_publish_repo_event(*args: object, **kwargs: object) -> None:
        return None

    async def _fake_repo_template_context(
        name: str,
        redis_client: object | None,
        *,
        coder_update_message: str | None = None,
        include_metrics: bool = False,
    ) -> dict[str, object]:
        return {"coder_update_message": coder_update_message or ""}

    def _fake_template_response(
        request: object,
        template_name: str,
        context: dict[str, object],
    ) -> HTMLResponse:
        return HTMLResponse(str(context["coder_update_message"]))

    monkeypatch.setattr(web_app, "publish_repo_event", _fake_publish_repo_event)
    monkeypatch.setattr(web_app, "update_repository", lambda *args, **kwargs: None)
    monkeypatch.setattr(web_app, "_repo_template_context", _fake_repo_template_context)
    monkeypatch.setattr(web_app.templates, "TemplateResponse", _fake_template_response)

    with TestClient(app) as client:
        client.app.state.redis = fake
        response = client.post(
            "/repos/example__alpha/coder",
            data={"coder": "codex"},
        )

    assert response.status_code == 200
    assert response.text == "Switching to Codex CLI - applies after current PR completes."
    stored = RepoState.model_validate_json(fake.store["pipeline:example__alpha"])
    assert stored.coder == "claude"


def test_post_repo_detail_coder_updates_idle_repo_via_transaction(
    two_repo_config: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    idle = RepoState(
        url="https://github.com/example/alpha.git",
        name="example__alpha",
        state=PipelineState.IDLE,
        coder="claude",
        last_updated=datetime(2026, 4, 20, 17, 0, 0, tzinfo=timezone.utc),
    )

    class _TrackingRedis(_FakeRedis):
        def __init__(self, store: dict[str, str] | None = None) -> None:
            super().__init__(store)
            self.transactions: list[tuple[str, ...]] = []

        async def ping(self) -> bool:
            return True

        async def transaction(
            self,
            func,
            *watches: str,
            value_from_callable: bool = False,
            **_kwargs: object,
        ):
            self.transactions.append(tuple(watches))
            return await super().transaction(
                func,
                *watches,
                value_from_callable=value_from_callable,
                **_kwargs,
            )

    fake = _TrackingRedis({"pipeline:example__alpha": idle.model_dump_json()})

    async def _fake_publish_repo_event(*args: object, **kwargs: object) -> None:
        return None

    async def _fake_repo_template_context(
        name: str,
        redis_client: object | None,
        *,
        coder_update_message: str | None = None,
        include_metrics: bool = False,
    ) -> dict[str, object]:
        return {"coder_update_message": coder_update_message or ""}

    def _fake_template_response(
        request: object,
        template_name: str,
        context: dict[str, object],
    ) -> HTMLResponse:
        return HTMLResponse(str(context["coder_update_message"]))

    monkeypatch.setattr(web_app, "publish_repo_event", _fake_publish_repo_event)
    monkeypatch.setattr(web_app, "_repo_template_context", _fake_repo_template_context)
    monkeypatch.setattr(web_app.templates, "TemplateResponse", _fake_template_response)

    with TestClient(app) as client:
        client.app.state.redis = fake
        response = client.post(
            "/repos/example__alpha/coder",
            data={"coder": "codex"},
        )

    assert response.status_code == 200
    assert response.text == "Switching to Codex CLI."
    stored = RepoState.model_validate_json(fake.store["pipeline:example__alpha"])
    assert stored.coder == "codex"
    assert ("pipeline:example__alpha",) in fake.transactions


def test_post_repo_detail_coder_skips_missing_state_during_transaction(
    two_repo_config: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    idle = RepoState(
        url="https://github.com/example/alpha.git",
        name="example__alpha",
        state=PipelineState.IDLE,
        coder="claude",
        last_updated=datetime(2026, 4, 20, 17, 0, 0, tzinfo=timezone.utc),
    )

    class _MissingStateRedis(_FakeRedis):
        async def ping(self) -> bool:
            return True

        async def transaction(
            self,
            func,
            *watches: str,
            value_from_callable: bool = False,
            **_kwargs: object,
        ):
            self.store.pop("pipeline:example__alpha", None)
            return await super().transaction(
                func,
                *watches,
                value_from_callable=value_from_callable,
                **_kwargs,
            )

    fake = _MissingStateRedis({"pipeline:example__alpha": idle.model_dump_json()})

    async def _fake_publish_repo_event(*args: object, **kwargs: object) -> None:
        return None

    async def _fake_repo_template_context(
        name: str,
        redis_client: object | None,
        *,
        coder_update_message: str | None = None,
        include_metrics: bool = False,
    ) -> dict[str, object]:
        return {"coder_update_message": coder_update_message or ""}

    def _fake_template_response(
        request: object,
        template_name: str,
        context: dict[str, object],
    ) -> HTMLResponse:
        return HTMLResponse(str(context["coder_update_message"]))

    monkeypatch.setattr(web_app, "publish_repo_event", _fake_publish_repo_event)
    monkeypatch.setattr(web_app, "_repo_template_context", _fake_repo_template_context)
    monkeypatch.setattr(web_app.templates, "TemplateResponse", _fake_template_response)

    with TestClient(app) as client:
        client.app.state.redis = fake
        response = client.post(
            "/repos/example__alpha/coder",
            data={"coder": "codex"},
        )

    assert response.status_code == 200
    assert response.text == "Switching to Codex CLI."
    assert "pipeline:example__alpha" not in fake.store


def test_post_repo_detail_coder_skips_transaction_when_state_turns_hung(
    two_repo_config: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    idle = RepoState(
        url="https://github.com/example/alpha.git",
        name="example__alpha",
        state=PipelineState.IDLE,
        coder="claude",
        last_updated=datetime(2026, 4, 20, 17, 0, 0, tzinfo=timezone.utc),
    )

    class _HungDuringTransactionRedis(_FakeRedis):
        async def ping(self) -> bool:
            return True

        async def transaction(
            self,
            func,
            *watches: str,
            value_from_callable: bool = False,
            **_kwargs: object,
        ):
            self.store["pipeline:example__alpha"] = RepoState(
                url="https://github.com/example/alpha.git",
                name="example__alpha",
                state=PipelineState.HUNG,
                coder="claude",
                current_pr=PRInfo(number=17, branch="pr-017-sample"),
                last_updated=datetime(2026, 4, 20, 17, 0, 0, tzinfo=timezone.utc),
            ).model_dump_json()
            return await super().transaction(
                func,
                *watches,
                value_from_callable=value_from_callable,
                **_kwargs,
            )

    fake = _HungDuringTransactionRedis(
        {"pipeline:example__alpha": idle.model_dump_json()}
    )

    async def _fake_publish_repo_event(*args: object, **kwargs: object) -> None:
        return None

    async def _fake_repo_template_context(
        name: str,
        redis_client: object | None,
        *,
        coder_update_message: str | None = None,
        include_metrics: bool = False,
    ) -> dict[str, object]:
        return {"coder_update_message": coder_update_message or ""}

    def _fake_template_response(
        request: object,
        template_name: str,
        context: dict[str, object],
    ) -> HTMLResponse:
        return HTMLResponse(str(context["coder_update_message"]))

    monkeypatch.setattr(web_app, "publish_repo_event", _fake_publish_repo_event)
    monkeypatch.setattr(web_app, "_repo_template_context", _fake_repo_template_context)
    monkeypatch.setattr(web_app.templates, "TemplateResponse", _fake_template_response)

    with TestClient(app) as client:
        client.app.state.redis = fake
        response = client.post(
            "/repos/example__alpha/coder",
            data={"coder": "codex"},
        )

    assert response.status_code == 200
    assert response.text == "Switching to Codex CLI - applies after current PR completes."
    stored = RepoState.model_validate_json(fake.store["pipeline:example__alpha"])
    assert stored.coder == "claude"


def test_post_repo_detail_coder_returns_success_when_publish_event_fails(
    two_repo_config: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    idle = RepoState(
        url="https://github.com/example/alpha.git",
        name="example__alpha",
        state=PipelineState.IDLE,
        coder="claude",
        last_updated=datetime(2026, 4, 20, 17, 0, 0, tzinfo=timezone.utc),
    )
    fake = _FakeRedis({"pipeline:example__alpha": idle.model_dump_json()})

    async def _failing_publish_repo_event(*args: object, **kwargs: object) -> None:
        raise RuntimeError("event bus unavailable")

    async def _fake_repo_template_context(
        name: str,
        redis_client: object | None,
        *,
        coder_update_message: str | None = None,
        include_metrics: bool = False,
    ) -> dict[str, object]:
        return {"coder_update_message": coder_update_message or ""}

    def _fake_template_response(
        request: object,
        template_name: str,
        context: dict[str, object],
    ) -> HTMLResponse:
        return HTMLResponse(str(context["coder_update_message"]))

    monkeypatch.setattr(web_app, "publish_repo_event", _failing_publish_repo_event)
    monkeypatch.setattr(web_app, "_repo_template_context", _fake_repo_template_context)
    monkeypatch.setattr(web_app.templates, "TemplateResponse", _fake_template_response)

    with TestClient(app) as client:
        client.app.state.redis = fake
        response = client.post(
            "/repos/example__alpha/coder",
            data={"coder": "codex"},
        )

    assert response.status_code == 200
    assert response.text == "Switching to Codex CLI."
    stored = RepoState.model_validate_json(fake.store["pipeline:example__alpha"])
    assert stored.coder == "codex"
    reloaded = load_config(str(two_repo_config))
    assert reloaded.repositories[0].coder == "codex"


def test_post_repo_detail_coder_publishes_coder_swap_wake_event(
    two_repo_config: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    idle = RepoState(
        url="https://github.com/example/alpha.git",
        name="example__alpha",
        state=PipelineState.IDLE,
        coder="claude",
    )
    fake = _FakeRedis({"pipeline:example__alpha": idle.model_dump_json()})

    async def _fake_publish_repo_event(*args: object, **kwargs: object) -> None:
        return None

    async def _fake_repo_template_context(
        name: str,
        redis_client: object | None,
        *,
        coder_update_message: str | None = None,
        include_metrics: bool = False,
    ) -> dict[str, object]:
        return {"coder_update_message": coder_update_message or ""}

    def _fake_template_response(
        request: object,
        template_name: str,
        context: dict[str, object],
    ) -> HTMLResponse:
        return HTMLResponse(str(context["coder_update_message"]))

    monkeypatch.setattr(web_app, "publish_repo_event", _fake_publish_repo_event)
    monkeypatch.setattr(web_app, "_repo_template_context", _fake_repo_template_context)
    monkeypatch.setattr(web_app.templates, "TemplateResponse", _fake_template_response)

    with TestClient(app) as client:
        client.app.state.redis = fake
        response = client.post(
            "/repos/example__alpha/coder",
            data={"coder": "codex"},
        )

    assert response.status_code == 200
    wake_events = [
        (channel, message)
        for channel, message in fake.published
        if channel == "orchestrator:wake:example__alpha"
    ]
    assert len(wake_events) == 1
    payload = json.loads(wake_events[0][1])
    assert payload["event_type"] == "coder_swap"
    assert payload["repo"] == "example__alpha"


def test_post_repo_detail_coder_succeeds_when_publish_wake_fails(
    two_repo_config: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Coder swap must persist even when the wake publish raises."""

    class _PublishBoomRedis(_FakeRedis):
        async def publish(self, channel: str, message: str) -> int:
            raise RuntimeError("publish boom")

    idle = RepoState(
        url="https://github.com/example/alpha.git",
        name="example__alpha",
        state=PipelineState.IDLE,
        coder="claude",
    )
    fake = _PublishBoomRedis({"pipeline:example__alpha": idle.model_dump_json()})

    async def _fake_repo_template_context(
        name: str,
        redis_client: object | None,
        *,
        coder_update_message: str | None = None,
        include_metrics: bool = False,
    ) -> dict[str, object]:
        return {"coder_update_message": coder_update_message or ""}

    def _fake_template_response(
        request: object,
        template_name: str,
        context: dict[str, object],
    ) -> HTMLResponse:
        return HTMLResponse(str(context["coder_update_message"]))

    monkeypatch.setattr(web_app, "_repo_template_context", _fake_repo_template_context)
    monkeypatch.setattr(web_app.templates, "TemplateResponse", _fake_template_response)

    with TestClient(app) as client:
        client.app.state.redis = fake
        with caplog.at_level("WARNING", logger=web_app.logger.name):
            response = client.post(
                "/repos/example__alpha/coder",
                data={"coder": "codex"},
            )

    assert response.status_code == 200
    stored = RepoState.model_validate_json(fake.store["pipeline:example__alpha"])
    assert stored.coder == "codex"
    reloaded = load_config(str(two_repo_config))
    assert reloaded.repositories[0].coder == "codex"
    assert any(
        "publish_wake failed for example__alpha" in rec.getMessage()
        and "coder_swap" in rec.getMessage()
        for rec in caplog.records
    )


def test_put_repo_detail_coder_publishes_settings_wake_event(
    two_repo_config: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake = _FakeRedis()

    with TestClient(app) as client:
        client.app.state.redis = fake
        response = client.put(
            "/settings/repo/example__alpha",
            data={"coder": "codex"},
        )

    assert response.status_code == 200
    wake_events = [
        (channel, message)
        for channel, message in fake.published
        if channel == "orchestrator:wake:example__alpha"
    ]
    assert len(wake_events) == 1
    payload = json.loads(wake_events[0][1])
    assert payload["event_type"] == "settings"
    assert payload["repo"] == "example__alpha"


def test_put_repo_detail_coder_succeeds_when_publish_wake_fails(
    two_repo_config: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Per-repo settings save must persist even when the wake publish raises."""

    class _PublishBoomRedis(_FakeRedis):
        async def publish(self, channel: str, message: str) -> int:
            raise RuntimeError("publish boom")

    fake = _PublishBoomRedis()

    with TestClient(app) as client:
        client.app.state.redis = fake
        with caplog.at_level("WARNING", logger=web_app.logger.name):
            response = client.put(
                "/settings/repo/example__alpha",
                data={"coder": "codex"},
            )

    assert response.status_code == 200
    reloaded = load_config(str(two_repo_config))
    assert reloaded.repositories[0].coder is not None
    assert reloaded.repositories[0].coder.value == "codex"
    assert any(
        "publish_wake failed for example__alpha" in rec.getMessage()
        and "settings" in rec.getMessage()
        for rec in caplog.records
    )


def test_coder_swap_wake_message_resets_daemon_last_run(
    two_repo_config: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """End-to-end: coder swap publishes a wake message recognised by the
    daemon, so a coder change during long IDLE polling is applied within the
    next ``_wait_or_wake`` tick instead of waiting up to
    ``idle_extended_poll_interval_sec`` (default 300s).
    """
    from src.daemon import main as daemon_main

    idle = RepoState(
        url="https://github.com/example/alpha.git",
        name="example__alpha",
        state=PipelineState.IDLE,
        coder="claude",
    )
    fake = _FakeRedis({"pipeline:example__alpha": idle.model_dump_json()})

    async def _fake_publish_repo_event(*args: object, **kwargs: object) -> None:
        return None

    async def _fake_repo_template_context(
        name: str,
        redis_client: object | None,
        *,
        coder_update_message: str | None = None,
        include_metrics: bool = False,
    ) -> dict[str, object]:
        return {"coder_update_message": coder_update_message or ""}

    def _fake_template_response(
        request: object,
        template_name: str,
        context: dict[str, object],
    ) -> HTMLResponse:
        return HTMLResponse(str(context["coder_update_message"]))

    monkeypatch.setattr(web_app, "publish_repo_event", _fake_publish_repo_event)
    monkeypatch.setattr(web_app, "_repo_template_context", _fake_repo_template_context)
    monkeypatch.setattr(web_app.templates, "TemplateResponse", _fake_template_response)

    with TestClient(app) as client:
        client.app.state.redis = fake
        response = client.post(
            "/repos/example__alpha/coder",
            data={"coder": "codex"},
        )

    assert response.status_code == 200
    wake_events = [
        (channel, message)
        for channel, message in fake.published
        if channel == "orchestrator:wake:example__alpha"
    ]
    assert len(wake_events) == 1
    channel, message = wake_events[0]
    payload = json.loads(message)
    assert payload["event_type"] == "coder_swap"

    last_run = {"alpha-key": 100.0, "beta-key": 200.0}
    slug_to_key = {"example__alpha": "alpha-key", "example__beta": "beta-key"}
    daemon_main._apply_wake_message(
        {"channel": channel}, last_run, slug_to_key
    )

    assert last_run["alpha-key"] == 0.0
    assert last_run["beta-key"] == 200.0


def _make_bucket_redis(
    *,
    rest_remaining: int | None = None,
    graphql_remaining: int | None = None,
    limit: int = 5000,
    reset_at: datetime | None = None,
) -> _FakeRedis:
    from src.daemon.github_rate_limit import (
        BUDGET_GRAPHQL_REDIS_KEY,
        BUDGET_REST_REDIS_KEY,
        RateLimitBudget,
    )

    reset = reset_at or (datetime.now(timezone.utc) + timedelta(seconds=1800))
    store: dict[str, str] = {}
    if rest_remaining is not None:
        store[BUDGET_REST_REDIS_KEY] = RateLimitBudget(
            installation_id=None,
            remaining=rest_remaining,
            limit=limit,
            reset_at=reset,
        ).to_redis_payload()
    if graphql_remaining is not None:
        store[BUDGET_GRAPHQL_REDIS_KEY] = RateLimitBudget(
            installation_id=None,
            remaining=graphql_remaining,
            limit=limit,
            reset_at=reset,
        ).to_redis_payload()
    return _FakeRedis(store)


def _claude_state(
    *,
    name: str = "octo__demo",
    coder: str | None = "claude",
    session_percent: int | None = None,
    session_resets_at: int | None = None,
    weekly_percent: int | None = None,
    weekly_resets_at: int | None = None,
    last_updated: datetime | None = None,
    active: bool = True,
) -> RepoState:
    return RepoState(
        url=f"https://github.com/{name.replace('__', '/')}",
        name=name,
        coder=coder,
        usage_session_percent=session_percent,
        usage_session_resets_at=session_resets_at,
        usage_weekly_percent=weekly_percent,
        usage_weekly_resets_at=weekly_resets_at,
        last_updated=last_updated or datetime.now(timezone.utc),
        active=active,
    )


def test_resource_zone_boundaries() -> None:
    # Spec boundaries: 50% → green, 49% → amber, 20% → amber, 19% → red.
    assert _resource_zone(100.0) == "green"
    assert _resource_zone(50.0) == "green"
    assert _resource_zone(49.9) == "amber"
    assert _resource_zone(20.0) == "amber"
    assert _resource_zone(19.9) == "red"
    assert _resource_zone(0.0) == "red"
    assert _resource_zone(None) == "none"


def test_format_reset_unix_renders_utc_clock() -> None:
    assert _format_reset_unix(0) == "unknown"
    assert _format_reset_unix(None) == "unknown"
    # 2024-01-01 12:34:56 UTC.
    assert _format_reset_unix(1704112496) == "12:34 UTC"


def test_build_resources_view_missing_data_renders_neutral() -> None:
    """No GitHub or Claude data anywhere → all four chips render as none."""
    view = asyncio.run(_build_resources_view(_FakeRedis(), []))

    assert set(view) == {"github_rest", "github_graphql", "claude_5h", "claude_weekly"}
    for chip in view.values():
        assert chip["remaining"] is None
        assert chip["percent_remaining"] is None
        assert chip["zone"] == "none"


def test_build_resources_view_populates_github_buckets_independently() -> None:
    """REST and GraphQL chips must come from their own keys, not the constrained min."""
    redis = _make_bucket_redis(rest_remaining=4500, graphql_remaining=1000)

    view = asyncio.run(_build_resources_view(redis, []))

    assert view["github_rest"]["remaining"] == 4500
    assert view["github_rest"]["zone"] == "green"  # 90%
    assert view["github_graphql"]["remaining"] == 1000
    assert view["github_graphql"]["zone"] == "amber"  # 20%
    # Claude chips are still neutral when no Claude state is provided.
    assert view["claude_5h"]["zone"] == "none"
    assert view["claude_weekly"]["zone"] == "none"


def test_build_resources_view_red_zone_when_buckets_near_exhaustion() -> None:
    redis = _make_bucket_redis(rest_remaining=10, graphql_remaining=900)

    view = asyncio.run(_build_resources_view(redis, []))

    # 0.2% remaining → red.
    assert view["github_rest"]["zone"] == "red"
    # 18% remaining → red.
    assert view["github_graphql"]["zone"] == "red"


def test_build_resources_view_neutralizes_expired_bucket_snapshot() -> None:
    """Snapshots whose ``reset_at`` has passed must render neutral, not red.

    GitHub rolls the bucket over at ``reset_at``; a stale low-remaining
    snapshot from before the rollover would otherwise hold a critical
    chip indefinitely even though the daemon already stopped throttling.
    """
    expired_reset = datetime.now(timezone.utc) - timedelta(seconds=60)
    redis = _make_bucket_redis(
        rest_remaining=10, graphql_remaining=10, reset_at=expired_reset
    )

    view = asyncio.run(_build_resources_view(redis, []))

    for key in ("github_rest", "github_graphql"):
        assert view[key]["zone"] == "none"
        assert view[key]["percent_remaining"] is None
        assert view[key]["remaining"] is None
        assert view[key]["reset_unix"] is None


def test_build_resources_view_picks_most_recent_claude_state() -> None:
    """Aggregation walks repos and uses the freshest Claude snapshot."""
    older = _claude_state(
        name="octo__a",
        session_percent=10,
        weekly_percent=5,
        last_updated=datetime(2026, 1, 1, tzinfo=timezone.utc),
    )
    newer = _claude_state(
        name="octo__b",
        session_percent=70,
        session_resets_at=1700000000,
        weekly_percent=80,
        weekly_resets_at=1700100000,
        last_updated=datetime(2026, 5, 1, tzinfo=timezone.utc),
    )
    # A non-Claude state must be ignored even if it carries usage fields.
    other = _claude_state(
        name="octo__c",
        coder="codex",
        session_percent=99,
        weekly_percent=99,
        last_updated=datetime(2026, 6, 1, tzinfo=timezone.utc),
    )

    view = asyncio.run(_build_resources_view(_FakeRedis(), [older, newer, other]))

    # 100 - 70 = 30% remaining → amber.
    assert view["claude_5h"]["percent_remaining"] == 30.0
    assert view["claude_5h"]["zone"] == "amber"
    assert view["claude_5h"]["reset_unix"] == 1700000000
    # 100 - 80 = 20% remaining → amber.
    assert view["claude_weekly"]["percent_remaining"] == 20.0
    assert view["claude_weekly"]["zone"] == "amber"


def test_claude_usage_chip_returns_neutral_when_no_claude_state() -> None:
    only_codex = _claude_state(coder="codex", session_percent=10, weekly_percent=10)
    chip = _claude_usage_chip([only_codex], window="session")
    assert chip["zone"] == "none"
    assert chip["percent_remaining"] is None


def test_claude_usage_chip_skips_states_without_window_data() -> None:
    no_session = _claude_state(weekly_percent=20)
    no_weekly = _claude_state(session_percent=20)
    assert _claude_usage_chip([no_session], window="session")["zone"] == "none"
    assert _claude_usage_chip([no_weekly], window="weekly")["zone"] == "none"


def test_claude_usage_chip_excludes_inactive_repos() -> None:
    """Disabled Claude repos must not win the freshness race.

    Inactive runners stop refreshing usage fields but still bump
    ``last_updated`` each publish, so without this gate a disabled repo's
    stale snapshot would supplant the active account's current observation.
    """
    active = _claude_state(
        name="octo__active",
        session_percent=10,
        session_resets_at=1700000000,
        weekly_percent=15,
        weekly_resets_at=1700100000,
        last_updated=datetime(2026, 1, 1, tzinfo=timezone.utc),
        active=True,
    )
    inactive_newer = _claude_state(
        name="octo__inactive",
        session_percent=90,
        session_resets_at=1700200000,
        weekly_percent=95,
        weekly_resets_at=1700300000,
        last_updated=datetime(2026, 5, 1, tzinfo=timezone.utc),
        active=False,
    )

    session = _claude_usage_chip(
        [active, inactive_newer], window="session"
    )
    weekly = _claude_usage_chip(
        [active, inactive_newer], window="weekly"
    )

    # Active repo's snapshot wins despite being older.
    assert session["percent_remaining"] == 90.0
    assert session["reset_unix"] == 1700000000
    assert weekly["percent_remaining"] == 85.0
    assert weekly["reset_unix"] == 1700100000


def test_claude_usage_chip_neutral_when_only_candidate_inactive() -> None:
    """Only inactive Claude repos available → render neutral, not stale data."""
    only_inactive = _claude_state(
        session_percent=20,
        weekly_percent=30,
        active=False,
    )
    assert (
        _claude_usage_chip([only_inactive], window="session")["zone"] == "none"
    )
    assert (
        _claude_usage_chip([only_inactive], window="weekly")["zone"] == "none"
    )


def test_build_recent_graphql_burns_view_returns_none_without_data() -> None:
    assert (
        asyncio.run(_build_recent_graphql_burns_view(_FakeRedis(), "octo__demo"))
        is None
    )


def test_build_recent_graphql_burns_view_computes_avg_and_max() -> None:
    from src.daemon.github_rate_limit import BURNS_REDIS_KEY_PREFIX

    redis = _FakeRedis(
        lists={f"{BURNS_REDIS_KEY_PREFIX}octo__demo": ["10", "20", "0"]}
    )

    view = asyncio.run(_build_recent_graphql_burns_view(redis, "octo__demo"))

    assert view is not None
    assert view["burns"] == [10, 20, 0]
    assert view["max"] == 20
    assert view["avg"] == 10.0


def test_repo_template_context_exposes_recent_graphql_burns(
    two_repo_config: Path,
) -> None:
    """The per-repo render context surfaces the burn summary when present."""
    from src.daemon.github_rate_limit import BURNS_REDIS_KEY_PREFIX

    stored = RepoState(
        url="https://github.com/example/alpha.git",
        name="example__alpha",
        state=PipelineState.IDLE,
    )
    fake = _FakeRedis(
        store={"pipeline:example__alpha": stored.model_dump_json()},
        lists={f"{BURNS_REDIS_KEY_PREFIX}example__alpha": ["7", "13"]},
    )

    context = asyncio.run(web_app._repo_template_context("example__alpha", fake))

    assert context["recent_graphql_burns"] == {
        "burns": [7, 13],
        "avg": 10.0,
        "max": 13,
    }


def test_repo_summary_renders_recent_graphql_burns(
    two_repo_config: Path,
) -> None:
    """The detail page surfaces avg/max without breaking the existing layout."""
    from src.daemon.github_rate_limit import BURNS_REDIS_KEY_PREFIX

    stored = RepoState(
        url="https://github.com/example/alpha.git",
        name="example__alpha",
        state=PipelineState.IDLE,
    )
    fake = _FakeRedis(
        store={"pipeline:example__alpha": stored.model_dump_json()},
        lists={f"{BURNS_REDIS_KEY_PREFIX}example__alpha": ["20", "10"]},
    )

    context = asyncio.run(web_app._repo_template_context("example__alpha", fake))
    rendered = web_app.templates.get_template(
        "components/repo_summary.html"
    ).render(context)

    assert "data-recent-graphql-burns" in rendered
    assert "GraphQL/cycle: avg 15.0 (max 20)" in rendered
    # Existing Current Task / Current PR layout still intact.
    assert "Current Task" in rendered
    assert "Current PR" in rendered


def test_repo_summary_hides_recent_graphql_burns_when_absent(
    two_repo_config: Path,
) -> None:
    """No burns recorded yet — the metric block is hidden, layout intact."""
    stored = RepoState(
        url="https://github.com/example/alpha.git",
        name="example__alpha",
        state=PipelineState.IDLE,
    )
    fake = _FakeRedis(store={"pipeline:example__alpha": stored.model_dump_json()})

    context = asyncio.run(web_app._repo_template_context("example__alpha", fake))
    rendered = web_app.templates.get_template(
        "components/repo_summary.html"
    ).render(context)

    assert "data-recent-graphql-burns" not in rendered
    assert "GraphQL/cycle" not in rendered
    assert "Current Task" in rendered
    assert "Current PR" in rendered


def test_repo_summary_omits_stalled_artifacts_when_last_updated_is_old(
    two_repo_config: Path,
) -> None:
    old = datetime.now(timezone.utc) - timedelta(seconds=60)
    stored = RepoState(
        url="https://github.com/example/alpha.git",
        name="example__alpha",
        state=PipelineState.IDLE,
        last_updated=old,
    )
    fake = _FakeRedis({"pipeline:example__alpha": stored.model_dump_json()})

    context = asyncio.run(web_app._repo_template_context("example__alpha", fake))
    rendered = web_app.templates.get_template(
        "components/repo_summary.html"
    ).render(context)

    assert 'class="stalled"' not in rendered
    assert "data-stalled-hint" not in rendered
    assert "data-state-indicator" not in rendered
    assert "data-updated-at" not in rendered


def test_repo_cards_omit_stalled_artifacts_when_last_updated_is_old(
    two_repo_config: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    old = datetime.now(timezone.utc) - timedelta(seconds=60)
    stored = RepoState(
        url="https://github.com/example/alpha.git",
        name="example__alpha",
        state=PipelineState.IDLE,
        last_updated=old,
    )

    class _StubClient:
        async def ping(self) -> bool:
            return True

        async def get(self, key: str) -> str | None:
            if key == "pipeline:example__alpha":
                return stored.model_dump_json()
            return None

        async def aclose(self) -> None:
            return None

    class _StubAioredisOne:
        @staticmethod
        def from_url(url: str, decode_responses: bool = True) -> _StubClient:
            return _StubClient()

    monkeypatch.setattr(web_app, "aioredis", _StubAioredisOne())

    with TestClient(app) as client:
        response = client.get("/partials/repo-list")

    assert response.status_code == 200
    body = response.text
    assert 'class="stalled"' not in body
    assert "data-stalled-hint" not in body
    assert "data-state-indicator" not in body
    assert "data-updated-at" not in body


def test_repo_controls_have_spinners_with_unique_ids_per_repo(
    two_repo_config: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Pause/Stop buttons rendered by `_controls.html` for active states must
    each carry an `hx-indicator` pointing at a per-repo spinner partial, so two
    repo cards on the dashboard cannot share the same in-flight indicator."""
    now = datetime(2026, 4, 28, 12, 0, 0, tzinfo=timezone.utc)
    alpha = RepoState(
        url="https://github.com/example/alpha.git",
        name="example__alpha",
        state=PipelineState.CODING,
        last_updated=now,
    )
    beta = RepoState(
        url="https://github.com/example/beta.git",
        name="example__beta",
        state=PipelineState.CODING,
        last_updated=now,
    )
    fake = _FakeRedis(
        {
            "pipeline:example__alpha": alpha.model_dump_json(),
            "pipeline:example__beta": beta.model_dump_json(),
        }
    )
    monkeypatch.setattr(web_app, "aioredis", _stub_aioredis_with_state(fake))

    with TestClient(app) as client:
        response = client.get("/partials/repo-list")

    assert response.status_code == 200
    body = response.text
    for name in ("example__alpha", "example__beta"):
        assert f'hx-indicator="#controls-pause-spinner-{name}"' in body
        assert f'hx-indicator="#controls-stop-spinner-{name}"' in body
        assert f'id="controls-pause-spinner-{name}"' in body
        assert f'id="controls-stop-spinner-{name}"' in body
    assert body.count('id="controls-pause-spinner-example__alpha"') == 1
    assert body.count('id="controls-pause-spinner-example__beta"') == 1
    assert body.count('id="controls-stop-spinner-example__alpha"') == 1
    assert body.count('id="controls-stop-spinner-example__beta"') == 1
    assert body.count("htmx-indicator") >= 4
    assert body.count("animate-spin") >= 4


def test_repo_controls_resume_button_has_spinner(
    two_repo_config: Path,
) -> None:
    """The Resume button shown for IDLE-with-queued-tasks repos must include
    a per-repo spinner partial wired up via `hx-indicator`."""
    now = datetime(2026, 4, 28, 12, 0, 0, tzinfo=timezone.utc)
    stored = RepoState(
        url="https://github.com/example/alpha.git",
        name="example__alpha",
        state=PipelineState.IDLE,
        queue_done=0,
        queue_total=3,
        last_updated=now,
    )
    fake = _FakeRedis({"pipeline:example__alpha": stored.model_dump_json()})

    context = asyncio.run(web_app._repo_template_context("example__alpha", fake))
    rendered = web_app.templates.get_template(
        "components/repo_summary.html"
    ).render(context)

    assert (
        'hx-indicator="#controls-resume-spinner-example__alpha"' in rendered
    )
    assert 'id="controls-resume-spinner-example__alpha"' in rendered
    assert "htmx-indicator" in rendered
    assert "animate-spin" in rendered


def test_repo_coder_selector_form_has_spinner(
    two_repo_config: Path,
) -> None:
    """The per-repo coder dropdown form on the repo detail page must render
    a `hx-indicator` plus the shared spinner partial so users see feedback
    while the coder change is being persisted."""
    now = datetime(2026, 4, 28, 12, 0, 0, tzinfo=timezone.utc)
    stored = RepoState(
        url="https://github.com/example/alpha.git",
        name="example__alpha",
        state=PipelineState.IDLE,
        last_updated=now,
    )
    fake = _FakeRedis({"pipeline:example__alpha": stored.model_dump_json()})

    context = asyncio.run(web_app._repo_template_context("example__alpha", fake))
    rendered = web_app.templates.get_template(
        "components/repo_summary.html"
    ).render(context)

    assert 'hx-indicator="#coder-spinner-example__alpha"' in rendered
    assert 'id="coder-spinner-example__alpha"' in rendered
    assert "Saving..." in rendered
    assert "htmx-indicator" in rendered
    assert "animate-spin" in rendered


@pytest.mark.parametrize(
    "route_module",
    [
        "src.web.routes.dashboard",
        "src.web.routes.onboarding",
        "src.web.routes.repo_control",
        "src.web.routes.settings",
        "src.web.routes.uploads",
    ],
)
def test_route_module_entry_point_registers_all_routes(route_module: str) -> None:
    """Importing any route module before ``src.web.app`` must not drop routes.

    Each route module imports ``src.web.app`` to reach shared helpers. If the
    route module is the entry point of the import (a test or tool that does
    ``import src.web.routes.<name>`` first), Python returns the partial route
    module to ``app.py``'s ``from src.web.routes import <name> as ...`` line
    while the route module's ``@router`` decorators have not yet run.
    FastAPI snapshots ``router.routes`` at ``app.include_router`` time, so a
    naive early import would silently strip every endpoint declared after the
    import. Guard via subprocess so each entry point starts with a fresh
    ``sys.modules``.
    """
    import subprocess
    import sys

    script = (
        f"import {route_module}\n"
        "from src.web.app import app\n"
        "import json, sys\n"
        "json.dump(sorted({getattr(r, 'path', '') for r in app.routes}), sys.stdout)\n"
    )
    proc = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        check=True,
    )
    paths = set(json.loads(proc.stdout))

    baseline_script = (
        "from src.web.app import app\n"
        "import json, sys\n"
        "json.dump(sorted({getattr(r, 'path', '') for r in app.routes}), sys.stdout)\n"
    )
    baseline_proc = subprocess.run(
        [sys.executable, "-c", baseline_script],
        capture_output=True,
        text=True,
        check=True,
    )
    baseline = set(json.loads(baseline_proc.stdout))

    assert paths == baseline, (
        f"{route_module} as entry point produced different routes than baseline: "
        f"missing={baseline - paths}, extra={paths - baseline}"
    )


# ---------------------------------------------------------------------------
# operator_heartbeat_middleware (PR-255)
# ---------------------------------------------------------------------------


class _MiddlewareApp:
    """Minimal stand-in for a Starlette ``app`` exposing ``state.redis``."""

    def __init__(self, redis_client: object | None) -> None:
        self.state = type("_State", (), {"redis": redis_client})()


class _MiddlewareRequest:
    """Minimal Request shape consumed by ``operator_heartbeat_middleware``."""

    def __init__(
        self,
        method: str,
        redis_client: object | None,
        headers: dict[str, str] | None = None,
    ) -> None:
        self.method = method
        self.app = _MiddlewareApp(redis_client)
        self.headers = headers or {}


class _MiddlewareResponse:
    def __init__(self, status_code: int) -> None:
        self.status_code = status_code


class _RecordingRedis:
    """Captures ``set`` calls so tests can assert when (not) refreshed."""

    def __init__(self) -> None:
        self.set_calls: list[tuple[str, str, dict[str, object]]] = []

    async def set(self, key: str, value: str, **kwargs: object) -> None:
        self.set_calls.append((key, value, dict(kwargs)))


class _BoomSetRedis:
    async def set(self, key: str, value: str, **_kwargs: object) -> None:
        raise RuntimeError("redis is down")


def _run_heartbeat(
    *,
    method: str,
    status_code: int,
    redis_client: object | None,
    headers: dict[str, str] | None = None,
) -> _MiddlewareResponse:
    """Invoke the middleware with synthesized request/call_next."""

    request = _MiddlewareRequest(method, redis_client, headers=headers)
    response = _MiddlewareResponse(status_code)

    async def call_next(_req: _MiddlewareRequest) -> _MiddlewareResponse:
        return response

    returned = asyncio.run(
        web_app.operator_heartbeat_middleware(request, call_next)
    )
    assert returned is response
    return returned


def test_heartbeat_middleware_refreshes_on_successful_mutation() -> None:
    redis_client = _RecordingRedis()
    _run_heartbeat(method="POST", status_code=200, redis_client=redis_client)
    assert redis_client.set_calls == [
        ("operator_heartbeat", "1", {"ex": 300})
    ]


@pytest.mark.parametrize("method", ["PUT", "PATCH", "DELETE"])
def test_heartbeat_middleware_refreshes_on_other_mutating_methods(
    method: str,
) -> None:
    redis_client = _RecordingRedis()
    _run_heartbeat(method=method, status_code=204, redis_client=redis_client)
    assert len(redis_client.set_calls) == 1


def test_heartbeat_middleware_refreshes_on_htmx_get() -> None:
    """HTMX-driven GET = operator dashboard polling.

    The dashboard is GET-dominated; HTMX polls carry ``HX-Request: true``
    and must keep the heartbeat fresh while the operator watches.
    """
    redis_client = _RecordingRedis()
    _run_heartbeat(
        method="GET",
        status_code=200,
        redis_client=redis_client,
        headers={"hx-request": "true"},
    )
    assert redis_client.set_calls == [
        ("operator_heartbeat", "1", {"ex": 300})
    ]


@pytest.mark.parametrize("fetch_site", ["same-origin", "same-site"])
def test_heartbeat_middleware_refreshes_on_trusted_fetch_site_get(
    fetch_site: str,
) -> None:
    """Browser GET with trusted Sec-Fetch-Site counts as operator.

    Only ``same-origin``/``same-site`` are accepted: those imply the
    request originated from within the dashboard itself.
    """
    redis_client = _RecordingRedis()
    _run_heartbeat(
        method="GET",
        status_code=200,
        redis_client=redis_client,
        headers={"sec-fetch-site": fetch_site},
    )
    assert redis_client.set_calls == [
        ("operator_heartbeat", "1", {"ex": 300})
    ]


@pytest.mark.parametrize("fetch_site", ["cross-site", "none", "unknown", ""])
def test_heartbeat_middleware_skips_untrusted_fetch_site_get(
    fetch_site: str,
) -> None:
    """Untrusted Sec-Fetch-Site values must not refresh the heartbeat.

    ``cross-site`` = embedded by a third-party page; ``none`` = direct
    address-bar navigation or a curl probe spoofing the header; any
    unexpected value is treated the same way. None of these imply the
    operator is actively using the dashboard, so they must not pin
    ``operator_heartbeat`` alive and defeat off-hours AWAY behavior.
    """
    redis_client = _RecordingRedis()
    _run_heartbeat(
        method="GET",
        status_code=200,
        redis_client=redis_client,
        headers={"sec-fetch-site": fetch_site},
    )
    assert redis_client.set_calls == []


def test_heartbeat_middleware_skips_unattributed_get() -> None:
    """Bare GET (no HX-Request, no Sec-Fetch-*) = automated polling.

    Health checks, ``curl`` probes, and load-balancer pings must not
    pin the heartbeat key alive when no operator is present —
    otherwise ``HeartbeatSource`` would falsely report AVAILABLE
    during off-hours.
    """
    redis_client = _RecordingRedis()
    _run_heartbeat(method="GET", status_code=200, redis_client=redis_client)
    assert redis_client.set_calls == []


@pytest.mark.parametrize("method", ["HEAD", "OPTIONS"])
def test_heartbeat_middleware_skips_probe_methods(method: str) -> None:
    """HEAD/OPTIONS = probes and CORS preflight — not operator presence.

    Browser-style headers must not rescue these methods; CORS preflight
    sends ``Sec-Fetch-Mode: cors`` but is still not operator presence.
    """
    redis_client = _RecordingRedis()
    _run_heartbeat(
        method=method,
        status_code=200,
        redis_client=redis_client,
        headers={"sec-fetch-site": "same-origin"},
    )
    assert redis_client.set_calls == []


@pytest.mark.parametrize("status", [301, 302, 400, 401, 403, 404, 500, 503])
def test_heartbeat_middleware_skips_non_2xx_responses(status: int) -> None:
    """Failed/redirected mutations must not pin the heartbeat key alive."""
    redis_client = _RecordingRedis()
    _run_heartbeat(method="POST", status_code=status, redis_client=redis_client)
    assert redis_client.set_calls == []


def test_heartbeat_middleware_no_op_when_redis_missing() -> None:
    """Lifespan runs before requests, but a torn-down state must not crash."""
    _run_heartbeat(method="POST", status_code=200, redis_client=None)


def test_heartbeat_middleware_swallows_redis_failure() -> None:
    """Redis outage must not break the underlying request."""
    _run_heartbeat(
        method="POST", status_code=200, redis_client=_BoomSetRedis()
    )
