"""Tests for src/web/app.py."""

from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone
from pathlib import Path

import pytest
from fastapi.responses import HTMLResponse
from fastapi.testclient import TestClient
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
    _find_repo_config_by_name,
    _format_duration_ms,
    _get_repo_state_safe,
    _parse_iso8601,
    _recent_repo_metrics_payload,
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
        return self.redis.store.get(key)

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
    monkeypatch.setattr(web_app, "aioredis", _stub_aioredis_with_state(fake))

    with TestClient(app) as client:
        pause = client.post("/repos/example__alpha/pause")
        assert pause.status_code == 200
        paused = RepoState.model_validate_json(fake.store["pipeline:example__alpha"])
        assert paused.user_paused is True

        stop = client.post("/repos/example__alpha/stop")
        assert stop.status_code == 200
        stopped = RepoState.model_validate_json(fake.store["pipeline:example__alpha"])
        assert stopped.user_paused is True
        assert fake.store["control:example__alpha:stop"] == "1"

        resume = client.post("/repos/example__alpha/resume")
        assert resume.status_code == 200
        resumed = RepoState.model_validate_json(fake.store["pipeline:example__alpha"])
        assert resumed.user_paused is False
        assert "control:example__alpha:stop" not in fake.store


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
    assert 'hx-trigger="every 5s"' in body
    assert "Current Task" in body
    assert "Current PR" in body
    assert 'hx-post="/repos/example__alpha/coder"' in body
    assert 'name="coder"' in body
    assert "Any (bandit picks per-PR)" in body
    assert "Recent PRs" not in body
    assert "Event log" in body


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
    assert '<div class="mb-4 rounded-lg border border-accent/30 bg-accent/10 px-4 py-3 text-sm text-accent">' in rendered
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
    assert "CODING" in body
    # Event log lives on /partials/repo/{name}/events now, so history
    # entries are no longer rendered by the summary partial.
    assert "claude started" not in body


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
    assert "event.target.closest('label,input,button,a')" in body


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
    assert 'h-11 w-11' in body


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
    assert body.count("pulse-dot") == (1 if should_pulse else 0)


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


def test_upload_rejects_queue_with_duplicate_pr_id(
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
    assert response.status_code == 400
    assert "duplicate pr_id" in response.text


def test_upload_rejects_queue_with_cycle(
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
    assert response.status_code == 400
    assert "dependency cycle" in response.text


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
