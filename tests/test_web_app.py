"""Tests for src/web/app.py."""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from pathlib import Path

import pytest
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
    app,
    get_all_repo_states,
    get_repo_state,
    repo_name_from_url,
)


class _FakeRedis:
    def __init__(self, store: dict[str, str] | None = None) -> None:
        self.store = store or {}

    async def get(self, key: str) -> str | None:
        return self.store.get(key)


class _BoomRedis:
    def __init__(self) -> None:
        self.calls: list[str] = []

    async def get(self, key: str) -> str | None:
        self.calls.append(key)
        raise RuntimeError("redis is down")


class _StubAioredisClient:
    async def get(self, key: str) -> str | None:
        return None

    async def aclose(self) -> None:
        return None


class _StubAioredis:
    """Drop-in replacement for ``redis.asyncio`` used by the lifespan."""

    @staticmethod
    def from_url(url: str, decode_responses: bool = True) -> _StubAioredisClient:
        return _StubAioredisClient()


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


def test_repo_name_from_url_strips_git_suffix() -> None:
    assert repo_name_from_url("https://github.com/example/alpha.git") == "alpha"


def test_repo_name_from_url_without_git_suffix() -> None:
    assert repo_name_from_url("https://github.com/example/beta") == "beta"


def test_repo_name_from_url_trailing_slash() -> None:
    assert repo_name_from_url("https://github.com/example/gamma/") == "gamma"


def test_get_all_repo_states_no_redis_returns_idle_defaults(
    two_repo_config: Path,
) -> None:
    states = asyncio.run(get_all_repo_states(redis_client=None))

    assert [s.name for s in states] == ["alpha", "beta"]
    for state in states:
        assert state.state == PipelineState.IDLE
        assert state.current_task is None
        assert state.current_pr is None
        assert state.error_message is None


def test_get_all_repo_states_uses_redis_when_present(
    two_repo_config: Path,
) -> None:
    now = datetime(2026, 4, 10, 12, 0, 0, tzinfo=timezone.utc)
    stored = RepoState(
        url="https://github.com/example/alpha.git",
        name="alpha",
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
    fake = _FakeRedis({"pipeline:alpha": stored.model_dump_json()})

    states = asyncio.run(get_all_repo_states(redis_client=fake))

    assert states[0].state == PipelineState.CODING
    assert states[0].current_task is not None
    assert states[0].current_task.pr_id == "PR-099"
    assert states[0].current_pr is not None
    assert states[0].current_pr.number == 42
    # second repo has no redis entry -> idle default
    assert states[1].state == PipelineState.IDLE
    assert states[1].current_task is None


def test_get_all_repo_states_falls_back_when_redis_raises(
    two_repo_config: Path,
) -> None:
    boom = _BoomRedis()
    states = asyncio.run(get_all_repo_states(redis_client=boom))

    assert len(states) == 2
    for state in states:
        assert state.state == PipelineState.IDLE
    # After the first failure further Redis lookups must be skipped so an
    # unreachable broker cannot turn each repo into another timing-out call.
    assert boom.calls == ["pipeline:alpha"]


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
    assert "Repositories" in body
    assert 'hx-get="/partials/repo-list"' in body


def test_api_states_returns_json(
    two_repo_config: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(web_app, "aioredis", _StubAioredis())

    with TestClient(app) as client:
        response = client.get("/api/states")

    assert response.status_code == 200
    payload = response.json()
    assert isinstance(payload, list)
    assert {p["name"] for p in payload} == {"alpha", "beta"}
    for entry in payload:
        assert entry["state"] == "IDLE"


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
    assert "IDLE" in body


def test_partial_repo_list_empty_state(
    empty_config: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(web_app, "aioredis", _StubAioredis())

    with TestClient(app) as client:
        response = client.get("/partials/repo-list")

    assert response.status_code == 200
    assert "No repositories configured" in response.text


def test_get_repo_state_unknown_repo_returns_idle_default(
    empty_config: Path,
) -> None:
    state = asyncio.run(get_repo_state("ghost", redis_client=None))

    assert state.name == "ghost"
    assert state.url == ""
    assert state.state == PipelineState.IDLE
    assert state.current_task is None
    assert state.current_pr is None


def test_get_repo_state_uses_redis_payload(two_repo_config: Path) -> None:
    now = datetime(2026, 4, 10, 12, 0, 0, tzinfo=timezone.utc)
    stored = RepoState(
        url="https://github.com/example/alpha.git",
        name="alpha",
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
    fake = _FakeRedis({"pipeline:alpha": stored.model_dump_json()})

    state = asyncio.run(get_repo_state("alpha", redis_client=fake))

    assert state.state == PipelineState.WATCH
    assert state.current_pr is not None
    assert state.current_pr.number == 17
    assert state.history[-1]["event"] == "watching CI"


def test_repo_detail_route_renders_full_page(
    two_repo_config: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(web_app, "aioredis", _StubAioredis())

    with TestClient(app) as client:
        response = client.get("/repo/alpha")

    assert response.status_code == 200
    assert "text/html" in response.headers["content-type"]
    body = response.text
    assert "<!DOCTYPE" in body
    assert "alpha" in body
    assert "All repositories" in body
    assert 'hx-get="/partials/repo/alpha"' in body
    assert 'hx-trigger="every 5s"' in body
    assert "Current Task" in body
    assert "Current PR" in body
    assert "Event log" in body


def test_partial_repo_detail_returns_html_fragment(
    two_repo_config: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(web_app, "aioredis", _StubAioredis())

    with TestClient(app) as client:
        response = client.get("/partials/repo/alpha")

    assert response.status_code == 200
    body = response.text
    assert "<!DOCTYPE" not in body  # fragment, not full document
    assert "alpha" in body
    assert "IDLE" in body
    assert "Current Task" in body
    assert "Current PR" in body
    assert "Event log" in body


def test_partial_repo_detail_renders_redis_payload(
    two_repo_config: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    now = datetime(2026, 4, 10, 12, 0, 0, tzinfo=timezone.utc)
    stored = RepoState(
        url="https://github.com/example/alpha.git",
        name="alpha",
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
        async def get(self, key: str) -> str | None:
            if key == "pipeline:alpha":
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
        response = client.get("/partials/repo/alpha")

    assert response.status_code == 200
    body = response.text
    assert "PR-042" in body
    assert "Wire it up" in body
    assert "#99" in body
    assert "https://github.com/example/alpha/pull/99" in body
    assert "pr-042-wire-it-up" in body
    assert "claude started" in body
    assert "CODING" in body
