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
from src.web.app import app, get_all_repo_states, repo_name_from_url


class _FakeRedis:
    def __init__(self, store: dict[str, str] | None = None) -> None:
        self.store = store or {}

    async def get(self, key: str) -> str | None:
        return self.store.get(key)


class _BoomRedis:
    async def get(self, key: str) -> str | None:
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
    states = asyncio.run(get_all_repo_states(redis_client=_BoomRedis()))

    assert len(states) == 2
    for state in states:
        assert state.state == PipelineState.IDLE


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
