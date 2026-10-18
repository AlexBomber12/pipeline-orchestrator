"""Operator-facing publication bootstrap request and exact evidence display."""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from src.keyspace import pipeline_state
from src.models import PipelineState, PRInfo, PublicationMigration, RepoState
from src.publication_migration import (
    MigrationChanged,
    MigrationCommand,
    command_key,
    enqueue,
    migration_binding,
    pending_key,
)
from src.web import app as web_app
from src.web.app import app

from tests.runner._helpers import _FakeRedis


class _Redis(_FakeRedis):
    async def ping(self) -> bool:
        return True

    async def aclose(self) -> None:
        return None

    async def publish(self, _channel: str, _message: str) -> int:
        return 1


def _proposal(*, published: bool = True) -> PublicationMigration:
    return PublicationMigration(
        repo="example/alpha", pr_number=620, head_repo="example/alpha",
        ref="refs/heads/pr-459", head_sha="a" * 40,
        published_at=datetime(2026, 10, 17, 21, 6, 35, tzinfo=timezone.utc)
        if published else None,
        provenance="github-activity:push" if published else "",
        reason="Historical publication needs an operator trust anchor.",
    )


@pytest.fixture
def client(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    cfg = tmp_path / "config.yml"
    cfg.write_text("repositories:\n  - url: https://github.com/example/alpha.git\n")
    monkeypatch.chdir(tmp_path)
    redis = _Redis()
    monkeypatch.setattr(
        web_app, "aioredis",
        type("StubRedis", (), {"from_url": staticmethod(lambda *_args, **_kwargs: redis)})(),
    )
    with TestClient(app) as http:
        yield http, redis


def test_panel_displays_exact_observation_and_confirmation_binding() -> None:
    observation = _proposal()
    state = RepoState(
        url="https://github.com/example/alpha.git", name="example__alpha",
        state=PipelineState.WATCH, publication_migration=observation,
        current_pr=PRInfo(number=620, branch="pr-459", head_sha="a" * 40),
    )
    html = web_app.templates.get_template(
        "components/_publication_migration_panel.html"
    ).render(
        repo=state, publication_migration_binding=migration_binding(observation),
        publication_migration_confirmable=True, publication_migration_command=None,
    )
    assert "#620" in html
    assert "2026-10-17T21:06:35+00:00" in html
    assert "github-activity:push" in html
    assert "a" * 40 in html
    assert f'"binding": "{migration_binding(observation)}"' in html


def test_confirmation_queues_one_exact_command(client) -> None:
    http, redis = client
    observation = _proposal()
    state = RepoState(
        url="https://github.com/example/alpha.git", name="example__alpha",
        state=PipelineState.WATCH, publication_migration=observation,
        current_pr=PRInfo(number=620, branch="pr-459", head_sha="a" * 40),
    )
    redis.store[pipeline_state(state.name)] = state.model_dump_json()
    binding = migration_binding(observation)
    path = f"/repos/{state.name}/publication-migration/confirm"
    response = http.post(path, data={"binding": binding})
    assert response.status_code == 202
    assert MigrationCommand.model_validate_json(redis.store[command_key(state.name, binding)]).status == "pending"
    assert http.post(path, data={"binding": binding}).status_code == 202
    assert len([key for key in redis.store if ":publication_migration:command:" in key]) == 1


def test_confirmation_refuses_unknown_or_changed_observation(client) -> None:
    http, redis = client
    observation = _proposal(published=False)
    state = RepoState(
        url="https://github.com/example/alpha.git", name="example__alpha",
        state=PipelineState.WATCH, publication_migration=observation,
        current_pr=PRInfo(number=620, branch="pr-459", head_sha="a" * 40),
    )
    redis.store[pipeline_state(state.name)] = state.model_dump_json()
    path = f"/repos/{state.name}/publication-migration/confirm"
    assert http.post(path, data={"binding": migration_binding(observation)}).status_code == 409
    changed = _proposal()
    state.publication_migration = changed
    redis.store[pipeline_state(state.name)] = state.model_dump_json()
    assert http.post(path, data={"binding": migration_binding(observation)}).status_code == 409


def test_command_rejects_missing_state_and_competing_request() -> None:
    redis = _Redis()
    observation = _proposal()
    binding = migration_binding(observation)
    with pytest.raises(MigrationChanged, match="state is unavailable"):
        asyncio.run(enqueue(redis, "example__alpha", binding))
    state = RepoState(
        url="https://github.com/example/alpha.git", name="example__alpha",
        state=PipelineState.WATCH, publication_migration=observation,
        current_pr=PRInfo(number=620, branch="pr-459", head_sha="a" * 40),
    )
    redis.store[pipeline_state(state.name)] = state.model_dump_json()
    redis.store[pending_key(state.name)] = "b" * 64
    with pytest.raises(MigrationChanged, match="Another publication migration"):
        asyncio.run(enqueue(redis, state.name, binding))
