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


def test_failed_receipt_can_be_retried_for_same_valid_observation(client) -> None:
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
    assert http.post(path, data={"binding": binding}).status_code == 202
    key = command_key(state.name, binding)
    failed = MigrationCommand.model_validate_json(redis.store[key])
    failed.status = "failed"
    failed.reason = "PR was temporarily missing from the GitHub response."
    redis.store[key] = failed.model_dump_json()
    redis.store.pop(pending_key(state.name))
    html = web_app.templates.get_template(
        "components/_publication_migration_panel.html"
    ).render(
        repo=state, publication_migration_binding=binding,
        publication_migration_confirmable=True, publication_migration_command=failed,
    )
    assert 'data-migration-command-status="failed"' in html
    assert "Confirm initial publication floor" in html
    assert http.post(path, data={"binding": binding}).status_code == 202
    assert MigrationCommand.model_validate_json(redis.store[key]).status == "pending"
    assert redis.store[pending_key(state.name)] == binding


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


def test_confirmation_http_failures_and_durable_wake(
    client, monkeypatch: pytest.MonkeyPatch,
) -> None:
    http, redis = client
    name = "example__alpha"
    path = f"/repos/{name}/publication-migration/confirm"
    observation = _proposal()
    binding = migration_binding(observation)
    assert http.post(path, data={"binding": "bad"}).status_code == 400
    assert http.post("/repos/unknown__repo/publication-migration/confirm",
                     data={"binding": binding}).status_code == 404
    original = http.app.state.redis
    http.app.state.redis = None
    assert http.post(path, data={"binding": binding}).status_code == 503
    http.app.state.redis = original

    state = RepoState(
        url="https://github.com/example/alpha.git", name=name,
        state=PipelineState.WATCH, publication_migration=observation,
        current_pr=PRInfo(number=620, branch="pr-459", head_sha="a" * 40),
    )
    redis.store[pipeline_state(name)] = state.model_dump_json()

    async def failed_enqueue(*_args):
        raise ValueError("redis unavailable")

    monkeypatch.setattr("src.web.routes.repo_control.enqueue_publication_migration", failed_enqueue)
    assert http.post(path, data={"binding": binding}).status_code == 503
    monkeypatch.setattr("src.web.routes.repo_control.enqueue_publication_migration", enqueue)

    async def failed_wake(*_args):
        raise RuntimeError("wake unavailable")

    monkeypatch.setattr(web_app, "publish_wake", failed_wake)
    assert http.post(path, data={"binding": binding}).status_code == 202
    assert redis.store[pending_key(name)] == binding


def test_summary_displays_receipt_and_survives_redis_read_error(
    client, monkeypatch: pytest.MonkeyPatch,
) -> None:
    http, redis = client
    observation = _proposal()
    name = "example__alpha"
    state = RepoState(
        url="https://github.com/example/alpha.git", name=name,
        state=PipelineState.WATCH, publication_migration=observation,
        current_pr=PRInfo(number=620, branch="pr-459", head_sha="a" * 40),
    )
    redis.store[pipeline_state(name)] = state.model_dump_json()
    binding = migration_binding(observation)
    receipt = MigrationCommand(binding=binding, observation=observation, status="deferred",
                               reason="Waiting for current evidence.")
    redis.store[command_key(name, binding)] = receipt.model_dump_json()
    response = http.get(f"/partials/repo/{name}")
    assert response.status_code == 200
    assert "Waiting for current evidence." in response.text

    original_get = redis.get

    async def failed_receipt_read(key: str):
        if key == command_key(name, binding):
            raise RuntimeError("temporary outage")
        return await original_get(key)

    monkeypatch.setattr(redis, "get", failed_receipt_read)
    response = http.get(f"/partials/repo/{name}")
    assert response.status_code == 200
    assert "data-publication-migration" in response.text
