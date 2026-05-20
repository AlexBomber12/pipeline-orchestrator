"""Tests for per-inhibitor badges on paused repository cards."""

from __future__ import annotations

import asyncio
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from fastapi.testclient import TestClient
from src.inhibitor import InhibitorType, WorkInhibitor
from src.keyspace import control_stop, pipeline_state
from src.models import PipelineState, RepoState
from src.web import app as web_app
from src.web.app import app
from src.web.routes import repo_control


class _FakeRedis:
    def __init__(self, store: dict[str, str] | None = None) -> None:
        self.store = dict(store or {})
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
        func: Any,
        *_watches: str,
        value_from_callable: bool = False,
        **_kwargs: object,
    ) -> Any:
        pipe = _FakePipeline(self)
        value = func(pipe)
        if asyncio.iscoroutine(value):
            value = await value
        await pipe.execute()
        return value if value_from_callable else None

    async def aclose(self) -> None:
        return None


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

    async def execute(self) -> None:
        for command, args, kwargs in self.commands:
            if command == "set":
                await self.redis.set(args[0], args[1], **kwargs)
            elif command == "delete":
                await self.redis.delete(args[0])


class _DeleteBoomRedis(_FakeRedis):
    async def delete(self, key: str) -> int:
        raise RuntimeError(f"failed to delete {key}")


class _PublishBoomRedis(_FakeRedis):
    async def publish(self, channel: str, message: str) -> int:
        raise RuntimeError(f"failed to publish {channel}: {message}")


def _stub_aioredis(redis_client: _FakeRedis) -> object:
    return type(
        "_StubAioredis",
        (),
        {"from_url": staticmethod(lambda url, decode_responses=True: redis_client)},
    )()


@pytest.fixture
def repo_config(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    cfg = tmp_path / "config.yml"
    cfg.write_text(
        "repositories:\n"
        "  - url: https://github.com/example/alpha.git\n",
        encoding="utf-8",
    )
    monkeypatch.chdir(tmp_path)
    return cfg


def _inhibitor(
    inhibitor_type: InhibitorType,
    *,
    expires_at: datetime | None = None,
) -> WorkInhibitor:
    return WorkInhibitor(
        inhibitor_type=inhibitor_type,
        expires_at=expires_at,
        reason_text=f"{inhibitor_type.value} reason",
        source_key=f"test:{inhibitor_type.value}",
    )


def _state(
    *,
    state: PipelineState = PipelineState.PAUSED,
    active_inhibitors: list[WorkInhibitor] | None = None,
    user_paused: bool = False,
) -> RepoState:
    return RepoState(
        url="https://github.com/example/alpha.git",
        name="example__alpha",
        state=state,
        user_paused=user_paused,
        active_inhibitors=active_inhibitors or [],
        last_updated=datetime(2026, 5, 20, 12, tzinfo=timezone.utc),
    )


def _render_card(repo: RepoState) -> str:
    return web_app.templates.get_template("components/repo_cards.html").render(
        repos=[repo],
        resources={},
        cancellation_subsources={},
        subsource_lookup=lambda _name: None,
        drain_progress={},
        inhibitor_labels=web_app.INHIBITOR_LABELS,
    )


def test_paused_card_renders_one_badge_per_inhibitor() -> None:
    future = datetime.now(timezone.utc) + timedelta(minutes=15)
    html = _render_card(
        _state(
            active_inhibitors=[
                _inhibitor(InhibitorType.USER_PAUSE),
                _inhibitor(InhibitorType.RATE_LIMIT, expires_at=future),
                _inhibitor(InhibitorType.SPEND_CEILING),
            ],
            user_paused=True,
        )
    )

    assert html.count('data-inhibitor="') == 3
    assert 'data-inhibitor="user_pause"' in html
    assert 'data-inhibitor="rate_limit"' in html
    assert 'data-inhibitor="spend_ceiling"' in html
    assert "Operator paused" in html
    assert "Rate-limited" in html
    assert "Spend ceiling" in html


def test_clear_inhibitor_user_pause_returns_200(
    repo_config: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    stored = _state(
        active_inhibitors=[_inhibitor(InhibitorType.USER_PAUSE)],
        user_paused=True,
    )
    redis = _FakeRedis({pipeline_state("example__alpha"): stored.model_dump_json()})
    monkeypatch.setattr(web_app, "aioredis", _stub_aioredis(redis))

    with TestClient(app) as client:
        response = client.post("/repos/example__alpha/inhibitors/clear/user_pause")

    assert response.status_code == 200
    updated = RepoState.model_validate_json(redis.store[pipeline_state("example__alpha")])
    assert updated.user_paused is False


def test_clear_inhibitor_user_stop_returns_200(
    repo_config: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    stored = _state(active_inhibitors=[_inhibitor(InhibitorType.USER_STOP)])
    redis = _FakeRedis(
        {
            pipeline_state("example__alpha"): stored.model_dump_json(),
            control_stop("example__alpha"): "1",
        }
    )
    monkeypatch.setattr(web_app, "aioredis", _stub_aioredis(redis))

    with TestClient(app) as client:
        response = client.post("/repos/example__alpha/inhibitors/clear/user_stop")

    assert response.status_code == 200
    assert control_stop("example__alpha") not in redis.store


def test_clear_inhibitor_non_clearable_returns_400(
    repo_config: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    redis = _FakeRedis()
    monkeypatch.setattr(web_app, "aioredis", _stub_aioredis(redis))

    with TestClient(app) as client:
        response = client.post("/repos/example__alpha/inhibitors/clear/spend_ceiling")

    assert response.status_code == 400


def test_clear_inhibitor_without_redis_returns_503() -> None:
    request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace()))

    response = asyncio.run(
        repo_control.clear_inhibitor(request, "example__alpha", "user_pause")
    )

    assert response.status_code == 503


def test_clear_inhibitor_user_pause_returns_mutation_error(
    repo_config: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    redis = _FakeRedis({pipeline_state("example__alpha"): "not-json"})
    monkeypatch.setattr(web_app, "aioredis", _stub_aioredis(redis))

    with TestClient(app) as client:
        response = client.post("/repos/example__alpha/inhibitors/clear/user_pause")

    assert response.status_code == 503


def test_clear_inhibitor_user_stop_delete_failure_returns_503(
    repo_config: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    stored = _state(active_inhibitors=[_inhibitor(InhibitorType.USER_STOP)])
    redis = _DeleteBoomRedis(
        {
            pipeline_state("example__alpha"): stored.model_dump_json(),
            control_stop("example__alpha"): "1",
        }
    )
    monkeypatch.setattr(web_app, "aioredis", _stub_aioredis(redis))

    with TestClient(app) as client:
        response = client.post("/repos/example__alpha/inhibitors/clear/user_stop")

    assert response.status_code == 503


def test_clear_inhibitor_succeeds_when_publish_wake_fails(
    repo_config: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    stored = _state(active_inhibitors=[_inhibitor(InhibitorType.USER_STOP)])
    redis = _PublishBoomRedis(
        {
            pipeline_state("example__alpha"): stored.model_dump_json(),
            control_stop("example__alpha"): "1",
        }
    )
    monkeypatch.setattr(web_app, "aioredis", _stub_aioredis(redis))

    with TestClient(app) as client:
        with caplog.at_level("WARNING", logger=web_app.logger.name):
            response = client.post("/repos/example__alpha/inhibitors/clear/user_stop")

    assert response.status_code == 200
    assert any(
        "publish_wake failed for example__alpha" in record.getMessage()
        for record in caplog.records
    )


def test_paused_card_with_no_inhibitors_no_stack() -> None:
    html = _render_card(_state())

    assert "data-inhibitor-stack" not in html


def test_idle_card_no_inhibitor_stack() -> None:
    html = _render_card(
        _state(
            state=PipelineState.IDLE,
            active_inhibitors=[_inhibitor(InhibitorType.USER_PAUSE)],
            user_paused=True,
        )
    )

    assert "data-inhibitor-stack" not in html


def test_clear_action_triggers_publish_wake(
    repo_config: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    stored = _state(active_inhibitors=[_inhibitor(InhibitorType.USER_STOP)])
    redis = _FakeRedis(
        {
            pipeline_state("example__alpha"): stored.model_dump_json(),
            control_stop("example__alpha"): "1",
        }
    )
    monkeypatch.setattr(web_app, "aioredis", _stub_aioredis(redis))

    with TestClient(app) as client:
        response = client.post("/repos/example__alpha/inhibitors/clear/user_stop")

    assert response.status_code == 200
    wake_payloads = [
        json.loads(message)
        for channel, message in redis.published
        if channel == "orchestrator:wake:example__alpha"
    ]
    assert wake_payloads[-1]["event_type"] == "inhibitor_cleared"
