"""Tests for the daemon-level operator control endpoints (PR-308b)."""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from src.keyspace import daemon_panic_state
from src.web import app as web_app
from src.web.app import app


class _PanicResumeRedis:
    """In-memory Redis stub for the panic-resume endpoint."""

    def __init__(self, store: dict[str, str] | None = None) -> None:
        self.store: dict[str, str] = dict(store or {})
        self.deleted: list[str] = []

    async def ping(self) -> bool:
        return True

    async def get(self, key: str) -> str | None:
        return self.store.get(key)

    async def delete(self, key: str) -> int:
        self.deleted.append(key)
        return 1 if self.store.pop(key, None) is not None else 0

    async def aclose(self) -> None:
        return None


def _aioredis(redis_client: _PanicResumeRedis) -> object:
    return type(
        "_Aioredis",
        (),
        {"from_url": staticmethod(lambda url, decode_responses=True: redis_client)},
    )()


def test_panic_resume_clears_state_returns_204(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    redis_client = _PanicResumeRedis({daemon_panic_state(): '{"enabled": true}'})
    monkeypatch.setattr(web_app, "aioredis", _aioredis(redis_client))

    with TestClient(app) as client:
        response = client.post("/daemon/panic/resume")

    assert response.status_code == 204
    assert response.content == b""
    assert daemon_panic_state() in redis_client.deleted
    assert daemon_panic_state() not in redis_client.store


def test_panic_resume_when_inactive_returns_204_idempotent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    redis_client = _PanicResumeRedis()
    monkeypatch.setattr(web_app, "aioredis", _aioredis(redis_client))

    with TestClient(app) as client:
        first = client.post("/daemon/panic/resume")
        second = client.post("/daemon/panic/resume")

    assert first.status_code == 204
    assert second.status_code == 204
    # Both calls reached Redis: idempotent endpoint must not 404 when the
    # key is already absent. Two delete attempts, neither finds anything.
    assert redis_client.deleted == [daemon_panic_state(), daemon_panic_state()]


def test_panic_resume_swallows_redis_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _BrokenRedis(_PanicResumeRedis):
        async def delete(self, key: str) -> int:
            raise RuntimeError("redis down")

    redis_client = _BrokenRedis({daemon_panic_state(): '{"enabled": true}'})
    monkeypatch.setattr(web_app, "aioredis", _aioredis(redis_client))

    with TestClient(app) as client:
        response = client.post("/daemon/panic/resume")

    # The button must remain clickable even when Redis is unreachable —
    # surfacing a 500 to HTMX would leave the banner stuck on screen.
    assert response.status_code == 204


def test_panic_resume_without_redis_returns_204(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _NullAioredis:
        @staticmethod
        def from_url(url: str, decode_responses: bool = True) -> None:
            return None

    monkeypatch.setattr(web_app, "aioredis", _NullAioredis())

    with TestClient(app) as client:
        response = client.post("/daemon/panic/resume")

    assert response.status_code == 204
