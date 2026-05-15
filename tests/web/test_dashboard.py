"""Tests for the dashboard cascade-panic banner (PR-308b)."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from src.keyspace import daemon_panic_state
from src.web import app as web_app
from src.web.app import app
from src.web.routes import dashboard as dashboard_routes


class _DashboardRedis:
    """In-memory Redis stub serving the dashboard's panic-state read."""

    def __init__(self, store: dict[str, str] | None = None) -> None:
        self.store: dict[str, str] = dict(store or {})

    async def ping(self) -> bool:
        return True

    async def get(self, key: str) -> str | None:
        return self.store.get(key)

    async def aclose(self) -> None:
        return None


def _aioredis(redis_client: _DashboardRedis) -> object:
    return type(
        "_Aioredis",
        (),
        {"from_url": staticmethod(lambda url, decode_responses=True: redis_client)},
    )()


@pytest.fixture
def empty_config(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    cfg = tmp_path / "config.yml"
    cfg.write_text("repositories: []\n", encoding="utf-8")
    monkeypatch.chdir(tmp_path)
    return cfg


def test_panic_banner_renders_when_state_enabled(
    empty_config: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    triggered_at = datetime(2026, 5, 15, 11, 30, tzinfo=timezone.utc).isoformat()
    payload = {
        "enabled": True,
        "reason": "cascade_escalate_threshold_exceeded",
        "triggered_at": triggered_at,
        "affected_repos": ["example__alpha", "example__beta", "example__gamma"],
        "threshold_at_trigger": 3,
    }
    redis_client = _DashboardRedis({daemon_panic_state(): json.dumps(payload)})
    monkeypatch.setattr(web_app, "aioredis", _aioredis(redis_client))

    with TestClient(app) as client:
        response = client.get("/")

    assert response.status_code == 200
    body = response.text
    assert 'id="cascade-panic-banner"' in body
    assert "CASCADE PANIC" in body
    assert "cascade_escalate_threshold_exceeded" in body
    assert "example__alpha" in body
    assert 'hx-post="/daemon/panic/resume"' in body


def test_panic_banner_omits_when_state_absent(
    empty_config: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    redis_client = _DashboardRedis()
    monkeypatch.setattr(web_app, "aioredis", _aioredis(redis_client))

    with TestClient(app) as client:
        response = client.get("/")

    assert response.status_code == 200
    assert 'id="cascade-panic-banner"' not in response.text


def test_panic_banner_omits_when_state_disabled(
    empty_config: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An ``enabled: false`` record must render no banner."""
    payload = {"enabled": False, "reason": "manual_resume"}
    redis_client = _DashboardRedis({daemon_panic_state(): json.dumps(payload)})
    monkeypatch.setattr(web_app, "aioredis", _aioredis(redis_client))

    with TestClient(app) as client:
        response = client.get("/")

    assert 'id="cascade-panic-banner"' not in response.text


@pytest.mark.asyncio
async def test_read_panic_state_for_banner_handles_bytes_payload() -> None:
    """Bytes-mode Redis returns must decode cleanly into the banner dict."""

    class _BytesRedis:
        async def get(self, key: str) -> bytes:
            return b'{"enabled": true, "reason": "x"}'

    parsed = await dashboard_routes._read_panic_state_for_banner(_BytesRedis())
    assert parsed == {"enabled": True, "reason": "x"}


@pytest.mark.asyncio
async def test_read_panic_state_for_banner_returns_none_for_invalid_json() -> None:
    class _BrokenJsonRedis:
        async def get(self, key: str) -> str:
            return "{not json"

    assert (
        await dashboard_routes._read_panic_state_for_banner(_BrokenJsonRedis())
        is None
    )


@pytest.mark.asyncio
async def test_read_panic_state_for_banner_returns_none_when_not_dict() -> None:
    class _ListRedis:
        async def get(self, key: str) -> str:
            return "[1, 2, 3]"

    assert (
        await dashboard_routes._read_panic_state_for_banner(_ListRedis())
        is None
    )


@pytest.mark.asyncio
async def test_read_panic_state_for_banner_swallows_redis_errors() -> None:
    class _BoomRedis:
        async def get(self, key: str) -> str:
            raise RuntimeError("redis down")

    assert (
        await dashboard_routes._read_panic_state_for_banner(_BoomRedis())
        is None
    )


@pytest.mark.asyncio
async def test_read_panic_state_for_banner_returns_none_without_redis() -> None:
    assert await dashboard_routes._read_panic_state_for_banner(None) is None
