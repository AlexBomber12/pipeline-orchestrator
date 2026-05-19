"""Dashboard rendering tests for the 3-state sandbox badge (PR-353)."""

from __future__ import annotations

from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from src.sandbox.runtime_state import (
    REDIS_SANDBOX_STATE_KEY,
    SandboxState,
)
from src.web import app as web_app
from src.web.app import app
from src.web.routes import dashboard as dashboard_routes


class _DashboardRedis:
    """In-memory Redis stub for dashboard reads."""

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


def test_dashboard_renders_active_badge_when_state_is_active(
    empty_config: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    redis_client = _DashboardRedis(
        {REDIS_SANDBOX_STATE_KEY: SandboxState.ACTIVE.value}
    )
    monkeypatch.setattr(web_app, "aioredis", _aioredis(redis_client))

    with TestClient(app) as client:
        response = client.get("/")

    assert response.status_code == 200
    body = response.text
    assert 'data-sandbox-badge="active"' in body
    assert "sandbox: active" in body
    assert "bg-ok/10" in body
    assert "text-ok" in body


def test_dashboard_renders_unavailable_badge_when_state_is_unavailable(
    empty_config: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    redis_client = _DashboardRedis(
        {REDIS_SANDBOX_STATE_KEY: SandboxState.UNAVAILABLE.value}
    )
    monkeypatch.setattr(web_app, "aioredis", _aioredis(redis_client))

    with TestClient(app) as client:
        response = client.get("/")

    assert response.status_code == 200
    body = response.text
    assert 'data-sandbox-badge="unavailable"' in body
    assert "sandbox: unavailable" in body
    assert "bg-fail/10" in body
    assert "text-fail" in body


def test_dashboard_renders_disabled_badge_when_state_is_disabled(
    empty_config: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    redis_client = _DashboardRedis(
        {REDIS_SANDBOX_STATE_KEY: SandboxState.DISABLED.value}
    )
    monkeypatch.setattr(web_app, "aioredis", _aioredis(redis_client))

    with TestClient(app) as client:
        response = client.get("/")

    assert response.status_code == 200
    body = response.text
    assert 'data-sandbox-badge="disabled"' in body
    assert "sandbox: disabled" in body
    assert "bg-gray-500/10" in body
    assert "text-gray-400" in body


def test_dashboard_falls_back_to_disabled_when_redis_value_missing(
    empty_config: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # No Redis value and config flag off: badge should render 'disabled'
    # so the operator can still tell that the sandbox is intentionally
    # off rather than "no data".
    redis_client = _DashboardRedis()
    monkeypatch.setattr(web_app, "aioredis", _aioredis(redis_client))

    with TestClient(app) as client:
        response = client.get("/")

    assert response.status_code == 200
    assert 'data-sandbox-badge="disabled"' in response.text


def test_dashboard_falls_back_to_unavailable_when_isolation_on_but_value_missing(
    empty_config: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Config flag on but daemon never wrote a probe result: render the
    # safe "unavailable" badge so the operator does not see a misleading
    # "active" claim on a fresh boot.
    empty_config.write_text(
        "repositories: []\ndaemon:\n  coder_filesystem_isolation: true\n",
        encoding="utf-8",
    )
    redis_client = _DashboardRedis()
    monkeypatch.setattr(web_app, "aioredis", _aioredis(redis_client))

    with TestClient(app) as client:
        response = client.get("/")

    assert response.status_code == 200
    assert 'data-sandbox-badge="unavailable"' in response.text


def test_dashboard_falls_back_when_redis_returns_garbage(
    empty_config: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    redis_client = _DashboardRedis({REDIS_SANDBOX_STATE_KEY: "not-a-state"})
    monkeypatch.setattr(web_app, "aioredis", _aioredis(redis_client))

    with TestClient(app) as client:
        response = client.get("/")

    assert response.status_code == 200
    # Default is the disabled view because the empty config has isolation
    # off; the badge falls back rather than rendering the garbage value.
    assert 'data-sandbox-badge="disabled"' in response.text


@pytest.mark.asyncio
async def test_read_sandbox_state_handles_bytes_payload() -> None:
    class _BytesRedis:
        async def get(self, key: str) -> bytes:
            return SandboxState.ACTIVE.value.encode("utf-8")

    from src.config import AppConfig

    state = await dashboard_routes._read_sandbox_state(
        _BytesRedis(), AppConfig()
    )
    assert state == SandboxState.ACTIVE.value


@pytest.mark.asyncio
async def test_read_sandbox_state_swallows_redis_errors() -> None:
    class _BoomRedis:
        async def get(self, key: str) -> str:
            raise RuntimeError("redis down")

    from src.config import AppConfig

    # Falls back to 'disabled' because the default AppConfig has
    # isolation off; the Redis exception must not propagate.
    state = await dashboard_routes._read_sandbox_state(
        _BoomRedis(), AppConfig()
    )
    assert state == SandboxState.DISABLED.value


@pytest.mark.asyncio
async def test_read_sandbox_state_without_redis_uses_config_fallback() -> None:
    from src.config import AppConfig, DaemonConfig

    cfg_off = AppConfig(daemon=DaemonConfig(coder_filesystem_isolation=False))
    assert (
        await dashboard_routes._read_sandbox_state(None, cfg_off)
        == SandboxState.DISABLED.value
    )

    cfg_on = AppConfig(daemon=DaemonConfig(coder_filesystem_isolation=True))
    assert (
        await dashboard_routes._read_sandbox_state(None, cfg_on)
        == SandboxState.UNAVAILABLE.value
    )
