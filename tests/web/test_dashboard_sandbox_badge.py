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


@pytest.fixture
def isolation_on_config(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    cfg = tmp_path / "config.yml"
    cfg.write_text(
        "repositories: []\ndaemon:\n  coder_filesystem_isolation: true\n",
        encoding="utf-8",
    )
    monkeypatch.chdir(tmp_path)
    return cfg


def test_dashboard_renders_active_badge_when_state_is_active(
    isolation_on_config: Path, monkeypatch: pytest.MonkeyPatch
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
    isolation_on_config: Path, monkeypatch: pytest.MonkeyPatch
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


def test_dashboard_forces_disabled_when_config_off_overrides_stale_active(
    empty_config: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Config flag is off but Redis still holds the previous daemon
    # boot's "active" probe (daemon down, inotify reload not yet
    # processed, etc.). The current config wins: badge must render
    # "disabled" so the dashboard cannot show a green sandbox while
    # operators have explicitly turned it off.
    redis_client = _DashboardRedis(
        {REDIS_SANDBOX_STATE_KEY: SandboxState.ACTIVE.value}
    )
    monkeypatch.setattr(web_app, "aioredis", _aioredis(redis_client))

    with TestClient(app) as client:
        response = client.get("/")

    assert response.status_code == 200
    body = response.text
    assert 'data-sandbox-badge="disabled"' in body
    assert 'data-sandbox-badge="active"' not in body


def test_dashboard_forces_unavailable_when_config_on_overrides_stale_disabled(
    isolation_on_config: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Config flag just flipped to on but Redis still holds the previous
    # daemon boot's "disabled" value. Trusting that cached value would
    # mis-render a gray badge while operators have enabled the sandbox.
    # Fall through to the safe "unavailable" fallback until the daemon
    # re-probes.
    redis_client = _DashboardRedis(
        {REDIS_SANDBOX_STATE_KEY: SandboxState.DISABLED.value}
    )
    monkeypatch.setattr(web_app, "aioredis", _aioredis(redis_client))

    with TestClient(app) as client:
        response = client.get("/")

    assert response.status_code == 200
    assert 'data-sandbox-badge="unavailable"' in response.text


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


def test_dashboard_falls_back_to_unavailable_when_redis_returns_garbage_with_isolation_on(
    isolation_on_config: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Config flag on; Redis value is not a recognized SandboxState. The
    # garbage parses as ValueError and falls through to the safe
    # "unavailable" view rather than crashing the render.
    redis_client = _DashboardRedis({REDIS_SANDBOX_STATE_KEY: "not-a-state"})
    monkeypatch.setattr(web_app, "aioredis", _aioredis(redis_client))

    with TestClient(app) as client:
        response = client.get("/")

    assert response.status_code == 200
    assert 'data-sandbox-badge="unavailable"' in response.text


def test_partial_sandbox_badge_returns_current_redis_state(
    isolation_on_config: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The /partials/sandbox-badge endpoint exists so the dashboard can
    # re-read daemon:sandbox_state via an HTMX trigger after a config
    # reload or bwrap install/removal updates the cached probe. Without
    # this endpoint the badge would only reflect the page-render value.
    redis_client = _DashboardRedis(
        {REDIS_SANDBOX_STATE_KEY: SandboxState.ACTIVE.value}
    )
    monkeypatch.setattr(web_app, "aioredis", _aioredis(redis_client))

    with TestClient(app) as client:
        response = client.get("/partials/sandbox-badge")

    assert response.status_code == 200
    body = response.text
    assert 'data-sandbox-badge="active"' in body
    assert "sandbox: active" in body
    assert "bg-ok/10" in body


def test_partial_sandbox_badge_reflects_state_transition(
    isolation_on_config: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Simulate the live-transition scenario the reviewer flagged: the
    # operator opens the dashboard while the daemon reports ``active``,
    # then bwrap is removed and the daemon's next probe writes
    # ``unavailable``. The HTMX-driven partial must pick up the new
    # value without a full browser reload.
    redis_client = _DashboardRedis(
        {REDIS_SANDBOX_STATE_KEY: SandboxState.ACTIVE.value}
    )
    monkeypatch.setattr(web_app, "aioredis", _aioredis(redis_client))

    with TestClient(app) as client:
        first = client.get("/partials/sandbox-badge")
        assert first.status_code == 200
        assert 'data-sandbox-badge="active"' in first.text

        redis_client.store[REDIS_SANDBOX_STATE_KEY] = (
            SandboxState.UNAVAILABLE.value
        )
        second = client.get("/partials/sandbox-badge")

    assert second.status_code == 200
    assert 'data-sandbox-badge="unavailable"' in second.text
    assert 'data-sandbox-badge="active"' not in second.text


def test_partial_sandbox_badge_honors_config_off(
    empty_config: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Even if Redis still holds a stale ``active`` probe, the partial
    # must respect the current config flag — matching the behavior of
    # the initial page render so a manual toggle off cannot stay green.
    redis_client = _DashboardRedis(
        {REDIS_SANDBOX_STATE_KEY: SandboxState.ACTIVE.value}
    )
    monkeypatch.setattr(web_app, "aioredis", _aioredis(redis_client))

    with TestClient(app) as client:
        response = client.get("/partials/sandbox-badge")

    assert response.status_code == 200
    assert 'data-sandbox-badge="disabled"' in response.text


def test_dashboard_wraps_badge_in_htmx_polling_container(
    empty_config: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The dashboard must wrap the badge in a container that polls the
    # /partials/sandbox-badge endpoint so daemon-driven state changes
    # propagate to an already-open page without a full reload.
    redis_client = _DashboardRedis()
    monkeypatch.setattr(web_app, "aioredis", _aioredis(redis_client))

    with TestClient(app) as client:
        response = client.get("/")

    assert response.status_code == 200
    body = response.text
    assert 'id="sandbox-badge-wrapper"' in body
    assert 'hx-get="/partials/sandbox-badge"' in body
    assert "every 30s" in body


@pytest.mark.asyncio
async def test_read_sandbox_state_handles_bytes_payload() -> None:
    class _BytesRedis:
        async def get(self, key: str) -> bytes:
            return SandboxState.ACTIVE.value.encode("utf-8")

    from src.config import AppConfig, DaemonConfig

    state = await dashboard_routes._read_sandbox_state(
        _BytesRedis(),
        AppConfig(daemon=DaemonConfig(coder_filesystem_isolation=True)),
    )
    assert state == SandboxState.ACTIVE.value


@pytest.mark.asyncio
async def test_read_sandbox_state_config_off_overrides_redis_active() -> None:
    class _ActiveRedis:
        async def get(self, key: str) -> str:
            return SandboxState.ACTIVE.value

    from src.config import AppConfig, DaemonConfig

    state = await dashboard_routes._read_sandbox_state(
        _ActiveRedis(),
        AppConfig(daemon=DaemonConfig(coder_filesystem_isolation=False)),
    )
    assert state == SandboxState.DISABLED.value


@pytest.mark.asyncio
async def test_read_sandbox_state_config_on_rejects_stale_disabled() -> None:
    class _DisabledRedis:
        async def get(self, key: str) -> str:
            return SandboxState.DISABLED.value

    from src.config import AppConfig, DaemonConfig

    state = await dashboard_routes._read_sandbox_state(
        _DisabledRedis(),
        AppConfig(daemon=DaemonConfig(coder_filesystem_isolation=True)),
    )
    assert state == SandboxState.UNAVAILABLE.value


@pytest.mark.asyncio
async def test_read_sandbox_state_swallows_redis_errors() -> None:
    class _BoomRedis:
        async def get(self, key: str) -> str:
            raise RuntimeError("redis down")

    from src.config import AppConfig, DaemonConfig

    # With isolation enabled the Redis read path is exercised; the
    # exception must be swallowed and the badge must fall back to the
    # safe "unavailable" view rather than propagating the error.
    state = await dashboard_routes._read_sandbox_state(
        _BoomRedis(),
        AppConfig(daemon=DaemonConfig(coder_filesystem_isolation=True)),
    )
    assert state == SandboxState.UNAVAILABLE.value


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
