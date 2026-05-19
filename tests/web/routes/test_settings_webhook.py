"""Tests for the guardrail-notification webhook settings endpoints (PR-344b).

These cover the four ``daemon.*`` fields exposed under the Settings
Notifications section:

* ``guardrail_notification_webhook_url`` (optional, http(s) URL)
* ``guardrail_notification_min_tier`` (int, 1-2)
* ``guardrail_notification_timeout_seconds`` (int, 1-30; stored as int,
  Pydantic coerces to float on reload)
* ``dashboard_base_url`` (optional, http(s) URL)

The endpoint reuses the ruamel.yaml round-trip writer from PR-344a, so
comment preservation and atomic writes are exercised by the shared
``test_settings_spend_ceiling.py`` suite. This file focuses on the
field-specific validators, allow-list extension, and "blank clears
optional URL" semantics that distinguish the notification fields.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from src.config import load_config
from src.web import app as web_app
from src.web.app import app


class _StubAioredisClient:
    async def ping(self) -> bool:
        return True

    async def get(self, key: str) -> str | None:
        return None

    async def aclose(self) -> None:
        return None


class _StubAioredis:
    @staticmethod
    def from_url(url: str, decode_responses: bool = True) -> _StubAioredisClient:
        return _StubAioredisClient()


class _FakeCompleted:
    def __init__(self, returncode: int, stdout: str = "", stderr: str = "") -> None:
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr


@pytest.fixture(autouse=True)
def _stub_auth_subprocess(monkeypatch: pytest.MonkeyPatch) -> None:
    """Stub out auth probes so the settings page renders without real CLIs."""

    def fake_run(cmd: list[str], *args: object, **kwargs: object) -> _FakeCompleted:
        if cmd and cmd[0] == "claude":
            return _FakeCompleted(0, stdout="claude 1.2.3\n")
        if cmd and cmd[0] == "codex":
            if cmd[1:] == ["--version"]:
                return _FakeCompleted(0, stdout="codex-cli 0.121.0\n")
            return _FakeCompleted(0, stdout="Logged in with ChatGPT\n")
        if cmd and cmd[0] == "gh":
            return _FakeCompleted(
                0,
                stderr=(
                    "github.com\n"
                    "  ✓ Logged in to github.com as octocat (oauth_token)\n"
                ),
            )
        raise AssertionError(f"unexpected command: {cmd}")

    monkeypatch.setattr(web_app.subprocess, "run", fake_run)
    monkeypatch.setattr(web_app, "aioredis", _StubAioredis())


@pytest.fixture
def base_config(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Minimal config.yml with a ``daemon:`` block and one repo."""
    cfg = tmp_path / "config.yml"
    cfg.write_text(
        "repositories:\n"
        "  - url: https://github.com/example/alpha.git\n"
        "    branch: main\n"
        "daemon:\n"
        "  poll_interval_sec: 60\n",
        encoding="utf-8",
    )
    monkeypatch.chdir(tmp_path)
    return cfg


def test_settings_renders_4_webhook_fields(base_config: Path) -> None:
    """All four notification inputs/selects render in the settings page."""
    with TestClient(app) as client:
        response = client.get("/settings")

    assert response.status_code == 200
    body = response.text
    assert 'name="guardrail_notification_webhook_url"' in body
    assert 'name="guardrail_notification_min_tier"' in body
    assert 'name="guardrail_notification_timeout_seconds"' in body
    assert 'name="dashboard_base_url"' in body
    assert "Notifications" in body


def test_update_webhook_url_persists_to_yaml(base_config: Path) -> None:
    url = "https://hooks.slack.com/services/T000/B000/abc"
    with TestClient(app) as client:
        response = client.post(
            "/settings/config/guardrail_notification_webhook_url",
            data={"guardrail_notification_webhook_url": url},
        )

    assert response.status_code == 200
    cfg = load_config(str(base_config))
    assert cfg.daemon.guardrail_notification_webhook_url == url
    assert url in base_config.read_text(encoding="utf-8")


def test_update_webhook_url_validates_http_prefix(base_config: Path) -> None:
    with TestClient(app) as client:
        response = client.post(
            "/settings/config/guardrail_notification_webhook_url",
            data={"guardrail_notification_webhook_url": "ftp://example"},
        )

    assert response.status_code == 400
    # Error message must not echo the rejected URL back to the operator.
    assert "ftp://example" not in response.text
    cfg = load_config(str(base_config))
    assert cfg.daemon.guardrail_notification_webhook_url is None


def test_update_webhook_url_empty_allowed(base_config: Path) -> None:
    """Blank URL clears the key so the Pydantic default (None) re-applies."""
    base_config.write_text(
        "repositories:\n"
        "  - url: https://github.com/example/alpha.git\n"
        "    branch: main\n"
        "daemon:\n"
        "  poll_interval_sec: 60\n"
        "  guardrail_notification_webhook_url: https://hooks.slack.com/x\n",
        encoding="utf-8",
    )

    with TestClient(app) as client:
        response = client.post(
            "/settings/config/guardrail_notification_webhook_url",
            data={"guardrail_notification_webhook_url": ""},
        )

    assert response.status_code == 200
    body = base_config.read_text(encoding="utf-8")
    assert "guardrail_notification_webhook_url" not in body
    cfg = load_config(str(base_config))
    assert cfg.daemon.guardrail_notification_webhook_url is None


def test_update_min_tier_validates_enum(base_config: Path) -> None:
    """Out-of-range or non-numeric tier inputs must return 400."""
    with TestClient(app) as client:
        response_str = client.post(
            "/settings/config/guardrail_notification_min_tier",
            data={"guardrail_notification_min_tier": "P5"},
        )
        response_int = client.post(
            "/settings/config/guardrail_notification_min_tier",
            data={"guardrail_notification_min_tier": "5"},
        )

    assert response_str.status_code == 400
    assert response_int.status_code == 400
    cfg = load_config(str(base_config))
    assert cfg.daemon.guardrail_notification_min_tier == 1


def test_update_min_tier_persists_valid_value(base_config: Path) -> None:
    with TestClient(app) as client:
        response = client.post(
            "/settings/config/guardrail_notification_min_tier",
            data={"guardrail_notification_min_tier": "2"},
        )

    assert response.status_code == 200
    cfg = load_config(str(base_config))
    assert cfg.daemon.guardrail_notification_min_tier == 2


def test_update_timeout_validates_range(base_config: Path) -> None:
    with TestClient(app) as client:
        response_high = client.post(
            "/settings/config/guardrail_notification_timeout_seconds",
            data={"guardrail_notification_timeout_seconds": "100"},
        )
        response_zero = client.post(
            "/settings/config/guardrail_notification_timeout_seconds",
            data={"guardrail_notification_timeout_seconds": "0"},
        )

    assert response_high.status_code == 400
    assert response_zero.status_code == 400
    cfg = load_config(str(base_config))
    assert cfg.daemon.guardrail_notification_timeout_seconds == 5.0


def test_update_timeout_persists_valid_value(base_config: Path) -> None:
    with TestClient(app) as client:
        response = client.post(
            "/settings/config/guardrail_notification_timeout_seconds",
            data={"guardrail_notification_timeout_seconds": "12"},
        )

    assert response.status_code == 200
    cfg = load_config(str(base_config))
    assert cfg.daemon.guardrail_notification_timeout_seconds == 12.0


def test_update_dashboard_base_url_persists(base_config: Path) -> None:
    url = "https://orchestrator.alexbomber.com"
    with TestClient(app) as client:
        response = client.post(
            "/settings/config/dashboard_base_url",
            data={"dashboard_base_url": url},
        )

    assert response.status_code == 200
    cfg = load_config(str(base_config))
    assert cfg.daemon.dashboard_base_url == url
    assert url in base_config.read_text(encoding="utf-8")


def test_update_dashboard_base_url_empty_allowed(base_config: Path) -> None:
    """Blank dashboard URL clears the optional field (parity with webhook URL)."""
    base_config.write_text(
        "repositories:\n"
        "  - url: https://github.com/example/alpha.git\n"
        "    branch: main\n"
        "daemon:\n"
        "  poll_interval_sec: 60\n"
        "  dashboard_base_url: https://old.example.com\n",
        encoding="utf-8",
    )

    with TestClient(app) as client:
        response = client.post(
            "/settings/config/dashboard_base_url",
            data={"dashboard_base_url": ""},
        )

    assert response.status_code == 200
    body = base_config.read_text(encoding="utf-8")
    assert "dashboard_base_url" not in body
    cfg = load_config(str(base_config))
    assert cfg.daemon.dashboard_base_url is None


def test_webhook_section_renders_when_field_unset(base_config: Path) -> None:
    """Unset webhook URL must render value="" (not the literal "None" string).

    Pydantic ``str | None`` defaults to ``None``; the template must coerce
    to empty string so the operator sees an empty placeholder-driven input
    rather than the word "None" pre-filled.
    """
    with TestClient(app) as client:
        response = client.get("/settings")

    assert response.status_code == 200
    body = response.text
    assert 'name="guardrail_notification_webhook_url"' in body
    assert 'name="dashboard_base_url"' in body
    # Pydantic ``str | None`` defaults to ``None``; the template's
    # ``or ''`` coercion must prevent the literal "None" leaking into a
    # value attribute.
    assert 'value="None"' not in body


def test_unrelated_yaml_fields_preserved_after_webhook_update(
    base_config: Path,
) -> None:
    """Updating one notification field must not perturb other ``daemon:`` keys."""
    base_config.write_text(
        "repositories:\n"
        "  - url: https://github.com/example/alpha.git\n"
        "    branch: main\n"
        "daemon:\n"
        "  poll_interval_sec: 60\n"
        "  review_timeout_min: 45\n"
        "  claude_model: sonnet\n",
        encoding="utf-8",
    )

    with TestClient(app) as client:
        response = client.post(
            "/settings/config/guardrail_notification_webhook_url",
            data={
                "guardrail_notification_webhook_url": "https://hooks.slack.com/y"
            },
        )

    assert response.status_code == 200
    cfg = load_config(str(base_config))
    assert cfg.daemon.guardrail_notification_webhook_url == (
        "https://hooks.slack.com/y"
    )
    assert cfg.daemon.poll_interval_sec == 60
    assert cfg.daemon.review_timeout_min == 45
    assert cfg.daemon.claude_model == "sonnet"


def test_update_webhook_url_returns_503_when_disk_write_fails(
    base_config: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from src.web.routes import settings as settings_routes

    def boom(*_args: object, **_kwargs: object) -> None:
        raise OSError("read-only file system")

    monkeypatch.setattr(settings_routes, "write_daemon_field", boom)

    with TestClient(app) as client:
        response = client.post(
            "/settings/config/guardrail_notification_webhook_url",
            data={
                "guardrail_notification_webhook_url": "https://hooks.slack.com/z"
            },
        )

    assert response.status_code == 503
    assert "Failed to write config.yml" in response.text


def test_update_webhook_url_blank_503_on_disk_error(
    base_config: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A failing ``delete_daemon_fields`` on the blank URL path surfaces as 503."""
    from src.web.routes import settings as settings_routes

    def boom(*_args: object, **_kwargs: object) -> None:
        raise OSError("read-only file system")

    monkeypatch.setattr(settings_routes, "delete_daemon_fields", boom)

    with TestClient(app) as client:
        response = client.post(
            "/settings/config/guardrail_notification_webhook_url",
            data={"guardrail_notification_webhook_url": ""},
        )

    assert response.status_code == 503
    assert "Failed to write config.yml" in response.text


def test_coerce_config_field_rejects_unknown_field() -> None:
    """``_coerce_config_field`` guards against future allow-list drift.

    ``update_config_field`` already filters by ``EDITABLE_CONFIG_FIELDS``,
    so the helper's final ``raise`` is unreachable from the HTTP path. The
    test pins the contract directly so a future maintainer who adds a
    field to the allow-list without a validator gets an immediate, loud
    failure instead of silently writing ``None`` to ``config.yml``.
    """
    from src.web.routes.settings import _coerce_config_field

    with pytest.raises(ValueError, match="Unhandled field"):
        _coerce_config_field("not_a_real_field", "1")
