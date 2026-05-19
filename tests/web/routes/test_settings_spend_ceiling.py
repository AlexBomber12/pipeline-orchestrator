"""Tests for the spending-ceiling settings endpoints (PR-344a).

These cover the three ``daemon.spend_ceiling_*`` fields exposed via the
Settings page. The endpoint uses ``ruamel.yaml`` round-trip writes, so
unrelated YAML keys and any operator comments in ``config.yml`` must
survive a single-field update.
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


def test_settings_renders_spend_ceiling_fields(base_config: Path) -> None:
    """All three inputs must be present with the correct ``name`` attributes."""
    with TestClient(app) as client:
        response = client.get("/settings")

    assert response.status_code == 200
    body = response.text
    assert 'name="spend_ceiling_session_percent"' in body
    assert 'name="spend_ceiling_weekly_percent"' in body
    assert 'name="spend_ceiling_warning_percent"' in body
    assert "Spending controls" in body


def test_update_session_percent_persists_to_yaml(base_config: Path) -> None:
    with TestClient(app) as client:
        response = client.post(
            "/settings/config/spend_ceiling_session_percent",
            data={"spend_ceiling_session_percent": "80"},
        )

    assert response.status_code == 200
    cfg = load_config(str(base_config))
    assert cfg.daemon.spend_ceiling_session_percent == 80


def test_update_weekly_percent_persists_to_yaml(base_config: Path) -> None:
    with TestClient(app) as client:
        response = client.post(
            "/settings/config/spend_ceiling_weekly_percent",
            data={"spend_ceiling_weekly_percent": "90"},
        )

    assert response.status_code == 200
    cfg = load_config(str(base_config))
    assert cfg.daemon.spend_ceiling_weekly_percent == 90


def test_update_warning_percent_persists_to_yaml(base_config: Path) -> None:
    with TestClient(app) as client:
        response = client.post(
            "/settings/config/spend_ceiling_warning_percent",
            data={"spend_ceiling_warning_percent": "70"},
        )

    assert response.status_code == 200
    cfg = load_config(str(base_config))
    assert cfg.daemon.spend_ceiling_warning_percent == 70


def test_update_rejects_out_of_range(base_config: Path) -> None:
    """Values above 100 must be rejected with status 400, config untouched."""
    with TestClient(app) as client:
        response = client.post(
            "/settings/config/spend_ceiling_session_percent",
            data={"spend_ceiling_session_percent": "150"},
        )

    assert response.status_code == 400
    cfg = load_config(str(base_config))
    assert cfg.daemon.spend_ceiling_session_percent is None


def test_update_rejects_non_integer(base_config: Path) -> None:
    with TestClient(app) as client:
        response = client.post(
            "/settings/config/spend_ceiling_session_percent",
            data={"spend_ceiling_session_percent": "abc"},
        )

    assert response.status_code == 400
    cfg = load_config(str(base_config))
    assert cfg.daemon.spend_ceiling_session_percent is None


def test_update_rejects_unknown_field(base_config: Path) -> None:
    """The endpoint allow-lists the three spend_ceiling fields only."""
    with TestClient(app) as client:
        response = client.post(
            "/settings/config/poll_interval_sec",
            data={"poll_interval_sec": "999"},
        )

    assert response.status_code == 400
    cfg = load_config(str(base_config))
    assert cfg.daemon.poll_interval_sec == 60


def test_reset_to_defaults_restores_factory_values(base_config: Path) -> None:
    """After custom values are set, reset removes them so defaults apply."""
    base_config.write_text(
        "repositories:\n"
        "  - url: https://github.com/example/alpha.git\n"
        "    branch: main\n"
        "daemon:\n"
        "  poll_interval_sec: 60\n"
        "  spend_ceiling_session_percent: 70\n"
        "  spend_ceiling_weekly_percent: 80\n"
        "  spend_ceiling_warning_percent: 60\n",
        encoding="utf-8",
    )

    with TestClient(app) as client:
        response = client.post("/settings/config/reset/spend_ceiling")

    assert response.status_code == 200
    cfg = load_config(str(base_config))
    assert cfg.daemon.spend_ceiling_session_percent is None
    assert cfg.daemon.spend_ceiling_weekly_percent is None
    # Default for warning is 80, set by Pydantic when the key is absent.
    assert cfg.daemon.spend_ceiling_warning_percent == 80


def test_unrelated_yaml_fields_preserved_after_update(base_config: Path) -> None:
    """Updating one field must not perturb any other ``daemon:`` key."""
    base_config.write_text(
        "repositories:\n"
        "  - url: https://github.com/example/alpha.git\n"
        "    branch: main\n"
        "daemon:\n"
        "  poll_interval_sec: 60\n"
        "  review_timeout_min: 45\n"
        "  claude_model: sonnet\n"
        "  fix_idle_timeout_sec: 1800\n",
        encoding="utf-8",
    )

    with TestClient(app) as client:
        response = client.post(
            "/settings/config/spend_ceiling_warning_percent",
            data={"spend_ceiling_warning_percent": "75"},
        )

    assert response.status_code == 200
    cfg = load_config(str(base_config))
    assert cfg.daemon.spend_ceiling_warning_percent == 75
    assert cfg.daemon.poll_interval_sec == 60
    assert cfg.daemon.review_timeout_min == 45
    assert cfg.daemon.claude_model == "sonnet"
    assert cfg.daemon.fix_idle_timeout_sec == 1800


def test_yaml_comments_preserved_after_update(base_config: Path) -> None:
    """Operator comments in ``config.yml`` must survive a UI write.

    PyYAML drops comments on dump. This endpoint uses ruamel.yaml's
    round-trip mode, so the test asserts that two distinct comment
    markers (a top-of-file note and an inline ``# foo`` comment) both
    remain in the rewritten file body.
    """
    base_config.write_text(
        "# Operator note: review every Friday.\n"
        "repositories:\n"
        "  - url: https://github.com/example/alpha.git\n"
        "    branch: main\n"
        "daemon:\n"
        "  poll_interval_sec: 60  # short cadence for staging\n",
        encoding="utf-8",
    )

    with TestClient(app) as client:
        response = client.post(
            "/settings/config/spend_ceiling_warning_percent",
            data={"spend_ceiling_warning_percent": "65"},
        )

    assert response.status_code == 200
    body = base_config.read_text(encoding="utf-8")
    assert "Operator note: review every Friday." in body
    assert "short cadence for staging" in body
    assert "spend_ceiling_warning_percent: 65" in body


def test_update_missing_form_field_returns_400(base_config: Path) -> None:
    with TestClient(app) as client:
        response = client.post(
            "/settings/config/spend_ceiling_session_percent",
        )

    assert response.status_code == 400


def test_update_session_blank_clears_existing_value(base_config: Path) -> None:
    """Blank session input deletes the key so the None default applies again."""
    base_config.write_text(
        "repositories:\n"
        "  - url: https://github.com/example/alpha.git\n"
        "    branch: main\n"
        "daemon:\n"
        "  poll_interval_sec: 60\n"
        "  spend_ceiling_session_percent: 70\n",
        encoding="utf-8",
    )

    with TestClient(app) as client:
        response = client.post(
            "/settings/config/spend_ceiling_session_percent",
            data={"spend_ceiling_session_percent": ""},
        )

    assert response.status_code == 200
    body = base_config.read_text(encoding="utf-8")
    assert "spend_ceiling_session_percent" not in body
    cfg = load_config(str(base_config))
    assert cfg.daemon.spend_ceiling_session_percent is None


def test_update_weekly_blank_clears_existing_value(base_config: Path) -> None:
    """Blank weekly input deletes the key so the None default applies again."""
    base_config.write_text(
        "repositories:\n"
        "  - url: https://github.com/example/alpha.git\n"
        "    branch: main\n"
        "daemon:\n"
        "  poll_interval_sec: 60\n"
        "  spend_ceiling_weekly_percent: 90\n",
        encoding="utf-8",
    )

    with TestClient(app) as client:
        response = client.post(
            "/settings/config/spend_ceiling_weekly_percent",
            data={"spend_ceiling_weekly_percent": ""},
        )

    assert response.status_code == 200
    body = base_config.read_text(encoding="utf-8")
    assert "spend_ceiling_weekly_percent" not in body
    cfg = load_config(str(base_config))
    assert cfg.daemon.spend_ceiling_weekly_percent is None


def test_update_blank_session_preserves_weekly(base_config: Path) -> None:
    """Clearing one optional ceiling must not disturb the other."""
    base_config.write_text(
        "repositories:\n"
        "  - url: https://github.com/example/alpha.git\n"
        "    branch: main\n"
        "daemon:\n"
        "  poll_interval_sec: 60\n"
        "  spend_ceiling_session_percent: 70\n"
        "  spend_ceiling_weekly_percent: 90\n",
        encoding="utf-8",
    )

    with TestClient(app) as client:
        response = client.post(
            "/settings/config/spend_ceiling_session_percent",
            data={"spend_ceiling_session_percent": ""},
        )

    assert response.status_code == 200
    cfg = load_config(str(base_config))
    assert cfg.daemon.spend_ceiling_session_percent is None
    assert cfg.daemon.spend_ceiling_weekly_percent == 90


def test_update_warning_blank_still_returns_400(base_config: Path) -> None:
    """Warning has a non-None default, so blank must remain a 400 error."""
    base_config.write_text(
        "repositories:\n"
        "  - url: https://github.com/example/alpha.git\n"
        "    branch: main\n"
        "daemon:\n"
        "  poll_interval_sec: 60\n"
        "  spend_ceiling_warning_percent: 70\n",
        encoding="utf-8",
    )

    with TestClient(app) as client:
        response = client.post(
            "/settings/config/spend_ceiling_warning_percent",
            data={"spend_ceiling_warning_percent": ""},
        )

    assert response.status_code == 400
    cfg = load_config(str(base_config))
    assert cfg.daemon.spend_ceiling_warning_percent == 70


def test_update_blank_session_503_on_disk_error(
    base_config: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A failing ``delete_daemon_fields`` on the blank path surfaces as 503."""
    from src.web.routes import settings as settings_routes

    def boom(*_args: object, **_kwargs: object) -> None:
        raise OSError("read-only file system")

    monkeypatch.setattr(settings_routes, "delete_daemon_fields", boom)

    with TestClient(app) as client:
        response = client.post(
            "/settings/config/spend_ceiling_session_percent",
            data={"spend_ceiling_session_percent": ""},
        )

    assert response.status_code == 503
    assert "Failed to write config.yml" in response.text


def test_reset_rerenders_section_with_default_values(base_config: Path) -> None:
    """The reset endpoint returns the spend-ceiling partial for HTMX swap."""
    with TestClient(app) as client:
        response = client.post("/settings/config/reset/spend_ceiling")

    assert response.status_code == 200
    body = response.text
    assert 'id="settings-spend-ceiling"' in body
    assert 'name="spend_ceiling_warning_percent"' in body
    # session/weekly defaults are None: rendered as empty value.
    assert 'name="spend_ceiling_session_percent"' in body


def test_update_returns_503_when_disk_write_fails(
    base_config: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``OSError`` from the ruamel write surfaces as HTML 503."""
    from src.web.routes import settings as settings_routes

    def boom(*_args: object, **_kwargs: object) -> None:
        raise OSError("read-only file system")

    monkeypatch.setattr(settings_routes, "write_daemon_field", boom)

    with TestClient(app) as client:
        response = client.post(
            "/settings/config/spend_ceiling_warning_percent",
            data={"spend_ceiling_warning_percent": "65"},
        )

    assert response.status_code == 503
    assert "Failed to write config.yml" in response.text


def test_reset_returns_503_when_disk_write_fails(
    base_config: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Same 503 surface for the reset endpoint when the YAML write trips."""
    from src.web.routes import settings as settings_routes

    def boom(*_args: object, **_kwargs: object) -> None:
        raise OSError("read-only file system")

    monkeypatch.setattr(settings_routes, "delete_daemon_fields", boom)

    with TestClient(app) as client:
        response = client.post("/settings/config/reset/spend_ceiling")

    assert response.status_code == 503
    assert "Failed to write config.yml" in response.text


def test_update_creates_config_when_file_absent(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """POSTing a spend-ceiling field must create config.yml on a fresh deploy.

    Mirrors ``save_config`` behavior in the daemon/repo settings paths:
    when ``CONFIG_PATH`` does not exist yet, the endpoint persists the
    value by creating the file rather than returning a 503.
    """
    monkeypatch.chdir(tmp_path)
    cfg = tmp_path / "config.yml"
    assert not cfg.exists()

    with TestClient(app) as client:
        response = client.post(
            "/settings/config/spend_ceiling_warning_percent",
            data={"spend_ceiling_warning_percent": "65"},
        )

    assert response.status_code == 200
    assert cfg.exists()
    loaded = load_config(str(cfg))
    assert loaded.daemon.spend_ceiling_warning_percent == 65


def test_reset_is_noop_when_config_absent(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Reset on a missing config.yml succeeds without creating a stub file.

    Pydantic defaults already apply when no file exists, so the reset
    endpoint must short-circuit instead of returning 503.
    """
    monkeypatch.chdir(tmp_path)
    cfg = tmp_path / "config.yml"
    assert not cfg.exists()

    with TestClient(app) as client:
        response = client.post("/settings/config/reset/spend_ceiling")

    assert response.status_code == 200
    assert not cfg.exists()
