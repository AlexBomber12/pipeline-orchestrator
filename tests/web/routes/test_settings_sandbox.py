"""Tests for the sandbox/backup/audit settings endpoints (PR-344c).

These cover the four ``daemon.*`` fields exposed under the Settings
"Sandbox and backup" section:

* ``coder_filesystem_isolation`` (bool, bubblewrap toggle)
* ``git_bundle_backup_enabled`` (bool, bundle-before-merge toggle)
* ``git_bundle_backup_daily_retention`` (int, 1-365 days)
* ``main_commit_audit_interval_idle_cycles`` (int, 1-100 cycles)

The endpoint reuses the ruamel.yaml round-trip writer from PR-344a, so
comment preservation and atomic-write semantics are exercised by the
existing ``test_settings_spend_ceiling.py`` suite. This file focuses on
the field-specific validators, the unchecked-checkbox "absent value =
False" semantics, and the bwrap-availability badge driven by
``_sandbox_actual_state``.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from src.config import load_config
from src.web import app as web_app
from src.web.app import app
from src.web.routes import settings as settings_routes


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


def test_settings_renders_sandbox_section(base_config: Path) -> None:
    """All four sandbox/backup inputs render on the settings page."""
    with TestClient(app) as client:
        response = client.get("/settings")

    assert response.status_code == 200
    body = response.text
    assert 'name="coder_filesystem_isolation"' in body
    assert 'name="git_bundle_backup_enabled"' in body
    assert 'name="git_bundle_backup_daily_retention"' in body
    assert 'name="main_commit_audit_interval_idle_cycles"' in body
    assert "Sandbox and backup" in body


def test_toggle_sandbox_off_persists(base_config: Path) -> None:
    """Posting ``false`` to the sandbox toggle writes the key as ``false``."""
    base_config.write_text(
        "repositories:\n"
        "  - url: https://github.com/example/alpha.git\n"
        "    branch: main\n"
        "daemon:\n"
        "  poll_interval_sec: 60\n"
        "  coder_filesystem_isolation: true\n",
        encoding="utf-8",
    )

    with TestClient(app) as client:
        response = client.post(
            "/settings/config/coder_filesystem_isolation",
            data={"coder_filesystem_isolation": "false"},
        )

    assert response.status_code == 200
    cfg = load_config(str(base_config))
    assert cfg.daemon.coder_filesystem_isolation is False
    assert "coder_filesystem_isolation: false" in base_config.read_text(
        encoding="utf-8"
    )


def test_toggle_sandbox_on_persists(base_config: Path) -> None:
    """Posting ``true`` writes the key and Pydantic reloads it as True."""
    with TestClient(app) as client:
        response = client.post(
            "/settings/config/coder_filesystem_isolation",
            data={"coder_filesystem_isolation": "true"},
        )

    assert response.status_code == 200
    cfg = load_config(str(base_config))
    assert cfg.daemon.coder_filesystem_isolation is True


def test_toggle_sandbox_unchecked_defaults_to_false(base_config: Path) -> None:
    """An HTMX POST with no form data must persist the toggle as ``false``.

    HTML checkboxes omit their name from the form payload when unchecked,
    so a literal "click to disable" interaction sends an empty body. The
    endpoint must resolve that absence to ``False`` rather than returning
    a "field is required" 400.
    """
    base_config.write_text(
        "repositories:\n"
        "  - url: https://github.com/example/alpha.git\n"
        "    branch: main\n"
        "daemon:\n"
        "  poll_interval_sec: 60\n"
        "  coder_filesystem_isolation: true\n",
        encoding="utf-8",
    )

    with TestClient(app) as client:
        response = client.post("/settings/config/coder_filesystem_isolation")

    assert response.status_code == 200
    cfg = load_config(str(base_config))
    assert cfg.daemon.coder_filesystem_isolation is False


def test_toggle_backup_enabled_persists(base_config: Path) -> None:
    with TestClient(app) as client:
        response = client.post(
            "/settings/config/git_bundle_backup_enabled",
            data={"git_bundle_backup_enabled": "true"},
        )

    assert response.status_code == 200
    cfg = load_config(str(base_config))
    assert cfg.daemon.git_bundle_backup_enabled is True


def test_backup_retention_validates_range(base_config: Path) -> None:
    """Values outside 1-365 must be rejected with 400, config untouched."""
    with TestClient(app) as client:
        response_high = client.post(
            "/settings/config/git_bundle_backup_daily_retention",
            data={"git_bundle_backup_daily_retention": "500"},
        )
        response_zero = client.post(
            "/settings/config/git_bundle_backup_daily_retention",
            data={"git_bundle_backup_daily_retention": "0"},
        )

    assert response_high.status_code == 400
    assert response_zero.status_code == 400
    cfg = load_config(str(base_config))
    assert cfg.daemon.git_bundle_backup_daily_retention == 7


def test_backup_retention_accepts_valid(base_config: Path) -> None:
    with TestClient(app) as client:
        response = client.post(
            "/settings/config/git_bundle_backup_daily_retention",
            data={"git_bundle_backup_daily_retention": "30"},
        )

    assert response.status_code == 200
    cfg = load_config(str(base_config))
    assert cfg.daemon.git_bundle_backup_daily_retention == 30


def test_audit_interval_validates_range(base_config: Path) -> None:
    """Values outside 1-100 must be rejected with 400."""
    with TestClient(app) as client:
        response_high = client.post(
            "/settings/config/main_commit_audit_interval_idle_cycles",
            data={"main_commit_audit_interval_idle_cycles": "999"},
        )
        response_zero = client.post(
            "/settings/config/main_commit_audit_interval_idle_cycles",
            data={"main_commit_audit_interval_idle_cycles": "0"},
        )

    assert response_high.status_code == 400
    assert response_zero.status_code == 400
    cfg = load_config(str(base_config))
    assert cfg.daemon.main_commit_audit_interval_idle_cycles == 20


def test_audit_interval_accepts_valid(base_config: Path) -> None:
    with TestClient(app) as client:
        response = client.post(
            "/settings/config/main_commit_audit_interval_idle_cycles",
            data={"main_commit_audit_interval_idle_cycles": "50"},
        )

    assert response.status_code == 200
    cfg = load_config(str(base_config))
    assert cfg.daemon.main_commit_audit_interval_idle_cycles == 50
    assert (
        "main_commit_audit_interval_idle_cycles: 50"
        in base_config.read_text(encoding="utf-8")
    )


def test_sandbox_badge_unavailable_when_bwrap_missing(
    base_config: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Badge says "bwrap unavailable" when isolation is on but the binary missing."""
    base_config.write_text(
        "repositories:\n"
        "  - url: https://github.com/example/alpha.git\n"
        "    branch: main\n"
        "daemon:\n"
        "  poll_interval_sec: 60\n"
        "  coder_filesystem_isolation: true\n",
        encoding="utf-8",
    )

    monkeypatch.setattr(settings_routes.shutil, "which", lambda _name: None)

    with TestClient(app) as client:
        response = client.get("/settings")

    assert response.status_code == 200
    assert "bwrap unavailable" in response.text


def test_sandbox_badge_active_when_enabled_and_bwrap_present(
    base_config: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    base_config.write_text(
        "repositories:\n"
        "  - url: https://github.com/example/alpha.git\n"
        "    branch: main\n"
        "daemon:\n"
        "  poll_interval_sec: 60\n"
        "  coder_filesystem_isolation: true\n",
        encoding="utf-8",
    )

    monkeypatch.setattr(settings_routes.shutil, "which", lambda _name: "/usr/bin/bwrap")

    with TestClient(app) as client:
        response = client.get("/settings")

    assert response.status_code == 200
    body = response.text
    assert ">active<" in body
    assert "bwrap unavailable" not in body


def test_sandbox_badge_omitted_when_isolation_disabled(
    base_config: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """When the toggle is off, neither badge variant should render."""
    monkeypatch.setattr(settings_routes.shutil, "which", lambda _name: None)

    with TestClient(app) as client:
        response = client.get("/settings")

    assert response.status_code == 200
    body = response.text
    assert "bwrap unavailable" not in body
    assert ">active<" not in body


def test_update_backup_retention_returns_503_on_disk_error(
    base_config: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def boom(*_args: object, **_kwargs: object) -> None:
        raise OSError("read-only file system")

    monkeypatch.setattr(settings_routes, "write_daemon_field", boom)

    with TestClient(app) as client:
        response = client.post(
            "/settings/config/git_bundle_backup_daily_retention",
            data={"git_bundle_backup_daily_retention": "10"},
        )

    assert response.status_code == 503
    assert "Failed to write config.yml" in response.text
