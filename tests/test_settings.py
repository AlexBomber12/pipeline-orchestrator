"""Tests for the /settings page in src/web/app.py."""

from __future__ import annotations

import subprocess
import threading
from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from src import config as src_config
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


@pytest.fixture(autouse=True)
def _stub_auth_subprocess(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_run(
        cmd: list[str], *args: object, **kwargs: object
    ) -> _FakeCompleted:
        if cmd and cmd[0] == "claude":
            return _FakeCompleted(0, stdout="claude 1.2.3\n")
        if cmd and cmd[0] == "codex":
            if cmd[1:] == ["--version"]:
                return _FakeCompleted(0, stdout="codex-cli 0.121.0\n")
            return _FakeCompleted(0, stdout="Logged in with ChatGPT\n")
        if cmd and cmd[0] == "gh":
            return _FakeCompleted(
                0,
                stderr="github.com\n  ✓ Logged in to github.com as octocat (oauth_token)\n",
            )
        raise AssertionError(f"unexpected command: {cmd}")

    monkeypatch.setattr(web_app.subprocess, "run", fake_run)


@pytest.fixture
def empty_config(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    cfg = tmp_path / "config.yml"
    cfg.write_text("repositories: []\n", encoding="utf-8")
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(web_app, "aioredis", _StubAioredis())
    return cfg


@pytest.fixture
def one_repo_config(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    cfg = tmp_path / "config.yml"
    cfg.write_text(
        "repositories:\n"
        "  - url: https://github.com/example/alpha.git\n"
        "    branch: main\n"
        "    auto_merge: true\n"
        "    review_timeout_min: 60\n",
        encoding="utf-8",
    )
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(web_app, "aioredis", _StubAioredis())
    return cfg


def test_settings_page_returns_html(empty_config: Path) -> None:
    with TestClient(app) as client:
        response = client.get("/settings")

    assert response.status_code == 200
    assert "text/html" in response.headers["content-type"]
    body = response.text
    assert "<!DOCTYPE" in body
    assert "Settings" in body
    assert "Repositories" in body
    assert 'id="settings-repo-list"' in body
    assert "Add Repository" in body


def test_settings_nav_link_present_on_dashboard(empty_config: Path) -> None:
    with TestClient(app) as client:
        response = client.get("/")

    assert response.status_code == 200
    assert 'href="/settings"' in response.text


def test_settings_partial_returns_fragment(one_repo_config: Path) -> None:
    with TestClient(app) as client:
        response = client.get("/partials/settings/repo-list")

    assert response.status_code == 200
    body = response.text
    assert "<!DOCTYPE" not in body
    assert "alpha" in body
    assert "Add Repository" in body


def test_post_repo_adds_to_config(empty_config: Path) -> None:
    with TestClient(app) as client:
        response = client.post(
            "/settings/repos",
            data={"url": "https://github.com/example/new-repo.git"},
        )

    assert response.status_code == 200
    assert "new-repo" in response.text

    cfg = load_config(str(empty_config))
    assert len(cfg.repositories) == 1
    assert cfg.repositories[0].url == "https://github.com/example/new-repo.git"
    assert cfg.repositories[0].branch == "main"
    assert cfg.repositories[0].auto_merge is True


def test_post_repo_with_branch_and_auto_merge(empty_config: Path) -> None:
    with TestClient(app) as client:
        response = client.post(
            "/settings/repos",
            data={
                "url": "https://github.com/example/repo2",
                "branch": "develop",
                "auto_merge": "false",
            },
        )

    assert response.status_code == 200
    cfg = load_config(str(empty_config))
    assert cfg.repositories[0].branch == "develop"
    assert cfg.repositories[0].auto_merge is False


def test_post_repo_duplicate_returns_422(one_repo_config: Path) -> None:
    with TestClient(app) as client:
        response = client.post(
            "/settings/repos",
            data={"url": "https://github.com/example/alpha.git"},
        )

    assert response.status_code == 422
    assert "already configured" in response.text

    cfg = load_config(str(one_repo_config))
    assert len(cfg.repositories) == 1


def test_delete_repo_removes_from_config(one_repo_config: Path) -> None:
    with TestClient(app) as client:
        response = client.delete(
            "/settings/repos",
            params={"url": "https://github.com/example/alpha.git"},
        )

    assert response.status_code == 200
    cfg = load_config(str(one_repo_config))
    assert cfg.repositories == []


def test_delete_nonexistent_repo_returns_404(empty_config: Path) -> None:
    with TestClient(app) as client:
        response = client.delete(
            "/settings/repos",
            params={"url": "https://github.com/example/ghost"},
        )

    assert response.status_code == 404
    assert "not found" in response.text.lower()


def test_put_repo_updates_branch(one_repo_config: Path) -> None:
    with TestClient(app) as client:
        response = client.put(
            "/settings/repos",
            params={"url": "https://github.com/example/alpha.git"},
            data={"branch": "develop"},
        )

    assert response.status_code == 200
    cfg = load_config(str(one_repo_config))
    assert cfg.repositories[0].branch == "develop"
    # Other fields untouched.
    assert cfg.repositories[0].auto_merge is True
    assert cfg.repositories[0].review_timeout_min == 60


def test_put_repo_updates_multiple_fields(one_repo_config: Path) -> None:
    with TestClient(app) as client:
        response = client.put(
            "/settings/repos",
            params={"url": "https://github.com/example/alpha.git"},
            data={
                "auto_merge": "false",
                "review_timeout_min": "120",
            },
        )

    assert response.status_code == 200
    cfg = load_config(str(one_repo_config))
    assert cfg.repositories[0].auto_merge is False
    assert cfg.repositories[0].review_timeout_min == 120
    assert cfg.repositories[0].branch == "main"


def test_put_repo_empty_review_timeout_clears_override(
    one_repo_config: Path,
) -> None:
    """Cleared review_timeout_min must clear the per-repo override so the
    runner falls back to ``daemon.review_timeout_min``."""
    with TestClient(app) as client:
        response = client.put(
            "/settings/repos",
            params={"url": "https://github.com/example/alpha.git"},
            data={
                "branch": "develop",
                "review_timeout_min": "",
            },
        )

    assert response.status_code == 200
    assert "text/html" in response.headers["content-type"]
    cfg = load_config(str(one_repo_config))
    assert cfg.repositories[0].branch == "develop"
    assert cfg.repositories[0].review_timeout_min is None


def test_put_repo_clear_review_timeout_override_lets_daemon_default_apply(
    one_repo_config: Path,
) -> None:
    """Clearing ``review_timeout_min`` must both persist ``None`` and
    keep the saved YAML free of the stale override.

    Regression for a round-3 Codex P2: changing ``RepoConfig.review_timeout_min``
    to ``Optional[int]`` alone does not help upgraded deployments because
    the old ``config.yml`` entries already have explicit
    ``review_timeout_min: 60``. Clearing the field through the Settings
    UI must now write ``None`` (which ``save_config`` then omits from
    YAML via ``exclude_none=True``), so the runner picks up the daemon
    default on subsequent cycles.
    """
    with TestClient(app) as client:
        response = client.put(
            "/settings/repos",
            params={"url": "https://github.com/example/alpha.git"},
            data={"review_timeout_min": ""},
        )

    assert response.status_code == 200
    cfg = load_config(str(one_repo_config))
    assert cfg.repositories[0].review_timeout_min is None

    # The override must also be gone from the on-disk YAML (save_config
    # uses ``exclude_none``), so a subsequent ``load_config`` on a fresh
    # process re-reads ``None`` rather than being rehydrated from a stale
    # explicit value. We only inspect the ``repositories:`` block because
    # ``daemon.review_timeout_min`` remains a required int.
    on_disk = one_repo_config.read_text(encoding="utf-8")
    repos_section = on_disk.split("daemon:", 1)[0]
    assert "review_timeout_min" not in repos_section, on_disk


def test_put_repo_invalid_int_returns_422_html(
    one_repo_config: Path,
) -> None:
    """Non-numeric values for ``review_timeout_min`` render the error partial
    with status 422 (HTML), not FastAPI's default JSON 422."""
    with TestClient(app) as client:
        response = client.put(
            "/settings/repos",
            params={"url": "https://github.com/example/alpha.git"},
            data={"review_timeout_min": "abc"},
        )

    assert response.status_code == 422
    assert "text/html" in response.headers["content-type"]
    body = response.text
    assert 'id="settings-error"' in body
    assert "review_timeout_min" in body
    # Config untouched.
    cfg = load_config(str(one_repo_config))
    assert cfg.repositories[0].review_timeout_min == 60


def test_put_repo_non_positive_review_timeout_returns_422(
    one_repo_config: Path,
) -> None:
    """``review_timeout_min`` must stay >= 1 server-side.

    Regression for a P2 bug where ``_coerce_int`` only parsed the value
    (``min="1"`` on the ``<input>`` is client-side only), so a request
    with ``review_timeout_min=0`` or a negative number would be persisted
    to ``config.yml`` and the daemon would mark every PR on that repo as
    hung immediately because ``elapsed_min >= timeout_min``.
    """
    with TestClient(app) as client:
        for bad in ("0", "-5"):
            response = client.put(
                "/settings/repos",
                params={"url": "https://github.com/example/alpha.git"},
                data={"review_timeout_min": bad},
            )
            assert response.status_code == 422, bad
            assert "text/html" in response.headers["content-type"]
            assert "review_timeout_min" in response.text
            assert "at least 1" in response.text

    # Config untouched across both attempts.
    cfg = load_config(str(one_repo_config))
    assert cfg.repositories[0].review_timeout_min == 60


def test_put_repo_invalid_bool_returns_422_html(
    one_repo_config: Path,
) -> None:
    """Unknown bool strings for ``auto_merge`` render the error partial."""
    with TestClient(app) as client:
        response = client.put(
            "/settings/repos",
            params={"url": "https://github.com/example/alpha.git"},
            data={"auto_merge": "maybe"},
        )

    assert response.status_code == 422
    assert "text/html" in response.headers["content-type"]
    assert "auto_merge" in response.text
    cfg = load_config(str(one_repo_config))
    assert cfg.repositories[0].auto_merge is True


def test_put_nonexistent_repo_returns_404(empty_config: Path) -> None:
    with TestClient(app) as client:
        response = client.put(
            "/settings/repos",
            params={"url": "https://github.com/example/ghost"},
            data={"branch": "develop"},
        )

    assert response.status_code == 404
    assert "not found" in response.text.lower()


def test_basename_collision_put_and_delete_target_correct_repo(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Two repos with the same basename must be keyed off full URL.

    Regression for a P1 bug where ``_find_repo_by_name`` matched the first
    repo whose basename equaled ``{name}``, which silently mutated or
    deleted the wrong entry whenever two owners published a repo with the
    same trailing segment (for example ``owner-a/api`` and ``owner-b/api``).
    """
    cfg = tmp_path / "config.yml"
    cfg.write_text(
        "repositories:\n"
        "  - url: https://github.com/owner-a/api\n"
        "    branch: main\n"
        "  - url: https://github.com/owner-b/api\n"
        "    branch: main\n",
        encoding="utf-8",
    )
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(web_app, "aioredis", _StubAioredis())

    with TestClient(app) as client:
        # Update the second repo; the first must be untouched.
        response = client.put(
            "/settings/repos",
            params={"url": "https://github.com/owner-b/api"},
            data={"branch": "develop"},
        )
        assert response.status_code == 200

        loaded = load_config(str(cfg))
        assert loaded.repositories[0].url == "https://github.com/owner-a/api"
        assert loaded.repositories[0].branch == "main"
        assert loaded.repositories[1].url == "https://github.com/owner-b/api"
        assert loaded.repositories[1].branch == "develop"

        # Delete the first repo; the second (now on develop) must survive.
        response = client.delete(
            "/settings/repos",
            params={"url": "https://github.com/owner-a/api"},
        )
        assert response.status_code == 200

        loaded = load_config(str(cfg))
        assert len(loaded.repositories) == 1
        assert loaded.repositories[0].url == "https://github.com/owner-b/api"
        assert loaded.repositories[0].branch == "develop"


def _raise_permission_error(*args: object, **kwargs: object) -> None:
    raise PermissionError("Read-only file system: config.yml")


def test_post_repo_handles_readonly_config(
    empty_config: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``save_config`` failures (e.g. read-only mount) render the HTML
    error partial with status 503 instead of bubbling up as a 500.

    Regression for a P1 bug where the default ``docker-compose.yml`` used
    to mount ``config.yml`` read-only into the ``web`` service, so every
    settings mutation raised ``PermissionError`` and crashed the handler.
    """
    monkeypatch.setattr(src_config, "save_config", _raise_permission_error)

    with TestClient(app) as client:
        response = client.post(
            "/settings/repos",
            data={"url": "https://github.com/example/new-repo"},
        )

    assert response.status_code == 503
    assert "text/html" in response.headers["content-type"]
    body = response.text
    assert 'id="settings-error"' in body
    assert "Failed to write config.yml" in body


def test_delete_repo_handles_readonly_config(
    one_repo_config: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(src_config, "save_config", _raise_permission_error)

    with TestClient(app) as client:
        response = client.delete(
            "/settings/repos",
            params={"url": "https://github.com/example/alpha.git"},
        )

    assert response.status_code == 503
    assert "Failed to write config.yml" in response.text
    # Config untouched.
    cfg = load_config(str(one_repo_config))
    assert len(cfg.repositories) == 1


def test_put_repo_handles_readonly_config(
    one_repo_config: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(src_config, "save_config", _raise_permission_error)

    with TestClient(app) as client:
        response = client.put(
            "/settings/repos",
            params={"url": "https://github.com/example/alpha.git"},
            data={"branch": "develop"},
        )

    assert response.status_code == 503
    assert "Failed to write config.yml" in response.text
    cfg = load_config(str(one_repo_config))
    assert cfg.repositories[0].branch == "main"


def test_successful_mutation_oob_clears_stale_settings_error(
    one_repo_config: Path,
) -> None:
    """Successful POST/PUT/DELETE responses must OOB-clear ``#settings-error``.

    Regression for a P2 bug where ``_render_settings_repo_list`` only
    swapped ``#settings-repo-list`` on success, so an OOB error banner
    posted by a previous 422/503 response persisted unchanged through
    subsequent successful mutations and the UI kept showing a stale
    failure message.
    """
    with TestClient(app) as client:
        # POST success clears the error div.
        post = client.post(
            "/settings/repos",
            data={"url": "https://github.com/example/second"},
        )
        assert post.status_code == 200
        assert 'id="settings-error"' in post.text
        assert 'hx-swap-oob="innerHTML"' in post.text

        # PUT success clears the error div.
        put = client.put(
            "/settings/repos",
            params={"url": "https://github.com/example/alpha.git"},
            data={"branch": "develop"},
        )
        assert put.status_code == 200
        assert 'id="settings-error"' in put.text
        assert 'hx-swap-oob="innerHTML"' in put.text

        # DELETE success clears the error div.
        delete = client.delete(
            "/settings/repos",
            params={"url": "https://github.com/example/second"},
        )
        assert delete.status_code == 200
        assert 'id="settings-error"' in delete.text
        assert 'hx-swap-oob="innerHTML"' in delete.text


def test_post_repo_error_includes_error_message(one_repo_config: Path) -> None:
    with TestClient(app) as client:
        response = client.post(
            "/settings/repos",
            data={"url": "https://github.com/example/alpha.git"},
        )

    assert response.status_code == 422
    body = response.text
    assert 'id="settings-error"' in body
    assert "already configured" in body


# ---------------------------------------------------------------------------
# Daemon settings
# ---------------------------------------------------------------------------


def test_settings_page_renders_daemon_section(empty_config: Path) -> None:
    with TestClient(app) as client:
        response = client.get("/settings")

    assert response.status_code == 200
    body = response.text
    assert "Daemon Settings" in body
    assert 'id="settings-daemon"' in body
    assert 'name="poll_interval_sec"' in body
    assert 'name="review_timeout_min"' in body
    assert 'name="hung_fallback_codex_review"' in body
    assert 'name="error_handler_use_ai"' in body
    assert "Coders" in body
    assert "GitHub CLI" in body


def test_partial_daemon_returns_fragment(empty_config: Path) -> None:
    with TestClient(app) as client:
        response = client.get("/partials/settings/daemon")

    assert response.status_code == 200
    body = response.text
    assert "<!DOCTYPE" not in body
    assert 'name="poll_interval_sec"' in body
    assert 'id="settings-daemon-error"' in body
    assert 'hx-swap-oob="innerHTML"' in body


def test_put_daemon_updates_numeric_fields(empty_config: Path) -> None:
    with TestClient(app) as client:
        response = client.put(
            "/settings/daemon",
            data={"poll_interval_sec": "45", "review_timeout_min": "90"},
        )

    assert response.status_code == 200
    assert "text/html" in response.headers["content-type"]
    cfg = load_config(str(empty_config))
    assert cfg.daemon.poll_interval_sec == 45
    assert cfg.daemon.review_timeout_min == 90
    # Booleans untouched.
    assert cfg.daemon.hung_fallback_codex_review is True
    assert cfg.daemon.error_handler_use_ai is True


def test_put_daemon_updates_boolean_fields(empty_config: Path) -> None:
    with TestClient(app) as client:
        response = client.put(
            "/settings/daemon",
            data={
                "hung_fallback_codex_review": "false",
                "error_handler_use_ai": "false",
            },
        )

    assert response.status_code == 200
    cfg = load_config(str(empty_config))
    assert cfg.daemon.hung_fallback_codex_review is False
    assert cfg.daemon.error_handler_use_ai is False


def test_put_daemon_empty_numeric_inputs_are_no_ops(empty_config: Path) -> None:
    """Cleared number inputs must not trip FastAPI's request parser.

    Mirrors the /settings/repos regression: declaring the form fields as
    ``int | None`` would have FastAPI reject the request during parsing
    with a raw JSON 422, which HTMX would then swap into the daemon form
    and wedge the UI.
    """
    with TestClient(app) as client:
        response = client.put(
            "/settings/daemon",
            data={
                "poll_interval_sec": "",
                "review_timeout_min": "",
                "hung_fallback_codex_review": "false",
            },
        )

    assert response.status_code == 200
    cfg = load_config(str(empty_config))
    assert cfg.daemon.poll_interval_sec == 60
    assert cfg.daemon.review_timeout_min == 60
    assert cfg.daemon.hung_fallback_codex_review is False


def test_put_daemon_non_positive_poll_interval_returns_422(
    empty_config: Path,
) -> None:
    with TestClient(app) as client:
        for bad in ("0", "-5"):
            response = client.put(
                "/settings/daemon",
                data={"poll_interval_sec": bad},
            )
            assert response.status_code == 422, bad
            assert "text/html" in response.headers["content-type"]
            assert "poll_interval_sec" in response.text
            assert "at least 1" in response.text

    cfg = load_config(str(empty_config))
    assert cfg.daemon.poll_interval_sec == 60


def test_put_daemon_non_positive_review_timeout_returns_422(
    empty_config: Path,
) -> None:
    with TestClient(app) as client:
        for bad in ("0", "-10"):
            response = client.put(
                "/settings/daemon",
                data={"review_timeout_min": bad},
            )
            assert response.status_code == 422, bad
            assert "review_timeout_min" in response.text
            assert "at least 1" in response.text

    cfg = load_config(str(empty_config))
    assert cfg.daemon.review_timeout_min == 60


def test_put_daemon_invalid_int_returns_422_html(empty_config: Path) -> None:
    with TestClient(app) as client:
        response = client.put(
            "/settings/daemon",
            data={"poll_interval_sec": "abc"},
        )

    assert response.status_code == 422
    assert "text/html" in response.headers["content-type"]
    body = response.text
    assert 'id="settings-daemon-error"' in body
    assert "poll_interval_sec" in body


def test_put_daemon_invalid_bool_returns_422_html(empty_config: Path) -> None:
    with TestClient(app) as client:
        response = client.put(
            "/settings/daemon",
            data={"hung_fallback_codex_review": "maybe"},
        )

    assert response.status_code == 422
    assert "hung_fallback_codex_review" in response.text
    cfg = load_config(str(empty_config))
    assert cfg.daemon.hung_fallback_codex_review is True


def test_put_daemon_handles_readonly_config(
    empty_config: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(src_config, "save_config", _raise_permission_error)

    with TestClient(app) as client:
        response = client.put(
            "/settings/daemon",
            data={"poll_interval_sec": "45"},
        )

    assert response.status_code == 503
    assert "Failed to write config.yml" in response.text
    cfg = load_config(str(empty_config))
    assert cfg.daemon.poll_interval_sec == 60


def test_put_daemon_success_oob_clears_stale_error(empty_config: Path) -> None:
    """A successful PUT must OOB-clear ``#settings-daemon-error``.

    Same regression class as the repo list: without the OOB swap an error
    banner left over from a prior 422/503 would persist unchanged through
    subsequent successful mutations.
    """
    with TestClient(app) as client:
        response = client.put(
            "/settings/daemon",
            data={"poll_interval_sec": "45"},
        )

    assert response.status_code == 200
    body = response.text
    assert 'id="settings-daemon-error"' in body
    assert 'hx-swap-oob="innerHTML"' in body


# ---------------------------------------------------------------------------
# Auth status
# ---------------------------------------------------------------------------


class _FakeCompleted:
    def __init__(self, returncode: int, stdout: str = "", stderr: str = "") -> None:
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr


def _install_fake_subprocess(
    monkeypatch: pytest.MonkeyPatch,
    claude: _FakeCompleted | Exception,
    gh: _FakeCompleted | Exception,
    codex: _FakeCompleted | Exception | None = None,
    codex_version: _FakeCompleted | Exception | None = None,
) -> None:
    """Patch ``subprocess.run`` inside src.web.app with canned auth probes."""
    if codex is None:
        codex = _FakeCompleted(127, stderr="codex not found")
    if codex_version is None:
        codex_version = codex

    def fake_run(
        cmd: list[str], *args: object, **kwargs: object
    ) -> _FakeCompleted:
        if cmd and cmd[0] == "claude":
            if isinstance(claude, Exception):
                raise claude
            return claude
        if cmd and cmd[0] == "codex":
            result = codex_version if cmd[1:] == ["--version"] else codex
            if isinstance(result, Exception):
                raise result
            return result
        if cmd and cmd[0] == "gh":
            if isinstance(gh, Exception):
                raise gh
            return gh
        raise AssertionError(f"unexpected command: {cmd}")

    monkeypatch.setattr(web_app.subprocess, "run", fake_run)


def test_api_auth_status_returns_ok_for_both(
    empty_config: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _install_fake_subprocess(
        monkeypatch,
        claude=_FakeCompleted(0, stdout="claude 1.2.3\n"),
        gh=_FakeCompleted(
            0,
            stderr="github.com\n  ✓ Logged in to github.com as octocat (oauth_token)\n",
        ),
    )

    with TestClient(app) as client:
        response = client.get("/api/auth-status")

    assert response.status_code == 200
    payload = response.json()
    assert set(payload.keys()) == {"claude", "codex", "gh"}
    assert payload["claude"]["status"] == "ok"
    assert "1.2.3" in payload["claude"]["detail"]
    assert payload["gh"]["status"] == "ok"
    assert "Logged in" in payload["gh"]["detail"]
    assert "octocat" in payload["gh"]["detail"]


def test_api_auth_status_reports_errors(
    empty_config: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _install_fake_subprocess(
        monkeypatch,
        claude=FileNotFoundError("claude"),
        gh=_FakeCompleted(
            1, stderr="You are not logged into any GitHub hosts.\n"
        ),
    )

    with TestClient(app) as client:
        response = client.get("/api/auth-status")

    assert response.status_code == 200
    payload = response.json()
    assert payload["claude"]["status"] == "error"
    assert "not found" in payload["claude"]["detail"]
    assert payload["gh"]["status"] == "error"
    assert "not logged" in payload["gh"]["detail"].lower()


def test_api_auth_status_handles_timeout(
    empty_config: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _install_fake_subprocess(
        monkeypatch,
        claude=subprocess.TimeoutExpired(cmd=["claude", "--version"], timeout=5),
        gh=subprocess.TimeoutExpired(cmd=["gh", "auth", "status"], timeout=5),
    )

    with TestClient(app) as client:
        response = client.get("/api/auth-status")

    assert response.status_code == 200
    payload = response.json()
    assert payload["claude"]["status"] == "error"
    assert "timed out" in payload["claude"]["detail"]
    assert payload["gh"]["status"] == "error"
    assert "timed out" in payload["gh"]["detail"]


def test_auth_probes_inject_config_auth_dirs_into_env(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Auth CLI probes must inject ``CLAUDE_CONFIG_DIR`` / ``GH_CONFIG_DIR``.

    Regression for a P1 Codex finding on PR-016: ``docker-compose.yml``
    only wires those env vars on the ``daemon`` service, so the ``web``
    service inherited neither and ``claude --version`` / ``gh auth status``
    were reading the web container's home directory instead of the
    shared ``/data/auth`` location the daemon uses. The dashboard would
    then report "not authorized" even when the daemon was correctly
    logged in. The probes now read ``auth.claude_config_dir`` and
    ``auth.gh_config_dir`` from ``config.yml`` and inject them into the
    subprocess environment, so the Auth Status panel reflects the real
    auth context operators actually care about.
    """
    cfg = tmp_path / "config.yml"
    cfg.write_text(
        "repositories: []\n"
        "auth:\n"
        "  claude_config_dir: /custom/claude-home\n"
        "  gh_config_dir: /custom/gh-home\n",
        encoding="utf-8",
    )
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(web_app, "aioredis", _StubAioredis())
    # Scrub any inherited auth dirs so the assertion below can't be
    # fooled by the developer's ambient environment.
    monkeypatch.delenv("CLAUDE_CONFIG_DIR", raising=False)
    monkeypatch.delenv("GH_CONFIG_DIR", raising=False)

    captured: dict[str, dict[str, str]] = {}

    def fake_run(
        cmd: list[str], *args: object, **kwargs: object
    ) -> _FakeCompleted:
        env = kwargs.get("env") or {}
        if cmd and cmd[0] == "claude":
            captured["claude"] = dict(env)
            return _FakeCompleted(0, stdout="claude 1.2.3\n")
        if cmd and cmd[0] == "codex":
            return _FakeCompleted(127, stderr="codex not found")
        if cmd and cmd[0] == "gh":
            captured["gh"] = dict(env)
            return _FakeCompleted(
                0,
                stderr="  ✓ Logged in to github.com as octocat\n",
            )
        raise AssertionError(f"unexpected command: {cmd}")

    monkeypatch.setattr(web_app.subprocess, "run", fake_run)

    with TestClient(app) as client:
        response = client.get("/api/auth-status")

    assert response.status_code == 200
    assert captured["claude"].get("CLAUDE_CONFIG_DIR") == "/custom/claude-home"
    assert captured["gh"].get("GH_CONFIG_DIR") == "/custom/gh-home"


def test_api_auth_status_uses_overridden_config_path_for_claude_probe(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Claude auth probe must honor the web app's overridden ``CONFIG_PATH``.

    Regression for a P2 Codex finding on PR-074: `_check_claude_auth()`
    delegated to `ClaudePlugin.check_auth()` without passing
    `web_app.CONFIG_PATH`, so the probe reloaded `config.yml` from the
    plugin module's default location and ignored the web app's test/runtime
    override. That made Claude auth status drift from the rest of the
    dashboard whenever the app was pointed at a non-default config file.
    """
    default_cfg = tmp_path / "config.yml"
    default_cfg.write_text(
        "repositories: []\n"
        "auth:\n"
        "  claude_config_dir: /default/claude-home\n"
        "  gh_config_dir: /default/gh-home\n",
        encoding="utf-8",
    )
    override_cfg = tmp_path / "override.yml"
    override_cfg.write_text(
        "repositories: []\n"
        "auth:\n"
        "  claude_config_dir: /override/claude-home\n"
        "  gh_config_dir: /override/gh-home\n",
        encoding="utf-8",
    )

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(web_app, "aioredis", _StubAioredis())
    monkeypatch.setattr(web_app, "CONFIG_PATH", str(override_cfg))

    captured: dict[str, dict[str, str]] = {}

    def fake_run(
        cmd: list[str], *args: object, **kwargs: object
    ) -> _FakeCompleted:
        env = kwargs.get("env") or {}
        if cmd and cmd[0] == "claude":
            captured["claude"] = dict(env)
            return _FakeCompleted(0, stdout="claude 1.2.3\n")
        if cmd and cmd[0] == "codex":
            return _FakeCompleted(127, stderr="codex not found")
        if cmd and cmd[0] == "gh":
            captured["gh"] = dict(env)
            return _FakeCompleted(
                0,
                stderr="  ✓ Logged in to github.com as octocat\n",
            )
        raise AssertionError(f"unexpected command: {cmd}")

    monkeypatch.setattr(web_app.subprocess, "run", fake_run)

    with TestClient(app) as client:
        response = client.get("/api/auth-status")

    assert response.status_code == 200
    assert captured["claude"].get("CLAUDE_CONFIG_DIR") == "/override/claude-home"
    assert captured["gh"].get("GH_CONFIG_DIR") == "/override/gh-home"


def test_auth_status_probes_run_concurrently_off_loop(
    empty_config: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Both probes must dispatch to the threadpool in parallel.

    Regression for a P1 Codex finding: the original implementation ran
    `_check_claude_auth` and `_check_gh_auth` serially from the async
    handler, blocking the event loop for up to ~10s (two 5s timeouts)
    whenever a CLI was missing. The fix uses `asyncio.gather` +
    `asyncio.to_thread` so both probes run concurrently in the threadpool.

    This test proves the fix by installing a `threading.Barrier(parties=2)`
    that both probes must rendez-vous on before `subprocess.run` returns.
    If the probes still run serially, the first one blocks forever waiting
    for the second to show up and the test times out; if they run
    concurrently, both reach the barrier and the request completes.
    """
    barrier = threading.Barrier(parties=3, timeout=5)

    def fake_run(
        cmd: list[str], *args: object, **kwargs: object
    ) -> _FakeCompleted:
        # Block until all sibling probes also reach the barrier. With a
        # serial implementation this wait times out because the later
        # probes are never dispatched.
        barrier.wait()
        if cmd and cmd[0] == "claude":
            return _FakeCompleted(0, stdout="claude 1.2.3\n")
        if cmd and cmd[0] == "codex":
            return _FakeCompleted(127, stderr="codex not found")
        if cmd and cmd[0] == "gh":
            return _FakeCompleted(
                0, stderr="  ✓ Logged in to github.com as octocat\n"
            )
        raise AssertionError(f"unexpected command: {cmd}")

    monkeypatch.setattr(web_app.subprocess, "run", fake_run)

    with TestClient(app) as client:
        response = client.get("/api/auth-status")

    assert response.status_code == 200
    payload = response.json()
    assert payload["claude"]["status"] == "ok"
    assert payload["gh"]["status"] == "ok"


def test_partial_auth_status_renders_status_dots(
    empty_config: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _install_fake_subprocess(
        monkeypatch,
        claude=_FakeCompleted(0, stdout="claude 1.2.3\n"),
        gh=_FakeCompleted(1, stderr="not logged in\n"),
    )

    with TestClient(app) as client:
        response = client.get("/partials/settings/auth-status")

    assert response.status_code == 200
    assert "text/html" in response.headers["content-type"]
    body = response.text
    assert "<!DOCTYPE" not in body
    assert "Claude CLI" in body
    assert "GitHub CLI" in body
    # One green dot (bg-ok) for claude, one red dot (bg-fail) for gh.
    assert "bg-ok" in body
    assert "bg-fail" in body
    assert "1.2.3" in body
    assert "not logged in" in body


def test_api_auth_status_reports_codex_version_and_installation(
    empty_config: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _install_fake_subprocess(
        monkeypatch,
        claude=_FakeCompleted(0, stdout="claude 1.2.3\n"),
        gh=_FakeCompleted(
            0,
            stderr="github.com\n  ✓ Logged in to github.com as octocat (oauth_token)\n",
        ),
        codex=_FakeCompleted(
            0, stdout="Logged in with ChatGPT\n", stderr="WARNING: ignored\n"
        ),
        codex_version=_FakeCompleted(
            0, stdout="codex-cli 0.121.0\n", stderr="WARNING: ignored\n"
        ),
    )

    with TestClient(app) as client:
        response = client.get("/api/auth-status")

    assert response.status_code == 200
    payload = response.json()
    assert payload["codex"]["status"] == "ok"
    assert "codex-cli 0.121.0" in payload["codex"]["detail"]
    assert "installed" in payload["codex"]["detail"]
    assert "Logged in with ChatGPT" in payload["codex"]["detail"]


def test_settings_repo_list_shows_no_ci_merge_checkbox(
    one_repo_config: Path,
) -> None:
    """The settings repo list must render a checkbox for allow_merge_without_checks."""
    with TestClient(app) as client:
        response = client.get("/partials/settings/repo-list")

    assert response.status_code == 200
    body = response.text
    assert 'name="allow_merge_without_checks"' in body
    assert "No CI merge" in body


def test_put_repo_updates_allow_merge_without_checks(
    one_repo_config: Path,
) -> None:
    """The PUT handler must accept and persist allow_merge_without_checks."""
    with TestClient(app) as client:
        response = client.put(
            "/settings/repos",
            params={"url": "https://github.com/example/alpha.git"},
            data={"allow_merge_without_checks": "true"},
        )

    assert response.status_code == 200
    cfg = load_config(str(one_repo_config))
    assert cfg.repositories[0].allow_merge_without_checks is True


# ---------------------------------------------------------------------------
# PR-060: Settings UX - group headers, hints, and default placeholder
# ---------------------------------------------------------------------------


def test_settings_daemon_renders_with_group_headers(empty_config: Path) -> None:
    """Settings page must show the four logical group headers."""
    with TestClient(app) as client:
        response = client.get("/settings")

    assert response.status_code == 200
    body = response.text
    for header in ("Timing", "Review handling", "Rate limits", "Error recovery"):
        assert header in body, f"Missing group header: {header}"


def test_settings_daemon_renders_hints_for_all_fields(empty_config: Path) -> None:
    """Every daemon field must have a non-empty hint below the input."""
    with TestClient(app) as client:
        response = client.get("/partials/settings/daemon")

    assert response.status_code == 200
    body = response.text
    # Each field should have at least one hint (text-xs text-gray-500) nearby.
    hints = [
        "How often the daemon checks each repo",
        "Max time Claude gets before being killed",
        "Codex doesn't review in time",
        "Kills Claude during FIX if no push",
        "re-ping Codex on the anchor comment",
        "session usage exceeds this threshold",
        "run until API returns 429",
        "analyze failures before falling back",
    ]
    for hint_fragment in hints:
        assert hint_fragment in body, f"Missing hint: {hint_fragment}"


def test_settings_repo_list_shows_default_placeholder(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Repo without review_timeout_min override shows placeholder containing 'default'."""
    cfg = tmp_path / "config.yml"
    cfg.write_text(
        "repositories:\n"
        "  - url: https://github.com/example/nohint\n"
        "    branch: main\n",
        encoding="utf-8",
    )
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(web_app, "aioredis", _StubAioredis())

    with TestClient(app) as client:
        response = client.get("/partials/settings/repo-list")

    assert response.status_code == 200
    body = response.text
    assert "default" in body.lower()


def test_settings_repo_list_shows_value_when_override_set(
    one_repo_config: Path,
) -> None:
    """Repo with review_timeout_min override shows the actual value, not placeholder."""
    with TestClient(app) as client:
        response = client.get("/partials/settings/repo-list")

    assert response.status_code == 200
    body = response.text
    # The one_repo_config fixture sets review_timeout_min: 60 explicitly.
    assert 'value="60"' in body


def test_update_daemon_rate_limit_session(
    empty_config: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(web_app, "CONFIG_PATH", str(empty_config))

    with TestClient(app) as client:
        response = client.put(
            "/settings/daemon",
            data={"rate_limit_session_pause_percent": "75"},
        )

    assert response.status_code == 200
    cfg = load_config(str(empty_config))
    assert cfg.daemon.rate_limit_session_pause_percent == 75


def test_update_daemon_rate_limit_weekly(
    empty_config: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(web_app, "CONFIG_PATH", str(empty_config))

    with TestClient(app) as client:
        response = client.put(
            "/settings/daemon",
            data={"rate_limit_weekly_pause_percent": "90"},
        )

    assert response.status_code == 200
    cfg = load_config(str(empty_config))
    assert cfg.daemon.rate_limit_weekly_pause_percent == 90


# -----------------------------------------------------------------------
# PR-065: Coder settings tests
# -----------------------------------------------------------------------


def test_coder_dropdown_renders_with_current_value(
    empty_config: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(web_app, "CONFIG_PATH", str(empty_config))

    with TestClient(app) as client:
        response = client.get("/settings")

    assert response.status_code == 200
    assert "Coders" in response.text
    assert "Claude Code" in response.text
    assert "Codex CLI" in response.text
    assert 'type="radio"' in response.text


def test_coder_setting_saves_and_reloads(
    empty_config: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(web_app, "CONFIG_PATH", str(empty_config))

    with TestClient(app) as client:
        response = client.put(
            "/settings/daemon",
            data={"coder": "codex"},
        )

    assert response.status_code == 200
    cfg = load_config(str(empty_config))
    assert cfg.daemon.coder.value == "codex"


def test_codex_model_setting_saves(
    empty_config: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(web_app, "CONFIG_PATH", str(empty_config))

    with TestClient(app) as client:
        response = client.put(
            "/settings/daemon",
            data={"codex_model": "o4-mini"},
        )

    assert response.status_code == 200
    cfg = load_config(str(empty_config))
    assert cfg.daemon.codex_model == "o4-mini"


def test_claude_model_setting_saves(
    empty_config: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(web_app, "CONFIG_PATH", str(empty_config))

    with TestClient(app) as client:
        response = client.put(
            "/settings/daemon",
            data={"claude_model": "sonnet"},
        )

    assert response.status_code == 200
    cfg = load_config(str(empty_config))
    assert cfg.daemon.claude_model == "sonnet"


def test_repo_coder_override_saves(
    one_repo_config: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(web_app, "CONFIG_PATH", str(one_repo_config))

    with TestClient(app) as client:
        response = client.put(
            "/settings/repos?url=https://github.com/example/alpha.git",
            data={"coder": "codex"},
        )

    assert response.status_code == 200
    cfg = load_config(str(one_repo_config))
    assert cfg.repositories[0].coder is not None
    assert cfg.repositories[0].coder.value == "codex"


def test_repo_coder_override_clear(
    one_repo_config: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(web_app, "CONFIG_PATH", str(one_repo_config))

    with TestClient(app) as client:
        # First set to codex
        client.put(
            "/settings/repos?url=https://github.com/example/alpha.git",
            data={"coder": "codex"},
        )
        # Then clear (inherit)
        response = client.put(
            "/settings/repos?url=https://github.com/example/alpha.git",
            data={"coder": ""},
        )

    assert response.status_code == 200
    cfg = load_config(str(one_repo_config))
    assert cfg.repositories[0].coder is None


def test_coders_table_shows_auth_status(
    empty_config: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _install_fake_subprocess(
        monkeypatch,
        claude=_FakeCompleted(0, stdout="claude 1.2.3\n"),
        gh=_FakeCompleted(
            0, stderr="github.com\n  ✓ Logged in to github.com as octocat\n"
        ),
        codex=_FakeCompleted(1, stderr="codex not authenticated"),
        codex_version=_FakeCompleted(0, stdout="codex-cli 0.121.0\n"),
    )

    with TestClient(app) as client:
        response = client.get("/settings")

    assert response.status_code == 200
    body = response.text
    assert "Authorized" in body
    assert "codex not authenticated" in body
    assert "GitHub CLI" in body


def test_repo_detail_coder_dropdown_renders(
    one_repo_config: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(web_app, "CONFIG_PATH", str(one_repo_config))

    with TestClient(app) as client:
        response = client.get("/repo/example__alpha")

    assert response.status_code == 200
    body = response.text
    assert 'hx-put="/settings/repo/example__alpha"' in body
    assert "Inherit (Claude)" in body
    assert "Codex CLI" in body


def test_repo_coder_change_saves_to_config(
    one_repo_config: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(web_app, "CONFIG_PATH", str(one_repo_config))

    with TestClient(app) as client:
        response = client.put(
            "/settings/repo/example__alpha",
            data={"coder": "codex"},
        )

    assert response.status_code == 200
    cfg = load_config(str(one_repo_config))
    assert cfg.repositories[0].coder is not None
    assert cfg.repositories[0].coder.value == "codex"
