"""Tests for the /settings page in src/web/app.py."""

from __future__ import annotations

import asyncio
import re
import subprocess
import threading
from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from src import coder_auth as _coder_auth
from src import coder_auth_worker as _auth_worker
from src import config as src_config
from src.coder_registry import (
    CoderRegistry,
    ModelCatalog,
    ModelMetadata,
    ModelSetting,
)
from src.coders.claude import ClaudePlugin
from src.coders.codex import CodexPlugin
from src.coders.codex_models import CodexModel
from src.config import AppConfig, load_config
from src.models import PipelineState, RepoState
from src.web import app as web_app
from src.web.app import app
from src.web.services import auth_probe as _auth_probe
from src.web.services import model_catalog as _model_catalog
from src.web.services.model_catalog import ModelCatalogCache


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


class _FakeRedis:
    def __init__(self) -> None:
        self.store: dict[str, str] = {}

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

    async def exists(self, key: str) -> int:
        return int(key in self.store)

    async def transaction(
        self,
        func,
        *watches: str,
        value_from_callable: bool = False,
        **_kwargs: object,
    ):
        pipe = _FakePipeline(self)
        func_value = func(pipe)
        if hasattr(func_value, "__await__"):
            func_value = await func_value
        exec_value = await pipe.execute()
        if value_from_callable:
            return func_value
        return exec_value


class _FakePipeline:
    def __init__(self, redis: _FakeRedis) -> None:
        self.redis = redis
        self.commands: list[tuple[str, tuple[object, ...], dict[str, object]]] = []

    async def get(self, key: str) -> str | None:
        return self.redis.store.get(key)

    def multi(self) -> None:
        return None

    def set(self, key: str, value: str, **kwargs: object) -> "_FakePipeline":
        self.commands.append(("set", (key, value), kwargs))
        return self

    async def execute(self) -> list[object]:
        results: list[object] = []
        for command, args, kwargs in self.commands:
            if command == "set":
                await self.redis.set(args[0], args[1], **kwargs)
                results.append(True)
        return results


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


def test_settings_add_repo_form_uses_global_spinner(empty_config: Path) -> None:
    """The Add Repository form relies on the base global spinner."""
    with TestClient(app) as client:
        response = client.get("/settings")

    assert response.status_code == 200
    body = response.text
    assert 'id="global-spinner"' in body
    assert 'hx-indicator="#settings-add-repo-spinner"' not in body
    assert 'id="settings-add-repo-spinner"' not in body


def test_settings_nav_link_present_on_dashboard(empty_config: Path) -> None:
    with TestClient(app) as client:
        response = client.get("/")

    assert response.status_code == 200
    assert 'href="/settings"' in response.text


def test_settings_page_styles_dark_select_options(empty_config: Path) -> None:
    """Dark theme must style native <select> option dropdowns so they don't
    fall back to the OS default (e.g. white-on-black on macOS Chrome)."""
    with TestClient(app) as client:
        response = client.get("/settings")

    assert response.status_code == 200
    body = response.text
    assert 'html[data-theme="dark"] select option' in body
    assert 'html[data-theme="dark"] select option:checked' in body
    assert 'html[data-theme="dark"] select option:hover' in body


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
    assert 'name="auto_fallback"' in body
    assert 'name="exploration_epsilon"' in body
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
    assert 'name="exploration_epsilon"' in body
    assert 'id="settings-daemon-error"' in body
    assert 'hx-swap-oob="innerHTML"' in body


def test_partial_coders_returns_static_fragment(empty_config: Path) -> None:
    with TestClient(app) as client:
        response = client.get("/partials/settings/coders")

    assert response.status_code == 200
    body = response.text
    assert "<!DOCTYPE" not in body
    assert 'id="settings-coders"' in body
    # PR-228: settings_coders no longer self-polls; the SSE migration
    # replaces the 30s refresh and operators reload the page for fresh
    # auth/usage snapshots.
    assert 'hx-trigger="every' not in body
    assert 'hx-get="/partials/settings/coders"' not in body


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
                "auto_fallback": "false",
                "hung_fallback_codex_review": "false",
                "error_handler_use_ai": "false",
            },
        )

    assert response.status_code == 200
    cfg = load_config(str(empty_config))
    assert cfg.daemon.auto_fallback is False
    assert cfg.daemon.hung_fallback_codex_review is False
    assert cfg.daemon.error_handler_use_ai is False


def test_put_daemon_updates_exploration_epsilon(empty_config: Path) -> None:
    """PR-233: form posts percent (0-50); server stores fraction (0.0-0.5)."""
    with TestClient(app) as client:
        response = client.put(
            "/settings/daemon",
            data={"exploration_epsilon": "10"},
        )

    assert response.status_code == 200
    cfg = load_config(str(empty_config))
    assert cfg.daemon.exploration_epsilon == 0.10


def test_settings_daemon_renders_alternative_coder_label(
    empty_config: Path,
) -> None:
    """PR-233: the operator-facing label replaces the implementation name."""
    with TestClient(app) as client:
        response = client.get("/settings")

    assert response.status_code == 200
    body = response.text
    assert "Try alternative coder (%)" in body
    # The implementation name must not surface as a visible label.
    assert ">exploration_epsilon<" not in body


def test_settings_daemon_input_displays_percent_value(
    empty_config: Path,
) -> None:
    """PR-233: stored fraction renders as integer percent in the input."""
    with TestClient(app) as client:
        client.put(
            "/settings/daemon",
            data={"exploration_epsilon": "10"},
        )
        response = client.get("/settings")

    assert response.status_code == 200
    body = response.text
    assert re.search(
        r'name="exploration_epsilon"[^>]*value="10"',
        body,
    )


def test_put_daemon_updates_optional_timeout_fields(empty_config: Path) -> None:
    with TestClient(app) as client:
        response = client.put(
            "/settings/daemon",
            data={
                "planned_pr_timeout_sec": "1200",
                "fix_idle_timeout_sec": "300",
                "rate_limit_weekly_pause_percent": "90",
            },
        )

    assert response.status_code == 200
    cfg = load_config(str(empty_config))
    assert cfg.daemon.planned_pr_timeout_sec == 1200
    assert cfg.daemon.fix_idle_timeout_sec == 300
    assert cfg.daemon.rate_limit_weekly_pause_percent == 90


def test_model_save_rejects_unknown_codex_model_atomically(
    empty_config: Path,
) -> None:
    with TestClient(app) as client:
        response = client.put(
            "/settings/daemon",
            data={
                "claude_model": "sonnet",
                "codex_model": "not-a-real-model",
            },
        )

    assert response.status_code == 422
    assert "text/html" in response.headers["content-type"]
    assert "codex_model is not advertised by Codex CLI" in response.text

    cfg = load_config(str(empty_config))
    assert cfg.daemon.claude_model == "opus"
    assert cfg.daemon.codex_model == ""


def test_model_save_rejects_unknown_claude_model(empty_config: Path) -> None:
    with TestClient(app) as client:
        response = client.put(
            "/settings/daemon",
            data={"claude_model": "not-a-real-model"},
        )

    assert response.status_code == 422
    assert "claude_model is not advertised by Claude Code" in response.text
    assert load_config(str(empty_config)).daemon.claude_model == "opus"


def test_model_save_accepts_valid_model(empty_config: Path) -> None:
    with TestClient(app) as client:
        response = client.put(
            "/settings/daemon",
            data={
                "claude_model": ClaudePlugin.models[-1],
                "codex_model": "gpt-5.4",
            },
        )

    assert response.status_code == 200
    cfg = load_config(str(empty_config))
    assert cfg.daemon.claude_model == ClaudePlugin.models[-1]
    assert cfg.daemon.codex_model == "gpt-5.4"


def test_model_dropdown_includes_default_option(empty_config: Path) -> None:
    with TestClient(app) as client:
        response = client.get("/partials/settings/coders")

    assert response.status_code == 200
    body = response.text
    assert '<option value=""' in body
    assert "(default)" in body
    assert "CLI default" in body
    assert 'name="coder_settings.claude.model"' in body
    assert 'name="coder_settings.codex.model"' in body
    assert 'name="claude_model"' not in body
    assert 'name="codex_model"' not in body
    for model in ClaudePlugin.models:
        if model != "":
            assert f'value="{model}"' in body
    assert 'value="gpt-5.4"' in body


class _ThirdCatalogPlugin:
    name = "third"
    display_name = "Third Coder"
    models = ["legacy-third"]
    model_setting = ModelSetting(None, "third-default", "Default")
    model_catalog_refreshable = False

    def model_catalog_cache_key(
        self, *, config: AppConfig, config_path: str
    ) -> str:
        del config, config_path
        return "third-static"

    async def get_model_catalog(
        self, *, config: AppConfig, config_path: str
    ) -> ModelCatalog:
        del config, config_path
        return ModelCatalog(
            (ModelMetadata("third-invoke", "Third Display"),),
            "static_compatibility",
            "Third-party static catalog.",
        )

    def resolve_model(self, daemon_config: object) -> str:
        return self.model_setting.resolve(self.name, daemon_config)

    def build_run_kwargs(
        self, *, daemon_config: object, **_kwargs: object
    ) -> dict[str, str]:
        return {"model": self.resolve_model(daemon_config)}

    def check_auth(self) -> dict[str, str]:
        return {"status": "ok", "detail": "third authenticated"}


def test_shared_catalog_rendering_supports_third_plugin_without_branches(
    empty_config: Path,
) -> None:
    with TestClient(app) as client:
        client.app.state.coder_registry.register(_ThirdCatalogPlugin())
        response = client.get("/partials/settings/coders")

    assert response.status_code == 200
    assert "Third Coder" in response.text
    assert 'value="third-invoke"' in response.text
    assert "Third Display" in response.text
    assert "Third-party static catalog." in response.text
    assert 'name="coder_settings.third.model"' in response.text
    assert "/partials/settings/coders/third/models/refresh" not in response.text


def test_refresh_indicator_is_css_safe_for_digit_leading_plugin_id(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cfg_path = tmp_path / "config.yml"
    cfg_path.write_text(
        "coder_plugins:\n"
        "  3rd: tests.configured_coder_plugin:build_digit_leading_plugin\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(web_app, "CONFIG_PATH", str(cfg_path))
    monkeypatch.setattr(web_app, "aioredis", _StubAioredis())

    with TestClient(app) as client:
        response = client.get("/partials/settings/coders")

    assert response.status_code == 200
    assert 'hx-indicator="#coder-3rd-model-refreshing"' in response.text
    assert 'id="coder-3rd-model-refreshing"' in response.text


def test_arbitrary_plugin_model_round_trips_without_core_field(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cfg_path = tmp_path / "config.yml"
    cfg_path.write_text(
        "coder_plugins:\n"
        "  third: tests.configured_coder_plugin:build_test_plugin\n"
        "daemon:\n"
        "  coder_settings:\n"
        "    unrelated:\n"
        "      model: keep-me\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(web_app, "CONFIG_PATH", str(cfg_path))
    monkeypatch.setattr(web_app, "aioredis", _StubAioredis())

    with TestClient(app) as client:
        plugin = client.app.state.coder_registry.get("third")
        rendered = client.get("/partials/settings/coders")
        saved = client.put(
            "/settings/daemon",
            data={"coder_settings.third.model": "third-invoke"},
        )

    cfg = load_config(str(cfg_path))
    assert rendered.status_code == 200
    assert saved.status_code == 200
    assert "Configured Test Coder" in rendered.text
    assert "Metadata only" in rendered.text
    assert not re.search(
        r'<input type="radio"[^>]*value="third"',
        rendered.text,
        re.DOTALL,
    )
    assert "third_model" not in type(cfg.daemon).model_fields
    assert cfg.daemon.coder_settings == {
        "unrelated": {"model": "keep-me"},
        "third": {"model": "third-invoke"},
    }
    assert plugin.resolve_model(cfg.daemon) == "third-invoke"
    assert plugin.build_run_kwargs(daemon_config=cfg.daemon) == {
        "model": "third-invoke"
    }


def test_generic_model_submission_rejects_unregistered_plugin(
    empty_config: Path,
) -> None:
    with TestClient(app) as client:
        response = client.put(
            "/settings/daemon",
            data={"coder_settings.not-registered.model": "anything"},
        )

    assert response.status_code == 422
    assert "Unknown coder settings plugin ID" in response.text
    assert load_config(str(empty_config)).daemon.coder_settings == {}


def test_generic_model_submission_rejects_unknown_setting_key(
    empty_config: Path,
) -> None:
    with TestClient(app) as client:
        response = client.put(
            "/settings/daemon",
            data={"coder_settings.codex.not-model": "anything"},
        )

    assert response.status_code == 422
    assert "Unknown coder setting" in response.text


class _NonStringForm(dict[str, object]):
    def multi_items(self) -> list[tuple[str, object]]:
        return list(self.items())


@pytest.mark.parametrize(
    ("form", "message"),
    [
        (
            _NonStringForm({"coder_settings.codex.model": object()}),
            "coder_settings.codex.model must be a string",
        ),
        (
            _NonStringForm({"codex_model": object()}),
            "codex_model must be a string",
        ),
    ],
)
def test_model_submission_parser_rejects_non_string_values(
    form: _NonStringForm,
    message: str,
) -> None:
    from src.coders import build_coder_registry
    from src.web.routes.settings import _submitted_coder_models

    with pytest.raises(ValueError, match=message):
        _submitted_coder_models(form, build_coder_registry())


def test_model_submission_parser_rejects_unavailable_legacy_metadata() -> None:
    from src.coder_registry import CoderMetadataView
    from src.web.routes.settings import _submitted_coder_models

    registry = CoderRegistry()
    registry.register(
        CoderMetadataView(
            name="codex",
            display_name="codex (metadata unavailable)",
            models=[],
            model_setting=ModelSetting(
                "codex_model",
                "",
                "Metadata unavailable",
            ),
            model_catalog_refreshable=False,
            metadata_available=False,
        )
    )

    with pytest.raises(
        ValueError,
        match="Coder metadata is unavailable: codex",
    ):
        _submitted_coder_models(
            _NonStringForm({"codex_model": "gpt-test"}),
            registry,
        )


def test_dynamic_codex_choice_persists_invocation_slug_and_api_metadata(
    empty_config: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    catalog = (
        CodexModel("invoke-future", "GPT Future", False, None, ()),
        CodexModel("invoke-default", "GPT Provider Default", True, None, ()),
    )

    async def discover(**_kwargs: object) -> tuple[CodexModel, ...]:
        return catalog

    with TestClient(app) as client:
        client.app.state.coder_registry.get("codex")._discover = discover
        fragment = client.get("/partials/settings/coders")
        response = client.put(
            "/settings/daemon",
            data={"coder_settings.codex.model": "invoke-future"},
        )
        reloaded = client.get("/partials/settings/coders")
        api_response = client.get("/api/coders")

    assert fragment.status_code == 200
    assert '<option value="invoke-future" >\n                                GPT Future' in fragment.text
    assert "GPT Provider Default (advertised default)" in fragment.text
    assert fragment.text.index("invoke-future") < fragment.text.index("invoke-default")
    assert response.status_code == 200
    assert 'value="invoke-future" selected' in reloaded.text
    cfg = load_config(str(empty_config))
    assert cfg.daemon.codex_model == ""
    assert cfg.daemon.coder_settings["codex"]["model"] == "invoke-future"
    run_kwargs = CodexPlugin().build_run_kwargs(daemon_config=cfg.daemon)
    assert run_kwargs == {
        "model": "invoke-future"
    }
    captured: dict[str, object] = {}

    async def run_cli(
        repo_path: str,
        *,
        model: str | None,
        timeout: int,
        **_kwargs: object,
    ) -> tuple[int, str, str]:
        captured.update(repo_path=repo_path, model=model, timeout=timeout)
        return (0, "ok", "")

    monkeypatch.setattr(
        "src.coders.codex.codex_cli.run_planned_pr_async",
        run_cli,
    )
    assert asyncio.run(
        CodexPlugin().run_planned_pr(
            "/repo",
            timeout=60,
            **run_kwargs,
        )
    ) == (0, "ok", "")
    assert captured == {
        "repo_path": "/repo",
        "model": "invoke-future",
        "timeout": 60,
    }

    codex_row = next(
        row for row in api_response.json()["coders"] if row["name"] == "codex"
    )
    assert "gpt-5.4" in codex_row["models"]
    assert codex_row["model_catalog"]["choices"] == [
        {
            "invocation_id": "invoke-future",
            "display_name": "GPT Future",
            "is_default": False,
            "default_reasoning_effort": None,
            "reasoning_efforts": [],
        },
        {
            "invocation_id": "invoke-default",
            "display_name": "GPT Provider Default",
            "is_default": True,
            "default_reasoning_effort": None,
            "reasoning_efforts": [],
        },
    ]
    claude_row = next(
        row for row in api_response.json()["coders"] if row["name"] == "claude"
    )
    assert claude_row["models"] == ["opus", "sonnet"]
    assert claude_row["model_catalog"]["source"] == "static_compatibility"
    assert "not live or account-verified" in claude_row["model_catalog"]["message"]


def test_codex_discovery_uses_configured_session_context_without_api_key(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cfg_path = tmp_path / "config.yml"
    codex_home = tmp_path / "codex-home"
    cfg_path.write_text(
        "repositories: []\nauth:\n"
        f"  codex_home_dir: {codex_home}\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(web_app, "CONFIG_PATH", str(cfg_path))
    monkeypatch.setattr(web_app, "aioredis", _StubAioredis())
    monkeypatch.setenv("OPENAI_API_KEY", "must-not-be-used")
    captured: dict[str, object] = {}

    async def discover(**kwargs: object) -> tuple[CodexModel, ...]:
        captured.update(kwargs)
        return (CodexModel("session-model", "Session Model", False, None, ()),)

    with TestClient(app) as client:
        client.app.state.coder_registry.get("codex")._discover = discover
        response = client.get("/api/coders")

    assert response.status_code == 200
    assert captured["cwd"] == str(tmp_path)
    env = captured["env"]
    assert isinstance(env, dict)
    assert env["HOME"] == str(codex_home)
    assert "OPENAI_API_KEY" not in env


def test_codex_empty_catalog_rejects_new_slug_but_unrelated_save_works(
    empty_config: Path,
) -> None:
    calls = 0

    async def discover(**_kwargs: object) -> tuple[CodexModel, ...]:
        nonlocal calls
        calls += 1
        return ()

    with TestClient(app) as client:
        client.app.state.coder_registry.get("codex")._discover = discover
        settings_response = client.get("/settings")
        invalid_response = client.put(
            "/settings/daemon",
            data={"coder_settings.codex.model": "unadvertised"},
        )
        unrelated_response = client.put(
            "/settings/daemon",
            data={"poll_interval_sec": "41"},
        )

    assert settings_response.status_code == 200
    assert 'data-model-catalog-status="empty"' in settings_response.text
    assert invalid_response.status_code == 422
    assert "no usable Codex CLI model catalog" in invalid_response.text
    assert unrelated_response.status_code == 200
    assert calls == 1
    cfg = load_config(str(empty_config))
    assert cfg.daemon.codex_model == ""
    assert cfg.daemon.poll_interval_sec == 41


def test_codex_discovery_failure_retains_saved_value_and_refresh_recovers(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cfg_path = tmp_path / "config.yml"
    cfg_path.write_text(
        "daemon:\n"
        "  coder_settings:\n"
        "    codex:\n"
        "      model: saved-custom\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(web_app, "CONFIG_PATH", str(cfg_path))
    monkeypatch.setattr(web_app, "aioredis", _StubAioredis())

    async def fail(**_kwargs: object) -> tuple[CodexModel, ...]:
        raise RuntimeError("raw protocol secret")

    async def recover(**_kwargs: object) -> tuple[CodexModel, ...]:
        return (CodexModel("recovered", "Recovered Model", True, None, ()),)

    with TestClient(app) as client:
        client.app.state.coder_registry.get("codex")._discover = fail
        failed = client.get("/settings")
        retained = client.put(
            "/settings/daemon",
            data={"coder_settings.codex.model": "saved-custom"},
        )
        client.app.state.coder_registry.get("codex")._discover = recover
        refreshed = client.post(
            "/partials/settings/coders/codex/models/refresh"
        )

    assert failed.status_code == 200
    assert 'data-model-catalog-status="unavailable"' in failed.text
    assert "raw protocol secret" not in failed.text
    assert "saved-custom (saved; not advertised)" in failed.text
    assert retained.status_code == 200
    assert refreshed.status_code == 200
    assert 'value="recovered"' in refreshed.text
    assert "saved-custom (saved; not advertised)" in refreshed.text
    assert (
        load_config(str(cfg_path)).daemon.coder_settings["codex"]["model"]
        == "saved-custom"
    )


def test_codex_default_can_be_saved_while_discovery_is_offline(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cfg_path = tmp_path / "config.yml"
    cfg_path.write_text(
        "daemon:\n"
        "  coder_settings:\n"
        "    codex:\n"
        "      model: saved-custom\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(web_app, "CONFIG_PATH", str(cfg_path))
    monkeypatch.setattr(web_app, "aioredis", _StubAioredis())

    async def fail(**_kwargs: object) -> tuple[CodexModel, ...]:
        raise RuntimeError("offline")

    with TestClient(app) as client:
        client.app.state.coder_registry.get("codex")._discover = fail
        failed = client.get("/settings")
        defaulted = client.put(
            "/settings/daemon",
            data={"coder_settings.codex.model": ""},
        )

    assert failed.status_code == 200
    assert defaulted.status_code == 200
    cfg = load_config(str(cfg_path))
    assert cfg.daemon.coder_settings["codex"]["model"] == ""
    assert CodexPlugin().resolve_model(cfg.daemon) == ""


def test_codex_refresh_updates_choices_without_changing_selection(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cfg_path = tmp_path / "config.yml"
    cfg_path.write_text(
        "daemon:\n  codex_model: original-slug\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(web_app, "CONFIG_PATH", str(cfg_path))
    monkeypatch.setattr(web_app, "aioredis", _StubAioredis())

    async def initial(**_kwargs: object) -> tuple[CodexModel, ...]:
        return (CodexModel("original-slug", "Original", True, None, ()),)

    async def updated(**_kwargs: object) -> tuple[CodexModel, ...]:
        return (CodexModel("new-slug", "New Model", True, None, ()),)

    with TestClient(app) as client:
        plugin = client.app.state.coder_registry.get("codex")
        plugin._discover = initial
        first = client.get("/partials/settings/coders")
        plugin._discover = updated
        refreshed = client.post(
            "/partials/settings/coders/codex/models/refresh"
        )

    assert 'value="original-slug" selected' in first.text
    assert 'value="new-slug"' in refreshed.text
    assert "original-slug (saved; not advertised)" in refreshed.text
    assert 'hx-indicator="#coder-codex-model-refreshing"' in refreshed.text
    assert "Refreshing…" in refreshed.text
    assert load_config(str(cfg_path)).daemon.codex_model == "original-slug"


def test_model_refresh_rejects_unknown_and_static_plugins(
    empty_config: Path,
) -> None:
    with TestClient(app) as client:
        unknown = client.post(
            "/partials/settings/coders/missing/models/refresh"
        )
        static = client.post(
            "/partials/settings/coders/claude/models/refresh"
        )

    assert unknown.status_code == 404
    assert unknown.text == "Unknown coder"
    assert static.status_code == 422
    assert static.text == "Model catalog is static"


@pytest.mark.asyncio
async def test_codex_catalog_cache_coalesces_concurrent_refreshes() -> None:
    started = asyncio.Event()
    release = asyncio.Event()
    calls = 0
    models = (
        CodexModel("first", "First", True, None, ()),
        CodexModel("second", "Second", False, None, ()),
    )

    async def discover(**_kwargs: object) -> tuple[CodexModel, ...]:
        nonlocal calls
        calls += 1
        started.set()
        await release.wait()
        return models

    plugin = CodexPlugin(discover=discover)
    config = AppConfig.model_validate(
        {"auth": {"codex_home_dir": "/auth"}}
    )
    cache = ModelCatalogCache()
    first = asyncio.create_task(
        cache.get(plugin, config=config, config_path="/workspace/config.yml")
    )
    await started.wait()
    second = asyncio.create_task(
        cache.get(
            plugin,
            config=config,
            config_path="/workspace/config.yml",
            refresh=True,
        )
    )
    await asyncio.sleep(0)
    release.set()

    first_snapshot, second_snapshot = await asyncio.gather(first, second)
    cached_snapshot = await cache.get(
        plugin,
        config=config,
        config_path="/workspace/config.yml",
    )

    assert calls == 1
    assert first_snapshot == second_snapshot == cached_snapshot
    assert [model.invocation_id for model in cached_snapshot.models] == [
        "first",
        "second",
    ]
    assert cached_snapshot.status == "available"
    assert cached_snapshot.message == "2 models advertised by Codex CLI."
    assert (
        cache.peek(
            plugin,
            config=config,
            config_path="/workspace/config.yml",
        )
        == cached_snapshot
    )
    other_config = AppConfig.model_validate(
        {"auth": {"codex_home_dir": "/other-auth"}}
    )
    await cache.get(
        plugin,
        config=other_config,
        config_path="/workspace/config.yml",
    )
    assert calls == 2
    await cache.close()


@pytest.mark.asyncio
async def test_static_catalog_bypasses_daemon_loader() -> None:
    async def unavailable_loader(*_args: object, **_kwargs: object) -> ModelCatalog:
        raise AssertionError("static catalog must not use daemon loader")

    cache = ModelCatalogCache(loader=unavailable_loader)
    config = AppConfig()
    snapshot = await cache.get(
        ClaudePlugin(),
        config=config,
        config_path="/workspace/config.yml",
    )

    assert snapshot.status == "available"
    assert [model.invocation_id for model in snapshot.models] == [
        "opus",
        "sonnet",
    ]
    assert snapshot.source == "static_compatibility"


@pytest.mark.asyncio
async def test_configured_static_catalog_uses_daemon_loader() -> None:
    calls: list[str] = []

    class UnsafeConfiguredCatalog(_ThirdCatalogPlugin):
        def model_catalog_cache_key(self, **_kwargs: object) -> str:
            raise AssertionError("configured cache key must stay out of web")

    async def daemon_loader(
        plugin: object, **_kwargs: object
    ) -> ModelCatalog:
        calls.append(plugin.name)
        return ModelCatalog(
            (ModelMetadata("isolated", "Daemon-owned"),),
            "daemon",
            "Loaded outside the web process.",
        )

    cache = ModelCatalogCache(
        loader=daemon_loader,
        daemon_owned_plugins={"third"},
    )
    snapshot = await cache.get(
        UnsafeConfiguredCatalog(),
        config=AppConfig(),
        config_path="/workspace/config.yml",
    )
    changed = await cache.get(
        UnsafeConfiguredCatalog(),
        config=AppConfig(
            daemon={"coder_settings": {"third": {"variant": "preview"}}}
        ),
        config_path="/workspace/config.yml",
    )

    assert calls == ["third", "third"]
    assert len(cache._entries) == 1
    assert snapshot.source == "daemon"
    assert [model.invocation_id for model in snapshot.models] == ["isolated"]
    assert changed.source == "daemon"


@pytest.mark.asyncio
async def test_codex_catalog_cache_expires_retains_last_known_and_handles_empty(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = [0.0]
    monkeypatch.setattr(_model_catalog.time, "monotonic", lambda: clock[0])
    model = CodexModel("known", "Known", False, None, ())
    should_fail = False

    async def discover(**_kwargs: object) -> tuple[CodexModel, ...]:
        if should_fail:
            raise RuntimeError("unsafe provider detail")
        return (model,)

    config = AppConfig.model_validate(
        {"auth": {"codex_home_dir": "/auth"}}
    )
    plugin = CodexPlugin(discover=discover)
    cache = ModelCatalogCache(ttl_seconds=10)
    ready = await cache.get(
        plugin, config=config, config_path="/workspace/config.yml"
    )
    clock[0] = 11
    expired = cache.peek(
        plugin, config=config, config_path="/workspace/config.yml"
    )
    should_fail = True
    stale = await cache.get(
        plugin, config=config, config_path="/workspace/config.yml"
    )

    assert ready.status == "available"
    assert expired.status == "stale"
    assert "refresh is due" in expired.message
    assert stale.status == "stale"
    assert [candidate.invocation_id for candidate in stale.models] == ["known"]
    assert "unsafe provider detail" not in stale.message

    async def empty(**_kwargs: object) -> tuple[CodexModel, ...]:
        return ()

    empty_plugin = CodexPlugin(discover=empty)
    empty_cache = ModelCatalogCache(ttl_seconds=10)
    empty_snapshot = await empty_cache.get(
        empty_plugin, config=config, config_path="/workspace/config.yml"
    )
    clock[0] = 22
    expired_empty = empty_cache.peek(
        empty_plugin, config=config, config_path="/workspace/config.yml"
    )
    assert empty_snapshot.status == "empty"
    assert expired_empty.status == "empty"
    assert not expired_empty.has_usable_models

    async def unavailable(**_kwargs: object) -> tuple[CodexModel, ...]:
        raise RuntimeError("provider failed")

    unavailable_plugin = CodexPlugin(discover=unavailable)
    unavailable_cache = ModelCatalogCache()
    unavailable_snapshot = await unavailable_cache.get(
        unavailable_plugin,
        config=config,
        config_path="/workspace/config.yml",
    )
    assert unavailable_snapshot.status == "unavailable"
    assert (
        unavailable_cache.peek(
            unavailable_plugin,
            config=config,
            config_path="/workspace/config.yml",
        )
        == unavailable_snapshot
    )
    other_config = AppConfig.model_validate(
        {"auth": {"codex_home_dir": "/other"}}
    )
    assert (
        cache.peek(
            plugin,
            config=other_config,
            config_path="/workspace/config.yml",
        ).status
        == "not_loaded"
    )

    with pytest.raises(ValueError, match="TTL must be positive"):
        ModelCatalogCache(ttl_seconds=0)


@pytest.mark.asyncio
async def test_codex_catalog_cache_cancels_owned_discovery_on_close() -> None:
    started = asyncio.Event()
    cancelled = asyncio.Event()

    async def discover(**_kwargs: object) -> tuple[CodexModel, ...]:
        started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            cancelled.set()
            raise

    plugin = CodexPlugin(discover=discover)
    config = AppConfig.model_validate(
        {"auth": {"codex_home_dir": "/auth"}}
    )
    cache = ModelCatalogCache()
    pending = asyncio.create_task(
        cache.get(
            plugin,
            config=config,
            config_path="/workspace/config.yml",
        )
    )
    await started.wait()
    await cache.close()

    with pytest.raises(asyncio.CancelledError):
        await pending
    assert cancelled.is_set()


def test_slow_codex_refresh_renders_selection_saved_during_request(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cfg_path = tmp_path / "config.yml"
    cfg_path.write_text(
        "daemon:\n  codex_model: old-selection\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(web_app, "CONFIG_PATH", str(cfg_path))
    monkeypatch.setattr(web_app, "aioredis", _StubAioredis())
    started = threading.Event()
    release = threading.Event()
    refresh_result: dict[str, object] = {}

    async def slow_discovery(**_kwargs: object) -> tuple[CodexModel, ...]:
        started.set()
        await asyncio.to_thread(release.wait)
        return (CodexModel("new-choice", "New Choice", True, None, ()),)

    with TestClient(app) as client:
        client.app.state.coder_registry.get("codex")._discover = slow_discovery

        def refresh() -> None:
            refresh_result["response"] = client.post(
                "/partials/settings/coders/codex/models/refresh"
            )

        thread = threading.Thread(target=refresh)
        thread.start()
        assert started.wait(timeout=2)
        saved = client.put(
            "/settings/daemon",
            data={"codex_model": ""},
        )
        release.set()
        thread.join(timeout=2)

    assert not thread.is_alive()
    assert saved.status_code == 200
    refreshed = refresh_result["response"]
    assert hasattr(refreshed, "text")
    assert '<option value="" selected>' in refreshed.text
    assert 'value="old-selection"' not in refreshed.text
    assert load_config(str(cfg_path)).daemon.codex_model == ""


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
    assert cfg.daemon.review_timeout_min == 20
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
    assert cfg.daemon.review_timeout_min == 20


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


def test_put_daemon_invalid_exploration_epsilon_returns_422_html(
    empty_config: Path,
) -> None:
    with TestClient(app) as client:
        response = client.put(
            "/settings/daemon",
            data={"exploration_epsilon": "75"},
        )

    assert response.status_code == 422
    assert "exploration_epsilon" in response.text
    cfg = load_config(str(empty_config))
    assert cfg.daemon.exploration_epsilon == 0.15


def test_put_daemon_rejects_invalid_coder_and_update_validation_error(
    empty_config: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    with TestClient(app) as client:
        invalid = client.put("/settings/daemon", data={"coder": "other"})
    assert invalid.status_code == 422

    def _raise_value_error(*args: object, **kwargs: object) -> None:
        raise ValueError("update rejected")

    monkeypatch.setattr(web_app, "update_daemon_config", _raise_value_error)

    with TestClient(app) as client:
        response = client.put("/settings/daemon", data={"poll_interval_sec": "45"})

    assert response.status_code == 422
    assert "update rejected" in response.text


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
    assert 'id="settings-coders"' in body
    assert 'hx-swap-oob="outerHTML"' in body


def test_put_daemon_error_oob_refreshes_coder_controls(
    empty_config: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(src_config, "save_config", _raise_permission_error)

    with TestClient(app) as client:
        response = client.put(
            "/settings/daemon",
            data={"coder": "codex"},
        )

    assert response.status_code == 503
    body = response.text
    assert 'id="settings-coders"' in body
    assert 'hx-swap-oob="outerHTML"' in body
    assert re.search(
        r'name="coder"[\s\S]*value="claude"[\s\S]*checked',
        body,
    )
    assert re.search(
        r'name="coder"[\s\S]*value="codex"',
        body,
    )
    assert not re.search(
        r'name="coder"[\s\S]*value="codex"[\s\S]*checked',
        body,
    )


def test_put_daemon_rejects_values_above_maximums(empty_config: Path) -> None:
    with TestClient(app) as client:
        int_response = client.put(
            "/settings/daemon",
            data={"rate_limit_weekly_pause_percent": "101"},
        )
        epsilon_response = client.put(
            "/settings/daemon",
            data={"exploration_epsilon": "51"},
        )

    assert int_response.status_code == 422
    assert "at most 100" in int_response.text
    assert epsilon_response.status_code == 422
    assert "at most 50" in epsilon_response.text


def test_put_daemon_rejects_invalid_and_too_small_exploration_epsilon(
    empty_config: Path,
) -> None:
    with TestClient(app) as client:
        invalid = client.put(
            "/settings/daemon",
            data={"exploration_epsilon": "abc"},
        )
        too_small = client.put(
            "/settings/daemon",
            data={"exploration_epsilon": "-1"},
        )

    assert invalid.status_code == 422
    assert "must be an integer" in invalid.text
    assert too_small.status_code == 422
    assert "at least 0" in too_small.text


def test_put_daemon_does_not_probe_auth_status(
    empty_config: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls: list[list[str]] = []

    def fail_if_called(
        cmd: list[str], *args: object, **kwargs: object
    ) -> _FakeCompleted:
        calls.append(cmd)
        raise AssertionError(f"unexpected auth probe during daemon PUT: {cmd}")

    monkeypatch.setattr(web_app.subprocess, "run", fail_if_called)

    with TestClient(app) as client:
        response = client.put(
            "/settings/daemon",
            data={"poll_interval_sec": "45"},
        )

    assert response.status_code == 200
    assert calls == []


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


def test_api_auth_status_uses_every_configured_plugin(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cfg = tmp_path / "config.yml"
    cfg.write_text(
        "repositories: []\n"
        "coder_plugins:\n"
        "  claude: tests.configured_coder_plugin:build_claude_override\n"
        "  third: tests.configured_coder_plugin:build_test_plugin\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(web_app, "CONFIG_PATH", str(cfg))
    monkeypatch.setattr(web_app, "aioredis", _StubAioredis())

    with TestClient(app) as client:
        response = client.get("/api/auth-status")

    assert response.status_code == 200
    assert response.json()["claude"] == {
        "status": "ok",
        "detail": "configured plugin auth",
    }
    assert response.json()["third"] == {
        "status": "ok",
        "detail": "configured test plugin auth",
    }


def test_configured_plugin_auth_failure_is_isolated_and_redacted(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cfg = tmp_path / "config.yml"
    cfg.write_text(
        "repositories: []\n"
        "coder_plugins:\n"
        "  third: tests.configured_coder_plugin:build_raising_auth_plugin\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(web_app, "CONFIG_PATH", str(cfg))
    monkeypatch.setattr(web_app, "aioredis", _StubAioredis())

    with TestClient(app) as client:
        api_response = client.get("/api/auth-status")
        settings_response = client.get("/settings")

    assert api_response.status_code == 200
    assert settings_response.status_code == 200
    assert api_response.json()["third"] == {
        "status": "error",
        "detail": "Configured Test Coder auth check failed (RuntimeError)",
    }
    assert "must-not-leak" not in api_response.text
    assert "must-not-leak" not in settings_response.text


def test_configured_plugin_auth_probe_has_response_timeout(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cfg = tmp_path / "config.yml"
    cfg.write_text(
        "repositories: []\n"
        "coder_plugins:\n"
        "  third: tests.configured_coder_plugin:build_slow_auth_plugin\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(web_app, "CONFIG_PATH", str(cfg))
    monkeypatch.setattr(web_app, "aioredis", _StubAioredis())
    monkeypatch.setattr(_auth_probe, "_AUTH_CHECK_TIMEOUT_SEC", 0.01)

    with TestClient(app) as client:
        response = client.get("/api/auth-status")

    assert response.status_code == 200
    assert response.json()["third"] == {
        "status": "error",
        "detail": "Configured Test Coder auth check timed out after 0.01s",
    }


def test_direct_coder_auth_probe_redacts_plugin_exception() -> None:
    from tests.configured_coder_plugin import RaisingAuthTestPlugin

    registry = CoderRegistry()
    registry.register(RaisingAuthTestPlugin())

    assert _auth_probe._check_coder_auth(registry, "third") == {
        "status": "error",
        "detail": "Configured Test Coder auth check failed (RuntimeError)",
    }


def test_configured_auth_probe_degrades_without_daemon_bridge(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.coder_registry import CoderMetadataView

    registry = CoderRegistry()
    registry.register(
        CoderMetadataView(
            name="third",
            display_name="Third Coder",
            models=[],
            model_setting=ModelSetting(None, "", "Default"),
            model_catalog_refreshable=False,
        ),
        reference="operator.plugin:factory",
    )
    monkeypatch.delattr(web_app.app.state, "plugin_bridge", raising=False)

    result = asyncio.run(
        _auth_probe._bounded_coder_auth_probe(registry, "third")
    )

    assert result == {
        "status": "error",
        "detail": "Third Coder auth check is unavailable",
    }


def test_auth_probe_worker_normalizes_results(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seen: list[str] = []

    class _WithConfigPath:
        display_name = "Worker Coder"

        def check_auth(self, *, config_path: str) -> dict[str, str]:
            seen.append(config_path)
            return {"status": "ok", "detail": "ready"}

    monkeypatch.setattr(
        _auth_worker,
        "_load_plugin",
        lambda _plugin_id, _reference: _WithConfigPath(),
    )
    assert _auth_worker.run_probe("third", "module:factory", "/cfg") == {
        "status": "ok",
        "detail": "ready",
    }
    assert seen == ["/cfg"]

    class _InvalidResult:
        display_name = "Invalid Coder"

        def check_auth(self) -> object:
            return {"status": object()}

    monkeypatch.setattr(
        _auth_worker,
        "_load_plugin",
        lambda _plugin_id, _reference: _InvalidResult(),
    )
    assert _auth_worker.run_probe("third", "module:factory", "/cfg") == {
        "status": "error",
        "detail": "Invalid Coder auth check failed (TypeError)",
    }

    monkeypatch.setattr(
        _auth_worker,
        "_load_plugin",
        lambda _plugin_id, _reference: (_ for _ in ()).throw(
            RuntimeError("must-not-leak")
        ),
    )
    assert _auth_worker.run_probe("third", "module:factory", "/cfg") == {
        "status": "error",
        "detail": "third auth check failed (RuntimeError)",
    }


def test_auth_probe_worker_main_validates_arguments_and_prints_result(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(_auth_worker.sys, "argv", ["auth-probe-worker"])
    with pytest.raises(SystemExit, match="2"):
        _auth_worker.main()

    monkeypatch.setattr(
        _auth_worker,
        "run_probe",
        lambda *_args: {"status": "ok", "detail": "ready"},
    )
    monkeypatch.setattr(
        _auth_worker.sys,
        "argv",
        ["auth-probe-worker", "third", "module:factory", "/cfg"],
    )
    _auth_worker.main()

    assert capsys.readouterr().out.strip() == (
        _auth_worker.RESULT_PREFIX + '{"status":"ok","detail":"ready"}'
    )


class _FakeAuthProbeProcess:
    def __init__(self, stdout: bytes, returncode: int) -> None:
        self.pid = 12345
        self.returncode = returncode
        self._stdout = stdout

    async def communicate(self) -> tuple[bytes, bytes]:
        return self._stdout, b""


@pytest.mark.parametrize(
    ("stdout", "returncode", "expected_detail"),
    [
        (b"", 1, "Worker auth check worker failed"),
        (
            b"PIPELINE_AUTH_RESULT:{bad json}\nnoise\n",
            0,
            "Worker auth check returned an invalid result",
        ),
        (
            b"PIPELINE_AUTH_RESULT:[]\n",
            0,
            "Worker auth check returned an invalid result",
        ),
    ],
)
def test_isolated_auth_probe_rejects_worker_failures_and_invalid_output(
    monkeypatch: pytest.MonkeyPatch,
    stdout: bytes,
    returncode: int,
    expected_detail: str,
) -> None:
    async def fake_subprocess(*_args: object, **_kwargs: object) -> object:
        return _FakeAuthProbeProcess(stdout, returncode)

    monkeypatch.setattr(
        _coder_auth.asyncio,
        "create_subprocess_exec",
        fake_subprocess,
    )

    result = asyncio.run(
        _coder_auth.isolated_auth_probe(
            "third",
            "module:factory",
            "Worker",
            config_path="/cfg",
        )
    )

    assert result == {"status": "error", "detail": expected_detail}


def test_isolated_auth_probe_redacts_worker_start_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def failing_subprocess(*_args: object, **_kwargs: object) -> object:
        raise OSError("must-not-leak")

    monkeypatch.setattr(
        _coder_auth.asyncio,
        "create_subprocess_exec",
        failing_subprocess,
    )

    result = asyncio.run(
        _coder_auth.isolated_auth_probe(
            "third",
            "module:factory",
            "Worker",
            config_path="/cfg",
        )
    )

    assert result == {
        "status": "error",
        "detail": "Worker auth check failed (OSError)",
    }


def test_isolated_auth_probe_handles_worker_exit_during_timeout_cleanup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _SlowProcess:
        pid = 12345
        returncode: int | None = None

        async def communicate(self) -> tuple[bytes, bytes]:
            await asyncio.Event().wait()
            raise AssertionError("unreachable")

        async def wait(self) -> int:
            self.returncode = 0
            return 0

    async def fake_subprocess(*_args: object, **_kwargs: object) -> object:
        return _SlowProcess()

    def exited_process_group(_pid: int, _signal: int) -> None:
        raise ProcessLookupError

    monkeypatch.setattr(
        _coder_auth.asyncio,
        "create_subprocess_exec",
        fake_subprocess,
    )
    monkeypatch.setattr(_coder_auth.os, "killpg", exited_process_group)

    result = asyncio.run(
        _coder_auth.isolated_auth_probe(
            "third",
            "module:factory",
            "Worker",
            config_path="/cfg",
            timeout=0.001,
        )
    )

    assert result == {
        "status": "error",
        "detail": "Worker auth check timed out after 0.001s",
    }


def test_isolated_auth_probe_terminates_worker_when_caller_is_cancelled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _BlockedProcess:
        pid = 12345
        returncode: int | None = None
        reaped = False

        async def communicate(self) -> tuple[bytes, bytes]:
            await asyncio.Event().wait()
            raise AssertionError("unreachable")

        async def wait(self) -> int:
            self.reaped = True
            self.returncode = -9
            return -9

    process = _BlockedProcess()
    killed: list[tuple[int, int]] = []

    async def fake_subprocess(*_args: object, **_kwargs: object) -> object:
        return process

    monkeypatch.setattr(
        _coder_auth.asyncio,
        "create_subprocess_exec",
        fake_subprocess,
    )
    monkeypatch.setattr(
        _coder_auth.os,
        "killpg",
        lambda pid, sig: killed.append((pid, sig)),
    )

    async def scenario() -> None:
        task = asyncio.create_task(
            _coder_auth.isolated_auth_probe(
                "third",
                "module:factory",
                "Worker",
                config_path="/cfg",
                timeout=60,
            )
        )
        await asyncio.sleep(0)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    asyncio.run(scenario())

    assert killed == [(process.pid, _coder_auth.signal.SIGKILL)]
    assert process.reaped is True


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


def test_default_auth_status_and_first_probe_line_helpers() -> None:
    default = web_app._default_auth_status()
    assert default["claude"] == {
        "status": "error",
        "detail": "Status unavailable",
    }
    assert default["codex"] == {
        "status": "error",
        "detail": "Status unavailable",
    }
    assert default["gh"] == {
        "status": "error",
        "detail": "Status unavailable",
    }
    assert web_app._first_probe_line("") == ""
    assert (
        web_app._first_probe_line("warning: noisy\n\nready\nnext")
        == "ready"
    )


def test_run_auth_command_reports_missing_binary_and_permission_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def _raise_missing(*args: object, **kwargs: object) -> None:
        raise FileNotFoundError("missing")

    monkeypatch.setattr(web_app.subprocess, "run", _raise_missing)
    assert web_app._run_auth_command(["ghost"]) == (127, "", "ghost not found")

    def _raise_permission(*args: object, **kwargs: object) -> None:
        raise PermissionError("denied")

    monkeypatch.setattr(web_app.subprocess, "run", _raise_permission)
    assert web_app._run_auth_command(["ghost"]) == (126, "", "denied")


def test_check_gh_auth_without_output_reports_not_configured(
    empty_config: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        _auth_probe,
        "_run_auth_command",
        lambda *args, **kwargs: (1, "", ""),
    )

    status = web_app._check_gh_auth()

    assert status == {"status": "error", "detail": "gh CLI not configured"}


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


def test_api_coders_returns_rows(empty_config: Path) -> None:
    with TestClient(app) as client:
        response = client.get("/api/coders")

    assert response.status_code == 200
    payload = response.json()
    assert "coders" in payload
    assert {row["name"] for row in payload["coders"]} == {"claude", "codex"}


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


def test_put_repo_rejects_invalid_coder_value(one_repo_config: Path) -> None:
    with TestClient(app) as client:
        response = client.put(
            "/settings/repos",
            params={"url": "https://github.com/example/alpha.git"},
            data={"coder": "other"},
        )

    assert response.status_code == 422
    assert "coder must be" in response.text


# ---------------------------------------------------------------------------
# PR-060: Settings UX - group headers, hints, and default placeholder
# ---------------------------------------------------------------------------


def test_settings_daemon_renders_with_group_headers(empty_config: Path) -> None:
    """Settings page must show the reorganized daemon section headers."""
    with TestClient(app) as client:
        response = client.get("/settings")

    assert response.status_code == 200
    body = response.text
    for header in ("Timeouts", "Self-Heal", "Coders"):
        assert header in body, f"Missing group header: {header}"
    assert "Rate Limits" not in body


def test_settings_daemon_renders_hints_for_all_fields(empty_config: Path) -> None:
    """Every daemon field must keep a single concise hint after regrouping."""
    with TestClient(app) as client:
        response = client.get("/partials/settings/daemon")

    assert response.status_code == 200
    body = response.text
    field_names = [
        "poll_interval_sec",
        "planned_pr_timeout_sec",
        "fix_idle_timeout_sec",
        "review_timeout_min",
        "auto_fallback",
        "hung_fallback_codex_review",
        "error_handler_use_ai",
        "exploration_epsilon",
    ]
    for field_name in field_names:
        assert f'name="{field_name}"' in body, f"Missing field: {field_name}"

    hints = [
        "Seconds between daemon checks across configured repositories.",
        "Seconds before the coder subprocess is killed if no activity.",
        "FIX time budget, reset on each successful push.",
        "Minutes to wait for review before the PR moves to HUNG.",
        "Switch to another eligible coder when the preferred one is unavailable.",
        "Re-post the review trigger when a PR times out waiting for Codex.",
        "Use the coder to diagnose ERROR before generic recovery kicks in.",
        "Percent (0-50) of runs that use a different eligible coder for comparison data.",
    ]
    for hint_text in hints:
        assert hint_text in body, f"Missing hint: {hint_text}"

    assert "Kills the coder during FIX if no push. Resets on each push." not in body
    assert "When the preferred coder is unavailable, let the selector pick another eligible coder." not in body
    assert "If Codex doesn't review in time, PR flips to HUNG." not in body
    assert "Probability 0-0.5 that selector picks a non-top eligible coder" not in body


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


def _input_tag(body: str, field_name: str) -> str:
    match = re.search(rf'<input\b[^>]*name="{field_name}"[^>]*>', body)
    assert match is not None, f"Missing input: {field_name}"
    return match.group(0)


def test_single_usage_limits_section(empty_config: Path) -> None:
    with TestClient(app) as client:
        response = client.get("/settings")

    assert response.status_code == 200
    body = response.text
    assert body.count(">Usage limits<") == 1
    assert "Rate Limits" not in body
    assert "Spending controls" not in body


def test_all_controls_present(empty_config: Path) -> None:
    with TestClient(app) as client:
        response = client.get("/settings")

    assert response.status_code == 200
    body = response.text
    for field_name in (
        "rate_limit_session_pause_percent",
        "rate_limit_weekly_pause_percent",
        "spend_ceiling_session_percent",
        "spend_ceiling_weekly_percent",
        "spend_ceiling_warning_percent",
    ):
        assert f'name="{field_name}"' in body


def test_explains_proactive_vs_reactive(
    empty_config: Path,
) -> None:
    with TestClient(app) as client:
        response = client.get("/settings")

    assert response.status_code == 200
    body = response.text
    assert "proactively before the provider returns a limit response" in body
    assert "Reactive provider-limit detection is always on" in body


def test_persists_to_same_fields(
    empty_config: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(web_app, "CONFIG_PATH", str(empty_config))

    with TestClient(app) as client:
        assert client.put(
            "/settings/daemon",
            data={"rate_limit_session_pause_percent": "76"},
        ).status_code == 200
        assert client.put(
            "/settings/daemon",
            data={"rate_limit_weekly_pause_percent": "91"},
        ).status_code == 200
        assert client.post(
            "/settings/config/spend_ceiling_session_percent",
            data={"spend_ceiling_session_percent": "72"},
        ).status_code == 200
        assert client.post(
            "/settings/config/spend_ceiling_weekly_percent",
            data={"spend_ceiling_weekly_percent": "88"},
        ).status_code == 200
        assert client.post(
            "/settings/config/spend_ceiling_warning_percent",
            data={"spend_ceiling_warning_percent": "66"},
        ).status_code == 200

    cfg = load_config(str(empty_config))
    assert cfg.daemon.rate_limit_session_pause_percent == 76
    assert cfg.daemon.rate_limit_weekly_pause_percent == 91
    assert cfg.daemon.spend_ceiling_session_percent == 72
    assert cfg.daemon.spend_ceiling_weekly_percent == 88
    assert cfg.daemon.spend_ceiling_warning_percent == 66
    assert cfg.daemon.usage_gate_rate_limit_session_pause_percent == 76
    assert cfg.daemon.usage_gate_rate_limit_weekly_pause_percent == 91
    assert cfg.daemon.usage_gate_spend_ceiling_session_percent == 72
    assert cfg.daemon.usage_gate_spend_ceiling_weekly_percent == 88


def test_reset_to_defaults_present(empty_config: Path) -> None:
    with TestClient(app) as client:
        response = client.get("/settings")

    assert response.status_code == 200
    body = response.text
    assert "Reset to defaults" in body
    assert 'hx-post="/settings/config/reset/spend_ceiling"' in body
    assert 'hx-target="#settings-usage-limits"' in body


def test_htmx_persistence_preserved(empty_config: Path) -> None:
    with TestClient(app) as client:
        response = client.get("/settings")

    assert response.status_code == 200
    body = response.text
    assert 'hx-put="/settings/daemon"' in _input_tag(
        body, "rate_limit_session_pause_percent"
    )
    assert 'hx-put="/settings/daemon"' in _input_tag(
        body, "rate_limit_weekly_pause_percent"
    )
    for field_name in (
        "spend_ceiling_session_percent",
        "spend_ceiling_weekly_percent",
        "spend_ceiling_warning_percent",
    ):
        assert f'hx-post="/settings/config/{field_name}"' in _input_tag(
            body, field_name
        )
    for field_name in (
        "rate_limit_session_pause_percent",
        "rate_limit_weekly_pause_percent",
        "spend_ceiling_session_percent",
        "spend_ceiling_weekly_percent",
        "spend_ceiling_warning_percent",
    ):
        assert 'hx-trigger="change"' in _input_tag(body, field_name)


def test_no_orphaned_rate_limits_heading() -> None:
    daemon_template = Path("src/web/templates/components/settings_daemon.html")
    assert "Rate Limits" not in daemon_template.read_text(encoding="utf-8")


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
            data={"codex_model": "gpt-5.4"},
        )

    assert response.status_code == 200
    cfg = load_config(str(empty_config))
    assert cfg.daemon.codex_model == "gpt-5.4"
    assert cfg.daemon.coder_settings["codex"]["model"] == "gpt-5.4"
    assert CodexPlugin().resolve_model(cfg.daemon) == "gpt-5.4"


def test_codex_model_setting_clears_to_default(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cfg_path = tmp_path / "config.yml"
    cfg_path.write_text("daemon:\n  codex_model: o4-mini\n", encoding="utf-8")
    monkeypatch.setattr(web_app, "CONFIG_PATH", str(cfg_path))

    with TestClient(app) as client:
        response = client.put(
            "/settings/daemon",
            data={"codex_model": ""},
        )

    assert response.status_code == 200
    cfg = load_config(str(cfg_path))
    assert cfg.daemon.codex_model == ""
    assert cfg.daemon.coder_settings["codex"]["model"] == ""
    assert CodexPlugin().resolve_model(cfg.daemon) == ""


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
    assert cfg.daemon.coder_settings["claude"]["model"] == "sonnet"
    assert ClaudePlugin().resolve_model(cfg.daemon) == "sonnet"


def test_claude_model_setting_empty_uses_default(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cfg_path = tmp_path / "config.yml"
    cfg_path.write_text("daemon:\n  claude_model: sonnet\n", encoding="utf-8")
    monkeypatch.setattr(web_app, "CONFIG_PATH", str(cfg_path))

    with TestClient(app) as client:
        response = client.put(
            "/settings/daemon",
            data={"claude_model": ""},
        )

    assert response.status_code == 200
    cfg = load_config(str(cfg_path))
    assert cfg.daemon.claude_model == "opus"
    assert cfg.daemon.coder_settings["claude"]["model"] == "opus"
    assert ClaudePlugin().resolve_model(cfg.daemon) == "opus"


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


def test_coders_table_renders_without_polling_after_sse_migration(
    empty_config: Path,
) -> None:
    """PR-228: the coders fragment no longer polls; SSE-driven dashboards
    replace the legacy 30s refresh on the settings page."""
    with TestClient(app) as client:
        response = client.get("/settings")

    assert response.status_code == 200
    body = response.text
    assert 'id="settings-coders"' in body
    # The redis-banner in base.html is the only sanctioned `every` poll
    # after PR-228; the coders fragment must no longer self-refresh.
    assert 'hx-get="/partials/settings/coders"' not in body


def test_coders_table_omits_unknown_selected_model(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    cfg_path = tmp_path / "config.yml"
    cfg_path.write_text(
        "daemon:\n  coder: codex\n  codex_model: custom-model\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(web_app, "CONFIG_PATH", str(cfg_path))

    with TestClient(app) as client:
        response = client.get("/settings")

    assert response.status_code == 200
    body = response.text
    assert '<option value="custom-model" selected>' in body
    assert "custom-model (saved; not advertised)" in body
    assert 'value=""' in body
    assert "CLI default" in body


def test_repo_detail_coder_display_renders_readonly(
    one_repo_config: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(web_app, "CONFIG_PATH", str(one_repo_config))

    with TestClient(app) as client:
        response = client.get("/repo/example__alpha")

    assert response.status_code == 200
    body = response.text
    assert 'data-coder-display' in body
    assert '<select name="coder"' not in body
    assert 'hx-post="/repos/example__alpha/coder"' not in body
    assert "Any (bandit)" in body
    assert "inherits Claude" in body
    assert 'href="/settings"' in body


def test_repo_detail_coder_display_shows_repo_override(
    one_repo_config: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    one_repo_config.write_text(
        "daemon:\n"
        "  coder: claude\n"
        "repositories:\n"
        "  - url: https://github.com/example/alpha.git\n"
        "    branch: main\n"
        "    auto_merge: true\n"
        "    review_timeout_min: 60\n"
        "    coder: codex\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(web_app, "CONFIG_PATH", str(one_repo_config))

    with TestClient(app) as client:
        response = client.get("/repo/example__alpha")

    assert response.status_code == 200
    body = response.text
    assert '<select name="coder"' not in body
    assert "Codex" in body
    assert "inherits Claude" not in body


def test_repo_coder_change_posts_selector_updates_config_and_sets_dirty_flag(
    one_repo_config: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(web_app, "CONFIG_PATH", str(one_repo_config))
    fake_redis = _FakeRedis()
    published: list[tuple[str, str, dict[str, str]]] = []

    async def fake_publish(
        repo_name: str,
        event_type: str,
        payload: dict[str, str],
        redis_client: object | None = None,
    ) -> None:
        published.append((repo_name, event_type, payload))

    monkeypatch.setattr(web_app, "publish_repo_event", fake_publish)

    with TestClient(app) as client:
        client.app.state.redis = fake_redis
        response = client.post(
            "/repos/example__alpha/coder",
            data={"coder": "codex"},
        )

    assert response.status_code == 200
    assert "Switching to Codex CLI." in response.text
    cfg = load_config(str(one_repo_config))
    assert cfg.repositories[0].coder is not None
    assert cfg.repositories[0].coder.value == "codex"
    assert fake_redis.store["control:example__alpha:config_dirty"] == "1"
    assert published == [
        (
            "example__alpha",
            "config_reloaded",
            {"coder": "codex", "effective_coder": "codex"},
        )
    ]


def test_repo_coder_change_updates_idle_repo_state_payload(
    one_repo_config: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(web_app, "CONFIG_PATH", str(one_repo_config))
    fake_redis = _FakeRedis()
    fake_redis.store["pipeline:example__alpha"] = RepoState(
        url="https://github.com/example/alpha.git",
        name="example__alpha",
        state=PipelineState.IDLE,
        coder="claude",
    ).model_dump_json()

    async def fake_publish(
        repo_name: str,
        event_type: str,
        payload: dict[str, str],
        redis_client: object | None = None,
    ) -> None:
        return None

    with TestClient(app) as client:
        client.app.state.redis = fake_redis
        monkeypatch.setattr(web_app, "publish_repo_event", fake_publish)
        response = client.post(
            "/repos/example__alpha/coder",
            data={"coder": "codex"},
        )

    assert response.status_code == 200
    persisted = RepoState.model_validate_json(fake_redis.store["pipeline:example__alpha"])
    assert persisted.coder == "codex"


def test_repo_coder_change_during_active_pr_is_deferred(
    one_repo_config: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(web_app, "CONFIG_PATH", str(one_repo_config))
    fake_redis = _FakeRedis()
    fake_redis.store["pipeline:example__alpha"] = RepoState(
        url="https://github.com/example/alpha.git",
        name="example__alpha",
        state=PipelineState.WATCH,
        coder="claude",
    ).model_dump_json()

    async def fake_publish(
        repo_name: str,
        event_type: str,
        payload: dict[str, str],
        redis_client: object | None = None,
    ) -> None:
        return None

    with TestClient(app) as client:
        client.app.state.redis = fake_redis
        monkeypatch.setattr(web_app, "publish_repo_event", fake_publish)
        response = client.post(
            "/repos/example__alpha/coder",
            data={"coder": "any"},
        )

    assert response.status_code == 200
    assert "applies after current PR completes" in response.text
    persisted = RepoState.model_validate_json(fake_redis.store["pipeline:example__alpha"])
    assert persisted.coder == "claude"


def test_post_repo_detail_coder_handles_missing_invalid_and_write_error(
    one_repo_config: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(web_app, "CONFIG_PATH", str(one_repo_config))

    async def fake_publish(
        repo_name: str,
        event_type: str,
        payload: dict[str, str],
        redis_client: object | None = None,
    ) -> None:
        return None

    with TestClient(app) as client:
        client.app.state.redis = _FakeRedis()
        monkeypatch.setattr(web_app, "publish_repo_event", fake_publish)

        missing = client.post("/repos/example__ghost/coder", data={"coder": "codex"})
        assert missing.status_code == 404
        assert "Repository not found" in missing.text

        invalid = client.post(
            "/repos/example__alpha/coder",
            data={"coder": "other"},
        )
        assert invalid.status_code == 422
        assert "coder must be one of: any, claude, codex" in invalid.text

        cleared = client.post("/repos/example__alpha/coder", data={"coder": "any"})
        assert cleared.status_code == 200

    cfg = load_config(str(one_repo_config))
    assert cfg.repositories[0].coder is None

    monkeypatch.setattr(src_config, "save_config", _raise_permission_error)
    with TestClient(app) as client:
        client.app.state.redis = _FakeRedis()
        monkeypatch.setattr(web_app, "publish_repo_event", fake_publish)
        write_error = client.post(
            "/repos/example__alpha/coder",
            data={"coder": "codex"},
        )

    assert write_error.status_code == 503
    assert "Failed to write config.yml" in write_error.text


def test_post_repo_detail_coder_requires_redis(
    one_repo_config: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(web_app, "CONFIG_PATH", str(one_repo_config))

    with TestClient(app) as client:
        client.app.state.redis = None
        response = client.post(
            "/repos/example__alpha/coder",
            data={"coder": "codex"},
        )

    assert response.status_code == 503
    assert "Redis unavailable" in response.text


def test_post_repo_detail_coder_returns_success_when_state_update_fails(
    one_repo_config: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(web_app, "CONFIG_PATH", str(one_repo_config))

    class _BoomRedis(_FakeRedis):
        async def set(self, key: str, value: str, **_kwargs: object) -> None:
            raise RuntimeError("boom")

    async def fake_publish(
        repo_name: str,
        event_type: str,
        payload: dict[str, str],
        redis_client: object | None = None,
    ) -> None:
        return None

    with TestClient(app) as client:
        client.app.state.redis = _BoomRedis()
        monkeypatch.setattr(web_app, "publish_repo_event", fake_publish)
        response = client.post(
            "/repos/example__alpha/coder",
            data={"coder": "codex"},
        )

    assert response.status_code == 200
    assert "Switching to Codex CLI." in response.text
    reloaded = load_config(str(one_repo_config))
    assert reloaded.repositories[0].coder == "codex"


def test_put_repo_detail_coder_still_updates_summary_fragment(
    one_repo_config: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(web_app, "CONFIG_PATH", str(one_repo_config))

    with TestClient(app) as client:
        client.app.state.redis = _FakeRedis()
        response = client.put(
            "/settings/repo/example__alpha",
            data={"coder": "codex"},
        )

    assert response.status_code == 200
    assert 'data-coder-display' in response.text
    assert 'hx-post="/repos/example__alpha/coder"' not in response.text
    cfg = load_config(str(one_repo_config))
    assert cfg.repositories[0].coder is not None
    assert cfg.repositories[0].coder.value == "codex"


def test_put_repo_detail_coder_handles_missing_invalid_clear_and_write_error(
    one_repo_config: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(web_app, "CONFIG_PATH", str(one_repo_config))

    with TestClient(app) as client:
        client.app.state.redis = _FakeRedis()

        missing = client.put("/settings/repo/example__ghost", data={"coder": "codex"})
        assert missing.status_code == 404
        assert "Repository not found" in missing.text

        invalid = client.put(
            "/settings/repo/example__alpha",
            data={"coder": "other"},
        )
        assert invalid.status_code == 422
        assert "coder must be 'claude', 'codex', or empty" in invalid.text

        cleared = client.put("/settings/repo/example__alpha", data={"coder": ""})
        assert cleared.status_code == 200

    cfg = load_config(str(one_repo_config))
    assert cfg.repositories[0].coder is None

    monkeypatch.setattr(src_config, "save_config", _raise_permission_error)
    with TestClient(app) as client:
        client.app.state.redis = _FakeRedis()
        write_error = client.put(
            "/settings/repo/example__alpha",
            data={"coder": "codex"},
        )

    assert write_error.status_code == 503
    assert "Failed to write config.yml" in write_error.text


def test_settings_helpers_resolve_via_module_getattr() -> None:
    """Settings helpers stay importable as ``web_app.X`` after the PR-225b split.

    Tests historically reached for ``web_app._coerce_int`` and friends; the
    helpers now live in ``src.web.routes.settings`` and are re-exported
    through :func:`src.web.app.__getattr__`. Probing one entry per
    ``_SETTINGS_REEXPORTS`` set keeps that proxy branch exercised.
    """
    assert web_app._coerce_int("3", "field", min_value=1) == 3
    assert callable(web_app._render_settings_repo_list)
