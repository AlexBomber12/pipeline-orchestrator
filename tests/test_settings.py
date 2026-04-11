"""Tests for the /settings page in src/web/app.py."""

from __future__ import annotations

from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from src import config as src_config
from src.config import load_config
from src.web import app as web_app
from src.web.app import app


class _StubAioredisClient:
    async def get(self, key: str) -> str | None:
        return None

    async def aclose(self) -> None:
        return None


class _StubAioredis:
    @staticmethod
    def from_url(url: str, decode_responses: bool = True) -> _StubAioredisClient:
        return _StubAioredisClient()


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
        "    review_timeout_min: 60\n"
        "    poll_interval_sec: 60\n",
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
    assert cfg.repositories[0].poll_interval_sec == 60


def test_put_repo_updates_multiple_fields(one_repo_config: Path) -> None:
    with TestClient(app) as client:
        response = client.put(
            "/settings/repos",
            params={"url": "https://github.com/example/alpha.git"},
            data={
                "auto_merge": "false",
                "review_timeout_min": "120",
                "poll_interval_sec": "30",
            },
        )

    assert response.status_code == 200
    cfg = load_config(str(one_repo_config))
    assert cfg.repositories[0].auto_merge is False
    assert cfg.repositories[0].review_timeout_min == 120
    assert cfg.repositories[0].poll_interval_sec == 30
    # Branch untouched.
    assert cfg.repositories[0].branch == "main"


def test_put_repo_empty_numeric_inputs_are_no_ops(
    one_repo_config: Path,
) -> None:
    """Cleared number inputs (``review_timeout_min=``, ``poll_interval_sec=``)
    must not trip FastAPI's request parser.

    Regression for a P1 bug where declaring the fields as
    ``int | None = Form(None)`` caused FastAPI to reject the request during
    parsing with a raw JSON 422; combined with ``htmx:beforeSwap`` forcing
    422 swaps, HTMX would replace ``#settings-repo-list`` with the JSON
    payload and wedge the UI until a full reload.
    """
    with TestClient(app) as client:
        response = client.put(
            "/settings/repos",
            params={"url": "https://github.com/example/alpha.git"},
            data={
                "branch": "develop",
                "review_timeout_min": "",
                "poll_interval_sec": "",
            },
        )

    assert response.status_code == 200
    assert "text/html" in response.headers["content-type"]
    cfg = load_config(str(one_repo_config))
    # Branch updated, numerics untouched by empty submissions.
    assert cfg.repositories[0].branch == "develop"
    assert cfg.repositories[0].review_timeout_min == 60
    assert cfg.repositories[0].poll_interval_sec == 60


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


def test_put_repo_non_positive_poll_interval_returns_422(
    one_repo_config: Path,
) -> None:
    """``poll_interval_sec`` must also stay >= 1 server-side."""
    with TestClient(app) as client:
        for bad in ("0", "-30"):
            response = client.put(
                "/settings/repos",
                params={"url": "https://github.com/example/alpha.git"},
                data={"poll_interval_sec": bad},
            )
            assert response.status_code == 422, bad
            assert "text/html" in response.headers["content-type"]
            assert "poll_interval_sec" in response.text
            assert "at least 1" in response.text

    cfg = load_config(str(one_repo_config))
    assert cfg.repositories[0].poll_interval_sec == 60


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
