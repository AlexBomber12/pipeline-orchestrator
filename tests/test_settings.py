"""Tests for the /settings page in src/web/app.py."""

from __future__ import annotations

from pathlib import Path

import pytest
from fastapi.testclient import TestClient

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
        response = client.delete("/settings/repos/alpha")

    assert response.status_code == 200
    cfg = load_config(str(one_repo_config))
    assert cfg.repositories == []


def test_delete_nonexistent_repo_returns_404(empty_config: Path) -> None:
    with TestClient(app) as client:
        response = client.delete("/settings/repos/ghost")

    assert response.status_code == 404
    assert "not found" in response.text.lower()


def test_put_repo_updates_branch(one_repo_config: Path) -> None:
    with TestClient(app) as client:
        response = client.put(
            "/settings/repos/alpha",
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
            "/settings/repos/alpha",
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


def test_put_nonexistent_repo_returns_404(empty_config: Path) -> None:
    with TestClient(app) as client:
        response = client.put(
            "/settings/repos/ghost",
            data={"branch": "develop"},
        )

    assert response.status_code == 404
    assert "not found" in response.text.lower()


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
