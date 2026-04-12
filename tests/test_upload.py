"""Tests for the upload-tasks endpoint."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from src.web import app as web_app
from src.web.app import app


class _StubAioredisClient:
    def __init__(self) -> None:
        self._store: dict[str, str] = {}

    async def get(self, key: str) -> str | None:
        if key in self._store:
            return self._store[key]
        if key.startswith("pipeline:"):
            name = key.split(":", 1)[1]
            return f'{{"url":"","name":"{name}","state":"IDLE"}}'
        return None

    async def set(self, key: str, value: str, **kwargs: object) -> None:
        self._store[key] = value

    async def aclose(self) -> None:
        return None


class _StubAioredis:
    def __init__(self) -> None:
        self.client = _StubAioredisClient()

    def from_url(self, url: str, decode_responses: bool = True) -> _StubAioredisClient:
        return self.client


@pytest.fixture
def one_repo_config(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    cfg = tmp_path / "config.yml"
    cfg.write_text(
        "repositories:\n"
        "  - url: https://github.com/example/alpha.git\n",
        encoding="utf-8",
    )
    monkeypatch.chdir(tmp_path)
    stub = _StubAioredis()
    monkeypatch.setattr(web_app, "aioredis", stub)
    return cfg


@pytest.fixture
def repo_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Create a fake repo dir and point REPOS_DIR at it."""
    repos = tmp_path / "repos"
    repos.mkdir()
    alpha = repos / "alpha"
    alpha.mkdir()
    monkeypatch.setattr(web_app, "REPOS_DIR", str(repos))
    return alpha


@pytest.fixture
def uploads_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Create and point UPLOADS_DIR at a temp directory."""
    uploads = tmp_path / "uploads"
    uploads.mkdir()
    monkeypatch.setattr(web_app, "UPLOADS_DIR", str(uploads))
    return uploads


def _queue_file() -> tuple[str, tuple[str, bytes, str]]:
    return ("files", ("QUEUE.md", b"# Task Queue\n", "text/markdown"))


def _pr_file(name: str = "PR-001.md") -> tuple[str, tuple[str, bytes, str]]:
    return ("files", (name, b"# PR\n", "text/markdown"))


def _bad_file() -> tuple[str, tuple[str, bytes, str]]:
    return ("files", ("README.md", b"# Readme\n", "text/markdown"))


def test_upload_nonexistent_repo(
    one_repo_config: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    with TestClient(app) as client:
        resp = client.post(
            "/repos/ghost/upload-tasks",
            files=[_queue_file()],
        )
    assert resp.status_code == 404
    assert "not found" in resp.text


def test_upload_repo_not_cloned(
    one_repo_config: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    empty_repos = tmp_path / "empty_repos"
    empty_repos.mkdir()
    monkeypatch.setattr(web_app, "REPOS_DIR", str(empty_repos))

    with TestClient(app) as client:
        resp = client.post(
            "/repos/alpha/upload-tasks",
            files=[_queue_file()],
        )
    assert resp.status_code == 422
    assert "not cloned" in resp.text


def test_upload_blocked_when_redis_key_absent(
    one_repo_config: Path,
    repo_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _EmptyRedis:
        async def get(self, key: str) -> str | None:
            return None
        async def set(self, key: str, value: str, **kwargs: object) -> None:
            pass
        async def aclose(self) -> None:
            return None

    monkeypatch.setattr(web_app, "aioredis", type("R", (), {
        "from_url": staticmethod(lambda *a, **kw: _EmptyRedis()),
    })())

    with TestClient(app) as client:
        resp = client.post(
            "/repos/alpha/upload-tasks",
            files=[_queue_file()],
        )
    assert resp.status_code == 503
    assert "no state recorded" in resp.text


def test_upload_without_queue_md(
    one_repo_config: Path,
    repo_dir: Path,
    uploads_dir: Path,
) -> None:
    with TestClient(app) as client:
        resp = client.post(
            "/repos/alpha/upload-tasks",
            files=[_pr_file()],
        )
    assert resp.status_code == 422
    assert "QUEUE.md is required" in resp.text


def test_upload_invalid_filename(
    one_repo_config: Path,
    repo_dir: Path,
    uploads_dir: Path,
) -> None:
    with TestClient(app) as client:
        resp = client.post(
            "/repos/alpha/upload-tasks",
            files=[_queue_file(), _bad_file()],
        )
    assert resp.status_code == 422
    assert "Invalid file name" in resp.text


def test_upload_stages_files_and_sets_redis_key(
    one_repo_config: Path,
    repo_dir: Path,
    uploads_dir: Path,
) -> None:
    with TestClient(app) as client:
        resp = client.post(
            "/repos/alpha/upload-tasks",
            files=[_queue_file(), _pr_file()],
        )

    assert resp.status_code == 200
    assert "queued" in resp.text.lower()

    staging = uploads_dir / "alpha"
    assert (staging / "QUEUE.md").exists()
    assert (staging / "PR-001.md").exists()
    assert (staging / "QUEUE.md").read_bytes() == b"# Task Queue\n"


def test_upload_writes_redis_manifest(
    one_repo_config: Path,
    repo_dir: Path,
    uploads_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    redis_sets: dict[str, str] = {}

    class _TrackingRedis:
        async def get(self, key: str) -> str | None:
            if key.startswith("pipeline:"):
                name = key.split(":", 1)[1]
                return f'{{"url":"","name":"{name}","state":"IDLE"}}'
            return None
        async def set(self, key: str, value: str, **kwargs: object) -> None:
            redis_sets[key] = value
        async def aclose(self) -> None:
            return None

    monkeypatch.setattr(web_app, "aioredis", type("R", (), {
        "from_url": staticmethod(lambda *a, **kw: _TrackingRedis()),
    })())

    with TestClient(app) as client:
        resp = client.post(
            "/repos/alpha/upload-tasks",
            files=[_queue_file(), _pr_file("PR-002.md")],
        )

    assert resp.status_code == 200
    assert "upload:alpha:pending" in redis_sets
    manifest = json.loads(redis_sets["upload:alpha:pending"])
    assert manifest["repo"] == "alpha"
    assert set(manifest["files"]) == {"QUEUE.md", "PR-002.md"}
