"""Tests for the upload-tasks endpoint."""

from __future__ import annotations

from pathlib import Path

import pytest
from fastapi.testclient import TestClient

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
def one_repo_config(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    cfg = tmp_path / "config.yml"
    cfg.write_text(
        "repositories:\n"
        "  - url: https://github.com/example/alpha.git\n",
        encoding="utf-8",
    )
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(web_app, "aioredis", _StubAioredis())
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
    # Point REPOS_DIR to a dir that has no "alpha" sub-dir
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


def test_upload_without_queue_md(
    one_repo_config: Path,
    repo_dir: Path,
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
) -> None:
    with TestClient(app) as client:
        resp = client.post(
            "/repos/alpha/upload-tasks",
            files=[_queue_file(), _bad_file()],
        )
    assert resp.status_code == 422
    assert "Invalid file name" in resp.text


def test_upload_success_calls_git(
    one_repo_config: Path,
    repo_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    git_calls: list[tuple[str, ...]] = []

    async def fake_git_run(repo_path: str, *args: str, timeout: int = 30) -> None:
        git_calls.append(args)

    monkeypatch.setattr("src.web.app._git_run", fake_git_run)

    with TestClient(app) as client:
        resp = client.post(
            "/repos/alpha/upload-tasks",
            files=[_queue_file(), _pr_file()],
        )

    assert resp.status_code == 200
    assert git_calls[0][0] == "checkout"
    assert git_calls[1][0] == "pull"
    assert git_calls[2][0] == "add"
    assert git_calls[3][0] == "commit"
    assert git_calls[4][0] == "push"


def test_upload_writes_files_to_tasks_dir(
    one_repo_config: Path,
    repo_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def _noop(*a, **kw): pass
    monkeypatch.setattr("src.web.app._git_run", _noop)

    with TestClient(app) as client:
        resp = client.post(
            "/repos/alpha/upload-tasks",
            files=[_queue_file(), _pr_file("PR-001.md")],
        )

    assert resp.status_code == 200
    tasks_dir = repo_dir / "tasks"
    assert (tasks_dir / "QUEUE.md").exists()
    assert (tasks_dir / "PR-001.md").exists()
    assert (tasks_dir / "QUEUE.md").read_text() == "# Task Queue\n"
