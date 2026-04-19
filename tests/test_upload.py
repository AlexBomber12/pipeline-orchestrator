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

    async def ping(self) -> bool:
        return True

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
    alpha = repos / "example__alpha"
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


def _task_file(
    name: str = "PR-001.md",
    *,
    pr_id: str = "PR-001",
    depends_on: str | None = "none",
) -> tuple[str, tuple[str, bytes, str]]:
    header = [
        f"# {pr_id}: Example task",
        "",
        f"Branch: {pr_id.lower()}-example-task",
        "- Type: feature",
        "- Complexity: low",
    ]
    if depends_on is not None:
        header.append(f"- Depends on: {depends_on}")
    header.extend(["- Priority: 1", "- Coder: any", ""])
    return ("files", (name, "\n".join(header).encode("utf-8"), "text/markdown"))


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
            "/repos/example__alpha/upload-tasks",
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
            "/repos/example__alpha/upload-tasks",
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
            "/repos/example__alpha/upload-tasks",
            files=[_task_file()],
        )
    assert resp.status_code == 200
    assert "queued" in resp.text.lower()

    repo_upload_dir = uploads_dir / "example__alpha"
    subdirs = list(repo_upload_dir.iterdir())
    assert len(subdirs) == 1
    staging = subdirs[0]
    assert not (staging / "QUEUE.md").exists()
    assert (staging / "PR-001.md").exists()


def test_upload_rejects_malformed_queue_md(
    one_repo_config: Path,
    repo_dir: Path,
    uploads_dir: Path,
) -> None:
    malformed_queue = (
        "files",
        (
            "QUEUE.md",
            b"## PR-001: First\n- Status: TODO\n- Tasks file: PR-001.md\n- Branch: same\n"
            b"## PR-001: Duplicate\n- Status: TODO\n- Tasks file: PR-002.md\n- Branch: same\n",
            "text/markdown",
        ),
    )

    with TestClient(app) as client:
        resp = client.post(
            "/repos/example__alpha/upload-tasks",
            files=[malformed_queue, _task_file()],
        )

    assert resp.status_code == 400
    assert "QUEUE.md validation failed" in resp.text


def test_upload_invalid_filename(
    one_repo_config: Path,
    repo_dir: Path,
    uploads_dir: Path,
) -> None:
    with TestClient(app) as client:
        resp = client.post(
            "/repos/example__alpha/upload-tasks",
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
            "/repos/example__alpha/upload-tasks",
            files=[_queue_file(), _task_file()],
        )

    assert resp.status_code == 200
    assert "queued" in resp.text.lower()

    repo_upload_dir = uploads_dir / "example__alpha"
    subdirs = list(repo_upload_dir.iterdir())
    assert len(subdirs) == 1
    staging = subdirs[0]
    assert (staging / "QUEUE.md").exists()
    assert (staging / "PR-001.md").exists()
    assert (staging / "QUEUE.md").read_bytes() == b"# Task Queue\n"


def test_upload_rejects_task_without_depends_on(
    one_repo_config: Path,
    repo_dir: Path,
    uploads_dir: Path,
) -> None:
    with TestClient(app) as client:
        resp = client.post(
            "/repos/example__alpha/upload-tasks",
            files=[_queue_file(), _task_file(depends_on=None)],
        )

    assert resp.status_code == 400
    assert "Task file missing required field: Depends on." in resp.text
    assert "Depends on: none" in resp.text


def test_upload_rejects_non_numeric_task_without_depends_on(
    one_repo_config: Path,
    repo_dir: Path,
    uploads_dir: Path,
) -> None:
    with TestClient(app) as client:
        resp = client.post(
            "/repos/example__alpha/upload-tasks",
            files=[
                _queue_file(),
                _task_file(name="PR-ABC.md", pr_id="PR-ABC", depends_on=None),
            ],
        )

    assert resp.status_code == 400
    assert "Task file missing required field: Depends on." in resp.text
    assert "Depends on: none" in resp.text


def test_upload_validates_only_last_duplicate_task_copy(
    one_repo_config: Path,
    repo_dir: Path,
    uploads_dir: Path,
) -> None:
    with TestClient(app) as client:
        resp = client.post(
            "/repos/example__alpha/upload-tasks",
            files=[
                _queue_file(),
                _task_file(depends_on=None),
                _task_file(depends_on="none"),
            ],
        )

    assert resp.status_code == 200


def test_upload_task_validation_errors_reference_uploaded_filename(
    one_repo_config: Path,
    repo_dir: Path,
    uploads_dir: Path,
) -> None:
    with TestClient(app) as client:
        resp = client.post(
            "/repos/example__alpha/upload-tasks",
            files=[
                _queue_file(),
                _task_file(name="PR-ABC.md", pr_id="PR-ABC", depends_on="PR-001, nope"),
            ],
        )

    assert resp.status_code == 400
    assert "PR-ABC.md: invalid Depends on" in resp.text
    assert "/tmp/" not in resp.text


def test_upload_accepts_task_with_depends_on_none(
    one_repo_config: Path,
    repo_dir: Path,
    uploads_dir: Path,
) -> None:
    with TestClient(app) as client:
        resp = client.post(
            "/repos/example__alpha/upload-tasks",
            files=[_queue_file(), _task_file(depends_on="none")],
        )

    assert resp.status_code == 200


def test_upload_accepts_task_with_dependencies(
    one_repo_config: Path,
    repo_dir: Path,
    uploads_dir: Path,
) -> None:
    with TestClient(app) as client:
        resp = client.post(
            "/repos/example__alpha/upload-tasks",
            files=[_queue_file(), _task_file(depends_on="PR-001, PR-002")],
        )

    assert resp.status_code == 200


def test_upload_allows_non_task_files_without_validation(
    one_repo_config: Path,
    repo_dir: Path,
    uploads_dir: Path,
) -> None:
    queue = ("files", ("QUEUE.md", b"# Task Queue\n", "text/markdown"))
    agents = ("files", ("AGENTS.md", b"# AGENTS\n", "text/markdown"))

    with TestClient(app) as client:
        resp = client.post(
            "/repos/example__alpha/upload-tasks",
            files=[queue, agents],
        )

    assert resp.status_code == 200


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
            "/repos/example__alpha/upload-tasks",
            files=[_queue_file(), _task_file(name="PR-002.md")],
        )

    assert resp.status_code == 200
    assert "upload:example__alpha:pending" in redis_sets
    manifest = json.loads(redis_sets["upload:example__alpha:pending"])
    assert manifest["repo"] == "example__alpha"
    assert set(manifest["files"]) == {"QUEUE.md", "PR-002.md"}
    assert "staging_dir" in manifest
    assert "/example__alpha/" in manifest["staging_dir"]


# ---------------------------------------------------------------------------
# Cleanup guard tests
# ---------------------------------------------------------------------------


def test_upload_cleans_staging_on_redis_failure(
    one_repo_config: Path,
    repo_dir: Path,
    uploads_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Staging directory is removed when Redis set raises."""

    class _FailingRedis:
        async def get(self, key: str) -> str | None:
            if key.startswith("pipeline:"):
                name = key.split(":", 1)[1]
                return f'{{"url":"","name":"{name}","state":"IDLE"}}'
            return None

        async def set(self, key: str, value: str, **kw: object) -> None:
            raise RuntimeError("Redis unavailable")

        async def aclose(self) -> None:
            return None

    monkeypatch.setattr(
        web_app,
        "aioredis",
        type("R", (), {"from_url": staticmethod(lambda *a, **kw: _FailingRedis())})(),
    )

    with TestClient(app) as client:
        resp = client.post(
            "/repos/example__alpha/upload-tasks",
            files=[_queue_file()],
        )

    assert resp.status_code == 503
    assert "Redis error" in resp.text

    # The staging directory should have been cleaned up.
    repo_upload = uploads_dir / "example__alpha"
    if repo_upload.exists():
        assert list(repo_upload.iterdir()) == []


def test_upload_preserves_staging_on_success(
    one_repo_config: Path,
    repo_dir: Path,
    uploads_dir: Path,
) -> None:
    """Staging directory remains when upload succeeds."""
    with TestClient(app) as client:
        resp = client.post(
            "/repos/example__alpha/upload-tasks",
            files=[_queue_file()],
        )

    assert resp.status_code == 200
    repo_upload = uploads_dir / "example__alpha"
    subdirs = list(repo_upload.iterdir())
    assert len(subdirs) >= 1
    assert (subdirs[0] / "QUEUE.md").exists()


# ---------------------------------------------------------------------------
# Sweep tests
# ---------------------------------------------------------------------------

import time  # noqa: E402

from src.web.app import sweep_abandoned_staging  # noqa: E402


def test_sweep_removes_old_orphans(tmp_path: Path) -> None:
    """Directories older than max_age_hours with no Redis key are removed."""
    uploads = tmp_path / "uploads"
    repo_dir_s = uploads / "myrepo"
    old = repo_dir_s / "old-submission"
    old.mkdir(parents=True)
    (old / "QUEUE.md").write_text("# Queue\n")
    # Set mtime to 48 hours ago.
    old_time = time.time() - 48 * 3600
    import os  # noqa: E402

    os.utime(old, (old_time, old_time))

    removed = sweep_abandoned_staging(str(uploads), set(), max_age_hours=24)
    assert removed == 1
    assert not old.exists()


def test_sweep_preserves_young_directories(tmp_path: Path) -> None:
    """Directories newer than max_age_hours are preserved."""
    uploads = tmp_path / "uploads"
    repo_dir_s = uploads / "myrepo"
    young = repo_dir_s / "young-submission"
    young.mkdir(parents=True)
    (young / "QUEUE.md").write_text("# Queue\n")

    removed = sweep_abandoned_staging(str(uploads), set(), max_age_hours=24)
    assert removed == 0
    assert young.exists()


def test_sweep_preserves_keyed_directories(tmp_path: Path) -> None:
    """Directories matching an active Redis key are preserved even if old."""
    uploads = tmp_path / "uploads"
    repo_dir_s = uploads / "myrepo"
    old = repo_dir_s / "active-submission"
    old.mkdir(parents=True)
    (old / "QUEUE.md").write_text("# Queue\n")
    old_time = time.time() - 48 * 3600
    import os as _os  # noqa: E402

    _os.utime(old, (old_time, old_time))

    removed = sweep_abandoned_staging(
        str(uploads), {str(old)}, max_age_hours=24
    )
    assert removed == 0
    assert old.exists()
