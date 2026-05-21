"""Atomic upload validation coverage for the dashboard route."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from src.keyspace import upload_pending
from src.web import app as web_app
from src.web.app import app
from src.web.routes import uploads as upload_routes

from tests.test_upload import (
    _post_upload,
    _StubAioredis,
    _task_bytes,
    _task_file,
    _zip_file,
)
from tests.web.routes.test_upload_preserve import _task_text

pytestmark = pytest.mark.usefixtures("one_repo_config", "repo_dir", "uploads_dir")


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
    repos = tmp_path / "repos"
    repos.mkdir()
    alpha = repos / "example__alpha"
    alpha.mkdir()
    monkeypatch.setattr(web_app, "REPOS_DIR", str(repos))
    return alpha


@pytest.fixture
def uploads_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    uploads = tmp_path / "uploads"
    uploads.mkdir()
    monkeypatch.setattr(web_app, "UPLOADS_DIR", str(uploads))
    return uploads


def _staging_dir(uploads_dir: Path) -> Path:
    return next((uploads_dir / "example__alpha").iterdir())


def _upload_temp_dirs() -> list[Path]:
    return list(Path("/tmp").glob("upload-*"))


def test_all_valid_commits_all(uploads_dir: Path) -> None:
    resp = _post_upload(
        [
            _task_file(name="PR-001.md", pr_id="PR-001"),
            _task_file(name="PR-002.md", pr_id="PR-002"),
            _task_file(name="PR-003.md", pr_id="PR-003"),
        ]
    )

    assert resp.status_code == 200
    assert "Accepted 3 task files" in resp.text
    assert {path.name for path in _staging_dir(uploads_dir).iterdir()} == {
        "PR-001.md",
        "PR-002.md",
        "PR-003.md",
    }


def test_one_invalid_rejects_all(uploads_dir: Path) -> None:
    resp = _post_upload(
        [
            _task_file(name="PR-001.md", pr_id="PR-001"),
            _task_file(name="PR-002.md", pr_id="PR-002"),
            _task_file(name="PR-003.md", pr_id="PR-003", task_type="badtype"),
        ]
    )

    assert resp.status_code == 400
    assert "validation failed" in resp.text
    assert "PR-003.md" in resp.text
    assert not (uploads_dir / "example__alpha").exists()


def test_dependency_closure_in_batch(uploads_dir: Path) -> None:
    resp = _post_upload(
        [
            _task_file(name="PR-001.md", pr_id="PR-001"),
            _task_file(name="PR-002.md", pr_id="PR-002", depends_on="PR-001"),
        ]
    )

    assert resp.status_code == 200
    assert {path.name for path in _staging_dir(uploads_dir).iterdir()} == {
        "PR-001.md",
        "PR-002.md",
    }


def test_dependency_closure_missing_in_batch_and_disk(uploads_dir: Path) -> None:
    resp = _post_upload(
        [_task_file(name="PR-002.md", pr_id="PR-002", depends_on="PR-999")]
    )

    assert resp.status_code == 400
    assert "Depends on PR-999 which is not in this upload and not in tasks/" in resp.text
    assert not (uploads_dir / "example__alpha").exists()


def test_dependency_closure_preserves_existing_file_errors(
    uploads_dir: Path,
) -> None:
    task_text = (
        _task_file(name="PR-002.md", pr_id="PR-002", depends_on="PR-999")[1][1]
        .decode("utf-8")
        + "\ngh pr create --draft\n"
    )

    resp = _post_upload(
        [("files", ("PR-002.md", task_text.encode("utf-8"), "text/markdown"))]
    )

    assert resp.status_code == 400
    assert "AGENTS.md anti-pattern" in resp.text
    assert "Depends on PR-999 which is not in this upload and not in tasks/" in resp.text
    assert not (uploads_dir / "example__alpha").exists()


def test_dependency_closure_satisfied_by_existing_tasks_dir(
    repo_dir: Path, uploads_dir: Path
) -> None:
    tasks_dir = repo_dir / "tasks"
    tasks_dir.mkdir()
    (tasks_dir / "PR-001.md").write_bytes(_task_bytes("PR-001.md", pr_id="PR-001"))

    resp = _post_upload(
        [_task_file(name="PR-002.md", pr_id="PR-002", depends_on="PR-001")]
    )

    assert resp.status_code == 200
    assert (_staging_dir(uploads_dir) / "PR-002.md").is_file()


def test_dependency_closure_uses_existing_task_header_pr_id(
    repo_dir: Path,
    uploads_dir: Path,
) -> None:
    tasks_dir = repo_dir / "tasks"
    tasks_dir.mkdir()
    (tasks_dir / "PR-001.md").write_bytes(
        _task_bytes("PR-001.md", pr_id="PR-999")
    )

    accepted = _post_upload(
        [_task_file(name="PR-002.md", pr_id="PR-002", depends_on="PR-999")]
    )

    assert accepted.status_code == 200
    assert (_staging_dir(uploads_dir) / "PR-002.md").is_file()


def test_dependency_closure_ignores_existing_filename_when_header_differs(
    repo_dir: Path,
    uploads_dir: Path,
) -> None:
    tasks_dir = repo_dir / "tasks"
    tasks_dir.mkdir()
    (tasks_dir / "PR-001.md").write_bytes(
        _task_bytes("PR-001.md", pr_id="PR-999")
    )

    rejected = _post_upload(
        [_task_file(name="PR-002.md", pr_id="PR-002", depends_on="PR-001")]
    )

    assert rejected.status_code == 400
    assert "Depends on PR-001 which is not in this upload and not in tasks/" in rejected.text
    assert not (uploads_dir / "example__alpha").exists()


def test_dependency_closure_accepts_legacy_existing_task_header(
    repo_dir: Path,
    uploads_dir: Path,
) -> None:
    tasks_dir = repo_dir / "tasks"
    tasks_dir.mkdir()
    (tasks_dir / "PR-001.md").write_text(
        "# PR-001: Legacy task\n\n"
        "Branch: pr-001-legacy-task\n",
        encoding="utf-8",
    )

    resp = _post_upload(
        [_task_file(name="PR-002.md", pr_id="PR-002", depends_on="PR-001")]
    )

    assert resp.status_code == 200
    assert (_staging_dir(uploads_dir) / "PR-002.md").is_file()


def test_parse_existing_task_header_returns_none_for_unreadable_path(
    tmp_path: Path,
) -> None:
    assert upload_routes._parse_existing_task_header(tmp_path) is None


def test_parse_existing_task_header_returns_none_when_legacy_read_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def _raise_validation_error(path: Path) -> None:
        raise upload_routes.QueueValidationError([f"{path}: missing Branch"])

    monkeypatch.setattr(upload_routes, "parse_task_header", _raise_validation_error)

    assert upload_routes._parse_existing_task_header(tmp_path) is None


def test_parse_existing_task_header_returns_none_for_interrupted_legacy_header(
    tmp_path: Path,
) -> None:
    task_file = tmp_path / "PR-001.md"
    task_file.write_text(
        "# PR-001: Legacy task\n\n"
        "- Type: feature\n"
        "Branch: pr-001-legacy-task\n",
        encoding="utf-8",
    )

    assert upload_routes._parse_existing_task_header(task_file) is None


def test_dependency_closure_satisfied_by_merged_on_github(
    monkeypatch: pytest.MonkeyPatch, uploads_dir: Path
) -> None:
    monkeypatch.setattr(upload_routes, "get_merged_pr_ids", lambda *args: {"PR-001"})

    resp = _post_upload(
        [_task_file(name="PR-002.md", pr_id="PR-002", depends_on="PR-001")]
    )

    assert resp.status_code == 200
    assert (_staging_dir(uploads_dir) / "PR-002.md").is_file()


def test_dependency_closure_satisfied_by_merged_split_child(
    monkeypatch: pytest.MonkeyPatch, uploads_dir: Path
) -> None:
    monkeypatch.setattr(upload_routes, "get_merged_pr_ids", lambda *args: {"PR-305a"})

    resp = _post_upload(
        [_task_file(name="PR-306.md", pr_id="PR-306", depends_on="PR-305")]
    )

    assert resp.status_code == 200
    assert (_staging_dir(uploads_dir) / "PR-306.md").is_file()


def test_upload_skips_merged_history_probe_without_dependencies(
    monkeypatch: pytest.MonkeyPatch, uploads_dir: Path
) -> None:
    def _fail_if_called(*args: object) -> set[str]:
        raise AssertionError("get_merged_pr_ids should not run without dependencies")

    monkeypatch.setattr(upload_routes, "get_merged_pr_ids", _fail_if_called)

    resp = _post_upload([_task_file(name="PR-001.md", pr_id="PR-001")])

    assert resp.status_code == 200
    assert (_staging_dir(uploads_dir) / "PR-001.md").is_file()


def test_dependency_closure_satisfied_by_pending_upload(
    uploads_dir: Path,
) -> None:
    pending_staging = uploads_dir / "example__alpha" / "pending"
    pending_staging.mkdir(parents=True)
    (pending_staging / "PR-001.md").write_bytes(
        _task_bytes("PR-001.md", pr_id="PR-001")
    )
    with TestClient(app) as client:
        client.app.state.redis._store[upload_pending("example__alpha")] = json.dumps(
            {
                "repo": "example__alpha",
                "files": ["PR-001.md"],
                "staging_dir": str(pending_staging),
            }
        )

        resp = client.post(
            "/repos/example__alpha/upload-tasks",
            files=[
                _task_file(name="PR-002.md", pr_id="PR-002", depends_on="PR-001")
            ],
        )
        manifest = json.loads(
            client.app.state.redis._store[upload_pending("example__alpha")]
        )

    assert resp.status_code == 200
    assert sorted(manifest["files"]) == ["PR-001.md", "PR-002.md"]
    new_staging = Path(manifest["staging_dir"])
    assert (new_staging / "PR-001.md").is_file()
    assert (new_staging / "PR-002.md").is_file()


def test_dependency_closure_rejects_missing_pending_manifest_file(
    uploads_dir: Path,
) -> None:
    pending_staging = uploads_dir / "example__alpha" / "pending"
    pending_staging.mkdir(parents=True)
    with TestClient(app) as client:
        client.app.state.redis._store[upload_pending("example__alpha")] = json.dumps(
            {
                "repo": "example__alpha",
                "files": ["PR-001.md"],
                "staging_dir": str(pending_staging),
            }
        )

        resp = client.post(
            "/repos/example__alpha/upload-tasks",
            files=[
                _task_file(name="PR-002.md", pr_id="PR-002", depends_on="PR-001")
            ],
        )

    assert resp.status_code == 400
    assert "Depends on PR-001 which is not in this upload and not in tasks/" in resp.text


def test_dependency_closure_rechecks_pending_manifest_inside_upload_lock(
    uploads_dir: Path,
) -> None:
    pending_staging = uploads_dir / "example__alpha" / "pending"
    pending_staging.mkdir(parents=True)
    (pending_staging / "PR-001.md").write_bytes(
        _task_bytes("PR-001.md", pr_id="PR-001")
    )
    pending_manifest = json.dumps(
        {
            "repo": "example__alpha",
            "files": ["PR-001.md"],
            "staging_dir": str(pending_staging),
        }
    )

    class _ChangingPendingRedis:
        def __init__(self) -> None:
            self.pending_reads = 0
            self.set_calls = 0

        async def get(self, key: str) -> str | None:
            if key == "pipeline:example__alpha":
                return '{"url":"","name":"example__alpha","state":"IDLE"}'
            if key == upload_pending("example__alpha"):
                self.pending_reads += 1
                if self.pending_reads == 1:
                    return pending_manifest
                return None
            return None

        async def set(self, key: str, value: str, **kwargs: object) -> None:
            self.set_calls += 1

        async def scan_iter(self, match: str | None = None):
            if False:
                yield ""

        async def aclose(self) -> None:
            return None

    with TestClient(app) as client:
        redis_client = _ChangingPendingRedis()
        client.app.state.redis = redis_client
        resp = client.post(
            "/repos/example__alpha/upload-tasks",
            files=[
                _task_file(name="PR-002.md", pr_id="PR-002", depends_on="PR-001")
            ],
        )

    assert resp.status_code == 400
    assert "Depends on PR-001 which is not in this upload and not in tasks/" in resp.text
    assert redis_client.set_calls == 0
    assert not any(
        path.name != "pending" for path in (uploads_dir / "example__alpha").iterdir()
    )


def test_dependency_closure_locked_recheck_refreshes_tasks_dir(
    repo_dir: Path,
    uploads_dir: Path,
) -> None:
    pending_staging = uploads_dir / "example__alpha" / "pending"
    pending_staging.mkdir(parents=True)
    (pending_staging / "PR-001.md").write_bytes(
        _task_bytes("PR-001.md", pr_id="PR-001")
    )
    pending_manifest = json.dumps(
        {
            "repo": "example__alpha",
            "files": ["PR-001.md"],
            "staging_dir": str(pending_staging),
        }
    )

    class _PendingMovesToTasksRedis:
        def __init__(self) -> None:
            self.pending_reads = 0
            self.manifest: str | None = None

        async def get(self, key: str) -> str | None:
            if key == "pipeline:example__alpha":
                return '{"url":"","name":"example__alpha","state":"IDLE"}'
            if key == upload_pending("example__alpha"):
                self.pending_reads += 1
                if self.pending_reads == 1:
                    return pending_manifest
                tasks_dir = repo_dir / "tasks"
                tasks_dir.mkdir(exist_ok=True)
                (tasks_dir / "PR-001.md").write_bytes(
                    _task_bytes("PR-001.md", pr_id="PR-001")
                )
                return None
            return None

        async def set(self, key: str, value: str, **kwargs: object) -> None:
            if key == upload_pending("example__alpha"):
                self.manifest = value

        async def scan_iter(self, match: str | None = None):
            if False:
                yield ""

        async def aclose(self) -> None:
            return None

    with TestClient(app) as client:
        redis_client = _PendingMovesToTasksRedis()
        client.app.state.redis = redis_client
        resp = client.post(
            "/repos/example__alpha/upload-tasks",
            files=[
                _task_file(name="PR-002.md", pr_id="PR-002", depends_on="PR-001")
            ],
        )

    assert resp.status_code == 200
    assert redis_client.manifest is not None
    manifest = json.loads(redis_client.manifest)
    assert manifest["files"] == ["PR-002.md"]


def test_pending_upload_task_ids_ignores_invalid_manifest(tmp_path: Path) -> None:
    staging_dir = tmp_path / "pending"
    staging_dir.mkdir()
    (staging_dir / "PR-001.md").write_bytes(
        _task_bytes("PR-001.md", pr_id="PR-999")
    )
    assert upload_routes._pending_upload_task_ids(None) == set()
    assert upload_routes._pending_upload_task_ids("{not-json") == set()
    assert upload_routes._pending_upload_task_ids('{"files": "PR-001.md"}') == set()
    assert upload_routes._pending_upload_task_ids(
        json.dumps(
            {
                "files": ["PR-001.md", "QUEUE.md", 123, "PR-002.md"],
                "staging_dir": str(staging_dir),
            }
        )
    ) == {"PR-999"}


def test_dependency_closure_continues_when_pending_lookup_fails(
    uploads_dir: Path,
) -> None:
    class _PendingExplodes:
        async def get(self, key: str) -> str | None:
            if key == upload_pending("example__alpha"):
                raise RuntimeError("boom")
            return '{"url":"","name":"example__alpha","state":"IDLE"}'

        async def set(self, key: str, value: str, **kwargs: object) -> None:
            return None

        async def scan_iter(self, match: str | None = None):
            if False:
                yield ""

        async def aclose(self) -> None:
            return None

    with TestClient(app) as client:
        client.app.state.redis = _PendingExplodes()
        resp = client.post(
            "/repos/example__alpha/upload-tasks",
            files=[
                _task_file(name="PR-001.md", pr_id="PR-001"),
                _task_file(name="PR-002.md", pr_id="PR-002", depends_on="PR-001"),
            ],
        )

    assert resp.status_code == 200


def test_error_response_lists_all_failures(uploads_dir: Path) -> None:
    resp = _post_upload(
        [
            _task_file(name="PR-001.md", pr_id="PR-001", task_type="bad1"),
            _task_file(name="PR-002.md", pr_id="PR-002", task_type="bad2"),
            _task_file(name="PR-003.md", pr_id="PR-003", task_type="bad3"),
        ]
    )

    assert resp.status_code == 400
    for filename in {"PR-001.md", "PR-002.md", "PR-003.md"}:
        assert filename in resp.text
    assert not (uploads_dir / "example__alpha").exists()


def test_anti_pattern_scan_rejects_batch(uploads_dir: Path) -> None:
    bad = _task_bytes().decode("utf-8") + "\ngh pr create --draft\n"

    resp = _post_upload(
        [
            ("files", ("PR-001.md", bad.encode("utf-8"), "text/markdown")),
            _task_file(name="PR-002.md", pr_id="PR-002"),
        ]
    )

    assert resp.status_code == 400
    assert "AGENTS.md anti-pattern" in resp.text
    assert not (uploads_dir / "example__alpha").exists()


def test_preserve_terminal_status_on_collision_applied(
    repo_dir: Path, uploads_dir: Path
) -> None:
    tasks_dir = repo_dir / "tasks"
    tasks_dir.mkdir()
    (tasks_dir / "PR-001.md").write_text(
        _task_text("PR-001", status="DONE"),
        encoding="utf-8",
    )

    resp = _post_upload(
        [
            (
                "files",
                (
                    "PR-001.md",
                    _task_text("PR-001", status="TODO").encode("utf-8"),
                    "text/markdown",
                ),
            )
        ]
    )

    assert resp.status_code == 200
    staged = (_staging_dir(uploads_dir) / "PR-001.md").read_text(encoding="utf-8")
    assert "status: DONE" in staged
    assert "status: TODO" not in staged


def test_temp_dir_cleaned_on_validation_failure(uploads_dir: Path) -> None:
    resp = _post_upload([_zip_file({"PR-001.md": _task_file(task_type="bad")[1][1]})])

    assert resp.status_code == 400
    assert _upload_temp_dirs() == []
    assert not (uploads_dir / "example__alpha").exists()


def test_temp_dir_cleaned_on_success(uploads_dir: Path) -> None:
    resp = _post_upload([_zip_file({"PR-001.md": _task_bytes()})])

    assert resp.status_code == 200
    assert _upload_temp_dirs() == []


def test_zip_with_non_md_files_rejected(uploads_dir: Path) -> None:
    resp = _post_upload([_zip_file({"note.txt": b"nope"})])

    assert resp.status_code == 422
    assert "Invalid file name" in resp.text
    assert not (uploads_dir / "example__alpha").exists()


def test_zip_with_path_traversal_rejected(uploads_dir: Path) -> None:
    resp = _post_upload([_zip_file({"../etc/passwd": b"nope"})])

    assert resp.status_code == 422
    assert "must not use absolute paths" in resp.text
    assert not (uploads_dir / "example__alpha").exists()


def test_commit_message_default() -> None:
    with TestClient(app) as client:
        resp = client.post(
            "/repos/example__alpha/upload-tasks",
            files=[
                _task_file(name="PR-001.md", pr_id="PR-001"),
                _task_file(name="PR-002.md", pr_id="PR-002"),
                _task_file(name="PR-003.md", pr_id="PR-003"),
            ],
        )
        manifest = json.loads(
            client.app.state.redis._store[upload_pending("example__alpha")]
        )

    assert resp.status_code == 200
    assert manifest["commit_subject"] == "tasks: upload batch (3 files)"


def test_commit_message_custom() -> None:
    with TestClient(app) as client:
        resp = client.post(
            "/repos/example__alpha/upload-tasks",
            data={"subject": "My batch"},
            files=[_task_file(name="PR-001.md", pr_id="PR-001")],
        )
        manifest = json.loads(
            client.app.state.redis._store[upload_pending("example__alpha")]
        )

    assert resp.status_code == 200
    assert manifest["commit_subject"] == "My batch"


def test_commit_message_rejects_queue_pr_prefix() -> None:
    with TestClient(app) as client:
        resp = client.post(
            "/repos/example__alpha/upload-tasks",
            data={"subject": "PR-305: upload batch"},
            files=[_task_file(name="PR-001.md", pr_id="PR-001")],
        )

    assert resp.status_code == 400
    assert "must not start with a queue PR ID prefix" in resp.text
