"""Tests for the upload-tasks endpoint."""

from __future__ import annotations

import io
import json
import zipfile
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
    task_type: str = "feature",
) -> tuple[str, tuple[str, bytes, str]]:
    header = [
        f"# {pr_id}: Example task",
        "",
        f"Branch: {pr_id.lower()}-example-task",
        f"- Type: {task_type}",
        "- Complexity: low",
    ]
    if depends_on is not None:
        header.append(f"- Depends on: {depends_on}")
    header.extend(["- Priority: 1", "- Coder: any", ""])
    return ("files", (name, "\n".join(header).encode("utf-8"), "text/markdown"))


def _zip_file(
    entries: dict[str, bytes],
    *,
    name: str = "tasks.zip",
) -> tuple[str, tuple[str, bytes, str]]:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        for entry_name, content in entries.items():
            archive.writestr(entry_name, content)
    return ("files", (name, buffer.getvalue(), "application/zip"))


def _task_bytes(name: str = "PR-001.md", *, pr_id: str = "PR-001") -> bytes:
    return _task_file(name, pr_id=pr_id)[1][1]


def _post_upload(files: list[tuple[str, tuple[str, bytes, str]]]):
    with TestClient(app) as client:
        return client.post("/repos/example__alpha/upload-tasks", files=files)


def _make_zip_error_test(
    files: list[tuple[str, tuple[str, bytes, str]]], status: int, text: str
):
    def _test(one_repo_config: Path, repo_dir: Path) -> None:
        assert (resp := _post_upload(files)).status_code == status and text in resp.text

    return _test


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


def test_upload_blocked_when_redis_is_unavailable(
    one_repo_config: Path,
    repo_dir: Path,
) -> None:
    with TestClient(app) as client:
        client.app.state.redis = None
        resp = client.post(
            "/repos/example__alpha/upload-tasks",
            files=[_queue_file()],
        )

    assert resp.status_code == 503
    assert "Redis unavailable" in resp.text


def test_upload_blocked_when_redis_get_fails(
    one_repo_config: Path,
    repo_dir: Path,
) -> None:
    class _ExplodingRedis:
        async def get(self, key: str) -> str | None:
            raise RuntimeError("boom")

        async def aclose(self) -> None:
            return None

    with TestClient(app) as client:
        client.app.state.redis = _ExplodingRedis()
        resp = client.post(
            "/repos/example__alpha/upload-tasks",
            files=[_queue_file()],
        )

    assert resp.status_code == 503
    assert "Redis error" in resp.text


def test_upload_blocked_when_repo_state_is_corrupt_or_not_idle(
    one_repo_config: Path,
    repo_dir: Path,
) -> None:
    class _CorruptRedis:
        async def get(self, key: str) -> str | None:
            return "{not-json"

        async def aclose(self) -> None:
            return None

    with TestClient(app) as client:
        client.app.state.redis = _CorruptRedis()
        corrupt = client.post(
            "/repos/example__alpha/upload-tasks",
            files=[_queue_file()],
        )
        assert corrupt.status_code == 503
        assert "corrupt data" in corrupt.text

        class _BusyRedis:
            async def get(self, key: str) -> str | None:
                return '{"url":"","name":"example__alpha","state":"CODING"}'

            async def aclose(self) -> None:
                return None

        client.app.state.redis = _BusyRedis()
        busy = client.post(
            "/repos/example__alpha/upload-tasks",
            files=[_queue_file()],
        )

    assert busy.status_code == 422
    assert "while repo is CODING" in busy.text


def test_upload_requires_files(
    one_repo_config: Path,
    repo_dir: Path,
) -> None:
    with TestClient(app) as client:
        resp = client.post("/repos/example__alpha/upload-tasks")

    assert resp.status_code == 422
    assert "No files uploaded" in resp.text


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
    assert "Accepted 1 task file (PR-001)." in resp.text
    assert "Daemon will commit and push after the next polling cycle." in resp.text
    assert "Auto-dismissing in 30 seconds." in resp.text

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


def test_upload_rejects_non_utf8_queue_and_task_files(
    one_repo_config: Path,
    repo_dir: Path,
) -> None:
    with TestClient(app) as client:
        queue_resp = client.post(
            "/repos/example__alpha/upload-tasks",
            files=[("files", ("QUEUE.md", b"\xff", "text/markdown"))],
        )
        assert queue_resp.status_code == 400
        assert "QUEUE.md is not valid UTF-8" in queue_resp.text

        task_resp = client.post(
            "/repos/example__alpha/upload-tasks",
            files=[_queue_file(), ("files", ("PR-001.md", b"\xff", "text/markdown"))],
        )

    assert task_resp.status_code == 400
    assert "PR-001.md is not valid UTF-8" in task_resp.text


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
    assert "Accepted 1 task file (PR-001)." in resp.text
    assert "Also uploaded helper file: QUEUE.md." in resp.text
    assert resp.headers["HX-Retarget"] == "#upload-feedback-example__alpha"
    assert "Dismiss upload feedback" in resp.text
    assert "::load" in resp.text

    repo_upload_dir = uploads_dir / "example__alpha"
    subdirs = list(repo_upload_dir.iterdir())
    assert len(subdirs) == 1
    staging = subdirs[0]
    assert (staging / "QUEUE.md").exists()
    assert (staging / "PR-001.md").exists()
    assert (staging / "QUEUE.md").read_bytes() == b"# Task Queue\n"


def test_upload_zip_with_pr_files_extracts_and_succeeds(
    one_repo_config: Path, repo_dir: Path, uploads_dir: Path
) -> None:
    resp = _post_upload([_zip_file({"PR-001.md": _task_bytes(), "PR-002.md": _task_bytes("PR-002.md", pr_id="PR-002")})])  # noqa: E501
    assert resp.status_code == 200
    assert "Accepted 2 task files (PR-001 through PR-002)." in resp.text
    staging = next((uploads_dir / "example__alpha").iterdir())
    assert {path.name for path in staging.iterdir()} == {"PR-001.md", "PR-002.md"}


def test_upload_zip_with_sparse_pr_files_lists_explicit_ids(
    one_repo_config: Path, repo_dir: Path, uploads_dir: Path
) -> None:
    resp = _post_upload(
        [
            _zip_file(
                {
                    "PR-124.md": _task_bytes("PR-124.md", pr_id="PR-124"),
                    "PR-141.md": _task_bytes("PR-141.md", pr_id="PR-141"),
                }
            )
        ]
    )
    assert resp.status_code == 200
    assert "Accepted 2 task files (PR-124, PR-141)." in resp.text


def test_upload_single_file_zip_success_message(
    one_repo_config: Path,
    repo_dir: Path,
    uploads_dir: Path,
) -> None:
    resp = _post_upload([_zip_file({"PR-001.md": _task_bytes()})])

    assert resp.status_code == 200
    assert "Accepted 1 task file (PR-001)." in resp.text


test_upload_zip_with_nested_directories_rejected = _make_zip_error_test([_zip_file({"nested/PR-001.md": _task_bytes()})], 422, "path separators")  # noqa: E501
test_upload_zip_with_non_md_entries_rejected = _make_zip_error_test([_zip_file({"README.md": b"# nope\n"})], 422, "Invalid file name")  # noqa: E501
test_upload_zip_corrupt_returns_400 = _make_zip_error_test([("files", ("broken.zip", b"not-a-zip", "application/zip"))], 400, "corrupt or unreadable")  # noqa: E501


def test_task_upload_summary_lists_non_numeric_ids_explicitly() -> None:
    assert web_app._task_upload_summary(["PR-ABC.md", "PR-XYZ.md"]) == "PR-ABC, PR-XYZ"


def test_upload_zip_entry_read_error_returns_400(
    one_repo_config: Path,
    repo_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    zip_upload = _zip_file({"PR-001.md": _task_bytes()})
    original_open = zipfile.ZipFile.open

    def _raising_open(self, name, mode="r", pwd=None, *, force_zip64=False):
        if mode == "r":
            raise RuntimeError("password required")
        return original_open(
            self, name, mode=mode, pwd=pwd, force_zip64=force_zip64
        )

    monkeypatch.setattr(zipfile.ZipFile, "open", _raising_open)
    resp = _post_upload([zip_upload])
    assert resp.status_code == 400
    assert "corrupt, encrypted, unsupported, or unreadable entries" in resp.text


def test_upload_zip_entry_decompression_error_returns_400(
    one_repo_config: Path,
    repo_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    zip_upload = _zip_file({"PR-001.md": _task_bytes()})

    def _raising_read(self, n=-1):
        raise EOFError("truncated payload")

    monkeypatch.setattr(zipfile.ZipExtFile, "read", _raising_read)
    resp = _post_upload([zip_upload])
    assert resp.status_code == 400
    assert "corrupt, encrypted, unsupported, or unreadable entries" in resp.text


def test_upload_zip_raw_size_enforced_before_parse(
    one_repo_config: Path,
    repo_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _UnexpectedZipFile:
        def __init__(self, *args, **kwargs) -> None:
            raise AssertionError("zip parser should not run for oversized uploads")

    monkeypatch.setattr(web_app, "_UPLOAD_MAX_TOTAL_BYTES", 200)
    monkeypatch.setattr(web_app.zipfile, "ZipFile", _UnexpectedZipFile)
    resp = _post_upload([("files", ("tasks.zip", b"x" * 250, "application/zip"))])
    assert resp.status_code == 422 and "Total upload size exceeds 1 MB" in resp.text


def test_upload_zip_total_raw_size_enforced_across_archives(
    one_repo_config: Path,
    repo_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    zip_one = _zip_file({"PR-001.md": b"a" * 20}, name="one.zip")
    zip_two = _zip_file({"PR-002.md": b"b" * 20}, name="two.zip")
    original_zipfile = web_app.zipfile.ZipFile
    calls = {"count": 0}

    class _CountingZipFile(original_zipfile):
        def __init__(self, *args, **kwargs) -> None:
            calls["count"] += 1
            super().__init__(*args, **kwargs)

    monkeypatch.setattr(
        web_app,
        "_UPLOAD_MAX_TOTAL_BYTES",
        len(zip_one[1][1]) + len(zip_two[1][1]) - 1,
    )
    monkeypatch.setattr(web_app.zipfile, "ZipFile", _CountingZipFile)
    resp = _post_upload([zip_one, zip_two])
    assert resp.status_code == 422
    assert "Total upload size exceeds 1 MB" in resp.text
    assert calls["count"] == 1


def test_upload_empty_zip_rejected(
    one_repo_config: Path,
    repo_dir: Path,
) -> None:
    resp = _post_upload([_zip_file({})])
    assert resp.status_code == 422
    assert "does not contain any task files" in resp.text


def test_upload_zip_directory_entries_are_skipped(
    one_repo_config: Path,
    repo_dir: Path,
    uploads_dir: Path,
) -> None:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr("folder/", b"")
        archive.writestr("PR-001.md", _task_bytes())

    resp = _post_upload(
        [("files", ("tasks.zip", buffer.getvalue(), "application/zip"))]
    )

    assert resp.status_code == 200
    staging = next((uploads_dir / "example__alpha").iterdir())
    assert {path.name for path in staging.iterdir()} == {"PR-001.md"}


def test_upload_zip_unicode_decode_error_returns_400(
    one_repo_config: Path,
    repo_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _BrokenZipFile:
        def __init__(self, *args, **kwargs) -> None:
            raise UnicodeDecodeError("utf-8", b"\xff", 0, 1, "invalid start byte")

    monkeypatch.setattr(web_app.zipfile, "ZipFile", _BrokenZipFile)
    resp = _post_upload([("files", ("broken.zip", b"zip-bytes", "application/zip"))])
    assert resp.status_code == 400
    assert "corrupt or unreadable" in resp.text


def test_upload_zip_total_extracted_size_enforced(
    one_repo_config: Path,
    repo_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    zip_upload = _zip_file({"PR-001.md": b"a" * 250})
    original_open = zipfile.ZipFile.open

    def _unexpected_open(self, name, mode="r", pwd=None, *, force_zip64=False):
        if mode == "r":
            raise AssertionError("zip entry should not be opened when size limit already fails")
        return original_open(
            self, name, mode=mode, pwd=pwd, force_zip64=force_zip64
        )

    monkeypatch.setattr(web_app, "_UPLOAD_MAX_TOTAL_BYTES", 200)
    monkeypatch.setattr(zipfile.ZipFile, "open", _unexpected_open)
    resp = _post_upload([zip_upload])
    assert resp.status_code == 422 and "Total upload size exceeds 1 MB" in resp.text


def test_upload_zip_largezipfile_returns_400(
    one_repo_config: Path,
    repo_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _HugeZipFile:
        def __init__(self, *args, **kwargs) -> None:
            raise zipfile.LargeZipFile("too large")

    monkeypatch.setattr(web_app.zipfile, "ZipFile", _HugeZipFile)
    resp = _post_upload([("files", ("huge.zip", b"zip-bytes", "application/zip"))])
    assert resp.status_code == 400
    assert "too large to extract" in resp.text


def test_upload_zip_staged_size_limit_from_metadata_and_streaming(
    one_repo_config: Path,
    repo_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    zip_upload = ("files", ("tasks.zip", b"zip-bytes", "application/zip"))

    class _FakeInfo:
        def __init__(self, filename: str, file_size: int) -> None:
            self.filename = filename
            self.file_size = file_size

        def is_dir(self) -> bool:
            return False

    class _ChunkyReader:
        def __init__(self, chunks: list[bytes]) -> None:
            self._chunks = list(chunks)

        def read(self, n: int = -1) -> bytes:
            return self._chunks.pop(0) if self._chunks else b""

        def __enter__(self) -> "_ChunkyReader":
            return self

        def __exit__(self, exc_type, exc, tb) -> bool:
            return False

    monkeypatch.setattr(web_app, "_UPLOAD_MAX_TOTAL_BYTES", 10)

    class _MetadataZipFile:
        def __init__(self, *args, **kwargs) -> None:
            pass

        def __enter__(self) -> "_MetadataZipFile":
            return self

        def __exit__(self, exc_type, exc, tb) -> bool:
            return False

        def infolist(self) -> list[_FakeInfo]:
            return [_FakeInfo("PR-001.md", 20)]

    monkeypatch.setattr(web_app.zipfile, "ZipFile", _MetadataZipFile)
    metadata_resp = _post_upload([zip_upload])
    assert metadata_resp.status_code == 422
    assert "Total upload size exceeds 1 MB" in metadata_resp.text

    monkeypatch.setattr(web_app, "_UPLOAD_MAX_TOTAL_BYTES", 15)

    class _StreamingZipFile(_MetadataZipFile):
        def infolist(self) -> list[_FakeInfo]:
            return [_FakeInfo("PR-001.md", 5)]

        def open(self, entry: _FakeInfo) -> _ChunkyReader:
            return _ChunkyReader([b"a" * 20, b""])

    monkeypatch.setattr(web_app.zipfile, "ZipFile", _StreamingZipFile)
    stream_resp = _post_upload([zip_upload])
    assert stream_resp.status_code == 422
    assert "Total upload size exceeds 1 MB" in stream_resp.text


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
    assert "Task file validation failed: PR-001.md: missing Depends on field." in resp.text
    assert "Depends on: none" in resp.text
    assert "after-settle" not in resp.text


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
    assert "Task file validation failed: PR-ABC.md: missing Depends on field." in resp.text
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
    assert "Task file validation failed:" in resp.text
    assert "PR-ABC.md: invalid Depends on" in resp.text
    assert "/tmp/" not in resp.text


def test_upload_surfaces_invalid_type_details(
    one_repo_config: Path,
    repo_dir: Path,
    uploads_dir: Path,
) -> None:
    with TestClient(app) as client:
        resp = client.post(
            "/repos/example__alpha/upload-tasks",
            files=[_queue_file(), _task_file(task_type="chore")],
        )

    assert resp.status_code == 400
    assert "Task file validation failed:" in resp.text
    assert "PR-001.md: invalid Type" in resp.text
    assert "chore" in resp.text
    assert "expected one of" in resp.text
    assert "Dismiss upload error" in resp.text


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


def test_upload_success_message_handles_non_numeric_pr_ids(
    one_repo_config: Path,
    repo_dir: Path,
    uploads_dir: Path,
) -> None:
    with TestClient(app) as client:
        resp = client.post(
            "/repos/example__alpha/upload-tasks",
            files=[
                _queue_file(),
                _task_file(name="PR-ABC.md", pr_id="PR-ABC", depends_on="none"),
            ],
        )

    assert resp.status_code == 200
    assert "Accepted 1 task file (PR-ABC)." in resp.text


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
    assert "Accepted 0 task files." in resp.text
    assert "Also uploaded helper files: AGENTS.md, QUEUE.md." in resp.text


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


def test_upload_large_plain_file_is_rejected(
    one_repo_config: Path,
    repo_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(web_app, "_UPLOAD_MAX_TOTAL_BYTES", 10)

    with TestClient(app) as client:
        resp = client.post(
            "/repos/example__alpha/upload-tasks",
            files=[("files", ("QUEUE.md", b"x" * 20, "text/markdown"))],
        )

    assert resp.status_code == 422
    assert "Total upload size exceeds 1 MB" in resp.text


def test_upload_merge_pending_manifest_and_ignore_scan_errors(
    one_repo_config: Path,
    repo_dir: Path,
    uploads_dir: Path,
) -> None:
    old_staging = uploads_dir / "example__alpha" / "existing"
    old_staging.mkdir(parents=True)
    (old_staging / "AGENTS.md").write_text("# AGENTS\n", encoding="utf-8")

    class _PendingRedis:
        def __init__(self) -> None:
            self.manifest: str | None = None

        async def get(self, key: str) -> str | None:
            if key == "pipeline:example__alpha":
                return '{"url":"","name":"example__alpha","state":"IDLE"}'
            if key == "upload:example__alpha:pending":
                return json.dumps(
                    {
                        "repo": "example__alpha",
                        "files": ["AGENTS.md"],
                        "staging_dir": str(old_staging),
                    }
                )
            if key == b"upload:active:pending":
                return json.dumps({"staging_dir": str(old_staging)})
            if key == b"upload:other:pending":
                raise RuntimeError("ignore sweep lookup failure")
            return None

        async def set(self, key: str, value: str, **kwargs: object) -> None:
            self.manifest = value

        async def scan_iter(self, match: str):
            yield b"upload:active:pending"
            yield b"upload:other:pending"

        async def aclose(self) -> None:
            return None

    with TestClient(app) as client:
        client.app.state.redis = _PendingRedis()
        resp = client.post(
            "/repos/example__alpha/upload-tasks",
            files=[_queue_file()],
        )
        redis_client = client.app.state.redis

    assert resp.status_code == 200
    assert redis_client.manifest is not None
    manifest = json.loads(redis_client.manifest)
    assert set(manifest["files"]) == {"QUEUE.md", "AGENTS.md"}
    assert (Path(manifest["staging_dir"]) / "AGENTS.md").read_text(
        encoding="utf-8"
    ) == "# AGENTS\n"
    assert "Accepted 0 task files." in resp.text
    assert "Also uploaded helper file: QUEUE.md." in resp.text
    assert "AGENTS.md" not in resp.text


def test_upload_ignores_pending_manifest_read_errors(
    one_repo_config: Path,
    repo_dir: Path,
) -> None:
    class _ManifestReadErrorRedis:
        async def get(self, key: str) -> str | None:
            if key == "pipeline:example__alpha":
                return '{"url":"","name":"example__alpha","state":"IDLE"}'
            if key == "upload:example__alpha:pending":
                raise RuntimeError("manifest read failed")
            return None

        async def set(self, key: str, value: str, **kwargs: object) -> None:
            return None

        async def scan_iter(self, match: str):
            if False:
                yield ""

        async def aclose(self) -> None:
            return None

    with TestClient(app) as client:
        client.app.state.redis = _ManifestReadErrorRedis()
        resp = client.post(
            "/repos/example__alpha/upload-tasks",
            files=[_queue_file()],
        )

    assert resp.status_code == 200


def test_upload_ignores_invalid_existing_manifest_payload(
    one_repo_config: Path,
    repo_dir: Path,
) -> None:
    class _BadExistingManifestRedis:
        async def get(self, key: str) -> str | None:
            if key == "pipeline:example__alpha":
                return '{"url":"","name":"example__alpha","state":"IDLE"}'
            if key == "upload:example__alpha:pending":
                return "{bad-json"
            return None

        async def set(self, key: str, value: str, **kwargs: object) -> None:
            return None

        async def scan_iter(self, match: str):
            if False:
                yield ""

        async def aclose(self) -> None:
            return None

    with TestClient(app) as client:
        client.app.state.redis = _BadExistingManifestRedis()
        resp = client.post(
            "/repos/example__alpha/upload-tasks",
            files=[_queue_file()],
        )

    assert resp.status_code == 200


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


def test_sweep_handles_missing_root_files_and_stat_errors(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    uploads = tmp_path / "uploads"
    assert sweep_abandoned_staging(str(uploads), set(), max_age_hours=24) == 0

    uploads.mkdir()
    (uploads / "README.txt").write_text("note", encoding="utf-8")
    repo_dir_s = uploads / "myrepo"
    repo_dir_s.mkdir()
    (repo_dir_s / "manifest.json").write_text("{}", encoding="utf-8")
    broken = repo_dir_s / "broken"
    broken.mkdir()

    original_stat = Path.stat
    calls = {"broken": 0}

    def _broken_stat(self: Path, *args: object, **kwargs: object):
        if self == broken:
            calls["broken"] += 1
            if calls["broken"] > 1:
                raise OSError("no stat")
        return original_stat(self, *args, **kwargs)

    monkeypatch.setattr(Path, "stat", _broken_stat)
    assert sweep_abandoned_staging(str(uploads), set(), max_age_hours=24) == 0
