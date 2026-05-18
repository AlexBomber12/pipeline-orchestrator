"""Tests for upload-route collision-preserve logic (PR-337).

The dashboard upload route rewrites incoming task-file content so that a
re-uploaded ``status: TODO`` spec cannot regress an on-disk
``status: DONE`` or ``status: ERROR`` task. The transform happens before
files are staged under ``/data/uploads/``, so the daemon's later
``shutil.copy2`` into ``tasks/`` carries the preserved terminal status.
"""

from __future__ import annotations

import io
import logging
import zipfile
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from src.web import app as web_app
from src.web.app import app
from src.web.services import upload_validation


class _StubRedisClient:
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

    async def delete(self, key: str) -> int:
        existed = key in self._store
        self._store.pop(key, None)
        return int(existed)

    async def scan_iter(self, match: str | None = None):
        if False:
            yield ""

    async def aclose(self) -> None:
        return None


class _StubAioredis:
    def __init__(self) -> None:
        self.client = _StubRedisClient()

    def from_url(self, url: str, decode_responses: bool = True) -> _StubRedisClient:
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
    monkeypatch.setattr(web_app, "aioredis", _StubAioredis())
    return cfg


@pytest.fixture
def repo_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    repos = tmp_path / "repos"
    repos.mkdir()
    alpha = repos / "example__alpha"
    alpha.mkdir()
    (alpha / "tasks").mkdir()
    monkeypatch.setattr(web_app, "REPOS_DIR", str(repos))
    return alpha


@pytest.fixture
def uploads_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    uploads = tmp_path / "uploads"
    uploads.mkdir()
    monkeypatch.setattr(web_app, "UPLOADS_DIR", str(uploads))
    return uploads


def _task_body(pr_id: str = "PR-322", title: str = "Example task") -> str:
    return (
        f"# {pr_id}: {title}\n"
        "\n"
        f"Branch: {pr_id.lower()}-example-task\n"
        "- Type: bugfix\n"
        "- Complexity: low\n"
        "- Depends on: none\n"
        "- Priority: 1\n"
        "- Coder: any\n"
    )


def _task_text(
    pr_id: str = "PR-322",
    *,
    status: str | None = "TODO",
    title: str = "Example task",
) -> str:
    body = _task_body(pr_id, title)
    if status is None:
        return body
    return f"---\nstatus: {status}\n---\n\n{body}"


def _task_upload(
    pr_id: str = "PR-322",
    *,
    status: str | None = "TODO",
    title: str = "Example task",
) -> tuple[str, tuple[str, bytes, str]]:
    return (
        "files",
        (
            f"{pr_id}.md",
            _task_text(pr_id, status=status, title=title).encode("utf-8"),
            "text/markdown",
        ),
    )


def _zip_upload(
    entries: list[tuple[str, str]],
    *,
    name: str = "tasks.zip",
) -> tuple[str, tuple[str, bytes, str]]:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        for entry_name, content in entries:
            archive.writestr(entry_name, content)
    return ("files", (name, buffer.getvalue(), "application/zip"))


def _staged_text(uploads_root: Path, fname: str) -> str:
    repo_upload_dir = uploads_root / "example__alpha"
    subdirs = list(repo_upload_dir.iterdir())
    assert len(subdirs) == 1, subdirs
    staging = subdirs[0]
    staged = staging / fname
    assert staged.is_file()
    return staged.read_text(encoding="utf-8")


def _post(files: list[tuple[str, tuple[str, bytes, str]]]):
    with TestClient(app) as client:
        return client.post("/repos/example__alpha/upload-tasks", files=files)


def test_upload_preserves_done_status_on_collision(
    one_repo_config: Path,
    repo_dir: Path,
    uploads_dir: Path,
) -> None:
    (repo_dir / "tasks" / "PR-322.md").write_text(
        _task_text("PR-322", status="DONE", title="Already merged"),
        encoding="utf-8",
    )

    resp = _post([_task_upload("PR-322", status="TODO", title="Regenerated spec")])

    assert resp.status_code == 200
    staged = _staged_text(uploads_dir, "PR-322.md")
    assert "status: DONE" in staged.splitlines()[1]
    assert "status: TODO" not in staged
    assert "Regenerated spec" in staged


def test_upload_preserves_error_status_on_collision(
    one_repo_config: Path,
    repo_dir: Path,
    uploads_dir: Path,
) -> None:
    (repo_dir / "tasks" / "PR-322.md").write_text(
        _task_text("PR-322", status="ERROR"),
        encoding="utf-8",
    )

    resp = _post([_task_upload("PR-322", status="TODO", title="Regenerated body")])

    assert resp.status_code == 200
    staged = _staged_text(uploads_dir, "PR-322.md")
    assert "status: ERROR" in staged.splitlines()[1]
    assert "Regenerated body" in staged


def test_upload_replaces_todo_status_on_collision(
    one_repo_config: Path,
    repo_dir: Path,
    uploads_dir: Path,
) -> None:
    (repo_dir / "tasks" / "PR-322.md").write_text(
        _task_text("PR-322", status="TODO", title="Old body"),
        encoding="utf-8",
    )

    resp = _post([_task_upload("PR-322", status="TODO", title="New body")])

    assert resp.status_code == 200
    staged = _staged_text(uploads_dir, "PR-322.md")
    assert staged == _task_text("PR-322", status="TODO", title="New body")


def test_upload_replaces_when_no_existing_file(
    one_repo_config: Path,
    repo_dir: Path,
    uploads_dir: Path,
) -> None:
    assert not (repo_dir / "tasks" / "PR-322.md").exists()

    resp = _post([_task_upload("PR-322", status="TODO")])

    assert resp.status_code == 200
    staged = _staged_text(uploads_dir, "PR-322.md")
    assert staged == _task_text("PR-322", status="TODO")


def test_upload_preserves_when_existing_has_no_frontmatter(
    one_repo_config: Path,
    repo_dir: Path,
    uploads_dir: Path,
) -> None:
    # Existing without frontmatter is treated as TODO — operator intent is
    # full replace.
    (repo_dir / "tasks" / "PR-322.md").write_text(
        _task_text("PR-322", status=None),
        encoding="utf-8",
    )

    resp = _post([_task_upload("PR-322", status="TODO", title="Replacement")])

    assert resp.status_code == 200
    staged = _staged_text(uploads_dir, "PR-322.md")
    assert "Replacement" in staged
    assert staged.startswith("---\nstatus: TODO\n---")


def test_upload_replaces_malformed_existing_file_with_unclosed_frontmatter(
    one_repo_config: Path,
    repo_dir: Path,
    uploads_dir: Path,
) -> None:
    # Existing file has ``status: DONE`` inside an unclosed frontmatter block —
    # treat as malformed and allow full replacement, otherwise operators cannot
    # repair corrupted task files via re-upload.
    body = _task_body("PR-322")
    (repo_dir / "tasks" / "PR-322.md").write_text(
        f"---\nstatus: DONE\n{body}",
        encoding="utf-8",
    )

    resp = _post([_task_upload("PR-322", status="TODO", title="Repair upload")])

    assert resp.status_code == 200
    staged = _staged_text(uploads_dir, "PR-322.md")
    assert staged == _task_text("PR-322", status="TODO", title="Repair upload")


def test_upload_zip_partial_preserve(
    one_repo_config: Path,
    repo_dir: Path,
    uploads_dir: Path,
) -> None:
    (repo_dir / "tasks" / "PR-100.md").write_text(
        _task_text("PR-100", status="DONE", title="Done already"),
        encoding="utf-8",
    )
    (repo_dir / "tasks" / "PR-101.md").write_text(
        _task_text("PR-101", status="TODO", title="In progress"),
        encoding="utf-8",
    )

    entries = [
        ("PR-100.md", _task_text("PR-100", status="TODO", title="Regenerated 100")),
        ("PR-101.md", _task_text("PR-101", status="TODO", title="Regenerated 101")),
        ("PR-102.md", _task_text("PR-102", status="TODO", title="Brand new 102")),
    ]
    resp = _post([_zip_upload(entries)])

    assert resp.status_code == 200
    staged_100 = _staged_text(uploads_dir, "PR-100.md")
    staged_101 = _staged_text(uploads_dir, "PR-101.md")
    staged_102 = _staged_text(uploads_dir, "PR-102.md")
    assert "status: DONE" in staged_100.splitlines()[1]
    assert "Regenerated 100" in staged_100
    assert staged_101 == _task_text("PR-101", status="TODO", title="Regenerated 101")
    assert staged_102 == _task_text("PR-102", status="TODO", title="Brand new 102")


def test_upload_audit_event_records_preserved_collisions(
    one_repo_config: Path,
    repo_dir: Path,
    uploads_dir: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    (repo_dir / "tasks" / "PR-322.md").write_text(
        _task_text("PR-322", status="DONE"),
        encoding="utf-8",
    )

    with caplog.at_level(logging.INFO, logger=web_app.logger.name):
        resp = _post([_task_upload("PR-322", status="TODO")])

    assert resp.status_code == 200
    matching = [
        record
        for record in caplog.records
        if "preserved terminal frontmatter status" in record.getMessage()
    ]
    assert matching, "expected an info-level event log entry for preserved collision"
    message = matching[0].getMessage()
    assert "PR-322.md=DONE" in message
    assert "example__alpha" in message


def test_upload_preserves_done_status_with_quoted_value(
    one_repo_config: Path,
    repo_dir: Path,
    uploads_dir: Path,
) -> None:
    body = _task_body("PR-322")
    (repo_dir / "tasks" / "PR-322.md").write_text(
        f"---\nstatus: \"done\"\n---\n\n{body}",
        encoding="utf-8",
    )

    resp = _post([_task_upload("PR-322", status="TODO", title="Regenerated")])

    assert resp.status_code == 200
    staged = _staged_text(uploads_dir, "PR-322.md")
    assert "status: DONE" in staged.splitlines()[1]
    assert "Regenerated" in staged


def test_upload_preserves_done_status_with_trailing_comment(
    one_repo_config: Path,
    repo_dir: Path,
    uploads_dir: Path,
) -> None:
    # The canonical task parser accepts ``status: done # reviewer override``
    # (see queue_parser._normalize_frontmatter_status). The upload guard
    # must agree so the inline comment does not bypass terminal-status
    # preservation.
    body = _task_body("PR-322")
    (repo_dir / "tasks" / "PR-322.md").write_text(
        f"---\nstatus: done # reviewer override\n---\n\n{body}",
        encoding="utf-8",
    )

    resp = _post([_task_upload("PR-322", status="TODO", title="Regenerated")])

    assert resp.status_code == 200
    staged = _staged_text(uploads_dir, "PR-322.md")
    assert "status: DONE" in staged.splitlines()[1]
    assert "Regenerated" in staged


def test_read_frontmatter_status_strips_inline_comment() -> None:
    text = "---\nstatus: done # reviewer override\n---\n\nbody\n"
    assert upload_validation.read_frontmatter_status(text) == "DONE"


def test_read_frontmatter_status_keeps_hash_inside_quotes() -> None:
    text = "---\nstatus: 'done # not a comment'\n---\n\nbody\n"
    assert (
        upload_validation.read_frontmatter_status(text) == "DONE # NOT A COMMENT"
    )


def test_read_frontmatter_status_no_frontmatter() -> None:
    assert upload_validation.read_frontmatter_status("# heading\n") is None


def test_read_frontmatter_status_empty_content() -> None:
    assert upload_validation.read_frontmatter_status("") is None


def test_read_frontmatter_status_block_without_status_field() -> None:
    text = "---\nother: value\n---\n\nbody\n"
    assert upload_validation.read_frontmatter_status(text) is None


def test_read_frontmatter_status_open_block_never_closed() -> None:
    text = "---\nother: value\nstill: open\n"
    assert upload_validation.read_frontmatter_status(text) is None


def test_read_frontmatter_status_status_inside_unclosed_block() -> None:
    text = "---\nstatus: DONE\nstill: open\n"
    assert upload_validation.read_frontmatter_status(text) is None


def test_read_frontmatter_status_empty_value() -> None:
    text = "---\nstatus:\n---\n\nbody\n"
    assert upload_validation.read_frontmatter_status(text) is None


def test_replace_frontmatter_status_appends_status_when_field_missing() -> None:
    upload = "---\nother: value\n---\n\nbody\n"
    rewritten = upload_validation._replace_frontmatter_status(upload, "DONE")
    assert rewritten == "---\nother: value\nstatus: DONE\n---\n\nbody\n"


def test_replace_frontmatter_status_prepends_when_no_frontmatter() -> None:
    upload = "# PR-322: title\n"
    rewritten = upload_validation._replace_frontmatter_status(upload, "DONE")
    assert rewritten == "---\nstatus: DONE\n---\n\n# PR-322: title\n"


def test_replace_frontmatter_status_prepends_when_block_unclosed() -> None:
    upload = "---\nother: value\nstill: open\n"
    rewritten = upload_validation._replace_frontmatter_status(upload, "ERROR")
    assert rewritten.startswith("---\nstatus: ERROR\n---\n\n")
    assert rewritten.endswith(upload)


def test_replace_frontmatter_status_handles_crlf_line_endings() -> None:
    upload = "---\r\nstatus: TODO\r\n---\r\n\r\nbody\r\n"
    rewritten = upload_validation._replace_frontmatter_status(upload, "DONE")
    assert "status: DONE\r\n" in rewritten


def test_read_frontmatter_status_skips_leading_blank_lines() -> None:
    text = "\n\n---\nstatus: DONE\n---\n\nbody\n"
    assert upload_validation.read_frontmatter_status(text) == "DONE"


def test_read_frontmatter_status_uses_last_duplicate_status_key() -> None:
    text = "---\nstatus: TODO\nstatus: DONE\n---\n\nbody\n"
    assert upload_validation.read_frontmatter_status(text) == "DONE"


def test_upload_preserves_done_with_leading_blank_lines_on_existing(
    one_repo_config: Path,
    repo_dir: Path,
    uploads_dir: Path,
) -> None:
    body = _task_body("PR-322")
    (repo_dir / "tasks" / "PR-322.md").write_text(
        f"\n\n---\nstatus: DONE\n---\n\n{body}",
        encoding="utf-8",
    )

    resp = _post([_task_upload("PR-322", status="TODO", title="Regenerated")])

    assert resp.status_code == 200
    staged = _staged_text(uploads_dir, "PR-322.md")
    assert "status: DONE" in staged
    assert "status: TODO" not in staged
    assert "Regenerated" in staged


def test_upload_preserves_done_when_upload_has_leading_blank_lines(
    one_repo_config: Path,
    repo_dir: Path,
    uploads_dir: Path,
) -> None:
    # ``parse_task_header`` skips leading blank lines before the opening
    # ``---`` of the frontmatter block, so an upload like ``\n\n---\n...``
    # is still a valid task spec. The collision-preserve rewrite must edit
    # that block in place instead of prepending a second frontmatter section
    # that would leave the uploaded ``status: TODO`` in the body.
    (repo_dir / "tasks" / "PR-322.md").write_text(
        _task_text("PR-322", status="DONE", title="Already merged"),
        encoding="utf-8",
    )
    upload_payload = "\n\n" + _task_text(
        "PR-322", status="TODO", title="Regenerated spec"
    )
    files = [
        (
            "files",
            (
                "PR-322.md",
                upload_payload.encode("utf-8"),
                "text/markdown",
            ),
        )
    ]

    resp = _post(files)

    assert resp.status_code == 200
    staged = _staged_text(uploads_dir, "PR-322.md")
    assert staged.count("---\nstatus:") == 1
    assert "status: DONE" in staged
    assert "status: TODO" not in staged
    assert "Regenerated spec" in staged


def test_replace_frontmatter_status_skips_leading_blank_lines() -> None:
    upload = "\n\n---\nstatus: TODO\n---\n\nbody\n"
    rewritten = upload_validation._replace_frontmatter_status(upload, "DONE")
    assert rewritten == "\n\n---\nstatus: DONE\n---\n\nbody\n"


def test_upload_preserves_done_when_existing_has_duplicate_status_keys(
    one_repo_config: Path,
    repo_dir: Path,
    uploads_dir: Path,
) -> None:
    body = _task_body("PR-322")
    (repo_dir / "tasks" / "PR-322.md").write_text(
        f"---\nstatus: TODO\nstatus: DONE\n---\n\n{body}",
        encoding="utf-8",
    )

    resp = _post([_task_upload("PR-322", status="TODO", title="Regenerated")])

    assert resp.status_code == 200
    staged = _staged_text(uploads_dir, "PR-322.md")
    assert "status: DONE" in staged
    assert "Regenerated" in staged


def test_preserve_terminal_status_missing_existing_file(tmp_path: Path) -> None:
    new_text, preserved = upload_validation.preserve_terminal_status_on_collision(
        tmp_path / "nope.md", "upload-body\n"
    )
    assert preserved is None
    assert new_text == "upload-body\n"


def test_preserve_terminal_status_existing_unreadable_bytes(tmp_path: Path) -> None:
    # A non-UTF-8 byte sequence on disk falls back to leaving the upload
    # untouched rather than raising; daemon-written task files are always
    # UTF-8 so reaching this branch indicates a malformed working tree.
    path = tmp_path / "PR-001.md"
    path.write_bytes(b"\xff\xfe garbage")
    new_text, preserved = upload_validation.preserve_terminal_status_on_collision(
        path, "upload-body\n"
    )
    assert preserved is None
    assert new_text == "upload-body\n"


def test_upload_zip_with_duplicate_entry_earlier_non_utf8(
    one_repo_config: Path,
    repo_dir: Path,
    uploads_dir: Path,
) -> None:
    # A zip with two ``PR-322.md`` entries where the earlier one carries
    # non-UTF-8 bytes must not crash the collision-preserve loop. Validation
    # runs on the deduplicated last-wins map, so the upload completes 200 and
    # the staged file matches the later (valid) entry with the on-disk DONE
    # status preserved.
    (repo_dir / "tasks" / "PR-322.md").write_text(
        _task_text("PR-322", status="DONE", title="Already merged"),
        encoding="utf-8",
    )

    valid_payload = _task_text(
        "PR-322", status="TODO", title="Regenerated body"
    ).encode("utf-8")
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr("PR-322.md", b"\xff\xfe not utf-8 garbage")
        archive.writestr("PR-322.md", valid_payload)
    zip_field = ("files", ("dup.zip", buffer.getvalue(), "application/zip"))

    resp = _post([zip_field])

    assert resp.status_code == 200, resp.text
    staged = _staged_text(uploads_dir, "PR-322.md")
    assert "status: DONE" in staged.splitlines()[1]
    assert "Regenerated body" in staged
