from __future__ import annotations

from io import StringIO
from pathlib import Path

import pytest
from scripts import migrate_task_format
from src.queue_parser import parse_task_header


def _legacy_task(
    *,
    status: str | None = None,
    body: str = "## Scope\n\nDo the work.\n",
) -> str:
    status_line = "" if status is None else f"- Status: {status}\n"
    return (
        "# PR-999: Migration fixture\n\n"
        "Branch: pr-999-migration-fixture\n"
        f"{status_line}"
        "- Type: refactor\n"
        "- Complexity: high\n"
        "- Depends on: PR-001, PR-002\n"
        "- Priority: 2\n"
        "- Coder: codex\n\n"
        f"{body}"
    )


def _write_task(tasks_dir: Path, content: str, name: str = "PR-999.md") -> Path:
    tasks_dir.mkdir()
    path = tasks_dir / name
    path.write_text(content, encoding="utf-8")
    return path


def test_legacy_to_frontmatter_preserves_status(tmp_path: Path) -> None:
    path = _write_task(tmp_path / "tasks", _legacy_task(status="DONE"))

    migrate_task_format.migrate_tasks(path.parent, apply=True, stdout=StringIO())

    text = path.read_text(encoding="utf-8")
    assert text.startswith("---\nstatus: DONE\n---\n\n")
    assert parse_task_header(path).frontmatter_status == "done"


def test_legacy_no_status_becomes_todo(tmp_path: Path) -> None:
    path = _write_task(tmp_path / "tasks", _legacy_task())

    migrate_task_format.migrate_tasks(path.parent, apply=True, stdout=StringIO())

    assert path.read_text(encoding="utf-8").startswith("---\nstatus: TODO\n---\n\n")
    assert parse_task_header(path).frontmatter_status == "todo"


def test_all_header_fields_preserved(tmp_path: Path) -> None:
    path = _write_task(tmp_path / "tasks", _legacy_task(status="ERROR"))
    before = migrate_task_format._parse_or_legacy_issues(path)
    assert not isinstance(before, tuple)

    migrate_task_format.migrate_tasks(path.parent, apply=True, stdout=StringIO())

    after = parse_task_header(path)
    assert after.branch == before.branch
    assert after.task_type == before.task_type
    assert after.complexity == before.complexity
    assert after.depends_on == before.depends_on
    assert after.priority == before.priority
    assert after.coder == before.coder
    assert after.frontmatter_status == "error"


def test_body_preserved_verbatim(tmp_path: Path) -> None:
    body = (
        "## Scope\n\n"
        "Line with trailing spaces  \n\n"
        "### Details\n\n"
        "- keep bullets\n"
        "```\n"
        "literal block\n"
        "```\n"
    )
    original = _legacy_task(body=body)
    path = _write_task(tmp_path / "tasks", original)

    migrate_task_format.migrate_tasks(path.parent, apply=True, stdout=StringIO())

    migrated = path.read_text(encoding="utf-8")
    assert migrated.split("---\n\n", 1)[1] == original


def test_idempotent(tmp_path: Path) -> None:
    path = _write_task(tmp_path / "tasks", _legacy_task())
    migrate_task_format.migrate_tasks(path.parent, apply=True, stdout=StringIO())
    after_first = path.read_bytes()
    output = StringIO()

    result = migrate_task_format.migrate_tasks(path.parent, apply=True, stdout=output)

    assert result.changed == 0
    assert "changed=0" in output.getvalue()
    assert path.read_bytes() == after_first


def test_already_frontmatter_unchanged(tmp_path: Path) -> None:
    content = "---\nstatus: DONE\n---\n\n" + _legacy_task()
    path = _write_task(tmp_path / "tasks", content)

    result = migrate_task_format.migrate_tasks(path.parent, apply=True, stdout=StringIO())

    assert result.changed == 0
    assert path.read_text(encoding="utf-8") == content


def test_dry_run_writes_nothing(tmp_path: Path) -> None:
    content = _legacy_task(status="DONE")
    path = _write_task(tmp_path / "tasks", content)

    result = migrate_task_format.migrate_tasks(path.parent, stdout=StringIO())

    assert result.changed == 1
    assert path.read_text(encoding="utf-8") == content


def test_resolve_tasks_dir_rejects_missing_repo_path(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError, match="tasks directory not found"):
        migrate_task_format.resolve_tasks_dir(tmp_path / "missing")


def test_resolve_tasks_dir_rejects_non_task_directory(tmp_path: Path) -> None:
    empty_dir = tmp_path / "not-tasks"
    empty_dir.mkdir()

    with pytest.raises(FileNotFoundError, match="tasks directory not found"):
        migrate_task_format.resolve_tasks_dir(empty_dir)


def test_parser_rejects_apply_and_verify_together() -> None:
    parser = migrate_task_format.build_parser()

    with pytest.raises(SystemExit):
        parser.parse_args(["--apply", "--verify"])


def test_main_reports_invalid_repo_without_traceback(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    result = migrate_task_format.main(["--repo", str(tmp_path / "missing")])

    captured = capsys.readouterr()
    assert result == 1
    assert captured.err.startswith("ERROR: tasks directory not found")
    assert "Traceback" not in captured.err


def test_apply_creates_backup(tmp_path: Path) -> None:
    content = _legacy_task(status="DONE")
    path = _write_task(tmp_path / "tasks", content)
    backups_dir = tmp_path / "backups"

    result = migrate_task_format.migrate_tasks(
        path.parent,
        apply=True,
        backups_dir=backups_dir,
        stdout=StringIO(),
    )

    assert result.backups_dir == backups_dir
    assert (backups_dir / "PR-999.md").read_text(encoding="utf-8") == content


def test_apply_backs_up_unchanged_frontmatter_when_any_task_changes(
    tmp_path: Path,
) -> None:
    tasks_dir = tmp_path / "tasks"
    legacy_content = _legacy_task(status="DONE")
    frontmatter_content = "---\nstatus: TODO\n---\n\n" + _legacy_task()
    _write_task(tasks_dir, legacy_content, "PR-999.md")
    frontmatter_path = tasks_dir / "PR-998.md"
    frontmatter_path.write_text(frontmatter_content, encoding="utf-8")
    backups_dir = tmp_path / "backups"

    migrate_task_format.migrate_tasks(
        tasks_dir,
        apply=True,
        backups_dir=backups_dir,
        stdout=StringIO(),
    )

    assert (backups_dir / "PR-998.md").read_text(encoding="utf-8") == frontmatter_content


def test_apply_creates_backup_for_noop_frontmatter_run(tmp_path: Path) -> None:
    content = "---\nstatus: DONE\n---\n\n" + _legacy_task()
    path = _write_task(tmp_path / "tasks", content)
    backups_dir = tmp_path / "backups"

    result = migrate_task_format.migrate_tasks(
        path.parent,
        apply=True,
        backups_dir=backups_dir,
        stdout=StringIO(),
    )

    assert result.changed == 0
    assert (backups_dir / "PR-999.md").read_text(encoding="utf-8") == content


def test_apply_rejects_existing_backup_file(tmp_path: Path) -> None:
    content = _legacy_task(status="DONE")
    path = _write_task(tmp_path / "tasks", content)
    backups_dir = tmp_path / "backups"
    migrate_task_format.migrate_tasks(
        path.parent,
        apply=True,
        backups_dir=backups_dir,
        stdout=StringIO(),
    )
    path.write_text("---\nstatus: ERROR\n---\n\n" + content, encoding="utf-8")

    with pytest.raises(FileExistsError, match="backup already exists"):
        migrate_task_format.migrate_tasks(
            path.parent,
            apply=True,
            backups_dir=backups_dir,
            stdout=StringIO(),
        )

    assert (backups_dir / "PR-999.md").read_text(encoding="utf-8") == content


def test_verify_detects_mismatch(tmp_path: Path) -> None:
    path = _write_task(tmp_path / "tasks", _legacy_task(status="DONE"))
    backups_dir = tmp_path / "backups"
    migrate_task_format.migrate_tasks(
        path.parent,
        apply=True,
        backups_dir=backups_dir,
        stdout=StringIO(),
    )
    corrupted = path.read_text(encoding="utf-8").replace(
        "status: DONE",
        "status: TODO",
    )
    path.write_text(corrupted, encoding="utf-8")

    with pytest.raises(RuntimeError, match="verify failed"):
        migrate_task_format.verify_tasks(
            path.parent,
            backups_dir=backups_dir,
            stdout=StringIO(),
        )


def test_verify_requires_current_file_frontmatter(tmp_path: Path) -> None:
    original = _legacy_task(status="DONE")
    path = _write_task(tmp_path / "tasks", original)
    backups_dir = tmp_path / "backups"
    migrate_task_format.migrate_tasks(
        path.parent,
        apply=True,
        backups_dir=backups_dir,
        stdout=StringIO(),
    )
    path.write_text(original, encoding="utf-8")

    with pytest.raises(RuntimeError, match="missing valid frontmatter status"):
        migrate_task_format.verify_tasks(
            path.parent,
            backups_dir=backups_dir,
            stdout=StringIO(),
        )


def test_verify_requires_current_frontmatter_status(tmp_path: Path) -> None:
    path = _write_task(tmp_path / "tasks", _legacy_task(status="DONE"))
    backups_dir = tmp_path / "backups"
    migrate_task_format.migrate_tasks(
        path.parent,
        apply=True,
        backups_dir=backups_dir,
        stdout=StringIO(),
    )
    without_status = path.read_text(encoding="utf-8").replace("status: DONE\n", "")
    path.write_text(without_status, encoding="utf-8")

    with pytest.raises(RuntimeError, match="missing valid frontmatter status"):
        migrate_task_format.verify_tasks(
            path.parent,
            backups_dir=backups_dir,
            stdout=StringIO(),
        )


def test_verify_fails_when_backup_missing(tmp_path: Path) -> None:
    path = _write_task(tmp_path / "tasks", _legacy_task(status="DONE"))
    backups_dir = tmp_path / "backups"
    migrate_task_format.migrate_tasks(
        path.parent,
        apply=True,
        backups_dir=backups_dir,
        stdout=StringIO(),
    )
    (backups_dir / "PR-999.md").unlink()

    with pytest.raises(RuntimeError, match="missing backup"):
        migrate_task_format.verify_tasks(
            path.parent,
            backups_dir=backups_dir,
            stdout=StringIO(),
        )


def test_verify_fails_when_task_missing_from_backup(tmp_path: Path) -> None:
    path = _write_task(tmp_path / "tasks", _legacy_task(status="DONE"))
    backups_dir = tmp_path / "backups"
    migrate_task_format.migrate_tasks(
        path.parent,
        apply=True,
        backups_dir=backups_dir,
        stdout=StringIO(),
    )
    path.unlink()

    with pytest.raises(RuntimeError, match="missing task"):
        migrate_task_format.verify_tasks(
            path.parent,
            backups_dir=backups_dir,
            stdout=StringIO(),
        )


def test_verify_allows_unchanged_legacy_validation_gaps(tmp_path: Path) -> None:
    content = (
        "# PR-999: Older legacy fixture\n\n"
        "Branch: pr-999-older-legacy-fixture\n"
        "- Status: DONE\n"
        "- Type: refactor\n"
        "- Complexity: medium\n\n"
        "## Body\n\n"
        "Still missing Depends on.\n"
    )
    path = _write_task(tmp_path / "tasks", content)
    backups_dir = tmp_path / "backups"
    migrate_task_format.migrate_tasks(
        path.parent,
        apply=True,
        backups_dir=backups_dir,
        stdout=StringIO(),
    )

    result = migrate_task_format.verify_tasks(
        path.parent,
        backups_dir=backups_dir,
        stdout=StringIO(),
    )

    assert result.checked == 1


def test_verify_uses_frontmatter_before_backup_fallback_status(
    tmp_path: Path,
) -> None:
    tasks_dir = tmp_path / "tasks"
    legacy_content = _legacy_task(status="DONE")
    frontmatter_content = (
        "---\nstatus: DONE\n---\n\n"
        "# PR-998: Frontmatter validation gap\n\n"
        "Branch: pr-998-frontmatter-validation-gap\n"
        "- Type: refactor\n"
        "- Complexity: medium\n\n"
        "## Body\n\n"
        "Still missing Depends on.\n"
    )
    _write_task(tasks_dir, legacy_content, "PR-999.md")
    frontmatter_path = tasks_dir / "PR-998.md"
    frontmatter_path.write_text(frontmatter_content, encoding="utf-8")
    backups_dir = tmp_path / "backups"
    migrate_task_format.migrate_tasks(
        tasks_dir,
        apply=True,
        backups_dir=backups_dir,
        stdout=StringIO(),
    )

    result = migrate_task_format.verify_tasks(
        tasks_dir,
        backups_dir=backups_dir,
        stdout=StringIO(),
    )

    assert result.checked == 2


def test_atomic_write_no_partial(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    path = _write_task(tmp_path / "tasks", _legacy_task(status="DONE"))
    original = path.read_text(encoding="utf-8")

    def fail_replace(source: Path, destination: Path) -> None:
        raise OSError("interrupted")

    monkeypatch.setattr(migrate_task_format.os, "replace", fail_replace)

    with pytest.raises(OSError, match="interrupted"):
        migrate_task_format.migrate_tasks(path.parent, apply=True, stdout=StringIO())

    assert path.read_text(encoding="utf-8") == original


def test_apply_validates_all_statuses_before_writing(tmp_path: Path) -> None:
    tasks_dir = tmp_path / "tasks"
    first = _write_task(tasks_dir, _legacy_task(status="DONE"), "PR-998.md")
    second_content = _legacy_task(status="DOING")
    second = tasks_dir / "PR-999.md"
    second.write_text(second_content, encoding="utf-8")
    first_content = first.read_text(encoding="utf-8")
    backups_dir = tmp_path / "backups"

    with pytest.raises(ValueError, match="cannot be represented in frontmatter"):
        migrate_task_format.migrate_tasks(
            tasks_dir,
            apply=True,
            backups_dir=backups_dir,
            stdout=StringIO(),
        )

    assert first.read_text(encoding="utf-8") == first_content
    assert second.read_text(encoding="utf-8") == second_content
    assert not backups_dir.exists()
