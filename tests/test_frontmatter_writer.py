from __future__ import annotations

from pathlib import Path

import pytest
from src.queue_parser import write_frontmatter_status
from src.subsource_registry import SuppressionReason


def test_write_status_error_to_file_without_frontmatter(tmp_path: Path) -> None:
    task = tmp_path / "PR-001.md"
    task.write_text("# PR-001: Test\n\nBody\n", encoding="utf-8")

    write_frontmatter_status(task, "ERROR", SuppressionReason.CRASH)

    assert task.read_text(encoding="utf-8").startswith(
        "---\nstatus: ERROR\nblocked_reason: crash\n---\n\n"
    )


def test_write_status_replaces_existing_field(tmp_path: Path) -> None:
    task = tmp_path / "PR-001.md"
    task.write_text("---\nstatus: TODO\n---\n\nBody\n", encoding="utf-8")

    write_frontmatter_status(task, "ERROR", SuppressionReason.CRASH)

    assert task.read_text(encoding="utf-8") == (
        "---\nstatus: ERROR\nblocked_reason: crash\n---\n\nBody\n"
    )


def test_write_status_replaces_existing_field_with_crlf_frontmatter(
    tmp_path: Path,
) -> None:
    task = tmp_path / "PR-001.md"
    task.write_text("---\r\nstatus: TODO\r\n---\r\n\r\nBody\r\n", encoding="utf-8")

    write_frontmatter_status(task, "ERROR", SuppressionReason.CRASH)

    assert task.read_bytes() == (
        b"---\nstatus: ERROR\nblocked_reason: crash\n---\n\r\nBody\r\n"
    )


def test_write_status_inserts_into_existing_frontmatter_with_other_fields(
    tmp_path: Path,
) -> None:
    task = tmp_path / "PR-001.md"
    task.write_text(
        "---\nowner: ops\npriority: 1\n---\n\nBody\n",
        encoding="utf-8",
    )

    write_frontmatter_status(task, "DONE")

    assert task.read_text(encoding="utf-8") == (
        "---\nowner: ops\npriority: 1\nstatus: DONE\n---\n\nBody\n"
    )


def test_write_status_invalid_value_raises(tmp_path: Path) -> None:
    task = tmp_path / "PR-001.md"
    task.write_text("Body\n", encoding="utf-8")

    with pytest.raises(ValueError):
        write_frontmatter_status(task, "invalid")


def test_write_status_lowercase_input_raises(tmp_path: Path) -> None:
    task = tmp_path / "PR-001.md"
    task.write_text("Body\n", encoding="utf-8")

    with pytest.raises(ValueError):
        write_frontmatter_status(task, "error")


def test_write_status_preserves_body_content(tmp_path: Path) -> None:
    task = tmp_path / "PR-001.md"
    body = "\n# PR-001: Test\n\nLine with trailing spaces  \n\n"
    task.write_text("---\nstatus: TODO\n---\n" + body, encoding="utf-8")

    write_frontmatter_status(task, "ERROR", SuppressionReason.CRASH)

    assert task.read_text(encoding="utf-8").split("---\n", 2)[2] == body


def test_write_status_prepends_when_frontmatter_is_unclosed(
    tmp_path: Path,
) -> None:
    task = tmp_path / "PR-001.md"
    task.write_text("---\nstatus: TODO\n\n# body\n", encoding="utf-8")

    write_frontmatter_status(task, "ERROR", SuppressionReason.CRASH)

    assert task.read_text(encoding="utf-8").startswith(
        "---\nstatus: ERROR\nblocked_reason: crash\n---\n\n---\nstatus: TODO\n"
    )


def test_write_status_replaces_non_mapping_frontmatter(tmp_path: Path) -> None:
    task = tmp_path / "PR-001.md"
    task.write_text("---\n- old\n---\n\nBody\n", encoding="utf-8")

    write_frontmatter_status(task, "DONE")

    assert task.read_text(encoding="utf-8") == (
        "---\nstatus: DONE\n---\n\nBody\n"
    )
