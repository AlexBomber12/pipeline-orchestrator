from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
from src.queue_parser import (
    QueueValidationError,
    parse_task_header,
    write_frontmatter_status,
)
from src.subsource_registry import SuppressionReason


def _task_body(pr_id: str = "PR-378") -> str:
    return (
        f"# {pr_id}: Frontmatter blocked reason\n\n"
        "Branch: pr-378-frontmatter-blocked-reason\n"
        "- Type: refactor\n"
        "- Complexity: medium\n"
        "- Depends on: none\n"
        "- Priority: 1\n"
        "- Coder: any\n\n"
        "## Body\n\n"
        "Details stay here.\n"
    )


def _write_task(tmp_path: Path, content: str) -> Path:
    task_path = tmp_path / "PR-378.md"
    task_path.write_text(content, encoding="utf-8")
    return task_path


def _body_bytes_after_frontmatter(task_path: Path) -> bytes:
    data = task_path.read_bytes()
    lines = data.splitlines(keepends=True)
    closing_index = next(
        index for index, line in enumerate(lines[1:], start=1) if line.strip() == b"---"
    )
    return b"".join(lines[closing_index + 1 :])


def test_write_status_error_with_reason(tmp_path: Path) -> None:
    task = _write_task(tmp_path, f"---\nstatus: TODO\n---\n\n{_task_body()}")

    write_frontmatter_status(task, "ERROR", SuppressionReason.GUARDRAIL)

    text = task.read_text(encoding="utf-8")
    assert "status: ERROR\n" in text
    assert "blocked_reason: guardrail\n" in text
    header = parse_task_header(task)
    assert header.frontmatter_status == "error"
    assert header.blocked_reason == "guardrail"


def test_write_status_error_replaces_existing_reason(tmp_path: Path) -> None:
    task = _write_task(
        tmp_path,
        f"---\nstatus: ERROR\nblocked_reason: crash\n---\n\n{_task_body()}",
    )

    write_frontmatter_status(task, "ERROR", SuppressionReason.GUARDRAIL)

    text = task.read_text(encoding="utf-8")
    assert "blocked_reason: guardrail\n" in text
    assert "blocked_reason: crash\n" not in text


def test_write_status_unknown_reason_rejected(tmp_path: Path) -> None:
    task = _write_task(tmp_path, f"---\nstatus: TODO\n---\n\n{_task_body()}")

    with pytest.raises(ValueError, match="unknown blocked_reason"):
        write_frontmatter_status(task, "ERROR", "not_a_real_reason")


def test_single_commit_single_dump(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    task = _write_task(tmp_path, f"---\nstatus: TODO\n---\n\n{_task_body()}")
    original_open = Path.open
    write_opens = 0

    def counting_open(self: Path, *args: Any, **kwargs: Any) -> Any:
        nonlocal write_opens
        mode = args[0] if args else kwargs.get("mode", "r")
        if self == task and "w" in mode:
            write_opens += 1
        return original_open(self, *args, **kwargs)

    monkeypatch.setattr(Path, "open", counting_open)

    write_frontmatter_status(task, "ERROR", SuppressionReason.GUARDRAIL)

    assert write_opens == 1


def test_todo_write_clears_reason(tmp_path: Path) -> None:
    task = _write_task(tmp_path, f"---\nstatus: TODO\n---\n\n{_task_body()}")

    write_frontmatter_status(task, "ERROR", SuppressionReason.GUARDRAIL)
    write_frontmatter_status(task, "TODO")

    text = task.read_text(encoding="utf-8")
    assert "status: TODO\n" in text
    assert "blocked_reason:" not in text
    assert parse_task_header(task).blocked_reason is None


def test_file_without_reason_parses_none(tmp_path: Path) -> None:
    task = _write_task(tmp_path, f"---\nstatus: ERROR\n---\n\n{_task_body()}")

    assert parse_task_header(task).blocked_reason is None


def test_legacy_file_unaffected(tmp_path: Path) -> None:
    task = _write_task(tmp_path, _task_body())

    header = parse_task_header(task)

    assert header.frontmatter_status is None
    assert header.blocked_reason is None


def test_unknown_reason_rejected(tmp_path: Path) -> None:
    task = _write_task(
        tmp_path,
        f"---\nstatus: ERROR\nblocked_reason: not_a_real_reason\n---\n\n{_task_body()}",
    )

    with pytest.raises(QueueValidationError, match="invalid blocked_reason"):
        parse_task_header(task)


def test_empty_reason_rejected(tmp_path: Path) -> None:
    task = _write_task(
        tmp_path,
        f"---\nstatus: ERROR\nblocked_reason:\n---\n\n{_task_body()}",
    )

    with pytest.raises(QueueValidationError, match="invalid blocked_reason"):
        parse_task_header(task)


def test_error_without_reason_defaults(tmp_path: Path) -> None:
    task = _write_task(tmp_path, f"---\nstatus: TODO\n---\n\n{_task_body()}")

    write_frontmatter_status(task, "ERROR")

    text = task.read_text(encoding="utf-8")
    assert "blocked_reason: crash\n" in text
    assert parse_task_header(task).blocked_reason == "crash"


@pytest.mark.parametrize("reason", list(SuppressionReason))
def test_round_trip_all_canonical_reasons(
    tmp_path: Path,
    reason: SuppressionReason,
) -> None:
    task = _write_task(tmp_path, f"---\nstatus: TODO\n---\n\n{_task_body()}")

    write_frontmatter_status(task, "ERROR", reason)

    assert parse_task_header(task).blocked_reason == reason.value


def test_body_preserved(tmp_path: Path) -> None:
    body = _task_body() + "\n".join(f"Line {index}  " for index in range(50)) + "\n"
    task = _write_task(tmp_path, f"---\nowner: ops\nstatus: TODO\n---\n\n{body}")
    original_body = _body_bytes_after_frontmatter(task)

    write_frontmatter_status(task, "ERROR", SuppressionReason.GUARDRAIL)

    assert _body_bytes_after_frontmatter(task) == original_body
