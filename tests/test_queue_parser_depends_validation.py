"""Focused Depends on validation cases for task-file headers."""

from __future__ import annotations

from pathlib import Path

import pytest

from src.queue_parser import QueueValidationError, parse_task_header


def _write_task_file(tmp_path: Path, content: str) -> Path:
    task_path = tmp_path / "PR-999.md"
    task_path.write_text(content, encoding="utf-8")
    return task_path


@pytest.mark.parametrize("depends_on_value", ["foo", "nonee", "PR-001, foo"])
def test_parse_task_header_rejects_invalid_depends_on_tokens(
    tmp_path: Path, depends_on_value: str
) -> None:
    task_path = _write_task_file(
        tmp_path,
        f"""# PR-084: Task file header parser

Branch: pr-084-task-header-parser
- Type: feature
- Complexity: medium
- Depends on: {depends_on_value}
""",
    )

    with pytest.raises(QueueValidationError, match="invalid Depends on"):
        parse_task_header(task_path)
