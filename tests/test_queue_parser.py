"""Tests for src/queue_parser.py."""

from __future__ import annotations

import logging
from pathlib import Path

import pytest
from src.models import TaskStatus
from src.queue_parser import (
    QueueValidationError,
    TaskHeader,
    get_next_task,
    mark_task_done,
    parse_queue,
    parse_queue_text,
    parse_task_header,
)

SAMPLE_QUEUE = """## PR-001: Bootstrap
- Status: DONE
- Tasks file: tasks/PR-001.md
- Branch: pr-001-bootstrap

## PR-002: Models
- Status: DONE
- Tasks file: tasks/PR-002.md
- Branch: pr-002-models
- Depends on: PR-001

## PR-003: Parser
- Status: TODO
- Tasks file: tasks/PR-003.md
- Branch: pr-003-parser
- Depends on: PR-002

## PR-004: Client
- Status: TODO
- Tasks file: tasks/PR-004.md
- Branch: pr-004-client
- Depends on: PR-002
"""


def _write_queue(tmp_path: Path, content: str = SAMPLE_QUEUE) -> str:
    queue_path = tmp_path / "QUEUE.md"
    queue_path.write_text(content, encoding="utf-8")
    return str(queue_path)


def _write_task_file(tmp_path: Path, content: str) -> Path:
    task_path = tmp_path / "PR-999.md"
    task_path.write_text(content, encoding="utf-8")
    return task_path


def test_parse_queue_missing_file_returns_empty(tmp_path: Path) -> None:
    assert parse_queue(str(tmp_path / "missing.md")) == []


def test_parse_queue_extracts_all_tasks(tmp_path: Path) -> None:
    tasks = parse_queue(_write_queue(tmp_path))
    assert len(tasks) == 4
    assert [task.pr_id for task in tasks] == ["PR-001", "PR-002", "PR-003", "PR-004"]


def test_parse_queue_extracts_fields(tmp_path: Path) -> None:
    tasks = parse_queue(_write_queue(tmp_path))

    pr1 = tasks[0]
    assert pr1.pr_id == "PR-001"
    assert pr1.title == "Bootstrap"
    assert pr1.status == TaskStatus.DONE
    assert pr1.task_file == "tasks/PR-001.md"
    assert pr1.branch == "pr-001-bootstrap"
    assert pr1.depends_on == []

    pr2 = tasks[1]
    assert pr2.pr_id == "PR-002"
    assert pr2.title == "Models"
    assert pr2.status == TaskStatus.DONE
    assert pr2.task_file == "tasks/PR-002.md"
    assert pr2.branch == "pr-002-models"
    assert pr2.depends_on == ["PR-001"]

    pr3 = tasks[2]
    assert pr3.pr_id == "PR-003"
    assert pr3.title == "Parser"
    assert pr3.status == TaskStatus.TODO
    assert pr3.task_file == "tasks/PR-003.md"
    assert pr3.branch == "pr-003-parser"
    assert pr3.depends_on == ["PR-002"]


def test_parse_task_header_complete(tmp_path: Path) -> None:
    task_path = _write_task_file(
        tmp_path,
        """# PR-084: Task file header parser

Branch: pr-084-task-header-parser
- Type: feature
- Complexity: medium
- Depends on: PR-063, PR-065
- Priority: 2
- Coder: codex
""",
    )

    header = parse_task_header(task_path)

    assert header == TaskHeader(
        pr_id="PR-084",
        title="Task file header parser",
        branch="pr-084-task-header-parser",
        task_type="feature",
        complexity="medium",
        depends_on=["PR-063", "PR-065"],
        priority=2,
        coder="codex",
    )


def test_parse_task_header_depends_on_none(tmp_path: Path) -> None:
    task_path = _write_task_file(
        tmp_path,
        """# PR-084: Task file header parser

Branch: pr-084-task-header-parser
- Type: feature
- Complexity: medium
- Depends on: none
""",
    )

    header = parse_task_header(task_path)

    assert header.depends_on == []


def test_parse_task_header_depends_on_missing_raises(
    tmp_path: Path,
) -> None:
    task_path = _write_task_file(
        tmp_path,
        """# PR-084: Task file header parser

Branch: pr-084-task-header-parser
- Type: feature
- Complexity: medium
""",
    )

    with pytest.raises(QueueValidationError, match="missing Depends on"):
        parse_task_header(task_path)


def test_parse_task_header_depends_on_empty_raises(tmp_path: Path) -> None:
    task_path = _write_task_file(
        tmp_path,
        """# PR-084: Task file header parser

Branch: pr-084-task-header-parser
- Type: feature
- Complexity: medium
- Depends on:
""",
    )

    with pytest.raises(QueueValidationError, match="invalid Depends on"):
        parse_task_header(task_path)


def test_parse_task_header_allows_existing_repo_task_types(
    tmp_path: Path,
) -> None:
    task_path = _write_task_file(
        tmp_path,
        """# PR-084: Task file header parser

Branch: pr-084-task-header-parser
- Type: config
- Complexity: low
- Depends on: none
""",
    )

    header = parse_task_header(task_path)

    assert header.task_type == "config"


def test_parse_task_header_ignores_metadata_like_bullets_after_header(
    tmp_path: Path,
) -> None:
    task_path = _write_task_file(
        tmp_path,
        """# PR-084: Task file header parser

Branch: pr-084-task-header-parser
- Type: feature
- Complexity: medium
- Depends on: none

## Notes

- Depends on: PR-999
- Type: docs
""",
    )

    header = parse_task_header(task_path)

    assert header.depends_on == []
    assert header.task_type == "feature"

def test_parse_task_header_priority_default(tmp_path: Path) -> None:
    task_path = _write_task_file(
        tmp_path,
        """# PR-084: Task file header parser

Branch: pr-084-task-header-parser
- Type: feature
- Complexity: medium
- Depends on: none
""",
    )

    header = parse_task_header(task_path)

    assert header.priority == 3


def test_parse_task_header_coder_default(tmp_path: Path) -> None:
    task_path = _write_task_file(
        tmp_path,
        """# PR-084: Task file header parser

Branch: pr-084-task-header-parser
- Type: feature
- Complexity: medium
- Depends on: none
- Priority: 4
""",
    )

    header = parse_task_header(task_path)

    assert header.coder == "any"


def test_parse_task_header_multiple_deps(tmp_path: Path) -> None:
    task_path = _write_task_file(
        tmp_path,
        """# PR-084: Task file header parser

Branch: pr-084-task-header-parser
- Type: feature
- Complexity: medium
- Depends on: PR-063, PR-065
""",
    )

    header = parse_task_header(task_path)

    assert header.depends_on == ["PR-063", "PR-065"]


def test_get_next_task_returns_first_todo_no_deps(tmp_path: Path) -> None:
    content = """## PR-001: First
- Status: TODO
- Tasks file: tasks/PR-001.md
- Branch: pr-001-first

## PR-002: Second
- Status: TODO
- Tasks file: tasks/PR-002.md
- Branch: pr-002-second
"""
    tasks = parse_queue(_write_queue(tmp_path, content))
    nxt = get_next_task(tasks)
    assert nxt is not None
    assert nxt.pr_id == "PR-001"


def test_get_next_task_prefers_doing_over_todo(tmp_path: Path) -> None:
    content = """## PR-001: First
- Status: TODO
- Tasks file: tasks/PR-001.md
- Branch: pr-001-first

## PR-002: Second
- Status: DOING
- Tasks file: tasks/PR-002.md
- Branch: pr-002-second
"""
    tasks = parse_queue(_write_queue(tmp_path, content))
    nxt = get_next_task(tasks)
    assert nxt is not None
    assert nxt.pr_id == "PR-002"


def test_get_next_task_skips_todo_with_unmet_dependencies(tmp_path: Path) -> None:
    content = """## PR-001: First
- Status: TODO
- Tasks file: tasks/PR-001.md
- Branch: pr-001-first

## PR-002: Second
- Status: TODO
- Tasks file: tasks/PR-002.md
- Branch: pr-002-second
- Depends on: PR-001
"""
    tasks = parse_queue(_write_queue(tmp_path, content))
    nxt = get_next_task(tasks)
    assert nxt is not None
    assert nxt.pr_id == "PR-001"


def test_get_next_task_returns_none_when_all_done(tmp_path: Path) -> None:
    content = """## PR-001: First
- Status: DONE
- Tasks file: tasks/PR-001.md
- Branch: pr-001-first

## PR-002: Second
- Status: DONE
- Tasks file: tasks/PR-002.md
- Branch: pr-002-second
"""
    tasks = parse_queue(_write_queue(tmp_path, content))
    assert get_next_task(tasks) is None


def test_get_next_task_returns_todo_when_dependency_done(tmp_path: Path) -> None:
    tasks = parse_queue(_write_queue(tmp_path))
    nxt = get_next_task(tasks)
    assert nxt is not None
    assert nxt.pr_id == "PR-003"


def test_parse_real_queue_file() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    real_queue = repo_root / "tasks" / "QUEUE.md"
    tasks = parse_queue(str(real_queue))

    assert len(tasks) >= 3
    pr_ids = [task.pr_id for task in tasks]
    assert "PR-001" in pr_ids
    assert "PR-002" in pr_ids
    assert "PR-003" in pr_ids

    by_id = {task.pr_id: task for task in tasks}
    assert by_id["PR-003"].depends_on == ["PR-002"]
    assert by_id["PR-003"].branch == "pr-003-queue-parser"


def test_mark_task_done_standard_layout() -> None:
    content = (
        "## PR-001: first\n"
        "- Status: DOING\n"
        "- Tasks file: tasks/PR-001.md\n"
    )
    updated = mark_task_done(content, "PR-001")
    assert updated is not None
    assert "- Status: DONE\n" in updated
    assert "DOING" not in updated


def test_mark_task_done_ignores_other_tasks() -> None:
    content = (
        "## PR-001: first\n- Status: DOING\n\n"
        "## PR-002: second\n- Status: TODO\n"
    )
    updated = mark_task_done(content, "PR-001")
    assert updated is not None
    assert "## PR-001: first\n- Status: DONE\n" in updated
    assert "## PR-002: second\n- Status: TODO\n" in updated


def test_mark_task_done_handles_flexible_field_order() -> None:
    """Status line can come after Branch/Depends on/Tasks file."""
    content = (
        "## PR-030: swap layout\n"
        "- Branch: pr-030-swap\n"
        "- Tasks file: tasks/PR-030.md\n"
        "- Depends on: PR-029\n"
        "- Status: TODO\n"
    )
    updated = mark_task_done(content, "PR-030")
    assert updated is not None
    assert "- Status: DONE\n" in updated
    # Other fields untouched
    assert "- Branch: pr-030-swap\n" in updated
    assert "- Depends on: PR-029\n" in updated


def test_mark_task_done_case_insensitive_key_and_value() -> None:
    content = "## PR-001: first\n-  status:  doing\n"
    updated = mark_task_done(content, "PR-001")
    assert updated is not None
    # Prefix (including extra spacing) preserved, value replaced with DONE.
    assert "-  status:  DONE" in updated


def test_mark_task_done_returns_none_when_already_done() -> None:
    content = "## PR-001: first\n- Status: DONE\n"
    assert mark_task_done(content, "PR-001") is None


def test_mark_task_done_clears_malformed_status() -> None:
    """parse_queue_text coerces unknown status values to TODO at
    selection time, so mark_task_done must also clear them — otherwise
    a merged task with a malformed status silently stays selectable."""
    content = "## PR-001: first\n- Status: TODO,\n"
    updated = mark_task_done(content, "PR-001")
    assert updated is not None
    assert "- Status: DONE" in updated
    assert "TODO," not in updated


def test_mark_task_done_clears_arbitrary_non_done_status() -> None:
    content = "## PR-001: first\n- Status: WORKING\n"
    updated = mark_task_done(content, "PR-001")
    assert updated is not None
    assert "- Status: DONE" in updated
    assert "WORKING" not in updated


def test_mark_task_done_rewrites_every_status_line_in_section() -> None:
    """parse_queue_text walks every field line so later `- Status:` lines
    override earlier ones. If a malformed section has DONE followed by
    TODO, the parser selects the task; mark_task_done must flip both
    lines so no stale status remains selectable."""
    content = (
        "## PR-001: first\n"
        "- Status: DONE\n"
        "- Status: TODO\n"
    )
    updated = mark_task_done(content, "PR-001")
    assert updated is not None
    # Both status lines must now read DONE.
    assert updated.count("- Status: DONE\n") == 2
    assert "TODO" not in updated


def test_mark_task_done_returns_none_when_every_status_already_done() -> None:
    content = (
        "## PR-001: first\n"
        "- Status: DONE\n"
        "- Status: DONE\n"
    )
    assert mark_task_done(content, "PR-001") is None


def test_mark_task_done_rewrites_decorated_done_value() -> None:
    """parse_queue_text reads the full value and falls back to TODO
    when the token is not a TaskStatus member (e.g. "DONE # merged"
    becomes TODO at selection time). mark_task_done must not treat
    those decorated values as already DONE."""
    content = "## PR-001: first\n- Status: DONE # merged\n"
    updated = mark_task_done(content, "PR-001")
    assert updated is not None
    assert "- Status: DONE\n" in updated
    assert "# merged" not in updated


def test_mark_task_done_rewrites_blank_status_line() -> None:
    """parse_queue_text falls back to TODO for a blank `- Status:` line,
    so the task is still selectable. mark_task_done must match and
    rewrite those lines too, not only lines with a non-whitespace
    token after the colon."""
    content = "## PR-001: first\n- Status:\n"
    updated = mark_task_done(content, "PR-001")
    assert updated is not None
    assert "DONE" in updated


def test_mark_task_done_rewrites_whitespace_only_status_line() -> None:
    content = "## PR-001: first\n- Status:   \n"
    updated = mark_task_done(content, "PR-001")
    assert updated is not None
    assert "DONE" in updated


def test_mark_task_done_returns_none_when_task_missing() -> None:
    content = "## PR-001: first\n- Status: DOING\n"
    assert mark_task_done(content, "PR-999") is None


def test_mark_task_done_stops_at_next_header() -> None:
    """Do not cross into a sibling task's section when the target has
    no status line in its own section."""
    content = (
        "## PR-001: first\n"
        "- Branch: pr-001\n"
        "\n"
        "## PR-002: second\n"
        "- Status: TODO\n"
    )
    assert mark_task_done(content, "PR-001") is None


def test_unknown_status_defaults_to_todo_with_warning(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """Unknown status values silently coerced to TODO masks queue
    authoring bugs. Emit a warning so operators see malformed entries
    instead of the runner quietly treating them as selectable."""
    content = (
        "## PR-050: weird status\n"
        "- Status: INVALID\n"
        "- Tasks file: tasks/PR-050.md\n"
        "- Branch: pr-050-weird\n"
    )
    with caplog.at_level(logging.WARNING, logger="src.queue_parser"):
        tasks = parse_queue(_write_queue(tmp_path, content))

    assert len(tasks) == 1
    assert tasks[0].status == TaskStatus.TODO
    assert any(
        "Unknown status" in rec.message and "PR-050" in rec.message
        for rec in caplog.records
    )


# ---------------------------------------------------------------------------
# Strict validation tests
# ---------------------------------------------------------------------------

VALID_QUEUE = """\
## PR-001: Bootstrap
- Status: DONE
- Tasks file: tasks/PR-001.md
- Branch: pr-001-bootstrap

## PR-002: Models
- Status: TODO
- Tasks file: tasks/PR-002.md
- Branch: pr-002-models
- Depends on: PR-001

## PR-003: Parser
- Status: TODO
- Tasks file: tasks/PR-003.md
- Branch: pr-003-parser
- Depends on: PR-002
"""


def test_validate_accepts_valid_queue() -> None:
    tasks = parse_queue_text(VALID_QUEUE, strict=True)
    assert len(tasks) == 3


def test_validate_rejects_duplicate_pr_id() -> None:
    content = """\
## PR-001: First
- Status: DONE
- Branch: pr-001-first

## PR-001: Duplicate
- Status: TODO
- Branch: pr-001-dup
"""
    with pytest.raises(QueueValidationError, match="duplicate pr_id"):
        parse_queue_text(content, strict=True)


def test_validate_rejects_duplicate_branch() -> None:
    content = """\
## PR-001: First
- Status: DONE
- Branch: shared-branch

## PR-002: Second
- Status: TODO
- Branch: shared-branch
"""
    with pytest.raises(QueueValidationError, match="duplicate branch"):
        parse_queue_text(content, strict=True)


def test_validate_rejects_missing_dependency() -> None:
    content = """\
## PR-001: First
- Status: DONE
- Branch: pr-001

## PR-002: Second
- Status: TODO
- Branch: pr-002
- Depends on: PR-999
"""
    with pytest.raises(QueueValidationError, match="unknown task"):
        parse_queue_text(content, strict=True)


def test_validate_rejects_simple_cycle() -> None:
    content = """\
## PR-A: First
- Status: TODO
- Branch: pr-a
- Depends on: PR-B

## PR-B: Second
- Status: TODO
- Branch: pr-b
- Depends on: PR-A
"""
    with pytest.raises(QueueValidationError, match="dependency cycle"):
        parse_queue_text(content, strict=True)


def test_validate_rejects_longer_cycle() -> None:
    content = """\
## PR-A: First
- Status: TODO
- Branch: pr-a
- Depends on: PR-B

## PR-B: Second
- Status: TODO
- Branch: pr-b
- Depends on: PR-C

## PR-C: Third
- Status: TODO
- Branch: pr-c
- Depends on: PR-A
"""
    with pytest.raises(QueueValidationError, match="dependency cycle"):
        parse_queue_text(content, strict=True)


def test_parse_queue_strict_rejects_unknown_status(tmp_path: Path) -> None:
    content = """\
## PR-050: Weird
- Status: INPROGRESS
- Branch: pr-050
"""
    queue_path = tmp_path / "QUEUE.md"
    queue_path.write_text(content, encoding="utf-8")
    with pytest.raises(QueueValidationError, match="unknown status"):
        parse_queue(str(queue_path), strict=True)


def test_parse_queue_non_strict_degrades_unknown_status_to_todo(
    tmp_path: Path,
) -> None:
    content = """\
## PR-050: Weird
- Status: INPROGRESS
- Branch: pr-050
"""
    queue_path = tmp_path / "QUEUE.md"
    queue_path.write_text(content, encoding="utf-8")
    tasks = parse_queue(str(queue_path), strict=False)
    assert len(tasks) == 1
    assert tasks[0].status == TaskStatus.TODO
