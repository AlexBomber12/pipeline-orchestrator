"""Tests for src/queue_parser.py."""

from __future__ import annotations

from pathlib import Path

from src.models import TaskStatus
from src.queue_parser import get_next_task, mark_task_done, parse_queue

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
