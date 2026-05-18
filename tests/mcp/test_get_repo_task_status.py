"""Tests for the ``get_repo_task_status`` MCP tool (PR-338)."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pytest


def _write_spec(tasks_dir: Path, name: str, status: str | None) -> None:
    if status is None:
        body = f"# {name[: -len('.md')]}: stub\n"
    else:
        body = (
            f"---\nstatus: {status}\n---\n\n"
            f"# {name[: -len('.md')]}: stub\n"
        )
    (tasks_dir / name).write_text(body, encoding="utf-8")


def test_returns_status_map_for_repo(tmp_path):
    from src.mcp.tools import functional

    fake_root = tmp_path / "data" / "repos"
    tasks = fake_root / "owner__repo" / "tasks"
    tasks.mkdir(parents=True)
    _write_spec(tasks, "PR-001.md", "DONE")
    _write_spec(tasks, "PR-002.md", "TODO")
    _write_spec(tasks, "PR-003.md", "ERROR")

    with patch.object(functional, "_REPOS_ROOT", fake_root):
        result = functional.get_repo_task_status("owner__repo")

    assert result == {"PR-001": "DONE", "PR-002": "TODO", "PR-003": "ERROR"}


def test_returns_todo_for_missing_status_frontmatter(tmp_path):
    from src.mcp.tools import functional

    fake_root = tmp_path / "data" / "repos"
    tasks = fake_root / "owner__repo" / "tasks"
    tasks.mkdir(parents=True)
    _write_spec(tasks, "PR-010.md", None)

    with patch.object(functional, "_REPOS_ROOT", fake_root):
        result = functional.get_repo_task_status("owner__repo")

    assert result == {"PR-010": "TODO"}


def test_returns_empty_for_missing_repo(tmp_path):
    from src.mcp.tools import functional

    fake_root = tmp_path / "data" / "repos"
    fake_root.mkdir(parents=True)

    with patch.object(functional, "_REPOS_ROOT", fake_root):
        assert functional.get_repo_task_status("owner__missing") == {}


def test_returns_empty_for_repo_without_tasks_dir(tmp_path):
    from src.mcp.tools import functional

    fake_root = tmp_path / "data" / "repos"
    (fake_root / "owner__repo").mkdir(parents=True)

    with patch.object(functional, "_REPOS_ROOT", fake_root):
        assert functional.get_repo_task_status("owner__repo") == {}


def test_uppercase_normalization(tmp_path):
    from src.mcp.tools import functional

    fake_root = tmp_path / "data" / "repos"
    tasks = fake_root / "owner__repo" / "tasks"
    tasks.mkdir(parents=True)
    _write_spec(tasks, "PR-050.md", "done")

    with patch.object(functional, "_REPOS_ROOT", fake_root):
        result = functional.get_repo_task_status("owner__repo")

    assert result == {"PR-050": "DONE"}


def test_invalid_slug_raises_value_error():
    from src.mcp.tools.functional import get_repo_task_status

    for bad in (" owner__repo", "owner __repo", 'owner"__repo', "owner__re po"):
        with pytest.raises(ValueError, match="Invalid repo_slug"):
            get_repo_task_status(bad)


def test_canonical_slug_pattern_accepts_owner_repo(tmp_path):
    from src.mcp.tools import functional

    fake_root = tmp_path / "data" / "repos"
    tasks = fake_root / "AlexBomber12__pipeline-orchestrator" / "tasks"
    tasks.mkdir(parents=True)
    _write_spec(tasks, "PR-100.md", "TODO")

    with patch.object(functional, "_REPOS_ROOT", fake_root):
        result = functional.get_repo_task_status(
            "AlexBomber12__pipeline-orchestrator"
        )

    assert result == {"PR-100": "TODO"}


def test_canonical_slug_pattern_rejects_single_underscore():
    from src.mcp.tools.functional import get_repo_task_status

    with pytest.raises(ValueError, match="Invalid repo_slug"):
        get_repo_task_status("owner_repo")


def test_handles_unreadable_file_with_fallback_to_todo(tmp_path):
    from src.mcp.tools import functional

    fake_root = tmp_path / "data" / "repos"
    tasks = fake_root / "owner__repo" / "tasks"
    tasks.mkdir(parents=True)
    _write_spec(tasks, "PR-200.md", "DONE")

    real_read_text = Path.read_text

    def flaky_read_text(self, *args, **kwargs):
        if self.name == "PR-200.md":
            raise OSError("simulated read failure")
        return real_read_text(self, *args, **kwargs)

    with patch.object(functional, "_REPOS_ROOT", fake_root), patch.object(
        Path, "read_text", flaky_read_text
    ):
        result = functional.get_repo_task_status("owner__repo")

    assert result == {"PR-200": "TODO"}


def test_skips_directories_matching_pr_glob(tmp_path):
    """A directory whose name matches ``PR-*.md`` must be ignored."""
    from src.mcp.tools import functional

    fake_root = tmp_path / "data" / "repos"
    tasks = fake_root / "owner__repo" / "tasks"
    tasks.mkdir(parents=True)
    _write_spec(tasks, "PR-300.md", "DONE")
    (tasks / "PR-999.md").mkdir()

    with patch.object(functional, "_REPOS_ROOT", fake_root):
        result = functional.get_repo_task_status("owner__repo")

    assert result == {"PR-300": "DONE"}


def test_empty_frontmatter_block_returns_todo(tmp_path):
    """Frontmatter that opens and closes with no ``status:`` line → TODO."""
    from src.mcp.tools import functional

    fake_root = tmp_path / "data" / "repos"
    tasks = fake_root / "owner__repo" / "tasks"
    tasks.mkdir(parents=True)
    (tasks / "PR-400.md").write_text(
        "---\nother: value\n---\n\n# PR-400: stub\n", encoding="utf-8"
    )

    with patch.object(functional, "_REPOS_ROOT", fake_root):
        result = functional.get_repo_task_status("owner__repo")

    assert result == {"PR-400": "TODO"}


def test_unterminated_frontmatter_with_unrecognized_status_returns_todo(tmp_path):
    """Frontmatter without a closing ``---`` and a junk status → TODO."""
    from src.mcp.tools import functional

    fake_root = tmp_path / "data" / "repos"
    tasks = fake_root / "owner__repo" / "tasks"
    tasks.mkdir(parents=True)
    (tasks / "PR-500.md").write_text(
        "---\nstatus: bogus\n# PR-500: stub\n", encoding="utf-8"
    )

    with patch.object(functional, "_REPOS_ROOT", fake_root):
        result = functional.get_repo_task_status("owner__repo")

    assert result == {"PR-500": "TODO"}


def test_unterminated_frontmatter_without_status_returns_todo(tmp_path):
    """Frontmatter opened but never closed and no ``status:`` line → TODO."""
    from src.mcp.tools import functional

    fake_root = tmp_path / "data" / "repos"
    tasks = fake_root / "owner__repo" / "tasks"
    tasks.mkdir(parents=True)
    (tasks / "PR-600.md").write_text(
        "---\nother: value\nmore: stuff\n", encoding="utf-8"
    )

    with patch.object(functional, "_REPOS_ROOT", fake_root):
        result = functional.get_repo_task_status("owner__repo")

    assert result == {"PR-600": "TODO"}


def test_closed_frontmatter_with_unrecognized_status_returns_todo(tmp_path):
    """Closed frontmatter whose status value is not in the canonical
    set (TODO/DONE/ERROR) falls back to TODO.
    """
    from src.mcp.tools import functional

    fake_root = tmp_path / "data" / "repos"
    tasks = fake_root / "owner__repo" / "tasks"
    tasks.mkdir(parents=True)
    (tasks / "PR-650.md").write_text(
        "---\nstatus: bogus\n---\n\n# PR-650: stub\n", encoding="utf-8"
    )

    with patch.object(functional, "_REPOS_ROOT", fake_root):
        result = functional.get_repo_task_status("owner__repo")

    assert result == {"PR-650": "TODO"}


def test_unterminated_frontmatter_with_valid_status_returns_todo(tmp_path):
    """Unterminated ``---`` block with a canonical status must still
    fall back to TODO so MCP clients do not see ``DONE`` for malformed
    files. Mirrors ``parse_task_header`` which ignores the status line
    when the frontmatter block is not closed.
    """
    from src.mcp.tools import functional

    fake_root = tmp_path / "data" / "repos"
    tasks = fake_root / "owner__repo" / "tasks"
    tasks.mkdir(parents=True)
    (tasks / "PR-700.md").write_text(
        "---\nstatus: DONE\n# PR-700: stub\n", encoding="utf-8"
    )

    with patch.object(functional, "_REPOS_ROOT", fake_root):
        result = functional.get_repo_task_status("owner__repo")

    assert result == {"PR-700": "TODO"}


def test_multiple_status_lines_uses_last_value(tmp_path):
    """When the frontmatter has multiple ``status:`` lines, the LAST
    one wins, matching ``parse_task_header`` in ``src/queue_parser.py``.
    """
    from src.mcp.tools import functional

    fake_root = tmp_path / "data" / "repos"
    tasks = fake_root / "owner__repo" / "tasks"
    tasks.mkdir(parents=True)
    (tasks / "PR-800.md").write_text(
        "---\nstatus: TODO\nstatus: DONE\n---\n\n# PR-800: stub\n",
        encoding="utf-8",
    )

    with patch.object(functional, "_REPOS_ROOT", fake_root):
        result = functional.get_repo_task_status("owner__repo")

    assert result == {"PR-800": "DONE"}


def test_handles_non_utf8_file_with_fallback_to_todo(tmp_path):
    """A non-UTF8 task file must not break the whole repo scan; the
    individual file falls back to TODO instead of raising.
    """
    from src.mcp.tools import functional

    fake_root = tmp_path / "data" / "repos"
    tasks = fake_root / "owner__repo" / "tasks"
    tasks.mkdir(parents=True)
    _write_spec(tasks, "PR-900.md", "DONE")
    (tasks / "PR-901.md").write_bytes(b"---\nstatus: \xff\xfe broken\n---\n")

    with patch.object(functional, "_REPOS_ROOT", fake_root):
        result = functional.get_repo_task_status("owner__repo")

    assert result == {"PR-900": "DONE", "PR-901": "TODO"}


def test_inline_hash_without_whitespace_is_not_a_comment(tmp_path):
    """``status: DONE#merged`` (no whitespace before ``#``) must not be
    truncated to ``DONE``. The canonical normalization in
    ``src/queue_parser.py`` only treats ``#`` as an inline comment
    when it sits at the start of the value or follows whitespace, so
    a glued-on ``#merged`` makes the whole token unrecognized and the
    file falls back to TODO.
    """
    from src.mcp.tools import functional

    fake_root = tmp_path / "data" / "repos"
    tasks = fake_root / "owner__repo" / "tasks"
    tasks.mkdir(parents=True)
    (tasks / "PR-850.md").write_text(
        "---\nstatus: DONE#merged\n---\n\n# PR-850: stub\n",
        encoding="utf-8",
    )

    with patch.object(functional, "_REPOS_ROOT", fake_root):
        result = functional.get_repo_task_status("owner__repo")

    assert result == {"PR-850": "TODO"}


def test_inline_hash_after_whitespace_is_stripped_as_comment(tmp_path):
    """``status: DONE # merged`` keeps the canonical ``DONE`` value;
    the whitespace-preceded ``#`` is treated as an inline comment.
    """
    from src.mcp.tools import functional

    fake_root = tmp_path / "data" / "repos"
    tasks = fake_root / "owner__repo" / "tasks"
    tasks.mkdir(parents=True)
    (tasks / "PR-851.md").write_text(
        "---\nstatus: DONE # merged\n---\n\n# PR-851: stub\n",
        encoding="utf-8",
    )

    with patch.object(functional, "_REPOS_ROOT", fake_root):
        result = functional.get_repo_task_status("owner__repo")

    assert result == {"PR-851": "DONE"}


def test_quoted_status_preserves_inline_hash(tmp_path):
    """A quoted ``status:`` value preserves an embedded ``#`` rather
    than treating it as a comment. ``"DONE#merged"`` therefore stays
    unrecognized and falls back to TODO.
    """
    from src.mcp.tools import functional

    fake_root = tmp_path / "data" / "repos"
    tasks = fake_root / "owner__repo" / "tasks"
    tasks.mkdir(parents=True)
    (tasks / "PR-852.md").write_text(
        '---\nstatus: "DONE#merged"\n---\n\n# PR-852: stub\n',
        encoding="utf-8",
    )

    with patch.object(functional, "_REPOS_ROOT", fake_root):
        result = functional.get_repo_task_status("owner__repo")

    assert result == {"PR-852": "TODO"}


def test_get_repo_task_status_registered_with_mcp_server():
    """Tool appears in the MCP server's tool registry after import."""
    import asyncio

    from src.mcp.server import mcp
    from src.mcp.tools import functional  # noqa: F401

    tools = asyncio.run(mcp.list_tools())
    tool_names = {t.name for t in tools}
    assert "get_repo_task_status" in tool_names
