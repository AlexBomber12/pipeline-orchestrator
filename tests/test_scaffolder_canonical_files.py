"""Tests for PR-242: scaffolder overwrites CLAUDE.md and places SKILL.md."""

from __future__ import annotations

from src.daemon.scaffolder import (
    _CLAUDE_MD_CANONICAL,
    _SKILL_MD_CANONICAL,
    _SKILL_MD_REL_PATH,
    _ensure_canonical_file,
)


def test_overwrites_arbitrary_existing_claude_md(tmp_path):
    """User-authored CLAUDE.md is replaced with canonical redirect."""
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / "CLAUDE.md").write_text(
        "# Project notes\n\nUse storcli for RAID inspection.\n"
    )

    changed = _ensure_canonical_file(repo, "CLAUDE.md", _CLAUDE_MD_CANONICAL)

    assert changed is True
    assert (repo / "CLAUDE.md").read_text() == _CLAUDE_MD_CANONICAL


def test_does_not_rewrite_already_canonical_claude_md(tmp_path):
    """Repos with the canonical redirect already in place are not touched."""
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / "CLAUDE.md").write_text(_CLAUDE_MD_CANONICAL)

    changed = _ensure_canonical_file(repo, "CLAUDE.md", _CLAUDE_MD_CANONICAL)

    assert changed is False


def test_creates_missing_claude_md(tmp_path):
    repo = tmp_path / "repo"
    repo.mkdir()

    changed = _ensure_canonical_file(repo, "CLAUDE.md", _CLAUDE_MD_CANONICAL)

    assert changed is True
    assert (repo / "CLAUDE.md").read_text() == _CLAUDE_MD_CANONICAL


def test_creates_skill_md_with_nested_directories(tmp_path):
    """SKILL.md placement creates .claude/skills/orch-context/ if missing."""
    repo = tmp_path / "repo"
    repo.mkdir()

    changed = _ensure_canonical_file(
        repo, _SKILL_MD_REL_PATH, _SKILL_MD_CANONICAL
    )

    assert changed is True
    skill = repo / ".claude" / "skills" / "orch-context" / "SKILL.md"
    assert skill.exists()
    assert skill.read_text() == _SKILL_MD_CANONICAL


def test_skill_md_canonical_content_includes_orientation():
    """v1 SKILL.md content orients coder to AGENTS.md and tasks/."""
    assert "AGENTS.md" in _SKILL_MD_CANONICAL
    assert "tasks/PR-" in _SKILL_MD_CANONICAL
    # MCP reference is forward-looking but should be present so the
    # eventual Sprint 13.5 update is additive, not breaking.
    assert "MCP" in _SKILL_MD_CANONICAL or "mcp" in _SKILL_MD_CANONICAL


def test_ensure_canonical_file_replaces_unreadable_existing_file(
    tmp_path, monkeypatch
):
    """An ``OSError`` while reading the existing file is treated as 'differs'."""
    repo = tmp_path / "repo"
    repo.mkdir()
    target = repo / "CLAUDE.md"
    target.write_text("garbage")

    real_read_text = target.__class__.read_text

    def explode(self, *args, **kwargs):
        if self == target:
            raise OSError("simulated unreadable file")
        return real_read_text(self, *args, **kwargs)

    monkeypatch.setattr(target.__class__, "read_text", explode)

    changed = _ensure_canonical_file(repo, "CLAUDE.md", _CLAUDE_MD_CANONICAL)

    assert changed is True
