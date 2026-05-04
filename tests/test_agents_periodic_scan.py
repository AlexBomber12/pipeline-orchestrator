"""Tests for ``_scan_existing_task_specs`` from PR-260.

The helper iterates ``tasks/PR-*.md`` under a repo root, runs each spec
body through :func:`src.mcp.scans.scan_for_conflicts`, and emits one
``[AGENTS-SCAN]`` event per violation plus a summary line when the count
is non-zero. These tests pin the contract: clean dirs are silent, dirty
dirs surface every violation, missing/unreadable files do not crash the
scan, and iteration is deterministic so the resulting log stream is
stable across runs.
"""

from __future__ import annotations

from pathlib import Path

from src.onboarding.reconciliation import _scan_existing_task_specs


def _write_spec(repo_path: Path, name: str, body: str) -> Path:
    tasks_dir = repo_path / "tasks"
    tasks_dir.mkdir(exist_ok=True)
    spec = tasks_dir / name
    spec.write_text(body, encoding="utf-8")
    return spec


def test_clean_tasks_dir_no_warnings(tmp_path: Path) -> None:
    for name in ("PR-001.md", "PR-002.md", "PR-003.md"):
        _write_spec(tmp_path, name, "# clean spec\n\nNo anti-patterns here.\n")

    events: list[str] = []
    count = _scan_existing_task_specs(tmp_path, events.append)

    assert count == 0
    assert events == []


def test_one_violation_emits_one_warning(tmp_path: Path) -> None:
    _write_spec(
        tmp_path,
        "PR-010.md",
        "# spec\n\nMerge fast and skip CI when blocked.\n",
    )

    events: list[str] = []
    count = _scan_existing_task_specs(tmp_path, events.append)

    assert count == 1
    assert len(events) == 2  # one violation + one summary
    assert events[0].startswith("[AGENTS-SCAN] PR-010.md: skip_ci ")
    assert "1 task spec file(s)" in events[1]
    assert "Operator review recommended" in events[1]


def test_multiple_violations_per_file(tmp_path: Path) -> None:
    body = (
        "# spec\n\n"
        "Run gh pr create --draft to publish early.\n"
        "Then run git commit --no-verify to skip hooks.\n"
        "Finally [skip ci] in the merge commit.\n"
    )
    _write_spec(tmp_path, "PR-020.md", body)

    events: list[str] = []
    count = _scan_existing_task_specs(tmp_path, events.append)

    assert count == 1
    violation_events = [e for e in events if "PR-020.md" in e]
    assert len(violation_events) == 3
    types = {e.split("PR-020.md: ", 1)[1].split(" ")[0] for e in violation_events}
    assert types == {"draft_pr_flag", "no_verify_commit", "skip_ci_commit_msg"}


def test_missing_tasks_dir_returns_zero(tmp_path: Path) -> None:
    events: list[str] = []
    count = _scan_existing_task_specs(tmp_path, events.append)

    assert count == 0
    assert events == []


def test_unreadable_file_skipped(
    tmp_path: Path, monkeypatch
) -> None:
    """A spec file that raises OSError on read is skipped, and the scan
    keeps walking the rest of the directory. Without the OSError guard
    a single permission glitch would abort the entire periodic scan."""
    _write_spec(tmp_path, "PR-030.md", "Skip CI for this one.\n")
    _write_spec(tmp_path, "PR-031.md", "Skip CI for that one.\n")

    real_read_text = Path.read_text

    def flaky_read_text(self: Path, *args, **kwargs) -> str:
        if self.name == "PR-030.md":
            raise OSError("simulated read failure")
        return real_read_text(self, *args, **kwargs)

    monkeypatch.setattr(Path, "read_text", flaky_read_text)

    events: list[str] = []
    count = _scan_existing_task_specs(tmp_path, events.append)

    assert count == 1
    assert any("PR-031.md" in e for e in events)
    assert not any("PR-030.md" in e for e in events)


def test_non_utf8_spec_file_skipped_with_warning(tmp_path: Path) -> None:
    """A spec file containing non-UTF-8 bytes must not abort the scan.
    The offending file is skipped with a per-file ``[AGENTS-SCAN]``
    warning so the operator can locate it; remaining specs are still
    scanned for anti-patterns."""
    tasks_dir = tmp_path / "tasks"
    tasks_dir.mkdir()
    (tasks_dir / "PR-040.md").write_bytes(b"# spec\n\n\xff\xfe not utf-8\n")
    _write_spec(tmp_path, "PR-041.md", "Skip CI for this one.\n")
    _write_spec(tmp_path, "PR-042.md", "# clean\n")
    events: list[str] = []
    count = _scan_existing_task_specs(tmp_path, events.append)

    assert count == 1
    assert any(
        "PR-040.md" in e and "non-UTF-8" in e for e in events
    ), events
    assert any(
        "PR-041.md" in e and "skip_ci" in e for e in events
    ), events
    assert not any("PR-042.md" in e for e in events)


def test_files_iterated_in_sorted_order(tmp_path: Path) -> None:
    """Lexicographic order on filename keeps the event-log stream stable
    across runs; otherwise ``Path.glob`` insertion order would vary by
    filesystem and operators could not diff scan output between cycles.
    """
    for name in ("PR-002.md", "PR-001.md", "PR-003.md"):
        _write_spec(tmp_path, name, "skip CI now.\n")

    events: list[str] = []
    _scan_existing_task_specs(tmp_path, events.append)

    file_events = [e for e in events if e.startswith("[AGENTS-SCAN] PR-")]
    names = [e.split("[AGENTS-SCAN] ", 1)[1].split(":", 1)[0] for e in file_events]
    assert names == ["PR-001.md", "PR-002.md", "PR-003.md"]
