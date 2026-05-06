"""Tests for src/daemon/scaffolder.py."""

from __future__ import annotations

import subprocess
from pathlib import Path
from typing import Any

import pytest
from src.daemon import scaffolder


class _FakeCompletedProcess:
    def __init__(
        self,
        args: list[str] | None = None,
        stdout: str = "",
        stderr: str = "",
        returncode: int = 0,
    ) -> None:
        self.args = args or []
        self.stdout = stdout
        self.stderr = stderr
        self.returncode = returncode


def _patch_git(
    monkeypatch: pytest.MonkeyPatch, *, synced: bool = False
) -> list[list[str]]:
    """Capture git subprocess calls issued by scaffolder.

    By default models a fresh-clone scenario where
    ``origin/{branch}`` does not yet exist, so the scaffolder's
    unpushed-commits probe decides the local branch needs to be
    pushed. Pass ``synced=True`` to model a fully sync'd repo where
    both the local tree and the remote already have every scaffolding
    file and the probe returns False (no push). ``HEAD`` is treated as
    born in both modes — the unborn-HEAD path is covered by its own
    dedicated tests with hand-rolled ``fake_run`` functions.
    """
    calls: list[list[str]] = []

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        calls.append(cmd)
        if cmd[:2] == ["git", "check-ignore"]:
            # Default: nothing in the patched git is gitignored.
            return _FakeCompletedProcess(args=cmd, returncode=1)
        if cmd[:3] == ["git", "rev-parse", "--verify"]:
            ref = cmd[-1]
            if ref.startswith("refs/remotes/origin/"):
                return _FakeCompletedProcess(
                    args=cmd, returncode=0 if synced else 1
                )
            # HEAD probe: always born in the default helper.
            return _FakeCompletedProcess(args=cmd, returncode=0)
        if cmd[:2] == ["git", "rev-list"]:
            # Reached only when origin ref exists. Synced mode reports
            # zero commits ahead so no push is issued.
            return _FakeCompletedProcess(
                args=cmd, returncode=0, stdout="0\n"
            )
        return _FakeCompletedProcess(args=cmd)

    monkeypatch.setattr(scaffolder.subprocess, "run", fake_run)
    return calls


def _init_empty_repo(tmp_path: Path) -> Path:
    """Create an empty directory stand-in for a freshly cloned repo."""
    repo = tmp_path / "sample-repo"
    repo.mkdir()
    return repo


def _init_scaffolded_repo(tmp_path: Path) -> Path:
    """Create a repo with the baseline scaffolding already present."""
    repo = _init_empty_repo(tmp_path)
    (repo / "AGENTS.md").write_text("# AGENTS\n")
    (repo / "CLAUDE.md").write_text(scaffolder._CLAUDE_MD_CANONICAL)
    skill = repo / ".claude" / "skills" / "orch-context" / "SKILL.md"
    skill.parent.mkdir(parents=True)
    skill.write_text(scaffolder._SKILL_MD_CANONICAL)
    (repo / "tasks").mkdir()
    (repo / "tasks" / "QUEUE.md").write_text("# Task Queue\n")
    (repo / "scripts").mkdir()
    (repo / "scripts" / "ci.sh").write_text("#!/usr/bin/env bash\n")
    (repo / "scripts" / "ci.sh").chmod(0o755)
    (repo / "scripts" / "make-review-artifacts.sh").write_text(
        "#!/usr/bin/env bash\n"
    )
    (repo / "scripts" / "make-review-artifacts.sh").chmod(0o755)
    (repo / "artifacts").mkdir()
    (repo / ".gitignore").write_text("artifacts/\ntasks/QUEUE.md\n")
    return repo


def test_scaffold_repo_creates_all_files_when_empty(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo = _init_empty_repo(tmp_path)
    calls = _patch_git(monkeypatch)

    actions = scaffolder.scaffold_repo(str(repo), "main")

    # All baseline files must be on disk after scaffolding.
    assert (repo / "AGENTS.md").exists()
    assert (repo / "tasks").is_dir()
    assert not (repo / "tasks" / "QUEUE.md").exists()
    assert (repo / "scripts").is_dir()
    assert (repo / "scripts" / "ci.sh").exists()
    assert (repo / "scripts" / "make-review-artifacts.sh").exists()
    assert (repo / "artifacts").is_dir()
    assert (repo / ".gitignore").exists()
    gitignore_lines = (repo / ".gitignore").read_text().splitlines()
    assert "artifacts/" in gitignore_lines
    assert "tasks/QUEUE.md" in gitignore_lines

    # Shell helpers must be executable so bash can run them directly.
    assert (repo / "scripts" / "ci.sh").stat().st_mode & 0o111
    assert (repo / "scripts" / "make-review-artifacts.sh").stat().st_mode & 0o111

    # Every tracked path (directories filtered out) should be reported as
    # an action so the caller can log it.
    for path in (
        "AGENTS.md",
        "scripts/ci.sh",
        "scripts/make-review-artifacts.sh",
        ".gitignore",
    ):
        assert path in actions

    # Checkout must be the first git call so the working tree reflects
    # the configured base branch before any file is inspected or written.
    assert calls[0] == ["git", "checkout", "main"]

    # git add must stage every concrete file we created.
    add_cmds = [cmd for cmd in calls if cmd[:2] == ["git", "add"]]
    assert len(add_cmds) == 1
    staged = add_cmds[0][2:]
    assert "AGENTS.md" in staged
    assert "scripts/ci.sh" in staged
    assert "scripts/make-review-artifacts.sh" in staged
    assert ".gitignore" in staged
    assert "tasks/QUEUE.md" not in staged
    assert "tasks/" not in staged
    assert "artifacts/" not in staged

    # Commit and push must follow add, in that order.
    subcommands = [cmd[1] for cmd in calls if cmd[0] == "git"]
    assert subcommands.index("add") < subcommands.index("commit")
    assert subcommands.index("commit") < subcommands.index("push")
    push_cmd = next(cmd for cmd in calls if cmd[:2] == ["git", "push"])
    assert push_cmd == ["git", "push", "origin", "main"]


def test_scaffold_repo_uses_configured_base_branch(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Scaffolding must check out and push to the configured base
    branch, not whatever branch ``git clone`` left ``HEAD`` on.

    On a fresh clone of a repo whose default branch differs from
    ``repo_config.branch`` (e.g. legacy ``master`` vs. configured
    ``develop``), pushing to the wrong branch would leave
    ``origin/{configured_branch}`` without ``tasks/QUEUE.md`` and break
    recovery/preflight logic on the next cycle.
    """
    repo = _init_empty_repo(tmp_path)
    calls = _patch_git(monkeypatch)

    scaffolder.scaffold_repo(str(repo), "develop")

    # The first git call must be a checkout of the configured branch so
    # nothing runs against whatever branch the clone landed on.
    assert calls[0] == ["git", "checkout", "develop"]
    # Push must target the same configured branch.
    push_cmd = next(cmd for cmd in calls if cmd[:2] == ["git", "push"])
    assert push_cmd == ["git", "push", "origin", "develop"]
    # Branch discovery via rev-parse is gone: we trust the caller's
    # branch argument and never consult HEAD.
    assert not any(
        cmd[:3] == ["git", "rev-parse", "--abbrev-ref"] for cmd in calls
    )


def test_scaffold_repo_preserves_existing_agents(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo = _init_empty_repo(tmp_path)
    existing = "# Custom AGENTS\n"
    (repo / "AGENTS.md").write_text(existing)
    _patch_git(monkeypatch)

    actions = scaffolder.scaffold_repo(str(repo), "main")

    assert (repo / "AGENTS.md").read_text() == existing
    assert "AGENTS.md" not in actions


def test_scaffold_repo_backfills_agents_when_only_claude_md_exists(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A repo with CLAUDE.md but no AGENTS.md must gain AGENTS.md so the
    canonical CLAUDE.md redirect points at a real file. Skipping the
    backfill (the prior PR-242 behaviour) left CLAUDE.md saying "Read
    AGENTS.md" while AGENTS.md did not exist on disk, breaking the
    coder's first task pick.
    """
    repo = _init_empty_repo(tmp_path)
    (repo / "CLAUDE.md").write_text("# Project rules\n")
    _patch_git(monkeypatch)

    actions = scaffolder.scaffold_repo(str(repo), "main")

    assert (repo / "AGENTS.md").exists()
    assert "AGENTS.md" in actions
    # CLAUDE.md was overwritten to the canonical redirect.
    assert (repo / "CLAUDE.md").read_text() == scaffolder._CLAUDE_MD_CANONICAL


def test_scaffold_repo_stages_all_when_check_ignore_times_out(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A ``git check-ignore`` timeout must NOT abort scaffolding; the
    scaffolder falls back to "assume no paths ignored" so the cycle can
    still publish AGENTS.md, ci.sh, and friends. Aborting on a probe
    timeout would leave the runner unable to onboard a repo whose git
    is briefly under lock contention.
    """
    repo = _init_empty_repo(tmp_path)
    calls: list[list[str]] = []

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        calls.append(cmd)
        if cmd[:2] == ["git", "check-ignore"]:
            raise subprocess.TimeoutExpired(cmd, kwargs.get("timeout", 0))
        if cmd[:3] == ["git", "rev-parse", "--verify"]:
            ref = cmd[-1]
            if ref.startswith("refs/remotes/origin/"):
                return _FakeCompletedProcess(args=cmd, returncode=1)
            return _FakeCompletedProcess(args=cmd, returncode=0)
        return _FakeCompletedProcess(args=cmd)

    monkeypatch.setattr(scaffolder.subprocess, "run", fake_run)

    scaffolder.scaffold_repo(str(repo), "main")

    add_cmds = [cmd for cmd in calls if cmd[:2] == ["git", "add"]]
    assert len(add_cmds) == 1
    staged = add_cmds[0][2:]
    # The fall-back assumption is "nothing ignored", so SKILL.md is
    # staged alongside the rest. The point of the test is that the
    # scaffolder did not abort.
    assert scaffolder._SKILL_MD_REL_PATH in staged


def test_scaffold_repo_stages_all_when_check_ignore_fails_with_unexpected_rc(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A non-{0,1} exit from ``git check-ignore`` (e.g. rc=128 for a
    fatal error) is treated as 'cannot decide' and falls back to
    'no paths ignored', so a transient git failure cannot abort
    onboarding.
    """
    repo = _init_empty_repo(tmp_path)
    calls: list[list[str]] = []

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        calls.append(cmd)
        if cmd[:2] == ["git", "check-ignore"]:
            return _FakeCompletedProcess(
                args=cmd, returncode=128, stderr="fatal: bad index"
            )
        if cmd[:3] == ["git", "rev-parse", "--verify"]:
            ref = cmd[-1]
            if ref.startswith("refs/remotes/origin/"):
                return _FakeCompletedProcess(args=cmd, returncode=1)
            return _FakeCompletedProcess(args=cmd, returncode=0)
        return _FakeCompletedProcess(args=cmd)

    monkeypatch.setattr(scaffolder.subprocess, "run", fake_run)

    scaffolder.scaffold_repo(str(repo), "main")

    add_cmds = [cmd for cmd in calls if cmd[:2] == ["git", "add"]]
    assert len(add_cmds) == 1
    staged = add_cmds[0][2:]
    assert "AGENTS.md" in staged


def test_scaffold_repo_skips_staging_gitignored_skill_md(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """When the repo gitignores ``.claude/`` (a common pattern), the
    scaffolder must place SKILL.md on disk for local Claude Code use
    but skip it from ``git add``. Staging a gitignored path under
    ``check=True`` would fail and abort onboarding entirely.
    """
    repo = _init_empty_repo(tmp_path)
    (repo / ".gitignore").write_text(".claude/\n")

    calls: list[list[str]] = []

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        calls.append(cmd)
        if cmd[:2] == ["git", "check-ignore"]:
            paths = cmd[cmd.index("--") + 1:]
            ignored = [p for p in paths if p.startswith(".claude/")]
            return _FakeCompletedProcess(
                args=cmd,
                returncode=0 if ignored else 1,
                stdout="\n".join(ignored) + ("\n" if ignored else ""),
            )
        if cmd[:3] == ["git", "rev-parse", "--verify"]:
            ref = cmd[-1]
            if ref.startswith("refs/remotes/origin/"):
                return _FakeCompletedProcess(args=cmd, returncode=1)
            return _FakeCompletedProcess(args=cmd, returncode=0)
        return _FakeCompletedProcess(args=cmd)

    monkeypatch.setattr(scaffolder.subprocess, "run", fake_run)

    actions = scaffolder.scaffold_repo(str(repo), "main")

    # SKILL.md was placed on disk for local use and reported in actions.
    skill_rel = scaffolder._SKILL_MD_REL_PATH
    assert (repo / skill_rel).exists()
    assert skill_rel in actions

    # But it was NOT included in `git add`.
    add_cmds = [cmd for cmd in calls if cmd[:2] == ["git", "add"]]
    assert len(add_cmds) == 1, "scaffolder should still stage the other files"
    staged = add_cmds[0][2:]
    assert skill_rel not in staged
    # Non-ignored paths still got staged so the rest of the scaffolding
    # commit lands.
    assert "AGENTS.md" in staged
    assert "scripts/ci.sh" in staged


def test_ensure_claude_md_returns_false_for_missing_repo(tmp_path: Path) -> None:
    missing = tmp_path / "missing-repo"

    assert scaffolder.ensure_claude_md(str(missing), "main") is False


def test_ensure_claude_md_skips_when_remote_already_has_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo = _init_empty_repo(tmp_path)
    calls: list[tuple[str, ...]] = []

    def fake_run_git(repo_path: str, *args: str, timeout: int = 30) -> _FakeCompletedProcess:
        del repo_path, timeout
        calls.append(args)
        return _FakeCompletedProcess(args=["git", *args])

    monkeypatch.setattr(scaffolder, "_run_git", fake_run_git)

    assert scaffolder.ensure_claude_md(str(repo), "main") is False
    assert calls == [("cat-file", "-e", "origin/main:CLAUDE.md")]
    assert not (repo / "CLAUDE.md").exists()


def test_ensure_claude_md_skips_when_file_already_exists(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo = _init_empty_repo(tmp_path)
    (repo / "CLAUDE.md").write_text("existing\n")

    def fail_run_git(repo_path: str, *args: str, timeout: int = 30) -> _FakeCompletedProcess:
        raise AssertionError(f"_run_git should not be called: {repo_path} {args} {timeout}")

    monkeypatch.setattr(scaffolder, "_run_git", fail_run_git)

    assert scaffolder.ensure_claude_md(str(repo), "main") is False


def test_ensure_claude_md_creates_and_pushes_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo = _init_empty_repo(tmp_path)
    calls: list[tuple[str, ...]] = []

    def fake_run_git(repo_path: str, *args: str, timeout: int = 30) -> _FakeCompletedProcess:
        del timeout
        calls.append(args)
        if args[:2] == ("cat-file", "-e"):
            raise subprocess.CalledProcessError(1, ["git", *args])
        if args[:3] == ("reset", "--hard", "origin/main"):
            assert repo_path == str(repo)
        return _FakeCompletedProcess(args=["git", *args])

    monkeypatch.setattr(scaffolder, "_run_git", fake_run_git)

    assert scaffolder.ensure_claude_md(str(repo), "main") is True
    assert (repo / "CLAUDE.md").exists()
    assert calls == [
        ("cat-file", "-e", "origin/main:CLAUDE.md"),
        ("checkout", "main"),
        ("reset", "--hard", "origin/main"),
        ("add", "CLAUDE.md"),
        ("commit", "-m", "chore: backfill CLAUDE.md"),
        ("push", "origin", "main"),
    ]


def test_ensure_claude_md_returns_false_when_reset_restores_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo = _init_empty_repo(tmp_path)

    def fake_run_git(repo_path: str, *args: str, timeout: int = 30) -> _FakeCompletedProcess:
        del timeout
        if args[:2] == ("cat-file", "-e"):
            raise subprocess.CalledProcessError(1, ["git", *args])
        if args[:3] == ("reset", "--hard", "origin/main"):
            Path(repo_path, "CLAUDE.md").write_text("restored from origin\n")
        return _FakeCompletedProcess(args=["git", *args])

    monkeypatch.setattr(scaffolder, "_run_git", fake_run_git)

    assert scaffolder.ensure_claude_md(str(repo), "main") is False
    assert (repo / "CLAUDE.md").read_text() == "restored from origin\n"


def test_ensure_claude_md_uses_unborn_head_fallback_and_resets_on_push_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo = _init_empty_repo(tmp_path)
    calls: list[tuple[str, ...]] = []

    def fake_run_git(repo_path: str, *args: str, timeout: int = 30) -> _FakeCompletedProcess:
        del repo_path, timeout
        calls.append(args)
        if args[:2] == ("cat-file", "-e"):
            raise subprocess.CalledProcessError(1, ["git", *args])
        if args[:2] == ("checkout", "main"):
            raise subprocess.CalledProcessError(1, ["git", *args])
        if args[:2] == ("push", "origin"):
            raise subprocess.TimeoutExpired(["git", *args], scaffolder._PUSH_GIT_TIMEOUT)
        return _FakeCompletedProcess(args=["git", *args])

    monkeypatch.setattr(scaffolder, "_run_git", fake_run_git)
    monkeypatch.setattr(scaffolder, "_head_is_unborn", lambda repo_path: True)

    assert scaffolder.ensure_claude_md(str(repo), "main") is False
    assert (repo / "CLAUDE.md").exists()
    assert ("symbolic-ref", "HEAD", "refs/heads/main") in calls
    assert calls[-1] == ("reset", "--hard", "origin/main")


def test_ensure_claude_md_reraises_checkout_failure_when_head_is_born(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo = _init_empty_repo(tmp_path)

    def fake_run_git(repo_path: str, *args: str, timeout: int = 30) -> _FakeCompletedProcess:
        del repo_path, timeout
        if args[:2] == ("cat-file", "-e"):
            raise subprocess.CalledProcessError(1, ["git", *args])
        if args[:2] == ("checkout", "main"):
            raise subprocess.CalledProcessError(1, ["git", *args])
        return _FakeCompletedProcess(args=["git", *args])

    monkeypatch.setattr(scaffolder, "_run_git", fake_run_git)
    monkeypatch.setattr(scaffolder, "_head_is_unborn", lambda repo_path: False)

    with pytest.raises(subprocess.CalledProcessError):
        scaffolder.ensure_claude_md(str(repo), "main")


def test_scaffold_repo_preserves_existing_queue(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo = _init_empty_repo(tmp_path)
    (repo / "tasks").mkdir()
    existing = "# Task Queue\n\n## PR-001\n- Status: DOING\n"
    (repo / "tasks" / "QUEUE.md").write_text(existing)
    _patch_git(monkeypatch)

    actions = scaffolder.scaffold_repo(str(repo), "main")

    assert (repo / "tasks" / "QUEUE.md").read_text() == existing
    assert "tasks/QUEUE.md" not in actions


def test_scaffolder_template_directory_no_queue_md() -> None:
    assert not (scaffolder.TEMPLATES_DIR / "QUEUE.md").exists()


def test_scaffold_repo_preserves_existing_ci_sh(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo = _init_empty_repo(tmp_path)
    (repo / "scripts").mkdir()
    existing = "#!/usr/bin/env bash\nmake test\n"
    (repo / "scripts" / "ci.sh").write_text(existing)
    _patch_git(monkeypatch)

    actions = scaffolder.scaffold_repo(str(repo), "main")

    assert (repo / "scripts" / "ci.sh").read_text() == existing
    assert "scripts/ci.sh" not in actions


def test_scaffold_repo_skips_commit_when_fully_provisioned(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A fully-provisioned, fully-synced repo yields an empty action
    list and performs no add/commit/push — scaffolding is idempotent
    across repeated calls. The unpushed-commits probe still runs
    (checkout, rev-parse, rev-list) but reports no stranded commit.
    """
    repo = _init_empty_repo(tmp_path)
    (repo / "AGENTS.md").write_text("# AGENTS\n")
    (repo / "CLAUDE.md").write_text(scaffolder._CLAUDE_MD_CANONICAL)
    skill = repo / ".claude" / "skills" / "orch-context" / "SKILL.md"
    skill.parent.mkdir(parents=True)
    skill.write_text(scaffolder._SKILL_MD_CANONICAL)
    (repo / "tasks").mkdir()
    (repo / "tasks" / "QUEUE.md").write_text("# Task Queue\n")
    (repo / "scripts").mkdir()
    (repo / "scripts" / "ci.sh").write_text("#!/usr/bin/env bash\n")
    (repo / "scripts" / "ci.sh").chmod(0o755)
    (repo / "scripts" / "make-review-artifacts.sh").write_text(
        "#!/usr/bin/env bash\n"
    )
    (repo / "scripts" / "make-review-artifacts.sh").chmod(0o755)
    (repo / "artifacts").mkdir()
    (repo / ".gitignore").write_text("artifacts/\ntasks/QUEUE.md\n")
    calls = _patch_git(monkeypatch, synced=True)

    actions = scaffolder.scaffold_repo(str(repo), "main")

    assert actions == []
    # Checkout first, then only read-only git probes (rev-parse,
    # rev-list). Critically: no add/commit/push.
    assert calls[0] == ["git", "checkout", "main"]
    assert not any(cmd[:2] == ["git", "add"] for cmd in calls)
    assert not any(cmd[:2] == ["git", "commit"] for cmd in calls)
    assert not any(cmd[:2] == ["git", "push"] for cmd in calls)


def test_scaffold_repo_skips_git_when_only_artifacts_dir_missing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """If the only missing entry is the untrackable ``artifacts/``
    directory (the repo was fully provisioned and synced upstream
    previously), scaffolding must create it locally but skip add/
    commit/push — git would otherwise fail with "nothing to commit"
    and surface as a scaffolding error for a case that needed no
    upstream change at all.
    """
    repo = _init_empty_repo(tmp_path)
    (repo / "AGENTS.md").write_text("# AGENTS\n")
    (repo / "CLAUDE.md").write_text(scaffolder._CLAUDE_MD_CANONICAL)
    skill = repo / ".claude" / "skills" / "orch-context" / "SKILL.md"
    skill.parent.mkdir(parents=True)
    skill.write_text(scaffolder._SKILL_MD_CANONICAL)
    (repo / "tasks").mkdir()
    (repo / "tasks" / "QUEUE.md").write_text("# Task Queue\n")
    (repo / "scripts").mkdir()
    (repo / "scripts" / "ci.sh").write_text("#!/usr/bin/env bash\n")
    (repo / "scripts" / "ci.sh").chmod(0o755)
    (repo / "scripts" / "make-review-artifacts.sh").write_text(
        "#!/usr/bin/env bash\n"
    )
    (repo / "scripts" / "make-review-artifacts.sh").chmod(0o755)
    (repo / ".gitignore").write_text("artifacts/\ntasks/QUEUE.md\n")
    # Note: no artifacts/ directory — this is the only gap. The remote
    # is fully in sync, so ``synced=True``.
    calls = _patch_git(monkeypatch, synced=True)

    actions = scaffolder.scaffold_repo(str(repo), "main")

    # The empty directory was created locally and is reported for
    # logging.
    assert (repo / "artifacts").is_dir()
    assert actions == ["artifacts/"]
    # No git writes: no add, no commit, no push. The initial checkout
    # and the read-only unpushed-commits probes are the only calls.
    assert calls[0] == ["git", "checkout", "main"]
    assert not any(cmd[:2] == ["git", "add"] for cmd in calls)
    assert not any(cmd[:2] == ["git", "commit"] for cmd in calls)
    assert not any(cmd[:2] == ["git", "push"] for cmd in calls)


def test_scaffold_repo_appends_artifacts_without_duplicating(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo = _init_empty_repo(tmp_path)
    existing = "node_modules/\n*.pyc\n"
    (repo / ".gitignore").write_text(existing)
    _patch_git(monkeypatch)

    scaffolder.scaffold_repo(str(repo), "main")

    lines = (repo / ".gitignore").read_text().splitlines()
    assert lines.count("artifacts/") == 1
    assert lines.count("tasks/QUEUE.md") == 1
    assert "node_modules/" in lines
    assert "*.pyc" in lines

    # Running it again must not add a second entry.
    _patch_git(monkeypatch)
    scaffolder.scaffold_repo(str(repo), "main")
    lines_after = (repo / ".gitignore").read_text().splitlines()
    assert lines_after.count("artifacts/") == 1
    assert lines_after.count("tasks/QUEUE.md") == 1


def test_scaffold_repo_propagates_git_push_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo = _init_empty_repo(tmp_path)

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        if cmd[:2] == ["git", "push"]:
            raise subprocess.CalledProcessError(1, cmd, stderr="push denied")
        if cmd[:2] == ["git", "check-ignore"]:
            return _FakeCompletedProcess(args=cmd, returncode=1)
        if cmd[:3] == ["git", "rev-parse", "--verify"]:
            ref = cmd[-1]
            if ref.startswith("refs/remotes/origin/"):
                # origin/main missing → probe decides a push is needed.
                return _FakeCompletedProcess(args=cmd, returncode=1)
            return _FakeCompletedProcess(args=cmd, returncode=0)
        return _FakeCompletedProcess(args=cmd)

    monkeypatch.setattr(scaffolder.subprocess, "run", fake_run)

    with pytest.raises(subprocess.CalledProcessError):
        scaffolder.scaffold_repo(str(repo), "main")


def test_scaffold_repo_sets_timeouts_on_every_git_call(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Every git subprocess must enforce a ``timeout`` so a stalled
    network operation or auth prompt on first connect cannot hang the
    runner cycle. ``push`` is the only network-facing call and gets the
    higher ceiling; local ops use the lower limit.
    """
    repo = _init_empty_repo(tmp_path)
    captured: list[tuple[list[str], dict[str, Any]]] = []

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        captured.append((cmd, kwargs))
        if cmd[:2] == ["git", "check-ignore"]:
            return _FakeCompletedProcess(args=cmd, returncode=1)
        if cmd[:3] == ["git", "rev-parse", "--verify"]:
            ref = cmd[-1]
            if ref.startswith("refs/remotes/origin/"):
                # Force the unpushed-commits probe to decide a push
                # is needed so the push call is exercised too.
                return _FakeCompletedProcess(args=cmd, returncode=1)
            return _FakeCompletedProcess(args=cmd, returncode=0)
        return _FakeCompletedProcess(args=cmd)

    monkeypatch.setattr(scaffolder.subprocess, "run", fake_run)

    scaffolder.scaffold_repo(str(repo), "main")

    # Every git call carried a timeout kwarg.
    assert captured, "scaffolder did not issue any git calls"
    for cmd, kwargs in captured:
        assert "timeout" in kwargs, f"{cmd} ran without a timeout"
        assert kwargs["timeout"] > 0

    # A push call was reached so both the local and the push limits
    # are actually exercised.
    assert any(cmd[:2] == ["git", "push"] for cmd, _ in captured)

    # The network-facing push gets the higher ceiling; every other git
    # call uses the lower local-op limit.
    for cmd, kwargs in captured:
        if cmd[:2] == ["git", "push"]:
            assert kwargs["timeout"] == scaffolder._PUSH_GIT_TIMEOUT
        else:
            assert kwargs["timeout"] == scaffolder._LOCAL_GIT_TIMEOUT


def test_scaffold_repo_handles_unborn_head(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Onboarding a brand-new empty GitHub repo leaves ``HEAD`` unborn
    (no commits on any branch), and ``git checkout {branch}`` fails
    with ``pathspec ... did not match``. The scaffolder must recover
    by pointing ``HEAD`` at the configured branch via ``symbolic-ref``
    and then proceed with scaffolding, the initial commit, and the
    initial push that creates ``origin/{branch}``.
    """
    repo = _init_empty_repo(tmp_path)
    calls: list[list[str]] = []
    # ``HEAD`` starts unborn (empty clone) and becomes born after the
    # scaffolding commit lands. The fake git responds accordingly so
    # the unborn-detection probe returns True before the commit and
    # False afterward.
    committed = {"done": False}

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        calls.append(cmd)
        if cmd[:2] == ["git", "checkout"]:
            raise subprocess.CalledProcessError(
                1,
                cmd,
                stderr="error: pathspec 'main' did not match any file(s)",
            )
        if cmd[:2] == ["git", "commit"]:
            committed["done"] = True
            return _FakeCompletedProcess(args=cmd)
        if cmd[:2] == ["git", "check-ignore"]:
            return _FakeCompletedProcess(args=cmd, returncode=1)
        if cmd[:3] == ["git", "rev-parse", "--verify"]:
            ref = cmd[-1]
            if ref.startswith("refs/remotes/origin/"):
                # Remote branch does not yet exist on a brand-new
                # empty GitHub repo.
                return _FakeCompletedProcess(args=cmd, returncode=1)
            # HEAD probe: unborn until the scaffolding commit.
            return _FakeCompletedProcess(
                args=cmd, returncode=0 if committed["done"] else 1
            )
        return _FakeCompletedProcess(args=cmd)

    monkeypatch.setattr(scaffolder.subprocess, "run", fake_run)

    actions = scaffolder.scaffold_repo(str(repo), "main")

    # Scaffolding continued past the failed checkout, wrote the files,
    # and reported them.
    assert (repo / "AGENTS.md").exists()
    assert not (repo / "tasks" / "QUEUE.md").exists()
    assert "AGENTS.md" in actions

    # The unborn-HEAD fallback must call symbolic-ref with the configured
    # branch, not ``git checkout -b`` or a generic retry.
    symbolic_ref_calls = [
        cmd for cmd in calls if cmd[:2] == ["git", "symbolic-ref"]
    ]
    assert symbolic_ref_calls == [
        ["git", "symbolic-ref", "HEAD", "refs/heads/main"]
    ]

    # And the happy path below it still runs: add -> commit -> push
    # targeting the configured branch, creating the branch upstream.
    assert any(cmd[:2] == ["git", "add"] for cmd in calls)
    assert any(cmd[:2] == ["git", "commit"] for cmd in calls)
    push_cmd = next(cmd for cmd in calls if cmd[:2] == ["git", "push"])
    assert push_cmd == ["git", "push", "origin", "main"]


def test_scaffold_repo_reraises_checkout_failure_when_head_is_born(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """When checkout fails on a non-empty repo (e.g. the configured
    branch genuinely does not exist), the scaffolder must re-raise
    instead of silently recovering — that's a real misconfiguration
    and the operator needs to see it.
    """
    repo = _init_empty_repo(tmp_path)

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        if cmd[:2] == ["git", "checkout"]:
            raise subprocess.CalledProcessError(
                1,
                cmd,
                stderr="error: pathspec 'missing' did not match",
            )
        if cmd[:3] == ["git", "rev-parse", "--verify"]:
            # Born HEAD: rev-parse succeeds, so we know the repo has
            # commits and the checkout failure is not about an unborn
            # HEAD — it's a real missing branch.
            return _FakeCompletedProcess(args=cmd, returncode=0)
        return _FakeCompletedProcess(args=cmd)

    monkeypatch.setattr(scaffolder.subprocess, "run", fake_run)

    with pytest.raises(subprocess.CalledProcessError):
        scaffolder.scaffold_repo(str(repo), "missing")


def test_scaffold_repo_pushes_when_rev_list_probe_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """If ``git rev-list --count`` returns non-zero or non-integer
    output, scaffolder must err on the side of pushing rather than
    silently declaring success. Returning "synced" on a probe error
    would let a just-committed scaffolding (or a stranded commit
    from a prior cycle) stay unpublished while ``scaffold_repo``
    reports success and the runner sets ``_scaffolded = True``.
    Pushing an already-synced branch is a cheap "Everything up-to-
    date" no-op, so erring on the side of True is safe.
    """
    repo = _init_scaffolded_repo(tmp_path)

    calls: list[list[str]] = []

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        calls.append(cmd)
        if cmd[:3] == ["git", "rev-parse", "--verify"]:
            # origin/main exists so the rev-list probe actually runs.
            return _FakeCompletedProcess(args=cmd, returncode=0)
        if cmd[:2] == ["git", "rev-list"]:
            # Probe failure — e.g. corrupted refs, concurrent gc,
            # or a transient git bug.
            return _FakeCompletedProcess(
                args=cmd, returncode=128, stdout=""
            )
        return _FakeCompletedProcess(args=cmd)

    monkeypatch.setattr(scaffolder.subprocess, "run", fake_run)

    scaffolder.scaffold_repo(str(repo), "main")

    # scaffold_repo must have pushed despite the probe failure.
    push_cmds = [cmd for cmd in calls if cmd[:2] == ["git", "push"]]
    assert push_cmds == [["git", "push", "origin", "main"]]


def test_scaffold_repo_pushes_when_rev_list_probe_is_not_an_integer(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo = _init_scaffolded_repo(tmp_path)
    calls: list[list[str]] = []

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        calls.append(cmd)
        if cmd[:3] == ["git", "rev-parse", "--verify"]:
            return _FakeCompletedProcess(args=cmd, returncode=0)
        if cmd[:2] == ["git", "rev-list"]:
            return _FakeCompletedProcess(args=cmd, returncode=0, stdout="nope\n")
        return _FakeCompletedProcess(args=cmd)

    monkeypatch.setattr(scaffolder.subprocess, "run", fake_run)

    assert scaffolder.scaffold_repo(str(repo), "main") == []
    assert [cmd for cmd in calls if cmd[:2] == ["git", "push"]] == [
        ["git", "push", "origin", "main"]
    ]


def test_scaffold_repo_pushes_when_rev_list_probe_times_out(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """If the ``git rev-list --count`` probe raises
    ``TimeoutExpired`` (e.g. a lock-contention stall), scaffolder
    must fall back to "cannot verify sync, push to be safe" instead
    of letting the exception abort ``scaffold_repo``. Aborting
    before push would keep a previously stranded scaffolding commit
    unpublished until manual intervention.
    """
    repo = _init_scaffolded_repo(tmp_path)

    calls: list[list[str]] = []

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        calls.append(cmd)
        if cmd[:3] == ["git", "rev-parse", "--verify"]:
            # origin/main exists so rev-list runs.
            return _FakeCompletedProcess(args=cmd, returncode=0)
        if cmd[:2] == ["git", "rev-list"]:
            raise subprocess.TimeoutExpired(cmd, kwargs.get("timeout", 0))
        return _FakeCompletedProcess(args=cmd)

    monkeypatch.setattr(scaffolder.subprocess, "run", fake_run)

    # Must NOT raise TimeoutExpired out of scaffold_repo.
    scaffolder.scaffold_repo(str(repo), "main")

    # Push must still have happened despite the probe timeout.
    push_cmds = [cmd for cmd in calls if cmd[:2] == ["git", "push"]]
    assert push_cmds == [["git", "push", "origin", "main"]]


def test_scaffold_repo_retries_stranded_push_with_no_new_commit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A prior cycle committed the scaffolding locally but the push
    failed transiently, so the local commit is stranded and
    ``origin/{branch}`` still lacks ``tasks/QUEUE.md``. On the retry
    cycle every file is already present locally, so ``created`` is
    empty, but the scaffolder must still re-push the stranded commit
    — otherwise the runner's ``_parse_base_queue`` keeps reading an
    empty QUEUE.md from origin and stays stuck in ERROR forever.
    """
    repo = _init_empty_repo(tmp_path)
    # Fully provision the repo locally — this is what the filesystem
    # looks like after a successful local commit whose push timed out.
    (repo / "AGENTS.md").write_text("# AGENTS\n")
    (repo / "CLAUDE.md").write_text(scaffolder._CLAUDE_MD_CANONICAL)
    skill = repo / ".claude" / "skills" / "orch-context" / "SKILL.md"
    skill.parent.mkdir(parents=True)
    skill.write_text(scaffolder._SKILL_MD_CANONICAL)
    (repo / "tasks").mkdir()
    (repo / "tasks" / "QUEUE.md").write_text("# Task Queue\n")
    (repo / "scripts").mkdir()
    (repo / "scripts" / "ci.sh").write_text("#!/usr/bin/env bash\n")
    (repo / "scripts" / "ci.sh").chmod(0o755)
    (repo / "scripts" / "make-review-artifacts.sh").write_text(
        "#!/usr/bin/env bash\n"
    )
    (repo / "scripts" / "make-review-artifacts.sh").chmod(0o755)
    (repo / "artifacts").mkdir()
    (repo / ".gitignore").write_text("artifacts/\ntasks/QUEUE.md\n")

    calls: list[list[str]] = []

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        calls.append(cmd)
        if cmd[:3] == ["git", "rev-parse", "--verify"]:
            # origin/main exists on the remote (fetched earlier) and
            # HEAD is born — the stranded-commit state is signalled by
            # rev-list --count returning 1.
            return _FakeCompletedProcess(args=cmd, returncode=0)
        if cmd[:2] == ["git", "rev-list"]:
            return _FakeCompletedProcess(args=cmd, returncode=0, stdout="1\n")
        return _FakeCompletedProcess(args=cmd)

    monkeypatch.setattr(scaffolder.subprocess, "run", fake_run)

    actions = scaffolder.scaffold_repo(str(repo), "main")

    # Nothing new was created: every file is already in place.
    assert actions == []
    # But the push must have run to publish the stranded commit.
    push_cmds = [cmd for cmd in calls if cmd[:2] == ["git", "push"]]
    assert push_cmds == [["git", "push", "origin", "main"]]
    # And no new commit was produced — the local commit from the
    # previous cycle is exactly what's being re-pushed.
    assert not any(cmd[:2] == ["git", "add"] for cmd in calls)
    assert not any(cmd[:2] == ["git", "commit"] for cmd in calls)


def test_scaffold_repo_reraises_git_commit_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo = _init_empty_repo(tmp_path)

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        if cmd[:2] == ["git", "commit"]:
            raise subprocess.CalledProcessError(1, cmd, stderr="commit blocked")
        if cmd[:2] == ["git", "check-ignore"]:
            return _FakeCompletedProcess(args=cmd, returncode=1)
        if cmd[:3] == ["git", "rev-parse", "--verify"]:
            ref = cmd[-1]
            if ref.startswith("refs/remotes/origin/"):
                return _FakeCompletedProcess(args=cmd, returncode=1)
            return _FakeCompletedProcess(args=cmd, returncode=0)
        return _FakeCompletedProcess(args=cmd)

    monkeypatch.setattr(scaffolder.subprocess, "run", fake_run)

    with pytest.raises(subprocess.CalledProcessError):
        scaffolder.scaffold_repo(str(repo), "main")


def test_scaffold_repo_reraises_git_commit_timeout(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo = _init_empty_repo(tmp_path)

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        if cmd[:2] == ["git", "commit"]:
            raise subprocess.TimeoutExpired(cmd, kwargs.get("timeout", 0))
        if cmd[:2] == ["git", "check-ignore"]:
            return _FakeCompletedProcess(args=cmd, returncode=1)
        if cmd[:3] == ["git", "rev-parse", "--verify"]:
            ref = cmd[-1]
            if ref.startswith("refs/remotes/origin/"):
                return _FakeCompletedProcess(args=cmd, returncode=1)
            return _FakeCompletedProcess(args=cmd, returncode=0)
        return _FakeCompletedProcess(args=cmd)

    monkeypatch.setattr(scaffolder.subprocess, "run", fake_run)

    with pytest.raises(subprocess.TimeoutExpired):
        scaffolder.scaffold_repo(str(repo), "main")


def test_scaffold_repo_propagates_git_push_timeout(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A stalled ``git push`` must raise ``TimeoutExpired`` out of the
    scaffolder so ``ensure_repo_cloned``'s broad error handler can log
    and move on, rather than the runner cycle hanging indefinitely.
    """
    repo = _init_empty_repo(tmp_path)

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        if cmd[:2] == ["git", "push"]:
            raise subprocess.TimeoutExpired(cmd, kwargs.get("timeout", 0))
        if cmd[:2] == ["git", "check-ignore"]:
            return _FakeCompletedProcess(args=cmd, returncode=1)
        if cmd[:3] == ["git", "rev-parse", "--verify"]:
            ref = cmd[-1]
            if ref.startswith("refs/remotes/origin/"):
                return _FakeCompletedProcess(args=cmd, returncode=1)
            return _FakeCompletedProcess(args=cmd, returncode=0)
        return _FakeCompletedProcess(args=cmd)

    monkeypatch.setattr(scaffolder.subprocess, "run", fake_run)

    with pytest.raises(subprocess.TimeoutExpired):
        scaffolder.scaffold_repo(str(repo), "main")


# --- PR-272: pre-push branch-validation hook installation ----------------


def _patch_git_passthrough_install_hook(
    monkeypatch: pytest.MonkeyPatch,
) -> list[list[str]]:
    """Like ``_patch_git`` but lets the install-pre-push-hook.sh call run
    for real so the test can assert the hook file was written to disk.

    The bash invocation path is matched by ``cmd[0] == "bash"`` — every
    other subprocess (git checkout, add, commit, push, check-ignore) is
    served by the local fake exactly like ``_patch_git``.

    The PR-272 in-tree-hook helpers issue ``git rev-parse --git-path``
    (for ``hooks/pre-push`` and ``info/exclude``), ``--show-toplevel``,
    ``--git-dir``, and ``git ls-files --error-unmatch`` against the real
    repo to resolve the effective hook path, decide whether the
    destination is a user-versioned hook, and locate the per-clone
    exclude file; those probes are passed through as well.
    """
    real_run = subprocess.run
    calls: list[list[str]] = []
    _GUARD_REV_PARSE_ARGS = {
        ("--git-path", "hooks/pre-push"),
        ("--show-toplevel",),
        ("--git-dir",),
        ("--git-path", "info/exclude"),
    }

    def fake_run(cmd: list[str], **kwargs: Any):
        calls.append(cmd)
        if (
            cmd
            and cmd[0] == "bash"
            and len(cmd) > 1
            and cmd[1].endswith("install-pre-push-hook.sh")
        ):
            return real_run(cmd, **kwargs)
        if cmd[:2] == ["git", "rev-parse"] and tuple(cmd[2:]) in _GUARD_REV_PARSE_ARGS:
            return real_run(cmd, **kwargs)
        if cmd[:3] == ["git", "ls-files", "--error-unmatch"]:
            return real_run(cmd, **kwargs)
        if cmd[:2] == ["git", "check-ignore"]:
            return _FakeCompletedProcess(args=cmd, returncode=1)
        if cmd[:3] == ["git", "rev-parse", "--verify"]:
            ref = cmd[-1]
            if ref.startswith("refs/remotes/origin/"):
                return _FakeCompletedProcess(args=cmd, returncode=1)
            return _FakeCompletedProcess(args=cmd, returncode=0)
        if cmd[:2] == ["git", "rev-list"]:
            return _FakeCompletedProcess(args=cmd, returncode=0, stdout="0\n")
        return _FakeCompletedProcess(args=cmd)

    monkeypatch.setattr(scaffolder.subprocess, "run", fake_run)
    return calls


def test_scaffolder_installs_pre_push_hook(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``scaffold_repo`` must install the pre-push hook on every pass so
    existing managed repos gain the PR-272 branch-validation defense
    automatically.
    """
    repo = _init_empty_repo(tmp_path)
    subprocess.run(
        ["git", "init", "-q", "-b", "main", str(repo)], check=True
    )
    _patch_git_passthrough_install_hook(monkeypatch)

    scaffolder.scaffold_repo(str(repo), "main")

    hook = repo / ".git" / "hooks" / "pre-push"
    assert hook.exists()
    assert hook.stat().st_mode & 0o111
    content = hook.read_text()
    assert "[pre-push-hook]" in content
    assert "expected-branch" in content


def test_scaffolder_idempotent_pre_push_install(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Re-running the scaffolder must not duplicate or corrupt the hook
    file. The install script overwrites unconditionally, so the second
    pass leaves the file content unchanged.
    """
    repo = _init_empty_repo(tmp_path)
    subprocess.run(
        ["git", "init", "-q", "-b", "main", str(repo)], check=True
    )
    _patch_git_passthrough_install_hook(monkeypatch)

    scaffolder.scaffold_repo(str(repo), "main")
    hook = repo / ".git" / "hooks" / "pre-push"
    first_content = hook.read_text()

    scaffolder.scaffold_repo(str(repo), "main")
    second_content = hook.read_text()

    assert second_content == first_content
    # The shebang appears exactly once — no accidental duplication
    # from a double-write that misses the overwrite path. A unique
    # one-shot anchor at the top of the script is a stable proxy for
    # the entire payload not being concatenated to itself.
    assert first_content.count("#!/bin/bash") == 1


def test_scaffolder_logs_warning_on_pre_push_install_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """A non-zero install exit must be logged but must NOT abort the
    cycle: the hook is defense-in-depth and a failed install reduces
    protection without breaking the dispatch path.
    """
    repo = _init_empty_repo(tmp_path)

    def fake_run(cmd: list[str], **kwargs: Any):
        if (
            cmd
            and cmd[0] == "bash"
            and len(cmd) > 1
            and cmd[1].endswith("install-pre-push-hook.sh")
        ):
            raise subprocess.CalledProcessError(returncode=1, cmd=cmd)
        if cmd[:2] == ["git", "check-ignore"]:
            return _FakeCompletedProcess(args=cmd, returncode=1)
        if cmd[:3] == ["git", "rev-parse", "--verify"]:
            ref = cmd[-1]
            if ref.startswith("refs/remotes/origin/"):
                return _FakeCompletedProcess(args=cmd, returncode=1)
            return _FakeCompletedProcess(args=cmd, returncode=0)
        if cmd[:2] == ["git", "rev-list"]:
            return _FakeCompletedProcess(args=cmd, returncode=0, stdout="0\n")
        return _FakeCompletedProcess(args=cmd)

    monkeypatch.setattr(scaffolder.subprocess, "run", fake_run)

    with caplog.at_level("WARNING", logger="src.daemon.scaffolder"):
        scaffolder.scaffold_repo(str(repo), "main")

    assert any(
        "pre-push hook install failed" in record.message
        for record in caplog.records
    )


def test_hooks_path_inside_worktree_default_returns_false(
    tmp_path: Path,
) -> None:
    """A repo with no ``core.hooksPath`` keeps hooks under ``.git/hooks/``.
    That path is technically inside the worktree directory tree but
    invisible to ``git status``, so the worktree-dirty guard must NOT
    trigger on the default and the installer must run as before.
    """
    repo = tmp_path / "repo"
    repo.mkdir()
    subprocess.run(
        ["git", "init", "-q", "-b", "main", str(repo)], check=True
    )
    assert scaffolder._hooks_path_inside_worktree(str(repo)) is False


def test_hooks_path_inside_worktree_relative_in_tree_returns_true(
    tmp_path: Path,
) -> None:
    """``core.hooksPath=.githooks`` resolves to ``<repo>/.githooks/`` —
    inside the worktree but outside ``.git/``. The guard must trigger so
    scaffold_repo skips the install and avoids dirtying ``git status``.
    """
    repo = tmp_path / "repo"
    repo.mkdir()
    subprocess.run(
        ["git", "init", "-q", "-b", "main", str(repo)], check=True
    )
    subprocess.run(
        ["git", "-C", str(repo), "config", "core.hooksPath", ".githooks"],
        check=True,
    )
    assert scaffolder._hooks_path_inside_worktree(str(repo)) is True


def test_hooks_path_inside_worktree_absolute_outside_returns_false(
    tmp_path: Path,
) -> None:
    """An absolute ``core.hooksPath`` outside the worktree is safe; the
    bash installer writes there directly without touching the worktree.
    """
    repo = tmp_path / "repo"
    repo.mkdir()
    custom = tmp_path / "custom-hooks"
    subprocess.run(
        ["git", "init", "-q", "-b", "main", str(repo)], check=True
    )
    subprocess.run(
        ["git", "-C", str(repo), "config", "core.hooksPath", str(custom)],
        check=True,
    )
    assert scaffolder._hooks_path_inside_worktree(str(repo)) is False


def test_hooks_path_inside_worktree_probe_failure_returns_false(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A failing rev-parse probe must NOT silently disable the install
    path. The guard returns False and ``_install_pre_push_hook`` proceeds
    to invoke the bash installer (which has its own diagnostics).
    """

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        if cmd[:2] == ["git", "rev-parse"]:
            raise subprocess.CalledProcessError(returncode=128, cmd=cmd)
        return _FakeCompletedProcess(args=cmd)

    monkeypatch.setattr(scaffolder.subprocess, "run", fake_run)
    assert scaffolder._hooks_path_inside_worktree(str(tmp_path)) is False


def test_scaffolder_installs_pre_push_when_hooks_path_in_tree(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When ``core.hooksPath`` points inside the worktree (e.g. a
    relative ``.githooks/`` versioned with the repo) and the destination
    is not a tracked file, scaffold_repo must still install the hook —
    skipping would disable the branch-guard for repos that intentionally
    version hooks in-tree, defeating the defense-in-depth guarantee.
    The path is appended to the per-clone ``info/exclude`` so the new
    hook does not show up as untracked under ``git status --porcelain``
    and stall later preflight checks.
    """
    repo = _init_empty_repo(tmp_path)
    subprocess.run(
        ["git", "init", "-q", "-b", "main", str(repo)], check=True
    )
    subprocess.run(
        ["git", "-C", str(repo), "config", "core.hooksPath", ".githooks"],
        check=True,
    )
    _patch_git_passthrough_install_hook(monkeypatch)

    scaffolder.scaffold_repo(str(repo), "main")

    hook = repo / ".githooks" / "pre-push"
    assert hook.exists()
    assert hook.stat().st_mode & 0o111
    content = hook.read_text()
    assert "[pre-push-hook]" in content
    assert "expected-branch" in content

    exclude = repo / ".git" / "info" / "exclude"
    assert exclude.exists()
    assert "/.githooks/pre-push" in exclude.read_text().splitlines()

    # Sanity: git itself agrees the installed hook is ignored, so
    # ``git status --porcelain`` will not surface it during preflight.
    check_ignore = subprocess.run(
        ["git", "-C", str(repo), "check-ignore", ".githooks/pre-push"],
        capture_output=True,
        text=True,
    )
    assert check_ignore.returncode == 0


def test_scaffolder_in_tree_install_idempotent_exclude_entry(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Re-running scaffold_repo against an in-tree hooks path must not
    duplicate the ``info/exclude`` entry on every pass — the helper
    appends only when missing.
    """
    repo = _init_empty_repo(tmp_path)
    subprocess.run(
        ["git", "init", "-q", "-b", "main", str(repo)], check=True
    )
    subprocess.run(
        ["git", "-C", str(repo), "config", "core.hooksPath", ".githooks"],
        check=True,
    )
    _patch_git_passthrough_install_hook(monkeypatch)

    scaffolder.scaffold_repo(str(repo), "main")
    scaffolder.scaffold_repo(str(repo), "main")

    exclude_text = (repo / ".git" / "info" / "exclude").read_text()
    assert exclude_text.count("/.githooks/pre-push") == 1


def test_scaffolder_skips_install_when_in_tree_dest_is_tracked(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """When ``core.hooksPath`` points inside the worktree AND the
    destination file is already tracked by git, scaffold_repo must
    refuse to clobber the user-versioned hook. The skip is logged at
    WARNING so operators see the reduced redundancy.
    """
    repo = _init_empty_repo(tmp_path)
    subprocess.run(
        ["git", "init", "-q", "-b", "main", str(repo)], check=True
    )
    subprocess.run(
        ["git", "-C", str(repo), "config", "core.hooksPath", ".githooks"],
        check=True,
    )
    githooks = repo / ".githooks"
    githooks.mkdir()
    user_hook_body = "#!/bin/bash\n# user-versioned hook\nexit 0\n"
    user_hook = githooks / "pre-push"
    user_hook.write_text(user_hook_body)
    user_hook.chmod(0o755)
    subprocess.run(
        ["git", "-C", str(repo), "add", ".githooks/pre-push"], check=True
    )
    subprocess.run(
        [
            "git",
            "-C",
            str(repo),
            "-c",
            "user.email=test@example.test",
            "-c",
            "user.name=Test",
            "commit",
            "-q",
            "-m",
            "init hook",
        ],
        check=True,
    )

    calls = _patch_git_passthrough_install_hook(monkeypatch)

    with caplog.at_level("WARNING", logger="src.daemon.scaffolder"):
        scaffolder.scaffold_repo(str(repo), "main")

    assert not any(
        len(cmd) > 1
        and cmd[0] == "bash"
        and cmd[1].endswith("install-pre-push-hook.sh")
        for cmd in calls
    )
    assert user_hook.read_text() == user_hook_body
    assert any(
        "tracked file" in record.message
        and "skipping pre-push hook install" in record.message
        for record in caplog.records
    )


def test_hook_dest_is_tracked_returns_true_for_committed_path(
    tmp_path: Path,
) -> None:
    """A path committed to the index returns True so the install path
    can detect the clobber risk.
    """
    repo = tmp_path / "repo"
    repo.mkdir()
    subprocess.run(
        ["git", "init", "-q", "-b", "main", str(repo)], check=True
    )
    (repo / ".githooks").mkdir()
    (repo / ".githooks" / "pre-push").write_text("#!/bin/bash\nexit 0\n")
    subprocess.run(
        ["git", "-C", str(repo), "add", ".githooks/pre-push"], check=True
    )
    subprocess.run(
        [
            "git",
            "-C",
            str(repo),
            "-c",
            "user.email=test@example.test",
            "-c",
            "user.name=Test",
            "commit",
            "-q",
            "-m",
            "init",
        ],
        check=True,
    )

    assert (
        scaffolder._hook_dest_is_tracked(
            str(repo), Path(".githooks/pre-push")
        )
        is True
    )


def test_hook_dest_is_tracked_returns_false_for_untracked_path(
    tmp_path: Path,
) -> None:
    """An untracked-on-disk file is not tracked, so install proceeds."""
    repo = tmp_path / "repo"
    repo.mkdir()
    subprocess.run(
        ["git", "init", "-q", "-b", "main", str(repo)], check=True
    )
    (repo / ".githooks").mkdir()
    (repo / ".githooks" / "pre-push").write_text("#!/bin/bash\nexit 0\n")

    assert (
        scaffolder._hook_dest_is_tracked(
            str(repo), Path(".githooks/pre-push")
        )
        is False
    )


def test_hook_dest_is_tracked_probe_failure_returns_false(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A timeout on the ``ls-files`` probe must NOT silently mark the
    destination as tracked — that would skip install even when the
    repo has no committed hook.
    """

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        if cmd[:2] == ["git", "ls-files"]:
            raise subprocess.TimeoutExpired(cmd=cmd, timeout=1)
        return _FakeCompletedProcess(args=cmd)

    monkeypatch.setattr(scaffolder.subprocess, "run", fake_run)
    assert (
        scaffolder._hook_dest_is_tracked(
            str(tmp_path), Path(".githooks/pre-push")
        )
        is False
    )


def test_add_to_local_exclude_creates_file_when_missing(
    tmp_path: Path,
) -> None:
    """``info/exclude`` may not exist on a fresh ``git init`` (depending
    on git version); the helper must create it rather than silently
    failing.
    """
    repo = tmp_path / "repo"
    repo.mkdir()
    subprocess.run(
        ["git", "init", "-q", "-b", "main", str(repo)], check=True
    )
    exclude = repo / ".git" / "info" / "exclude"
    if exclude.exists():
        exclude.unlink()

    scaffolder._add_to_local_exclude(
        str(repo), Path(".githooks/pre-push")
    )

    assert exclude.exists()
    assert "/.githooks/pre-push" in exclude.read_text().splitlines()


def test_add_to_local_exclude_logs_on_rev_parse_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A failing ``rev-parse --git-path info/exclude`` must be logged at
    WARNING and swallowed; the install path itself succeeded, so a
    failure to add the exclude entry only degrades preflight cleanliness,
    not the hook's branch-guard function.
    """

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        if cmd[:3] == ["git", "rev-parse", "--git-path"]:
            raise subprocess.CalledProcessError(returncode=128, cmd=cmd)
        return _FakeCompletedProcess(args=cmd)

    monkeypatch.setattr(scaffolder.subprocess, "run", fake_run)

    with caplog.at_level("WARNING", logger="src.daemon.scaffolder"):
        scaffolder._add_to_local_exclude(
            str(tmp_path), Path(".githooks/pre-push")
        )

    assert any(
        "cannot resolve info/exclude" in record.message
        for record in caplog.records
    )


def test_add_to_local_exclude_logs_on_write_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """An ``OSError`` writing the exclude file is logged and swallowed
    so install completion is not penalised by a read-only ``.git/info``.
    """
    repo = tmp_path / "repo"
    repo.mkdir()
    subprocess.run(
        ["git", "init", "-q", "-b", "main", str(repo)], check=True
    )

    real_write_text = Path.write_text

    def boom(self: Path, *args: Any, **kwargs: Any) -> int:
        if self.name == "exclude":
            raise OSError("read-only filesystem")
        return real_write_text(self, *args, **kwargs)

    monkeypatch.setattr(Path, "write_text", boom)

    with caplog.at_level("WARNING", logger="src.daemon.scaffolder"):
        scaffolder._add_to_local_exclude(
            str(repo), Path(".githooks/pre-push")
        )

    assert any(
        "failed to update" in record.message
        and "pre-push" in record.message
        for record in caplog.records
    )


# ---------------------------------------------------------------------------
# PR-273: scaffolder template AGENTS.md aligned with the AUTO PR /
# PLANNED PR / MICRO PR / FIX FEEDBACK four-trigger model. Newly onboarded
# repos must receive the same trigger contract on the very first scaffold
# pass instead of waiting for a daemon reconciliation cycle.
# ---------------------------------------------------------------------------


def test_scaffolder_template_lists_four_triggers(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A freshly scaffolded AGENTS.md enumerates AUTO PR alongside the
    three manual triggers so the coder sees the daemon's invocation mode
    documented from day one."""
    repo = _init_empty_repo(tmp_path)
    _patch_git(monkeypatch)

    scaffolder.scaffold_repo(str(repo), "main")

    agents = (repo / "AGENTS.md").read_text()
    assert "### Work Modes" in agents
    for trigger in ("AUTO PR", "PLANNED PR", "MICRO PR", "FIX FEEDBACK"):
        assert trigger in agents, f"missing trigger {trigger!r}"


def test_scaffolder_template_includes_auto_pr_runbook_marker(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The scaffolded AGENTS.md ships BEGIN/END markers for the
    auto_pr_runbook section so the daemon's PR-192a/b/c reconciliation
    framework has a target block to fill in on the first sync. Without
    the markers reconciliation would append the canonical content at
    EOF, leaving the scaffold-time stub orphaned higher up."""
    repo = _init_empty_repo(tmp_path)
    _patch_git(monkeypatch)

    scaffolder.scaffold_repo(str(repo), "main")

    agents = (repo / "AGENTS.md").read_text()
    assert (
        "<!-- pipeline-orchestrator: managed BEGIN auto_pr_runbook -->"
        in agents
    )
    assert (
        "<!-- pipeline-orchestrator: managed END auto_pr_runbook -->"
        in agents
    )


def test_scaffolder_template_quick_rules_mentions_auto_pr(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The Quick rules block tells the coder that a daemon-driven prompt
    starts with AUTO PR and carries explicit Task/File headers, so the
    coder routes to the AUTO PR runbook instead of falling back to
    PLANNED PR queue discovery."""
    repo = _init_empty_repo(tmp_path)
    _patch_git(monkeypatch)

    scaffolder.scaffold_repo(str(repo), "main")

    agents = (repo / "AGENTS.md").read_text()
    quick_rules_start = agents.index("Quick rules")
    work_modes_start = agents.index("### Work Modes")
    quick_rules = agents[quick_rules_start:work_modes_start]
    assert "AUTO PR" in quick_rules
    assert "pipeline-orchestrator daemon" in quick_rules


def test_scaffolder_template_preserves_planned_pr_runbook(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The manual-mode runbooks (PLANNED PR, MICRO PR) remain in the
    scaffold template — manual VS Code workflows are not deprecated by
    the AUTO PR rollout."""
    repo = _init_empty_repo(tmp_path)
    _patch_git(monkeypatch)

    scaffolder.scaffold_repo(str(repo), "main")

    agents = (repo / "AGENTS.md").read_text()
    assert "### PLANNED PR runbook" in agents
    assert "### MICRO PR runbook" in agents


def test_scaffolder_template_matches_daemon_managed_content(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Drift guard between the scaffolder's standalone templates/AGENTS.md
    Work Modes content and the daemon-managed work_modes region from
    src/onboarding/agents_md_template.py. Both surfaces must agree on
    the four trigger phrases so a freshly scaffolded repo and a
    reconciled repo cannot disagree on which triggers exist."""
    from src.onboarding.agents_md_template import daemon_managed_content

    repo = _init_empty_repo(tmp_path)
    _patch_git(monkeypatch)

    scaffolder.scaffold_repo(str(repo), "main")

    agents = (repo / "AGENTS.md").read_text()
    work_modes_start = agents.index("### Work Modes")
    daemon_mode_start = agents.index("### Daemon Mode")
    template_work_modes = agents[work_modes_start:daemon_mode_start]

    daemon_work_modes = daemon_managed_content()["work_modes"]
    for trigger in ("AUTO PR", "PLANNED PR", "MICRO PR", "FIX FEEDBACK"):
        assert trigger in template_work_modes
        assert trigger in daemon_work_modes
