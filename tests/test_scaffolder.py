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


def test_scaffold_repo_creates_all_files_when_empty(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo = _init_empty_repo(tmp_path)
    calls = _patch_git(monkeypatch)

    actions = scaffolder.scaffold_repo(str(repo), "main")

    # All baseline files must be on disk after scaffolding.
    assert (repo / "AGENTS.md").exists()
    assert (repo / "tasks").is_dir()
    assert (repo / "tasks" / "QUEUE.md").exists()
    assert (repo / "scripts").is_dir()
    assert (repo / "scripts" / "ci.sh").exists()
    assert (repo / "scripts" / "make-review-artifacts.sh").exists()
    assert (repo / "artifacts").is_dir()
    assert (repo / ".gitignore").exists()
    assert "artifacts/" in (repo / ".gitignore").read_text().splitlines()

    # Shell helpers must be executable so bash can run them directly.
    assert (repo / "scripts" / "ci.sh").stat().st_mode & 0o111
    assert (repo / "scripts" / "make-review-artifacts.sh").stat().st_mode & 0o111

    # Every tracked path (directories filtered out) should be reported as
    # an action so the caller can log it.
    for path in (
        "AGENTS.md",
        "tasks/QUEUE.md",
        "scripts/ci.sh",
        "scripts/make-review-artifacts.sh",
        ".gitignore",
    ):
        assert path in actions

    # Checkout must be the first git call so the working tree reflects
    # the configured base branch before any file is inspected or written.
    assert calls[0] == ["git", "checkout", "main"]

    # git add must stage exactly the files we created, not the directory
    # entries that appear in the action log.
    add_cmds = [cmd for cmd in calls if cmd[:2] == ["git", "add"]]
    assert len(add_cmds) == 1
    staged = add_cmds[0][2:]
    assert "AGENTS.md" in staged
    assert "tasks/QUEUE.md" in staged
    assert "scripts/ci.sh" in staged
    assert "scripts/make-review-artifacts.sh" in staged
    assert ".gitignore" in staged
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


def test_scaffold_repo_accepts_claude_md_as_agents(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """CLAUDE.md is accepted in place of AGENTS.md (either satisfies the
    runbook)."""
    repo = _init_empty_repo(tmp_path)
    (repo / "CLAUDE.md").write_text("# Project rules\n")
    _patch_git(monkeypatch)

    actions = scaffolder.scaffold_repo(str(repo), "main")

    assert not (repo / "AGENTS.md").exists()
    assert "AGENTS.md" not in actions


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
    (repo / ".gitignore").write_text("artifacts/\n")
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
    (repo / "tasks").mkdir()
    (repo / "tasks" / "QUEUE.md").write_text("# Task Queue\n")
    (repo / "scripts").mkdir()
    (repo / "scripts" / "ci.sh").write_text("#!/usr/bin/env bash\n")
    (repo / "scripts" / "ci.sh").chmod(0o755)
    (repo / "scripts" / "make-review-artifacts.sh").write_text(
        "#!/usr/bin/env bash\n"
    )
    (repo / "scripts" / "make-review-artifacts.sh").chmod(0o755)
    (repo / ".gitignore").write_text("artifacts/\n")
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
    assert "node_modules/" in lines
    assert "*.pyc" in lines

    # Running it again must not add a second entry.
    _patch_git(monkeypatch)
    scaffolder.scaffold_repo(str(repo), "main")
    lines_after = (repo / ".gitignore").read_text().splitlines()
    assert lines_after.count("artifacts/") == 1


def test_scaffold_repo_propagates_git_push_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo = _init_empty_repo(tmp_path)

    def fake_run(cmd: list[str], **kwargs: Any) -> _FakeCompletedProcess:
        if cmd[:2] == ["git", "push"]:
            raise subprocess.CalledProcessError(1, cmd, stderr="push denied")
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
    assert (repo / "tasks" / "QUEUE.md").exists()
    assert "AGENTS.md" in actions
    assert "tasks/QUEUE.md" in actions

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
    repo = _init_empty_repo(tmp_path)
    # Fully provision locally so ``to_stage`` is empty and scaffold
    # goes through the retry/no-commit branch that calls
    # ``_local_has_unpushed_commits``.
    (repo / "AGENTS.md").write_text("# AGENTS\n")
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
    (repo / ".gitignore").write_text("artifacts/\n")

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
    (repo / ".gitignore").write_text("artifacts/\n")

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
        if cmd[:3] == ["git", "rev-parse", "--verify"]:
            ref = cmd[-1]
            if ref.startswith("refs/remotes/origin/"):
                return _FakeCompletedProcess(args=cmd, returncode=1)
            return _FakeCompletedProcess(args=cmd, returncode=0)
        return _FakeCompletedProcess(args=cmd)

    monkeypatch.setattr(scaffolder.subprocess, "run", fake_run)

    with pytest.raises(subprocess.TimeoutExpired):
        scaffolder.scaffold_repo(str(repo), "main")
