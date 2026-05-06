"""Tests for scripts/install-pre-push-hook.sh and the installed hook.

The install script and the resulting hook are bash, so these tests run
the scripts via ``subprocess.run`` against ``tmp_path`` repos rather than
exercising Python source. They verify both the install side (idempotent
overwrite, executable bit) and the hook behavior (no-op when expected
file is absent, block on mismatch, pass on match).
"""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
INSTALL_SCRIPT = REPO_ROOT / "scripts" / "install-pre-push-hook.sh"


def _init_repo(tmp_path: Path) -> Path:
    repo = tmp_path / "repo"
    repo.mkdir()
    subprocess.run(
        ["git", "init", "-q", "-b", "main", str(repo)],
        check=True,
    )
    return repo


def _install(
    repo: Path, *, check: bool = True
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["bash", str(INSTALL_SCRIPT), str(repo)],
        check=check,
        capture_output=True,
        text=True,
    )


_ZERO_OID = "0000000000000000000000000000000000000000"
_FAKE_OID = "0000000000000000000000000000000000000001"


def _push_line(local_ref: str, *, remote_ref: str | None = None) -> str:
    """Build one stdin line in the format git passes to pre-push.

    Per githooks(5): ``<local-ref> <local-oid> <remote-ref>
    <remote-oid>`` for each ref to be pushed.
    """
    return f"{local_ref} {_FAKE_OID} {remote_ref or local_ref} {_ZERO_OID}\n"


def _run_hook(
    repo: Path,
    *,
    stdin: str = "",
    hook_path: Path | None = None,
    cwd: Path | None = None,
) -> subprocess.CompletedProcess[str]:
    """Invoke the installed hook directly with the same args git passes."""
    if hook_path is None:
        hook_path = repo / ".git" / "hooks" / "pre-push"
    return subprocess.run(
        [str(hook_path), "origin", "ssh://x"],
        cwd=str(cwd or repo),
        capture_output=True,
        text=True,
        input=stdin,
    )


def test_install_creates_hook_file(tmp_path: Path) -> None:
    repo = _init_repo(tmp_path)
    _install(repo)
    hook = repo / ".git" / "hooks" / "pre-push"
    assert hook.exists()
    assert hook.stat().st_mode & 0o111


def test_install_overwrites_existing_hook(tmp_path: Path) -> None:
    repo = _init_repo(tmp_path)
    hook = repo / ".git" / "hooks" / "pre-push"
    hook.parent.mkdir(parents=True, exist_ok=True)
    hook.write_text("#!/bin/bash\necho stale\n")
    hook.chmod(0o755)
    _install(repo)
    content = hook.read_text()
    assert "stale" not in content
    assert "[pre-push-hook]" in content
    assert "expected-branch" in content
    assert hook.stat().st_mode & 0o111


def test_install_requires_repo_arg() -> None:
    """Running the install script with no argument must fail with usage."""
    result = subprocess.run(
        ["bash", str(INSTALL_SCRIPT)],
        capture_output=True,
        text=True,
    )
    assert result.returncode != 0
    assert "usage" in result.stderr.lower()


def test_install_creates_hooks_dir_if_missing(tmp_path: Path) -> None:
    """The script must mkdir -p .git/hooks even when it is missing.

    A freshly cloned repo always has the directory, but covering the
    self-heal path (operator manually deleted .git/hooks) makes the
    idempotency claim concrete.
    """
    repo = _init_repo(tmp_path)
    hooks_dir = repo / ".git" / "hooks"
    for child in hooks_dir.iterdir():
        child.unlink()
    hooks_dir.rmdir()
    _install(repo)
    assert (hooks_dir / "pre-push").exists()


def test_hook_no_op_when_expected_branch_missing(tmp_path: Path) -> None:
    repo = _init_repo(tmp_path)
    _install(repo)
    expected = repo / ".git" / "info" / "expected-branch"
    assert not expected.exists()
    # Even when refs are queued for push, an absent marker is no-op:
    # operator pushes (no daemon CODING run in flight) must not be
    # affected by the defense-in-depth gate.
    result = _run_hook(repo, stdin=_push_line("refs/heads/main"))
    assert result.returncode == 0
    assert result.stderr == ""


def test_hook_no_op_when_no_refs_pushed(tmp_path: Path) -> None:
    """Empty stdin means git has no refs queued for push (e.g., the
    hook was invoked but nothing matched the refspec). With nothing
    being pushed there is no branch to validate, so the hook must
    exit 0 even with the marker present.
    """
    repo = _init_repo(tmp_path)
    _install(repo)
    info = repo / ".git" / "info"
    info.mkdir(parents=True, exist_ok=True)
    (info / "expected-branch").write_text("expected-branch-name\n")
    result = _run_hook(repo, stdin="")
    assert result.returncode == 0, result.stderr


def test_hook_blocks_when_branch_mismatch(tmp_path: Path) -> None:
    repo = _init_repo(tmp_path)
    _install(repo)
    info = repo / ".git" / "info"
    info.mkdir(parents=True, exist_ok=True)
    (info / "expected-branch").write_text("expected-branch-name\n")
    result = _run_hook(repo, stdin=_push_line("refs/heads/main"))
    assert result.returncode == 1
    assert "[pre-push-hook] BLOCKED" in result.stderr
    assert "expected branch 'expected-branch-name'" in result.stderr
    assert "push includes 'main'" in result.stderr
    assert "Aborting push" in result.stderr


def test_hook_passes_when_branch_matches(tmp_path: Path) -> None:
    """The pushed local ref equals the expected branch — pass."""
    repo = _init_repo(tmp_path)
    _install(repo)
    info = repo / ".git" / "info"
    info.mkdir(parents=True, exist_ok=True)
    (info / "expected-branch").write_text("pr-001\n")
    result = _run_hook(repo, stdin=_push_line("refs/heads/pr-001"))
    assert result.returncode == 0, result.stderr


def test_hook_blocks_when_pushing_other_branch_from_expected_head(
    tmp_path: Path,
) -> None:
    """Regression for the HEAD-based check: ``git push origin main`` while
    HEAD is on the expected feature branch must be blocked. The previous
    HEAD-based logic incorrectly passed because HEAD matched expected;
    the stdin-based logic blocks because the pushed local ref does not.
    """
    env = os.environ.copy()
    env.update(
        {
            "GIT_AUTHOR_NAME": "t",
            "GIT_AUTHOR_EMAIL": "t@example.com",
            "GIT_COMMITTER_NAME": "t",
            "GIT_COMMITTER_EMAIL": "t@example.com",
        }
    )
    repo = _init_repo(tmp_path)
    (repo / "x.txt").write_text("ok\n")
    subprocess.run(["git", "-C", str(repo), "add", "x.txt"], check=True, env=env)
    subprocess.run(
        ["git", "-C", str(repo), "commit", "-q", "-m", "init"],
        check=True,
        env=env,
    )
    subprocess.run(
        ["git", "-C", str(repo), "checkout", "-q", "-b", "pr-001"],
        check=True,
        env=env,
    )
    _install(repo)
    info = repo / ".git" / "info"
    info.mkdir(parents=True, exist_ok=True)
    (info / "expected-branch").write_text("pr-001\n")
    # HEAD is on pr-001 (the expected branch) but we are pushing main.
    result = _run_hook(repo, stdin=_push_line("refs/heads/main"))
    assert result.returncode == 1, result.stdout
    assert "push includes 'main'" in result.stderr


def test_hook_passes_when_pushing_expected_branch_from_detached_head(
    tmp_path: Path,
) -> None:
    """Regression for the HEAD-based check: pushing the expected branch
    from a detached/other checkout must pass. The previous HEAD-based
    logic failed because ``git symbolic-ref --short HEAD`` returned
    nothing in detached state; the stdin-based logic passes because the
    pushed local ref matches.
    """
    env = os.environ.copy()
    env.update(
        {
            "GIT_AUTHOR_NAME": "t",
            "GIT_AUTHOR_EMAIL": "t@example.com",
            "GIT_COMMITTER_NAME": "t",
            "GIT_COMMITTER_EMAIL": "t@example.com",
        }
    )
    repo = _init_repo(tmp_path)
    (repo / "x.txt").write_text("ok\n")
    subprocess.run(["git", "-C", str(repo), "add", "x.txt"], check=True, env=env)
    subprocess.run(
        ["git", "-C", str(repo), "commit", "-q", "-m", "init"],
        check=True,
        env=env,
    )
    subprocess.run(
        ["git", "-C", str(repo), "checkout", "-q", "--detach"],
        check=True,
        env=env,
    )
    _install(repo)
    info = repo / ".git" / "info"
    info.mkdir(parents=True, exist_ok=True)
    (info / "expected-branch").write_text("pr-001\n")
    # HEAD is detached, but we are pushing the expected branch by ref.
    result = _run_hook(repo, stdin=_push_line("refs/heads/pr-001"))
    assert result.returncode == 0, result.stderr


def test_hook_skips_delete_lines(tmp_path: Path) -> None:
    """Ref deletions appear as ``(delete) <oid> <remote-ref> <oid>`` on
    stdin. Nothing is pushed from the local side, so the hook must skip
    the line rather than block on the literal token ``(delete)``.
    """
    repo = _init_repo(tmp_path)
    _install(repo)
    info = repo / ".git" / "info"
    info.mkdir(parents=True, exist_ok=True)
    (info / "expected-branch").write_text("pr-001\n")
    stdin = (
        f"(delete) {_ZERO_OID} refs/heads/old-branch {_FAKE_OID}\n"
        + _push_line("refs/heads/pr-001")
    )
    result = _run_hook(repo, stdin=stdin)
    assert result.returncode == 0, result.stderr


def test_hook_blocks_on_non_branch_ref(tmp_path: Path) -> None:
    """Refs outside ``refs/heads/`` (tags, notes, raw HEAD pushes) are
    treated as their full ref so a non-branch push surfaces as a
    mismatch instead of being silently accepted — the daemon's AUTO PR
    flow only ever pushes the expected feature branch.
    """
    repo = _init_repo(tmp_path)
    _install(repo)
    info = repo / ".git" / "info"
    info.mkdir(parents=True, exist_ok=True)
    (info / "expected-branch").write_text("pr-001\n")
    result = _run_hook(repo, stdin=_push_line("refs/tags/v1.0"))
    assert result.returncode == 1
    assert "push includes 'refs/tags/v1.0'" in result.stderr


def test_hook_blocks_when_any_ref_mismatches(tmp_path: Path) -> None:
    """Multiple refs may be queued in one push (e.g.,
    ``git push origin pr-001 main``); blocking only when *all* refs
    mismatch would let the wrong-branch one through. Block on the
    first mismatch.
    """
    repo = _init_repo(tmp_path)
    _install(repo)
    info = repo / ".git" / "info"
    info.mkdir(parents=True, exist_ok=True)
    (info / "expected-branch").write_text("pr-001\n")
    stdin = _push_line("refs/heads/pr-001") + _push_line("refs/heads/main")
    result = _run_hook(repo, stdin=stdin)
    assert result.returncode == 1
    assert "push includes 'main'" in result.stderr


def test_install_honors_core_hooks_path(tmp_path: Path) -> None:
    """When ``core.hooksPath`` redirects hooks to a custom directory the
    installer must land the hook there — not in ``.git/hooks`` — so git
    actually executes it on push.
    """
    repo = _init_repo(tmp_path)
    custom = tmp_path / "custom-hooks"
    subprocess.run(
        ["git", "-C", str(repo), "config", "core.hooksPath", str(custom)],
        check=True,
    )
    _install(repo)
    installed = custom / "pre-push"
    assert installed.exists()
    assert installed.stat().st_mode & 0o111
    content = installed.read_text()
    assert "[pre-push-hook]" in content
    assert not (repo / ".git" / "hooks" / "pre-push").exists()


def test_install_warns_when_core_hooks_path_disables_hooks(
    tmp_path: Path,
) -> None:
    """``core.hooksPath=/dev/null`` is the canonical way to disable hooks
    entirely. The installer cannot write into ``/dev/null`` and must exit
    non-zero with a warning so the scaffolder log surfaces that branch
    protection is bypassed by config rather than silently succeeding.
    """
    repo = _init_repo(tmp_path)
    subprocess.run(
        ["git", "-C", str(repo), "config", "core.hooksPath", "/dev/null"],
        check=True,
    )
    result = _install(repo, check=False)
    assert result.returncode != 0
    assert "core.hooksPath" in result.stderr
    assert not (repo / ".git" / "hooks" / "pre-push").exists()


@pytest.mark.parametrize("payload", ["pr-001\n", "pr-001"])
def test_hook_strips_trailing_newline_from_expected(
    tmp_path: Path, payload: str
) -> None:
    """The hook's read of expected-branch must compare without the
    trailing newline so the daemon's ``branch + \"\\n\"`` write format
    matches the bare branch name extracted from the pushed local ref.
    """
    repo = _init_repo(tmp_path)
    _install(repo)
    info = repo / ".git" / "info"
    info.mkdir(parents=True, exist_ok=True)
    (info / "expected-branch").write_text(payload)
    result = _run_hook(repo, stdin=_push_line("refs/heads/pr-001"))
    assert result.returncode == 0, result.stderr


def test_hook_resolves_marker_in_linked_worktree(tmp_path: Path) -> None:
    """In a linked worktree ``.git`` is a *file* pointing at the per-checkout
    git directory under ``<main>/.git/worktrees/<name>/``; the hook must
    resolve ``info/expected-branch`` via ``git rev-parse --git-path`` so the
    marker is read from the per-worktree gitdir. A hardcoded
    ``.git/info/expected-branch`` lookup would silently no-op (the path
    does not resolve to a regular file under the ``.git`` *file*) and
    branch validation would be silently disabled for every linked
    worktree.
    """
    env = os.environ.copy()
    env.update(
        {
            "GIT_AUTHOR_NAME": "t",
            "GIT_AUTHOR_EMAIL": "t@example.com",
            "GIT_COMMITTER_NAME": "t",
            "GIT_COMMITTER_EMAIL": "t@example.com",
        }
    )
    main_repo = _init_repo(tmp_path)
    (main_repo / "x.txt").write_text("ok\n")
    subprocess.run(
        ["git", "-C", str(main_repo), "add", "x.txt"], check=True, env=env
    )
    subprocess.run(
        ["git", "-C", str(main_repo), "commit", "-q", "-m", "init"],
        check=True,
        env=env,
    )
    worktree = tmp_path / "wt"
    subprocess.run(
        [
            "git",
            "-C",
            str(main_repo),
            "worktree",
            "add",
            "-q",
            "-b",
            "feature",
            str(worktree),
        ],
        check=True,
        env=env,
    )
    # In a linked worktree ``.git`` is a file, not a directory.
    assert (worktree / ".git").is_file()
    _install(worktree)
    # Resolve where git stored the hook (linked worktrees inherit
    # ``hooks/`` from the main repo's gitdir) and where it stores
    # ``info/expected-branch`` for this worktree. The hook must read
    # from the per-worktree marker path, not from
    # ``<worktree>/.git/info/...`` (which does not exist because
    # ``.git`` is a file).
    def _git_path(rel: str) -> Path:
        out = subprocess.run(
            ["git", "-C", str(worktree), "rev-parse", "--git-path", rel],
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
        path = Path(out)
        if not path.is_absolute():
            path = worktree / path
        return path

    hook_file = _git_path("hooks/pre-push")
    assert hook_file.exists()
    marker = _git_path("info/expected-branch")
    marker.parent.mkdir(parents=True, exist_ok=True)
    marker.write_text("expected-branch-name\n")
    result = _run_hook(
        worktree,
        stdin=_push_line("refs/heads/feature"),
        hook_path=hook_file,
        cwd=worktree,
    )
    assert result.returncode == 1
    assert "[pre-push-hook] BLOCKED" in result.stderr
    assert "expected branch 'expected-branch-name'" in result.stderr
