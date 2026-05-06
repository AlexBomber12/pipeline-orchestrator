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


def _run_hook(repo: Path) -> subprocess.CompletedProcess[str]:
    """Invoke the installed hook directly with the same args git passes."""
    return subprocess.run(
        [str(repo / ".git" / "hooks" / "pre-push"), "origin", "ssh://x"],
        cwd=str(repo),
        capture_output=True,
        text=True,
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
    result = _run_hook(repo)
    assert result.returncode == 0
    assert result.stderr == ""


def test_hook_blocks_when_branch_mismatch(tmp_path: Path) -> None:
    repo = _init_repo(tmp_path)
    _install(repo)
    info = repo / ".git" / "info"
    info.mkdir(parents=True, exist_ok=True)
    (info / "expected-branch").write_text("expected-branch-name\n")
    result = _run_hook(repo)
    assert result.returncode == 1
    assert "[pre-push-hook] BLOCKED" in result.stderr
    assert "expected branch 'expected-branch-name'" in result.stderr
    assert "Aborting push" in result.stderr


def test_hook_passes_when_branch_matches(tmp_path: Path) -> None:
    repo = _init_repo(tmp_path)
    _install(repo)
    info = repo / ".git" / "info"
    info.mkdir(parents=True, exist_ok=True)
    # Stage and commit so symbolic-ref --short HEAD resolves to the branch.
    env = os.environ.copy()
    env.update(
        {
            "GIT_AUTHOR_NAME": "t",
            "GIT_AUTHOR_EMAIL": "t@example.com",
            "GIT_COMMITTER_NAME": "t",
            "GIT_COMMITTER_EMAIL": "t@example.com",
        }
    )
    (repo / "x.txt").write_text("ok\n")
    subprocess.run(["git", "-C", str(repo), "add", "x.txt"], check=True, env=env)
    subprocess.run(
        ["git", "-C", str(repo), "commit", "-q", "-m", "init"],
        check=True,
        env=env,
    )
    head_branch = subprocess.run(
        ["git", "-C", str(repo), "symbolic-ref", "--short", "HEAD"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    (info / "expected-branch").write_text(head_branch + "\n")
    result = _run_hook(repo)
    assert result.returncode == 0, result.stderr


def test_hook_blocks_when_head_unresolvable(tmp_path: Path) -> None:
    """When ``git symbolic-ref --short HEAD`` fails the hook falls back
    to ``<detached>`` and treats the mismatch as a block. Cover the
    fallback by pointing HEAD at a non-existent ref.
    """
    repo = _init_repo(tmp_path)
    _install(repo)
    info = repo / ".git" / "info"
    info.mkdir(parents=True, exist_ok=True)
    (info / "expected-branch").write_text("any-branch\n")
    subprocess.run(
        ["git", "-C", str(repo), "symbolic-ref", "HEAD", "refs/heads/__none__"],
        check=True,
    )
    result = _run_hook(repo)
    assert result.returncode == 1
    assert "[pre-push-hook] BLOCKED" in result.stderr


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
    matches a plain branch name from ``symbolic-ref --short HEAD``.
    """
    repo = _init_repo(tmp_path)
    _install(repo)
    info = repo / ".git" / "info"
    info.mkdir(parents=True, exist_ok=True)
    (info / "expected-branch").write_text(payload)
    env = os.environ.copy()
    env.update(
        {
            "GIT_AUTHOR_NAME": "t",
            "GIT_AUTHOR_EMAIL": "t@example.com",
            "GIT_COMMITTER_NAME": "t",
            "GIT_COMMITTER_EMAIL": "t@example.com",
        }
    )
    subprocess.run(
        ["git", "-C", str(repo), "checkout", "-q", "-b", "pr-001"],
        check=True,
        env=env,
    )
    (repo / "x.txt").write_text("ok\n")
    subprocess.run(["git", "-C", str(repo), "add", "x.txt"], check=True, env=env)
    subprocess.run(
        ["git", "-C", str(repo), "commit", "-q", "-m", "init"],
        check=True,
        env=env,
    )
    result = _run_hook(repo)
    assert result.returncode == 0, result.stderr
