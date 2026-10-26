"""PR-410 experiment only: no production capture API or cleanup authorization."""
from __future__ import annotations

import asyncio
import shutil
import subprocess
from pathlib import Path

import pytest
from src.daemon.backups import create_repo_bundle

from tests.test_backups import _init_git_repo


def git(repo: Path, *args: str, data: bytes | None = None) -> bytes:
    return subprocess.run(
        ["git", "-C", str(repo), *args], input=data, capture_output=True,
        check=True, timeout=30,
    ).stdout


def files(repo: Path) -> dict[str, tuple[bytes, int]]:
    """Regular-file experiment; retain ignored bytes too for rmtree's loss set."""
    return {
        str(p.relative_to(repo)): (p.read_bytes(), p.stat().st_mode & 0o777)
        for p in repo.rglob("*")
        if ".git" not in p.relative_to(repo).parts and p.is_file()
    }


@pytest.mark.parametrize("detached", [False, True], ids=["branch", "detached"])
def test_bundle_plus_missing_bytes_restores_separate_repo(tmp_path: Path, detached: bool):
    source = tmp_path / "source"
    _init_git_repo(source)
    git(source, "config", "user.name", "Preservation test")
    git(source, "config", "user.email", "test@example.invalid")
    git(source, "config", "core.autocrlf", "false")
    git(source, "config", "core.filemode", "true")
    for name in ("rename me", "staged-delete", "worktree-delete"):
        (source / name).write_bytes(b"original\x00\xff\n")
    (source / ".gitignore").write_text("ignored.bin\n")
    git(source, "add", ".")
    git(source, "commit", "-qm", "baseline")
    base = git(source, "rev-parse", "HEAD").strip()
    # A bounded remote-tracking ref models the last published commit.
    git(source, "update-ref", "refs/remotes/origin/main", base.decode())
    if detached:
        git(source, "checkout", "--detach")
    (source / "local-commit").write_bytes(b"unpushed commit\n")
    git(source, "add", ".")
    git(source, "commit", "-qm", "local only")
    head = git(source, "rev-parse", "HEAD").strip()
    git(source, "tag", "local-tag", base.decode())
    (source / "README.md").write_bytes(b"staged version\x00\xfe\n")
    git(source, "mv", "rename me", "renamed\nfile")
    git(source, "rm", "staged-delete")
    (source / "staged-only.bin").write_bytes(b"index only\x00\xff")
    git(source, "add", "README.md", "staged-only.bin")
    (source / "staged-only.bin").unlink()
    (source / "README.md").write_bytes(b"unstaged version\x00\xff\n")
    (source / "worktree-delete").unlink()
    (source / "untracked.bin").write_bytes(bytes(range(256)))
    (source / "ignored.bin").write_bytes(b"ignored but lost to rmtree\x00")
    (source / "executable").write_bytes(b"#!/bin/sh\nexit 0\n")
    (source / "executable").chmod(0o755)

    # Capture the raw index BEFORE status can refresh its stat cache.
    index = (source / ".git/index").read_bytes()
    entries = git(source, "ls-files", "--stage", "-z")
    index_oids = sorted({entry.split(b" ", 2)[1] for entry in entries.split(b"\0") if entry})
    refs = git(source, "show-ref")
    head_file = (source / ".git/HEAD").read_bytes()
    work = files(source)
    staged_diff = git(source, "diff", "--cached", "--binary")
    work_diff = git(source, "diff", "--binary")
    status = git(source, "status", "--porcelain=v1", "-z", "--untracked-files=all")
    bundle = asyncio.run(create_repo_bundle(
        repo_path=str(source), repo_name="experiment", backup_dir=str(tmp_path / "saved"),
    ))
    assert bundle is not None
    # --all includes current detached HEAD, but not arbitrary index-only blobs.
    heads = git(source, "bundle", "list-heads", str(bundle))
    assert head + b" HEAD\n" in heads
    pack = git(source, "pack-objects", "--stdout", data=b"\n".join(index_oids) + b"\n")
    staged_blob = git(source, "rev-parse", ":staged-only.bin").strip()

    assert files(source) == work
    assert git(source, "show-ref") == refs
    # Destroy only this test's own disposable clone, proving no object borrowing.
    shutil.rmtree(source)

    restored = tmp_path / "restored"
    restored.mkdir()
    git(restored, "init", "-q", "-b", "restore-placeholder")
    git(restored, "config", "core.autocrlf", "false")
    git(restored, "config", "core.filemode", "true")
    git(restored, "fetch", str(bundle), "+refs/*:refs/*")
    git(restored, "fetch", str(bundle), "HEAD")
    git(restored, "checkout", "--detach", head.decode())
    # Bundle-only restore proves both what survives and what is missing.
    assert git(restored, "show-ref") == refs
    assert git(restored, "rev-parse", "HEAD").strip() == head != base
    assert (restored / "local-commit").read_bytes() == b"unpushed commit\n"
    assert (restored / "README.md").read_bytes() == b"hello\n"
    assert not (restored / "untracked.bin").exists()
    assert not (restored / "ignored.bin").exists()
    assert (restored / "staged-delete").exists()
    assert (restored / "worktree-delete").exists()
    missing = subprocess.run(
        ["git", "-C", str(restored), "cat-file", "-e", staged_blob.decode()],
        capture_output=True, timeout=30,
    )
    assert missing.returncode != 0

    # Restore only into the fixture-owned separate directory; never the source.
    git(restored, "index-pack", "--stdin", data=pack)
    for name in files(restored):
        (restored / name).unlink()
    for name, (content, mode) in work.items():
        target = restored / name
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(content)
        target.chmod(mode)
    (restored / ".git/HEAD").write_bytes(head_file)
    (restored / ".git/index").write_bytes(index)
    assert (restored / ".git/index").read_bytes() == index
    assert (restored / ".git/HEAD").read_bytes() == head_file
    assert git(restored, "show-ref") == refs
    assert git(restored, "ls-files", "--stage", "-z") == entries
    assert files(restored) == work
    assert git(restored, "show", ":staged-only.bin") == b"index only\x00\xff"
    assert git(restored, "show", ":README.md") == b"staged version\x00\xfe\n"
    assert git(restored, "diff", "--cached", "--binary") == staged_diff
    assert git(restored, "diff", "--binary") == work_diff
    assert git(restored, "status", "--porcelain=v1", "-z", "--untracked-files=all") == status
    assert not source.exists()


def test_bundle_read_failure_does_not_authorize_cleanup(tmp_path: Path):
    source = tmp_path / "unknown-directory"
    source.mkdir()
    payload = source / "irreplaceable.bin"
    payload.write_bytes(bytes(range(32)))
    before = files(source)
    assert asyncio.run(create_repo_bundle(
        repo_path=str(source), repo_name="unknown", backup_dir=str(tmp_path / "saved"),
    )) is None
    assert files(source) == before
