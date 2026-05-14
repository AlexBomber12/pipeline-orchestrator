"""Tests for the git bundle backup primitives in src/daemon/backups.py."""

from __future__ import annotations

import asyncio
import os
import re
import subprocess
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pytest
from src.daemon import backups
from src.daemon.backups import create_repo_bundle, prune_old_bundles


def _init_git_repo(repo_path: Path) -> None:
    """Initialize a tiny git repo with a single commit at ``repo_path``."""
    repo_path.mkdir(parents=True, exist_ok=True)
    env = {**os.environ, "GIT_AUTHOR_NAME": "t", "GIT_AUTHOR_EMAIL": "t@e",
           "GIT_COMMITTER_NAME": "t", "GIT_COMMITTER_EMAIL": "t@e"}
    subprocess.run(
        ["git", "init", "-q", "-b", "main", str(repo_path)],
        check=True, env=env,
    )
    (repo_path / "README.md").write_text("hello\n", encoding="utf-8")
    subprocess.run(
        ["git", "-C", str(repo_path), "add", "README.md"],
        check=True, env=env,
    )
    subprocess.run(
        ["git", "-C", str(repo_path), "commit", "-q", "-m", "init"],
        check=True, env=env,
    )


def _touch_bundle(path: Path, mtime: float) -> None:
    """Create an empty bundle file and stamp its mtime."""
    path.write_bytes(b"")
    os.utime(path, (mtime, mtime))


def test_create_repo_bundle_creates_valid_file(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    _init_git_repo(repo)
    backup_dir = tmp_path / "backups"

    bundle_path = asyncio.run(create_repo_bundle(
        repo_path=str(repo), repo_name="repo", backup_dir=str(backup_dir),
    ))

    assert bundle_path is not None
    assert bundle_path.exists()
    verify = subprocess.run(
        ["git", "-C", str(repo), "bundle", "verify", str(bundle_path)],
        capture_output=True, text=True,
    )
    assert verify.returncode == 0


def test_create_repo_bundle_returns_none_for_non_git_directory(
    tmp_path: Path,
) -> None:
    repo = tmp_path / "not_git"
    repo.mkdir()
    backup_dir = tmp_path / "backups"

    result = asyncio.run(create_repo_bundle(
        repo_path=str(repo), repo_name="not_git", backup_dir=str(backup_dir),
    ))

    assert result is None
    bundle_root = backup_dir / "not_git"
    if bundle_root.exists():
        assert list(bundle_root.glob("*.bundle")) == []


def test_create_repo_bundle_returns_none_on_subprocess_timeout(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    backup_dir = tmp_path / "backups"

    def _fake_run(*args: Any, **kwargs: Any) -> Any:
        raise subprocess.TimeoutExpired(cmd=args[0], timeout=1)

    monkeypatch.setattr(backups.subprocess, "run", _fake_run)

    result = asyncio.run(create_repo_bundle(
        repo_path=str(tmp_path), repo_name="repo", backup_dir=str(backup_dir),
    ))

    assert result is None
    assert list((backup_dir / "repo").glob("*.bundle")) == []


def test_create_repo_bundle_returns_none_on_verify_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    backup_dir = tmp_path / "backups"
    calls: list[list[str]] = []

    class _Completed:
        def __init__(self, returncode: int) -> None:
            self.returncode = returncode

    def _fake_run(args: list[str], **kwargs: Any) -> _Completed:
        calls.append(args)
        if "create" in args:
            Path(args[3]).write_bytes(b"partial-bundle-bytes")
            return _Completed(0)
        return _Completed(1)

    monkeypatch.setattr(backups.subprocess, "run", _fake_run)

    result = asyncio.run(create_repo_bundle(
        repo_path=str(tmp_path), repo_name="repo", backup_dir=str(backup_dir),
    ))

    assert result is None
    assert len(calls) == 2
    assert list((backup_dir / "repo").glob("*.bundle")) == []


def test_create_repo_bundle_returns_none_on_oserror(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    backup_dir = tmp_path / "backups"

    def _fake_run(*args: Any, **kwargs: Any) -> Any:
        raise OSError("disk full")

    monkeypatch.setattr(backups.subprocess, "run", _fake_run)

    result = asyncio.run(create_repo_bundle(
        repo_path=str(tmp_path), repo_name="repo", backup_dir=str(backup_dir),
    ))

    assert result is None
    assert list((backup_dir / "repo").glob("*.bundle")) == []


def test_create_repo_bundle_returns_none_on_mkdir_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    backup_dir = tmp_path / "readonly"

    def _raising_mkdir(self: Path, *args: Any, **kwargs: Any) -> None:
        raise PermissionError("read-only filesystem")

    monkeypatch.setattr(Path, "mkdir", _raising_mkdir)

    result = asyncio.run(create_repo_bundle(
        repo_path=str(tmp_path), repo_name="repo", backup_dir=str(backup_dir),
    ))

    assert result is None
    assert not (backup_dir / "repo").exists()


def test_create_repo_bundle_filename_format(tmp_path: Path) -> None:
    repo = tmp_path / "myrepo"
    _init_git_repo(repo)
    backup_dir = tmp_path / "backups"

    bundle_path = asyncio.run(create_repo_bundle(
        repo_path=str(repo), repo_name="myrepo", backup_dir=str(backup_dir),
    ))

    assert bundle_path is not None
    assert re.fullmatch(r"myrepo-\d{8}T\d{6}Z\.bundle", bundle_path.name)


def test_prune_old_bundles_keeps_daily_retention(tmp_path: Path) -> None:
    backup_dir = tmp_path / "backups"
    backup_root = backup_dir / "repo"
    backup_root.mkdir(parents=True)
    now = datetime(2026, 5, 4, 12, 0, 0, tzinfo=timezone.utc).timestamp()
    bundles = []
    for i in range(10):
        p = backup_root / f"repo-bundle-{i:02d}.bundle"
        _touch_bundle(p, now - i * 3600)
        bundles.append(p)

    removed = asyncio.run(prune_old_bundles(
        backup_dir=str(backup_dir), repo_name="repo",
        daily_retention=3, weekly_retention=0,
    ))

    assert removed == 7
    remaining = sorted(p.name for p in backup_root.glob("*.bundle"))
    assert remaining == [b.name for b in bundles[:3]]


def test_prune_old_bundles_keeps_sunday_for_weekly(tmp_path: Path) -> None:
    backup_dir = tmp_path / "backups"
    backup_root = backup_dir / "repo"
    backup_root.mkdir(parents=True)
    # Four Sundays: 2026-04-26, 2026-05-03, 2026-05-10, 2026-05-17.
    sundays = [
        datetime(2026, 4, 26, 12, 0, 0, tzinfo=timezone.utc).timestamp(),
        datetime(2026, 5, 3, 12, 0, 0, tzinfo=timezone.utc).timestamp(),
        datetime(2026, 5, 10, 12, 0, 0, tzinfo=timezone.utc).timestamp(),
        datetime(2026, 5, 17, 12, 0, 0, tzinfo=timezone.utc).timestamp(),
    ]
    # Three non-Sundays: 2026-05-13 Wed, 2026-05-14 Thu, 2026-05-15 Fri.
    weekdays = [
        datetime(2026, 5, 13, 12, 0, 0, tzinfo=timezone.utc).timestamp(),
        datetime(2026, 5, 14, 12, 0, 0, tzinfo=timezone.utc).timestamp(),
        datetime(2026, 5, 15, 12, 0, 0, tzinfo=timezone.utc).timestamp(),
    ]
    for idx, ts in enumerate(sundays):
        _touch_bundle(backup_root / f"repo-sun-{idx}.bundle", ts)
    for idx, ts in enumerate(weekdays):
        _touch_bundle(backup_root / f"repo-wd-{idx}.bundle", ts)

    removed = asyncio.run(prune_old_bundles(
        backup_dir=str(backup_dir), repo_name="repo",
        daily_retention=2, weekly_retention=2,
    ))

    remaining = {p.name for p in backup_root.glob("*.bundle")}
    # 2 newest by mtime: repo-sun-3 (May 17) and repo-wd-2 (May 15).
    # 2 newest Sundays: repo-sun-3 (May 17) and repo-sun-2 (May 10).
    # Union (set semantics dedupes repo-sun-3): three survivors.
    assert remaining == {
        "repo-sun-3.bundle",
        "repo-wd-2.bundle",
        "repo-sun-2.bundle",
    }
    assert removed == 4


def test_prune_old_bundles_zero_weekly_retention_removes_all_non_daily(
    tmp_path: Path,
) -> None:
    backup_dir = tmp_path / "backups"
    backup_root = backup_dir / "repo"
    backup_root.mkdir(parents=True)
    # Mix Sundays with weekdays; only the 2 newest by mtime should survive.
    sunday_anchor = datetime(2026, 5, 10, 12, 0, 0, tzinfo=timezone.utc)
    times = [
        sunday_anchor.timestamp(),                       # Sunday newest
        (sunday_anchor - timedelta(days=1)).timestamp(),  # Sat (kept by daily)
        (sunday_anchor - timedelta(days=7)).timestamp(),  # Older Sunday
        (sunday_anchor - timedelta(days=8)).timestamp(),  # Older Sat
    ]
    for idx, ts in enumerate(times):
        _touch_bundle(backup_root / f"repo-b-{idx}.bundle", ts)

    removed = asyncio.run(prune_old_bundles(
        backup_dir=str(backup_dir), repo_name="repo",
        daily_retention=2, weekly_retention=0,
    ))

    assert removed == 2
    remaining = sorted(p.name for p in backup_root.glob("*.bundle"))
    assert remaining == ["repo-b-0.bundle", "repo-b-1.bundle"]


def test_prune_old_bundles_no_directory_returns_zero(tmp_path: Path) -> None:
    removed = asyncio.run(prune_old_bundles(
        backup_dir=str(tmp_path / "missing"), repo_name="repo",
        daily_retention=7, weekly_retention=4,
    ))

    assert removed == 0


def test_prune_old_bundles_empty_directory_returns_zero(tmp_path: Path) -> None:
    backup_root = tmp_path / "backups" / "repo"
    backup_root.mkdir(parents=True)

    removed = asyncio.run(prune_old_bundles(
        backup_dir=str(tmp_path / "backups"), repo_name="repo",
        daily_retention=7, weekly_retention=4,
    ))

    assert removed == 0


def test_prune_old_bundles_swallows_unlink_oserror(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    backup_dir = tmp_path / "backups"
    backup_root = backup_dir / "repo"
    backup_root.mkdir(parents=True)
    now = datetime(2026, 5, 4, 12, 0, 0, tzinfo=timezone.utc).timestamp()
    for i in range(3):
        _touch_bundle(backup_root / f"repo-{i}.bundle", now - i * 3600)

    def _raising_unlink(self: Path) -> None:
        raise OSError("locked")

    monkeypatch.setattr(Path, "unlink", _raising_unlink)

    removed = asyncio.run(prune_old_bundles(
        backup_dir=str(backup_dir), repo_name="repo",
        daily_retention=1, weekly_retention=0,
    ))

    assert removed == 0
