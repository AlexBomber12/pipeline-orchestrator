"""Git bundle backup primitives for daemon-managed repositories.

A working clone tracks origin's HEAD, so it is not an independent backup.
``git bundle create --all`` packages every branch and tag into a single
self-contained file that survives force-push, repo deletion, disk
corruption, or credential compromise. PR-311b wires this into IDLE.
"""

from __future__ import annotations

import asyncio
import contextlib
import subprocess
from datetime import datetime, timezone
from pathlib import Path


def _create_and_verify_sync(repo_path: str, bundle_path: Path) -> bool:
    """Bundle and verify; clean up the partial file on any failure path."""
    try:
        bundle_path.parent.mkdir(parents=True, exist_ok=True)
        create = subprocess.run(
            ["git", "bundle", "create", str(bundle_path), "--all"],
            cwd=repo_path, capture_output=True, text=True, timeout=300,
        )
        if create.returncode != 0:
            with contextlib.suppress(OSError):
                bundle_path.unlink(missing_ok=True)
            return False
        verify = subprocess.run(
            ["git", "bundle", "verify", str(bundle_path)],
            cwd=repo_path, capture_output=True, text=True, timeout=60,
        )
        if verify.returncode != 0:
            with contextlib.suppress(OSError):
                bundle_path.unlink(missing_ok=True)
            return False
        return True
    except (subprocess.TimeoutExpired, OSError):
        with contextlib.suppress(OSError):
            bundle_path.unlink(missing_ok=True)
        return False


async def create_repo_bundle(
    *, repo_path: str, repo_name: str, backup_dir: str,
) -> Path | None:
    """Create and verify a ``--all`` git bundle off the asyncio thread."""
    backup_root = Path(backup_dir) / repo_name
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S.%fZ")
    bundle_path = backup_root / f"{repo_name}-{timestamp}.bundle"
    ok = await asyncio.to_thread(_create_and_verify_sync, repo_path, bundle_path)
    if not ok:
        return None
    return bundle_path


async def prune_old_bundles(
    *, backup_dir: str, repo_name: str,
    daily_retention: int, weekly_retention: int,
) -> int:
    """Keep N newest-by-mtime + M newest Sunday bundles; remove the rest."""
    backup_root = Path(backup_dir) / repo_name
    if not backup_root.exists():
        return 0
    bundles = sorted(
        backup_root.glob(f"{repo_name}-*.bundle"),
        key=lambda p: p.stat().st_mtime, reverse=True,
    )
    keep: set[Path] = set(bundles[:daily_retention])
    sundays = [
        b for b in bundles
        if datetime.fromtimestamp(b.stat().st_mtime, tz=timezone.utc).weekday() == 6
    ]
    keep.update(sundays[:weekly_retention])
    removed = 0
    for bundle in bundles:
        if bundle in keep:
            continue
        try:
            bundle.unlink()
        except OSError:
            continue
        removed += 1
    return removed
