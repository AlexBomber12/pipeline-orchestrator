"""Helpers to reset the testbed repository to a known-clean state.

Used by the session-scoped fixture at pytest session start and the per-test
reset_testbed fixture in conftest.
"""

from __future__ import annotations

import os
import shutil
import subprocess
import tempfile
from pathlib import Path
from urllib.parse import quote

TESTBED_REPO = "AlexBomber12/pipeline-orchestrator-testbed"
TESTBED_URL = f"https://github.com/{TESTBED_REPO}.git"
TESTBED_COMPOSE_FILE = "docker-compose.test.yml"


def _clone_url() -> str:
    # Embed GH_TOKEN (set on the integration job) so `git push` is
    # authenticated without needing a host-side `gh auth setup-git`.
    token = os.environ.get("GH_TOKEN", "").strip()
    if not token:
        return TESTBED_URL
    return f"https://x-access-token:{token}@github.com/{TESTBED_REPO}.git"


def close_all_open_prs() -> int:
    """Close every open PR on testbed with --delete-branch. Returns count closed.

    Raises RuntimeError if the underlying ``gh api`` call fails. Per-PR
    close failures are tolerated (count reflects only successes) so a single
    bad PR cannot block the rest of the cleanup.
    """
    listing = subprocess.run(
        [
            "gh",
            "api",
            "--paginate",
            f"repos/{TESTBED_REPO}/pulls?state=open&per_page=100",
            "--jq",
            '.[] | [.number, .head.ref] | @tsv',
        ],
        capture_output=True,
        text=True,
        check=False,
        timeout=30,
    )
    if listing.returncode != 0:
        # Surface listing failures (typically gh auth/API errors) instead of
        # silently treating them as "no PRs to close" — otherwise stale PRs
        # would survive the session-start reset.
        raise RuntimeError(
            f"close_all_open_prs: gh api listing failed (rc={listing.returncode}): "
            f"{(listing.stderr or listing.stdout).strip()}"
        )
    closed = 0
    for line in listing.stdout.splitlines():
        parts = line.strip().split("\t")
        n = parts[0] if parts else ""
        branch = parts[1] if len(parts) > 1 else ""
        if not n:
            continue
        result = subprocess.run(
            [
                "gh",
                "api",
                "-X",
                "PATCH",
                f"repos/{TESTBED_REPO}/pulls/{n}",
                "-f",
                "state=closed",
            ],
            capture_output=True,
            text=True,
            check=False,
            timeout=30,
        )
        if result.returncode == 0:
            closed += 1
        if branch and branch != "main":
            subprocess.run(
                [
                    "gh",
                    "api",
                    "-X",
                    "DELETE",
                    f"repos/{TESTBED_REPO}/git/refs/heads/{quote(branch, safe='')}",
                ],
                capture_output=True,
                text=True,
                check=False,
                timeout=30,
            )
    return closed


def delete_non_main_branches() -> int:
    """Delete every branch on testbed that is NOT main. Returns count deleted.

    Raises RuntimeError if the underlying ``gh api`` listing call fails.
    Per-branch delete failures are tolerated (count reflects only
    successes) so a single bad branch cannot block the rest of the cleanup.
    """
    # `gh api` returns one page (30) by default; --paginate walks every page so
    # the cleanup sees all branches when the testbed accumulates more than 30.
    listing = subprocess.run(
        [
            "gh",
            "api",
            "--paginate",
            f"repos/{TESTBED_REPO}/branches",
            "--jq",
            ".[].name",
        ],
        capture_output=True,
        text=True,
        check=False,
        timeout=30,
    )
    if listing.returncode != 0:
        # Surface listing failures so we never silently treat an auth/API
        # error as "no non-main branches present" and leave stale refs.
        raise RuntimeError(
            f"delete_non_main_branches: gh api listing failed "
            f"(rc={listing.returncode}): "
            f"{(listing.stderr or listing.stdout).strip()}"
        )
    deleted = 0
    for line in listing.stdout.splitlines():
        name = line.strip()
        if not name or name == "main":
            continue
        result = subprocess.run(
            [
                "gh",
                "api",
                "-X",
                "DELETE",
                f"repos/{TESTBED_REPO}/git/refs/heads/{name}",
            ],
            capture_output=True,
            text=True,
            check=False,
            timeout=30,
        )
        if result.returncode == 0:
            deleted += 1
    return deleted


def wipe_tasks_dir_on_main() -> bool:
    """Clone testbed, remove tasks/ if present, push back. Returns True iff a push happened.

    Returns False ONLY when there is nothing to wipe (the cloned main branch
    has no ``tasks/`` directory). Real failures — clone, commit, or push —
    raise RuntimeError so the session-start fixture can surface auth/API
    regressions instead of silently leaving a polluted testbed.
    """
    workdir = Path(tempfile.mkdtemp(prefix="testbed-reset-"))
    try:
        clone = subprocess.run(
            ["git", "clone", "--depth", "1", _clone_url(), str(workdir / "repo")],
            capture_output=True,
            text=True,
            check=False,
            timeout=60,
        )
        if clone.returncode != 0:
            raise RuntimeError(
                f"wipe_tasks_dir_on_main: git clone failed "
                f"(rc={clone.returncode}): {clone.stderr.strip()}"
            )
        repo = workdir / "repo"
        tasks = repo / "tasks"
        if not tasks.exists():
            return False
        subprocess.run(
            ["git", "-C", str(repo), "config", "user.email", "testbed-reset@test.invalid"],
            check=False,
            timeout=10,
        )
        subprocess.run(
            ["git", "-C", str(repo), "config", "user.name", "Testbed Reset Fixture"],
            check=False,
            timeout=10,
        )
        subprocess.run(
            ["git", "-C", str(repo), "rm", "-rf", "tasks/"],
            capture_output=True,
            check=False,
            timeout=10,
        )
        commit = subprocess.run(
            ["git", "-C", str(repo), "commit", "-m", "test: reset tasks/ at session start"],
            capture_output=True,
            text=True,
            check=False,
            timeout=10,
        )
        if commit.returncode != 0:
            raise RuntimeError(
                f"wipe_tasks_dir_on_main: git commit failed "
                f"(rc={commit.returncode}): {commit.stderr.strip()}"
            )
        push = subprocess.run(
            ["git", "-C", str(repo), "push", "origin", "main"],
            capture_output=True,
            text=True,
            check=False,
            timeout=30,
        )
        if push.returncode != 0:
            raise RuntimeError(
                f"wipe_tasks_dir_on_main: git push failed "
                f"(rc={push.returncode}): {push.stderr.strip()}"
            )
        return True
    finally:
        shutil.rmtree(workdir, ignore_errors=True)


def clear_testbed_redis_state(slug: str) -> int:
    """Clear daemon/upload state for the testbed slug through docker compose exec."""
    base_cmd = [
        "docker",
        "compose",
        "-f",
        TESTBED_COMPOSE_FILE,
        "exec",
        "-T",
        "redis-test",
        "redis-cli",
    ]
    try:
        keys_result = subprocess.run(
            [*base_cmd, "KEYS", f"control:{slug}:*"],
            capture_output=True,
            text=True,
            check=False,
            timeout=10,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise RuntimeError(f"clear_testbed_redis_state: redis KEYS failed: {exc}") from exc
    if keys_result.returncode != 0:
        raise RuntimeError(
            "clear_testbed_redis_state: redis KEYS failed "
            f"(rc={keys_result.returncode}): "
            f"{(keys_result.stderr or keys_result.stdout).strip()}"
        )

    control_keys = [line.strip() for line in keys_result.stdout.splitlines() if line.strip()]
    try:
        del_result = subprocess.run(
            [
                *base_cmd,
                "DEL",
                f"pipeline:{slug}",
                f"upload:{slug}:pending",
                *control_keys,
            ],
            capture_output=True,
            text=True,
            check=False,
            timeout=10,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise RuntimeError(f"clear_testbed_redis_state: redis DEL failed: {exc}") from exc
    if del_result.returncode != 0:
        raise RuntimeError(
            "clear_testbed_redis_state: redis DEL failed "
            f"(rc={del_result.returncode}): "
            f"{(del_result.stderr or del_result.stdout).strip()}"
        )
    try:
        return int(del_result.stdout.strip())
    except ValueError as exc:
        raise RuntimeError(
            "clear_testbed_redis_state: redis DEL returned non-integer output: "
            f"{del_result.stdout.strip()}"
        ) from exc


def reset_testbed_full(slug: str) -> dict:
    """Full reset: wipe tasks/, delete branches, close PRs. Returns counts dict.

    Operation order is causally aligned with daemon awareness: 1) wipe tasks/ on main removes the source of work; 2) delete non-main branches removes feature artifacts; 3) close PRs is the daemon-visible signal that work has ended. Future contributors adding cleanup operations must keep this monotonic ordering so the daemon sampled at any point during cleanup never observes work-in-progress after the PR-close signal.

    The GitHub cleanup is repository-wide; callers pass the same slug they use
    for paired Redis cleanup so fixture call sites reset one testbed identity.

    Propagates RuntimeError from any helper that detects a hard failure
    (listing call failed, clone/commit/push failed). The session-scoped
    fixture relies on this to abort before tests run against a polluted
    testbed.
    """
    tasks_wiped = wipe_tasks_dir_on_main()
    branches_deleted = delete_non_main_branches()
    prs_closed = close_all_open_prs()
    return {
        "prs_closed": prs_closed,
        "branches_deleted": branches_deleted,
        "tasks_wiped": tasks_wiped,
    }
