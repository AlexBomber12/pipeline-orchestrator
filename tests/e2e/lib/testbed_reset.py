"""Helpers to reset the testbed repository to a known-clean state.

Used by the session-scoped fixture at pytest session start. NOT used between
individual tests; the per-test reset_testbed fixture in conftest handles the
lighter per-test cleanup.
"""

from __future__ import annotations

import os
import shutil
import subprocess
import tempfile
from pathlib import Path

TESTBED_REPO = "AlexBomber12/pipeline-orchestrator-testbed"
TESTBED_URL = f"https://github.com/{TESTBED_REPO}.git"


def _clone_url() -> str:
    # Embed GH_TOKEN (set on the integration job) so `git push` is
    # authenticated without needing a host-side `gh auth setup-git`.
    token = os.environ.get("GH_TOKEN", "").strip()
    if not token:
        return TESTBED_URL
    return f"https://x-access-token:{token}@github.com/{TESTBED_REPO}.git"


def close_all_open_prs() -> int:
    """Close every open PR on testbed with --delete-branch. Returns count closed."""
    # `gh pr list` defaults to --limit 30; pass an explicit large cap so the
    # session-start reset cannot leave older open PRs behind on a busy testbed.
    listing = subprocess.run(
        [
            "gh",
            "pr",
            "list",
            "-R",
            TESTBED_REPO,
            "--state",
            "open",
            "--limit",
            "1000",
            "--json",
            "number",
            "--jq",
            ".[].number",
        ],
        capture_output=True,
        text=True,
        check=False,
        timeout=30,
    )
    closed = 0
    for line in listing.stdout.splitlines():
        n = line.strip()
        if not n:
            continue
        result = subprocess.run(
            ["gh", "pr", "close", n, "-R", TESTBED_REPO, "--delete-branch"],
            capture_output=True,
            text=True,
            check=False,
            timeout=30,
        )
        if result.returncode == 0:
            closed += 1
    return closed


def delete_non_main_branches() -> int:
    """Delete every branch on testbed that is NOT main. Returns count deleted."""
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
    """Clone testbed, remove tasks/ if present, push back. Returns True if a push happened."""
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
            return False
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
            return False
        push = subprocess.run(
            ["git", "-C", str(repo), "push", "origin", "main"],
            capture_output=True,
            text=True,
            check=False,
            timeout=30,
        )
        return push.returncode == 0
    finally:
        shutil.rmtree(workdir, ignore_errors=True)


def reset_testbed_full() -> dict:
    """Full reset: close PRs, delete branches, wipe tasks/. Returns counts dict."""
    return {
        "prs_closed": close_all_open_prs(),
        "branches_deleted": delete_non_main_branches(),
        "tasks_wiped": wipe_tasks_dir_on_main(),
    }
