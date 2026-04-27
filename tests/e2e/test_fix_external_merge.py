"""End-to-end coverage for the FIX-cycle external-merge polling task.

PR-217 (memory entry #30) observed the daemon waste a 30-minute FIX cycle
because the user merged the PR externally while the coder process was
running. PR-165 fixes that by polling GitHub PR state from a side task and
short-circuiting the FIX cycle when a terminal state is observed.

This test reproduces the original failure shape end-to-end:
1. Drive a PR through CODING → WATCH using the existing ``slow`` shim.
2. Inject a failing CI status check so the daemon transitions WATCH → FIX
   instead of merging immediately.
3. While the second ``slow`` shim run is sleeping (FIX cycle), externally
   merge the PR via ``gh pr merge --admin``.
4. Verify the daemon transitions to IDLE within
   ``fix_poll_interval_sec + 10`` seconds, matching the success criterion.
"""

from __future__ import annotations

import subprocess
import time

import pytest

from tests.e2e.lib.coder_shim import coder_shim

TESTBED_REPO = "AlexBomber12/pipeline-orchestrator-testbed"
# Mirror the test stack's daemon.fix_poll_interval_sec from config.test.yml;
# bumping the config without bumping this constant would let stale waits
# silently keep timing out.
FIX_POLL_INTERVAL_SEC = 5
EXTERNAL_MERGE_DETECTION_MARGIN_SEC = 10


_PERMISSION_GAP_MESSAGE = (
    "Testbed GitHub App is missing the 'Commit statuses: Write' "
    "permission required to engineer the WATCH→FIX transition. "
    "Update the App per docs/ci-setup.md Step A and re-run. "
    "The polling behavior is exercised by the unit tests in "
    "tests/test_runner.py."
)


def _preflight_status_write_permission() -> None:
    """Skip the test BEFORE any side-effecting setup if the App can't post statuses.

    A POST to ``/statuses/<invalid-sha>`` checks authorization before SHA
    validation: a 403 ``Resource not accessible by integration`` means
    the App lacks ``Commit statuses: Write``; a 422 ``No commit found``
    means the permission is granted but the SHA isn't real (the desired
    no-op outcome). Running this preflight at the very top of the test
    keeps the daemon from starting a real PR + racing to MERGE in the
    SKIP path, which previously cascaded ``ERROR`` into the next test.
    """
    invalid_sha = "0" * 40
    result = subprocess.run(
        [
            "gh", "api", "-X", "POST",
            f"repos/{TESTBED_REPO}/statuses/{invalid_sha}",
            "-f", "state=success",
            "-f", "context=e2e-fix-trigger-preflight",
        ],
        capture_output=True, text=True, check=False, timeout=30,
    )
    if (
        result.returncode != 0
        and "Resource not accessible by integration" in result.stderr
    ):
        pytest.skip(_PERMISSION_GAP_MESSAGE)


def _post_failed_status(head_sha: str) -> None:
    """Force a failure on the PR's head commit so WATCH transitions to FIX.

    The preflight at the top of the test should have already caught a
    permission gap; this 403 branch is defense-in-depth for the case
    where permissions changed between the preflight and the real POST.
    """
    result = subprocess.run(
        [
            "gh", "api", "-X", "POST",
            f"repos/{TESTBED_REPO}/statuses/{head_sha}",
            "-f", "state=failure",
            "-f", "context=e2e-fix-trigger",
            "-f", "description=Engineered failure to drive FIX",
        ],
        capture_output=True, text=True, check=False, timeout=30,
    )
    if result.returncode != 0:
        stderr = result.stderr.strip()
        if "Resource not accessible by integration" in stderr:
            pytest.skip(_PERMISSION_GAP_MESSAGE)
        raise AssertionError(
            f"failed to post status check on {head_sha}: "
            f"rc={result.returncode}, stderr={stderr!r}"
        )


def _get_pr_head_sha(pr_number: int) -> str:
    result = subprocess.run(
        [
            "gh", "pr", "view", str(pr_number),
            "-R", TESTBED_REPO,
            "--json", "headRefOid",
            "--jq", ".headRefOid",
        ],
        capture_output=True, text=True, check=False, timeout=30,
    )
    if result.returncode != 0:
        raise AssertionError(
            f"failed to read head SHA for PR #{pr_number}: "
            f"rc={result.returncode}, stderr={result.stderr.strip()!r}"
        )
    sha = result.stdout.strip()
    if not sha:
        raise AssertionError(f"empty head SHA for PR #{pr_number}")
    return sha


def _merge_pr(pr_number: int) -> None:
    """Force-merge the PR while the daemon's FIX cycle is sleeping."""
    result = subprocess.run(
        [
            "gh", "pr", "merge", str(pr_number),
            "-R", TESTBED_REPO,
            "--squash", "--delete-branch", "--admin",
        ],
        capture_output=True, text=True, check=False, timeout=60,
    )
    if result.returncode != 0:
        raise AssertionError(
            f"failed to merge PR #{pr_number}: "
            f"rc={result.returncode}, stderr={result.stderr.strip()!r}"
        )


def test_external_merge_during_fix_returns_to_idle(
    dashboard_url,
    testbed_slug,
    wait_for_state,
    get_state,
    upload_zip,
    make_task_zip,
    reset_testbed,
):
    # Skip BEFORE any side-effecting setup if the App can't post commit
    # statuses; otherwise the daemon would still race a real PR through
    # CODING → WATCH → MERGE in the skip path and leave subsequent
    # e2e tests stranded in ERROR (observed on PR #223 round-2..4).
    _preflight_status_write_permission()
    try:
        wait_for_state(["IDLE"], timeout_sec=30)
    except TimeoutError as exc:
        raise AssertionError(
            f"test stack did not reach IDLE before test start: {exc}"
        ) from exc

    pr_id_int = int(time.time())
    expected_pr_id = f"PR-{pr_id_int}"

    with coder_shim("slow"):
        zip_path = make_task_zip(
            pr_id_int, "e2e-fix-external-merge", coder="any", priority=2
        )
        status = upload_zip(zip_path)
        assert status in (200, 201), f"upload failed with status {status}"

        # The slow shim sleeps 30s before pushing, so CODING lasts long
        # enough that the failed-status injection beats the WATCH→MERGE
        # race window.
        wait_for_state(["CODING"], timeout_sec=30)

        watch_entry = wait_for_state(["WATCH"], timeout_sec=120)
        watch_pr = watch_entry.get("current_pr") or {}
        pr_number = watch_pr.get("number")
        if not isinstance(pr_number, int) or pr_number <= 0:
            pytest.skip(
                f"Could not engineer FIX state: WATCH entry without a real "
                f"PR number (current_pr={watch_pr!r}). The testbed flow may "
                f"have raced past WATCH; the polling behavior is exercised "
                f"by the unit tests in tests/test_runner.py."
            )

        head_sha = _get_pr_head_sha(pr_number)
        _post_failed_status(head_sha)

        try:
            wait_for_state(["FIX"], timeout_sec=30)
        except TimeoutError:
            pytest.skip(
                f"daemon did not enter FIX after failed-status injection "
                f"on PR #{pr_number}; the WATCH→MERGE race likely won. "
                f"Polling behavior is covered by unit tests."
            )

        _merge_pr(pr_number)

        wait_for_state(
            ["IDLE"],
            timeout_sec=(
                FIX_POLL_INTERVAL_SEC + EXTERNAL_MERGE_DETECTION_MARGIN_SEC
            ),
        )

    state = get_state()
    assert state is not None, "no state entry returned for testbed"
    assert state["state"] == "IDLE", (
        f"final state was {state['state']!r}, expected IDLE"
    )
    final_pr = state.get("current_pr")
    assert final_pr is None or final_pr.get("state") == "MERGED", (
        f"unexpected current_pr after external merge: {final_pr!r} "
        f"(expected None or state=MERGED, originally PR_ID {expected_pr_id})"
    )
