"""End-to-end coverage for WATCH-to-FIX on a real CHANGES_REQUESTED review.

PR-248 wired the WATCH-to-FIX transition on
``ReviewStatus.CHANGES_REQUESTED`` with the
``_has_new_codex_feedback_since_last_push`` freshness gate so the daemon
does not loop into FIX on a stale review. The transition path lives in
``src/daemon/handlers/watch.py`` and depends on three signals working
together: GitHub's report of the review state, the head commit
timestamp, and the review/comment cache. Unit tests cover each signal
in isolation; this e2e drives an actual ``CHANGES_REQUESTED`` review
through the WATCH cycle so an integration regression in any one of
those layers surfaces here as a single failure.

The task spec (``tasks/PR-258c.md``) explicitly forbids skipping on a
permission gap for ``Pull requests: Write``: that scope is in the
testbed App's documented standard set per ``docs/ci-setup.md`` Step A
and a missing scope would also break a slice of production. If
``gh api`` returns 403 on the review post the test fails loud rather
than skipping.

The test does skip when the App lacks ``Commit statuses: Write``, the
permission required by the ``success_pending_ci`` shim to gate the
WATCH merge race. That is the same skip pattern as
``test_fix_external_merge.py`` and is justified for the same reason:
the gating mechanism is purely a test-side workaround and is not part
of the production contract under test.
"""

from __future__ import annotations

import subprocess
import time

import pytest

from tests.e2e.lib.coder_shim import SHIM_SCENARIO_PATH, coder_shim

TESTBED_REPO = "AlexBomber12/pipeline-orchestrator-testbed"

# WATCH poll period in the test stack is 5–10s; allow two polls plus the
# freshness round-trip plus the FIX entry transition with comfortable
# slack before declaring failure.
FIX_TRANSITION_DEADLINE_SEC = 90
# After flipping the shim to ``escalate``, the FIX-cycle parser routes
# the runner back to IDLE without further coder work.
ESCALATE_TO_IDLE_DEADLINE_SEC = 60

_PERMISSION_GAP_MESSAGE = (
    "Testbed GitHub App is missing the 'Commit statuses: Write' "
    "permission required by the success_pending_ci shim to block the "
    "WATCH merge race. Update the App per docs/ci-setup.md Step A and "
    "re-run. The CHANGES_REQUESTED transition is also exercised by the "
    "unit tests in tests/runner/test_handle_watch.py."
)


def _preflight_status_write_permission() -> None:
    """Skip if the testbed App can't post commit statuses.

    The success_pending_ci shim posts a pending commit status on the
    head SHA to block the WATCH→MERGE race window before the test posts
    REQUEST_CHANGES. Without that permission the shim's status post is a
    no-op, the daemon merges the PR on the next 2s WATCH poll, and the
    REQUEST_CHANGES post returns HTTP 422 (cannot review a merged PR).
    Probing with ``/statuses/<invalid-sha>`` checks authorization before
    SHA validation: 403 ``Resource not accessible by integration`` means
    the App lacks the permission; 422 ``No commit found`` means the
    permission is granted but the SHA isn't real (the desired no-op).
    Mirrors the preflight in ``test_fix_external_merge.py``.
    """
    invalid_sha = "0" * 40
    result = subprocess.run(
        [
            "gh", "api", "-X", "POST",
            f"repos/{TESTBED_REPO}/statuses/{invalid_sha}",
            "-f", "state=pending",
            "-f", "context=e2e/watch-merge-gate-preflight",
        ],
        capture_output=True, text=True, check=False, timeout=30,
    )
    if (
        result.returncode != 0
        and "Resource not accessible by integration" in result.stderr
    ):
        pytest.skip(_PERMISSION_GAP_MESSAGE)


def test_changes_requested_review_drives_watch_to_fix(
    dashboard_url,
    testbed_slug,
    wait_for_state,
    get_state,
    upload_zip,
    make_task_zip,
    post_review,
    reset_testbed,
):
    # Block side-effecting setup if the App can't post the gating status;
    # otherwise the test would race a real PR through CODING → WATCH →
    # MERGE and HTTP-422 on REQUEST_CHANGES against an already-merged PR.
    _preflight_status_write_permission()
    try:
        wait_for_state(["IDLE"], timeout_sec=30)
    except TimeoutError as exc:
        raise AssertionError(
            f"test stack did not reach IDLE before test start: {exc}"
        ) from exc

    pr_id_int = int(time.time())
    expected_pr_id = f"PR-{pr_id_int}"
    expected_branch = f"pr-{pr_id_int}-e2e-watch-changes-requested"

    # The success_pending_ci shim posts a pending commit status on the
    # head SHA before exiting CODING. ``config.test.yml`` enables
    # ``auto_merge`` and ``allow_merge_without_review``, and
    # ``handle_watch`` merges the moment CI is green and the review is
    # still PENDING. Posting the pending status from inside the shim
    # makes CI=PENDING visible on the daemon's first WATCH poll, so the
    # PR cannot be merged before the test posts REQUEST_CHANGES below.
    with coder_shim("success_pending_ci"):
        zip_path = make_task_zip(
            pr_id_int, "e2e-watch-changes-requested", coder="any", priority=2
        )
        status = upload_zip(zip_path)
        assert status in (200, 201), f"upload failed with status {status}"

        coding_entry = wait_for_state(["CODING"], timeout_sec=30)
        coding_task = coding_entry.get("current_task") or {}
        assert coding_task.get("pr_id") == expected_pr_id, (
            f"CODING was for current_task={coding_task!r}, "
            f"expected pr_id={expected_pr_id!r}"
        )

        watch_entry = wait_for_state(["WATCH"], timeout_sec=120)
        watch_pr = watch_entry.get("current_pr") or {}
        watch_pr_number = watch_pr.get("number")
        assert isinstance(watch_pr_number, int) and watch_pr_number > 0, (
            f"expected WATCH entry with a real PR number, got "
            f"current_pr={watch_pr!r}"
        )
        assert watch_pr.get("branch") == expected_branch, (
            f"WATCH current_pr.branch={watch_pr.get('branch')!r}, "
            f"expected {expected_branch!r}"
        )

        # Post a CHANGES_REQUESTED review via REST. The freshness gate
        # anchors on the head commit timestamp; the review is submitted
        # AFTER the shim's push, so the gate sees it as fresh feedback.
        post_review(
            watch_pr_number,
            event="REQUEST_CHANGES",
            body="e2e: please address review feedback",
        )

        # Poll for FIX with the same PR number. The 90s deadline covers
        # two WATCH polls (5–10s each) plus the freshness round-trip
        # plus the FIX entry transition.
        deadline = time.monotonic() + FIX_TRANSITION_DEADLINE_SEC
        fix_entry: dict | None = None
        last_state = None
        last_pr_number = None
        while time.monotonic() < deadline:
            entry = get_state(testbed_slug)
            if entry is not None:
                last_state = entry.get("state")
                current_pr = entry.get("current_pr") or {}
                last_pr_number = current_pr.get("number")
                if last_state == "FIX" and last_pr_number == watch_pr_number:
                    fix_entry = entry
                    break
            time.sleep(1)
        assert fix_entry is not None, (
            f"daemon did not transition to FIX on PR #{watch_pr_number} "
            f"within {FIX_TRANSITION_DEADLINE_SEC}s of CHANGES_REQUESTED "
            f"review; last_state={last_state!r}, "
            f"last_pr_number={last_pr_number!r}"
        )

        # Verify the production WATCH log emitted the expected line so
        # a regression that bypasses the log path (e.g. a refactor that
        # drops the ``[WATCH] PR #{n}`` prefix) surfaces here too.
        history = fix_entry.get("history") or []
        events = [
            item.get("event", "")
            for item in history
            if isinstance(item, dict)
        ]
        watch_log_marker = f"[WATCH] PR #{watch_pr_number}"
        assert any(watch_log_marker in event for event in events), (
            f"no WATCH log line referencing PR #{watch_pr_number} "
            f"appears in history; recent events={events[-10:]!r}"
        )

        # Switch the shim mid-flight: the next FIX-cycle invocation
        # emits the ESCALATE marker, parking the runner in IDLE without
        # a second push or merge so the testbed is left in a clean
        # state for the next test.
        SHIM_SCENARIO_PATH.write_text("escalate\n")

        wait_for_state(
            ["IDLE"], timeout_sec=ESCALATE_TO_IDLE_DEADLINE_SEC,
        )

    final_state = get_state()
    assert final_state is not None, "no state entry returned for testbed"
    assert final_state["state"] == "IDLE", (
        f"final state was {final_state['state']!r}, expected IDLE"
    )
