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
``gh api`` returns 403 the test fails loud rather than skipping.
"""

from __future__ import annotations

import time

import pytest

from tests.e2e.lib.coder_shim import SHIM_SCENARIO_PATH, coder_shim

# WATCH poll period in the test stack is 5–10s; allow two polls plus the
# freshness round-trip plus the FIX entry transition with comfortable
# slack before declaring failure.
FIX_TRANSITION_DEADLINE_SEC = 90
# After flipping the shim to ``escalate``, the FIX-cycle parser routes
# the runner back to IDLE without further coder work.
ESCALATE_TO_IDLE_DEADLINE_SEC = 60


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
    try:
        wait_for_state(["IDLE"], timeout_sec=30)
    except TimeoutError as exc:
        raise AssertionError(
            f"test stack did not reach IDLE before test start: {exc}"
        ) from exc

    pr_id_int = int(time.time())
    expected_pr_id = f"PR-{pr_id_int}"
    expected_branch = f"pr-{pr_id_int}-e2e-watch-changes-requested"

    with coder_shim("success"):
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
