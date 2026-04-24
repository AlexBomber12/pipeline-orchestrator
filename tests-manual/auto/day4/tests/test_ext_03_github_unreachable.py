"""
EXT-03: GitHub unreachable.

Block DNS for github.com inside daemon container. Daemon cannot clone,
push, create PR, query PR status. Expected behavior:
- Daemon retries with backoff (not infinite fast retry).
- UI shows the error, not hangs.
- When GitHub becomes reachable again, daemon self-recovers.
"""

import time
import pytest
from ..lib.failure_helpers import block_host


def test_ext_03_github_unreachable_no_fast_retry(
    page,
    testbed_url,
    upload_zip,
    make_task_zip,
    wait_for_state,
    get_state,
    take_screenshot,
):
    """
    Scenario:
    1. Block github.com DNS in daemon container.
    2. Upload a task. Daemon will try to push to GitHub and fail.
    3. Verify daemon does not spam retries faster than backoff interval.
    4. Verify error is surfaced in event log.
    5. Unblock github.com.
    6. Verify daemon recovers and completes task.
    """
    page.goto(testbed_url)
    take_screenshot("01_before_block")

    with block_host("github.com"):
        zip_path = make_task_zip(pr_num=220, label="ext03test")
        response = upload_zip(zip_path)
        # Upload itself may succeed (just writes to local staging)
        assert response.status_code == 200

        # Wait 2 minutes and verify daemon did not spam retries
        time.sleep(120)

        state = get_state()
        take_screenshot("02_during_block")
        history = state.get("history", [])

        # Count GitHub-related failures in the last 2 minutes
        import datetime
        threshold = datetime.datetime.utcnow() - datetime.timedelta(minutes=2)

        github_failure_events = [
            e for e in history
            if "github" in e.get("message", "").lower()
            or "could not resolve" in e.get("message", "").lower()
            or "0.0.0.0" in e.get("message", "").lower()
        ]

        # Backoff should keep retry count bounded. If daemon spams, we'd see >20.
        # Normal backoff (e.g. exponential) should produce <10 in 2 minutes.
        assert len(github_failure_events) < 20, (
            f"Daemon spammed GitHub retries without backoff: "
            f"{len(github_failure_events)} events in 2 minutes. "
            f"Sample: {github_failure_events[-5:]}"
        )

        # At least one event should mention the failure, so user is informed
        assert len(github_failure_events) >= 1, (
            "No GitHub-failure events found in history. User not informed about outage."
        )

    # GitHub restored - verify recovery within 3 minutes
    time.sleep(180)
    final = get_state()
    take_screenshot("03_after_unblock")

    # Either task completed (IDLE, queue_done increased) or picked up normally (CODING/WATCH)
    # Main criterion: daemon is not stuck in a failure loop anymore
    assert final["state"] in ["IDLE", "CODING", "WATCH", "FIX", "MERGE"], (
        f"Daemon did not recover after github restored. State: {final['state']}"
    )
