"""
EXT-01: Claude CLI exits 0 without creating PR.

Verifies that MICRO PR-190a retry counter prevents livelock.
After 2 consecutive "coder succeeded but no PR found" events on the same
task, daemon must transition to HUNG state with the user-visible blocked
event message.

This is the FIRST test of Day 4 because all subsequent tests depend on
the livelock prevention working. If this test fails, the rest of the suite
is SKIPPED to avoid wasting claude API tokens in potential loops.
"""

import time
import pytest
from ..lib.failure_helpers import claude_shim


pytestmark = pytest.mark.order(1)  # Run first


def test_ext_01_claude_no_pr_triggers_hung_after_2_attempts(
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
    1. Install claude shim that exits 0 without creating PR.
    2. Upload task PR-200.
    3. Wait for daemon to pick up, first failure (counter=1, state returns IDLE via FIX).
    4. Daemon auto-re-picks same task, second failure (counter=2, state HUNG).
    5. Verify state=HUNG in Redis.
    6. Verify event log contains blocked message.
    7. Verify dashboard badge shows HUNG.
    """
    page.goto(testbed_url)
    take_screenshot("01_before")

    with claude_shim("exit_0_no_pr"):
        # Upload the task
        zip_path = make_task_zip(pr_num=200, label="test200")
        response = upload_zip(zip_path)
        assert response.status_code == 200, f"Upload failed: {response.status_code}"
        upload_ts = time.time()

        # Wait for first pick up and failure
        # Expected timeline:
        #   ~10s:    Pick task PR-200, state=CODING
        #   ~30s:    claude shim exits, "no PR found" detected, retry 1/3
        #   ~45s:    retry exhausted, state FIX then IDLE
        #   ~2-3m:   second pickup (same task, counter=2 trigger)
        #   ~3-4m:   second failure, state=HUNG
        try:
            final_state = wait_for_state("HUNG", timeout_sec=360)
        except TimeoutError:
            # Maybe it took longer, or maybe PR-190a does not work
            curr = get_state()
            take_screenshot("02_timeout_final")
            pytest.fail(
                f"State did not become HUNG within 360s. "
                f"Current state: {curr}. "
                f"Possible: PR-190a retry counter not working."
            )

    # Verify state fields
    assert final_state["state"] == "HUNG", f"Expected HUNG, got {final_state['state']}"

    # Verify event log contains blocked message
    history = final_state.get("history", [])
    blocked_events = [
        e for e in history
        if "blocked" in e.get("message", "").lower()
        and "manual intervention" in e.get("message", "").lower()
    ]
    assert blocked_events, (
        f"Expected 'blocked ... manual intervention' event in history. "
        f"Last 5 events: {history[-5:]}"
    )

    # Verify UI shows HUNG
    page.goto(testbed_url)
    page.wait_for_load_state("domcontentloaded")
    page.wait_for_timeout(2000)  # let SSE catch up
    take_screenshot("03_after_hung")

    # Check badge contains HUNG text (flexible match for any case/decoration)
    badge_text = page.locator("[data-state-badge]").first.text_content() or ""
    page_content = page.content()
    assert "HUNG" in badge_text.upper() or "HUNG" in page_content, (
        f"Dashboard does not show HUNG state. Badge text: '{badge_text}'"
    )


def test_ext_01_counter_resets_on_different_task(
    page,
    testbed_url,
    upload_zip,
    make_task_zip,
    wait_for_state,
    get_state,
    take_screenshot,
):
    """
    Verify that after a task enters HUNG with counter=2, a different task
    can be picked and processed normally (counter does not leak across tasks).

    This runs AFTER test_ext_01_claude_no_pr_triggers_hung so the previous
    task PR-200 is in HUNG state. We resume, upload a different task PR-201
    (without shim), and verify it processes normally.
    """
    take_screenshot("00_before_resume")

    # Click Resume (Play) to clear HUNG
    # The endpoint is /repos/{slug}/resume
    import requests
    resume_url = testbed_url.replace("/repo/", "/repos/") + "/resume"
    r = requests.post(resume_url, timeout=10)
    # If resume does not clear HUNG directly, the daemon will re-pick same task
    # and fail again. That's fine: we verify the counter reset logic by
    # uploading a DIFFERENT task next.

    # Upload a different task, without shim (claude works normally)
    zip_path = make_task_zip(pr_num=201, label="test201")
    response = upload_zip(zip_path)
    assert response.status_code == 200

    # Wait for daemon to pick up this new task (state=CODING on PR-201)
    # OR wait up to 5 minutes - if PR-201 merges, even better.
    # We only need to verify state progressed past HUNG-for-PR-200.
    time.sleep(90)
    state = get_state()
    take_screenshot("01_after_new_task")

    # State must not be HUNG anymore. Could be CODING, WATCH, IDLE, MERGE, etc.
    assert state["state"] != "HUNG", (
        f"Expected state to progress past HUNG after different task picked. "
        f"Still HUNG with state: {state}"
    )
    # Verify current_task is PR-201, not PR-200
    current = state.get("current_task") or {}
    current_pr = current.get("pr_id", "")
    # Either currently working on PR-201 or already finished it (IDLE)
    assert current_pr != "PR-200" or state["state"] == "IDLE", (
        f"Expected PR-201 to be picked or completed. Current: {current_pr}, "
        f"state: {state['state']}"
    )
