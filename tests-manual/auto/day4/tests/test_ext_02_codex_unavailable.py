"""
EXT-02: Codex CLI unavailable.

When codex CLI is uninstalled or crashes, daemon should handle gracefully:
either retry with fallback coder if configured, or mark task BLOCKED/HUNG
with clear error message. Should not loop indefinitely.
"""

import time
import pytest
from ..lib.failure_helpers import codex_shim


def test_ext_02_codex_not_found_graceful_fallback(
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
    1. Simulate codex CLI uninstalled (command not found).
    2. Try to trigger a codex-invoked code path (review retry).
    3. Verify daemon does NOT enter infinite loop.
    4. Daemon state should eventually stabilize in IDLE, HUNG, or ERROR with
       a user-visible event message about codex unavailable.

    Note: Codex is invoked in multiple places. This test focuses on
    observable behavior: daemon does not lock up or livelock.
    """
    page.goto(testbed_url)
    take_screenshot("01_before")

    # Get initial state for comparison
    initial = get_state()
    initial_state_name = initial["state"] if initial else "unknown"

    with codex_shim("not_found"):
        # Upload a task that normally completes
        zip_path = make_task_zip(pr_num=210, label="ext02test")
        response = upload_zip(zip_path)
        assert response.status_code == 200

        # Wait up to 6 minutes for state to stabilize.
        # Success criteria: state ends up in {IDLE, HUNG, ERROR}.
        # Failure criteria: state still transitioning (CODING/WATCH/FIX) after 6 min,
        # which suggests livelock.
        stable_states = ["IDLE", "HUNG", "ERROR"]

        try:
            final = wait_for_state(stable_states, timeout_sec=360)
            take_screenshot("02_stabilized")
        except TimeoutError:
            curr = get_state()
            take_screenshot("02_timeout")
            pytest.fail(
                f"Daemon did not stabilize in {stable_states} after 360s. "
                f"Current: {curr}. Suggests codex-related livelock."
            )

    # Verify there is a user-visible event about the failure
    history = final.get("history", [])
    error_mentions = [
        e for e in history
        if any(kw in e.get("message", "").lower() for kw in ["codex", "coder", "error", "blocked", "failed"])
    ]
    assert error_mentions, (
        "Expected at least one event mentioning codex/coder/error/blocked/failed. "
        f"Last 10 events: {history[-10:]}"
    )
