"""
EXT-01: PR-190a counter triggers HUNG after 2 consecutive no-PR failures.

Day 4 result:
- sub-test 1 (no_pr_triggers_hung): FAIL due to OBS-9 (shim install ran as runner, not root).
- sub-test 2 (counter_resets_on_different_task): PASSED.

Day 5 re-verify: with OBS-9 fixed (claude_shim now uses -u root), sub-test 1 should
work. Both sub-tests kept.

OBS-1 natural experiment from Day 4 cleanup already proved PR-190a works in production,
so a Day 5 fail here would be a shim-install bug, not a product bug.
"""

import time

import pytest

from lib.failure_helpers import claude_shim


def test_ext_01_claude_no_pr_triggers_hung_after_2_attempts(
    page,
    testbed_url,
    testbed_slug,
    wait_for_state,
    get_state,
    upload_zip,
    make_task_zip,
    take_screenshot,
    reset_testbed_clean,
):
    page.goto(testbed_url)
    take_screenshot("01_before")

    wait_for_state(["IDLE"], timeout_sec=30)

    with claude_shim("exit_0_no_pr"):
        zip_path = make_task_zip(310, "ext01test", coder="claude")
        response = upload_zip(zip_path)
        assert response.status_code in (200, 201), f"upload failed: {response.status_code}"

        try:
            state = wait_for_state(["HUNG"], timeout_sec=300)
            page.goto(testbed_url)
            take_screenshot("02_hung_state")

            assert state.get("error_message", "").lower().count("2 times") >= 1 or \
                   state.get("error_message", "").lower().count("manual intervention") >= 1, \
                f"HUNG message does not mention counter: {state.get('error_message')}"
        except TimeoutError:
            current = get_state()
            pytest.fail(
                f"Did not reach HUNG within 5 minutes with exit_0_no_pr shim. "
                f"Current state: {current.get('state') if current else None}. "
                f"Error: {current.get('error_message') if current else None}"
            )


def test_ext_01_counter_resets_on_different_task(
    page,
    testbed_url,
    testbed_slug,
    wait_for_state,
    get_state,
    upload_zip,
    make_task_zip,
    take_screenshot,
    reset_testbed_clean,
):
    page.goto(testbed_url)
    take_screenshot("00_before_resume")

    wait_for_state(["IDLE"], timeout_sec=30)

    zip_path = make_task_zip(311, "ext01reset", coder="any")
    response = upload_zip(zip_path)
    assert response.status_code in (200, 201), f"upload failed: {response.status_code}"

    time.sleep(10)
    page.goto(testbed_url)
    take_screenshot("01_after_new_task")

    state = get_state()
    assert state is not None
    retry_counter_value = state.get("no_pr_retry_count", 0)
    assert retry_counter_value <= 1, \
        f"Counter not reset on new task pickup: {retry_counter_value}"
