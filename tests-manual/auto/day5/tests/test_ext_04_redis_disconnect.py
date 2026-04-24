"""
EXT-04: Redis down and recover — UI gracefully degrades, daemon resumes after recovery.

Day 4 result: PASSED. OBS-11 positive finding documented.
Day 5 re-verify: confirm regression-free.
"""

import time

import pytest

from lib.failure_helpers import redis_down


def test_ext_04_redis_down_and_recover(
    page,
    testbed_url,
    testbed_slug,
    dashboard_url,
    wait_for_state,
    get_state,
    take_screenshot,
    reset_testbed_clean,
):
    page.goto(testbed_url)
    take_screenshot("01_before")

    with redis_down(duration_sec=15):
        time.sleep(3)
        page.goto(testbed_url)
        take_screenshot("02_during_down_error")

        body_text = page.locator("body").text_content() or ""
        has_degraded_ui = any(
            marker in body_text.lower()
            for marker in ["redis unavailable", "state unknown", "preflight"]
        )
        assert has_degraded_ui, \
            f"Expected UI to show Redis-down indicator, did not find markers. " \
            f"Body text excerpt: {body_text[:500]}"

    time.sleep(5)
    page.goto(testbed_url)
    take_screenshot("03_after_recover")

    state = get_state()
    assert state is not None, "state unreadable after recovery"
    assert state.get("state") in ("IDLE", "CODING", "WATCH", "FIX"), \
        f"daemon did not reach known state after Redis recovery: {state.get('state')}"
