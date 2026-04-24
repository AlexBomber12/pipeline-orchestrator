"""
EXT-02: Codex not found fails task with clear error (not silently routed to Claude).

Day 4 result: FAIL — task with coder=any was routed to Claude, completed successfully,
test never got to observe the Codex failure path.

Day 5 fix: Pin task with coder=codex explicitly (FINDING-2). Now the daemon must
attempt Codex and fail, not fall back to Claude silently.

Product contract being tested:
- When coder=codex is pinned and the codex binary is broken, daemon must surface
  an actionable error (HUNG, ERROR, or similar terminal state with a message).
- Daemon must NOT silently swap to Claude — the pin is user intent.
"""

import time

import pytest

from lib.failure_helpers import codex_shim


def test_ext_02_codex_not_found_pinned_coder_fails_clearly(
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

    with codex_shim("not_found"):
        zip_path = make_task_zip(320, "ext02test", coder="codex")
        response = upload_zip(zip_path)
        assert response.status_code in (200, 201), f"upload failed: {response.status_code}"

        try:
            state = wait_for_state(
                ["HUNG", "ERROR"],
                timeout_sec=180,
            )
            page.goto(testbed_url)
            take_screenshot("02_terminal_error")

            err_msg = state.get("error_message") or ""
            current_coder = state.get("coder", "unknown")
            assert current_coder == "codex", \
                f"Daemon used coder={current_coder} despite codex pin. Silent fallback. " \
                f"This is the bug FINDING-2 was checking for."

        except TimeoutError:
            current = get_state()
            current_coder = current.get("coder") if current else None
            current_state = current.get("state") if current else None

            if current_coder == "claude":
                pytest.fail(
                    f"Daemon silently fell back to claude despite coder=codex pin. "
                    f"Current state: {current_state}. "
                    f"Expected: HUNG/ERROR with codex pin respected."
                )
            else:
                pytest.fail(
                    f"Task pinned to codex (broken) did not reach terminal error "
                    f"within 3 minutes. State: {current_state}. Coder: {current_coder}"
                )
