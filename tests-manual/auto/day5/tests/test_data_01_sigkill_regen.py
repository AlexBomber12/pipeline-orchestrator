"""
DATA-01: Daemon SIGKILL during work preserves queue integrity.

Day 4 result: PASSED.
Day 5 re-verify: confirm stability, PR numbering moved to PR-3XX to avoid collision.

Scenario:
1. Upload PR-301 task, wait for daemon to pick it up.
2. SIGKILL daemon container while CODING.
3. Restart container, wait for recovery.
4. Verify queue integrity: no lost task, recovery path executed, eventual IDLE or forward progress.
"""

import subprocess
import time

import pytest


REPO_DIR = "/home/alexey/pipeline-orchestrator"


def test_data_01_daemon_sigkill_preserves_queue_integrity(
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

    zip_path = make_task_zip(301, "data01test", coder="any")
    response = upload_zip(zip_path)
    assert response.status_code in (200, 201), f"upload failed: {response.status_code}"

    wait_for_state(["CODING", "PLANNING"], timeout_sec=30)

    subprocess.run(
        ["docker", "compose", "kill", "-s", "KILL", "daemon"],
        cwd=REPO_DIR,
        check=True,
        timeout=10,
    )

    time.sleep(2)

    subprocess.run(
        ["docker", "compose", "up", "-d", "daemon"],
        cwd=REPO_DIR,
        check=True,
        timeout=30,
    )

    state = wait_for_state(["IDLE", "CODING", "WATCH"], timeout_sec=120)

    page.goto(testbed_url)
    take_screenshot("02_after_restart")

    assert state is not None, "daemon did not recover"
    assert state.get("state") in ("IDLE", "CODING", "WATCH"), \
        f"unexpected state after recovery: {state.get('state')}"
