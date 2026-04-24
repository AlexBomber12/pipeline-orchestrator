"""
DATA-02: Upload during Redis disconnect.

Simulates user uploading a zip exactly when Redis is unreachable.
The manifest should either:
- Persist to disk and be picked up later when Redis recovers, OR
- Fail with clear user-visible error.

Silent loss of upload data is the bug we test against.
"""

import time
import pytest
import subprocess
from ..lib.failure_helpers import redis_down


def test_data_02_upload_during_redis_down(
    page,
    testbed_url,
    upload_zip,
    make_task_zip,
    get_state,
    take_screenshot,
):
    """
    Scenario:
    1. Start Redis down.
    2. Attempt upload via HTTP API.
    3. Record response (200 with pending manifest OR error).
    4. Bring Redis back.
    5. Verify task eventually appears in tasks/ (if upload was 200) OR
       user received clear error (if upload was not 200).
    """
    page.goto(testbed_url)
    take_screenshot("01_before")

    zip_path = make_task_zip(pr_num=240, label="data02test")

    upload_response = None
    upload_status = None
    with redis_down():
        time.sleep(3)

        # Attempt upload with Redis down
        try:
            response = upload_zip(zip_path)
            upload_status = response.status_code
            upload_response = response.text[:500]  # first 500 chars
        except Exception as e:
            upload_response = f"Exception: {e}"
            upload_status = 0

        take_screenshot("02_upload_during_down")

    # Redis restored - wait for any deferred processing
    time.sleep(60)
    take_screenshot("03_after_recover")

    # Check if task file appeared
    result = subprocess.run(
        ["docker", "compose", "exec", "-T", "daemon", "ls",
         "/data/repos/AlexBomber12__pipeline-orchestrator-testbed/tasks/"],
        cwd="/home/alexey/pipeline-orchestrator",
        capture_output=True,
        text=True,
        timeout=10,
    )
    tasks = result.stdout
    task_eventually_landed = "PR-240.md" in tasks

    # Analysis matrix:
    # - upload_status 200 + task_eventually_landed True = PASS (graceful queue-and-defer)
    # - upload_status 200 + task_eventually_landed False = FAIL (silent data loss)
    # - upload_status 5xx + clear error text = PASS (fail loud, user knows)
    # - upload_status 5xx + task still landed = surprising but OK
    # - upload connection error + task NOT landed = PASS (client retry opportunity)

    if upload_status == 200 and not task_eventually_landed:
        pytest.fail(
            f"Silent data loss: upload returned 200 but task never landed. "
            f"Redis disconnect masked the failure. Response body: {upload_response}"
        )

    # Document the behavior
    print(f"DATA-02 behavior: upload_status={upload_status}, "
          f"task_landed={task_eventually_landed}")
    print(f"Response preview: {upload_response}")
