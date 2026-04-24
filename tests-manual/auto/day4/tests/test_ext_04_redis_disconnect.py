"""
EXT-04: Redis disconnect mid-operation.

Stop Redis container briefly. Daemon and web both lose state connectivity.
Expected:
- Dashboard shows banner indicating Redis unreachable.
- Daemon catches exception and retries instead of crashing.
- When Redis returns, both services self-recover without manual restart.
"""

import time
import pytest
import subprocess
from ..lib.failure_helpers import redis_down

REPO_DIR = "/home/alexey/pipeline-orchestrator"


def test_ext_04_redis_down_and_recover(
    page,
    testbed_url,
    get_state,
    take_screenshot,
):
    """
    Scenario:
    1. Initial: daemon and web running, state stable.
    2. Stop redis container.
    3. Navigate dashboard - should show banner "Redis unreachable" or similar.
    4. Start redis.
    5. Wait 30s. Dashboard should recover, no manual web restart needed.
    """
    page.goto(testbed_url)
    page.wait_for_load_state("domcontentloaded")
    take_screenshot("01_before")

    with redis_down():
        # Redis is down for the duration of this block
        time.sleep(5)

        # Navigate dashboard - should handle gracefully
        # Use shorter timeout because requests might hang
        try:
            page.goto(testbed_url, timeout=15000)
            page.wait_for_load_state("domcontentloaded", timeout=10000)
        except Exception as e:
            take_screenshot("02_during_down_error")
            # Browser may fail to load, that's acceptable
            # but we don't pytest.fail here - we measure behavior
            print(f"Dashboard load during Redis-down: {e}")

        take_screenshot("02_during_down")

        # Check daemon logs for graceful handling vs crash
        result = subprocess.run(
            ["docker", "compose", "logs", "daemon", "--tail", "50", "--no-color"],
            cwd=REPO_DIR,
            capture_output=True,
            text=True,
            timeout=10,
        )
        logs = result.stdout.lower()

        # Crash indicators (bad): unhandled Traceback, container restart
        # Graceful indicators (good): "redis connection error, retrying"
        has_crash = "unhandled exception" in logs or "fatal" in logs
        assert not has_crash, (
            f"Daemon crashed during Redis down. Logs tail: {result.stdout[-2000:]}"
        )

    # Redis restored - wait for recovery
    time.sleep(30)

    # Verify dashboard works again
    page.goto(testbed_url)
    page.wait_for_load_state("domcontentloaded", timeout=15000)
    take_screenshot("03_after_recover")

    # Verify state is queryable again
    state = get_state()
    assert state is not None, "State unreachable after Redis recovery"
    assert "state" in state, f"State malformed: {state}"
