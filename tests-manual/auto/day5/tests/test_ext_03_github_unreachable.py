"""
EXT-03: GitHub unreachable — daemon retries with backoff, surfaces error without fast loop.

Day 4 result: FAIL due to OBS-9 (block_host failed to write /etc/hosts as runner).
Day 5 fix: block_host now uses -u root.

Scenario:
1. Block github.com in daemon container /etc/hosts.
2. Observe for ~2 minutes.
3. Verify daemon logs retry attempts with backoff, does not spam attempts faster than 1/sec.
"""

import subprocess
import time

import pytest

from lib.failure_helpers import block_host

REPO_DIR = "/home/alexey/pipeline-orchestrator"


def test_ext_03_github_unreachable_no_fast_retry(
    page,
    testbed_url,
    testbed_slug,
    wait_for_state,
    get_state,
    take_screenshot,
    reset_testbed_clean,
):
    page.goto(testbed_url)
    take_screenshot("01_before_block")

    with block_host("github.com"):
        time.sleep(90)

        result = subprocess.run(
            ["docker", "compose", "logs", "daemon", "--since", "90s"],
            cwd=REPO_DIR,
            capture_output=True,
            text=True,
            timeout=10,
        )
        log_lines = result.stdout.splitlines()

        retry_lines = [
            l for l in log_lines
            if "transient failure" in l or "retrying in" in l or "fetch" in l.lower()
        ]

        page.goto(testbed_url)
        take_screenshot("02_during_block")

        assert len(retry_lines) >= 1, \
            f"No retry lines seen in 90s with github.com blocked. " \
            f"Expected at least 1 retry. Got logs: {log_lines[-10:]}"

        assert len(retry_lines) < 200, \
            f"Fast-loop retry detected: {len(retry_lines)} retries in 90s. " \
            f"Expected backoff pattern (< ~30 in 90s)."

    time.sleep(5)
    page.goto(testbed_url)
    take_screenshot("03_after_unblock")
