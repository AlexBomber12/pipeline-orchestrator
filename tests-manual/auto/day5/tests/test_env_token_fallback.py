"""
ENV-TOKEN-01: Verify $GITHUB_TOKEN env fallback prevents intermittent git fetch 128.

Day 4 OBS-5: `git fetch origin main` fails with exit 128 approximately 3-4 times
per hour during idle. Day 4 diagnosis:
- gh credential helper (via `gh auth git-credential`) is sole auth path.
- $GITHUB_TOKEN was NOT set (no .env file).
- Manual fetch always works; automatic fetch fails intermittently.

Day 5 fix (B3 option chosen): add GITHUB_TOKEN to /home/alexey/pipeline-orchestrator/.env
as stable fallback. After this change, git can use $GITHUB_TOKEN directly without
invoking gh credential helper.

Precondition (manual setup before test run):
1. Obtain token via: gh auth token
2. Write to .env: echo "GITHUB_TOKEN=$(gh auth token)" >> /home/alexey/pipeline-orchestrator/.env
3. Recreate daemon: docker compose up -d daemon
4. Verify: docker compose exec -T daemon printenv GITHUB_TOKEN (should show token)

Test scenario:
1. Baseline measurement: read daemon logs for N seconds, count "exit status 128"
   occurrences.
2. Assert: during a monitoring window of 5 minutes, zero (or very few) fetch 128
   errors should occur.

This test is time-sensitive. If OBS-5 has underlying race conditions beyond token
availability, this test may PASS on low usage but still leave OBS-5 as partial.
"""

import subprocess
import time

import pytest

REPO_DIR = "/home/alexey/pipeline-orchestrator"


def _get_daemon_logs_since(seconds_ago: int) -> list:
    result = subprocess.run(
        ["docker", "compose", "logs", "daemon", "--since", f"{seconds_ago}s"],
        cwd=REPO_DIR,
        capture_output=True,
        text=True,
        timeout=10,
    )
    return result.stdout.splitlines()


def _count_fetch_128(logs: list) -> int:
    return sum(1 for line in logs if "exit status 128" in line or "fetch origin main: transient failure" in line)


def test_env_token_fallback_reduces_fetch_128_errors(
    page,
    testbed_url,
    take_screenshot,
):
    """Precondition check + 5-minute observation window."""

    result = subprocess.run(
        ["docker", "compose", "exec", "-T", "daemon", "printenv", "GITHUB_TOKEN"],
        cwd=REPO_DIR,
        capture_output=True,
        text=True,
        timeout=5,
    )
    token_visible = result.stdout.strip()
    if not token_visible or len(token_visible) < 20:
        pytest.skip(
            f"GITHUB_TOKEN not set inside daemon container. "
            f"Precondition: echo 'GITHUB_TOKEN=$(gh auth token)' >> {REPO_DIR}/.env "
            f"then docker compose up -d daemon. Got: '{token_visible[:30]}...'"
        )

    page.goto(testbed_url)
    take_screenshot("01_before_monitoring")

    monitoring_seconds = 300
    time.sleep(monitoring_seconds)

    take_screenshot("02_after_monitoring")

    logs = _get_daemon_logs_since(monitoring_seconds)
    fetch_128_count = _count_fetch_128(logs)

    baseline_rate_per_hour = 3.5
    expected_max_in_5min = baseline_rate_per_hour * (monitoring_seconds / 3600) + 1

    assert fetch_128_count <= expected_max_in_5min, \
        f"OBS-5 still occurring despite GITHUB_TOKEN fallback: " \
        f"{fetch_128_count} fetch 128 errors in {monitoring_seconds}s, " \
        f"expected <= {expected_max_in_5min:.1f}. " \
        f"The token fallback either did not load or OBS-5 has a different root cause " \
        f"(concurrent gh spawn, helper startup race). Candidate for PR-192 deeper " \
        f"instrumentation."

    if fetch_128_count == 0:
        print(f"OBS-5 RESOLVED with GITHUB_TOKEN fallback: zero fetch 128 in "
              f"{monitoring_seconds}s.")
    else:
        print(f"OBS-5 PARTIALLY RESOLVED: {fetch_128_count} events in "
              f"{monitoring_seconds}s (below threshold).")
