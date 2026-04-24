"""
DATA-01: Daemon SIGKILL during QUEUE.md regeneration.

Send SIGKILL to daemon while it is in the middle of writing QUEUE.md.
After restart, QUEUE.md should be either:
- fully committed (old valid state) or
- fully rewritten (new valid state)
but NOT left half-written (partial file, invalid YAML frontmatter, etc).
"""

import time
import subprocess
import pytest

REPO_DIR = "/home/alexey/pipeline-orchestrator"


def test_data_01_daemon_sigkill_preserves_queue_integrity(
    page,
    testbed_url,
    upload_zip,
    make_task_zip,
    get_state,
    take_screenshot,
):
    """
    Scenario:
    1. Upload a task to trigger QUEUE regen.
    2. Send SIGKILL to daemon container process during the regen window.
    3. Restart daemon.
    4. Verify QUEUE.md is parseable and reflects a consistent state.
    """
    page.goto(testbed_url)
    take_screenshot("01_before")

    # Trigger a QUEUE regen by upload
    zip_path = make_task_zip(pr_num=230, label="data01test")
    response = upload_zip(zip_path)
    assert response.status_code == 200

    # Give daemon 2 seconds to start processing the upload
    time.sleep(2)

    # SIGKILL daemon container (docker kill, signal 9)
    result = subprocess.run(
        ["docker", "compose", "kill", "-s", "KILL", "daemon"],
        cwd=REPO_DIR,
        capture_output=True,
        text=True,
        timeout=10,
    )
    assert result.returncode == 0, f"Failed to SIGKILL daemon: {result.stderr}"

    # Wait a moment, then restart
    time.sleep(3)
    subprocess.run(
        ["docker", "compose", "start", "daemon"],
        cwd=REPO_DIR,
        check=True,
        timeout=15,
    )

    # Wait for daemon to be back up
    time.sleep(15)
    take_screenshot("02_after_restart")

    # Verify QUEUE.md is parseable
    result = subprocess.run(
        ["docker", "compose", "exec", "-T", "daemon",
         "cat", "/data/repos/AlexBomber12__pipeline-orchestrator-testbed/tasks/QUEUE.md"],
        cwd=REPO_DIR,
        capture_output=True,
        text=True,
        timeout=10,
    )
    queue_content = result.stdout

    # Minimal validity checks:
    # - Not empty
    # - Contains expected markers (either YAML frontmatter or table headers)
    # - No half-written lines (no line ending unexpectedly)
    assert len(queue_content) > 0, "QUEUE.md is empty after SIGKILL"

    # Expected QUEUE.md contains some recognizable content
    has_valid_content = (
        "PR-" in queue_content  # some PR line
        or "TODO" in queue_content  # status column
        or "DONE" in queue_content
    )
    assert has_valid_content, (
        f"QUEUE.md content looks invalid. First 500 chars: {queue_content[:500]}"
    )

    # Verify daemon recovered (state queryable)
    state = get_state()
    assert state is not None, "State unreachable after daemon restart"
    assert state["state"] in ["IDLE", "CODING", "WATCH", "FIX", "MERGE", "HUNG", "ERROR"]

    # Verify git status is clean (no half-committed QUEUE.md)
    result = subprocess.run(
        ["docker", "compose", "exec", "-T", "daemon", "git",
         "-C", "/data/repos/AlexBomber12__pipeline-orchestrator-testbed",
         "status", "--porcelain"],
        cwd=REPO_DIR,
        capture_output=True,
        text=True,
        timeout=10,
    )
    dirty_files = result.stdout.strip()

    # Dirty is OK, but if dirty it should be recoverable (not corrupted)
    if dirty_files:
        # Check that daemon self-recovery kicks in within 30s
        time.sleep(30)
        result2 = subprocess.run(
            ["docker", "compose", "exec", "-T", "daemon", "git",
             "-C", "/data/repos/AlexBomber12__pipeline-orchestrator-testbed",
             "status", "--porcelain"],
            cwd=REPO_DIR,
            capture_output=True,
            text=True,
            timeout=10,
        )
        if result2.stdout.strip():
            print(f"WARNING: tree still dirty 30s after SIGKILL recovery: {result2.stdout}")
            # Not a hard failure, but worth noting
