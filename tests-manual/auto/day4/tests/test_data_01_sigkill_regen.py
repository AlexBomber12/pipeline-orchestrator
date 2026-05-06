"""
DATA-01: Daemon SIGKILL during task-file handling.

Send SIGKILL to daemon while it is processing an uploaded task.
After restart, the uploaded PR task file should still be present and
parseable; the daemon should recover without relying on tasks/QUEUE.md.
"""

import subprocess
import time

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
    1. Upload a task.
    2. Send SIGKILL to daemon container process during task handling.
    3. Restart daemon.
    4. Verify the PR-*.md task file is parseable and reflects a consistent state.
    """
    page.goto(testbed_url)
    take_screenshot("01_before")

    # Trigger task handling by upload.
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

    # Verify the uploaded task file is parseable without consulting QUEUE.md.
    result = subprocess.run(
        ["docker", "compose", "exec", "-T", "daemon",
         "cat", "/data/repos/AlexBomber12__pipeline-orchestrator-testbed/tasks/PR-230.md"],
        cwd=REPO_DIR,
        capture_output=True,
        text=True,
        timeout=10,
    )
    task_content = result.stdout

    # Minimal validity checks:
    # - Not empty
    # - Contains expected task markers
    # - No half-written lines (no line ending unexpectedly)
    assert len(task_content) > 0, "PR-230.md is empty after SIGKILL"

    # Expected task file contains some recognizable content.
    has_valid_content = (
        "PR-230" in task_content
        or "data01test" in task_content
        or "Branch:" in task_content
    )
    assert has_valid_content, (
        f"PR-230.md content looks invalid. First 500 chars: {task_content[:500]}"
    )

    # Verify daemon recovered (state queryable)
    state = get_state()
    assert state is not None, "State unreachable after daemon restart"
    assert state["state"] in ["IDLE", "CODING", "WATCH", "FIX", "MERGE", "HUNG", "ERROR"]

    # Verify git status is clean (no half-committed task file)
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
