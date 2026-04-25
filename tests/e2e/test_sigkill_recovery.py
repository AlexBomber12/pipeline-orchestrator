import subprocess
import time

from tests.e2e.lib.coder_shim import coder_shim

DAEMON_CONTAINER = "pipeline-orchestrator-daemon-test"
COMPOSE_FILE = "docker-compose.test.yml"


def _is_daemon_running() -> str:
    result = subprocess.run(
        [
            "docker",
            "inspect",
            "--format",
            "{{.State.Running}}",
            DAEMON_CONTAINER,
        ],
        capture_output=True,
        text=True,
        check=True,
        timeout=10,
    )
    return result.stdout.strip()


def _daemon_exit_code() -> int:
    result = subprocess.run(
        [
            "docker",
            "inspect",
            "--format",
            "{{.State.ExitCode}}",
            DAEMON_CONTAINER,
        ],
        capture_output=True,
        text=True,
        check=True,
        timeout=10,
    )
    return int(result.stdout.strip())


def test_sigkill_during_coding_recovers_correctly(
    dashboard_url,
    testbed_slug,
    wait_for_state,
    get_state,
    upload_zip,
    make_task_zip,
    reset_testbed,
):
    try:
        wait_for_state(["IDLE"], timeout_sec=30)
    except TimeoutError as exc:
        raise AssertionError(
            f"test stack did not reach IDLE before test start: {exc}"
        ) from exc

    pr_id_int = int(time.time())
    expected_pr_id = f"PR-{pr_id_int}"

    with coder_shim("slow"):
        zip_path = make_task_zip(
            pr_id_int, "e2e-sigkill-recovery", coder="any", priority=2
        )
        status = upload_zip(zip_path)
        assert status in (200, 201), f"upload failed with status {status}"

        wait_for_state(["CODING"], timeout_sec=30)

        subprocess.run(
            ["docker", "kill", "--signal=KILL", DAEMON_CONTAINER],
            check=True,
            timeout=10,
        )

        try:
            running = _is_daemon_running()
            assert running == "false", (
                f"daemon container still reports Running={running!r} after SIGKILL"
            )
            exit_code = _daemon_exit_code()
            assert exit_code == 137, (
                f"daemon container exit code was {exit_code}, "
                f"expected 137 (128 + SIGKILL)"
            )
        finally:
            subprocess.run(
                ["docker", "compose", "-f", COMPOSE_FILE, "up", "-d", "daemon-test"],
                check=True,
                timeout=60,
            )

        deadline = time.monotonic() + 30
        running_again = "false"
        while time.monotonic() < deadline:
            running_again = _is_daemon_running()
            if running_again == "true":
                break
            time.sleep(1)
        assert running_again == "true", (
            f"daemon-test did not return to Running=true within 30s "
            f"after restart; last value was {running_again!r}"
        )

        recovered = wait_for_state(
            ["IDLE", "CODING", "WATCH"], timeout_sec=90
        )

        recovered_state = recovered.get("state")
        assert recovered_state not in ("ERROR", "HUNG"), (
            f"daemon recovered into stuck state {recovered_state!r}"
        )

        current_task = recovered.get("current_task")
        if recovered_state in ("CODING", "WATCH"):
            assert current_task is not None, (
                f"recovered into {recovered_state!r} with no current_task"
            )
            assert current_task.get("pr_id") == expected_pr_id, (
                f"recovered {recovered_state!r} current_task={current_task!r}, "
                f"expected pr_id={expected_pr_id!r}"
            )
        else:
            assert current_task is None, (
                f"recovered into IDLE but current_task={current_task!r} is not None"
            )

    final = get_state()
    assert final is not None, "no state entry returned for testbed after recovery"
    assert final["slug"] == testbed_slug, (
        f"final slug was {final['slug']!r}, expected {testbed_slug!r}"
    )
