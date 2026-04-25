import subprocess
import time
import urllib.error
import urllib.request

DAEMON_CONTAINER = "pipeline-orchestrator-daemon-test"
COMPOSE_FILE = "docker-compose.test.yml"


def _get_restart_count() -> int:
    result = subprocess.run(
        [
            "docker",
            "inspect",
            "--format",
            "{{.RestartCount}}",
            DAEMON_CONTAINER,
        ],
        capture_output=True,
        text=True,
        check=True,
        timeout=10,
    )
    return int(result.stdout.strip())


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


def test_redis_kill_during_idle_does_not_crash_daemon(
    dashboard_url,
    testbed_slug,
    wait_for_state,
    get_state,
    reset_testbed,
):
    try:
        wait_for_state(["IDLE"], timeout_sec=30)
    except TimeoutError as exc:
        raise AssertionError(
            f"test stack did not reach IDLE before test start: {exc}"
        ) from exc

    initial_restart_count = _get_restart_count()

    subprocess.run(
        ["docker", "compose", "-f", COMPOSE_FILE, "stop", "redis-test"],
        check=True,
        timeout=30,
    )

    try:
        time.sleep(5)

        try:
            with urllib.request.urlopen(
                f"{dashboard_url}/api/states", timeout=5
            ) as resp:
                # Acceptable: 200 (degraded but serving) or 5xx (cannot serve).
                # Body is intentionally not asserted; the only forbidden
                # outcome is a daemon restart, which step "alive" checks.
                assert resp.status < 600
        except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError, OSError):
            # Acceptable: connection refused / reset / timeout while Redis
            # is unreachable. The daemon being unable to serve is fine; the
            # daemon being killed by Docker is not.
            pass

        assert _is_daemon_running() == "true", (
            "daemon-test container is not running after Redis stop"
        )

        post_stop_restart_count = _get_restart_count()
        assert post_stop_restart_count == initial_restart_count, (
            f"daemon restart count changed from {initial_restart_count} to "
            f"{post_stop_restart_count}: daemon crashed and Docker restarted it"
        )
    finally:
        subprocess.run(
            ["docker", "compose", "-f", COMPOSE_FILE, "start", "redis-test"],
            check=True,
            timeout=30,
        )

    deadline = time.monotonic() + 30
    redis_healthy = False
    while time.monotonic() < deadline:
        health = subprocess.run(
            [
                "docker",
                "inspect",
                "--format",
                "{{.State.Health.Status}}",
                "pipeline-orchestrator-redis-test",
            ],
            capture_output=True,
            text=True,
            check=False,
            timeout=10,
        )
        if health.returncode == 0 and health.stdout.strip() == "healthy":
            redis_healthy = True
            break
        time.sleep(1)
    assert redis_healthy, "redis-test did not return to healthy within 30s"

    wait_for_state(["IDLE"], timeout_sec=60)

    state = get_state()
    assert state is not None, "no state entry returned for testbed after recovery"
    assert state["state"] == "IDLE", (
        f"final state was {state['state']!r}, expected IDLE"
    )
    assert state["slug"] == testbed_slug, (
        f"final slug was {state['slug']!r}, expected {testbed_slug!r}"
    )

    assert _is_daemon_running() == "true", (
        "daemon-test container is not running after Redis recovery"
    )

    final_restart_count = _get_restart_count()
    assert final_restart_count == initial_restart_count, (
        f"daemon restart count changed from {initial_restart_count} to "
        f"{final_restart_count} during the test"
    )
