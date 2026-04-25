import subprocess

from tests.e2e.lib.coder_shim import verify_shim_installed


def test_shim_is_mounted_and_callable():
    verify_shim_installed()

    result = subprocess.run(
        [
            "docker",
            "compose",
            "-f",
            "docker-compose.test.yml",
            "exec",
            "-T",
            "daemon-test",
            "/usr/local/bin/claude",
        ],
        capture_output=True,
        text=True,
        timeout=5,
    )
    assert result.returncode == 0 or "no DOING task" in result.stderr, (
        f"shim invocation failed: rc={result.returncode}, "
        f"stdout={result.stdout!r}, stderr={result.stderr!r}"
    )
