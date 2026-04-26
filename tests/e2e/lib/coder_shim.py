"""Helpers for the e2e mock coder shim.

The shim replaces the real claude/codex CLIs inside the test stack and reads
its scenario from a file written by ``coder_shim`` (the Python context
manager below). ``verify_shim_installed`` confirms the docker-compose mounts
are in place so misconfiguration fails fast and visibly.
"""

from __future__ import annotations

import contextlib
import subprocess
from pathlib import Path

REPO_DIR = Path(__file__).resolve().parents[3]
SHIM_SCENARIO_PATH = REPO_DIR / "tests/e2e/data/shim-scenario"

VALID_SCENARIOS = {
    "success",
    "no_pr",
    "exit_nonzero",
    "hang",
    "malformed_pr",
    "slow",
    "no_push_fail_ci",
}


@contextlib.contextmanager
def coder_shim(scenario: str):
    assert scenario in VALID_SCENARIOS, (
        f"unknown shim scenario {scenario!r}; expected one of {sorted(VALID_SCENARIOS)}"
    )
    SHIM_SCENARIO_PATH.parent.mkdir(parents=True, exist_ok=True)
    previous = (
        SHIM_SCENARIO_PATH.read_text() if SHIM_SCENARIO_PATH.exists() else None
    )
    SHIM_SCENARIO_PATH.write_text(scenario + "\n")
    try:
        yield
    finally:
        if previous is None:
            SHIM_SCENARIO_PATH.write_text("success\n")
        else:
            SHIM_SCENARIO_PATH.write_text(previous)


def verify_shim_installed(compose_file: str = "docker-compose.test.yml") -> None:
    for binary in ("claude", "codex"):
        result = subprocess.run(
            [
                "docker",
                "compose",
                "-f",
                compose_file,
                "exec",
                "-T",
                "daemon-test",
                "which",
                binary,
            ],
            capture_output=True,
            text=True,
            check=False,
        )
        actual = result.stdout.strip()
        expected = f"/usr/local/bin/{binary}"
        if result.returncode != 0 or actual != expected:
            raise RuntimeError(
                f"shim not mounted: `which {binary}` returned "
                f"{actual!r} (rc={result.returncode}, stderr={result.stderr!r}); "
                f"expected {expected!r}. "
                f"Check the volumes section of {compose_file} for the "
                f"coder_shim.sh -> /usr/local/bin/{binary} bind mount."
            )
