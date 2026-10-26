"""Hermetic replay of the SIGKILL test's recovery deadline and outcome gates."""

from contextlib import nullcontext
from types import SimpleNamespace

import pytest

from tests.e2e import test_sigkill_recovery as scenario


@pytest.mark.parametrize("outcome", ["recovered", "stale", "stuck", "missing_crash"])
def test_sigkill_recovery_after_discovery_retry(monkeypatch, outcome):
    elapsed = 0
    restarted = False

    def sleep(seconds):
        nonlocal elapsed
        elapsed += seconds

    def run(args, **kwargs):
        nonlocal restarted
        if args[:2] == ["docker", "compose"]:
            restarted = True

    def get_state():
        # Hosted CI recovered 136s after restart, following stale-cache
        # discovery and a daemon poll delay. Replay without Docker or GitHub.
        recovered = restarted and elapsed >= 136 and outcome != "stuck"
        return {
            "name": "testbed",
            "state": "IDLE" if recovered else "CODING",
            "current_task": None if recovered else {"pr_id": "PR-1"},
            "last_updated": (
                "2026-10-26T14:04:12+00:00"
                if recovered and outcome != "stale"
                else "2026-10-26T14:01:54+00:00"
            ),
            "history": [] if outcome == "missing_crash" else [{
                "event": "[INFRA] Task PR-1 crashed, marking ERROR. "
                "Manually re-upload to retry. (base_branch=main)",
            }],
        }

    monkeypatch.setattr(scenario, "time", SimpleNamespace(
        time=lambda: 1, monotonic=lambda: elapsed, sleep=sleep,
    ))
    monkeypatch.setattr(scenario, "subprocess", SimpleNamespace(run=run))
    monkeypatch.setattr(scenario, "coder_shim", lambda mode: nullcontext())
    monkeypatch.setattr(scenario, "_is_daemon_running", lambda: "true" if restarted else "false")
    monkeypatch.setattr(scenario, "_daemon_exit_code", lambda: 137)

    def exercise():
        scenario.test_sigkill_during_coding_recovers_correctly(
            "unused", "testbed", lambda *a, **kw: None, get_state,
            lambda path: 201, lambda *a, **kw: "unused.zip", None,
        )

    if outcome == "recovered":
        exercise()
        assert elapsed == 136
    else:
        message = "crash-mark log entry" if outcome == "missing_crash" else "did not quiesce"
        with pytest.raises(AssertionError, match=message):
            exercise()
        assert elapsed == (136 if outcome == "missing_crash" else scenario.RECOVERY_DEADLINE_SEC)
