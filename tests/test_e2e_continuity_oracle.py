"""Replay readiness published before blocking restart/resume calls return."""

import json
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from tests.e2e import test_failure_continuity as scenario
from tests.e2e.lib.ci_outcome import LifecycleTimeout


@pytest.mark.parametrize("scan_phase", ["before_restart", "restart", "resume"])
def test_restart_readiness_uses_restart_boundary(monkeypatch, scan_phase):
    boundary = datetime(2026, 11, 8, tzinfo=timezone.utc)
    elapsed = 0
    published = boundary - timedelta(seconds=1)
    operations = []

    def advance(seconds):
        nonlocal elapsed
        elapsed += seconds

    def operation(phase):
        nonlocal published
        operations.append(phase)
        advance(1)
        if scan_phase == phase:
            published = boundary + timedelta(seconds=elapsed)
        # Model the response returning after the publication, not before it.
        advance(1)

    clock = SimpleNamespace(now=lambda zone: boundary + timedelta(seconds=elapsed))
    monkeypatch.setattr(scenario, "datetime", clock)
    monkeypatch.setattr(scenario, "_restart", lambda: operation("restart"))
    monkeypatch.setattr(scenario, "_resume_daemon", lambda slug: operation("resume"))

    def snapshot(*args, **kwargs):
        response = MagicMock()
        response.__enter__.return_value.read.return_value = json.dumps({"states": [{
            "name": "slug", "state": "IDLE", "user_paused": False,
            "current_task": None, "current_pr": None,
            "last_updated": (boundary + timedelta(seconds=elapsed)).isoformat(),
            "history": [{"event": "[INFRA] No tasks available.", "state": "IDLE",
                         "time": published.isoformat()}],
        }]}).encode()
        return response

    # Use the actual readiness oracle: a new heartbeat alone cannot make a
    # pre-restart scan fresh, but a scan during either blocking call is valid.
    globals_ = scenario._wait_for_fresh_idle.__globals__
    monkeypatch.setitem(globals_, "time", SimpleNamespace(
        monotonic=lambda: elapsed, sleep=advance,
    ))
    monkeypatch.setattr(globals_["urllib"].request, "urlopen", snapshot)
    if scan_phase == "before_restart":
        with pytest.raises(LifecycleTimeout):
            scenario._restart_and_wait_for_idle("slug")
        assert elapsed == 184
    else:
        scenario._restart_and_wait_for_idle("slug")
        assert elapsed == 4
    assert operations == ["restart", "resume"]
