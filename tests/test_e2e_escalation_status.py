"""Exercise status propagation without GitHub credentials or a running stack."""

import json
import subprocess
from types import SimpleNamespace

import pytest

from tests.e2e import test_fix_coder_escalate as escalation


@pytest.mark.parametrize("outcome", ["converged", "missing", "untrusted", "api-error"])
def test_posted_failure_waits_for_exact_history_receipt(monkeypatch, outcome):
    receipt = {
        "id": 42, "state": "failure", "context": escalation.WATCH_GATE_CONTEXT,
        "creator": {"login": "trusted-ci"},
    }
    stale = {**receipt, "id": 41, "state": "pending"}
    history_reads = []
    posts = []
    clock = [0.0]
    monkeypatch.setattr(escalation.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(escalation.time, "sleep", lambda delay: clock.__setitem__(0, clock[0] + delay))
    config = SimpleNamespace(repositories=[SimpleNamespace(
        ci_fix_trusted_statuses={escalation.WATCH_GATE_CONTEXT: ["user:trusted-ci"]},
    )])
    monkeypatch.setattr(escalation, "_load_shared_config", lambda: config)

    def run(args, **kwargs):
        assert kwargs["timeout"] == 30
        if "POST" in args:
            posts.append(args)
            response = receipt
        elif args[-1].endswith("/status"):
            response = {"statuses": [receipt]}
        else:
            assert args[-1].endswith("/test-head/statuses?per_page=100")
            history_reads.append(args)
            if outcome == "api-error":
                raise subprocess.CalledProcessError(1, args)
            response = [stale]
            if len(history_reads) >= 3 and outcome != "missing":
                current = dict(receipt)
                if outcome == "untrusted":
                    current["creator"] = {"login": "untrusted-ci"}
                response.insert(0, current)
        return subprocess.CompletedProcess(args, 0, json.dumps(response))

    monkeypatch.setattr(escalation.subprocess, "run", run)
    if outcome == "converged":
        escalation._post_failed_status("test-head")
        assert len(history_reads) == 3
        assert clock[0] == 1.0
    elif outcome == "api-error":
        with pytest.raises(subprocess.CalledProcessError):
            escalation._post_failed_status("test-head")
        assert len(history_reads) == 1
    else:
        message = "absent from status history" if outcome == "missing" else "does not match configured"
        with pytest.raises(AssertionError, match=message):
            escalation._post_failed_status("test-head")
        if outcome == "missing":
            assert clock[0] == escalation.STATUS_PROPAGATION_TIMEOUT_SEC
        else:
            assert len(history_reads) == 3
    assert len(posts) == 1


@pytest.mark.parametrize("converges", [True, False])
def test_failed_gate_waits_for_its_own_status_snapshot(monkeypatch, converges):
    clock = [0.0]
    reads = []
    monkeypatch.setattr(escalation.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(escalation.time, "sleep", lambda delay: clock.__setitem__(0, clock[0] + delay))

    def gate_state(head_sha):
        assert head_sha == "test-head"
        reads.append(head_sha)
        return "failure" if converges and len(reads) == 3 else "pending"

    monkeypatch.setattr(escalation, "_watch_gate_state", gate_state)
    if converges:
        escalation._wait_for_failed_watch_gate("test-head")
        assert len(reads) == 3
        assert clock[0] == 1.0
    else:
        with pytest.raises(AssertionError, match="remained 'pending'"):
            escalation._wait_for_failed_watch_gate("test-head")
        assert clock[0] == escalation.STATUS_PROPAGATION_TIMEOUT_SEC
