"""Replay cancellation discovery delays while retaining terminal outcome gates."""

from contextlib import nullcontext
from types import SimpleNamespace

import pytest

from tests.e2e import test_cancellation_cause_crash as scenario


@pytest.mark.parametrize("outcome", [
    "released", "missing", "wrong_task", "wrong_category", "wrong_subsource",
    "missing_terminal", "missing_dependent", "missing_retained_cause",
])
def test_cancellation_after_inflight_empty_scan(monkeypatch, outcome):
    elapsed = 0
    released = False

    def sleep(seconds):
        nonlocal elapsed
        elapsed += seconds

    def get_state():
        nonlocal released
        released = elapsed >= 136
        return {
            "state": "IDLE", "current_task": None,
            "history": [{"event": (
                "[RECOVERY] Failed attempt retained; independent work released."
                if released else "[INFRA] No tasks available."
            )}],
        }

    def cancellations(*_args):
        if elapsed < 136 or outcome == "missing":
            return []
        if released and outcome == "missing_retained_cause":
            return []
        return [{
            "task_id": "PR-999" if outcome == "wrong_task" else "PR-1",
            "category": "OTHER" if outcome == "wrong_category" else "ERROR",
            "payload": {"subsource": (
                "other" if outcome == "wrong_subsource" else
                "diagnose_exhausted" if released else "crash"
            )},
            "dependents_count": 1,
        }]

    class Response:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return None

        def read(self):
            root = "" if outcome == "missing_terminal" else 'id="task-content-error-PR-1"'
            dependent = "" if outcome == "missing_dependent" else 'id="task-content-todo-PR-2"'
            return f"{root} {dependent}".encode()

    monkeypatch.setattr(scenario, "time", SimpleNamespace(
        time=lambda: 1, monotonic=lambda: elapsed, sleep=sleep,
    ))
    monkeypatch.setattr(scenario, "coder_shim", lambda mode: nullcontext())
    monkeypatch.setattr(scenario, "_fetch_cancellations", cancellations)
    monkeypatch.setattr(scenario.urllib.request, "urlopen", lambda *a, **kw: Response())

    def exercise():
        scenario.test_coder_crash_records_cancellation_cause_and_propagates(
            "unused", "testbed", lambda *a, **kw: None, get_state,
            lambda path: 201, lambda *a, **kw: "unused.zip", None,
        )

    if outcome == "released":
        exercise()
        assert elapsed == 136
    else:
        with pytest.raises(AssertionError):
            exercise()
        if outcome in {"missing", "wrong_task"}:
            assert elapsed == scenario.CANCELLATION_DEADLINE_SEC
