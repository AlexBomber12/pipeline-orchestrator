"""Real testbed outcomes after a bound clean failure; dirty recovery stays held."""

from __future__ import annotations

import base64
import json
import subprocess
import time
from datetime import datetime, timezone
from uuid import uuid4

import pytest
import requests

from tests.e2e.conftest import (
    EVIDENCE_DIR,
    REPO_DIR,
    _resume_daemon,
    _stop_daemon_and_wait_paused,
    _tested_sha,
    _wait_for_fresh_idle,
)
from tests.e2e.lib.coder_shim import SHIM_SCENARIO_PATH, coder_shim
from tests.e2e.test_cancellation_cause_crash import _entry_for, _fetch_cancellations

TESTBED = "AlexBomber12/pipeline-orchestrator-testbed"
CALLS = SHIM_SCENARIO_PATH.with_name(SHIM_SCENARIO_PATH.name + ".calls")
RELEASE = "[RECOVERY] Failed attempt retained; independent work released."


def _until(predicate, *, seconds=180):
    deadline = time.monotonic() + seconds
    last = None
    while time.monotonic() < deadline:
        last = predicate()
        if last:
            return last
        time.sleep(1)
    raise AssertionError(f"predicate {predicate.__name__} did not succeed in {seconds}s; last={last!r}")


def _gh(path):
    result = subprocess.run(
        ["gh", "api", path], capture_output=True, text=True, check=True, timeout=30,
    )
    return json.loads(result.stdout)


def _calls():
    return CALLS.read_text().splitlines() if CALLS.exists() else []


def _queue(url, slug):
    response = requests.get(f"{url}/api/repo/{slug}/queue", timeout=10)
    response.raise_for_status()
    return {row["pr_id"]: row for row in response.json()["queue"]}


def _dirty_held_with_cleanup(state, task_id, branch):
    """Require bound cleanup and an observed refusal to discard dirty work."""
    attempt = state.get("publication_attempt") or {}
    task = state.get("current_task") or {}
    events = [row.get("event", "") for row in state.get("history") or []]
    return (
        state.get("state") == "ERROR"
        and task.get("pr_id") == task_id and task.get("branch") == branch
        and attempt.get("task_id") == task_id and attempt.get("branch") == branch
        and attempt.get("cleanup_confirmed") is True
        and attempt.get("exit_classification") == "failure"
        and any(
            "Unpublished local branch cleanup deferred: worktree is dirty" in event
            or "Terminal release wait" in event
            for event in events
        )
    )


def _assert_retained(url, slug, a, c, *, exhausted):
    cause = _entry_for(_fetch_cancellations(url, slug), a)
    assert cause and cause["category"] == "ERROR", cause
    assert cause["dependents_count"] == 1, cause
    if exhausted:
        assert cause["payload"]["subsource"] == "diagnose_exhausted", cause
    queue = _queue(url, slug)
    assert queue[a]["status"] == "ERROR", queue
    assert queue[c]["status"] == "TODO" and queue[c]["depends_on"] == [a], queue
    # The API's dependent count and the exact unsatisfied edge explicitly
    # identify C as blocked; the invocation journal proves it never executed.
    return cause


def _restart():
    subprocess.run(
        ["docker", "compose", "-f", "docker-compose.test.yml", "restart", "daemon-test"],
        cwd=REPO_DIR, check=True, timeout=60,
    )


@pytest.mark.parametrize("dirty", [False, True], ids=["clean-merged", "dirty-held"])
def test_failed_a_independent_b(
    dirty, dashboard_url, testbed_slug, get_state, upload_zip,
    make_task_zip_multi, reset_testbed,
):
    # All task/branch/marker identities are fresh, never preset DONE.
    base = int(time.time())
    a, b, c = [f"PR-{base + offset}" for offset in range(3)]
    nonce = uuid4().hex[:12]
    root = f"e2e-{nonce}-{'dirty' if dirty else 'clean'}-root"
    result = f"e2e-{nonce}-result"
    dependent = f"e2e-{nonce}-dependent"
    branches = [f"pr-{base}-{root}", f"pr-{base + 1}-{result}", f"pr-{base + 2}-{dependent}"]
    expected_a = f"{a}\t{branches[0]}"
    expected_b = f"{b}\t{branches[1]}"
    # Reset owns the disposable runner. Refuse to overwrite another journal.
    assert not CALLS.exists(), "unexpected continuity journal from another owner"
    evidence = {"tested_sha": _tested_sha(), "tasks": [a, b, c], "dirty": dirty}
    with coder_shim("failure_continuity"):
        try:
            _stop_daemon_and_wait_paused(testbed_slug)
            archive = make_task_zip_multi([
                (base, root, [], 1),
                (base + 1, result, [], 3),
                (base + 2, dependent, [a], 2),
            ])
            assert upload_zip(archive) in (200, 201)
            # Negative control: an admitted upload must not override manual
            # pause. Observe several complete 2s test-daemon poll cycles.
            for _ in range(8):
                state = get_state()
                assert state and state["user_paused"] is True, state
                assert _calls() == [], _calls()
                time.sleep(1)
            _resume_daemon(testbed_slug)

            def failed_outcome():
                cause = _entry_for(_fetch_cancellations(dashboard_url, testbed_slug), a)
                return cause if cause and cause.get("dependents_count") == 1 else None

            _until(failed_outcome)
            if dirty:
                def held_with_cleanup():
                    state = get_state() or {}
                    return state if _dirty_held_with_cleanup(state, a, branches[0]) else None

                held = _until(held_with_cleanup)
                # Cleanup is proved for this exited shim; dirty work is NOT
                # proved preserved. Restart must retain the hold and identity.
                evidence["cleanup_confirmed"] = held["publication_attempt"]["cleanup_confirmed"]
                evidence["held_attempt"] = {
                    key: held["publication_attempt"].get(key)
                    for key in ("task_id", "branch", "pending", "exit_classification", "cleanup_confirmed")
                }
                dirty_path = REPO_DIR / "tests/e2e/data/repos" / testbed_slug / "continuity-dirty.txt"
                assert dirty_path.read_text() == f"unpreserved work for {a}\n"
                assert _calls() == [expected_a], _calls()
                restarted_after = datetime.now(timezone.utc)
                _restart()

                def fresh_held():
                    state = get_state() or {}
                    updated = datetime.fromisoformat(state.get("last_updated") or "1970-01-01T00:00:00+00:00")
                    return state if (
                        updated > restarted_after and state.get("state") == "ERROR"
                        and (state.get("current_task") or {}).get("pr_id") == a
                    ) else None

                _until(fresh_held)
                for _ in range(12):
                    state = get_state()
                    assert state and state["state"] == "ERROR", state
                    assert (state.get("current_task") or {}).get("pr_id") == a, state
                    assert _calls() == [expected_a], _calls()
                    time.sleep(1)
                cause = _assert_retained(dashboard_url, testbed_slug, a, c, exhausted=False)
                assert _queue(dashboard_url, testbed_slug)[b]["status"] == "TODO"
                # Read only fixture-owned work; never clean or take over it.
                assert dirty_path.read_text() == f"unpreserved work for {a}\n"
                evidence["outcome"] = "dirty restart held; B and C never invoked"
            else:
                def matching_merged_pr():
                    prs = _gh(f"repos/{TESTBED}/pulls?state=closed&head=AlexBomber12:{branches[1]}")
                    matches = [pr for pr in prs if pr["head"]["ref"] == branches[1] and pr["merged_at"]]
                    assert len(matches) <= 1, matches
                    return matches[0] if matches else None

                merged = _until(matching_merged_pr, seconds=300)
                exact = _gh(f"repos/{TESTBED}/pulls/{merged['number']}")
                assert exact["merged"] is True and exact["merge_commit_sha"], exact
                assert exact["base"]["ref"] == "main" and exact["head"]["ref"] == branches[1], exact
                assert exact["head"]["repo"]["full_name"] == TESTBED, exact
                marker = f"shim marker for {b} at "
                for ref in (exact["head"]["sha"], exact["merge_commit_sha"], "main"):
                    content = _gh(f"repos/{TESTBED}/contents/tests/e2e-shim-marker.txt?ref={ref}")
                    assert marker in base64.b64decode(content["content"]).decode(), content["sha"]

                def done_and_released():
                    state = get_state() or {}
                    events = [row.get("event") for row in state.get("history") or []]
                    queue = _queue(dashboard_url, testbed_slug)
                    return state if (
                        state.get("state") == "IDLE" and state.get("current_task") is None
                        and RELEASE in events and queue.get(b, {}).get("status") == "DONE"
                    ) else None

                _until(done_and_released)
                cause = _assert_retained(dashboard_url, testbed_slug, a, c, exhausted=True)
                assert _calls() == [expected_a, expected_b], _calls()
                # No active process: the released A and completed B both passed
                # production cleanup. Restart only this fixture-owned daemon.
                _stop_daemon_and_wait_paused(testbed_slug)
                _restart()
                _resume_daemon(testbed_slug)
                _wait_for_fresh_idle(testbed_slug, after=datetime.now(timezone.utc))
                for _ in range(12):
                    assert _calls() == [expected_a, expected_b], _calls()
                    assert _queue(dashboard_url, testbed_slug)[b]["status"] == "DONE"
                    assert _assert_retained(dashboard_url, testbed_slug, a, c, exhausted=True) == cause
                    time.sleep(1)
                evidence.update(outcome="B merged and DONE; A ERROR and C blocked across restart",
                                pr=exact["number"], head_sha=exact["head"]["sha"],
                                merge_commit_sha=exact["merge_commit_sha"], base="main")
            evidence["cause"] = {"task_id": cause["task_id"], "category": cause["category"],
                                 "subsource": cause["payload"]["subsource"]}
            evidence["calls"] = _calls()
        finally:
            # Context exits before reset; normal reset owns dirty checkout
            # disposal. No git clean, signal, Redis injection or preset status.
            _stop_daemon_and_wait_paused(testbed_slug)
            CALLS.unlink(missing_ok=True)
            EVIDENCE_DIR.mkdir(parents=True, exist_ok=True)
            (EVIDENCE_DIR / f"continuity-{nonce}.json").write_text(json.dumps(evidence, indent=2) + "\n")
