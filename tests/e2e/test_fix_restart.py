"""Restart the disposable daemon at real FIX Git/process boundaries.

The shim proves orchestration and publication, not paid-provider access. Killing
our test container supplies fixture cleanup, never a recovered supervisor handle.
"""

from __future__ import annotations

import json
import subprocess
import time
from datetime import datetime, timezone

import pytest
from src.github.checks import _classify_ci_retrieval, _retrieve_ci_status_evidence
from src.github.reviewer_policy import reviewer_policy_from_config
from src.github.reviews import clear_review_status_cache, get_pr_review_status
from src.models import CIStatus, ReviewStatus

from tests.e2e.conftest import EVIDENCE_DIR, _load_shared_config, _stop_daemon_and_wait_paused, _tested_sha
from tests.e2e.lib.coder_shim import SHIM_SCENARIO_PATH, coder_shim
from tests.e2e.lib.review_api import MergeOutcomeApi, ReviewApi, task_released_and_done
from tests.e2e.test_failure_continuity import _restart, _until
from tests.e2e.test_fix_feedback import _runs
from tests.e2e.test_sigkill_recovery import DAEMON_CONTAINER, _daemon_exit_code, _is_daemon_running

CALLS = SHIM_SCENARIO_PATH.with_name(SHIM_SCENARIO_PATH.name + ".restart-calls")
PUBLISHED = SHIM_SCENARIO_PATH.with_name(SHIM_SCENARIO_PATH.name + ".fix-published")


def _calls():
    return CALLS.read_text().splitlines() if CALLS.exists() else []


def _recovered_watch(state, task, branch, number, head, restarted_after):
    pr = state.get("current_pr") or {}
    return (
        state.get("state") == "WATCH"
        and (state.get("current_task") or {}).get("pr_id") == task
        and pr.get("branch") == branch and pr.get("number") == number
        and pr.get("head_sha") == head and bool(pr.get("publication_at"))
        and pr.get("publication_head_sha") == head
        and datetime.fromisoformat(state["last_updated"]) > restarted_after
    )


def _status(repo, head, state):
    subprocess.run(
        ["gh", "api", "-X", "POST", f"repos/{repo}/statuses/{head}",
         "-f", f"state={state}", "-f", "context=e2e/watch-merge-gate"],
        check=True, capture_output=True, text=True, timeout=30,
    )


@pytest.mark.parametrize("interrupted", [True, False], ids=["unknown-owner-held", "completed-fix-merged"])
def test_restart_preserves_fix_work(
    interrupted, testbed_slug, get_state, upload_zip, make_task_zip, post_review, reset_testbed,
):
    repo = testbed_slug.replace("__", "/", 1)
    config = _load_shared_config()
    configured = next(item for item in config.repositories if repo in item.url)
    outcome = MergeOutcomeApi(repo, configured.branch)
    reviewer = ReviewApi.from_environment()
    policy = reviewer_policy_from_config(config)
    identity = time.time_ns()
    task, branch = f"PR-{identity}", f"pr-{identity}-e2e-fix-restart"
    call = f"{task}\t{branch}"
    assert not CALLS.exists() and not PUBLISHED.exists(), "restart evidence has another owner"
    evidence = {"tested_sha": _tested_sha(), "task": task, "branch": branch, "interrupted": interrupted}

    def state_matching(predicate):
        def observe():
            state = get_state() or {}
            evidence["last_observed"] = {
                key: state.get(key) for key in (
                    "state", "last_updated", "current_task", "current_pr",
                    "active_invocation", "publication_attempt", "publication_watermark",
                    "publication_migration", "review_request_head", "review_request_pr",
                    "review_request_task", "error_message",
                )
            }
            return state if predicate(state) else None
        return _until(observe, seconds=300)

    def bound(state):
        return ((state.get("current_task") or {}).get("pr_id") == task
                and (state.get("current_pr") or {}).get("branch") == branch)

    def ready(state):
        pr = state.get("current_pr") or {}
        return (bound(state) and state.get("state") == "WATCH" and pr.get("publication_at")
                and state.get("review_request_head") == pr.get("head_sha")
                and state.get("review_request_pr") == pr.get("number")
                and state.get("review_request_task") == task)

    def review_status(head):
        clear_review_status_cache()
        return get_pr_review_status(repo, number, author, head, policy=policy)

    with coder_shim("success_pending_ci"):
        try:
            assert upload_zip(make_task_zip(identity, "e2e-fix-restart")) in (200, 201)
            first = state_matching(ready)
            number, old_head = first["current_pr"]["number"], first["current_pr"]["head_sha"]
            author = outcome.assert_pull(number, branch, old_head)["user"]["login"]
            old_marker = outcome.published_marker(number, branch, old_head, task)
            original_runs = _runs(testbed_slug, task)
            assert len(original_runs) == 1, original_runs
            original = original_runs[0]
            assert original["ended_at"] and original["exit_reason"] == "coding_complete", original
            assert _calls() == [call]
            evidence.update(number=number, old_head=old_head, original_run=original)

            # Old approval and green CI must never authorize the new FIX HEAD.
            reviewer.approve_head(number, old_head, first["current_pr"]["publication_at"])
            assert review_status(old_head) == ReviewStatus.APPROVED
            SHIM_SCENARIO_PATH.write_text(
                "fix_published_wait\n" if interrupted else "fix_existing_pr_pending_ci\n"
            )
            time.sleep(2)
            assert post_review(number)["commit_id"] == old_head
            if interrupted:
                _until(lambda: PUBLISHED.exists())
                handshake = PUBLISHED.read_text().strip().split("\t")
                assert handshake[:2] == [task, branch] and len(handshake) == 3, handshake
                head = handshake[2]
                before = state_matching(lambda s: bound(s) and s.get("state") == "FIX"
                                        and (s.get("active_invocation") or {}).get("phase") == "fix")
                attempt = before["publication_attempt"]
                assert attempt["pending"] and not attempt["cleanup_confirmed"], attempt
                assert attempt["exit_classification"] is None, attempt
            else:
                before = state_matching(lambda s: ready(s) and s["current_pr"]["head_sha"] != old_head)
                head = before["current_pr"]["head_sha"]

            marker = outcome.fixed_marker(number, branch, head, task, old_head, old_marker)
            assert [p["sha"] for p in outcome._get(f"commits/{head}")["parents"]] == [old_head]
            inv = before["active_invocation"]
            assert inv["phase"] == "fix" and inv["fix_iteration"] == 1, inv
            before_runs = _runs(testbed_slug, task)
            assert len(before_runs) == 2, before_runs
            fixed_run = next(row for row in before_runs if row["run_id"] == inv["run_id"])
            assert next(row for row in before_runs if row["run_id"] == original["run_id"]) == original
            if interrupted:
                assert fixed_run["ended_at"] is None, fixed_run
            assert fixed_run["attempt_index"] == original["attempt_index"] + 1 == inv["attempt_index"]
            assert [i["phase"] for row in before_runs for i in row["invocations"]].count("fix") == 1
            assert _calls() == [call, call]
            evidence.update(head=head, invocation=inv, before_runs=before_runs,
                            publication_attempt=before.get("publication_attempt"))

            restarted_after = datetime.now(timezone.utc)
            if interrupted:
                # Kill only the disposable fixture-owned container. Container
                # teardown kills its shim too; that is not daemon cleanup proof.
                try:
                    subprocess.run(
                        ["docker", "kill", "--signal=KILL", DAEMON_CONTAINER], check=True, timeout=10,
                    )
                    assert _is_daemon_running() == "false"
                    assert _daemon_exit_code() == 137
                finally:
                    _restart()
            else:
                _restart()

            def fresh(state):
                return bound(state) and datetime.fromisoformat(state["last_updated"]) > restarted_after

            if interrupted:
                recovered = state_matching(lambda s: fresh(s) and s.get("state") == "ERROR")
                assert recovered["current_pr"]["number"] == number
                assert recovered["error_message"] == "Publication ownership unresolved; retaining coder attempt"
                retained = recovered["publication_attempt"]
                for key in ("task_id", "branch", "run_id", "attempt_index", "pr_number"):
                    assert retained[key] == attempt[key], (key, retained, attempt)
                assert retained["pending"] and not retained["cleanup_confirmed"], retained
                assert retained["exit_classification"] is None
                assert recovered["active_invocation"] == inv
                # Even positive current-HEAD evidence cannot prove process
                # ownership or classify an interrupted coder as successful.
                _status(repo, head, "success")
                anchors = outcome._get(f"issues/{number}/comments")
                anchor = next(row for row in reversed(anchors) if row.get("body") == "@codex review"
                              and row["user"]["login"] == author)
                reviewer.approve_head(number, head, anchor["created_at"])
                for _ in range(12):
                    held = get_state()
                    assert held and bound(held) and held["state"] == "ERROR", held
                    assert held["active_invocation"] == inv
                    assert held["publication_attempt"]["cleanup_confirmed"] is False
                    assert _calls() == [call, call]
                    time.sleep(1)
                assert _runs(testbed_slug, task) == before_runs
                pull = outcome.assert_pull(number, branch, head)
                assert pull["state"] == "open" and not pull["merged"], pull
                evidence["outcome"] = "unknown owner held; no duplicate execution or false success"
            else:
                # Recovery rebinds the published work; review-request scheduling
                # fields are rebuilt separately from that ownership contract.
                recovered = state_matching(
                    lambda s: _recovered_watch(s, task, branch, number, head, restarted_after)
                )
                assert recovered["current_pr"]["number"] == number
                assert recovered["active_invocation"] == inv
                assert review_status(head) != ReviewStatus.APPROVED
                # Green CI on the old commit leaves the new one pending.
                _status(repo, old_head, "success")
                assert _classify_ci_retrieval(_retrieve_ci_status_evidence(repo, head, fresh=True)) == CIStatus.PENDING
                reviewer.approve_head(number, head, recovered["current_pr"]["publication_at"])
                assert review_status(head) == ReviewStatus.APPROVED
                for _ in range(6):
                    waiting = get_state()
                    assert waiting and bound(waiting) and waiting["state"] == "WATCH", waiting
                    assert _calls() == [call, call]
                    time.sleep(1)
                _status(repo, head, "success")
                state_matching(lambda s: task_released_and_done(s, task, branch))
                evidence["merged"] = outcome.assert_merged(number, branch, head, task, marker)
                runs = _runs(testbed_slug, task)
                assert len(runs) == 2, runs
                successor = next(row for row in runs if row["run_id"] == inv["run_id"])
                assert successor["exit_reason"] == "success_merged" and successor["outcome"] == "merged", runs
                assert successor["head_sha"] == head and successor["invocations"] == [inv]
                assert next(row for row in runs if row["run_id"] == original["run_id"]) == original
                evidence.update(outcome="same FIX merged with current-HEAD CI/review", runs=runs)

            assert outcome.published_marker(number, branch, head, task) == marker
            pulls = outcome._get(f"pulls?state=all&head={repo.split('/')[0]}:{branch}")
            assert [pull["number"] for pull in pulls] == [number], pulls
            assert _calls() == [call, call]
            evidence["recovered"] = {
                key: recovered.get(key) for key in ("state", "active_invocation", "publication_attempt")
            }
        finally:
            # The fixture owns the daemon and only these two evidence files.
            # Never signal a PID inferred from a missing supervisor handle.
            try:
                _stop_daemon_and_wait_paused(testbed_slug)
            finally:
                evidence["calls"] = _calls()
                EVIDENCE_DIR.mkdir(parents=True, exist_ok=True)
                (EVIDENCE_DIR / f"fix-restart-{task}.json").write_text(json.dumps(evidence, indent=2) + "\n")
            CALLS.unlink(missing_ok=True)
            PUBLISHED.unlink(missing_ok=True)
