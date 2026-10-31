"""A real review-driven FIX publishes once, obtains fresh evidence and merges.

Uses the independent reviewer App and disposable GHA stack, never a paid coder.
The pending CI status holds each HEAD while review evidence is inspected.
"""

import json
import subprocess
import time
from datetime import datetime
from pathlib import Path
from uuid import NAMESPACE_URL, uuid5

from src.github.checks import _classify_ci_retrieval, _retrieve_ci_status_evidence
from src.github.reviewer_policy import reviewer_policy_from_config
from src.github.reviews import clear_review_status_cache, get_pr_review_status, read_review_feedback
from src.models import CIStatus, ReviewStatus

from tests.e2e.conftest import _load_shared_config
from tests.e2e.lib.coder_shim import SHIM_SCENARIO_PATH, coder_shim
from tests.e2e.lib.review_api import MergeOutcomeApi, ReviewApi, task_released_and_done


def _wait(get_state, predicate, timeout=180):
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        last = get_state()
        if last and predicate(last):
            return last
        assert not last or last.get("state") != "ERROR", last
        time.sleep(1)
    raise AssertionError(f"FIX lifecycle predicate timed out; last state={last!r}")


def _runs(slug, task):
    """Read retained records from the fixture-owned Redis, without mutation."""
    def read(*args):
        return subprocess.run(
            ["docker", "compose", "-f", "docker-compose.test.yml", "exec", "-T",
             "redis-test", "redis-cli", "--raw", *args],
            check=True, capture_output=True, text=True, timeout=15,
        ).stdout.strip()
    ids = read("SMEMBERS", f"metrics:task_runs:{slug}:{task}").splitlines()
    return [json.loads(read("GET", f"metrics:run:{run_id}")) for run_id in ids]


def test_review_feedback_reaches_new_reviewed_head_and_merge(
    testbed_slug, get_state, upload_zip, make_task_zip, post_review, reset_testbed,
):
    repo = testbed_slug.replace("__", "/", 1)
    config = _load_shared_config()
    configured = next(item for item in config.repositories if repo in item.url)
    assert configured.allow_merge_without_checks  # explicit testbed exemption
    policy = reviewer_policy_from_config(config)
    outcome = MergeOutcomeApi(repo, configured.branch)
    reviewer = ReviewApi.from_environment()
    number_id = time.time_ns()
    task, branch = f"PR-{number_id}", f"pr-{number_id}-e2e-fix-feedback"

    def ready(state, head=None):
        pr = state.get("current_pr") or {}
        return (state.get("state") == "WATCH"
                and (state.get("current_task") or {}).get("pr_id") == task
                and pr.get("branch") == branch and pr.get("publication_at")
                and (head is None or pr.get("head_sha") == head)
                and state.get("review_request_head") == pr.get("head_sha")
                and state.get("review_request_pr") == pr.get("number")
                and state.get("review_request_task") == task)

    def review_status(head, author):
        clear_review_status_cache()
        return get_pr_review_status(repo, number, author, head, policy=policy)

    with coder_shim("success_pending_ci"):
        assert upload_zip(make_task_zip(number_id, "e2e-fix-feedback")) in (200, 201)
        first = _wait(get_state, ready, timeout=300)
        pr = first["current_pr"]
        number, old_head = pr["number"], pr["head_sha"]
        old_marker = outcome.published_marker(number, branch, old_head, task)
        pull = outcome.assert_pull(number, branch, old_head)
        author = pull["user"]["login"]
        initial_runs = _runs(testbed_slug, task)
        assert len(initial_runs) == 1, initial_runs
        original_run = initial_runs[0]
        assert [i["phase"] for i in original_run["invocations"]] == ["coding"]

        # Valid approval of the old HEAD must not authorize the FIX HEAD.
        reviewer.approve_head(number, old_head, pr["publication_at"])
        assert review_status(old_head, author) == ReviewStatus.APPROVED
        SHIM_SCENARIO_PATH.write_text("fix_existing_pr_pending_ci\n")
        time.sleep(2)  # GitHub evidence timestamps have second resolution.
        review = post_review(number)
        assert review["commit_id"] == review["head_sha"] == old_head
        assert review["state"] == "CHANGES_REQUESTED" and review["reviewer"] != author

        fixed = _wait(get_state, lambda state: ready(state)
                      and state["current_pr"]["head_sha"] != old_head, timeout=240)
        new_pr = fixed["current_pr"]
        assert new_pr["number"] == number, new_pr
        head = new_pr["head_sha"]
        marker = outcome.fixed_marker(number, branch, head, task, old_head, old_marker)
        assert review_status(head, author) != ReviewStatus.APPROVED

        # Receipt, progress and provider-error comments are not findings or
        # approvals. The production evaluator must keep this HEAD unapproved.
        time.sleep(2)
        for body in ("LGTM", "Review in progress", "Unable to review: transport unavailable"):
            reviewer._call("post_nonfinding", number, f"repos/{repo}/issues/{number}/comments",
                           method="POST", payload={"body": body})
        assert review_status(head, author) != ReviewStatus.APPROVED
        feedback = read_review_feedback(
            repo, number, author, policy,
            after=datetime.fromisoformat(new_pr["publication_at"]), head_sha=head,
        )
        assert not feedback.findings and feedback.provider_error_only, feedback
        time.sleep(2)
        reviewer.approve_head(number, head, new_pr["publication_at"])
        assert review_status(head, author) == ReviewStatus.APPROVED
        # Open the CI gate only after current-change evidence is admitted.
        subprocess.run(
            ["gh", "api", "-X", "POST", f"repos/{repo}/statuses/{head}",
             "-f", "state=success", "-f", "context=e2e/watch-merge-gate"],
            check=True, capture_output=True, text=True, timeout=30,
        )
        # Exercise the production evidence reader against GitHub's real payload,
        # including producer hydration from the SHA-pinned status history.
        _wait(
            lambda: {"ci": _retrieve_ci_status_evidence(repo, head, fresh=True)},
            lambda evidence: _classify_ci_retrieval(evidence["ci"]) == CIStatus.SUCCESS,
            timeout=45,
        )
        final = _wait(get_state, lambda state: task_released_and_done(state, task, branch), timeout=240)
        evidence = outcome.assert_merged(number, branch, head, task, marker)

        states = [row["state"] for row in final["history"]]
        cursor = 0
        for expected in ("WATCH", "FIX", "WATCH", "MERGE"):
            cursor = states.index(expected, cursor) + 1
        pulls = outcome._get(f"pulls?state=all&head={repo.split('/')[0]}:{branch}")
        assert [p["number"] for p in pulls] == [number], pulls
        runs = _runs(testbed_slug, task)
        # Preserve actual records alongside the synthetic CI checkout identity,
        # even if an accounting assertion fails after the positive merge path.
        destination = Path(__file__).parent / "evidence" / f"fix-feedback-{task}.json"
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_text(json.dumps({
            "tested_sha": subprocess.check_output(
                ["git", "rev-parse", "HEAD"], cwd=Path(__file__).resolve().parents[2], text=True,
            ).strip(),
            "repo": repo, "task": task, "branch": branch, "old_head": old_head,
            "merged": evidence, "original_run": original_run, "runs": runs,
        }, indent=2, sort_keys=True) + "\n")

        # CODING completion is immutable history, not an active record that FIX
        # can reopen. The successor is admitted once from that predecessor.
        by_id = {record["run_id"]: record for record in runs}
        assert len(by_id) == len(runs), runs
        assert by_id[original_run["run_id"]] == original_run, runs
        assert original_run["exit_reason"] == "coding_complete"
        assert original_run["outcome"] == "superseded"
        assert original_run["run_phase"] == "coding"
        assert original_run["head_sha"] == old_head
        assert original_run["fix_iterations"] == 0
        assert sorted(record["attempt_index"] for record in runs) == [
            original_run["attempt_index"], original_run["attempt_index"] + 1,
        ], runs
        successor_id = str(uuid5(
            NAMESPACE_URL, f"{testbed_slug}:coding_complete:{original_run['run_id']}:fix",
        ))
        retained = by_id[successor_id]
        assert retained["coder_session_id"] != original_run["coder_session_id"]
        assert retained["task_spec_hash"] == original_run["task_spec_hash"] != ""
        assert retained["base_sha"] == original_run["base_sha"]
        assert datetime.fromisoformat(retained["started_at"]) >= datetime.fromisoformat(original_run["ended_at"])
        for record in runs:
            assert record["task_id"] == task and record["repo_name"] == testbed_slug, record
            assert record["base_branch"] == configured.branch
            assert record["ended_at"] and record["duration_ms"] is not None, record
            assert record["cause"] is None and not record["operator_intervention"], record
            assert record["fix_recovery_baseline"] == 0, record
            for invocation in record["invocations"]:
                assert invocation["run_id"] == record["run_id"], invocation
                assert invocation["attempt_index"] == record["attempt_index"], invocation
        # Count all stored invocations, so duplicate execution cannot hide behind
        # repeated WATCH/FIX events or a record containing extra invocations.
        assert sorted(i["phase"] for record in runs for i in record["invocations"]) == ["coding", "fix"]
        assert [i["phase"] for i in retained["invocations"]] == ["fix"]
        invocation = retained["invocations"][0]
        assert invocation["fix_iteration"] == retained["fix_iterations"] == 1
        assert invocation["fix_trigger"] == {"ci": None, "review": True}
        assert retained["exit_reason"] == "success_merged" and retained["outcome"] == "merged", retained
        assert retained["run_phase"] == "merge" and retained["head_sha"] == evidence["head_sha"] == head
        assert [parent["sha"] for parent in outcome._get(f"commits/{head}")["parents"]] == [old_head]
        print(f"review-driven FIX merged outcome: {evidence}; predecessor={original_run['run_id']}; "
              f"successor={retained['run_id']}")
