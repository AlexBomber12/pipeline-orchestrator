"""A real review-driven FIX publishes once, obtains fresh evidence and merges.

Uses the independent reviewer App and disposable GHA stack, never a paid coder.
The pending CI status holds each HEAD while review evidence is inspected.
"""

import json
import subprocess
import time
from datetime import datetime
from pathlib import Path

from src.config import load_config
from src.github.reviewer_policy import reviewer_policy_from_config
from src.github.reviews import clear_review_status_cache, get_pr_review_status, read_review_feedback
from src.models import ReviewStatus

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
    config = load_config(str(Path(__file__).parent / "data/config.test.yml"))
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
        final = _wait(get_state, lambda state: task_released_and_done(state, task, branch), timeout=240)
        evidence = outcome.assert_merged(number, branch, head, task, marker)

        states = [row["state"] for row in final["history"]]
        cursor = 0
        for expected in ("WATCH", "FIX", "WATCH", "MERGE"):
            cursor = states.index(expected, cursor) + 1
        pulls = outcome._get(f"pulls?state=all&head={repo.split('/')[0]}:{branch}")
        assert [p["number"] for p in pulls] == [number], pulls
        runs = _runs(testbed_slug, task)
        assert len(runs) == 1 and runs[0]["run_id"] == original_run["run_id"], runs
        retained = runs[0]
        assert retained["attempt_index"] == original_run["attempt_index"]
        assert retained["fix_iterations"] == 1, retained
        assert retained["invocations"][0] == original_run["invocations"][0]
        assert [i["phase"] for i in retained["invocations"]] == ["coding", "fix"]
        assert retained["invocations"][1]["fix_iteration"] == 1
        assert retained["invocations"][1]["fix_trigger"] == {"ci": None, "review": True}
        assert retained["exit_reason"] == "success_merged", retained
        print(f"review-driven FIX merged outcome: {evidence}; run={retained['run_id']}")
