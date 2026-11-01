import base64
import copy
import json
import subprocess
from datetime import datetime, timezone

import pytest

from tests.e2e.lib.review_api import MergeOutcomeApi, ReviewApi, ReviewApiError, task_released_and_done


def _result(returncode=0, stdout="{}", stderr=""):
    return subprocess.CompletedProcess([], returncode, stdout, stderr)


def test_reviewer_token_isolated_from_author_token(monkeypatch):
    monkeypatch.setenv("GH_TOKEN", "author-token")
    payloads = [
        {"user": {"login": "author[bot]"}, "head": {"sha": "abc123"}},
        [{"user": {"login": "author[bot]"}, "body": "@codex review", "created_at": "2020-01-01T00:00:01Z"}],
        {"commit": {"committer": {"date": "2020-01-01T00:00:00Z"}}},
        [{"filename": "tests/e2e-shim-marker.txt", "patch": "@@ -2 +2,2 @@\n old\n+new\n"}],
        {"id": 91, "state": "CHANGES_REQUESTED", "user": {"login": "codex-reviewer[bot]"}, "commit_id": "abc123"},
        [{"pull_request_review_id": 91, "path": "tests/e2e-shim-marker.txt", "created_at": "2020-01-01T00:00:02Z"}],
    ]
    calls = []

    def run(command, **kwargs):
        if "--include" in command:
            assert kwargs["env"]["GH_TOKEN"] == "reviewer-token"
            return _result(stdout="HTTP/2.0 200 OK\r\nDate: Wed, 01 Jan 2020 00:00:03 GMT\r\n\r\n{}")
        calls.append((command, kwargs))
        return _result(stdout=json.dumps(payloads.pop(0)))

    api = ReviewApi.from_environment(
        environ={"TESTBED_REVIEWER_TOKEN": "reviewer-token", "GH_TOKEN": "author-token"},
        runner=run,
    )
    review = api.post_changes_requested(17)
    assert review["state"] == "CHANGES_REQUESTED"
    assert all(kwargs["env"]["GH_TOKEN"] == "reviewer-token" for _, kwargs in calls)
    assert all("reviewer-token" not in command for command, _ in calls)
    request = json.loads(calls[4][1]["input"])
    assert request["commit_id"] == "abc123" and request["comments"] == [
        {"path": "tests/e2e-shim-marker.txt", "line": 3, "side": "RIGHT", "body": "P1: e2e review feedback"}
    ]


def test_missing_reviewer_token_never_falls_back_to_author():
    with pytest.raises(ReviewApiError, match="author GH_TOKEN is never used"):
        ReviewApi.from_environment(environ={"GH_TOKEN": "author-token"})


def test_api_error_reports_bounded_structured_details():
    response = {
        "message": "Validation Failed",
        "errors": [
            {"resource": "PullRequestReview", "field": "user_id", "code": "custom", "value": "omit-me"}
        ],
    }

    api = ReviewApi(
        "reviewer-token",
        runner=lambda *args, **kwargs: _result(1, json.dumps(response), "gh: HTTP 422"),
    )
    with pytest.raises(ReviewApiError) as caught:
        api._call("create_request_changes_review", 3635, "repos/example/test/pulls/3635/reviews")
    message = str(caught.value)
    assert "operation=create_request_changes_review" in message
    assert "pr_number=3635, return_code=1" in message
    assert "Validation Failed" in message
    assert "PullRequestReview" in message
    assert "omit-me" not in message
    assert "reviewer-token" not in message


@pytest.fixture
def merge_evidence():
    repo, branch, head, merge, base = "example/testbed", "pr-42-upload", "a" * 40, "b" * 40, "c" * 40
    marker = "shim marker for PR-42 at 2026-10-27T00:00:00Z"

    def content(text):
        return {"content": base64.b64encode(text.encode()).decode()}

    pull = {
        "number": 17, "merged": True, "state": "closed", "merge_commit_sha": merge,
        "head": {"ref": branch, "sha": head, "repo": {"full_name": repo}},
        "base": {"ref": "release/test", "repo": {"full_name": repo}},
    }
    responses = {
        "pulls/17": pull,
        "commits/release%2Ftest": {"sha": base},
        f"compare/{merge}...{base}": {"status": "ahead"},
        **{f"contents/tests/e2e-shim-marker.txt?ref={ref}": content(marker + "\n")
           for ref in (head, merge, base)},
        f"contents/tasks/PR-42.md?ref={base}": content(
            f"---\nstatus: DONE\n---\n# PR-42: upload\nBranch: {branch}\n"
        ),
    }
    calls = []

    def run(command, **kwargs):
        calls.append(command[-1])
        path = command[-1].removeprefix(f"repos/{repo}/")
        return _result(stdout=json.dumps(responses[path]))

    return MergeOutcomeApi(repo, "release/test", runner=run), responses, calls, marker


def test_matching_publication_and_merge_on_configured_base(merge_evidence):
    api, _, calls, marker = merge_evidence
    assert api.published_marker(17, "pr-42-upload", "a" * 40, "PR-42") == marker
    evidence = api.assert_merged(17, "pr-42-upload", "a" * 40, "PR-42", marker)
    assert evidence == {
        "number": 17, "head_sha": "a" * 40, "merge_commit_sha": "b" * 40, "base_sha": "c" * 40,
    }
    assert any("commits/release%2Ftest" in call for call in calls)


@pytest.mark.parametrize("fault", [
    "closed_unmerged", "no_merge_commit", "wrong_pr", "wrong_head", "short_head", "wrong_branch",
    "wrong_base", "wrong_repo", "merge_off_base", "missing_marker_on_merge", "missing_marker_on_base",
    "wrong_task", "wrong_task_branch", "not_done",
])
def test_merge_evidence_rejects_false_completion(merge_evidence, fault):
    api, responses, _, marker = merge_evidence
    pull = responses["pulls/17"]
    changes = {
        "closed_unmerged": (pull, "merged", False),
        "no_merge_commit": (pull, "merge_commit_sha", None),
        "wrong_pr": (pull, "number", 18),
        "wrong_head": (pull["head"], "sha", "d" * 40),
        "wrong_branch": (pull["head"], "ref", "other"),
        "wrong_base": (pull["base"], "ref", "main"),
        "wrong_repo": (pull["head"]["repo"], "full_name", "example/other"),
        "merge_off_base": (responses[f"compare/{'b' * 40}...{'c' * 40}"], "status", "diverged"),
    }
    if fault in changes:
        target, key, value = changes[fault]
        target[key] = value
    elif fault.startswith("missing_marker"):
        ref = "b" * 40 if fault.endswith("merge") else "c" * 40
        responses[f"contents/tests/e2e-shim-marker.txt?ref={ref}"]["content"] = ""
    elif fault != "short_head":
        text = "---\nstatus: DONE\n---\n# PR-42: upload\nBranch: pr-42-upload\n"
        old = {"wrong_task": "PR-42:", "wrong_task_branch": "pr-42-upload", "not_done": "DONE"}[fault]
        text = text.replace(old, "other")
        responses[f"contents/tasks/PR-42.md?ref={'c' * 40}"]["content"] = base64.b64encode(text.encode()).decode()
    with pytest.raises(AssertionError):
        api.assert_merged(17, "pr-42-upload", "a" * (7 if fault == "short_head" else 40), "PR-42", marker)


@pytest.mark.parametrize("content", ["", "shim marker for PR-420 at yesterday\n"])
def test_publication_requires_marker_for_exact_task(merge_evidence, content):
    api, responses, _, _ = merge_evidence
    encoded = base64.b64encode(content.encode()).decode()
    responses[f"contents/tests/e2e-shim-marker.txt?ref={'a' * 40}"]["content"] = encoded
    with pytest.raises(AssertionError):
        api.published_marker(17, "pr-42-upload", "a" * 40, "PR-42")


def test_convergence_requires_positive_task_and_released_identities():
    state = {
        "state": "IDLE", "current_task": None, "current_pr": None,
        "current_queue": [{"pr_id": "PR-42", "branch": "pr-42-upload", "status": "DONE"}],
    }
    assert task_released_and_done(state, "PR-42", "pr-42-upload")
    for changes in [
        {"state": "WATCH"}, {"current_task": {"pr_id": "PR-42"}}, {"current_pr": {"state": "MERGED"}},
        {"current_queue": []},
        *[{"current_queue": [{**state["current_queue"][0], key: value}]}
          for key, value in [("pr_id", "PR-43"), ("branch", "other"), ("status", "TODO")]],
        {"current_queue": state["current_queue"] * 2},
    ]:
        changed = {**copy.deepcopy(state), **changes}
        assert not task_released_and_done(changed, "PR-42", "pr-42-upload"), changed
    assert not task_released_and_done(None, "PR-42", "pr-42-upload")


@pytest.mark.parametrize("actual_head", ["a" * 40, "b" * 40])
def test_approval_binds_current_head_and_fresh_anchor(actual_head):
    head = "a" * 40
    payloads = [
        {"head": {"sha": actual_head}, "user": {"login": "author"}},
        {"commit_id": head, "state": "APPROVED"},
        [{"id": 19, "user": {"login": "author"}, "body": "@codex review",
          "created_at": "2020-01-01T00:00:02Z"}],
        {"content": "+1"},
    ]
    calls = []

    def run(command, **kwargs):
        if "--include" in command:
            assert kwargs["env"]["GH_TOKEN"] == "reviewer-token"
            return _result(stdout="HTTP/2.0 200 OK\r\nDate: Wed, 01 Jan 2020 00:00:03 GMT\r\n\r\n{}")
        calls.append((command, kwargs))
        return _result(stdout=json.dumps(payloads.pop(0)))

    api = ReviewApi("reviewer-token", runner=run)
    if actual_head != head:
        with pytest.raises(AssertionError):
            api.approve_head(17, head, "2020-01-01T00:00:01Z")
        assert len(calls) == 1
        return
    api.approve_head(17, head, "2020-01-01T00:00:01Z")
    assert json.loads(calls[1][1]["input"])["commit_id"] == head
    assert calls[-1][0][4].endswith("issues/comments/19/reactions")
    assert all(call[1]["env"]["GH_TOKEN"] == "reviewer-token" for call in calls)


@pytest.mark.parametrize("fault", [None, "no_push", "unchanged_marker", "unmatched_pr", "remote_mismatch"])
def test_fix_outcome_requires_changed_marker_on_matching_new_remote_head(merge_evidence, fault):
    api, responses, _, marker = merge_evidence
    head, old_head = "a" * 40, "d" * 40
    old_marker = "shim marker for PR-42 at old"
    responses["git/ref/heads/pr-42-upload"] = {"object": {"sha": head}}
    if fault == "no_push":
        old_head = head
    elif fault == "unchanged_marker":
        old_marker = marker
    elif fault == "unmatched_pr":
        responses["pulls/17"]["number"] = 18
    elif fault == "remote_mismatch":
        responses["git/ref/heads/pr-42-upload"]["object"]["sha"] = old_head
    if fault:
        with pytest.raises(AssertionError):
            api.fixed_marker(17, "pr-42-upload", head, "PR-42", old_head, old_marker)
    else:
        assert api.fixed_marker(17, "pr-42-upload", head, "PR-42", old_head, old_marker) == marker


def test_review_transport_failure_is_unknown_and_never_an_actionable_finding(monkeypatch):
    from src.github import reviews
    from src.github.reviewer_policy import ReviewerPolicy

    def unavailable(*args, **kwargs):
        raise RuntimeError("controlled transport unavailable")

    monkeypatch.setattr(reviews.cache, "_gh_api_paginated", unavailable)
    feedback = reviews.read_review_feedback("example/testbed", 17, "author", ReviewerPolicy([]))
    assert feedback.findings == () and feedback.source_ids == ()
    assert "review feedback unavailable" in feedback.unknown_reason
    assert not feedback.provider_error_only


@pytest.mark.parametrize("runner_hour", [8, 10])
def test_review_clock_uses_github_time_despite_runner_skew(monkeypatch, runner_hour):
    from tests.e2e.lib import review_api

    class SkewedClock(datetime):
        @classmethod
        def now(cls, tz=None):
            return cls(2026, 11, 1, runner_hour, tzinfo=tz)

    monkeypatch.setattr(review_api, "datetime", SkewedClock)
    sleeps, calls = [], []
    monkeypatch.setattr(review_api.time, "sleep", sleeps.append)
    responses = iter([
        "Sun, 01 Nov 2026 09:00:00 GMT",
        "Sun, 01 Nov 2026 09:00:01 GMT",
        "Sun, 01 Nov 2026 09:00:02 GMT",
    ])

    def run(command, **kwargs):
        calls.append((command, kwargs))
        return _result(stdout=f"HTTP/2.0 200 OK\r\ndate: {next(responses)}\r\n\r\n{{}}")

    ReviewApi("reviewer-token", runner=run)._wait_after(
        17, datetime(2026, 11, 1, 9, tzinfo=timezone.utc),
    )
    assert sleeps == [0.25, 0.25]
    assert len(calls) == 3
    assert all("--include" in cmd and "Cache-Control: no-cache" in cmd for cmd, _ in calls)
    assert all(kwargs["env"]["GH_TOKEN"] == "reviewer-token" for _, kwargs in calls)


@pytest.mark.parametrize("response", [
    "HTTP/2.0 200 OK\n\n{\nDate: Sun, 01 Nov 2026 09:00:02 GMT\n}",
    "HTTP/2.0 200 OK\nDate: invalid\n\n{}",
    "HTTP/2.0 200 OK\nDate: Sun, 01 Nov 2026 09:00:02\n\n{}",
])
def test_review_clock_requires_valid_server_date(response):
    api = ReviewApi("reviewer-token", runner=lambda *a, **kw: _result(stdout=response))
    with pytest.raises(ReviewApiError, match="missing or invalid GitHub Date"):
        api._wait_after(17, datetime(2026, 11, 1, 9, tzinfo=timezone.utc))


def test_review_clock_rejects_transport_failure():
    api = ReviewApi("reviewer-token", runner=lambda *a, **kw: _result(1, '{"message":"unavailable"}'))
    with pytest.raises(ReviewApiError, match="GitHub clock read failed.*unavailable"):
        api._wait_after(17, datetime(2026, 11, 1, 9, tzinfo=timezone.utc))


def test_review_clock_wait_is_bounded(monkeypatch):
    from tests.e2e.lib import review_api

    elapsed = iter([0, 11])
    monkeypatch.setattr(review_api.time, "monotonic", lambda: next(elapsed))
    api = ReviewApi("reviewer-token", runner=lambda *a, **kw: _result(
        stdout="HTTP/2.0 200 OK\nDate: Sun, 01 Nov 2026 09:00:00 GMT\n\n{}",
    ))
    with pytest.raises(ReviewApiError, match="timed out waiting for GitHub review clock"):
        api._wait_after(17, datetime(2026, 11, 1, 9, tzinfo=timezone.utc))
