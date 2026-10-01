import json
import subprocess

import pytest

from tests.e2e.lib.review_api import ReviewApi, ReviewApiError


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
