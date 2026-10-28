import base64
import copy
import json
import subprocess

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
