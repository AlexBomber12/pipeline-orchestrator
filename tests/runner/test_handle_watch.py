"""PR-224a: handle_watch handler tests for src/daemon/runner.py

Mechanical move from tests/test_runner.py. Helpers and fixtures still live in
tests/test_runner.py and are referenced via the ``h`` alias.
"""

from __future__ import annotations

import asyncio
import json
import time
import types
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock

import pytest
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import TimeoutError as RedisTimeoutError
from src.cancellation.storage import task_spec_content_hash
from src.coders import claude as claude_plugin_module
from src.config import AppConfig, DaemonConfig, RepoConfig, TrustedReviewerIdentity
from src.daemon import runner as runner_module
from src.daemon.handlers import fix as fix_module
from src.daemon.handlers import watch as watch_module
from src.daemon.runner import PipelineRunner
from src.github import cache as gh_cache
from src.github import reviews as gh_reviews
from src.keyspace import pipeline_state
from src.models import (
    CIStatus,
    FeedbackCheckResult,
    InvocationSnapshot,
    PipelineState,
    PRInfo,
    PublicationMigration,
    QueueTask,
    RepoState,
    ReviewStatus,
    SizeFixCompletion,
    TaskStatus,
)
from src.publication_migration import (
    MigrationCommand,
    migration_binding,
)
from src.publication_migration import (
    command_key as migration_command_key,
)
from src.publication_migration import enqueue as enqueue_migration
from src.subsource_registry import SuppressionReason

from tests.legacy_recovery_git import git as real_git
from tests.legacy_recovery_git import legacy_repo
from tests.runner import _helpers as h

claude_cli = claude_plugin_module.claude_cli


def _admit_ci_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        fix_module, "_current_ci_failure",
        lambda repo, number, sha, required, empty: {
            "repo": repo, "pr_number": number, "head_sha": sha,
            "contexts": [{
                "name": "unit", "producer": "app:1", "source": "check",
                "check_id": 1, "attempt": 1, "state": "failure",
                "workflow_run_id": 2, "job_id": 3,
            }],
        },
    )


def _publication_transport(
    monkeypatch: pytest.MonkeyPatch, pr: PRInfo, timestamp: str,
) -> None:
    """Feed the real publication reader recorded-shape head and activity data."""
    def run_gh(args: list[str], **_kwargs: object) -> object:
        if "/pulls/" in args[1]:
            return {"branch": pr.branch, "repo": "octo/demo", "sha": pr.head_sha}
        if "/activity?" in args[1]:
            return [{"activity_type": "branch_creation", "ref": f"refs/heads/{pr.branch}",
                     "before": "0" * 40, "after": pr.head_sha, "timestamp": timestamp}]
        raise AssertionError(args)

    monkeypatch.setattr("src.github.gh_runner.run_gh", run_gh)


@pytest.mark.parametrize(
    ("finding", "ci_status", "expected_feedback", "feedback_unavailable", "missing_poll", "missing_push"),
    [
        (False, CIStatus.PENDING, [], False, False, False),
        (True, CIStatus.PENDING, ["P1: fix the null check"], False, False, False),
        (False, CIStatus.FAILURE, [None], False, False, False),
        (False, CIStatus.FAILURE, [None], False, False, True),
        (True, CIStatus.FAILURE, ["P1: fix the null check"], False, False, False),
        (True, CIStatus.FAILURE, ["P1: fix the null check"], True, False, False),
        (True, CIStatus.FAILURE, [None], False, True, False),
    ],
)
def test_watch_620_evidence_drives_fix_without_rereading(
    monkeypatch: pytest.MonkeyPatch,
    finding: bool,
    ci_status: CIStatus,
    expected_feedback: list[str | None],
    feedback_unavailable: bool,
    missing_poll: bool,
    missing_push: bool,
) -> None:
    receipt = json.loads(
        (Path(__file__).parents[1] / "fixtures/review_receipt_620.json").read_text()
    )
    push_time = datetime.now(timezone.utc) - timedelta(minutes=2)
    receipt["created_at"] = (push_time + timedelta(minutes=1)).isoformat()
    issue_comments = [
        {"id": 10, "user": {"login": "author"}, "body": "@codex review",
         "created_at": (push_time + timedelta(seconds=10)).isoformat()},
        receipt,
    ]
    if finding:
        issue_comments.remove(receipt)
        issue_comments.append({
            "id": 11, "user": receipt["user"], "body": "P1: fix the null check",
            "created_at": (push_time + timedelta(minutes=1, seconds=10)).isoformat(),
        })

    def transport(path: str) -> list[dict]:
        if path.endswith("/issues/42/comments"):
            return issue_comments
        return []

    monkeypatch.setattr(gh_cache, "_gh_api_paginated", transport)
    if feedback_unavailable:
        monkeypatch.setattr(
            gh_reviews, "read_review_feedback",
            lambda *_: pytest.fail("WATCH must reuse status-poll evidence"),
        )
    head = "63f07ef48a035c9aa31c37e0fbf3dc215ba0045e"

    def open_prs(repo: str, **kwargs: object) -> list[PRInfo]:
        result = PRInfo(
            number=42, branch="pr-459", head_sha=head, ci_status=ci_status,
            quarantine_labels={"quarantine:large_diff"},
            review_status=gh_reviews.get_pr_review_status(
                repo, 42, "author", head, policy=kwargs["reviewer_policy"]
            ),
        )
        if missing_poll:
            gh_reviews.clear_review_status_cache()
        return [result]

    monkeypatch.setattr(watch_module.gh_prs, "get_open_prs", open_prs)
    monkeypatch.setattr(watch_module.gh_prs, "get_pr_state", lambda *_: "OPEN")
    gh_reviews.clear_review_status_cache()
    runner = h._make_runner(review_timeout_min=120)
    runner.state.current_pr = PRInfo(number=42, branch="pr-459", head_sha=head)
    runner.state.state = PipelineState.WATCH
    runner._last_push_at = None if missing_push else push_time
    runner._last_push_at_pr_number = 42
    if missing_push:
        monkeypatch.setattr(runner, "_rehydrate_last_push_at", lambda *_: None)
    calls: list[str | None] = []

    async def fake_fix() -> None:
        calls.append(getattr(runner, "_fix_review_feedback", None))

    async def no_scan() -> None:
        return None

    monkeypatch.setattr(runner, "handle_fix", fake_fix)
    monkeypatch.setattr(runner, "_scan_pr_diff_once", no_scan)
    asyncio.run(runner.handle_watch())
    assert calls == expected_feedback
    assert runner.state.state == PipelineState.WATCH
    if feedback_unavailable:
        assert not any("combined FIX deferred" in entry["event"] for entry in runner.state.history)
    if missing_poll:
        assert any("CI FIX continues" in entry["event"] for entry in runner.state.history)


@pytest.mark.parametrize("unresolved", [False, True])
def test_structured_summary_after_trigger_does_not_dispatch_fix(
    monkeypatch: pytest.MonkeyPatch,
    unresolved: bool,
) -> None:
    summary = json.loads(
        (Path(__file__).parents[1] / "fixtures/review_summary_structured.json").read_text()
    )
    if unresolved:
        summary["body"] = summary["body"].replace(
            '"has_any_findings":false,"has_unresolved_findings":false',
            '"has_any_findings":true,"has_unresolved_findings":true',
        )
    comments = [
        {"id": 10, "user": {"login": "author"}, "body": "@codex review",
         "created_at": "2026-10-17T21:05:00Z"},
        summary,
    ]
    def transport(path: str) -> list[dict]:
        if path.endswith("/issues/42/comments"):
            return comments
        if unresolved and path.endswith("/issues/42/reactions"):
            return [{
                "content": "+1", "user": summary["user"],
                "created_at": "2026-10-17T21:05:30Z",
            }]
        return []

    monkeypatch.setattr(gh_cache, "_gh_api_paginated", transport)
    monkeypatch.setattr(
        gh_reviews, "_get_pr_push_time",
        lambda *_: datetime(2026, 10, 17, 21, 4, tzinfo=timezone.utc),
    )
    head = "a" * 40

    def open_prs(repo: str, **kwargs: object) -> list[PRInfo]:
        return [PRInfo(
            number=42, branch="pr-459", head_sha=head, ci_status=CIStatus.SUCCESS,
            review_status=gh_reviews.get_pr_review_status(
                repo, 42, "author", head, policy=kwargs["reviewer_policy"]
            ),
        )]

    monkeypatch.setattr(watch_module.gh_prs, "get_open_prs", open_prs)
    monkeypatch.setattr(watch_module.gh_prs, "get_pr_state", lambda *_: "OPEN")
    gh_reviews.clear_review_status_cache()
    runner = h._make_runner(review_timeout_min=120)
    runner.state.current_pr = PRInfo(number=42, branch="pr-459", head_sha=head)
    runner.state.state = PipelineState.WATCH
    runner._last_push_at = datetime(2026, 10, 17, 21, 4, tzinfo=timezone.utc)
    runner._last_push_at_pr_number = 42
    calls: list[str] = []

    async def fake_fix() -> None:
        calls.append("fix")

    async def fake_merge() -> None:
        calls.append("merge")

    async def no_scan() -> None:
        return None

    monkeypatch.setattr(runner, "handle_fix", fake_fix)
    monkeypatch.setattr(runner, "handle_merge", fake_merge)
    monkeypatch.setattr(runner, "_scan_pr_diff_once", no_scan)
    asyncio.run(runner.handle_watch())
    assert calls == []
    assert runner.state.state == PipelineState.WATCH
    assert runner.state.current_pr.review_status == ReviewStatus.PENDING


@pytest.mark.parametrize(
    ("summary_created", "summary_updated", "finding_at", "approval_at", "summary_sha", "expected"),
    [
        ("21:01:00", "21:09:00", None, "21:08:00", "current", (ReviewStatus.PENDING, [])),
        ("21:06:00", "21:09:00", None, "21:08:00", "current", (ReviewStatus.PENDING, [])),
        ("21:06:00", "21:08:00", None, "21:08:00", "current", (ReviewStatus.PENDING, [])),
        (None, None, "21:08:00", "21:08:00", "current", (ReviewStatus.PENDING, [])),
        (None, None, "21:07:00", "21:08:00", "current", (ReviewStatus.APPROVED, ["merge"])),
        (None, None, "21:09:00", "21:08:00", "current", (ReviewStatus.CHANGES_REQUESTED, ["fix"])),
        ("21:01:00", "21:09:00", None, "21:08:00", "old", (ReviewStatus.APPROVED, ["merge"])),
        ("21:01:00", "21:09:00", None, "21:08:00", "unsupported", (ReviewStatus.PENDING, [])),
    ],
)
def test_watch_orders_mutable_summary_and_approval_evidence(
    monkeypatch: pytest.MonkeyPatch,
    summary_created: str | None,
    summary_updated: str | None,
    finding_at: str | None,
    approval_at: str,
    summary_sha: str,
    expected: tuple[ReviewStatus, list[str]],
) -> None:
    head = "a" * 40
    actor = {"id": 199175422, "login": "chatgpt-codex-connector[bot]", "type": "Bot"}
    def stamp(value: str) -> str:
        return f"2026-10-17T{value}Z"
    issue_comments = [{
        "id": 10, "user": {"login": "author"}, "body": "@codex review",
        "created_at": stamp("21:05:00"),
    }]
    if summary_created:
        summary = json.loads(
            (Path(__file__).parents[1] / "fixtures/review_summary_structured.json").read_text()
        )
        summary["created_at"] = stamp(summary_created)
        summary["updated_at"] = stamp(summary_updated)
        summary["body"] = summary["body"].replace(
            '"has_any_findings":false,"has_unresolved_findings":false',
            '"has_any_findings":true,"has_unresolved_findings":true',
        )
        if summary_sha == "old":
            summary["body"] = summary["body"].replace(head, "b" * 40)
        elif summary_sha == "unsupported":
            summary["body"] = summary["body"].replace(head, "unknown")
        issue_comments.append(summary)
    if finding_at:
        issue_comments.append({
            "id": 12, "user": actor, "body": "P1: repair the null check",
            "created_at": stamp(finding_at),
        })

    def transport(path: str) -> list[dict]:
        if path.endswith("/issues/42/comments"):
            return issue_comments
        if path.endswith("/issues/42/reactions"):
            return [{"content": "+1", "user": actor, "created_at": stamp(approval_at)}]
        return []

    monkeypatch.setattr(gh_cache, "_gh_api_paginated", transport)
    monkeypatch.setattr(
        gh_reviews, "_get_pr_push_time",
        lambda *_: datetime(2026, 10, 17, 21, 4, tzinfo=timezone.utc),
    )

    def open_prs(repo: str, **kwargs: object) -> list[PRInfo]:
        return [PRInfo(
            number=42, branch="pr-459", head_sha=head, ci_status=CIStatus.SUCCESS,
            diff_scanned_at_sha=head,
            review_status=gh_reviews.get_pr_review_status(
                repo, 42, "author", head, policy=kwargs["reviewer_policy"]
            ),
        )]

    monkeypatch.setattr(watch_module.gh_prs, "get_open_prs", open_prs)
    monkeypatch.setattr(watch_module.gh_prs, "get_pr_state", lambda *_: "OPEN")
    gh_reviews.clear_review_status_cache()
    runner = h._make_runner(review_timeout_min=120, auto_merge=True)
    runner.state.current_pr = PRInfo(number=42, branch="pr-459", head_sha=head, diff_scanned_at_sha=head)
    runner.state.state = PipelineState.WATCH
    runner._last_push_at = datetime(2026, 10, 17, 21, 4, tzinfo=timezone.utc)
    runner._last_push_at_pr_number = 42
    calls: list[str] = []

    async def fake_fix() -> None:
        calls.append("fix")
        if finding_at and expected[0] == ReviewStatus.CHANGES_REQUESTED:
            assert runner._fix_review_feedback == "P1: repair the null check"

    async def fake_merge() -> None:
        calls.append("merge")

    async def no_scan() -> None:
        return None

    monkeypatch.setattr(runner, "handle_fix", fake_fix)
    monkeypatch.setattr(runner, "handle_merge", fake_merge)
    monkeypatch.setattr(runner, "_scan_pr_diff_once", no_scan)
    asyncio.run(runner.handle_watch())
    assert runner.state.current_pr.review_status == expected[0]
    assert calls == expected[1]
    if summary_created and summary_sha != "old":
        evidence = gh_reviews.cached_review_feedback(
            runner.owner_repo, 42, head, gh_reviews.reviewer_policy_from_config(runner.app_config),
        )
        assert evidence is not None and evidence.findings == ()
        assert evidence.unknown_reason


@pytest.mark.parametrize(
    ("ci_status", "missing_push"),
    [(CIStatus.PENDING, False), (CIStatus.FAILURE, False), (CIStatus.FAILURE, True)],
)
def test_watch_supplies_only_admitted_findings_to_fix_prompt(
    monkeypatch: pytest.MonkeyPatch,
    ci_status: CIStatus,
    missing_push: bool,
) -> None:
    h._patch_subprocess(monkeypatch)
    _admit_ci_failure(monkeypatch)
    captured = h._capture_fix_kwargs(monkeypatch)
    push_time = datetime.now(timezone.utc) - timedelta(minutes=2)
    actor = {"id": 199175422, "login": "chatgpt-codex-connector[bot]"}
    issue_comments = [
        {"id": 10, "user": {"login": "author"}, "body": "@codex review",
         "created_at": (push_time + timedelta(seconds=10)).isoformat()},
        {"id": 11, "user": actor, "body": "P1: fix the null check",
         "created_at": (push_time + timedelta(minutes=1)).isoformat()},
        {"id": 12, "user": {"id": 999, "login": "other"},
         "body": "P1: unrelated advice",
         "created_at": (push_time + timedelta(minutes=1, seconds=10)).isoformat()},
        {"id": 13, "user": actor, "body": "This dereferences None on empty input",
         "created_at": (push_time + timedelta(minutes=1, seconds=15)).isoformat()},
    ]
    comment_reads: list[str] = []

    def transport(path: str) -> list[dict]:
        if path.endswith("/issues/42/comments"):
            comment_reads.append(path)
            return issue_comments
        return []

    monkeypatch.setattr(gh_cache, "_gh_api_paginated", transport)
    head = "63f07ef48a035c9aa31c37e0fbf3dc215ba0045e"

    def open_prs(repo: str, **kwargs: object) -> list[PRInfo]:
        return [PRInfo(
            number=42, branch="pr-459", head_sha=head, ci_status=ci_status,
            review_status=gh_reviews.get_pr_review_status(
                repo, 42, "author", head, policy=kwargs["reviewer_policy"]
            ),
        )]

    monkeypatch.setattr(watch_module.gh_prs, "get_open_prs", open_prs)
    monkeypatch.setattr(watch_module.gh_prs, "get_pr_state", lambda *_: "OPEN")
    monkeypatch.setattr(watch_module.gh_prs, "get_pr_metadata", lambda *_: {"head_sha": head})
    monkeypatch.setattr("src.daemon.handlers.fix._fetch_failed_ci_logs", lambda *_: "ci-boom")
    gh_reviews.clear_review_status_cache()
    runner = h._make_runner(review_timeout_min=120)
    runner.state.current_pr = PRInfo(number=42, branch="pr-459", head_sha=head)
    runner.state.state = PipelineState.WATCH
    runner._last_push_at = None if missing_push else push_time
    runner._last_push_at_pr_number = 42
    if missing_push:
        monkeypatch.setattr(runner, "_rehydrate_last_push_at", lambda *_: None)

    async def no_scan() -> None:
        return None

    monkeypatch.setattr(runner, "_scan_pr_diff_once", no_scan)
    asyncio.run(runner.handle_watch())
    context = captured["kwargs"]["extra_context"]
    if missing_push:
        assert "Latest review feedback:" not in context
        assert "Review evidence limitation: push provenance unavailable" in context
    else:
        assert "Latest review feedback:\nP1: fix the null check" in context
    assert "unrelated advice" not in context
    if not missing_push:
        assert "This dereferences None on empty input" in context
    assert comment_reads == ["repos/octo/demo/issues/42/comments"]
    if ci_status == CIStatus.FAILURE:
        assert "ci-boom" in context
    if missing_push:
        assert "push provenance unavailable" in context


def _ci_retrieval(
    repo: str,
    sha: str,
    check_runs: list[dict],
) -> Any:
    source = watch_module.gh_checks._CiSourceResult(True, not check_runs)
    status_source = watch_module.gh_checks._CiSourceResult(True, True)
    return watch_module.gh_checks._make_ci_retrieval(
        repo,
        sha,
        0.0,
        check_runs,
        {"state": "success", "statuses": []},
        source,
        status_source,
    )


def test_observe_watch_event_signature_resets_retrigger_count() -> None:
    runner = h._make_runner()
    runner.state.current_pr = PRInfo(
        number=5,
        branch="pr-001",
        watch_retrigger_count=2,
    )
    prior = PRInfo(
        number=5,
        branch="pr-001",
        ci_status=CIStatus.PENDING,
        review_status=ReviewStatus.PENDING,
    )
    updated = PRInfo(
        number=5,
        branch="pr-001",
        ci_status=CIStatus.SUCCESS,
        review_status=ReviewStatus.PENDING,
    )

    runner._observe_watch_event_signature(prior)
    runner._observe_watch_event_signature(updated)

    assert runner.state.current_pr.watch_retrigger_count == 0


def test_handle_watch_reuses_poll_reviewer_policy_for_feedback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fresh_reviewer_id = 559
    fresh_config = AppConfig(
        daemon=DaemonConfig(
            trusted_reviewer_identities=[
                TrustedReviewerIdentity(
                    user_id=fresh_reviewer_id,
                    login="fresh-codex[bot]",
                )
            ]
        )
    )
    observed_policies: list[object] = []
    fix_calls: list[int] = []

    def fake_get_open_prs(
        repo: str,
        allow_merge_without_checks: bool = False,
        reviewer_policy: object = None,
    ) -> list[PRInfo]:
        observed_policies.append(reviewer_policy)
        review_status = gh_reviews.get_pr_review_status(
            repo, 400, "author", "abc123", policy=reviewer_policy
        )
        return [
            PRInfo(
                number=400,
                branch="pr-400-reviewer-policy",
                head_sha="abc123",
                ci_status=CIStatus.PENDING,
                review_status=review_status,
            )
        ]

    def fake_paginated(path: str) -> list[dict[str, object]]:
        if "/issues/" in path:
            return [
                {
                    "user": {"id": fresh_reviewer_id, "login": "fresh-codex[bot]"},
                    "body": "P1: fix the missing guard",
                    "created_at": "2026-01-01T00:00:02Z",
                }
            ]
        return []

    async def fake_handle_fix() -> None:
        fix_calls.append(400)

    runner = h._make_runner()
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(
        number=400,
        branch="pr-400-reviewer-policy",
        head_sha="abc123",
    )
    runner._last_push_at = datetime(2026, 1, 1, tzinfo=timezone.utc)
    runner._last_push_at_pr_number = 400

    monkeypatch.setattr(watch_module, "load_config", lambda: fresh_config)
    monkeypatch.setattr(watch_module.gh_prs, "get_open_prs", fake_get_open_prs)
    monkeypatch.setattr(watch_module.gh_prs, "get_pr_state", lambda *args: "OPEN")
    monkeypatch.setattr(watch_module.gh_cache, "_gh_api_paginated", fake_paginated)
    monkeypatch.setattr(runner, "handle_fix", fake_handle_fix)
    gh_reviews.clear_review_status_cache()

    asyncio.run(runner.handle_watch())

    assert len(observed_policies) == 1
    assert fix_calls == [400]


def test_external_resolution_clears_current_pr_without_task() -> None:
    runner = h._make_runner()
    runner.state.state = PipelineState.WATCH
    runner.state.current_task = None
    runner.state.current_pr = PRInfo(number=443, branch="pr-443")
    runner.state.quarantined_prs.add(443)
    publishes: list[str] = []

    async def fake_publish() -> None:
        publishes.append("published")

    runner.publish_state = fake_publish  # type: ignore[method-assign]

    asyncio.run(
        runner._handle_external_pr_resolution(
            PRInfo(number=443, branch="pr-443"),
            "CLOSED",
        )
    )

    assert runner.state.state == PipelineState.IDLE
    assert runner.state.current_task is None
    assert runner.state.current_pr is None
    assert 443 not in runner.state.quarantined_prs
    assert publishes == ["published"]


def test_retry_failed_workflow_returns_true_after_rerun(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = h._make_runner()
    fetch_calls: list[str] = []
    rerun_calls: list[list[str]] = []

    def fake_fetch(repo: str, head_sha: str):
        fetch_calls.append(head_sha)
        return (
            [
                {
                    "id": 7,
                    "run_attempt": 1,
                    "conclusion": "FAILURE",
                    "details_url": (
                        "https://github.com/o/r/actions/runs/123456/job/1"
                    ),
                }
            ],
            [],
            True,
        )

    def fake_run_gh(args: list[str], **kwargs: Any) -> str:
        assert runner._infra_retry_baseline == [[7, 1]]
        rerun_calls.append(args)
        return ""

    monkeypatch.setattr(watch_module.gh_checks, "_fetch_ci_status_rest", fake_fetch)
    monkeypatch.setattr(watch_module.gh_runner, "run_gh", fake_run_gh)

    assert runner._retry_failed_workflow(443, "abcdef123") is True
    assert fetch_calls == ["abcdef123"]
    assert rerun_calls == [["run", "rerun", "--failed", "123456"]]
    assert any(
        "infra retry: re-ran failed jobs of 1 workflow run(s)" in entry["event"]
        for entry in runner.state.history
    )


@pytest.mark.parametrize("labels", [set(), {"quarantine:large_diff"}])
def test_handle_watch_approved_and_green_merges(
    monkeypatch: pytest.MonkeyPatch,
    labels: set[str],
) -> None:
    monkeypatch.setattr(
        "src.daemon.handlers.merge.MergeMixin._confirm_remote_merge", AsyncMock(return_value=True)
    )
    h._patch_subprocess(monkeypatch)
    pr = PRInfo(
        number=5,
        branch="pr-001",
        ci_status=CIStatus.SUCCESS,
        review_status=ReviewStatus.APPROVED,
        head_sha="green001",
        diff_scanned_at_sha="green001",
        quarantine_labels=labels,
    )
    monkeypatch.setattr("src.github.prs.get_open_prs", lambda repo, **kw: [pr])

    merged: list[tuple[str, int]] = []

    def fake_merge(repo: str, number: int, expected_head_sha: str) -> None:
        merged.append((repo, number))

    monkeypatch.setattr("src.github.prs.merge_pr", fake_merge)
    monkeypatch.setattr(runner_module.PipelineRunner, "_mark_task_done_in_snapshot", lambda self: None)

    runner = h._make_runner()
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=5, branch="pr-001")
    runner.state.current_task = QueueTask(pr_id="PR-001", title="t", status=TaskStatus.DOING)
    runner._start_current_run_record("claude", "opus")
    asyncio.run(runner.handle_watch())

    assert merged == [(runner.owner_repo, 5)]
    assert runner.state.state == PipelineState.IDLE
    assert runner.state.current_pr is None
    assert runner.state.current_task is None


def test_handle_watch_rehydrates_quarantine_before_merge(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    h._patch_subprocess(monkeypatch)
    pr = PRInfo(
        number=5,
        branch="pr-001",
        ci_status=CIStatus.SUCCESS,
        review_status=ReviewStatus.APPROVED,
        head_sha="green001",
        diff_scanned_at_sha="green001",
        quarantine_labels={"quarantine:workflow"},
    )
    monkeypatch.setattr("src.github.prs.get_open_prs", lambda repo, **kw: [pr])
    monkeypatch.setattr(
        "src.github.gh_runner.run_gh",
        lambda cmd, **kw: "quarantine:workflow\n",
    )

    merged: list[tuple[str, int]] = []
    monkeypatch.setattr(
        "src.github.prs.merge_pr",
        lambda repo, number, expected_head_sha: merged.append((repo, number)),
    )

    runner = h._make_runner()
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=5, branch="pr-001")
    runner.state.current_task = QueueTask(
        pr_id="PR-001",
        title="t",
        status=TaskStatus.DOING,
    )

    asyncio.run(runner.handle_watch())

    assert merged == []
    assert runner.state.state == PipelineState.WATCH
    assert runner.state.quarantined_prs == {5}
    assert any(
        "quarantine rehydrated from GitHub labels" in entry["event"]
        for entry in runner.state.history
    )
    assert any(
        "is quarantined; refusing to merge" in entry["event"]
        for entry in runner.state.history
    )


@pytest.mark.parametrize("labels", [set(), {"size-fix:" + "a" * 40}])
@pytest.mark.parametrize("change", [
    "none", "generation", "head", "invocation", "guardrail", "cause",
    "process", "process_unavailable", "task_unavailable", "status_raise",
    "status_false", "todo", "missing_revision",
    "redis_suppression_unavailable", "redis_cause_unavailable",
])
def test_watch_reconciles_legacy_receipt_without_label_dependency(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, labels: set[str], change: str,
) -> None:
    h._patch_subprocess(monkeypatch)
    task_path = tmp_path / "tasks" / "PR-001.md"
    task_path.parent.mkdir()
    task_path.write_text("approved generation\n")
    task_hash = task_spec_content_hash(task_path.read_text())
    invocation = InvocationSnapshot(
        plugin_id="claude", model_override=None, reasoning_effort_override=None,
        run_id="run-1", attempt_index=1, phase="fix", fix_iteration=1,
    )
    pr = PRInfo(number=5, branch="pr-001", pr_id="PR-001", head_sha="a" * 40,
                quarantine_labels=set(labels))
    runner = h._make_runner()
    runner.repo_path = str(tmp_path)
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = pr
    runner.state.current_task = QueueTask(
        pr_id="PR-001", title="t", status=TaskStatus.ERROR,
        branch="pr-001", task_file="tasks/PR-001.md",
    )
    runner.state.active_invocation = invocation
    runner.state.size_fix_completion = SizeFixCompletion(
        task_id="PR-001", branch="pr-001", pr_number=5,
        head_sha="a" * 40, fixing_label="size-fix:" + "a" * 40,
        error_revision="e" * 64, task_spec_hash=task_hash, invocation=invocation,
    )
    runner._current_run_record = types.SimpleNamespace(
        run_id="run-1", task_id="PR-001", invocations=[invocation],
        fix_iterations=1, attempt_index=1, task_spec_hash=task_hash,
    )
    if change == "generation":
        task_path.write_text("revised generation\n")
    elif change == "task_unavailable":
        monkeypatch.setattr(
            watch_module, "task_spec_content_hash",
            lambda *_args: (_ for _ in ()).throw(OSError("task unavailable")),
        )
    elif change == "head":
        pr.head_sha = "b" * 40
    elif change == "invocation":
        runner.state.active_invocation = invocation.__class__(
            plugin_id="claude", model_override=None, reasoning_effort_override=None,
            run_id="later-run", attempt_index=2, phase="fix", fix_iteration=2,
        )
    elif change == "guardrail":
        pr.quarantine_labels.add("quarantine:workflow")
    elif change == "cause":
        from src.cancellation.storage import CancellationCause, cause_key
        cause = CancellationCause(category="ERROR", payload={"subsource": "guardrail", "rule": "workflow_destruction"})
        asyncio.run(runner.redis.set(cause_key(runner.name, "PR-001"), cause.to_redis()))
    elif change == "redis_suppression_unavailable":
        monkeypatch.setattr(runner, "_detect_external_quarantine_release", AsyncMock())
        monkeypatch.setattr(
            runner, "_suppression_record_for_task",
            AsyncMock(side_effect=RedisConnectionError("Redis unavailable")),
        )
    elif change == "redis_cause_unavailable":
        from src.cancellation.storage import cause_key

        monkeypatch.setattr(runner, "_detect_external_quarantine_release", AsyncMock())
        monkeypatch.setattr(runner, "_suppression_record_for_task", AsyncMock(return_value=None))
        original_get = runner.redis.get

        async def unavailable_cause(key):
            if key == cause_key(runner.name, "PR-001"):
                raise RedisTimeoutError("Redis timed out")
            return await original_get(key)

        monkeypatch.setattr(runner.redis, "get", unavailable_cause)
    monkeypatch.setattr("src.github.prs.get_open_prs", lambda *_a, **_k: [pr])
    monkeypatch.setattr("src.github.prs.get_pr_state", lambda *_a, **_k: "OPEN")
    monkeypatch.setattr("src.github.gh_runner.run_gh", lambda *_a, **_k: None)
    monkeypatch.setattr(runner, "_remote_frontmatter_error_status_for_current_task",
                        lambda: (("todo", None, "t" * 64) if change == "todo" else
                                 ("error", SuppressionReason.CRASH,
                                  None if change == "missing_revision" else "e" * 64)))
    def process_blocker(*_args):
        if change == "process_unavailable":
            raise OSError("inspection unavailable")
        return "active coder" if change == "process" else None
    monkeypatch.setattr("src.daemon.handlers.watch.checkout_process_blocker", process_blocker)
    monkeypatch.setattr("src.daemon.handlers.watch.bound_checkout_work_blocker_for_ref", lambda *_a: None)
    write = AsyncMock(return_value=change != "status_false")
    if change == "status_raise":
        write.side_effect = OSError("status unavailable")
    monkeypatch.setattr(runner, "_commit_task_status_change", write)
    asyncio.run(runner.handle_watch())
    if change in {"none", "todo"}:
        assert write.await_count == 1
        assert write.await_args.args[1] == "TODO"
        assert runner.state.size_fix_completion is None
        assert runner.state.current_task.status == TaskStatus.DOING
    else:
        if change not in {"status_raise", "status_false"}:
            write.assert_not_awaited()
        deferred = {
            "process", "process_unavailable", "task_unavailable",
            "status_raise", "status_false", "missing_revision",
            "redis_suppression_unavailable", "redis_cause_unavailable",
        }
        assert runner.state.state == (
            PipelineState.WATCH if change in deferred else PipelineState.ERROR
        ), runner.state.error_message
        if change.startswith("redis_"):
            assert runner.state.size_fix_completion is not None
            assert any(
                "legacy completion validation deferred" in item["event"]
                for item in runner.state.history
            )


@pytest.mark.parametrize("remote_case", [
    "todo_same", "todo_revised", "error_valid", "error_guardrail", "pr_mismatch",
])
def test_watch_legacy_receipt_validates_real_remote_generation(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, remote_case: str,
) -> None:
    case = legacy_repo(tmp_path, remote_error=remote_case in {"error_valid", "error_guardrail"})
    if remote_case == "todo_revised":
        case.remote_task.write_text(
            case.remote_task.read_text().replace("Original generation", "Revised generation"),
            encoding="utf-8",
        )
    elif remote_case == "error_guardrail":
        case.remote_task.write_text(
            case.remote_task.read_text().replace("blocked_reason: crash", "blocked_reason: guardrail"),
            encoding="utf-8",
        )
    if remote_case in {"todo_revised", "error_guardrail"}:
        real_git(case.writer, "add", "tasks/PR-100.md")
        real_git(case.writer, "commit", "-m", "newer task decision")
        real_git(case.writer, "push", "origin", "main")
    base_before = real_git(case.writer, "rev-parse", "HEAD")
    local_before = real_git(case.repo, "rev-parse", "HEAD")
    task_hash = task_spec_content_hash(case.task.read_text())
    invocation = InvocationSnapshot(
        plugin_id="claude", model_override=None, reasoning_effort_override=None,
        run_id="run-100", attempt_index=2, phase="fix", fix_iteration=1,
    )
    pr = PRInfo(
        number=100, branch="pr-100-size", pr_id="PR-OTHER" if remote_case == "pr_mismatch" else "PR-100",
        head_sha=case.head, fix_iteration_count=1, no_push_fix_count=2,
    )
    runner = h._make_runner()
    runner.repo_path = str(case.repo)
    runner.repo_config.url = str(case.remote)
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = pr
    runner.state.current_task = QueueTask(
        pr_id="PR-100", title="Original generation", status=TaskStatus.ERROR,
        branch="pr-100-size", task_file="tasks/PR-100.md",
    )
    runner.state.active_invocation = invocation
    runner.state.size_fix_completion = SizeFixCompletion(
        task_id="PR-100", branch="pr-100-size", pr_number=100,
        head_sha=case.head, fixing_label="size-fix:" + case.head,
        error_revision=case.revision, task_spec_hash=task_hash, invocation=invocation,
    )
    record = types.SimpleNamespace(
        run_id="run-100", task_id="PR-100", invocations=[invocation],
        fix_iterations=1, attempt_index=2, task_spec_hash=task_hash,
    )
    runner._current_run_record = record
    coder = AsyncMock(side_effect=AssertionError("legacy completion must not rerun coder"))
    monkeypatch.setattr(runner, "handle_coding", coder)
    monkeypatch.setattr("src.github.prs.get_open_prs", lambda *_a, **_k: [pr])
    monkeypatch.setattr("src.github.prs.get_pr_state", lambda *_a, **_k: "OPEN")
    monkeypatch.setattr("src.github.gh_runner.run_gh", lambda *_a, **_k: None)
    monkeypatch.setattr(watch_module, "checkout_process_blocker", lambda *_a: None)
    asyncio.run(runner.handle_watch())
    coder.assert_not_awaited()
    assert real_git(case.repo, "rev-parse", "HEAD") == local_before
    assert real_git(case.repo, "rev-parse", "origin/pr-100-size") == case.head
    assert runner._current_run_record is record
    assert runner.state.current_pr.fix_iteration_count == 1
    assert runner.state.current_pr.no_push_fix_count == 2
    if remote_case in {"todo_same", "error_valid"}:
        assert runner.state.state == PipelineState.WATCH
        assert runner.state.current_task.status == TaskStatus.DOING
        assert runner.state.size_fix_completion is None
        remote_head = real_git(case.writer, "ls-remote", "origin", "refs/heads/main").split()[0]
        assert (remote_head == base_before) == (remote_case == "todo_same")
        real_git(case.writer, "fetch", "origin", "main")
        assert "status: TODO" in real_git(case.writer, "show", "FETCH_HEAD:tasks/PR-100.md")
    else:
        if remote_case == "todo_revised":
            assert runner.state.state == PipelineState.WATCH
            assert runner.state.size_fix_completion is not None
        else:
            assert runner.state.state == PipelineState.ERROR
        assert real_git(case.writer, "ls-remote", "origin", "refs/heads/main").split()[0] == base_before



def test_watch_quarantine_rehydrate_ignores_invalid_or_existing_pr() -> None:
    runner = h._make_runner()
    invalid = types.SimpleNamespace(
        number="5",
        quarantine_labels={"quarantine:workflow"},
    )

    asyncio.run(runner._rehydrate_quarantine_from_pr_labels(invalid))

    assert runner.state.quarantined_prs == set()

    runner.state.quarantined_prs.add(5)
    existing = PRInfo(
        number=5,
        branch="pr-001",
        quarantine_labels={"quarantine:workflow"},
    )

    asyncio.run(runner._rehydrate_quarantine_from_pr_labels(existing))

    assert runner.state.quarantined_prs == {5}


def test_handle_watch_without_current_pr_returns_to_idle() -> None:
    runner = h._make_runner()
    runner.state.state = PipelineState.WATCH

    asyncio.run(runner.handle_watch())

    assert runner.state.state == PipelineState.IDLE
    assert runner.state.history[-1]["event"] == "[WATCH] WATCH without current_pr -> IDLE."


def test_handle_watch_open_pr_lookup_failure_sets_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = h._make_runner()
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=5, branch="pr-001")

    def _raise(repo: str, **kwargs: Any) -> list[PRInfo]:
        raise RuntimeError("boom")

    monkeypatch.setattr(
        "src.github.prs.get_open_prs",
        _raise,
    )

    asyncio.run(runner.handle_watch())

    assert runner.state.state == PipelineState.ERROR
    assert runner.state.error_message == "get_open_prs failed: boom"
    assert runner.state.history[-1]["event"] == "[WATCH] boom."


def test_handle_watch_green_but_auto_merge_disabled_stays_watching(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pr = PRInfo(
        number=5,
        branch="pr-001",
        ci_status=CIStatus.SUCCESS,
        review_status=ReviewStatus.APPROVED,
        head_sha="green005",
        diff_scanned_at_sha="green005",
    )
    monkeypatch.setattr("src.github.prs.get_open_prs", lambda repo, **kw: [pr])

    merged: list[tuple[str, int]] = []
    monkeypatch.setattr(
        "src.github.prs.merge_pr",
        lambda repo, number, expected_head_sha: merged.append((repo, number)),
    )

    runner = h._make_runner(auto_merge=False)
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=5, branch="pr-001")
    runner.state.current_task = QueueTask(pr_id="PR-001", title="t", status=TaskStatus.DOING)
    runner._start_current_run_record("claude", "opus")
    asyncio.run(runner.handle_watch())

    assert merged == []
    assert runner.state.state == PipelineState.WATCH
    assert runner.state.current_pr is not None
    assert any("auto_merge disabled" in e["event"] for e in runner.state.history)


def test_handle_watch_changes_requested_triggers_fix(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    h._patch_subprocess(monkeypatch)
    pr = PRInfo(
        number=5,
        branch="pr-001",
        ci_status=CIStatus.SUCCESS,
        review_status=ReviewStatus.CHANGES_REQUESTED,
    )
    monkeypatch.setattr("src.github.prs.get_open_prs", lambda repo, **kw: [pr])
    monkeypatch.setattr(claude_cli, "fix_review_async", h._async_cli_result(0, "", ""))
    monkeypatch.setattr(
        "src.github.comments.post_comment",
        lambda repo, number, body: None,
    )

    runner = h._make_runner()
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=5, branch="pr-001")
    asyncio.run(runner.handle_watch())

    assert runner.state.state == PipelineState.WATCH
    assert runner.state.current_pr is not None
    assert runner.state.current_pr.push_count == 0
    assert any("Review feedback pending:" in e["event"] for e in runner.state.history)


def test_handle_watch_preserves_fix_iteration_count_for_same_pr(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pr = PRInfo(
        number=5,
        branch="pr-001",
        ci_status=CIStatus.PENDING,
        review_status=ReviewStatus.PENDING,
        last_activity=datetime.now(timezone.utc),
    )
    monkeypatch.setattr("src.github.prs.get_open_prs", lambda repo, **kw: [pr])

    runner = h._make_runner()
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(
        number=5,
        branch="pr-001",
        fix_iteration_count=7,
    )

    asyncio.run(runner.handle_watch())

    assert runner.state.current_pr is not None
    assert runner.state.current_pr.fix_iteration_count == 7


def test_handle_watch_preserves_no_push_fix_count_for_same_pr(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The no-push counter must survive the per-cycle PRInfo refresh from
    ``get_open_prs``; otherwise the deadlock circuit breaker can never
    accumulate past 1 across WATCH↔FIX cycles."""
    pr = PRInfo(
        number=9,
        branch="pr-009",
        ci_status=CIStatus.PENDING,
        review_status=ReviewStatus.PENDING,
        last_activity=datetime.now(timezone.utc),
    )
    monkeypatch.setattr("src.github.prs.get_open_prs", lambda repo, **kw: [pr])

    runner = h._make_runner()
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(
        number=9,
        branch="pr-009",
        no_push_fix_count=2,
    )

    asyncio.run(runner.handle_watch())

    assert runner.state.current_pr is not None
    assert runner.state.current_pr.no_push_fix_count == 2


def test_handle_watch_counts_new_head_sha_after_pre_pr195_upgrade(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Pre-PR-195 persisted ``current_pr`` (``push_count > 0`` with empty
    ``observed_head_shas``) must register a freshly polled head SHA as a
    new push. The earlier ``max(len(merged), push_count)`` formula left
    the legacy counter winning until the SHA set caught up, dropping the
    polled push.
    """
    polled = PRInfo(
        number=15,
        branch="pr-015",
        ci_status=CIStatus.PENDING,
        review_status=ReviewStatus.PENDING,
        last_activity=datetime.now(timezone.utc),
        observed_head_shas={"polled-head-sha"},
        push_count=1,
    )
    monkeypatch.setattr("src.github.prs.get_open_prs", lambda repo, **kw: [polled])

    runner = h._make_runner()
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(
        number=15,
        branch="pr-015",
        push_count=4,
        observed_head_shas=set(),
    )

    asyncio.run(runner.handle_watch())

    assert runner.state.current_pr is not None
    assert runner.state.current_pr.observed_head_shas == {"polled-head-sha"}
    assert runner.state.current_pr.push_count == 5


def test_watch_changes_requested_with_pending_ci_triggers_fix(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """PR-248: when CI is PENDING and review is CHANGES_REQUESTED with new
    Codex feedback, CHANGES_REQUESTED takes precedence over PENDING and a
    fix is triggered. Pre-PR-248 the PENDING branch swallowed this path."""
    last_push = datetime.now(timezone.utc) - timedelta(minutes=10)
    recent = datetime.now(timezone.utc) - timedelta(minutes=1)
    pr = PRInfo(
        number=42,
        branch="pr-001",
        head_sha="abc123",
        ci_status=CIStatus.PENDING,
        review_status=ReviewStatus.CHANGES_REQUESTED,
        last_activity=last_push,
    )
    monkeypatch.setattr(
        "src.github.prs.get_open_prs",
        lambda repo, **kw: [pr.model_copy(update={
            "review_status": gh_reviews.get_pr_review_status(
                repo, 42, "author", pr.head_sha, policy=kw["reviewer_policy"]
            )
        })],
    )

    comments = [
        {
            "user": {"id": 199175422, "login": "chatgpt-codex-connector"},
            "body": "P1: missing null check",
            "created_at": recent.strftime("%Y-%m-%dT%H:%M:%SZ"),
        }
    ]

    def fake_paginated(path: str) -> list[dict]:
        if "issues" in path:
            return comments
        return []

    monkeypatch.setattr(
        "src.github.cache._gh_api_paginated",
        fake_paginated,
    )

    fix_called: list[bool] = []

    async def fake_fix() -> None:
        fix_called.append(True)

    runner = h._make_runner()
    runner._last_push_at = last_push
    runner._last_push_at_pr_number = pr.number
    runner.state.current_pr = pr
    runner.state.state = PipelineState.WATCH
    runner.handle_fix = fake_fix  # type: ignore[assignment]
    gh_reviews.clear_review_status_cache()
    asyncio.run(runner.handle_watch())

    assert fix_called == [True]


def test_watch_changes_requested_with_pending_ci_no_feedback_logs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """PR-248: CI=PENDING + review=CHANGES_REQUESTED + no new Codex
    feedback must NOT be swallowed by the PENDING branch — the
    CHANGES_REQUESTED no-new-feedback log line must be emitted instead."""
    last_push = datetime.now(timezone.utc)
    pr = PRInfo(
        number=42,
        branch="pr-001",
        head_sha="abc123",
        ci_status=CIStatus.PENDING,
        review_status=ReviewStatus.CHANGES_REQUESTED,
        last_activity=last_push,
    )
    monkeypatch.setattr(
        "src.github.prs.get_open_prs",
        lambda repo, **kw: [pr.model_copy(update={
            "review_status": gh_reviews.get_pr_review_status(
                repo, 42, "author", pr.head_sha, policy=kw["reviewer_policy"]
            )
        })],
    )
    monkeypatch.setattr(
        "src.github.cache._gh_api_paginated",
        lambda path: [{
            "user": {"id": 199175422, "login": "chatgpt-codex-connector"},
            "body": "P1: earlier finding",
            "created_at": (last_push - timedelta(minutes=1)).isoformat(),
        }] if path.endswith("/issues/42/comments") else [],
    )

    fix_called: list[bool] = []

    async def fake_fix() -> None:
        fix_called.append(True)

    runner = h._make_runner()
    runner._last_push_at = last_push
    runner._last_push_at_pr_number = pr.number
    runner.state.current_pr = pr
    runner.state.state = PipelineState.WATCH
    runner.handle_fix = fake_fix  # type: ignore[assignment]
    gh_reviews.clear_review_status_cache()
    asyncio.run(runner.handle_watch())

    assert fix_called == []
    assert any(
        "CHANGES_REQUESTED but no new" in e["event"]
        for e in runner.state.history
    )


def test_handle_watch_fix_when_ci_success_and_changes_requested(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    h._patch_subprocess(monkeypatch)
    pr = PRInfo(
        number=5,
        branch="pr-001",
        ci_status=CIStatus.SUCCESS,
        review_status=ReviewStatus.CHANGES_REQUESTED,
    )
    monkeypatch.setattr("src.github.prs.get_open_prs", lambda repo, **kw: [pr])
    monkeypatch.setattr(claude_cli, "fix_review_async", h._async_cli_result(0, "", ""))
    monkeypatch.setattr(
        "src.github.comments.post_comment",
        lambda repo, number, body: None,
    )

    runner = h._make_runner()
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=5, branch="pr-001")
    asyncio.run(runner.handle_watch())

    assert runner.state.state == PipelineState.WATCH
    assert runner.state.current_pr is not None
    assert runner.state.current_pr.push_count == 0
    assert any("Review feedback pending:" in e["event"] for e in runner.state.history)


def test_handle_watch_ci_failure_triggers_fix(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    h._patch_subprocess(monkeypatch)
    _admit_ci_failure(monkeypatch)
    pr = PRInfo(
        number=5,
        branch="pr-001",
        ci_status=CIStatus.FAILURE,
        review_status=ReviewStatus.PENDING,
    )
    monkeypatch.setattr("src.github.prs.get_open_prs", lambda repo, **kw: [pr])
    monkeypatch.setattr(claude_cli, "fix_review_async", h._async_cli_result(0, "", ""))
    monkeypatch.setattr(
        "src.github.comments.post_comment",
        lambda repo, number, body: None,
    )

    runner = h._make_runner()
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=5, branch="pr-001")
    asyncio.run(runner.handle_watch())

    assert runner.state.state == PipelineState.WATCH
    assert runner.state.current_pr is not None
    assert runner.state.current_pr.push_count == 1


def test_handle_watch_overdue_pending_waits_for_terminal_evidence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A long pending interval reports overdue CI and keeps WATCH active.

    The PR's canonical CI status is PENDING even though its raw runs look
    successful (for example, because producer identity is missing), and the
    Redis tracker shows the
    same head_sha has been PENDING longer than ``ci_pending_max_min``
    minutes. It cannot become a code failure solely through elapsed time.
    """
    from src.github import checks as gh_checks

    h._patch_subprocess(monkeypatch)
    pr = PRInfo(
        number=42,
        branch="pr-042",
        ci_status=CIStatus.PENDING,
        review_status=ReviewStatus.PENDING,
        head_sha="c" * 40,
    )
    monkeypatch.setattr("src.github.prs.get_open_prs", lambda repo, **kw: [pr])
    monkeypatch.setattr(
        "src.github.checks._retrieve_ci_status_evidence",
        lambda repo, sha: _ci_retrieval(
            repo,
            sha,
            [
                {
                    "id": 1,
                    "name": "unit",
                    "conclusion": "success",
                    "head_sha": sha,
                }
            ],
        ),
    )

    fix_calls: list[None] = []

    async def fake_handle_fix(self: Any) -> None:
        fix_calls.append(None)

    monkeypatch.setattr(runner_module.PipelineRunner, "handle_fix", fake_handle_fix)
    monkeypatch.setattr(
        "src.github.comments.post_comment",
        lambda repo, number, body: None,
    )

    runner = h._make_runner()
    runner.app_config.daemon.ci_pending_max_min = 30
    fixed_first_seen = 1_700_000_000.0
    key = gh_checks._pending_tracker_key(runner.owner_repo, 42, "c" * 40)
    asyncio.run(runner.redis.set(key, str(fixed_first_seen)))
    monkeypatch.setattr(
        gh_checks.time, "time", lambda: fixed_first_seen + 60 * 60
    )

    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=42, branch="pr-042")
    asyncio.run(runner.handle_watch())

    assert fix_calls == []
    assert runner.state.current_pr is not None
    assert runner.state.current_pr.ci_status == CIStatus.PENDING
    history_events = " ".join(entry.get("event", "") for entry in runner.state.history)
    assert "CI still pending after 30min" in history_events


def test_long_ci_wait_reaches_current_head_merge_after_real_checks_pass(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.github import checks as gh_checks

    h._patch_subprocess(monkeypatch)
    runner = h._make_runner()
    runner.repo_config.required_checks = ["unit", "integration"]
    runner.repo_config.auto_merge = True
    sha = "a" * 40
    integration_state = {"status": "in_progress", "conclusion": None}

    def read_ci(repo: str, head: str) -> gh_checks._CiRetrieval:
        runs = [
            {"id": 1, "name": "unit", "head_sha": sha, "app": {"id": 1},
             "status": "completed", "conclusion": "success"},
            {"id": 2, "name": "integration", "head_sha": sha, "app": {"id": 1},
             **integration_state},
        ]
        return gh_checks._make_ci_retrieval(
            repo, sha, 0.0, runs,
            {"sha": sha, "state": "pending", "statuses": []},
            gh_checks._CiSourceResult(True, False), gh_checks._CiSourceResult(True, True),
        )

    monkeypatch.setattr(gh_checks, "_retrieve_ci_status_evidence", read_ci)

    def open_prs(repo: str, **kwargs: object) -> list[PRInfo]:
        ci = gh_checks._classify_ci_retrieval(
            read_ci(repo, sha), required_contexts=runner.repo_config.required_checks,
        )
        return [PRInfo(
            number=42, branch="pr-042", head_sha=sha, ci_status=ci,
            review_status=ReviewStatus.APPROVED, diff_scanned_at_sha=sha,
        )]

    monkeypatch.setattr(watch_module.gh_prs, "get_open_prs", open_prs)
    monkeypatch.setattr(watch_module.gh_prs, "get_pr_state", lambda *a: "OPEN")
    async def scanned() -> None:
        runner.state.current_pr.diff_scanned_at_sha = sha

    monkeypatch.setattr(runner, "_scan_pr_diff_once", scanned)
    merge = AsyncMock()
    fix = AsyncMock()
    monkeypatch.setattr(runner, "handle_merge", merge)
    monkeypatch.setattr(runner, "handle_fix", fix)
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=42, branch="pr-042", head_sha=sha)
    key = gh_checks._pending_tracker_key(runner.owner_repo, 42, sha)
    asyncio.run(runner.redis.set(key, str(time.time() - 60 * 60)))

    asyncio.run(runner.handle_watch())
    assert runner.state.state == PipelineState.WATCH
    fix.assert_not_awaited()
    merge.assert_not_awaited()

    asyncio.run(runner.handle_watch())
    assert sum(
        "CI still pending after 30min" in entry.get("event", "")
        for entry in runner.state.history
    ) == 1

    integration_state.update(status="completed", conclusion="success")
    asyncio.run(runner.handle_watch())
    fix.assert_not_awaited()
    assert merge.await_count == 1, [entry.get("event") for entry in runner.state.history]


def test_handle_watch_pending_within_threshold_does_not_reclassify(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """PR-250: PENDING freshness short of ``ci_pending_max_min`` stays PENDING."""
    from src.github import checks as gh_checks

    h._patch_subprocess(monkeypatch)
    pr = PRInfo(
        number=43,
        branch="pr-043",
        ci_status=CIStatus.PENDING,
        review_status=ReviewStatus.PENDING,
        head_sha="d" * 40,
    )
    monkeypatch.setattr("src.github.prs.get_open_prs", lambda repo, **kw: [pr])
    monkeypatch.setattr(
        "src.github.checks._retrieve_ci_status_evidence",
        lambda repo, sha: _ci_retrieval(
            repo,
            sha,
            [
                {
                    "id": 1,
                    "name": "unit",
                    "status": "in_progress",
                    "head_sha": sha,
                    "app": {"id": 1},
                }
            ],
        ),
    )

    runner = h._make_runner()
    runner.app_config.daemon.ci_pending_max_min = 30
    fixed_first_seen = 1_700_000_000.0
    key = gh_checks._pending_tracker_key(runner.owner_repo, 43, "d" * 40)
    asyncio.run(runner.redis.set(key, str(fixed_first_seen)))
    # 5 min elapsed, threshold is 30 — wrapper returns PENDING with no reason.
    monkeypatch.setattr(
        gh_checks.time, "time", lambda: fixed_first_seen + 5 * 60
    )

    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=43, branch="pr-043")
    asyncio.run(runner.handle_watch())

    assert runner.state.current_pr is not None
    assert runner.state.current_pr.ci_status == CIStatus.PENDING
    history_events = " ".join(entry.get("event", "") for entry in runner.state.history)
    assert "stuck_pending" not in history_events
    assert "reclassified" not in history_events


def test_fresh_success_clears_stale_pending_age(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A fresh canonical SUCCESS must not age a stale hydrated PENDING."""
    from src.github import checks as gh_checks

    head_sha = "e" * 40
    found = PRInfo(number=44, branch="pr-044", head_sha=head_sha)
    monkeypatch.setattr(
        gh_checks,
        "_retrieve_ci_status_evidence",
        lambda repo, sha: _ci_retrieval(
            repo,
            sha,
            [
                {
                    "id": 1,
                    "name": "unit",
                    "conclusion": "success",
                    "head_sha": sha,
                    "app": {"id": 1},
                }
            ],
        ),
    )

    runner = h._make_runner()
    runner.app_config.daemon.ci_pending_max_min = 30
    key = gh_checks._pending_tracker_key(runner.owner_repo, found.number, head_sha)
    asyncio.run(runner.redis.set(key, "1700000000.0"))
    monkeypatch.setattr(gh_checks.time, "time", lambda: 1_700_003_600.0)

    asyncio.run(runner._maybe_reclassify_stuck_pending(found))

    assert found.ci_status == CIStatus.PENDING
    assert asyncio.run(runner.redis.get(key)) is None
    history = " ".join(entry.get("event", "") for entry in runner.state.history)
    assert "stuck_pending" not in history












def test_handle_watch_within_timeout_stays_watching(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fresh = datetime.now(timezone.utc) - timedelta(minutes=2)
    pr = PRInfo(
        number=5,
        branch="pr-001",
        ci_status=CIStatus.PENDING,
        review_status=ReviewStatus.EYES,
        last_activity=fresh,
    )
    monkeypatch.setattr("src.github.prs.get_open_prs", lambda repo, **kw: [pr])

    runner = h._make_runner(review_timeout_min=30)
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=5, branch="pr-001")
    asyncio.run(runner.handle_watch())

    assert runner.state.state == PipelineState.WATCH








def test_handle_watch_repo_timeout_override_wins_over_daemon_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An explicit per-repo ``review_timeout_min`` must override the
    daemon-level default.

    Belt-and-suspenders for the P2 fix: raising the daemon default must
    not silently shorten or lengthen the timeout on repos that pinned
    their own value via the existing per-repo Settings control (PR-015).
    """
    stale = datetime.now(timezone.utc) - timedelta(minutes=90)
    pr = PRInfo(
        number=8,
        branch="pr-003",
        ci_status=CIStatus.PENDING,
        review_status=ReviewStatus.EYES,
        last_activity=stale,
    )
    monkeypatch.setattr("src.github.prs.get_open_prs", lambda repo, **kw: [pr])

    # repo pins 120 min, daemon default is 30 min. 90 minutes of
    # inactivity is below the repo override, so the PR stays WATCH.
    repo_cfg = RepoConfig(
        url="https://github.com/octo/demo.git",
        branch="main",
        auto_merge=True,
        review_timeout_min=120,
        poll_interval_sec=60,
    )
    app_cfg = AppConfig(repositories=[], daemon=DaemonConfig(review_timeout_min=30))
    runner = PipelineRunner(
        repo_cfg,
        app_cfg,
        h._FakeRedis(),
        *h._usage_providers(),
    )
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=8, branch="pr-003")
    asyncio.run(runner.handle_watch())

    assert runner.state.state == PipelineState.WATCH




def test_handle_watch_captures_success_merged_on_external_merge(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("src.github.prs.get_open_prs", lambda repo, **kw: [])
    monkeypatch.setattr("src.github.prs.is_pr_merged", lambda repo, number: True)

    runner = h._make_runner()
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=5, branch="pr-001")
    runner.state.current_task = QueueTask(pr_id="PR-001", title="t", status=TaskStatus.DOING)
    runner._start_current_run_record("claude", "opus")
    asyncio.run(runner.handle_watch())

    assert runner.state.state == PipelineState.IDLE
    assert runner.state.current_pr is None
    assert runner.state.current_task is None
    recent = asyncio.run(
        runner._metrics_store.recent(
            task_id="PR-001",
            limit=1,
            repo_name=runner.name,
        )
    )
    assert len(recent) == 1
    assert recent[0].exit_reason == "success_merged"


def test_external_merge_retries_failed_done_status_before_releasing_task(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("src.github.prs.get_open_prs", lambda *_args, **_kwargs: [])
    monkeypatch.setattr("src.github.prs.get_pr_state", lambda *_args, **_kwargs: "MERGED")
    runner = h._make_runner()
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=5, branch="pr-001", pr_id="PR-001")
    runner.state.current_task = QueueTask(
        pr_id="PR-001", title="t", status=TaskStatus.ERROR,
        branch="pr-001", task_file="tasks/PR-001.md",
    )
    write = AsyncMock(side_effect=[False, True])
    monkeypatch.setattr(runner, "_commit_task_status_change", write)
    saved = AsyncMock()
    monkeypatch.setattr(runner, "_save_current_run_record", saved)
    asyncio.run(runner.handle_watch())
    assert runner.state.state == PipelineState.WATCH
    assert runner.state.current_task.pr_id == "PR-001"
    saved.assert_not_awaited()
    asyncio.run(runner.handle_watch())
    assert write.await_count == 2
    assert runner.state.state == PipelineState.IDLE
    assert runner.state.current_task is None
    saved.assert_awaited_once()


def test_handle_watch_captures_closed_unmerged(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("src.github.prs.get_open_prs", lambda repo, **kw: [])
    monkeypatch.setattr("src.github.prs.is_pr_merged", lambda repo, number: False)

    runner = h._make_runner()
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=5, branch="pr-001")
    runner.state.current_task = QueueTask(pr_id="PR-001", title="t", status=TaskStatus.DOING)
    runner._start_current_run_record("claude", "opus")

    asyncio.run(runner.handle_watch())

    recent = asyncio.run(
        runner._metrics_store.recent(
            task_id="PR-001",
            limit=1,
            repo_name=runner.name,
        )
    )

    assert runner.state.state == PipelineState.IDLE
    assert len(recent) == 1
    assert recent[0].exit_reason == "closed_unmerged"


def test_handle_watch_unknown_merge_state_logs_but_does_not_save(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("src.github.prs.get_open_prs", lambda repo, **kw: [])
    monkeypatch.setattr("src.github.prs.is_pr_merged", lambda repo, number: None)

    runner = h._make_runner()
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=5, branch="pr-001")
    runner.state.current_task = QueueTask(pr_id="PR-001", title="t", status=TaskStatus.DOING)
    runner._start_current_run_record("claude", "opus")

    asyncio.run(runner.handle_watch())

    recent = asyncio.run(
        runner._metrics_store.recent(
            task_id="PR-001",
            limit=1,
            repo_name=runner.name,
        )
    )

    assert runner.state.state == PipelineState.IDLE
    assert recent == []
    assert any("state unknown" in entry["event"] for entry in runner.state.history)


def test_handle_watch_skips_fix_no_new_feedback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """CHANGES_REQUESTED with no new Codex P1/P2 after last push must not
    trigger handle_fix — the stale review is waiting for a fresh pass."""
    last_push = datetime.now(timezone.utc)
    pr = PRInfo(
        number=42,
        branch="pr-001",
        ci_status=CIStatus.SUCCESS,
        review_status=ReviewStatus.CHANGES_REQUESTED,
        last_activity=last_push,
    )
    monkeypatch.setattr(
        "src.github.prs.get_open_prs",
        lambda repo, **kw: [pr],
    )
    # No Codex comments at all -> no new feedback.
    monkeypatch.setattr(
        "src.github.cache._gh_api_paginated",
        lambda path: [],
    )
    fix_called: list[bool] = []

    async def fake_fix() -> None:
        fix_called.append(True)

    runner = h._make_runner()
    runner._last_push_at = last_push
    runner._last_push_at_pr_number = pr.number
    runner.state.current_pr = pr
    runner.state.state = PipelineState.WATCH
    runner.handle_fix = fake_fix  # type: ignore[assignment]
    asyncio.run(runner.handle_watch())

    assert fix_called == []
    assert any("no new Codex feedback" in e["event"] for e in runner.state.history)


def test_handle_watch_triggers_fix_new_feedback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """CHANGES_REQUESTED with fresh P1 feedback after last push triggers fix."""
    last_push = datetime.now(timezone.utc) - timedelta(minutes=10)
    recent = datetime.now(timezone.utc) - timedelta(minutes=1)
    pr = PRInfo(
        number=42,
        branch="pr-001",
        ci_status=CIStatus.SUCCESS,
        review_status=ReviewStatus.CHANGES_REQUESTED,
        last_activity=last_push,
    )
    monkeypatch.setattr(
        "src.github.prs.get_open_prs",
        lambda repo, **kw: [pr],
    )

    comments = [
        {
            "user": {"id": 199175422, "login": "chatgpt-codex-connector"},
            "body": "P1: missing null check",
            "created_at": recent.strftime("%Y-%m-%dT%H:%M:%SZ"),
        }
    ]

    def fake_paginated(path: str) -> list[dict]:
        if "issues" in path:
            return comments
        return []

    monkeypatch.setattr(
        "src.github.cache._gh_api_paginated",
        fake_paginated,
    )
    fix_called: list[bool] = []

    async def fake_fix() -> None:
        fix_called.append(True)

    runner = h._make_runner()
    runner._last_push_at = last_push
    runner._last_push_at_pr_number = pr.number
    runner.state.current_pr = pr
    runner.state.state = PipelineState.WATCH
    runner.handle_fix = fake_fix  # type: ignore[assignment]
    asyncio.run(runner.handle_watch())

    assert fix_called == [True]


def test_handle_watch_triggers_fix_for_trusted_reviewer_without_codex_login(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Fresh feedback from a configured reviewer uses stable user IDs."""
    last_push = datetime.now(timezone.utc) - timedelta(minutes=10)
    recent = datetime.now(timezone.utc) - timedelta(minutes=1)
    pr = PRInfo(
        number=42,
        branch="pr-001",
        ci_status=CIStatus.SUCCESS,
        review_status=ReviewStatus.CHANGES_REQUESTED,
        last_activity=last_push,
    )
    monkeypatch.setattr(
        "src.github.prs.get_open_prs",
        lambda repo, **kw: [pr],
    )

    comments = [
        {
            "user": {"id": 123456789, "login": "trusted-reviewer[bot]"},
            "body": "P1: missing null check",
            "created_at": recent.strftime("%Y-%m-%dT%H:%M:%SZ"),
        }
    ]

    def fake_paginated(path: str) -> list[dict]:
        if "issues" in path:
            return comments
        return []

    monkeypatch.setattr(
        "src.github.cache._gh_api_paginated",
        fake_paginated,
    )
    fix_called: list[bool] = []

    async def fake_fix() -> None:
        fix_called.append(True)

    runner = h._make_runner()
    runner.app_config.daemon.trusted_reviewer_identities = [
        TrustedReviewerIdentity(user_id=123456789, login="trusted-reviewer[bot]")
    ]
    monkeypatch.setattr(watch_module, "load_config", lambda: runner.app_config)
    runner._last_push_at = last_push
    runner._last_push_at_pr_number = pr.number
    runner.state.current_pr = pr
    runner.state.state = PipelineState.WATCH
    runner.handle_fix = fake_fix  # type: ignore[assignment]
    asyncio.run(runner.handle_watch())

    assert fix_called == [True]


def test_handle_watch_still_fixes_ci_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """CI failure must still trigger fix regardless of Codex feedback state."""
    pr = PRInfo(
        number=42,
        branch="pr-001",
        ci_status=CIStatus.FAILURE,
        review_status=ReviewStatus.PENDING,
        last_activity=datetime.now(timezone.utc),
    )
    monkeypatch.setattr(
        "src.github.prs.get_open_prs",
        lambda repo, **kw: [pr],
    )
    fix_called: list[bool] = []

    async def fake_fix() -> None:
        fix_called.append(True)

    runner = h._make_runner()
    runner.state.current_pr = pr
    runner.state.state = PipelineState.WATCH
    runner.handle_fix = fake_fix  # type: ignore[assignment]
    asyncio.run(runner.handle_watch())

    assert fix_called == [True]










def test_handle_watch_does_not_retrigger_recent_changes_requested_review(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = datetime.now(timezone.utc)
    pr = PRInfo(
        number=42,
        branch="pr-042-fix",
        ci_status=CIStatus.SUCCESS,
        review_status=ReviewStatus.CHANGES_REQUESTED,
        last_activity=now,
    )
    monkeypatch.setattr(
        "src.github.prs.get_open_prs",
        lambda repo, **kw: [pr],
    )
    monkeypatch.setattr(
        "src.github.cache._gh_api_paginated",
        lambda path: [],
    )
    monkeypatch.setattr(
        "src.github.prs.get_last_push_age_seconds",
        lambda repo, number: 5 * 60,
    )
    retriggers: list[int] = []

    runner = h._make_runner()
    runner.state.current_pr = pr
    runner.state.state = PipelineState.WATCH
    runner._last_push_at = now - timedelta(minutes=5)
    runner._last_push_at_pr_number = pr.number
    runner._post_codex_review = retriggers.append  # type: ignore[assignment]

    asyncio.run(runner.handle_watch())

    assert retriggers == []
    assert runner.state.last_stale_retrigger_at is None


def test_handle_watch_does_not_retrigger_within_debounce_window(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = datetime(2026, 4, 21, 12, 0, tzinfo=timezone.utc)
    pr = PRInfo(
        number=42,
        branch="pr-042-fix",
        ci_status=CIStatus.SUCCESS,
        review_status=ReviewStatus.CHANGES_REQUESTED,
        last_activity=now,
    )

    class _FrozenDateTime(datetime):
        @classmethod
        def now(cls, tz: timezone | None = None) -> datetime:
            return now if tz is None else now.astimezone(tz)

    monkeypatch.setattr(watch_module, "datetime", _FrozenDateTime)
    monkeypatch.setattr(
        "src.github.prs.get_open_prs",
        lambda repo, **kw: [pr],
    )
    monkeypatch.setattr(
        "src.github.cache._gh_api_paginated",
        lambda path: [],
    )
    monkeypatch.setattr(
        "src.github.prs.get_last_push_age_seconds",
        lambda repo, number: 60 * 60,
    )
    retriggers: list[int] = []

    runner = h._make_runner()
    runner.state.current_pr = pr
    runner.state.state = PipelineState.WATCH
    runner.state.last_stale_retrigger_at = now - timedelta(minutes=30)
    runner._last_push_at = now - timedelta(hours=1)
    runner._last_push_at_pr_number = pr.number
    runner._post_codex_review = retriggers.append  # type: ignore[assignment]

    asyncio.run(runner.handle_watch())

    assert retriggers == []
    assert runner.state.last_stale_retrigger_at == now - timedelta(minutes=30)


def test_handle_watch_normalizes_naive_stale_retrigger_timestamps(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = datetime(2026, 4, 21, 12, 0, tzinfo=timezone.utc)
    pr = PRInfo(
        number=42,
        branch="pr-042-fix",
        ci_status=CIStatus.SUCCESS,
        review_status=ReviewStatus.CHANGES_REQUESTED,
        last_activity=now,
    )

    class _FrozenDateTime(datetime):
        @classmethod
        def now(cls, tz: timezone | None = None) -> datetime:
            return now if tz is None else now.astimezone(tz)

    monkeypatch.setattr(watch_module, "datetime", _FrozenDateTime)
    monkeypatch.setattr(
        "src.github.prs.get_open_prs",
        lambda repo, **kw: [pr],
    )
    monkeypatch.setattr(
        "src.github.cache._gh_api_paginated",
        lambda path: [],
    )
    monkeypatch.setattr(
        "src.github.prs.get_last_push_age_seconds",
        lambda repo, number: 60 * 60,
    )
    retriggers: list[int] = []

    runner = h._make_runner()
    runner.state.current_pr = pr
    runner.state.state = PipelineState.WATCH
    runner.state.last_stale_retrigger_at = datetime(2026, 4, 21, 11, 30)
    runner._last_push_at = now - timedelta(hours=1)
    runner._last_push_at_pr_number = pr.number
    runner._post_codex_review = retriggers.append  # type: ignore[assignment]

    asyncio.run(runner.handle_watch())

    assert retriggers == []




def test_handle_watch_does_not_retrigger_eyes_below_threshold(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """EYES younger than stale_review_threshold_eyes_min must not retrigger."""
    now = datetime(2026, 4, 21, 12, 0, tzinfo=timezone.utc)
    pr = PRInfo(
        number=42,
        branch="pr-042-fix",
        ci_status=CIStatus.PENDING,
        review_status=ReviewStatus.EYES,
        last_activity=now,
    )

    class _FrozenDateTime(datetime):
        @classmethod
        def now(cls, tz: timezone | None = None) -> datetime:
            return now if tz is None else now.astimezone(tz)

    monkeypatch.setattr(watch_module, "datetime", _FrozenDateTime)
    monkeypatch.setattr(
        "src.github.prs.get_open_prs",
        lambda repo, **kw: [pr],
    )
    monkeypatch.setattr(
        "src.github.cache._gh_api_paginated",
        lambda path: [],
    )
    monkeypatch.setattr(
        "src.github.prs.get_last_push_age_seconds",
        lambda repo, number: 2 * 60,
    )

    retriggers: list[int] = []
    runner = h._make_runner(review_timeout_min=120)
    runner.state.current_pr = pr
    runner.state.state = PipelineState.WATCH
    runner._last_push_at = now - timedelta(minutes=2)
    runner._last_push_at_pr_number = pr.number
    runner._post_codex_review = retriggers.append  # type: ignore[assignment]

    asyncio.run(runner.handle_watch())

    assert retriggers == []
    assert runner.state.last_stale_retrigger_at is None


def test_handle_watch_does_not_retrigger_changes_requested_below_default_threshold(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """CHANGES_REQUESTED below the default 10-minute threshold must not retrigger.

    Confirms EYES's shorter 5-minute threshold is not applied to
    CHANGES_REQUESTED — a push 6 minutes old is past the EYES threshold
    but still inside the CHANGES_REQUESTED threshold, so no retrigger.
    """
    now = datetime(2026, 4, 21, 12, 0, tzinfo=timezone.utc)
    pr = PRInfo(
        number=42,
        branch="pr-042-fix",
        ci_status=CIStatus.SUCCESS,
        review_status=ReviewStatus.CHANGES_REQUESTED,
        last_activity=now,
    )

    class _FrozenDateTime(datetime):
        @classmethod
        def now(cls, tz: timezone | None = None) -> datetime:
            return now if tz is None else now.astimezone(tz)

    monkeypatch.setattr(watch_module, "datetime", _FrozenDateTime)
    monkeypatch.setattr(
        "src.github.prs.get_open_prs",
        lambda repo, **kw: [pr],
    )
    monkeypatch.setattr(
        "src.github.cache._gh_api_paginated",
        lambda path: [],
    )
    monkeypatch.setattr(
        "src.github.prs.get_last_push_age_seconds",
        lambda repo, number: 6 * 60,
    )

    retriggers: list[int] = []
    runner = h._make_runner(review_timeout_min=120)
    runner.state.current_pr = pr
    runner.state.state = PipelineState.WATCH
    runner._last_push_at = now - timedelta(minutes=6)
    runner._last_push_at_pr_number = pr.number
    runner._post_codex_review = retriggers.append  # type: ignore[assignment]

    asyncio.run(runner.handle_watch())

    assert retriggers == []
    assert runner.state.last_stale_retrigger_at is None


@pytest.mark.parametrize(
    "review_status",
    [ReviewStatus.APPROVED, ReviewStatus.PENDING],
)
def test_handle_watch_does_not_retrigger_for_non_changes_requested_status(
    monkeypatch: pytest.MonkeyPatch,
    review_status: ReviewStatus,
) -> None:
    now = datetime.now(timezone.utc)
    pr = PRInfo(
        number=42,
        branch="pr-042-fix",
        ci_status=CIStatus.SUCCESS,
        review_status=review_status,
        last_activity=now,
    )
    monkeypatch.setattr(
        "src.github.prs.get_open_prs",
        lambda repo, **kw: [pr],
    )
    retriggers: list[int] = []

    runner = h._make_runner()
    runner.state.current_pr = pr
    runner.state.state = PipelineState.WATCH
    runner._post_codex_review = retriggers.append  # type: ignore[assignment]

    asyncio.run(runner.handle_watch())

    assert retriggers == []


def test_handle_watch_allows_pending_review_only_when_repo_bypasses_review(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pr = PRInfo(
        number=42,
        branch="pr-042-test",
        ci_status=CIStatus.SUCCESS,
        review_status=ReviewStatus.PENDING,
        last_activity=datetime.now(timezone.utc),
        head_sha="green042",
        diff_scanned_at_sha="green042",
    )
    monkeypatch.setattr(
        "src.github.prs.get_open_prs",
        lambda repo, **kw: [pr],
    )
    merged: list[int] = []

    async def fake_handle_merge() -> None:
        merged.append(42)

    runner = h._make_runner(allow_merge_without_review=True)
    runner.state.current_pr = pr
    runner.state.state = PipelineState.WATCH
    runner.handle_merge = fake_handle_merge  # type: ignore[method-assign]

    asyncio.run(runner.handle_watch())

    assert merged == [42]


def test_handle_watch_does_not_bypass_changes_requested_review(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pr = PRInfo(
        number=42,
        branch="pr-042-test",
        ci_status=CIStatus.SUCCESS,
        review_status=ReviewStatus.CHANGES_REQUESTED,
        last_activity=datetime.now(timezone.utc),
    )
    monkeypatch.setattr(
        "src.github.prs.get_open_prs",
        lambda repo, **kw: [pr],
    )
    monkeypatch.setattr(
        "src.github.cache._gh_api_paginated",
        lambda path: [],
    )
    merged: list[int] = []

    async def fake_handle_merge() -> None:
        merged.append(42)

    runner = h._make_runner(allow_merge_without_review=True)
    runner.state.current_pr = pr
    runner.state.state = PipelineState.WATCH
    runner._last_push_at = datetime.now(timezone.utc) - timedelta(minutes=1)
    runner._last_push_at_pr_number = pr.number
    runner.handle_merge = fake_handle_merge  # type: ignore[method-assign]

    asyncio.run(runner.handle_watch())

    assert merged == []
    assert runner.state.state == PipelineState.WATCH


def test_handle_watch_retries_rehydrate_last_push_at(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """If recover_state's rehydrate failed (e.g. transient API hiccup),
    handle_watch must retry so a stuck-None last_push_at doesn't stale-fix
    loop forever."""
    pr = PRInfo(
        number=42,
        branch="pr-001",
        head_sha="a" * 40,
        ci_status=CIStatus.SUCCESS,
        review_status=ReviewStatus.PENDING,
        last_activity=datetime.now(timezone.utc),
    )
    monkeypatch.setattr(
        "src.github.prs.get_open_prs",
        lambda repo, **kw: [pr],
    )
    _publication_transport(monkeypatch, pr, "2026-04-14T18:00:00Z")

    runner = h._make_runner()
    runner.state.current_pr = pr
    runner.state.state = PipelineState.WATCH
    assert runner._last_push_at is None
    async def no_reclassify(_pr: PRInfo) -> None:
        return None
    monkeypatch.setattr(runner, "_maybe_reclassify_stuck_pending", no_reclassify)
    asyncio.run(runner.handle_watch())
    assert runner._last_push_at is not None
    assert runner._last_push_at.isoformat() == "2026-04-14T18:00:00+00:00"






def test_handle_watch_rehydrates_on_pr_number_mismatch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """If handle_watch is tracking a different PR than the last-push
    number, rehydrate must fire even when _last_push_at is non-None —
    otherwise a transient rehydrate failure on the prior PR switch
    would keep the stale previous-PR timestamp forever."""
    head_iso = "2026-04-14T18:00:00Z"
    pr = PRInfo(
        number=55,
        branch="pr-new",
        head_sha="a" * 40,
        ci_status=CIStatus.SUCCESS,
        review_status=ReviewStatus.PENDING,
        last_activity=datetime.now(timezone.utc),
    )
    monkeypatch.setattr(
        "src.github.prs.get_open_prs",
        lambda repo, **kw: [pr],
    )
    _publication_transport(monkeypatch, pr, head_iso)

    runner = h._make_runner()
    # Stale last_push_at from a previously-tracked PR (different number).
    runner._last_push_at = datetime(2026, 4, 20, tzinfo=timezone.utc)
    runner._last_push_at_pr_number = 999
    runner.state.current_pr = pr
    runner.state.state = PipelineState.WATCH
    async def no_reclassify(_pr: PRInfo) -> None:
        return None
    monkeypatch.setattr(runner, "_maybe_reclassify_stuck_pending", no_reclassify)
    asyncio.run(runner.handle_watch())

    assert runner._last_push_at_pr_number == 55
    assert runner._last_push_at is not None
    assert runner._last_push_at.isoformat() == "2026-04-14T18:00:00+00:00"


def test_handle_watch_stays_in_watch_on_unknown_feedback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """CHANGES_REQUESTED + UNKNOWN feedback check -> stay in WATCH, no FIX."""
    last_push = datetime.now(timezone.utc)
    pr = PRInfo(
        number=42,
        branch="pr-001",
        ci_status=CIStatus.SUCCESS,
        review_status=ReviewStatus.CHANGES_REQUESTED,
        last_activity=last_push,
    )
    monkeypatch.setattr(
        "src.github.prs.get_open_prs",
        lambda repo, **kw: [pr],
    )

    def _raise(path: str) -> list:
        raise RuntimeError("GitHub API unavailable")

    monkeypatch.setattr(
        "src.github.cache._gh_api_paginated",
        _raise,
    )
    fix_called: list[bool] = []

    async def fake_fix() -> None:
        fix_called.append(True)

    runner = h._make_runner()
    runner._last_push_at = last_push
    runner._last_push_at_pr_number = pr.number
    runner.state.current_pr = pr
    runner.state.state = PipelineState.WATCH
    runner.handle_fix = fake_fix  # type: ignore[assignment]
    asyncio.run(runner.handle_watch())

    assert fix_called == []
    assert runner.state.state == PipelineState.WATCH
    assert any("feedback check failed" in e["event"] for e in runner.state.history)


def test_handle_watch_skips_hung_timeout_on_unknown(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """CHANGES_REQUESTED + UNKNOWN + elapsed > timeout_min -> stay WATCH, not HUNG.

    When the observation itself is unreliable we cannot trust the elapsed
    time either. The runner must stay in WATCH and retry next cycle.
    """
    last_push = datetime.now(timezone.utc) - timedelta(hours=2)
    pr = PRInfo(
        number=42,
        branch="pr-001",
        ci_status=CIStatus.SUCCESS,
        review_status=ReviewStatus.CHANGES_REQUESTED,
        last_activity=last_push,
    )
    monkeypatch.setattr(
        "src.github.prs.get_open_prs",
        lambda repo, **kw: [pr],
    )

    def _raise(path: str) -> list:
        raise RuntimeError("GitHub API unavailable")

    monkeypatch.setattr(
        "src.github.cache._gh_api_paginated",
        _raise,
    )
    fix_called: list[bool] = []

    async def fake_fix() -> None:
        fix_called.append(True)

    runner = h._make_runner(review_timeout_min=30)
    runner._last_push_at = last_push
    runner._last_push_at_pr_number = pr.number
    runner.state.current_pr = pr
    runner.state.state = PipelineState.WATCH
    runner.handle_fix = fake_fix  # type: ignore[assignment]
    asyncio.run(runner.handle_watch())

    assert fix_called == []
    assert runner.state.state == PipelineState.WATCH
    assert any("feedback check failed" in e["event"] for e in runner.state.history)




def test_handle_watch_does_not_retrigger_on_non_matching_comment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pr = h._codex_bot_pr()
    monkeypatch.setattr(
        "src.github.prs.get_open_prs",
        lambda repo, **kw: [pr],
    )
    monkeypatch.setattr(
        "src.github.cache._gh_api_paginated",
        lambda path: [
            h._codex_bot_error_comment(body="LGTM, all clear from Codex"),
        ],
    )

    posted: list[int] = []

    def fake_post(
        number: int,
        *,
        bypass_same_head_dedup: bool = False,
    ) -> tuple[bool, bool, datetime | None]:
        posted.append(number)
        return True, True, None

    runner = h._make_runner()
    runner.state.current_pr = pr
    runner.state.state = PipelineState.WATCH
    runner._post_codex_review_result = fake_post  # type: ignore[assignment]

    asyncio.run(runner.handle_watch())

    assert posted == []
    assert runner.state.last_codex_retrigger_at is None


def test_handle_watch_does_not_retrigger_on_non_bot_author(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pr = h._codex_bot_pr()
    monkeypatch.setattr(
        "src.github.prs.get_open_prs",
        lambda repo, **kw: [pr],
    )
    monkeypatch.setattr(
        "src.github.cache._gh_api_paginated",
        lambda path: [
            h._codex_bot_error_comment(user="some-human"),
        ],
    )

    posted: list[int] = []

    def fake_post(
        number: int,
        *,
        bypass_same_head_dedup: bool = False,
    ) -> tuple[bool, bool, datetime | None]:
        posted.append(number)
        return True, True, None

    runner = h._make_runner()
    runner.state.current_pr = pr
    runner.state.state = PipelineState.WATCH
    runner._post_codex_review_result = fake_post  # type: ignore[assignment]

    asyncio.run(runner.handle_watch())

    assert posted == []
    assert runner.state.last_codex_retrigger_at is None


def test_handle_watch_codex_bot_error_cooldown_blocks_rapid_retriggers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = datetime(2026, 4, 30, 12, 0, tzinfo=timezone.utc)
    pr = h._codex_bot_pr()
    monkeypatch.setattr(
        "src.github.prs.get_open_prs",
        lambda repo, **kw: [pr],
    )
    monkeypatch.setattr(
        "src.github.cache._gh_api_paginated",
        lambda path: [
            h._codex_bot_error_comment(
                created_at=now.strftime("%Y-%m-%dT%H:%M:%SZ"),
            ),
        ],
    )

    class _FrozenDateTime(datetime):
        @classmethod
        def now(cls, tz: timezone | None = None) -> datetime:
            return now if tz is None else now.astimezone(tz)

    monkeypatch.setattr(watch_module, "datetime", _FrozenDateTime)

    posted: list[int] = []

    def fake_post(
        number: int,
        *,
        bypass_same_head_dedup: bool = False,
    ) -> tuple[bool, bool, datetime | None]:
        posted.append(number)
        return True, True, None

    runner = h._make_runner()
    runner.state.current_pr = pr
    runner.state.state = PipelineState.WATCH
    runner.state.last_codex_retrigger_at = now - timedelta(minutes=2)
    runner._post_codex_review_result = fake_post  # type: ignore[assignment]

    asyncio.run(runner.handle_watch())

    assert posted == []
    assert runner.state.last_codex_retrigger_at == now - timedelta(minutes=2)


def test_handle_watch_codex_bot_error_skips_already_handled_comment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An error comment older than ``last_codex_retrigger_at`` was already
    handled in a prior cycle and must not retrigger again."""
    pr = h._codex_bot_pr()
    monkeypatch.setattr(
        "src.github.prs.get_open_prs",
        lambda repo, **kw: [pr],
    )
    monkeypatch.setattr(
        "src.github.cache._gh_api_paginated",
        lambda path: [
            h._codex_bot_error_comment(
                created_at="2026-04-30T11:00:00Z",
            ),
        ],
    )

    posted: list[int] = []

    def fake_post(
        number: int,
        *,
        bypass_same_head_dedup: bool = False,
    ) -> tuple[bool, bool, datetime | None]:
        posted.append(number)
        return True, True, None

    runner = h._make_runner()
    runner.state.current_pr = pr
    runner.state.state = PipelineState.WATCH
    runner.state.last_codex_retrigger_at = datetime(2026, 4, 30, 12, 0, tzinfo=timezone.utc)
    runner._post_codex_review_result = fake_post  # type: ignore[assignment]

    asyncio.run(runner.handle_watch())

    assert posted == []




def test_handle_watch_codex_bot_error_skips_unparseable_created_at(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pr = h._codex_bot_pr()
    monkeypatch.setattr(
        "src.github.prs.get_open_prs",
        lambda repo, **kw: [pr],
    )
    monkeypatch.setattr(
        "src.github.gh_runner._parse_iso",
        lambda value: None,
    )
    monkeypatch.setattr(
        "src.github.cache._gh_api_paginated",
        lambda path: [
            h._codex_bot_error_comment(),
            h._codex_bot_error_comment(),
        ],
    )

    posted: list[int] = []

    def fake_post(
        number: int,
        *,
        bypass_same_head_dedup: bool = False,
    ) -> tuple[bool, bool, datetime | None]:
        posted.append(number)
        return True, True, None

    runner = h._make_runner()
    runner.state.current_pr = pr
    runner.state.state = PipelineState.WATCH
    runner._post_codex_review_result = fake_post  # type: ignore[assignment]

    asyncio.run(runner.handle_watch())

    assert posted == []
    assert runner.state.last_codex_retrigger_at is None










@pytest.mark.parametrize(
    "review",
    [ReviewStatus.PENDING, ReviewStatus.APPROVED, ReviewStatus.CHANGES_REQUESTED],
)
def test_handle_watch_skips_codex_bot_error_check_outside_eyes(
    monkeypatch: pytest.MonkeyPatch,
    review: ReviewStatus,
) -> None:
    """The codex-bot-error fetch must be gated to ``review == EYES`` so
    the daemon does not paginate ``issues/{pr}/comments`` on every WATCH
    poll during normal CI=PENDING/PR-waiting states."""
    pr = h._codex_bot_pr(review=review)
    monkeypatch.setattr(
        "src.github.prs.get_open_prs",
        lambda repo, **kw: [pr],
    )

    api_calls: list[str] = []

    def fake_paginated(path: str) -> list[dict[str, Any]]:
        api_calls.append(path)
        return []

    monkeypatch.setattr(
        "src.github.cache._gh_api_paginated",
        fake_paginated,
    )
    monkeypatch.setattr(
        "src.github.prs.get_last_push_age_seconds",
        lambda repo, number: None,
    )

    posted: list[int] = []

    def fake_post(
        number: int,
        *,
        bypass_same_head_dedup: bool = False,
    ) -> tuple[bool, bool, datetime | None]:
        posted.append(number)
        return True, True, None

    runner = h._make_runner()
    runner.state.current_pr = pr
    runner.state.state = PipelineState.WATCH
    runner._post_codex_review_result = fake_post  # type: ignore[assignment]

    asyncio.run(runner.handle_watch())

    assert posted == []
    assert all("issues/42/comments" not in path for path in api_calls)
    assert runner.state.last_codex_retrigger_at is None


# ---------------------------------------------------------------------------
# PR-224b moved from tests/test_runner.py — handle_watch group
# ---------------------------------------------------------------------------


def test_run_cycle_dispatches_watch_handler(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[str] = []
    publishes: list[str] = []
    runner = h._make_runner()
    runner._recovered = True
    runner._scaffolded = True
    runner.state.state = PipelineState.WATCH
    handler_name = "handle_watch"

    async def fake_ensure_repo_cloned() -> None:
        return None

    async def fake_handler() -> None:
        calls.append(handler_name)

    async def fake_publish_state() -> None:
        publishes.append("published")

    monkeypatch.setattr(runner, "ensure_repo_cloned", fake_ensure_repo_cloned)
    monkeypatch.setattr(runner, "preflight", h._preflight_true_stub)
    monkeypatch.setattr(runner, handler_name, fake_handler)
    monkeypatch.setattr(runner, "publish_state", fake_publish_state)

    asyncio.run(runner.run_cycle())

    assert calls == [handler_name]
    assert publishes == ["published"]


def test_effective_watch_poll_interval_is_slow_immediately_after_entry() -> None:
    """First WATCH cycle returns the slow interval (default 300s)."""
    runner = h._make_runner(poll_interval_sec=60)
    h._configure_watch_adaptive_defaults(runner)

    runner._watch_entered_at = datetime.now(timezone.utc)

    assert runner.effective_watch_poll_interval == 300


def test_effective_watch_poll_interval_still_slow_inside_window() -> None:
    """Four minutes after WATCH entry still uses the slow interval."""
    runner = h._make_runner(poll_interval_sec=60)
    h._configure_watch_adaptive_defaults(runner)

    runner._watch_entered_at = datetime.now(timezone.utc) - timedelta(minutes=4)

    assert runner.effective_watch_poll_interval == 300


def test_effective_watch_poll_interval_becomes_fast_past_window() -> None:
    """Past 5 min the slow window closes and polling drops to fast (45s)."""
    runner = h._make_runner(poll_interval_sec=60)
    h._configure_watch_adaptive_defaults(runner)

    runner._watch_entered_at = datetime.now(timezone.utc) - timedelta(minutes=5, seconds=1)

    assert runner.effective_watch_poll_interval == 45


def test_effective_watch_poll_interval_event_resets_slow_window() -> None:
    """A detected GitHub event re-anchors the slow window from event time."""
    runner = h._make_runner(poll_interval_sec=60)
    h._configure_watch_adaptive_defaults(runner)

    # WATCH entered 6 minutes ago — past the slow window.
    runner._watch_entered_at = datetime.now(timezone.utc) - timedelta(minutes=6)
    assert runner.effective_watch_poll_interval == 45

    # Prime a baseline signature, then observe a different signature.
    baseline = PRInfo(number=42, branch="pr-042", ci_status=CIStatus.PENDING)
    runner._observe_watch_event_signature(baseline)
    changed = PRInfo(
        number=42,
        branch="pr-042",
        ci_status=CIStatus.PENDING,
        review_status=ReviewStatus.EYES,
    )
    runner._observe_watch_event_signature(changed)

    assert runner._watch_last_event_at is not None
    # Event reset → new anchor is recent → back inside slow window.
    assert runner.effective_watch_poll_interval == 300


def test_watch_polling_anchors_cleared_on_transition_out_of_watch() -> None:
    """Leaving WATCH wipes the adaptive polling anchors for the next session."""
    runner = h._make_runner(poll_interval_sec=60)
    h._configure_watch_adaptive_defaults(runner)

    runner._watch_entered_at = datetime.now(timezone.utc)
    runner._watch_last_event_at = datetime.now(timezone.utc)
    runner._watch_last_event_signature = (42, CIStatus.PENDING, ReviewStatus.PENDING, None)

    runner._reset_watch_polling()

    assert runner._watch_entered_at is None
    assert runner._watch_last_event_at is None
    assert runner._watch_last_event_signature is None
    # No anchor → falls back to the static base interval.
    assert runner.effective_watch_poll_interval == 60


def test_run_cycle_resets_watch_anchors_when_state_leaves_watch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``_run_cycle_body`` clears watch anchors when the cycle exits WATCH."""
    runner = h._make_runner(poll_interval_sec=60)
    h._configure_watch_adaptive_defaults(runner)
    runner._recovered = True
    runner._watch_entered_at = datetime.now(timezone.utc) - timedelta(minutes=2)
    runner._watch_last_event_at = datetime.now(timezone.utc) - timedelta(minutes=1)
    runner._watch_last_event_signature = (42, CIStatus.PENDING, ReviewStatus.PENDING, None)
    runner.state.state = PipelineState.WATCH

    async def _noop_cloned(self: PipelineRunner) -> None:
        return None

    async def _budget_ok(self: PipelineRunner) -> bool:
        return True

    async def _no_user_paused(self: PipelineRunner) -> None:
        self.state.user_paused = False

    async def _preflight_ok(self: PipelineRunner) -> bool:
        return True

    async def _watch_to_idle(self: PipelineRunner) -> None:
        self.state.state = PipelineState.IDLE

    async def _publish_state(self: PipelineRunner) -> None:
        return None

    monkeypatch.setattr(PipelineRunner, "ensure_repo_cloned", _noop_cloned)
    monkeypatch.setattr(PipelineRunner, "_check_github_api_budget", _budget_ok)
    monkeypatch.setattr(PipelineRunner, "_refresh_user_paused_from_redis", _no_user_paused)
    monkeypatch.setattr(PipelineRunner, "preflight", _preflight_ok)
    monkeypatch.setattr(PipelineRunner, "handle_watch", _watch_to_idle)
    monkeypatch.setattr(PipelineRunner, "publish_state", _publish_state)

    asyncio.run(runner._run_cycle_body())

    assert runner.state.state == PipelineState.IDLE
    assert runner._watch_entered_at is None
    assert runner._watch_last_event_at is None
    assert runner._watch_last_event_signature is None


def test_run_cycle_anchors_watch_entered_at_on_transition_into_watch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``_run_cycle_body`` anchors ``_watch_entered_at`` at the transition.

    A handler that pushes the runner from a non-WATCH state into WATCH
    must leave the anchor set before ``_run_cycle_body`` returns so the
    daemon's *next* call to ``_runner_poll_interval`` already sees the
    slow cadence — otherwise the first interval after entry uses the
    fast base poll and the slow-start window is wasted.
    """
    runner = h._make_runner(poll_interval_sec=60)
    h._configure_watch_adaptive_defaults(runner)
    runner._recovered = True
    runner.state.state = PipelineState.IDLE
    assert runner._watch_entered_at is None

    async def _noop_cloned(self: PipelineRunner) -> None:
        return None

    async def _budget_ok(self: PipelineRunner) -> bool:
        return True

    async def _no_user_paused(self: PipelineRunner) -> None:
        self.state.user_paused = False

    async def _preflight_ok(self: PipelineRunner) -> bool:
        return True

    async def _idle_to_watch(self: PipelineRunner) -> None:
        self.state.state = PipelineState.WATCH

    async def _publish_state(self: PipelineRunner) -> None:
        return None

    monkeypatch.setattr(PipelineRunner, "ensure_repo_cloned", _noop_cloned)
    monkeypatch.setattr(PipelineRunner, "_check_github_api_budget", _budget_ok)
    monkeypatch.setattr(PipelineRunner, "_refresh_user_paused_from_redis", _no_user_paused)
    monkeypatch.setattr(PipelineRunner, "preflight", _preflight_ok)
    monkeypatch.setattr(PipelineRunner, "handle_idle", _idle_to_watch)
    monkeypatch.setattr(PipelineRunner, "publish_state", _publish_state)

    asyncio.run(runner._run_cycle_body())

    assert runner.state.state == PipelineState.WATCH
    assert runner._watch_entered_at is not None
    # The next poll interval — computed by the daemon main loop *after*
    # this cycle — already sees the slow cadence, not the base fallback.
    assert runner.effective_watch_poll_interval == 300


def test_check_budget_no_skip_when_state_is_watch() -> None:
    """WATCH cadence already absorbs the slowdown; do not skip cycles.

    ``effective_watch_poll_interval`` already takes
    ``max(target, base * multiplier)`` when the rate-limit slowdown is
    active. If ``_check_github_api_budget`` also skipped (multiplier-1)
    out of every multiplier cycles, the effective poll spacing would
    become ``effective_watch_poll_interval * multiplier`` and could
    delay merge/fix transitions by an hour or more when quota is low.
    """
    runner = h._make_runner(poll_interval_sec=60)
    runner.app_config.daemon.github_api_pause_threshold_percent = 5
    runner.app_config.daemon.github_api_slowdown_threshold_percent = 20
    runner.app_config.daemon.github_api_slowdown_multiplier = 10
    h._set_budget(runner, h._budget(remaining=500, limit=5000))  # 10%

    runner.state.state = PipelineState.WATCH

    decisions = [asyncio.run(runner._check_github_api_budget()) for _ in range(5)]

    assert decisions == [True] * 5
    assert runner._github_api_slowdown_cycle == 0
    assert runner._github_api_slowdown_attempts == 5


def _patch_infra_failure_pr(
    monkeypatch: pytest.MonkeyPatch,
    pr_number: int = 77,
    head_sha: str = "feedface",
    *,
    check_runs: list[dict] | None = None,
) -> PRInfo:
    """Wire ``get_open_prs`` and ``_fetch_ci_status_rest`` so WATCH sees
    a single infra-class failing check-run. Returns the polled PR so the
    caller can also seed ``runner.state.current_pr`` with the same PR
    number/branch.

    ``check_runs`` defaults to a single ``cancelled`` Actions check-run
    whose ``details_url`` carries workflow run ID ``9876``; this matches
    the run ID asserted by the WATCH integration test below. Pass
    ``check_runs=[]`` to drive the "no failing check-run" branch.
    """
    pr = PRInfo(
        number=pr_number,
        branch=f"pr-{pr_number:03d}",
        ci_status=CIStatus.INFRA_FAILURE,
        review_status=ReviewStatus.PENDING,
        head_sha=head_sha,
        last_activity=datetime.now(timezone.utc),
    )
    monkeypatch.setattr("src.github.prs.get_open_prs", lambda repo, **kw: [pr])
    if check_runs is None:
        check_runs = [
            {
                "conclusion": "cancelled",
                "details_url": (
                    "https://github.com/example/repo/actions/runs/9876/"
                    "job/12345"
                ),
            }
        ]
    # ``_maybe_reclassify_stuck_pending`` and ``_retry_failed_workflow``
    # both call this; the cached payload feeds the retry helper too.
    monkeypatch.setattr(
        "src.github.checks._fetch_ci_status_rest",
        lambda repo, sha: (list(check_runs), {}, True),
    )
    return pr


def test_first_infra_failure_retries_workflow(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """PR-251: a fresh INFRA_FAILURE on a head_sha triggers ``gh run rerun
    --failed`` once; ``handle_fix`` is NOT invoked, the infra-retry
    marker is written to Redis, and a log line records the retry."""
    pr = _patch_infra_failure_pr(monkeypatch)

    gh_calls: list[list[str]] = []

    def fake_run_gh(args: list[str], repo: str | None = None, **kwargs: Any) -> Any:
        gh_calls.append(list(args))
        return ""

    monkeypatch.setattr(watch_module.gh_runner, "run_gh", fake_run_gh)

    fix_calls: list[None] = []

    async def fake_handle_fix(self: Any) -> None:
        fix_calls.append(None)

    monkeypatch.setattr(runner_module.PipelineRunner, "handle_fix", fake_handle_fix)

    runner = h._make_runner()
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=pr.number, branch=pr.branch)
    asyncio.run(runner.handle_watch())

    assert fix_calls == []
    # PR-251 follow-up: workflow run ID is parsed from the failing
    # check-run's ``details_url`` (Actions job URL). No ``gh run list``
    # call is issued — that endpoint mis-keys ``pull_request`` runs to
    # the merge commit and would silently miss the failing PR run.
    assert all(args[:2] != ["run", "list"] for args in gh_calls)
    assert ["run", "rerun", "--failed", "9876"] in gh_calls
    # The marker records both the rerun time and original check identities.
    from src.keyspace import ci_infra_retried as _ci_infra_retried_key

    key = _ci_infra_retried_key(runner.owner_repo, pr.number, pr.head_sha)
    raw = asyncio.run(runner.redis.get(key))
    assert raw is not None
    marker_ts = json.loads(raw)["at"]
    assert marker_ts > 0
    history = " ".join(entry.get("event", "") for entry in runner.state.history)
    assert "INFRA_FAILURE detected" in history
    assert "infra retry: re-ran failed jobs" in history


def test_failed_infra_rerun_issuance_is_bounded(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.keyspace import ci_infra_retried

    pr = _patch_infra_failure_pr(monkeypatch, pr_number=81, head_sha="d" * 40)
    runner = h._make_runner()
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=pr.number, branch=pr.branch)
    reruns: list[list[str]] = []

    def unavailable(args: list[str], **kwargs: Any) -> str:
        if args[:2] == ["run", "rerun"]:
            reruns.append(args)
            raise RuntimeError("GitHub unavailable")
        return ""

    monkeypatch.setattr(watch_module.gh_runner, "run_gh", unavailable)
    marker_key = ci_infra_retried(runner.owner_repo, pr.number, pr.head_sha)
    for attempt in (1, 2):
        asyncio.run(runner.handle_watch())
        marker = json.loads(asyncio.run(runner.redis.get(marker_key)))
        assert marker["issued"] is False
        assert marker["tries"] == attempt
        marker["at"] = time.time() - 120
        asyncio.run(runner.redis.set(marker_key, json.dumps(marker)))
    asyncio.run(runner.handle_watch())
    assert len(reruns) == 2
    assert runner.state.state == PipelineState.ERROR
    assert "operator Retry required" in runner.state.error_message


def test_failed_infra_rerun_still_allows_current_review_fix(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.keyspace import ci_infra_retried

    pr = _patch_infra_failure_pr(monkeypatch, pr_number=82, head_sha="d" * 40)
    pr.review_status = ReviewStatus.CHANGES_REQUESTED
    runner = h._make_runner()
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=pr.number, branch=pr.branch)
    asyncio.run(runner.redis.set(
        ci_infra_retried(runner.owner_repo, pr.number, pr.head_sha),
        json.dumps({"at": time.time() - 120, "checks": [[1, 1]],
                    "issued": False, "tries": 1}),
    ))
    monkeypatch.setattr(
        runner, "_has_new_codex_feedback_since_last_push",
        lambda policy: FeedbackCheckResult.NEW,
    )
    fix = AsyncMock()
    monkeypatch.setattr(runner, "handle_fix", fix)
    asyncio.run(runner.handle_watch())
    fix.assert_awaited_once()


def test_partial_infra_rerun_retries_only_unissued_workflow(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.keyspace import ci_infra_retried

    pr = _patch_infra_failure_pr(
        monkeypatch, pr_number=83, head_sha="d" * 40,
        check_runs=[
            {"id": 101, "run_attempt": 1, "conclusion": "cancelled",
             "details_url": _actions_url(11, 101)},
            {"id": 202, "run_attempt": 1, "conclusion": "cancelled",
             "details_url": _actions_url(22, 202)},
        ],
    )
    runner = h._make_runner()
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=pr.number, branch=pr.branch)
    attempts: list[int] = []

    def partial(args: list[str], **kwargs: Any) -> str:
        if args[:2] == ["run", "rerun"]:
            run_id = int(args[-1])
            attempts.append(run_id)
            if run_id == 22 and attempts.count(22) == 1:
                raise RuntimeError("GitHub unavailable")
        return ""

    monkeypatch.setattr(watch_module.gh_runner, "run_gh", partial)
    marker_key = ci_infra_retried(runner.owner_repo, pr.number, pr.head_sha)
    asyncio.run(runner.handle_watch())
    marker = json.loads(asyncio.run(runner.redis.get(marker_key)))
    assert marker["issued"] is False
    assert marker["targets"] == [11, 22]
    assert marker["issued_runs"] == [11]
    marker["at"] = time.time() - 120
    asyncio.run(runner.redis.set(marker_key, json.dumps(marker)))
    asyncio.run(runner.handle_watch())
    marker = json.loads(asyncio.run(runner.redis.get(marker_key)))
    assert attempts == [11, 22, 22]
    assert marker["issued"] is True
    assert marker["issued_runs"] == [11, 22]


def test_infra_rerun_marker_without_issuance_proof_needs_operator_recovery() -> None:
    from src.keyspace import ci_infra_retried

    runner = h._make_runner()
    sha = "d" * 40
    runner.redis = None
    assert asyncio.run(runner._infra_retry_issue_state(81, sha)) == (False, 2)
    runner = h._make_runner()
    asyncio.run(runner.redis.set(
        ci_infra_retried(runner.owner_repo, 81, sha), "1",
    ))
    assert asyncio.run(runner._infra_retry_issue_state(81, sha)) == (False, 2)
    asyncio.run(runner.redis.set(
        ci_infra_retried(runner.owner_repo, 81, sha), "malformed",
    ))
    assert asyncio.run(runner._infra_retry_issue_state(81, sha)) == (False, 2)


@pytest.mark.parametrize(
    ("review_status", "feedback_result"),
    [
        (ReviewStatus.PENDING, FeedbackCheckResult.NEW),
        (ReviewStatus.CHANGES_REQUESTED, FeedbackCheckResult.NEW),
        (ReviewStatus.CHANGES_REQUESTED, FeedbackCheckResult.UNKNOWN),
    ],
)
def test_second_infra_failure_routes_to_fix(
    monkeypatch: pytest.MonkeyPatch,
    review_status: ReviewStatus,
    feedback_result: FeedbackCheckResult,
) -> None:
    """PR-251: a second INFRA_FAILURE classification on the same head_sha
    (the prior retry didn't help) routes WATCH straight through
    ``handle_fix`` without issuing another ``gh run rerun``."""
    pr = _patch_infra_failure_pr(monkeypatch, pr_number=78, head_sha="d00df00d")
    pr.review_status = review_status

    gh_calls: list[list[str]] = []

    def fake_run_gh(args: list[str], repo: str | None = None, **kwargs: Any) -> Any:
        gh_calls.append(list(args))
        return ""

    monkeypatch.setattr(watch_module.gh_runner, "run_gh", fake_run_gh)

    fix_calls: list[None] = []

    async def fake_handle_fix(self: Any) -> None:
        fix_calls.append(None)

    monkeypatch.setattr(runner_module.PipelineRunner, "handle_fix", fake_handle_fix)

    runner = h._make_runner()
    monkeypatch.setattr(runner, "_infra_retry_has_new_failed_attempt", AsyncMock(return_value=True))
    feedback_policies: list[object] = []

    def feedback_check(policy: object) -> FeedbackCheckResult:
        feedback_policies.append(policy)
        return feedback_result

    monkeypatch.setattr(runner, "_has_new_codex_feedback_since_last_push", feedback_check)
    # Pre-seed a successful issuance with an elapsed grace window.
    from src.keyspace import ci_infra_retried as _ci_infra_retried_key

    key = _ci_infra_retried_key(runner.owner_repo, pr.number, pr.head_sha)
    stale_marker_ts = (
        time.time() - watch_module._INFRA_RETRY_GRACE_SECONDS - 1.0
    )
    asyncio.run(runner.redis.set(key, json.dumps({
        "at": stale_marker_ts, "checks": [[1, 1]], "issued": True, "tries": 1,
    })))

    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=pr.number, branch=pr.branch)
    asyncio.run(runner.handle_watch())

    assert fix_calls == [None]
    assert len(feedback_policies) == (1 if review_status == ReviewStatus.CHANGES_REQUESTED else 0)
    if feedback_policies and feedback_result == FeedbackCheckResult.NEW:
        assert runner._fix_feedback_reviewer_policy is feedback_policies[0]
    # No workflow rerun calls were made — straight to FIX.
    assert all(args[:2] != ["run", "rerun"] for args in gh_calls)
    history = " ".join(entry.get("event", "") for entry in runner.state.history)
    assert "INFRA_FAILURE persisted after retry" in history
    if feedback_result == FeedbackCheckResult.UNKNOWN and feedback_policies:
        assert "CI FIX continues" in history
    # PR-251 follow-up: ``handle_fix`` reads ``ci_status`` to decide
    # whether to inject CI logs into the FIX prompt; the persisted-INFRA
    # path must downgrade to FAILURE before invoking it so the coder
    # actually receives the failure logs the daemon promised in the
    # "effective FAILURE" log line.
    assert runner.state.current_pr is not None
    assert runner.state.current_pr.ci_status == CIStatus.FAILURE


@pytest.mark.parametrize(
    ("new_state", "new_identity", "expected"),
    [("failure", True, True), ("failure", False, False), ("pending", True, False)],
)
def test_infra_retry_waits_for_new_terminal_attempt(
    monkeypatch: pytest.MonkeyPatch, new_state: str,
    new_identity: bool, expected: bool,
) -> None:
    from src.github import checks as gh_checks
    from src.keyspace import ci_infra_retried

    runner = h._make_runner()
    sha = "d" * 40
    retry_at = time.time() - 120
    asyncio.run(runner.redis.set(ci_infra_retried(runner.owner_repo, 78, sha), json.dumps({
        "at": retry_at, "checks": [[1, 1]],
    })))
    # A rerun may start before WATCH writes its marker; identity still proves it is new.
    started = datetime.fromtimestamp(retry_at - 10, timezone.utc).isoformat()
    runs = [
        {
            "id": 1, "name": "unit", "head_sha": sha, "app": {"id": 1},
            "status": "completed", "conclusion": "cancelled", "run_attempt": 1,
            "started_at": datetime.fromtimestamp(retry_at - 100, timezone.utc).isoformat(),
        },
        *([{
            "id": 2, "name": "unit", "head_sha": sha, "app": {"id": 1},
            "status": "in_progress" if new_state == "pending" else "completed",
            "conclusion": None if new_state == "pending" else "failure",
            "run_attempt": 2, "started_at": started,
        }] if new_identity else []),
    ]
    retrieval = gh_checks._make_ci_retrieval(
        runner.owner_repo, sha, 0.0, runs,
        {"sha": sha, "state": "pending", "statuses": []},
        gh_checks._CiSourceResult(True, False), gh_checks._CiSourceResult(True, True),
    )
    monkeypatch.setattr(
        gh_checks, "_retrieve_ci_status_evidence",
        lambda repo, head, *, fresh: retrieval if fresh else None,
    )
    assert asyncio.run(runner._infra_retry_has_new_failed_attempt(78, sha)) is expected


@pytest.mark.parametrize(
    "case",
    ["no_redis", "missing_marker", "legacy_marker", "incomplete",
     "malformed_status", "status_only", "same_identity", "empty_baseline",
     "malformed_marker"],
)
def test_infra_retry_requires_complete_new_check_provenance(
    monkeypatch: pytest.MonkeyPatch, case: str,
) -> None:
    from src.github import checks as gh_checks
    from src.keyspace import ci_infra_retried

    runner = h._make_runner()
    sha = "d" * 40
    retry_at = time.time() - 120
    if case == "no_redis":
        runner.redis = None
        assert not asyncio.run(runner._infra_retry_has_new_failed_attempt(78, sha))
        return
    if case != "missing_marker":
        asyncio.run(runner.redis.set(
            ci_infra_retried(runner.owner_repo, 78, sha),
            "1" if case == "legacy_marker" else (
                "bad marker" if case == "malformed_marker" else json.dumps({
                    "at": retry_at, "checks": [] if case == "empty_baseline" else [[1, 1]],
                })
            ),
        ))
    run = {
        "id": 1 if case == "same_identity" else 2,
        "name": "unit", "head_sha": sha, "app": {"id": 1},
        "status": "completed", "conclusion": "failure",
        "run_attempt": 1 if case == "same_identity" else 2,
        "started_at": datetime.fromtimestamp(retry_at + 10, timezone.utc).isoformat(),
    }
    runs = [] if case == "status_only" else [run]
    statuses = (
        [{"context": "unit", "sha": sha, "state": "failure",
          "creator": {"login": "ci"}}] if case == "status_only" else []
    )
    status_payload = {
        "sha": sha, "state": "failure" if case == "status_only" else "pending",
        "statuses": "malformed" if case == "malformed_status" else statuses,
    }
    retrieval = gh_checks._make_ci_retrieval(
        runner.owner_repo, sha, 0.0, runs, status_payload,
        gh_checks._CiSourceResult(case != "incomplete", False),
        gh_checks._CiSourceResult(True, not statuses),
    )
    monkeypatch.setattr(gh_checks, "_retrieve_ci_status_evidence", lambda *a, **kw: retrieval)
    assert not asyncio.run(runner._infra_retry_has_new_failed_attempt(78, sha))


def test_persisted_infra_result_without_new_attempt_remains_in_watch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.keyspace import ci_infra_retried

    pr = _patch_infra_failure_pr(monkeypatch, pr_number=78, head_sha="d" * 40)
    runner = h._make_runner()
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=pr.number, branch=pr.branch)
    marker = ci_infra_retried(runner.owner_repo, pr.number, pr.head_sha)
    asyncio.run(runner.redis.set(marker, json.dumps({
        "at": time.time() - 120, "checks": [[1, 1]], "issued": True, "tries": 1,
    })))
    monkeypatch.setattr(runner, "_infra_retry_has_new_failed_attempt", AsyncMock(return_value=False))
    fix = AsyncMock()
    rerun = []
    monkeypatch.setattr(runner, "handle_fix", fix)
    monkeypatch.setattr(runner, "_retry_failed_workflow", lambda *a: rerun.append(a))
    asyncio.run(runner.handle_watch())
    assert runner.state.state == PipelineState.WATCH
    fix.assert_not_awaited()
    assert rerun == []
    assert any("awaiting post-retry terminal CI evidence" in e["event"]
               for e in runner.state.history)


def test_infra_failure_on_fork_pr_logs_no_retry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """PR-251: fork PRs cannot be auto-fixed; INFRA_FAILURE on a fork
    falls through to the manual-wait log without issuing a rerun."""
    pr = PRInfo(
        number=79,
        branch="pr-079",
        ci_status=CIStatus.INFRA_FAILURE,
        review_status=ReviewStatus.PENDING,
        head_sha="cafed00d",
        last_activity=datetime.now(timezone.utc),
        is_cross_repository=True,
    )
    monkeypatch.setattr("src.github.prs.get_open_prs", lambda repo, **kw: [pr])
    monkeypatch.setattr(
        "src.github.checks._fetch_ci_status_rest",
        lambda repo, sha: ([{"conclusion": "cancelled"}], {}, True),
    )

    gh_calls: list[list[str]] = []

    def fake_run_gh(args: list[str], repo: str | None = None, **kwargs: Any) -> Any:
        gh_calls.append(list(args))
        return ""

    monkeypatch.setattr(watch_module.gh_runner, "run_gh", fake_run_gh)

    fix_calls: list[None] = []

    async def fake_handle_fix(self: Any) -> None:
        fix_calls.append(None)

    monkeypatch.setattr(runner_module.PipelineRunner, "handle_fix", fake_handle_fix)

    runner = h._make_runner()
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=pr.number, branch=pr.branch)
    asyncio.run(runner.handle_watch())

    assert fix_calls == []
    assert all(args[:2] != ["run", "rerun"] for args in gh_calls)
    history = " ".join(entry.get("event", "") for entry in runner.state.history)
    assert "fork PR cannot be auto-fixed" in history
    assert "ci=INFRA_FAILURE" in history


def test_infra_retry_attempted_returns_true_when_redis_or_sha_missing() -> None:
    """PR-251: helpers conservatively report ``(True, True)`` when state
    cannot be persisted, so the caller routes straight to FIX rather
    than re-running the workflow forever against a missing tracker."""
    runner = h._make_runner()
    assert asyncio.run(runner._infra_retry_attempted(11, "")) == (True, True)
    runner.redis = None  # type: ignore[assignment]
    assert asyncio.run(runner._infra_retry_attempted(11, "abc")) == (True, True)


def test_infra_retry_attempted_returns_false_when_marker_absent() -> None:
    """PR-251 follow-up: with redis available but no marker, both flags are
    False so the caller proceeds to issue the rerun + write the marker."""
    runner = h._make_runner()
    assert asyncio.run(runner._infra_retry_attempted(11, "abc")) == (False, False)


def test_infra_retry_attempted_within_grace_window_returns_marker_only() -> None:
    """PR-251 follow-up: a marker written less than
    ``_INFRA_RETRY_GRACE_SECONDS`` ago reports ``(True, False)`` so the
    WATCH cycle stays put while the cached pre-rerun CI payload is
    still in play (CI cache TTL is 15s; e2e config polls every 2s)."""
    runner = h._make_runner()
    from src.keyspace import ci_infra_retried as _ci_infra_retried_key

    key = _ci_infra_retried_key(runner.owner_repo, 11, "abc")
    asyncio.run(runner.redis.set(key, str(time.time())))
    marker_exists, grace_elapsed = asyncio.run(
        runner._infra_retry_attempted(11, "abc")
    )
    assert marker_exists is True
    assert grace_elapsed is False


def test_infra_retry_attempted_handles_legacy_marker_value() -> None:
    """PR-251 follow-up: legacy markers written by an older daemon
    (value ``"1"``) parse as float ``1.0`` — a timestamp at the unix
    epoch — so the elapsed comparison naturally returns
    ``(True, True)``. This preserves the prior single-bit semantics
    so an in-flight upgrade can't get stuck in WATCH on a sha that
    already exhausted its retry budget."""
    runner = h._make_runner()
    from src.keyspace import ci_infra_retried as _ci_infra_retried_key

    key = _ci_infra_retried_key(runner.owner_repo, 11, "abc")
    asyncio.run(runner.redis.set(key, "1"))
    assert asyncio.run(runner._infra_retry_attempted(11, "abc")) == (True, True)


def test_infra_retry_attempted_handles_corrupted_marker_value() -> None:
    """PR-251 follow-up: a non-numeric marker (Redis corruption or
    operator override) cannot be parsed as a timestamp; the helper
    treats it as ``(True, True)`` so the caller routes to FIX rather
    than crashing the WATCH cycle on the bad value."""
    runner = h._make_runner()
    from src.keyspace import ci_infra_retried as _ci_infra_retried_key

    key = _ci_infra_retried_key(runner.owner_repo, 11, "abc")
    asyncio.run(runner.redis.set(key, "not-a-number"))
    assert asyncio.run(runner._infra_retry_attempted(11, "abc")) == (True, True)


def test_infra_failure_within_grace_window_stays_in_watch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """PR-251 follow-up (review fix): when the marker exists but the
    grace window has not elapsed, WATCH must NOT escalate to FIX and
    must NOT issue another rerun. This prevents fast-cadence pollers
    (e2e config polls every 2s, well below the 15s CI cache TTL) from
    routing to ``handle_fix`` based on the pre-rerun cached payload."""
    pr = _patch_infra_failure_pr(monkeypatch, pr_number=81, head_sha="beadface")

    gh_calls: list[list[str]] = []

    def fake_run_gh(args: list[str], repo: str | None = None, **kwargs: Any) -> Any:
        gh_calls.append(list(args))
        return ""

    monkeypatch.setattr(watch_module.gh_runner, "run_gh", fake_run_gh)

    fix_calls: list[None] = []

    async def fake_handle_fix(self: Any) -> None:
        fix_calls.append(None)

    monkeypatch.setattr(runner_module.PipelineRunner, "handle_fix", fake_handle_fix)

    runner = h._make_runner()
    # Marker was written "just now" (well within the grace window).
    from src.keyspace import ci_infra_retried as _ci_infra_retried_key

    key = _ci_infra_retried_key(runner.owner_repo, pr.number, pr.head_sha)
    asyncio.run(runner.redis.set(key, str(time.time())))

    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=pr.number, branch=pr.branch)
    asyncio.run(runner.handle_watch())

    # Did NOT escalate to FIX, did NOT issue another rerun.
    assert fix_calls == []
    assert all(args[:2] != ["run", "rerun"] for args in gh_calls)
    history = " ".join(entry.get("event", "") for entry in runner.state.history)
    assert "within retry grace window" in history
    # ci_status must remain INFRA_FAILURE — the downgrade to FAILURE
    # only happens on the persisted-after-retry path.
    assert runner.state.current_pr is not None
    assert runner.state.current_pr.ci_status == CIStatus.INFRA_FAILURE


def test_mark_infra_retry_attempted_noops_when_redis_or_sha_missing() -> None:
    """PR-251: marking is a no-op without ``redis`` or ``head_sha``; the
    Redis double records no writes."""
    runner = h._make_runner()
    asyncio.run(runner._mark_infra_retry_attempted(11, ""))
    assert runner.redis.writes == []
    runner.redis = None  # type: ignore[assignment]
    asyncio.run(runner._mark_infra_retry_attempted(11, "abc"))


def _patch_check_runs(
    monkeypatch: pytest.MonkeyPatch,
    check_runs: list[dict],
) -> None:
    """Replace ``_fetch_ci_status_rest`` with a stub returning ``check_runs``.

    PR-251 follow-up: ``_retry_failed_workflow`` reads check-runs (not
    ``gh run list``) to find Actions workflow run IDs. Tests that
    exercise the helper directly install this stub so the helper sees
    the desired failing-check-run shape.
    """
    monkeypatch.setattr(
        "src.github.checks._fetch_ci_status_rest",
        lambda repo, sha: (list(check_runs), {}, True),
    )


def _actions_url(run_id: int, job_id: int = 1) -> str:
    """Return a canonical GitHub Actions check-run ``details_url``."""
    return (
        f"https://github.com/example/repo/actions/runs/{run_id}/job/{job_id}"
    )


def test_retry_failed_workflow_short_circuits_without_head_sha() -> None:
    """PR-251: an empty ``head_sha`` returns ``False`` with no fetch."""
    runner = h._make_runner()
    assert runner._retry_failed_workflow(11, "") is False


def test_retry_failed_workflow_handles_no_failing_check_runs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """PR-251 follow-up: when the cached check-runs payload has no
    failing entries (transient race against the classifier), the helper
    logs and returns ``False`` without issuing ``gh run rerun``."""
    runner = h._make_runner()
    _patch_check_runs(monkeypatch, [])
    rerun_calls: list[list[str]] = []

    def fake_run_gh(args: list[str], repo: str | None = None, **kwargs: Any) -> Any:
        rerun_calls.append(list(args))
        return ""

    monkeypatch.setattr(watch_module.gh_runner, "run_gh", fake_run_gh)

    assert runner._retry_failed_workflow(11, "abc1234") is False
    assert rerun_calls == []
    history = " ".join(entry.get("event", "") for entry in runner.state.history)
    assert "no failing check-run found" in history


def test_retry_failed_workflow_skips_check_runs_without_actions_url(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """PR-251 follow-up: a failing check-run from a non-Actions GitHub
    App carries a ``details_url`` outside the ``/actions/runs/<id>``
    shape; the helper cannot rerun it via ``gh run rerun --failed`` and
    must skip rather than crash."""
    runner = h._make_runner()
    _patch_check_runs(
        monkeypatch,
        [
            {
                "conclusion": "cancelled",
                "details_url": "https://example.com/custom-app/check/42",
            }
        ],
    )
    rerun_calls: list[list[str]] = []

    def fake_run_gh(args: list[str], repo: str | None = None, **kwargs: Any) -> Any:
        rerun_calls.append(list(args))
        return ""

    monkeypatch.setattr(watch_module.gh_runner, "run_gh", fake_run_gh)

    assert runner._retry_failed_workflow(11, "abc1234") is False
    assert rerun_calls == []
    history = " ".join(entry.get("event", "") for entry in runner.state.history)
    assert "no Actions workflow run ID" in history


def test_retry_failed_workflow_handles_rerun_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """PR-251: when every ``gh run rerun --failed`` call raises, the
    helper logs the failure and returns ``False`` without crashing."""
    runner = h._make_runner()
    _patch_check_runs(
        monkeypatch,
        [
            {"conclusion": "cancelled", "details_url": _actions_url(555)},
        ],
    )

    def fake_run_gh(args: list[str], repo: str | None = None, **kwargs: Any) -> Any:
        raise RuntimeError("permission denied")

    monkeypatch.setattr(watch_module.gh_runner, "run_gh", fake_run_gh)
    assert runner._retry_failed_workflow(11, "abc1234") is False
    history = " ".join(entry.get("event", "") for entry in runner.state.history)
    assert "infra retry rerun failed" in history


def test_retry_failed_workflow_extracts_run_id_from_details_url(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """PR-251 follow-up: the workflow run ID is parsed from the failing
    check-run's ``details_url``. This is the only reliable selection
    path for ``pull_request`` workflows — ``gh run list --commit``
    keys on ``workflow_run.head_sha`` which for PR events points at
    the synthetic merge commit, not at ``pull_request.head.sha`` the
    daemon tracks.
    """
    runner = h._make_runner()
    _patch_check_runs(
        monkeypatch,
        [
            {"conclusion": "cancelled", "details_url": _actions_url(9876, 12345)},
        ],
    )
    calls: list[list[str]] = []

    def fake_run_gh(args: list[str], repo: str | None = None, **kwargs: Any) -> Any:
        calls.append(list(args))
        return ""

    monkeypatch.setattr(watch_module.gh_runner, "run_gh", fake_run_gh)
    assert runner._retry_failed_workflow(11, "abc1234") is True
    assert calls == [["run", "rerun", "--failed", "9876"]]


def test_retry_failed_workflow_dedupes_jobs_in_same_workflow_run(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """PR-251 follow-up: a workflow with several failing matrix jobs
    posts one check-run per job, all sharing the same ``run_id`` in
    ``details_url``. ``gh run rerun --failed <run_id>`` operates on the
    whole run, so the helper must dedupe the run IDs before calling.
    """
    runner = h._make_runner()
    _patch_check_runs(
        monkeypatch,
        [
            {"conclusion": "cancelled", "details_url": _actions_url(700, 1)},
            {"conclusion": "failure", "details_url": _actions_url(700, 2)},
            {"conclusion": "stale", "details_url": _actions_url(800, 3)},
        ],
    )
    calls: list[list[str]] = []

    def fake_run_gh(args: list[str], repo: str | None = None, **kwargs: Any) -> Any:
        calls.append(list(args))
        return ""

    monkeypatch.setattr(watch_module.gh_runner, "run_gh", fake_run_gh)
    assert runner._retry_failed_workflow(11, "abc1234") is True
    assert calls == [
        ["run", "rerun", "--failed", "700"],
        ["run", "rerun", "--failed", "800"],
    ]


def test_retry_failed_workflow_skips_succeeding_check_runs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """PR-251 follow-up: only failing check-runs feed the rerun set;
    the helper must not invoke ``gh run rerun`` against runs whose
    conclusion is ``success`` or ``neutral`` (e.g. a re-dispatch that
    superseded the failure that drove the INFRA_FAILURE classification).
    """
    runner = h._make_runner()
    _patch_check_runs(
        monkeypatch,
        [
            {"conclusion": "success", "details_url": _actions_url(1)},
            {"conclusion": "neutral", "details_url": _actions_url(2)},
            {"conclusion": "skipped", "details_url": _actions_url(3)},
        ],
    )
    calls: list[list[str]] = []

    def fake_run_gh(args: list[str], repo: str | None = None, **kwargs: Any) -> Any:
        calls.append(list(args))
        return ""

    monkeypatch.setattr(watch_module.gh_runner, "run_gh", fake_run_gh)
    assert runner._retry_failed_workflow(11, "abc1234") is False
    assert calls == []
    history = " ".join(entry.get("event", "") for entry in runner.state.history)
    assert "no failing check-run found" in history


def test_retry_failed_workflow_matches_failing_status_with_empty_conclusion(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """PR-251 follow-up: when a check-run carries an empty
    ``conclusion`` (e.g. an in-flight job that GitHub reports with only
    ``status``), a failure-class ``status`` value still selects the run
    for rerun. Without this branch the WATCH cycle would consume its
    one-shot retry marker without ever calling ``gh run rerun``.
    """
    runner = h._make_runner()
    _patch_check_runs(
        monkeypatch,
        [
            {
                "conclusion": None,
                "status": "failed",
                "details_url": _actions_url(7),
            },
        ],
    )
    calls: list[list[str]] = []

    def fake_run_gh(args: list[str], repo: str | None = None, **kwargs: Any) -> Any:
        calls.append(list(args))
        return ""

    monkeypatch.setattr(watch_module.gh_runner, "run_gh", fake_run_gh)
    assert runner._retry_failed_workflow(11, "abc1234") is True
    assert calls == [["run", "rerun", "--failed", "7"]]


def test_retry_failed_workflow_partial_rerun_failure_remains_unissued(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """PR-251 follow-up: when several workflow runs need rerunning and
    one of the calls fails, the marker must retain that workflow as
    unissued so the bounded follow-up retries only its request.
    """
    runner = h._make_runner()
    _patch_check_runs(
        monkeypatch,
        [
            {"conclusion": "cancelled", "details_url": _actions_url(900)},
            {"conclusion": "stale", "details_url": _actions_url(901)},
        ],
    )
    calls: list[list[str]] = []

    def fake_run_gh(args: list[str], repo: str | None = None, **kwargs: Any) -> Any:
        calls.append(list(args))
        if args == ["run", "rerun", "--failed", "900"]:
            raise RuntimeError("transient")
        return ""

    monkeypatch.setattr(watch_module.gh_runner, "run_gh", fake_run_gh)
    assert runner._retry_failed_workflow(11, "abc1234") is False
    assert runner._infra_retry_target_run_ids == [900, 901]
    assert runner._infra_retry_issued_run_ids == [901]
    history = " ".join(entry.get("event", "") for entry in runner.state.history)
    assert "infra retry rerun failed" in history
    assert "infra retry: re-ran failed jobs" in history


def test_retry_failed_workflow_handles_no_run_found(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """PR-251: when the failing check-runs carry no Actions workflow
    run IDs (non-Actions GitHub Apps only), the retry helper logs and
    returns ``False``; the marker is still written by the caller so the
    next cycle routes to FIX."""
    pr = _patch_infra_failure_pr(
        monkeypatch,
        pr_number=80,
        head_sha="11223344",
        check_runs=[
            {
                "conclusion": "cancelled",
                "details_url": "https://example.com/custom-app/check/1",
            }
        ],
    )

    monkeypatch.setattr(
        watch_module.gh_runner,
        "run_gh",
        lambda args, repo=None, **kw: "",
    )

    fix_calls: list[None] = []

    async def fake_handle_fix(self: Any) -> None:
        fix_calls.append(None)

    monkeypatch.setattr(runner_module.PipelineRunner, "handle_fix", fake_handle_fix)

    runner = h._make_runner()
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=pr.number, branch=pr.branch)
    asyncio.run(runner.handle_watch())

    # First cycle: no rerun was issued (no run found), but the marker
    # is written (with a timestamp) so a later cycle — once the grace
    # window has elapsed — waits for proven post-rerun failure evidence.
    assert fix_calls == []
    from src.keyspace import ci_infra_retried as _ci_infra_retried_key

    key = _ci_infra_retried_key(runner.owner_repo, pr.number, pr.head_sha)
    raw = asyncio.run(runner.redis.get(key))
    assert raw is not None
    assert json.loads(raw)["at"] > 0
    history = " ".join(entry.get("event", "") for entry in runner.state.history)
    assert "no Actions workflow run ID" in history


# ---------------------------------------------------------------------------
# PR-290a: _scan_pr_diff_once (diff content scan infrastructure)
# ---------------------------------------------------------------------------


def test_scan_pr_diff_once_skips_when_no_current_pr() -> None:
    """The dispatcher cannot scan a diff for a PR that does not exist;
    in that case the call returns ``False`` and changes no state."""
    runner = h._make_runner()
    runner.state.current_pr = None

    result = asyncio.run(runner._scan_pr_diff_once())

    assert result is False


def test_scan_pr_diff_once_skips_when_cache_matches_head_sha(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """SHA-keyed cache hit: when ``diff_scanned_at_sha`` equals the
    current ``head_sha`` the scan must short-circuit (no gh CLI call)
    so each WATCH cycle costs nothing on a HEAD it already cleared."""
    import re as _re

    monkeypatch.setattr(
        watch_module.guardrails,
        "_DIFF_PATTERNS",
        {"x": _re.compile(r"x")},
    )

    called: list[int] = []

    def fail_fetch(repo: str, number: int) -> str:
        called.append(number)
        raise AssertionError("get_pr_diff must not be called on cache hit")

    monkeypatch.setattr("src.github.prs.get_pr_diff", fail_fetch)

    runner = h._make_runner()
    runner.state.current_pr = PRInfo(
        number=7,
        branch="pr-007",
        head_sha="cafe1234",
        diff_scanned_at_sha="cafe1234",
    )

    result = asyncio.run(runner._scan_pr_diff_once())

    assert result is False
    assert called == []


def test_scan_pr_diff_once_runs_when_cache_mismatches_head_sha(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Fresh HEAD SHA (new coder push) re-arms the scan. The dispatcher
    fetches the diff, runs the catalogue (empty in this assertion, so
    no violation), and updates ``diff_scanned_at_sha`` to the new SHA
    on success."""
    import re as _re

    monkeypatch.setattr(
        watch_module.guardrails,
        "_DIFF_PATTERNS",
        {"never_match": _re.compile(r"NEVER_MATCH_TOKEN")},
    )
    monkeypatch.setattr(
        watch_module.guardrails,
        "_DIFF_RULES",
        {"never_match": "test rule"},
    )

    fetched: list[tuple[str, int]] = []

    def fake_fetch(repo: str, number: int) -> str:
        fetched.append((repo, number))
        return "diff --git a/x b/x\n+innocuous\n"

    monkeypatch.setattr("src.github.prs.get_pr_diff", fake_fetch)

    runner = h._make_runner()
    runner.state.current_pr = PRInfo(
        number=8,
        branch="pr-008",
        head_sha="deadbeef",
        diff_scanned_at_sha="oldcafe1",
    )

    result = asyncio.run(runner._scan_pr_diff_once())

    assert result is True
    assert fetched == [(runner.owner_repo, 8)]
    assert runner.state.current_pr.diff_scanned_at_sha == "deadbeef"
    assert runner.state.state != PipelineState.ERROR


def test_large_diff_is_review_signal_after_each_push(monkeypatch: pytest.MonkeyPatch) -> None:
    import re as _re

    monkeypatch.setattr(watch_module.guardrails, "_DIFF_PATTERNS", {"never": _re.compile("NEVER")})
    diff = "diff --git a/src/large.py b/src/large.py\n--- a/src/large.py\n+++ b/src/large.py\n"
    diff += "".join(f"+line {index}\n" for index in range(1600))
    monkeypatch.setattr("src.github.prs.get_pr_diff", lambda *_: diff)
    runner = h._make_runner()
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=8, branch="pr-008", head_sha="first")

    assert asyncio.run(runner._scan_pr_diff_once())
    assert runner.state.state == PipelineState.WATCH
    assert runner.state.quarantined_prs == set()
    assert sum("[WATCH] PR #8 diff size review signal" in item["event"] for item in runner.state.history) == 1
    assert not asyncio.run(runner._scan_pr_diff_once())
    runner.state.current_pr.head_sha = "second"
    assert asyncio.run(runner._scan_pr_diff_once())
    assert sum("[WATCH] PR #8 diff size review signal" in item["event"] for item in runner.state.history) == 2


def test_scan_pr_diff_once_leaves_cache_unchanged_on_fetch_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A transient ``gh pr diff`` failure must NOT mark the HEAD as
    scanned; otherwise the next WATCH cycle would skip the SHA and a
    prohibited diff could slip past the catalogue. Fetch failures are
    retried on subsequent cycles."""
    import re as _re

    monkeypatch.setattr(
        watch_module.guardrails,
        "_DIFF_PATTERNS",
        {"x": _re.compile(r"x")},
    )

    def boom(repo: str, number: int) -> str:
        raise RuntimeError("gh CLI timed out")

    monkeypatch.setattr("src.github.prs.get_pr_diff", boom)

    runner = h._make_runner()
    runner.state.current_pr = PRInfo(
        number=9,
        branch="pr-009",
        head_sha="freshSHA",
        diff_scanned_at_sha="prior",
    )

    result = asyncio.run(runner._scan_pr_diff_once())

    assert result is False
    assert runner.state.current_pr.diff_scanned_at_sha == "prior"


def test_scan_pr_diff_once_empty_catalogue_does_not_mark_head(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """PR-290a skeleton: with an empty pattern catalogue, the dispatcher
    must NOT record ``diff_scanned_at_sha``. Catalogue contents change at
    deploy time; marking a HEAD as scanned while patterns are empty would
    let the cache-hit gate short-circuit the same HEAD forever after the
    PR-290b/c rollout populated the rules, bypassing the new catalogue
    until a fresh push moved HEAD."""
    monkeypatch.setattr(watch_module.guardrails, "_DIFF_PATTERNS", {})
    monkeypatch.setattr(watch_module.guardrails, "_DIFF_RULES", {})

    def fail_fetch(repo: str, number: int) -> str:
        raise AssertionError("get_pr_diff must not be called on empty catalogue")

    monkeypatch.setattr("src.github.prs.get_pr_diff", fail_fetch)

    runner = h._make_runner()
    runner.state.current_pr = PRInfo(
        number=10,
        branch="pr-010",
        head_sha="bee1bee1",
        diff_scanned_at_sha=None,
    )

    result = asyncio.run(runner._scan_pr_diff_once())

    assert result is False
    assert runner.state.current_pr.diff_scanned_at_sha is None


def test_scan_pr_diff_once_violation_transitions_to_error_with_guardrail_subsource(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When the catalogue is populated and the diff matches, the
    dispatcher must transition through ``_transition_to_error`` with a
    structured ``CancellationCause`` whose ``payload.subsource`` is
    ``"guardrail"`` (PR-315/PR-320 single-ERROR-category model)."""
    import re as _re

    monkeypatch.setattr(
        watch_module.guardrails,
        "_DIFF_PATTERNS",
        {
            "workflow_permissions_write_all": _re.compile(
                r"permissions:\s*write-all"
            )
        },
    )
    monkeypatch.setattr(
        watch_module.guardrails,
        "_DIFF_RULES",
        {"workflow_permissions_write_all": "Workflow permissions write-all"},
    )
    monkeypatch.setattr(
        "src.github.prs.get_pr_diff",
        lambda repo, number: (
            "diff --git a/.github/workflows/ci.yml b/.github/workflows/ci.yml\n"
            "+permissions: write-all\n"
        ),
    )

    captured: list[dict[str, Any]] = []

    async def fake_transition(
        self: Any,
        message: str,
        **kwargs: Any,
    ) -> None:
        captured.append({"message": message, **kwargs})
        self.state.state = PipelineState.ERROR

    monkeypatch.setattr(
        runner_module.PipelineRunner,
        "_transition_to_error",
        fake_transition,
    )

    runner = h._make_runner()
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(
        number=11,
        branch="pr-011",
        head_sha="newsha01",
        diff_scanned_at_sha=None,
    )

    result = asyncio.run(runner._scan_pr_diff_once())

    assert result is True
    assert len(captured) == 1
    call = captured[0]
    assert "[GUARDRAIL]" in call["message"]
    assert "workflow_permissions_write_all" in call["message"]
    cause = call["cancellation_cause"]
    assert cause.category == "ERROR"
    assert cause.payload["subsource"] == "guardrail"
    assert cause.payload["tier"] == 1
    assert cause.payload["category"] == "workflow_permissions_write_all"
    assert cause.payload["excerpt"] == "permissions: write-all"
    # Cache must update on a successful scan, even when a violation fires,
    # so the same HEAD does not re-run the scan after an operator Retry.
    assert runner.state.current_pr.diff_scanned_at_sha == "newsha01"


def test_scan_pr_diff_once_uses_runner_daemon_config_snapshot(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """WATCH diff scans must use the runner's loaded config snapshot.

    Re-loading ``config.yml`` inside the guardrail scan would make WATCH
    sensitive to temporary config edit failures and could apply thresholds
    that differ from the active runner cycle.
    """
    import re as _re

    monkeypatch.setattr(
        watch_module.guardrails,
        "_DIFF_PATTERNS",
        {"workflow_permissions_write_all": _re.compile(r"permissions:\s*write-all")},
    )
    monkeypatch.setattr("src.github.prs.get_pr_diff", lambda repo, number: "clean\n")

    captured: list[tuple[DaemonConfig, RepoConfig]] = []

    def fake_scan(
        diff_text: str,
        *,
        daemon_config: DaemonConfig | None = None,
        repo_config: RepoConfig | None = None,
    ) -> list[Any]:
        assert diff_text == "clean\n"
        assert daemon_config is not None
        assert repo_config is not None
        captured.append((daemon_config, repo_config))
        return []

    monkeypatch.setattr(watch_module.guardrails, "scan_pr_diff", fake_scan)

    runner = h._make_runner()
    runner.app_config = h._app_cfg(
        large_diff_addition_threshold=250,
        large_diff_files_threshold=8,
    )
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(
        number=12,
        branch="pr-012",
        head_sha="snapshot1",
        diff_scanned_at_sha=None,
    )

    result = asyncio.run(runner._scan_pr_diff_once())

    assert result is True
    assert captured == [(runner.app_config.daemon, runner.repo_config)]
    assert captured[0][0].large_diff_addition_threshold == 250
    assert captured[0][0].large_diff_files_threshold == 8
    assert runner.state.current_pr.diff_scanned_at_sha == "snapshot1"


def test_handle_watch_invokes_diff_scan_once_per_cycle(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The WATCH cycle must call ``_scan_pr_diff_once`` exactly once per
    poll. With an empty catalogue this is a no-op that still updates
    the SHA cache so future cycles short-circuit cheaply."""
    pr = PRInfo(
        number=12,
        branch="pr-012",
        ci_status=CIStatus.PENDING,
        review_status=ReviewStatus.PENDING,
        last_activity=datetime.now(timezone.utc),
        head_sha="poll0001",
    )
    monkeypatch.setattr("src.github.prs.get_open_prs", lambda repo, **kw: [pr])

    invocations: list[int] = []

    async def fake_scan(self: Any) -> bool:
        invocations.append(1)
        return False

    monkeypatch.setattr(
        runner_module.PipelineRunner,
        "_scan_pr_diff_once",
        fake_scan,
    )

    runner = h._make_runner()
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=12, branch="pr-012")
    asyncio.run(runner.handle_watch())

    assert invocations == [1]


def test_handle_watch_returns_when_diff_scan_transitions_to_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """If the diff scan finds a violation and transitions to ERROR, the
    WATCH cycle must short-circuit; otherwise the trailing merge / fix
    / timeout dispatch could re-enter handling on a now-ERROR state."""
    pr = PRInfo(
        number=13,
        branch="pr-013",
        ci_status=CIStatus.SUCCESS,
        review_status=ReviewStatus.APPROVED,
        last_activity=datetime.now(timezone.utc),
        head_sha="violate1",
    )
    monkeypatch.setattr("src.github.prs.get_open_prs", lambda repo, **kw: [pr])

    async def fake_scan(self: Any) -> bool:
        self.state.state = PipelineState.ERROR
        return True

    monkeypatch.setattr(
        runner_module.PipelineRunner,
        "_scan_pr_diff_once",
        fake_scan,
    )

    merged: list[tuple[str, int]] = []
    monkeypatch.setattr(
        "src.github.prs.merge_pr",
        lambda repo, number, expected_head_sha: merged.append((repo, number)),
    )

    runner = h._make_runner()
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=13, branch="pr-013")
    asyncio.run(runner.handle_watch())

    assert merged == []
    assert runner.state.state == PipelineState.ERROR


def test_handle_watch_preserves_diff_scanned_at_sha_when_head_unchanged(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``get_open_prs`` returns a fresh ``PRInfo`` whose
    ``diff_scanned_at_sha`` defaults to ``None``. When the polled HEAD
    matches the in-memory HEAD, ``handle_watch`` must carry the cached
    SHA forward through ``model_copy`` so the ``_scan_pr_diff_once``
    cache gate keeps holding and ``gh pr diff`` is not re-run on every
    poll for an unchanged HEAD."""
    pr = PRInfo(
        number=21,
        branch="pr-021",
        ci_status=CIStatus.PENDING,
        review_status=ReviewStatus.PENDING,
        last_activity=datetime.now(timezone.utc),
        head_sha="stable01",
    )
    monkeypatch.setattr("src.github.prs.get_open_prs", lambda repo, **kw: [pr])

    observed: list[str | None] = []

    async def fake_scan(self: Any) -> bool:
        observed.append(self.state.current_pr.diff_scanned_at_sha)
        return False

    monkeypatch.setattr(
        runner_module.PipelineRunner,
        "_scan_pr_diff_once",
        fake_scan,
    )

    runner = h._make_runner()
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(
        number=21,
        branch="pr-021",
        head_sha="stable01",
        diff_scanned_at_sha="stable01",
    )
    asyncio.run(runner.handle_watch())

    assert observed == ["stable01"]
    assert runner.state.current_pr.diff_scanned_at_sha == "stable01"


def test_handle_watch_drops_diff_scanned_at_sha_when_head_changes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A fresh coder push (new HEAD SHA) must re-arm the diff scan:
    the cached ``diff_scanned_at_sha`` from the prior HEAD must NOT be
    carried forward, otherwise a prohibited diff introduced by the
    new push could slip past the catalogue."""
    pr = PRInfo(
        number=22,
        branch="pr-022",
        ci_status=CIStatus.PENDING,
        review_status=ReviewStatus.PENDING,
        last_activity=datetime.now(timezone.utc),
        head_sha="newhead2",
    )
    monkeypatch.setattr("src.github.prs.get_open_prs", lambda repo, **kw: [pr])

    observed: list[str | None] = []

    async def fake_scan(self: Any) -> bool:
        observed.append(self.state.current_pr.diff_scanned_at_sha)
        return False

    monkeypatch.setattr(
        runner_module.PipelineRunner,
        "_scan_pr_diff_once",
        fake_scan,
    )

    runner = h._make_runner()
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(
        number=22,
        branch="pr-022",
        head_sha="oldhead1",
        diff_scanned_at_sha="oldhead1",
    )
    asyncio.run(runner.handle_watch())

    assert observed == [None]
    assert runner.state.current_pr.diff_scanned_at_sha is None


def test_handle_watch_preserves_diff_scanned_at_sha_when_head_sha_empty_on_both_sides(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``PRInfo`` models a transiently empty ``head_sha`` for ``gh``
    payloads that omit the SHA. When both the in-memory and polled HEAD
    SHA are empty strings the HEAD has not changed, so the cache-carry
    logic must preserve ``diff_scanned_at_sha`` (equality alone, no
    truthy guard) — otherwise ``_scan_pr_diff_once`` would re-run
    ``gh pr diff`` on every WATCH cycle for the same PR state and, with
    populated diff guardrails, repeatedly hit fetch failures and hold
    merges noisily."""
    pr = PRInfo(
        number=23,
        branch="pr-023",
        ci_status=CIStatus.PENDING,
        review_status=ReviewStatus.PENDING,
        last_activity=datetime.now(timezone.utc),
        head_sha="",
    )
    monkeypatch.setattr("src.github.prs.get_open_prs", lambda repo, **kw: [pr])

    observed: list[str | None] = []

    async def fake_scan(self: Any) -> bool:
        observed.append(self.state.current_pr.diff_scanned_at_sha)
        return False

    monkeypatch.setattr(
        runner_module.PipelineRunner,
        "_scan_pr_diff_once",
        fake_scan,
    )

    runner = h._make_runner()
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(
        number=23,
        branch="pr-023",
        head_sha="",
        diff_scanned_at_sha="cachedsha",
    )
    asyncio.run(runner.handle_watch())

    assert observed == ["cachedsha"]
    assert runner.state.current_pr.diff_scanned_at_sha == "cachedsha"


def test_handle_watch_holds_merge_when_diff_fetch_fails_with_populated_patterns(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A transient ``gh pr diff`` failure on a merge-eligible cycle must
    NOT fall through to the green-path merge logic when ``_DIFF_PATTERNS``
    is populated. The diff scan is the only enforcement point for the
    diff catalogue; treating a fetch failure as equivalent to a clean
    scan would let a coder slip prohibited content past the catalogue by
    timing a ``gh`` outage with an otherwise-green PR.
    """
    import re as _re

    monkeypatch.setattr(
        watch_module.guardrails,
        "_DIFF_PATTERNS",
        {"workflow_permissions_write_all": _re.compile(r"permissions:\s*write-all")},
    )
    monkeypatch.setattr(
        watch_module.guardrails,
        "_DIFF_RULES",
        {"workflow_permissions_write_all": "Workflow permissions write-all"},
    )

    def boom(repo: str, number: int) -> str:
        raise RuntimeError("gh CLI timed out")

    monkeypatch.setattr("src.github.prs.get_pr_diff", boom)

    pr = PRInfo(
        number=33,
        branch="pr-033",
        ci_status=CIStatus.SUCCESS,
        review_status=ReviewStatus.APPROVED,
        last_activity=datetime.now(timezone.utc),
        head_sha="freshhd1",
    )
    monkeypatch.setattr("src.github.prs.get_open_prs", lambda repo, **kw: [pr])

    merged: list[tuple[str, int]] = []
    monkeypatch.setattr(
        "src.github.prs.merge_pr",
        lambda repo, number, expected_head_sha: merged.append((repo, number)),
    )

    runner = h._make_runner()
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(
        number=33,
        branch="pr-033",
        head_sha="freshhd1",
        diff_scanned_at_sha=None,
    )
    asyncio.run(runner.handle_watch())

    assert merged == []
    assert runner.state.state == PipelineState.WATCH
    assert runner.state.current_pr is not None
    assert runner.state.current_pr.diff_scanned_at_sha != "freshhd1"
    assert any(
        "merge held: diff scan did not complete" in entry.get("event", "")
        for entry in runner.state.history
    )












def _freeze_watch_datetime(
    monkeypatch: pytest.MonkeyPatch, now: datetime
) -> None:
    class _FrozenDateTime(datetime):
        @classmethod
        def now(cls, tz: timezone | None = None) -> datetime:
            return now if tz is None else now.astimezone(tz)

    monkeypatch.setattr(watch_module, "datetime", _FrozenDateTime)












def test_pending_retrigger_counter_resets_on_fresh_review_activity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Genuine GitHub event observed between WATCH cycles zeros the counter
    so the next stale cycle gets the full retry budget back."""
    now = datetime(2026, 5, 12, 12, 0, tzinfo=timezone.utc)
    pr = PRInfo(
        number=42,
        branch="pr-042-fix",
        ci_status=CIStatus.PENDING,
        review_status=ReviewStatus.EYES,
        last_activity=now,
        head_sha="freshhd",
        watch_retrigger_count=2,
    )
    monkeypatch.setattr(
        "src.github.prs.get_open_prs",
        lambda repo, **kw: [pr],
    )
    monkeypatch.setattr(
        "src.github.cache._gh_api_paginated",
        lambda path: [],
    )
    monkeypatch.setattr(
        "src.github.prs.get_last_push_age_seconds",
        lambda repo, number: 60,
    )
    _freeze_watch_datetime(monkeypatch, now)

    runner = h._make_runner(review_timeout_min=120)
    runner.app_config.daemon.hung_fallback_codex_review = True
    runner.app_config.daemon.stale_review_threshold_eyes_min = 5
    runner.app_config.daemon.watch_retrigger_cap = 3
    runner.state.current_pr = pr
    runner.state.state = PipelineState.WATCH
    runner._last_push_at = now - timedelta(minutes=12)
    runner._last_push_at_pr_number = pr.number
    runner._watch_last_event_signature = (
        42,
        CIStatus.PENDING,
        ReviewStatus.PENDING,
        now - timedelta(minutes=20),
    )

    asyncio.run(runner.handle_watch())

    assert runner.state.current_pr is not None
    assert runner.state.current_pr.watch_retrigger_count == 0







@pytest.mark.parametrize("case", ["creation", "later_push", "wrong_head", "missing", "backward"])
def test_recorded_branch_creation_review_reaches_watch_merge_handoff(
    monkeypatch: pytest.MonkeyPatch, case: str,
) -> None:
    """Only proven publication preceding a trusted +1 reaches WATCH merge."""
    head = "63f07ef48a035c9aa31c37e0fbf3dc215ba0045e"
    branch = "pr-459-e2e-failure-evidence"
    created = {
        "activity_type": "branch_creation", "after": head, "before": "0" * 40,
        "ref": f"refs/heads/{branch}", "timestamp": "2026-10-17T21:06:35Z",
    }
    activity = [created]
    if case == "later_push":
        activity.insert(0, {**created, "activity_type": "push", "before": head,
                            "timestamp": "2026-10-17T21:10:00Z"})
    elif case == "wrong_head":
        activity[0] = {**created, "after": "b" * 40}
    elif case == "missing":
        activity.clear()

    def run_gh(args: list[str], **_kwargs: object) -> object:
        if "/pulls/" in args[1]:
            return {"branch": branch, "repo": "octo/demo", "sha": head}
        if "/activity?" in args[1]:
            return activity
        raise AssertionError(args)

    actor = {"id": 199175422, "login": "chatgpt-codex-connector[bot]", "type": "Bot"}
    def paginated(path: str) -> list[dict]:
        if path.endswith("/issues/620/reactions"):
            return [{"content": "+1", "user": actor,
                     "created_at": "2026-10-17T21:08:48Z"}]
        return []

    monkeypatch.setattr("src.github.gh_runner.run_gh", run_gh)
    monkeypatch.setattr(gh_cache, "_gh_api_paginated", paginated)
    monkeypatch.setattr(watch_module.gh_prs, "get_pr_state", lambda *_: "OPEN")
    gh_reviews.clear_review_status_cache()
    def open_prs(repo: str, **kwargs: object) -> list[PRInfo]:
        publication = watch_module.gh_prs.get_pr_head_publication(
            repo, 620, expected_head_sha=head
        )
        review = gh_reviews.get_pr_review_status(
            repo, 620, "author", head, policy=kwargs.get("reviewer_policy"),
            publication_evidence=publication,
        )
        return [PRInfo(
            number=620, branch=branch, head_sha=head, ci_status=CIStatus.SUCCESS,
            review_status=review, diff_scanned_at_sha=head,
            publication_observed=True, publication_at=publication.published_at,
            publication_reason=publication.unknown_reason,
            publication_head_repo=publication.head_repo,
            publication_ref=publication.ref,
            publication_repo=publication.repo,
            publication_head_sha=publication.head_sha,
        )]
    monkeypatch.setattr(watch_module.gh_prs, "get_open_prs", open_prs)
    runner = h._make_runner(
        auto_merge=True,
        allow_merge_without_review=case in {"wrong_head", "missing"},
    )
    runner.state.current_pr = PRInfo(number=620, branch=branch, head_sha=head,
                                     diff_scanned_at_sha=head)
    runner.state.state = PipelineState.WATCH
    if case == "backward":
        floor = datetime(2026, 10, 17, 21, 7, tzinfo=timezone.utc)
        runner._last_push_at = floor
        runner._last_push_at_pr_number = 620
        runner._last_push_at_head_sha = head
        runner._publication_high_water_identity = (
            runner.owner_repo, 620, "octo/demo", f"refs/heads/{branch}", head,
        )
        runner._publication_high_water_at = floor
    calls: list[str] = []
    async def merge() -> None:
        calls.append("merge")
    async def scan() -> None:
        return None
    async def reclassify(_pr: PRInfo) -> None:
        return None
    monkeypatch.setattr(runner, "handle_merge", merge)
    monkeypatch.setattr(runner, "_scan_pr_diff_once", scan)
    monkeypatch.setattr(runner, "_maybe_reclassify_stuck_pending", reclassify)
    asyncio.run(runner.handle_watch())
    assert calls == (["merge"] if case == "creation" else [])
    assert runner.state.current_pr.review_status == (
        ReviewStatus.APPROVED if case == "creation" else ReviewStatus.PENDING
    )
    if case == "backward":
        assert runner.state.current_pr.publication_at is None
        assert runner.state.current_pr.publication_reason == "publication time moved backward for unchanged branch"
        asyncio.run(runner.handle_watch())
        assert calls == []
        assert runner.state.current_pr.publication_at is None


@pytest.mark.parametrize("case", ["creation", "advanced", "missing", "contradictory"])
def test_parent_snapshot_620_recovery_reaches_normal_watch_merge_handoff(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, case: str,
) -> None:
    """Upgrade, current activity, trusted reaction and green CI compose."""
    repo = "AlexBomber12/pipeline-orchestrator"
    branch = "pr-459-e2e-failure-evidence"
    saved_head = "63f07ef48a035c9aa31c37e0fbf3dc215ba0045e"
    head = "b" * 40 if case == "advanced" else saved_head
    snapshot = (Path(__file__).parents[1] / "fixtures/recovery/pr_620_parent_snapshot.json").read_text()
    activity = [{
        "activity_type": "branch_creation", "after": head, "before": "0" * 40,
        "ref": f"refs/heads/{branch}", "timestamp": "2026-10-17T21:06:35Z",
    }]
    if case == "missing":
        activity.clear()
    elif case == "advanced":
        activity[0].update(
            activity_type="push", before=saved_head, timestamp="2026-10-17T21:07:00Z"
        )
    elif case == "contradictory":
        activity[0]["after"] = "b" * 40

    def run_gh(args: list[str], **_kwargs: object) -> object:
        if "/pulls/" in args[1]:
            return {"branch": branch, "repo": repo, "sha": head}
        if "/activity?" in args[1]:
            return activity
        raise AssertionError(args)

    actor = {"id": 199175422, "login": "chatgpt-codex-connector[bot]", "type": "Bot"}

    def paginated(path: str) -> list[dict]:
        if path.endswith("/issues/620/reactions"):
            return [{"content": "+1", "user": actor,
                     "created_at": "2026-10-17T21:08:48Z"}]
        return []

    monkeypatch.setattr("src.github.gh_runner.run_gh", run_gh)
    monkeypatch.setattr(gh_cache, "_gh_api_paginated", paginated)
    monkeypatch.setattr(watch_module.gh_prs, "get_pr_state", lambda *_: "OPEN")
    gh_reviews.clear_review_status_cache()

    def open_prs(owner: str, *_args: object, **kwargs: object) -> list[PRInfo]:
        publication = watch_module.gh_prs.get_pr_head_publication(
            owner, 620, expected_head_sha=head
        )
        review = gh_reviews.get_pr_review_status(
            owner, 620, "author", head, policy=kwargs.get("reviewer_policy"),
            publication_evidence=publication,
        )
        return [PRInfo(
            number=620, branch=branch, title="PR-459: e2e failure evidence",
            pr_id="PR-459", head_sha=head, ci_status=CIStatus.SUCCESS,
            review_status=review, publication_observed=True,
            diff_scanned_at_sha=head,
            publication_at=publication.published_at,
            publication_reason=publication.unknown_reason,
            publication_head_repo=publication.head_repo,
            publication_ref=publication.ref, publication_repo=publication.repo,
            publication_head_sha=publication.head_sha,
            publication_provenance=publication.provenance,
        )]

    monkeypatch.setattr(watch_module.gh_prs, "get_open_prs", open_prs)
    runner = h._make_runner(
        url=f"https://github.com/{repo}.git", auto_merge=True,
        allow_merge_without_review=case not in {"creation", "advanced"},
    )
    runner.repo_path = str(tmp_path)
    task = QueueTask(pr_id="PR-459", title="e2e failure evidence", status=TaskStatus.DOING,
                     branch=branch, task_file="tasks/PR-459.md")
    monkeypatch.setattr(runner, "_parse_tasks_from_headers", lambda: [task])
    asyncio.run(runner.redis.set(pipeline_state(runner.name), snapshot))
    merged: list[int] = []

    async def merge() -> None:
        merged.append(620)

    async def no_work(*_args: object) -> None:
        return None

    async def scan() -> None:
        runner.state.current_pr.diff_scanned_at_sha = head

    monkeypatch.setattr(runner, "handle_merge", merge)
    monkeypatch.setattr(runner, "_scan_pr_diff_once", scan)
    monkeypatch.setattr(runner, "_maybe_reclassify_stuck_pending", no_work)
    assert asyncio.run(runner.recover_state()) is True
    assert runner.state.publication_watermark is None
    assert runner.state.publication_migration is not None
    if case == "advanced":
        assert runner.state.publication_migration.head_sha == head
        assert runner.state.publication_migration.published_at == datetime(
            2026, 10, 17, 21, 7, tzinfo=timezone.utc
        )
    asyncio.run(runner.handle_watch())
    assert merged == []
    if case in {"creation", "advanced"}:
        asyncio.run(runner.publish_state())
        binding = migration_binding(runner.state.publication_migration)
        asyncio.run(enqueue_migration(runner.redis, runner.name, binding))
        assert asyncio.run(runner._consume_publication_migration_command()) is True
        assert runner.state.publication_watermark is not None, runner.state.history[-3:]
        asyncio.run(runner.handle_watch())
    assert runner.state.current_pr.review_status == (
        ReviewStatus.APPROVED if case in {"creation", "advanced"} else ReviewStatus.PENDING
    )
    assert merged == ([620] if case in {"creation", "advanced"} else []), (
        runner.state.state, runner.state.current_pr.ci_status,
        runner.state.current_pr.publication_at, runner.state.current_pr.publication_reason,
        [item["event"] for item in runner.state.history[-5:]],
    )


@pytest.mark.parametrize("change", ["same", "restart", "head", "ref", "head_repo", "event"])
def test_deferred_publication_command_survives_unknown_watch_poll(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, change: str,
) -> None:
    """A queued exact command makes progress through ordinary runner cycles."""
    from src.github import prs as gh_prs

    repo = "AlexBomber12/pipeline-orchestrator"
    branch = "pr-459-e2e-failure-evidence"
    head = "a" * 40
    published = datetime(2026, 10, 17, 21, 6, 35, tzinfo=timezone.utc)
    current = {
        "activity": False, "ci": CIStatus.SUCCESS, "watch_polls": 0,
        "head": head, "branch": branch, "head_repo": repo,
        "timestamp": "2026-10-17T21:06:35Z",
    }

    def run_gh(args: list[str], **_kwargs: object) -> object:
        if "/pulls/620" in args[1]:
            return {"branch": current["branch"], "repo": current["head_repo"],
                    "sha": current["head"]}
        if "/activity?" in args[1]:
            return ([{"activity_type": "push", "ref": f"refs/heads/{current['branch']}",
                     "before": "0" * 40, "after": current["head"],
                     "timestamp": current["timestamp"]}]
                    if current["activity"] else [])
        raise AssertionError(args)

    def open_prs(owner: str, *_args: object, **_kwargs: object) -> list[PRInfo]:
        current["watch_polls"] += 1
        evidence = gh_prs.get_pr_head_publication(
            owner, 620, expected_head_sha=current["head"]
        )
        return [PRInfo(
            number=620, branch=current["branch"], pr_id="PR-459", head_sha=current["head"],
            ci_status=current["ci"], review_status=ReviewStatus.APPROVED,
            is_cross_repository=current["head_repo"] != repo,
            publication_observed=True, publication_at=evidence.published_at,
            publication_reason=evidence.unknown_reason,
            publication_head_repo=evidence.head_repo, publication_ref=evidence.ref,
            publication_repo=evidence.repo, publication_head_sha=evidence.head_sha,
            publication_provenance=evidence.provenance,
        )]

    monkeypatch.setattr("src.github.gh_runner.run_gh", run_gh)
    monkeypatch.setattr(gh_prs, "get_open_prs", open_prs)
    monkeypatch.setattr(gh_prs, "get_pr_state", lambda *_args: "OPEN")
    runner = h._make_runner(url=f"https://github.com/{repo}.git",
                            auto_merge=True, allow_merge_without_review=True)
    runner.repo_path = str(tmp_path)
    runner.state.state = PipelineState.WATCH
    runner.state.current_task = QueueTask(
        pr_id="PR-459", title="e2e failure evidence", status=TaskStatus.DOING,
        branch=branch, task_file="tasks/PR-459.md",
    )
    runner.state.current_pr = open_prs(repo)[0]
    runner.state.publication_migration = PublicationMigration(
        repo=repo, pr_number=620, head_repo=repo, ref=f"refs/heads/{branch}",
        head_sha=head, published_at=published, provenance="github-activity:push",
        reason="Confirm initial publication.",
    )
    runner._publication_recovery_cutoff_at = datetime.now(timezone.utc)
    runner._recovered = True
    binding = migration_binding(runner.state.publication_migration)
    asyncio.run(runner.publish_state())
    original_publish = runner.publish_state
    queued = False

    async def publish_with_interleaved_confirmation() -> None:
        nonlocal queued
        if not queued and runner.state.publication_migration.published_at is None:
            queued = True
            await enqueue_migration(runner.redis, runner.name, binding)
        await original_publish()

    async def no_work(*_args: object) -> None:
        return None

    async def yes() -> bool:
        return True

    fixes: list[str] = []
    retry_checks: list[str] = []

    async def fix() -> None:
        fixes.append("ci")

    async def retry() -> runner_module.RetryDispatch:
        retry_checks.append("checked")
        return runner_module.RetryDispatch.NONE

    monkeypatch.setattr(runner, "publish_state", publish_with_interleaved_confirmation)
    monkeypatch.setattr(runner, "ensure_repo_cloned", no_work)
    monkeypatch.setattr(runner, "_check_github_api_budget", yes)
    monkeypatch.setattr(runner, "preflight", yes)
    monkeypatch.setattr(runner, "_maybe_reclassify_stuck_pending", no_work)
    monkeypatch.setattr(runner, "_scan_pr_diff_once", no_work)
    monkeypatch.setattr(runner, "handle_fix", fix)
    monkeypatch.setattr(runner, "_consume_retry_command", retry)
    monkeypatch.setattr(runner, "handle_merge", lambda: (_ for _ in ()).throw(AssertionError("merge")))

    asyncio.run(runner.run_cycle())
    assert queued
    assert runner.state.publication_migration.published_at is None
    key = migration_command_key(runner.name, binding)
    assert MigrationCommand.model_validate_json(runner.redis.store[key]).status == "pending"
    current["ci"] = CIStatus.FAILURE
    polls_before = current["watch_polls"]
    retry_checks_before = len(retry_checks)
    asyncio.run(runner.run_cycle())
    assert MigrationCommand.model_validate_json(runner.redis.store[key]).status == "deferred"
    assert current["watch_polls"] > polls_before
    assert len(retry_checks) > retry_checks_before
    assert fixes == ["ci"]
    assert runner.state.publication_watermark is None
    asyncio.run(runner.run_cycle())
    assert MigrationCommand.model_validate_json(runner.redis.store[key]).status == "deferred"
    assert fixes == ["ci", "ci"]
    assert runner.state.publication_watermark is None

    if change == "restart":
        restarted = h._make_runner(url=f"https://github.com/{repo}.git")
        restarted.redis = runner.redis
        restarted.repo_path = str(tmp_path)
        restarted._parse_tasks_from_headers = lambda: [runner.state.current_task]  # type: ignore[method-assign]
        assert asyncio.run(restarted.recover_state()) is True
        assert restarted.state.publication_migration is not None
        assert restarted.state.publication_migration.published_at is None
        runner = restarted
    current["activity"] = True
    current["ci"] = CIStatus.SUCCESS
    if change == "head":
        current["head"] = "b" * 40
        current["timestamp"] = "2026-10-17T21:07:00Z"
    elif change == "ref":
        current["branch"] = "other-branch"
    elif change == "head_repo":
        current["head_repo"] = "other/repo"
    elif change == "event":
        current["timestamp"] = "2026-10-17T21:07:00Z"
    assert asyncio.run(runner._consume_publication_migration_command()) is True
    receipt = MigrationCommand.model_validate_json(runner.redis.store[key])
    if change in {"same", "restart"}:
        assert receipt.status == "applied"
        assert runner.state.publication_watermark is not None
        assert runner.state.publication_watermark.published_at == published
    else:
        assert receipt.status == "failed"
        assert runner.state.publication_watermark is None
        if change != "restart":
            asyncio.run(runner.run_cycle())
            proposal = runner.state.publication_migration
            if change == "ref":
                assert proposal.status == "blocked"
            else:
                assert proposal.status == "legacy"
                assert proposal.published_at is not None
                replacement_binding = migration_binding(proposal)
                assert replacement_binding != binding
                asyncio.run(enqueue_migration(runner.redis, runner.name, replacement_binding))
                assert asyncio.run(runner._consume_publication_migration_command()) is True
                assert runner.state.publication_watermark is not None
                assert runner.state.publication_watermark.head_sha == current["head"]
                assert runner.state.publication_watermark.head_repo == current["head_repo"]
                assert MigrationCommand.model_validate_json(
                    runner.redis.store[migration_command_key(runner.name, replacement_binding)]
                ).status == "applied"


def test_contextless_failure_cannot_erase_publication_watermark(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A later stale read cannot approve after a contextless transport failure."""
    head = "63f07ef48a035c9aa31c37e0fbf3dc215ba0045e"
    branch = "pr-459-e2e-failure-evidence"
    created = {
        "activity_type": "branch_creation", "before": "0" * 40, "after": head,
        "ref": f"refs/heads/{branch}", "timestamp": "2026-10-17T21:06:35Z",
    }
    newer = {**created, "activity_type": "push", "timestamp": "2026-10-17T21:10:00Z"}
    mode = {"value": "new"}

    def run_gh(args: list[str], **_kw: object) -> object:
        if "/pulls/" in args[1]:
            if mode["value"] == "unavailable":
                raise RuntimeError("transport unavailable")
            return {"branch": branch, "repo": "octo/demo", "sha": head}
        if "/activity?" in args[1]:
            return [newer] if mode["value"] == "new" else [created]
        raise AssertionError(args)

    actor = {"id": 199175422, "login": "chatgpt-codex-connector[bot]", "type": "Bot"}
    def paginated(path: str) -> list[dict]:
        if path.endswith("/issues/620/reactions"):
            return [{"content": "+1", "user": actor,
                     "created_at": "2026-10-17T21:08:48Z"}]
        return []

    def open_prs(repo: str, **kwargs: object) -> list[PRInfo]:
        gh_reviews.clear_review_status_cache()
        publication = watch_module.gh_prs.get_pr_head_publication(
            repo, 620, expected_head_sha=head
        )
        review = gh_reviews.get_pr_review_status(
            repo, 620, "author", head, policy=kwargs.get("reviewer_policy"),
            publication_evidence=publication,
        )
        return [PRInfo(
            number=620, branch=branch, head_sha=head, ci_status=CIStatus.SUCCESS,
            review_status=review, diff_scanned_at_sha=head,
            publication_observed=True, publication_at=publication.published_at,
            publication_reason=publication.unknown_reason,
            publication_head_repo=publication.head_repo, publication_ref=publication.ref,
            publication_repo=publication.repo, publication_head_sha=publication.head_sha,
        )]

    monkeypatch.setattr("src.github.gh_runner.run_gh", run_gh)
    monkeypatch.setattr(gh_cache, "_gh_api_paginated", paginated)
    monkeypatch.setattr(watch_module.gh_prs, "get_open_prs", open_prs)
    monkeypatch.setattr(watch_module.gh_prs, "get_pr_state", lambda *_: "OPEN")
    runner = h._make_runner(auto_merge=True)
    runner.state.current_pr = PRInfo(number=620, branch=branch, head_sha=head,
                                     diff_scanned_at_sha=head)
    runner.state.state = PipelineState.WATCH
    merged: list[int] = []
    async def merge() -> None:
        merged.append(620)
    async def no_work(*_args: object) -> None:
        return None
    monkeypatch.setattr(runner, "handle_merge", merge)
    monkeypatch.setattr(runner, "_scan_pr_diff_once", no_work)
    monkeypatch.setattr(runner, "_maybe_reclassify_stuck_pending", no_work)

    floor = datetime(2026, 10, 17, 21, 10, tzinfo=timezone.utc)
    for value in ("new", "unavailable", "old"):
        mode["value"] = value
        asyncio.run(runner.handle_watch())
        assert merged == []
        assert runner._publication_high_water_at == floor
        assert runner.state.current_pr.review_status == ReviewStatus.PENDING
    assert runner.state.current_pr.publication_at is None
    assert runner.state.current_pr.publication_reason == "publication time moved backward for unchanged branch"


def _publication_safety_boundary(monkeypatch: pytest.MonkeyPatch):
    """Exercise the real reader, review classifier, WATCH, and recovery boundary."""
    head = "63f07ef48a035c9aa31c37e0fbf3dc215ba0045e"
    branch = "pr-459-e2e-failure-evidence"
    floor = datetime(2026, 10, 17, 21, 10, tzinfo=timezone.utc)
    mode = {"value": "new"}
    reaction_at = {"value": "2026-10-17T21:08:48Z"}
    def run_gh(args: list[str], **_kwargs: object) -> object:
        if "/pulls/" in args[1]:
            if mode["value"] == "unavailable":
                raise RuntimeError("transport unavailable")
            return {"branch": branch, "repo": "octo/demo", "sha": head}
        if "/activity?" in args[1]:
            timestamp = "2026-10-17T21:10:00Z" if mode["value"] == "new" else "2026-10-17T21:06:35Z"
            return [{"activity_type": "branch_creation", "before": "0" * 40,
                     "after": head, "ref": f"refs/heads/{branch}", "timestamp": timestamp}]
        raise AssertionError(args)
    def paginated(path: str) -> list[dict]:
        if path.endswith("/issues/620/reactions"):
            return [{"content": "+1", "created_at": reaction_at["value"],
                     "user": {"id": 199175422, "login": "chatgpt-codex-connector[bot]", "type": "Bot"}}]
        return []
    def open_prs(repo: str, **kwargs: object) -> list[PRInfo]:
        gh_reviews.clear_review_status_cache()
        publication = watch_module.gh_prs.get_pr_head_publication(
            repo, 620, expected_head_sha=head,
        )
        review = gh_reviews.get_pr_review_status(
            repo, 620, "author", head, policy=kwargs.get("reviewer_policy"),
            publication_evidence=publication,
        )
        return [PRInfo(
            number=620, branch=branch, head_sha=head, ci_status=CIStatus.SUCCESS,
            review_status=review, diff_scanned_at_sha=head,
            publication_observed=True, publication_at=publication.published_at,
            publication_reason=publication.unknown_reason,
            publication_head_repo=publication.head_repo, publication_ref=publication.ref,
            publication_repo=publication.repo, publication_head_sha=publication.head_sha,
        )]
    monkeypatch.setattr("src.github.gh_runner.run_gh", run_gh)
    monkeypatch.setattr(gh_cache, "_gh_api_paginated", paginated)
    monkeypatch.setattr(watch_module.gh_prs, "get_open_prs", open_prs)
    monkeypatch.setattr(watch_module.gh_prs, "get_pr_state", lambda *_: "OPEN")
    runner = h._make_runner(auto_merge=True)
    runner.state.current_pr = PRInfo(number=620, branch=branch, head_sha=head,
                                     diff_scanned_at_sha=head)
    runner.state.state = PipelineState.WATCH
    merged: list[int] = []
    async def merge() -> None:
        merged.append(620)
    async def no_work(*_args: object) -> None:
        return None
    monkeypatch.setattr(runner, "handle_merge", merge)
    monkeypatch.setattr(runner, "_scan_pr_diff_once", no_work)
    monkeypatch.setattr(runner, "_maybe_reclassify_stuck_pending", no_work)
    return runner, mode, reaction_at, floor, merged, no_work, head, branch


@pytest.mark.parametrize("helper", ["eyes", "author_post"])
def test_auxiliary_failed_read_keeps_publication_floor(
    monkeypatch: pytest.MonkeyPatch, helper: str,
) -> None:
    from src.daemon import git_ops
    from src.daemon.handlers import hung as hung_module
    from src.github import reactions

    runner, mode, _reaction_at, floor, merged, _no_work, head, _branch = (
        _publication_safety_boundary(monkeypatch)
    )
    asyncio.run(runner.handle_watch())
    assert runner._publication_high_water_at == floor
    mode["value"] = "unavailable"
    if helper == "eyes":
        original_reactions = reactions._get_codex_issue_reactions
        monkeypatch.setattr(reactions, "_get_codex_issue_reactions", lambda *a, **kw: [
            {"content": "eyes", "created_at": "2026-10-17T21:08:48Z",
             "user": {"id": 199175422, "login": "chatgpt-codex-connector[bot]", "type": "Bot"}},
        ])
        assert runner._should_skip_codex_review_post(620) is False
        monkeypatch.setattr(reactions, "_get_codex_issue_reactions", original_reactions)
    else:
        posted: list[str] = []
        monkeypatch.setattr(git_ops, "_git", lambda *a, **kw: h._FakeCompletedProcess(
            args=[], stdout=head + "\n", returncode=0,
        ))
        monkeypatch.setattr(watch_module.gh_prs, "get_pr_metadata",
                            lambda *a: {"author": "author"})
        monkeypatch.setattr(hung_module.gh_comments, "post_comment",
                            lambda _repo, _number, body: posted.append(body))
        assert runner._post_codex_review_result(620)[0] is True
        assert posted == ["@codex review"]
    assert runner._publication_high_water_at == floor
    mode["value"] = "old"
    for _ in range(2):
        asyncio.run(runner.handle_watch())
        assert runner.state.current_pr.review_status == ReviewStatus.PENDING
        assert runner.state.current_pr.publication_at is None
        assert runner._publication_high_water_at == floor
        assert merged == []


@pytest.mark.parametrize("unknown_before_save", [False, True])
def test_recovery_restores_publication_floor_before_stale_watch(
    monkeypatch: pytest.MonkeyPatch, unknown_before_save: bool,
) -> None:
    runner, mode, reaction_at, floor, _merged, no_work, head, branch = (
        _publication_safety_boundary(monkeypatch)
    )
    task = QueueTask(pr_id="PR-459", title="publication safety", status=TaskStatus.DOING,
                     branch=branch)
    runner.state.current_task = task
    asyncio.run(runner.handle_watch())
    assert runner._publication_high_water_at == floor
    if unknown_before_save:
        mode["value"] = "unavailable"
        asyncio.run(runner.handle_watch())
        assert runner.state.current_pr.publication_at is None
    asyncio.run(runner.publish_state())
    raw = asyncio.run(runner.redis.get(pipeline_state(runner.name)))
    persisted = RepoState.model_validate_json(raw)
    assert persisted.publication_watermark is not None
    assert persisted.publication_watermark.published_at == floor
    if unknown_before_save:
        assert persisted.current_pr.publication_at is None
    recovered = h._make_runner(auto_merge=True)
    recovered.redis = runner.redis
    monkeypatch.setattr(recovered, "_parse_tasks_from_headers", lambda: [task])
    merged: list[int] = []
    async def merge() -> None:
        merged.append(620)
    monkeypatch.setattr(recovered, "handle_merge", merge)
    monkeypatch.setattr(recovered, "_scan_pr_diff_once", no_work)
    monkeypatch.setattr(recovered, "_maybe_reclassify_stuck_pending", no_work)
    mode["value"] = "old"
    assert asyncio.run(recovered.recover_state()) is True, (
        recovered.state.error_message, recovered.state.history,
    )
    assert recovered.state.state == PipelineState.WATCH
    assert recovered._publication_high_water_at == floor
    assert recovered._last_push_at is None
    asyncio.run(recovered.handle_watch())
    assert recovered.state.current_pr.review_status == ReviewStatus.PENDING
    assert recovered.state.current_pr.publication_at is None
    assert merged == []
    # The saved floor is only a veto. A fresh matching publication and a
    # later reaction can still follow the ordinary merge path.
    mode["value"] = "new"
    reaction_at["value"] = "2026-10-17T21:11:00Z"
    asyncio.run(recovered.handle_watch())
    assert recovered.state.current_pr.review_status == ReviewStatus.APPROVED
    assert merged == [620]
    assert head == recovered.state.current_pr.head_sha


@pytest.mark.parametrize("damage", [
    "missing", "wrong_repo", "wrong_pr", "malformed_head", "wrong_ref",
    "wrong_head_repo", "missing_context",
    "naive_timestamp", "malformed_timestamp",
])
def test_recovery_without_usable_floor_holds_historical_approval(
    monkeypatch: pytest.MonkeyPatch, damage: str,
) -> None:
    runner, mode, _reaction_at, floor, _merged, no_work, head, branch = (
        _publication_safety_boundary(monkeypatch)
    )
    task = QueueTask(pr_id="PR-459", title="publication safety", status=TaskStatus.DOING,
                     branch=branch)
    runner.state.current_task = task
    asyncio.run(runner.handle_watch())
    assert runner._publication_high_water_at == floor
    asyncio.run(runner.publish_state())
    key = pipeline_state(runner.name)
    raw = asyncio.run(runner.redis.get(key))
    snapshot = json.loads(raw)
    watermark = snapshot["publication_watermark"]
    if damage == "missing":
        asyncio.run(runner.redis.delete(key))
    else:
        if damage == "wrong_repo":
            watermark["repo"] = "other/repo"
        elif damage == "wrong_pr":
            watermark["pr_number"] = 621
        elif damage == "malformed_head":
            watermark["head_sha"] = "not-a-sha"
        elif damage == "wrong_ref":
            watermark["ref"] = "refs/heads/other"
        elif damage == "wrong_head_repo":
            watermark["head_repo"] = "other/repo"
        elif damage == "missing_context":
            watermark["head_repo"] = ""
        elif damage == "naive_timestamp":
            watermark["published_at"] = "2026-10-17T21:10:00"
        else:
            watermark["published_at"] = "not a timestamp"
        snapshot["current_pr"]["publication_at"] = None
        asyncio.run(runner.redis.set(key, json.dumps(snapshot)))
    recovered = h._make_runner(auto_merge=True)
    recovered.redis = runner.redis
    monkeypatch.setattr(recovered, "_parse_tasks_from_headers", lambda: [task])
    merged: list[int] = []
    async def merge() -> None:
        merged.append(620)
    monkeypatch.setattr(recovered, "handle_merge", merge)
    monkeypatch.setattr(recovered, "_scan_pr_diff_once", no_work)
    monkeypatch.setattr(recovered, "_maybe_reclassify_stuck_pending", no_work)
    mode["value"] = "old"
    assert asyncio.run(recovered.recover_state()) is True
    assert recovered._last_push_at is None
    assert recovered.state.publication_watermark is None
    asyncio.run(recovered.handle_watch())
    assert recovered.state.current_pr.review_status == ReviewStatus.PENDING
    assert recovered.state.current_pr.publication_at is None
    assert merged == []
