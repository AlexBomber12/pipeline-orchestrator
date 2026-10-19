"""PR-212: Recovery interaction regression tests.

The daemon has multiple defense-in-depth recovery layers — preflight
dirty-tree auto-reset, ``recover_state``'s crashed-task marker, and the
three FIX-cycle escalation primitives in ``fix.py`` — each owning a
distinct counter, threshold, and terminal state. They are coordinated
by ordering in ``run_cycle`` rather than an explicit contract, so a
single incident can fire more than one layer.

These four tests pin the **current** layer composition before PR-220
replaces the three ``_escalate_fix_*`` methods with a single
``_escalate_and_skip`` primitive. Each test asserts the terminal state,
the side effect (label apply, comment post, ``is_escalated`` flag)
and the operator-visible log line so the upcoming refactor preserves
behavior verbatim:

- Test 1 — dirty-tree auto-reset composes with recovery's crashed-task
  marker without colliding (terminal state IDLE, both events logged).
- Test 2 — no-push deadlock parks in HUNG and the
  ``hung_fallback_codex_review`` path does not re-fire ``@codex review``
  thanks to the ``is_escalated`` guard in ``handle_hung``.
- Test 3 — coder ESCALATE with a label-apply failure parks in HUNG
  (not IDLE) so the in-memory ``is_escalated`` flag remains the
  load-bearing parking signal during a GitHub outage.
- Test 4 — iteration-cap reached with a label-apply failure parks in
  ERROR (the iteration-cap site has stricter durability requirements
  than the coder-initiated path because it is daemon-driven, not
  coder-self-reported).

Tests do not change production behavior; they only assert it.
"""

from __future__ import annotations

import asyncio
import json

# PR-224a: imports needed by tests moved from tests/test_runner.py
import random  # noqa: F401
import re  # noqa: F401
import subprocess
import time  # noqa: F401
import types  # noqa: F401
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pytest
from src.daemon import recovery as recovery_module
from src.daemon import runner as runner_module
from src.daemon.handlers import coding as coding_module  # noqa: F401,F811
from src.daemon.handlers import error as error_module  # noqa: F401,F811
from src.daemon.handlers import idle as idle_module  # noqa: F811
from src.daemon.handlers import merge as merge_module  # noqa: F401,F811
from src.daemon.handlers import watch as watch_module  # noqa: F401,F811
from src.github import prs as gh_prs
from src.keyspace import pipeline_state
from src.models import (
    PipelineState,  # noqa: F811
    PRInfo,  # noqa: F811
    PublicationWatermark,
    QueueTask,  # noqa: F811
    RepoState,
    TaskStatus,  # noqa: F811
)
from src.publication_migration import (
    MigrationCommand,
    migration_binding,
)
from src.publication_migration import (
    command_key as migration_command_key,
)
from src.publication_migration import (
    enqueue as enqueue_migration,
)
from src.publication_migration import pending_key as migration_pending_key
from src.task_status import MergedState

from tests.runner import _helpers as h


def test_recovery_retry_keeps_initial_publication_cutoff(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = h._make_runner()
    monkeypatch.setattr(runner, "_cleanup_expected_branch", lambda: None)

    class RecoveryClock:
        calls = 0

        @classmethod
        def now(cls, _tz: timezone) -> datetime:
            cls.calls += 1
            return datetime(2026, 10, 18, 19, tzinfo=timezone.utc) + timedelta(
                minutes=cls.calls,
            )

    monkeypatch.setattr(recovery_module, "datetime", RecoveryClock)

    async def unavailable() -> bool:
        return False

    monkeypatch.setattr(runner, "_recover_state_headers", unavailable)
    assert asyncio.run(runner.recover_state()) is False
    first_cutoff = runner._publication_recovery_cutoff_at
    assert first_cutoff is not None

    assert asyncio.run(runner.recover_state()) is False
    assert runner._publication_recovery_cutoff_at == first_cutoff
    head = "a" * 40
    assert runner._validate_publication_observation(
        42, head, first_cutoff + timedelta(seconds=30), None,
        head_repo=runner.owner_repo, ref="refs/heads/feature",
        observed_repo=runner.owner_repo, observed_head_sha=head,
    ) == (first_cutoff + timedelta(seconds=30), None)
    assert runner._publication_recovery_cutoff_at == first_cutoff
    assert asyncio.run(runner.recover_state()) is False
    assert runner._publication_recovery_cutoff_at == first_cutoff


def test_parent_snapshot_620_bootstraps_exact_branch_creation(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    """Real parent serialization enters recovery before current activity is read."""
    snapshot = (Path(__file__).parents[1] / "fixtures/recovery/pr_620_parent_snapshot.json").read_text()
    raw = json.loads(snapshot)
    assert "publication_watermark" not in raw
    assert "publication_observed" not in raw["current_pr"]
    head = "63f07ef48a035c9aa31c37e0fbf3dc215ba0045e"
    branch = "pr-459-e2e-failure-evidence"
    repo = "AlexBomber12/pipeline-orchestrator"
    published = datetime(2026, 10, 17, 21, 6, 35, tzinfo=timezone.utc)
    pr = PRInfo(number=620, branch=branch, title="PR-459: e2e failure evidence",
                pr_id="PR-459", head_sha=head)

    def transport(args: list[str], **_kwargs: object) -> object:
        if "/pulls/620" in args[1]:
            return {"branch": branch, "repo": repo, "sha": head}
        if "/activity?" in args[1]:
            return [{"activity_type": "branch_creation", "ref": f"refs/heads/{branch}",
                     "before": "0" * 40, "after": head,
                     "timestamp": "2026-10-17T21:06:35Z"}]
        raise AssertionError(args)

    monkeypatch.setattr("src.github.gh_runner.run_gh", transport)
    monkeypatch.setattr(gh_prs, "get_open_prs", lambda *_args, **_kwargs: [pr])
    runner = h._make_runner(url=f"https://github.com/{repo}.git")
    runner.repo_path = str(tmp_path)
    runner._parse_tasks_from_headers = lambda: [QueueTask(  # type: ignore[method-assign]
        pr_id="PR-459", title="e2e-failure-evidence", status=TaskStatus.DOING,
        branch=branch, task_file="tasks/PR-459.md",
    )]
    asyncio.run(runner.redis.set(pipeline_state(runner.name), snapshot))
    assert asyncio.run(runner.recover_state()) is True
    assert runner.state.state == PipelineState.WATCH
    assert runner.state.publication_migration is None
    assert runner.state.publication_watermark is not None
    assert runner.state.publication_watermark.published_at == published
    assert runner.state.publication_watermark.provenance == "github-activity:branch_creation"
    assert runner._last_push_at == published

    asyncio.run(runner.publish_state())
    restarted = h._make_runner(url=f"https://github.com/{repo}.git")
    restarted.redis = runner.redis
    restarted.repo_path = str(tmp_path)
    restarted._parse_tasks_from_headers = runner._parse_tasks_from_headers  # type: ignore[method-assign]
    assert asyncio.run(restarted.recover_state()) is True
    assert restarted.state.publication_watermark == runner.state.publication_watermark

    # The saved floor cannot replace a failed current activity read.
    monkeypatch.setattr("src.github.gh_runner.run_gh", lambda *_args, **_kwargs: {})
    assert restarted._canonical_push_timestamp(620) is None


@pytest.mark.parametrize("activity_available", [True, False])
def test_missing_snapshot_requires_exact_operator_migration(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, activity_available: bool,
) -> None:
    """Header/GitHub recovery with no Redis snapshot must offer a bounded route."""
    repo = "AlexBomber12/pipeline-orchestrator"
    branch = "pr-459-e2e-failure-evidence"
    head = "63f07ef48a035c9aa31c37e0fbf3dc215ba0045e"
    pr = PRInfo(number=620, branch=branch, title="PR-459: e2e failure evidence",
                pr_id="PR-459", head_sha=head)

    def transport(args: list[str], **_kwargs: object) -> object:
        if "/pulls/620" in args[1]:
            return {"branch": branch, "repo": repo, "sha": head}
        if "/activity?" in args[1]:
            if activity_available:
                return [{"activity_type": "branch_creation", "ref": f"refs/heads/{branch}",
                         "before": "0" * 40, "after": head,
                         "timestamp": "2026-10-17T21:06:35Z"}]
            return []
        raise AssertionError(args)

    monkeypatch.setattr("src.github.gh_runner.run_gh", transport)
    monkeypatch.setattr(gh_prs, "get_open_prs", lambda *_args, **_kwargs: [pr])
    runner = h._make_runner(url=f"https://github.com/{repo}.git")
    runner.repo_path = str(tmp_path)
    runner._parse_tasks_from_headers = lambda: [QueueTask(  # type: ignore[method-assign]
        pr_id="PR-459", title="e2e-failure-evidence", status=TaskStatus.DOING,
        branch=branch, task_file="tasks/PR-459.md",
    )]
    assert asyncio.run(runner.recover_state()) is True
    assert runner.state.state == PipelineState.WATCH
    proposal = runner.state.publication_migration
    assert proposal is not None and proposal.source == "missing_snapshot"
    if activity_available:
        assert "Saved state is absent" in proposal.reason
    assert runner.state.publication_watermark is None
    assert runner._canonical_push_timestamp(620) is None
    if not activity_available:
        assert proposal.published_at is None
        return
    assert proposal.published_at == datetime(2026, 10, 17, 21, 6, 35, tzinfo=timezone.utc)
    asyncio.run(runner.publish_state())
    pending_restart = h._make_runner(url=f"https://github.com/{repo}.git")
    pending_restart.redis = runner.redis
    pending_restart.repo_path = str(tmp_path)
    pending_restart._parse_tasks_from_headers = runner._parse_tasks_from_headers  # type: ignore[method-assign]
    assert asyncio.run(pending_restart.recover_state()) is True
    assert pending_restart.state.publication_migration is not None
    assert pending_restart.state.publication_migration.source == "missing_snapshot"
    assert pending_restart.state.publication_watermark is None
    runner = pending_restart
    proposal = runner.state.publication_migration
    binding = migration_binding(proposal)
    asyncio.run(enqueue_migration(runner.redis, runner.name, binding))
    assert asyncio.run(runner._consume_publication_migration_command()) is True
    assert runner.state.publication_watermark is not None

    restarted = h._make_runner(url=f"https://github.com/{repo}.git")
    restarted.redis = runner.redis
    restarted.repo_path = str(tmp_path)
    restarted._parse_tasks_from_headers = runner._parse_tasks_from_headers  # type: ignore[method-assign]
    assert asyncio.run(restarted.recover_state()) is True
    assert restarted.state.publication_watermark == runner.state.publication_watermark


@pytest.mark.parametrize("snapshot_kind", ["coding", "idle", "parent_idle"])
def test_snapshot_without_pr_offers_explicit_publication_migration(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, snapshot_kind: str,
) -> None:
    """A PR found after a saved checkpoint has no saved publication proof."""
    repo = "octo/demo"
    branch = "pr-001"
    head = "a" * 40
    pr = PRInfo(number=7, branch=branch, head_sha=head, pr_id="PR-001")

    def transport(args: list[str], **_kwargs: object) -> object:
        if "/pulls/7" in args[1]:
            return {"branch": branch, "repo": repo, "sha": head}
        if "/activity?" in args[1]:
            return [{"activity_type": "branch_creation", "ref": f"refs/heads/{branch}",
                     "before": "0" * 40, "after": head,
                     "timestamp": "2026-04-10T12:00:00Z"}]
        raise AssertionError(args)

    monkeypatch.setattr("src.github.gh_runner.run_gh", transport)
    monkeypatch.setattr(gh_prs, "get_open_prs", lambda *_args, **_kwargs: [pr])
    runner = h._make_runner()
    runner.repo_path = str(tmp_path)
    task = QueueTask(pr_id="PR-001", title="test", status=TaskStatus.DOING,
                     branch=branch, task_file="tasks/PR-001.md")
    runner._parse_tasks_from_headers = lambda: [task]  # type: ignore[method-assign]
    saved = RepoState(
        url=runner.state.url, name=runner.state.name,
        state=PipelineState.CODING if snapshot_kind == "coding" else PipelineState.IDLE,
        current_task=task if snapshot_kind == "coding" else None,
    )
    raw = saved.model_dump(mode="json")
    if snapshot_kind == "parent_idle":
        for key in ("publication_schema_version", "publication_watermark", "publication_migration"):
            raw.pop(key)
    asyncio.run(runner.redis.set(pipeline_state(runner.name), json.dumps(raw)))
    assert asyncio.run(runner.recover_state()) is True
    assert runner.state.state == PipelineState.WATCH
    proposal = runner.state.publication_migration
    assert proposal is not None and proposal.source == "pr_undiscovered_snapshot"
    assert proposal.published_at == datetime(2026, 4, 10, 12, tzinfo=timezone.utc)
    assert runner.state.publication_watermark is None
    assert runner._canonical_push_timestamp(7) is None
    asyncio.run(runner.publish_state())

    restarted = h._make_runner()
    restarted.redis = runner.redis
    restarted.repo_path = str(tmp_path)
    restarted._parse_tasks_from_headers = runner._parse_tasks_from_headers  # type: ignore[method-assign]
    assert asyncio.run(restarted.recover_state()) is True
    assert restarted.state.publication_watermark is None
    assert restarted.state.publication_migration.source == "pr_undiscovered_snapshot"


def test_recovery_retries_transient_snapshot_read_before_pr_discovery(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    repo = "octo/demo"
    branch = "pr-001"
    head = "a" * 40
    publication = datetime(2026, 4, 10, 12, tzinfo=timezone.utc)
    pr = PRInfo(number=7, branch=branch, head_sha=head, pr_id="PR-001")
    monkeypatch.setattr(gh_prs, "get_open_prs", lambda *_args, **_kwargs: [pr])
    monkeypatch.setattr(gh_prs, "get_pr_head_publication", lambda *_args, **_kwargs:
                        gh_prs.HeadPublicationEvidence(repo, 7, repo,
                            f"refs/heads/{branch}", head, publication,
                            "github-activity:branch_creation"))
    runner = h._make_runner()
    runner.repo_path = str(tmp_path)
    task = QueueTask(pr_id="PR-001", title="test", status=TaskStatus.DOING,
                     branch=branch, task_file="tasks/PR-001.md")
    runner._parse_tasks_from_headers = lambda: [task]  # type: ignore[method-assign]
    saved = RepoState(
        url=runner.state.url, name=runner.state.name, state=PipelineState.WATCH,
        current_task=task, current_pr=pr,
        publication_watermark=PublicationWatermark(
            repo=repo, pr_number=7, head_repo=repo, ref=f"refs/heads/{branch}",
            head_sha=head, published_at=publication,
            provenance="github-activity:branch_creation",
        ),
    )
    asyncio.run(runner.redis.set(pipeline_state(runner.name), saved.model_dump_json()))
    original_get = runner.redis.get
    reads = 0

    async def interrupted_read(key: str):
        nonlocal reads
        if key == pipeline_state(runner.name) and reads == 0:
            reads += 1
            raise ConnectionError("temporary Redis outage")
        return await original_get(key)

    monkeypatch.setattr(runner.redis, "get", interrupted_read)
    assert asyncio.run(runner.recover_state()) is False
    assert runner.state.publication_watermark is None
    assert asyncio.run(runner.recover_state()) is True
    assert runner.state.publication_watermark == saved.publication_watermark
    assert runner._last_push_at == publication


def test_saved_floor_for_other_pr_keeps_recovery_cutoff(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    """A restored floor for PR A cannot bootstrap historical PR B activity."""
    repo = "octo/demo"
    old_head = "a" * 40
    new_head = "b" * 40
    event_time = ["2026-04-10T12:00:00Z"]
    new_pr = PRInfo(number=8, branch="pr-008", head_sha=new_head, pr_id="PR-008")

    def transport(args: list[str], **_kwargs: object) -> object:
        if "/pulls/8" in args[1]:
            return {"branch": "pr-008", "repo": repo, "sha": new_head}
        if "/activity?" in args[1]:
            return [{"activity_type": "push", "ref": "refs/heads/pr-008",
                     "before": "c" * 40, "after": new_head,
                     "timestamp": event_time[0]}]
        raise AssertionError(args)

    monkeypatch.setattr("src.github.gh_runner.run_gh", transport)
    monkeypatch.setattr(gh_prs, "get_open_prs", lambda *_args, **_kwargs: [new_pr])
    runner = h._make_runner()
    runner.repo_path = str(tmp_path)
    task = QueueTask(pr_id="PR-008", title="test", status=TaskStatus.DOING,
                     branch="pr-008", task_file="tasks/PR-008.md")
    runner._parse_tasks_from_headers = lambda: [task]  # type: ignore[method-assign]
    old_pr = PRInfo(number=7, branch="pr-007", head_sha=old_head, pr_id="PR-007")
    saved = RepoState(
        url=runner.state.url, name=runner.state.name, state=PipelineState.WATCH,
        current_pr=old_pr,
        publication_watermark=PublicationWatermark(
            repo=repo, pr_number=7, head_repo=repo, ref="refs/heads/pr-007",
            head_sha=old_head, published_at=datetime(2026, 4, 10, 11, tzinfo=timezone.utc),
            provenance="github-activity:branch_creation",
        ),
    )
    asyncio.run(runner.redis.set(pipeline_state(runner.name), saved.model_dump_json()))
    assert asyncio.run(runner.recover_state()) is True
    assert runner.state.current_pr.number == 8
    assert runner._publication_recovery_cutoff_at is not None
    assert runner.state.publication_watermark is None
    assert runner.state.publication_migration is not None
    assert runner.state.publication_migration.source == "rebound_pr"
    assert runner.state.publication_migration.pr_number == 8
    assert runner.state.publication_migration.published_at is not None
    assert runner._last_push_at is None
    assert runner._canonical_push_timestamp(8) is None

    fresh = runner._publication_recovery_cutoff_at + timedelta(seconds=1)
    event_time[0] = fresh.isoformat().replace("+00:00", "Z")
    assert runner._canonical_push_timestamp(8) == fresh
    assert runner.state.publication_watermark.pr_number == 8


def test_parent_snapshot_rebinds_replacement_pr_for_explicit_migration(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    repo = "octo/demo"
    branch = "pr-007"
    head = "a" * 40
    old_pr = PRInfo(number=7, branch=branch, head_sha=head, pr_id="PR-007")
    new_pr = PRInfo(number=8, branch=branch, head_sha=head, pr_id="PR-007")
    published = datetime(2026, 4, 10, 12, tzinfo=timezone.utc)

    def transport(args: list[str], **_kwargs: object) -> object:
        if "/pulls/8" in args[1]:
            return {"branch": branch, "repo": repo, "sha": head}
        if "/activity?" in args[1]:
            return [{"activity_type": "branch_creation", "ref": f"refs/heads/{branch}",
                     "before": "0" * 40, "after": head,
                     "timestamp": "2026-04-10T12:00:00Z"}]
        raise AssertionError(args)

    monkeypatch.setattr("src.github.gh_runner.run_gh", transport)
    monkeypatch.setattr(gh_prs, "get_open_prs", lambda *_args, **_kwargs: [new_pr])
    runner = h._make_runner()
    runner.repo_path = str(tmp_path)
    task = QueueTask(pr_id="PR-007", title="test", status=TaskStatus.DOING,
                     branch=branch, task_file="tasks/PR-007.md")
    runner._parse_tasks_from_headers = lambda: [task]  # type: ignore[method-assign]
    saved = RepoState(url=runner.state.url, name=runner.state.name,
                      state=PipelineState.WATCH, current_task=task, current_pr=old_pr)
    raw = saved.model_dump(mode="json")
    for key in ("publication_schema_version", "publication_watermark", "publication_migration"):
        raw.pop(key)
    for key in list(raw["current_pr"]):
        if key.startswith("publication_"):
            raw["current_pr"].pop(key)
    asyncio.run(runner.redis.set(pipeline_state(runner.name), json.dumps(raw)))
    assert asyncio.run(runner.recover_state()) is True
    proposal = runner.state.publication_migration
    assert proposal is not None and proposal.status == "legacy"
    assert proposal.source == "rebound_pr"
    assert proposal.pr_number == 8
    assert proposal.published_at == published
    assert runner.state.publication_watermark is None
    assert runner._canonical_push_timestamp(8) is None
    asyncio.run(runner.publish_state())
    binding = migration_binding(proposal)
    asyncio.run(enqueue_migration(runner.redis, runner.name, binding))
    assert asyncio.run(runner._consume_publication_migration_command()) is True
    assert runner.state.publication_watermark.pr_number == 8


def test_pre_cutoff_poll_for_saved_pr_does_not_release_other_pr(
) -> None:
    runner = h._make_runner()
    cutoff = datetime(2026, 4, 11, tzinfo=timezone.utc)
    old_event = cutoff - timedelta(days=1)
    runner._publication_recovery_cutoff_at = cutoff
    old_pr = PRInfo(number=7, branch="pr-007", head_sha="a" * 40)
    saved = RepoState(
        url=runner.state.url, name=runner.state.name, current_pr=old_pr,
        publication_watermark=PublicationWatermark(
            repo="octo/demo", pr_number=7, head_repo="octo/demo",
            ref="refs/heads/pr-007", head_sha="a" * 40,
            published_at=old_event, provenance="github-activity:branch_creation",
        ),
    )
    runner._restore_publication_watermark(saved)
    assert runner._validate_publication_observation(
        7, "a" * 40, old_event, None, head_repo="octo/demo",
        ref="refs/heads/pr-007", observed_repo="octo/demo",
        observed_head_sha="a" * 40,
    )[0] == old_event
    assert runner._publication_recovery_cutoff_at == cutoff
    assert runner._validate_publication_observation(
        7, "a" * 40, cutoff + timedelta(seconds=1), None,
        head_repo="octo/demo", ref="refs/heads/pr-007",
        observed_repo="octo/demo", observed_head_sha="a" * 40,
    )[0] == cutoff + timedelta(seconds=1)
    assert runner._publication_recovery_cutoff_at == cutoff
    assert runner._validate_publication_observation(
        8, "b" * 40, old_event, None, head_repo="octo/demo",
        ref="refs/heads/pr-008", observed_repo="octo/demo",
        observed_head_sha="b" * 40,
    )[0] is None


@pytest.mark.parametrize("change_evidence", [False, True])
def test_parent_snapshot_push_requires_exact_operator_command(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, change_evidence: bool,
) -> None:
    snapshot = (Path(__file__).parents[1] / "fixtures/recovery/pr_620_parent_snapshot.json").read_text()
    repo = "AlexBomber12/pipeline-orchestrator"
    branch = "pr-459-e2e-failure-evidence"
    head = "63f07ef48a035c9aa31c37e0fbf3dc215ba0045e"
    event_time = ["2026-10-17T21:06:35Z"]

    def transport(args: list[str], **_kwargs: object) -> object:
        if "/pulls/620" in args[1]:
            return {"branch": branch, "repo": repo, "sha": head}
        if "/activity?" in args[1]:
            return [{"activity_type": "push", "ref": f"refs/heads/{branch}",
                     "before": "a" * 40, "after": head, "timestamp": event_time[0]}]
        raise AssertionError(args)

    monkeypatch.setattr("src.github.gh_runner.run_gh", transport)
    pr = PRInfo(number=620, branch=branch, title="PR-459: e2e failure evidence",
                pr_id="PR-459", head_sha=head)
    monkeypatch.setattr(gh_prs, "get_open_prs", lambda *_args, **_kwargs: [pr])
    runner = h._make_runner(url=f"https://github.com/{repo}.git", allow_merge_without_review=True)
    runner.repo_path = str(tmp_path)
    runner._parse_tasks_from_headers = lambda: [QueueTask(  # type: ignore[method-assign]
        pr_id="PR-459", title="e2e-failure-evidence", status=TaskStatus.DOING,
        branch=branch, task_file="tasks/PR-459.md",
    )]
    asyncio.run(runner.redis.set(pipeline_state(runner.name), snapshot))
    assert asyncio.run(runner.recover_state()) is True
    proposal = runner.state.publication_migration
    assert proposal is not None and proposal.published_at is not None
    assert proposal.provenance == "github-activity:push"
    assert runner.state.publication_watermark is None
    assert runner._canonical_push_timestamp(620) is None
    asyncio.run(runner.publish_state())
    binding = migration_binding(proposal)
    first = asyncio.run(enqueue_migration(runner.redis, runner.name, binding))
    assert asyncio.run(enqueue_migration(runner.redis, runner.name, binding)) == first
    if change_evidence:
        event_time[0] = "2026-10-17T21:07:00Z"
    assert asyncio.run(runner._consume_publication_migration_command()) is True
    command = MigrationCommand.model_validate_json(asyncio.run(
        runner.redis.get(migration_command_key(runner.name, binding))
    ))
    if change_evidence:
        assert command.status == "failed"
        assert runner.state.publication_watermark is None
    else:
        assert command.status == "applied"
        assert runner.state.publication_watermark is not None
        assert runner.state.publication_watermark.provenance == "github-activity:push"
        assert runner._canonical_push_timestamp(620) == proposal.published_at


def test_migration_command_retries_transient_github_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo = "octo/demo"
    head = "a" * 40
    published = datetime(2026, 10, 17, 21, 6, 35, tzinfo=timezone.utc)
    runner = h._make_runner()
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=42, branch="feature", head_sha=head)
    runner.state.publication_migration = recovery_module.PublicationMigration(
        repo=repo, pr_number=42, head_repo=repo, ref="refs/heads/feature",
        head_sha=head, published_at=published, provenance="github-activity:push",
        reason="Operator confirmation required.",
    )
    asyncio.run(runner.redis.set(pipeline_state(runner.name), runner.state.model_dump_json()))
    binding = migration_binding(runner.state.publication_migration)
    asyncio.run(enqueue_migration(runner.redis, runner.name, binding))

    def unavailable(*_args: object, **_kwargs: object) -> None:
        raise RuntimeError("temporary outage")

    monkeypatch.setattr(gh_prs, "get_open_prs", unavailable)
    assert asyncio.run(runner._consume_publication_migration_command()) is True
    key = migration_command_key(runner.name, binding)
    assert MigrationCommand.model_validate_json(asyncio.run(runner.redis.get(key))).status == "deferred"
    assert runner.state.publication_watermark is None

    monkeypatch.setattr(gh_prs, "get_open_prs", lambda *_args, **_kwargs: [runner.state.current_pr])
    monkeypatch.setattr(
        gh_prs, "get_pr_head_publication",
        lambda *_args, **_kwargs: gh_prs.HeadPublicationEvidence(
            repo, 42, repo, "refs/heads/feature", head, published, "github-activity:push"
        ),
    )
    assert asyncio.run(runner._consume_publication_migration_command()) is True
    assert MigrationCommand.model_validate_json(asyncio.run(runner.redis.get(key))).status == "applied"
    assert runner.state.publication_watermark is not None


@pytest.mark.parametrize("damage", ["corrupt_watermark", "newer_without_floor", "incompatible_version"])
def test_newer_or_corrupt_snapshot_cannot_use_parent_bootstrap(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, damage: str,
) -> None:
    raw = json.loads((Path(__file__).parents[1] / "fixtures/recovery/pr_620_parent_snapshot.json").read_text())
    if damage == "corrupt_watermark":
        raw["publication_watermark"] = {"repo": "wrong"}
    else:
        raw["publication_watermark"] = None
        raw["publication_schema_version"] = 2 if damage == "incompatible_version" else 1
    branch = "pr-459-e2e-failure-evidence"
    head = "63f07ef48a035c9aa31c37e0fbf3dc215ba0045e"
    repo = "AlexBomber12/pipeline-orchestrator"
    pr = PRInfo(number=620, branch=branch, pr_id="PR-459", head_sha=head)
    monkeypatch.setattr(gh_prs, "get_open_prs", lambda *_args, **_kwargs: [pr])

    def transport(args: list[str], **_kwargs: object) -> object:
        if "/pulls/620" in args[1]:
            return {"branch": branch, "repo": repo, "sha": head}
        return [{"activity_type": "branch_creation", "ref": f"refs/heads/{branch}",
                 "before": "0" * 40, "after": head, "timestamp": "2026-10-17T21:06:35Z"}]

    monkeypatch.setattr("src.github.gh_runner.run_gh", transport)
    runner = h._make_runner(url=f"https://github.com/{repo}.git")
    runner.repo_path = str(tmp_path)
    runner._parse_tasks_from_headers = lambda: [QueueTask(  # type: ignore[method-assign]
        pr_id="PR-459", title="test", status=TaskStatus.DOING,
        branch=branch, task_file="tasks/PR-459.md",
    )]
    asyncio.run(runner.redis.set(pipeline_state(runner.name), json.dumps(raw)))
    assert asyncio.run(runner.recover_state()) is True
    assert runner.state.publication_watermark is None
    assert runner.state.publication_migration is not None
    assert runner.state.publication_migration.status == "blocked"
    assert runner._canonical_push_timestamp(620) is None


@pytest.mark.parametrize("changed_field", ["repo", "pr_number", "head_repo", "ref", "head_sha"])
def test_operator_command_rejects_changed_target(
    changed_field: str,
) -> None:
    runner = h._make_runner()
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=42, branch="feature", head_sha="a" * 40)
    proposal = recovery_module.PublicationMigration(
        repo="octo/demo", pr_number=42, head_repo="octo/demo",
        ref="refs/heads/feature", head_sha="a" * 40,
        published_at=datetime(2026, 10, 17, 21, 6, 35, tzinfo=timezone.utc),
        provenance="github-activity:push", reason="Operator confirmation required.",
    )
    runner.state.publication_migration = proposal
    asyncio.run(runner.redis.set(pipeline_state(runner.name), runner.state.model_dump_json()))
    binding = migration_binding(proposal)
    asyncio.run(enqueue_migration(runner.redis, runner.name, binding))
    new_value = {
        "repo": "other/demo", "pr_number": 43, "head_repo": "other/demo",
        "ref": "refs/heads/other", "head_sha": "b" * 40,
    }[changed_field]
    changed = proposal.model_copy(update={changed_field: new_value})
    runner.state.publication_migration = changed
    asyncio.run(runner.redis.set(pipeline_state(runner.name), runner.state.model_dump_json()))
    assert asyncio.run(runner._consume_publication_migration_command()) is True
    command = MigrationCommand.model_validate_json(asyncio.run(
        runner.redis.get(migration_command_key(runner.name, binding))
    ))
    assert command.status == "failed"
    assert runner.state.publication_watermark is None


def _migration_command_runner() -> tuple[runner_module.PipelineRunner, str]:
    runner = h._make_runner()
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=42, branch="feature", head_sha="a" * 40)
    runner.state.publication_migration = recovery_module.PublicationMigration(
        repo="octo/demo", pr_number=42, head_repo="octo/demo",
        ref="refs/heads/feature", head_sha="a" * 40,
        published_at=datetime(2026, 10, 17, 21, 6, 35, tzinfo=timezone.utc),
        provenance="github-activity:push", reason="Operator confirmation required.",
    )
    asyncio.run(runner.redis.set(pipeline_state(runner.name), runner.state.model_dump_json()))
    binding = migration_binding(runner.state.publication_migration)
    asyncio.run(enqueue_migration(runner.redis, runner.name, binding))
    return runner, binding


@pytest.mark.parametrize("case", [
    "missing_receipt", "applied_receipt", "failed_receipt", "missing_time",
    "changed_binding", "changed_pr", "changed_evidence", "unknown_evidence",
])
def test_migration_command_revalidates_untrusted_inputs(
    monkeypatch: pytest.MonkeyPatch, case: str,
) -> None:
    runner, binding = _migration_command_runner()
    key = migration_command_key(runner.name, binding)
    if case == "missing_receipt":
        runner.redis.store.pop(key)
    elif case in {"applied_receipt", "failed_receipt"}:
        command = MigrationCommand.model_validate_json(runner.redis.store[key])
        command.status = "applied" if case == "applied_receipt" else "failed"
        runner.redis.store[key] = command.model_dump_json()
    elif case == "missing_time":
        runner.state.publication_migration.published_at = None
        runner.redis.store[pipeline_state(runner.name)] = runner.state.model_dump_json()
    elif case == "changed_binding":
        runner.state.publication_migration.published_at += timedelta(seconds=1)
        runner.redis.store[pipeline_state(runner.name)] = runner.state.model_dump_json()
    elif case == "changed_pr":
        monkeypatch.setattr(gh_prs, "get_open_prs", lambda *_args, **_kwargs: [])
    elif case == "changed_evidence":
        monkeypatch.setattr(gh_prs, "get_open_prs", lambda *_args, **_kwargs: [runner.state.current_pr])
        monkeypatch.setattr(gh_prs, "get_pr_head_publication", lambda *_args, **_kwargs:
                            gh_prs.HeadPublicationEvidence("other/repo", 42, "octo/demo",
                                "refs/heads/feature", "a" * 40,
                                runner.state.publication_migration.published_at,
                                "github-activity:push"))
    elif case == "unknown_evidence":
        monkeypatch.setattr(gh_prs, "get_open_prs", lambda *_args, **_kwargs: [runner.state.current_pr])
        monkeypatch.setattr(gh_prs, "get_pr_head_publication", lambda *_args, **_kwargs:
                            gh_prs.HeadPublicationEvidence("octo/demo", 42, "octo/demo",
                                "refs/heads/feature", "a" * 40, None,
                                "github-activity:push", unknown_reason="activity unavailable"))
    assert asyncio.run(runner._consume_publication_migration_command()) is True
    if case == "missing_receipt":
        assert any("receipt missing" in e["event"] for e in runner.state.history)
    elif case == "unknown_evidence":
        assert MigrationCommand.model_validate_json(runner.redis.store[key]).status == "deferred"
    elif case not in {"applied_receipt", "failed_receipt"}:
        assert MigrationCommand.model_validate_json(runner.redis.store[key]).status in {"failed", "deferred"}
        assert runner.state.publication_watermark is None


@pytest.mark.parametrize("change", ["pointer_bytes", "command_done", "observation", "target"])
def test_migration_transaction_checks_snapshot_again(
    monkeypatch: pytest.MonkeyPatch, change: str,
) -> None:
    runner, binding = _migration_command_runner()
    monkeypatch.setattr(gh_prs, "get_open_prs", lambda *_args, **_kwargs: [runner.state.current_pr])
    monkeypatch.setattr(gh_prs, "get_pr_head_publication", lambda *_args, **_kwargs:
                        gh_prs.HeadPublicationEvidence("octo/demo", 42, "octo/demo",
                            "refs/heads/feature", "a" * 40,
                            runner.state.publication_migration.published_at,
                            "github-activity:push"))
    original_transaction = runner.redis.transaction

    async def interleaved_transaction(func, *keys, **kwargs):
        if change == "pointer_bytes":
            runner.redis.store[migration_pending_key(runner.name)] = binding.encode()
        elif change == "command_done":
            key = migration_command_key(runner.name, binding)
            command = MigrationCommand.model_validate_json(runner.redis.store[key])
            command.status = "applied"
            runner.redis.store[key] = command.model_dump_json()
        else:
            state = RepoState.model_validate_json(runner.redis.store[pipeline_state(runner.name)])
            if change == "observation":
                state.publication_migration.published_at += timedelta(seconds=1)
            else:
                state.current_pr.head_sha = "b" * 40
            runner.redis.store[pipeline_state(runner.name)] = state.model_dump_json()
        return await original_transaction(func, *keys, **kwargs)

    monkeypatch.setattr(runner.redis, "transaction", interleaved_transaction)
    assert asyncio.run(runner._consume_publication_migration_command()) is True
    receipt = MigrationCommand.model_validate_json(
        runner.redis.store[migration_command_key(runner.name, binding)])
    assert receipt.status == ("applied" if change in {"pointer_bytes", "command_done"} else "failed")


def test_hydration_restores_pending_publication_proposal() -> None:
    runner, _binding = _migration_command_runner()
    proposal = runner.state.publication_migration.model_copy(deep=True)
    runner.state.publication_migration = None
    asyncio.run(runner._hydrate_current_task_from_persisted_state())
    assert runner.state.publication_migration == proposal


@pytest.mark.parametrize("change", ["target", "head_repo"])
def test_parent_proposal_blocks_changed_pr_identity(change: str) -> None:
    runner, _binding = _migration_command_runner()
    proposal = runner.state.publication_migration
    pr = runner.state.current_pr
    evidence = gh_prs.HeadPublicationEvidence(
        "octo/demo", 42, "octo/demo", "refs/heads/feature", "a" * 40,
        proposal.published_at, "github-activity:push",
    )
    if change == "target":
        pr = pr.model_copy(update={"head_sha": "b" * 40})
    else:
        pr = pr.model_copy(update={"is_cross_repository": True})
        evidence = gh_prs.HeadPublicationEvidence(
            "octo/demo", 42, "other/repo", "refs/heads/feature", "a" * 40,
            proposal.published_at, "github-activity:push",
        )
    runner._offer_publication_migration(pr, evidence)
    assert proposal.status == "blocked"


def test_recovered_cycle_consumes_publication_command_before_other_work(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    h._patch_subprocess(monkeypatch, stdout="")
    runner = h._make_runner()
    runner._recovered = True
    runner._scaffolded = True
    calls = []

    async def consume() -> bool:
        calls.append("publication")
        return True

    async def retry():
        calls.append("retry")
        raise AssertionError("Retry must wait")

    monkeypatch.setattr(runner, "_consume_publication_migration_command", consume)
    monkeypatch.setattr(runner, "_consume_retry_command", retry)
    asyncio.run(runner.run_cycle())
    assert calls == ["publication"]


def _merged_state(
    pr_ids: set[str] | None = None,
    branches: set[str] | None = None,
    *,
    api_available: bool = True,
) -> MergedState:
    return MergedState(set(pr_ids or ()), set(branches or ()), api_available)


# ---------------------------------------------------------------------------
# Test 1 — dirty-tree auto-reset composes with crashed-task marker
# ---------------------------------------------------------------------------


def test_dirty_tree_recovery_composes_with_crashed_task_marker(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Recovery + preflight defense layers compose without colliding.

    Setup mirrors a daemon restart after a mid-CODING crash: a DOING
    task PR-100 in the queue with no matching open PR, and a working
    tree left dirty by the crashed coder run. Two distinct recovery
    layers must engage:

    1. ``recover_state`` calls ``_preserve_crashed_run_commits`` to
       push any unpushed work, adds PR-100 to ``_crashed_task_pr_ids``,
       transitions to IDLE.
    2. The next ``preflight`` cycle observes the dirty tree, increments
       the consecutive-dirty counter to its threshold, and fires
       ``_auto_reset_dirty_tree`` to hard-reset back to a clean IDLE.

    Pre-seeding ``_consecutive_dirty_cycles=2`` so the dirty preflight
    crosses the threshold on this single call keeps the test focused
    on the layer composition rather than the policy mechanics tested
    elsewhere.
    """
    task = QueueTask(
        pr_id="PR-100",
        title="Crashed mid-CODING",
        status=TaskStatus.DOING,
        branch="pr-100-crashed",
    )

    monkeypatch.setattr(
        "src.github.prs.get_open_prs",
        lambda repo, **kw: [],
    )

    runner = h._make_runner()
    runner._parse_tasks_from_headers = lambda: [task]  # type: ignore[method-assign]

    preserve_calls: list[str] = []

    def fake_preserve(branch: str) -> bool:
        preserve_calls.append(branch)
        return True

    runner._preserve_crashed_run_commits = (  # type: ignore[method-assign]
        fake_preserve
    )

    asyncio.run(runner.recover_state())

    assert preserve_calls == ["pr-100-crashed"]
    assert runner._crashed_task_pr_ids == {"PR-100"}
    assert runner.state.state == PipelineState.IDLE
    assert runner.state.current_task is None
    assert runner.state.current_pr is None
    assert any(
        e["event"].startswith("[INFRA] Task PR-100 crashed, marking ERROR. Manually re-upload to retry.")
        for e in runner.state.history
    )

    runner._consecutive_dirty_cycles = 2

    reset_commands: list[list[str]] = []

    def fake_run(cmd: list[str], **kwargs: Any) -> h._FakeCompletedProcess:
        reset_commands.append(cmd)
        if cmd[:3] == ["git", "status", "--porcelain"]:
            return h._FakeCompletedProcess(args=cmd, stdout=" M src/foo.py\n", returncode=0)
        return h._FakeCompletedProcess(args=cmd, stdout="", returncode=0)

    monkeypatch.setattr(runner_module.subprocess, "run", fake_run)

    assert asyncio.run(runner.preflight()) is True
    assert runner.state.state == PipelineState.IDLE
    assert runner._consecutive_dirty_cycles == 0
    assert any(cmd[:2] == ["git", "reset"] and "--hard" in cmd for cmd in reset_commands)
    assert any("Auto-recovered from dirty tree" in e["event"] for e in runner.state.history)
    assert any("PR-100 crashed, marking ERROR" in e["event"] for e in runner.state.history)


def test_recovery_finalizes_checkpointed_coding_invocation_without_pr(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    task = QueueTask(
        pr_id="PR-395",
        title="Invocation settings",
        status=TaskStatus.DOING,
        branch="manual-20261008-coder-invocation-settings",
    )
    monkeypatch.setattr(
        "src.github.prs.get_open_prs",
        lambda repo, **kwargs: [],
    )
    runner = h._make_runner()
    runner._parse_tasks_from_headers = lambda: [task]  # type: ignore[method-assign]
    runner._preserve_crashed_run_commits = (  # type: ignore[method-assign]
        lambda branch: True
    )
    runner.state.state = PipelineState.CODING
    runner.state.current_task = task
    runner._start_current_run_record("codex", "gpt-5.4")
    snapshot = runner._capture_invocation_snapshot(
        "codex",
        {"model": "gpt-5.4", "reasoning_effort": "high"},
        phase="coding",
    )
    asyncio.run(runner._checkpoint_current_run_record())
    asyncio.run(runner.publish_state())
    runner._current_run_record = None
    runner.state = RepoState(url=runner.state.url, name=runner.state.name)

    asyncio.run(runner.recover_state())

    records = asyncio.run(
        runner._metrics_store.recent(
            task_id=task.pr_id,
            limit=1,
            repo_name=runner.name,
        )
    )
    assert len(records) == 1
    record = records[0]
    assert record.ended_at is not None
    assert record.exit_reason == "error"
    assert record.outcome == "failed"
    assert record.cause == "CRASH"
    assert record.run_phase == "recovery"
    assert record.invocations == [snapshot]
    assert runner.state.current_task is None
    assert runner.state.active_invocation is None


def test_recovery_does_not_rewrite_completed_run_without_current_snapshot(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    task = QueueTask(
        pr_id="PR-395",
        title="Invocation settings retry",
        status=TaskStatus.DOING,
        branch="manual-20261008-coder-invocation-settings",
    )
    monkeypatch.setattr(
        "src.github.prs.get_open_prs",
        lambda repo, **kwargs: [],
    )
    runner = h._make_runner()
    runner._parse_tasks_from_headers = lambda: [task]  # type: ignore[method-assign]
    runner._preserve_crashed_run_commits = (  # type: ignore[method-assign]
        lambda branch: True
    )
    runner.state.current_task = task
    runner._start_current_run_record("codex", "gpt-5.4")
    runner._capture_invocation_snapshot(
        "codex", {"model": "gpt-5.4"}, phase="coding"
    )
    assert runner._current_run_record is not None
    completed_run_id = runner._current_run_record.run_id
    asyncio.run(
        runner._save_current_run_record(
            "success_merged",
            diff_stats={},
            base_branch="main",
        )
    )
    runner.state.current_task = None
    runner.state.state = PipelineState.CODING
    runner.state.current_task = task
    asyncio.run(runner.publish_state())
    runner._current_run_record = None
    runner.state = RepoState(url=runner.state.url, name=runner.state.name)

    asyncio.run(runner.recover_state())

    record = asyncio.run(runner._metrics_store.get(completed_run_id))
    assert record is not None
    assert record.exit_reason == "success_merged"
    assert record.outcome == "merged"
    assert record.cause is None
    assert record.run_phase == "merge"


def test_recovery_does_not_finalize_checkpoint_for_different_run() -> None:
    task = QueueTask(
        pr_id="PR-395",
        title="Invocation settings retry",
        status=TaskStatus.DOING,
        branch="manual-20261008-coder-invocation-settings",
    )
    runner = h._make_runner()
    runner.state.current_task = task
    runner._start_current_run_record("codex", "gpt-5.4")
    snapshot = runner._capture_invocation_snapshot(
        "codex", {"model": "gpt-5.4"}, phase="coding"
    )
    asyncio.run(runner._checkpoint_current_run_record())
    assert snapshot.run_id is not None
    runner.state.active_invocation = replace(snapshot, run_id=f"retry-{snapshot.run_id}")
    runner._current_run_record = None

    asyncio.run(runner._finalize_interrupted_coding_invocation())

    record = asyncio.run(runner._metrics_store.get(snapshot.run_id))
    assert record is not None
    assert record.ended_at is None
    assert record.exit_reason == ""


def test_recovery_finalizes_checkpoint_while_user_paused(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    task = QueueTask(
        pr_id="PR-396",
        title="Paused invocation settings",
        status=TaskStatus.DOING,
        branch="manual-20261008-paused-invocation-settings",
    )
    monkeypatch.setattr(
        "src.github.prs.get_open_prs",
        lambda repo, **kwargs: [],
    )
    runner = h._make_runner()
    runner._parse_tasks_from_headers = lambda: [task]  # type: ignore[method-assign]
    runner._preserve_crashed_run_commits = (  # type: ignore[method-assign]
        lambda branch: True
    )
    runner.state.state = PipelineState.CODING
    runner.state.user_paused = True
    runner.state.current_task = task
    runner._start_current_run_record("codex", "")
    snapshot = runner._capture_invocation_snapshot(
        "codex", {"model": ""}, phase="coding"
    )
    asyncio.run(runner._checkpoint_current_run_record())
    asyncio.run(runner.publish_state())
    runner._current_run_record = None
    runner.state = RepoState(
        url=runner.state.url,
        name=runner.state.name,
        user_paused=True,
    )

    asyncio.run(runner.recover_state())

    records = asyncio.run(
        runner._metrics_store.recent(
            task_id=task.pr_id,
            limit=1,
            repo_name=runner.name,
        )
    )
    assert len(records) == 1
    record = records[0]
    assert record.ended_at is not None
    assert record.exit_reason == "error"
    assert record.cause == "CRASH"
    assert record.invocations == [snapshot]
    assert runner.state.state == PipelineState.IDLE
    assert runner.state.current_task == task
    assert runner.state.active_invocation == snapshot


def test_recovery_restores_checkpointed_fix_snapshot_before_state_publish(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    task = QueueTask(
        pr_id="PR-397",
        title="Interrupted FIX snapshot",
        status=TaskStatus.DOING,
        branch="manual-20261008-fix-snapshot",
    )
    matching_pr = PRInfo(number=397, branch=task.branch)
    monkeypatch.setattr(
        "src.github.prs.get_open_prs",
        lambda repo, **kwargs: [matching_pr],
    )
    runner = h._make_runner()
    runner._parse_tasks_from_headers = lambda: [task]  # type: ignore[method-assign]
    runner.state.current_task = task
    runner._start_current_run_record("codex", "gpt-5.4")
    coding_snapshot = runner._capture_invocation_snapshot(
        "codex", {"model": "gpt-5.4"}, phase="coding"
    )
    assert runner._current_run_record is not None
    runner._current_run_record.fix_iterations = 1
    fix_snapshot = runner._capture_invocation_snapshot(
        "codex",
        {"model": "gpt-5.5", "reasoning_effort": "high"},
        phase="fix",
        fix_iteration=1,
    )
    asyncio.run(runner._checkpoint_current_run_record())
    runner._current_run_record = None
    runner.state = RepoState(url=runner.state.url, name=runner.state.name)

    asyncio.run(runner.recover_state())

    assert runner.state.state == PipelineState.WATCH
    assert runner.state.current_task == task
    assert runner.state.current_pr == matching_pr
    assert runner.state.active_invocation == fix_snapshot
    assert runner._current_run_record is not None
    assert runner._current_run_record.invocations == [
        coding_snapshot,
        fix_snapshot,
    ]


# ---------------------------------------------------------------------------
# Test 2 — no-push deadlock cancels the task and returns to IDLE (PR-258)
# ---------------------------------------------------------------------------


def test_no_push_escalation_cancels_task_and_returns_to_idle(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """PR-258 (OBS-BB) replaces the prior HUNG transition with a
    cancellation policy v1 transition: write a ``NO_PUSH_DEADLOCK``
    cause and return to IDLE so the daemon picks up the next pickable
    task.
    """
    monkeypatch.setattr(
        "src.github.gh_runner.run_gh", lambda cmd, **kwargs: ""
    )

    recorded: list[tuple[str, str, str]] = []

    async def fake_record(redis_client, repo_slug, task_id, cause):
        recorded.append((repo_slug, task_id, cause.category))

    monkeypatch.setattr(
        "src.cancellation.record_cancellation_cause", fake_record
    )

    runner = h._make_runner()
    pr = PRInfo(number=400, branch="pr-400-feedback", no_push_fix_count=3)
    runner.state.state = PipelineState.FIX
    runner.state.current_task = QueueTask(
        pr_id="PR-400",
        title="t",
        status=TaskStatus.DOING,
        branch="pr-400-feedback",
    )
    runner.state.current_pr = pr

    asyncio.run(runner._escalate_fix_no_push_deadlock(pr))

    assert runner.state.state == PipelineState.IDLE
    assert runner.state.current_task is None
    assert runner.state.current_pr is None
    assert pr.no_push_fix_count == 0
    assert not hasattr(runner, "_recovered_task_pr_ids")
    assert recorded == [(runner.name, "PR-400", "ERROR")]
    assert any(
        "PR #400 no-push deadlock after 3 attempts; canceling task"
        in e["event"]
        for e in runner.state.history
    )


# ---------------------------------------------------------------------------
# Test 3 — coder ESCALATE + label apply failure skips to IDLE
# ---------------------------------------------------------------------------


def test_coder_escalate_label_apply_failure_parks_in_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """PR-317: coder ESCALATE with label-apply failure now parks in ERROR.

    The terminal state migrated from IDLE to ERROR so the picker stops
    re-selecting the still-status:TODO task. The expanded failure context
    (mentioning the failed label apply) remains the operator-visible
    ``error_message`` so dashboards surface the GitHub mutation outage.
    """
    posted: list[tuple[str, int, str]] = []

    monkeypatch.setattr(
        "src.github.comments.post_comment",
        lambda repo, number, body: posted.append((repo, number, body)),
    )

    def fake_run_gh(cmd: list[str], **kwargs: Any) -> str:
        if cmd[:2] == ["pr", "edit"]:
            raise RuntimeError("gh down")
        return ""

    monkeypatch.setattr("src.github.gh_runner.run_gh", fake_run_gh)

    runner = h._make_runner()
    pr = PRInfo(number=401, branch="pr-401-coder-escalate")
    runner.state.state = PipelineState.FIX
    runner.state.current_pr = pr

    asyncio.run(runner._escalate_fix_coder_initiated(pr, "transient infra failure"))

    assert runner.state.state == PipelineState.ERROR
    assert runner.state.skip_ai_error_diagnose is False
    assert pr.is_escalated is True
    assert runner.state.error_message is not None
    assert "FIX coder ESCALATE on PR #401" in runner.state.error_message
    assert "failed to apply `escalated` label" in runner.state.error_message
    assert "transient infra failure" in runner.state.error_message
    assert posted == [
        (
            runner.owner_repo,
            401,
            "Coder explicitly escalated this PR. Reason: transient infra failure. Manual review required.",
        )
    ]
    assert any("failed to apply escalated label to PR #401: gh down" in e["event"] for e in runner.state.history)


# ---------------------------------------------------------------------------
# Test 4 — iteration-cap + label apply failure transitions to ERROR
# ---------------------------------------------------------------------------


def test_iteration_cap_label_apply_failure_transitions_to_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Iteration-cap escalation with a label-apply failure goes to ERROR.

    The iteration-cap escalation path differs intentionally from the
    coder-initiated path: a daemon-driven escalation does not have a
    self-reported parking signal to fall back on, so the failure to
    publish the durable ``escalated`` label is a hard error. The
    runner records the GitHub mutation reason in ``error_message``
    and surfaces it via a ``[FIX]`` log prefix so it is grouped with
    other FIX-handler diagnostics. PR-220 will replace this method
    with a parameterised escalation primitive; the ERROR terminal
    state must remain distinct from coder ESCALATE's HUNG.
    """
    posted: list[tuple[str, int, str]] = []
    label_create_calls: list[list[str]] = []

    monkeypatch.setattr(
        "src.github.comments.post_comment",
        lambda repo, number, body: posted.append((repo, number, body)),
    )

    def fake_run_gh(cmd: list[str], **kwargs: Any) -> str:
        if cmd[:3] == ["label", "create", "escalated"]:
            label_create_calls.append(cmd)
            return ""
        if cmd[:2] == ["pr", "edit"]:
            raise subprocess.CalledProcessError(1, cmd, stderr="rate limit exceeded")
        return ""

    monkeypatch.setattr("src.github.gh_runner.run_gh", fake_run_gh)

    runner = h._make_runner()
    cap = runner.app_config.daemon.fix_iteration_cap
    pr = PRInfo(
        number=402,
        branch="pr-402-cap",
        fix_iteration_count=cap,
    )
    runner.state.state = PipelineState.FIX
    runner.state.current_pr = pr

    asyncio.run(runner._escalate_fix_iteration_cap(pr))

    assert runner.state.state == PipelineState.ERROR
    assert pr.is_escalated is False
    assert runner.state.error_message is not None
    assert runner.state.error_message.startswith("pr edit failed:")
    assert posted == [
        (
            runner.owner_repo,
            402,
            f"@AlexBomber12 FIX iteration cap reached ({cap}/{cap}). Escalating for manual review.",
        )
    ]
    assert label_create_calls and label_create_calls[0][:3] == [
        "label",
        "create",
        "escalated",
    ]
    assert any(e["event"].startswith("[FIX] pr edit failed:") for e in runner.state.history)


# ---------------------------------------------------------------------------
# PR-224a moved from tests/test_runner.py
# ---------------------------------------------------------------------------


def test_select_next_task_from_dag_skips_crashed_task_marked_canceled(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """PR-186: After recovery marks a task crashed, the next IDLE cycle's
    selector must override its derived status to ERROR so
    get_eligible_tasks excludes it. Without the override the same crashed
    task would be re-picked as TODO and dispatched into another doomed
    CODING run on the very next cycle."""
    h._patch_subprocess(monkeypatch)
    monkeypatch.setattr(
        idle_module.IdleMixin,
        "_select_next_task_from_dag",
        h._ORIGINAL_SELECT_NEXT_TASK_FROM_DAG,
    )

    tasks_dir = tmp_path / "tasks"
    tasks_dir.mkdir()
    (tasks_dir / "PR-001.md").write_text(
        "---\n---\n"
        "# PR-001: Crashed task\n\n"
        "Branch: pr-001-crashed\n"
        "- Type: feature\n"
        "- Complexity: low\n"
        "- Depends on: none\n"
        "- Priority: 1\n"
        "- Coder: any\n",
        encoding="utf-8",
    )
    (tasks_dir / "PR-002.md").write_text(
        "---\n---\n"
        "# PR-002: Healthy follow-up\n\n"
        "Branch: pr-002-healthy\n"
        "- Type: feature\n"
        "- Complexity: low\n"
        "- Depends on: none\n"
        "- Priority: 1\n"
        "- Coder: any\n",
        encoding="utf-8",
    )

    monkeypatch.setattr(idle_module, "_resolve_merged_state", lambda *args, **kwargs: _merged_state())

    runner = h._make_runner()
    runner.repo_path = str(tmp_path)
    runner._idle_open_prs = []
    runner._idle_merged_prs = []
    runner._crashed_task_pr_ids.add("PR-001")

    task = asyncio.run(runner._select_next_task_from_dag())

    assert task is not None
    assert task.pr_id == "PR-002"
    assert task.status == TaskStatus.TODO
    assert runner._idle_dag_statuses == {
        "PR-001": TaskStatus.ERROR,
        "PR-002": TaskStatus.TODO,
    }


def test_select_next_task_from_dag_preserves_doing_for_crashed_task_with_visible_pr(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Codex P1: a crashed task whose open PR becomes visible on a later
    cycle (e.g. ``get_open_prs`` was stale during recovery) must not be
    downgraded to ERROR by the selector. Preserve the DOING ruling so
    the runner can resume WATCH/merge for the real PR, and clear the
    crashed flag so subsequent cycles treat the task as live again."""
    h._patch_subprocess(monkeypatch)
    monkeypatch.setattr(
        idle_module.IdleMixin,
        "_select_next_task_from_dag",
        h._ORIGINAL_SELECT_NEXT_TASK_FROM_DAG,
    )

    tasks_dir = tmp_path / "tasks"
    tasks_dir.mkdir()
    (tasks_dir / "PR-001.md").write_text(
        "---\n---\n"
        "# PR-001: Crashed task with visible PR\n\n"
        "Branch: pr-001-crashed\n"
        "- Type: feature\n"
        "- Complexity: low\n"
        "- Depends on: none\n"
        "- Priority: 1\n"
        "- Coder: any\n",
        encoding="utf-8",
    )

    monkeypatch.setattr(idle_module, "_resolve_merged_state", lambda *args, **kwargs: _merged_state())

    runner = h._make_runner()
    runner.repo_path = str(tmp_path)
    runner._idle_open_prs = [PRInfo(number=42, branch="pr-001-crashed", pr_id="PR-001")]
    runner._idle_merged_prs = []
    runner._crashed_task_pr_ids.add("PR-001")

    task = asyncio.run(runner._select_next_task_from_dag())

    assert task is not None
    assert task.pr_id == "PR-001"
    assert task.status == TaskStatus.DOING
    assert runner._idle_dag_statuses == {"PR-001": TaskStatus.DOING}
    assert "PR-001" not in runner._crashed_task_pr_ids

def test_select_next_task_from_dag_uses_frontmatter_error_with_visible_pr(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Frontmatter ``status: ERROR`` is the durable exclusion signal,
    even when a still-open PR exists for the same branch."""
    h._patch_subprocess(monkeypatch)
    monkeypatch.setattr(
        idle_module.IdleMixin,
        "_select_next_task_from_dag",
        h._ORIGINAL_SELECT_NEXT_TASK_FROM_DAG,
    )

    tasks_dir = tmp_path / "tasks"
    tasks_dir.mkdir()
    (tasks_dir / "PR-001.md").write_text(
        "---\n"
        "status: ERROR\n"
        "---\n\n"
        "# PR-001: Trapped task\n\n"
        "Branch: pr-001-trapped\n"
        "- Type: feature\n"
        "- Complexity: low\n"
        "- Depends on: none\n"
        "- Priority: 1\n"
        "- Coder: any\n",
        encoding="utf-8",
    )
    (tasks_dir / "PR-002.md").write_text(
        "---\n---\n"
        "# PR-002: Healthy follow-up\n\n"
        "Branch: pr-002-healthy\n"
        "- Type: feature\n"
        "- Complexity: low\n"
        "- Depends on: none\n"
        "- Priority: 1\n"
        "- Coder: any\n",
        encoding="utf-8",
    )

    monkeypatch.setattr(idle_module, "_resolve_merged_state", lambda *args, **kwargs: _merged_state())

    runner = h._make_runner()
    runner.repo_path = str(tmp_path)
    runner._idle_open_prs = [
        PRInfo(number=42, branch="pr-001-trapped", pr_id="PR-001"),
    ]
    runner._idle_merged_prs = []

    task = asyncio.run(runner._select_next_task_from_dag())

    assert task is not None
    assert task.pr_id == "PR-002"
    assert task.status == TaskStatus.TODO
    assert runner._idle_dag_statuses == {
        "PR-001": TaskStatus.ERROR,
        "PR-002": TaskStatus.TODO,
    }
    assert runner._crashed_task_pr_ids == set()


def test_select_next_task_from_dag_clears_crashed_flag_when_done(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """A crashed task that ends up DONE (e.g. a stale merge surfaced after
    recovery) must clear the crashed flag so the task is not perpetually
    marked ERROR in regenerated QUEUE.md, and DONE remains terminal."""
    h._patch_subprocess(monkeypatch)
    monkeypatch.setattr(
        idle_module.IdleMixin,
        "_select_next_task_from_dag",
        h._ORIGINAL_SELECT_NEXT_TASK_FROM_DAG,
    )

    tasks_dir = tmp_path / "tasks"
    tasks_dir.mkdir()
    (tasks_dir / "PR-001.md").write_text(
        "---\n---\n"
        "# PR-001: Crashed but merged\n\n"
        "Branch: pr-001-merged\n"
        "- Type: feature\n"
        "- Complexity: low\n"
        "- Depends on: none\n"
        "- Priority: 1\n"
        "- Coder: any\n",
        encoding="utf-8",
    )

    monkeypatch.setattr(idle_module, "_resolve_merged_state", lambda *args, **kwargs: _merged_state({"PR-001"}))

    runner = h._make_runner()
    runner.repo_path = str(tmp_path)
    runner._idle_open_prs = []
    runner._idle_merged_prs = []
    runner._crashed_task_pr_ids.add("PR-001")

    asyncio.run(runner._select_next_task_from_dag())

    assert runner._idle_dag_statuses == {"PR-001": TaskStatus.DONE}
    assert "PR-001" not in runner._crashed_task_pr_ids


def test_recovery_uses_frontmatter_only_for_error_rehydrate(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Frontmatter ``status: ERROR`` is the only recovery rehydrate source."""
    monkeypatch.setattr("src.github.prs.get_open_prs", lambda repo, **kw: [])
    monkeypatch.setattr(
        "src.daemon.recovery._resolve_merged_state",
        lambda *args, **kwargs: _merged_state(),
    )

    tasks_dir = tmp_path / "tasks"
    tasks_dir.mkdir()
    (tasks_dir / "PR-100.md").write_text(
        "---\n"
        "status: ERROR\n"
        "---\n\n"
        "# PR-100: Error task\n\n"
        "Branch: pr-100-error\n"
        "- Type: feature\n"
        "- Complexity: low\n"
        "- Depends on: none\n"
        "- Priority: 1\n"
        "- Coder: any\n",
        encoding="utf-8",
    )

    runner = h._make_runner()
    runner.repo_path = str(tmp_path)

    asyncio.run(runner.recover_state())

    assert not hasattr(runner, "_recovered_task_pr_ids")
    assert runner._crashed_task_pr_ids == {"PR-100"}
    assert runner.state.state == PipelineState.IDLE


def test_dirty_tree_auto_recovery_after_3_cycles(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """After three consecutive dirty preflights the runner hard-resets
    to ``origin/{branch}`` and returns to IDLE instead of staying stuck
    in ERROR requiring manual intervention."""
    commands: list[list[str]] = []

    def fake_run(cmd: list[str], **kwargs: Any) -> h._FakeCompletedProcess:
        commands.append(cmd)
        if cmd[:3] == ["git", "status", "--porcelain"]:
            return h._FakeCompletedProcess(args=cmd, stdout=" M src/foo.py\n", returncode=0)
        return h._FakeCompletedProcess(args=cmd, stdout="", returncode=0)

    monkeypatch.setattr(runner_module.subprocess, "run", fake_run)
    runner = h._make_runner()

    assert asyncio.run(runner.preflight()) is False
    assert runner._consecutive_dirty_cycles == 1
    assert runner.state.state == PipelineState.ERROR

    assert asyncio.run(runner.preflight()) is False
    assert runner._consecutive_dirty_cycles == 2
    assert runner.state.state == PipelineState.ERROR

    assert asyncio.run(runner.preflight()) is True
    assert runner._consecutive_dirty_cycles == 0
    assert runner.state.state == PipelineState.IDLE
    assert runner.state.error_message is None
    assert any(cmd[:2] == ["git", "reset"] and "--hard" in cmd for cmd in commands)
    assert any(cmd[:3] == ["git", "clean", "-fd"] for cmd in commands)
    assert any("Auto-recovered from dirty tree" in e["event"] for e in runner.state.history)


def test_dirty_tree_auto_recovery_preserves_watch_with_open_pr(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When auto-recovery fires while a PR is being tracked, the runner
    must resume WATCH, not IDLE. Dropping to IDLE lets the next cycle
    re-pick the still-TODO task from origin/main's QUEUE.md and open a
    duplicate PR — exactly the churn this safety net is meant to
    avoid."""

    def fake_run(cmd: list[str], **kwargs: Any) -> h._FakeCompletedProcess:
        if cmd[:3] == ["git", "status", "--porcelain"]:
            return h._FakeCompletedProcess(args=cmd, stdout=" M src/foo.py\n", returncode=0)
        return h._FakeCompletedProcess(args=cmd, stdout="", returncode=0)

    monkeypatch.setattr(runner_module.subprocess, "run", fake_run)
    runner = h._make_runner()
    runner.state.current_pr = PRInfo(number=99, branch="pr-099-wip")
    runner.state.current_task = QueueTask(pr_id="PR-099", title="wip", status=TaskStatus.DOING, branch="pr-099-wip")

    asyncio.run(runner.preflight())
    asyncio.run(runner.preflight())
    assert asyncio.run(runner.preflight()) is True
    assert runner.state.state == PipelineState.WATCH
    assert runner.state.current_pr is not None
    assert runner.state.current_pr.number == 99
    assert runner.state.current_task is not None
    assert any("auto-recovered from dirty tree -> watch" in e["event"].lower() for e in runner.state.history)


def test_dirty_tree_counter_resets_on_clean(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The dirty-cycle counter must return to zero after any clean
    preflight so a transient glitch does not push a later cycle over
    the auto-reset threshold."""
    h._patch_subprocess(monkeypatch, stdout=" M foo.py")
    runner = h._make_runner()
    assert asyncio.run(runner.preflight()) is False
    assert runner._consecutive_dirty_cycles == 1

    h._patch_subprocess(monkeypatch, stdout="")
    assert asyncio.run(runner.preflight()) is True
    assert runner._consecutive_dirty_cycles == 0


def test_dirty_tree_auto_recovery_failure_stays_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When the auto-reset git commands themselves fail, preflight must
    leave the runner in ERROR so the operator still sees the issue
    rather than silently declaring the tree clean."""

    def fake_run(cmd: list[str], **kwargs: Any) -> h._FakeCompletedProcess:
        if cmd[:3] == ["git", "status", "--porcelain"]:
            return h._FakeCompletedProcess(args=cmd, stdout=" M src/foo.py\n", returncode=0)
        if cmd[:2] == ["git", "reset"]:
            raise subprocess.CalledProcessError(1, cmd, stderr="reset refused")
        return h._FakeCompletedProcess(args=cmd, stdout="", returncode=0)

    monkeypatch.setattr(runner_module.subprocess, "run", fake_run)
    runner = h._make_runner()

    asyncio.run(runner.preflight())
    asyncio.run(runner.preflight())
    assert asyncio.run(runner.preflight()) is False
    assert runner.state.state == PipelineState.ERROR
    assert any("Auto-recovery failed" in e["event"] for e in runner.state.history)


@pytest.mark.parametrize("validated_floor", [True, False])
def test_recover_state_rehydrates_last_push_at(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    validated_floor: bool,
) -> None:
    """Only a validated floor permits a pre-restart publication timestamp."""
    parsed_tasks = [
        QueueTask(
            pr_id="PR-001",
            title="t",
            status=TaskStatus.DOING,
            branch="pr-001",
            task_file="tasks/PR-001.md",
        )
    ]
    (tmp_path / "tasks").mkdir(parents=True)
    (tmp_path / "tasks" / "PR-001.md").write_text("# PR-001\n")

    head_iso = "2026-04-10T12:00:00Z"
    head = "a" * 40
    def publication_transport(args: list[str], **_kwargs: object) -> object:
        if "/pulls/" in args[1]:
            return {"branch": "pr-001", "repo": "octo/demo", "sha": head}
        if "/activity?" in args[1]:
            return [{"activity_type": "branch_creation", "ref": "refs/heads/pr-001",
                     "before": "0" * 40, "after": head, "timestamp": head_iso}]
        raise AssertionError(args)
    monkeypatch.setattr("src.github.gh_runner.run_gh", publication_transport)
    monkeypatch.setattr(
        "src.github.prs.get_open_prs",
        lambda repo, **kw: [PRInfo(number=7, branch="pr-001", head_sha=head)],
    )

    runner = h._make_runner()
    runner.repo_path = str(tmp_path)
    runner._parse_tasks_from_headers = lambda: parsed_tasks  # type: ignore[method-assign]
    # A raw PR observation can be saved by _record_pre_pause_pr without
    # validation. It must not become a safety floor on restart.
    saved = RepoState(
        url=runner.state.url, name=runner.state.name, state=PipelineState.WATCH,
        current_pr=PRInfo(
            number=7, branch="pr-001", head_sha=head,
            publication_observed=True,
            publication_at=runner_module.datetime.fromisoformat(
                head_iso.replace("Z", "+00:00")
            ),
            publication_head_repo="octo/demo", publication_ref="refs/heads/pr-001",
        ),
        publication_watermark=PublicationWatermark(
            repo="octo/demo", pr_number=7, head_repo="octo/demo",
            ref="refs/heads/pr-001", head_sha=head,
            published_at=datetime.fromisoformat(head_iso.replace("Z", "+00:00")),
            provenance="github-activity:branch_creation",
        ) if validated_floor else None,
    )
    asyncio.run(runner.redis.set(pipeline_state(runner.name), saved.model_dump_json()))
    assert runner._last_push_at is None
    assert runner._watch_entered_at is None
    asyncio.run(runner.recover_state())
    assert runner.state.state == PipelineState.WATCH
    if validated_floor:
        assert runner._last_push_at is not None
        assert runner._last_push_at.isoformat() == "2026-04-10T12:00:00+00:00"
    else:
        assert runner._last_push_at is None
        assert runner.state.publication_watermark is None
        assert runner.state.publication_migration is not None
        assert runner.state.publication_migration.status == "blocked"
    # PR-202: recovery anchors the slow-start window so the first
    # post-restart poll already uses the slow cadence.
    assert runner._watch_entered_at is not None


def test_recover_state_restores_prior_idle_pr_snapshots(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("src.github.prs.get_open_prs", lambda repo, **kw: [])

    runner = h._make_runner()
    prior_open = [PRInfo(number=1, branch="prior-open")]
    prior_merged = [PRInfo(number=2, branch="prior-merged")]
    runner._idle_open_prs = prior_open
    runner._idle_merged_prs = prior_merged
    runner._parse_tasks_from_headers = lambda: []  # type: ignore[method-assign]

    assert asyncio.run(runner.recover_state()) is True
    assert runner._idle_open_prs is prior_open
    assert runner._idle_merged_prs is prior_merged


def test_hydrate_current_task_ignores_corrupt_persisted_state() -> None:
    runner = h._make_runner()
    asyncio.run(runner.redis.set(f"pipeline:{runner.name}", "{not-json"))

    asyncio.run(runner._hydrate_current_task_from_persisted_state())

    assert runner.state.current_task is None


def test_hydrate_current_task_skips_when_current_task_already_set() -> None:
    runner = h._make_runner()
    task = QueueTask(
        pr_id="PR-266",
        title="Existing task",
        status=TaskStatus.DOING,
        branch="pr-266",
    )
    runner.state.current_task = task

    asyncio.run(runner._hydrate_current_task_from_persisted_state())

    assert runner.state.current_task == task


def test_hydrate_current_task_skips_non_coding_snapshot() -> None:
    runner = h._make_runner()
    task = QueueTask(
        pr_id="PR-266",
        title="Idle task",
        status=TaskStatus.DOING,
        branch="pr-266",
    )
    persisted = runner.state.model_copy(
        update={"state": PipelineState.IDLE, "current_task": task}
    )
    asyncio.run(runner.redis.set(f"pipeline:{runner.name}", persisted.model_dump_json()))

    asyncio.run(runner._hydrate_current_task_from_persisted_state())

    assert runner.state.current_task is None


def test_hydrate_current_task_restores_coding_snapshot() -> None:
    runner = h._make_runner()
    task = QueueTask(
        pr_id="PR-266",
        title="Recovered coding task",
        status=TaskStatus.DOING,
        branch="pr-266",
    )
    persisted = runner.state.model_copy(
        update={"state": PipelineState.CODING, "current_task": task}
    )
    asyncio.run(runner.redis.set(f"pipeline:{runner.name}", persisted.model_dump_json()))

    asyncio.run(runner._hydrate_current_task_from_persisted_state())

    assert runner.state.current_task == task


# ---------------------------------------------------------------------------
# PR-224b moved from tests/test_runner.py — recovery group
# ---------------------------------------------------------------------------


def test_run_cycle_resets_stale_transient_state(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    h._patch_subprocess(monkeypatch, stdout="")
    monkeypatch.setattr("src.github.prs.get_open_prs", lambda repo, **kw: [])

    runner = h._make_runner()
    # _recovered=True skips recover_state so this test exercises the
    # defensive transient-state reset, not the (separately tested)
    # recovery path that would have caught a mid-coding crash first.
    # _scaffolded=True skips the scaffold retry in ensure_repo_cloned
    # so this test focuses on the transient-state reset rather than
    # scaffolding behavior.
    runner._recovered = True
    runner._scaffolded = True
    runner.state.state = PipelineState.CODING  # simulate crash mid-coding
    asyncio.run(runner.run_cycle())

    # The stale CODING state was reset and handle_idle ran to completion.
    assert runner.state.state == PipelineState.IDLE
    assert any("stale transient state" in e["event"] for e in runner.state.history)
    assert isinstance(runner.redis, h._FakeRedis)
    assert runner.redis.writes, "publish_state should have been called"


def test_run_cycle_marks_recovery_complete_and_returns(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    publishes: list[str] = []
    runner = h._make_runner()

    async def fake_ensure_repo_cloned() -> None:
        return None

    async def fake_recover_state() -> bool:
        return True

    async def fake_publish_state() -> None:
        publishes.append("published")

    monkeypatch.setattr(runner, "ensure_repo_cloned", fake_ensure_repo_cloned)
    monkeypatch.setattr(runner, "recover_state", fake_recover_state)
    monkeypatch.setattr(runner, "publish_state", fake_publish_state)

    asyncio.run(runner.run_cycle())

    assert runner._recovered is True
    assert publishes == ["published"]


def test_run_cycle_runs_recovery_before_honoring_user_pause(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    publishes: list[str] = []
    recovery_calls: list[str] = []
    preflight_calls: list[str] = []
    runner = h._make_runner()
    runner.state.state = PipelineState.IDLE

    async def fake_ensure_repo_cloned() -> None:
        return None

    async def fake_refresh_user_paused_from_redis() -> None:
        runner.state.user_paused = True

    async def fake_recover_state() -> bool:
        recovery_calls.append("recover")
        return True

    async def fake_publish_state() -> None:
        publishes.append("published")

    monkeypatch.setattr(runner, "ensure_repo_cloned", fake_ensure_repo_cloned)
    monkeypatch.setattr(
        runner,
        "_refresh_user_paused_from_redis",
        fake_refresh_user_paused_from_redis,
    )
    monkeypatch.setattr(runner, "recover_state", fake_recover_state)
    monkeypatch.setattr(
        runner,
        "preflight",
        h._preflight_recording_stub(preflight_calls),
    )
    monkeypatch.setattr(runner, "publish_state", fake_publish_state)

    asyncio.run(runner.run_cycle())

    assert recovery_calls == ["recover"]
    assert preflight_calls == []
    assert publishes == ["published"]
    assert runner._recovered is True
    assert not any(entry["event"] == "[INFRA] Paused. Press Play to resume." for entry in runner.state.history)


# ---------------------------------------------------------------------------
# PR-266b: HEADERS_ONLY mode parameterized over PR-266a's golden fixtures
# ---------------------------------------------------------------------------


def _write_pr_md_for_fixture(
    repo: Path, task: dict[str, Any], priority_default: int = 3
) -> None:
    task_dir = repo / "tasks"
    task_dir.mkdir(parents=True, exist_ok=True)
    if task.get("legacy_unstructured"):
        (task_dir / f"{task['pr_id']}.md").write_text(
            f"# {task['pr_id']}: {task['title']}\n\nLegacy body\n"
        )
        return
    depends = task.get("depends_on") or []
    depends_value = ", ".join(depends) if depends else "none"
    frontmatter = ["---"]
    if task.get("frontmatter_status"):
        frontmatter.append(f"status: {task['frontmatter_status']}")
    frontmatter.extend(["---", ""])
    (task_dir / f"{task['pr_id']}.md").write_text(
        "\n".join(
            frontmatter
            + [
                f"# {task['pr_id']}: {task['title']}",
                "",
                f"Branch: {task['branch']}",
                f"- Type: {task.get('type', 'feature')}",
                f"- Complexity: {task.get('complexity', 'low')}",
                f"- Depends on: {depends_value}",
                f"- Priority: {task.get('priority', priority_default)}",
                f"- Coder: {task.get('coder', 'codex')}",
                "",
                "## Problem",
                "Body.",
                "",
            ]
        ),
        encoding="utf-8",
    )


def _setup_headers_recovery_fixture(
    monkeypatch: pytest.MonkeyPatch,
    repo: Path,
    before: dict[str, Any],
):
    repo.mkdir(parents=True, exist_ok=True)
    for task in before.get("tasks", []):
        _write_pr_md_for_fixture(repo, task)
    open_prs = [PRInfo(**pr) for pr in before.get("open_prs", [])]
    monkeypatch.setattr(
        "src.github.prs.get_open_prs", lambda repo_id, **kw: list(open_prs)
    )

    from src.daemon import recovery as recovery_module

    def fake_resolve(*args: object, **kwargs: object) -> MergedState:
        return MergedState(
            set(before.get("merged_pr_ids_via_git_log", [])),
            set(before.get("merged_branches_via_api", [])),
            True,
        )

    monkeypatch.setattr(recovery_module, "_resolve_merged_state", fake_resolve)

    runner = h._make_runner()
    runner.repo_path = str(repo)
    runner._crashed_task_pr_ids = set(before.get("crashed_task_pr_ids", []))

    if before.get("current_task_pr_id"):
        runner.state.current_task = QueueTask(
            pr_id=before["current_task_pr_id"],
            title="current",
            status=TaskStatus.DOING,
        )

    # PR-186/PR-220 crash fixtures: skip the unpushed-commit preserve probe.
    runner._preserve_crashed_run_commits = (  # type: ignore[method-assign]
        lambda branch: True
    )
    return runner


def _projection(runner: runner_module.PipelineRunner, tasks) -> dict[str, Any]:
    pr_id = (
        runner.state.current_task.pr_id
        if runner.state.current_task is not None
        else None
    )
    return {
        "pipeline_state": runner.state.state.value,
        "current_task_pr_id": pr_id,
        "current_pr_number": (
            runner.state.current_pr.number
            if runner.state.current_pr is not None
            else None
        ),
        "pending_queue_sync_branch": runner.state.pending_queue_sync_branch,
        "current_queue": [
            {
                "pr_id": t.pr_id,
                "title": t.title,
                "status": t.status.value,
                "task_file": t.task_file,
                "depends_on": list(t.depends_on),
                "branch": t.branch,
                "priority": t.priority,
            }
            for t in (tasks or [])
        ],
    }


def test_headers_mode_recovers_each_golden_fixture(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    recovery_golden_cases,
) -> None:
    """PR-266b success criterion #3: HEADERS_ONLY recovery on each fixture
    matches the golden expected.json projection."""
    import json as _json

    for scenario_name, before, expected in recovery_golden_cases:
        repo = tmp_path / scenario_name
        runner = _setup_headers_recovery_fixture(monkeypatch, repo, before)
        asyncio.run(runner.recover_state())
        actual = _projection(runner, runner.state.current_queue)
        assert actual == expected, (
            f"{scenario_name}: {_json.dumps(actual, sort_keys=True)} != "
            f"{_json.dumps(expected, sort_keys=True)}"
        )


def test_megaraid_scenario_resolved_in_headers_mode(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    recovery_golden_cases,
) -> None:
    """PR-266b success criterion #5: the MegaRAID golden scenario settles
    in IDLE under HEADERS_ONLY mode because the headers helper consults
    the API merged-branches set and produces DONE for the stale DOING
    entry, even though the legacy QUEUE.md row would have re-CODED."""
    case = next(
        (case for case in recovery_golden_cases if case[0] == "megaraid_already_merged_via_api"),
        None,
    )
    assert case is not None
    _, before, _ = case
    repo = tmp_path / "megaraid"
    runner = _setup_headers_recovery_fixture(monkeypatch, repo, before)

    asyncio.run(runner.recover_state())

    assert runner.state.state == PipelineState.IDLE
    assert runner.state.current_task is None
    assert runner.state.current_queue is not None
    assert [t.status for t in runner.state.current_queue] == [TaskStatus.DONE]
