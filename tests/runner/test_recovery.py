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
import hashlib
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
from src.cancellation import (
    CancellationCause,
    get_cancellation_cause,
    safe_record_cancellation_cause,
)
from src.cancellation.storage import task_spec_content_hash
from src.daemon import recovery as recovery_module
from src.daemon import runner as runner_module
from src.daemon.handlers import coding as coding_module  # noqa: F401,F811
from src.daemon.handlers import error as error_module  # noqa: F401,F811
from src.daemon.handlers import idle as idle_module  # noqa: F811
from src.daemon.handlers import merge as merge_module  # noqa: F401,F811
from src.daemon.handlers import watch as watch_module  # noqa: F401,F811
from src.github import checks as gh_checks
from src.github import prs as gh_prs
from src.keyspace import pipeline_state
from src.metrics import RunRecord
from src.models import (
    CIStatus,
    InvocationSnapshot,
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
from src.subsource_registry import SuppressionReason
from src.task_status import MergedState

from tests.runner import _helpers as h

_CONFIRM_REMOTE_MERGE = merge_module.MergeMixin._confirm_remote_merge


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


def test_parent_snapshot_620_requires_confirmation_for_branch_creation(
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
    proposal = runner.state.publication_migration
    assert proposal is not None and proposal.source == "parent_snapshot"
    assert proposal.published_at == published
    assert proposal.provenance == "github-activity:branch_creation"
    assert runner.state.publication_watermark is None
    assert runner._last_push_at is None

    asyncio.run(runner.publish_state())
    restarted = h._make_runner(url=f"https://github.com/{repo}.git")
    restarted.redis = runner.redis
    restarted.repo_path = str(tmp_path)
    restarted._parse_tasks_from_headers = runner._parse_tasks_from_headers  # type: ignore[method-assign]
    assert asyncio.run(restarted.recover_state()) is True
    assert restarted.state.publication_migration is not None
    assert restarted.state.publication_watermark is None
    binding = migration_binding(restarted.state.publication_migration)
    asyncio.run(enqueue_migration(restarted.redis, restarted.name, binding))
    assert asyncio.run(restarted._consume_publication_migration_command()) is True
    assert restarted.state.publication_watermark is not None
    assert restarted.state.publication_watermark.published_at == published
    asyncio.run(restarted.publish_state())
    confirmed = h._make_runner(url=f"https://github.com/{repo}.git")
    confirmed.redis = restarted.redis
    confirmed.repo_path = str(tmp_path)
    confirmed._parse_tasks_from_headers = runner._parse_tasks_from_headers  # type: ignore[method-assign]
    assert asyncio.run(confirmed.recover_state()) is True
    assert confirmed.state.publication_watermark == restarted.state.publication_watermark

    # The saved floor cannot replace a failed current activity read.
    monkeypatch.setattr("src.github.gh_runner.run_gh", lambda *_args, **_kwargs: {})
    assert restarted._canonical_push_timestamp(620) is None


@pytest.mark.parametrize("phase", ["pending", "deferred"])
@pytest.mark.parametrize("change", ["same", "head", "event", "generation"])
def test_saved_parent_proposal_command_survives_paused_restart(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, phase: str, change: str,
) -> None:
    """A raw PR observation must not replace an already authorized proposal."""
    repo = "AlexBomber12/pipeline-orchestrator"
    branch = "pr-459-e2e-failure-evidence"
    head = "63f07ef48a035c9aa31c37e0fbf3dc215ba0045e"
    published = datetime(2026, 10, 17, 21, 6, 35, tzinfo=timezone.utc)
    current = {"activity": True, "head": head, "timestamp": "2026-10-17T21:06:35Z"}

    def transport(args: list[str], **_kwargs: object) -> object:
        if "/pulls/620" in args[1]:
            return {"branch": branch, "repo": repo, "sha": current["head"]}
        if "/activity?" in args[1]:
            return ([{"activity_type": "branch_creation", "ref": f"refs/heads/{branch}",
                     "before": "0" * 40, "after": current["head"],
                     "timestamp": current["timestamp"]}]
                    if current["activity"] else [])
        raise AssertionError(args)

    def open_prs(owner: str, *_args: object, **_kwargs: object) -> list[PRInfo]:
        evidence = gh_prs.get_pr_head_publication(owner, 620, expected_head_sha=current["head"])
        return [PRInfo(
            number=620, branch=branch, pr_id="PR-459", head_sha=current["head"],
            publication_observed=True, publication_at=evidence.published_at,
            publication_reason=evidence.unknown_reason,
            publication_repo=evidence.repo, publication_head_repo=evidence.head_repo,
            publication_ref=evidence.ref, publication_head_sha=evidence.head_sha,
            publication_provenance=evidence.provenance,
        )]

    monkeypatch.setattr("src.github.gh_runner.run_gh", transport)
    monkeypatch.setattr(gh_prs, "get_open_prs", open_prs)
    runner = h._make_runner(url=f"https://github.com/{repo}.git")
    runner.repo_path = str(tmp_path)
    task = QueueTask(pr_id="PR-459", title="e2e failure evidence",
                     status=TaskStatus.DOING, branch=branch,
                     task_file="tasks/PR-459.md")
    runner._parse_tasks_from_headers = lambda: [task]  # type: ignore[method-assign]
    snapshot = (Path(__file__).parents[1] / "fixtures/recovery/pr_620_parent_snapshot.json").read_text()
    asyncio.run(runner.redis.set(pipeline_state(runner.name), snapshot))
    assert asyncio.run(runner.recover_state()) is True
    proposal = runner.state.publication_migration
    assert proposal is not None and proposal.source == "parent_snapshot"
    assert runner.state.current_pr.publication_at == published
    proposal.generation = 3
    proposal.last_complete_published_at = published
    runner.state.user_paused = True
    asyncio.run(runner.redis.set(pipeline_state(runner.name), runner.state.model_dump_json()))
    binding = migration_binding(proposal)
    asyncio.run(enqueue_migration(runner.redis, runner.name, binding))
    key = migration_command_key(runner.name, binding)

    if phase == "deferred":
        current["activity"] = False
        runner._recovered = True

        async def no_work() -> None:
            return None

        async def yes() -> bool:
            return True

        monkeypatch.setattr(runner, "ensure_repo_cloned", no_work)
        monkeypatch.setattr(runner, "_check_github_api_budget", yes)
        asyncio.run(runner.run_cycle())
        assert MigrationCommand.model_validate_json(runner.redis.store[key]).status == "deferred"
        saved = RepoState.model_validate_json(runner.redis.store[pipeline_state(runner.name)])
        assert saved.user_paused and saved.publication_watermark is None
        assert saved.current_pr.publication_at == published
        assert saved.publication_migration.source == "parent_snapshot"
        current["activity"] = True

    restarted = h._make_runner(url=f"https://github.com/{repo}.git")
    restarted.redis = runner.redis
    restarted.repo_path = str(tmp_path)
    restarted._parse_tasks_from_headers = runner._parse_tasks_from_headers  # type: ignore[method-assign]
    assert asyncio.run(restarted.recover_state()) is True
    restored = restarted.state.publication_migration
    assert restored is not None
    assert restored.source == "parent_snapshot"
    assert restored.generation == 3
    assert restored.last_complete_published_at == published
    assert migration_binding(restored) == binding
    assert restarted.state.publication_watermark is None

    current["activity"] = False
    assert asyncio.run(restarted._consume_publication_migration_command()) is False
    assert MigrationCommand.model_validate_json(restarted.redis.store[key]).status == "deferred"
    assert restarted.state.publication_watermark is None
    current["activity"] = True
    if change == "head":
        current["head"] = "b" * 40
    elif change == "event":
        current["timestamp"] = "2026-10-17T21:07:00Z"
    elif change == "generation":
        restarted.state.publication_migration.generation += 1
        asyncio.run(restarted.publish_state())
    assert asyncio.run(restarted._consume_publication_migration_command()) is True
    receipt = MigrationCommand.model_validate_json(restarted.redis.store[key])
    assert receipt.status == ("applied" if change == "same" else "failed")
    assert (restarted.state.publication_watermark is not None) == (change == "same")
    assert asyncio.run(restarted.redis.get(migration_pending_key(restarted.name))) is None


@pytest.mark.parametrize("kind", [
    "missing", "blocked", "foreign_repo", "foreign_task", "foreign_url",
    "incompatible", "corrupt",
])
def test_raw_observation_does_not_rescue_invalid_saved_proposal(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, kind: str,
) -> None:
    runner = h._make_runner()
    runner.repo_path = str(tmp_path)
    published = datetime(2026, 10, 17, 21, 6, 35, tzinfo=timezone.utc)
    task = QueueTask(pr_id="PR-42", title="feature", status=TaskStatus.DOING,
                     branch="feature", task_file="tasks/PR-42.md")
    pr = PRInfo(
        number=42, branch="feature", pr_id="PR-42", head_sha="a" * 40,
        publication_observed=True, publication_at=published,
        publication_repo="octo/demo", publication_head_repo="octo/demo",
        publication_ref="refs/heads/feature", publication_head_sha="a" * 40,
        publication_provenance="github-activity:push",
    )
    runner._parse_tasks_from_headers = lambda: [task]  # type: ignore[method-assign]
    monkeypatch.setattr(gh_prs, "get_open_prs", lambda *_args, **_kwargs: [pr])
    monkeypatch.setattr(gh_prs, "get_pr_head_publication", lambda *_args, **_kwargs:
                        gh_prs.HeadPublicationEvidence(
                            "octo/demo", 42, "octo/demo", "refs/heads/feature",
                            "a" * 40, published, "github-activity:push"))
    proposal = recovery_module.PublicationMigration(
        repo="octo/demo", pr_number=42, head_repo="octo/demo",
        ref="refs/heads/feature", head_sha="a" * 40,
        published_at=published, provenance="github-activity:push",
        source="parent_snapshot", generation=2,
        reason="Historical publication needs an operator trust anchor.",
    )
    saved = RepoState(url=runner.state.url, name=runner.name,
                      state=PipelineState.WATCH, current_task=task,
                      current_pr=pr, publication_migration=proposal)
    raw = saved.model_dump(mode="json")
    if kind == "missing":
        raw["publication_migration"] = None
    elif kind == "blocked":
        raw["publication_migration"]["status"] = "blocked"
    elif kind == "foreign_repo":
        raw["publication_migration"]["repo"] = "other/repo"
    elif kind == "foreign_task":
        raw["current_task"]["branch"] = "other-branch"
    elif kind == "foreign_url":
        raw["url"] = "https://github.com/other/repo.git"
    elif kind == "incompatible":
        raw["publication_schema_version"] = 2
    else:
        raw["publication_migration"] = {"repo": "broken"}
    asyncio.run(runner.redis.set(pipeline_state(runner.name), json.dumps(raw)))
    assert asyncio.run(runner.recover_state()) is True
    assert runner.state.publication_watermark is None
    assert runner.state.publication_migration is not None
    if kind == "missing":
        assert runner.state.publication_migration.source == "unvalidated_current_snapshot"
        assert runner.state.publication_migration.status == "legacy"
    else:
        assert runner.state.publication_migration.status == "blocked"


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


def test_failed_github_recovery_preserves_saved_floor_until_retry(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    runner = h._make_runner()
    runner.repo_path = str(tmp_path)
    task = QueueTask(pr_id="PR-007", title="test", status=TaskStatus.DOING,
                     branch="pr-007", task_file="tasks/PR-007.md")
    pr = PRInfo(number=7, branch="pr-007", head_sha="a" * 40, pr_id="PR-007")
    publication = datetime(2026, 4, 10, 12, tzinfo=timezone.utc)
    saved = RepoState(
        url=runner.state.url, name=runner.state.name, state=PipelineState.WATCH,
        current_task=task, current_pr=pr,
        publication_watermark=PublicationWatermark(
            repo="octo/demo", pr_number=7, head_repo="octo/demo",
            ref="refs/heads/pr-007", head_sha="a" * 40,
            published_at=publication, provenance="github-activity:branch_creation",
        ),
    )
    original = saved.model_dump_json()
    asyncio.run(runner.redis.set(pipeline_state(runner.name), original))
    runner._parse_tasks_from_headers = lambda: [task]  # type: ignore[method-assign]
    monkeypatch.setattr(runner, "_cleanup_expected_branch", lambda: None)

    async def no_clone() -> None:
        return None

    async def enough_budget() -> bool:
        return True

    monkeypatch.setattr(runner, "ensure_repo_cloned", no_clone)
    monkeypatch.setattr(runner, "_check_github_api_budget", enough_budget)
    attempts = 0

    def fetch_prs(*_args, **_kwargs):
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise RuntimeError("temporary GitHub outage")
        return [pr]

    monkeypatch.setattr(gh_prs, "get_open_prs", fetch_prs)
    monkeypatch.setattr(gh_prs, "get_pr_head_publication", lambda *_args, **_kwargs:
                        gh_prs.HeadPublicationEvidence(
                            "octo/demo", 7, "octo/demo", "refs/heads/pr-007",
                            "a" * 40, publication, "github-activity:branch_creation",
                        ))
    asyncio.run(runner.run_cycle())
    assert runner._recovered is False
    assert asyncio.run(runner.redis.get(pipeline_state(runner.name))) == original

    asyncio.run(runner.run_cycle())
    assert runner._recovered is True
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
    assert asyncio.run(runner._consume_publication_migration_command()) is False
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
    "tampered_receipt", "missing_bound_time", "changed_source", "changed_generation",
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
    elif case in {"tampered_receipt", "missing_bound_time"}:
        command = MigrationCommand.model_validate_json(runner.redis.store[key])
        if case == "tampered_receipt":
            command.observation.published_at += timedelta(seconds=1)
        else:
            command.observation.published_at = None
        runner.redis.store[key] = command.model_dump_json()
    elif case in {"changed_source", "changed_generation"}:
        if case == "changed_source":
            runner.state.publication_migration.source = "rebound_pr"
        else:
            runner.state.publication_migration.generation = 1
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
    assert asyncio.run(runner._consume_publication_migration_command()) is (
        case not in {"missing_receipt", "applied_receipt", "failed_receipt", "missing_time", "unknown_evidence"}
    )
    if case == "missing_receipt":
        assert any("receipt missing" in e["event"] for e in runner.state.history)
    elif case == "unknown_evidence":
        assert MigrationCommand.model_validate_json(runner.redis.store[key]).status == "deferred"
    elif case not in {"applied_receipt", "failed_receipt"}:
        assert MigrationCommand.model_validate_json(runner.redis.store[key]).status in {"failed", "deferred"}
        assert runner.state.publication_watermark is None


def test_failed_migration_receipt_retries_only_current_bound_observation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner, binding = _migration_command_runner()
    key = migration_command_key(runner.name, binding)
    monkeypatch.setattr(gh_prs, "get_open_prs", lambda *_args, **_kwargs: [])
    assert asyncio.run(runner._consume_publication_migration_command()) is True
    assert MigrationCommand.model_validate_json(runner.redis.store[key]).status == "failed"
    assert runner.state.publication_watermark is None

    monkeypatch.setattr(gh_prs, "get_open_prs", lambda *_args, **_kwargs: [runner.state.current_pr])
    monkeypatch.setattr(gh_prs, "get_pr_head_publication", lambda *_args, **_kwargs:
                        gh_prs.HeadPublicationEvidence(
                            "octo/demo", 42, "octo/demo", "refs/heads/feature",
                            "a" * 40, runner.state.publication_migration.published_at,
                            "github-activity:push",
                        ))
    retried = asyncio.run(enqueue_migration(runner.redis, runner.name, binding))
    assert retried.status == "pending"
    assert asyncio.run(runner._consume_publication_migration_command()) is True
    assert MigrationCommand.model_validate_json(runner.redis.store[key]).status == "applied"
    assert runner.state.publication_watermark.pr_number == 42


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
def test_parent_proposal_rebinds_changed_pr_identity(
    change: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner, _binding = _migration_command_runner()
    proposal = runner.state.publication_migration
    pr = runner.state.current_pr
    evidence = gh_prs.HeadPublicationEvidence(
        "octo/demo", 42, "octo/demo", "refs/heads/feature", "a" * 40,
        proposal.published_at, "github-activity:push",
    )
    if change == "target":
        pr = pr.model_copy(update={"head_sha": "b" * 40})
        evidence = gh_prs.HeadPublicationEvidence(
            "octo/demo", 42, "octo/demo", "refs/heads/feature", "b" * 40,
            proposal.published_at + timedelta(seconds=1), "github-activity:push",
        )
    else:
        pr = pr.model_copy(update={"is_cross_repository": True})
        evidence = gh_prs.HeadPublicationEvidence(
            "octo/demo", 42, "other/repo", "refs/heads/feature", "a" * 40,
            proposal.published_at, "github-activity:push",
        )
    runner._offer_publication_migration(pr, evidence)
    refreshed = runner.state.publication_migration
    assert refreshed is not proposal
    assert refreshed.status == "legacy"
    assert refreshed.published_at == evidence.published_at
    assert migration_binding(refreshed) != _binding
    runner.state.current_pr = pr
    asyncio.run(runner.redis.set(pipeline_state(runner.name), runner.state.model_dump_json()))
    monkeypatch.setattr(gh_prs, "get_open_prs", lambda *_args, **_kwargs: [pr])
    monkeypatch.setattr(gh_prs, "get_pr_head_publication", lambda *_args, **_kwargs: evidence)
    assert asyncio.run(runner._consume_publication_migration_command()) is True
    assert MigrationCommand.model_validate_json(
        runner.redis.store[migration_command_key(runner.name, _binding)]
    ).status == "failed"
    assert runner.state.publication_watermark is None
    fresh_binding = migration_binding(refreshed)
    asyncio.run(enqueue_migration(runner.redis, runner.name, fresh_binding))
    assert asyncio.run(runner._consume_publication_migration_command()) is True
    assert MigrationCommand.model_validate_json(
        runner.redis.store[migration_command_key(runner.name, fresh_binding)]
    ).status == "applied"
    assert runner.state.publication_watermark.head_sha == pr.head_sha
    assert runner.state.publication_watermark.head_repo == evidence.head_repo


def test_migration_binding_does_not_revert_after_a_to_b_to_a(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner, original_binding = _migration_command_runner()
    original = runner.state.publication_migration
    head_a = original.head_sha
    head_b = "b" * 40
    event_a = gh_prs.HeadPublicationEvidence(
        "octo/demo", 42, "octo/demo", "refs/heads/feature", head_a,
        original.published_at, "github-activity:push",
    )
    event_b = gh_prs.HeadPublicationEvidence(
        "octo/demo", 42, "octo/demo", "refs/heads/feature", head_b,
        original.published_at + timedelta(seconds=10), "github-activity:push",
    )
    pr_b = runner.state.current_pr.model_copy(update={"head_sha": head_b})
    runner._offer_publication_migration(pr_b, event_b)
    runner._offer_publication_migration(runner.state.current_pr, event_a)
    proposal = runner.state.publication_migration
    assert proposal.head_sha == head_a
    assert proposal.published_at == original.published_at
    assert migration_binding(proposal) != original_binding
    asyncio.run(runner.redis.set(pipeline_state(runner.name), runner.state.model_dump_json()))
    monkeypatch.setattr(gh_prs, "get_open_prs", lambda *_args, **_kwargs: [runner.state.current_pr])
    monkeypatch.setattr(gh_prs, "get_pr_head_publication", lambda *_args, **_kwargs: event_a)
    assert asyncio.run(runner._consume_publication_migration_command()) is True
    assert MigrationCommand.model_validate_json(
        runner.redis.store[migration_command_key(runner.name, original_binding)]
    ).status == "failed"
    assert runner.state.publication_watermark is None


@pytest.mark.parametrize("foreign_context", ["repo", "task_branch"])
def test_migration_reconciliation_rejects_foreign_context(foreign_context: str) -> None:
    runner, _binding = _migration_command_runner()
    runner.state.current_task = QueueTask(
        pr_id="PR-42", title="feature", status=TaskStatus.DOING,
        branch="feature", task_file="tasks/PR-42.md",
    )
    proposal = runner.state.publication_migration
    pr = runner.state.current_pr
    if foreign_context == "repo":
        proposal.repo = "other/repo"
    else:
        pr = pr.model_copy(update={"branch": "other-branch"})
    evidence = gh_prs.HeadPublicationEvidence(
        "octo/demo", 42, "octo/demo", f"refs/heads/{pr.branch}", pr.head_sha,
        proposal.published_at, "github-activity:push",
    )
    runner._offer_publication_migration(pr, evidence)
    assert runner.state.publication_migration.status == "blocked"
    assert runner.state.publication_watermark is None


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
        if cmd[1:4] == ["rev-parse", "--abbrev-ref", "HEAD"]:
            return h._FakeCompletedProcess(args=cmd, stdout="pr-099-wip\n")
        if cmd[1:5] == ["rev-parse", "-q", "--verify", "MERGE_HEAD"]:
            return h._FakeCompletedProcess(args=cmd, returncode=1)
        if cmd[1:3] == ["rev-parse", "HEAD"]:
            return h._FakeCompletedProcess(args=cmd, stdout="a" * 40)
        return h._FakeCompletedProcess(args=cmd, stdout="", returncode=0)

    monkeypatch.setattr(runner_module.subprocess, "run", fake_run)
    runner = h._make_runner()
    runner.state.current_pr = PRInfo(number=99, branch="pr-099-wip", head_sha="a" * 40)
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
        lambda repo, *args, **kw: [PRInfo(number=7, branch="pr-001", head_sha=head)],
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
            publication_repo="octo/demo", publication_head_sha=head,
                publication_provenance="github-activity:branch_creation",
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
        assert runner.state.publication_migration.status == "legacy"
        assert runner.state.publication_migration.source == "unvalidated_current_snapshot"
        assert runner.state.publication_migration.published_at is not None
        asyncio.run(runner.publish_state())
        binding = migration_binding(runner.state.publication_migration)
        asyncio.run(enqueue_migration(runner.redis, runner.name, binding))
        assert asyncio.run(runner._consume_publication_migration_command()) is True
        assert runner.state.publication_watermark is not None
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


def _saved_review_state(
    runner: object, tmp_path: Path, *, error: bool,
) -> tuple[QueueTask, PRInfo, RepoState]:
    runner.repo_path = str(tmp_path)
    task_path = tmp_path / "tasks" / "PR-405.md"
    task_path.parent.mkdir()
    task_path.write_text("# PR-405\nBranch: pr-405\n", encoding="utf-8")
    task = QueueTask(
        pr_id="PR-405", title="Review", branch="pr-405",
        task_file="tasks/PR-405.md",
        status=TaskStatus.ERROR if error else TaskStatus.DOING,
    )
    pr = PRInfo(number=640, branch="pr-405", head_sha="a" * 40, pr_id="PR-405")
    saved = runner.state.model_copy(deep=True)
    saved.state = PipelineState.ERROR if error else PipelineState.WATCH
    saved.current_task = task
    saved.current_pr = pr
    saved.review_request_pr = pr.number
    saved.review_request_head = pr.head_sha
    saved.review_request_task = task.pr_id
    saved.review_request_generation = task_spec_content_hash(task_path.read_text())
    saved.review_requested_at = datetime.now(timezone.utc) - timedelta(minutes=31)
    saved.review_post_attempted_at = datetime.now(timezone.utc) - timedelta(minutes=11)
    saved.review_reminder_count = 3
    return task, pr, saved


def test_watch_recovery_restores_current_head_reminder_budget(
    tmp_path: Path,
) -> None:
    runner = h._make_runner()
    task, pr, saved = _saved_review_state(runner, tmp_path, error=False)
    asyncio.run(runner.redis.set(pipeline_state(runner.name), saved.model_dump_json()))
    runner._recovery_task_headers = {}
    runner._rehydrate_last_push_at = lambda *_args: None
    assert asyncio.run(runner._hydrate_current_task_from_persisted_state()) is True
    asyncio.run(runner._apply_recovery_decisions([task], [pr]))
    assert runner.state.state == PipelineState.WATCH
    assert runner.state.review_reminder_count == 3
    assert runner.state.review_post_attempted_at == saved.review_post_attempted_at


def test_watch_recovery_restores_unconfirmed_initial_attempt(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = h._make_runner()
    task, pr, saved = _saved_review_state(runner, tmp_path, error=False)
    saved.review_requested_at = None
    saved.review_post_attempted_at = datetime.now(timezone.utc) - timedelta(minutes=31)
    saved.review_reminder_count = 0
    asyncio.run(runner.redis.set(pipeline_state(runner.name), saved.model_dump_json()))

    fresh = h._make_runner()
    fresh.redis = runner.redis
    fresh.repo_path = str(tmp_path)
    fresh._recovery_task_headers = {}
    fresh._rehydrate_last_push_at = lambda *_args: None
    assert asyncio.run(fresh._hydrate_current_task_from_persisted_state()) is True
    asyncio.run(fresh._apply_recovery_decisions([task], [pr]))
    assert fresh.state.state == PipelineState.WATCH
    assert fresh.state.review_request_pr == pr.number
    assert fresh.state.review_request_head == pr.head_sha
    assert fresh.state.review_request_task == task.pr_id
    assert fresh.state.review_request_generation == saved.review_request_generation
    assert fresh.state.review_requested_at is None
    assert fresh.state.review_post_attempted_at == saved.review_post_attempted_at
    assert fresh.state.review_reminder_count == 0
    monkeypatch.setattr(
        "src.github.comments.latest_codex_review_request_time",
        lambda *_args, **_kwargs: None,
    )
    posts: list[int] = []
    fresh._post_codex_review_result = lambda number, **_kwargs: (
        posts.append(number) is None, True, None
    )
    fresh._watch_retrigger_inhibited = lambda *_args: False
    observed = pr.model_copy(update={
        "publication_at": datetime.now(timezone.utc) - timedelta(minutes=40),
    })
    asyncio.run(fresh._maybe_remind_review(observed))
    asyncio.run(fresh._maybe_remind_review(observed))
    assert posts == [pr.number]
    assert fresh.state.review_reminder_count == 1


@pytest.mark.parametrize("change", ["changed_generation", "missing_task_file"])
@pytest.mark.parametrize("unconfirmed", [False, True])
def test_watch_recovery_rejects_unverified_task_generation(
    tmp_path: Path, change: str, unconfirmed: bool,
) -> None:
    runner = h._make_runner()
    task, pr, saved = _saved_review_state(runner, tmp_path, error=False)
    if unconfirmed:
        saved.review_requested_at = None
    asyncio.run(runner.redis.set(pipeline_state(runner.name), saved.model_dump_json()))
    task_path = tmp_path / "tasks/PR-405.md"
    if change == "changed_generation":
        task_path.write_text("# PR-405 changed\nBranch: pr-405\n", encoding="utf-8")
    else:
        task_path.unlink()
    runner._recovery_task_headers = {}
    runner._rehydrate_last_push_at = lambda *_args: None
    assert asyncio.run(runner._hydrate_current_task_from_persisted_state()) is True
    asyncio.run(runner._apply_recovery_decisions([task], [pr]))
    assert runner.state.state == PipelineState.WATCH
    assert runner.state.review_reminder_count == 0


@pytest.mark.parametrize("single_error_exit", [False, True])
def test_legacy_review_timeout_recovery_keeps_bound_pr(
    tmp_path: Path, single_error_exit: bool,
) -> None:
    runner = h._make_runner()
    runner.repo_config.feature_flags.use_single_error_exit = single_error_exit
    task, pr, saved = _saved_review_state(runner, tmp_path, error=True)
    saved.skip_ai_error_diagnose = True
    saved.error_message = "review timeout"
    asyncio.run(runner.redis.set(pipeline_state(runner.name), saved.model_dump_json()))
    runner._recovery_task_headers = {
        task.pr_id: types.SimpleNamespace(
            frontmatter_status="error",
            blocked_reason=SuppressionReason.REVIEW_TIMEOUT.value,
        )
    }
    assert asyncio.run(runner._hydrate_current_task_from_persisted_state()) is True
    asyncio.run(runner._apply_recovery_decisions([task], [pr]))
    assert runner.state.state == PipelineState.ERROR
    assert runner.state.current_task.pr_id == task.pr_id
    assert runner.state.current_pr.head_sha == pr.head_sha
    assert runner.state.review_reminder_count == 3
    assert runner.state.skip_ai_error_diagnose is True
    assert task.pr_id not in runner._crashed_task_pr_ids


@pytest.mark.parametrize("status_write_pending", [False, True])
@pytest.mark.parametrize("paused", [False, True])
def test_restart_restores_bound_merge_failure_and_timeout_evidence(
    tmp_path: Path, status_write_pending: bool, paused: bool,
) -> None:
    original = h._make_runner()
    task, pr, saved = _saved_review_state(
        original, tmp_path, error=not status_write_pending
    )
    saved.error_message = (
        "Merge conflict resolution failed (exit -1): Timeout after 300s; "
        "operator Retry required"
    )
    if paused:
        saved.state = PipelineState.PAUSED
    asyncio.run(original.redis.set(pipeline_state(original.name), saved.model_dump_json()))

    fresh = h._make_runner()
    fresh.redis = original.redis
    fresh.repo_path = str(tmp_path)
    fresh._recovery_task_headers = {
        task.pr_id: types.SimpleNamespace(
            frontmatter_status="doing" if status_write_pending else "error",
            blocked_reason=SuppressionReason.CRASH.value,
        )
    }
    assert asyncio.run(fresh._hydrate_current_task_from_persisted_state()) is True
    if status_write_pending:
        fresh._status_write_failed_task_pr_ids.add(task.pr_id)

    asyncio.run(fresh._apply_recovery_decisions([task], [pr]))

    assert fresh.state.state == PipelineState.ERROR
    assert fresh.state.current_task.pr_id == task.pr_id
    assert fresh.state.current_pr.number == pr.number
    assert fresh.state.error_message == saved.error_message


@pytest.mark.parametrize(
    "checkout", ["feature", "detached", "other", "status_error", "branch_error"],
)
def test_restart_without_snapshot_retains_dirty_error_pr_checkout(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, checkout: str,
) -> None:
    from unittest.mock import AsyncMock

    from src.daemon import git_ops as git_ops_module
    from src.daemon.retry_commands import RetryDispatch

    def git(*args: str, check: bool = True) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            ["git", *args], cwd=tmp_path, check=check, text=True,
            capture_output=True,
        )

    git("init", "-q", "-b", "main")
    git("config", "user.email", "test@example.com")
    git("config", "user.name", "Test")
    (tmp_path / "example.txt").write_text("base\n")
    git("add", "example.txt")
    git("commit", "-qm", "base")
    git("checkout", "-qb", "feature")
    (tmp_path / "example.txt").write_text("feature\n")
    git("commit", "-qam", "feature")
    feature_head = git("rev-parse", "HEAD").stdout.strip()
    git("checkout", "main")
    (tmp_path / "example.txt").write_text("main\n")
    git("commit", "-qam", "main")
    git("update-ref", "refs/remotes/origin/main", "HEAD")
    if checkout == "detached":
        git("checkout", "--detach", "feature")
    elif checkout == "other":
        git("checkout", "-qb", "other")
    else:
        git("checkout", "feature")
    checkout_head = git("rev-parse", "HEAD").stdout.strip()
    (tmp_path / "example.txt").write_text("unpublished resolution\n")
    (tmp_path / "notes.txt").write_text("untracked resolver notes\n")
    unpublished = (tmp_path / "example.txt").read_text()

    runner = h._make_runner()
    runner.repo_path = str(tmp_path)
    task = QueueTask(
        pr_id="PR-404", title="conflict", status=TaskStatus.ERROR,
        branch="feature", task_file="tasks/PR-404.md",
    )
    pr = PRInfo(number=636, branch="feature", pr_id=task.pr_id,
                head_sha=feature_head)
    runner._recovery_task_headers = {
        task.pr_id: types.SimpleNamespace(
            frontmatter_status="error",
            blocked_reason=SuppressionReason.CRASH.value,
        )
    }

    assert asyncio.run(runner._hydrate_current_task_from_persisted_state()) is True
    assert runner._publication_snapshot_absent is True
    if checkout.endswith("_error"):
        original_git = git_ops_module._git
        failed_probe = "status" if checkout == "status_error" else "rev-parse"

        def unavailable(repo_path: str, *args: str, **kwargs: Any):
            if args[0] == failed_probe:
                raise subprocess.TimeoutExpired(["git", *args], 30)
            return original_git(repo_path, *args, **kwargs)

        monkeypatch.setattr(git_ops_module, "_git", unavailable)
    asyncio.run(runner._apply_recovery_decisions([task], [pr]))
    if checkout.endswith("_error"):
        monkeypatch.setattr(git_ops_module, "_git", original_git)

    assert runner.state.state == PipelineState.ERROR
    assert runner.state.error_message.startswith(
        "Published task failure unresolved; unpublished checkout retained"
    )
    runner._recovered = True
    runner._consecutive_dirty_cycles = 2
    for method in (
        "_hold_for_coder_cleanup", "_consume_approval_command",
        "_consume_publication_migration_command", "_maybe_resume_review_timeout",
    ):
        monkeypatch.setattr(runner, method, AsyncMock(return_value=False))
    monkeypatch.setattr(runner, "_consume_retry_command", AsyncMock(return_value=RetryDispatch.NONE))
    monkeypatch.setattr(runner, "ensure_repo_cloned", AsyncMock())
    monkeypatch.setattr(runner, "_refresh_user_paused_from_redis", AsyncMock())
    monkeypatch.setattr(runner, "_check_github_api_budget", AsyncMock(return_value=True))
    monkeypatch.setattr(runner, "_maybe_resume_obsolete_ci_error", AsyncMock(return_value=None))
    monkeypatch.setattr(
        runner, "_remote_frontmatter_error_status_for_current_task",
        lambda: ("error", SuppressionReason.CRASH.value, "revision"),
    )
    for _ in range(4):
        asyncio.run(runner._run_cycle_body())
        assert runner.state.state == PipelineState.ERROR
        assert runner._consecutive_dirty_cycles == 0
        assert git("rev-parse", "HEAD").stdout.strip() == checkout_head
        assert (tmp_path / "example.txt").read_text() == unpublished
        assert (tmp_path / "notes.txt").read_text() == "untracked resolver notes\n"


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

    # Golden scenarios model historical completions, whose accounting has
    # already been written. Seed durable evidence rather than relying on Redis.
    import os

    from src.analytics.outcome_logger import compute_task_id_hash

    analytics_dir = Path(os.environ["PO_ANALYTICS_DIR"])
    analytics_dir.mkdir(parents=True, exist_ok=True)
    with (analytics_dir / "2026-04.jsonl").open("a") as outcomes:
        for task in before.get("tasks", []):
            outcomes.write(json.dumps({
                "task_id_hash": compute_task_id_hash(task["pr_id"], "octo__demo"),
            }) + "\n")

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


@pytest.mark.parametrize("pr_id", ["PR-401", None])
@pytest.mark.parametrize("snapshot", ["valid", "absent", "damaged"])
@pytest.mark.parametrize("phase", ["merging", "post_merge_cleanup"])
@pytest.mark.parametrize("status,missing_record", [
    (TaskStatus.DOING, False), (TaskStatus.DONE, False),
    (TaskStatus.TODO, True), (TaskStatus.DOING, True), (TaskStatus.DONE, True),
])
def test_restart_retains_merge_until_remote_bookkeeping_finishes(
    monkeypatch, phase, status, snapshot, missing_record, pr_id,
):
    from unittest.mock import AsyncMock

    from src.daemon.retry_commands import RetryDispatch

    monkeypatch.setattr(merge_module.MergeMixin, "_confirm_remote_merge", _CONFIRM_REMOTE_MERGE)
    original = h._make_runner()
    task = QueueTask(pr_id="PR-401", title="Merge", branch="pr-401",
                     task_file="tasks/PR-401.md", status=TaskStatus.DOING)
    original.state.current_task = task
    original.state.current_pr = PRInfo(number=629, pr_id=pr_id, title="Confirm remote merge",
                                       branch=task.branch, head_sha="a" * 40)
    original.state.state = PipelineState.MERGE
    original.state.merge_phase = phase
    original._start_current_run_record("claude", "opus")
    record = None if missing_record else original._current_run_record
    runner = h._make_runner()
    if snapshot != "absent":
        runner.redis.store[pipeline_state(runner.name)] = (
            original.state.model_dump_json() if snapshot == "valid" else "damaged"
        )
    queued = task.model_copy(update={"status": status})
    monkeypatch.setattr(gh_prs, "get_open_prs", lambda *a, **kw: [])
    def parse():
        runner._recovery_task_headers = {task.pr_id: types.SimpleNamespace(
            frontmatter_status=status.value.lower(),
        )}
        return [queued.model_copy(update={"status": TaskStatus.DONE})]

    monkeypatch.setattr(runner, "_parse_tasks_from_headers", parse)
    monkeypatch.setattr(
        runner._metrics_store, "list_task_runs", AsyncMock(return_value=[] if missing_record else [record]),
    )
    monkeypatch.setattr(gh_prs, "get_merged_prs_discovery", lambda *a: gh_prs.PRDiscovery(
        runner.owner_repo, "main", [original.state.current_pr], True,
    ))
    monkeypatch.setattr(runner, "_cleanup_expected_branch", lambda: None)
    monkeypatch.setattr(runner._metrics_store, "recent", AsyncMock(return_value=[] if missing_record else [record]))
    monkeypatch.setattr(runner._metrics_store, "save", AsyncMock())
    h._patch_subprocess(monkeypatch)
    assert asyncio.run(runner.recover_state())
    assert runner.state.state == PipelineState.MERGE
    assert runner.state.merge_phase == (phase if snapshot == "valid" else "post_merge_cleanup")
    assert runner.state.current_task.pr_id == queued.pr_id
    assert runner.state.current_pr.head_sha == "a" * 40
    assert runner.state.current_pr.pr_id == pr_id
    assert runner._current_run_record is None

    # The first cycle after recovery uses the retained identity, even though
    # GitHub's open-PR list is empty and headers may already say DONE. A dirty
    # checkout must not let preflight erase MERGE before bookkeeping replays.
    runner._recovered = True
    for method in ("ensure_repo_cloned", "_refresh_user_paused_from_redis"):
        monkeypatch.setattr(runner, method, AsyncMock())
    for method in ("_hold_for_coder_cleanup", "_consume_approval_command",
                   "_consume_publication_migration_command"):
        monkeypatch.setattr(runner, method, AsyncMock(return_value=False))
    monkeypatch.setattr(runner, "_consume_retry_command", AsyncMock(return_value=RetryDispatch.NONE))
    monkeypatch.setattr(runner, "_check_github_api_budget", AsyncMock(return_value=True))
    preflight = AsyncMock(wraps=runner.preflight)
    monkeypatch.setattr(runner, "preflight", preflight)
    monkeypatch.setattr(runner, "handle_idle", AsyncMock(side_effect=AssertionError("duplicate discovery")))
    def dirty_git(repo_path, *args, **kwargs):
        if args == ("status", "--porcelain"):
            return subprocess.CompletedProcess(args, 0, stdout="?? crash-leftover.txt\n")
        pytest.fail("branch access during reconciliation")

    monkeypatch.setattr(recovery_module.git_ops, "_git", dirty_git)
    monkeypatch.setattr(runner, "_compute_diff_stats", lambda base, head_ref="HEAD": {"files_touched_count": 0})
    analytics = []
    monkeypatch.setattr(merge_module, "log_merged_pr", analytics.append)
    write = AsyncMock(return_value=True)
    monkeypatch.setattr(runner, "_commit_task_status_change", write)

    reads = []

    def transport(args, **kwargs):
        reads.append(args)
        assert args == ["api", "repos/octo/demo/pulls/629", "--include"]
        return "HTTP/2.0 200 OK\r\n\r\n" + json.dumps({
            "number": 629, "base": {"ref": "main", "repo": {"full_name": "octo/demo"}},
            "head": {"sha": "a" * 40}, "state": "closed", "merged": True,
            "merge_commit_sha": "b" * 40,
            "merged_at": "2020-01-31T23:59:00Z",
        })

    monkeypatch.setattr(merge_module.gh_runner, "run_gh", transport)
    asyncio.run(runner._run_cycle_body())
    preflight.assert_not_awaited()
    assert runner.state.state == PipelineState.IDLE
    assert runner.state.current_task is None
    assert runner.state.current_queue[0].status == TaskStatus.DONE
    assert write.await_args.args[0].pr_id == queued.pr_id
    assert write.await_args.args[1] == "DONE"
    if missing_record:
        record = runner._metrics_store.save.await_args.args[0]
        assert record.task_id == task.pr_id and record.repo_name == runner.name
        assert record.profile_id == "recovery:unknown:container"
    assert record.exit_reason == "success_merged"
    assert analytics[0]["merged_at"] == "2020-01-31T23:59:00+00:00"
    assert reads == [["api", "repos/octo/demo/pulls/629", "--include"]]
    assert runner._metrics_store.save.await_count == 2


@pytest.mark.parametrize("mismatch", [
    "name", "url", "phase", "task", "pr", "number", "head", "branch", "pr_id",
    "queue_branch", "queue_task", "empty_branch",
])
def test_recovery_rejects_unbound_merge_snapshot(monkeypatch, mismatch):
    runner = h._make_runner()
    task = QueueTask(pr_id="PR-401", title="Merge", branch="pr-401", status=TaskStatus.DONE)
    pr = PRInfo(number=629, pr_id=task.pr_id, branch=task.branch, head_sha="a" * 40)
    saved = runner.state.model_copy(update={
        "state": PipelineState.MERGE, "merge_phase": "merging",
        "current_task": task, "current_pr": pr,
    })
    if mismatch in {"name", "url"}:
        setattr(saved, mismatch, "other")
    elif mismatch == "phase":
        saved.merge_phase = "pre_merge_sync"
    elif mismatch in {"task", "pr"}:
        setattr(saved, f"current_{mismatch}", None)
    elif mismatch == "number":
        pr.number = 0
    elif mismatch in {"head", "branch", "pr_id"}:
        setattr(pr, {"head": "head_sha"}.get(mismatch, mismatch), "other")
    if mismatch == "empty_branch":
        task.branch = pr.branch = ""
    queued = task.model_copy()
    if mismatch == "queue_branch":
        queued.branch = "other"
    if mismatch == "queue_task":
        queued.pr_id = "PR-999"
    runner.redis.store[pipeline_state(runner.name)] = saved.model_dump_json()
    monkeypatch.setattr(gh_prs, "get_open_prs", lambda *a, **kw: [])
    monkeypatch.setattr(runner, "_parse_tasks_from_headers", lambda: [queued])
    monkeypatch.setattr(runner, "_cleanup_expected_branch", lambda: None)
    assert asyncio.run(runner.recover_state())
    assert runner.state.state == PipelineState.IDLE
    assert runner.state.current_task is None


@pytest.mark.parametrize("case", [
    "pending_header", "pending_record", "historical", "finished", "wrong_record",
    "incomplete", "metrics_error", "unmatched", "ambiguous", "open", "not_done", "no_branch",
    "unprefixed", "wrong_pr_id", "unprefixed_ambiguous", "unprefixed_wrong_branch",
])
def test_header_merge_recovery_uses_complete_bound_github_evidence(
    monkeypatch, isolate_analytics_dir, case,
):
    from dataclasses import replace
    from unittest.mock import AsyncMock

    runner = h._make_runner()
    task = QueueTask(pr_id="PR-401", title="Merge", branch="pr-401",
                     task_file="tasks/PR-401.md", status=TaskStatus.DONE)
    runner.state.current_task = task
    runner._start_current_run_record("claude", "opus")
    record = runner._current_run_record
    runner.state.current_task = None
    runner._current_run_record = None
    runner._recovery_task_headers = {task.pr_id: types.SimpleNamespace(
        frontmatter_status="todo" if case == "pending_header" else "done",
    )}
    records = [record]
    if case in {"pending_header", "historical"}:
        records = []
    if case == "historical":
        from src.analytics.outcome_logger import compute_task_id_hash

        isolate_analytics_dir.mkdir(parents=True, exist_ok=True)
        (isolate_analytics_dir / "2026-04.jsonl").write_text(json.dumps({
            "task_id_hash": compute_task_id_hash(task.pr_id, runner.name),
        }) + "\n")
    if case == "finished":
        from src.analytics.outcome_logger import compute_task_id_hash

        record.exit_reason = "success_merged"
        isolate_analytics_dir.mkdir(parents=True, exist_ok=True)
        (isolate_analytics_dir / "2026-04.jsonl").write_text(json.dumps({
            "task_id_hash": compute_task_id_hash(task.pr_id, runner.name),
        }) + "\n")
    if case == "wrong_record":
        records = [replace(record, task_id="PR-999"), replace(record, repo_name="other")]
    if case == "not_done":
        task.status = TaskStatus.ERROR
    if case == "no_branch":
        task.branch = ""
    lookup = AsyncMock(return_value=records)
    if case == "metrics_error":
        lookup.side_effect = RuntimeError("metrics unavailable")
    monkeypatch.setattr(runner._metrics_store, "list_task_runs", lookup)
    from src.github import cache

    cache.clear_etag_cache()
    calls = []

    def transport(args, **kwargs):
        calls.append(args)
        assert args[1].startswith("repos/octo/demo/pulls?state=closed&base=main&per_page=100")
        if case == "incomplete":
            return "HTTP/2.0 200 OK\r\n\r\nbroken"
        entry = {
            "number": 629, "title": "PR-401: Merge", "merged_at": "2026-10-20T00:00:00Z",
            "base": {"ref": "main"}, "head": {"ref": "pr-401", "sha": "a" * 40},
            "merge_commit_sha": "b" * 40,
        }
        if case.startswith("unprefixed"):
            entry["title"] = "Confirm remote merge"
        if case == "wrong_pr_id":
            entry["title"] = "PR-999: Different task"
        if case in {"unmatched", "unprefixed_wrong_branch"}:
            entry["head"]["ref"] = "other"
        entries = ([entry, dict(entry, number=630)]
                   if case in {"ambiguous", "unprefixed_ambiguous"} else [entry])
        return "HTTP/2.0 200 OK\r\n\r\n" + json.dumps(entries)

    monkeypatch.setattr(gh_prs.gh_runner, "run_gh", transport)
    open_prs = [PRInfo(number=629, pr_id=task.pr_id, branch=task.branch)] if case == "open" else []
    if case in {"incomplete", "metrics_error"}:
        monkeypatch.setattr(gh_prs, "get_open_prs", lambda *a, **kw: open_prs)
        monkeypatch.setattr(runner, "_parse_tasks_from_headers", lambda: [task])
        monkeypatch.setattr(runner, "_cleanup_expected_branch", lambda: None)
        result = asyncio.run(runner.recover_state())
        assert runner.state.current_task is None
    else:
        result = asyncio.run(runner._recover_header_merge_attempt([task], open_prs))
    assert result is (case not in {"incomplete", "metrics_error", "ambiguous", "unprefixed_ambiguous"})
    saved = getattr(runner, "_persisted_merge_recovery", None)
    if case in {"pending_header", "pending_record", "unprefixed", "wrong_record"}:
        assert saved.current_task is task
        assert saved.current_pr.number == 629
        assert saved.current_pr.head_sha == "a" * 40
        assert saved.merge_phase == "post_merge_cleanup"
        assert runner._current_run_record is None
    else:
        assert saved is None
    if case in {"historical", "finished", "open", "not_done", "no_branch"}:
        assert not calls


@pytest.mark.parametrize("current_evidence", ["todo", "doing", "run"])
def test_header_merge_recovery_prioritizes_unfinished_attempt(
    monkeypatch, isolate_analytics_dir, current_evidence,
):
    """Missing historical analytics cannot consume the one startup recovery."""
    from unittest.mock import AsyncMock

    runner = h._make_runner()
    historical = QueueTask(pr_id="PR-001", title="Historical", branch="pr-001",
                           task_file="tasks/PR-001.md", status=TaskStatus.DONE)
    current = QueueTask(pr_id="PR-401", title="Interrupted", branch="pr-401",
                        task_file="tasks/PR-401.md", status=TaskStatus.DONE)
    runner._recovery_task_headers = {
        historical.pr_id: types.SimpleNamespace(frontmatter_status="done"),
        current.pr_id: types.SimpleNamespace(
            frontmatter_status="done" if current_evidence == "run" else current_evidence,
        ),
    }
    runner.state.current_task = current
    runner._start_current_run_record("claude", "opus")
    record = runner._current_run_record
    runner.state.current_task = None
    runner._current_run_record = None
    lookup = AsyncMock(side_effect=lambda repo, task_id:
                       [record] if task_id == current.pr_id and current_evidence == "run" else [])
    monkeypatch.setattr(runner._metrics_store, "list_task_runs", lookup)
    monkeypatch.setattr(gh_prs, "get_merged_prs_discovery", lambda *a: gh_prs.PRDiscovery(
        runner.owner_repo, "main",
        [PRInfo(number=1, pr_id=historical.pr_id, branch=historical.branch, head_sha="b" * 40),
         PRInfo(number=629, pr_id=current.pr_id, branch=current.branch, head_sha="a" * 40)],
        True,
    ))

    assert asyncio.run(runner._recover_header_merge_attempt([historical, current], []))
    saved = runner._persisted_merge_recovery
    assert saved.current_task is current
    assert saved.current_pr.number == 629
    assert saved.current_pr.head_sha == "a" * 40
    assert saved.merge_phase == "post_merge_cleanup"
    assert runner._current_run_record is None


@pytest.mark.parametrize("mode", ["missing", "incomplete", "status-incomplete", "passing"])
@pytest.mark.parametrize("changed_sha", [False, True])
def test_restart_rehydrates_required_ci_for_exact_head(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, ci_policy_transport: dict,
    mode: str, changed_sha: bool,
) -> None:
    """Persisted SUCCESS is never used in place of current policy evidence."""
    from src.github import checks
    from src.models import CIStatus

    h._patch_subprocess(monkeypatch)
    runner = h._make_runner(required_checks=["unit", "integration"])
    runner.repo_path = str(tmp_path)
    task = QueueTask(pr_id="PR-404", title="CI policy", status=TaskStatus.DOING,
                     branch="pr-404", task_file="tasks/PR-404.md")
    runner.state.state = PipelineState.WATCH
    runner.state.current_task = task
    runner.state.current_pr = PRInfo(number=404, branch="pr-404", pr_id="PR-404",
                                   head_sha="a" * 40, ci_status=CIStatus.SUCCESS)
    asyncio.run(runner.redis.set(pipeline_state(runner.name), runner.state.model_dump_json()))
    ci_policy_transport.update(mode=mode, sha=("b" if changed_sha else "a") * 40)
    restarted = h._make_runner(required_checks=["unit", "integration"])
    restarted.repo_path = str(tmp_path)
    restarted.redis = runner.redis
    monkeypatch.setattr(restarted, "_parse_tasks_from_headers", lambda: [task])
    assert asyncio.run(restarted.recover_state()) is True
    assert restarted.state.state == PipelineState.WATCH
    found = restarted.state.current_pr
    assert found.head_sha == ci_policy_transport["sha"]
    expected = CIStatus.SUCCESS if mode == "passing" else CIStatus.PENDING
    assert found.ci_status == expected
    # Persist and restart again on the same SHA, retaining incomplete cache metadata.
    asyncio.run(restarted.redis.set(pipeline_state(restarted.name), restarted.state.model_dump_json()))
    again = h._make_runner(required_checks=["unit", "integration"])
    again.repo_path = str(tmp_path)
    again.redis = restarted.redis
    monkeypatch.setattr(again, "_parse_tasks_from_headers", lambda: [task])
    assert asyncio.run(again.recover_state()) is True
    assert again.state.current_pr.ci_status == expected
    assert again.state.current_pr.head_sha == found.head_sha
    retrieval = checks._retrieve_ci_status_evidence(again.owner_repo, found.head_sha)
    assert retrieval.evidence.sources_complete == (mode in {"missing", "passing"})
    assert ci_policy_transport["reads"] == (2 if mode == "incomplete" else 1)


@pytest.mark.parametrize(
    "case",
    ["success", "success_live", "success_live_stale_startup",
     "success_default_dispatch", "stale_coder_checkout",
     "legacy_observations", "cause_delete_ack_lost_default_dispatch",
     "cause_delete_ack_lost_single_dispatch", "cause_delete_ack_lost_restart",
     "suppression_disappeared", "identity_changed",
     "provider_unavailable", "incomplete_evidence", "still_failed",
     "still_failed_default_dispatch", "wrong_run_cause",
     "unrelated_hold", "status_write_failed", "label_cleanup_failed", "cleanup_failed",
     "continuation_corrupt", "continuation_save_failed", "paused",
     "mismatched_hold", "different_live_hold"],
)
def test_structured_obsolete_ci_error_recovers_once_after_snapshot_reload(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, case: str,
) -> None:
    from unittest.mock import AsyncMock

    runner = h._make_runner()
    monkeypatch.setattr(runner, "repo_path", str(tmp_path))
    monkeypatch.setattr(recovery_module, "checkout_process_blocker", lambda path: False)
    task_path = tmp_path / "tasks" / "PR-019.md"
    task_path.parent.mkdir()
    task_path.write_text("---\nPR_ID: PR-019\nBranch: pr-019\nstatus: ERROR\n---\nTask\n")
    task_hash = task_spec_content_hash(task_path.read_text())
    task = QueueTask(
        pr_id="PR-019", title="CI", status=TaskStatus.ERROR,
        branch="pr-019", task_file="tasks/PR-019.md",
    )
    pr = PRInfo(number=19, pr_id="PR-019", branch="pr-019", head_sha="a" * 40,
                is_escalated=True)
    ci = {
        "repo": runner.owner_repo, "pr_number": 19, "head_sha": pr.head_sha,
        "contexts": [{"name": "unit", "source": "check", "check_id": 41, "attempt": 1,
                      "state": "failure", "workflow_run_id": 99, "job_id": 41}],
    }
    snapshot = InvocationSnapshot(
        plugin_id="claude", model_override=None, reasoning_effort_override=None,
        run_id="run-19", attempt_index=1, phase="fix", fix_iteration=1,
        fix_trigger={"ci": ci, "review": False},
    )
    claim = {
        "ci": ci, "task_id": task.pr_id, "branch": task.branch,
        "run_id": snapshot.run_id, "attempt_index": 1,
        "fix_iteration": 1, "task_spec_hash": task_hash,
    }
    if case == "legacy_observations":
        claim["ci"] = dict(ci, _observed_attempts={"99": 2})
    runner.state.current_task = task
    runner.state.current_pr = pr
    runner.state.active_invocation = snapshot
    runner.state.state = PipelineState.ERROR
    if case in {"success", "mismatched_hold", "different_live_hold"}:
        runner._begin_publication_attempt(started_at=datetime.now(timezone.utc))
        if case in {"mismatched_hold", "different_live_hold"}:
            runner.state.publication_attempt.exit_classification = "failure"
            if case == "mismatched_hold":
                runner.state.publication_attempt.head_sha = "b" * 40
    runner._persisted_fix_recovery = RepoState.model_validate_json(
        runner.state.model_dump_json(),
    )
    if case == "different_live_hold":
        runner.state.state = PipelineState.WATCH
        runner.state.publication_attempt.head_sha = "b" * 40
    cause = CancellationCause(category="ERROR", payload={
        "subsource": "coder_escalate", "reason_text": "OBSOLETE_CI_EVIDENCE",
        "obsolete_ci_trigger": claim, "label_applied": True,
    })
    if case == "success":
        monkeypatch.setattr("src.github.comments.post_comment", lambda *args: None)
        monkeypatch.setattr(
            recovery_module.gh_runner, "run_gh", lambda *args, **kwargs: "",
        )
        monkeypatch.setattr(runner, "_commit_task_status_change", AsyncMock(return_value=True))
        monkeypatch.setattr(runner, "_save_current_run_record", AsyncMock())
        runner.state.state = PipelineState.FIX
        runner.state.publication_attempt.exit_classification = "failure"
        asyncio.run(runner._escalate_fix_coder_initiated(
            pr, "OBSOLETE_CI_EVIDENCE", ci_trigger=claim,
        ))
        recorded = asyncio.run(get_cancellation_cause(
            runner.redis, runner.name, task.pr_id, refresh_ttl=False,
        ))
        assert recorded is not None
        assert recorded.payload["reason_text"] == "OBSOLETE_CI_EVIDENCE"
        assert recorded.payload["label_applied"] is True
        assert recorded.payload["obsolete_ci_trigger"] == claim
        assert runner.state.publication_attempt.pending
        assert runner.state.publication_attempt.exit_classification == "failure"
    else:
        asyncio.run(safe_record_cancellation_cause(
            runner.redis, runner.name, task.pr_id, cause,
        ))
    run = RunRecord(
        run_id=snapshot.run_id, task_id=task.pr_id, repo_name=runner.name,
        profile_id="claude:default:container", task_type="fix", complexity="low",
        started_at="2026-10-24T00:00:00Z", ended_at="2026-10-24T00:01:00Z",
        duration_ms=60000, fix_iterations=1, tokens_in=0, tokens_out=0,
        exit_reason="ESCALATE", operator_intervention=False,
        outcome="failed", cause="ESCALATE", cause_subsource="coder_escalate",
        run_phase="fix", attempt_index=1, task_spec_hash=task_hash,
        invocations=[snapshot],
    )
    stored_runs: dict[str, RunRecord] = {run.run_id: run}
    monkeypatch.setattr(
        runner._metrics_store, "get",
        AsyncMock(side_effect=lambda run_id: stored_runs.get(run_id)),
    )
    async def save_run(record: RunRecord) -> None:
        stored_runs[record.run_id] = record

    monkeypatch.setattr(runner._metrics_store, "save", save_run)
    monkeypatch.setattr(runner, "_git_rev_parse", lambda ref: "b" * 40)
    monkeypatch.setattr(gh_prs, "get_pr_metadata", lambda *args: {"head_sha": pr.head_sha})
    retrieval = gh_checks._make_ci_retrieval(
        runner.owner_repo, pr.head_sha, 0.0,
        [{"id": 42, "name": "unit", "head_sha": pr.head_sha,
          "app": {"id": 1}, "status": "completed",
          "conclusion": "failure" if case in {"still_failed", "still_failed_default_dispatch"}
          else "success"}],
        {"sha": pr.head_sha, "state": "pending", "statuses": []},
        gh_checks._CiSourceResult(case != "incomplete_evidence", False),
        gh_checks._CiSourceResult(True, True),
    )
    monkeypatch.setattr(gh_checks, "_retrieve_ci_status_evidence", lambda *a, **kw: retrieval)
    labels: list[list[str]] = []
    def edit_label(args: list[str], **kwargs: object) -> None:
        if case == "label_cleanup_failed":
            raise RuntimeError("label unavailable")
        labels.append(args)

    monkeypatch.setattr(recovery_module.gh_runner, "run_gh", edit_label)
    write = AsyncMock(return_value=case != "status_write_failed")
    monkeypatch.setattr(runner, "_commit_task_status_change", write)
    monkeypatch.setattr(runner, "_rehydrate_last_push_at", lambda pr: None)
    runner._recovery_task_headers = {
        task.pr_id: types.SimpleNamespace(
            frontmatter_status="error", blocked_reason="coder_escalate",
        ),
    }

    if case == "identity_changed":
        pr.head_sha = "b" * 40
    elif case == "provider_unavailable":
        monkeypatch.setattr(gh_prs, "get_pr_metadata", lambda *args: 1 / 0)
    elif case == "unrelated_hold":
        monkeypatch.setattr(
            runner, "_suppression_record_for_task",
            AsyncMock(return_value=types.SimpleNamespace(reason=SuppressionReason.GUARDRAIL)),
        )
    elif case == "wrong_run_cause":
        run.cause_subsource = "guardrail"
    elif case == "stale_coder_checkout":
        (tmp_path / ".git").mkdir()
        task_path.write_text(task_path.read_text().replace("status: ERROR", "status: TODO"))
        remote_text = "---\nPR_ID: PR-019\nBranch: pr-019\nstatus: ERROR\n---\nTask\n"
        monkeypatch.setattr(
            recovery_module.git_ops, "_git",
            lambda *args, **kwargs: types.SimpleNamespace(stdout=remote_text),
        )
    elif case == "suppression_disappeared":
        monkeypatch.setattr(runner, "_suppression_record_for_task", AsyncMock(return_value=None))
    elif case == "paused":
        runner.state.user_paused = True
    elif case == "cleanup_failed":
        original_clear = runner._clear_task_suppression
        clear_count = 0

        async def flaky_clear(task_id: str) -> None:
            nonlocal clear_count
            clear_count += 1
            if clear_count == 1:
                raise RuntimeError("Redis unavailable")
            await original_clear(task_id)

        monkeypatch.setattr(runner, "_clear_task_suppression", flaky_clear)
    elif case.startswith("cause_delete_ack_lost"):
        original_clear = runner._clear_task_suppression
        clear_count = 0

        async def lost_ack(task_id: str) -> None:
            nonlocal clear_count
            await original_clear(task_id)
            clear_count += 1
            if clear_count == 1:
                raise RuntimeError("clear acknowledgement lost")

        monkeypatch.setattr(runner, "_clear_task_suppression", lost_ack)
    elif case == "continuation_corrupt":
        import uuid

        bad = runner._new_fix_run_record(run, task_spec_hash="wrong")
        bad.run_id = str(uuid.uuid5(
            uuid.NAMESPACE_URL,
            f"{runner.name}:{task.pr_id}:{snapshot.run_id}:obsolete-ci-continuation",
        ))
        stored_runs[bad.run_id] = bad
    elif case == "continuation_save_failed":
        monkeypatch.setattr(
            runner._metrics_store, "save",
            AsyncMock(side_effect=RuntimeError("metrics unavailable")),
        )

    if case not in {"success", "success_live", "success_live_stale_startup",
                    "legacy_observations",
                    "success_default_dispatch",
                    "stale_coder_checkout",
                    "suppression_disappeared"}:
        assert not asyncio.run(runner._recover_obsolete_ci_error([task], [pr]))
        assert runner.state.state == (
            PipelineState.WATCH if case == "different_live_hold" else PipelineState.ERROR
        )
        assert write.await_count == (1 if case in {
            "status_write_failed", "cleanup_failed",
            "cause_delete_ack_lost_default_dispatch", "cause_delete_ack_lost_single_dispatch",
            "cause_delete_ack_lost_restart",
        } else 0)
        if case == "cleanup_failed":
            assert asyncio.run(runner._suppression_record_for_task(task.pr_id)) is not None
            resumed = task.model_copy(update={"status": TaskStatus.DOING})
            task_path.write_text(task_path.read_text().replace("status: ERROR", "status: DOING"))
            pr.is_escalated = False
            assert asyncio.run(runner._recover_obsolete_ci_error([resumed], [pr]))
            assert runner.state.state == PipelineState.WATCH
            assert write.await_count == 1
        if case.startswith("cause_delete_ack_lost"):
            assert asyncio.run(get_cancellation_cause(
                runner.redis, runner.name, task.pr_id, refresh_ttl=False,
            )) is None
            resumed = task.model_copy(update={"status": TaskStatus.DOING})
            task_path.write_text(task_path.read_text().replace("status: ERROR", "status: DOING"))
            pr.is_escalated = False
            monkeypatch.setattr(runner, "_parse_tasks_from_headers", lambda: [resumed])
            monkeypatch.setattr(gh_prs, "get_open_prs", lambda *a, **kw: [pr])
            if case.endswith("restart"):
                asyncio.run(runner._apply_recovery_decisions([resumed], [pr]))
            elif case.endswith("default_dispatch"):
                runner._recovered = True
                runner._scaffolded = True
                monkeypatch.setattr(runner, "ensure_repo_cloned", AsyncMock())
                monkeypatch.setattr(runner, "preflight", AsyncMock(return_value=True))
                monkeypatch.setattr(runner, "reload_repo_config_if_dirty", AsyncMock())
                monkeypatch.setattr(runner, "_maybe_auto_pause_for_error_rate", AsyncMock(return_value=False))
                asyncio.run(runner.run_cycle())
            else:
                asyncio.run(runner._handle_single_error_exit())
            assert runner.state.state == PipelineState.WATCH
            assert runner.state.current_task.status == TaskStatus.DOING
            assert write.await_count == 1
        if case == "still_failed":
            runner.repo_config.feature_flags.use_single_error_exit = True
            monkeypatch.setattr(runner, "_restore_current_run_record", AsyncMock())
            asyncio.run(runner._apply_recovery_decisions([task], [pr]))
            suppression = asyncio.run(runner._suppression_record_for_task(task.pr_id))
            assert suppression is not None
            assert suppression.reason == SuppressionReason.CODER_ESCALATE
        if case == "still_failed_default_dispatch":
            runner._recovered = True
            runner._scaffolded = True
            monkeypatch.setattr(runner, "ensure_repo_cloned", AsyncMock())
            monkeypatch.setattr(runner, "preflight", AsyncMock(return_value=True))
            monkeypatch.setattr(runner, "reload_repo_config_if_dirty", AsyncMock())
            monkeypatch.setattr(runner, "_maybe_auto_pause_for_error_rate", AsyncMock(return_value=False))
            monkeypatch.setattr(runner, "_parse_tasks_from_headers", lambda: [task])
            monkeypatch.setattr(gh_prs, "get_open_prs", lambda *a, **kw: [pr])
            diagnose = AsyncMock()
            monkeypatch.setattr(runner, "handle_error", diagnose)
            asyncio.run(runner.run_cycle())
            assert runner.state.state == PipelineState.ERROR
            diagnose.assert_not_awaited()
        return

    if case != "success_live_stale_startup":
        assert asyncio.run(runner._recover_unresolved_error_pr([task], [pr])) == (task, pr)
    if case in {"success_live", "success_live_stale_startup", "success_default_dispatch"}:
        if case == "success_live_stale_startup":
            runner._persisted_fix_recovery.state = PipelineState.WATCH
        monkeypatch.setattr(runner, "_parse_tasks_from_headers", lambda: [task])
        monkeypatch.setattr(gh_prs, "get_open_prs", lambda *a, **kw: [pr])
        if case == "success_default_dispatch":
            runner._recovered = True
            runner._scaffolded = True
            monkeypatch.setattr(runner, "ensure_repo_cloned", AsyncMock())
            monkeypatch.setattr(runner, "preflight", AsyncMock(return_value=True))
            monkeypatch.setattr(runner, "reload_repo_config_if_dirty", AsyncMock())
            monkeypatch.setattr(runner, "_maybe_auto_pause_for_error_rate", AsyncMock(return_value=False))
            watch = AsyncMock()
            monkeypatch.setattr(runner, "handle_watch", watch)
            asyncio.run(runner.run_cycle())
            watch.assert_awaited_once()
        else:
            asyncio.run(runner._handle_single_error_exit())
    else:
        asyncio.run(runner._apply_recovery_decisions([task], [pr]))
    assert runner.state.state == PipelineState.WATCH
    assert runner.state.current_task.status == TaskStatus.DOING
    assert runner.state.active_invocation is None
    if case == "success":
        assert runner.state.publication_attempt is not None
        assert not runner.state.publication_attempt.pending
        assert not asyncio.run(runner._hold_unresolved_publication())
    assert runner._current_run_record is not run
    assert runner._current_run_record.fix_iterations == run.fix_iterations
    assert runner._current_run_record.ended_at is None
    if case == "stale_coder_checkout":
        assert write.await_args.kwargs["expected_task_sha256"] == hashlib.sha256(
            remote_text.encode("utf-8")
        ).hexdigest()
    if case == "success":
        from src.coders.claude import claude_cli
        from src.daemon.handlers import fix as fix_module

        h._patch_subprocess(monkeypatch)
        monkeypatch.setattr(claude_cli, "fix_review_async", h._async_cli_result(0, "", ""))
        monkeypatch.setattr(fix_module, "_current_ci_failure", lambda *args: ci)
        monkeypatch.setattr(fix_module, "_fetch_failed_ci_logs", lambda *args: None)
        runner.state.current_pr.ci_status = CIStatus.FAILURE
        asyncio.run(runner.handle_fix())
        assert runner._current_run_record.fix_iterations == run.fix_iterations + 1
        assert not any(
            "Completed or unrelated run record" in event["event"]
            for event in runner.state.history
        )
    assert labels[0] == ["pr", "edit", "19", "--remove-label", "escalated"]
    assert run.fix_iterations == 1
    if case in {"success", "success_live_stale_startup"}:
        assert not asyncio.run(runner._recover_obsolete_ci_error([task], [pr]))
    else:
        resumed = task.model_copy(update={"status": TaskStatus.DOING})
        monkeypatch.setattr(
            gh_checks, "_retrieve_ci_status_evidence",
            lambda *args, **kwargs: 1 / 0,
        )
        assert asyncio.run(runner._recover_obsolete_ci_error([resumed], [pr]))
        assert len(stored_runs) == 2
    assert write.await_count == 1

    unrelated = CancellationCause(category="ERROR", payload={
        "subsource": "coder_escalate", "reason_text": "permission required",
        "obsolete_ci_trigger": claim,
    })
    asyncio.run(safe_record_cancellation_cause(
        runner.redis, runner.name, task.pr_id, unrelated,
    ))
    assert not asyncio.run(runner._recover_obsolete_ci_error([task], [pr]))
    assert write.await_count == 1

    legacy = replace(snapshot, fix_trigger=None)
    runner._persisted_fix_recovery.active_invocation = legacy
    assert not asyncio.run(runner._recover_obsolete_ci_error([task], [pr]))


def test_live_obsolete_ci_recovery_defers_provider_outage(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from unittest.mock import AsyncMock

    runner = h._make_runner()
    task = QueueTask(
        pr_id="PR-019", title="CI", status=TaskStatus.ERROR,
        branch="pr-019", task_file="tasks/PR-019.md",
    )
    runner.state.current_task = task
    runner.state.current_pr = PRInfo(number=19, branch=task.branch, head_sha="a" * 40)
    runner.state.state = PipelineState.ERROR
    runner.state.active_invocation = InvocationSnapshot(
        plugin_id="claude", model_override=None, reasoning_effort_override=None,
        run_id="run-19", attempt_index=1, phase="fix",
        fix_trigger={"ci": {"head_sha": "a" * 40}, "review": False},
    )
    asyncio.run(safe_record_cancellation_cause(
        runner.redis, runner.name, task.pr_id,
        CancellationCause(category="ERROR", payload={
            "subsource": "coder_escalate", "reason_text": "OBSOLETE_CI_EVIDENCE",
            "obsolete_ci_trigger": {"head_sha": "a" * 40},
        }),
    ))
    monkeypatch.setattr(runner, "_parse_tasks_from_headers", lambda: [task])
    monkeypatch.setattr(gh_prs, "get_open_prs", lambda *a, **kw: 1 / 0)
    monkeypatch.setattr(runner, "_error_suppression_reason", AsyncMock(return_value=(None, False)))
    monkeypatch.setattr(runner, "_remote_frontmatter_error_status_for_current_task", lambda: (None, None, None))
    asyncio.run(runner._handle_single_error_exit())
    assert runner.state.state == PipelineState.ERROR
    assert any("Obsolete CI observation deferred" in e["event"] for e in runner.state.history)


@pytest.mark.parametrize("mode", ["missing", "incomplete", "status-incomplete", "passing"])
def test_migration_refresh_uses_required_ci_policy(
    monkeypatch: pytest.MonkeyPatch, ci_policy_transport: dict, mode: str,
) -> None:
    from src.models import CIStatus

    ci_policy_transport["mode"] = mode
    runner = h._make_runner(required_checks=["unit", "integration"])
    head = ci_policy_transport["sha"]
    evidence = gh_prs.get_pr_head_publication(runner.owner_repo, 404, expected_head_sha=head)
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=404, branch="pr-404", head_sha=head,
                                   ci_status=CIStatus.SUCCESS)
    runner.state.publication_migration = recovery_module.PublicationMigration(
        repo=runner.owner_repo, pr_number=404, head_repo=runner.owner_repo,
        ref="refs/heads/pr-404", head_sha=head, published_at=evidence.published_at,
        provenance=evidence.provenance, reason="Operator confirmation required.",
    )
    asyncio.run(runner.redis.set(pipeline_state(runner.name), runner.state.model_dump_json()))
    binding = migration_binding(runner.state.publication_migration)
    asyncio.run(enqueue_migration(runner.redis, runner.name, binding))
    read_prs = gh_prs.get_open_prs
    observed = []

    def read(*args, **kwargs):
        prs = read_prs(*args, **kwargs)
        observed.extend(prs)
        return prs

    monkeypatch.setattr(gh_prs, "get_open_prs", read)
    assert asyncio.run(runner._consume_publication_migration_command()) is True
    command = MigrationCommand.model_validate_json(asyncio.run(
        runner.redis.get(migration_command_key(runner.name, binding)),
    ))
    assert command.status == "applied"
    assert len(observed) == 1
    assert observed[0].head_sha == head
    assert observed[0].ci_status == (CIStatus.SUCCESS if mode == "passing" else CIStatus.PENDING)


def test_recovery_retains_canonical_successful_comment_verdict(monkeypatch, tmp_path):
    """Restart uses the same bound receipt evidence as WATCH and final admission."""
    from src.github import cache, reviews
    from src.models import ReviewStatus

    recorded = json.loads((Path(__file__).parents[1] / "fixtures/review_receipt_651.json").read_text())
    head = recorded["head"]
    branch = "receipt-recovery-test"
    runner = h._make_runner()
    runner.repo_path = str(tmp_path)
    runner._parse_tasks_from_headers = lambda: [QueueTask(
        pr_id="PR-001", title="test", status=TaskStatus.DOING, branch=branch, task_file="tasks/PR-001.md",
    )]
    publication = datetime(2026, 10, 26, 22, tzinfo=timezone.utc)

    def transport(args, **kwargs):
        if args[:2] == ["pr", "list"]:
            return [{"number": 651, "title": "PR-001: test", "headRefName": branch,
                     "headRefOid": head, "author": {"login": recorded["author"]}}]
        raise AssertionError(args)

    def pages(path):
        for suffix, key in (("/issues/651/comments", "issue_comments"),
                            ("/pulls/651/comments", "review_comments"),
                            ("/pulls/651/reviews", "review_events")):
            if path.endswith(suffix):
                return recorded[key]
        return []

    monkeypatch.setattr(recovery_module.gh_runner, "run_gh", transport)
    monkeypatch.setattr(cache, "_gh_api_paginated", pages)
    monkeypatch.setattr(cache, "_etag_get", lambda path: {"sha": head})
    monkeypatch.setattr(gh_prs, "get_pr_head_publication", lambda repo, number, **kwargs:
        gh_prs.HeadPublicationEvidence(repo, number, repo, f"refs/heads/{branch}", head,
                                      publication, "github-activity:branch_creation"))
    monkeypatch.setattr(gh_checks, "_retrieve_ci_status_evidence", lambda *args, **kwargs: None)
    monkeypatch.setattr(gh_checks, "_classify_ci_retrieval", lambda *args, **kwargs: CIStatus.PENDING)
    reviews.clear_review_status_cache()
    assert asyncio.run(runner.recover_state()) is True
    assert runner.state.state == PipelineState.WATCH
    assert runner.state.current_pr.number == 651
    assert runner.state.current_pr.review_status == ReviewStatus.APPROVED
