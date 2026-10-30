"""Disposable restore investigation; no runtime restore or replay authority.

The JSON capsule below is a test-only serialization probe, not a Redis backup.
Real Redis RDB/AOF durability and cross-store consistency remain unproved.
"""

# ruff: noqa: F811 -- imported pytest fixture is injected by name

import json
import shutil
from dataclasses import replace
from pathlib import Path

import pytest
from src.analytics.outcome_logger import has_merged_outcome, log_merged_pr
from src.analytics.schema import OUTCOME_FIELDS
from src.approval_commands import (
    ApprovalChanged,
    build_approval,
    enqueue_approval,
    list_approvals,
    matches_attempt,
)
from src.cancellation.storage import CancellationCause, cause_key
from src.config import load_config
from src.daemon.backups import create_repo_bundle
from src.keyspace import control_stop, pipeline_state, recovery_backup_branch, retry_command_pending
from src.metrics import MetricsStore
from src.models import PipelineState, RepoState
from src.retry_commands import enqueue_retry_command, load_retry_command

from tests.runner import _helpers as h
from tests.runner.test_retry_commands import _command
from tests.test_approval_commands import approval, git  # noqa: F401
from tests.test_backups import _init_git_repo
from tests.test_work_preservation_contract import files


@pytest.fixture(autouse=True)
def hide_live_checkout_candidates(monkeypatch):
    """Keep the reused runner constructor away from its default /data paths."""
    exists = Path.exists

    def isolated_exists(path):
        if path in (Path("/data/repos/octo__demo"), Path("/data/repos/demo")):
            return False
        return exists(path)

    monkeypatch.setattr(Path, "exists", isolated_exists)


async def test_bundle_restores_git_but_omits_operational_data(tmp_path, monkeypatch):
    source = tmp_path / "source"
    _init_git_repo(source)
    git(source, "config", "user.name", "Restore fixture")
    git(source, "config", "user.email", "restore@example.invalid")
    (source / ".gitignore").write_text("config.production.yml\nignored.bin\n")
    (source / "config.yml").write_text("daemon:\n  poll_interval_sec: 60\n")
    (source / "tasks").mkdir()
    task = source / "tasks/PR-42.md"
    task.write_text(
        "---\nstatus: ERROR\n---\n\n# PR-42: Synthetic rejected task\n"
        "Branch: fix/pr-42\n- Type: bugfix\n- Complexity: low\n"
    )
    git(source, "add", ".")
    git(source, "commit", "-m", "synthetic config and task")
    base = git(source, "rev-parse", "HEAD")
    git(source, "update-ref", "refs/remotes/origin/main", base)
    git(source, "checkout", "-b", "fix/pr-42")
    (source / "local-work.bin").write_bytes(b"local committed work\x00\xff")
    git(source, "add", "local-work.bin")
    git(source, "commit", "-m", "unpushed work")
    head = git(source, "rev-parse", "HEAD")
    git(source, "tag", "restore-point")
    refs = git(source, "show-ref")
    committed_task = task.read_bytes()
    (source / "config.production.yml").write_text("daemon:\n  poll_interval_sec: 17\n")
    assert load_config(str(source / "config.yml")).daemon.poll_interval_sec == 17
    (source / "README.md").write_bytes(b"staged only\x00")
    git(source, "add", "README.md")
    (source / "README.md").write_bytes(b"unstaged work\xff")
    (source / "untracked.bin").write_bytes(b"untracked\x00")
    (source / "ignored.bin").write_bytes(b"ignored\xff")

    # These synthetic side stores are outside the managed checkout, as in /data.
    runtime = tmp_path / "runtime"
    runtime.mkdir()
    (runtime / "auth-references.json").write_text(json.dumps({
        "claude": "/data/auth/claude", "gh": "/data/auth/gh",
        "codex": "/data/auth/.codex", "secrets": "/data/secrets",
    }))  # References only: no credential files are created or read.
    monkeypatch.setenv("PO_ANALYTICS_DIR", str(runtime / "analytics"))
    record = dict.fromkeys(OUTCOME_FIELDS)
    record.update(pr_id="PR-41", repo_slug="fixture__restore", outcome="merged",
                  merged_at="2026-10-30T00:00:00Z")
    log_merged_pr(record)
    assert has_merged_outcome("PR-41", "fixture__restore")
    outcome_bytes = (runtime / "analytics/2026-10.jsonl").read_bytes()
    before = files(source)
    index = (source / ".git/index").read_bytes()
    bundle = await create_repo_bundle(
        repo_path=str(source), repo_name="fixture__restore",
        backup_dir=str(tmp_path / "backup"),
    )
    assert bundle is not None
    assert files(source) == before
    assert (source / ".git/index").read_bytes() == index
    shutil.rmtree(source)  # Only this fixture's source; force independence.

    restored = tmp_path / "restored"
    git(tmp_path, "init", "-b", "unborn", str(restored))
    git(restored, "fetch", str(bundle), "+refs/*:refs/*")
    git(restored, "checkout", "fix/pr-42")
    assert git(restored, "show-ref") == refs
    assert git(restored, "rev-parse", "HEAD") == head
    assert git(restored, "rev-parse", "refs/remotes/origin/main") == base
    assert (restored / "local-work.bin").read_bytes() == b"local committed work\x00\xff"
    assert (restored / "README.md").read_bytes() == b"hello\n"
    assert (restored / "tasks/PR-42.md").read_bytes() == committed_task
    for missing in ("config.production.yml", "untracked.bin", "ignored.bin",
                    "auth-references.json", "analytics", "dump.rdb"):
        assert not (restored / missing).exists()
    assert load_config(str(restored / "config.yml")).daemon.poll_interval_sec == 60
    assert git(restored, "remote") == ""  # No live provider or coder is started.
    monkeypatch.setenv("PO_ANALYTICS_DIR", str(tmp_path / "restored-analytics"))
    assert not has_merged_outcome("PR-41", "fixture__restore")
    # An explicitly selected side file can retain exact bytes; bundles omit it.
    shutil.copytree(runtime / "analytics", tmp_path / "restored-analytics")
    assert (tmp_path / "restored-analytics/2026-10.jsonl").read_bytes() == outcome_bytes
    assert has_merged_outcome("PR-41", "fixture__restore")


async def test_crash_backup_restores_dirty_git_state_but_not_ignored_files(approval, tmp_path):
    runner, _, source, remote = approval
    work = source / "implementation.py"
    work.write_text("staged recovery work\n")
    git(source, "add", "implementation.py")
    work.write_text("unstaged recovery work\n")
    (source / "untracked.txt").write_text("unfinished input\n")
    (source / ".git/info/exclude").write_text("ignored.txt\n")
    (source / "ignored.txt").write_text("ignored work outside capture\n")
    head = git(source, "rev-parse", "HEAD")
    staged = git(source, "diff", "--cached", "--binary")
    unstaged = git(source, "diff", "--binary")
    # Existing recovery producer really stashes and pushes, only to this bare
    # fixture remote. No GitHub service or live checkout is involved.
    assert runner._preserve_crashed_run_commits("fix/pr-42")
    task_id, branch = runner._pending_backup_branch_write
    assert task_id == "PR-42"
    assert branch.startswith("crash-backup/PR-42/")
    stash_sha = git(source, "rev-parse", "stash@{0}")
    assert git(remote, "rev-parse", f"refs/heads/{branch}") == stash_sha
    await runner._persist_pending_backup_branch_write()
    pointer = recovery_backup_branch(runner.name, task_id)
    assert await runner.redis.get(pointer) == branch
    assert runner.redis.ttls[pointer] == 30 * 24 * 3600
    assert work.read_text() == "implementation\n"  # Stashing mutates source.
    assert not (source / "untracked.txt").exists()
    assert (source / "ignored.txt").read_text() == "ignored work outside capture\n"
    shutil.rmtree(source)
    restored = tmp_path / "restored-crash"
    git(tmp_path, "init", "-b", "unborn", str(restored))
    git(restored, "fetch", str(remote), f"refs/heads/{branch}:refs/heads/saved-state")
    git(restored, "checkout", "-b", "inspection", head)
    git(restored, "stash", "apply", "--index", "saved-state")
    assert (restored / "implementation.py").read_text() == "unstaged recovery work\n"
    assert (restored / "untracked.txt").read_text() == "unfinished input\n"
    assert git(restored, "diff", "--cached", "--binary") == staged
    assert git(restored, "diff", "--binary") == unstaged
    assert not (restored / "ignored.txt").exists()


@pytest.mark.parametrize("rejected", [False, True], ids=["active", "rejected"])
async def test_state_capsule_retains_intent_without_replay(approval, tmp_path, rejected):
    runner, _, repo, _ = approval
    runner.state.user_paused = True
    runner.state.state = PipelineState.ERROR if rejected else PipelineState.CODING
    runner._start_current_run_record("codex", "synthetic-model")
    invocation = runner._capture_invocation_snapshot("codex", {}, phase="coding")
    await runner._checkpoint_current_run_record()
    raw_guardrail = await runner.redis.get(cause_key(runner.name, "PR-42"))
    decision = build_approval(runner.name, runner.state, raw_guardrail, repo)
    await runner.redis.set(pipeline_state(runner.name), runner.state.model_dump_json())
    await enqueue_approval(runner.redis, decision)
    text = (repo / "tasks/PR-42.md").read_text()
    retry, created = await enqueue_retry_command(runner.redis, _command(text))
    assert created
    if rejected:
        cause = CancellationCause(category="ERROR", payload={
            "subsource": "operator_reject", "reason_text": "synthetic rejection",
            "original_rule": "test", "original_excerpt": "test",
        })
        await runner.redis.set(cause_key(runner.name, "PR-42"), cause.to_redis())
    await runner.redis.set(control_stop(runner.name), "1")
    await runner.publish_state()  # Existing production snapshot writer.

    # No production Redis export exists. This diagnostic file preserves the
    # double's values/indexes, not wall-clock expiry or an atomic Redis image.
    capsule = tmp_path / "backup/state-diagnostic.json"
    capsule.parent.mkdir()
    capsule.write_text(json.dumps({
        "values": runner.redis.store, "indexes": runner.redis.zsets,
        "lists": runner.redis.lists,
        "sets": {key: sorted(value) for key, value in runner.redis.sets.items()},
        "ttl_seconds": runner.redis.ttls,
    }, sort_keys=True))
    restored_dir = tmp_path / "restore-state"
    restored_dir.mkdir()
    shutil.copy2(capsule, restored_dir / capsule.name)
    assert (restored_dir / capsule.name).read_bytes() == capsule.read_bytes()
    saved = json.loads((restored_dir / capsule.name).read_text())
    fresh = h._make_runner()
    fresh.repo_path = str(repo)
    fresh.redis.store = saved["values"]
    fresh.redis.zsets = saved["indexes"]
    fresh.redis.lists = saved["lists"]
    fresh.redis.sets = {key: set(value) for key, value in saved["sets"].items()}
    fresh.redis.ttls = saved["ttl_seconds"]
    assert fresh.redis.store == runner.redis.store
    assert fresh.redis.zsets == runner.redis.zsets
    assert fresh.redis.lists == runner.redis.lists
    assert fresh.redis.sets == runner.redis.sets
    assert fresh.redis.ttls == runner.redis.ttls  # Declared durations, not expiry proof.
    fresh.state = RepoState.model_validate_json(await fresh.redis.get(pipeline_state(fresh.name)))
    assert fresh.state == RepoState.model_validate_json(
        await runner.redis.get(pipeline_state(runner.name)),
    )
    fresh._metrics_store = MetricsStore(fresh.redis)
    await fresh._restore_current_run_record()
    assert fresh.state.active_invocation == invocation
    assert fresh._current_run_record == runner._current_run_record
    assert await fresh._pop_stop_request()
    fresh.state.user_paused = False
    await fresh._refresh_user_paused_from_redis()  # Existing pause consumer.
    assert fresh.state.user_paused
    assert await list_approvals(fresh.redis, runner.name) == [decision]
    assert await load_retry_command(fresh.redis, retry.repo_slug, retry.command_id) == retry
    assert retry.command_id in fresh.redis.zsets[retry_command_pending(retry.repo_slug)]
    raw = await fresh.redis.get(cause_key(fresh.name, "PR-42"))
    assert raw == await runner.redis.get(cause_key(runner.name, "PR-42"))
    if rejected:
        with pytest.raises(ApprovalChanged, match="no pending guardrail decision"):
            build_approval(fresh.name, fresh.state, raw, repo)
    fresh.state.active_invocation = replace(invocation, attempt_index=invocation.attempt_index + 1)
    assert not matches_attempt(decision, fresh.state)
    # Hydration is not dispatch, process ownership, or new-attempt authority.
    assert fresh._current_coder_process is None
    assert fresh._current_coder_supervised_process is None
    assert fresh.redis.writes == []

    # Losing Redis loses the pause/Stop/decision, even while task bytes survive.
    empty = h._make_runner()
    await empty._refresh_user_paused_from_redis()
    assert not empty.state.user_paused  # Characterized gap; do not start it.
    assert not await empty._pop_stop_request()
    assert await list_approvals(empty.redis, runner.name) == []
    assert (repo / "tasks/PR-42.md").read_text() == text
