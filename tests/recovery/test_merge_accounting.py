"""Merge accounting admission with real Git generations and isolated DONE writes."""

from __future__ import annotations

import asyncio
import json
from dataclasses import asdict, replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from src.cancellation import task_spec_content_hash
from src.daemon import git_ops
from src.daemon.handlers import merge as merge_module
from src.github import cache, prs
from src.keyspace import pipeline_state
from src.metrics import MetricsStore
from src.models import PipelineState, PRInfo, QueueTask, TaskStatus
from src.task_status import MergedState

from tests.legacy_recovery_git import git
from tests.runner import _helpers as h


@pytest.fixture
def incident(tmp_path, monkeypatch):
    remote, repo = (tmp_path / name for name in ("remote.git", "repo"))
    git(tmp_path, "init", "--bare", str(remote))
    git(tmp_path, "init", "-b", "main", str(repo))
    git(repo, "config", "user.name", "Merge Test")
    git(repo, "config", "user.email", "merge@example.test")
    task_file = "tasks/PR-395.md"
    path = repo / task_file
    path.parent.mkdir()
    generation_a = (
        "---\nstatus: TODO\n---\n\n# PR-395: Fetch\n\n"
        "Branch: pr-395-ci-complete-fetch\n- Type: bugfix\n- Complexity: low\n"
        "- Depends on: none\n\nGeneration A\n"
    )
    path.write_text(generation_a)
    git(repo, "add", ".")
    git(repo, "commit", "-m", "old generation")
    old_head = git(repo, "rev-parse", "HEAD")
    generation_b = generation_a.replace("Generation A", "Generation B")
    path.write_text(generation_b)
    (repo / "tasks/PR-404.md").write_text(
        generation_b.replace("PR-395", "PR-404").replace("pr-395-ci-complete-fetch", "pr-404"),
    )
    git(repo, "commit", "-am", "new generation")
    git(repo, "add", ".")
    git(repo, "commit", "-m", "pending task")
    git(repo, "remote", "add", "origin", str(remote))
    git(repo, "checkout", "-b", "pr-395-ci-complete-fetch")
    (repo / "implementation.py").write_text("first = True\n")
    git(repo, "add", ".")
    git(repo, "commit", "-m", "intermediate implementation")
    intermediate = git(repo, "rev-parse", "HEAD")
    (repo / "implementation.py").write_text("fixed = True\n")
    git(repo, "commit", "-am", "subsequent fix")
    head = git(repo, "rev-parse", "HEAD")
    git(repo, "checkout", "main")
    git(repo, "merge", "--squash", "pr-395-ci-complete-fetch")
    git(repo, "commit", "-m", "merged PR")
    merged = git(repo, "rev-parse", "HEAD")
    git(repo, "push", "origin", "main")
    git(repo, "push", "origin", f"{head}:refs/pull/547/head")
    # Unpublished checkout contents must survive all bookkeeping.
    git(repo, "checkout", "pr-395-ci-complete-fetch")
    (repo / "retained.txt").write_text("unpublished evidence\n")
    original_checkout = git(repo, "status", "--porcelain")

    task = QueueTask(pr_id="PR-395", title="Fetch", branch="pr-395-ci-complete-fetch",
                     status=TaskStatus.TODO, task_file=task_file)
    pr = PRInfo(number=547, pr_id=task.pr_id, branch=task.branch, head_sha=head)
    pending = QueueTask(pr_id="PR-404", title="Pending", branch="pr-404",
                        task_file="tasks/PR-404.md", status=TaskStatus.DOING)
    pending_pr = PRInfo(number=636, pr_id=pending.pr_id, branch=pending.branch, head_sha=head)
    runner = h._make_runner()
    runner.repo_path = str(repo)
    runner.repo_config.url = str(remote)
    runner.state.url = str(remote)
    runner.state.current_task = task
    runner.state.current_pr = pr
    runner.state.state = PipelineState.MERGE
    runner.state.merge_phase = "post_merge_cleanup"
    runner._start_current_run_record("claude", "opus")
    runner._capture_invocation_snapshot("claude", {"model": "opus"}, phase="fix")
    old = runner._current_run_record
    old.started_at = old.ended_at = "2026-10-02T00:00:00+00:00"
    old.exit_reason, old.outcome = "escalated", "failed"
    old.cause, old.cause_subsource = "ESCALATE", "coder_escalate"
    old.head_sha, old.task_spec_hash = old_head, task_spec_content_hash(generation_a)
    old.tokens_in, old.tokens_out = 123, 456
    asyncio.run(runner._metrics_store.save(old))
    old_bytes = runner.redis.store[MetricsStore._record_key(old.run_id)]
    cache.clear_etag_cache()
    payload = {
        "number": 547, "base": {"ref": "main", "repo": {"full_name": runner.owner_repo}},
        "head": {"sha": head}, "state": "closed", "merged": True,
        "merge_commit_sha": merged, "merged_at": "2026-10-20T00:00:00Z",
    }

    def transport(args, **kwargs):
        assert args == ["api", f"repos/{runner.owner_repo}/pulls/547", "--include"]
        return "HTTP/2.0 200 OK\r\n\r\n" + json.dumps(payload)

    monkeypatch.setattr(prs.gh_runner, "run_gh", transport)
    monkeypatch.setattr(prs, "get_open_prs", lambda *a, **kw: [pending_pr])
    monkeypatch.setattr(prs, "get_merged_prs_discovery", lambda *a: prs.PRDiscovery(
        runner.owner_repo, "main", [pr], True,
    ))
    monkeypatch.setattr(prs, "merge_pr", lambda *a: pytest.fail("duplicate merge"))

    def fresh():
        result = h._make_runner()
        result.repo_path, result.repo_config.url, result.state.url = str(repo), str(remote), str(remote)
        result.redis = runner.redis
        result._metrics_store = MetricsStore(result.redis)
        return result

    def recover(result):
        git(repo, "fetch", "origin", "main")
        # Use the real remote-header parser, with provider merge discovery
        # isolated from its separate derived-status cache.
        monkeypatch.setattr("src.daemon.recovery._resolve_merged_state",
                            lambda *a, **kw: MergedState(set(), set(), True))
        monkeypatch.setattr(result, "_cleanup_expected_branch", lambda: None)
        assert asyncio.run(result.recover_state())

    def remote_task():
        return git(remote, "show", f"main:{task_file}")

    def preserved():
        assert runner.redis.store[MetricsStore._record_key(old.run_id)] == old_bytes
        assert git(repo, "status", "--porcelain") == original_checkout
        assert git(repo, "branch", "--show-current") == task.branch

    return SimpleNamespace(**locals())


@pytest.mark.parametrize("snapshot", ["valid", "absent", "damaged"])
@pytest.mark.parametrize("compatible", [False, True])
def test_incident_releases_merged_generation_without_rewriting_failed_attempt(
    incident, monkeypatch, snapshot, compatible, isolate_analytics_dir,
):
    c = incident
    if compatible:
        good = replace(c.old, run_id="compatible", ended_at=None, exit_reason="",
                       task_spec_hash=task_spec_content_hash(c.generation_b), head_sha=c.intermediate,
                       invocations=[], cause="CRASH", cause_subsource=None, started_at="2026-10-01T00:00:00Z")
        asyncio.run(c.runner._metrics_store.save(good))
        # A newer incompatible run must not hide the compatible checkpoint.
        asyncio.run(c.runner._metrics_store.save(replace(c.old, run_id="newer", invocations=[])))
    if snapshot != "absent":
        c.runner.redis.store[pipeline_state(c.runner.name)] = (
            c.runner.state.model_dump_json() if snapshot == "valid" else "damaged"
        )
    runner = c.fresh()
    c.recover(runner)
    assert runner.state.state == PipelineState.MERGE
    asyncio.run(runner.handle_merge())
    assert "status: DONE" in c.remote_task()
    records = asyncio.run(runner._metrics_store.list_task_runs(runner.name, c.task.pr_id))
    merged = [r for r in records if r.outcome == "merged"]
    assert len(merged) == 1
    assert merged[0].run_id == "compatible" if compatible else merged[0].run_id != c.old.run_id
    assert merged[0].task_spec_hash == task_spec_content_hash(c.generation_b)
    assert merged[0].head_sha == c.head
    assert runner.state.state == PipelineState.IDLE
    asyncio.run(runner.publish_state())
    c.preserved()
    # Header recovery sees durable completion; it can now follow the existing
    # open PR to WATCH, with no coding or PR creation.
    restarted = c.fresh()
    monkeypatch.setattr(restarted, "handle_coding", AsyncMock(side_effect=AssertionError("duplicate CODING")))
    c.recover(restarted)
    assert restarted.state.state == PipelineState.WATCH
    assert restarted.state.current_pr.number == 636
    assert len(list(isolate_analytics_dir.glob("*.jsonl"))) == 1
    c.preserved()


@pytest.mark.parametrize("conflict", ["generation", "race", "branch", "head_task", "merge_task"])
def test_conflicting_generation_parks_without_done_or_failed_run_rewrite(incident, monkeypatch, conflict):
    c = incident
    real_git = git_ops._git
    changed = False

    def replace_remote():
        git(c.repo, "checkout", "main")
        c.path.write_text(c.generation_b + "\nGeneration C\n")
        git(c.repo, "commit", "-am", "replacement task")
        git(c.repo, "push", "origin", "main")
        git(c.repo, "checkout", c.task.branch)

    def racing(path, *args, **kwargs):
        nonlocal changed
        if conflict == "race" and args[:2] == ("push", "origin") and not changed:
            changed = True
            replace_remote()
        if conflict == "head_task" and args == ("show", f"{c.head}:{c.task_file}"):
            return SimpleNamespace(stdout=c.generation_a)
        if conflict == "merge_task" and args == ("show", f"{c.merged}:{c.task_file}"):
            return SimpleNamespace(stdout=c.generation_a)
        return real_git(path, *args, **kwargs)

    monkeypatch.setattr(git_ops, "_git", racing)
    if conflict == "generation":
        replace_remote()
    if conflict == "branch":
        c.runner.state.current_pr.branch = "other"
    asyncio.run(c.runner.handle_merge())
    assert c.runner.state.state == PipelineState.ERROR
    assert c.runner.state.error_message.startswith("Merge accounting conflict:")
    assert "status: DONE" not in c.remote_task()
    before = c.remote_task()
    for _ in range(2):
        asyncio.run(c.runner.handle_error())
    assert c.remote_task() == before
    records = asyncio.run(c.runner._metrics_store.list_task_runs(c.runner.name, c.task.pr_id))
    assert all(r.outcome != "merged" for r in records)
    if conflict != "branch":
        restarted = c.fresh()
        c.recover(restarted)
        assert restarted.state.state == PipelineState.ERROR
        asyncio.run(restarted.handle_error())
        assert c.remote_task() == before
    c.preserved()


@pytest.mark.parametrize("failure", ["github", "git", "redis_read", "checkpoint", "metrics", "outcome"])
def test_transient_failures_keep_restart_safe_accounting(incident, monkeypatch, failure, isolate_analytics_dir):
    c = incident
    runner = c.runner
    real_git, read, save = git_ops._git, runner._metrics_store.list_task_runs, runner._metrics_store.save
    log = merge_module.log_merged_pr
    failed = False

    def fail_once():
        nonlocal failed
        if not failed:
            failed = True
            raise RuntimeError("temporary outage")

    async def failing_read(*args):
        fail_once()
        return await read(*args)

    async def failing_save(record):
        if (failure == "metrics") == (record.exit_reason == "success_merged"):
            fail_once()
        await save(record)

    def failing_git(path, *args, **kwargs):
        if args[:3] == ("fetch", "--no-tags", "origin"):
            fail_once()
        return real_git(path, *args, **kwargs)

    def failing_log(record):
        fail_once()
        log(record)

    if failure == "github":
        c.payload.update(merged=None)
    elif failure == "git":
        monkeypatch.setattr(git_ops, "_git", failing_git)
    elif failure == "redis_read":
        monkeypatch.setattr(runner._metrics_store, "list_task_runs", failing_read)
    elif failure in {"checkpoint", "metrics"}:
        monkeypatch.setattr(runner._metrics_store, "save", failing_save)
    elif failure == "outcome":
        monkeypatch.setattr(merge_module, "log_merged_pr", failing_log)
    asyncio.run(runner.handle_merge())
    if failure != "outcome":
        assert runner.state.state == PipelineState.MERGE
        assert runner.state.current_task is c.task
    if failure in {"github", "git", "redis_read", "checkpoint"}:
        assert "status: DONE" not in c.remote_task()
    c.payload.update(merged=True)
    cache.clear_etag_cache()
    monkeypatch.setattr(git_ops, "_git", real_git)
    monkeypatch.setattr(merge_module, "log_merged_pr", log)
    restarted = c.fresh()
    c.recover(restarted)
    asyncio.run(restarted.handle_merge())
    assert "status: DONE" in c.remote_task()
    records = asyncio.run(restarted._metrics_store.list_task_runs(restarted.name, c.task.pr_id))
    assert len([r for r in records if r.outcome == "merged"]) == 1
    rows = [json.loads(line) for path in isolate_analytics_dir.glob("*.jsonl")
            for line in path.read_text().splitlines()]
    assert len(rows) == 1
    c.preserved()


def test_lost_status_push_response_still_confirms_real_done(incident, monkeypatch):
    c = incident
    real_git = git_ops._git

    def lost_response(path, *args, **kwargs):
        result = real_git(path, *args, **kwargs)
        if args[:2] == ("push", "origin"):
            return SimpleNamespace(returncode=1, stderr="response lost")
        return result

    monkeypatch.setattr(git_ops, "_git", lost_response)
    asyncio.run(c.runner.handle_merge())
    assert "status: DONE" in c.remote_task()
    assert c.runner.state.state == PipelineState.IDLE
    assert asdict(asyncio.run(c.runner._metrics_store.get(c.old.run_id))) == asdict(c.old)
    c.preserved()


@pytest.mark.parametrize("boundary", ["checkpoint", "status", "metrics", "outcome"])
@pytest.mark.parametrize("snapshot", ["retained", "absent"])
def test_crashes_replay_one_accounting_identity(incident, monkeypatch, boundary, snapshot, isolate_analytics_dir):
    c = incident
    save, write, log = c.runner._metrics_store.save, c.runner._commit_task_status_change, merge_module.log_merged_pr
    checkpoint_id = None

    async def save_then_crash(record):
        nonlocal checkpoint_id
        await save(record)
        checkpoint_id = record.run_id
        if boundary == ("metrics" if record.exit_reason == "success_merged" else "checkpoint"):
            raise SystemExit("crash after durable save")

    async def write_then_crash(*args, **kwargs):
        assert await write(*args, **kwargs)
        raise SystemExit("crash after real DONE push")

    def log_then_crash(record):
        log(record)
        raise SystemExit("crash after durable outcome")

    monkeypatch.setattr(c.runner._metrics_store, "save", save_then_crash)
    if boundary == "status":
        monkeypatch.setattr(c.runner, "_commit_task_status_change", write_then_crash)
    if boundary == "outcome":
        monkeypatch.setattr(merge_module, "log_merged_pr", log_then_crash)
    with pytest.raises(SystemExit, match="crash after"):
        asyncio.run(c.runner.handle_merge())
    assert checkpoint_id is not None
    final_before = asyncio.run(c.runner._metrics_store.get(checkpoint_id))
    if snapshot == "absent":
        c.runner.redis.store.pop(pipeline_state(c.runner.name), None)
    monkeypatch.setattr(merge_module, "log_merged_pr", log)
    restarted = c.fresh()
    c.recover(restarted)
    if restarted.state.state == PipelineState.MERGE:
        asyncio.run(restarted.handle_merge())
        asyncio.run(restarted.publish_state())
    assert "status: DONE" in c.remote_task()
    records = asyncio.run(restarted._metrics_store.list_task_runs(restarted.name, c.task.pr_id))
    merged = [r for r in records if r.outcome == "merged"]
    assert len(merged) == 1 and merged[0].run_id == checkpoint_id
    if boundary in {"metrics", "outcome"}:
        assert asdict(merged[0]) == asdict(final_before)
    again = c.fresh()
    c.recover(again)
    assert again.state.state == PipelineState.WATCH
    assert again.state.current_pr.number == 636
    rows = [line for path in isolate_analytics_dir.glob("*.jsonl") for line in path.read_text().splitlines()]
    assert len(rows) == 1
    c.preserved()


@pytest.mark.parametrize("candidate", [
    "terminal_failed", "wrong_task", "wrong_repo", "wrong_invocation", "wrong_attempt",
    "invalid_head", "unrelated_head", "old_head_generation", "missing_head_task",
])
def test_contradictory_run_evidence_never_becomes_merge_owner(incident, monkeypatch, candidate):
    c = incident
    fields = dict(run_id="candidate", ended_at=None, exit_reason="", cause="CRASH", cause_subsource=None,
                  task_spec_hash=task_spec_content_hash(c.generation_b), head_sha=c.intermediate, invocations=[])
    if candidate == "terminal_failed":
        fields.update(ended_at=c.old.ended_at, exit_reason="escalated", cause="ESCALATE")
    elif candidate in {"wrong_task", "wrong_repo"}:
        fields["task_id" if candidate == "wrong_task" else "repo_name"] = "other"
    elif candidate in {"wrong_invocation", "wrong_attempt"}:
        snapshot = replace(c.old.invocations[0], run_id="candidate")
        fields["invocations"] = [replace(snapshot, **{
            "run_id" if candidate == "wrong_invocation" else "attempt_index": "other"
            if candidate == "wrong_invocation" else 2,
        })]
    elif candidate == "invalid_head":
        fields["head_sha"] = "invalid"
    elif candidate == "unrelated_head":
        fields["head_sha"] = c.merged  # squash commit is not an ancestor of the PR head
    elif candidate == "old_head_generation":
        fields["head_sha"] = c.old_head
    else:
        real_git = git_ops._git

        def missing(path, *args, **kwargs):
            if args == ("ls-tree", c.intermediate, "--", c.task_file):
                return SimpleNamespace(stdout="")
            return real_git(path, *args, **kwargs)

        monkeypatch.setattr(git_ops, "_git", missing)
    bad = replace(c.old, **fields)
    asyncio.run(c.runner._metrics_store.save(bad))
    before = c.runner.redis.store[MetricsStore._record_key(bad.run_id)]
    asyncio.run(c.runner.handle_merge())
    assert "status: DONE" in c.remote_task()
    assert c.runner.redis.store[MetricsStore._record_key(bad.run_id)] == before
    c.preserved()


@pytest.mark.parametrize("proof", [
    "unconfirmed", "unsafe_path", "origin", "base", "missing_task", "invalid_task", "identity",
    "lost_merge", "merge_ancestry_unavailable", "run_ancestry_unavailable", "fresh_clone", "fetch_run_head",
])
def test_merge_proof_failures_are_classified_before_status_publication(incident, monkeypatch, proof):
    c = incident
    runner, real_git = c.runner, git_ops._git
    transient = {"unconfirmed", "merge_ancestry_unavailable", "run_ancestry_unavailable"}
    if proof in {"run_ancestry_unavailable", "fetch_run_head"}:
        record = replace(c.old, run_id="unfinished", exit_reason="", ended_at=None,
                         cause="CRASH", cause_subsource=None, invocations=[], head_sha=c.intermediate,
                         task_spec_hash=task_spec_content_hash(c.generation_b))
        asyncio.run(runner._metrics_store.save(record))
    if proof == "unsafe_path":
        runner.state.current_task.task_file = "../tasks/PR-395.md"
    elif proof == "base":
        runner.repo_config.branch = "-invalid"
    elif proof == "fresh_clone":
        fresh_repo = c.tmp_path / "fresh"
        git(c.tmp_path, "clone", "--single-branch", "--no-local", "-b", "main", str(c.remote), str(fresh_repo))
        runner.repo_path = str(fresh_repo)
    if proof != "unconfirmed":
        # Preserve exact transport validation while testing proof independently
        # of the handler's separate diff-statistics prerequisite.
        runner.repo_config.branch = "main" if proof == "base" else runner.repo_config.branch
        assert asyncio.run(runner._confirm_remote_merge())
        if proof == "base":
            runner.repo_config.branch = "-invalid"
            runner._confirmed_merge_outcome = replace(runner._confirmed_merge_outcome, base_branch="-invalid")

    def evidence(path, *args, **kwargs):
        if proof == "origin" and args == ("config", "--get", "remote.origin.url"):
            return SimpleNamespace(stdout="https://github.com/other/repo.git")
        if proof == "missing_task" and args[:1] == ("ls-tree",):
            return SimpleNamespace(stdout="")
        if args[:1] == ("show",):
            if proof == "invalid_task":
                return SimpleNamespace(stdout="---\nstatus: TODO\n---\ninvalid")
            if proof == "identity":
                return SimpleNamespace(stdout=c.generation_b.replace(
                    "Branch: pr-395-ci-complete-fetch", "Branch: other",
                ))
        if args[:2] == ("merge-base", "--is-ancestor"):
            if proof in {"lost_merge", "merge_ancestry_unavailable"} and args[2] == c.merged:
                return SimpleNamespace(returncode=1 if proof == "lost_merge" else 128)
            if proof == "run_ancestry_unavailable" and args[2] == c.intermediate:
                return SimpleNamespace(returncode=128)
        if proof == "fetch_run_head" and args == ("cat-file", "-e", c.intermediate):
            return SimpleNamespace(returncode=1)
        return real_git(path, *args, **kwargs)

    monkeypatch.setattr(git_ops, "_git", evidence)
    asyncio.run(runner._finish_confirmed_merge({"files_touched_count": 1}))
    if proof in {"fresh_clone", "fetch_run_head"}:
        assert "status: DONE" in c.remote_task()
    else:
        assert "status: DONE" not in c.remote_task()
        assert runner.state.state == (PipelineState.MERGE if proof in transient else PipelineState.ERROR)
    # unsafe_path intentionally changes only the in-memory task binding.
    c.task.task_file = c.task_file
    c.preserved()


def test_partial_checkpoint_and_missing_snapshot_reuse_deterministic_run(incident, monkeypatch):
    c = incident
    sadd = c.runner.redis.sadd

    async def lost_index(*args):
        raise RuntimeError("index unavailable after run SET")

    monkeypatch.setattr(c.runner.redis, "sadd", lost_index)
    asyncio.run(c.runner.handle_merge())
    accounting_id = c.runner._current_run_record.run_id
    assert c.runner.state.state == PipelineState.MERGE
    assert "status: DONE" not in c.remote_task()
    assert asyncio.run(c.runner._metrics_store.get(accounting_id)) is not None
    c.runner.redis.store.pop(pipeline_state(c.runner.name), None)
    monkeypatch.setattr(c.runner.redis, "sadd", sadd)
    restarted = c.fresh()
    c.recover(restarted)
    asyncio.run(restarted.handle_merge())
    assert "status: DONE" in c.remote_task()
    records = asyncio.run(restarted._metrics_store.list_task_runs(restarted.name, c.task.pr_id))
    assert [r.run_id for r in records if r.outcome == "merged"] == [accounting_id]
    c.preserved()


def test_conflicting_deterministic_record_is_preserved(incident, monkeypatch):
    c = incident
    checkpoint = c.runner._checkpoint_current_run_record

    async def hold():
        raise RuntimeError("checkpoint held")

    monkeypatch.setattr(c.runner, "_checkpoint_current_run_record", hold)
    asyncio.run(c.runner.handle_merge())
    accounting_id = c.runner._current_run_record.run_id
    bad = replace(c.old, run_id=accounting_id, invocations=[])
    raw = json.dumps(asdict(bad), sort_keys=True)
    c.runner.redis.store[MetricsStore._record_key(accounting_id)] = raw
    c.runner._current_run_record = None
    monkeypatch.setattr(c.runner, "_checkpoint_current_run_record", checkpoint)
    asyncio.run(c.runner.handle_merge())
    assert c.runner.state.state == PipelineState.ERROR
    assert "recovery accounting identity already incompatible" in c.runner.state.error_message
    assert c.runner.redis.store[MetricsStore._record_key(accounting_id)] == raw
    assert "status: DONE" not in c.remote_task()
    c.preserved()


def test_finalized_owner_wins_replay_over_stale_compatible_invocation(incident):
    c = incident
    asyncio.run(c.runner.handle_merge())
    records = asyncio.run(c.runner._metrics_store.list_task_runs(c.runner.name, c.task.pr_id))
    completed = next(r for r in records if r.outcome == "merged")
    before = asdict(completed)
    invocation = replace(c.old.invocations[0], run_id="unfinished")
    unfinished = replace(completed, run_id="unfinished", head_sha=c.intermediate,
                         ended_at=None, exit_reason="", outcome="failed", cause="CRASH", invocations=[invocation])
    asyncio.run(c.runner._metrics_store.save(unfinished))
    restarted = c.fresh()
    restarted.state.current_task, restarted.state.current_pr = c.task, c.pr
    restarted.state.active_invocation = invocation
    restarted.state.state, restarted.state.merge_phase = PipelineState.MERGE, "post_merge_cleanup"
    asyncio.run(restarted.handle_merge())
    records = asyncio.run(restarted._metrics_store.list_task_runs(restarted.name, c.task.pr_id))
    merged = [r for r in records if r.outcome == "merged"]
    assert len(merged) == 1 and asdict(merged[0]) == before
    assert "status: DONE" in c.remote_task()
    c.preserved()


def test_completed_accounting_discovery_defers_unavailable_generation_read(incident, monkeypatch):
    c = incident
    asyncio.run(c.runner.handle_merge())
    asyncio.run(c.runner.publish_state())
    asyncio.run(c.runner._metrics_store.save(replace(c.old, run_id="newer", started_at="2099", invocations=[])))
    restarted = c.fresh()
    c.recover(restarted)
    assert restarted.state.state == PipelineState.WATCH
    read = restarted._merge_task_generation_at_ref

    def unavailable(*args, **kwargs):
        raise RuntimeError("Git storage unavailable")

    monkeypatch.setattr(restarted, "_merge_task_generation_at_ref", unavailable)
    assert not asyncio.run(restarted.recover_state())
    assert "status: DONE" in c.remote_task()
    monkeypatch.setattr(restarted, "_merge_task_generation_at_ref", read)
    c.recover(restarted)
    assert restarted.state.state == PipelineState.WATCH
    c.preserved()


def test_legacy_optional_invocation_binding_uses_proved_generation_and_ancestor(incident):
    c = incident
    invocation = replace(c.old.invocations[0], run_id=None, attempt_index=None)
    good = replace(c.old, run_id="legacy-compatible", ended_at=None, exit_reason="",
                   cause="CRASH", cause_subsource=None, head_sha=c.intermediate,
                   task_spec_hash=task_spec_content_hash(c.generation_b), invocations=[invocation])
    asyncio.run(c.runner._metrics_store.save(good))
    asyncio.run(c.runner.handle_merge())
    saved = asyncio.run(c.runner._metrics_store.get(good.run_id))
    assert saved.outcome == "merged" and saved.head_sha == c.head
    assert saved.invocations == [invocation]
    assert (saved.tokens_in, saved.tokens_out) == (good.tokens_in, good.tokens_out)
    assert "status: DONE" in c.remote_task()
    c.preserved()


def test_compatible_legacy_checkpoint_is_recovered_from_shared_recent_index(incident):
    c = incident
    good = replace(c.old, run_id="legacy-recent", ended_at=None, exit_reason="",
                   cause="CRASH", cause_subsource=None, head_sha=c.intermediate,
                   task_spec_hash=task_spec_content_hash(c.generation_b), invocations=[])
    # Legacy metrics may predate the per-task index. Newer unrelated entries
    # in the shared repository index must still pass the same admission rule.
    c.runner.redis.store[MetricsStore._record_key(good.run_id)] = json.dumps(asdict(good))
    recent_key = MetricsStore._recent_key(c.task.pr_id, c.runner.name)
    c.runner.redis.lists[recent_key].append(good.run_id)
    unrelated = replace(c.old, run_id="unrelated", task_id="PR-999", invocations=[])
    asyncio.run(c.runner._metrics_store.save(unrelated))
    c.runner._current_run_record = None
    c.runner.state.active_invocation = None
    asyncio.run(c.runner.handle_merge())
    assert "status: DONE" in c.remote_task()
    saved = asyncio.run(c.runner._metrics_store.get(good.run_id))
    assert saved.outcome == "merged" and saved.head_sha == c.head
    assert (saved.tokens_in, saved.tokens_out) == (good.tokens_in, good.tokens_out)
    c.preserved()
