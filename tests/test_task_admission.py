"""Admission, restart and branch ownership regressions for final Reject."""

from __future__ import annotations

import hashlib
import json
import subprocess
from unittest.mock import AsyncMock

import httpx
import pytest
from src.approval_commands import build_approval, enqueue_approval
from src.cancellation.storage import cause_key
from src.daemon import git_ops
from src.daemon import task_admission as daemon_admission
from src.github.gh_runner import run_gh as cli_run_gh
from src.keyspace import pipeline_state, upload_pending
from src.models import PipelineState, QueueTask, TaskStatus
from src.rejection_commands import load_rejection
from src.retry_commands import enqueue_retry_command, new_retry_command
from src.task_admission import admission_candidate, validate_admission_graph
from src.task_attempts import (
    AdmissionRejected,
    AttemptChanged,
    attempt_key,
    clear_failed_pr_creation,
    load_attempt,
    new_attempt,
    save_attempt,
)

from tests.test_approval_commands import git, isolated_daemon_process_view  # noqa: F401
from tests.test_rejection_commands import post_reject
from tests.test_rejection_commands import rejected as rejection_fixture

rejected = rejection_fixture


async def finish_reject(fixture):
    assert (await post_reject(fixture)).status_code == 202
    runner, command, *_ = fixture
    await runner._consume_rejection_commands()
    assert (await load_rejection(runner.redis, runner.name, command.binding)).released


def rewritten(repo):
    return (
        (repo / "tasks/PR-42.md")
        .read_text()
        .replace("Original specification.", "New specification.")
        .replace("status: ERROR", "status: TODO")
        .replace("blocked_reason: guardrail\n", "")
    )


async def stage(fixture, text):
    return await stage_files(fixture, [("PR-42.md", text)])


async def stage_files(fixture, files):
    runner, _, _, _, _, app = fixture
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        return await client.post(
            f"/repos/{runner.name}/upload-tasks",
            files=[("files", (name, text, "text/markdown")) for name, text in files],
        )


def task_43_from(repo, *, depends_on="none"):
    text = (
        rewritten(repo)
        .replace("PR-42:", "PR-43:")
        .replace("fix/pr-42", "fix/pr-43")
    )
    return task_with_dependency(text, depends_on)


def task_42_from(repo, *, depends_on="none"):
    return task_with_dependency(rewritten(repo), depends_on)


def task_with_dependency(text, depends_on):
    lines = [
        f"- Depends on: {depends_on}" if line.startswith("- Depends on: ") else line
        for line in text.splitlines()
    ]
    return "\n".join(lines) + ("\n" if text.endswith("\n") else "")


def test_admission_graph_tolerates_legacy_noise_but_rejects_structured_errors(tmp_path):
    tasks = tmp_path / "tasks"
    tasks.mkdir()
    (tasks / "PR-90.md").write_text("# PR-90: Legacy note\n\nUnstructured body.\n")
    (tasks / "PR-91.md").write_text("Loose operator notes without a task header.\n")
    validate_admission_graph(tmp_path)

    (tasks / "PR-92.md").write_text(
        "---\nstatus: TODO\n---\n\n"
        "# PR-92: Broken structured task\n"
        "Branch: task/92\n"
        "- Type: invalid\n"
        "- Complexity: low\n"
        "- Depends on: none\n"
    )
    with pytest.raises(AdmissionRejected, match="invalid Type"):
        validate_admission_graph(tmp_path)


async def test_direct_admission_rejects_malformed_incoming_task(rejected, tmp_path):
    runner, _, *_ = rejected
    incoming = tmp_path / "PR-42.md"
    incoming.write_text("not a structured task\n")
    with pytest.raises(AdmissionRejected, match="missing task header"):
        await runner._validate_admission(incoming, upload=True)


@pytest.mark.parametrize(
    "mode",
    [
        "same",
        "status_only",
        "missing_dependency",
        "default_branch",
        "wrong_id",
        "api_unavailable",
        "merged_pr",
        "alternative_completion",
        "unknown_completion_identity",
    ],
)
async def test_admission_refuses_unsafe_reuse(rejected, tmp_path, monkeypatch, mode):
    await finish_reject(rejected)
    runner, command, repo, _, _, _ = rejected
    content = rewritten(repo)
    if mode == "same":
        content = (repo / "tasks/PR-42.md").read_text()
    elif mode == "status_only":
        content = content.replace("New specification.", "Original specification.")
    elif mode == "missing_dependency":
        content = content.replace("Depends on: none", "Depends on: PR-999")
    elif mode == "default_branch":
        content = content.replace("Branch: fix/pr-42", "Branch: main")
    elif mode == "wrong_id":
        content = content.replace("# PR-42:", "# PR-43:")
    elif mode == "api_unavailable":
        monkeypatch.setattr(
            "src.task_admission.gh_pr_get_merged_branches", lambda *a: (_ for _ in ()).throw(OSError("offline"))
        )
    elif mode == "merged_pr":
        monkeypatch.setattr("src.task_admission.gh_pr_get_merged_branches", lambda *a: {"fix/pr-42"})
    elif mode in {"alternative_completion", "unknown_completion_identity"}:
        old = await load_attempt(runner.redis, runner.name, "PR-42")
        (repo / "tasks/completions.json").write_text(
            json.dumps(
                {
                    "schema_version": 1,
                    "repository": "octo/demo",
                    "base_branch": "main",
                    "completions": {
                        "PR-42": {
                            "task_sha256": old.file_sha256 if mode == "alternative_completion" else "0" * 64,
                            "merge_commit": git(repo, "rev-parse", "main"),
                            "pull_request": 17,
                            "reason": "Implemented by alternative PR",
                        }
                    },
                }
            )
        )
    before = (repo / "tasks/PR-42.md").read_bytes()
    response = await stage(rejected, content)
    if mode in {"same", "status_only", "missing_dependency"}:
        assert response.status_code in {400, 409}, response.text
        assert await runner.redis.get(upload_pending(runner.name)) is None
    else:
        # Staging acknowledges the input; only the daemon verifies and admits it.
        assert response.status_code == 200, response.text
        temporary = mode in {"api_unavailable", "unknown_completion_identity"}
        assert await runner.process_pending_uploads() is (None if temporary else False)
        assert (await runner.redis.get(upload_pending(runner.name)) is not None) is temporary
    assert (repo / "tasks/PR-42.md").read_bytes() == before
    assert (await load_attempt(runner.redis, runner.name, "PR-42")).rejection == command.binding


async def test_rejected_completion_matches_rejection_time_task_bytes(rejected):
    runner, command, repo, _, _, _ = rejected
    path = repo / "tasks/PR-42.md"
    error_text = path.read_text()
    pre_error_text = error_text.replace("status: ERROR", "status: TODO").replace("blocked_reason: guardrail\n", "")
    prior = new_attempt(
        runner.repo_config.url,
        runner.state.current_task.model_copy(update={"status": TaskStatus.TODO}),
        pre_error_text,
        attempt_id=command.attempt_id,
        started=True,
        coder_dispatched=True,
    )
    assert prior.fingerprint == command.fingerprint
    assert prior.file_sha256 != command.file_sha256
    await save_attempt(runner.redis, runner.name, prior, expected=None)
    await finish_reject(rejected)
    (repo / "tasks/completions.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "repository": "octo/demo",
                "base_branch": "main",
                "completions": {
                    "PR-42": {
                        "task_sha256": command.file_sha256,
                        "merge_commit": git(repo, "rev-parse", "main"),
                        "pull_request": 17,
                        "reason": "Implemented by alternative PR after ERROR status was recorded",
                    }
                },
            }
        )
    )
    before = path.read_bytes()
    assert (await stage(rejected, rewritten(repo))).status_code == 200

    assert await runner.process_pending_uploads() is False

    assert await runner.redis.get(upload_pending(runner.name)) is None
    assert path.read_bytes() == before
    current = await load_attempt(runner.redis, runner.name, "PR-42")
    assert current.rejection == command.binding
    assert current.file_sha256 == prior.file_sha256


async def test_configured_base_git_rewrite_admits_once(rejected):
    await finish_reject(rejected)
    runner, command, repo, _, _, _ = rejected
    old = await load_attempt(runner.redis, runner.name, "PR-42")
    (repo / "tasks/PR-42.md").write_text(rewritten(repo))
    git(repo, "commit", "-am", "rewrite unfinished task")
    git(repo, "push", "origin", "main")
    assert await runner._reconcile_git_admissions() == set()
    accepted = await load_attempt(runner.redis, runner.name, "PR-42")
    assert accepted.attempt_id != old.attempt_id
    key = f"metrics:retry_count:{runner.name}:PR-42"
    await runner.redis.set(key, "2")
    assert await runner._reconcile_git_admissions() == set()
    assert await runner.redis.get(key) == "2"
    assert (await load_attempt(runner.redis, runner.name, "PR-42")).attempt_id == accepted.attempt_id


async def test_pending_upload_from_before_reject_does_not_reactivate(rejected):
    runner, command, repo, _, _, _ = rejected
    # The upload originated when this task was still queued. Execution and
    # the guardrail decision happen after HTTP staging but before consumption.
    path = repo / "tasks/PR-42.md"
    original = path.read_text()
    path.write_text(original.replace("status: ERROR", "status: TODO").replace("blocked_reason: guardrail\n", ""))
    persisted = runner.state.model_dump_json()
    queued = runner.state.model_copy(deep=True)
    queued.current_task = None
    queued.state = PipelineState.IDLE
    await runner.redis.set(pipeline_state(runner.name), queued.model_dump_json())
    assert (await stage(rejected, rewritten(repo))).status_code == 200
    await runner.redis.set(pipeline_state(runner.name), persisted)
    path.write_text(original)
    await finish_reject(rejected)
    before = (repo / "tasks/PR-42.md").read_bytes()
    assert await runner.process_pending_uploads() is False
    assert (repo / "tasks/PR-42.md").read_bytes() == before
    assert (await load_attempt(runner.redis, runner.name, "PR-42")).rejection == command.binding


async def test_git_deletion_is_not_undone_by_pending_upload(rejected):
    await finish_reject(rejected)
    runner, _, repo, _, _, _ = rejected
    assert (await stage(rejected, rewritten(repo))).status_code == 200
    git(repo, "rm", "tasks/PR-42.md")
    git(repo, "commit", "-m", "explicit task deletion")
    git(repo, "push", "origin", "main")
    assert await runner.process_pending_uploads() is False
    assert not (repo / "tasks/PR-42.md").exists()
    assert (
        await runner._fence_recovery_tasks([QueueTask(pr_id="PR-42", title="old snapshot", status=TaskStatus.TODO)])
        == []
    )


async def test_upload_lost_ack_reconciles_without_resetting_again(rejected, monkeypatch):
    await finish_reject(rejected)
    runner, _, repo, _, _, _ = rejected
    assert (await stage(rejected, rewritten(repo))).status_code == 200
    original = runner._finish_admission

    async def lose_reply(attempt):
        await original(attempt)
        raise RuntimeError("lost acknowledgement after Redis EXEC")

    monkeypatch.setattr(runner, "_finish_admission", lose_reply)
    assert await runner.process_pending_uploads() is None
    attempt = await load_attempt(runner.redis, runner.name, "PR-42")
    assert not attempt.admission_pending
    counter = f"metrics:retry_count:{runner.name}:PR-42"
    await runner.redis.set(counter, "2")
    monkeypatch.setattr(runner, "_finish_admission", original)
    assert await runner.process_pending_uploads() is True
    assert (await load_attempt(runner.redis, runner.name, "PR-42")).attempt_id == attempt.attempt_id
    assert await runner.redis.get(counter) == "2"


@pytest.mark.parametrize("operation", ["handle_coding", "handle_fix", "handle_watch", "handle_merge", "handle_error"])
async def test_rejected_attempt_blocks_all_execution_paths(rejected, monkeypatch, operation):
    runner, _, _, _, github, _ = rejected
    await post_reject(rejected)
    monkeypatch.setattr(runner, "_get_coder", lambda: pytest.fail("rejected attempt cannot dispatch coder"))
    await getattr(runner, operation)()
    assert not github["calls"]


async def test_old_approval_and_retry_cannot_cross_replacement(rejected):
    runner, command, repo, _, _, _ = rejected
    approval = build_approval(runner.name, runner.state, command.failure, repo)
    await enqueue_approval(runner.redis, approval)
    retry = new_retry_command(
        repo_slug=runner.name,
        task_id="PR-42",
        task_file="tasks/PR-42.md",
        task_branch="fix/pr-42",
        task_fingerprint=command.fingerprint,
        request_binding="old",
        failure_id="old",
        retry_cap=3,
    )
    await enqueue_retry_command(runner.redis, retry)
    await finish_reject(rejected)
    assert (await stage(rejected, rewritten(repo))).status_code == 200
    assert await runner.process_pending_uploads() is True
    assert not await runner._consume_approval_command()
    await runner._consume_retry_command()
    stored = await load_attempt(runner.redis, runner.name, "PR-42")
    assert not stored.rejection and not stored.started
    assert runner.state.current_pr is None


@pytest.mark.parametrize("change", ["local", "remote", "dirty", "process", "reopened"])
async def test_reused_branch_cleanup_refuses_ambiguous_ownership(rejected, monkeypatch, change):
    await finish_reject(rejected)
    runner, _, repo, remote, github, _ = rejected
    assert (await stage(rejected, rewritten(repo))).status_code == 200
    assert await runner.process_pending_uploads() is True
    attempt = await load_attempt(runner.redis, runner.name, "PR-42")
    runner.state.current_task = attempt.task.model_copy(deep=True)
    if change in {"local", "remote"}:
        git(repo, "checkout", "fix/pr-42")
        (repo / "external.txt").write_text("external work")
        git(repo, "add", ".")
        git(repo, "commit", "-m", "unexpected external update")
        if change == "remote":
            git(repo, "push", "origin", "fix/pr-42")
        git(repo, "checkout", "main")
    elif change == "dirty":
        (repo / "unrelated.txt").write_text("preserve")
    elif change == "process":
        monkeypatch.setattr(daemon_admission, "checkout_process_blocker", lambda path: "orphan still running")
    else:
        github["state"] = "open"
    remote_before = git(remote, "show-ref")
    assert not await runner._prepare_task_attempt((repo / "tasks/PR-42.md").read_text())
    assert git(remote, "show-ref") == remote_before
    assert not (await load_attempt(runner.redis, runner.name, "PR-42")).started


async def test_new_attempt_resets_only_its_counters_once_and_preserves_controls(rejected):
    await finish_reject(rejected)
    runner, _, repo, _, _, _ = rejected
    per_attempt = [
        "diagnose_exhausted",
        "metrics:retry_count",
        "metrics:retry_fingerprint",
        "metrics:attempt_count",
        "current_run_started_at",
    ]
    for prefix in per_attempt:
        await runner.redis.set(f"{prefix}:{runner.name}:PR-42", "old")
        await runner.redis.set(f"{prefix}:{runner.name}:PR-43", "sibling")
    preserved = {
        f"control:{runner.name}:user_paused": "1",
        f"control:{runner.name}:stop": "1",
        f"metrics:errors:{runner.name}": "history",
        "budget:account": "123",
    }
    for key, value in preserved.items():
        await runner.redis.set(key, value)
    runner._crashed_task_pr_ids.update({"PR-42", "PR-43"})
    runner._status_write_failed_task_pr_ids.update({"PR-42", "PR-43"})
    assert (await stage(rejected, rewritten(repo))).status_code == 200
    assert await runner.process_pending_uploads() is True
    for prefix in per_attempt:
        assert await runner.redis.get(f"{prefix}:{runner.name}:PR-42") is None
        assert await runner.redis.get(f"{prefix}:{runner.name}:PR-43") == "sibling"
    for key, value in preserved.items():
        assert await runner.redis.get(key) == value
    assert runner._crashed_task_pr_ids == {"PR-43"}
    assert runner._status_write_failed_task_pr_ids == {"PR-43"}


@pytest.mark.parametrize("selected", [False, True])
async def test_branch_conflict_never_enters_destructive_preflight_retry(rejected, selected):
    await finish_reject(rejected)
    runner, _, repo, _, _, _ = rejected
    assert (await stage(rejected, rewritten(repo))).status_code == 200
    assert await runner.process_pending_uploads() is True
    attempt = await load_attempt(runner.redis, runner.name, "PR-42")
    runner.state.current_task = attempt.task if selected else None
    unrelated = repo / "unrelated.txt"
    unrelated.write_text("preserve")
    refs = git(repo, "show-ref")
    for _ in range(5):
        assert not await runner.preflight()
        assert unrelated.read_text() == "preserve"
        assert git(repo, "show-ref") == refs


async def test_ambiguous_pr_creation_is_not_repeated_after_restart(rejected, monkeypatch):
    from tests.runner import _helpers as h

    await finish_reject(rejected)
    runner, _, repo, _, github, _ = rejected
    assert (await stage(rejected, rewritten(repo))).status_code == 200
    assert await runner.process_pending_uploads() is True
    attempt = await load_attempt(runner.redis, runner.name, "PR-42")
    runner.state.current_task = attempt.task
    assert await runner._prepare_task_attempt((repo / "tasks/PR-42.md").read_text())
    original = daemon_admission.gh_runner.run_gh

    def timeout(args, *a, **kw):
        result = original(args, *a, **kw)
        if args[:2] == ["pr", "create"]:
            raise TimeoutError("lost create acknowledgement")
        return result

    monkeypatch.setattr(daemon_admission.gh_runner, "run_gh", timeout)
    assert not await runner._daemon_create_pr_for_branch("fix/pr-42", "claude")
    assert (await load_attempt(runner.redis, runner.name, "PR-42")).pr_creation_pending
    fresh = h._make_runner()
    fresh.redis, fresh.repo_path, fresh.state = runner.redis, runner.repo_path, runner.state
    fresh.state.current_task = attempt.task
    assert await fresh._daemon_create_pr_for_branch("fix/pr-42", "claude")
    assert sum(call[:2] == ["pr", "create"] for call in github["calls"]) == 1
    assert not await fresh._prepare_task_attempt((repo / "tasks/PR-42.md").read_text())


@pytest.mark.parametrize(
    "case",
    [
        "invalid_ref",
        "legacy_reject",
        "not_final",
        "obsolete_upload",
        "foreign_receipt",
        "missing_dependency",
        "duplicate_branch",
        "completed",
        "active_attempt",
    ],
)
async def test_shared_admission_refuses_invalid_or_obsolete_ownership(rejected, tmp_path, case):
    await finish_reject(rejected)
    runner, command, repo, *_ = rejected
    incoming = tmp_path / "PR-42.md"
    content = rewritten(repo)
    token = command.binding
    receipt = await load_attempt(runner.redis, runner.name, "PR-42")
    if case == "invalid_ref":
        content = content.replace("fix/pr-42", "fix/../bad")
    elif case == "legacy_reject":
        await runner.redis.delete(attempt_key(runner.name, "PR-42"))
    elif case == "not_final":
        from src.rejection_commands import rejection_key

        await runner.redis.delete(rejection_key(runner.name, command.binding))
    elif case in {"obsolete_upload", "foreign_receipt", "completed", "active_attempt"}:
        receipt.rejection = None
        await runner.redis.delete(cause_key(runner.name, "PR-42"))
        if case == "foreign_receipt":
            receipt.repo_url = "https://github.com/other/repo.git"
            token = None
        elif case == "completed":
            receipt.completed = True
            token = None
        elif case == "active_attempt":
            receipt.started = True
            token = None
        await runner.redis.set(attempt_key(runner.name, "PR-42"), receipt.model_dump_json())
    elif case == "missing_dependency":
        content = content.replace("Depends on: none", "Depends on: PR-99")
    elif case == "duplicate_branch":
        (repo / "tasks/PR-43.md").write_text(content.replace("PR-42:", "PR-43:"))
    incoming.write_text(content)
    with pytest.raises(AttemptChanged):
        await admission_candidate(
            runner.redis,
            runner.name,
            runner.repo_config.url,
            "main",
            repo,
            incoming,
            expected_rejection=token,
            upload=True,
        )


async def test_prior_base_snapshot_preserves_legacy_rejection_and_ignores_unstructured_history(rejected):
    runner, _, repo, *_ = rejected
    git(repo, "checkout", "main")
    path = repo / "tasks/PR-42.md"
    path.write_text(path.read_text().replace("blocked_reason: guardrail", "blocked_reason: operator_reject"))
    (repo / "tasks/README.md").write_text("helper")
    (repo / "tasks/PR-99.md").write_text("# PR-99: Historical loose notes\n")
    git(repo, "add", "tasks")
    git(repo, "commit", "-m", "legacy headers")
    await runner._snapshot_accepted_specs()
    receipt = await load_attempt(runner.redis, runner.name, "PR-42")
    assert receipt.rejection == "legacy-missing-identity"
    assert receipt.file_sha256 == hashlib.sha256(path.read_bytes()).hexdigest()
    assert await load_attempt(runner.redis, runner.name, "PR-99") is None
    await runner._snapshot_accepted_specs()
    assert (await load_attempt(runner.redis, runner.name, "PR-42")).attempt_id == receipt.attempt_id


@pytest.mark.parametrize(
    "case", ["checkout", "origin", "superseded", "status_failure", "status_success", "lost_finish"]
)
async def test_partial_admission_is_not_runnable_until_git_and_receipt_agree(rejected, tmp_path, monkeypatch, case):
    await finish_reject(rejected)
    runner, command, repo, *_ = rejected
    incoming = tmp_path / "PR-42.md"
    incoming.write_text(rewritten(repo))
    if case.startswith("status_"):
        incoming.write_text(
            incoming.read_text().replace("status: TODO", "status: ERROR\nblocked_reason: operator_reject")
        )
    pending = await runner._reserve_admission(incoming, token=command.binding, upload=True)
    if case != "checkout":
        (repo / "tasks/PR-42.md").write_text(incoming.read_text())
        git(repo, "commit", "-am", "rewritten spec")
        if case != "origin":
            git(repo, "push", "origin", "main")
    if case == "superseded":
        await runner.redis.set(
            attempt_key(runner.name, "PR-42"),
            pending.model_copy(update={"attempt_id": "replacement"}).model_dump_json(),
        )
    elif case == "status_failure":
        monkeypatch.setattr(runner, "_commit_task_status_change", AsyncMock(return_value=False))
    if case in {"status_success", "lost_finish"}:
        runner.state.current_task = pending.task
        await runner._finish_admission(pending)
        counter = f"metrics:attempt_count:{runner.name}:PR-42"
        await runner.redis.set(counter, "2")
        await runner._finish_admission(pending)
        assert await runner.redis.get(counter) == "2"
        assert runner.state.current_task is None
    else:
        with pytest.raises(AttemptChanged):
            await runner._finish_admission(pending)
        assert (await load_attempt(runner.redis, runner.name, "PR-42")).admission_pending


@pytest.mark.parametrize("case", ["pending", "rejected", "completed", "legacy_reject"])
async def test_recovery_fences_attempts_even_with_todo_frontmatter(rejected, case):
    runner, _, repo, *_ = rejected
    if case == "legacy_reject":
        from src.cancellation.storage import CancellationCause

        cause = CancellationCause(category="ERROR", payload={"subsource": "operator_reject"})
        await runner.redis.set(cause_key(runner.name, "PR-42"), cause.to_redis())
        assert await runner._reconcile_git_admissions() == {"PR-42"}
    else:
        attempt = new_attempt(runner.repo_config.url, runner.state.current_task, (repo / "tasks/PR-42.md").read_text())
        if case == "pending":
            attempt.admission_pending = True
        elif case == "rejected":
            attempt.rejection = "old"
        else:
            attempt.completed = True
        await save_attempt(runner.redis, runner.name, attempt, expected=None)
    result = await runner._fence_recovery_tasks(
        [runner.state.current_task.model_copy(update={"status": TaskStatus.TODO})]
    )
    assert result[0].status == (TaskStatus.DONE if case == "completed" else TaskStatus.ERROR)


async def test_completed_rejected_attempt_is_not_held_as_failed_admission(rejected):
    await finish_reject(rejected)
    runner, command, *_ = rejected
    prior = await load_attempt(runner.redis, runner.name, "PR-42")
    completed = await save_attempt(
        runner.redis,
        runner.name,
        prior.model_copy(update={"completed": True}),
        expected=prior,
    )

    assert completed.rejection == command.binding
    assert await runner._reconcile_git_admissions() == set()
    assert runner._admission_held_task_ids == set()
    fenced = await runner._fence_recovery_tasks([completed.task.model_copy(update={"status": TaskStatus.TODO})])
    assert fenced[0].status == TaskStatus.DONE


@pytest.mark.parametrize("case", ["success", "origin_pending", "changed_again"])
async def test_git_admission_restart_resumes_partial_receipt(rejected, tmp_path, case):
    await finish_reject(rejected)
    runner, command, repo, *_ = rejected
    incoming = tmp_path / "PR-42.md"
    incoming.write_text(rewritten(repo))
    pending = await runner._reserve_admission(incoming, token=command.binding, upload=True)
    (repo / "tasks/PR-42.md").write_text(incoming.read_text())
    git(repo, "commit", "-am", "accepted input")
    if case == "success":
        git(repo, "push", "origin", "main")
    elif case == "changed_again":
        (repo / "tasks/PR-42.md").write_text(incoming.read_text() + "\nAnother change.\n")
    held = await runner._reconcile_git_admissions()
    assert held == (set() if case == "success" else {"PR-42"})
    assert (await load_attempt(runner.redis, runner.name, "PR-42")).admission_pending is (case != "success")
    if case == "success":
        assert (await load_attempt(runner.redis, runner.name, "PR-42")).attempt_id == pending.attempt_id


@pytest.mark.parametrize("case", ["missing_receipt", "foreign_branch", "foreign_origin", "another_pr", "base_advanced"])
async def test_branch_recreation_requires_all_ownership_evidence(rejected, monkeypatch, case):
    await finish_reject(rejected)
    runner, command, repo, remote, *_ = rejected
    assert (await stage(rejected, rewritten(repo))).status_code == 200
    assert await runner.process_pending_uploads()
    attempt = await load_attempt(runner.redis, runner.name, "PR-42")
    before = git(repo, "rev-parse", "fix/pr-42")
    if case == "missing_receipt":
        from src.rejection_commands import rejection_key

        await runner.redis.delete(rejection_key(runner.name, command.binding))
    elif case == "foreign_branch":
        attempt.task.branch = "main"
    elif case == "foreign_origin":
        original = git_ops._git
        from tests.runner import _helpers as h

        monkeypatch.setattr(
            git_ops,
            "_git",
            lambda path, *args, **kw: h._FakeCompletedProcess(stdout="https://github.com/other/repo.git")
            if args == ("remote", "get-url", "origin")
            else original(path, *args, **kw),
        )
    elif case == "another_pr":
        original = daemon_admission.gh_runner.run_gh
        monkeypatch.setattr(
            daemon_admission.gh_runner,
            "run_gh",
            lambda args, *a, **kw: [{"number": 99}] if args[:2] == ["pr", "list"] else original(args, *a, **kw),
        )
    else:
        git(repo, "commit", "--allow-empty", "-m", "upstream advanced")
        git(repo, "push", "origin", "main")
        # A branch ref transaction models a concurrent base advance while
        # the local checkout still names the earlier clean base.
        git(repo, "reset", "--hard", "HEAD~1")
    with pytest.raises(AttemptChanged):
        await runner._prepare_reused_branch(attempt)
    assert git(repo, "rev-parse", "fix/pr-42") == before
    assert git(remote, "rev-parse", "fix/pr-42") == before


async def test_git_input_already_present_at_rejection_does_not_become_new_attempt(rejected):
    runner, command, repo, *_ = rejected
    new_text = rewritten(repo)
    git(repo, "checkout", "main")
    (repo / "tasks/PR-42.md").write_text(new_text)
    git(repo, "commit", "-am", "earlier input")
    git(repo, "push", "origin", "main")
    git(repo, "checkout", "fix/pr-42")
    await finish_reject(rejected)
    assert await runner._reconcile_git_admissions() == {"PR-42"}
    assert (await load_attempt(runner.redis, runner.name, "PR-42")).rejection == command.binding


@pytest.mark.parametrize("single", [False, True])
@pytest.mark.parametrize("fetch_failure", [False, True])
async def test_reject_fetches_base_before_anchoring_later_git_rewrites(
    rejected, monkeypatch, tmp_path, single, fetch_failure
):
    from tests.runner import _helpers as h

    runner, command, repo, remote, github, _ = rejected
    runner.repo_config.feature_flags.use_single_error_exit = single
    stale_base = git(repo, "rev-parse", "origin/main")
    writer = tmp_path / "operator"
    git(tmp_path, "clone", "--branch", "main", str(remote), str(writer))
    git(writer, "config", "user.name", "Operator")
    git(writer, "config", "user.email", "operator@example.test")
    (writer / "tasks/PR-42.md").write_text(rewritten(repo))
    git(writer, "commit", "-am", "task edit before Reject")
    git(writer, "push", "origin", "main")
    before_reject = git(writer, "rev-parse", "HEAD")
    assert before_reject != stale_base
    assert git(repo, "rev-parse", "origin/main") == stale_base
    assert (await post_reject(rejected)).status_code == 202
    original_git = git_ops._git

    def transport(path, *args, **kwargs):
        if fetch_failure and args[:2] == ("fetch", "origin"):
            raise OSError("remote temporarily unavailable")
        return original_git(path, *args, **kwargs)

    monkeypatch.setattr(git_ops, "_git", transport)
    await runner._run_cycle_body()
    stored = await load_rejection(runner.redis, runner.name, command.binding)
    if fetch_failure:
        assert stored.status == "deferred" and not stored.released and not stored.base_commit
        assert github["state"] == "open" and github["calls"] == []
        assert git(repo, "rev-parse", "origin/main") == stale_base
        fresh = h._make_runner()
        fresh.redis, fresh.repo_path = runner.redis, runner.repo_path
        fresh.repo_config.feature_flags.use_single_error_exit = single
        runner = fresh
        fetch_failure = False
        await runner._run_cycle_body()
        stored = await load_rejection(runner.redis, runner.name, command.binding)
    assert stored.released and stored.base_commit == before_reject
    assert github["state"] == "closed"
    runner.sync_to_main()
    assert await runner._reconcile_git_admissions() == {"PR-42"}
    prior = await load_attempt(runner.redis, runner.name, "PR-42")
    assert prior.rejection == command.binding and not prior.admission_pending

    # A later, genuinely changed specification follows the same Git path
    # and can now be admitted without inheriting the rejected attempt.
    (writer / "tasks/PR-42.md").write_text(rewritten(repo) + "\nRewrite after final rejection.\n")
    git(writer, "commit", "-am", "later specification rewrite")
    git(writer, "push", "origin", "main")
    runner.sync_to_main()
    assert await runner._reconcile_git_admissions() == set()
    current = await load_attempt(runner.redis, runner.name, "PR-42")
    assert current.attempt_id != prior.attempt_id and current.previous_rejection == command.binding
    assert not current.admission_pending and current.rejection is None
    assert github["new_pr"] is None


async def test_completed_dependency_and_unstructured_sibling_are_preserved(rejected, tmp_path, monkeypatch):
    await finish_reject(rejected)
    runner, command, repo, *_ = rejected
    incoming = tmp_path / "PR-42.md"
    incoming.write_text(rewritten(repo).replace("Depends on: none", "Depends on: PR-99"))
    (repo / "tasks/PR-90.md").write_text("# PR-90: Historical task\n")
    monkeypatch.setattr("src.task_admission.get_merged_pr_ids", lambda path, base, ids: ids & {"PR-99"})
    _, candidate = await admission_candidate(
        runner.redis,
        runner.name,
        runner.repo_config.url,
        "main",
        repo,
        incoming,
        expected_rejection=command.binding,
        upload=True,
    )
    assert candidate.task.depends_on == ["PR-99"]


async def test_admission_detects_new_decision_between_validation_and_reservation(rejected, tmp_path, monkeypatch):
    await finish_reject(rejected)
    runner, command, repo, *_ = rejected
    incoming = tmp_path / "PR-42.md"
    incoming.write_text(rewritten(repo))
    original = daemon_admission.admission_candidate

    async def validate(*args, **kwargs):
        result = await original(*args, **kwargs)
        previous = result[0]
        await runner.redis.set(
            attempt_key(runner.name, "PR-42"), previous.model_copy(update={"completed": True}).model_dump_json()
        )
        return result

    monkeypatch.setattr(daemon_admission, "admission_candidate", validate)
    with pytest.raises(AttemptChanged, match="during admission"):
        await runner._reserve_admission(incoming, token=command.binding, upload=True)
    assert (await load_attempt(runner.redis, runner.name, "PR-42")).completed


async def test_retry_queued_before_reject_cannot_delete_rejection_evidence(rejected):
    runner, command, *_ = rejected
    retry = new_retry_command(
        repo_slug=runner.name,
        task_id="PR-42",
        task_file="tasks/PR-42.md",
        task_branch="fix/pr-42",
        task_fingerprint=command.fingerprint,
        request_binding="before",
        failure_id="before",
        retry_cap=3,
    )
    await enqueue_retry_command(runner.redis, retry)
    await post_reject(rejected)
    cause = await runner.redis.get(cause_key(runner.name, "PR-42"))
    with pytest.raises(AttemptChanged):
        await runner._clear_retry_failure_evidence(retry)
    assert await runner.redis.get(cause_key(runner.name, "PR-42")) == cause
    with pytest.raises(AttemptChanged):
        await enqueue_retry_command(
            runner.redis, retry.model_copy(update={"command_id": "different", "request_binding": "after"})
        )


async def test_previous_run_history_cannot_complete_or_attach_to_reused_task(rejected, monkeypatch):
    runner, _, repo, *_ = rejected
    runner._start_current_run_record("claude", "test")
    previous_record = runner._current_run_record
    previous_record.attempt_id = "abandoned-attempt"
    await finish_reject(rejected)
    assert (await stage(rejected, rewritten(repo))).status_code == 200
    assert await runner.process_pending_uploads()
    attempt = await load_attempt(runner.redis, runner.name, "PR-42")
    runner.state.current_task = attempt.task
    monkeypatch.setattr(runner._metrics_store, "recent", AsyncMock(return_value=[previous_record]))
    await runner._restore_current_run_record()
    assert runner._current_run_record is None
    runner._current_run_record = previous_record
    await runner._save_current_run_record("success_merged")
    assert not (await load_attempt(runner.redis, runner.name, "PR-42")).completed
    runner._current_run_record = None
    monkeypatch.setattr(runner.redis, "get", AsyncMock(side_effect=OSError("unavailable")))
    await runner._save_current_run_record("success_merged")


async def test_new_branch_name_leaves_the_closed_attempt_branch_intact(rejected):
    await finish_reject(rejected)
    runner, _, repo, remote, *_ = rejected
    content = rewritten(repo).replace("fix/pr-42", "fix/revised-pr-42")
    assert (await stage(rejected, content)).status_code == 200
    assert await runner.process_pending_uploads()
    attempt = await load_attempt(runner.redis, runner.name, "PR-42")
    old = git(remote, "rev-parse", "fix/pr-42")
    await runner._prepare_reused_branch(attempt)
    assert git(remote, "rev-parse", "fix/pr-42") == old
    assert git(repo, "rev-parse", "fix/pr-42") == old


async def test_unaccepted_specification_never_reaches_coder(rejected, monkeypatch):
    runner, _, repo, *_ = rejected
    attempt = new_attempt(runner.repo_config.url, runner.state.current_task, (repo / "tasks/PR-42.md").read_text())
    await save_attempt(runner.redis, runner.name, attempt, expected=None)
    (repo / "tasks/PR-42.md").write_text(rewritten(repo))
    monkeypatch.setattr(
        "src.claude_cli.run_auto_pr_async", AsyncMock(side_effect=AssertionError("must not code changed spec"))
    )
    await runner.handle_coding()
    assert not (await load_attempt(runner.redis, runner.name, "PR-42")).started


async def test_legacy_upload_manifest_cannot_resurrect_deleted_task(rejected):
    await finish_reject(rejected)
    runner, _, repo, *_ = rejected
    assert (await stage(rejected, rewritten(repo))).status_code == 200
    manifest = json.loads(await runner.redis.get(upload_pending(runner.name)))
    manifest.pop("prior_spec_files")
    await runner.redis.set(upload_pending(runner.name), json.dumps(manifest))
    git(repo, "rm", "tasks/PR-42.md")
    git(repo, "commit", "-m", "explicit deletion")
    git(repo, "push", "origin", "main")
    assert await runner.process_pending_uploads() is False
    assert not (repo / "tasks/PR-42.md").exists()


async def test_first_startup_cannot_hide_completion_by_rewriting_its_file_hash(rejected):
    runner, _, repo, *_ = rejected
    (repo / "tasks/completions.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "repository": "octo/demo",
                "base_branch": "main",
                "completions": {
                    "PR-42": {
                        "task_sha256": "0" * 64,
                        "merge_commit": git(repo, "rev-parse", "main"),
                        "pull_request": 17,
                        "reason": "Historical completed implementation",
                    }
                },
            }
        )
    )
    assert not await runner._prepare_task_attempt((repo / "tasks/PR-42.md").read_text())
    attempt = await load_attempt(runner.redis, runner.name, "PR-42")
    assert not attempt.started
    assert any("unresolved prior task identity" in item["event"] for item in runner.state.history)


async def test_empty_active_task_cannot_prepare_an_attempt(rejected):
    runner, *_ = rejected
    runner.state.current_task = None
    assert not await runner._prepare_task_attempt("irrelevant")


async def test_legacy_todo_header_does_not_hide_a_live_pr_owner(rejected):
    runner, _, repo, *_ = rejected
    path = repo / "tasks/PR-42.md"
    path.write_text(
        path.read_text().replace("status: ERROR", "status: TODO").replace("blocked_reason: guardrail\n", "")
    )
    response = await stage(rejected, rewritten(repo))
    assert response.status_code == 200
    assert await runner.process_pending_uploads() is False
    assert any("active attempt owns" in entry["event"] for entry in runner.state.history)
    assert await runner.redis.get(upload_pending(runner.name)) is None
    assert "New specification." not in path.read_text()


async def test_reject_accepted_during_http_upload_prevents_staging(rejected, monkeypatch):
    from datetime import datetime, timezone

    from src.rejection_commands import enqueue_rejection
    from src.web.routes import uploads

    runner, command, repo, *_ = rejected
    original = uploads.load_attempt

    async def accept_reject(*args):
        await enqueue_rejection(runner.redis, command.model_copy(update={"requested_at": datetime.now(timezone.utc)}))
        return await original(*args)

    monkeypatch.setattr(uploads, "load_attempt", accept_reject)
    response = await stage(rejected, rewritten(repo))
    assert response.status_code == 400
    assert "Upload began before Reject" in response.text
    assert await runner.redis.get(upload_pending(runner.name)) is None


async def test_independent_task_is_selected_while_rejected_task_stays_error(rejected, monkeypatch):
    await finish_reject(rejected)
    runner, _, repo, *_ = rejected
    (repo / "tasks/PR-43.md").write_text(rewritten(repo).replace("PR-42:", "PR-43:").replace("fix/pr-42", "fix/pr-43"))
    git(repo, "add", "tasks/PR-43.md")
    git(repo, "commit", "-m", "independent task")
    git(repo, "push", "origin", "main")
    received = []

    async def coder_dispatch():
        received.append(runner.state.current_task.pr_id)

    monkeypatch.setattr(runner, "handle_coding", coder_dispatch)
    monkeypatch.setattr("src.github.prs.get_open_prs", lambda *a, **kw: [])
    await runner.handle_idle()
    assert received == ["PR-43"]
    rejected_task = next(t for t in runner.state.current_queue if t.pr_id == "PR-42")
    assert rejected_task.status == TaskStatus.ERROR


async def test_pending_admission_cannot_roll_back_to_rejected_spec_after_history_expires(rejected, tmp_path):
    await finish_reject(rejected)
    runner, command, repo, *_ = rejected
    incoming = tmp_path / "PR-42.md"
    incoming.write_text(rewritten(repo))
    pending = await runner._reserve_admission(incoming, token=command.binding, upload=True)
    # Redis reservation succeeded, but Git never received the rewritten bytes.
    # Expiring the old display cause cannot make the old base file a new input.
    await runner.redis.delete(cause_key(runner.name, "PR-42"))
    assert await runner._reconcile_git_admissions() == {"PR-42"}
    current = await load_attempt(runner.redis, runner.name, "PR-42")
    assert current.attempt_id == pending.attempt_id and current.admission_pending
    assert not current.started


async def test_multiple_queued_rewrites_keep_abandoned_branch_cleanup_obligation(rejected):
    await finish_reject(rejected)
    runner, command, repo, remote, *_ = rejected
    assert (await stage(rejected, rewritten(repo))).status_code == 200
    assert await runner.process_pending_uploads()
    first = await load_attempt(runner.redis, runner.name, "PR-42")
    changed = (repo / "tasks/PR-42.md").read_text() + "\nFurther clarified requirement.\n"
    assert (await stage(rejected, changed)).status_code == 200
    assert await runner.process_pending_uploads()
    current = await load_attempt(runner.redis, runner.name, "PR-42")
    assert current.attempt_id != first.attempt_id
    assert current.previous_rejection == command.binding
    runner.state.current_task = current.task
    assert await runner._prepare_task_attempt(changed)
    assert "refs/heads/fix/pr-42" not in git(remote, "show-ref")
    assert not (repo / "abandoned-marker.txt").exists()


async def test_new_guardrail_panel_does_not_associate_an_old_pr_approval(rejected):
    from src.cancellation.storage import CancellationCause, record_cancellation_cause
    from src.web.routes import dashboard

    runner, command, repo, *_ = rejected
    approval = build_approval(runner.name, runner.state, command.failure, repo)
    await enqueue_approval(runner.redis, approval)
    await finish_reject(rejected)
    assert (await stage(rejected, rewritten(repo))).status_code == 200
    assert await runner.process_pending_uploads()
    attempt = await load_attempt(runner.redis, runner.name, "PR-42")
    runner.state.current_task = attempt.task
    from src.models import PRInfo

    runner.state.current_pr = PRInfo(
        number=43, branch="fix/pr-42", pr_id="PR-42", head_sha=git(repo, "rev-parse", "HEAD")
    )
    runner.state.state = PipelineState.ERROR
    cause = CancellationCause(
        category="ERROR",
        task_id="PR-42",
        repo_slug=runner.name,
        payload={"subsource": "guardrail", "rule": "new rule", "excerpt": "new finding"},
    )
    await record_cancellation_cause(runner.redis, runner.name, "PR-42", cause)
    views = await dashboard._build_guardrail_pending_view(runner.redis, runner.name, runner.state)
    active = next(view for view in views if view["is_active"])
    assert "approval" not in active
    assert active["approval_binding"] != approval.binding


@pytest.mark.parametrize("single", [False, True])
@pytest.mark.parametrize("visibility", ["empty", "outage"])
async def test_pending_created_pr_recovers_in_later_cycle_without_restart_or_coder(
    rejected, monkeypatch, single, visibility
):
    import asyncio

    from src.models import PRInfo

    await finish_reject(rejected)
    runner, _, repo, _, github, _ = rejected
    runner.repo_config.feature_flags.use_single_error_exit = single
    assert (await stage(rejected, rewritten(repo))).status_code == 200
    assert await runner.process_pending_uploads()
    attempt = await load_attempt(runner.redis, runner.name, "PR-42")
    runner.state.current_task = attempt.task
    assert await runner._prepare_task_attempt((repo / "tasks/PR-42.md").read_text())
    git(repo, "checkout", "-b", "fix/pr-42", "origin/main")
    (repo / "new-work.txt").write_text("preserve the new attempt")
    git(repo, "add", "new-work.txt")
    git(repo, "commit", "-m", "new implementation")
    git(repo, "push", "-u", "origin", "fix/pr-42")
    runner.state.state = PipelineState.CODING
    visible = False
    lookups = []
    reviews = []

    def get_open_prs(*args, **kwargs):
        lookups.append(visible)
        if not visible:
            if visibility == "outage":
                raise OSError("GitHub list unavailable")
            return []
        return [PRInfo(number=99, branch="unrelated"), github["new_pr"]]

    original_sleep = asyncio.sleep

    async def short_sleep(_delay):
        await original_sleep(0)

    original_transport = daemon_admission.gh_runner.run_gh
    discovery_lookups = []

    def transport(args, *a, **kwargs):
        if args[:3] == ["api", "--paginate", "--slurp"] and "/pulls?" in args[-1]:
            discovery_lookups.append(visible)
            if not visible:
                if visibility == "outage":
                    raise OSError("GitHub list unavailable")
                return [[]]
        return original_transport(args, *a, **kwargs)

    monkeypatch.setattr(daemon_admission.gh_runner, "run_gh", transport)
    monkeypatch.setattr("src.github.prs.get_open_prs", get_open_prs)
    monkeypatch.setattr("src.daemon.handlers.coding.asyncio.sleep", short_sleep)
    monkeypatch.setattr(runner, "_post_codex_review", lambda number: reviews.append(number))
    await runner._diagnose_exit_zero_no_pr("fix/pr-42", "claude", AsyncMock(return_value=False))
    assert len(lookups) == 3
    assert runner.state.state == PipelineState.ERROR
    assert (await load_attempt(runner.redis, runner.name, "PR-42")).pr_creation_pending
    preserved_head = git(repo, "rev-parse", "fix/pr-42")

    # Another failed cycle keeps polling read-only, then the same running
    # daemon adopts the PR once GitHub's list becomes visible.
    await runner._run_cycle_body()
    assert discovery_lookups == [False]
    assert runner.state.state == PipelineState.ERROR
    visible = True
    await runner._run_cycle_body()
    assert discovery_lookups == [False, True]
    assert runner.state.state == PipelineState.WATCH
    assert runner.state.current_pr.number == 43
    current = await load_attempt(runner.redis, runner.name, "PR-42")
    assert current.attempt_id == attempt.attempt_id
    assert not current.pr_creation_pending and current.pr_number == 43
    assert sum(call[:2] == ["pr", "create"] for call in github["calls"]) == 1
    assert reviews == [43]
    assert github["state"] == "closed"
    assert git(repo, "rev-parse", "fix/pr-42") == preserved_head
    assert git(repo, "show", "fix/pr-42:new-work.txt") == "preserve the new attempt"
    assert not await runner._reconcile_pending_pr_creation()


@pytest.mark.parametrize("single", [False, True])
@pytest.mark.parametrize(
    "failure", ["before_exec", "after_exec", "repeated_loss", "rejection", "replacement", "missing"]
)
async def test_creation_intent_acknowledgement_replays_before_github_request(
    rejected, monkeypatch, single, failure
):
    from redis.exceptions import ConnectionError as RedisConnectionError

    await finish_reject(rejected)
    runner, _, repo, _, github, _ = rejected
    runner.repo_config.feature_flags.use_single_error_exit = single
    assert (await stage(rejected, rewritten(repo))).status_code == 200
    assert await runner.process_pending_uploads()
    attempt = await load_attempt(runner.redis, runner.name, "PR-42")
    runner.state.current_task = attempt.task
    assert await runner._prepare_task_attempt((repo / "tasks/PR-42.md").read_text())
    git(repo, "checkout", "-b", "fix/pr-42", "origin/main")
    (repo / "intent-work.txt").write_text("preserve implementation across lost EXEC reply")
    git(repo, "add", "intent-work.txt")
    git(repo, "commit", "-m", "new implementation")
    git(repo, "push", "-u", "origin", "fix/pr-42")
    work_head = git(repo, "rev-parse", "HEAD")
    accepted_base = git(repo, "rev-parse", "origin/main")
    runner.state.state = PipelineState.CODING
    original_transaction = runner.redis.transaction
    key = attempt_key(runner.name, "PR-42")
    calls = 0
    concurrent = None

    async def transaction(callback, *keys, **kwargs):
        nonlocal calls, concurrent
        if key not in keys or github["new_pr"] is not None:
            return await original_transaction(callback, *keys, **kwargs)
        calls += 1
        assert github["new_pr"] is None, "GitHub must wait until receipt acknowledgement is confirmed"
        if calls == 1 and failure == "before_exec":
            raise RedisConnectionError("connection lost before EXEC")
        result = await original_transaction(callback, *keys, **kwargs)
        if calls == 1 and failure in {"rejection", "replacement", "missing"}:
            current = await load_attempt(runner.redis, runner.name, "PR-42")
            if failure == "missing":
                await runner.redis.delete(key)
            else:
                concurrent = (
                    current.model_copy(update={"rejection": "concurrent-reject"})
                    if failure == "rejection"
                    else new_attempt(runner.repo_config.url, current.task, "replacement specification")
                )
                await runner.redis.set(key, concurrent.model_dump_json())
        if calls == 1 or failure == "repeated_loss":
            raise RedisConnectionError("EXEC reply lost")
        return result

    monkeypatch.setattr(runner.redis, "transaction", transaction)
    monkeypatch.setattr("src.github.prs.get_open_prs", lambda *a, **kw: [github["new_pr"]] if github["new_pr"] else [])
    reviews = []
    monkeypatch.setattr(runner, "_post_codex_review", lambda number: reviews.append(number))
    await runner._diagnose_exit_zero_no_pr("fix/pr-42", "claude", AsyncMock(return_value=False))
    assert calls == 2
    creations = sum(call[:2] == ["pr", "create"] for call in github["calls"])
    if failure in {"before_exec", "after_exec"}:
        assert runner.state.state == PipelineState.WATCH
        assert runner.state.current_pr.number == 43
        assert creations == 1 and reviews == [43]
        current = await load_attempt(runner.redis, runner.name, "PR-42")
        assert current.pr_number == 43 and not current.pr_creation_pending
        assert current.attempt_id == attempt.attempt_id
    else:
        assert runner.state.state == PipelineState.ERROR
        assert "Cannot confirm PR creation intent" in runner.state.error_message
        assert creations == 0 and reviews == []
        assert json.loads(await runner.redis.get(pipeline_state(runner.name)))["state"] == "ERROR"
        current = await load_attempt(runner.redis, runner.name, "PR-42")
        if failure == "repeated_loss":
            assert current.pr_creation_pending and current.attempt_id == attempt.attempt_id
        else:
            assert current == concurrent
    assert git(repo, "rev-parse", "fix/pr-42") == work_head
    assert git(repo, "show", "fix/pr-42:intent-work.txt") == "preserve implementation across lost EXEC reply"
    assert git(repo, "rev-parse", "origin/main") == accepted_base
    assert git(repo, "rev-parse", "HEAD") == work_head
    assert github["state"] == "closed"


@pytest.mark.parametrize("single", [False, True])
@pytest.mark.parametrize("failure", ["authentication", "validation"])
async def test_definitive_create_failure_allows_http_retry_without_losing_work(
    rejected, monkeypatch, single, failure, clear_ack="ok"
):
    import asyncio

    from redis.exceptions import ConnectionError as RedisConnectionError
    from src.daemon.handlers import coding as coding_handler
    from src.web.routes import repo_control

    from tests.runner import _helpers as h

    await finish_reject(rejected)
    runner, _, repo, _, github, app = rejected
    runner.repo_config.feature_flags.use_single_error_exit = single
    assert (await stage(rejected, rewritten(repo))).status_code == 200
    assert await runner.process_pending_uploads()
    attempt = await load_attempt(runner.redis, runner.name, "PR-42")
    runner.state.current_task = attempt.task
    content = (repo / "tasks/PR-42.md").read_text()
    assert await runner._prepare_task_attempt(content)
    git(repo, "checkout", "-b", "fix/pr-42", "origin/main")
    (repo / "retry-work.txt").write_text("keep this implementation")
    git(repo, "add", "retry-work.txt")
    git(repo, "commit", "-m", "new attempt work")
    git(repo, "push", "-u", "origin", "fix/pr-42")
    work_head = git(repo, "rev-parse", "HEAD")
    runner.state.state = PipelineState.CODING
    failed = True
    create_calls = []
    original_transport = daemon_admission.gh_runner.run_gh
    original_subprocess = subprocess.run

    def subprocess_boundary(cmd, *a, **kw):
        if cmd[:3] == ["gh", "pr", "create"]:
            code, stderr = (4, "Authentication required") if failure == "authentication" else (
                1, "pull request create failed: GraphQL: No commits between main and fix/pr-42 (createPullRequest)"
            )
            return subprocess.CompletedProcess(cmd, code, stdout="", stderr=stderr)
        return original_subprocess(cmd, *a, **kw)

    def transport(args, *a, **kw):
        if args[:2] == ["pr", "create"]:
            create_calls.append(args)
            if failed:
                return cli_run_gh(args, *a, **kw)
        return original_transport(args, *a, **kw)

    original_sleep = asyncio.sleep
    original_clear = coding_handler.clear_failed_pr_creation
    original_load = coding_handler.load_attempt
    clear_calls = 0

    async def short_sleep(_delay):
        await original_sleep(0)

    async def clear_transport(redis, name, attempt_to_clear):
        nonlocal clear_calls
        clear_calls += 1
        if clear_ack in {"before_exec_twice", "before_exec_and_load_outage"}:
            raise RedisConnectionError("connection lost before clearing creation intent")
        await original_clear(redis, name, attempt_to_clear)
        if clear_ack == "after_exec" and clear_calls == 1:
            raise RedisConnectionError("EXEC reply lost while clearing creation intent")
        if clear_ack == "after_exec_twice":
            raise RedisConnectionError("repeated EXEC reply loss while clearing creation intent")

    async def load_transport(redis, name, task_id):
        if clear_ack == "before_exec_and_load_outage" and clear_calls >= 2:
            raise RedisConnectionError("connection lost while confirming creation intent clear")
        return await original_load(redis, name, task_id)

    monkeypatch.setattr(subprocess, "run", subprocess_boundary)
    monkeypatch.setattr(daemon_admission.gh_runner, "run_gh", transport)
    monkeypatch.setattr(coding_handler, "clear_failed_pr_creation", clear_transport)
    monkeypatch.setattr(coding_handler, "load_attempt", load_transport)
    monkeypatch.setattr("src.github.prs.get_open_prs", lambda *a, **kw: [github["new_pr"]] if github["new_pr"] else [])
    monkeypatch.setattr("src.daemon.handlers.coding.asyncio.sleep", short_sleep)
    await runner._diagnose_exit_zero_no_pr("fix/pr-42", "claude", AsyncMock(return_value=False))
    assert runner.state.state == PipelineState.ERROR
    assert clear_calls == (1 if clear_ack == "ok" else 2)
    receipt = await load_attempt(runner.redis, runner.name, "PR-42")
    if clear_ack in {"before_exec_twice", "before_exec_and_load_outage"}:
        assert receipt.pr_creation_pending and receipt.attempt_id == attempt.attempt_id
        assert "Cannot confirm failed PR creation cleanup" in runner.state.error_message
        assert github["new_pr"] is None
        assert git(repo, "rev-parse", "fix/pr-42") == work_head
        assert not await runner._reconcile_pending_pr_creation()
        receipt = await load_attempt(runner.redis, runner.name, "PR-42")
        assert not receipt.pr_creation_pending and receipt.attempt_id == attempt.attempt_id
        return
    assert not receipt.pr_creation_pending and receipt.attempt_id == attempt.attempt_id
    assert not await runner._reconcile_pending_pr_creation()
    assert github["new_pr"] is None
    assert git(repo, "rev-parse", "fix/pr-42") == work_head

    # Bind a real browser Retry to this failure, then let a restarted daemon
    # consume it through the normal cycle and preserve the implementation.
    await runner.publish_state()
    context = await repo_control._retry_binding_context(
        runner.redis, runner.name, runner.state.current_task, repo / "tasks/PR-42.md",
        "tasks/PR-42.md", retry_count=0, state=runner.state,
    )
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        response = await client.post(
            f"/repos/{runner.name}/tasks/PR-42/retry", data={"retry_binding": context["binding"]},
        )
    assert response.status_code == 202, response.text
    fresh = h._make_runner()
    fresh.redis, fresh.repo_path, fresh.state = runner.redis, runner.repo_path, runner.state
    fresh.repo_config.feature_flags.use_single_error_exit = single
    fresh._recovered = True
    received = []

    async def coder(path, pr_id, task_file, task_body, **kwargs):
        received.append(task_body)
        assert git(repo, "rev-parse", "fix/pr-42") == work_head
        assert git(repo, "show", "fix/pr-42:retry-work.txt") == "keep this implementation"
        return 0, "ok", ""

    monkeypatch.setattr(h.claude_cli, "run_auto_pr_async", coder)
    monkeypatch.setattr(fresh, "_post_codex_review", lambda *a, **kw: True)
    failed = False
    await fresh._run_cycle_body()
    assert len(received) == 1
    assert fresh.state.state == PipelineState.WATCH
    assert fresh.state.current_pr.number == 43
    assert len(create_calls) == 2
    assert (await load_attempt(fresh.redis, fresh.name, "PR-42")).attempt_id == attempt.attempt_id
    assert github["state"] == "closed"


@pytest.mark.parametrize("single", [False, True])
@pytest.mark.parametrize(
    "clear_ack", ["after_exec", "after_exec_twice", "before_exec_twice", "before_exec_and_load_outage"]
)
async def test_definitive_create_failure_replays_lost_clear_ack(rejected, monkeypatch, single, clear_ack):
    await test_definitive_create_failure_allows_http_retry_without_losing_work(
        rejected,
        monkeypatch,
        single,
        "authentication",
        clear_ack=clear_ack,
    )


@pytest.mark.parametrize("change", ["rejection", "replacement", "missing"])
async def test_failed_creation_acknowledgement_preserves_concurrent_attempt_state(rejected, change):
    from src.task_attempts import new_attempt, save_attempt

    runner, _, repo, *_ = rejected
    task = runner.state.current_task
    attempt = new_attempt(runner.repo_config.url, task, (repo / task.task_file).read_text(), pr_creation_pending=True)
    await save_attempt(runner.redis, runner.name, attempt, expected=None)
    if change == "missing":
        await runner.redis.delete(attempt_key(runner.name, task.pr_id))
        with pytest.raises(AttemptChanged):
            await clear_failed_pr_creation(runner.redis, runner.name, attempt)
        assert await load_attempt(runner.redis, runner.name, task.pr_id) is None
        return
    current = attempt.model_copy(update={"rejection": "accepted-decision"}) if change == "rejection" else new_attempt(
        runner.repo_config.url, task, "replacement spec", pr_creation_pending=True,
    )
    await save_attempt(runner.redis, runner.name, current, expected=attempt)
    if change == "replacement":
        with pytest.raises(AttemptChanged):
            await clear_failed_pr_creation(runner.redis, runner.name, attempt)
        assert await load_attempt(runner.redis, runner.name, task.pr_id) == current
        return
    await clear_failed_pr_creation(runner.redis, runner.name, attempt)
    await clear_failed_pr_creation(runner.redis, runner.name, attempt)
    stored = await load_attempt(runner.redis, runner.name, task.pr_id)
    assert stored == current.model_copy(update={"pr_creation_pending": False})


async def test_creation_failure_acknowledgement_races_reject_without_erasing_it(rejected):
    import asyncio

    from src.rejection_commands import build_rejection, enqueue_rejection

    runner, _, repo, *_ = rejected
    task = runner.state.current_task
    attempt = new_attempt(runner.repo_config.url, task, (repo / task.task_file).read_text(), pr_creation_pending=True)
    task.attempt_id = attempt.attempt_id
    runner.state.current_pr = None
    await save_attempt(runner.redis, runner.name, attempt, expected=None)
    await runner.redis.set(pipeline_state(runner.name), runner.state.model_dump_json())
    cause = await runner.redis.get(cause_key(runner.name, task.pr_id))
    command = build_rejection(runner.name, runner.state, cause, repo)
    await asyncio.gather(
        clear_failed_pr_creation(runner.redis, runner.name, attempt),
        enqueue_rejection(runner.redis, command),
    )
    current = await load_attempt(runner.redis, runner.name, task.pr_id)
    assert current.rejection == command.binding and not current.pr_creation_pending
    assert (await load_rejection(runner.redis, runner.name, command.binding)).attempt_id == attempt.attempt_id
    assert await runner._attempt_execution_blocked()


@pytest.mark.parametrize("single", [False, True])
async def test_upload_stages_without_completion_queries_then_daemon_verifies(rejected, monkeypatch, single):
    from src import task_admission

    await finish_reject(rejected)
    runner, command, repo, *_ = rejected
    runner.repo_config.feature_flags.use_single_error_exit = single
    prior = await load_attempt(runner.redis, runner.name, "PR-42")
    content = rewritten(repo)
    original = (repo / "tasks/PR-42.md").read_bytes()
    calls = []
    available = False

    def completion_transport(owner, branches):
        calls.append((owner, branches))
        if not available:
            raise OSError("GitHub completion evidence unavailable")
        return set()

    monkeypatch.setattr(task_admission, "gh_pr_get_merged_branches", completion_transport)
    assert (await stage(rejected, content)).status_code == 200
    assert calls == []  # No GitHub verification ran inside the HTTP request.
    manifest = json.loads(await runner.redis.get(upload_pending(runner.name)))
    assert manifest["rejection_tokens"]["PR-42"] == command.binding
    assert manifest["prior_spec_files"]["PR-42"] == prior.fingerprint
    assert (await load_attempt(runner.redis, runner.name, "PR-42")) == prior
    assert (repo / "tasks/PR-42.md").read_bytes() == original

    assert await runner.process_pending_uploads() is None
    assert len(calls) == 1
    assert (await load_attempt(runner.redis, runner.name, "PR-42")) == prior
    assert (repo / "tasks/PR-42.md").read_bytes() == original
    available = True
    assert await runner.process_pending_uploads()
    current = await load_attempt(runner.redis, runner.name, "PR-42")
    assert current.attempt_id != prior.attempt_id and not current.admission_pending
    assert (repo / "tasks/PR-42.md").read_text() == content


@pytest.mark.parametrize("status_only", [False, True])
async def test_daemon_refuses_staged_replay_of_rejected_spec_even_after_http_acceptance(rejected, status_only):
    from pathlib import Path

    await finish_reject(rejected)
    runner, command, repo, *_ = rejected
    previous = await load_attempt(runner.redis, runner.name, "PR-42")
    old_text = (repo / "tasks/PR-42.md").read_text()
    assert (await stage(rejected, rewritten(repo))).status_code == 200
    manifest = json.loads(await runner.redis.get(upload_pending(runner.name)))
    # An obsolete staging writer changes the accepted upload before consumption.
    # The daemon must enforce final rejection independently of the HTTP check.
    replay = old_text.replace("status: ERROR", "status: TODO") if status_only else old_text
    if status_only:
        replay = "".join(line for line in replay.splitlines(keepends=True) if not line.startswith("blocked_reason:"))
    (Path(manifest["staging_dir"]) / "PR-42.md").write_text(replay)
    assert await runner.process_pending_uploads() is False
    assert await load_attempt(runner.redis, runner.name, "PR-42") == previous
    assert (repo / "tasks/PR-42.md").read_text() == old_text
    assert any("File unchanged. Reject is final" in row["event"] for row in runner.state.history)
    assert (await load_rejection(runner.redis, runner.name, command.binding)).released


@pytest.mark.parametrize("single", [False, True])
@pytest.mark.parametrize("change", ["delete", "rewrite"])
@pytest.mark.parametrize("invalid_first", [False, True])
async def test_stale_batch_is_retired_before_reservation_and_next_upload_succeeds(
    rejected, single, change, invalid_first
):
    from pathlib import Path

    await finish_reject(rejected)
    runner, _, repo, _, _, app = rejected
    runner.repo_config.feature_flags.use_single_error_exit = single
    valid_text = rewritten(repo).replace("PR-42:", "PR-43:").replace("fix/pr-42", "fix/pr-43")
    stale = ("files", ("PR-42.md", rewritten(repo), "text/markdown"))
    valid = ("files", ("PR-43.md", valid_text, "text/markdown"))
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        response = await client.post(
            f"/repos/{runner.name}/upload-tasks", files=[stale, valid] if invalid_first else [valid, stale],
        )
        assert response.status_code == 200
        manifest = json.loads(await runner.redis.get(upload_pending(runner.name)))
        if change == "delete":
            git(repo, "rm", "tasks/PR-42.md")
        else:
            path = repo / "tasks/PR-42.md"
            path.write_text(path.read_text() + "\nChanged on configured base.\n")
            git(repo, "add", "tasks/PR-42.md")
        git(repo, "commit", "-m", "operator changes task after staging")
        git(repo, "push", "origin", "main")
        before = git(repo, "rev-parse", "HEAD")
        counter = f"metrics:retry_count:{runner.name}:PR-42"
        await runner.redis.set(counter, "2")
        assert await runner.process_pending_uploads() is False
        assert await runner.redis.get(upload_pending(runner.name)) is None
        assert not Path(manifest["staging_dir"]).exists()
        assert await load_attempt(runner.redis, runner.name, "PR-43") is None
        assert await runner.redis.get(counter) == "2"
        assert git(repo, "rev-parse", "HEAD") == before
        assert not (repo / "tasks/PR-43.md").exists()
        assert await runner.process_pending_uploads() is False
        assert any("Discarded invalid upload batch" in row["event"] for row in runner.state.history)

        response = await client.post(f"/repos/{runner.name}/upload-tasks", files=[valid])
        assert response.status_code == 200
        next_manifest = json.loads(await runner.redis.get(upload_pending(runner.name)))
        assert next_manifest["files"] == ["PR-43.md"]
        assert await runner.process_pending_uploads() is True
    assert (repo / "tasks/PR-43.md").read_text() == valid_text
    current = await load_attempt(runner.redis, runner.name, "PR-43")
    assert not current.admission_pending
    if change == "delete":
        assert not (repo / "tasks/PR-42.md").exists()
    else:
        assert "Changed on configured base." in (repo / "tasks/PR-42.md").read_text()


@pytest.mark.parametrize("single", [False, True])
@pytest.mark.parametrize("retained_existing", [False, True])
async def test_upload_dependency_cycle_is_discarded_before_reservation_or_push(
    rejected, single, retained_existing
):
    from pathlib import Path

    await finish_reject(rejected)
    runner, command, repo, *_ = rejected
    runner.repo_config.feature_flags.use_single_error_exit = single
    prior42 = await load_attempt(runner.redis, runner.name, "PR-42")
    original42 = (repo / "tasks/PR-42.md").read_text()
    valid43 = task_43_from(repo, depends_on="PR-42")
    if retained_existing:
        git(repo, "checkout", "main")
        (repo / "tasks/PR-43.md").write_text(valid43)
        git(repo, "add", "tasks/PR-43.md")
        git(repo, "commit", "-m", "add retained dependent task")
        git(repo, "push", "origin", "main")

    valid42 = task_42_from(repo)
    cycle42 = task_42_from(repo, depends_on="PR-43")
    cycle43 = task_43_from(repo, depends_on="PR-42")
    files = [("PR-42.md", cycle42)]
    if not retained_existing:
        files.append(("PR-43.md", cycle43))
    response = await stage_files(rejected, files)
    assert response.status_code == 200, response.text
    manifest = json.loads(await runner.redis.get(upload_pending(runner.name)))
    before_head = git(repo, "rev-parse", "HEAD")
    before_origin = git(repo, "rev-parse", "origin/main")
    await runner.redis.set(f"metrics:retry_count:{runner.name}:PR-42", "4")

    assert await runner.process_pending_uploads() is False
    assert await runner.redis.get(upload_pending(runner.name)) is None
    assert not Path(manifest["staging_dir"]).exists()
    assert git(repo, "rev-parse", "HEAD") == before_head
    assert git(repo, "rev-parse", "origin/main") == before_origin
    assert (repo / "tasks/PR-42.md").read_text() == original42
    assert await load_attempt(runner.redis, runner.name, "PR-42") == prior42
    assert await load_attempt(runner.redis, runner.name, "PR-43") is None
    assert await runner.redis.get(f"metrics:retry_count:{runner.name}:PR-42") == "4"
    assert any("Dependency cycle" in row["event"] for row in runner.state.history)

    valid_files = [("PR-42.md", valid42)]
    if not retained_existing:
        valid_files.append(("PR-43.md", valid43))
    response = await stage_files(rejected, valid_files)
    assert response.status_code == 200, response.text
    assert await runner.process_pending_uploads() is True
    current42 = await load_attempt(runner.redis, runner.name, "PR-42")
    current43 = await load_attempt(runner.redis, runner.name, "PR-43")
    assert current42.attempt_id != prior42.attempt_id
    assert current42.previous_rejection == command.binding and current42.rejection is None
    assert not current42.admission_pending
    assert current43 is not None and not current43.admission_pending
    assert (repo / "tasks/PR-43.md").read_text() == valid43


@pytest.mark.parametrize("single", [False, True])
async def test_git_dependency_cycle_is_held_before_reserving_rewrites(
    rejected, single
):
    await finish_reject(rejected)
    runner, command, repo, *_ = rejected
    runner.repo_config.feature_flags.use_single_error_exit = single
    prior42 = await load_attempt(runner.redis, runner.name, "PR-42")
    valid42 = task_42_from(repo)

    git(repo, "checkout", "main")
    valid43 = task_43_from(repo, depends_on="none")
    (repo / "tasks/PR-43.md").write_text(valid43)
    git(repo, "add", "tasks/PR-43.md")
    git(repo, "commit", "-m", "add queued dependent task")
    git(repo, "push", "origin", "main")
    await runner._snapshot_accepted_specs()
    prior43 = await load_attempt(runner.redis, runner.name, "PR-43")
    await runner.redis.set(f"metrics:retry_count:{runner.name}:PR-42", "5")
    await runner.redis.set(f"metrics:retry_count:{runner.name}:PR-43", "6")

    (repo / "tasks/PR-42.md").write_text(task_42_from(repo, depends_on="PR-43"))
    (repo / "tasks/PR-43.md").write_text(task_43_from(repo, depends_on="PR-42"))
    git(repo, "commit", "-am", "introduce dependency cycle")
    git(repo, "push", "origin", "main")
    assert await runner._reconcile_git_admissions() == {"PR-42", "PR-43"}
    assert await load_attempt(runner.redis, runner.name, "PR-42") == prior42
    assert await load_attempt(runner.redis, runner.name, "PR-43") == prior43
    assert await runner.redis.get(f"metrics:retry_count:{runner.name}:PR-42") == "5"
    assert await runner.redis.get(f"metrics:retry_count:{runner.name}:PR-43") == "6"

    (repo / "tasks/PR-42.md").write_text(valid42)
    (repo / "tasks/PR-43.md").write_text(task_43_from(repo, depends_on="PR-42"))
    git(repo, "commit", "-am", "correct dependency graph")
    git(repo, "push", "origin", "main")
    assert await runner._reconcile_git_admissions() == set()
    current42 = await load_attempt(runner.redis, runner.name, "PR-42")
    current43 = await load_attempt(runner.redis, runner.name, "PR-43")
    assert current42.attempt_id != prior42.attempt_id
    assert current42.previous_rejection == command.binding and current42.rejection is None
    assert not current42.admission_pending
    assert current43.attempt_id != prior43.attempt_id
    assert current43.task.depends_on == ["PR-42"]
    assert not current43.admission_pending


async def test_invalid_upload_discard_cannot_delete_a_newer_submission(rejected, monkeypatch, after_read=False):
    from pathlib import Path

    await finish_reject(rejected)
    runner, _, repo, *_ = rejected
    assert (await stage(rejected, rewritten(repo))).status_code == 200
    stale_raw = await runner.redis.get(upload_pending(runner.name))
    stale = json.loads(stale_raw)
    path = repo / "tasks/PR-42.md"
    path.write_text(path.read_text() + "\nChanged on base.\n")
    git(repo, "commit", "-am", "base changed")
    git(repo, "push", "origin", "main")
    transaction = runner.redis.transaction
    raced = False
    corrected = rewritten(repo) + "\nFresh corrected upload.\n"

    async def upload_new():
        nonlocal raced
        raced = True
        assert (await stage(rejected, corrected)).status_code == 200

    async def race(func, *keys, **kwargs):
        if upload_pending(runner.name) in keys and not raced:
            if after_read:
                # Real Redis reruns the callback after this watched-key change.
                async def changed_while_watched(pipe):
                    result = await func(pipe)
                    if not raced:
                        await upload_new()
                    return result
                return await transaction(changed_while_watched, *keys, **kwargs)
            await upload_new()
        return await transaction(func, *keys, **kwargs)

    monkeypatch.setattr(runner.redis, "transaction", race)
    assert await runner.process_pending_uploads() is None
    latest = json.loads(await runner.redis.get(upload_pending(runner.name)))
    assert latest["staging_dir"] != stale["staging_dir"]
    assert (Path(latest["staging_dir"]) / "PR-42.md").read_text() == corrected
    assert await runner.process_pending_uploads() is True
    assert path.read_text() == corrected


async def test_invalid_upload_discard_waits_for_redis_and_preserves_unrelated_work(rejected, monkeypatch):
    from pathlib import Path

    await finish_reject(rejected)
    runner, _, repo, *_ = rejected
    assert (await stage(rejected, rewritten(repo))).status_code == 200
    raw = await runner.redis.get(upload_pending(runner.name))
    staging = Path(json.loads(raw)["staging_dir"])
    # Permanently invalid input, with unrelated uncommitted checkout work.
    incoming = staging / "PR-42.md"
    incoming.write_text(incoming.read_text().replace("Type: bugfix", "Type: invalid"))
    work = repo / "unrelated.txt"
    work.write_text("preserve")
    before = git(repo, "show-ref")
    transaction = runner.redis.transaction
    monkeypatch.setattr(runner.redis, "transaction", AsyncMock(side_effect=OSError("Redis unavailable")))
    assert await runner.process_pending_uploads() is None
    assert await runner.redis.get(upload_pending(runner.name)) == raw
    assert staging.exists() and work.read_text() == "preserve"
    monkeypatch.setattr(runner.redis, "transaction", transaction)
    assert await runner.process_pending_uploads() is False
    assert await runner.redis.get(upload_pending(runner.name)) is None
    assert not staging.exists() and work.read_text() == "preserve"
    assert git(repo, "show-ref") == before


async def test_upload_waiting_for_final_reject_is_retained_then_admitted(rejected):
    assert (await post_reject(rejected)).status_code == 202
    runner, _, repo, *_ = rejected
    assert (await stage(rejected, rewritten(repo))).status_code == 200
    pending = await runner.redis.get(upload_pending(runner.name))
    assert await runner.process_pending_uploads() is None
    assert await runner.redis.get(upload_pending(runner.name)) == pending
    await runner._consume_rejection_commands()
    assert await runner.process_pending_uploads() is True


@pytest.mark.parametrize("single", [False, True])
@pytest.mark.parametrize("existing_receipt", [False, True])
@pytest.mark.parametrize("changed", [False, True])
async def test_sprint_zip_replays_completed_tasks_without_reusing_them(
    rejected, monkeypatch, single, existing_receipt, changed
):
    import io
    import zipfile

    from src import task_admission
    from src.cancellation.storage import index_key

    runner, _, repo, _, _, app = rejected
    runner.repo_config.feature_flags.use_single_error_exit = single
    git(repo, "checkout", "main")
    path = repo / "tasks/PR-42.md"
    completed = path.read_text().replace("status: ERROR", "status: DONE").replace("blocked_reason: guardrail\n", "")
    path.write_text(completed)
    git(repo, "commit", "-am", "task completed")
    record = {
        "schema_version": 1, "repository": "octo/demo", "base_branch": "main",
        "completions": {"PR-42": {
            "task_sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
            "merge_commit": git(repo, "rev-parse", "main"), "pull_request": 42,
            "reason": "Completed implementation",
        }},
    }
    manifest_path = repo / "tasks/completions.json"
    manifest_path.write_text(json.dumps(record))
    git(repo, "add", "tasks/completions.json")
    git(repo, "commit", "-m", "record completed implementation")
    git(repo, "push", "origin", "main")
    runner.state.current_task = runner.state.current_pr = None
    runner.state.state = PipelineState.IDLE
    runner.state.current_queue[0].status = TaskStatus.DONE
    await runner.redis.delete(cause_key(runner.name, "PR-42"))
    await runner.redis.delete(index_key(runner.name))
    await runner.redis.set(pipeline_state(runner.name), runner.state.model_dump_json())
    prior = None
    if existing_receipt:
        await runner._snapshot_accepted_specs()
        prior = await load_attempt(runner.redis, runner.name, "PR-42")
        prior = await save_attempt(
            runner.redis, runner.name, prior.model_copy(update={"completed": True}), expected=prior,
        )
    counter = f"metrics:retry_count:{runner.name}:PR-42"
    await runner.redis.set(counter, "3")
    completed_check_calls = []
    original_verify = task_admission.verify_unfinished

    def verify(*args, **kwargs):
        completed_check_calls.append(args[-1].task.pr_id)
        return original_verify(*args, **kwargs)

    monkeypatch.setattr(task_admission, "verify_unfinished", verify)
    replay = completed.replace("status: DONE", "status: TODO")
    new_task = replay.replace("PR-42:", "PR-43:").replace("fix/pr-42", "fix/pr-43")
    if changed:
        replay += "\nChanged implementation scope for completed task.\n"
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr("PR-42.md", replay)
        archive.writestr("PR-43.md", new_task)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        response = await client.post(
            f"/repos/{runner.name}/upload-tasks", files={"files": ("sprint.zip", buffer.getvalue(), "application/zip")},
        )
    assert response.status_code == 200, response.text
    assert await runner.process_pending_uploads() is (not changed)
    current = await load_attempt(runner.redis, runner.name, "PR-42")
    assert current.task.status == TaskStatus.DONE
    assert current.file_sha256 == record["completions"]["PR-42"]["task_sha256"]
    assert not current.admission_pending and current.previous_rejection is None
    if prior:
        assert current == prior
    assert await runner.redis.get(counter) == "3"
    assert path.read_text() == completed
    assert json.loads(manifest_path.read_text()) == record
    assert completed_check_calls == (["PR-42"] if changed else [])
    new_attempt = await load_attempt(runner.redis, runner.name, "PR-43")
    if changed:
        assert new_attempt is None and not (repo / "tasks/PR-43.md").exists()
    else:
        assert not new_attempt.started and not new_attempt.admission_pending
        assert (repo / "tasks/PR-43.md").read_text() == new_task
