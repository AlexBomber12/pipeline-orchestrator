"""Characterize Reject gaps; these assertions do not implement final rejection."""

# ruff: noqa: F811 -- imported pytest fixture is injected by name

import subprocess
import sys
from dataclasses import replace
from types import SimpleNamespace

import pytest
from src.approval_commands import build_approval
from src.cancellation.storage import CancellationCause, cause_key, task_spec_content_hash
from src.keyspace import pipeline_state
from src.models import InvocationSnapshot
from src.web.routes import repo_control
from src.web.services.upload_validation import preserve_terminal_status_on_collision

from tests.test_approval_commands import approval, git  # noqa: F401
from tests.web.test_guardrail_decision_endpoint import _GuardrailRedis


async def exercise_close_transport(redis, state, config, monkeypatch, outcome):
    """Same production caller and assertions with fake or isolated real Redis.

    The transport is deliberately a controlled external service: close success
    and response delivery are independent. No real GitHub mutation is possible.
    """
    slug, task = state.name, state.current_task.pr_id
    key = cause_key(slug, task)
    original = CancellationCause(
        category="ERROR", task_id=task, repo_slug=slug,
        payload={"subsource": "guardrail", "category": "test", "excerpt": "proof"},
    ).to_redis()
    await redis.set(key, original)
    await redis.set(pipeline_state(slug), state.model_dump_json())
    calls = []
    external = {"state": "OPEN", "head": state.current_pr.head_sha}

    def transport(args):
        calls.append(args)
        assert args[:4] == ["gh", "pr", "close", str(state.current_pr.number)]
        assert args[args.index("--repo") + 1] == "octo/demo"
        assert "--delete-branch" not in args
        # An accepted close whose reply is lost is indistinguishable locally
        # from a timeout before the server applied anything.
        if outcome in {"confirmed", "lost_response"}:
            external["state"] = "CLOSED"
        if outcome in {"lost_response", "not_applied"}:
            raise subprocess.TimeoutExpired(args, 1)
        return 0, "closed"

    monkeypatch.setattr(repo_control, "_gh_subprocess", transport)
    result = await repo_control._reject_guardrail_decision(slug, task, config, redis)
    assert result.status_code == 204
    saved = CancellationCause.from_redis(await redis.get(key))
    assert saved.payload["subsource"] == "operator_reject"
    # No observed terminal state, attempt, spec hash, or structured PR receipt.
    assert set(saved.payload) == {
        "subsource", "reason_text", "original_rule", "original_excerpt",
    }
    assert len(calls) == 1  # No exact-PR readback even after successful delivery.
    assert external["state"] == ("OPEN" if outcome == "not_applied" else "CLOSED")
    replay = await repo_control._reject_guardrail_decision(slug, task, config, redis)
    assert replay.status_code == 404
    assert len(calls) == 1  # Failed closes cannot be resumed by replay.
    assert await redis.get(pipeline_state(slug)) == state.model_dump_json()
    return external


@pytest.mark.parametrize("outcome", ["confirmed", "lost_response", "not_applied"])
async def test_reject_receipt_does_not_prove_known_pr_closed(approval, monkeypatch, outcome):
    runner, _, _, _ = approval
    runner.state.name = runner.name
    await exercise_close_transport(
        _GuardrailRedis(), runner.state, SimpleNamespace(url="https://github.com/octo/demo"),
        monkeypatch, outcome,
    )


async def test_late_owned_push_survives_reject_without_a_writer_fence(approval, monkeypatch):
    runner, _, repo, remote = approval
    runner.state.name = runner.name
    before = git(remote, "rev-parse", "refs/heads/fix/pr-42")
    (repo / "late.txt").write_text("retained late work\n")
    git(repo, "add", "late.txt")
    git(repo, "commit", "-m", "late local work")
    late = git(repo, "rev-parse", "HEAD")
    # Fixture-owned real writer exists before rejection, released only afterwards.
    # stdin gates the push without timing assumptions or signalling other PIDs.
    process = subprocess.Popen(
        [sys.executable, "-c", "import subprocess,sys; sys.stdin.readline(); "
         "subprocess.run(['git','push','origin','HEAD:refs/heads/fix/pr-42'],check=True)"],
        cwd=repo, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        text=True,
    )
    try:
        external = await exercise_close_transport(
            _GuardrailRedis(), runner.state, SimpleNamespace(url="https://github.com/octo/demo"),
            monkeypatch, "confirmed",
        )
        assert process.poll() is None
        assert git(remote, "rev-parse", "refs/heads/fix/pr-42") == before
        _, stderr = process.communicate("release\n", timeout=10)
        assert process.returncode == 0, stderr
        assert git(remote, "rev-parse", "refs/heads/fix/pr-42") == late
        assert external["state"] == "CLOSED"
        assert external["head"] == before  # Old observation cannot attest the new head.
        assert git(repo, "show", f"{late}:late.txt") == "retained late work"
    finally:
        if process.poll() is None:
            process.kill()  # Only this fixture's owned child, never a cwd-scan PID.
        process.communicate(timeout=10)


async def test_changed_unfinished_spec_and_new_attempt_do_not_inherit_approval(approval):
    runner, old, repo, _ = approval
    path = repo / "tasks/PR-42.md"
    text = path.read_text()
    incoming = text.replace("status: ERROR", "status: TODO").replace("blocked_reason: guardrail\n", "")
    staged, preserved = preserve_terminal_status_on_collision(path, incoming)
    assert preserved is None
    assert task_spec_content_hash(staged) == old.task_fingerprint
    # Staging permits ERROR replacement, but the daemon treats identical spec
    # bytes as replay. A TODO status alone is not genuinely changed work.
    path.write_text(staged + "\nA genuinely revised requirement.\n")
    raw = await runner.redis.get(cause_key(runner.name, "PR-42"))
    revised = build_approval(runner.name, runner.state, raw, repo)
    assert revised.task.pr_id == old.task.pr_id and revised.task.branch == old.task.branch
    assert revised.task_fingerprint != old.task_fingerprint
    assert revised.binding != old.binding
    runner.state.active_invocation = InvocationSnapshot(
        plugin_id="codex", model_override=None, reasoning_effort_override=None,
        run_id="new-run", attempt_index=1, phase="coding",
    )
    first = build_approval(runner.name, runner.state, raw, repo)
    runner.state.active_invocation = replace(runner.state.active_invocation, attempt_index=2)
    second = build_approval(runner.name, runner.state, raw, repo)
    assert first.attempt != second.attempt and first.binding != second.binding
    assert first.task_fingerprint == second.task_fingerprint
    # Rejection must not be represented as DONE or weaken completion protection.
    path.write_text(text.replace("status: ERROR", "status: DONE"))
    _, preserved = preserve_terminal_status_on_collision(path, incoming)
    assert preserved == "DONE"
