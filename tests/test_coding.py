from __future__ import annotations

import asyncio
import subprocess
from pathlib import Path
from typing import Any

import pytest
from src.coders import claude as claude_plugin_module
from src.daemon import git_ops
from src.daemon.handlers import coding as coding_module
from src.github import gh_runner
from src.models import PipelineState, PRInfo, QueueTask, TaskStatus

from tests.runner import _helpers as h


def _runner(
    monkeypatch: pytest.MonkeyPatch,
    *,
    open_prs_after_create: list[PRInfo] | None = None,
    open_prs_initial: list[PRInfo] | None = None,
    raise_on_post_create_list: bool = False,
    post_create_empty_attempts: int = 0,
    post_create_failure_attempts: int = 0,
):
    h._patch_subprocess(monkeypatch)
    monkeypatch.setattr(h.claude_cli, "run_auto_pr_async", h._async_cli_result(0, "ok", ""))
    pr_list_calls = {"n": 0}

    def _fake_open_prs(repo: str, **kw: Any) -> list[PRInfo]:
        pr_list_calls["n"] += 1
        if pr_list_calls["n"] <= 3:
            return open_prs_initial or []
        if raise_on_post_create_list:
            raise RuntimeError("list-post-create boom")
        post_idx = pr_list_calls["n"] - 4
        if post_idx < post_create_failure_attempts:
            raise RuntimeError("transient list boom")
        if post_idx < post_create_failure_attempts + post_create_empty_attempts:
            return []
        return open_prs_after_create or []

    monkeypatch.setattr("src.github.prs.get_open_prs", _fake_open_prs)

    async def _sleep(_seconds: float) -> None:
        return None

    monkeypatch.setattr(h.runner_module.asyncio, "sleep", _sleep)
    runner = h._make_runner()
    runner.state.current_task = QueueTask(
        pr_id="PR-001",
        title="Sample task",
        status=TaskStatus.DOING,
        branch="pr-001",
        task_file="tasks/PR-001.md",
    )
    runner._post_codex_review = lambda pr_number: True  # type: ignore[method-assign]
    return runner


def _patch_branch_state(
    monkeypatch: pytest.MonkeyPatch,
    *,
    local_exists: bool,
    remote_exists: bool,
) -> None:
    def fake_git(repo_path: str, *args: str, **kwargs: Any):
        if args[:3] == ("rev-parse", "--verify", "--quiet"):
            rc = 0 if local_exists else 1
            return subprocess.CompletedProcess(args=list(args), returncode=rc, stdout="", stderr="")
        if args[:1] == ("ls-remote",):
            if remote_exists:
                return subprocess.CompletedProcess(
                    args=list(args),
                    returncode=0,
                    stdout="abcdef refs/heads/pr-001\n",
                    stderr="",
                )
            return subprocess.CompletedProcess(args=list(args), returncode=2, stdout="", stderr="")
        return subprocess.CompletedProcess(args=list(args), returncode=0, stdout="", stderr="")

    monkeypatch.setattr(git_ops, "_git", fake_git)


def test_happy_path_pr_exists_transitions_to_watch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pr = PRInfo(number=42, branch="pr-001")
    runner = _runner(monkeypatch, open_prs_initial=[pr])
    asyncio.run(runner.handle_coding())
    assert runner.state.state == PipelineState.WATCH
    assert runner.state.current_pr is not None
    assert runner.state.current_pr.number == 42


def test_case_a_no_branch_no_remote_marks_hung(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = _runner(monkeypatch)
    _patch_branch_state(monkeypatch, local_exists=False, remote_exists=False)
    asyncio.run(runner.handle_coding())
    assert runner.state.state == PipelineState.HUNG
    assert "did nothing" in (runner.state.error_message or "")
    assert any("did nothing" in entry["event"] for entry in runner.state.history)


def test_branch_mismatch_after_coder_exit_escalates_explicitly(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When ``BranchContext`` reports an explicit divergence between
    ``task_branch`` and the actual checked-out branch after the coder
    exits 0, ``handle_coding`` parks in HUNG with a [BRANCH] log
    instead of degrading into the case-A "did nothing" diagnostic.

    This is the OBS-AI gap PR-222 closes: surfacing the divergence
    before the PR lookup loop turns a silent HUNG into a named branch
    mismatch with both surfaces named in the diagnostic.
    """
    runner = _runner(monkeypatch)

    def fake_run(cmd: list[str], **_kwargs: Any) -> subprocess.CompletedProcess[str]:
        if cmd[:3] == ["git", "rev-parse", "--abbrev-ref"]:
            return subprocess.CompletedProcess(args=cmd, returncode=0, stdout="pr-001-typo\n", stderr="")
        return subprocess.CompletedProcess(args=cmd, returncode=0, stdout="", stderr="")

    monkeypatch.setattr("src.branch_context.subprocess.run", fake_run)

    asyncio.run(runner.handle_coding())

    assert runner.state.state == PipelineState.HUNG
    error = runner.state.error_message or ""
    assert "Branch mismatch" in error
    assert "task_branch=pr-001" in error
    assert "current_git_branch=pr-001-typo" in error
    log_entries = [entry["event"] for entry in runner.state.history]
    assert any("[BRANCH] mismatch detected" in e for e in log_entries)


def test_case_b_local_branch_only_marks_hung(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = _runner(monkeypatch)
    _patch_branch_state(monkeypatch, local_exists=True, remote_exists=False)
    asyncio.run(runner.handle_coding())
    assert runner.state.state == PipelineState.HUNG
    assert "no push" in (runner.state.error_message or "")


def test_case_c_remote_branch_no_pr_daemon_creates_pr(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    created = PRInfo(number=99, branch="pr-001")
    runner = _runner(monkeypatch, open_prs_after_create=[created])
    _patch_branch_state(monkeypatch, local_exists=True, remote_exists=True)

    create_calls: list[list[str]] = []

    def fake_run_gh(args: list[str], repo: str | None = None, **_kw: Any):
        create_calls.append(args)
        return ""

    monkeypatch.setattr("src.github.gh_runner.run_gh", fake_run_gh)

    asyncio.run(runner.handle_coding())

    assert runner.state.state == PipelineState.WATCH
    assert runner.state.current_pr is not None
    assert runner.state.current_pr.number == 99
    assert create_calls and create_calls[0][:2] == ["pr", "create"]
    assert "--head" in create_calls[0]
    assert "pr-001" in create_calls[0]
    assert any("daemon creating PR" in entry["event"] for entry in runner.state.history)


def test_case_c_branch_mismatch_does_not_block_daemon_recovery(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A coder that pushed ``task_branch`` (case C, recoverable) but
    exited on a different local branch must still reach daemon ``gh
    pr create`` recovery: ``--head task_branch`` operates on the
    upstream ref regardless of the local checkout, so a divergence
    between ``task_branch`` and ``current_git_branch`` here is not a
    HUNG signal — escalating would skip the existing recovery path
    and strand a recoverable run.
    """
    created = PRInfo(number=88, branch="pr-001")
    runner = _runner(monkeypatch, open_prs_after_create=[created])
    _patch_branch_state(monkeypatch, local_exists=True, remote_exists=True)

    def fake_run(cmd: list[str], **_kwargs: Any) -> subprocess.CompletedProcess[str]:
        if cmd[:3] == ["git", "rev-parse", "--abbrev-ref"]:
            return subprocess.CompletedProcess(args=cmd, returncode=0, stdout="main\n", stderr="")
        return subprocess.CompletedProcess(args=cmd, returncode=0, stdout="", stderr="")

    monkeypatch.setattr("src.branch_context.subprocess.run", fake_run)
    monkeypatch.setattr("src.github.gh_runner.run_gh", lambda *a, **kw: "")

    asyncio.run(runner.handle_coding())

    assert runner.state.state == PipelineState.WATCH
    assert runner.state.current_pr is not None
    assert runner.state.current_pr.number == 88
    log_entries = [entry["event"] for entry in runner.state.history]
    assert not any("[BRANCH] mismatch detected" in e for e in log_entries)


def test_case_c_create_pr_failure_marks_hung(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = _runner(monkeypatch)
    _patch_branch_state(monkeypatch, local_exists=True, remote_exists=True)

    def fake_run_gh(args: list[str], repo: str | None = None, **_kw: Any):
        raise RuntimeError("gh boom")

    monkeypatch.setattr("src.github.gh_runner.run_gh", fake_run_gh)

    asyncio.run(runner.handle_coding())

    assert runner.state.state == PipelineState.HUNG
    assert "Daemon PR creation failed" in (runner.state.error_message or "")


def test_case_c_post_create_list_failure_persists_marks_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When every post-create list attempt raises, the diagnostic must
    degrade to ERROR rather than HUNG. The PR was created by ``gh pr
    create`` and is likely present; ERROR lets the daemon retry the
    cycle, while HUNG would strand the task in manual-intervention
    state on a purely transient read outage."""
    runner = _runner(monkeypatch, raise_on_post_create_list=True)
    _patch_branch_state(monkeypatch, local_exists=True, remote_exists=True)
    monkeypatch.setattr("src.github.gh_runner.run_gh", lambda *a, **kw: "")

    asyncio.run(runner.handle_coding())

    assert runner.state.state == PipelineState.ERROR
    assert "list failed after 3 attempts" in (runner.state.error_message or "")


def test_case_c_post_create_list_transient_failure_recovers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A single transient ``get_open_prs`` failure after ``gh pr create``
    must not park the runner. The diagnostic retries on exception within
    the same bounded 3x/5s schedule it uses for empty-list eventual
    consistency, and proceeds to WATCH once the PR becomes visible."""
    created = PRInfo(number=77, branch="pr-001")
    runner = _runner(
        monkeypatch,
        open_prs_after_create=[created],
        post_create_failure_attempts=1,
    )
    _patch_branch_state(monkeypatch, local_exists=True, remote_exists=True)
    monkeypatch.setattr("src.github.gh_runner.run_gh", lambda *a, **kw: "")

    asyncio.run(runner.handle_coding())

    assert runner.state.state == PipelineState.WATCH
    assert runner.state.current_pr is not None
    assert runner.state.current_pr.number == 77
    assert any("Daemon-created PR list failed" in entry["event"] for entry in runner.state.history)


def test_case_c_pr_not_found_after_create_marks_hung(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = _runner(monkeypatch, open_prs_after_create=[])
    _patch_branch_state(monkeypatch, local_exists=True, remote_exists=True)
    monkeypatch.setattr("src.github.gh_runner.run_gh", lambda *a, **kw: "")

    asyncio.run(runner.handle_coding())

    assert runner.state.state == PipelineState.HUNG
    assert "Daemon-created PR not found" in (runner.state.error_message or "")


def test_case_c_post_create_eventual_consistency_succeeds(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The PR list endpoint is eventually consistent: a PR opened via
    daemon-side ``gh pr create`` may be absent from the first list
    response but present on a later retry. The diagnostic must not
    park the task as HUNG when the PR is simply not yet visible."""
    created = PRInfo(number=123, branch="pr-001")
    runner = _runner(
        monkeypatch,
        open_prs_after_create=[created],
        post_create_empty_attempts=2,
    )
    _patch_branch_state(monkeypatch, local_exists=True, remote_exists=True)
    monkeypatch.setattr("src.github.gh_runner.run_gh", lambda *a, **kw: "")

    asyncio.run(runner.handle_coding())

    assert runner.state.state == PipelineState.WATCH
    assert runner.state.current_pr is not None
    assert runner.state.current_pr.number == 123
    assert any("Daemon-created PR not visible yet" in entry["event"] for entry in runner.state.history)


def test_local_branch_exists_handles_subprocess_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_git(*args: Any, **kwargs: Any):
        raise OSError("git not found")

    monkeypatch.setattr(git_ops, "_git", fake_git)
    assert coding_module._local_branch_exists("/tmp/nope", "any") is False


def test_remote_branch_exists_handles_subprocess_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_git(*args: Any, **kwargs: Any):
        raise subprocess.TimeoutExpired(cmd=["git"], timeout=30)

    monkeypatch.setattr(git_ops, "_git", fake_git)
    assert coding_module._remote_branch_exists("/tmp/nope", "any") is False


def test_remote_branch_exists_returns_false_on_empty_stdout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_git(*args: Any, **kwargs: Any):
        return subprocess.CompletedProcess(args=["git"], returncode=0, stdout="", stderr="")

    monkeypatch.setattr(git_ops, "_git", fake_git)
    assert coding_module._remote_branch_exists("/tmp/nope", "any") is False


def test_daemon_create_pr_uses_pr_id_when_title_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    created = PRInfo(number=7, branch="pr-001")
    runner = _runner(monkeypatch, open_prs_after_create=[created])
    runner.state.current_task = QueueTask(
        pr_id="PR-001",
        title="",
        status=TaskStatus.DOING,
        branch="pr-001",
    )
    _patch_branch_state(monkeypatch, local_exists=True, remote_exists=True)

    captured: list[list[str]] = []

    def fake_run_gh(args: list[str], repo: str | None = None, **_kw: Any):
        captured.append(args)
        return ""

    monkeypatch.setattr("src.github.gh_runner.run_gh", fake_run_gh)

    asyncio.run(runner.handle_coding())
    assert runner.state.state == PipelineState.WATCH
    create_args = next(args for args in captured if args[:2] == ["pr", "create"])
    title_idx = create_args.index("--title") + 1
    assert create_args[title_idx] == "PR-001"
    body_idx = create_args.index("--body") + 1
    assert "with no PR" in create_args[body_idx]


def test_case_c_already_exists_error_recovers_to_watch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``gh pr create`` returns non-zero when a PR for the same head branch
    already exists (e.g. PR visibility lagged the earlier list). The
    diagnostic must treat that as recoverable and hand off to WATCH using
    the existing PR rather than parking the runner as HUNG."""
    existing = PRInfo(number=314, branch="pr-001")
    runner = _runner(monkeypatch, open_prs_after_create=[existing])
    _patch_branch_state(monkeypatch, local_exists=True, remote_exists=True)

    def fake_run_gh(args: list[str], repo: str | None = None, **_kw: Any):
        raise RuntimeError(
            "gh pr create failed (exit 1): a pull request for branch "
            '"pr-001" into branch "main" already exists: '
            "https://github.com/octo/demo/pull/314"
        )

    monkeypatch.setattr("src.github.gh_runner.run_gh", fake_run_gh)

    asyncio.run(runner.handle_coding())

    assert runner.state.state == PipelineState.WATCH
    assert runner.state.current_pr is not None
    assert runner.state.current_pr.number == 314
    assert any("already exists" in entry["event"].lower() for entry in runner.state.history)


def test_case_c_already_exists_error_falls_through_when_pr_invisible(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When ``gh`` reports the PR exists but the post-create list never
    surfaces it, the diagnostic should still escalate after the bounded
    retry window — the recovery path defers to the existing 'PR not found'
    HUNG branch rather than silently swallowing the error."""
    runner = _runner(monkeypatch, open_prs_after_create=[])
    _patch_branch_state(monkeypatch, local_exists=True, remote_exists=True)

    def fake_run_gh(args: list[str], repo: str | None = None, **_kw: Any):
        raise RuntimeError("already exists: https://example/pull/1")

    monkeypatch.setattr("src.github.gh_runner.run_gh", fake_run_gh)

    asyncio.run(runner.handle_coding())

    assert runner.state.state == PipelineState.HUNG
    assert "Daemon-created PR not found" in (runner.state.error_message or "")


def test_diagnose_honors_stop_request_during_remote_probe_for_case_a(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``_remote_branch_exists`` blocks up to 30s; a stop pressed during
    that window must route to PAUSED rather than the A/B HUNG branch.
    Previously the diagnostic called ``pause_for_stop_if_requested`` only
    on the C path, so a user stop coinciding with cases A/B was silently
    swallowed and the task was parked as HUNG."""
    runner = _runner(monkeypatch)
    _patch_branch_state(monkeypatch, local_exists=False, remote_exists=False)

    pop_calls = {"n": 0}

    async def fake_pop_stop_request() -> bool:
        pop_calls["n"] += 1
        # Calls 1-6 belong to handle_coding (CLI exit + PR-visibility
        # retry loop). Call 7 is the diagnostic's new pre-decision pause
        # check covering the A/B/C fork; tripping it must route to
        # PAUSED before HUNG is recorded.
        return pop_calls["n"] == 7

    monkeypatch.setattr(runner, "_pop_stop_request", fake_pop_stop_request)

    asyncio.run(runner.handle_coding())

    assert runner.state.state == PipelineState.PAUSED
    assert runner.state.current_pr is None
    assert "did nothing" not in (runner.state.error_message or "")


def test_diagnose_honors_stop_request_before_pr_creation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A user stop pressed after coder exit but before the daemon creates
    its own PR must be honored: the runner pauses without invoking
    ``gh pr create`` and without transitioning to WATCH."""
    runner = _runner(monkeypatch)
    _patch_branch_state(monkeypatch, local_exists=True, remote_exists=True)

    pop_calls = {"n": 0}

    async def fake_pop_stop_request() -> bool:
        pop_calls["n"] += 1
        # Calls 1-6 cover the pause checks in handle_coding (around CLI
        # exit and the PR-visibility retry loop). Call 7 is the new check
        # the diagnostic performs immediately before daemon-side PR
        # creation; return True there to simulate a stop pressed during
        # that window.
        return pop_calls["n"] == 7

    monkeypatch.setattr(runner, "_pop_stop_request", fake_pop_stop_request)

    create_calls: list[list[str]] = []

    def fake_run_gh(args: list[str], repo: str | None = None, **_kw: Any):
        create_calls.append(args)
        return ""

    monkeypatch.setattr("src.github.gh_runner.run_gh", fake_run_gh)

    asyncio.run(runner.handle_coding())

    assert runner.state.state == PipelineState.PAUSED
    assert runner.state.current_pr is None
    assert create_calls == []


def test_diagnose_honors_stop_request_during_post_create_retry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A user stop pressed after the daemon called ``gh pr create`` but
    while still polling for PR visibility must short-circuit the retry
    loop and pause the runner instead of completing the WATCH handoff."""
    created = PRInfo(number=42, branch="pr-001")
    runner = _runner(
        monkeypatch,
        open_prs_after_create=[created],
        post_create_empty_attempts=2,
    )
    _patch_branch_state(monkeypatch, local_exists=True, remote_exists=True)

    pop_calls = {"n": 0}

    async def fake_pop_stop_request() -> bool:
        pop_calls["n"] += 1
        # Allow the pre-create pause check (call 7) to pass so PR creation
        # actually runs, then trip the second retry-loop pause (call 9)
        # to simulate a stop pressed mid-retry. Calls 1-6 belong to
        # handle_coding's earlier pause checks.
        return pop_calls["n"] == 9

    monkeypatch.setattr(runner, "_pop_stop_request", fake_pop_stop_request)
    monkeypatch.setattr("src.github.gh_runner.run_gh", lambda *a, **kw: "")

    asyncio.run(runner.handle_coding())

    assert runner.state.state == PipelineState.PAUSED
    assert runner.state.current_pr is None


def test_diagnose_honors_stop_request_after_post_create_loop(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A stop pressed after the post-create retry loop has located the PR
    but before the WATCH transition must still pause the runner — the
    daemon should not complete the WATCH handoff once an explicit stop is
    pending."""
    created = PRInfo(number=51, branch="pr-001")
    runner = _runner(monkeypatch, open_prs_after_create=[created])
    _patch_branch_state(monkeypatch, local_exists=True, remote_exists=True)

    pop_calls = {"n": 0}

    async def fake_pop_stop_request() -> bool:
        pop_calls["n"] += 1
        # Calls 1-6: handle_coding pauses (CLI exit + pre-list retries +
        # final post-loop check). Call 7: pre-create pause inside the
        # diagnostic. Call 8: pause before the first post-create list
        # (which finds the PR immediately). Call 9: the new post-loop
        # pause check that gates the WATCH transition.
        return pop_calls["n"] == 9

    monkeypatch.setattr(runner, "_pop_stop_request", fake_pop_stop_request)
    monkeypatch.setattr("src.github.gh_runner.run_gh", lambda *a, **kw: "")

    asyncio.run(runner.handle_coding())

    assert runner.state.state == PipelineState.PAUSED
    assert runner.state.current_pr is None


def _patch_codex_reactions(
    monkeypatch: pytest.MonkeyPatch,
    *,
    eyes_present: bool,
    eyes_stale: bool = False,
) -> None:
    """Stub ``_get_codex_issue_reactions`` for the EYES-skip pre-push gate.

    ``eyes_stale=True`` returns an EYES reaction whose ``created_at``
    predates the last push time, exercising the push-time freshness
    gate that prevents stale reactions from suppressing the review on
    a brand-new push.
    """
    last_push_iso = "2026-04-30T12:00:00Z"
    reaction_iso = "2026-04-30T11:00:00Z" if eyes_stale else "2026-04-30T12:30:00Z"
    payload = (
        [
            {
                "content": "eyes",
                "user": {"login": "chatgpt-codex-connector[bot]"},
                "created_at": reaction_iso,
            }
        ]
        if eyes_present
        else []
    )
    monkeypatch.setattr(
        "src.github.reactions._get_codex_issue_reactions",
        lambda repo, number: payload,
    )
    monkeypatch.setattr(
        "src.github.prs.get_pr_last_push_time",
        lambda repo, number: gh_runner._parse_iso(last_push_iso),
    )


def test_handle_coding_skips_codex_review_when_eyes_already_reacted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """OBS-Z: Codex auto-trigger already landed → skip duplicate mention."""
    pr = PRInfo(number=42, branch="pr-001")
    runner = _runner(monkeypatch, open_prs_initial=[pr])
    _patch_codex_reactions(monkeypatch, eyes_present=True)
    posted: list[int] = []
    runner._post_codex_review = lambda pr_number: (  # type: ignore[method-assign]
        posted.append(pr_number) or True
    )

    asyncio.run(runner.handle_coding())

    assert runner.state.state == PipelineState.WATCH
    assert posted == []
    assert any(
        "Codex auto-trigger detected, skipping duplicate @codex review post" in entry["event"]
        for entry in runner.state.history
    )


def test_handle_coding_posts_codex_review_when_no_eyes_reaction(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pr = PRInfo(number=42, branch="pr-001")
    runner = _runner(monkeypatch, open_prs_initial=[pr])
    _patch_codex_reactions(monkeypatch, eyes_present=False)
    posted: list[int] = []
    runner._post_codex_review = lambda pr_number: (  # type: ignore[method-assign]
        posted.append(pr_number) or True
    )

    asyncio.run(runner.handle_coding())

    assert runner.state.state == PipelineState.WATCH
    assert posted == [42]


def test_should_skip_codex_review_post_fails_open_on_api_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A GitHub API failure must not suppress the mention — fail open."""
    runner = h._make_runner()

    def boom(*_a: Any, **_kw: Any) -> list[dict]:
        raise RuntimeError("api boom")

    monkeypatch.setattr("src.github.reactions._get_codex_issue_reactions", boom)
    assert runner._should_skip_codex_review_post(42) is False


def test_should_skip_codex_review_post_fails_open_on_push_time_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Unresolvable last-push time must not suppress the mention."""
    runner = h._make_runner()

    monkeypatch.setattr(
        "src.github.reactions._get_codex_issue_reactions",
        lambda repo, number: [
            {
                "content": "eyes",
                "user": {"login": "chatgpt-codex-connector[bot]"},
                "created_at": "2026-04-30T12:30:00Z",
            }
        ],
    )

    def boom(*_a: Any, **_kw: Any) -> Any:
        raise RuntimeError("push-time boom")

    monkeypatch.setattr("src.github.prs.get_pr_last_push_time", boom)
    assert runner._should_skip_codex_review_post(42) is False


def test_should_skip_codex_review_post_fails_open_on_missing_push_time(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A ``None`` last-push time (activity API degraded) must fail open."""
    runner = h._make_runner()
    monkeypatch.setattr(
        "src.github.reactions._get_codex_issue_reactions",
        lambda repo, number: [
            {
                "content": "eyes",
                "user": {"login": "chatgpt-codex-connector[bot]"},
                "created_at": "2026-04-30T12:30:00Z",
            }
        ],
    )
    monkeypatch.setattr(
        "src.github.prs.get_pr_last_push_time",
        lambda repo, number: None,
    )
    assert runner._should_skip_codex_review_post(42) is False


def test_should_skip_codex_review_post_skips_when_eyes_after_push(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A fresh EYES reaction (after the last push) suppresses the mention."""
    runner = h._make_runner()
    monkeypatch.setattr(
        "src.github.reactions._get_codex_issue_reactions",
        lambda repo, number: [
            {
                "content": "eyes",
                "user": {"login": "chatgpt-codex-connector[bot]"},
                "created_at": "2026-04-30T12:30:00Z",
            }
        ],
    )
    monkeypatch.setattr(
        "src.github.prs.get_pr_last_push_time",
        lambda repo, number: gh_runner._parse_iso("2026-04-30T12:00:00Z"),
    )
    assert runner._should_skip_codex_review_post(42) is True


def test_should_skip_codex_review_post_does_not_skip_when_eyes_predates_push(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A stale EYES reaction must not suppress review after a new push."""
    runner = h._make_runner()
    monkeypatch.setattr(
        "src.github.reactions._get_codex_issue_reactions",
        lambda repo, number: [
            {
                "content": "eyes",
                "user": {"login": "chatgpt-codex-connector[bot]"},
                "created_at": "2026-04-30T11:00:00Z",
            }
        ],
    )
    monkeypatch.setattr(
        "src.github.prs.get_pr_last_push_time",
        lambda repo, number: gh_runner._parse_iso("2026-04-30T12:00:00Z"),
    )
    assert runner._should_skip_codex_review_post(42) is False


def test_should_skip_codex_review_post_does_not_skip_on_backdated_head_commit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An EYES reaction older than push time must not suppress the mention,
    even when the head commit's committer date is older still.

    A cherry-picked or amended commit can carry a committer date that
    predates the actual push that put it on the branch. The earlier
    head-commit-date gating treated such a stale EYES as fresh; the
    push-time gate must reject it.
    """
    runner = h._make_runner()
    monkeypatch.setattr(
        "src.github.reactions._get_codex_issue_reactions",
        lambda repo, number: [
            {
                "content": "eyes",
                "user": {"login": "chatgpt-codex-connector[bot]"},
                "created_at": "2026-04-30T11:30:00Z",
            }
        ],
    )
    monkeypatch.setattr(
        "src.github.prs.get_pr_last_push_time",
        lambda repo, number: gh_runner._parse_iso("2026-04-30T12:00:00Z"),
    )
    assert runner._should_skip_codex_review_post(42) is False


def test_should_skip_codex_review_post_normalizes_naive_timestamps(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Naive timestamps from upstream parsers are coerced to UTC so the
    comparison still fires correctly."""
    from datetime import datetime as _dt

    runner = h._make_runner()
    monkeypatch.setattr(
        "src.github.reactions._get_codex_issue_reactions",
        lambda repo, number: [
            {
                "content": "eyes",
                "user": {"login": "chatgpt-codex-connector[bot]"},
                "created_at": "2026-04-30T12:30:00",
            }
        ],
    )
    monkeypatch.setattr(
        "src.github.prs.get_pr_last_push_time",
        lambda repo, number: _dt(2026, 4, 30, 12, 0, 0),
    )
    monkeypatch.setattr(
        "src.github.gh_runner._parse_iso",
        lambda value: _dt(2026, 4, 30, 12, 30, 0) if value else None,
    )

    assert runner._should_skip_codex_review_post(42) is True


def test_should_skip_codex_review_post_ignores_eyes_without_created_at(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Reaction missing ``created_at`` cannot prove freshness; do not skip."""
    runner = h._make_runner()
    monkeypatch.setattr(
        "src.github.reactions._get_codex_issue_reactions",
        lambda repo, number: [
            {
                "content": "eyes",
                "user": {"login": "chatgpt-codex-connector[bot]"},
            }
        ],
    )
    monkeypatch.setattr(
        "src.github.prs.get_pr_last_push_time",
        lambda repo, number: gh_runner._parse_iso("2026-04-30T12:00:00Z"),
    )
    assert runner._should_skip_codex_review_post(42) is False


def test_diagnose_case_c_skips_codex_review_when_eyes_already_reacted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Daemon-created PR (case C) also honors the EYES race-window dedup."""
    created = PRInfo(number=99, branch="pr-001")
    runner = _runner(monkeypatch, open_prs_after_create=[created])
    _patch_branch_state(monkeypatch, local_exists=True, remote_exists=True)
    _patch_codex_reactions(monkeypatch, eyes_present=True)
    monkeypatch.setattr("src.github.gh_runner.run_gh", lambda *a, **kw: "")
    posted: list[int] = []
    runner._post_codex_review = lambda pr_number: (  # type: ignore[method-assign]
        posted.append(pr_number) or True
    )

    asyncio.run(runner.handle_coding())

    assert runner.state.state == PipelineState.WATCH
    assert posted == []
    assert any(
        "Codex auto-trigger detected, skipping duplicate @codex review post" in entry["event"]
        for entry in runner.state.history
    )


# ---------------------------------------------------------------------------
# PR-271: AUTO PR dispatch tests.
#
# The CODING handler now invokes ``plugin.run_auto_pr`` with explicit
# ``pr_id``, ``task_file``, and ``task_body`` arguments instead of the
# legacy ``plugin.run_planned_pr`` indirection that left the coder to
# discover its task via QUEUE.md. These tests pin the new contract.
# ---------------------------------------------------------------------------


def _runner_for_auto_pr_dispatch(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    *,
    pr_id: str = "PR-001",
    task_body: str = "# PR-001\n\nBranch: pr-001\n",
    write_task_file: bool = True,
):
    """Build a runner whose ``repo_path`` is a real on-disk tmp tree.

    Unlike ``_runner``, this helper does not stub ``Path.read_text`` so the
    AUTO PR file read path is exercised end-to-end against the actual
    filesystem. Used by tests that assert what the dispatch reads from disk.
    """
    h._patch_subprocess(monkeypatch, stub_auto_pr_read=False)
    monkeypatch.setattr(
        h.claude_cli, "run_auto_pr_async", h._async_cli_result(0, "ok", "")
    )

    pr = PRInfo(number=42, branch="pr-001")
    monkeypatch.setattr(
        "src.github.prs.get_open_prs", lambda repo, **kw: [pr]
    )

    async def _sleep(_seconds: float) -> None:
        return None

    monkeypatch.setattr(h.runner_module.asyncio, "sleep", _sleep)

    runner = h._make_runner()
    runner.repo_path = str(tmp_path)
    if write_task_file:
        (tmp_path / "tasks").mkdir(parents=True, exist_ok=True)
        (tmp_path / "tasks" / f"{pr_id}.md").write_text(
            task_body, encoding="utf-8"
        )
    runner.state.current_task = QueueTask(
        pr_id=pr_id,
        title="Sample task",
        status=TaskStatus.DOING,
        branch="pr-001",
        task_file=f"tasks/{pr_id}.md",
    )
    runner._post_codex_review = lambda pr_number: True  # type: ignore[method-assign]
    return runner


def test_handle_coding_invokes_run_auto_pr_with_pr_id_and_task_file(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """``plugin.run_auto_pr`` is invoked with the canonical ``pr_id`` and
    the ``tasks/{pr_id}.md`` task_file label derived from the active task."""
    runner = _runner_for_auto_pr_dispatch(
        monkeypatch, tmp_path, pr_id="PR-XYZ"
    )
    captured: dict[str, Any] = {}

    async def fake_run_auto_pr(
        repo_path: str, **kwargs: Any
    ) -> tuple[int, str, str]:
        captured["repo_path"] = repo_path
        captured["kwargs"] = dict(kwargs)
        return (0, "ok", "")

    coder_name, plugin = runner._get_coder()
    monkeypatch.setattr(plugin, "run_auto_pr", fake_run_auto_pr)

    asyncio.run(runner.handle_coding())

    assert captured["repo_path"] == runner.repo_path
    assert captured["kwargs"]["pr_id"] == "PR-XYZ"
    assert captured["kwargs"]["task_file"] == "tasks/PR-XYZ.md"


def test_handle_coding_passes_task_body_inline(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """The ``task_body`` kwarg passed to ``run_auto_pr`` equals the contents
    of ``repo_path/tasks/{pr_id}.md`` read at dispatch time."""
    body = (
        "# PR-XYZ: example\n\n"
        "Branch: pr-xyz-example\n"
        "- Type: feature\n\n"
        "## Scope\n\n"
        "Inline task body should be passed verbatim.\n"
    )
    runner = _runner_for_auto_pr_dispatch(
        monkeypatch, tmp_path, pr_id="PR-XYZ", task_body=body
    )
    captured: dict[str, Any] = {}

    async def fake_run_auto_pr(
        repo_path: str, **kwargs: Any
    ) -> tuple[int, str, str]:
        captured["task_body"] = kwargs["task_body"]
        return (0, "ok", "")

    _, plugin = runner._get_coder()
    monkeypatch.setattr(plugin, "run_auto_pr", fake_run_auto_pr)

    asyncio.run(runner.handle_coding())

    assert captured["task_body"] == body


def test_coding_dispatch_cross_repo_intent_transitions_to_error(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    runner = _runner_for_auto_pr_dispatch(
        monkeypatch,
        tmp_path,
        task_body="# PR-001\n\nBranch: pr-001\n\nThis PR ships in some-other-repo.\n",
    )
    _, plugin = runner._get_coder()
    run_auto_pr_calls: list[dict[str, Any]] = []

    async def fake_run_auto_pr(
        repo_path: str, **kwargs: Any
    ) -> tuple[int, str, str]:
        run_auto_pr_calls.append(dict(kwargs))
        return (0, "ok", "")

    monkeypatch.setattr(plugin, "run_auto_pr", fake_run_auto_pr)
    transition_calls: list[str] = []
    original_transition = runner._transition_to_error

    async def capture_transition(message: str, **kwargs: Any) -> None:
        transition_calls.append(message)
        await original_transition(message, **kwargs)

    monkeypatch.setattr(runner, "_transition_to_error", capture_transition)

    asyncio.run(runner.handle_coding())

    assert transition_calls
    assert transition_calls[0].startswith("CROSS_REPO_INTENT:")
    assert run_auto_pr_calls == []
    assert runner.state.state == PipelineState.ERROR
    assert (runner.state.error_message or "").startswith("CROSS_REPO_INTENT:")


def test_coding_dispatch_negated_intent_proceeds(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    runner = _runner_for_auto_pr_dispatch(
        monkeypatch,
        tmp_path,
        task_body=(
            "# PR-001\n\nBranch: pr-001\n\n"
            "This is NOT in some-other-repo repo; it is for this repo.\n"
        ),
    )
    _, plugin = runner._get_coder()
    run_auto_pr_calls: list[dict[str, Any]] = []

    async def fake_run_auto_pr(
        repo_path: str, **kwargs: Any
    ) -> tuple[int, str, str]:
        run_auto_pr_calls.append(dict(kwargs))
        return (0, "ok", "")

    monkeypatch.setattr(plugin, "run_auto_pr", fake_run_auto_pr)

    asyncio.run(runner.handle_coding())

    assert run_auto_pr_calls
    assert runner.state.state == PipelineState.WATCH


def test_handle_coding_does_not_invoke_run_planned_pr(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """The legacy ``run_planned_pr`` dispatch path is no longer reached.

    Pin the daemon's switch to AUTO PR by failing if the plugin's
    ``run_planned_pr`` (or the underlying ``run_planned_pr_async``) is
    called from ``handle_coding``. Manual VS Code workflows still call
    these methods, but the daemon must not.
    """
    runner = _runner_for_auto_pr_dispatch(monkeypatch, tmp_path)

    async def boom_run_planned_pr(*args: Any, **kwargs: Any) -> tuple[int, str, str]:
        raise AssertionError("plugin.run_planned_pr must not be called by daemon dispatch")

    async def boom_run_planned_pr_async(
        *args: Any, **kwargs: Any
    ) -> tuple[int, str, str]:
        raise AssertionError(
            "claude_cli.run_planned_pr_async must not be called by daemon dispatch"
        )

    _, plugin = runner._get_coder()
    monkeypatch.setattr(plugin, "run_planned_pr", boom_run_planned_pr)
    monkeypatch.setattr(
        claude_plugin_module.claude_cli,
        "run_planned_pr_async",
        boom_run_planned_pr_async,
    )

    asyncio.run(runner.handle_coding())

    assert runner.state.state == PipelineState.WATCH


def test_handle_coding_missing_task_file_transitions_to_error(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """A missing ``tasks/{pr_id}.md`` file must surface as a clean ERROR
    transition with a descriptive message instead of silently feeding an
    empty body to the coder."""
    runner = _runner_for_auto_pr_dispatch(
        monkeypatch, tmp_path, pr_id="PR-404", write_task_file=False
    )
    asyncio.run(runner.handle_coding())

    assert runner.state.state == PipelineState.ERROR
    assert "tasks/PR-404.md" in (runner.state.error_message or "")
    assert "Cannot read task file" in (runner.state.error_message or "")


def test_handle_coding_task_file_read_error_transitions_to_error(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """An OSError raised while reading the task file (e.g. permission
    denied, broken symlink) must route through the same ERROR transition
    as a missing file. No exception leaks out of the handler."""
    runner = _runner_for_auto_pr_dispatch(monkeypatch, tmp_path)

    def fake_read_text(self: Path, *args: Any, **kwargs: Any) -> str:
        raise OSError("permission denied")

    monkeypatch.setattr(coding_module.Path, "read_text", fake_read_text)

    asyncio.run(runner.handle_coding())

    assert runner.state.state == PipelineState.ERROR
    assert "permission denied" in (runner.state.error_message or "")
    assert "Cannot read task file" in (runner.state.error_message or "")


def test_handle_coding_uses_queue_task_file_path(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """When ``current_task.task_file`` points at a non-default location,
    the AUTO PR dispatch reads from that path instead of hardcoding
    ``tasks/{pr_id}.md``. Otherwise CODING fails on queue entries the
    parser legitimately registered with a non-default path."""
    pr_id = "PR-XYZ"
    body = "# PR-XYZ\n\nBranch: pr-xyz\n"
    custom_dir = tmp_path / "subdir" / "tasks"
    custom_dir.mkdir(parents=True)
    custom_path = custom_dir / "PR-XYZ.md"
    custom_path.write_text(body, encoding="utf-8")

    runner = _runner_for_auto_pr_dispatch(
        monkeypatch, tmp_path, pr_id=pr_id, write_task_file=False
    )
    runner.state.current_task = QueueTask(
        pr_id=pr_id,
        title="Sample task",
        status=TaskStatus.DOING,
        branch="pr-001",
        task_file="subdir/tasks/PR-XYZ.md",
    )
    runner._post_codex_review = lambda pr_number: True  # type: ignore[method-assign]

    captured: dict[str, Any] = {}

    async def fake_run_auto_pr(
        repo_path: str, **kwargs: Any
    ) -> tuple[int, str, str]:
        captured["kwargs"] = dict(kwargs)
        return (0, "ok", "")

    _, plugin = runner._get_coder()
    monkeypatch.setattr(plugin, "run_auto_pr", fake_run_auto_pr)

    asyncio.run(runner.handle_coding())

    assert captured["kwargs"]["task_file"] == "subdir/tasks/PR-XYZ.md"
    assert captured["kwargs"]["task_body"] == body


def test_handle_coding_falls_back_to_default_path_when_task_file_missing(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """A legacy queue entry without ``task_file`` set falls back to the
    conventional ``tasks/{pr_id}.md`` path so existing dispatch behavior
    is preserved for unmigrated queues."""
    pr_id = "PR-LEGACY"
    runner = _runner_for_auto_pr_dispatch(
        monkeypatch, tmp_path, pr_id=pr_id, task_body="# legacy body\n"
    )
    runner.state.current_task = QueueTask(
        pr_id=pr_id,
        title="Legacy task",
        status=TaskStatus.DOING,
        branch="pr-001",
        task_file=None,
    )
    runner._post_codex_review = lambda pr_number: True  # type: ignore[method-assign]

    captured: dict[str, Any] = {}

    async def fake_run_auto_pr(
        repo_path: str, **kwargs: Any
    ) -> tuple[int, str, str]:
        captured["kwargs"] = dict(kwargs)
        return (0, "ok", "")

    _, plugin = runner._get_coder()
    monkeypatch.setattr(plugin, "run_auto_pr", fake_run_auto_pr)

    asyncio.run(runner.handle_coding())

    assert captured["kwargs"]["task_file"] == f"tasks/{pr_id}.md"
    assert captured["kwargs"]["task_body"] == "# legacy body\n"


def _runner_for_path_validation(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    *,
    task_file: str,
):
    """Build a runner whose ``current_task.task_file`` is operator-controlled.

    Used by P1 path-validation tests to assert that an absolute path or a
    ``..`` traversal in the queue entry is rejected before the task body is
    read, instead of leaking arbitrary host content into ``run_auto_pr``.
    """
    h._patch_subprocess(monkeypatch, stub_auto_pr_read=False)

    captured: dict[str, Any] = {}

    async def fake_run_auto_pr(
        repo_path: str, **kwargs: Any
    ) -> tuple[int, str, str]:
        captured["kwargs"] = dict(kwargs)
        return (0, "ok", "")

    runner = h._make_runner()
    runner.repo_path = str(tmp_path)
    runner.state.current_task = QueueTask(
        pr_id="PR-EVIL",
        title="Evil task",
        status=TaskStatus.DOING,
        branch="pr-evil",
        task_file=task_file,
    )
    runner._post_codex_review = lambda pr_number: True  # type: ignore[method-assign]
    _, plugin = runner._get_coder()
    monkeypatch.setattr(plugin, "run_auto_pr", fake_run_auto_pr)
    return runner, captured


def test_handle_coding_rejects_absolute_task_file_path(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """An absolute ``task_file`` from a queue entry must be rejected so a
    malicious or mistaken row cannot make CODING read arbitrary host files
    and inline their contents into ``run_auto_pr``."""
    secret = tmp_path / "outside_secret.md"
    secret.write_text("HOST SECRET\n", encoding="utf-8")
    runner, captured = _runner_for_path_validation(
        monkeypatch, tmp_path, task_file=str(secret)
    )

    asyncio.run(runner.handle_coding())

    assert runner.state.state == PipelineState.ERROR
    assert "Invalid task file" in (runner.state.error_message or "")
    assert "absolute" in (runner.state.error_message or "")
    assert "kwargs" not in captured, (
        "run_auto_pr must not be invoked when validation fails"
    )


def test_handle_coding_rejects_dotdot_task_file_path(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """A queue entry with ``..`` segments must be rejected before the
    daemon resolves the path, even if the resolved location happens to land
    back inside the repo (defense-in-depth)."""
    runner, captured = _runner_for_path_validation(
        monkeypatch, tmp_path, task_file="tasks/../../etc/passwd"
    )

    asyncio.run(runner.handle_coding())

    assert runner.state.state == PipelineState.ERROR
    assert "Invalid task file" in (runner.state.error_message or "")
    assert "traversal" in (runner.state.error_message or "")
    assert "kwargs" not in captured


def test_handle_coding_rejects_symlink_escape_task_file(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """A symlink inside ``tasks/`` that points outside the repo root must
    be rejected so a planted symlink cannot smuggle external content past
    the absolute/``..`` checks."""
    repo = tmp_path / "repo"
    (repo / "tasks").mkdir(parents=True)
    outside = tmp_path / "outside.md"
    outside.write_text("OUTSIDE CONTENT\n", encoding="utf-8")
    (repo / "tasks" / "PR-EVIL.md").symlink_to(outside)

    runner, captured = _runner_for_path_validation(
        monkeypatch, repo, task_file="tasks/PR-EVIL.md"
    )

    asyncio.run(runner.handle_coding())

    assert runner.state.state == PipelineState.ERROR
    assert "Invalid task file" in (runner.state.error_message or "")
    assert "escapes repo root" in (runner.state.error_message or "")
    assert "kwargs" not in captured


def test_handle_coding_rejects_symlink_loop_task_file(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """A symlink loop under ``tasks/`` must fail closed through
    ``_transition_to_error``: ``Path.resolve()`` raises ``RuntimeError``
    on a self-referential or cyclic symlink chain, but the handler only
    catches ``ValueError`` here. Without normalizing resolver
    exceptions, the ``RuntimeError`` would escape ``handle_coding`` and
    crash ``run_cycle`` instead of producing a controlled ERROR
    transition with a user-facing message.
    """
    repo = tmp_path / "repo"
    (repo / "tasks").mkdir(parents=True)
    loop_a = repo / "tasks" / "PR-LOOP.md"
    loop_b = repo / "tasks" / "PR-LOOP-other.md"
    loop_a.symlink_to(loop_b)
    loop_b.symlink_to(loop_a)

    runner, captured = _runner_for_path_validation(
        monkeypatch, repo, task_file="tasks/PR-LOOP.md"
    )

    asyncio.run(runner.handle_coding())

    assert runner.state.state == PipelineState.ERROR
    assert "Invalid task file" in (runner.state.error_message or "")
    assert "cannot resolve" in (runner.state.error_message or "")
    assert "kwargs" not in captured


def test_handle_coding_handles_non_utf8_task_body(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """A ``tasks/{pr_id}.md`` written in a non-UTF-8 encoding must fail
    closed through ``_transition_to_error``. ``read_text(encoding="utf-8")``
    raises ``UnicodeDecodeError`` (a ``ValueError`` subclass) on byte
    sequences that are not valid UTF-8, so the handler must treat decode
    failures like other read failures instead of letting the exception
    escape and crash the cycle.
    """
    repo = tmp_path / "repo"
    (repo / "tasks").mkdir(parents=True)
    # Latin-1 byte 0xff is invalid as the leading byte of a UTF-8 sequence.
    (repo / "tasks" / "PR-BAD.md").write_bytes(b"\xff invalid utf-8 \xfe\n")

    runner, captured = _runner_for_path_validation(
        monkeypatch, repo, task_file="tasks/PR-BAD.md"
    )

    asyncio.run(runner.handle_coding())

    assert runner.state.state == PipelineState.ERROR
    assert "Cannot read task file" in (runner.state.error_message or "")
    assert "kwargs" not in captured


# --- PR-272: pre-push hook expected-branch marker ---------------------------


def _runner_with_repo_path(
    monkeypatch: pytest.MonkeyPatch,
    repo_path: Path,
    *,
    open_prs_initial: list[PRInfo] | None = None,
):
    """Build a coding-handler runner whose repo_path is a real tmp dir.

    Used by the PR-272 expected-branch tests to verify the daemon writes
    and removes ``.git/info/expected-branch`` on the real filesystem.
    Mirrors ``_runner`` but lets the caller place ``.git/info/`` under a
    tmp_path so write/cleanup is observable.
    """
    h._patch_subprocess(monkeypatch)
    monkeypatch.setattr("src.github.prs.get_open_prs", lambda repo, **kw: open_prs_initial or [])

    async def _sleep(_seconds: float) -> None:
        return None

    monkeypatch.setattr(h.runner_module.asyncio, "sleep", _sleep)
    runner = h._make_runner()
    runner.repo_path = str(repo_path)
    runner.state.current_task = QueueTask(
        pr_id="PR-001",
        title="Sample task",
        status=TaskStatus.DOING,
        branch="pr-001",
        task_file="tasks/PR-001.md",
    )
    runner._post_codex_review = lambda pr_number: True  # type: ignore[method-assign]
    return runner


def test_coding_writes_expected_branch_before_dispatch(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """The daemon must write the active task's branch to
    ``.git/info/expected-branch`` before invoking ``plugin.run_auto_pr``
    so the pre-push hook can validate the local branch when the coder
    pushes.
    """
    repo = tmp_path / "repo"
    (repo / ".git" / "info").mkdir(parents=True)
    expected_file = repo / ".git" / "info" / "expected-branch"

    captured: dict[str, str] = {}
    pr = PRInfo(number=42, branch="pr-001")
    runner = _runner_with_repo_path(monkeypatch, repo, open_prs_initial=[pr])

    async def fake_run_auto_pr(repo_path: str, **kwargs: Any):
        captured["content_at_dispatch"] = expected_file.read_text(encoding="utf-8")
        return (0, "ok", "")

    _, plugin = runner._get_coder()
    monkeypatch.setattr(plugin, "run_auto_pr", fake_run_auto_pr)

    asyncio.run(runner.handle_coding())

    assert captured["content_at_dispatch"] == "pr-001\n"


def test_coding_deletes_expected_branch_after_resolution(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """After ``handle_coding`` runs through ``_post_coder_resolution`` the
    expected-branch marker must be removed so manual operator pushes
    between dispatches are not gated on a stale value.
    """
    repo = tmp_path / "repo"
    (repo / ".git" / "info").mkdir(parents=True)
    expected_file = repo / ".git" / "info" / "expected-branch"

    pr = PRInfo(number=42, branch="pr-001")
    runner = _runner_with_repo_path(monkeypatch, repo, open_prs_initial=[pr])
    _, plugin = runner._get_coder()
    monkeypatch.setattr(
        plugin, "run_auto_pr", h._async_cli_result(0, "ok", "")
    )

    asyncio.run(runner.handle_coding())

    assert runner.state.state == PipelineState.WATCH
    assert not expected_file.exists()


def test_coding_writes_active_pr_runtime_file_in_shim_mode(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    repo = tmp_path / "repo"
    marker = repo / ".daemon-runtime" / "active-pr-id"
    pr = PRInfo(number=42, branch="pr-001")
    runner = _runner_with_repo_path(monkeypatch, repo, open_prs_initial=[pr])
    monkeypatch.setenv("PIPELINE_E2E_SHIM", "1")

    captured: dict[str, str] = {}

    async def fake_run_auto_pr(repo_path: str, **kwargs: Any):
        captured["active_pr"] = marker.read_text(encoding="utf-8")
        return (0, "ok", "")

    _, plugin = runner._get_coder()
    monkeypatch.setattr(plugin, "run_auto_pr", fake_run_auto_pr)

    asyncio.run(runner.handle_coding())

    assert captured["active_pr"] == "PR-001\n"


def test_coding_keeps_runtime_file_on_exit_for_fix_shim(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    repo = tmp_path / "repo"
    marker = repo / ".daemon-runtime" / "active-pr-id"
    pr = PRInfo(number=42, branch="pr-001")
    runner = _runner_with_repo_path(monkeypatch, repo, open_prs_initial=[pr])
    monkeypatch.setenv("PIPELINE_E2E_SHIM", "1")
    _, plugin = runner._get_coder()
    monkeypatch.setattr(
        plugin, "run_auto_pr", h._async_cli_result(0, "ok", "")
    )

    asyncio.run(runner.handle_coding())

    assert runner.state.state == PipelineState.WATCH
    assert marker.read_text(encoding="utf-8") == "PR-001\n"


def test_coding_does_not_write_runtime_file_in_production(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    repo = tmp_path / "repo"
    marker = repo / ".daemon-runtime" / "active-pr-id"
    pr = PRInfo(number=42, branch="pr-001")
    runner = _runner_with_repo_path(monkeypatch, repo, open_prs_initial=[pr])
    monkeypatch.delenv("PIPELINE_E2E_SHIM", raising=False)

    captured: dict[str, bool] = {}

    async def fake_run_auto_pr(repo_path: str, **kwargs: Any):
        captured["marker_exists"] = marker.exists()
        return (0, "ok", "")

    _, plugin = runner._get_coder()
    monkeypatch.setattr(plugin, "run_auto_pr", fake_run_auto_pr)

    asyncio.run(runner.handle_coding())

    assert captured["marker_exists"] is False
    assert not marker.exists()


def test_active_pr_runtime_write_error_logs_and_continues(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    runner = h._make_runner()
    runner.repo_path = str(tmp_path)
    monkeypatch.setenv("PIPELINE_E2E_SHIM", "1")
    (tmp_path / ".daemon-runtime").write_text("not a dir\n", encoding="utf-8")

    runner._write_active_pr_runtime_file("PR-001")

    log_entries = [entry["event"] for entry in runner.state.history]
    assert any("active-pr-id runtime write failed" in e for e in log_entries)


def test_coding_handles_expected_branch_write_error(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """A failed expected-branch write must NOT block dispatch — the hook
    is defense-in-depth on top of the prompt-level safeguards from
    PR-271. The handler logs a warning and continues.
    """
    repo = tmp_path / "repo"
    info_dir = repo / ".git" / "info"
    info_dir.mkdir(parents=True)
    # Pre-create a directory at the marker path so write_text raises
    # IsADirectoryError (a real OSError) without monkeypatching pathlib.
    (info_dir / "expected-branch").mkdir()

    pr = PRInfo(number=42, branch="pr-001")
    runner = _runner_with_repo_path(monkeypatch, repo, open_prs_initial=[pr])
    _, plugin = runner._get_coder()

    invoked = {"n": 0}

    async def fake_run_auto_pr(repo_path: str, **kwargs: Any):
        invoked["n"] += 1
        return (0, "ok", "")

    monkeypatch.setattr(plugin, "run_auto_pr", fake_run_auto_pr)

    asyncio.run(runner.handle_coding())

    # Dispatch still proceeded despite the write failure.
    assert invoked["n"] == 1
    assert runner.state.state == PipelineState.WATCH
    log_entries = [entry["event"] for entry in runner.state.history]
    assert any("expected-branch write failed" in e for e in log_entries)


def test_coding_deletes_expected_branch_on_user_stop_pause(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """User-stop pause must still clean up the expected-branch marker.

    ``_run_coder_with_supervision`` returns ``None`` and ``handle_coding``
    short-circuits before reaching ``_post_coder_resolution``. Without
    cleanup in a ``finally`` block on the write side, the stale marker
    persists and the pre-push hook then rejects unrelated pushes from
    the same worktree.
    """
    repo = tmp_path / "repo"
    (repo / ".git" / "info").mkdir(parents=True)
    expected_file = repo / ".git" / "info" / "expected-branch"

    runner = _runner_with_repo_path(monkeypatch, repo)
    _, plugin = runner._get_coder()

    async def fake_run_auto_pr(repo_path: str, **kwargs: Any):
        # Simulate the stop monitor: mark stop and raise CancelledError so
        # _run_coder_with_supervision takes the user-stop branch and returns
        # None.
        runner._stop_requested = True
        raise asyncio.CancelledError

    monkeypatch.setattr(plugin, "run_auto_pr", fake_run_auto_pr)

    asyncio.run(runner.handle_coding())

    assert runner.state.state == PipelineState.PAUSED
    assert not expected_file.exists()


def test_coding_deletes_expected_branch_on_late_breach_pause(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Late in-flight rate-limit breach pause must still clean up the marker.

    The late-breach branch in ``_run_coder_with_supervision`` runs after
    the coder exits cleanly but flips ``breach_flag["breached"]`` and
    returns ``None``, bypassing ``_post_coder_resolution``. Cleanup on
    the write side keeps the worktree's pre-push hook unblocked for
    subsequent operator activity.
    """
    repo = tmp_path / "repo"
    (repo / ".git" / "info").mkdir(parents=True)
    expected_file = repo / ".git" / "info" / "expected-branch"

    runner = _runner_with_repo_path(monkeypatch, repo)
    _, plugin = runner._get_coder()

    async def fake_run_auto_pr(repo_path: str, **kwargs: Any):
        return (0, "ok", "")

    def fake_check_late_breach(
        breach_dir: Any, run_id: Any, breach_flag: dict[str, bool]
    ) -> None:
        breach_flag["breached"] = True

    monkeypatch.setattr(plugin, "run_auto_pr", fake_run_auto_pr)
    monkeypatch.setattr(runner, "_check_late_breach", fake_check_late_breach)

    asyncio.run(runner.handle_coding())

    assert runner.state.state == PipelineState.PAUSED
    assert not expected_file.exists()


def test_expected_branch_path_falls_back_when_git_probe_raises(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Probe failure (missing git binary, IO error) must return the legacy
    ``.git/info/expected-branch`` path so the caller's existing
    ``OSError`` handling still runs on the actual write attempt instead
    of the helper itself raising and crashing the dispatch.
    """
    runner = h._make_runner()
    runner.repo_path = str(tmp_path)

    def boom(*_args: Any, **_kwargs: Any) -> Any:
        raise OSError("git missing")

    monkeypatch.setattr(coding_module.git_ops, "_git", boom)
    marker = runner._expected_branch_path()
    assert marker == tmp_path / ".git" / "info" / "expected-branch"


def test_expected_branch_path_anchors_relative_git_path(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """``git rev-parse --git-path`` returns a path relative to the repo
    root for a regular (non-worktree) repo. The helper must anchor that
    relative path at ``repo_path`` so the marker resolves to an absolute
    path the caller can ``write_text`` against.
    """
    runner = h._make_runner()
    runner.repo_path = str(tmp_path)

    class _R:
        returncode = 0
        stdout = ".git/info/expected-branch\n"

    monkeypatch.setattr(
        coding_module.git_ops, "_git", lambda *a, **k: _R()
    )
    marker = runner._expected_branch_path()
    assert marker == tmp_path / ".git" / "info" / "expected-branch"
    assert marker.is_absolute()


def test_expected_branch_path_resolves_via_git_for_linked_worktree(
    tmp_path: Path,
) -> None:
    """A linked worktree has ``.git`` as a *file* pointing into
    ``<main>/.git/worktrees/<name>/``; the per-worktree ``info/``
    directory lives under that gitdir, not under ``<worktree>/.git/``.

    The hardcoded ``Path(repo_path) / ".git" / "info"`` layout would
    raise ``NotADirectoryError`` on every write/cleanup, the existing
    ``OSError`` catches would swallow it, and push validation would be
    silently disabled for the entire CODING run. ``_expected_branch_path``
    must instead derive the marker path from ``git rev-parse
    --git-path info/expected-branch`` so it lands inside the
    per-worktree gitdir where the hook also reads it.
    """
    main_repo = tmp_path / "main"
    main_repo.mkdir()
    env = {
        "GIT_AUTHOR_NAME": "t",
        "GIT_AUTHOR_EMAIL": "t@example.com",
        "GIT_COMMITTER_NAME": "t",
        "GIT_COMMITTER_EMAIL": "t@example.com",
    }
    subprocess.run(
        ["git", "init", "-q", "-b", "main", str(main_repo)], check=True
    )
    (main_repo / "x.txt").write_text("ok\n")
    subprocess.run(
        ["git", "-C", str(main_repo), "add", "x.txt"], check=True, env=env
    )
    subprocess.run(
        ["git", "-C", str(main_repo), "commit", "-q", "-m", "init"],
        check=True,
        env=env,
    )
    worktree = tmp_path / "wt"
    subprocess.run(
        [
            "git",
            "-C",
            str(main_repo),
            "worktree",
            "add",
            "-q",
            "-b",
            "feature",
            str(worktree),
        ],
        check=True,
        env=env,
    )

    # Sanity: in a linked worktree, ``.git`` is a file (not a directory).
    assert (worktree / ".git").is_file()

    runner = h._make_runner()
    runner.repo_path = str(worktree)

    marker = runner._expected_branch_path()
    runner._write_expected_branch("feature")
    assert marker.exists()
    assert marker.read_text(encoding="utf-8") == "feature\n"
    # The marker must land where ``git rev-parse --git-path
    # info/expected-branch`` resolves (the main repo's ``.git/info/``
    # in shared-info layouts) and never under the worktree's ``.git``
    # *file*, which is what would make ``write_text`` raise
    # ``NotADirectoryError`` and silently disable validation.
    legacy_hardcoded = worktree / ".git" / "info" / "expected-branch"
    assert marker.resolve() != legacy_hardcoded.resolve()
    assert marker.parent.is_dir()

    runner._cleanup_expected_branch()
    assert not marker.exists()
