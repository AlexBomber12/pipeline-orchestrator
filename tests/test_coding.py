from __future__ import annotations

import asyncio
import subprocess
from typing import Any

import pytest
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
    monkeypatch.setattr(h.claude_cli, "run_planned_pr_async", h._async_cli_result(0, "ok", ""))
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
