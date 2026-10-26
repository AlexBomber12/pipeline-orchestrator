from __future__ import annotations

import asyncio
import subprocess
from pathlib import Path
from types import SimpleNamespace
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
    open_prs_initial: list[PRInfo] | None = None,
):
    h._patch_subprocess(monkeypatch)
    monkeypatch.setattr(h.claude_cli, "run_auto_pr_async", h._async_cli_result(0, "ok", ""))
    monkeypatch.setattr(
        "src.github.prs.get_open_prs", lambda repo, **kw: open_prs_initial or [],
    )

    async def _sleep(_seconds: float) -> None:
        return None

    monkeypatch.setattr(h.runner_module.asyncio, "sleep", _sleep)
    runner = h._make_runner()
    # The runner fixture has no real task branch or task file. Model the
    # successful absence probe and status write unless a test overrides them.
    monkeypatch.setattr(coding_module, "_initial_branch_head_sha", lambda *a: "")
    async def status_written(*args, **kwargs):
        return True
    monkeypatch.setattr(runner, "_commit_task_status_change", status_written)
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


def test_case_a_no_branch_no_remote_parks_in_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """PR-317: case-A 'coder did nothing' parks in ERROR, not IDLE."""
    runner = _runner(monkeypatch)
    _patch_branch_state(monkeypatch, local_exists=False, remote_exists=False)
    asyncio.run(runner.handle_coding())
    assert runner.state.state == PipelineState.ERROR
    assert runner.state.skip_ai_error_diagnose is False
    assert "did nothing" in (runner.state.error_message or "")
    assert any("did nothing" in entry["event"] for entry in runner.state.history)


def test_branch_mismatch_after_coder_exit_escalates_explicitly(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When ``BranchContext`` reports an explicit divergence between
    ``task_branch`` and the actual checked-out branch after the coder
    exits 0, ``handle_coding`` skips to IDLE with a [BRANCH] log
    instead of degrading into the case-A "did nothing" diagnostic.

    This is the OBS-AI gap PR-222 closes: surfacing the divergence
    before the PR lookup loop turns a silent skip into a named branch
    mismatch with both surfaces named in the diagnostic.
    """
    runner = _runner(monkeypatch)

    def fake_run(cmd: list[str], **_kwargs: Any) -> subprocess.CompletedProcess[str]:
        if cmd[:3] == ["git", "rev-parse", "--abbrev-ref"]:
            return subprocess.CompletedProcess(args=cmd, returncode=0, stdout="pr-001-typo\n", stderr="")
        if cmd[:2] == ["git", "ls-remote"]:
            return subprocess.CompletedProcess(args=cmd, returncode=2, stdout="", stderr="")
        return subprocess.CompletedProcess(args=cmd, returncode=0, stdout="", stderr="")

    monkeypatch.setattr("src.branch_context.subprocess.run", fake_run)

    asyncio.run(runner.handle_coding())

    assert runner.state.state == PipelineState.ERROR
    assert runner.state.skip_ai_error_diagnose is False
    error = runner.state.error_message or ""
    assert "Branch mismatch" in error
    assert "task_branch=pr-001" in error
    assert "current_git_branch=pr-001-typo" in error
    log_entries = [entry["event"] for entry in runner.state.history]
    assert any("[BRANCH] mismatch detected" in e for e in log_entries)


def test_case_b_local_branch_only_parks_in_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """PR-317: case-B 'local branch but no push' parks in ERROR, not IDLE."""
    runner = _runner(monkeypatch)
    _patch_branch_state(monkeypatch, local_exists=True, remote_exists=False)
    asyncio.run(runner.handle_coding())
    assert runner.state.state == PipelineState.ERROR
    assert runner.state.skip_ai_error_diagnose is False
    assert "no push" in (runner.state.error_message or "")


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
    assert coding_module._remote_branch_exists("/tmp/nope", "any") is None


def test_local_branch_head_sha_requires_verified_full_sha(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sha = "a" * 40
    monkeypatch.setattr(
        git_ops,
        "_git",
        lambda *args, **kwargs: subprocess.CompletedProcess(
            args=[], returncode=0, stdout=f"{sha}\n", stderr=""
        ),
    )
    assert coding_module._local_branch_head_sha("/repo", "pr-001") == sha

    monkeypatch.setattr(
        git_ops,
        "_git",
        lambda *args, **kwargs: subprocess.CompletedProcess(
            args=[], returncode=0, stdout="short\n", stderr=""
        ),
    )
    assert coding_module._local_branch_head_sha("/repo", "pr-001") is None

    monkeypatch.setattr(
        git_ops,
        "_git",
        lambda *args, **kwargs: (_ for _ in ()).throw(OSError("git failed")),
    )
    assert coding_module._local_branch_head_sha("/repo", "pr-001") is None


def test_remote_branch_exists_returns_unknown_on_empty_stdout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_git(*args: Any, **kwargs: Any):
        return subprocess.CompletedProcess(args=["git"], returncode=0, stdout="", stderr="")

    monkeypatch.setattr(git_ops, "_git", fake_git)
    assert coding_module._remote_branch_exists("/tmp/nope", "any") is None


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
                "user": {"id": 199175422, "login": "chatgpt-codex-connector[bot]"},
                "created_at": reaction_iso,
            }
        ]
        if eyes_present
        else []
    )
    monkeypatch.setattr(
        "src.github.reactions._get_codex_issue_reactions",
        lambda repo, number, policy=None: payload,
    )
    monkeypatch.setattr(
        "src.github.prs.get_pr_head_publication",
        lambda *args, **kwargs: SimpleNamespace(
            repo="octo/demo", head_repo="octo/demo", ref="refs/heads/pr-001",
            head_sha=kwargs["expected_head_sha"],
            published_at=gh_runner._parse_iso(last_push_iso), unknown_reason=None,
        ),
    )


def test_handle_coding_skips_codex_review_when_eyes_already_reacted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """OBS-Z: Codex auto-trigger already landed → skip duplicate mention."""
    pr = PRInfo(number=42, branch="pr-001", head_sha="a" * 40)
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


def test_should_skip_codex_review_post_reuses_one_reviewer_policy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = h._make_runner()
    runner.state.current_pr = PRInfo(number=42, branch="pr-001", head_sha="a" * 40)
    selected_policy = object()
    observed_policies: list[object | None] = []

    monkeypatch.setattr("src.daemon.handlers.hung.load_config", lambda: object())
    monkeypatch.setattr(
        "src.daemon.handlers.hung.reviewer_policy_from_config",
        lambda config: selected_policy,
    )

    def fake_reactions(
        repo: str,
        pr_number: int,
        policy: object | None = None,
    ) -> list[dict]:
        observed_policies.append(policy)
        return [
            {
                "content": "eyes",
                "user": {"id": 123, "login": "renamed-reviewer[bot]"},
                "created_at": "2026-04-30T12:30:00Z",
            }
        ]

    def fake_match(
        reaction: dict,
        content: str,
        policy: object | None = None,
    ) -> bool:
        observed_policies.append(policy)
        return content == "eyes"

    monkeypatch.setattr(
        "src.github.reactions._get_codex_issue_reactions",
        fake_reactions,
    )
    monkeypatch.setattr(
        "src.github.reactions._is_reaction_content",
        fake_match,
    )
    monkeypatch.setattr(
        "src.github.prs.get_pr_head_publication",
        lambda *args, **kwargs: SimpleNamespace(repo="octo/demo", head_repo="octo/demo",
            ref="refs/heads/pr-001", head_sha=kwargs["expected_head_sha"],
            published_at=gh_runner._parse_iso("2026-04-30T12:00:00Z")),
    )

    assert runner._should_skip_codex_review_post(42) is True
    assert observed_policies == [selected_policy, selected_policy]


def test_should_skip_codex_review_post_fails_open_on_push_time_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Unresolvable last-push time must not suppress the mention."""
    runner = h._make_runner()
    runner.state.current_pr = PRInfo(number=42, branch="pr-001", head_sha="a" * 40)

    monkeypatch.setattr(
        "src.github.reactions._get_codex_issue_reactions",
        lambda repo, number, policy=None: [
            {
                "content": "eyes",
                "user": {"id": 199175422, "login": "chatgpt-codex-connector[bot]"},
                "created_at": "2026-04-30T12:30:00Z",
            }
        ],
    )

    def boom(*_a: Any, **_kw: Any) -> Any:
        raise RuntimeError("push-time boom")

    monkeypatch.setattr("src.github.prs.get_pr_head_publication", boom)
    assert runner._should_skip_codex_review_post(42) is False


def test_should_skip_codex_review_post_fails_open_on_missing_push_time(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A ``None`` last-push time (activity API degraded) must fail open."""
    runner = h._make_runner()
    runner.state.current_pr = PRInfo(number=42, branch="pr-001", head_sha="a" * 40)
    monkeypatch.setattr(
        "src.github.reactions._get_codex_issue_reactions",
        lambda repo, number, policy=None: [
            {
                "content": "eyes",
                "user": {"id": 199175422, "login": "chatgpt-codex-connector[bot]"},
                "created_at": "2026-04-30T12:30:00Z",
            }
        ],
    )
    monkeypatch.setattr(
        "src.github.prs.get_pr_head_publication",
        lambda *args, **kwargs: SimpleNamespace(repo="octo/demo", head_repo="octo/demo",
            ref="refs/heads/pr-001", head_sha=kwargs["expected_head_sha"],
            published_at=None),
    )
    assert runner._should_skip_codex_review_post(42) is False


def test_should_skip_codex_review_post_skips_when_eyes_after_push(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A fresh EYES reaction (after the last push) suppresses the mention."""
    runner = h._make_runner()
    runner.state.current_pr = PRInfo(number=42, branch="pr-001", head_sha="a" * 40)
    monkeypatch.setattr(
        "src.github.reactions._get_codex_issue_reactions",
        lambda repo, number, policy=None: [
            {
                "content": "eyes",
                "user": {"id": 199175422, "login": "chatgpt-codex-connector[bot]"},
                "created_at": "2026-04-30T12:30:00Z",
            }
        ],
    )
    monkeypatch.setattr(
        "src.github.prs.get_pr_head_publication",
        lambda *args, **kwargs: SimpleNamespace(repo="octo/demo", head_repo="octo/demo",
            ref="refs/heads/pr-001", head_sha=kwargs["expected_head_sha"],
            published_at=gh_runner._parse_iso("2026-04-30T12:00:00Z")),
    )
    assert runner._should_skip_codex_review_post(42) is True


def test_should_skip_codex_review_post_does_not_skip_when_eyes_predates_push(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A stale EYES reaction must not suppress review after a new push."""
    runner = h._make_runner()
    runner.state.current_pr = PRInfo(number=42, branch="pr-001", head_sha="a" * 40)
    monkeypatch.setattr(
        "src.github.reactions._get_codex_issue_reactions",
        lambda repo, number, policy=None: [
            {
                "content": "eyes",
                "user": {"id": 199175422, "login": "chatgpt-codex-connector[bot]"},
                "created_at": "2026-04-30T11:00:00Z",
            }
        ],
    )
    monkeypatch.setattr(
        "src.github.prs.get_pr_head_publication",
        lambda *args, **kwargs: SimpleNamespace(repo="octo/demo", head_repo="octo/demo",
            ref="refs/heads/pr-001", head_sha=kwargs["expected_head_sha"],
            published_at=gh_runner._parse_iso("2026-04-30T12:00:00Z")),
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
    runner.state.current_pr = PRInfo(number=42, branch="pr-001", head_sha="a" * 40)
    monkeypatch.setattr(
        "src.github.reactions._get_codex_issue_reactions",
        lambda repo, number, policy=None: [
            {
                "content": "eyes",
                "user": {"id": 199175422, "login": "chatgpt-codex-connector[bot]"},
                "created_at": "2026-04-30T11:30:00Z",
            }
        ],
    )
    monkeypatch.setattr(
        "src.github.prs.get_pr_head_publication",
        lambda *args, **kwargs: SimpleNamespace(repo="octo/demo", head_repo="octo/demo",
            ref="refs/heads/pr-001", head_sha=kwargs["expected_head_sha"],
            published_at=gh_runner._parse_iso("2026-04-30T12:00:00Z")),
    )
    assert runner._should_skip_codex_review_post(42) is False


def test_should_skip_codex_review_post_normalizes_naive_timestamps(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Naive timestamps from upstream parsers are coerced to UTC so the
    comparison still fires correctly."""
    from datetime import datetime as _dt
    from datetime import timezone as _tz

    runner = h._make_runner()
    runner.state.current_pr = PRInfo(number=42, branch="pr-001", head_sha="a" * 40)
    monkeypatch.setattr(
        "src.github.reactions._get_codex_issue_reactions",
        lambda repo, number, policy=None: [
            {
                "content": "eyes",
                "user": {"id": 199175422, "login": "chatgpt-codex-connector[bot]"},
                "created_at": "2026-04-30T12:30:00",
            }
        ],
    )
    monkeypatch.setattr(
        "src.github.prs.get_pr_head_publication",
        lambda *args, **kwargs: SimpleNamespace(repo="octo/demo", head_repo="octo/demo",
            ref="refs/heads/pr-001", head_sha=kwargs["expected_head_sha"],
            published_at=_dt(2026, 4, 30, 12, 0, 0, tzinfo=_tz.utc)),
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
    runner.state.current_pr = PRInfo(number=42, branch="pr-001", head_sha="a" * 40)
    monkeypatch.setattr(
        "src.github.reactions._get_codex_issue_reactions",
        lambda repo, number, policy=None: [
            {
                "content": "eyes",
                "user": {"id": 199175422, "login": "chatgpt-codex-connector[bot]"},
            }
        ],
    )
    monkeypatch.setattr(
        "src.github.prs.get_pr_head_publication",
        lambda *args, **kwargs: SimpleNamespace(repo="octo/demo", head_repo="octo/demo",
            ref="refs/heads/pr-001", head_sha=kwargs["expected_head_sha"],
            published_at=gh_runner._parse_iso("2026-04-30T12:00:00Z")),
    )
    assert runner._should_skip_codex_review_post(42) is False


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

    def fake_read_bytes(self: Path, *args: Any, **kwargs: Any) -> bytes:
        raise OSError("permission denied")

    monkeypatch.setattr(coding_module.Path, "read_bytes", fake_read_bytes)

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
        breach_dir: Any,
        run_id: Any,
        coder_name: str,
        breach_flag: dict[str, bool],
    ) -> None:
        assert coder_name == "claude"
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


def test_should_skip_codex_review_post_rejects_other_bound_pr(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = h._make_runner()
    runner.state.current_pr = PRInfo(number=43, branch="pr-001", head_sha="a" * 40)
    _patch_codex_reactions(monkeypatch, eyes_present=True)
    assert runner._should_skip_codex_review_post(42) is False


def test_should_skip_codex_review_post_rejects_empty_bound_head(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = h._make_runner()
    runner.state.current_pr = PRInfo(number=42, branch="pr-001")
    _patch_codex_reactions(monkeypatch, eyes_present=True)
    monkeypatch.setattr(git_ops, "_git", lambda *a, **kw: SimpleNamespace(stdout=""))
    assert runner._should_skip_codex_review_post(42) is False


# Keep the production evaluators: these regressions replace only GitHub transport.
_REAL_PUBLICATIONS = coding_module.gh_prs.get_branch_publications
_REAL_EXACT_OUTCOME = coding_module.gh_prs.get_exact_pr_outcome
_REAL_LOCAL_HEAD = coding_module._local_branch_head_sha
_REAL_INITIAL_HEAD = coding_module._initial_branch_head_sha
_REAL_GIT_RUN = subprocess.run


def _publication_transport(monkeypatch, runner, *, code=1, stdout="", stderr="coder failed"):
    from datetime import datetime, timezone

    from src.github import cache

    remote = {"created": False, "omit": False, "exact_unknown": False, "calls": 0, "head": "a" * 40, "update": {}}

    def transport(args, **kwargs):
        assert kwargs["repo"] == runner.owner_repo
        assert args[:2] == ["pr", "list"]  # No second create is authorized.
        assert args[args.index("--state") + 1] == "all"
        if not remote["created"] or remote["omit"]:
            return []
        return [{
            "number": 402, "title": "PR-001: retained", "baseRefName": "main",
            "headRefName": "pr-001", "headRefOid": remote["head"], "state": "OPEN",
            "isDraft": False, "isCrossRepository": False,
            "createdAt": datetime.now(timezone.utc).isoformat(),
            "url": "https://github.com/octo/demo/pull/402",
            **remote["update"],
        }]

    def exact(path):
        assert path == "repos/octo/demo/pulls/402"
        if remote["exact_unknown"]:
            raise OSError("response lost")
        return {
            "number": 402, "base": {"ref": "main", "repo": {"full_name": "octo/demo"}},
            "head": {"ref": "pr-001", "sha": remote["head"], "repo": {"full_name": "octo/demo"}},
            "state": "closed" if remote.get("terminal") else "open",
            "merged": remote.get("terminal") == "MERGED",
            "merge_commit_sha": "c" * 40 if remote.get("terminal") == "MERGED" else None,
        }

    async def coder(*args, **kwargs):
        remote["calls"] += 1
        remote["created"] = True
        return code, stdout, stderr

    monkeypatch.setattr(coding_module.gh_prs, "get_branch_publications", _REAL_PUBLICATIONS)
    monkeypatch.setattr(coding_module.gh_prs, "get_exact_pr_outcome", _REAL_EXACT_OUTCOME)
    monkeypatch.setattr(gh_runner, "run_gh", transport)
    monkeypatch.setattr(cache, "_etag_get", exact)
    monkeypatch.setattr(cache, "_etag_payload_observed_at", lambda *a: None)
    monkeypatch.setattr(h.claude_cli, "run_auto_pr_async", coder)
    return remote


@pytest.mark.parametrize("code,stdout,reason", [
    (1, "", "coder failed"),
    (0, "ESCALATE: cannot finish", "cannot finish"),
    (1, "ESCALATE: cannot finish", "cannot finish"),
])
def test_failed_coder_retains_published_pr_across_restart(monkeypatch, code, stdout, reason):
    from src.keyspace import pipeline_state

    runner = _runner(monkeypatch)
    remote = _publication_transport(monkeypatch, runner, code=code, stdout=stdout)
    asyncio.run(runner.handle_coding())
    assert runner.state.state == PipelineState.ERROR
    assert reason in runner.state.error_message
    assert runner.state.current_pr.number == 402
    binding = runner.state.publication_attempt
    assert (binding.repo, binding.base, binding.task_id, binding.branch, binding.head_sha,
            binding.pr_number) == ("octo/demo", "main", "PR-001", "pr-001", "a" * 40, 402)
    assert binding.run_id == runner.state.active_invocation.run_id
    assert binding.failure == runner.state.error_message

    # Empty/partial lists and a failed exact read after restart cannot erase ownership.
    remote["omit"] = remote["exact_unknown"] = True
    restarted = h._make_runner()
    restarted.redis = runner.redis
    monkeypatch.setattr(coding_module.gh_prs, "get_open_prs", lambda *a, **kw: [])
    assert asyncio.run(restarted.recover_state())
    assert restarted.state.state == PipelineState.ERROR
    assert restarted.state.current_pr.number == 402
    assert restarted.state.publication_attempt == binding
    restarted.state.state = PipelineState.IDLE
    asyncio.run(restarted.handle_idle())
    asyncio.run(restarted.handle_coding())
    assert remote["calls"] == 1
    assert restarted.state.current_pr.number == 402
    assert reason in restarted.state.error_message
    saved = restarted.redis.store[pipeline_state(restarted.name)]
    assert '"pr_number":402' in saved


def test_lost_create_response_holds_until_exact_publication_discovered(monkeypatch):
    runner = _runner(monkeypatch)
    remote = _publication_transport(monkeypatch, runner)
    remote["omit"] = True
    asyncio.run(runner.handle_coding())
    assert runner.state.current_pr is None
    assert runner.state.publication_attempt.pending
    remote["omit"] = False
    runner.state.state = PipelineState.IDLE
    asyncio.run(runner.handle_idle())
    assert runner.state.state == PipelineState.ERROR
    assert runner.state.current_pr.number == 402
    assert runner.state.error_message == "coder failed"
    assert remote["calls"] == 1


@pytest.mark.parametrize("field,value", [
    ("repo", "other/repo"), ("base", "other"), ("task_id", "PR-002"),
    ("branch", "pr-001-similar"), ("run_id", "another-attempt"),
    ("attempt_index", 999), ("head_sha", "b" * 40),
    ("baseline_numbers", {402}), ("baseline_numbers", None),
])
def test_publication_rejects_wrong_dispatch_identity(monkeypatch, field, value):
    runner = _runner(monkeypatch)
    remote = _publication_transport(monkeypatch, runner)
    remote["omit"] = True
    asyncio.run(runner.handle_coding())
    setattr(runner.state.publication_attempt, field, value)
    remote["omit"] = False
    assert asyncio.run(runner._resolve_published_attempt()) is False
    assert runner.state.current_pr is None
    assert runner.state.publication_attempt.pr_number is None


def test_publication_unknown_preserves_cleanup_hold(monkeypatch):
    runner = _runner(monkeypatch)
    remote = _publication_transport(monkeypatch, runner)

    async def unconfirmed_cleanup(context):
        runner._coder_cleanup_failure_detail = "ownership not confirmed"
        await runner._transition_to_error("cleanup unconfirmed", publish=False)
        return False

    monkeypatch.setattr(runner, "_confirm_current_coder_cleanup", unconfirmed_cleanup)
    asyncio.run(runner.handle_coding())
    assert runner.state.current_pr.number == 402
    assert runner.state.publication_attempt.pr_number == 402
    assert runner.state.error_message == "cleanup unconfirmed"
    assert runner._coder_cleanup_failure_detail == "ownership not confirmed"
    remote["exact_unknown"] = True
    assert asyncio.run(runner._hold_unresolved_publication())
    assert runner.state.current_pr.number == 402
    assert runner.state.error_message == "cleanup unconfirmed"


@pytest.mark.parametrize("remote_exists", [False, True])
def test_diagnosis_honors_stop_after_remote_probe(monkeypatch, remote_exists):
    runner = _runner(monkeypatch)
    _patch_branch_state(monkeypatch, local_exists=True, remote_exists=remote_exists)

    async def stopped():
        runner.state.state = PipelineState.PAUSED
        return True

    asyncio.run(runner._diagnose_exit_zero_no_pr("pr-001", "claude", stopped))
    assert runner.state.state == PipelineState.PAUSED


@pytest.mark.parametrize("update", [
    {"baseRefName": "other"}, {"headRefName": "pr-001-similar"},
    {"title": "PR-002: unrelated"}, {"headRefOid": "b" * 40},
    {"isCrossRepository": True},
])
def test_publication_transport_rejects_unrelated_pr(monkeypatch, update):
    runner = _runner(monkeypatch)
    remote = _publication_transport(monkeypatch, runner)
    remote["update"] = update
    asyncio.run(runner.handle_coding())
    assert runner.state.state == PipelineState.ERROR
    assert runner.state.current_pr is None
    assert runner.state.publication_attempt.pr_number is None
    asyncio.run(runner.handle_error())
    assert runner.state.error_message == "coder failed"
    assert remote["calls"] == 1


def test_published_attempt_uses_real_task_branch_commit(monkeypatch, tmp_path):
    def git(*args):
        return _REAL_GIT_RUN(
            ["git", "-c", "user.name=Test", "-c", "user.email=test@example.com", *args],
            cwd=tmp_path, check=True, capture_output=True, text=True,
        ).stdout.strip()

    git("init", "-b", "pr-001")
    git("commit", "--allow-empty", "-m", "task checkpoint")
    head = git("rev-parse", "HEAD")
    runner = _runner(monkeypatch)
    runner.repo_path = str(tmp_path)
    remote = _publication_transport(monkeypatch, runner)
    remote["head"] = head
    monkeypatch.setattr(subprocess, "run", _REAL_GIT_RUN)
    monkeypatch.setattr(coding_module, "_local_branch_head_sha", _REAL_LOCAL_HEAD)
    monkeypatch.setattr(coding_module, "_initial_branch_head_sha", _REAL_INITIAL_HEAD)
    asyncio.run(runner.handle_coding())
    assert runner.state.publication_attempt.head_sha == head
    assert runner.state.current_pr.number == 402
    assert runner.state.current_pr.head_sha == head


def test_failed_fix_retains_exact_pr_and_failure_on_restart(monkeypatch):
    runner = _runner(monkeypatch)
    remote = _publication_transport(monkeypatch, runner, code=0, stderr="")
    asyncio.run(runner.handle_coding())
    assert runner.state.state == PipelineState.WATCH
    coding_run = runner.state.active_invocation.run_id

    async def failed_fix(*args, **kwargs):
        return 1, "", "fix failed after push"

    monkeypatch.setattr(h.claude_cli, "fix_review_async", failed_fix)
    asyncio.run(runner.handle_fix())
    assert runner.state.state == PipelineState.ERROR
    assert runner.state.current_pr.number == 402
    assert runner.state.publication_attempt.run_id != coding_run
    assert runner.state.publication_attempt.failure == "fix failed after push"
    assert runner.state.active_invocation.phase == "fix"
    remote["omit"] = True
    restarted = h._make_runner()
    restarted.redis = runner.redis
    monkeypatch.setattr(coding_module.gh_prs, "get_open_prs", lambda *a, **kw: [])
    assert asyncio.run(restarted.recover_state())
    assert restarted.state.current_pr.number == 402
    assert restarted.state.error_message == "fix failed after push"
    assert restarted.state.active_invocation.phase == "fix"


@pytest.mark.parametrize("paused", [False, True])
def test_restart_after_publication_checkpoint_preserves_pause_or_holds_unclassified_pr(monkeypatch, paused):
    runner = _runner(monkeypatch)
    remote = _publication_transport(monkeypatch, runner, code=0, stderr="")

    async def crash_before_terminal(*args, **kwargs):
        if paused:
            runner.state.state = PipelineState.PAUSED
            runner.state.user_paused = True
            # Model the operator's persisted pause flag, which publish merges.
            from src.keyspace import pipeline_state
            await runner.redis.set(pipeline_state(runner.name), runner.state.model_dump_json())
        raise RuntimeError("daemon stopped before terminal handoff")

    monkeypatch.setattr(runner, "_post_coder_resolution", crash_before_terminal)
    with pytest.raises(RuntimeError, match="daemon stopped"):
        asyncio.run(runner.handle_coding())
    remote["omit"] = True
    restarted = h._make_runner()
    restarted.redis = runner.redis
    restarted._post_codex_review = lambda *_a: True
    monkeypatch.setattr(coding_module.gh_prs, "get_open_prs", lambda *a, **kw: [])
    assert asyncio.run(restarted.recover_state())
    assert restarted.state.current_pr.number == 402
    assert restarted.state.state == (PipelineState.PAUSED if paused else PipelineState.ERROR)
    assert restarted.state.user_paused is paused
    assert remote["calls"] == 1


def test_missing_publication_binding_cannot_attach_a_pr(monkeypatch):
    from datetime import datetime, timezone

    runner = _runner(monkeypatch)
    observed = coding_module.gh_prs.BranchPublication(
        402, "PR-001: task", "main", "pr-001", "a" * 40, "OPEN", False, False,
        datetime.now(timezone.utc), "https://github.com/octo/demo/pull/402",
    )
    asyncio.run(runner._retain_observed_publication(observed))
    asyncio.run(runner._record_pre_pause_pr("pr-001", "before pause"))
    assert runner.state.current_pr is None
    asyncio.run(runner._post_coder_resolution(
        "claude", 0, "", "", target_branch="pr-001", current_pr_id="PR-001",
    ))
    assert runner.state.state == PipelineState.ERROR
    assert "Missing publication attempt" in runner.state.error_message


def test_corrupt_candidate_and_unavailable_discovery_keep_publication_held(monkeypatch):
    runner = _runner(monkeypatch)
    remote = _publication_transport(monkeypatch, runner)
    asyncio.run(runner.handle_coding())
    binding = runner.state.publication_attempt
    binding.candidate = binding.candidate.model_copy(update={"branch": "wrong"})
    assert asyncio.run(runner._resolve_published_attempt()) is False
    assert runner.state.current_pr.number == 402
    binding.pr_number = None

    def unavailable(*args, **kwargs):
        raise OSError("targeted discovery unavailable")

    monkeypatch.setattr(coding_module.gh_prs, "get_branch_publications", unavailable)
    assert asyncio.run(runner._resolve_published_attempt()) is False
    assert binding.pending
    asyncio.run(runner.handle_fix())
    assert runner.state.state == PipelineState.ERROR
    assert runner.state.error_message == "coder failed"
    assert remote["calls"] == 1


def test_published_pr_is_retained_when_later_stop_arrives_during_exact_read(monkeypatch):
    runner = _runner(monkeypatch)
    _publication_transport(monkeypatch, runner, code=0, stderr="")
    asyncio.run(runner.handle_coding())
    binding = runner.state.publication_attempt
    binding.pending = True
    original = runner._resolve_published_attempt

    async def stop_after_exact_read():
        resolved = await original()
        runner._stop_requested = True
        return resolved

    monkeypatch.setattr(runner, "_resolve_published_attempt", stop_after_exact_read)
    asyncio.run(runner._post_coder_resolution(
        "claude", 0, "", "", target_branch="pr-001", current_pr_id="PR-001",
    ))
    assert runner.state.state == PipelineState.PAUSED
    assert runner.state.current_pr.number == 402


def test_success_exit_with_pushed_branch_and_omitted_pr_never_recreates(monkeypatch):
    runner = _runner(monkeypatch)
    remote = _publication_transport(monkeypatch, runner, code=0, stderr="")
    remote["omit"] = True
    _patch_branch_state(monkeypatch, local_exists=True, remote_exists=True)
    asyncio.run(runner.handle_coding())
    assert runner.state.state == PipelineState.ERROR
    assert "without repeating PR creation" in runner.state.error_message
    assert runner.state.publication_attempt.pending
    remote["omit"] = False
    asyncio.run(runner.handle_idle())
    asyncio.run(runner.handle_coding())
    assert runner.state.current_pr.number == 402
    assert remote["calls"] == 1


@pytest.mark.parametrize("stop_call", [1, 2])
def test_stop_during_post_coder_checks_retains_published_identity(monkeypatch, stop_call):
    runner = _runner(monkeypatch)
    _publication_transport(monkeypatch, runner, code=0, stderr="")
    asyncio.run(runner.handle_coding())
    calls = 0

    async def stop():
        nonlocal calls
        calls += 1
        return calls == stop_call

    monkeypatch.setattr(runner, "_pop_stop_request", stop)
    asyncio.run(runner._post_coder_resolution(
        "claude", 0, "", "", target_branch="pr-001", current_pr_id="PR-001",
    ))
    assert runner.state.state == PipelineState.PAUSED
    assert runner.state.current_pr.number == 402


def test_wrong_persisted_attempt_does_not_adopt_snapshot_pr(monkeypatch):
    from src.keyspace import pipeline_state

    runner = _runner(monkeypatch)
    _publication_transport(monkeypatch, runner)
    asyncio.run(runner.handle_coding())
    runner.state.publication_attempt.run_id = "wrong-attempt"
    asyncio.run(runner.redis.set(pipeline_state(runner.name), runner.state.model_dump_json()))
    restarted = h._make_runner()
    restarted.redis = runner.redis
    monkeypatch.setattr(coding_module.gh_prs, "get_open_prs", lambda *a, **kw: [])
    assert asyncio.run(restarted.recover_state())
    assert restarted.state.current_pr is None
    assert restarted.state.state == PipelineState.ERROR
    assert restarted.state.publication_attempt.pending


def test_retained_failed_attempt_preserves_publication_safety_floor(monkeypatch):
    from datetime import datetime, timezone

    from src.keyspace import pipeline_state
    from src.models import PublicationWatermark

    runner = _runner(monkeypatch)
    _publication_transport(monkeypatch, runner)
    asyncio.run(runner.handle_coding())
    watermark = PublicationWatermark(
        repo="octo/demo", pr_number=402, head_repo="octo/demo",
        ref="refs/heads/pr-001", head_sha="a" * 40,
        published_at=datetime.now(timezone.utc),
    )
    runner.state.publication_watermark = watermark
    asyncio.run(runner.redis.set(pipeline_state(runner.name), runner.state.model_dump_json()))
    restarted = h._make_runner()
    restarted.redis = runner.redis
    monkeypatch.setattr(coding_module.gh_prs, "get_open_prs", lambda *a, **kw: [])
    assert asyncio.run(restarted.recover_state())
    assert restarted.state.publication_watermark == watermark
    assert restarted.state.current_pr.number == 402
    assert restarted.state.error_message == "coder failed"


@pytest.mark.parametrize("local_exists", [False, True])
@pytest.mark.parametrize("code", [0, 1])
def test_proven_no_push_releases_attempt_for_operator_retry(monkeypatch, local_exists, code):
    from tests.runner.test_retry_commands import _command, _task_text

    runner = _runner(monkeypatch)
    remote = _publication_transport(monkeypatch, runner, code=0, stderr="")
    # The coder exits without publishing anything.
    async def no_publication(*args, **kwargs):
        remote["calls"] += 1
        return code, "", "coder failed" if code else ""

    monkeypatch.setattr(h.claude_cli, "run_auto_pr_async", no_publication)
    _patch_branch_state(monkeypatch, local_exists=local_exists, remote_exists=False)
    monkeypatch.setattr(coding_module, "_local_branch_head_sha", lambda *a: "a" * 40 if local_exists else None)
    asyncio.run(runner.handle_coding())
    assert runner.state.state == PipelineState.ERROR
    assert not runner.state.publication_attempt.pending
    if code:
        assert runner.state.error_message == "coder failed"
    monkeypatch.setattr(coding_module.gh_prs, "get_open_prs", lambda *a, **kw: [])
    monkeypatch.setattr(coding_module.gh_prs, "get_merged_prs", lambda *a, **kw: [])
    command = _command(_task_text(task_id="PR-001", branch="pr-001"),
                       task_id="PR-001", branch="pr-001")
    assert asyncio.run(runner._select_retry_continuation(command)) == ("coding", None)
    assert remote["calls"] == 1


@pytest.mark.parametrize("probe_result", [0, 128, "timeout"])
@pytest.mark.parametrize("code", [0, 1])
def test_present_or_unavailable_remote_keeps_attempt_pending(monkeypatch, probe_result, code):
    runner = _runner(monkeypatch)
    remote = _publication_transport(monkeypatch, runner, code=code, stderr="coder failed" if code else "")
    remote["omit"] = True

    def git(*args, **kwargs):
        if "ls-remote" in args:
            if probe_result == "timeout":
                raise subprocess.TimeoutExpired("git", 30)
            return subprocess.CompletedProcess([], probe_result, "a" * 40 if probe_result == 0 else "", "offline")
        return subprocess.CompletedProcess([], 0, "", "")

    monkeypatch.setattr(git_ops, "_git", git)
    asyncio.run(runner.handle_coding())
    assert runner.state.state == PipelineState.ERROR
    assert runner.state.publication_attempt.pending
    if code:
        assert runner.state.error_message == "coder failed"
    else:
        assert "ownership unresolved" in runner.state.error_message


@pytest.mark.parametrize("pushed", [False, True])
def test_failed_fix_only_records_new_head_after_remote_verification(monkeypatch, pushed):
    from tests.runner.test_retry_commands import _command, _task_text

    runner = _runner(monkeypatch)
    remote = _publication_transport(monkeypatch, runner, code=0, stderr="")
    asyncio.run(runner.handle_coding())

    async def failed_fix(*args, **kwargs):
        monkeypatch.setattr(coding_module, "_local_branch_head_sha", lambda *a: "b" * 40)
        if pushed:
            remote["head"] = "b" * 40
        return 1, "", "fix failed"

    monkeypatch.setattr(h.claude_cli, "fix_review_async", failed_fix)
    asyncio.run(runner.handle_fix())
    assert runner.state.state == PipelineState.ERROR
    attempt = runner.state.publication_attempt
    assert attempt.head_sha == remote["head"]
    assert attempt.candidate.head_sha == remote["head"]
    assert runner.state.current_pr.head_sha == remote["head"]
    assert attempt.failure == "fix failed"
    command = _command(_task_text(task_id="PR-001", branch="pr-001"),
                       task_id="PR-001", branch="pr-001", bound_pr=runner.state.current_pr)
    if pushed:
        continuation, pr = asyncio.run(runner._select_retry_continuation(command))
        assert continuation == "watch"
        assert pr.number == 402
        assert pr.head_sha == remote["head"]
    else:
        assert not asyncio.run(runner._resolve_published_attempt())
        assert runner.state.publication_attempt.pending


@pytest.mark.parametrize("probe", ["stale", "unknown", "raises"])
@pytest.mark.parametrize("pushed", [False, True])
def test_fix_head_probe_failure_preserves_known_remote_head(monkeypatch, probe, pushed):
    runner = _runner(monkeypatch)
    remote = _publication_transport(monkeypatch, runner, code=0, stderr="")
    asyncio.run(runner.handle_coding())

    async def failed_fix(*args, **kwargs):
        monkeypatch.setattr(coding_module, "_local_branch_head_sha", lambda *a: "b" * 40)
        remote["exact_unknown"] = probe == "unknown"
        if probe == "raises":
            def unavailable(*args, **kwargs):
                raise OSError("exact probe unavailable")
            monkeypatch.setattr(coding_module.gh_prs, "get_exact_pr_outcome", unavailable)
        return 1, "", "fix failed"

    monkeypatch.setattr(h.claude_cli, "fix_review_async", failed_fix)
    asyncio.run(runner.handle_fix())
    assert runner.state.publication_attempt.head_sha == "a" * 40
    assert runner.state.current_pr.head_sha == "a" * 40
    assert runner.state.publication_attempt.unverified_head_sha == "b" * 40
    # Persist/reload before GitHub exposes the pushed commit. No recapture of
    # local git state should be required for reconciliation or operator Retry.
    runner.state = type(runner.state).model_validate_json(runner.state.model_dump_json())
    if pushed:
        remote["head"] = "b" * 40
    remote["exact_unknown"] = False
    monkeypatch.setattr(coding_module.gh_prs, "get_exact_pr_outcome", _REAL_EXACT_OUTCOME)
    assert asyncio.run(runner._resolve_published_attempt()) is pushed
    assert runner.state.publication_attempt.head_sha == remote["head"]
    assert runner.state.current_pr.head_sha == remote["head"]
    assert runner.state.publication_attempt.unverified_head_sha == (None if pushed else "b" * 40)
    assert runner.state.publication_attempt.failure == "fix failed"

    from tests.runner.test_retry_commands import _command, _task_text

    command = _command(_task_text(task_id="PR-001", branch="pr-001"),
                       task_id="PR-001", branch="pr-001", bound_pr=runner.state.current_pr)
    if pushed:
        continuation, pr = asyncio.run(runner._select_retry_continuation(command))
        assert continuation == "watch"
        assert pr.head_sha == remote["head"]
    else:
        assert runner.state.publication_attempt.pending


def test_baseline_outage_defers_until_publication_can_be_reconciled(monkeypatch, tmp_path):
    from src.keyspace import pipeline_state
    from src.task_status import MergedState

    from tests.runner.test_retry_commands import _task_text

    (tmp_path / "tasks").mkdir()
    (tmp_path / "tasks/PR-001.md").write_text(
        _task_text(task_id="PR-001", branch="pr-001").replace("status: ERROR", "status: TODO")
        .replace("blocked_reason: daemon\n", "").replace("- Coder: codex", "- Coder: claude")
    )
    for module in ("src.daemon.recovery", "src.daemon.handlers.idle"):
        monkeypatch.setattr(f"{module}._resolve_merged_state",
                            lambda *a, **kw: MergedState(set(), set(), True))

    runner = _runner(monkeypatch)
    runner.state.state = PipelineState.CODING
    remote = _publication_transport(monkeypatch, runner, code=0, stderr="")

    def unavailable(*args, **kwargs):
        raise OSError("baseline unavailable")

    monkeypatch.setattr(coding_module.gh_prs, "get_branch_publications", unavailable)
    asyncio.run(runner.handle_coding())
    assert runner.state.state == PipelineState.IDLE
    assert runner.state.current_task is None
    assert runner.state.active_invocation is None
    assert runner.state.publication_attempt is None
    assert remote["calls"] == 0

    saved = asyncio.run(runner.redis.get(pipeline_state(runner.name)))
    assert type(runner.state).model_validate_json(saved).state == PipelineState.IDLE
    restarted = h._make_runner()
    restarted.redis = runner.redis
    restarted.repo_path = str(tmp_path)
    monkeypatch.setattr(coding_module.gh_prs, "get_merged_prs", lambda *a, **kw: [])
    monkeypatch.setattr(coding_module.gh_prs, "get_merged_prs_discovery",
                        lambda *a: SimpleNamespace(complete=True, prs=[]))
    assert asyncio.run(restarted.recover_state())
    assert restarted.state.state == PipelineState.IDLE
    assert restarted.state.current_task is None
    assert [(task.pr_id, task.status) for task in restarted.state.current_queue] == [
        ("PR-001", TaskStatus.TODO)
    ]
    monkeypatch.setattr(coding_module.gh_prs, "get_branch_publications", _REAL_PUBLICATIONS)
    asyncio.run(restarted.handle_idle())
    assert remote["calls"] == 1
    assert restarted.state.state == PipelineState.WATCH
    assert restarted.state.current_pr.number == 402


@pytest.mark.parametrize("remote_exists", [False, True, None])
@pytest.mark.parametrize("local_exists", [False, True])
def test_rate_limited_no_publication_releases_only_proven_absence(
    monkeypatch, remote_exists, local_exists,
):
    from datetime import datetime, timedelta, timezone

    runner = _runner(monkeypatch)
    remote = _publication_transport(monkeypatch, runner)

    async def rate_limited(*args, **kwargs):
        remote["calls"] += 1
        return 1, "", "rate limit exceeded"

    monkeypatch.setattr(h.claude_cli, "run_auto_pr_async", rate_limited)
    _patch_branch_state(monkeypatch, local_exists=local_exists, remote_exists=False)
    monkeypatch.setattr(coding_module, "_local_branch_head_sha",
                        lambda *a: "a" * 40 if local_exists else None)
    monkeypatch.setattr(coding_module, "_remote_branch_exists", lambda *a: remote_exists)
    asyncio.run(runner.handle_coding())
    assert runner.state.state == PipelineState.PAUSED
    assert runner.state.publication_attempt.pending is (remote_exists is not False)
    assert runner.state.rate_limited_until is not None
    if remote_exists is False:
        # Expiry returns to IDLE without an unresolved-publication error hold.
        expired = datetime.now(timezone.utc) - timedelta(seconds=1)
        runner.state.rate_limited_until = expired
        runner.state.rate_limited_coder_until = {"claude": expired}
        asyncio.run(runner.handle_paused())
        assert runner.state.state == PipelineState.IDLE
        assert not asyncio.run(runner._hold_unresolved_publication())
    assert remote["calls"] == 1


@pytest.mark.parametrize("terminal", ["MERGED", "CLOSED"])
@pytest.mark.parametrize("entry", ["hold", "fix", "retry", "error", "post", "post_success"])
def test_retained_terminal_publication_releases_task(monkeypatch, terminal, entry):
    from tests.runner.test_retry_commands import _command, _task_text

    runner = _runner(monkeypatch)
    remote = _publication_transport(monkeypatch, runner)
    asyncio.run(runner.handle_coding())
    attempt = runner.state.publication_attempt
    assert attempt.pending
    remote["terminal"] = terminal
    outcomes = []

    async def save_outcome(outcome):
        outcomes.append(outcome)

    monkeypatch.setattr(runner, "_save_current_run_record", save_outcome)
    if entry == "hold":
        assert asyncio.run(runner._hold_unresolved_publication())
    elif entry == "fix":
        asyncio.run(runner.handle_fix())
    elif entry == "retry":
        command = _command(_task_text(task_id="PR-001", branch="pr-001"),
                           task_id="PR-001", branch="pr-001")
        asyncio.run(runner._select_retry_continuation(command))
    elif entry == "error":
        asyncio.run(runner.handle_error("FIX idle timeout: retained attempt"))
    else:
        asyncio.run(runner._post_coder_resolution(
            "claude", 0 if entry == "post_success" else 1, "", "coder failed",
            target_branch="pr-001", current_pr_id="PR-001",
        ))
    assert attempt.verified
    assert not attempt.pending
    assert runner.state.current_task is None
    assert runner.state.current_pr is None
    assert runner.state.error_message is None
    assert runner.state.state == PipelineState.IDLE
    assert outcomes == ["success_merged" if terminal == "MERGED" else "closed_unmerged"]
    assert remote["calls"] == 1


@pytest.mark.parametrize("phase", ["during", "after"])
@pytest.mark.parametrize("remote_exists", [False, True, None])
def test_stopped_unpublished_attempt_probes_absence_after_cleanup(monkeypatch, phase, remote_exists):
    from tests.runner.test_retry_commands import _command, _task_text

    runner = _runner(monkeypatch)
    remote = _publication_transport(monkeypatch, runner)
    monkeypatch.setattr(coding_module, "_local_branch_head_sha", lambda *a: None)
    cleanup = []

    async def confirm_cleanup(*args):
        cleanup.append(True)
        return True

    def probe(*args):
        assert cleanup
        return remote_exists

    async def stopped_coder(*args, **kwargs):
        remote["calls"] += 1
        if phase == "during":
            runner._stop_requested = True
            raise asyncio.CancelledError
        return 0, "", ""

    async def stop_after_exit(*args):
        runner._stop_requested = True

    monkeypatch.setattr(runner, "_confirm_current_coder_cleanup", confirm_cleanup)
    monkeypatch.setattr(coding_module, "_remote_branch_exists", probe)
    monkeypatch.setattr(h.claude_cli, "run_auto_pr_async", stopped_coder)
    if phase == "after":
        monkeypatch.setattr(runner, "_save_cli_log", stop_after_exit)
    asyncio.run(runner.handle_coding())
    assert runner.state.state == PipelineState.PAUSED
    assert runner.state.user_paused
    assert runner.state.publication_attempt.head_sha is None
    assert runner.state.publication_attempt.pending is (remote_exists is not False)
    if remote_exists is False:
        runner.state.user_paused = False
        runner.state.state = PipelineState.IDLE
        assert not asyncio.run(runner._hold_unresolved_publication())
        monkeypatch.setattr(coding_module.gh_prs, "get_open_prs", lambda *a, **kw: [])
        monkeypatch.setattr(coding_module.gh_prs, "get_merged_prs", lambda *a, **kw: [])
        command = _command(_task_text(task_id="PR-001", branch="pr-001"),
                           task_id="PR-001", branch="pr-001")
        assert asyncio.run(runner._select_retry_continuation(command)) == ("coding", None)
    assert remote["calls"] == 1


@pytest.mark.parametrize("terminal", ["MERGED", "CLOSED"])
def test_terminal_capture_retains_attempt_until_cleanup_consumer(monkeypatch, terminal):
    runner = _runner(monkeypatch)
    remote = _publication_transport(monkeypatch, runner)
    asyncio.run(runner.handle_coding())
    remote["terminal"] = terminal
    attempt = runner.state.publication_attempt
    # A pushed FIX head can become terminal before reconciliation catches up.
    attempt.unverified_head_sha = remote["head"] = "b" * 40
    monkeypatch.setattr(coding_module, "_local_branch_head_sha", lambda *a: "b" * 40)
    asyncio.run(runner._capture_published_attempt())
    assert attempt.pending
    assert runner.state.current_task is not None
    assert attempt.head_sha == "b" * 40
    assert attempt.unverified_head_sha is None
    assert asyncio.run(runner._hold_unresolved_publication())
    assert not attempt.pending
    assert runner.state.current_task is None


@pytest.mark.parametrize("terminal", ["MERGED", "CLOSED"])
def test_terminal_fix_reconciliation_records_verified_head(monkeypatch, terminal):
    runner = _runner(monkeypatch)
    remote = _publication_transport(monkeypatch, runner)
    asyncio.run(runner.handle_coding())
    attempt = runner.state.publication_attempt
    attempt.unverified_head_sha = remote["head"] = "b" * 40
    remote["terminal"] = terminal
    runner._start_current_run_record("claude", "opus")
    observed = []
    original_save = runner._save_current_run_record

    async def save_outcome(outcome):
        observed.append(runner.state.current_pr.head_sha)
        await original_save(outcome)

    monkeypatch.setattr(runner, "_save_current_run_record", save_outcome)
    assert asyncio.run(runner._resolve_published_attempt())
    assert observed == ["b" * 40]
    assert attempt.candidate.head_sha == attempt.head_sha == "b" * 40
    assert attempt.unverified_head_sha is None
    assert not attempt.pending
    recent = asyncio.run(runner._metrics_store.recent(
        task_id="PR-001", limit=1, repo_name=runner.name,
    ))
    assert recent[0].exit_reason == (
        "success_merged" if terminal == "MERGED" else "closed_unmerged"
    )
    if terminal == "MERGED":
        assert recent[0].head_sha == "b" * 40


@pytest.mark.parametrize("terminal", ["MERGED", "CLOSED"])
@pytest.mark.parametrize("observation", ["listed", "state", "fallback"])
@pytest.mark.parametrize("phase", ["coding", "fix"])
def test_watch_terminal_consumes_paused_publication(monkeypatch, terminal, observation, phase):
    from src.keyspace import pipeline_state

    runner = _runner(monkeypatch)
    remote = _publication_transport(monkeypatch, runner, code=0, stderr="")

    async def paused_coder(*args, **kwargs):
        remote["calls"] += 1
        remote["created"] = True
        runner._stop_requested = True
        runner.state.user_paused = True
        return 0, "", ""

    if phase == "coding":
        monkeypatch.setattr(h.claude_cli, "run_auto_pr_async", paused_coder)
    asyncio.run(runner.handle_coding())
    if phase == "fix":
        monkeypatch.setattr(h.claude_cli, "fix_review_async", paused_coder)
        asyncio.run(runner.handle_fix())
    attempt = runner.state.publication_attempt
    assert runner.state.state == PipelineState.PAUSED
    assert attempt.pending and attempt.verified
    runner._stop_requested = False
    runner.state.user_paused = False
    # Play resumes a paused run with a bound PR directly into WATCH.
    runner.state.state = PipelineState.WATCH
    asyncio.run(runner.redis.set(pipeline_state(runner.name), runner.state.model_dump_json()))
    remote["terminal"] = terminal
    pr = runner.state.current_pr.model_copy()
    monkeypatch.setattr(coding_module.gh_prs, "get_open_prs",
                        lambda *a, **kw: [pr] if observation == "listed" else [])
    monkeypatch.setattr(coding_module.gh_prs, "get_pr_state",
                        lambda *a: None if observation == "fallback" else terminal)
    monkeypatch.setattr(coding_module.gh_prs, "is_pr_merged", lambda *a: terminal == "MERGED")
    original_save = runner._save_current_run_record

    async def save_outcome(outcome):
        checkpoint = type(runner.state).model_validate_json(
            await runner.redis.get(pipeline_state(runner.name)),
        )
        assert checkpoint.current_task is not None
        assert checkpoint.publication_attempt.pending
        await original_save(outcome)

    monkeypatch.setattr(runner, "_save_current_run_record", save_outcome)
    asyncio.run(runner.handle_watch())
    assert runner.state.state == PipelineState.IDLE
    assert runner.state.current_task is None
    assert runner.state.current_pr is None
    assert not attempt.pending
    assert not asyncio.run(runner._hold_unresolved_publication())
    saved = asyncio.run(runner.redis.get(pipeline_state(runner.name)))
    restarted = h._make_runner()
    restarted.state = type(runner.state).model_validate_json(saved)
    assert not asyncio.run(restarted._hold_unresolved_publication())
    assert restarted.state.state == PipelineState.IDLE
    assert remote["calls"] == (1 if phase == "coding" else 2)


@pytest.mark.parametrize("mismatch", ["dispatch", "number", "branch"])
def test_terminal_resolution_does_not_consume_unrelated_attempt(monkeypatch, mismatch):
    runner = _runner(monkeypatch)
    _publication_transport(monkeypatch, runner)
    asyncio.run(runner.handle_coding())
    attempt = runner.state.publication_attempt
    pr = runner.state.current_pr.model_copy()
    if mismatch == "dispatch":
        attempt.run_id = "another-run"
    elif mismatch == "number":
        pr.number += 1
    else:
        pr.branch = "another-branch"
    asyncio.run(runner._handle_external_pr_resolution(pr, "CLOSED"))
    assert attempt.pending


def test_stopped_unpublished_attempt_never_probes_before_confirmed_cleanup(monkeypatch):
    runner = _runner(monkeypatch)
    _publication_transport(monkeypatch, runner)
    monkeypatch.setattr(coding_module, "_local_branch_head_sha", lambda *a: None)

    async def stopped_coder(*args, **kwargs):
        runner._stop_requested = True
        raise asyncio.CancelledError

    async def unconfirmed_cleanup(*args):
        await runner._transition_to_error("cleanup unconfirmed", publish=False)
        return False

    def probe(*args):
        pytest.fail("Remote absence cannot release an attempt before confirmed cleanup")

    monkeypatch.setattr(h.claude_cli, "run_auto_pr_async", stopped_coder)
    monkeypatch.setattr(runner, "_confirm_current_coder_cleanup", unconfirmed_cleanup)
    monkeypatch.setattr(coding_module, "_remote_branch_exists", probe)
    asyncio.run(runner.handle_coding())
    assert runner.state.publication_attempt.pending
    assert runner.state.error_message == "cleanup unconfirmed"


@pytest.mark.parametrize("path", ["escalate", "inflight_breach", "late_breach"])
@pytest.mark.parametrize("evidence", ["absent", "present", "unknown", "published"])
@pytest.mark.parametrize("cleanup_confirmed", [True, False])
def test_escalation_and_breach_release_only_proven_unpublished_attempts(
    monkeypatch, path, evidence, cleanup_confirmed,
):
    from datetime import datetime, timedelta, timezone

    from tests.runner.test_retry_commands import _command, _task_text

    runner = _runner(monkeypatch)
    remote = _publication_transport(monkeypatch, runner)
    monkeypatch.setattr(coding_module, "_local_branch_head_sha",
                        lambda *a: "a" * 40 if evidence == "published" else None)
    cleaned = []
    probes = []

    async def confirm_cleanup(*args):
        cleaned.append(cleanup_confirmed)
        if not cleanup_confirmed:
            await runner._transition_to_error("cleanup unconfirmed", publish=False)
        return cleanup_confirmed

    def probe(*args):
        assert cleaned == [True]
        assert evidence != "published"
        probes.append(True)
        return {"absent": False, "present": True, "unknown": None}[evidence]

    def mark_breach(flag):
        flag["breached"] = True
        runner.state.rate_limited_until = datetime.now(timezone.utc) + timedelta(minutes=5)

    async def breach_monitor(breach_dir, run_id, coder_name, task, flag):
        await asyncio.Future()

    async def coder(*args, **kwargs):
        remote["calls"] += 1
        remote["created"] = evidence == "published"
        if path == "inflight_breach":
            # Exercise the monitor's flag/cancellation contract deterministically.
            flag = breach_flags[0]
            mark_breach(flag)
            raise asyncio.CancelledError
        return 0, "ESCALATE: operator recovery needed" if path == "escalate" else "", ""

    breach_flags = []

    def late_breach(breach_dir, run_id, coder_name, flag):
        if path == "late_breach":
            mark_breach(flag)

    # The monitor is scheduled alongside the coder; capture its shared flag
    # synchronously when the coroutine is created, before either task runs.
    def monitor(breach_dir, run_id, coder_name, task, flag):
        breach_flags.append(flag)
        return breach_monitor(breach_dir, run_id, coder_name, task, flag)

    monkeypatch.setattr(runner, "_monitor_inflight_breach", monitor)
    monkeypatch.setattr(runner, "_check_late_breach", late_breach)
    monkeypatch.setattr(runner, "_confirm_current_coder_cleanup", confirm_cleanup)
    monkeypatch.setattr(coding_module, "_remote_branch_exists", probe)
    monkeypatch.setattr(h.claude_cli, "run_auto_pr_async", coder)
    asyncio.run(runner.handle_coding())
    attempt = runner.state.publication_attempt
    released = cleanup_confirmed and evidence == "absent"
    assert attempt.pending is not released
    assert bool(probes) is (cleanup_confirmed and evidence != "published")
    assert remote["calls"] == 1
    if not cleanup_confirmed:
        assert runner.state.state == PipelineState.ERROR
        assert runner.state.error_message == "cleanup unconfirmed"
        return
    if evidence == "published":
        assert attempt.pr_number == 402
        assert attempt.head_sha == "a" * 40
    else:
        assert attempt.head_sha is None
    if path == "escalate":
        assert runner.state.state == PipelineState.ERROR
        assert runner.state.error_message == "operator recovery needed"
    else:
        assert runner.state.state == PipelineState.PAUSED
        if evidence == "published":
            assert attempt.exit_classification == "failure"
            expired = datetime.now(timezone.utc) - timedelta(seconds=1)
            runner.state.rate_limited_until = expired
            runner.state.rate_limited_coder_until = {"claude": expired}
            asyncio.run(runner.handle_paused())
            assert runner.state.state == PipelineState.ERROR
            assert attempt.pending
            assert runner.state.current_pr.number == 402
            command = _command(_task_text(task_id="PR-001", branch="pr-001"),
                               task_id="PR-001", branch="pr-001")
            continuation, pr = asyncio.run(runner._select_retry_continuation(command))
            assert continuation == "watch"
            assert pr.number == 402
        if released:
            expired = datetime.now(timezone.utc) - timedelta(seconds=1)
            runner.state.rate_limited_until = expired
            runner.state.rate_limited_coder_until = {"claude": expired}
            asyncio.run(runner.handle_paused())
            assert runner.state.state == PipelineState.IDLE
            assert not asyncio.run(runner._hold_unresolved_publication())
    if released:
        monkeypatch.setattr(coding_module.gh_prs, "get_open_prs", lambda *a, **kw: [])
        monkeypatch.setattr(coding_module.gh_prs, "get_merged_prs", lambda *a, **kw: [])
        command = _command(_task_text(task_id="PR-001", branch="pr-001"),
                           task_id="PR-001", branch="pr-001")
        assert asyncio.run(runner._select_retry_continuation(command)) == ("coding", None)


@pytest.mark.parametrize("code", [0, 1])
@pytest.mark.parametrize("discovery", ["unavailable", "closed", "different_head", "empty", "baseline"])
def test_deleted_branch_requires_fresh_all_state_publication_lookup(monkeypatch, discovery, code):
    from tests.runner.test_retry_commands import _command, _task_text

    runner = _runner(monkeypatch)
    remote = _publication_transport(monkeypatch, runner, code=code)
    _patch_branch_state(monkeypatch, local_exists=True, remote_exists=False)
    monkeypatch.setattr(coding_module, "_local_branch_head_sha", lambda *a: "a" * 40)
    lookups = []
    probed = []

    def branch_absent(*args):
        probed.append(True)
        return False

    monkeypatch.setattr(coding_module, "_remote_branch_exists", branch_absent)

    def publications(*args):
        lookups.append(args)
        if not probed:
            # Baseline and capture omit the PR until the post-cleanup ref probe.
            remote["omit"] = True
            return _REAL_PUBLICATIONS(*args)
        if discovery == "unavailable":
            raise OSError("all-state lookup unavailable")
        remote["omit"] = discovery == "empty"
        remote["update"] = {"state": "CLOSED"}
        if discovery == "different_head":
            remote["update"]["headRefOid"] = "b" * 40
        if discovery == "baseline":
            runner.state.publication_attempt.baseline_numbers = {402}
        return _REAL_PUBLICATIONS(*args)

    monkeypatch.setattr(coding_module.gh_prs, "get_branch_publications", publications)
    asyncio.run(runner.handle_coding())
    attempt = runner.state.publication_attempt
    assert probed
    assert len(lookups) >= 3
    assert attempt.pending is (discovery not in {"empty", "baseline"})
    assert runner.state.state == PipelineState.ERROR
    assert remote["calls"] == 1

    if discovery in {"unavailable", "closed", "different_head"}:
        # Persist/reload and Retry cannot discard the hold and choose CODING.
        runner.state = type(runner.state).model_validate_json(runner.state.model_dump_json())
        command = _command(_task_text(task_id="PR-001", branch="pr-001"),
                           task_id="PR-001", branch="pr-001")
        remote["terminal"] = "CLOSED"
        if discovery != "closed":
            assert asyncio.run(runner._select_retry_continuation(command)) != ("coding", None)
            assert runner.state.publication_attempt.pending
        # When complete identity is visible, reconcile the closed publication.
        remote["omit"] = False
        remote["update"] = {"state": "CLOSED"}
        monkeypatch.setattr(coding_module.gh_prs, "get_branch_publications", _REAL_PUBLICATIONS)
        assert asyncio.run(runner._hold_unresolved_publication())
        assert not runner.state.publication_attempt.pending
        assert runner.state.current_task is None
        assert remote["calls"] == 1


@pytest.mark.parametrize("stderr", ["coder failed", "rate limit exceeded",
                                   "ESCALATE: operator recovery needed"])
def test_unpublished_release_survives_crash_before_terminal_bookkeeping(monkeypatch, stderr):
    from src.keyspace import pipeline_state

    from tests.runner.test_retry_commands import _command, _task_text

    runner = _runner(monkeypatch)
    remote = _publication_transport(monkeypatch, runner)

    async def unpublished_coder(*args, **kwargs):
        remote["calls"] += 1
        return 1, stderr if stderr.startswith("ESCALATE:") else "", stderr

    monkeypatch.setattr(h.claude_cli, "run_auto_pr_async", unpublished_coder)
    monkeypatch.setattr(coding_module, "_local_branch_head_sha", lambda *a: None)
    monkeypatch.setattr(coding_module, "_remote_branch_exists", lambda *a: False)
    release = runner._release_unpublished_attempt

    async def crash_after_release(branch):
        await release(branch)
        assert not runner.state.publication_attempt.pending
        raise asyncio.CancelledError("daemon stopped before terminal bookkeeping")

    monkeypatch.setattr(runner, "_release_unpublished_attempt", crash_after_release)
    with pytest.raises(asyncio.CancelledError, match="daemon stopped"):
        asyncio.run(runner.handle_coding())

    saved = asyncio.run(runner.redis.get(pipeline_state(runner.name)))
    restarted = _runner(monkeypatch)
    restarted.state = type(runner.state).model_validate_json(saved)
    assert restarted.state.publication_attempt.head_sha is None
    assert not restarted.state.publication_attempt.pending
    assert not asyncio.run(restarted._hold_unresolved_publication())
    monkeypatch.setattr(coding_module.gh_prs, "get_open_prs", lambda *a, **kw: [])
    monkeypatch.setattr(coding_module.gh_prs, "get_merged_prs", lambda *a, **kw: [])
    command = _command(_task_text(task_id="PR-001", branch="pr-001"),
                       task_id="PR-001", branch="pr-001")
    assert asyncio.run(restarted._select_retry_continuation(command)) == ("coding", None)
    assert remote["calls"] == 1


@pytest.mark.parametrize("git_verification", [None, True])
def test_successful_fix_releases_hold_after_exact_new_head_verification(monkeypatch, git_verification):
    runner = _runner(monkeypatch)
    remote = _publication_transport(monkeypatch, runner, code=0, stderr="")
    asyncio.run(runner.handle_coding())
    posted = []
    monkeypatch.setattr(runner, "_post_codex_review", lambda number: posted.append(number) or True)
    monkeypatch.setattr(runner, "_verify_pushes_since", lambda *a, **kw: git_verification)
    monkeypatch.setattr(coding_module, "_local_branch_head_sha", lambda *a: remote["head"])
    original_git = git_ops._git

    def git(repo_path, *args, **kwargs):
        if args == ("rev-parse", "HEAD"):
            return subprocess.CompletedProcess([], 0, remote["head"] + "\n", "")
        return original_git(repo_path, *args, **kwargs)

    monkeypatch.setattr(git_ops, "_git", git)

    async def successful_fix(*args, **kwargs):
        remote["head"] = "b" * 40
        return 0, "", ""

    monkeypatch.setattr(h.claude_cli, "fix_review_async", successful_fix)
    asyncio.run(runner.handle_fix())
    assert runner.state.state == PipelineState.WATCH
    assert not runner.state.publication_attempt.pending
    assert runner.state.publication_attempt.unverified_head_sha is None
    assert runner.state.publication_attempt.head_sha == runner.state.current_pr.head_sha == "b" * 40
    assert runner.state.current_pr.fix_iteration_count == 1
    assert posted == [402]


@pytest.mark.parametrize("probe", ["stale", "unknown", "raises"])
@pytest.mark.parametrize("pushed", [False, True])
@pytest.mark.parametrize("paused", [False, True])
def test_successful_fix_holds_unverified_head_until_exact_reconciliation(
    monkeypatch, probe, pushed, paused,
):
    from unittest.mock import AsyncMock

    from src.daemon.retry_commands import RetryDispatch

    from tests.runner.test_retry_commands import _command, _task_text

    runner = _runner(monkeypatch)
    remote = _publication_transport(monkeypatch, runner, code=0, stderr="")
    asyncio.run(runner.handle_coding())
    posted = []
    fixes = []
    pushes_before = runner.state.current_pr.push_count
    monkeypatch.setattr(runner, "_post_codex_review", lambda number: posted.append(number) or True)
    monkeypatch.setattr(runner, "_verify_pushes_since", lambda *a, **kw: None)

    async def successful_fix(*args, **kwargs):
        fixes.append(True)
        monkeypatch.setattr(coding_module, "_local_branch_head_sha", lambda *a: "b" * 40)
        remote["exact_unknown"] = probe == "unknown"
        if probe == "raises":
            def unavailable(*args, **kwargs):
                raise OSError("exact probe unavailable")
            monkeypatch.setattr(coding_module.gh_prs, "get_exact_pr_outcome", unavailable)
        if paused:
            runner._stop_requested = True
            runner.state.user_paused = True
        return 0, "", ""

    monkeypatch.setattr(h.claude_cli, "fix_review_async", successful_fix)
    asyncio.run(runner.handle_fix())
    attempt = runner.state.publication_attempt
    assert attempt.pending
    assert attempt.unverified_head_sha == "b" * 40
    assert attempt.head_sha == runner.state.current_pr.head_sha == "a" * 40
    assert runner.state.current_pr.fix_iteration_count == 0
    assert runner.state.current_pr.push_count == pushes_before
    assert runner.state.state == (PipelineState.PAUSED if paused else PipelineState.ERROR)
    assert posted == []

    # Restart with the old remote head available: neither hold reconciliation
    # nor another FIX may release ownership or invoke a second coder.
    runner.state = type(runner.state).model_validate_json(runner.state.model_dump_json())
    remote["exact_unknown"] = False
    monkeypatch.setattr(coding_module.gh_prs, "get_exact_pr_outcome", _REAL_EXACT_OUTCOME)
    assert not asyncio.run(runner._resolve_published_attempt())
    assert runner.state.publication_attempt.verified
    # Play can resume directly into WATCH. Its observation of the verified
    # old PR head must not consume the hold for the newer local FIX commit.
    runner._admit_observed_pr(runner.state.current_pr.model_copy())
    assert runner.state.publication_attempt.pending
    assert runner.state.publication_attempt.unverified_head_sha == "b" * 40
    asyncio.run(runner.handle_fix())
    assert fixes == [True]
    assert posted == []
    assert runner.state.publication_attempt.pending
    command = _command(_task_text(task_id="PR-001", branch="pr-001"),
                       task_id="PR-001", branch="pr-001", bound_pr=runner.state.current_pr)
    deferred = AsyncMock(return_value=RetryDispatch.ACTIVE_TASK_DEFERRED)
    monkeypatch.setattr(runner, "_defer_retry_command", deferred)
    assert asyncio.run(runner._select_retry_continuation(command)) == RetryDispatch.ACTIVE_TASK_DEFERRED
    deferred.assert_awaited_once()

    if pushed:
        remote["head"] = "b" * 40
    assert asyncio.run(runner._resolve_published_attempt()) is pushed
    assert runner.state.current_pr.head_sha == remote["head"]
    assert runner.state.publication_attempt.unverified_head_sha == (None if pushed else "b" * 40)
    if pushed:
        command.bound_pr_head_sha = "b" * 40
        continuation, pr = asyncio.run(runner._select_retry_continuation(command))
        assert continuation == "watch"
        assert pr.head_sha == "b" * 40
        runner._admit_observed_pr(pr)
        assert not runner.state.publication_attempt.pending


def test_deleted_branch_without_publication_baseline_keeps_hold(monkeypatch):
    runner = _runner(monkeypatch)
    remote = _publication_transport(monkeypatch, runner)
    remote["omit"] = True
    asyncio.run(runner.handle_coding())
    attempt = runner.state.publication_attempt
    attempt.baseline_numbers = None
    monkeypatch.setattr(coding_module, "_remote_branch_exists", lambda *a: False)
    asyncio.run(runner._release_unpublished_attempt(attempt.branch))
    assert attempt.pending


@pytest.mark.parametrize("local_head", [None, "a" * 40])
@pytest.mark.parametrize("terminal", [None, "MERGED", "CLOSED"])
def test_restart_recovers_publication_before_head_capture(monkeypatch, local_head, terminal):
    from src.keyspace import pipeline_state

    from tests.runner.test_retry_commands import _command, _task_text

    runner = _runner(monkeypatch)
    remote = _publication_transport(monkeypatch, runner, code=0, stderr="")
    # The coder publishes before the monitor gets a chance to observe its PR.
    remote["omit"] = True

    async def crash_before_capture(*args):
        raise asyncio.CancelledError("daemon stopped before capture")

    monkeypatch.setattr(runner, "_capture_published_attempt", crash_before_capture)
    with pytest.raises(asyncio.CancelledError, match="before capture"):
        asyncio.run(runner.handle_coding())
    saved = asyncio.run(runner.redis.get(pipeline_state(runner.name)))
    assert type(runner.state).model_validate_json(saved).publication_attempt.head_sha is None
    monkeypatch.setattr(coding_module, "_local_branch_head_sha", lambda *a: local_head)
    remote["omit"] = False
    remote["terminal"] = terminal
    if terminal is None:
        retry_runner = h._make_runner()
        retry_runner.state = type(runner.state).model_validate_json(saved)
        command = _command(_task_text(task_id="PR-001", branch="pr-001"),
                           task_id="PR-001", branch="pr-001")
        from src.daemon.retry_commands import RetryDispatch
        assert asyncio.run(retry_runner._select_retry_continuation(command)) == RetryDispatch.HANDLED
        assert retry_runner.state.publication_attempt.pending
    restarted = h._make_runner()
    restarted.redis = runner.redis
    assert asyncio.run(restarted.recover_state())
    attempt = restarted.state.publication_attempt
    assert attempt.head_sha == "a" * 40
    assert attempt.pr_number == 402
    assert attempt.verified
    assert restarted.state.state == PipelineState.ERROR
    assert restarted.state.current_task is not None
    assert attempt.pending
    assert attempt.exit_classification is None
    assert remote["calls"] == 1


@pytest.mark.parametrize("evidence", [
    "ambiguous", "missing", "unavailable", "bad_sha", "old", "baseline",
    "no_baseline", "base", "branch", "task", "cross_repo", "local_mismatch", "exact_unknown",
])
def test_headless_attempt_requires_bounded_unambiguous_exact_evidence(monkeypatch, evidence):
    from dataclasses import replace
    from datetime import timedelta

    runner = _runner(monkeypatch)
    remote = _publication_transport(monkeypatch, runner)
    asyncio.run(runner.handle_coding())
    attempt = runner.state.publication_attempt
    attempt.head_sha = attempt.pr_number = attempt.candidate = None
    runner.state.current_pr = None
    item = _REAL_PUBLICATIONS(attempt.repo, attempt.base, attempt.branch)[0]
    updates = {
        "bad_sha": {"head_sha": "invalid"},
        "old": {"created_at": attempt.started_at - timedelta(seconds=1)},
        "base": {"base_branch": "other"}, "branch": {"head_branch": "other"},
        "task": {"title": "PR-002: unrelated"}, "cross_repo": {"is_cross_repository": True},
    }
    items = [replace(item, **updates.get(evidence, {}))]
    if evidence == "ambiguous":
        items.append(replace(item, number=403))
    if evidence == "missing":
        items = []
    if evidence in {"baseline", "no_baseline"}:
        attempt.baseline_numbers = {402} if evidence == "baseline" else None
    remote["exact_unknown"] = evidence == "exact_unknown"
    monkeypatch.setattr(coding_module, "_local_branch_head_sha",
                        lambda *a: "b" * 40 if evidence == "local_mismatch" else None)

    def publications(*args):
        if evidence == "unavailable":
            raise OSError("discovery unavailable")
        return items

    monkeypatch.setattr(coding_module.gh_prs, "get_branch_publications", publications)
    assert not asyncio.run(runner._resolve_published_attempt())
    assert attempt.pending
    assert runner.state.current_pr is None
    # Exact-read outages retain the discovered identity for later reconciliation.
    remote["exact_unknown"] = False
    monkeypatch.setattr(coding_module.gh_prs, "get_branch_publications", _REAL_PUBLICATIONS)
    attempt.baseline_numbers = set()
    if evidence == "local_mismatch":
        attempt.head_sha = None
        monkeypatch.setattr(coding_module, "_local_branch_head_sha", lambda *a: None)
    assert asyncio.run(runner._resolve_published_attempt())
    assert runner.state.current_pr.number == 402
    assert remote["calls"] == 1


@pytest.mark.parametrize("local_head", [None, "a" * 40])
@pytest.mark.parametrize("terminal", [None, "MERGED", "CLOSED"])
def test_fallback_discovery_rejects_open_draft_but_reconciles_terminal(
    monkeypatch, local_head, terminal,
):
    runner = _runner(monkeypatch)
    remote = _publication_transport(monkeypatch, runner, code=0, stderr="")
    remote["update"] = {"isDraft": True, "state": terminal or "OPEN"}
    remote["terminal"] = terminal
    monkeypatch.setattr(coding_module, "_local_branch_head_sha", lambda *a: local_head)
    asyncio.run(runner.handle_coding())
    attempt = runner.state.publication_attempt
    if terminal:
        assert runner.state.state == PipelineState.IDLE
        assert not attempt.pending
        assert attempt.pr_number == 402
    else:
        assert runner.state.state == PipelineState.ERROR
        assert runner.state.current_pr is None
        assert attempt.pr_number is None
        assert attempt.pending
        remote["update"] = {}
        assert asyncio.run(runner._resolve_published_attempt())
        assert runner.state.current_pr.number == 402
    assert remote["calls"] == 1


def test_no_push_escalation_checkpoints_completed_attempt_before_clearing_task(monkeypatch):
    from src.keyspace import pipeline_state

    runner = _runner(monkeypatch)
    _publication_transport(monkeypatch, runner, code=0, stderr="")
    asyncio.run(runner.handle_coding())
    runner._app_config = h._app_cfg(fix_no_push_cap=1)
    h._patch_no_push_fix(monkeypatch, head_seq=lambda: "a" * 40)
    original_escalate = runner._escalate_fix_no_push_deadlock

    async def check_checkpoint(pr):
        saved = await runner.redis.get(pipeline_state(runner.name))
        checkpoint = type(runner.state).model_validate_json(saved)
        assert checkpoint.current_task is not None
        assert not checkpoint.publication_attempt.pending
        await original_escalate(pr)

    monkeypatch.setattr(runner, "_escalate_fix_no_push_deadlock", check_checkpoint)
    asyncio.run(runner.handle_fix())
    assert runner.state.state == PipelineState.IDLE
    assert runner.state.current_task is None
    assert not runner.state.publication_attempt.pending
    assert not asyncio.run(runner._hold_unresolved_publication())
    restarted = h._make_runner()
    restarted.redis = runner.redis
    assert asyncio.run(restarted.recover_state())
    assert restarted.state.state == PipelineState.IDLE
    assert not asyncio.run(restarted._hold_unresolved_publication())


@pytest.mark.parametrize("entrypoint", ["retry", "hold"])
@pytest.mark.parametrize("evidence", ["absent", "branch", "unknown", "pr", "outage", "cleanup", "active", "mismatch"])
def test_prelaunch_checkpoint_releases_only_proven_absent_attempt(monkeypatch, entrypoint, evidence):
    from src.keyspace import pipeline_state

    from tests.runner.test_retry_commands import _command, _task_text

    runner = _runner(monkeypatch)
    remote = _publication_transport(monkeypatch, runner, code=0, stderr="")
    saved = []

    async def crash_before_launch():
        saved.append(await runner.redis.get(pipeline_state(runner.name)))
        raise RuntimeError("crash before launch")

    monkeypatch.setattr(runner, "_checkpoint_current_run_record", crash_before_launch)
    with pytest.raises(RuntimeError, match="crash before launch"):
        asyncio.run(runner.handle_coding())
    assert remote["calls"] == 0
    restarted = h._make_runner()
    restarted.state = type(runner.state).model_validate_json(saved[0])
    assert restarted.state.publication_attempt.pending
    assert restarted.state.publication_attempt.head_sha is None
    cleanup = []

    async def confirm_cleanup(context):
        cleanup.append(context)
        return evidence != "cleanup"

    def branch_probe(*args):
        assert cleanup
        return {"branch": True, "unknown": None}.get(evidence, False)

    def publications(*args):
        if evidence == "outage":
            raise OSError("all-state read unavailable")
        if evidence == "pr":
            remote["created"] = True
            remote["update"] = {"title": "PR-999: unrelated publication"}
            return _REAL_PUBLICATIONS(*args)
        return []

    monkeypatch.setattr(restarted, "_confirm_current_coder_cleanup", confirm_cleanup)
    monkeypatch.setattr(coding_module, "_local_branch_head_sha", lambda *a: None)
    monkeypatch.setattr(coding_module, "_remote_branch_exists", branch_probe)
    monkeypatch.setattr(coding_module.gh_prs, "get_branch_publications", publications)
    monkeypatch.setattr(coding_module.gh_prs, "get_open_prs", lambda *a, **kw: [])
    monkeypatch.setattr(coding_module.gh_prs, "get_merged_prs", lambda *a, **kw: [])
    restarted._coder_invocation_active = evidence == "active"
    if evidence == "mismatch":
        restarted.state.publication_attempt.run_id = "other-run"
    if entrypoint == "retry":
        command = _command(_task_text(task_id="PR-001", branch="pr-001"),
                           task_id="PR-001", branch="pr-001")
        result = asyncio.run(restarted._select_retry_continuation(command))
        if evidence == "absent":
            assert result == ("coding", None)
    else:
        assert asyncio.run(restarted._hold_unresolved_publication()) is (evidence != "absent")
    assert restarted.state.publication_attempt.pending is (evidence != "absent")
    if evidence == "absent":
        checkpoint = asyncio.run(restarted.redis.get(pipeline_state(restarted.name)))
        assert not type(runner.state).model_validate_json(checkpoint).publication_attempt.pending
    assert remote["calls"] == 0


def test_play_after_stopped_unchanged_fix_resumes_without_coding_bookkeeping(monkeypatch):
    from src.keyspace import pipeline_state

    runner = _runner(monkeypatch)
    remote = _publication_transport(monkeypatch, runner, code=0, stderr="")
    asyncio.run(runner.handle_coding())
    runner._start_current_run_record("claude", "opus")

    async def stopped_fix(*args, **kwargs):
        runner._stop_requested = True
        runner.state.user_paused = True
        await runner.redis.set(pipeline_state(runner.name), runner.state.model_dump_json())
        raise asyncio.CancelledError

    monkeypatch.setattr(h.claude_cli, "fix_review_async", stopped_fix)
    asyncio.run(runner.handle_fix())
    assert runner.state.user_paused
    assert runner.state.publication_attempt.pending
    assert runner.state.active_invocation.phase == "fix"
    original_record = runner._current_run_record
    recorded = []
    monkeypatch.setattr(runner, "_save_current_run_record", lambda *a: recorded.append(a))
    monkeypatch.setattr(runner, "_post_codex_review", lambda *a: recorded.append(a))
    runner.state.user_paused = False
    runner.state.state = PipelineState.IDLE
    asyncio.run(runner.redis.set(pipeline_state(runner.name), runner.state.model_dump_json()))
    asyncio.run(runner.handle_idle())
    assert runner.state.state == PipelineState.ERROR
    assert runner.state.current_pr.head_sha == remote["head"] == "a" * 40
    assert runner.state.publication_attempt.pending
    assert runner._current_run_record is original_record
    assert recorded == []
    checkpoint = asyncio.run(runner.redis.get(pipeline_state(runner.name)))
    assert type(runner.state).model_validate_json(checkpoint).publication_attempt.pending


@pytest.mark.parametrize("local_state", ["absent", "branch", "commit"])
@pytest.mark.parametrize("blocker", [None, "dirty", "probe", "checkout", "delete", "base"])
def test_retry_cleans_unpublished_local_branch_before_release(monkeypatch, tmp_path, local_state, blocker):
    from src.keyspace import pipeline_state

    from tests.runner.test_retry_commands import _command, _task_text

    repo_path = tmp_path / "repo"
    repo_path.mkdir()

    def git(*args):
        return _REAL_GIT_RUN(
            ["git", "-c", "user.name=Test", "-c", "user.email=test@example.com", *args],
            cwd=repo_path, check=True, capture_output=True, text=True,
        ).stdout.strip()

    git("init", "-b", "main")
    git("commit", "--allow-empty", "-m", "base")
    base_sha = git("rev-parse", "HEAD")
    git("update-ref", "refs/remotes/origin/main", base_sha)
    if local_state != "absent":
        git("checkout", "-b", "pr-001")
        if local_state == "commit":
            git("commit", "--allow-empty", "-m", "unpublished task")
    runner = _runner(monkeypatch)
    remote = _publication_transport(monkeypatch, runner)
    remote["omit"] = True
    asyncio.run(runner.handle_coding())
    assert runner.state.publication_attempt.pending
    runner.repo_path = str(repo_path)
    monkeypatch.setattr(subprocess, "run", _REAL_GIT_RUN)
    monkeypatch.setattr(coding_module, "_local_branch_head_sha", _REAL_LOCAL_HEAD)
    monkeypatch.setattr(coding_module, "_initial_branch_head_sha", _REAL_INITIAL_HEAD)
    monkeypatch.setattr(coding_module, "_remote_branch_exists", lambda *a: False)
    monkeypatch.setattr(coding_module.gh_prs, "get_open_prs", lambda *a, **kw: [])
    monkeypatch.setattr(coding_module.gh_prs, "get_merged_prs", lambda *a, **kw: [])
    attempt = runner.state.publication_attempt
    # The temporary ref represents work created by this attempt after its
    # recorded pre-launch absence probe.
    attempt.initial_branch_head_sha = ""
    if local_state != "absent":
        attempt.head_sha = git("rev-parse", "refs/heads/pr-001")
    if blocker == "dirty":
        (repo_path / "operator.txt").write_text("preserve me")
    if blocker == "base":
        runner.repo_config.branch = "pr-001"
        attempt.base = "pr-001"
    real_git = git_ops._git

    def checked_git(repo_path, *args, **kwargs):
        if blocker == "probe" and args[0] == "rev-parse":
            return subprocess.CompletedProcess(args, 128, "", "probe failed")
        if (blocker == "checkout" and args[0] == "checkout") or (
            blocker == "delete" and args[0] == "branch"
        ):
            raise subprocess.CalledProcessError(1, args, stderr="cleanup failed")
        return real_git(repo_path, *args, **kwargs)

    monkeypatch.setattr(git_ops, "_git", checked_git)
    command = _command(_task_text(task_id="PR-001", branch="pr-001"),
                       task_id="PR-001", branch="pr-001")
    blocked = blocker in {"dirty", "probe", "base"} or (
        local_state != "absent" and blocker in {"checkout", "delete"}
    )
    # Exercise the production Retry selection, including recovery cleanup.
    result = asyncio.run(runner._select_retry_continuation(command))
    assert attempt.pending is blocked, runner.state.history
    if blocked:
        assert result != ("coding", None)
        if blocker == "dirty":
            assert (repo_path / "operator.txt").read_text() == "preserve me"
        assert any("local branch cleanup deferred" in item["event"] for item in runner.state.history)
        return
    assert result == ("coding", None)
    saved = asyncio.run(runner.redis.get(pipeline_state(runner.name)))
    assert not type(runner.state).model_validate_json(saved).publication_attempt.pending
    # The AUTO PR branch-creation command must succeed, including when the
    # abandoned branch had a clean unpublished commit or was checked out.
    git("checkout", "-b", "pr-001", "origin/main")
    assert git("rev-parse", "HEAD") == base_sha


@pytest.mark.parametrize("pushed", [False, True])
def test_restart_retains_fix_commit_created_before_capture(monkeypatch, pushed):
    from src.keyspace import pipeline_state

    runner = _runner(monkeypatch)
    remote = _publication_transport(monkeypatch, runner, code=0, stderr="")
    asyncio.run(runner.handle_coding())

    async def fix(*args, **kwargs):
        monkeypatch.setattr(coding_module, "_local_branch_head_sha", lambda *a: "b" * 40)
        return 0, "", ""

    async def crash(*args):
        raise asyncio.CancelledError("crash before FIX capture")

    monkeypatch.setattr(h.claude_cli, "fix_review_async", fix)
    monkeypatch.setattr(runner, "_capture_published_attempt", crash)
    with pytest.raises(asyncio.CancelledError, match="before FIX capture"):
        asyncio.run(runner.handle_fix())
    saved = asyncio.run(runner.redis.get(pipeline_state(runner.name)))
    assert type(runner.state).model_validate_json(saved).publication_attempt.head_sha == "a" * 40
    restarted = h._make_runner()
    restarted.redis = runner.redis
    assert asyncio.run(restarted.recover_state())
    attempt = restarted.state.publication_attempt
    assert restarted.state.state == PipelineState.ERROR
    assert attempt.pending
    assert attempt.head_sha == "a" * 40
    assert attempt.unverified_head_sha == "b" * 40
    checkpoint = asyncio.run(restarted.redis.get(pipeline_state(restarted.name)))
    assert type(runner.state).model_validate_json(checkpoint).publication_attempt.unverified_head_sha == "b" * 40
    if pushed:
        remote["head"] = "b" * 40
    assert asyncio.run(restarted._resolve_published_attempt()) is pushed
    assert attempt.pending
    assert attempt.unverified_head_sha == (None if pushed else "b" * 40)
    assert remote["calls"] == 1


@pytest.mark.parametrize("code,stdout", [(0, "ok"), (1, "failed"), (0, "gh repo create unsafe")])
def test_unclassified_coding_exit_cannot_resume_after_capture_crash(monkeypatch, code, stdout):
    runner = _runner(monkeypatch)
    remote = _publication_transport(monkeypatch, runner, code=code, stdout=stdout)

    async def crash(*args, **kwargs):
        raise RuntimeError("before classification")

    monkeypatch.setattr(runner, "_post_coder_resolution", crash)
    with pytest.raises(RuntimeError, match="before classification"):
        asyncio.run(runner.handle_coding())
    restarted = h._make_runner()
    restarted.redis = runner.redis
    assert asyncio.run(restarted.recover_state())
    assert restarted.state.current_pr.number == 402
    assert restarted.state.state == PipelineState.ERROR
    assert restarted.state.publication_attempt.pending
    assert restarted.state.publication_attempt.exit_classification is None
    assert remote["calls"] == 1


@pytest.mark.parametrize("stopped", [False, True])
@pytest.mark.parametrize("legacy_suppression", [False, True])
def test_guardrail_suppression_precedes_delayed_pr_discovery(monkeypatch, stopped, legacy_suppression):
    from src.subsource_registry import SuppressionReason

    from tests.runner.test_retry_commands import _command, _task_text

    runner = _runner(monkeypatch)
    remote = _publication_transport(monkeypatch, runner, stdout="gh repo create unsafe")
    remote["omit"] = True
    _patch_branch_state(monkeypatch, local_exists=True, remote_exists=True)
    applied = []
    monkeypatch.setattr(coding_module, "apply_quarantine_label_for_violation",
                        lambda runner, number, violation: applied.append((number, violation.category)))
    if stopped:
        original = runner._post_coder_resolution

        async def stop_then_classify(*args, **kwargs):
            runner._stop_requested = True
            await original(*args, **kwargs)

        monkeypatch.setattr(runner, "_post_coder_resolution", stop_then_classify)
    asyncio.run(runner.handle_coding())
    suppression = asyncio.run(runner._suppression_record_for_task("PR-001"))
    assert suppression.reason == SuppressionReason.GUARDRAIL
    assert runner.state.publication_attempt.pr_number is None
    if legacy_suppression:
        asyncio.run(runner._suppress_task("PR-001", SuppressionReason.GUARDRAIL))
    remote["omit"] = False
    assert asyncio.run(runner._resolve_published_attempt())
    assert runner.state.quarantined_prs == {402}
    assert applied == ([] if legacy_suppression else [(402, "repo_create")])
    command = _command(_task_text(task_id="PR-001", branch="pr-001"),
                       task_id="PR-001", branch="pr-001")
    assert asyncio.run(runner._retry_blocker(command)) is not None
    assert runner.state.publication_attempt.pending


@pytest.mark.parametrize("blocker", [
    None, "cleanup", "suppression", "absent", "unknown", "publication", "outage",
    "create", "unclassified", "active", "mismatch", "no_baseline", "no_head",
    "remote_drift", "malformed_remote", "baseline_closed", "baseline_draft",
    "preexisting_head", "changed_head", "unknown_initial_head", "late_breach",
])
def test_retry_recovers_pushed_branch_without_pr(monkeypatch, blocker):
    from src.subsource_registry import SuppressionReason

    from tests.runner.test_retry_commands import _command, _task_text

    runner = _runner(monkeypatch)
    remote = _publication_transport(monkeypatch, runner)
    remote["omit"] = True
    _patch_branch_state(monkeypatch, local_exists=True, remote_exists=True)
    initial_head = {"preexisting_head": "a" * 40, "changed_head": "b" * 40,
                    "unknown_initial_head": None}.get(blocker, "")
    monkeypatch.setattr(coding_module, "_initial_branch_head_sha", lambda *a: initial_head)
    if blocker == "late_breach":
        def late_breach(*args):
            args[-1]["breached"] = True
        monkeypatch.setattr(runner, "_check_late_breach", late_breach)
    asyncio.run(runner.handle_coding())
    attempt = runner.state.publication_attempt
    assert attempt.exit_classification == "failure"
    # Repeated normal reconciliation cannot release or rerun the coder.
    assert not asyncio.run(runner._resolve_published_attempt())
    assert not asyncio.run(runner._resolve_published_attempt())
    creates = []
    discovery = []
    async def cleanup(*args):
        return blocker != "cleanup"

    monkeypatch.setattr(runner, "_confirm_current_coder_cleanup", cleanup)
    real_git = git_ops._git

    def git_transport(repo_path, *args, **kwargs):
        if args[:1] != ("ls-remote",):
            return real_git(repo_path, *args, **kwargs)
        if blocker == "unknown":
            raise OSError("remote unavailable")
        sha = "b" * 40 if blocker == "remote_drift" else "a" * 40
        output = f"{sha}\trefs/heads/pr-001\n"
        if blocker == "malformed_remote":
            output = f"{sha}\trefs/heads/unrelated\n"
        return subprocess.CompletedProcess(
            args, 2 if blocker == "absent" else 0, stdout=output, stderr="",
        )

    monkeypatch.setattr(git_ops, "_git", git_transport)
    if blocker == "suppression":
        asyncio.run(runner._suppress_task("PR-001", SuppressionReason.GUARDRAIL))
    if blocker == "unclassified":
        attempt.exit_classification = None
    if blocker == "active":
        runner._coder_invocation_active = True
    if blocker == "mismatch":
        attempt.run_id = "wrong-run"
    if blocker == "no_baseline":
        attempt.baseline_numbers = None
    if blocker == "no_head":
        attempt.head_sha = None
        monkeypatch.setattr(coding_module, "_local_branch_head_sha", lambda *a: None)
    real_transport = gh_runner.run_gh
    if blocker in {"baseline_closed", "baseline_draft"}:
        attempt.baseline_numbers = {401}

    def transport(args, **kwargs):
        if args[:2] == ["pr", "create"]:
            creates.append(args)
            if blocker == "create":
                raise RuntimeError("create unavailable")
            remote["omit"] = False
            return "https://github.com/octo/demo/pull/402"
        discovery.append(args)
        # First lookup is reconciliation, second is fresh pre-create discovery.
        if len(discovery) == 2 and blocker in {"outage", "publication"}:
            if blocker == "outage":
                raise OSError("all-state unavailable")
            remote["omit"] = False
        publications = real_transport(args, **kwargs)
        if blocker in {"baseline_closed", "baseline_draft"}:
            publications.append({
                "number": 401, "title": "PR-001: historical", "baseRefName": "main",
                "headRefName": "pr-001", "headRefOid": "a" * 40,
                "state": "CLOSED" if blocker == "baseline_closed" else "OPEN",
                "isDraft": blocker == "baseline_draft", "isCrossRepository": False,
                "createdAt": "2020-01-01T00:00:00Z",
                "url": "https://github.com/octo/demo/pull/401",
            })
        return publications

    monkeypatch.setattr(gh_runner, "run_gh", transport)
    command = _command(_task_text(task_id="PR-001", branch="pr-001"),
                       task_id="PR-001", branch="pr-001")
    result = asyncio.run(runner._select_retry_continuation(command))
    if blocker in {None, "baseline_closed", "baseline_draft", "changed_head", "late_breach"}:
        assert result[0] == "watch"
        assert result[1].head_sha == "a" * 40
        assert attempt.verified
        assert creates[0][:2] == ["pr", "create"]
        assert creates[0][creates[0].index("--head") + 1] == "pr-001"
        assert creates[0][creates[0].index("--base") + 1] == "main"
    else:
        assert result != ("coding", None)
        assert len(creates) == (1 if blocker == "create" else 0)
    assert attempt.pending is (blocker != "absent")
    assert remote["calls"] == 1


def test_restart_after_successful_exit_classification_resumes_watch(monkeypatch):
    from src.keyspace import pipeline_state

    runner = _runner(monkeypatch)
    remote = _publication_transport(monkeypatch, runner, code=0, stderr="")

    async def crash_before_handoff(*args, **kwargs):
        raise RuntimeError("crash after successful classification")

    monkeypatch.setattr(runner, "_handoff_coding_pr_to_watch", crash_before_handoff)
    with pytest.raises(RuntimeError, match="after successful classification"):
        asyncio.run(runner.handle_coding())
    saved = asyncio.run(runner.redis.get(pipeline_state(runner.name)))
    checkpoint = type(runner.state).model_validate_json(saved)
    assert checkpoint.publication_attempt.exit_classification == "success"
    assert checkpoint.publication_attempt.pending
    remote["omit"] = True
    restarted = h._make_runner()
    restarted.redis = runner.redis
    reviews = []
    restarted._post_codex_review = lambda number: reviews.append(number) or True
    assert asyncio.run(restarted.recover_state())
    assert restarted.state.state == PipelineState.WATCH
    assert restarted.state.current_pr.number == 402
    assert not restarted.state.publication_attempt.pending
    assert reviews == [402]
    assert remote["calls"] == 1


@pytest.mark.parametrize("phase", ["coding", "fix"])
@pytest.mark.parametrize("visible", [False, True])
@pytest.mark.parametrize("status_context", [False, True])
@pytest.mark.parametrize("restart", [False, True])
def test_error_retries_status_write_before_retained_publication(
    monkeypatch, tmp_path, phase, visible, status_context, restart,
):
    from dataclasses import replace

    from src.subsource_registry import SuppressionReason

    runner = _runner(monkeypatch)
    remote = _publication_transport(monkeypatch, runner)
    asyncio.run(runner.handle_coding())
    attempt = runner.state.publication_attempt
    runner.state.active_invocation = replace(runner.state.active_invocation, phase=phase)
    remote["omit"] = remote["exact_unknown"] = not visible
    task = runner.state.current_task
    task_path = tmp_path / "task.md"
    task_path.write_text("status: DOING\n")
    if status_context:
        runner.state.error_message = "crash: status:ERROR write failed; operator Retry required"
    asyncio.run(runner._mark_status_write_failed_task(
        task, blocked_reason=SuppressionReason.CRASH,
    ))
    if restart:
        asyncio.run(runner.publish_state())
        restarted = h._make_runner()
        restarted.redis = runner.redis
        assert not restarted._status_write_failed_task_pr_ids
        monkeypatch.setattr(coding_module.gh_prs, "get_open_prs", lambda *a, **kw: [])
        assert asyncio.run(restarted.recover_state())
        runner = restarted
        task = runner.state.current_task
        attempt = runner.state.publication_attempt
        assert task.pr_id in runner._status_write_failed_task_pr_ids
        assert task_path.read_text() == "status: DOING\n"
        assert runner.state.state == PipelineState.ERROR
        assert attempt.pending
    writes = []

    async def commit(current_task, status, context, *, blocked_reason):
        assert current_task is task
        assert status == "ERROR"
        assert blocked_reason == SuppressionReason.CRASH
        writes.append(status)
        if len(writes) == 1:
            return False
        task_path.write_text(f"status: {status}\n")
        return True

    monkeypatch.setattr(runner, "_commit_task_status_change", commit)
    asyncio.run(runner.handle_error())
    assert task.pr_id in runner._status_write_failed_task_pr_ids
    assert task_path.read_text() == "status: DOING\n"
    asyncio.run(runner.handle_error())
    assert task_path.read_text() == "status: ERROR\n"
    assert task.pr_id not in runner._status_write_failed_task_pr_ids
    assert f"status_write_failed_tasks:{runner.name}" not in runner.redis.store
    asyncio.run(runner.handle_error())
    assert writes == ["ERROR", "ERROR"]
    assert runner.state.state == PipelineState.ERROR
    assert runner.state.current_task is task
    assert attempt.pending
    assert remote["calls"] == 1


@pytest.mark.parametrize("code", [0, 1])
def test_published_escalation_remains_retryable_after_restart(monkeypatch, code):
    from src.keyspace import pipeline_state

    from tests.runner.test_retry_commands import _command, _task_text

    runner = _runner(monkeypatch)
    remote = _publication_transport(
        monkeypatch, runner, code=code, stdout="ESCALATE: operator recovery needed",
    )
    asyncio.run(runner.handle_coding())
    assert runner.state.state == PipelineState.ERROR
    assert runner.state.error_message == "operator recovery needed"
    saved = asyncio.run(runner.redis.get(pipeline_state(runner.name)))
    checkpoint = type(runner.state).model_validate_json(saved)
    assert checkpoint.publication_attempt.exit_classification == "failure"
    assert checkpoint.publication_attempt.pending
    assert checkpoint.publication_attempt.pr_number == 402

    restarted = h._make_runner()
    restarted.redis = runner.redis
    assert asyncio.run(restarted.recover_state())
    # An escalation stays parked until the operator explicitly retries.
    assert restarted.state.state == PipelineState.ERROR
    assert restarted.state.publication_attempt.pending
    command = _command(_task_text(task_id="PR-001", branch="pr-001"),
                       task_id="PR-001", branch="pr-001")
    result = asyncio.run(restarted._select_retry_continuation(command))
    assert result[0] == "watch"
    assert result[1].number == 402
    assert result[1].head_sha == "a" * 40
    assert remote["calls"] == 1


@pytest.mark.parametrize("probe", ["absent", "present", "malformed", "failed", "exception"])
def test_initial_branch_snapshot_distinguishes_absence_and_uncertainty(monkeypatch, probe):
    def git_probe(*args, **kwargs):
        assert args[1:] == ("rev-parse", "--verify", "--quiet", "refs/heads/pr-001^{commit}")
        if probe == "exception":
            raise OSError("git unavailable")
        return subprocess.CompletedProcess(
            args, {"absent": 1, "failed": 128}.get(probe, 0),
            "short" if probe == "malformed" else "a" * 40, "",
        )

    monkeypatch.setattr(git_ops, "_git", git_probe)
    expected = {"absent": "", "present": "a" * 40}.get(probe)
    assert coding_module._initial_branch_head_sha("/repo", "pr-001") == expected


def test_initial_branch_snapshot_uses_real_git_refs(tmp_path):
    def git(*args):
        return _REAL_GIT_RUN(
            ["git", "-c", "user.name=Test", "-c", "user.email=test@example.com", *args],
            cwd=tmp_path, check=True, capture_output=True, text=True,
        ).stdout.strip()

    git("init", "-b", "main")
    git("commit", "--allow-empty", "-m", "base")
    assert coding_module._initial_branch_head_sha(str(tmp_path), "pr-001") == ""
    git("checkout", "-b", "pr-001")
    git("commit", "--allow-empty", "-m", "older attempt")
    old_head = git("rev-parse", "HEAD")
    assert coding_module._initial_branch_head_sha(str(tmp_path), "pr-001") == old_head
    # This is the required AUTO PR launch command when an older ref remains.
    result = _REAL_GIT_RUN(
        ["git", "checkout", "-b", "pr-001", "main"], cwd=tmp_path,
        capture_output=True, text=True,
    )
    assert result.returncode != 0
    assert coding_module._local_branch_head_sha(str(tmp_path), "pr-001") == old_head


@pytest.mark.parametrize(
    ("ownership", "released"),
    [
        ("preexisting", False),
        ("owned", True),
        ("prelaunch", False),
        ("unknown", False),
        ("changed", False),
    ],
)
def test_unpublished_cleanup_preserves_unowned_git_refs_after_restart(
    monkeypatch, tmp_path, ownership, released,
):
    from datetime import datetime, timezone

    from src.models import PublicationAttempt

    repo_path = tmp_path / "repo"
    repo_path.mkdir()

    def git(*args):
        return _REAL_GIT_RUN(
            ["git", "-c", "user.name=Test", "-c", "user.email=test@example.com", *args],
            cwd=repo_path, check=True, capture_output=True, text=True,
        ).stdout.strip()

    git("init", "-b", "main")
    git("commit", "--allow-empty", "-m", "base")
    git("update-ref", "refs/remotes/origin/main", git("rev-parse", "HEAD"))
    if ownership in {"preexisting", "prelaunch"}:
        git("checkout", "-b", "pr-001")
        git("commit", "--allow-empty", "-m", "earlier unpublished work")
        initial_head = git("rev-parse", "HEAD")
    else:
        initial_head = "" if ownership != "unknown" else None
        git("checkout", "-b", "pr-001")
        git("commit", "--allow-empty", "-m", "current attempt work")
    captured_head = git("rev-parse", "refs/heads/pr-001")
    if ownership == "changed":
        git("commit", "--allow-empty", "-m", "later unowned work")
    preserved_head = git("rev-parse", "refs/heads/pr-001")

    runner = _runner(monkeypatch)
    runner.repo_path = str(repo_path)
    monkeypatch.setattr(subprocess, "run", _REAL_GIT_RUN)
    monkeypatch.setattr(coding_module, "_initial_branch_head_sha", _REAL_INITIAL_HEAD)
    monkeypatch.setattr(coding_module, "_remote_branch_exists", lambda *a: False)
    monkeypatch.setattr(coding_module.gh_prs, "get_branch_publications", lambda *a: [])
    runner.state.publication_attempt = PublicationAttempt(
        repo=runner.owner_repo, base="main", task_id="PR-001", branch="pr-001",
        run_id="attempt", attempt_index=1, started_at=datetime.now(timezone.utc),
        baseline_numbers=set(), initial_branch_head_sha=initial_head,
        head_sha=None if ownership == "prelaunch" else captured_head,
    )
    # A daemon restart keeps the original pre-launch snapshot, even when its
    # local Git ref has since changed or the launch never happened.
    runner.state = type(runner.state).model_validate_json(runner.state.model_dump_json())
    for _ in range(2):
        asyncio.run(runner._release_unpublished_attempt("pr-001"))
        assert runner.state.publication_attempt.pending is not released
        if not released:
            assert git("rev-parse", "refs/heads/pr-001") == preserved_head
        else:
            assert _REAL_INITIAL_HEAD(str(repo_path), "pr-001") == ""


def test_retained_terminal_publication_survives_failed_status_write(monkeypatch):
    runner = _runner(monkeypatch)
    remote = _publication_transport(monkeypatch, runner)
    asyncio.run(runner.handle_coding())
    remote["terminal"] = "MERGED"
    writes = []

    async def write_status(*args, **kwargs):
        writes.append(True)
        return len(writes) > 1

    monkeypatch.setattr(runner, "_commit_task_status_change", write_status)
    assert not asyncio.run(runner._resolve_published_attempt())
    assert runner.state.publication_attempt.pending
    assert runner.state.current_task is not None
    assert asyncio.run(runner._resolve_published_attempt())
    assert not runner.state.publication_attempt.pending
    assert runner.state.current_task is None


def test_terminal_publish_rollback_keeps_live_publication_hold(monkeypatch):
    from unittest.mock import AsyncMock

    from src.daemon.retry_commands import RetryDispatch

    from tests.runner.test_retry_commands import _command, _task_text

    runner = _runner(monkeypatch)
    remote = _publication_transport(monkeypatch, runner)
    asyncio.run(runner.handle_coding())
    remote["terminal"] = "CLOSED"
    original_publish = runner.publish_state
    failures = []

    async def fail_terminal_publish():
        if runner.state.current_task is None and len(failures) < 2:
            failures.append(True)
            raise OSError("state checkpoint unavailable")
        await original_publish()

    monkeypatch.setattr(runner, "publish_state", fail_terminal_publish)
    assert asyncio.run(runner._hold_unresolved_publication())
    assert failures
    assert runner.state.current_task is not None
    assert runner.state.publication_attempt.pending
    assert runner.state.state == PipelineState.ERROR
    command = _command(_task_text(task_id="PR-001", branch="pr-001"),
                       task_id="PR-001", branch="pr-001")
    defer = AsyncMock(return_value=RetryDispatch.HANDLED)
    monkeypatch.setattr(runner, "_defer_retry_command", defer)
    assert asyncio.run(runner._select_retry_continuation(command)) == RetryDispatch.HANDLED
    assert runner.state.publication_attempt.pending
    defer.assert_awaited_once()
    assert asyncio.run(runner._hold_unresolved_publication())
    assert runner.state.current_task is None
    assert not runner.state.publication_attempt.pending


@pytest.mark.parametrize("scenario", ["clean", "dirty", "base_ahead", "upload_failure"])
@pytest.mark.parametrize("entry", ["idle", "error"])
def test_retained_publication_allows_safe_staged_upload_without_losing_task_ref(
    monkeypatch, tmp_path, scenario, entry,
):
    import json
    from datetime import datetime, timezone
    from unittest.mock import AsyncMock

    from src.keyspace import upload_pending
    from src.models import PublicationAttempt

    repo_path = tmp_path / "repo"
    remote_path = tmp_path / "remote.git"
    staging_dir = tmp_path / "staged"
    repo_path.mkdir()
    staging_dir.mkdir()

    def git(*args, cwd=repo_path):
        return _REAL_GIT_RUN(
            ["git", *args], cwd=cwd, check=True, capture_output=True, text=True,
        ).stdout.strip()

    git("init", "--bare", str(remote_path), cwd=tmp_path)
    git("init", "-b", "main")
    git("config", "user.name", "Test")
    git("config", "user.email", "test@example.com")
    git("commit", "--allow-empty", "-m", "base")
    git("remote", "add", "origin", str(remote_path))
    git("push", "-u", "origin", "main")
    if scenario == "base_ahead":
        git("commit", "--allow-empty", "-m", "unpublished base work")
        base_head = git("rev-parse", "HEAD")
    git("checkout", "-b", "pr-001")
    git("commit", "--allow-empty", "-m", "unpublished task work")
    task_head = git("rev-parse", "HEAD")
    if scenario == "dirty":
        (repo_path / "operator.txt").write_text("keep uncommitted work")
    (staging_dir / "PR-001.md").write_text("revised task specification\n")

    runner = _runner(monkeypatch)
    runner.repo_path = str(repo_path)
    monkeypatch.setattr(subprocess, "run", _REAL_GIT_RUN)
    runner.state.publication_attempt = PublicationAttempt(
        repo=runner.owner_repo, base="main", task_id="PR-001", branch="pr-001",
        run_id="retained", attempt_index=1, started_at=datetime.now(timezone.utc),
        baseline_numbers=set(), initial_branch_head_sha="", head_sha=task_head,
    )
    monkeypatch.setattr(runner, "_hold_unresolved_publication", AsyncMock(return_value=True))
    monkeypatch.setattr(runner, "_confirm_current_coder_cleanup", AsyncMock(return_value=True))
    if scenario == "upload_failure":
        monkeypatch.setattr(runner, "process_pending_uploads", AsyncMock(return_value=None))
    asyncio.run(runner.redis.set(upload_pending(runner.name), json.dumps({
        "staging_dir": str(staging_dir), "files": ["PR-001.md"],
    })))
    if entry == "idle":
        asyncio.run(runner.handle_idle())
    else:
        runner.state.state = PipelineState.ERROR
        asyncio.run(runner.handle_error("coder failed"))

    assert git("rev-parse", "refs/heads/pr-001") == task_head
    assert runner.state.publication_attempt.pending
    if scenario == "dirty":
        assert git("rev-parse", "--abbrev-ref", "HEAD") == "pr-001"
        assert (repo_path / "operator.txt").read_text() == "keep uncommitted work"
        assert asyncio.run(runner.redis.get(upload_pending(runner.name))) is not None
    elif scenario in {"base_ahead", "upload_failure"}:
        assert git("rev-parse", "--abbrev-ref", "HEAD") == "main"
        if scenario == "base_ahead":
            assert git("rev-parse", "HEAD") == base_head
        assert asyncio.run(runner.redis.get(upload_pending(runner.name))) is not None
    else:
        assert git("rev-parse", "--abbrev-ref", "HEAD") == "main"
        uploaded = git("show", "main:tasks/PR-001.md", cwd=remote_path)
        assert uploaded == "revised task specification"
        assert asyncio.run(runner.redis.get(upload_pending(runner.name))) is None


@pytest.mark.parametrize("failure", ["redis", "cleanup"])
def test_retained_upload_waits_for_evidence_before_git(monkeypatch, failure):
    from datetime import datetime, timezone
    from unittest.mock import AsyncMock

    from src.keyspace import upload_pending
    from src.models import PublicationAttempt

    runner = _runner(monkeypatch)
    runner.state.publication_attempt = PublicationAttempt(
        repo=runner.owner_repo, base="main", task_id="PR-001", branch="pr-001",
        run_id="retained", attempt_index=1, started_at=datetime.now(timezone.utc),
        baseline_numbers=set(), initial_branch_head_sha="", head_sha="a" * 40,
    )
    monkeypatch.setattr(runner, "_hold_unresolved_publication", AsyncMock(return_value=True))
    monkeypatch.setattr(git_ops, "_git", lambda *a, **kw: pytest.fail("Git must not run"))
    if failure == "redis":
        monkeypatch.setattr(runner.redis, "get", AsyncMock(side_effect=OSError("Redis unavailable")))
    else:
        asyncio.run(runner.redis.set(upload_pending(runner.name), "staged"))
        monkeypatch.setattr(runner, "_confirm_current_coder_cleanup", AsyncMock(return_value=False))
    asyncio.run(runner.handle_idle())
    assert runner.state.publication_attempt.pending


def test_crash_after_fix_capture_keeps_unclassified_publication_held(monkeypatch):
    from src.keyspace import pipeline_state

    from tests.runner.test_retry_commands import _command, _task_text

    runner = _runner(monkeypatch)
    remote = _publication_transport(monkeypatch, runner, code=0, stderr="")
    asyncio.run(runner.handle_coding())

    async def failed_fix(*args, **kwargs):
        remote["head"] = "b" * 40
        monkeypatch.setattr(coding_module, "_local_branch_head_sha", lambda *a: "b" * 40)
        return 1, "gh repo create unsafe", "coder failed"

    capture = runner._capture_published_attempt

    async def crash_after_capture(*args, **kwargs):
        await capture(*args, **kwargs)
        raise asyncio.CancelledError("daemon stopped after FIX capture")

    monkeypatch.setattr(h.claude_cli, "fix_review_async", failed_fix)
    monkeypatch.setattr(runner, "_capture_published_attempt", crash_after_capture)
    with pytest.raises(asyncio.CancelledError, match="after FIX capture"):
        asyncio.run(runner.handle_fix())
    saved = asyncio.run(runner.redis.get(pipeline_state(runner.name)))
    checkpoint = type(runner.state).model_validate_json(saved)
    assert checkpoint.publication_attempt.pending
    assert checkpoint.publication_attempt.exit_classification is None

    restarted = h._make_runner()
    restarted.redis = runner.redis
    assert asyncio.run(restarted.recover_state())
    assert restarted.state.state == PipelineState.ERROR
    assert restarted.state.publication_attempt.pending
    assert restarted.state.publication_attempt.exit_classification is None
    command = _command(_task_text(task_id="PR-001", branch="pr-001"),
                       task_id="PR-001", branch="pr-001", bound_pr=restarted.state.current_pr)
    result = asyncio.run(restarted._select_retry_continuation(command))
    assert result != ("watch", restarted.state.current_pr)
    assert restarted.state.publication_attempt.pending


def test_watch_admission_keeps_failed_publication_until_retry(monkeypatch):
    runner = _runner(monkeypatch)
    _publication_transport(monkeypatch, runner)
    asyncio.run(runner.handle_coding())
    attempt = runner.state.publication_attempt
    assert attempt.pending and attempt.verified
    # A failed invocation requires operator Retry even when a later WATCH
    # observation matches the retained PR exactly.
    runner._admit_observed_pr(runner.state.current_pr.model_copy())
    assert attempt.pending
    assert attempt.failure == "coder failed"


@pytest.mark.parametrize("stop_path", ["cancelled", "exited"])
@pytest.mark.parametrize("delayed", [False, True])
def test_stopped_published_coding_requires_retry_before_watch(monkeypatch, stop_path, delayed):
    from src.daemon.retry_commands import RetryDispatch

    from tests.runner.test_retry_commands import _command, _task_text

    runner = _runner(monkeypatch)
    remote = _publication_transport(monkeypatch, runner, code=0, stderr="")
    remote["omit"] = delayed

    async def stopped(*args, **kwargs):
        remote["created"] = True
        runner._stop_requested = True
        runner.state.user_paused = True
        if stop_path == "cancelled":
            raise asyncio.CancelledError
        return 0, "", ""

    monkeypatch.setattr(h.claude_cli, "run_auto_pr_async", stopped)
    asyncio.run(runner.handle_coding())
    attempt = runner.state.publication_attempt
    assert runner.state.state == PipelineState.PAUSED
    assert attempt.pending and attempt.exit_classification == "failure"
    remote["omit"] = False
    runner._stop_requested = False
    runner.state.user_paused = False
    asyncio.run(runner.handle_paused())
    assert runner.state.state == PipelineState.ERROR
    assert attempt.pending
    command = _command(_task_text(task_id="PR-001", branch="pr-001"),
                       task_id="PR-001", branch="pr-001")
    result = asyncio.run(runner._select_retry_continuation(command))
    assert not isinstance(result, RetryDispatch)
    assert result[0] == "watch" and result[1].number == 402


@pytest.mark.parametrize("failure", ["binding", "binding_missing", "reservation", "status", "cleanup", "ack"])
def test_retained_retry_durably_binds_before_admission_can_defer(monkeypatch, failure):
    from src.daemon import retry_commands as retry_module
    from src.daemon.retry_commands import RetryDispatch
    from src.retry_commands import enqueue_retry_command, load_retry_command

    from tests.runner.test_retry_commands import _command, _task_text

    runner = _runner(monkeypatch)
    _publication_transport(monkeypatch, runner)
    asyncio.run(runner.handle_coding())
    command = _command(_task_text(task_id="PR-001", branch="pr-001"),
                       task_id="PR-001", branch="pr-001")
    # Use the actual daemon namespace for the durable command.
    command.repo_slug = runner.name
    asyncio.run(enqueue_retry_command(runner.redis, command))
    task = runner.state.current_task

    async def validate(*args):
        return object(), task

    async def no_blocker(*args):
        return None

    async def fail(*args, **kwargs):
        raise RuntimeError("transient failure")

    async def commit(*args):
        return failure != "status"

    monkeypatch.setattr(runner, "_validate_retry_command", validate)
    monkeypatch.setattr(runner, "_retry_blocker", no_blocker)
    monkeypatch.setattr(runner, "_commit_task_status_change", commit)
    if failure in {"binding", "binding_missing"}:
        real_update = retry_module.update_retry_command

        async def fail_binding(*args, **kwargs):
            if kwargs.get("transition_reason") == "Bound Retry to the retained publication.":
                if failure == "binding_missing":
                    return None
                raise RuntimeError("transient binding failure")
            return await real_update(*args, **kwargs)

        monkeypatch.setattr(retry_module, "update_retry_command", fail_binding)
    elif failure == "reservation":
        monkeypatch.setattr(retry_module, "reserve_retry_attempt", fail)
    elif failure == "cleanup":
        monkeypatch.setattr(runner, "_clear_retry_failure_evidence", fail)
    elif failure == "ack":
        monkeypatch.setattr(runner, "_mark_retry_applied", fail)
    assert asyncio.run(runner._apply_retry_command(command)) == RetryDispatch.HANDLED
    stored = asyncio.run(load_retry_command(runner.redis, runner.name, command.command_id))
    if failure in {"binding", "binding_missing"}:
        assert runner.state.publication_attempt.pending
        assert stored.bound_pr_number is None
    else:
        assert stored.bound_pr_number == 402
        assert stored.bound_pr_branch == "pr-001"
        assert stored.bound_pr_head_sha == "a" * 40
        # List omission on a later attempt must never authorize CODING.
        monkeypatch.setattr(coding_module.gh_prs, "get_open_prs", lambda *a, **kw: [])
        monkeypatch.setattr(coding_module.gh_prs, "get_pr_state", lambda *a: "OPEN")
        selected = asyncio.run(runner._select_retry_continuation(stored))
        if failure == "reservation":
            assert runner.state.publication_attempt.pending
            assert selected[0] == "watch" and selected[1].number == 402
        else:
            assert selected == RetryDispatch.HANDLED


@pytest.mark.parametrize("discovery_available", [False, True])
def test_retained_publication_survives_retry_cap_and_next_error_cycle(
    monkeypatch, discovery_available,
):
    from src.daemon.retry_commands import RetryDispatch
    from src.keyspace import pipeline_state
    from src.models import RepoState
    from src.retry_commands import (
        RetryCommandStatus,
        enqueue_retry_command,
        list_pending_retry_commands,
        load_retry_command,
    )

    from tests.runner.test_retry_commands import _command, _task_text

    runner = _runner(monkeypatch)
    remote = _publication_transport(monkeypatch, runner)
    asyncio.run(runner.handle_coding())
    attempt = runner.state.publication_attempt
    invocation = runner.state.active_invocation
    command = _command(_task_text(task_id="PR-001", branch="pr-001"),
                       task_id="PR-001", branch="pr-001")
    command.repo_slug = runner.name
    asyncio.run(enqueue_retry_command(runner.redis, command))
    runner.redis.store[f"metrics:retry_count:{runner.name}:{command.task_id}"] = "3"
    task = runner.state.current_task

    async def validate(*args):
        return object(), task

    async def no_blocker(*args):
        return None

    def unexpected_diagnosis(*args, **kwargs):
        pytest.fail("exhausted Retry must not enter normal AI diagnosis")

    monkeypatch.setattr(runner, "_validate_retry_command", validate)
    monkeypatch.setattr(runner, "_retry_blocker", no_blocker)
    monkeypatch.setattr(runner, "_get_auxiliary_coder", unexpected_diagnosis)
    assert asyncio.run(runner._apply_retry_command(command)) == RetryDispatch.HANDLED
    stored = asyncio.run(load_retry_command(runner.redis, runner.name, command.command_id))
    assert stored.status == RetryCommandStatus.FAILED
    assert "Retry cap reached (3/3)" in stored.outcome_reason
    assert stored.bound_pr_number == 402
    assert asyncio.run(list_pending_retry_commands(runner.redis, runner.name)) == []
    assert attempt.pending
    assert runner.state.active_invocation == invocation

    # With the failed command removed, the publication alone must retain ERROR,
    # including when fresh GitHub discovery cannot confirm the retained PR.
    remote["omit"] = remote["exact_unknown"] = not discovery_available
    assert asyncio.run(runner._consume_retry_command()) == RetryDispatch.NONE
    asyncio.run(runner.handle_error())
    assert runner.state.state == PipelineState.ERROR
    assert attempt.pending
    assert runner.state.active_invocation == invocation
    assert runner.state.current_pr.number == 402
    assert remote["calls"] == 1
    saved = RepoState.model_validate_json(
        asyncio.run(runner.redis.get(pipeline_state(runner.name)))
    )
    assert saved.publication_attempt.pending
    assert saved.publication_attempt.pr_number == 402
    assert saved.active_invocation == invocation


@pytest.mark.parametrize("interruption", ["stop", "breach"])
@pytest.mark.parametrize("entry", ["paused", "watch", "terminal"])
@pytest.mark.parametrize("terminal", [None, "MERGED", "CLOSED"])
def test_interrupted_fix_cannot_release_unpublished_head(
    monkeypatch, interruption, entry, terminal,
):
    runner = _runner(monkeypatch)
    remote = _publication_transport(monkeypatch, runner, code=0, stderr="")
    asyncio.run(runner.handle_coding())
    calls = []

    async def interrupted_fix(*args, **kwargs):
        calls.append(True)
        monkeypatch.setattr(coding_module, "_local_branch_head_sha", lambda *a: "b" * 40)
        remote["exact_unknown"] = True
        if interruption == "stop":
            runner._stop_requested = True
            runner.state.user_paused = True
        return 0, "", ""

    if interruption == "breach":
        def late_breach(*args):
            args[-1]["breached"] = True
        monkeypatch.setattr(runner, "_check_late_breach", late_breach)
    monkeypatch.setattr(h.claude_cli, "fix_review_async", interrupted_fix)
    asyncio.run(runner.handle_fix())
    assert runner.state.state == PipelineState.PAUSED
    assert runner.state.publication_attempt.unverified_head_sha == "b" * 40
    # Restart, then Play. The old remote PR may already be terminal.
    runner.state = type(runner.state).model_validate_json(runner.state.model_dump_json())
    runner.state.user_paused = False
    runner._stop_requested = False
    remote["exact_unknown"] = False
    remote["terminal"] = terminal
    attempt = runner.state.publication_attempt
    if entry == "paused":
        asyncio.run(runner.handle_paused())
    elif entry == "watch":
        runner.state.state = PipelineState.WATCH
        def forbidden(*args, **kwargs):
            pytest.fail("WATCH cannot review or merge before FIX publication resolves")
        monkeypatch.setattr(coding_module.gh_prs, "get_open_prs", forbidden)
        asyncio.run(runner.handle_watch())
    else:
        asyncio.run(runner._handle_external_pr_resolution(
            runner.state.current_pr, terminal or "CLOSED",
        ))
    assert runner.state.state == PipelineState.ERROR
    assert attempt.pending
    assert attempt.unverified_head_sha == "b" * 40
    assert runner.state.current_task.pr_id == "PR-001"
    assert runner.state.current_pr.head_sha == "a" * 40
    assert calls == [True]
    # Fresh exact evidence of the candidate is required to resume or consume.
    remote["head"] = "b" * 40
    runner.state.error_message = None
    attempt.failure = None
    if entry == "paused":
        runner.state.state = PipelineState.PAUSED
        asyncio.run(runner.handle_paused())
    else:
        runner.state.state = PipelineState.WATCH
        asyncio.run(runner.handle_watch())
    assert attempt.unverified_head_sha is None
    if entry == "paused" and terminal is None:
        assert attempt.pending
        asyncio.run(runner.handle_idle())
    if interruption == "breach" and terminal is None:
        assert runner.state.state == PipelineState.ERROR
        assert attempt.pending
    else:
        assert runner.state.state == (PipelineState.IDLE if terminal else PipelineState.WATCH)
        assert not attempt.pending


def test_watch_terminal_keeps_unverified_publication(monkeypatch):
    runner = _runner(monkeypatch)
    _publication_transport(monkeypatch, runner)
    asyncio.run(runner.handle_coding())
    attempt = runner.state.publication_attempt
    attempt.verified = False
    asyncio.run(runner._handle_external_pr_resolution(runner.state.current_pr, "CLOSED"))
    assert attempt.pending
    assert runner.state.current_task is not None
    assert runner.state.state == PipelineState.ERROR


@pytest.mark.parametrize("scenario", ["missing", "incomplete", "incomplete_status", "complete", "empty", "testbed", "required_exempt"])
def test_coding_hydrates_ci_with_repository_policy(monkeypatch, scenario):
    from src.github import cache, checks
    from src.models import CIStatus

    runner = _runner(monkeypatch)
    _publication_transport(monkeypatch, runner, code=0, stderr="")
    runner.repo_config.required_checks = ["unit", "integration"]
    if scenario in {"empty", "testbed"}:
        runner.repo_config.required_checks = None
    assert runner.repo_config.allow_merge_without_checks is False
    if scenario in {"testbed", "required_exempt"}:
        runner.repo_config.allow_merge_without_checks = True
    clock = [100.0]
    source = {"complete": scenario != "incomplete", "status_complete": scenario != "incomplete_status",
              "names": ["unit", "integration"]}
    if scenario in {"missing", "required_exempt"}:
        source["names"] = ["unit"]
    if scenario in {"empty", "testbed"}:
        source["names"] = []
    calls = []

    def check_pages(path):
        assert path == "repos/octo/demo/commits/" + "a" * 40 + "/check-runs?per_page=100"
        calls.append(path)
        runs = [{"id": i, "name": name, "head_sha": "a" * 40,
                 "status": "completed", "conclusion": "success", "app": {"id": 1}}
                for i, name in enumerate(source["names"], 1)]
        return cache.PaginatedEvidence([{"check_runs": runs}], source["complete"], not runs)

    def status_pages(path, key):
        assert path == "repos/octo/demo/commits/" + "a" * 40 + "/status?per_page=100"
        assert key == "statuses"
        return cache.PaginatedEvidence(
            [{"sha": "a" * 40, "state": "pending", "total_count": 0, "statuses": []}], source["status_complete"], True,
        )

    checks.clear_ci_status_cache()
    monkeypatch.setattr(checks.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(cache, "_gh_api_paginated_evidence", check_pages)
    monkeypatch.setattr(cache, "_etag_get_object_pages_evidence", status_pages)
    try:
        asyncio.run(runner.handle_coding())
        assert runner.state.state == PipelineState.WATCH
        assert (runner.state.current_pr.number, runner.state.current_pr.head_sha) == (402, "a" * 40)
        expected = CIStatus.SUCCESS if scenario in {"complete", "testbed"} else CIStatus.PENDING
        assert runner.state.current_pr.ci_status == expected
        source.update(complete=True, status_complete=True, names=["unit", "integration"])
        assert asyncio.run(runner._resolve_published_attempt())
        assert runner.state.current_pr.ci_status == expected
        assert len(calls) == 1  # Cached partial evidence must retain its policy.
        clock[0] += checks._CI_STATUS_CACHE_TTL_SECONDS
        assert asyncio.run(runner._resolve_published_attempt())
        assert runner.state.current_pr.ci_status == CIStatus.SUCCESS
        assert len(calls) == 2
    finally:
        checks.clear_ci_status_cache()


def test_coding_ci_reader_error_clears_previous_success_without_losing_identity(monkeypatch):
    from src.github import checks
    from src.models import CIStatus

    runner = _runner(monkeypatch)
    _publication_transport(monkeypatch, runner, code=0, stderr="")

    def unavailable(*args):
        raise RuntimeError("CI transport unavailable")

    monkeypatch.setattr(checks, "_retrieve_ci_status_evidence", unavailable)
    asyncio.run(runner.handle_coding())
    runner.state.current_pr.ci_status = CIStatus.SUCCESS
    assert asyncio.run(runner._resolve_published_attempt())
    assert runner.state.current_pr.number == 402
    assert runner.state.current_pr.head_sha == "a" * 40
    assert runner.state.current_pr.ci_status == CIStatus.PENDING
    assert any("CI hydration deferred: CI transport unavailable" in e["event"] for e in runner.state.history)
