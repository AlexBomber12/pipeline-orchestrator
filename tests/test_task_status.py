from __future__ import annotations

import asyncio
import subprocess
from pathlib import Path

import pytest
from src.daemon.handlers import idle as idle_module
from src.github import GhPrMergedBranchesUnavailable
from src.models import PRInfo, QueueTask, TaskStatus
from src.queue_parser import QueueValidationError, TaskHeader
from src.task_status import (
    MergedState,
    _load_legacy_task_header,
    _load_task_header,
    _resolve_merged_state,
    derive_queue_task_statuses,
    derive_task_status,
    find_matching_merged_pr,
    find_matching_open_pr,
    get_merged_pr_ids,
)

from tests.runner import _helpers as h


def _header(
    branch: str,
    pr_id: str = "PR-085",
    *,
    frontmatter_status: str | None = None,
) -> TaskHeader:
    return TaskHeader(
        pr_id=pr_id,
        title="Status derivation from git",
        branch=branch,
        task_type="feature",
        complexity="medium",
        depends_on=[],
        priority=2,
        coder="any",
        frontmatter_status=frontmatter_status,
    )


def _merged_state(
    pr_ids: set[str] | None = None,
    branches: set[str] | None = None,
    *,
    api_available: bool = True,
) -> MergedState:
    return MergedState(set(pr_ids or ()), set(branches or ()), api_available)


def _write_task_file(
    tmp_path: Path,
    pr_id: str,
    title: str,
    branch: str,
    *,
    depends_on: str = "none",
    priority: int = 1,
) -> None:
    tasks_dir = tmp_path / "tasks"
    tasks_dir.mkdir(exist_ok=True)
    (tasks_dir / f"{pr_id}.md").write_text(
        f"# {pr_id}: {title}\n\n"
        f"Branch: {branch}\n"
        "- Type: feature\n"
        "- Complexity: low\n"
        f"- Depends on: {depends_on}\n"
        f"- Priority: {priority}\n"
        "- Coder: any\n",
        encoding="utf-8",
    )


def test_derive_done_when_pr_id_is_in_merged_history() -> None:
    status = derive_task_status(
        _header("pr-085-status-from-git"),
        _merged_state({"PR-085"}),
        [],
    )

    assert status == TaskStatus.DONE


@pytest.mark.parametrize("frontmatter_status", ["merged", "done"])
def test_derive_task_status_terminal_done_frontmatter_returns_done(
    frontmatter_status: str,
) -> None:
    status = derive_task_status(
        _header("pr-085-status-from-git", frontmatter_status=frontmatter_status),
        _merged_state(),
        [],
    )

    assert status == TaskStatus.DONE


def test_derive_task_status_in_progress_frontmatter_returns_doing() -> None:
    status = derive_task_status(
        _header("pr-085-status-from-git", frontmatter_status="in_progress"),
        _merged_state(),
        [],
    )

    assert status == TaskStatus.DOING


@pytest.mark.parametrize("frontmatter_status", ["blocked", "canceled", "error"])
def test_derive_task_status_stopped_frontmatter_returns_canceled(
    frontmatter_status: str,
) -> None:
    status = derive_task_status(
        _header("pr-085-status-from-git", frontmatter_status=frontmatter_status),
        _merged_state(),
        [
            PRInfo(
                number=109,
                branch="pr-085-status-from-git",
                title="PR-085: Status derivation from git",
            )
        ],
    )

    assert status == TaskStatus.CANCELED


@pytest.mark.parametrize("frontmatter_status", ["queued", "todo"])
def test_derive_task_status_todo_frontmatter_returns_todo(
    frontmatter_status: str,
) -> None:
    status = derive_task_status(
        _header("pr-085-status-from-git", frontmatter_status=frontmatter_status),
        _merged_state({"PR-085"}),
        [],
    )

    assert status == TaskStatus.TODO


def test_derive_task_status_no_frontmatter_uses_existing_logic() -> None:
    status = derive_task_status(
        _header("pr-085-status-from-git", frontmatter_status=None),
        _merged_state({"PR-085"}),
        [],
    )

    assert status == TaskStatus.DONE


def test_derive_doing_when_open_pr() -> None:
    status = derive_task_status(
        _header("pr-085-status-from-git"),
        _merged_state(),
        [
            PRInfo(
                number=109,
                branch="pr-085-status-from-git",
                title="PR-085: Status derivation from git",
            )
        ],
    )

    assert status == TaskStatus.DOING


def test_derive_doing_when_open_pr_title_loses_queue_prefix() -> None:
    status = derive_task_status(
        _header("pr-085-status-from-git"),
        _merged_state(),
        [
            PRInfo(
                number=109,
                branch="pr-085-status-from-git",
                title="status derivation follow-up",
            )
        ],
    )

    assert status == TaskStatus.DOING


def test_derive_todo_when_neither() -> None:
    status = derive_task_status(
        _header("pr-085-status-from-git"),
        _merged_state(),
        [],
    )

    assert status == TaskStatus.TODO


def test_derive_doing_when_current_task_matches_without_open_pr() -> None:
    status = derive_task_status(
        _header("pr-085-status-from-git"),
        _merged_state(),
        [],
        current_task_pr_id="PR-085",
    )

    assert status == TaskStatus.DOING


def test_derive_done_when_current_task_matches_but_pr_already_merged() -> None:
    status = derive_task_status(
        _header("pr-085-status-from-git"),
        _merged_state({"PR-085"}),
        [],
        current_task_pr_id="PR-085",
    )

    assert status == TaskStatus.DONE


def test_derive_doing_when_current_task_matches_and_open_pr_exists() -> None:
    status = derive_task_status(
        _header("pr-085-status-from-git"),
        _merged_state(),
        [
            PRInfo(
                number=109,
                branch="pr-085-status-from-git",
                title="PR-085: Status derivation from git",
            )
        ],
        current_task_pr_id="PR-085",
    )

    assert status == TaskStatus.DOING


def test_derive_todo_when_current_task_pr_id_is_unrelated() -> None:
    status = derive_task_status(
        _header("pr-085-status-from-git", pr_id="PR-085"),
        _merged_state(),
        [],
        current_task_pr_id="PR-999",
    )

    assert status == TaskStatus.TODO


def test_derive_default_current_task_pr_id_preserves_legacy_todo() -> None:
    status = derive_task_status(
        _header("pr-085-status-from-git"),
        _merged_state(),
        [],
    )

    assert status == TaskStatus.TODO


def test_resolve_merged_state_api_primary(monkeypatch) -> None:
    header = _header("pr-085-status-from-git")
    seen: dict[str, object] = {}

    def fake_merged_branches(repo: str, branches: set[str]) -> set[str]:
        seen["repo"] = repo
        seen["branches"] = set(branches)
        return {"pr-085-status-from-git"}

    def fake_merged_pr_ids(
        repo_path: str,
        base_branch: str,
        candidate_pr_ids,
    ) -> set[str]:
        seen["repo_path"] = repo_path
        seen["base_branch"] = base_branch
        seen["candidate_pr_ids"] = set(candidate_pr_ids)
        return set()

    monkeypatch.setattr(
        "src.task_status.gh_pr_get_merged_branches",
        fake_merged_branches,
    )
    monkeypatch.setattr(
        "src.task_status.get_merged_pr_ids",
        fake_merged_pr_ids,
    )

    state = _resolve_merged_state(
        "/repo",
        "main",
        "owner/repo",
        ["PR-085"],
        [header],
        log_event=lambda event: None,
    )

    assert state == _merged_state(branches={"pr-085-status-from-git"})
    assert seen == {
        "repo": "owner/repo",
        "branches": {"pr-085-status-from-git"},
        "repo_path": "/repo",
        "base_branch": "main",
        "candidate_pr_ids": {"PR-085"},
    }
    assert derive_task_status(header, state, []) == TaskStatus.DONE


def test_derive_todo_when_merged_branch_matches_different_pr_id() -> None:
    status = derive_task_status(
        _header("pr-999-reused-branch"),
        _merged_state(branches={"pr-999-reused-branch"}),
        [],
    )

    assert status == TaskStatus.TODO


def test_resolve_merged_state_git_log_fallback(monkeypatch) -> None:
    header = _header("pr-085-status-from-git")
    logs: list[str] = []

    def fake_merged_branches(repo: str, branches: set[str]) -> set[str]:
        raise GhPrMergedBranchesUnavailable("graphql unavailable")

    monkeypatch.setattr(
        "src.task_status.gh_pr_get_merged_branches",
        fake_merged_branches,
    )
    monkeypatch.setattr(
        "src.task_status.get_merged_pr_ids",
        lambda repo_path, base_branch, candidate_pr_ids: {"PR-085"},
    )

    state = _resolve_merged_state(
        "/repo",
        "main",
        "owner/repo",
        ["PR-085"],
        [header],
        log_event=logs.append,
    )

    assert state == _merged_state({"PR-085"}, api_available=False)
    assert derive_task_status(header, state, []) == TaskStatus.DONE
    assert logs == [
        "[INFRA] gh pr list merged-branches probe failed: graphql unavailable"
    ]


def test_resolve_merged_state_invalid_branch_falls_back_to_git_log(
    monkeypatch,
) -> None:
    header = _header("pr 085 invalid branch")
    logs: list[str] = []

    def fake_merged_branches(repo: str, branches: set[str]) -> set[str]:
        raise ValueError("Invalid branch name: 'pr 085 invalid branch'")

    monkeypatch.setattr(
        "src.task_status.gh_pr_get_merged_branches",
        fake_merged_branches,
    )
    monkeypatch.setattr(
        "src.task_status.get_merged_pr_ids",
        lambda repo_path, base_branch, candidate_pr_ids: {"PR-085"},
    )

    state = _resolve_merged_state(
        "/repo",
        "main",
        "owner/repo",
        ["PR-085"],
        [header],
        log_event=logs.append,
    )

    assert state == _merged_state({"PR-085"}, api_available=False)
    assert derive_task_status(header, state, []) == TaskStatus.DONE
    assert logs == [
        "[INFRA] gh pr list merged-branches probe failed: "
        "Invalid branch name: 'pr 085 invalid branch'"
    ]


def test_resolve_merged_state_timeout_falls_back_to_git_log(
    monkeypatch,
) -> None:
    header = _header("pr-085-status-from-git")
    logs: list[str] = []

    def fake_merged_branches(repo: str, branches: set[str]) -> set[str]:
        raise subprocess.TimeoutExpired(cmd=["gh", "api"], timeout=30)

    monkeypatch.setattr(
        "src.task_status.gh_pr_get_merged_branches",
        fake_merged_branches,
    )
    monkeypatch.setattr(
        "src.task_status.get_merged_pr_ids",
        lambda repo_path, base_branch, candidate_pr_ids: {"PR-085"},
    )

    state = _resolve_merged_state(
        "/repo",
        "main",
        "owner/repo",
        ["PR-085"],
        [header],
        log_event=logs.append,
    )

    assert state == _merged_state({"PR-085"}, api_available=False)
    assert derive_task_status(header, state, []) == TaskStatus.DONE
    assert logs[0].startswith(
        "[INFRA] gh pr list merged-branches probe failed: "
        "Command '['gh', 'api']' timed out"
    )


def test_resolve_merged_state_both_disagree_branch_wins(monkeypatch) -> None:
    header = _header("pr-262-megaraid-dashboard", pr_id="PR-262")
    monkeypatch.setattr(
        "src.task_status.gh_pr_get_merged_branches",
        lambda repo, branches: {"pr-262-megaraid-dashboard"},
    )
    monkeypatch.setattr(
        "src.task_status.get_merged_pr_ids",
        lambda repo_path, base_branch, candidate_pr_ids: set(),
    )

    state = _resolve_merged_state(
        "/repo",
        "main",
        "owner/repo",
        ["PR-262"],
        [header],
        log_event=lambda event: None,
    )

    assert derive_task_status(header, state, []) == TaskStatus.DONE


def test_resolve_merged_state_both_disagree_pr_id_only(monkeypatch) -> None:
    header = _header("pr-085-deleted-after-merge")
    monkeypatch.setattr(
        "src.task_status.gh_pr_get_merged_branches",
        lambda repo, branches: set(),
    )
    monkeypatch.setattr(
        "src.task_status.get_merged_pr_ids",
        lambda repo_path, base_branch, candidate_pr_ids: {"PR-085"},
    )

    state = _resolve_merged_state(
        "/repo",
        "main",
        "owner/repo",
        ["PR-085"],
        [header],
        log_event=lambda event: None,
    )

    assert derive_task_status(header, state, []) == TaskStatus.DONE


def test_resolve_merged_state_neither_returns_todo(monkeypatch) -> None:
    header = _header("pr-085-not-merged")
    monkeypatch.setattr(
        "src.task_status.gh_pr_get_merged_branches",
        lambda repo, branches: set(),
    )
    monkeypatch.setattr(
        "src.task_status.get_merged_pr_ids",
        lambda repo_path, base_branch, candidate_pr_ids: set(),
    )

    state = _resolve_merged_state(
        "/repo",
        "main",
        "owner/repo",
        ["PR-085"],
        [header],
        log_event=lambda event: None,
    )

    assert derive_task_status(header, state, []) == TaskStatus.TODO


def test_degraded_mode_logs_once(monkeypatch, tmp_path: Path) -> None:
    for index in range(1, 11):
        _write_task_file(
            tmp_path,
            f"PR-{index:03}",
            f"Candidate {index}",
            f"pr-{index:03}-candidate",
            priority=index % 5 + 1,
        )
    seen_candidate_ids: set[str] = set()

    def fake_resolve(
        repo_path: str,
        base_branch: str,
        owner_repo: str,
        candidate_pr_ids,
        headers,
        *,
        log_event,
    ) -> MergedState:
        assert repo_path == str(tmp_path)
        assert base_branch == "main"
        assert owner_repo == "octo/demo"
        seen_candidate_ids.update(candidate_pr_ids)
        assert len(list(headers)) == 10
        return _merged_state(api_available=False)

    monkeypatch.setattr(idle_module, "_resolve_merged_state", fake_resolve)

    runner = h._make_runner()
    runner.repo_path = str(tmp_path)
    task = asyncio.run(runner._select_next_task_from_dag())

    assert task is not None
    assert seen_candidate_ids == {f"PR-{index:03}" for index in range(1, 11)}
    degraded_logs = [
        event["event"]
        for event in runner.state.history
        if event["event"].startswith(
            "[INFRA] Operating without gh API done-check; "
        )
    ]
    assert degraded_logs == [
        "[INFRA] Operating without gh API done-check; relying on "
        "git log convention scan only"
    ]


def test_dag_selector_uses_resolved_state(monkeypatch, tmp_path: Path) -> None:
    _write_task_file(
        tmp_path,
        "PR-001",
        "Merged dependency",
        "pr-001-merged-dependency",
    )
    _write_task_file(
        tmp_path,
        "PR-002",
        "Unblocked follow-up",
        "pr-002-follow-up",
        depends_on="PR-001",
    )

    def fake_resolve(*args, **kwargs) -> MergedState:
        return _merged_state(branches={"pr-001-merged-dependency"})

    monkeypatch.setattr(idle_module, "_resolve_merged_state", fake_resolve)

    runner = h._make_runner()
    runner.repo_path = str(tmp_path)
    task = asyncio.run(runner._select_next_task_from_dag())

    assert task is not None
    assert task.pr_id == "PR-002"
    assert runner._idle_dag_statuses == {
        "PR-001": TaskStatus.DONE,
        "PR-002": TaskStatus.TODO,
    }


def test_branch_field_empty_falls_back_to_pr_id(monkeypatch) -> None:
    header = _header("", pr_id="PR-085")
    seen_branches: list[set[str]] = []

    def fake_merged_branches(repo: str, branches: set[str]) -> set[str]:
        seen_branches.append(set(branches))
        return set()

    monkeypatch.setattr(
        "src.task_status.gh_pr_get_merged_branches",
        fake_merged_branches,
    )
    monkeypatch.setattr(
        "src.task_status.get_merged_pr_ids",
        lambda repo_path, base_branch, candidate_pr_ids: {"PR-085"},
    )

    state = _resolve_merged_state(
        "/repo",
        "main",
        "owner/repo",
        ["PR-085"],
        [header],
        log_event=lambda event: None,
    )

    assert seen_branches == [set()]
    assert derive_task_status(header, state, []) == TaskStatus.DONE


def test_get_merged_pr_ids(monkeypatch) -> None:
    calls: list[list[str]] = []

    def fake_run(*args, **kwargs) -> subprocess.CompletedProcess[str]:
        calls.append(args[0])
        return subprocess.CompletedProcess(
            args=args[0],
            returncode=0,
            stdout=(
                "PR-084: add task file header parser (#108)\n"
                "Merge pull request #97 from AlexBomber12/micro-20260418-rate-limit-on-failure\n"
                "PR-085: Status derivation from git (#109)\n"
            ),
            stderr="",
        )

    monkeypatch.setattr("src.task_status.subprocess.run", fake_run)

    assert get_merged_pr_ids("/repo", "main") == {
        "PR-084",
        "PR-085",
    }
    assert "--max-count=2048" not in calls[0]


def test_get_merged_pr_ids_propagates_timeout(
    monkeypatch,
) -> None:
    def fake_run(*args, **kwargs) -> subprocess.CompletedProcess[str]:
        raise subprocess.TimeoutExpired(cmd=args[0], timeout=kwargs["timeout"])

    monkeypatch.setattr("src.task_status.subprocess.run", fake_run)

    with pytest.raises(subprocess.TimeoutExpired):
        get_merged_pr_ids("/repo", "main")


def test_get_merged_pr_ids_ignores_noncanonical_subject_mentions(
    monkeypatch,
) -> None:
    def fake_run(*args, **kwargs) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(
            args=args[0],
            returncode=0,
            stdout=(
                "Merge PR-777 by hand\n"
                "follow-up for PR-888 in docs\n"
                "PR-085: canonical queue subject\n"
            ),
            stderr="",
        )

    monkeypatch.setattr("src.task_status.subprocess.run", fake_run)

    assert get_merged_pr_ids("/repo", "main") == {"PR-085"}


def test_get_merged_pr_ids_accepts_full_queue_pr_id_grammar(
    monkeypatch,
) -> None:
    def fake_run(*args, **kwargs) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(
            args=args[0],
            returncode=0,
            stdout=(
                "PR-abc_1.2: queue parser parity\n"
                "Merge pull request #98 from AlexBomber12/pr-feature-x\n"
            ),
            stderr="",
        )

    monkeypatch.setattr("src.task_status.subprocess.run", fake_run)

    assert get_merged_pr_ids("/repo", "main") == {"PR-abc_1.2"}


def test_get_merged_pr_ids_limits_probe_to_requested_candidates(
    monkeypatch,
) -> None:
    calls: list[list[str]] = []

    def fake_run(*args, **kwargs) -> subprocess.CompletedProcess[str]:
        calls.append(args[0])
        return subprocess.CompletedProcess(
            args=args[0],
            returncode=0,
            stdout="PR-085: queue subject\n",
            stderr="",
        )

    monkeypatch.setattr("src.task_status.subprocess.run", fake_run)

    assert get_merged_pr_ids("/repo", "main", {"PR-085", "PR-099"}) == {"PR-085"}
    assert "--extended-regexp" in calls[0]
    assert not any(arg.startswith("--max-count=") for arg in calls[0])
    assert any(arg.startswith("--grep=^(") for arg in calls[0])


def test_get_merged_pr_ids_candidate_probe_does_not_cap_duplicate_matches(
    monkeypatch,
) -> None:
    calls: list[list[str]] = []

    def fake_run(*args, **kwargs) -> subprocess.CompletedProcess[str]:
        calls.append(args[0])
        return subprocess.CompletedProcess(
            args=args[0],
            returncode=0,
            stdout=(
                "PR-085: first merge\n"
                "PR-085: reland\n"
                "PR-099: separate task\n"
            ),
            stderr="",
        )

    monkeypatch.setattr("src.task_status.subprocess.run", fake_run)

    assert get_merged_pr_ids("/repo", "main", {"PR-085", "PR-099"}) == {
        "PR-085",
        "PR-099",
    }
    assert not any(arg.startswith("--max-count=") for arg in calls[0])


def test_find_matching_open_pr_rejects_conflicting_pr_identity() -> None:
    match = find_matching_open_pr(
        "PR-085",
        "pr-085-status-from-git",
        [
            PRInfo(
                number=110,
                branch="pr-085-status-from-git",
                title="PR-999: unrelated work",
            )
        ],
    )

    assert match is None


def test_find_matching_open_pr_returns_none_without_branch() -> None:
    match = find_matching_open_pr(
        "PR-085",
        "",
        [
            PRInfo(
                number=109,
                branch="pr-085-status-from-git",
                title="PR-085: Status derivation from git",
            )
        ],
    )

    assert match is None


def test_find_matching_open_pr_allows_same_branch_when_pr_id_is_unavailable() -> None:
    match = find_matching_open_pr(
        "PR-085",
        "pr-085-status-from-git",
        [
            PRInfo(
                number=109,
                branch="pr-085-status-from-git",
                title="docs: no queue prefix anymore",
            )
        ],
    )

    assert match is not None


def test_find_matching_open_pr_allows_cross_repository_pr_with_matching_identity() -> None:
    match = find_matching_open_pr(
        "PR-085",
        "pr-085-status-from-git",
        [
            PRInfo(
                number=109,
                branch="pr-085-status-from-git",
                title="PR-085: Status derivation from git",
                is_cross_repository=True,
            )
        ],
    )

    assert match is not None


def test_derive_done_when_merged_pr_branch_matches_without_queue_prefix() -> None:
    status = derive_task_status(
        _header("pr-085-status-from-git"),
        _merged_state(),
        [],
        [
            PRInfo(
                number=109,
                branch="pr-085-status-from-git",
                title="custom squash title",
            )
        ],
    )

    assert status == TaskStatus.DONE


def test_find_matching_merged_pr_matches_by_identity_when_branch_is_missing() -> None:
    match = find_matching_merged_pr(
        "PR-085",
        "",
        [
            PRInfo(
                number=109,
                branch="different-branch",
                title="PR-085: Status derivation from git",
            )
        ],
    )

    assert match is not None


def test_find_matching_merged_pr_ignores_mismatched_identity_when_branch_is_missing() -> None:
    match = find_matching_merged_pr(
        "PR-085",
        "",
        [
            PRInfo(
                number=109,
                branch="different-branch",
                title="PR-999: Unrelated work",
            )
        ],
    )

    assert match is None


def test_load_task_header_falls_back_for_legacy_task_files(
    tmp_path: Path,
) -> None:
    task_file = tmp_path / "tasks" / "PR-001.md"
    task_file.parent.mkdir()
    task_file.write_text(
        "# PR-001: Legacy task\n\n"
        "Branch: pr-001-legacy-task\n",
        encoding="utf-8",
    )
    task = QueueTask(
        pr_id="PR-001",
        title="Legacy task",
        status=TaskStatus.TODO,
        task_file="tasks/PR-001.md",
        depends_on=["PR-000"],
        branch="pr-001-legacy-task",
    )

    header = _load_task_header(task, str(tmp_path))

    assert header == TaskHeader(
        pr_id="PR-001",
        title="Legacy task",
        branch="pr-001-legacy-task",
        task_type="feature",
        complexity="medium",
        depends_on=["PR-000"],
        priority=3,
        coder="any",
    )


def test_load_task_header_raises_nonlegacy_validation_errors(
    tmp_path: Path,
    monkeypatch,
) -> None:
    task_file = tmp_path / "tasks" / "PR-001.md"
    task_file.parent.mkdir()
    task_file.write_text("# PR-001: Legacy task\n", encoding="utf-8")
    task = QueueTask(
        pr_id="PR-001",
        title="Legacy task",
        status=TaskStatus.TODO,
        task_file="tasks/PR-001.md",
        depends_on=[],
        branch="pr-001-legacy-task",
    )
    error = QueueValidationError(["tasks/PR-001.md: unsupported header problem"])

    monkeypatch.setattr("src.task_status.parse_task_header", lambda path: (_ for _ in ()).throw(error))

    with pytest.raises(QueueValidationError) as excinfo:
        _load_task_header(task, str(tmp_path))

    assert excinfo.value is error


def test_load_task_header_falls_back_to_queue_metadata_without_task_file() -> None:
    task = QueueTask(
        pr_id="PR-001",
        title="Queued task",
        status=TaskStatus.TODO,
        depends_on=["PR-000"],
        branch="pr-001-queued-task",
    )

    assert _load_task_header(task, "/repo") == TaskHeader(
        pr_id="PR-001",
        title="Queued task",
        branch="pr-001-queued-task",
        task_type="feature",
        complexity="medium",
        depends_on=["PR-000"],
        priority=3,
        coder="any",
    )


def test_load_task_header_falls_back_when_legacy_file_missing_branch(
    tmp_path: Path,
) -> None:
    task_file = tmp_path / "tasks" / "PR-001.md"
    task_file.parent.mkdir()
    task_file.write_text(
        "# PR-001: Legacy task\n\n",
        encoding="utf-8",
    )
    task = QueueTask(
        pr_id="PR-001",
        title="Legacy task",
        status=TaskStatus.TODO,
        task_file="tasks/PR-001.md",
        depends_on=["PR-000"],
        branch="pr-001-legacy-task",
    )

    header = _load_task_header(task, str(tmp_path))

    assert header == TaskHeader(
        pr_id="PR-001",
        title="Legacy task",
        branch="pr-001-legacy-task",
        task_type="feature",
        complexity="medium",
        depends_on=["PR-000"],
        priority=3,
        coder="any",
    )


def test_load_task_header_rejects_mismatched_legacy_task_files(
    tmp_path: Path,
) -> None:
    task_file = tmp_path / "tasks" / "PR-999.md"
    task_file.parent.mkdir()
    task_file.write_text(
        "# PR-999: Wrong task\n\n"
        "Branch: pr-999-wrong-task\n",
        encoding="utf-8",
    )
    task = QueueTask(
        pr_id="PR-001",
        title="Queued task",
        status=TaskStatus.TODO,
        task_file="tasks/PR-999.md",
        depends_on=[],
        branch="pr-001-queued-task",
    )

    with pytest.raises(QueueValidationError) as excinfo:
        _load_task_header(task, str(tmp_path))

    assert excinfo.value.issues == [
        "tasks/PR-999.md: header PR ID 'PR-999' does not match queue entry 'PR-001'"
    ]


def test_load_legacy_task_header_returns_none_for_nonlegacy_issues(
    tmp_path: Path,
) -> None:
    task_file = tmp_path / "tasks" / "PR-001.md"
    task_file.parent.mkdir()
    task_file.write_text(
        "# PR-001: Legacy task\n\n"
        "Branch: pr-001-legacy-task\n",
        encoding="utf-8",
    )
    task = QueueTask(
        pr_id="PR-001",
        title="Legacy task",
        status=TaskStatus.TODO,
        task_file="tasks/PR-001.md",
        depends_on=[],
        branch="pr-001-legacy-task",
    )

    header = _load_legacy_task_header(
        task,
        task_file,
        QueueValidationError(["tasks/PR-001.md: header missing Priority"]),
    )

    assert header is None


def test_load_legacy_task_header_returns_none_when_branch_is_interrupted_by_section(
    tmp_path: Path,
) -> None:
    task_file = tmp_path / "tasks" / "PR-001.md"
    task_file.parent.mkdir()
    task_file.write_text(
        "# PR-001: Legacy task\n\n"
        "## Notes\n"
        "Branch: pr-001-legacy-task\n",
        encoding="utf-8",
    )
    task = QueueTask(
        pr_id="PR-001",
        title="Legacy task",
        status=TaskStatus.TODO,
        task_file="tasks/PR-001.md",
        depends_on=[],
        branch="",
    )

    header = _load_legacy_task_header(
        task,
        task_file,
        QueueValidationError(["tasks/PR-001.md: missing Branch"]),
    )

    assert header is None


def test_load_legacy_task_header_returns_none_without_matching_header_line(
    tmp_path: Path,
) -> None:
    task_file = tmp_path / "tasks" / "PR-001.md"
    task_file.parent.mkdir()
    task_file.write_text(
        "PR-001: Legacy task\n\n"
        "Branch: pr-001-legacy-task\n",
        encoding="utf-8",
    )
    task = QueueTask(
        pr_id="PR-001",
        title="Legacy task",
        status=TaskStatus.TODO,
        task_file="tasks/PR-001.md",
        depends_on=[],
        branch="pr-001-legacy-task",
    )

    header = _load_legacy_task_header(
        task,
        task_file,
        QueueValidationError(["tasks/PR-001.md: missing Branch"]),
    )

    assert header is None


def test_derive_queue_task_statuses_does_not_trust_stale_done_queue_status(
    monkeypatch,
) -> None:
    task = QueueTask(
        pr_id="PR-001",
        title="Queued task",
        status=TaskStatus.DONE,
        branch="pr-001-queued-task",
    )

    monkeypatch.setattr(
        "src.task_status.get_merged_pr_ids",
        lambda repo_path, base_branch, candidate_pr_ids=None: set(),
    )
    monkeypatch.setattr(
        "src.task_status.gh_pr_get_merged_branches",
        lambda repo, branches: set(),
    )
    monkeypatch.setattr(
        "src.task_status._load_task_header",
        lambda current_task, repo_path: _header(
            "pr-001-queued-task",
            pr_id="PR-001",
        ),
    )

    derived = derive_queue_task_statuses(
        [task],
        "/repo",
        "main",
        "owner/repo",
        set(),
        log_event=lambda event: None,
    )

    assert derived[0].status == TaskStatus.TODO


def test_derive_queue_task_statuses_marks_done_from_merged_pr_history(
    monkeypatch,
) -> None:
    task = QueueTask(
        pr_id="PR-001",
        title="Completed task",
        status=TaskStatus.TODO,
        branch="pr-001-completed",
    )

    monkeypatch.setattr(
        "src.task_status.get_merged_pr_ids",
        lambda repo_path, base_branch, candidate_pr_ids=None: {"PR-001"},
    )
    monkeypatch.setattr(
        "src.task_status.gh_pr_get_merged_branches",
        lambda repo, branches: set(),
    )
    monkeypatch.setattr(
        "src.task_status._load_task_header",
        lambda current_task, repo_path: _header(
            "pr-001-deleted-branch",
            pr_id="PR-001",
        ),
    )

    derived = derive_queue_task_statuses(
        [task],
        "/repo",
        "main",
        "owner/repo",
        set(),
        log_event=lambda event: None,
    )

    assert derived[0].status == TaskStatus.DONE


def test_derive_queue_task_statuses_skips_merged_probe_when_queue_is_empty(
    monkeypatch,
) -> None:
    def _fail_if_called(*args, **kwargs):
        raise AssertionError("get_merged_pr_ids should not run for an empty queue")

    monkeypatch.setattr("src.task_status.get_merged_pr_ids", _fail_if_called)

    assert derive_queue_task_statuses(
        [],
        "/repo",
        "main",
        "owner/repo",
        set(),
        log_event=lambda event: None,
    ) == []


def test_derive_queue_task_statuses_rejects_mismatched_task_file_pr_id(
    monkeypatch,
) -> None:
    task = QueueTask(
        pr_id="PR-001",
        title="Queued task",
        status=TaskStatus.TODO,
        branch="pr-001-queued-task",
        task_file="tasks/PR-999.md",
    )

    monkeypatch.setattr(
        "src.task_status._resolve_merged_state",
        lambda *args, **kwargs: _merged_state(),
    )
    monkeypatch.setattr(
        "src.task_status._load_task_header",
        lambda current_task, repo_path: _header(
            "pr-999-other-task",
            pr_id="PR-999",
        ),
    )

    with pytest.raises(QueueValidationError) as excinfo:
        derive_queue_task_statuses(
            [task],
            "/repo",
            "main",
            "owner/repo",
            set(),
            log_event=lambda event: None,
        )

    assert excinfo.value.issues == [
        "tasks/PR-999.md: header PR ID 'PR-999' does not match queue entry 'PR-001'"
    ]


def test_get_merged_pr_ids_falls_back_when_candidate_origin_ref_is_ambiguous(
    monkeypatch,
) -> None:
    calls: list[list[str]] = []

    def fake_run(*args, **kwargs) -> subprocess.CompletedProcess[str]:
        calls.append(args[0])
        target_ref = args[0][4]
        if target_ref == "origin/main":
            return subprocess.CompletedProcess(
                args=args[0],
                returncode=128,
                stdout="",
                stderr="fatal: ambiguous argument 'origin/main': unknown revision or path not in the working tree.",
            )
        return subprocess.CompletedProcess(
            args=args[0],
            returncode=0,
            stdout="PR-085: queue subject\n",
            stderr="",
        )

    monkeypatch.setattr("src.task_status.subprocess.run", fake_run)

    assert get_merged_pr_ids("/repo", "main", {"PR-085"}) == {"PR-085"}
    assert [call[4] for call in calls[:2]] == ["origin/main", "main"]


def test_get_merged_pr_ids_raises_when_candidate_probe_fails_nonambiguously(
    monkeypatch,
) -> None:
    def fake_run(*args, **kwargs) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(
            args=args[0],
            returncode=1,
            stdout="",
            stderr="fatal: permissions failure",
        )

    monkeypatch.setattr("src.task_status.subprocess.run", fake_run)

    with pytest.raises(RuntimeError, match="permissions failure"):
        get_merged_pr_ids("/repo", "main", {"PR-085"})


def test_get_merged_pr_ids_falls_back_to_local_base_branch_probe(
    monkeypatch,
) -> None:
    calls: list[str] = []

    def fake_probe(repo_path: str, target_ref: str) -> subprocess.CompletedProcess[str]:
        calls.append(target_ref)
        if target_ref == "origin/main":
            return subprocess.CompletedProcess(
                args=["git"],
                returncode=1,
                stdout="",
                stderr="fatal: bad revision 'origin/main'",
            )
        return subprocess.CompletedProcess(
            args=["git"],
            returncode=0,
            stdout="PR-085: queue subject\n\n",
            stderr="",
        )

    monkeypatch.setattr("src.task_status._run_merged_pr_probe", fake_probe)

    assert get_merged_pr_ids("/repo", "main") == {"PR-085"}
    assert calls == ["origin/main", "main"]


def test_get_merged_pr_ids_raises_when_all_probes_fail(
    monkeypatch,
) -> None:
    def fake_probe(repo_path: str, target_ref: str) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(
            args=["git"],
            returncode=1,
            stdout="",
            stderr=f"fatal: bad revision '{target_ref}'",
        )

    monkeypatch.setattr("src.task_status._run_merged_pr_probe", fake_probe)

    with pytest.raises(RuntimeError, match="fatal: bad revision 'main'"):
        get_merged_pr_ids("/repo", "main")


def test_get_merged_pr_ids_skips_blank_subject_lines_in_full_probe(
    monkeypatch,
) -> None:
    def fake_probe(repo_path: str, target_ref: str) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(
            args=["git"],
            returncode=0,
            stdout="PR-085: queue subject\n   \nPR-099: another subject\n",
            stderr="",
        )

    monkeypatch.setattr("src.task_status._run_merged_pr_probe", fake_probe)

    assert get_merged_pr_ids("/repo", "main") == {"PR-085", "PR-099"}


def test_get_merged_pr_ids_skips_blank_subject_lines_in_candidate_probe(
    monkeypatch,
) -> None:
    def fake_run(*args, **kwargs) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(
            args=args[0],
            returncode=0,
            stdout="PR-085: queue subject\n \nPR-099: another subject\n",
            stderr="",
        )

    monkeypatch.setattr("src.task_status.subprocess.run", fake_run)

    assert get_merged_pr_ids("/repo", "main", {"PR-085", "PR-099"}) == {
        "PR-085",
        "PR-099",
    }
