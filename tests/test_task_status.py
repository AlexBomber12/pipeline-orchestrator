from __future__ import annotations

import subprocess
from pathlib import Path

import pytest
from src.models import PRInfo, QueueTask, TaskStatus
from src.queue_parser import QueueValidationError, TaskHeader
from src.task_status import (
    _load_legacy_task_header,
    _load_task_header,
    derive_queue_task_statuses,
    derive_task_status,
    find_matching_merged_pr,
    find_matching_open_pr,
    get_merged_pr_ids,
)


def _header(branch: str, pr_id: str = "PR-085") -> TaskHeader:
    return TaskHeader(
        pr_id=pr_id,
        title="Status derivation from git",
        branch=branch,
        task_type="feature",
        complexity="medium",
        depends_on=[],
        priority=2,
        coder="any",
    )


def test_derive_done_when_pr_id_is_in_merged_history() -> None:
    status = derive_task_status(
        _header("pr-085-status-from-git"),
        {"PR-085"},
        [],
    )

    assert status == TaskStatus.DONE


def test_derive_doing_when_open_pr() -> None:
    status = derive_task_status(
        _header("pr-085-status-from-git"),
        set(),
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
        set(),
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
        set(),
        [],
    )

    assert status == TaskStatus.TODO


def test_derive_doing_when_current_task_matches_without_open_pr() -> None:
    status = derive_task_status(
        _header("pr-085-status-from-git"),
        set(),
        [],
        current_task_pr_id="PR-085",
    )

    assert status == TaskStatus.DOING


def test_derive_done_when_current_task_matches_but_pr_already_merged() -> None:
    status = derive_task_status(
        _header("pr-085-status-from-git"),
        {"PR-085"},
        [],
        current_task_pr_id="PR-085",
    )

    assert status == TaskStatus.DONE


def test_derive_doing_when_current_task_matches_and_open_pr_exists() -> None:
    status = derive_task_status(
        _header("pr-085-status-from-git"),
        set(),
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
        set(),
        [],
        current_task_pr_id="PR-999",
    )

    assert status == TaskStatus.TODO


def test_derive_default_current_task_pr_id_preserves_legacy_todo() -> None:
    status = derive_task_status(
        _header("pr-085-status-from-git"),
        set(),
        [],
    )

    assert status == TaskStatus.TODO


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
        set(),
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
        set(),
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
        set(),
    )

    assert derived[0].status == TaskStatus.DONE


def test_derive_queue_task_statuses_skips_merged_probe_when_queue_is_empty(
    monkeypatch,
) -> None:
    def _fail_if_called(*args, **kwargs):
        raise AssertionError("get_merged_pr_ids should not run for an empty queue")

    monkeypatch.setattr("src.task_status.get_merged_pr_ids", _fail_if_called)

    assert derive_queue_task_statuses([], "/repo", "main", set()) == []


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
        "src.task_status.get_merged_pr_ids",
        lambda repo_path, base_branch, candidate_pr_ids=None: set(),
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
            set(),
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
