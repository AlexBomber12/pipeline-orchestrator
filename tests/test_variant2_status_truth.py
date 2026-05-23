from __future__ import annotations

from src.models import PRInfo, TaskStatus
from src.queue_parser import TaskHeader
from src.task_status import (
    MergedState,
    derive_task_status,
    merged_split_parent_aliases,
)


def _header(
    pr_id: str = "PR-383",
    branch: str = "pr-383-variant2-status-truth",
    *,
    frontmatter_status: str | None = None,
) -> TaskHeader:
    return TaskHeader(
        pr_id=pr_id,
        title="Variant 2 status truth",
        branch=branch,
        task_type="refactor",
        complexity="high",
        depends_on=[],
        priority=1,
        coder="any",
        frontmatter_status=frontmatter_status,
    )


def _state(
    *,
    merged_pr_ids: set[str] | None = None,
    merged_branches: set[str] | None = None,
    api_available: bool = True,
) -> MergedState:
    return MergedState(
        set(merged_pr_ids or ()),
        set(merged_branches or ()),
        api_available,
    )


def _open_pr(
    branch: str = "pr-383-variant2-status-truth",
    title: str = "PR-383: Variant 2 status truth",
) -> PRInfo:
    return PRInfo(number=383, branch=branch, title=title)


def test_zombie_task_prevented() -> None:
    status = derive_task_status(
        _header(frontmatter_status="done"),
        _state(),
        [_open_pr()],
    )

    assert status == TaskStatus.DOING


def test_done_requires_github_corroboration() -> None:
    status = derive_task_status(
        _header(frontmatter_status="done"),
        _state(merged_branches={"pr-383-variant2-status-truth"}),
        [],
    )

    assert status == TaskStatus.DONE


def test_frontmatter_done_alone_not_done() -> None:
    status = derive_task_status(
        _header(frontmatter_status="done"),
        _state(),
        [],
    )

    assert status == TaskStatus.TODO


def test_error_still_frontmatter_owned() -> None:
    status = derive_task_status(
        _header(frontmatter_status="error"),
        _state(merged_pr_ids={"PR-383"}),
        [_open_pr()],
    )

    assert status == TaskStatus.ERROR


def test_github_unreachable_safe() -> None:
    status = derive_task_status(
        _header(frontmatter_status="done"),
        _state(api_available=False),
        [],
    )

    assert status != TaskStatus.DONE
    assert status == TaskStatus.TODO


def test_open_pr_is_doing() -> None:
    status = derive_task_status(
        _header(),
        _state(),
        [_open_pr()],
    )

    assert status == TaskStatus.DOING


def test_split_parent_alias_preserved() -> None:
    assert merged_split_parent_aliases(
        structured_pr_ids=set(),
        merged_pr_ids={"PR-383c"},
    ) == {"PR-383"}


def test_branch_match_preserved() -> None:
    status = derive_task_status(
        _header(pr_id="PR-383", branch="PR-383.variant-status"),
        _state(merged_branches={"PR-383.variant-status"}),
        [],
    )

    assert status == TaskStatus.DONE
