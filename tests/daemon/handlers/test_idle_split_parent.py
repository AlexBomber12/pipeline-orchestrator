from __future__ import annotations

from pathlib import Path

from src.daemon.handlers.idle import (
    IdleMixin,
    _merged_split_parent_aliases,
    _split_parent_of,
)
from src.queue_parser import TaskHeader


def _header(pr_id: str, depends_on: list[str]) -> TaskHeader:
    return TaskHeader(
        pr_id=pr_id,
        title=f"{pr_id} title",
        branch=pr_id.lower(),
        task_type="feature",
        complexity="low",
        depends_on=depends_on,
        priority=1,
        coder="any",
    )


def _unresolved_for(
    tmp_path: Path,
    *,
    depends_on: list[str],
    merged_pr_ids: set[str],
    extra_headers: list[TaskHeader] | None = None,
    skipped_legacy_pr_ids: set[str] | None = None,
) -> dict[str, list[str]]:
    _, unresolved_deps_map = (
        IdleMixin._filter_dag_headers_with_available_dependencies(
            [_header("PR-N", depends_on), *(extra_headers or [])],
            skipped_legacy_pr_ids=skipped_legacy_pr_ids or set(),
            task_dir=tmp_path,
            merged_pr_ids=merged_pr_ids,
        )
    )
    return unresolved_deps_map


def test_dependency_satisfied_by_merged_sub_task_when_no_siblings_known(
    tmp_path: Path,
) -> None:
    unresolved_deps_map = _unresolved_for(
        tmp_path,
        depends_on=["PR-305"],
        merged_pr_ids={"PR-305a"},
    )

    assert unresolved_deps_map == {}


def test_dependency_satisfied_by_any_of_multiple_sub_tasks(
    tmp_path: Path,
) -> None:
    unresolved_deps_map = _unresolved_for(
        tmp_path,
        depends_on=["PR-305"],
        merged_pr_ids={"PR-305a", "PR-305b", "PR-305c"},
    )

    assert unresolved_deps_map == {}


def test_dependency_blocked_by_partial_split_with_pending_sibling(
    tmp_path: Path,
) -> None:
    unresolved_deps_map = _unresolved_for(
        tmp_path,
        depends_on=["PR-305"],
        merged_pr_ids={"PR-305a"},
        extra_headers=[
            _header("PR-305a", []),
            _header("PR-305b", []),
            _header("PR-305c", []),
        ],
    )

    assert unresolved_deps_map == {"PR-N": ["PR-305"]}


def test_dependency_blocked_by_partial_split_with_legacy_pending_sibling(
    tmp_path: Path,
) -> None:
    unresolved_deps_map = _unresolved_for(
        tmp_path,
        depends_on=["PR-305"],
        merged_pr_ids={"PR-305a"},
        skipped_legacy_pr_ids={"PR-305b"},
    )

    assert unresolved_deps_map == {"PR-N": ["PR-305"]}


def test_unsplit_parent_still_works(tmp_path: Path) -> None:
    unresolved_deps_map = _unresolved_for(
        tmp_path,
        depends_on=["PR-300"],
        merged_pr_ids={"PR-300"},
    )

    assert unresolved_deps_map == {}


def test_dependency_unresolved_when_no_sub_task_merged(
    tmp_path: Path,
) -> None:
    unresolved_deps_map = _unresolved_for(
        tmp_path,
        depends_on=["PR-305"],
        merged_pr_ids=set(),
    )

    assert unresolved_deps_map == {"PR-N": ["PR-305"]}


def test_nested_split_resolves_parent(tmp_path: Path) -> None:
    unresolved_deps_map = _unresolved_for(
        tmp_path,
        depends_on=["PR-289"],
        merged_pr_ids={"PR-289b-2"},
    )

    assert unresolved_deps_map == {}


def test_dot_pr_id_parent_resolves(tmp_path: Path) -> None:
    unresolved_deps_map = _unresolved_for(
        tmp_path,
        depends_on=["PR-1.2"],
        merged_pr_ids={"PR-1.2a"},
    )

    assert unresolved_deps_map == {}


def test_dependency_on_sub_task_directly_unchanged(tmp_path: Path) -> None:
    unresolved_deps_map = _unresolved_for(
        tmp_path,
        depends_on=["PR-305b"],
        merged_pr_ids={"PR-305b"},
    )

    assert unresolved_deps_map == {}


def test_random_letter_in_pr_id_not_treated_as_sub_task(
    tmp_path: Path,
) -> None:
    unresolved_deps_map = _unresolved_for(
        tmp_path,
        depends_on=["PR-305"],
        merged_pr_ids={"PR-305zzz"},
    )

    assert unresolved_deps_map == {"PR-N": ["PR-305"]}


def test_parent_alias_excludes_pending_known_split_children() -> None:
    assert (
        _merged_split_parent_aliases(
            structured_pr_ids={"PR-305b"},
            merged_pr_ids={"PR-305a"},
        )
        == set()
    )


def test_parent_alias_excludes_pending_legacy_split_children() -> None:
    assert (
        _merged_split_parent_aliases(
            structured_pr_ids=set(),
            merged_pr_ids={"PR-305a"},
            skipped_legacy_pr_ids={"PR-305b"},
        )
        == set()
    )


def test_parent_alias_excludes_legacy_parent_even_when_child_merged() -> None:
    assert (
        _merged_split_parent_aliases(
            structured_pr_ids=set(),
            merged_pr_ids={"PR-305a"},
            skipped_legacy_pr_ids={"PR-305"},
        )
        == set()
    )


def test_parent_alias_includes_parent_after_all_known_children_merge() -> None:
    assert _merged_split_parent_aliases(
        structured_pr_ids={"PR-305a", "PR-305b"},
        merged_pr_ids={"PR-305a", "PR-305b"},
    ) == {"PR-305"}


def test_parent_alias_excludes_structured_parent_even_when_child_merged() -> None:
    assert (
        _merged_split_parent_aliases(
            structured_pr_ids={"PR-305", "PR-305a"},
            merged_pr_ids={"PR-305a"},
        )
        == set()
    )


def test_helper_returns_none_for_non_sub_task() -> None:
    assert _split_parent_of("PR-305") is None
    assert _split_parent_of("foo") is None
    assert _split_parent_of("") is None
