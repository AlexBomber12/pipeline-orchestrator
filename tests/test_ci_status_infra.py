"""PR-251 (OBS-BC): infra-class CI failure classification tests.

Covers ``_map_rest_ci_status_to_enum``'s INFRA_FAILURE branch and the
``_is_infra_failure`` predicate. WATCH-handler retry logic is exercised
in ``tests/runner/test_handle_watch.py``.
"""

from __future__ import annotations

from typing import Any

import pytest

from src.github.checks import (
    _is_infra_failure,
    _map_rest_ci_status_to_enum,
    _maybe_hydrate_annotations,
)
from src.models import CIStatus


def _failed_logic_run() -> dict:
    """Logic-class failure check-run with no infra signals."""
    return {"conclusion": "failure", "annotations": []}


def _failed_infra_conclusion_run(conclusion: str = "cancelled") -> dict:
    """Failure check-run whose conclusion itself is infra-class."""
    return {"conclusion": conclusion, "annotations": []}


def _failed_infra_annotation_run(message: str) -> dict:
    """Failure check-run with conclusion=failure but an infra-keyword annotation."""
    return {"conclusion": "failure", "annotations": [{"message": message}]}


def test_pure_infra_failure_classifies_as_infra() -> None:
    runs = [
        _failed_infra_conclusion_run("cancelled"),
        _failed_infra_conclusion_run("action_required"),
        _failed_infra_conclusion_run("stale"),
    ]
    assert _map_rest_ci_status_to_enum(runs, {}) == CIStatus.INFRA_FAILURE


def test_pure_logic_failure_classifies_as_failure() -> None:
    runs = [_failed_logic_run(), _failed_logic_run()]
    assert _map_rest_ci_status_to_enum(runs, {}) == CIStatus.FAILURE


def test_mixed_failure_classifies_as_logic() -> None:
    """At least one logic-class failure must dominate the rollup."""
    runs = [
        _failed_infra_conclusion_run("cancelled"),
        _failed_logic_run(),
    ]
    assert _map_rest_ci_status_to_enum(runs, {}) == CIStatus.FAILURE


def test_annotation_keyword_match_case_insensitive() -> None:
    runs = [_failed_infra_annotation_run("Runner OFFLINE on host abc")]
    assert _map_rest_ci_status_to_enum(runs, {}) == CIStatus.INFRA_FAILURE


def test_annotation_keyword_no_match_keeps_failure() -> None:
    """An annotation message without any infra keyword leaves the result as FAILURE."""
    runs = [_failed_infra_annotation_run("AssertionError: expected 5 got 3")]
    assert _map_rest_ci_status_to_enum(runs, {}) == CIStatus.FAILURE


def test_combined_status_failure_dominates_infra_check_runs() -> None:
    """A legacy commit-status failure has no annotations to inspect, so it
    wins as logic FAILURE even when every check-run is infra-class —
    under-classifying infra is safer than skipping a real failing
    status context.
    """
    runs = [_failed_infra_conclusion_run("cancelled")]
    status_payload = {
        "state": "failure",
        "statuses": [{"context": "ci/legacy", "state": "failure"}],
    }
    assert (
        _map_rest_ci_status_to_enum(runs, status_payload) == CIStatus.FAILURE
    )


def test_stale_alone_classifies_as_infra() -> None:
    """``stale`` is a check-run conclusion that previously fell through to
    PENDING; PR-251 promotes it into the failure rollup so it can be
    rerun once.
    """
    runs = [_failed_infra_conclusion_run("stale")]
    assert _map_rest_ci_status_to_enum(runs, {}) == CIStatus.INFRA_FAILURE


def test_pending_with_no_failures_unchanged() -> None:
    """A non-failing rollup is unaffected by the new INFRA branch."""
    runs = [{"status": "in_progress"}]
    assert _map_rest_ci_status_to_enum(runs, {}) == CIStatus.PENDING


def test_success_rollup_unchanged() -> None:
    runs = [{"conclusion": "success"}]
    assert _map_rest_ci_status_to_enum(runs, {}) == CIStatus.SUCCESS


def test_is_infra_failure_handles_missing_annotations() -> None:
    """Logic-class conclusion with no annotations is not infra."""
    assert _is_infra_failure({"conclusion": "failure"}) is False


def test_is_infra_failure_handles_malformed_annotations() -> None:
    """Non-list / non-dict annotation payloads are tolerated."""
    assert _is_infra_failure({"conclusion": "failure", "annotations": "oops"}) is False
    assert (
        _is_infra_failure(
            {"conclusion": "failure", "annotations": [None, 7, {"message": None}]}
        )
        is False
    )


def test_is_infra_failure_uppercase_conclusion() -> None:
    """``CANCELLED`` (uppercase) is matched as infra by the predicate."""
    assert _is_infra_failure({"conclusion": "CANCELLED"}) is True


def test_maybe_hydrate_annotations_skips_when_already_populated(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """PR-251 follow-up: a pre-populated ``annotations`` field is left
    untouched (test fixtures inject the field directly; hydration would
    otherwise mask their intent)."""
    calls: list[str] = []

    def fake_paginated(path: str) -> Any:
        calls.append(path)
        return []

    monkeypatch.setattr("src.github.cache._gh_api_paginated", fake_paginated)
    run = {
        "id": 1,
        "conclusion": "failure",
        "annotations_count": 3,
        "annotations": [{"message": "preset"}],
    }
    _maybe_hydrate_annotations("owner/name", run)
    assert run["annotations"] == [{"message": "preset"}]
    assert calls == []


def test_maybe_hydrate_annotations_skips_when_count_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """PR-251 follow-up: a check-run without ``annotations_count`` (or
    a zero count) does not trigger an annotations REST call."""
    calls: list[str] = []

    def fake_paginated(path: str) -> Any:
        calls.append(path)
        return []

    monkeypatch.setattr("src.github.cache._gh_api_paginated", fake_paginated)
    run_no_count = {"id": 5, "conclusion": "failure"}
    _maybe_hydrate_annotations("owner/name", run_no_count)
    assert "annotations" not in run_no_count
    run_zero = {"id": 5, "conclusion": "failure", "annotations_count": 0}
    _maybe_hydrate_annotations("owner/name", run_zero)
    assert "annotations" not in run_zero
    assert calls == []


def test_maybe_hydrate_annotations_skips_when_id_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """PR-251 follow-up: a failing check-run without an integer ``id``
    can't have its annotations endpoint addressed; hydration must
    no-op rather than guess at a path."""
    calls: list[str] = []

    def fake_paginated(path: str) -> Any:
        calls.append(path)
        return []

    monkeypatch.setattr("src.github.cache._gh_api_paginated", fake_paginated)
    run = {"conclusion": "failure", "annotations_count": 1}
    _maybe_hydrate_annotations("owner/name", run)
    assert "annotations" not in run
    assert calls == []
