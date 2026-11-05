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

    def fake_run_gh(args: list[str], **kwargs: Any) -> Any:
        calls.append(" ".join(args))
        return []

    monkeypatch.setattr("src.github.gh_runner.run_gh", fake_run_gh)
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

    def fake_run_gh(args: list[str], **kwargs: Any) -> Any:
        calls.append(" ".join(args))
        return []

    monkeypatch.setattr("src.github.gh_runner.run_gh", fake_run_gh)
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

    def fake_run_gh(args: list[str], **kwargs: Any) -> Any:
        calls.append(" ".join(args))
        return []

    monkeypatch.setattr("src.github.gh_runner.run_gh", fake_run_gh)
    run = {"conclusion": "failure", "annotations_count": 1}
    _maybe_hydrate_annotations("owner/name", run)
    assert "annotations" not in run
    assert calls == []


def test_maybe_hydrate_annotations_bounds_to_first_and_last_page(monkeypatch):
    calls = []
    def fetch(args, **kwargs):
        calls.append(args)
        return [] if "&page=" in args[1] else [{"message": "Runner offline; could not start the job."}]
    monkeypatch.setattr("src.github.gh_runner.run_gh", fetch)
    run = {"id": 99, "conclusion": "failure", "annotations_count": 500}
    _maybe_hydrate_annotations("owner/name", run)
    assert len(calls) == 2
    assert all("--paginate" not in call and "--slurp" not in call for call in calls)
    assert calls[1][1].endswith("&page=10")
    assert len(run["annotations"]) == 1


def _structured_check(monkeypatch, **changes):
    import json
    from src.github import checks

    verdict = {"repo": "octo/demo", "sha": "a" * 40, "run_id": 123,
               "attempt": 1, "failed": 3, "infra": 3}
    verdict.update(changes)
    check = {"id": 456, "name": "integration", "head_sha": "a" * 40,
             "app": {"id": 15368}, "conclusion": "failure", "status": "completed",
             "details_url": "https://github.com/octo/demo/actions/runs/123/job/456",
             "annotations": [{"title": "pipeline-ci-outcome:v1", "message": json.dumps(verdict)}]}
    workflow = {"databaseId": 123, "headSha": "a" * 40, "attempt": 1,
                "status": "completed", "conclusion": "failure",
                "jobs": [{"databaseId": 456, "name": "integration", "conclusion": "failure"}]}
    monkeypatch.setattr(checks.gh_runner, "run_gh", lambda *args, **kwargs: workflow)
    return check, workflow


@pytest.mark.parametrize("changes", [
    {}, {"infra": 2}, {"failed": 0, "infra": 0}, {"attempt": True}, {"failed": "3"},
    {"attempt": 2}, {"sha": "b" * 40}, {"run_id": 124}, {"repo": "other/repo"},
])
def test_structured_job_requires_pure_external_failure_and_exact_attempt(monkeypatch, changes):
    check, _run = _structured_check(monkeypatch, **changes)
    expected = None if changes == {"attempt": 2} else not bool(changes)
    assert _is_infra_failure(check) is expected


@pytest.mark.parametrize("boundary", ["untrusted", "bad_json", "not_object", "missing_message",
                                     "duplicate", "wrong_job", "wrong_name", "running", "unavailable"])
def test_structured_job_never_uses_invalid_or_unavailable_proof_as_infra(monkeypatch, boundary):
    from src.github import checks
    check, run = _structured_check(monkeypatch)
    if boundary == "untrusted":
        check["app"] = None
    elif boundary == "bad_json":
        check["annotations"][0]["message"] = "infrastructure error HTTP 500"
    elif boundary == "not_object":
        check["annotations"][0]["message"] = "[]"
    elif boundary == "missing_message":
        check["annotations"][0]["message"] = None
    elif boundary == "duplicate":
        check["annotations"] *= 2
    elif boundary == "wrong_job":
        run["jobs"][0]["databaseId"] = 457
    elif boundary == "wrong_name":
        run["jobs"][0]["name"] = "unit"
    elif boundary == "running":
        run["status"] = "in_progress"
    else:
        def unavailable(*args, **kwargs):
            raise RuntimeError("HTTP 503")
        monkeypatch.setattr(checks.gh_runner, "run_gh", unavailable)
    assert _is_infra_failure(check) is (None if boundary in {"wrong_job", "wrong_name", "running", "unavailable"}
                                       else False)


@pytest.mark.parametrize("additional_logic", [False, True])
def test_structured_unknown_provenance_defers_without_hiding_real_failure(monkeypatch, additional_logic):
    from src.github import checks
    from src.github.checks import _CiSourceResult, _make_ci_retrieval
    check, _ = _structured_check(monkeypatch)
    def unavailable(*args, **kwargs):
        raise RuntimeError("HTTP 503")
    monkeypatch.setattr(checks.gh_runner, "run_gh", unavailable)
    runs = [check]
    if additional_logic:
        runs.append({**check, "id": 789, "name": "unit", "annotations": []})
    source = _CiSourceResult(True, False, None)
    retrieval = _make_ci_retrieval("octo/demo", "a" * 40, 1.0, runs,
                                  {"sha": "a" * 40, "state": "success", "statuses": []}, source, source, None)
    assert checks._classify_ci_retrieval(retrieval) == (CIStatus.FAILURE if additional_logic else CIStatus.PENDING)


def test_structured_verdict_is_bounded_and_invalid_does_not_use_keyword_fallback(monkeypatch):
    check, _ = _structured_check(monkeypatch)
    check["annotations"][0]["message"] = "operation timed out " * 200
    assert _is_infra_failure(check) is False


@pytest.mark.parametrize("unavailable", [False, True])
def test_final_job_verdict_hydrates_after_many_annotations_and_unknown_does_not_repair_code(monkeypatch, unavailable):
    check, _ = _structured_check(monkeypatch)
    summary = check.pop("annotations")
    check["annotations_count"] = 501
    calls = []
    def fetch(args, **kwargs):
        if args[0] != "api":
            return {"databaseId": 123, "headSha": "a" * 40, "attempt": 1,
                    "status": "completed", "conclusion": "failure",
                    "jobs": [{"databaseId": 456, "name": "integration", "conclusion": "failure"}]}
        calls.append(args)
        if "&page=" not in args[1]:
            return [{"message": "generic error"}] * 50
        if unavailable:
            return "malformed transport response"
        return summary
    monkeypatch.setattr("src.github.gh_runner.run_gh", fetch)
    _maybe_hydrate_annotations("octo/demo", check)
    assert len(calls) == 2 and calls[-1][1].endswith("&page=11")
    assert _is_infra_failure(check) is (None if unavailable else True)
