"""Real combined-status shapes must retain producer policy without invented trust."""

from copy import deepcopy

import pytest
from src.github import cache, checks
from src.models import CIStatus


@pytest.mark.parametrize("state", ["pending", "success", "failure"])
@pytest.mark.parametrize("identity", [{"creator": {"login": "testbed[bot]"}}, {"app_id": 123}])
def test_combined_status_without_creator_recovers_exact_history_identity(monkeypatch, state, identity):
    sha = "a" * 40
    status = {"id": 42, "context": "e2e/watch-merge-gate", "state": state,
              "created_at": "2026-10-29T05:47:21Z"}
    payload = {"state": state, "sha": sha, "total_count": 1, "statuses": [status]}
    paths = []

    def history(path):
        paths.append(path)
        # Older pending history must not override the combined endpoint's record.
        return [{**status, **identity}, {**status, "id": 41, "state": "pending", **identity}]

    checks.clear_ci_status_cache()
    monkeypatch.setattr(cache, "_gh_api_paginated_evidence", lambda path: cache.PaginatedEvidence(
        [{"total_count": 0, "check_runs": []}], True, True))
    monkeypatch.setattr(cache, "_etag_get_object_pages_evidence", lambda *args: cache.PaginatedEvidence(
        [deepcopy(payload)], True, False))
    monkeypatch.setattr(cache, "_gh_api_paginated", history)
    try:
        result = checks._retrieve_ci_status_evidence("owner/repo", sha, fresh=True)
        expected = {"pending": CIStatus.PENDING, "success": CIStatus.SUCCESS, "failure": CIStatus.FAILURE}[state]
        assert result.evidence.sources_complete
        assert checks._classify_ci_retrieval(result) == expected
        assert result.evidence.contexts[0].producer in {"user:testbed[bot]", "app:123"}
        assert paths == [f"repos/owner/repo/commits/{sha}/statuses?per_page=100"]
    finally:
        checks.clear_ci_status_cache()


@pytest.mark.parametrize("fault", [
    "unavailable", "nonlist", "empty", "wrong_id", "wrong_context", "wrong_state",
    "wrong_time", "wrong_sha", "no_identity", "duplicate", "missing_id", "zero_id", "bool_id",
])
def test_producer_hydration_does_not_guess_identity(monkeypatch, fault):
    sha = "a" * 40
    status = {"id": 42, "context": "gate", "state": "success", "created_at": "2026-10-29T05:47:21Z"}
    record = {**status, "creator": {"login": "testbed[bot]"}}
    if fault.startswith("wrong_"):
        record[{"wrong_id": "id", "wrong_context": "context", "wrong_state": "state",
                "wrong_time": "created_at", "wrong_sha": "sha"}[fault]] = "different"
    elif fault == "no_identity":
        record.pop("creator")
    elif fault == "missing_id":
        status.pop("id")
    elif fault in {"zero_id", "bool_id"}:
        status["id"] = 0 if fault == "zero_id" else True

    def history(path):
        if fault == "unavailable":
            raise RuntimeError("HTTP 403")
        if fault == "nonlist":
            return None
        if fault == "empty":
            return []
        return [record, record] if fault == "duplicate" else [record]

    monkeypatch.setattr(cache, "_gh_api_paginated", history)
    payload = {"statuses": [status]}
    checks._hydrate_status_producers("owner/repo", sha, payload)
    evidence = checks.evaluate_ci_evidence(repo="owner/repo", pr_number=1, sha=sha, statuses=payload["statuses"])
    assert evidence.policy_result == CIStatus.PENDING
    assert evidence.pending_reason == "missing_identity:gate"


@pytest.mark.parametrize("malformed", [{}, 7, None, "bad"])
def test_retrieve_ci_status_evidence_preserves_failure_with_malformed_entries(monkeypatch, malformed):
    sha = "a" * 40
    payload = {"sha": sha, "state": "failure", "total_count": 1, "statuses": [malformed]}
    checks.clear_ci_status_cache()
    monkeypatch.setattr(cache, "_gh_api_paginated_evidence", lambda path: cache.PaginatedEvidence(
        [{"total_count": 0, "check_runs": []}], True, True))
    monkeypatch.setattr(cache, "_etag_get_object_pages_evidence", lambda *args: cache.PaginatedEvidence(
        [deepcopy(payload)], True, False))
    try:
        result = checks._retrieve_ci_status_evidence("owner/repo", sha, fresh=True)
        assert result.status_source.complete is False
        assert result.status_source.error == "malformed"
        assert result.evidence.policy_result == CIStatus.FAILURE
        assert result.evidence.pending_reason is None
        assert result.status_payload == payload
    finally:
        checks.clear_ci_status_cache()


@pytest.mark.parametrize("scenario,expected", [
    ("success", CIStatus.SUCCESS), ("in_progress", CIStatus.PENDING),
    ("failure", CIStatus.FAILURE), ("missing_producer", CIStatus.PENDING),
    ("wrong_sha", CIStatus.PENDING), ("truncated", CIStatus.PENDING),
])
def test_full_check_history_recovers_hidden_sibling_without_accepting_stale_success(monkeypatch,scenario,expected):
    from urllib.parse import parse_qs, urlsplit
    from src.github import gh_runner
    sha = "fe32220c70efa4e4d08e58364b9b9171d536a0ce"
    unit = {"id":124802114874,"name":"unit","head_sha":sha,"status":"completed",
            "conclusion":"success","started_at":"2026-11-01T09:57:52Z","app":{"id":15368}}
    integration = {**unit,"id":124803304453,"name":"integration","started_at":"2026-11-01T09:03:33Z"}
    if scenario in {"in_progress", "failure"}:
        integration.update(
            status="in_progress" if scenario == "in_progress" else "completed",
            conclusion=None if scenario == "in_progress" else "failure",
        )
    elif scenario == "missing_producer":
        integration.pop("app")
    elif scenario == "wrong_sha":
        integration["head_sha"] = "b" * 40
    # History includes old failing unit attempts. They cannot override the
    # current unit, and its success cannot hide a newer integration retry.
    history = [unit] + [
        {**unit, "id": i, "conclusion": "failure", "started_at": "2026-10-31T09:00:00Z"} for i in range(1, 100)
    ]
    history.append(integration)
    if scenario == "in_progress":
        history.append(
            {
                **integration,
                "id": 124803304452,
                "status": "completed",
                "conclusion": "success",
                "started_at": "2026-11-01T08:00:00Z",
            }
        )
    paths = []

    def transport(args, **kwargs):
        assert args[0] == "api"
        url = urlsplit(args[1])
        query = parse_qs(url.query)
        paths.append(args[1])
        assert url.path == f"repos/owner/repo/commits/{sha}/check-runs"
        page = int(query["page"][0])
        # Reproduce PR-472: the default API/UI view exposes just unit.
        records = history if query.get("filter") == ["all"] else [unit]
        if scenario == "truncated" and page == 2:
            raise RuntimeError("HTTP 503 while reading second page")
        return {"total_count": len(records), "check_runs": deepcopy(records[(page - 1) * 100 : page * 100])}

    monkeypatch.setattr(gh_runner, "run_gh", transport)
    monkeypatch.setattr(cache, "retry_transient", lambda fn, **kwargs: fn())
    monkeypatch.setattr(
        cache, "_etag_get", lambda path: {"sha": sha, "state": "success", "total_count": 0, "statuses": []}
    )
    checks.clear_ci_status_cache()
    try:
        result = checks._retrieve_ci_status_evidence("owner/repo", sha, fresh=True)
        assert checks._classify_ci_retrieval(result, required_contexts=["unit", "integration"]) == expected
        assert len(paths) == 2 and all("filter=all" in path for path in paths)
        assert paths[-1].endswith("page=2")
        assert result.check_runs_source.complete is (scenario not in {"truncated", "wrong_sha"})
        if scenario == "success":
            assert len(result.check_runs) == 101
            assert {c.name for c in result.evidence.contexts} == {"unit", "integration"}
    finally:
        checks.clear_ci_status_cache()
