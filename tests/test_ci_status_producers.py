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
