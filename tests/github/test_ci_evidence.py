from __future__ import annotations

from datetime import datetime, timezone

from src.github.ci_evidence import CIContextEvidence, _newer, evaluate_ci_evidence
from src.models import CIStatus

SHA = "a" * 40
OTHER = "b" * 40
NOW = datetime(2026, 10, 1, 12, tzinfo=timezone.utc)


def run(name: str, conclusion: str | None = "success", **kw: object) -> dict:
    payload = {
        "name": name,
        "head_sha": kw.pop("sha", SHA),
        "conclusion": conclusion,
        "status": kw.pop("status", None),
        "run_attempt": kw.pop("attempt", 1),
        "completed_at": kw.pop("completed_at", "2026-10-01T12:00:00Z"),
    }
    app_id = kw.pop("app_id", 1)
    if app_id is not None:
        payload["app"] = {"id": app_id, "slug": f"app-{app_id}"}
    payload.update(kw)
    return payload


def status(name: str, state: str = "success", **kw: object) -> dict:
    payload = {"context": name, "state": state, "sha": kw.pop("sha", SHA), "updated_at": "2026-10-01T12:00:00Z"}
    creator = kw.pop("creator", "ci-bot")
    if creator is not None:
        payload["creator"] = {"login": creator}
    payload.update(kw)
    return payload


def evaluate(**kw: object):
    observed_at = kw.pop("observed_at", NOW)
    return evaluate_ci_evidence(repo="octo/demo", pr_number=7, sha=kw.pop("sha", SHA), observed_at=observed_at, **kw)


def test_required_contexts_must_be_successful_on_the_current_sha() -> None:
    missing = evaluate(check_runs=[run("unit")], required_contexts=["unit", "integration"])
    success = evaluate(
        check_runs=[run("unit"), run("integration", app_id=2)],
        required_contexts=["unit", "integration"],
    )
    foreign = evaluate(check_runs=[run("unit", sha=OTHER)], required_contexts=["unit"], empty_is_success=True)
    foreign_empty = evaluate(statuses=[status("legacy", sha="")], empty_is_success=True)
    unbound = evaluate(check_runs=[{"name": "unit", "conclusion": "success"}], empty_is_success=True)
    nameless_run = evaluate(check_runs=[{"head_sha": SHA, "conclusion": "failure"}], empty_is_success=True)
    nameless_status = evaluate(statuses=[{"sha": SHA, "state": "failure"}], empty_is_success=True)

    assert missing.policy_result == CIStatus.PENDING
    assert missing.pending_reason == "missing_required:integration"
    assert success.policy_result == CIStatus.SUCCESS
    assert success.pending_reason is None
    assert foreign.contexts == ()
    assert foreign.pending_reason == "conflicting_sha"
    assert foreign_empty.policy_result == CIStatus.PENDING
    assert foreign_empty.pending_reason == "conflicting_sha"
    assert unbound.policy_result == CIStatus.PENDING
    assert unbound.pending_reason == "conflicting_sha"
    assert nameless_run.pending_reason == "conflicting_sha"
    assert nameless_status.pending_reason == "conflicting_sha"
    assert missing.repo == "octo/demo"
    assert missing.pr_number == 7
    for invalid_sha in ("", SHA[:7]):
        invalid = evaluate(sha=invalid_sha, check_runs=[run("unit", sha=invalid_sha)], required_contexts=["unit"])
        assert invalid.pending_reason == "conflicting_sha"


def test_partial_sources_cannot_pass_but_known_failure_remains_visible() -> None:
    unknown = evaluate(check_runs_complete=False, required_contexts=[], empty_is_success=True)
    failed = evaluate(check_runs=[run("unit", "failure")], statuses_complete=False, required_contexts=["unit"])

    assert unknown.sources_complete is False
    assert unknown.policy_result == CIStatus.PENDING
    assert unknown.pending_reason == "sources_incomplete"
    assert failed.policy_result == CIStatus.FAILURE
    assert failed.pending_reason is None


def test_reruns_use_authoritative_attempt_ordering() -> None:
    pending = evaluate(
        check_runs=[run("unit", "success", attempt=1), run("unit", None, status="in_progress", attempt=2)],
        required_contexts=["unit"],
    )
    recovered = evaluate(
        check_runs=[run("unit", "failure", attempt=1), run("unit", "success", attempt=2)],
        required_contexts=["unit"],
    )
    equal_attempt = evaluate(
        check_runs=[
            run("unit", "success", completed_at="2026-10-01T11:00:00Z"),
            run("unit", "failure", completed_at="2026-10-01T12:00:00Z"),
        ],
        required_contexts=["unit"],
    )
    older = run("unit", "success", attempt=2, id=1, started_at="2026-10-01T10:00:00Z",
                completed_at="2026-10-01T13:00:00Z")
    for newer, expected in [
        (run("unit", None, status="in_progress", attempt=1, started_at="2026-10-01T12:00:00Z",
             completed_at=None), CIStatus.PENDING),
        (run("unit", "failure", attempt=1, started_at="2026-10-01T12:00:00Z",
             completed_at=None), CIStatus.FAILURE),
        (run("unit", "failure", attempt=2, id=2, started_at="2026-10-01T10:00:00Z",
             completed_at=None), CIStatus.FAILURE),
    ]:
        for records in ([older, newer], [newer, older]):
            overlapping = evaluate(check_runs=records, required_contexts=["unit"])
            assert overlapping.policy_result == expected

    assert pending.policy_result == CIStatus.PENDING
    assert pending.pending_reason == "required_not_success:unit"
    assert pending.contexts[-1].state == "pending"
    assert recovered.policy_result == CIStatus.SUCCESS
    assert equal_attempt.policy_result == CIStatus.FAILURE


def test_ambiguous_or_missing_identity_does_not_authorize_success() -> None:
    ambiguous = evaluate(
        check_runs=[run("unit", "success", app_id=1), run("unit", "success", app_id=2)],
        required_contexts=["unit"],
    )
    missing_identity = evaluate(check_runs=[run("unit", app_id=None, node_id="record")], required_contexts=["unit"])

    assert ambiguous.policy_result == CIStatus.PENDING
    assert ambiguous.pending_reason == "ambiguous_required:unit"
    assert missing_identity.policy_result == CIStatus.PENDING
    assert missing_identity.pending_reason == "missing_identity:unit"


def test_no_required_list_requires_complete_nonempty_success_unless_exempt() -> None:
    no_contexts = evaluate()
    empty_ok = evaluate(empty_is_success=True)
    empty_required = evaluate(required_contexts=["", " "])
    empty_required_ok = evaluate(required_contexts=[], empty_is_success=True)
    all_success = evaluate(check_runs=[run("unit", "neutral")], statuses=[status("legacy")])
    context_pending = evaluate(check_runs=[run("unit", None, status="completed")])
    ambiguous = evaluate(check_runs=[run("unit", app_id=1), run("unit", app_id=2)])
    missing_identity = evaluate(check_runs=[run("unit", app_id=None)])

    assert no_contexts.pending_reason == "no_contexts"
    assert empty_ok.policy_result == CIStatus.SUCCESS
    assert empty_required.pending_reason == "no_contexts"
    assert empty_required_ok.policy_result == CIStatus.SUCCESS
    assert all_success.policy_result == CIStatus.SUCCESS
    assert context_pending.policy_result == CIStatus.PENDING
    assert context_pending.pending_reason == "context_pending"
    assert ambiguous.pending_reason == "ambiguous_context:unit"
    assert missing_identity.pending_reason == "missing_identity:unit"


def test_status_contexts_and_normalization_fallbacks() -> None:
    naive = datetime(2026, 10, 1, 12)
    evidence = evaluate(
        check_runs=[
            run("slugged", "weird", app={"slug": "actions"}, app_id=None, attempt="2", completed_at=naive),
            run(
                "fallback", "success", app_id=None, target_url="https://ci.example/runs/1",
                completed_at="bad", started_at="bad"
            ),
            run("legacy", app_id=99),
        ],
        statuses=[
            status("foreign", sha=OTHER),
            {"context": "legacy", "state": "success", "app_id": 99},
        ],
        required_contexts=["legacy"],
        observed_at=naive,
    )

    by_name = {ctx.name: ctx for ctx in evidence.contexts}
    assert "foreign" not in by_name
    assert by_name["slugged"].producer == "app:actions"
    assert by_name["slugged"].attempt == 2
    assert by_name["slugged"].observed_at == naive.replace(tzinfo=timezone.utc)
    assert by_name["slugged"].state == "unknown"
    assert by_name["fallback"].producer is None
    assert by_name["legacy"].producer == "app:99"
    assert evidence.observed_at == naive.replace(tzinfo=timezone.utc)

    legacy_recovered = evaluate(
        statuses=[
            status("legacy", "failure", target_url="https://ci.example/old", updated_at="2026-10-01T11:00:00Z"),
            status("legacy", "success", target_url="https://ci.example/new", updated_at="2026-10-01T12:00:00Z"),
        ],
        required_contexts=["legacy"],
    )
    node_recovered = evaluate(
        statuses=[
            status("legacy", "failure", node_id="old", updated_at="2026-10-01T11:00:00Z"),
            status("legacy", "success", node_id="new", updated_at="2026-10-01T12:00:00Z"),
        ],
        required_contexts=["legacy"],
    )
    assert legacy_recovered.policy_result == CIStatus.SUCCESS
    assert node_recovered.policy_result == CIStatus.SUCCESS


def test_slug_node_id_datetime_now_and_helper_tie_breaks() -> None:
    observed = evaluate_ci_evidence(
        repo="octo/demo",
        pr_number=7,
        sha=SHA,
        check_runs=[{"name": "slug", "head_sha": SHA, "conclusion": "success", "app": {"slug": "actions"}}],
        statuses=[{"name": "legacy", "status": "success", "sha": SHA, "node_id": "node-1"}],
        required_contexts=["slug"],
    )
    left = CIContextEvidence("unit", "success", SHA)
    right = CIContextEvidence("unit", "pending", SHA)
    attempted = CIContextEvidence("unit", "success", SHA, attempt=1)
    identified = CIContextEvidence("unit", "success", SHA, run_id=1)
    newer_time = CIContextEvidence("unit", "success", SHA, observed_at=NOW)
    older_time = CIContextEvidence("unit", "pending", SHA, observed_at=datetime(2026, 10, 1, 11, tzinfo=timezone.utc))
    same_time = CIContextEvidence("unit", "success", SHA, observed_at=NOW)

    assert observed.policy_result == CIStatus.SUCCESS
    assert observed.observed_at.tzinfo is not None
    assert observed.contexts[0].producer == "app:actions"
    assert observed.contexts[1].producer is None
    assert _newer(left, right) is True
    assert _newer(newer_time, older_time) is True
    assert _newer(newer_time, same_time) is True
    assert _newer(attempted, right) is True
    assert _newer(right, attempted) is False
    assert _newer(identified, left) is True
    assert _newer(left, identified) is False
