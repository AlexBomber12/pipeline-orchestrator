"""Recorded-shape publication activity through the real gh extraction boundary."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest
from src.github.prs import get_pr_head_publication, get_pr_last_push_time

HEAD = "63f07ef48a035c9aa31c37e0fbf3dc215ba0045e"
OLD = "a" * 40
REF = "refs/heads/feature/fix&one#two"
CREATED = {
    "activity_type": "branch_creation",
    "after": HEAD,
    "before": "0" * 40,
    "ref": REF,
    "timestamp": "2026-10-17T21:06:35Z",
}


def _transport(monkeypatch: pytest.MonkeyPatch, activity: object, *, head: dict | None = None) -> list[list[str]]:
    calls: list[list[str]] = []
    head = head if head is not None else {
        "branch": "feature/fix&one#two", "repo": "fork-owner/fork-repo", "sha": HEAD,
    }

    def fake_run(cmd: list[str], **_kwargs: object) -> SimpleNamespace:
        calls.append(cmd)
        payload = head if "/pulls/" in cmd[2] else activity
        return SimpleNamespace(returncode=0, stdout=json.dumps(payload), stderr="")

    monkeypatch.setattr("src.github.gh_runner.subprocess.run", fake_run)
    return calls


def test_branch_creation_proves_current_head_and_uses_encoded_ref(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = _transport(monkeypatch, [CREATED])
    evidence = get_pr_head_publication("owner/name", 42, expected_head_sha=HEAD)
    assert evidence.published_at == datetime(2026, 10, 17, 21, 6, 35, tzinfo=timezone.utc)
    assert evidence.provenance == "github-activity:branch_creation"
    assert (evidence.repo, evidence.pr_number, evidence.head_repo, evidence.ref, evidence.head_sha) == (
        "owner/name", 42, "fork-owner/fork-repo", REF, HEAD,
    )
    assert calls == [
        ["gh", "api", "repos/owner/name/pulls/42", "--jq",
         "{branch: .head.ref, repo: .head.repo.full_name, sha: .head.sha}"],
        ["gh", "api", (
            "repos/fork-owner/fork-repo/activity?"
            "ref=refs%2Fheads%2Ffeature%2Ffix%26one%23two&per_page=100&direction=desc"
        )],
    ]
    assert get_pr_last_push_time("owner/name", 42) == evidence.published_at


@pytest.mark.parametrize("kind", ["push", "force_push", "branch_creation"])
def test_newer_publication_invalidates_older_head(
    monkeypatch: pytest.MonkeyPatch, kind: str,
) -> None:
    newer = {**CREATED, "activity_type": kind, "after": OLD,
             "timestamp": "2026-10-17T21:10:00Z"}
    _transport(monkeypatch, [newer, CREATED])
    evidence = get_pr_head_publication("owner/name", 42, expected_head_sha=HEAD)
    assert evidence.published_at is None
    assert "differs" in (evidence.unknown_reason or "")


def test_newer_recreation_of_same_head_resets_freshness(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    newer = {**CREATED, "timestamp": "2026-10-17T21:10:00Z"}
    _transport(monkeypatch, [newer, CREATED])
    assert get_pr_last_push_time("owner/name", 42) == datetime(
        2026, 10, 17, 21, 10, tzinfo=timezone.utc
    )


@pytest.mark.parametrize("activity", [[], {}, [None],
                                        [CREATED, CREATED],
                                        [{**CREATED, "activity_type": "branch_deletion",
                                          "after": "0" * 40}],
                                        [{**CREATED, "after": ""}],
                                        [{**CREATED, "ref": "refs/heads/other"}],
                                        [{**CREATED, "timestamp": "bad"}]])
def test_missing_or_inconsistent_activity_stays_unknown(
    monkeypatch: pytest.MonkeyPatch, activity: object,
) -> None:
    _transport(monkeypatch, activity)
    evidence = get_pr_head_publication("owner/name", 42, expected_head_sha=HEAD)
    assert evidence.published_at is None
    assert evidence.unknown_reason


def test_observed_head_mismatch_does_not_read_activity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = _transport(monkeypatch, [CREATED])
    evidence = get_pr_head_publication("owner/name", 42, expected_head_sha=OLD)
    assert evidence.published_at is None
    assert evidence.unknown_reason == "observed PR HEAD changed"
    assert len(calls) == 1


def test_missing_head_repo_is_unknown(monkeypatch: pytest.MonkeyPatch) -> None:
    _transport(monkeypatch, [CREATED], head={"branch": "feature", "repo": None, "sha": HEAD})
    assert get_pr_last_push_time("owner/name", 42) is None


@pytest.mark.parametrize("head", [
    {"branch": "bad branch", "repo": "owner/name", "sha": HEAD},
    {"branch": "feature", "repo": "owner/name", "sha": "short"},
])
def test_invalid_head_context_is_unknown(
    monkeypatch: pytest.MonkeyPatch, head: dict,
) -> None:
    calls = _transport(monkeypatch, [CREATED], head=head)
    evidence = get_pr_head_publication("owner/name", 42)
    assert evidence.published_at is None
    assert evidence.unknown_reason
    assert len(calls) == 1


def test_transport_failure_is_unknown(monkeypatch: pytest.MonkeyPatch) -> None:
    def fail(*_args: object, **_kwargs: object) -> None:
        raise RuntimeError("gh unavailable")

    monkeypatch.setattr("src.github.gh_runner.subprocess.run", fail)
    evidence = get_pr_head_publication("owner/name", 42)
    assert evidence.published_at is None
    assert evidence.unknown_reason == "publication read unavailable: RuntimeError"
