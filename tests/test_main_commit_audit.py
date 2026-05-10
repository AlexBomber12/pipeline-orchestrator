from __future__ import annotations

import asyncio
import logging

from src.daemon import main_commit_audit
from tests.runner._helpers import _FakeRedis


def _summary(sha: str) -> dict:
    return {"sha": sha}


def _commit(message: str, parents: list[str]) -> dict:
    return {
        "commit": {"message": message},
        "parents": [{"sha": sha} for sha in parents],
    }


def _check_runs(*conclusions: str) -> dict:
    return {"check_runs": [{"conclusion": conclusion} for conclusion in conclusions]}


def test_audit_direct_commit_flagged(monkeypatch):
    def fake_run_gh(args):
        path = args[1]
        if path.endswith("/commits?sha=main&per_page=10"):
            return [_summary("abc1234")]
        if path.endswith("/commits/abc1234"):
            return _commit("direct hotfix", ["parent"])
        raise AssertionError(args)

    monkeypatch.setattr(main_commit_audit, "run_gh", fake_run_gh)

    findings = main_commit_audit.audit_main_commits("octo/demo")

    assert [finding.violation_category for finding in findings] == [
        "direct_commit_no_pr"
    ]
    assert findings[0].short_sha == "abc1234"
    assert findings[0].parent_count == 1


def test_audit_merge_commit_with_pr_passing_ci_clean(monkeypatch):
    def fake_run_gh(args):
        path = args[1]
        if path.endswith("/commits?sha=main&per_page=10"):
            return [_summary("merge42")]
        if path.endswith("/commits/merge42"):
            return _commit("Merge pull request #42 from feature-x", ["base", "head"])
        if path.endswith("/commits/head/check-runs"):
            return _check_runs("success")
        raise AssertionError(args)

    monkeypatch.setattr(main_commit_audit, "run_gh", fake_run_gh)

    assert main_commit_audit.audit_main_commits("octo/demo") == []


def test_audit_merge_commit_with_pr_failed_ci_flagged(monkeypatch):
    def fake_run_gh(args):
        path = args[1]
        if path.endswith("/commits?sha=main&per_page=10"):
            return [_summary("merge42")]
        if path.endswith("/commits/merge42"):
            return _commit("Merge pull request #42 from feature-x", ["base", "head"])
        if path.endswith("/commits/head/check-runs"):
            return _check_runs("failure")
        raise AssertionError(args)

    monkeypatch.setattr(main_commit_audit, "run_gh", fake_run_gh)

    findings = main_commit_audit.audit_main_commits("octo/demo")

    assert [finding.violation_category for finding in findings] == [
        "merge_commit_pr_failed_ci"
    ]
    assert findings[0].pr_number == 42


def test_audit_merge_commit_no_check_runs_flagged(monkeypatch):
    def fake_run_gh(args):
        path = args[1]
        if path.endswith("/commits?sha=main&per_page=10"):
            return [_summary("merge42")]
        if path.endswith("/commits/merge42"):
            return _commit("Merge pull request #42 from feature-x", ["base", "head"])
        if path.endswith("/commits/head/check-runs"):
            return {"check_runs": []}
        raise AssertionError(args)

    monkeypatch.setattr(main_commit_audit, "run_gh", fake_run_gh)

    findings = main_commit_audit.audit_main_commits("octo/demo")

    assert [finding.violation_category for finding in findings] == [
        "merge_commit_pr_failed_ci"
    ]


def test_audit_merge_commit_message_unparseable_flagged(monkeypatch):
    def fake_run_gh(args):
        path = args[1]
        if path.endswith("/commits?sha=main&per_page=10"):
            return [_summary("mergeish")]
        if path.endswith("/commits/mergeish"):
            return _commit("hotfix", ["base", "head"])
        raise AssertionError(args)

    monkeypatch.setattr(main_commit_audit, "run_gh", fake_run_gh)

    findings = main_commit_audit.audit_main_commits("octo/demo")

    assert [finding.violation_category for finding in findings] == [
        "merge_commit_pr_unverified"
    ]


def test_audit_squash_merge_default_message(monkeypatch):
    def fake_run_gh(args):
        path = args[1]
        if path.endswith("/commits?sha=main&per_page=10"):
            return [_summary("squash42")]
        if path.endswith("/commits/squash42"):
            return _commit("Feature complete (#42)", ["base"])
        raise AssertionError(args)

    monkeypatch.setattr(main_commit_audit, "run_gh", fake_run_gh)

    findings = main_commit_audit.audit_main_commits("octo/demo")

    assert [finding.violation_category for finding in findings] == [
        "direct_commit_no_pr"
    ]
    assert findings[0].pr_number is None


def test_audit_octopus_merge_flagged(monkeypatch):
    def fake_run_gh(args):
        path = args[1]
        if path.endswith("/commits?sha=main&per_page=10"):
            return [_summary("octopus")]
        if path.endswith("/commits/octopus"):
            return _commit("octopus merge", ["base", "head1", "head2"])
        raise AssertionError(args)

    monkeypatch.setattr(main_commit_audit, "run_gh", fake_run_gh)

    findings = main_commit_audit.audit_main_commits("octo/demo")

    assert [finding.violation_category for finding in findings] == [
        "merge_commit_pr_unverified"
    ]
    assert findings[0].parent_count == 3


def test_audit_skips_already_audited_shas(monkeypatch):
    processed: list[str] = []

    def fake_run_gh(args):
        path = args[1]
        if path.endswith("/commits?sha=main&per_page=10"):
            return [_summary("abc123"), _summary("def456"), _summary("ghi789")]
        if "/commits/" in path:
            sha = path.rsplit("/", 1)[-1]
            processed.append(sha)
            return _commit(f"direct {sha}", ["parent"])
        raise AssertionError(args)

    monkeypatch.setattr(main_commit_audit, "run_gh", fake_run_gh)

    findings = main_commit_audit.audit_main_commits(
        "octo/demo",
        audited_shas={"abc123"},
    )

    assert processed == ["def456", "ghi789"]
    assert [finding.sha for finding in findings] == ["def456", "ghi789"]


def test_audit_returns_empty_when_lookback_zero(monkeypatch):
    calls = []

    def fake_run_gh(args):
        calls.append(args)
        return []

    monkeypatch.setattr(main_commit_audit, "run_gh", fake_run_gh)

    assert main_commit_audit.audit_main_commits("octo/demo", lookback_n=0) == []
    assert calls == []


def test_audit_handles_gh_api_error_gracefully(monkeypatch, caplog):
    def fake_run_gh(args):
        raise RuntimeError("gh unavailable")

    monkeypatch.setattr(main_commit_audit, "run_gh", fake_run_gh)

    with caplog.at_level(logging.ERROR):
        findings = main_commit_audit.audit_main_commits("octo/demo")

    assert findings == []
    assert "Failed to list recent main commits" in caplog.text


def test_audit_helper_handles_malformed_payloads(monkeypatch):
    def fake_run_gh(args):
        path = args[1]
        if path.endswith("/commits/bad"):
            return {"commit": {}, "parents": "not-a-list"}
        raise AssertionError(args)

    monkeypatch.setattr(main_commit_audit, "run_gh", fake_run_gh)

    findings, checked = main_commit_audit.audit_main_commit_shas("octo/demo", ["bad"])

    assert checked == ["bad"]
    assert [finding.violation_category for finding in findings] == [
        "merge_commit_pr_unverified"
    ]
    assert findings[0].message_first_line == ""


def test_audit_merge_commit_missing_head_sha_flagged(monkeypatch):
    def fake_run_gh(args):
        path = args[1]
        if path.endswith("/commits/merge42"):
            return {
                "commit": {"message": "Merge pull request #42 from feature-x"},
                "parents": [{"sha": "base"}, {}],
            }
        raise AssertionError(args)

    monkeypatch.setattr(main_commit_audit, "run_gh", fake_run_gh)

    findings, checked = main_commit_audit.audit_main_commit_shas(
        "octo/demo",
        ["merge42"],
    )

    assert checked == ["merge42"]
    assert [finding.violation_category for finding in findings] == [
        "merge_commit_pr_unverified"
    ]
    assert findings[0].pr_number == 42


def test_audit_commit_fetch_error_returns_partial_findings(monkeypatch, caplog):
    def fake_run_gh(args):
        path = args[1]
        if path.endswith("/commits/good"):
            return _commit("direct hotfix", ["parent"])
        if path.endswith("/commits/bad"):
            raise RuntimeError("gh unavailable")
        raise AssertionError(args)

    monkeypatch.setattr(main_commit_audit, "run_gh", fake_run_gh)

    with caplog.at_level(logging.ERROR):
        findings, checked = main_commit_audit.audit_main_commit_shas(
            "octo/demo",
            ["good", "bad", "later"],
        )

    assert checked == ["good"]
    assert [finding.sha for finding in findings] == ["good"]
    assert "Failed to audit main commit bad" in caplog.text


def test_list_recent_main_commit_shas_handles_empty_and_malformed(monkeypatch):
    calls = []

    def fake_run_gh(args):
        calls.append(args)
        return [{"sha": "good"}, {"no_sha": "bad"}, "bad"]

    monkeypatch.setattr(main_commit_audit, "run_gh", fake_run_gh)

    assert main_commit_audit.list_recent_main_commit_shas("octo/demo", 0) == []
    assert main_commit_audit.list_recent_main_commit_shas("octo/demo", 2) == [
        "good"
    ]
    assert calls == [
        ["api", "repos/octo/demo/commits?sha=main&per_page=2"],
    ]
    assert main_commit_audit._commit_shas({"sha": "not-a-list"}) == []


def test_audit_check_runs_malformed_payload_flagged(monkeypatch):
    def fake_run_gh(args):
        path = args[1]
        if path.endswith("/commits/merge42"):
            return _commit("Merge pull request #42 from feature-x", ["base", "head"])
        if path.endswith("/commits/head/check-runs"):
            return {"check_runs": "not-a-list"}
        raise AssertionError(args)

    monkeypatch.setattr(main_commit_audit, "run_gh", fake_run_gh)

    findings, checked = main_commit_audit.audit_main_commit_shas(
        "octo/demo",
        ["merge42"],
    )

    assert checked == ["merge42"]
    assert [finding.violation_category for finding in findings] == [
        "merge_commit_pr_failed_ci"
    ]


def test_redis_helpers_record_and_load_audit_state():
    redis = _FakeRedis()
    finding = main_commit_audit.MainCommitAuditFinding(
        sha="abc1234",
        short_sha="abc1234",
        message_first_line="direct hotfix",
        parent_count=1,
        pr_number=None,
        violation_category="direct_commit_no_pr",
        rule="revert",
    )

    asyncio.run(
        main_commit_audit.record_audit_findings_in_redis(
            redis,
            "octo-demo",
            [finding],
        )
    )
    asyncio.run(
        main_commit_audit.mark_shas_audited_in_redis(
            redis,
            "octo-demo",
            ["abc1234"],
        )
    )

    assert asyncio.run(
        main_commit_audit.load_audited_shas_from_redis(redis, "octo-demo")
    ) == {"abc1234"}
    assert redis.lists["audit:main_commits:octo-demo:findings"]
    assert redis.ttls["audit:main_commits:octo-demo:audited"] == 30 * 24 * 60 * 60
    assert redis.ttls["audit:main_commits:octo-demo:findings"] == 30 * 24 * 60 * 60


def test_redis_helpers_skip_empty_inputs():
    redis = _FakeRedis()

    asyncio.run(
        main_commit_audit.record_audit_findings_in_redis(redis, "octo-demo", [])
    )
    asyncio.run(main_commit_audit.mark_shas_audited_in_redis(redis, "octo-demo", []))

    assert redis.lists == {}
    assert redis.sets == {}


def test_redis_helpers_swallow_errors(caplog):
    class BrokenRedis:
        async def smembers(self, key):
            raise RuntimeError("redis down")

        async def lpush(self, key, value):
            raise RuntimeError("redis down")

        async def sadd(self, key, value):
            raise RuntimeError("redis down")

    finding = main_commit_audit.MainCommitAuditFinding(
        sha="abc1234",
        short_sha="abc1234",
        message_first_line="direct hotfix",
        parent_count=1,
        pr_number=None,
        violation_category="direct_commit_no_pr",
        rule="revert",
    )

    with caplog.at_level(logging.ERROR):
        loaded = asyncio.run(
            main_commit_audit.load_audited_shas_from_redis(
                BrokenRedis(),
                "octo-demo",
            )
        )
        asyncio.run(
            main_commit_audit.record_audit_findings_in_redis(
                BrokenRedis(),
                "octo-demo",
                [finding],
            )
        )
        asyncio.run(
            main_commit_audit.mark_shas_audited_in_redis(
                BrokenRedis(),
                "octo-demo",
                ["abc1234"],
            )
        )

    assert loaded == set()
    assert "Failed to load main commit audit cache" in caplog.text
    assert "Failed to record main commit audit findings" in caplog.text
    assert "Failed to update main commit audit cache" in caplog.text
