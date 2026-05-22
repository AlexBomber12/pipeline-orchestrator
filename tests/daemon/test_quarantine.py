from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pytest
from src.audit import operator_actions
from src.daemon import git_ops as git_ops_module
from src.daemon import runner as runner_module
from src.daemon.guardrails import GuardrailViolation
from src.daemon.handlers import merge as merge_module
from src.daemon.handlers import watch as watch_module
from src.daemon.quarantine import apply_quarantine_label_for_violation
from src.models import (
    CIStatus,
    PipelineState,
    PRInfo,
    QueueTask,
    ReviewStatus,
    TaskStatus,
)
from tests.runner import _helpers as h


def _violation(category: str = "large_diff_threshold") -> GuardrailViolation:
    return GuardrailViolation(
        tier=2,
        category=category,
        excerpt="+900 LOC",
        rule="Large diffs require operator review",
    )


def test_quarantine_apply_creates_label_and_comment(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    calls: list[list[str]] = []
    monkeypatch.setattr(operator_actions, "AUDIT_DIR", tmp_path)
    monkeypatch.setattr(
        "src.daemon.quarantine.gh_runner.run_gh",
        lambda args, repo: calls.append(args) or "",
    )

    assert apply_quarantine_label_for_violation(h._make_runner(), 42, _violation())

    assert calls[0][:3] == ["label", "create", "quarantine:large_diff"]
    assert calls[1] == ["pr", "edit", "42", "--add-label", "quarantine:large_diff"]
    assert calls[2][:3] == ["pr", "comment", "42"]


def test_quarantine_apply_soft_fails_on_label_create_failure(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    calls: list[list[str]] = []
    monkeypatch.setattr(operator_actions, "AUDIT_DIR", tmp_path)

    def fake_run_gh(args: list[str], repo: str) -> str:
        calls.append(args)
        if args[:2] == ["label", "create"]:
            raise RuntimeError("already exists")
        return ""

    monkeypatch.setattr("src.daemon.quarantine.gh_runner.run_gh", fake_run_gh)

    assert apply_quarantine_label_for_violation(h._make_runner(), 42, _violation())
    assert ["pr", "edit", "42", "--add-label", "quarantine:large_diff"] in calls


def test_quarantine_apply_returns_false_on_apply_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_run_gh(args: list[str], repo: str) -> str:
        if args[:2] == ["pr", "edit"]:
            raise RuntimeError("apply failed")
        return ""

    monkeypatch.setattr("src.daemon.quarantine.gh_runner.run_gh", fake_run_gh)

    assert not apply_quarantine_label_for_violation(h._make_runner(), 42, _violation())


def test_quarantine_apply_soft_fails_on_comment_failure(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(operator_actions, "AUDIT_DIR", tmp_path)

    def fake_run_gh(args: list[str], repo: str) -> str:
        if args[:2] == ["pr", "comment"]:
            raise RuntimeError("comment failed")
        return ""

    monkeypatch.setattr("src.daemon.quarantine.gh_runner.run_gh", fake_run_gh)

    assert apply_quarantine_label_for_violation(h._make_runner(), 42, _violation())


def test_quarantined_prs_populated_on_guardrail(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        watch_module.guardrails,
        "_DIFF_PATTERNS",
        {"x": __import__("re").compile("blocked")},
    )
    monkeypatch.setattr(watch_module.guardrails, "_DIFF_RULES", {"x": "blocked rule"})
    monkeypatch.setattr("src.github.prs.get_pr_diff", lambda repo, number: "blocked")
    monkeypatch.setattr(
        "src.daemon.handlers.watch.apply_quarantine_label_for_violation",
        lambda runner, pr_number, violation: True,
    )

    async def fake_transition(self: Any, message: str, **kwargs: Any) -> None:
        self.state.state = PipelineState.ERROR

    monkeypatch.setattr(
        runner_module.PipelineRunner,
        "_transition_to_error",
        fake_transition,
    )
    runner = h._make_runner()
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=11, branch="pr-011", head_sha="sha")

    asyncio.run(runner._scan_pr_diff_once())

    assert 11 in runner.state.quarantined_prs


def test_coding_guardrail_populates_quarantine(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = h._make_runner()
    runner.state.state = PipelineState.CODING
    runner.state.current_pr = PRInfo(number=12, branch="pr-012")
    applied: list[int] = []

    async def fake_transition_to_error(message: str, **kwargs: object) -> None:
        return None

    async def fake_save_cli_log(*args: object, **kwargs: object) -> None:
        return None

    monkeypatch.setattr(runner, "_transition_to_error", fake_transition_to_error)
    monkeypatch.setattr(runner, "_save_cli_log", fake_save_cli_log)
    monkeypatch.setattr(
        "src.daemon.handlers.coding.apply_quarantine_label_for_violation",
        lambda runner, pr_number, violation: applied.append(pr_number) or True,
    )

    asyncio.run(
        runner._post_coder_resolution(
            "claude",
            0,
            "gh repo create octo/demo\n",
            "",
            target_branch="pr-012",
            current_pr_id="PR-012",
        )
    )

    assert 12 in runner.state.quarantined_prs
    assert applied == [12]


def _stub_successful_merge(monkeypatch: pytest.MonkeyPatch) -> list[int]:
    merged: list[int] = []

    def fake_git(repo_path: str, *args: str, **kwargs: object):
        if args[:1] == ("merge",):
            return h._FakeCompletedProcess(stdout="Already up to date.\n")
        if args[:2] == ("rev-parse", "HEAD"):
            return h._FakeCompletedProcess(stdout="deadbeef\n")
        return h._FakeCompletedProcess(stdout="")

    monkeypatch.setattr(git_ops_module, "_git", fake_git)
    monkeypatch.setattr(merge_module, "retry_transient", lambda op, **_: op())
    monkeypatch.setattr("src.github.cache._invalidate_etag_cache", lambda prefix: None)
    monkeypatch.setattr("src.github.gh_runner.run_gh", lambda *a, **kw: "")
    monkeypatch.setattr("src.github.prs.merge_pr", lambda repo, num: merged.append(num))
    monkeypatch.setattr(
        runner_module.subprocess,
        "run",
        lambda *a, **kw: h._FakeCompletedProcess(stdout=""),
    )
    return merged


def test_merge_handler_skips_quarantined_pr(monkeypatch: pytest.MonkeyPatch) -> None:
    merged = _stub_successful_merge(monkeypatch)
    runner = h._make_runner()
    runner.state.state = PipelineState.MERGE
    runner.state.current_pr = PRInfo(number=7, branch="pr-007")
    runner.state.quarantined_prs.add(7)
    published: list[PipelineState] = []

    async def fake_publish() -> None:
        published.append(runner.state.state)

    runner.publish_state = fake_publish  # type: ignore[assignment]

    asyncio.run(runner.handle_merge())

    assert merged == []
    assert runner.state.state == PipelineState.WATCH
    assert published == [PipelineState.WATCH]
    assert any("is quarantined; refusing to merge" in e["event"] for e in runner.state.history)


def test_merge_handler_proceeds_for_non_quarantined_pr(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    merged = _stub_successful_merge(monkeypatch)
    runner = h._make_runner()
    runner.state.state = PipelineState.MERGE
    runner.state.current_pr = PRInfo(number=8, branch="pr-008")

    asyncio.run(runner.handle_merge())

    assert merged == [8]


def test_label_removal_detected_in_watch(monkeypatch: pytest.MonkeyPatch) -> None:
    runner = h._make_runner()
    runner.state.state = PipelineState.WATCH
    now = datetime.now(timezone.utc)
    runner.state.current_pr = PRInfo(
        number=9,
        branch="pr-009",
        ci_status=CIStatus.PENDING,
        review_status=ReviewStatus.PENDING,
        last_activity=now,
    )
    runner.state.quarantined_prs.add(9)
    monkeypatch.setattr(
        "src.github.prs.get_open_prs",
        lambda *a, **kw: [runner.state.current_pr],
    )
    monkeypatch.setattr("src.github.prs.get_pr_state", lambda *a, **kw: "OPEN")
    monkeypatch.setattr("src.github.gh_runner.run_gh", lambda *a, **kw: "")
    monkeypatch.setattr(
        runner,
        "_scan_pr_diff_once",
        lambda: asyncio.sleep(0, result=False),
    )
    monkeypatch.setattr(
        runner,
        "_maybe_reclassify_stuck_pending",
        lambda found: asyncio.sleep(0),
    )
    monkeypatch.setattr(
        runner,
        "_maybe_retrigger_stale_review",
        lambda number: asyncio.sleep(0),
    )

    asyncio.run(runner.handle_watch())

    assert 9 not in runner.state.quarantined_prs
    assert any("quarantine released externally" in e["event"] for e in runner.state.history)


def test_label_removal_check_soft_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    runner = h._make_runner()
    runner.state.quarantined_prs.add(10)
    monkeypatch.setattr(
        "src.github.gh_runner.run_gh",
        lambda *a, **kw: (_ for _ in ()).throw(RuntimeError("gh down")),
    )

    asyncio.run(runner._detect_external_quarantine_release(10))

    assert 10 in runner.state.quarantined_prs
    assert any("quarantine label check failed" in e["event"] for e in runner.state.history)


def test_label_removal_releases_legacy_quarantine_set_with_pr_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = h._make_runner()
    runner.state.quarantined_prs.add(10)
    monkeypatch.setattr("src.github.gh_runner.run_gh", lambda *a, **kw: "")

    asyncio.run(
        runner._detect_external_quarantine_release(
            PRInfo(number=10, branch="pr-382", pr_id="PR-382")
        )
    )

    assert 10 not in runner.state.quarantined_prs
    assert any("quarantine released externally" in e["event"] for e in runner.state.history)


def test_label_removal_clears_suppression_with_current_task_fallback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = h._make_runner()
    runner.state.current_task = QueueTask(
        pr_id="PR-382",
        title="PR-382",
        status=TaskStatus.ERROR,
    )
    runner.state.quarantined_prs.add(10)
    asyncio.run(
        runner._suppress_task(
            "PR-382",
            runner_module.SuppressionReason.GUARDRAIL,
            {"pr_number": 10},
        )
    )
    monkeypatch.setattr("src.github.gh_runner.run_gh", lambda *a, **kw: "")

    asyncio.run(
        runner._detect_external_quarantine_release(
            PRInfo(number=10, branch="renamed-title", pr_id="")
        )
    )

    assert 10 not in runner.state.quarantined_prs
    assert asyncio.run(runner._suppression_record_for_task("PR-382")) is None


def test_label_removal_check_keeps_labeled_pr(monkeypatch: pytest.MonkeyPatch) -> None:
    runner = h._make_runner()
    runner.state.quarantined_prs.add(10)
    monkeypatch.setattr(
        "src.github.gh_runner.run_gh",
        lambda *a, **kw: "quarantine:large_diff\n",
    )

    asyncio.run(runner._detect_external_quarantine_release(10))

    assert 10 in runner.state.quarantined_prs


@pytest.mark.parametrize("gh_output", [object(), [], {"labels": []}])
def test_label_removal_check_keeps_quarantine_on_unknown_output(
    monkeypatch: pytest.MonkeyPatch,
    gh_output: object,
) -> None:
    runner = h._make_runner()
    runner.state.quarantined_prs.add(10)
    monkeypatch.setattr("src.github.gh_runner.run_gh", lambda *a, **kw: gh_output)

    asyncio.run(runner._detect_external_quarantine_release(10))

    assert 10 in runner.state.quarantined_prs
    assert any(
        "keeping quarantine" in e["event"]
        for e in runner.state.history
    )


def test_audit_entry_written_on_apply(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(operator_actions, "AUDIT_DIR", tmp_path)
    monkeypatch.setattr("src.daemon.quarantine.gh_runner.run_gh", lambda *a, **kw: "")

    apply_quarantine_label_for_violation(h._make_runner(), 42, _violation())

    record = json.loads(next(tmp_path.glob("*.jsonl")).read_text().strip())
    assert record["event"] == "quarantine_apply"
    assert record["pr"] == 42


def test_operator_action_audit_swallows_oserror(tmp_path: Path) -> None:
    target = tmp_path / "not-a-dir"
    target.write_text("", encoding="utf-8")
    original = operator_actions.AUDIT_DIR
    operator_actions.AUDIT_DIR = target
    try:
        operator_actions.write_operator_action_audit(
            action="quarantine_release",
            repo="example__alpha",
            pr=1,
        )
    finally:
        operator_actions.AUDIT_DIR = original
