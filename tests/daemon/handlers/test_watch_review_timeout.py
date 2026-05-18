"""PR-358: WATCH review_timeout single-shot ``@codex review`` repost tests.

When WATCH hits ``review_timeout`` for the first time on a PR iteration,
the daemon must post ``@codex review`` once (bypassing the same-HEAD and
PR-author dedup gates), restart the review window, and stay in WATCH.
On the second hit (flag already True) the existing terminal ERROR path
fires with ``repost_attempted: True`` in the cancellation payload.

The flag is reset to ``False`` on PR-iteration boundaries: FIX entry,
MERGE entry, new ``current_pr`` assignment, and ``current_task = None``.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from typing import Any

import httpx
import pytest

from src.daemon.runner import PipelineRunner
from src.models import (
    CIStatus,
    PipelineState,
    PRInfo,
    QueueTask,
    ReviewStatus,
    TaskStatus,
)

from tests.runner import _helpers as h


def _stale_pr(
    *,
    number: int = 5,
    minutes_old: int = 90,
    review: ReviewStatus = ReviewStatus.PENDING,
    ci: CIStatus = CIStatus.SUCCESS,
) -> PRInfo:
    return PRInfo(
        number=number,
        branch=f"pr-{number:03d}",
        ci_status=ci,
        review_status=review,
        last_activity=datetime.now(timezone.utc) - timedelta(minutes=minutes_old),
    )


def _seed_runner_in_watch(
    monkeypatch: pytest.MonkeyPatch,
    pr: PRInfo,
    *,
    repost_attempted: bool = False,
) -> PipelineRunner:
    monkeypatch.setattr("src.github.prs.get_open_prs", lambda repo, **kw: [pr])
    runner = h._make_runner(review_timeout_min=30)
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=pr.number, branch=pr.branch)
    runner.state.current_task = QueueTask(
        pr_id=f"PR-{pr.number:03d}",
        title="t",
        status=TaskStatus.DOING,
        branch=pr.branch,
    )
    runner.state.review_timeout_repost_attempted = repost_attempted

    async def fake_commit(self, current_task, status, reason):
        return True

    monkeypatch.setattr(
        PipelineRunner, "_commit_task_status_change", fake_commit
    )
    return runner


def _stub_repost(
    runner: PipelineRunner,
    *,
    result: bool | Exception,
) -> list[dict[str, Any]]:
    calls: list[dict[str, Any]] = []

    def fake_post(
        pr_number: int,
        *,
        bypass_same_head_dedup: bool = False,
        bypass_author_dedup: bool = False,
    ) -> bool:
        calls.append(
            {
                "pr_number": pr_number,
                "bypass_same_head_dedup": bypass_same_head_dedup,
                "bypass_author_dedup": bypass_author_dedup,
            }
        )
        if isinstance(result, Exception):
            raise result
        return result

    runner._post_codex_review = fake_post  # type: ignore[method-assign]
    return calls


def test_first_timeout_posts_repost_stays_in_watch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pr = _stale_pr(minutes_old=45)
    runner = _seed_runner_in_watch(monkeypatch, pr)
    calls = _stub_repost(runner, result=True)

    transition_calls: list[Any] = []

    async def fake_transition(*args, **kwargs):
        transition_calls.append((args, kwargs))

    monkeypatch.setattr(
        PipelineRunner, "_transition_to_error", fake_transition
    )

    before = datetime.now(timezone.utc)
    asyncio.run(runner.handle_watch())
    after = datetime.now(timezone.utc)

    assert runner.state.state == PipelineState.WATCH
    assert runner.state.review_timeout_repost_attempted is True
    assert len(calls) == 1
    assert calls[0]["pr_number"] == pr.number
    assert calls[0]["bypass_same_head_dedup"] is True
    assert calls[0]["bypass_author_dedup"] is True
    assert transition_calls == []
    last_activity = runner.state.current_pr.last_activity
    assert last_activity is not None
    assert before <= last_activity <= after
    # PR-358 review feedback: durable anchor read by next cycle's
    # ``elapsed_min`` computation. Without this, the locally-stamped
    # ``current_pr.last_activity`` is wiped on the next poll by the
    # GitHub-fetched ``PRInfo`` and the second cycle escalates immediately.
    repost_at = runner.state.review_timeout_repost_at
    assert repost_at is not None
    assert before <= repost_at <= after


def test_second_cycle_after_successful_repost_does_not_immediately_escalate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """PR-358 review feedback regression guard.

    After a successful repost the next WATCH cycle must NOT transition to
    terminal ERROR even when GitHub still returns the pre-repost
    ``updatedAt`` on the freshly-fetched ``PRInfo``. The durable
    ``RepoState.review_timeout_repost_at`` is the floor that elapsed_min
    reads, so the local restart survives the PR refresh that overwrites
    ``current_pr`` each cycle.
    """
    stale_last_activity = datetime.now(timezone.utc) - timedelta(minutes=45)
    pr_first_cycle = PRInfo(
        number=11,
        branch="pr-011",
        ci_status=CIStatus.SUCCESS,
        review_status=ReviewStatus.PENDING,
        last_activity=stale_last_activity,
    )
    pr_second_cycle = PRInfo(
        number=11,
        branch="pr-011",
        ci_status=CIStatus.SUCCESS,
        review_status=ReviewStatus.PENDING,
        # GitHub still reports the same pre-repost updatedAt — the
        # @codex review comment has not yet propagated, or the cached
        # payload lags the actual mutation.
        last_activity=stale_last_activity,
    )

    cycle_prs = [pr_first_cycle, pr_second_cycle]

    def fake_get_open_prs(repo, **kw):
        return [cycle_prs.pop(0)] if cycle_prs else [pr_second_cycle]

    monkeypatch.setattr("src.github.prs.get_open_prs", fake_get_open_prs)
    runner = h._make_runner(review_timeout_min=30)
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=11, branch="pr-011")
    runner.state.current_task = QueueTask(
        pr_id="PR-011",
        title="t",
        status=TaskStatus.DOING,
        branch="pr-011",
    )

    async def fake_commit(self, current_task, status, reason):
        return True

    monkeypatch.setattr(
        PipelineRunner, "_commit_task_status_change", fake_commit
    )

    transition_calls: list[Any] = []

    async def fake_transition(*args, **kwargs):
        transition_calls.append((args, kwargs))

    monkeypatch.setattr(
        PipelineRunner, "_transition_to_error", fake_transition
    )

    _stub_repost(runner, result=True)

    asyncio.run(runner.handle_watch())
    assert runner.state.state == PipelineState.WATCH
    assert runner.state.review_timeout_repost_attempted is True
    repost_at = runner.state.review_timeout_repost_at
    assert repost_at is not None
    assert transition_calls == []

    asyncio.run(runner.handle_watch())
    assert runner.state.state == PipelineState.WATCH, (
        "Second cycle must not escalate to ERROR while the review_timeout "
        "floor anchored by review_timeout_repost_at is fresh."
    )
    assert transition_calls == []


def test_repost_at_naive_datetime_normalized_to_utc(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A persisted naive datetime (e.g. legacy JSON payload missing tzinfo)
    is normalized to UTC before the floor comparison so the elapsed_min
    arithmetic does not crash on a mixed tz-aware/naive subtraction.
    """
    stale_last_activity = datetime.now(timezone.utc) - timedelta(minutes=45)
    pr = PRInfo(
        number=12,
        branch="pr-012",
        ci_status=CIStatus.SUCCESS,
        review_status=ReviewStatus.PENDING,
        last_activity=stale_last_activity,
    )
    monkeypatch.setattr("src.github.prs.get_open_prs", lambda repo, **kw: [pr])
    runner = h._make_runner(review_timeout_min=30)
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(number=12, branch="pr-012")
    runner.state.current_task = QueueTask(
        pr_id="PR-012",
        title="t",
        status=TaskStatus.DOING,
        branch="pr-012",
    )
    runner.state.review_timeout_repost_attempted = True
    # Naive datetime simulates a JSON payload that lost tzinfo.
    runner.state.review_timeout_repost_at = datetime.now(timezone.utc).replace(
        tzinfo=None
    )

    transition_calls: list[Any] = []

    async def fake_transition(*args, **kwargs):
        transition_calls.append((args, kwargs))

    monkeypatch.setattr(
        PipelineRunner, "_transition_to_error", fake_transition
    )

    asyncio.run(runner.handle_watch())

    assert runner.state.state == PipelineState.WATCH
    assert transition_calls == []


def test_repost_at_floor_yields_to_genuinely_stale_activity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The floor lifts elapsed_min only when the repost timestamp is more
    recent than ``last_activity``. Once enough wall time has passed
    *since* the repost (second hit), the floor itself is far enough in
    the past that elapsed_min crosses the timeout threshold and the
    terminal ERROR path fires — repost_attempted=True in the payload.
    """
    pr = _stale_pr(minutes_old=60)
    runner = _seed_runner_in_watch(monkeypatch, pr, repost_attempted=True)
    runner.state.review_timeout_repost_at = (
        datetime.now(timezone.utc) - timedelta(minutes=45)
    )
    _stub_repost(runner, result=True)

    recorded: list[tuple[str, dict[str, Any]]] = []

    async def fake_safe_record(redis_client, repo_slug, task_id, cause, *, log=None):
        recorded.append((cause.category, dict(cause.payload)))

    monkeypatch.setattr(
        "src.daemon.runner.safe_record_cancellation_cause",
        fake_safe_record,
    )

    asyncio.run(runner.handle_watch())

    assert runner.state.state == PipelineState.ERROR
    assert recorded
    _category, payload = recorded[0]
    assert payload["subsource"] == "review_timeout"
    assert payload["repost_attempted"] is True


def test_first_timeout_failed_repost_falls_through_to_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pr = _stale_pr(minutes_old=45)
    runner = _seed_runner_in_watch(monkeypatch, pr)
    _stub_repost(runner, result=False)

    recorded: list[tuple[str, dict[str, Any]]] = []

    async def fake_safe_record(redis_client, repo_slug, task_id, cause, *, log=None):
        recorded.append((cause.category, dict(cause.payload)))

    monkeypatch.setattr(
        "src.daemon.runner.safe_record_cancellation_cause",
        fake_safe_record,
    )

    asyncio.run(runner.handle_watch())

    assert runner.state.state == PipelineState.ERROR
    assert runner.state.review_timeout_repost_attempted is False
    assert recorded, "expected cancellation_cause to be recorded"
    category, payload = recorded[0]
    assert category == "ERROR"
    assert payload["subsource"] == "review_timeout"
    assert payload["repost_attempted"] is False


def test_first_timeout_repost_raises_falls_through_to_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pr = _stale_pr(minutes_old=45)
    runner = _seed_runner_in_watch(monkeypatch, pr)
    _stub_repost(runner, result=httpx.RequestError("connection reset"))

    recorded: list[tuple[str, dict[str, Any]]] = []

    async def fake_safe_record(redis_client, repo_slug, task_id, cause, *, log=None):
        recorded.append((cause.category, dict(cause.payload)))

    monkeypatch.setattr(
        "src.daemon.runner.safe_record_cancellation_cause",
        fake_safe_record,
    )

    asyncio.run(runner.handle_watch())

    assert runner.state.state == PipelineState.ERROR
    assert recorded
    _category, payload = recorded[0]
    assert payload["subsource"] == "review_timeout"
    assert payload["repost_attempted"] is False
    assert any(
        "raised RequestError" in e["event"]
        for e in runner.state.history
    )


def test_second_timeout_after_successful_repost_transitions_to_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pr = _stale_pr(minutes_old=60)
    runner = _seed_runner_in_watch(
        monkeypatch, pr, repost_attempted=True
    )
    calls = _stub_repost(runner, result=True)

    recorded: list[tuple[str, dict[str, Any]]] = []

    async def fake_safe_record(redis_client, repo_slug, task_id, cause, *, log=None):
        recorded.append((cause.category, dict(cause.payload)))

    monkeypatch.setattr(
        "src.daemon.runner.safe_record_cancellation_cause",
        fake_safe_record,
    )

    asyncio.run(runner.handle_watch())

    assert runner.state.state == PipelineState.ERROR
    assert calls == [], "second cycle must not call _post_codex_review again"
    assert recorded
    _category, payload = recorded[0]
    assert payload["subsource"] == "review_timeout"
    assert payload["repost_attempted"] is True


def test_below_timeout_no_repost_no_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pr = _stale_pr(minutes_old=10)
    runner = _seed_runner_in_watch(monkeypatch, pr)
    calls = _stub_repost(runner, result=True)

    transition_calls: list[Any] = []

    async def fake_transition(*args, **kwargs):
        transition_calls.append((args, kwargs))

    monkeypatch.setattr(
        PipelineRunner, "_transition_to_error", fake_transition
    )

    asyncio.run(runner.handle_watch())

    assert runner.state.state == PipelineState.WATCH
    assert runner.state.review_timeout_repost_attempted is False
    assert calls == []
    assert transition_calls == []
    assert any("waiting" in e["event"] for e in runner.state.history)


def test_repost_flag_resets_on_fix_entry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = h._make_runner()
    runner.state.review_timeout_repost_attempted = True
    runner.state.review_timeout_repost_at = datetime.now(timezone.utc)
    runner.state.state = PipelineState.WATCH
    runner.state.current_pr = PRInfo(
        number=42,
        branch="pr-042",
        is_cross_repository=True,
    )

    async def fake_refresh_auth(self) -> None:
        return None

    monkeypatch.setattr(
        PipelineRunner, "_refresh_auth_status_cache", fake_refresh_auth
    )

    asyncio.run(runner.handle_fix())

    assert runner.state.review_timeout_repost_attempted is False
    assert runner.state.review_timeout_repost_at is None


def test_repost_flag_resets_on_merge_entry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = h._make_runner()
    runner.state.review_timeout_repost_attempted = True
    runner.state.review_timeout_repost_at = datetime.now(timezone.utc)
    runner.state.state = PipelineState.MERGE
    runner.state.current_pr = PRInfo(
        number=42,
        branch="pr-042",
        is_cross_repository=True,
    )

    async def fake_external_merge(self) -> bool:
        return False

    monkeypatch.setattr(
        PipelineRunner,
        "_handle_cross_repository_merge",
        fake_external_merge,
        raising=False,
    )

    # Cross-repo PR path inside handle_merge short-circuits before any
    # GitHub or git interaction, so the flag reset at handler entry is
    # observable without further stubbing.
    try:
        asyncio.run(runner.handle_merge())
    except Exception:
        # Cross-repo handling may attempt additional steps depending on
        # repo state; the only invariant under test is the flag reset
        # at the handler entry point.
        pass

    assert runner.state.review_timeout_repost_attempted is False
    assert runner.state.review_timeout_repost_at is None


def test_repost_flag_resets_on_new_pr_assignment() -> None:
    runner = h._make_runner()
    runner.state.current_pr = PRInfo(number=465, branch="pr-465")
    runner.state.review_timeout_repost_attempted = True

    runner.state.current_pr = PRInfo(number=466, branch="pr-466")

    assert runner.state.review_timeout_repost_attempted is False


def test_repost_flag_resets_on_current_task_none() -> None:
    runner = h._make_runner()
    runner.state.current_task = QueueTask(
        pr_id="PR-465",
        title="t",
        status=TaskStatus.DOING,
        branch="pr-465",
    )
    runner.state.review_timeout_repost_attempted = True

    runner.state.current_task = None

    assert runner.state.review_timeout_repost_attempted is False


def test_repost_payload_includes_repost_attempted_field(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pr = _stale_pr(minutes_old=60)
    runner = _seed_runner_in_watch(
        monkeypatch, pr, repost_attempted=True
    )
    _stub_repost(runner, result=True)

    recorded: list[dict[str, Any]] = []

    async def fake_safe_record(redis_client, repo_slug, task_id, cause, *, log=None):
        recorded.append(dict(cause.payload))

    monkeypatch.setattr(
        "src.daemon.runner.safe_record_cancellation_cause",
        fake_safe_record,
    )

    asyncio.run(runner.handle_watch())

    assert recorded
    payload = recorded[0]
    assert "repost_attempted" in payload
    assert payload["repost_attempted"] is True


def test_stale_retrigger_floor_suppresses_back_to_back_repost(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """PR-358 review feedback (P2): in-cycle retrigger blocks forced repost.

    If ``_maybe_retrigger_stale_review`` posted ``@codex review`` earlier
    in the same WATCH pass, the GitHub-fetched ``found.last_activity``
    still reflects the pre-retrigger ``updatedAt`` because the cached
    payload was captured at the top of the cycle. Without including
    ``last_stale_retrigger_at`` in the ``elapsed_min`` floor, the
    forced-repost branch would fire on the same cycle and emit a second
    back-to-back ``@codex review`` for the same hang.
    """
    pr = _stale_pr(minutes_old=60)
    runner = _seed_runner_in_watch(monkeypatch, pr)
    # Pretend the stale retrigger fired moments ago in this same cycle.
    runner.state.last_stale_retrigger_at = datetime.now(timezone.utc)
    calls = _stub_repost(runner, result=True)

    transition_calls: list[Any] = []

    async def fake_transition(*args, **kwargs):
        transition_calls.append((args, kwargs))

    monkeypatch.setattr(
        PipelineRunner, "_transition_to_error", fake_transition
    )

    asyncio.run(runner.handle_watch())

    assert runner.state.state == PipelineState.WATCH
    assert calls == [], (
        "Forced repost must not fire when a stale retrigger already "
        "posted @codex review in the same cycle."
    )
    assert transition_calls == []
    assert runner.state.review_timeout_repost_attempted is False


def test_codex_bot_error_retrigger_floor_suppresses_back_to_back_repost(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """PR-358 review feedback (P2): same floor applies to codex-bot-error retrigger."""
    pr = _stale_pr(minutes_old=60)
    runner = _seed_runner_in_watch(monkeypatch, pr)
    runner.state.last_codex_retrigger_at = datetime.now(timezone.utc)
    calls = _stub_repost(runner, result=True)

    transition_calls: list[Any] = []

    async def fake_transition(*args, **kwargs):
        transition_calls.append((args, kwargs))

    monkeypatch.setattr(
        PipelineRunner, "_transition_to_error", fake_transition
    )

    asyncio.run(runner.handle_watch())

    assert runner.state.state == PipelineState.WATCH
    assert calls == []
    assert transition_calls == []
    assert runner.state.review_timeout_repost_attempted is False


def test_repost_flag_resets_on_new_head_sha_same_pr() -> None:
    """PR-358 review feedback (P3): each push earns a new repost slot.

    A new commit on the same PR number/branch is a new review iteration.
    Without resetting the repost gate on HEAD change, the next
    ``review_timeout`` would skip the one-shot repost and escalate
    straight to terminal ERROR on the first hit, even though the new
    HEAD is genuinely a new review cycle that deserves its own attempt.
    """
    runner = h._make_runner()
    runner.state.current_pr = PRInfo(
        number=465, branch="pr-465", head_sha="aaa1111"
    )
    runner.state.review_timeout_repost_attempted = True
    runner.state.review_timeout_repost_at = datetime.now(timezone.utc)

    runner.state.current_pr = PRInfo(
        number=465, branch="pr-465", head_sha="bbb2222"
    )

    assert runner.state.review_timeout_repost_attempted is False
    assert runner.state.review_timeout_repost_at is None


def test_repost_flag_preserved_on_same_head_sha_same_pr() -> None:
    """A WATCH refresh that observes the same HEAD must NOT reset the gate."""
    runner = h._make_runner()
    runner.state.current_pr = PRInfo(
        number=465, branch="pr-465", head_sha="aaa1111"
    )
    runner.state.review_timeout_repost_attempted = True
    stamp = datetime.now(timezone.utc)
    runner.state.review_timeout_repost_at = stamp

    runner.state.current_pr = PRInfo(
        number=465, branch="pr-465", head_sha="aaa1111", title="refreshed"
    )

    assert runner.state.review_timeout_repost_attempted is True
    assert runner.state.review_timeout_repost_at == stamp


def test_repost_flag_preserved_when_head_sha_unknown_on_either_side() -> None:
    """Transient empty ``head_sha`` must not wipe the repost gate.

    ``get_open_prs`` can legitimately return a ``PRInfo`` with
    ``head_sha=""`` on transient errors. Treating that as a HEAD
    change would clobber the gate mid-window every time the upstream
    payload omitted the SHA.
    """
    runner = h._make_runner()
    runner.state.current_pr = PRInfo(
        number=465, branch="pr-465", head_sha="aaa1111"
    )
    runner.state.review_timeout_repost_attempted = True

    runner.state.current_pr = PRInfo(
        number=465, branch="pr-465", head_sha=""
    )

    assert runner.state.review_timeout_repost_attempted is True


def test_publish_state_called_after_successful_repost(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pr = _stale_pr(minutes_old=45)
    runner = _seed_runner_in_watch(monkeypatch, pr)
    _stub_repost(runner, result=True)

    publish_calls: list[int] = []

    async def fake_publish(self) -> None:
        publish_calls.append(1)

    monkeypatch.setattr(PipelineRunner, "publish_state", fake_publish)

    asyncio.run(runner.handle_watch())

    assert publish_calls, "publish_state must be invoked after the repost"
