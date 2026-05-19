"""PR-351: crash-backup fallback push when the primary push is rejected.

When a daemon crashes mid-CODING, ``recover_state`` calls
``_preserve_crashed_run_commits`` to push any unpushed local work to
``origin/{branch}`` before marking the task ERROR. If branch protection
or a non-fast-forward state rejects that push, PR-351 falls back to a
``crash-backup/{task_id}/{timestamp}`` ref so the work survives on
origin instead of being silently dropped, and writes the branch pointer
to ``recovery:backup_branch:{repo}:{task_id}`` so the dashboard
cancellation card can surface it for operator recovery.
"""

from __future__ import annotations

import asyncio
import re
import subprocess
from datetime import datetime, timezone
from typing import Any

import pytest
from src.cancellation.storage import CancellationCause
from src.config import AppConfig, DaemonConfig, RepoConfig
from src.daemon import recovery as recovery_module
from src.daemon.runner import PipelineRunner
from src.keyspace import recovery_backup_branch
from src.models import PipelineState, QueueTask, TaskStatus
from src.web import app as web_app
from src.web.routes import dashboard as dashboard_module


class _FakeRedis:
    """Minimal async Redis double capturing ``set`` calls with TTLs."""

    def __init__(self) -> None:
        self.writes: list[tuple[str, str, int | None]] = []
        self.store: dict[str, str] = {}
        self.fail_on_set: bool = False

    async def set(
        self,
        key: str,
        value: str,
        ex: int | None = None,
    ) -> None:
        if self.fail_on_set:
            raise RuntimeError("redis unavailable")
        self.writes.append((key, value, ex))
        self.store[key] = value

    async def get(self, key: str) -> str | None:
        return self.store.get(key)

    async def publish(self, key: str, value: str) -> int:
        return 1


class _FakeUsageProvider:
    def fetch(self) -> None:
        return None

    @property
    def consecutive_failures(self) -> int:
        return 0


def _repo_cfg(**overrides: Any) -> RepoConfig:
    base: dict[str, Any] = {
        "url": "https://github.com/octo/demo.git",
        "branch": "main",
        "auto_merge": True,
        "review_timeout_min": 30,
        "poll_interval_sec": 60,
    }
    base.update(overrides)
    return RepoConfig(**base)


def _make_runner() -> PipelineRunner:
    return PipelineRunner(
        _repo_cfg(),
        AppConfig(repositories=[], daemon=DaemonConfig()),
        _FakeRedis(),
        _FakeUsageProvider(),
        _FakeUsageProvider(),
    )


def _make_runner_with_doing(task_id: str = "PR-042") -> PipelineRunner:
    runner = _make_runner()
    runner.state.current_task = QueueTask(
        pr_id=task_id,
        title="In-flight",
        status=TaskStatus.DOING,
        branch="pr-042-inflight",
    )
    runner.state.state = PipelineState.CODING
    return runner


def _rejected_push_error(stderr: str) -> subprocess.CalledProcessError:
    return subprocess.CalledProcessError(
        returncode=1,
        cmd=["git", "push", "origin", "pr-042-inflight:pr-042-inflight"],
        stderr=stderr,
    )


def _stub_subprocess(
    monkeypatch: pytest.MonkeyPatch,
    handler: Any,
) -> list[list[str]]:
    """Route every ``git`` call through ``handler``; record commands."""
    calls: list[list[str]] = []

    def fake_run(cmd: list[str], **kwargs: Any) -> Any:
        calls.append(cmd)
        return handler(cmd, **kwargs)

    monkeypatch.setattr(recovery_module.subprocess, "run", fake_run)
    return calls


async def _async_zero_counts(repos_dir: str, repo: str, task_ids: set[str]) -> dict[str, int]:
    return {task_id: 0 for task_id in task_ids}


# ---------------------------------------------------------------------------
# _preserve_crashed_run_commits: primary success path (unchanged)
# ---------------------------------------------------------------------------


def test_primary_push_success_no_fallback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Primary push succeeds → no fallback attempted, no Redis write."""
    runner = _make_runner_with_doing()
    pushes: list[list[str]] = []

    def handler(cmd: list[str], **kwargs: Any) -> Any:
        if cmd[:4] == ["git", "rev-parse", "--verify", "--quiet"]:
            return subprocess.CompletedProcess(
                args=cmd, returncode=0, stdout="abc\n", stderr=""
            )
        if cmd[:2] == ["git", "push"]:
            pushes.append(cmd)
            return subprocess.CompletedProcess(
                args=cmd, returncode=0, stdout="", stderr=""
            )
        return subprocess.CompletedProcess(
            args=cmd, returncode=0, stdout="", stderr=""
        )

    _stub_subprocess(monkeypatch, handler)

    result = runner._preserve_crashed_run_commits("pr-042-inflight")

    assert result is True
    assert pushes == [
        ["git", "push", "origin", "pr-042-inflight:pr-042-inflight"]
    ]
    assert runner._pending_backup_branch_write is None
    asyncio.run(runner._persist_pending_backup_branch_write())
    assert runner.redis.writes == []


# ---------------------------------------------------------------------------
# _preserve_crashed_run_commits: rejected primary → fallback succeeds
# ---------------------------------------------------------------------------


def test_primary_push_rejected_falls_back_to_backup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Primary rejected → fallback push to crash-backup branch + Redis write."""
    runner = _make_runner_with_doing()
    pushes: list[str] = []

    def handler(cmd: list[str], **kwargs: Any) -> Any:
        if cmd[:4] == ["git", "rev-parse", "--verify", "--quiet"]:
            return subprocess.CompletedProcess(
                args=cmd, returncode=0, stdout="abc\n", stderr=""
            )
        if cmd[:2] == ["git", "push"]:
            refspec = cmd[-1]
            pushes.append(refspec)
            if refspec == "pr-042-inflight:pr-042-inflight":
                raise _rejected_push_error(
                    "remote: error: GH006: Protected branch update failed\n"
                    " ! [remote rejected] pr-042-inflight -> pr-042-inflight\n"
                )
            return subprocess.CompletedProcess(
                args=cmd, returncode=0, stdout="", stderr=""
            )
        return subprocess.CompletedProcess(
            args=cmd, returncode=0, stdout="", stderr=""
        )

    _stub_subprocess(monkeypatch, handler)

    result = runner._preserve_crashed_run_commits("pr-042-inflight")

    assert result is True
    assert pushes[0] == "pr-042-inflight:pr-042-inflight"
    assert pushes[1].startswith("pr-042-inflight:crash-backup/PR-042/")
    assert runner._pending_backup_branch_write is not None
    task_id, backup_branch = runner._pending_backup_branch_write
    assert task_id == "PR-042"
    assert backup_branch.startswith("crash-backup/PR-042/")

    asyncio.run(runner._persist_pending_backup_branch_write())
    assert len(runner.redis.writes) == 1
    key, value, ex = runner.redis.writes[0]
    assert key == recovery_backup_branch(runner.name, "PR-042")
    assert value == backup_branch
    assert ex == 30 * 86400
    assert runner._pending_backup_branch_write is None


# ---------------------------------------------------------------------------
# _attempt_backup_branch_push: naming pattern
# ---------------------------------------------------------------------------


def test_backup_branch_naming_includes_task_id_and_timestamp(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Fallback branch name matches ``crash-backup/PR-X/YYYYMMDD-HHMMSS``."""
    runner = _make_runner_with_doing(task_id="PR-123")
    pushes: list[str] = []

    def handler(cmd: list[str], **kwargs: Any) -> Any:
        if cmd[:4] == ["git", "rev-parse", "--verify", "--quiet"]:
            return subprocess.CompletedProcess(
                args=cmd, returncode=0, stdout="abc\n", stderr=""
            )
        if cmd[:2] == ["git", "push"]:
            refspec = cmd[-1]
            pushes.append(refspec)
            if "crash-backup" not in refspec:
                raise _rejected_push_error(
                    " ! [rejected]        feature-branch -> feature-branch"
                    " (non-fast-forward)"
                )
            return subprocess.CompletedProcess(
                args=cmd, returncode=0, stdout="", stderr=""
            )
        return subprocess.CompletedProcess(
            args=cmd, returncode=0, stdout="", stderr=""
        )

    _stub_subprocess(monkeypatch, handler)

    runner._preserve_crashed_run_commits("pr-042-inflight")

    assert runner._pending_backup_branch_write is not None
    _, backup_branch = runner._pending_backup_branch_write
    assert re.fullmatch(
        r"crash-backup/PR-123/\d{8}-\d{6}",
        backup_branch,
    ), backup_branch


# ---------------------------------------------------------------------------
# _persist_pending_backup_branch_write: TTL
# ---------------------------------------------------------------------------


def test_backup_redis_key_has_30_day_ttl() -> None:
    """Redis write uses the 30 * 86400 second TTL the dashboard expects."""
    runner = _make_runner_with_doing()
    runner._pending_backup_branch_write = (
        "PR-042",
        "crash-backup/PR-042/20260519-120000",
    )

    asyncio.run(runner._persist_pending_backup_branch_write())

    assert len(runner.redis.writes) == 1
    _, _, ex = runner.redis.writes[0]
    assert ex == 30 * 86400


# ---------------------------------------------------------------------------
# _preserve_crashed_run_commits: both pushes fail
# ---------------------------------------------------------------------------


def test_both_pushes_fail_returns_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Both primary and fallback rejected → return False, no Redis write."""
    runner = _make_runner_with_doing()
    primary_stderr = (
        " ! [remote rejected] pr-042-inflight -> pr-042-inflight"
    )
    events: list[str] = []
    runner.log_event = lambda msg: events.append(msg)  # type: ignore[method-assign]

    def handler(cmd: list[str], **kwargs: Any) -> Any:
        if cmd[:4] == ["git", "rev-parse", "--verify", "--quiet"]:
            return subprocess.CompletedProcess(
                args=cmd, returncode=0, stdout="abc\n", stderr=""
            )
        if cmd[:2] == ["git", "push"]:
            refspec = cmd[-1]
            if refspec == "pr-042-inflight:pr-042-inflight":
                raise _rejected_push_error(primary_stderr)
            raise subprocess.CalledProcessError(
                returncode=1,
                cmd=cmd,
                stderr="fatal: unable to access 'origin'",
            )
        return subprocess.CompletedProcess(
            args=cmd, returncode=0, stdout="", stderr=""
        )

    _stub_subprocess(monkeypatch, handler)

    result = runner._preserve_crashed_run_commits("pr-042-inflight")

    assert result is False
    assert runner._pending_backup_branch_write is None
    assert any("Crash-backup fallback push" in m for m in events)
    assert any(
        "Failed to preserve unpushed commits on pr-042-inflight" in m
        for m in events
    )
    asyncio.run(runner._persist_pending_backup_branch_write())
    assert runner.redis.writes == []


# ---------------------------------------------------------------------------
# _preserve_crashed_run_commits: non-rejected failure does not fall back
# ---------------------------------------------------------------------------


def test_fallback_path_only_on_rejected_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Auth/transport errors are not the rejected pattern → no fallback."""
    runner = _make_runner_with_doing()
    pushes: list[str] = []

    def handler(cmd: list[str], **kwargs: Any) -> Any:
        if cmd[:4] == ["git", "rev-parse", "--verify", "--quiet"]:
            return subprocess.CompletedProcess(
                args=cmd, returncode=0, stdout="abc\n", stderr=""
            )
        if cmd[:2] == ["git", "push"]:
            pushes.append(cmd[-1])
            raise subprocess.CalledProcessError(
                returncode=128,
                cmd=cmd,
                stderr="fatal: Authentication failed for 'https://github.com/'",
            )
        return subprocess.CompletedProcess(
            args=cmd, returncode=0, stdout="", stderr=""
        )

    _stub_subprocess(monkeypatch, handler)

    result = runner._preserve_crashed_run_commits("pr-042-inflight")

    assert result is False
    # Only the primary push attempted; no fallback to a crash-backup ref.
    assert pushes == ["pr-042-inflight:pr-042-inflight"]
    assert runner._pending_backup_branch_write is None


# ---------------------------------------------------------------------------
# _attempt_backup_branch_push: no current task → no fallback
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "exc",
    [
        subprocess.TimeoutExpired(["git", "push"], 120),
        OSError("network down"),
    ],
)
def test_push_transport_error_skips_fallback(
    monkeypatch: pytest.MonkeyPatch, exc: Exception
) -> None:
    """Push transport failures (timeout, OSError) are not the rejected pattern,
    so the fallback is not attempted and the function returns False."""
    runner = _make_runner_with_doing()

    def handler(cmd: list[str], **kwargs: Any) -> Any:
        if cmd[:4] == ["git", "rev-parse", "--verify", "--quiet"]:
            return subprocess.CompletedProcess(
                args=cmd, returncode=0, stdout="abc\n", stderr=""
            )
        if cmd[:2] == ["git", "push"]:
            raise exc
        return subprocess.CompletedProcess(
            args=cmd, returncode=0, stdout="", stderr=""
        )

    _stub_subprocess(monkeypatch, handler)

    result = runner._preserve_crashed_run_commits("pr-042-inflight")

    assert result is False
    assert runner._pending_backup_branch_write is None


def test_no_current_task_skips_fallback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Without ``state.current_task`` the fallback cannot name a task id."""
    runner = _make_runner()  # no current_task

    def handler(cmd: list[str], **kwargs: Any) -> Any:
        if cmd[:4] == ["git", "rev-parse", "--verify", "--quiet"]:
            return subprocess.CompletedProcess(
                args=cmd, returncode=0, stdout="abc\n", stderr=""
            )
        if cmd[:2] == ["git", "push"]:
            raise _rejected_push_error(
                " ! [remote rejected] foo -> foo (push declined)"
            )
        return subprocess.CompletedProcess(
            args=cmd, returncode=0, stdout="", stderr=""
        )

    _stub_subprocess(monkeypatch, handler)

    result = runner._preserve_crashed_run_commits("pr-042-inflight")

    assert result is False
    assert runner._pending_backup_branch_write is None


# ---------------------------------------------------------------------------
# _persist_pending_backup_branch_write: Redis failure is best-effort
# ---------------------------------------------------------------------------


def test_redis_failure_during_persist_does_not_raise() -> None:
    """Best-effort Redis write: failure logs but does not abort recovery."""
    runner = _make_runner_with_doing()
    runner.redis.fail_on_set = True
    runner._pending_backup_branch_write = (
        "PR-042",
        "crash-backup/PR-042/20260519-120000",
    )

    events: list[str] = []
    runner.log_event = lambda msg: events.append(msg)  # type: ignore[method-assign]

    asyncio.run(runner._persist_pending_backup_branch_write())

    assert runner._pending_backup_branch_write is None
    assert any("Failed to record crash-backup branch" in m for m in events)


# ---------------------------------------------------------------------------
# Dashboard surface: cancellation card renders the backup branch
# ---------------------------------------------------------------------------


def _render_card_with_dict(payload: dict[str, Any]) -> str:
    macro = web_app.templates.env.get_template(
        "components/cancellation_card.html"
    ).module.cancellation_card
    return macro(payload)


def _cause_dict_with_backup(branch: str | None) -> dict[str, Any]:
    return {
        "category": "ERROR",
        "task_id": "PR-042",
        "repo_slug": "octo__demo",
        "created_at": "2026-05-19T12:00:00+00:00",
        "payload": {"subsource": "crash"},
        "dependents_count": 0,
        "recovery_backup_branch": branch,
    }


def test_dashboard_surfaces_backup_branch_when_set() -> None:
    """Augmented cause carrying a backup branch renders the surface block."""
    branch = "crash-backup/PR-042/20260519-120000"
    rendered = _render_card_with_dict(_cause_dict_with_backup(branch))

    assert "Crash backup available on branch" in rendered
    assert branch in rendered
    assert 'data-clipboard-target="recovery_backup_branch"' in rendered


def test_dashboard_omits_backup_branch_when_unset() -> None:
    """No backup branch in the augmented cause → no surface block."""
    rendered = _render_card_with_dict(_cause_dict_with_backup(None))

    assert "Crash backup available on branch" not in rendered
    assert "recovery-backup-branch" not in rendered


def test_card_renders_for_legacy_cause_without_recovery_field() -> None:
    """Pre-PR-351 records lack ``recovery_backup_branch`` entirely; the
    template must still render without raising on the missing key."""
    cause = CancellationCause(
        category="ERROR",
        payload={"subsource": "crash"},
        created_at="2026-05-19T12:00:00+00:00",
        task_id="PR-042",
        repo_slug="octo__demo",
    )
    macro = web_app.templates.env.get_template(
        "components/cancellation_card.html"
    ).module.cancellation_card
    rendered = macro(cause)

    assert "Daemon crash" in rendered
    assert "Crash backup available on branch" not in rendered


# ---------------------------------------------------------------------------
# Dashboard helper: _read_recovery_backup_branch
# ---------------------------------------------------------------------------


def test_read_recovery_backup_branch_returns_value() -> None:
    """Redis read decodes the recorded branch name."""
    redis = _FakeRedis()
    asyncio.run(
        redis.set(
            recovery_backup_branch("octo__demo", "PR-042"),
            "crash-backup/PR-042/20260519-120000",
        )
    )

    result = asyncio.run(
        dashboard_module._read_recovery_backup_branch(
            redis, "octo__demo", "PR-042"
        )
    )

    assert result == "crash-backup/PR-042/20260519-120000"


def test_read_recovery_backup_branch_returns_none_when_missing() -> None:
    """Missing Redis key returns ``None`` so the card omits the surface."""
    redis = _FakeRedis()

    result = asyncio.run(
        dashboard_module._read_recovery_backup_branch(
            redis, "octo__demo", "PR-042"
        )
    )

    assert result is None


def test_augment_causes_attaches_recovery_backup_branch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``_augment_causes_with_dependents`` populates the new field per cause."""
    redis = _FakeRedis()
    asyncio.run(
        redis.set(
            recovery_backup_branch("octo__demo", "PR-042"),
            "crash-backup/PR-042/20260519-120000",
        )
    )
    monkeypatch.setattr(
        dashboard_module,
        "compute_repo_dependents_count",
        _async_zero_counts,
    )
    cause = CancellationCause(
        category="ERROR",
        payload={"subsource": "crash"},
        created_at="2026-05-19T12:00:00+00:00",
        task_id="PR-042",
        repo_slug="octo__demo",
    )

    augmented = asyncio.run(
        dashboard_module._augment_causes_with_dependents(
            "octo__demo", [cause], redis
        )
    )

    assert len(augmented) == 1
    assert (
        augmented[0]["recovery_backup_branch"]
        == "crash-backup/PR-042/20260519-120000"
    )


def test_read_recovery_backup_branch_tolerates_redis_failure() -> None:
    """Redis read errors degrade to ``None`` rather than 5xx-ing the card."""

    class _Boom:
        async def get(self, key: str) -> str:
            raise RuntimeError("redis down")

    result = asyncio.run(
        dashboard_module._read_recovery_backup_branch(
            _Boom(), "octo__demo", "PR-042"
        )
    )

    assert result is None


# ---------------------------------------------------------------------------
# Sanity check on stderr matcher
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "stderr",
    [
        " ! [remote rejected] pr-042 -> pr-042 (push declined)",
        " ! [rejected]        pr-042 -> pr-042 (non-fast-forward)",
    ],
)
def test_push_rejected_by_remote_matches_known_signatures(stderr: str) -> None:
    assert (
        recovery_module.RecoveryMixin._push_rejected_by_remote(stderr) is True
    )


@pytest.mark.parametrize(
    "stderr",
    [
        "fatal: Authentication failed for 'https://github.com/'",
        "fatal: unable to access 'origin'",
        "",
    ],
)
def test_push_rejected_by_remote_ignores_non_rejected_errors(
    stderr: str,
) -> None:
    assert (
        recovery_module.RecoveryMixin._push_rejected_by_remote(stderr)
        is False
    )


def test_timestamp_format_matches_utc_strftime_pattern(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The backup branch timestamp is the UTC ``%Y%m%d-%H%M%S`` string."""
    fixed = datetime(2026, 5, 19, 12, 34, 56, tzinfo=timezone.utc)

    class _FrozenDatetime(datetime):
        @classmethod
        def now(cls, tz: Any = None) -> datetime:
            return fixed

    runner = _make_runner_with_doing(task_id="PR-007")

    def handler(cmd: list[str], **kwargs: Any) -> Any:
        if cmd[:4] == ["git", "rev-parse", "--verify", "--quiet"]:
            return subprocess.CompletedProcess(
                args=cmd, returncode=0, stdout="abc\n", stderr=""
            )
        if cmd[:2] == ["git", "push"]:
            if "crash-backup" not in cmd[-1]:
                raise _rejected_push_error(
                    " ! [remote rejected] x -> x (push declined)"
                )
            return subprocess.CompletedProcess(
                args=cmd, returncode=0, stdout="", stderr=""
            )
        return subprocess.CompletedProcess(
            args=cmd, returncode=0, stdout="", stderr=""
        )

    _stub_subprocess(monkeypatch, handler)
    monkeypatch.setattr(recovery_module, "datetime", _FrozenDatetime)

    runner._preserve_crashed_run_commits("pr-042-inflight")

    assert runner._pending_backup_branch_write is not None
    _, backup_branch = runner._pending_backup_branch_write
    assert backup_branch == "crash-backup/PR-007/20260519-123456"
