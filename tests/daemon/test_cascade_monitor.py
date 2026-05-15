"""Unit tests for the cascade ESCALATE detector (PR-308a)."""

from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest
from src.cancellation.storage import (
    CancellationCause,
    cause_key,
    index_key,
)
from src.config import AppConfig, DaemonConfig
from src.daemon import cascade_monitor
from src.daemon.cascade_monitor import check_cascade_escalate_state
from src.keyspace import daemon_panic_state


class _FakeRedis:
    """In-memory Redis stub covering the slice ``cascade_monitor`` exercises."""

    def __init__(self) -> None:
        self.values: dict[str, str] = {}
        self.zsets: dict[str, dict[str, float]] = {}
        self.deleted: list[str] = []

    async def scan_iter(self, match: str):
        prefix = match.removesuffix("*")
        # Include both string and zset keys, mirroring real SCAN.
        for key in list(self.values) + list(self.zsets):
            if key.startswith(prefix):
                yield key

    async def get(self, key: str) -> str | None:
        return self.values.get(key)

    async def set(self, key: str, value: str) -> bool:
        self.values[key] = value
        return True

    async def delete(self, key: str) -> int:
        self.deleted.append(key)
        existed = key in self.values
        self.values.pop(key, None)
        return int(existed)

    async def zrangebyscore(
        self, key: str, min_score: Any, max_score: Any
    ) -> list[str]:
        bucket = self.zsets.get(key, {})
        lower = float("-inf") if min_score in ("-inf",) else float(min_score)
        upper = float("inf") if max_score in ("+inf",) else float(max_score)
        return [
            member
            for member, score in sorted(bucket.items(), key=lambda kv: kv[1])
            if lower <= score <= upper
        ]


def _config(**daemon: Any) -> AppConfig:
    base = {"cascade_escalate_threshold": 3, "cascade_escalate_window_min": 15}
    base.update(daemon)
    return AppConfig(repositories=[], daemon=DaemonConfig(**base))


def _record(
    redis: _FakeRedis,
    repo: str,
    task_id: str,
    *,
    when: datetime,
    subsource: str = "coder_escalate",
    legacy_category: str | None = None,
) -> None:
    payload: dict[str, Any] = {"subsource": subsource}
    if legacy_category is not None:
        payload["legacy_category"] = legacy_category
    cause = CancellationCause(
        category="ERROR",
        payload=payload,
        created_at=when.isoformat(),
        task_id=task_id,
        repo_slug=repo,
    )
    redis.values[cause_key(repo, task_id)] = cause.to_redis()
    redis.zsets.setdefault(index_key(repo), {})[task_id] = when.timestamp()


def _logger() -> logging.Logger:
    log = logging.getLogger("test.cascade_monitor")
    log.setLevel(logging.DEBUG)
    return log


@pytest.fixture(autouse=True)
def _freeze_now(monkeypatch: pytest.MonkeyPatch) -> datetime:
    frozen = datetime(2026, 5, 15, 12, 0, tzinfo=timezone.utc)

    class _FrozenDatetime(datetime):
        @classmethod
        def now(cls, tz: timezone | None = None) -> "datetime":
            return frozen if tz is None else frozen.astimezone(tz)

    monkeypatch.setattr(cascade_monitor, "datetime", _FrozenDatetime)
    return frozen


@pytest.mark.asyncio
async def test_check_below_threshold_returns_false(
    _freeze_now: datetime,
) -> None:
    redis = _FakeRedis()
    cfg = _config()
    for idx in range(2):
        _record(redis, f"r{idx}", "PR-1", when=_freeze_now - timedelta(minutes=1))

    assert await check_cascade_escalate_state(redis, cfg, _logger()) is False
    assert daemon_panic_state() not in redis.values


@pytest.mark.asyncio
async def test_check_at_threshold_activates_panic(
    _freeze_now: datetime,
) -> None:
    redis = _FakeRedis()
    cfg = _config()
    for idx in range(3):
        _record(redis, f"r{idx}", "PR-1", when=_freeze_now - timedelta(minutes=2))

    assert await check_cascade_escalate_state(redis, cfg, _logger()) is True
    panic = json.loads(redis.values[daemon_panic_state()])
    assert panic["enabled"] is True
    assert panic["reason"] == "cascade_escalate_threshold_exceeded"
    assert panic["affected_repos"] == ["r0", "r1", "r2"]
    assert panic["threshold_at_trigger"] == 3
    assert panic["triggered_at"] == _freeze_now.isoformat()


@pytest.mark.asyncio
async def test_check_above_threshold_activates_panic(
    _freeze_now: datetime,
) -> None:
    redis = _FakeRedis()
    cfg = _config()
    for idx in range(5):
        _record(redis, f"r{idx}", "PR-1", when=_freeze_now - timedelta(minutes=2))

    assert await check_cascade_escalate_state(redis, cfg, _logger()) is True
    panic = json.loads(redis.values[daemon_panic_state()])
    assert len(panic["affected_repos"]) == 5


@pytest.mark.asyncio
async def test_check_old_causes_outside_window_not_counted(
    _freeze_now: datetime,
) -> None:
    redis = _FakeRedis()
    cfg = _config(cascade_escalate_window_min=10)
    for idx in range(3):
        _record(
            redis,
            f"r{idx}",
            "PR-1",
            when=_freeze_now - timedelta(minutes=30),
        )

    assert await check_cascade_escalate_state(redis, cfg, _logger()) is False
    assert daemon_panic_state() not in redis.values


@pytest.mark.asyncio
async def test_check_disabled_when_threshold_zero(
    _freeze_now: datetime,
) -> None:
    redis = _FakeRedis()
    cfg = _config(cascade_escalate_threshold=0)
    for idx in range(10):
        _record(redis, f"r{idx}", "PR-1", when=_freeze_now)

    assert await check_cascade_escalate_state(redis, cfg, _logger()) is False
    assert daemon_panic_state() not in redis.values


@pytest.mark.asyncio
async def test_check_non_escalate_causes_not_counted(
    _freeze_now: datetime,
) -> None:
    redis = _FakeRedis()
    cfg = _config()
    for idx in range(3):
        _record(
            redis,
            f"r{idx}",
            "PR-1",
            when=_freeze_now - timedelta(minutes=1),
            subsource="review_timeout",
        )
    # An ESCALATE cause from one repo alone is still below threshold.
    _record(
        redis,
        "r-only-escalate",
        "PR-1",
        when=_freeze_now - timedelta(minutes=1),
        subsource="coder_escalate",
    )
    # legacy_category form is recognized too.
    _record(
        redis,
        "r-legacy",
        "PR-1",
        when=_freeze_now - timedelta(minutes=1),
        subsource="something_else",
        legacy_category="ESCALATE",
    )

    assert await check_cascade_escalate_state(redis, cfg, _logger()) is False
    assert daemon_panic_state() not in redis.values


@pytest.mark.asyncio
async def test_auto_resume_after_cooldown(_freeze_now: datetime) -> None:
    redis = _FakeRedis()
    cfg = _config(cascade_escalate_auto_resume_min=10)
    redis.values[daemon_panic_state()] = json.dumps(
        {
            "enabled": True,
            "reason": "cascade_escalate_threshold_exceeded",
            "triggered_at": (_freeze_now - timedelta(minutes=15)).isoformat(),
            "affected_repos": ["r0", "r1", "r2"],
            "threshold_at_trigger": 3,
        }
    )

    assert await check_cascade_escalate_state(redis, cfg, _logger()) is False
    assert daemon_panic_state() not in redis.values
    assert daemon_panic_state() in redis.deleted


@pytest.mark.asyncio
async def test_no_auto_resume_when_disabled(_freeze_now: datetime) -> None:
    redis = _FakeRedis()
    cfg = _config(cascade_escalate_auto_resume_min=0)
    redis.values[daemon_panic_state()] = json.dumps(
        {
            "enabled": True,
            "reason": "cascade_escalate_threshold_exceeded",
            "triggered_at": (_freeze_now - timedelta(hours=10)).isoformat(),
            "affected_repos": ["r0", "r1", "r2"],
            "threshold_at_trigger": 3,
        }
    )

    assert await check_cascade_escalate_state(redis, cfg, _logger()) is True
    # The panic record must survive a disabled auto-resume.
    assert daemon_panic_state() in redis.values


@pytest.mark.asyncio
async def test_repeat_panic_preserves_original_triggered_at(
    _freeze_now: datetime,
) -> None:
    redis = _FakeRedis()
    cfg = _config()
    earlier = (_freeze_now - timedelta(minutes=5)).isoformat()
    redis.values[daemon_panic_state()] = json.dumps(
        {
            "enabled": True,
            "reason": "cascade_escalate_threshold_exceeded",
            "triggered_at": earlier,
            "affected_repos": ["r0", "r1", "r2"],
            "threshold_at_trigger": 3,
        }
    )
    for idx in range(3):
        _record(redis, f"r{idx}", "PR-1", when=_freeze_now - timedelta(minutes=1))

    assert await check_cascade_escalate_state(redis, cfg, _logger()) is True
    panic = json.loads(redis.values[daemon_panic_state()])
    assert panic["triggered_at"] == earlier


@pytest.mark.asyncio
async def test_panic_persists_within_cooldown_when_threshold_subsides(
    _freeze_now: datetime,
) -> None:
    redis = _FakeRedis()
    cfg = _config(cascade_escalate_auto_resume_min=60)
    redis.values[daemon_panic_state()] = json.dumps(
        {
            "enabled": True,
            "reason": "cascade_escalate_threshold_exceeded",
            "triggered_at": (_freeze_now - timedelta(minutes=10)).isoformat(),
            "affected_repos": ["r0", "r1", "r2"],
            "threshold_at_trigger": 3,
        }
    )

    assert await check_cascade_escalate_state(redis, cfg, _logger()) is True
    assert daemon_panic_state() in redis.values


@pytest.mark.asyncio
async def test_malformed_state_is_ignored(_freeze_now: datetime) -> None:
    redis = _FakeRedis()
    cfg = _config()
    redis.values[daemon_panic_state()] = "{not json"

    assert await check_cascade_escalate_state(redis, cfg, _logger()) is False


@pytest.mark.asyncio
async def test_corrupt_state_with_bad_triggered_at_keeps_panic(
    _freeze_now: datetime,
) -> None:
    redis = _FakeRedis()
    cfg = _config(cascade_escalate_auto_resume_min=10)
    redis.values[daemon_panic_state()] = json.dumps(
        {"enabled": True, "triggered_at": "not-a-date"}
    )

    assert await check_cascade_escalate_state(redis, cfg, _logger()) is True


@pytest.mark.asyncio
async def test_naive_triggered_at_is_treated_as_utc(
    _freeze_now: datetime,
) -> None:
    redis = _FakeRedis()
    cfg = _config(cascade_escalate_auto_resume_min=10)
    naive = (_freeze_now - timedelta(minutes=15)).replace(tzinfo=None)
    redis.values[daemon_panic_state()] = json.dumps(
        {"enabled": True, "triggered_at": naive.isoformat()}
    )

    assert await check_cascade_escalate_state(redis, cfg, _logger()) is False
    assert daemon_panic_state() in redis.deleted


@pytest.mark.asyncio
async def test_stale_index_entry_without_cause_is_skipped(
    _freeze_now: datetime,
) -> None:
    redis = _FakeRedis()
    cfg = _config()
    # Two valid escalate causes, one repo with a dangling index entry.
    _record(redis, "r0", "PR-1", when=_freeze_now - timedelta(minutes=1))
    _record(redis, "r1", "PR-1", when=_freeze_now - timedelta(minutes=1))
    redis.zsets.setdefault(index_key("r-orphan"), {})["PR-1"] = (
        (_freeze_now - timedelta(minutes=1)).timestamp()
    )

    assert await check_cascade_escalate_state(redis, cfg, _logger()) is False


@pytest.mark.asyncio
async def test_malformed_cause_payload_is_skipped(
    _freeze_now: datetime,
) -> None:
    redis = _FakeRedis()
    cfg = _config()
    redis.values[cause_key("r-bad", "PR-1")] = "{not a cause"
    redis.zsets.setdefault(index_key("r-bad"), {})["PR-1"] = (
        (_freeze_now - timedelta(minutes=1)).timestamp()
    )

    assert await check_cascade_escalate_state(redis, cfg, _logger()) is False


@pytest.mark.asyncio
async def test_empty_index_key_is_ignored(_freeze_now: datetime) -> None:
    redis = _FakeRedis()
    cfg = _config()
    redis.zsets[index_key("")] = {"PR-1": _freeze_now.timestamp()}

    assert await check_cascade_escalate_state(redis, cfg, _logger()) is False


@pytest.mark.asyncio
async def test_cause_with_non_dict_payload_not_counted(
    _freeze_now: datetime,
) -> None:
    redis = _FakeRedis()
    cfg = _config()
    cause = CancellationCause(
        category="ERROR",
        payload={"subsource": "coder_escalate"},
        created_at=_freeze_now.isoformat(),
        task_id="PR-1",
        repo_slug="r0",
    )
    # Mutate the serialized form so payload is a list, not a dict.
    raw = json.loads(cause.to_redis())
    raw["payload"] = []
    redis.values[cause_key("r0", "PR-1")] = json.dumps(raw)
    redis.zsets.setdefault(index_key("r0"), {})["PR-1"] = _freeze_now.timestamp()

    assert await check_cascade_escalate_state(redis, cfg, _logger()) is False


@pytest.mark.asyncio
async def test_state_returns_none_when_payload_not_object(
    _freeze_now: datetime,
) -> None:
    redis = _FakeRedis()
    cfg = _config()
    redis.values[daemon_panic_state()] = json.dumps([1, 2, 3])

    assert await check_cascade_escalate_state(redis, cfg, _logger()) is False


@pytest.mark.asyncio
async def test_bytes_keys_and_values_are_decoded(_freeze_now: datetime) -> None:
    redis = _FakeRedis()
    cfg = _config(cascade_escalate_auto_resume_min=10)
    payload = json.dumps(
        {
            "enabled": True,
            "triggered_at": (_freeze_now - timedelta(minutes=15)).isoformat(),
        }
    )
    redis.values[daemon_panic_state()] = payload.encode("utf-8")  # type: ignore[assignment]

    assert await check_cascade_escalate_state(redis, cfg, _logger()) is False


class _CapturingPanicWebhook:
    """``httpx.AsyncClient`` stand-in capturing panic-notification POSTs."""

    posted: list[tuple[str, dict[str, Any], float]] = []

    def __init__(self, timeout: float) -> None:
        self._timeout = timeout

    async def __aenter__(self) -> "_CapturingPanicWebhook":
        return self

    async def __aexit__(self, *args: object) -> None:
        return None

    async def post(self, url: str, json: dict[str, Any]) -> "_OkResponse":
        type(self).posted.append((url, json, self._timeout))
        return _OkResponse()


class _OkResponse:
    def raise_for_status(self) -> None:
        return None


@pytest.mark.asyncio
async def test_webhook_fires_on_false_to_true_transition(
    _freeze_now: datetime,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    redis = _FakeRedis()
    cfg = _config()
    cfg.daemon.guardrail_notification_webhook_url = "https://hooks.example/x"
    cfg.daemon.guardrail_notification_timeout_seconds = 3.5
    for idx in range(3):
        _record(redis, f"r{idx}", "PR-1", when=_freeze_now - timedelta(minutes=2))

    _CapturingPanicWebhook.posted = []
    monkeypatch.setattr(cascade_monitor.httpx, "AsyncClient", _CapturingPanicWebhook)

    assert await check_cascade_escalate_state(redis, cfg, _logger()) is True
    assert len(_CapturingPanicWebhook.posted) == 1
    url, payload, timeout = _CapturingPanicWebhook.posted[0]
    assert url == "https://hooks.example/x"
    assert timeout == 3.5
    assert payload == {
        "event": "cascade_panic_activated",
        "reason": "cascade_escalate_threshold_exceeded",
        "affected_repos": ["r0", "r1", "r2"],
        "threshold": 3,
        "window_min": cfg.daemon.cascade_escalate_window_min,
    }


@pytest.mark.asyncio
async def test_webhook_does_not_fire_on_already_active(
    _freeze_now: datetime,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    redis = _FakeRedis()
    cfg = _config()
    cfg.daemon.guardrail_notification_webhook_url = "https://hooks.example/x"
    earlier = (_freeze_now - timedelta(minutes=5)).isoformat()
    redis.values[daemon_panic_state()] = json.dumps(
        {
            "enabled": True,
            "reason": "cascade_escalate_threshold_exceeded",
            "triggered_at": earlier,
            "affected_repos": ["r0", "r1", "r2"],
            "threshold_at_trigger": 3,
        }
    )
    for idx in range(3):
        _record(redis, f"r{idx}", "PR-1", when=_freeze_now - timedelta(minutes=1))

    _CapturingPanicWebhook.posted = []
    constructed: list[Any] = []

    class _ShouldNotConstruct:
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            constructed.append((args, kwargs))

    monkeypatch.setattr(cascade_monitor.httpx, "AsyncClient", _ShouldNotConstruct)

    assert await check_cascade_escalate_state(redis, cfg, _logger()) is True
    assert constructed == []


@pytest.mark.asyncio
async def test_panic_webhook_skipped_when_url_unset(
    _freeze_now: datetime,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cfg = _config()
    cfg.daemon.guardrail_notification_webhook_url = None

    constructed: list[Any] = []

    class _ShouldNotConstruct:
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            constructed.append((args, kwargs))

    monkeypatch.setattr(cascade_monitor.httpx, "AsyncClient", _ShouldNotConstruct)

    await cascade_monitor._fire_panic_activation_webhook(
        cfg,
        affected_repos=["r0", "r1", "r2"],
        threshold=3,
        window_min=15,
        log=_logger(),
    )
    assert constructed == []


@pytest.mark.asyncio
async def test_panic_webhook_swallows_post_exception(
    _freeze_now: datetime,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cfg = _config()
    cfg.daemon.guardrail_notification_webhook_url = "https://hooks.example/x"

    class _BoomClient:
        def __init__(self, timeout: float) -> None: ...
        async def __aenter__(self) -> "_BoomClient": return self
        async def __aexit__(self, *args: object) -> None: return None
        async def post(self, url: str, json: dict[str, Any]) -> None:
            raise RuntimeError("network unreachable")

    monkeypatch.setattr(cascade_monitor.httpx, "AsyncClient", _BoomClient)

    # Must not raise — the panic state has already been persisted and the
    # daemon loop must keep running even when notification delivery fails.
    await cascade_monitor._fire_panic_activation_webhook(
        cfg,
        affected_repos=["r0"],
        threshold=1,
        window_min=15,
        log=_logger(),
    )
