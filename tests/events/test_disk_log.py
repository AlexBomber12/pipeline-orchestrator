"""Tests for the disk-persistent event log mirror."""

from __future__ import annotations

import json
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path

import pytest
from src.events import disk_log, publisher


def _read_lines(path: Path) -> list[str]:
    return path.read_text(encoding="utf-8").splitlines()


def test_append_creates_dated_file_per_repo(isolate_events_dir: Path) -> None:
    when = datetime(2026, 5, 19, 12, 0, tzinfo=timezone.utc)
    disk_log.append_event_to_disk(
        "owner__repo", "state_changed", {"state": "WATCH"}, timestamp=when
    )

    target = isolate_events_dir / "owner__repo" / "2026-05-19.jsonl"
    assert target.exists()
    assert len(_read_lines(target)) == 1


def test_append_dates_separate_files(isolate_events_dir: Path) -> None:
    before_midnight = datetime(2026, 5, 19, 23, 59, 30, tzinfo=timezone.utc)
    after_midnight = datetime(2026, 5, 20, 0, 0, 5, tzinfo=timezone.utc)

    disk_log.append_event_to_disk(
        "owner__repo", "ev1", {}, timestamp=before_midnight
    )
    disk_log.append_event_to_disk(
        "owner__repo", "ev2", {}, timestamp=after_midnight
    )

    repo_dir = isolate_events_dir / "owner__repo"
    assert (repo_dir / "2026-05-19.jsonl").exists()
    assert (repo_dir / "2026-05-20.jsonl").exists()


def test_repos_separate_files(isolate_events_dir: Path) -> None:
    when = datetime(2026, 5, 19, 12, 0, tzinfo=timezone.utc)
    disk_log.append_event_to_disk("owner__alpha", "ev", {}, timestamp=when)
    disk_log.append_event_to_disk("owner__beta", "ev", {}, timestamp=when)

    alpha = isolate_events_dir / "owner__alpha" / "2026-05-19.jsonl"
    beta = isolate_events_dir / "owner__beta" / "2026-05-19.jsonl"
    assert alpha.exists()
    assert beta.exists()
    assert alpha.parent != beta.parent


def test_append_swallows_oserror(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    def _boom(*args: object, **kwargs: object) -> None:
        raise OSError("disk full")

    monkeypatch.setattr(disk_log, "open", _boom, raising=False)

    with caplog.at_level("WARNING", logger=disk_log.logger.name):
        disk_log.append_event_to_disk("owner__repo", "ev", {"k": "v"})

    assert any(
        "Event disk write failed" in record.message for record in caplog.records
    )


def test_concurrent_writes_no_corruption(isolate_events_dir: Path) -> None:
    when = datetime(2026, 5, 19, 12, 0, tzinfo=timezone.utc)
    barrier = threading.Barrier(parties=20)

    def _writer(index: int) -> None:
        barrier.wait()
        disk_log.append_event_to_disk(
            "owner__repo",
            "ev",
            {"index": index},
            timestamp=when,
        )

    with ThreadPoolExecutor(max_workers=20) as pool:
        list(pool.map(_writer, range(100)))

    target = isolate_events_dir / "owner__repo" / "2026-05-19.jsonl"
    lines = _read_lines(target)
    assert len(lines) == 100
    parsed_indices = sorted(json.loads(line)["payload"]["index"] for line in lines)
    assert parsed_indices == list(range(100))


def test_record_shape_correct(isolate_events_dir: Path) -> None:
    when = datetime(2026, 5, 19, 12, 0, tzinfo=timezone.utc)
    disk_log.append_event_to_disk(
        "owner__repo", "state_changed", {"state": "WATCH"}, timestamp=when
    )

    target = isolate_events_dir / "owner__repo" / "2026-05-19.jsonl"
    record = json.loads(_read_lines(target)[0])
    assert set(record.keys()) == {
        "timestamp",
        "event_type",
        "repo_slug",
        "payload",
        "tier",
        "kind",
    }
    assert record["timestamp"] == when.isoformat()
    assert record["event_type"] == "state_changed"
    assert record["repo_slug"] == "owner__repo"
    assert record["payload"] == {"state": "WATCH"}
    assert record["tier"] is None
    assert record["kind"] is None


def test_default_events_dir_when_env_unset(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("PO_EVENTS_DIR", raising=False)
    assert disk_log._resolve_events_dir() == Path("/data/events")


async def test_publish_repo_event_also_writes_disk(
    isolate_events_dir: Path,
) -> None:
    class _FakeRedis:
        def __init__(self) -> None:
            self.lpushes: list[tuple[str, str]] = []

        async def lpush(self, key: str, value: str) -> int:
            self.lpushes.append((key, value))
            return 1

        async def ltrim(self, key: str, start: int, stop: int) -> None:
            return None

        async def publish(self, channel: str, message: str) -> int:
            return 1

        async def aclose(self) -> None:
            return None

    redis = _FakeRedis()
    await publisher.publish_repo_event(
        "owner__repo", "state_changed", {"state": "WATCH"}, redis_client=redis
    )

    assert redis.lpushes, "Redis LPUSH did not happen"
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    target = isolate_events_dir / "owner__repo" / f"{today}.jsonl"
    assert target.exists()
    record = json.loads(_read_lines(target)[0])
    assert record["event_type"] == "state_changed"
    assert record["payload"] == {"state": "WATCH"}


async def test_publish_repo_event_uses_single_now_for_redis_and_disk(
    isolate_events_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Redis message and disk row must share one timestamp.

    Guards against a drift between the Redis message and the disk
    mirror when a publish happens near UTC midnight: the JSONL row
    must land in the same daily partition as the timestamp embedded
    in the Redis event.
    """

    class _FakeRedis:
        def __init__(self) -> None:
            self.messages: list[str] = []

        async def lpush(self, key: str, value: str) -> int:
            self.messages.append(value)
            return 1

        async def ltrim(self, key: str, start: int, stop: int) -> None:
            return None

        async def publish(self, channel: str, message: str) -> int:
            return 1

        async def aclose(self) -> None:
            return None

    near_midnight = datetime(2026, 5, 19, 23, 59, 59, 999_000, tzinfo=timezone.utc)
    calls = iter(
        [
            near_midnight,
            datetime(2026, 5, 20, 0, 0, 0, 1_000, tzinfo=timezone.utc),
        ]
    )

    def _fake_now() -> datetime:
        try:
            return next(calls)
        except StopIteration:
            return datetime(2026, 5, 20, 0, 0, 1, tzinfo=timezone.utc)

    monkeypatch.setattr(publisher, "_utc_now", _fake_now)

    redis = _FakeRedis()
    await publisher.publish_repo_event(
        "owner__repo", "state_changed", {"state": "WATCH"}, redis_client=redis
    )

    redis_payload = json.loads(redis.messages[0])
    assert redis_payload["timestamp"] == "2026-05-19T23:59:59.999000Z"

    target = isolate_events_dir / "owner__repo" / "2026-05-19.jsonl"
    assert target.exists(), "Disk row must land in the same daily partition as Redis"
    record = json.loads(_read_lines(target)[0])
    assert record["timestamp"] == near_midnight.isoformat()


def test_disk_log_resilient_to_event_with_non_serializable_payload(
    isolate_events_dir: Path, caplog: pytest.LogCaptureFixture
) -> None:
    when = datetime(2026, 5, 19, 12, 0, tzinfo=timezone.utc)

    payload_with_set: dict[str, object] = {"tags": {"alpha", "beta"}}
    disk_log.append_event_to_disk(
        "owner__repo", "ev_set", payload_with_set, timestamp=when
    )

    payload_with_datetime: dict[str, object] = {
        "scheduled_for": datetime(2026, 6, 1, tzinfo=timezone.utc)
    }
    disk_log.append_event_to_disk(
        "owner__repo", "ev_dt", payload_with_datetime, timestamp=when
    )

    circular: dict[str, object] = {}
    circular["self"] = circular
    with caplog.at_level("WARNING", logger=disk_log.logger.name):
        disk_log.append_event_to_disk(
            "owner__repo", "ev_circular", circular, timestamp=when
        )

    target = isolate_events_dir / "owner__repo" / "2026-05-19.jsonl"
    lines = _read_lines(target)
    assert len(lines) == 2
    for line in lines:
        record = json.loads(line)
        assert record["event_type"] in {"ev_set", "ev_dt"}

    assert any(
        "Event disk write failed" in record.message for record in caplog.records
    )
