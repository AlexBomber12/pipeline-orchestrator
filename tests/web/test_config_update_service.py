"""Unit tests for ``apply_config_mutation`` (PR-217).

The helper centralizes the post-write nudge that every config-mutating
endpoint owes the daemon: SET ``control:{repo}:config_dirty`` and PUBLISH
on ``orchestrator:wake:{repo}``. Tests cover the order of operations, the
multi-repo broadcast used by daemon-level writes, and the resilience
contract that any individual Redis failure is logged at WARNING and
swallowed so the endpoint can still return success.
"""

from __future__ import annotations

import json

import pytest
from src.web.services.config_updates import apply_config_mutation


class _FakeRedis:
    """Minimal async Redis double recording every SET and PUBLISH call."""

    def __init__(self) -> None:
        self.store: dict[str, str] = {}
        self.published: list[tuple[str, str]] = []
        self.set_calls: list[tuple[str, str]] = []

    async def set(self, key: str, value: str, **_kwargs: object) -> None:
        self.set_calls.append((key, value))
        self.store[key] = value

    async def publish(self, channel: str, message: str) -> int:
        self.published.append((channel, message))
        return 1


class _SetBoomRedis(_FakeRedis):
    async def set(self, key: str, value: str, **_kwargs: object) -> None:
        self.set_calls.append((key, value))
        raise ConnectionError("redis set unavailable")


class _PublishBoomRedis(_FakeRedis):
    async def publish(self, channel: str, message: str) -> int:
        self.published.append((channel, message))
        raise ConnectionError("redis publish unavailable")


async def test_sets_dirty_key_for_each_affected_repo() -> None:
    fake = _FakeRedis()

    await apply_config_mutation(
        redis_client=fake,
        affected_repo_names=["alpha", "beta"],
        event_type="settings",
    )

    assert fake.store["control:alpha:config_dirty"] == "1"
    assert fake.store["control:beta:config_dirty"] == "1"


async def test_publishes_wake_for_each_affected_repo() -> None:
    fake = _FakeRedis()

    await apply_config_mutation(
        redis_client=fake,
        affected_repo_names=["alpha", "beta"],
        event_type="settings",
    )

    channels = [channel for channel, _ in fake.published]
    assert channels == [
        "orchestrator:wake:alpha",
        "orchestrator:wake:beta",
    ]
    for _, message in fake.published:
        payload = json.loads(message)
        assert payload["event_type"] == "settings"


async def test_dirty_set_precedes_publish_for_each_repo() -> None:
    """Order matters: the inbox flag must be set before the doorbell rings.

    If the wake event fires before the dirty key is in place, the daemon
    can wake, find no dirty key, and immediately go back to sleep.
    """
    events: list[tuple[str, str]] = []

    class _OrderRecordingRedis(_FakeRedis):
        async def set(self, key: str, value: str, **_kwargs: object) -> None:
            events.append(("set", key))
            await super().set(key, value)

        async def publish(self, channel: str, message: str) -> int:
            events.append(("publish", channel))
            return await super().publish(channel, message)

    await apply_config_mutation(
        redis_client=_OrderRecordingRedis(),
        affected_repo_names=["alpha", "beta"],
        event_type="settings",
    )

    assert events == [
        ("set", "control:alpha:config_dirty"),
        ("publish", "orchestrator:wake:alpha"),
        ("set", "control:beta:config_dirty"),
        ("publish", "orchestrator:wake:beta"),
    ]


async def test_set_failure_is_logged_and_swallowed_publish_still_runs(
    caplog: pytest.LogCaptureFixture,
) -> None:
    fake = _SetBoomRedis()

    with caplog.at_level("WARNING", logger="src.web.services.config_updates"):
        await apply_config_mutation(
            redis_client=fake,
            affected_repo_names=["alpha"],
            event_type="settings",
        )

    # The set was attempted and recorded even though it raised.
    assert fake.set_calls == [("control:alpha:config_dirty", "1")]
    # publish_wake still ran after the set failure: the helper does not
    # short-circuit so the daemon still gets the wake nudge and the
    # config_watcher fallback covers the missing dirty flag.
    assert any(
        channel == "orchestrator:wake:alpha" for channel, _ in fake.published
    )
    assert any(
        "Failed to set control:alpha:config_dirty" in record.getMessage()
        for record in caplog.records
    )


async def test_publish_failure_is_logged_and_swallowed(
    caplog: pytest.LogCaptureFixture,
) -> None:
    fake = _PublishBoomRedis()

    with caplog.at_level("WARNING", logger="src.web.services.config_updates"):
        await apply_config_mutation(
            redis_client=fake,
            affected_repo_names=["alpha"],
            event_type="settings",
        )

    # The dirty set succeeded.
    assert fake.store["control:alpha:config_dirty"] == "1"
    # publish was attempted and the failure was logged at WARNING.
    assert fake.published == [
        (
            "orchestrator:wake:alpha",
            fake.published[0][1],
        )
    ]
    assert any(
        "publish_wake failed for alpha" in record.getMessage()
        and "settings" in record.getMessage()
        for record in caplog.records
    )


async def test_empty_affected_set_is_a_no_op() -> None:
    fake = _FakeRedis()

    await apply_config_mutation(
        redis_client=fake,
        affected_repo_names=[],
        event_type="settings",
    )

    assert fake.set_calls == []
    assert fake.published == []


async def test_failure_for_one_repo_does_not_block_remaining_repos(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """If publish raises for repo A, repo B must still get its dirty+wake."""

    class _SelectivelyFailingRedis(_FakeRedis):
        async def publish(self, channel: str, message: str) -> int:
            self.published.append((channel, message))
            if channel == "orchestrator:wake:alpha":
                raise ConnectionError("simulated alpha publish failure")
            return 1

    fake = _SelectivelyFailingRedis()

    with caplog.at_level("WARNING", logger="src.web.services.config_updates"):
        await apply_config_mutation(
            redis_client=fake,
            affected_repo_names=["alpha", "beta"],
            event_type="settings",
        )

    # Both repos got their dirty key set.
    assert fake.store["control:alpha:config_dirty"] == "1"
    assert fake.store["control:beta:config_dirty"] == "1"
    # Both wake channels saw an attempted publish.
    assert [channel for channel, _ in fake.published] == [
        "orchestrator:wake:alpha",
        "orchestrator:wake:beta",
    ]
    # The alpha failure was reported.
    assert any(
        "publish_wake failed for alpha" in record.getMessage()
        for record in caplog.records
    )
