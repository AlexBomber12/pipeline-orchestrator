from __future__ import annotations

import json
from datetime import datetime, timezone

import pytest
from src.events import publisher
from tests.runner import _helpers as h


def _latest_entry(runner: h.PipelineRunner) -> dict[str, object]:
    return dict(runner.state.history[-1])


def test_log_event_legacy_format_preserved() -> None:
    runner = h._make_runner()

    runner.log_event("Posted @codex review on PR #N.")

    entry = _latest_entry(runner)
    assert entry["event"] == "Posted @codex review on PR #N."
    assert entry["tier"] is None
    assert entry["kind"] is None


def test_log_event_with_tier() -> None:
    runner = h._make_runner()

    runner.log_event("Posted @codex review", tier="infra")

    entry = _latest_entry(runner)
    assert entry["tier"] == "infra"
    assert entry["kind"] is None


def test_log_event_with_tier_and_kind() -> None:
    runner = h._make_runner()

    runner.log_event("Posted @codex review", tier="infra", kind="review_post")

    entry = _latest_entry(runner)
    assert entry["tier"] == "infra"
    assert entry["kind"] == "review_post"


def test_log_event_invalid_tier_rejected() -> None:
    runner = h._make_runner()

    with pytest.raises(ValueError, match="Unknown event tier"):
        runner.log_event("bad tier", tier="bogus")


def test_log_event_debug_rejects_structured_bracket_prefix(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = h._make_runner()
    monkeypatch.setenv("DEBUG_EVENT_LOG_BRACKETS", "1")

    with pytest.raises(AssertionError, match="bracket prefixes"):
        runner.log_event("[INFRA] Posted @codex review", tier="infra")


async def test_disk_log_includes_tier_kind(isolate_events_dir) -> None:
    redis = h._FakeRedis()
    payload = {
        "entry": {
            "event": "PR #420 merged.",
            "tier": "merge",
            "kind": "pr_merge",
        }
    }

    await publisher.publish_repo_event(
        "owner__repo",
        "event_log_append",
        payload,
        redis_client=redis,
    )

    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    target = isolate_events_dir / "owner__repo" / f"{today}.jsonl"
    record = json.loads(target.read_text(encoding="utf-8").splitlines()[0])
    assert record["tier"] == "merge"
    assert record["kind"] == "pr_merge"


async def test_disk_log_omits_when_none(isolate_events_dir) -> None:
    redis = h._FakeRedis()

    await publisher.publish_repo_event(
        "owner__repo",
        "event_log_append",
        {"entry": {"event": "legacy"}},
        redis_client=redis,
    )

    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    target = isolate_events_dir / "owner__repo" / f"{today}.jsonl"
    record = json.loads(target.read_text(encoding="utf-8").splitlines()[0])
    assert record["tier"] is None
    assert record["kind"] is None
