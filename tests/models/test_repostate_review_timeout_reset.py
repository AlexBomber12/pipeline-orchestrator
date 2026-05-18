"""PR-358: RepoState.review_timeout_repost_attempted defaults and resets.

The single-shot ``@codex review`` repost flag must default to ``False``,
reset on PR-iteration boundaries via the ``__setattr__`` hook (new
``current_pr`` number/branch, ``current_pr = None``, ``current_task =
None``), survive a same-PR refresh, and round-trip through Pydantic JSON
serialization (including old payloads written before the field existed).
"""

from __future__ import annotations

import json
from datetime import datetime, timezone

from src.models import PRInfo, QueueTask, RepoState, TaskStatus


def _state(**overrides: object) -> RepoState:
    base: dict[str, object] = {
        "url": "https://github.com/example/repo.git",
        "name": "repo",
    }
    base.update(overrides)
    return RepoState(**base)  # type: ignore[arg-type]


def test_default_value_false() -> None:
    state = _state()
    assert state.review_timeout_repost_attempted is False


def test_setattr_resets_on_new_pr_number() -> None:
    state = _state(current_pr=PRInfo(number=1, branch="pr-001"))
    state.review_timeout_repost_attempted = True

    state.current_pr = PRInfo(number=2, branch="pr-002")

    assert state.review_timeout_repost_attempted is False


def test_setattr_preserves_on_same_pr() -> None:
    state = _state(current_pr=PRInfo(number=1, branch="pr-001"))
    state.review_timeout_repost_attempted = True

    state.current_pr = PRInfo(
        number=1, branch="pr-001", title="refreshed"
    )

    assert state.review_timeout_repost_attempted is True


def test_setattr_resets_on_pr_to_none() -> None:
    state = _state(current_pr=PRInfo(number=1, branch="pr-001"))
    state.review_timeout_repost_attempted = True

    state.current_pr = None

    assert state.review_timeout_repost_attempted is False


def test_setattr_resets_on_task_none() -> None:
    state = _state(
        current_task=QueueTask(
            pr_id="PR-001", title="t", status=TaskStatus.DOING,
        )
    )
    state.review_timeout_repost_attempted = True

    state.current_task = None

    assert state.review_timeout_repost_attempted is False


def test_json_round_trip_preserves_field() -> None:
    state = _state()
    state.review_timeout_repost_attempted = True

    restored = RepoState.model_validate_json(state.model_dump_json())

    assert restored.review_timeout_repost_attempted is True


def test_json_round_trip_old_payload_defaults_false() -> None:
    legacy_payload = json.dumps(
        {
            "url": "https://github.com/example/repo.git",
            "name": "repo",
            "state": "IDLE",
        }
    )

    state = RepoState.model_validate_json(legacy_payload)

    assert state.review_timeout_repost_attempted is False


# PR-358 review feedback: the durable repost timestamp follows the same
# default/reset semantics as the boolean flag so the elapsed_min floor
# survives PR refresh without leaking across PR-iteration boundaries.


def test_repost_at_default_none() -> None:
    state = _state()
    assert state.review_timeout_repost_at is None


def test_repost_at_resets_on_new_pr_number() -> None:
    state = _state(current_pr=PRInfo(number=1, branch="pr-001"))
    state.review_timeout_repost_at = datetime.now(timezone.utc)

    state.current_pr = PRInfo(number=2, branch="pr-002")

    assert state.review_timeout_repost_at is None


def test_repost_at_preserves_on_same_pr() -> None:
    state = _state(current_pr=PRInfo(number=1, branch="pr-001"))
    stamp = datetime.now(timezone.utc)
    state.review_timeout_repost_at = stamp

    state.current_pr = PRInfo(
        number=1, branch="pr-001", title="refreshed"
    )

    assert state.review_timeout_repost_at == stamp


def test_repost_at_resets_on_pr_to_none() -> None:
    state = _state(current_pr=PRInfo(number=1, branch="pr-001"))
    state.review_timeout_repost_at = datetime.now(timezone.utc)

    state.current_pr = None

    assert state.review_timeout_repost_at is None


def test_repost_at_resets_on_task_none() -> None:
    state = _state(
        current_task=QueueTask(
            pr_id="PR-001", title="t", status=TaskStatus.DOING,
        )
    )
    state.review_timeout_repost_at = datetime.now(timezone.utc)

    state.current_task = None

    assert state.review_timeout_repost_at is None


def test_repost_at_json_round_trip_preserves_value() -> None:
    state = _state()
    stamp = datetime(2026, 5, 18, 12, 30, 0, tzinfo=timezone.utc)
    state.review_timeout_repost_at = stamp

    restored = RepoState.model_validate_json(state.model_dump_json())

    assert restored.review_timeout_repost_at == stamp


def test_repost_at_json_round_trip_old_payload_defaults_none() -> None:
    legacy_payload = json.dumps(
        {
            "url": "https://github.com/example/repo.git",
            "name": "repo",
            "state": "IDLE",
        }
    )

    state = RepoState.model_validate_json(legacy_payload)

    assert state.review_timeout_repost_at is None
