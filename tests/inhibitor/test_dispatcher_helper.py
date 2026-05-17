"""Unit tests for ``is_work_inhibited`` and ``FeatureFlags`` (PR-329).

The helper reads ``state.active_inhibitors`` (populated by PR-328's
``derive_active_inhibitors``) and answers "is anything blocking now for
the candidate coder?" without re-walking Redis. It has no callers in
this PR; PR-330 wires the dispatcher under the per-repo
``feature_flags.use_unified_inhibitor_check`` flag.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

from src.config import FeatureFlags, RepoConfig, load_config
from src.inhibitor import InhibitorType, WorkInhibitor, is_work_inhibited
from src.models import RepoState


def _make_state(**overrides: object) -> RepoState:
    payload: dict[str, object] = {
        "url": "https://github.com/octo/demo",
        "name": "octo__demo",
    }
    payload.update(overrides)
    return RepoState(**payload)  # type: ignore[arg-type]


def _global_pause() -> WorkInhibitor:
    return WorkInhibitor(
        inhibitor_type=InhibitorType.USER_PAUSE,
        reason_text="Operator paused",
        source_key="state:octo__demo.user_paused",
    )


def _rate_limit_for(
    coder: str, expires_at: datetime | None = None
) -> WorkInhibitor:
    return WorkInhibitor(
        inhibitor_type=InhibitorType.RATE_LIMIT,
        coder_affected=coder,
        expires_at=expires_at,
        reason_text=f"{coder} rate-limited",
        source_key=f"state:octo__demo.rate_limited_coder_until.{coder}",
    )


def test_is_work_inhibited_empty_returns_false() -> None:
    state = _make_state()
    blocked, blocking = is_work_inhibited(state)
    assert blocked is False
    assert blocking == []


def test_is_work_inhibited_global_pause_blocks_any_coder() -> None:
    state = _make_state(active_inhibitors=[_global_pause()])

    blocked_none, blocking_none = is_work_inhibited(state)
    blocked_claude, blocking_claude = is_work_inhibited(state, coder="claude")
    blocked_codex, blocking_codex = is_work_inhibited(state, coder="codex")

    assert blocked_none is True
    assert [i.inhibitor_type for i in blocking_none] == [InhibitorType.USER_PAUSE]
    assert blocked_claude is True
    assert [i.inhibitor_type for i in blocking_claude] == [InhibitorType.USER_PAUSE]
    assert blocked_codex is True
    assert [i.inhibitor_type for i in blocking_codex] == [InhibitorType.USER_PAUSE]


def test_is_work_inhibited_per_coder_blocks_only_matching() -> None:
    future = datetime.now(timezone.utc) + timedelta(minutes=30)
    state = _make_state(active_inhibitors=[_rate_limit_for("claude", future)])

    blocked_claude, blocking_claude = is_work_inhibited(state, coder="claude")
    blocked_codex, blocking_codex = is_work_inhibited(state, coder="codex")

    assert blocked_claude is True
    assert [i.coder_affected for i in blocking_claude] == ["claude"]
    assert blocked_codex is False
    assert blocking_codex == []


def test_is_work_inhibited_per_coder_with_coder_none_matches() -> None:
    future = datetime.now(timezone.utc) + timedelta(minutes=30)
    state = _make_state(active_inhibitors=[_rate_limit_for("claude", future)])

    blocked, blocking = is_work_inhibited(state, coder=None)

    assert blocked is True
    assert [i.coder_affected for i in blocking] == ["claude"]


def test_is_work_inhibited_expired_inhibitor_does_not_block() -> None:
    past = datetime.now(timezone.utc) - timedelta(seconds=30)
    state = _make_state(active_inhibitors=[_rate_limit_for("claude", past)])

    blocked, blocking = is_work_inhibited(state)

    assert blocked is False
    assert blocking == []


def test_is_work_inhibited_returns_all_blocking_inhibitors() -> None:
    future = datetime.now(timezone.utc) + timedelta(minutes=30)
    state = _make_state(
        active_inhibitors=[
            _global_pause(),
            _rate_limit_for("claude", future),
            WorkInhibitor(
                inhibitor_type=InhibitorType.CASCADE_PANIC,
                reason_text="Cascade panic mode auto-stop",
                source_key="daemon:panic_state",
            ),
        ]
    )

    blocked, blocking = is_work_inhibited(state)

    assert blocked is True
    assert len(blocking) == 3
    assert {i.inhibitor_type for i in blocking} == {
        InhibitorType.USER_PAUSE,
        InhibitorType.RATE_LIMIT,
        InhibitorType.CASCADE_PANIC,
    }


def test_is_work_inhibited_skips_expired_keeps_active() -> None:
    past = datetime.now(timezone.utc) - timedelta(seconds=30)
    future = datetime.now(timezone.utc) + timedelta(minutes=30)
    state = _make_state(
        active_inhibitors=[
            _rate_limit_for("claude", past),
            _rate_limit_for("codex", future),
        ]
    )

    blocked, blocking = is_work_inhibited(state)

    assert blocked is True
    assert [i.coder_affected for i in blocking] == ["codex"]


def test_feature_flag_default_false() -> None:
    flags = FeatureFlags()
    assert flags.use_unified_inhibitor_check is False

    repo = RepoConfig(url="https://github.com/octo/demo")
    assert repo.feature_flags.use_unified_inhibitor_check is False


def test_feature_flag_loadable_from_yaml(tmp_path: Path) -> None:
    cfg_path = tmp_path / "config.yml"
    cfg_path.write_text(
        "repositories:\n"
        "  - url: https://github.com/octo/demo\n"
        "    feature_flags:\n"
        "      use_unified_inhibitor_check: true\n",
        encoding="utf-8",
    )

    config = load_config(str(cfg_path))

    assert len(config.repositories) == 1
    assert (
        config.repositories[0].feature_flags.use_unified_inhibitor_check
        is True
    )
