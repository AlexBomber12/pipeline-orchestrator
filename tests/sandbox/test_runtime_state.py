"""Unit tests for :mod:`src.sandbox.runtime_state` (PR-353)."""

from __future__ import annotations

import asyncio
import logging
from typing import Any

import pytest

from src.sandbox import runtime_state
from src.sandbox.runtime_state import (
    REDIS_SANDBOX_STATE_KEY,
    SMOKE_TEST_TIMEOUT_SEC,
    SandboxState,
    detect_sandbox_state,
    refresh_sandbox_state,
)


class _FakeProc:
    """Async-friendly stand-in for ``asyncio.subprocess.Process``."""

    def __init__(
        self,
        *,
        returncode: int = 0,
        wait_delay: float = 0.0,
        raise_on_wait: BaseException | None = None,
    ) -> None:
        self.returncode = returncode
        self._wait_delay = wait_delay
        self._raise_on_wait = raise_on_wait
        self.killed = False

    async def wait(self) -> int:
        if self._raise_on_wait is not None:
            raise self._raise_on_wait
        if self._wait_delay:
            await asyncio.sleep(self._wait_delay)
        return self.returncode

    def kill(self) -> None:
        self.killed = True


class _FakeRedis:
    """Capture the last value written to ``REDIS_SANDBOX_STATE_KEY``."""

    def __init__(self, *, raise_on_set: BaseException | None = None) -> None:
        self.store: dict[str, str] = {}
        self._raise_on_set = raise_on_set

    async def set(self, key: str, value: str) -> None:
        if self._raise_on_set is not None:
            raise self._raise_on_set
        self.store[key] = value


def _install_subprocess_factory(
    monkeypatch: pytest.MonkeyPatch, proc: _FakeProc
) -> dict[str, Any]:
    """Replace ``asyncio.create_subprocess_exec`` and record one invocation."""
    captured: dict[str, Any] = {"args": None, "kwargs": None}

    async def _factory(*args: Any, **kwargs: Any) -> _FakeProc:
        captured["args"] = args
        captured["kwargs"] = kwargs
        return proc

    monkeypatch.setattr(
        runtime_state.asyncio, "create_subprocess_exec", _factory
    )
    return captured


@pytest.mark.asyncio
async def test_detect_disabled_when_config_off(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # bwrap "present" must still be ignored when the config flag is off.
    monkeypatch.setattr(runtime_state.shutil, "which", lambda _: "/usr/bin/bwrap")

    async def _fail_factory(*_args: Any, **_kwargs: Any) -> _FakeProc:
        raise AssertionError("subprocess must not run when isolation disabled")

    monkeypatch.setattr(
        runtime_state.asyncio, "create_subprocess_exec", _fail_factory
    )

    assert await detect_sandbox_state(False) is SandboxState.DISABLED


@pytest.mark.asyncio
async def test_detect_unavailable_when_bwrap_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(runtime_state.shutil, "which", lambda _: None)
    assert await detect_sandbox_state(True) is SandboxState.UNAVAILABLE


@pytest.mark.asyncio
async def test_detect_active_when_bwrap_present_and_smoke_passes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(runtime_state.shutil, "which", lambda _: "/usr/bin/bwrap")
    captured = _install_subprocess_factory(monkeypatch, _FakeProc(returncode=0))

    assert await detect_sandbox_state(True) is SandboxState.ACTIVE
    # Smoke test must match the documented bwrap argv so a future
    # accidental change to the probe is caught by the tests.
    assert captured["args"] == (
        "bwrap",
        "--bind", "/", "/",
        "--dev-bind", "/dev", "/dev",
        "/bin/true",
    )


@pytest.mark.asyncio
async def test_detect_unavailable_when_smoke_test_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(runtime_state.shutil, "which", lambda _: "/usr/bin/bwrap")
    _install_subprocess_factory(monkeypatch, _FakeProc(returncode=1))

    assert await detect_sandbox_state(True) is SandboxState.UNAVAILABLE


@pytest.mark.asyncio
async def test_detect_unavailable_when_smoke_test_times_out(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(runtime_state.shutil, "which", lambda _: "/usr/bin/bwrap")
    # ``wait_delay`` longer than the timeout so ``asyncio.wait_for`` trips.
    proc = _FakeProc(returncode=0, wait_delay=SMOKE_TEST_TIMEOUT_SEC + 10.0)
    _install_subprocess_factory(monkeypatch, proc)

    # Replace ``asyncio.wait_for`` with a synchronous timeout so the test
    # does not actually wait five real seconds. The implementation calls
    # ``proc.kill()`` and a follow-up ``proc.wait()`` after the timeout
    # which must complete cleanly under the patched ``wait``.
    async def _instant_timeout(awaitable: Any, timeout: float) -> Any:
        # Drain the awaitable so the test does not leak a pending task.
        awaitable.close()
        raise asyncio.TimeoutError

    monkeypatch.setattr(runtime_state.asyncio, "wait_for", _instant_timeout)

    # Make the post-kill wait return immediately to avoid lingering.
    proc._wait_delay = 0.0  # type: ignore[attr-defined]

    assert await detect_sandbox_state(True) is SandboxState.UNAVAILABLE
    assert proc.killed is True


@pytest.mark.asyncio
async def test_detect_unavailable_when_spawn_raises(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(runtime_state.shutil, "which", lambda _: "/usr/bin/bwrap")

    async def _raise(*_args: Any, **_kwargs: Any) -> _FakeProc:
        raise OSError("EPERM")

    monkeypatch.setattr(
        runtime_state.asyncio, "create_subprocess_exec", _raise
    )

    assert await detect_sandbox_state(True) is SandboxState.UNAVAILABLE


@pytest.mark.asyncio
async def test_detect_unavailable_when_kill_post_timeout_raises(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(runtime_state.shutil, "which", lambda _: "/usr/bin/bwrap")

    proc = _FakeProc(returncode=0, raise_on_wait=RuntimeError("already gone"))
    _install_subprocess_factory(monkeypatch, proc)

    async def _instant_timeout(awaitable: Any, timeout: float) -> Any:
        awaitable.close()
        raise asyncio.TimeoutError

    monkeypatch.setattr(runtime_state.asyncio, "wait_for", _instant_timeout)

    assert await detect_sandbox_state(True) is SandboxState.UNAVAILABLE
    assert proc.killed is True


@pytest.mark.asyncio
async def test_refresh_writes_state_to_redis(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(runtime_state.shutil, "which", lambda _: "/usr/bin/bwrap")
    _install_subprocess_factory(monkeypatch, _FakeProc(returncode=0))
    fake = _FakeRedis()

    result = await refresh_sandbox_state(fake, True)

    assert result is SandboxState.ACTIVE
    assert fake.store[REDIS_SANDBOX_STATE_KEY] == SandboxState.ACTIVE.value


@pytest.mark.asyncio
async def test_refresh_writes_disabled_without_invoking_subprocess(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def _fail_factory(*_args: Any, **_kwargs: Any) -> _FakeProc:
        raise AssertionError("must not invoke bwrap when isolation disabled")

    monkeypatch.setattr(
        runtime_state.asyncio, "create_subprocess_exec", _fail_factory
    )
    fake = _FakeRedis()

    result = await refresh_sandbox_state(fake, False)

    assert result is SandboxState.DISABLED
    assert fake.store[REDIS_SANDBOX_STATE_KEY] == SandboxState.DISABLED.value


@pytest.mark.asyncio
async def test_refresh_logs_warning_when_enabled_but_unavailable(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    monkeypatch.setattr(runtime_state.shutil, "which", lambda _: None)
    fake = _FakeRedis()

    with caplog.at_level(logging.WARNING, logger=runtime_state.logger.name):
        result = await refresh_sandbox_state(fake, True)

    assert result is SandboxState.UNAVAILABLE
    assert fake.store[REDIS_SANDBOX_STATE_KEY] == SandboxState.UNAVAILABLE.value
    messages = [rec.getMessage() for rec in caplog.records]
    assert any(
        "bubblewrap unavailable" in msg.lower()
        and "install bwrap" in msg.lower()
        for msg in messages
    )


@pytest.mark.asyncio
async def test_refresh_does_not_warn_when_isolation_disabled(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    fake = _FakeRedis()
    with caplog.at_level(logging.WARNING, logger=runtime_state.logger.name):
        await refresh_sandbox_state(fake, False)
    assert not any(
        "bubblewrap" in rec.getMessage().lower() for rec in caplog.records
    )


@pytest.mark.asyncio
async def test_refresh_swallows_redis_set_errors(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    monkeypatch.setattr(runtime_state.shutil, "which", lambda _: "/usr/bin/bwrap")
    _install_subprocess_factory(monkeypatch, _FakeProc(returncode=0))
    fake = _FakeRedis(raise_on_set=RuntimeError("redis down"))

    with caplog.at_level(logging.WARNING, logger=runtime_state.logger.name):
        result = await refresh_sandbox_state(fake, True)

    assert result is SandboxState.ACTIVE
    assert any(
        REDIS_SANDBOX_STATE_KEY in rec.getMessage()
        for rec in caplog.records
    )


def test_sandbox_state_is_str_enum() -> None:
    # Storing the value verbatim in Redis is part of the contract — the
    # template reads back a string and compares against ``'disabled'`` /
    # ``'active'`` / ``'unavailable'``. Lock the on-the-wire format here.
    assert SandboxState.DISABLED.value == "disabled"
    assert SandboxState.ACTIVE.value == "active"
    assert SandboxState.UNAVAILABLE.value == "unavailable"
    assert SandboxState("active") is SandboxState.ACTIVE
