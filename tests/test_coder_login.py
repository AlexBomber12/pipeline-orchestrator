from __future__ import annotations

import asyncio
import json
import sys
from dataclasses import replace
from pathlib import Path
from typing import Any, Callable

import pytest
from src import coder_login
from src.coder_login import CoderCredentialReservations, CoderLoginSessionManager
from src.coder_registry import (
    CoderAuthStatus,
    CoderDeviceLoginFailure,
    CoderDeviceLoginPrompt,
    CoderRegistry,
    coder_auth_payload,
)
from src.coders.codex import CodexDeviceLoginAdapter
from src.config import AppConfig
from src.process_supervisor import (
    CleanupResult,
    CleanupStatus,
    ProcessRunResult,
    ProcessSupervisionError,
)

_REFERENCE = "tests.fake_login:factory"
_PROMPT = (
    "\nWelcome to Codex\n"
    "1. Open this link in your browser and sign in to your account\n"
    "   \x1b[94mhttps://auth.openai.com/codex/device\x1b[0m\n\n"
    "2. Enter this one-time code \x1b[90m(expires in 15 minutes)\x1b[0m\n"
    "   \x1b[94mABCD-EFGH\x1b[0m\n"
)


class _Plugin:
    name = "codex"
    display_name = "Codex CLI"

    def __init__(self, adapter: object) -> None:
        self.adapter = adapter

    def create_device_login(self, *, config_path: str) -> object:
        assert config_path == "/cfg/config.yml"
        return self.adapter

    def device_login_credential_location(self, *, config: AppConfig) -> str:
        del config
        return getattr(self.adapter, "credential_location", "/tmp/invalid-adapter")


class _MissingLocatorPlugin:
    name = "missing-locator"
    display_name = "Missing Locator"

    def __init__(self, adapter: object) -> None:
        self.adapter = adapter

    def create_device_login(self, *, config_path: str) -> object:
        assert config_path == "/cfg/config.yml"
        return self.adapter


class _FactoryFailurePlugin(_Plugin):
    def create_device_login(self, *, config_path: str) -> object:
        raise RuntimeError(f"must-not-leak:{config_path}")


class _UnsupportedPlugin:
    name = "claude"
    display_name = "Claude"


class _Managed:
    def __init__(
        self,
        *,
        quiescent: bool = True,
        reconcile_quiescent: bool | None = None,
        cleanup_error: Exception | None = None,
    ) -> None:
        self.quiescent = quiescent
        self.reconcile_quiescent = reconcile_quiescent
        self.cleanup_error = cleanup_error
        self.cleanup_calls = 0
        self.reconcile_calls = 0

    async def cleanup(self, **_kwargs: object) -> CleanupResult:
        self.cleanup_calls += 1
        if self.cleanup_error is not None:
            raise self.cleanup_error
        return CleanupResult(
            CleanupStatus.QUIESCENT if self.quiescent else CleanupStatus.FAILED,
            0 if self.quiescent else None,
            True,
            not self.quiescent,
            None if self.quiescent else "ownership uncertain",
        )

    async def reconcile_cleanup(self, **_kwargs: object) -> CleanupResult:
        self.reconcile_calls += 1
        if self.cleanup_error is not None:
            raise self.cleanup_error
        quiescent = (
            self.quiescent
            if self.reconcile_quiescent is None
            else self.reconcile_quiescent
        )
        return CleanupResult(
            CleanupStatus.QUIESCENT if quiescent else CleanupStatus.FAILED,
            0 if quiescent else None,
            False,
            False,
            None if quiescent else "ownership still uncertain",
        )


@pytest.fixture(autouse=True)
def _stub_login_config(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(coder_login, "load_config", lambda _path: AppConfig())


def _adapter(
    *,
    location: str = "/tmp/fake-codex-home",
    timeout: float = 960,
) -> CodexDeviceLoginAdapter:
    return CodexDeviceLoginAdapter(
        command=("fake-codex", "login", "--device-auth"),
        environment={"HOME": "/tmp/fake-home"},
        working_directory="/tmp",
        credential_location=location,
        application_timeout_seconds=timeout,
    )


def _auth(
    saved: bool | None,
    *,
    cli_available: bool | None = True,
) -> dict[str, Any]:
    if saved is True:
        return coder_auth_payload(
            CoderAuthStatus(
                status="ok",
                detail="Saved credentials found",
                cli_available=cli_available,
                cli_version="0.160.0",
                saved_credentials_present=True,
                authentication_mode="chatgpt",
            )
        )
    if saved is False:
        return coder_auth_payload(
            CoderAuthStatus(
                status="error",
                detail="No saved credentials",
                cli_available=cli_available,
                cli_version="0.160.0" if cli_available else None,
                saved_credentials_present=False,
                failure_reason=(
                    "credentials_missing" if cli_available else "cli_missing"
                ),
            )
        )
    return coder_auth_payload(
        CoderAuthStatus(
            status="error",
            detail="Status unavailable",
            cli_available=cli_available,
            failure_reason="probe_failed",
        )
    )


def _manager(
    adapter: object | None = None,
    *,
    plugin_name: str = "codex",
    active: Callable[[str], bool] | None = None,
    reservations: CoderCredentialReservations | None = None,
    wall_time: Callable[[], float] = lambda: 1_000.0,
    monotonic: Callable[[], float] = lambda: 10.0,
) -> tuple[CoderLoginSessionManager, CoderRegistry]:
    registry = CoderRegistry()
    plugin = _Plugin(adapter or _adapter())
    plugin.name = plugin_name
    registry.register(plugin, reference=_REFERENCE)
    return (
        CoderLoginSessionManager(
            registry,
            config_path="/cfg/config.yml",
            credential_location_in_use=active,
            credential_reservations=reservations,
            wall_time=wall_time,
            monotonic=monotonic,
        ),
        registry,
    )


def test_credential_reservations_allow_coder_sharing_but_exclude_login() -> None:
    reservations = CoderCredentialReservations()
    location = "/tmp/shared-codex-home"

    assert reservations.credential_version(location) == 0
    assert reservations.login_active(location) is False
    assert reservations.reserve_coder(location) is True
    assert reservations.reserve_coder(location) is True
    assert reservations.reserve_login(location) is False
    reservations.release_coder(location)
    assert reservations.reserve_login(location) is False
    reservations.release_coder(location)
    assert reservations.reserve_login(location) is True
    assert reservations.login_active(location) is True
    assert reservations.reserve_login(location) is False
    assert reservations.reserve_coder(location) is False
    reservations.release_login(location)
    assert reservations.credential_version(location) == 1
    assert reservations.login_active(location) is False
    reservations.release_login(location)
    assert reservations.credential_version(location) == 1
    reservations.release_coder(location)
    assert reservations.reserve_coder(location) is True
    reservations.release_coder(location)


@pytest.mark.asyncio
async def test_device_login_reserves_before_background_work_and_releases_on_cancel(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    reservations = CoderCredentialReservations()
    location = "/tmp/fake-codex-home"
    manager, _ = _manager(reservations=reservations)

    assert reservations.reserve_coder(location) is True
    blocked = await manager.start(
        "codex", expected_reference=_REFERENCE, replace_existing=False
    )
    assert blocked["failure_reason"] == "credential_in_use"
    reservations.release_coder(location)

    auth_release = asyncio.Event()

    async def blocked_auth(*_args: object, **_kwargs: object) -> dict[str, Any]:
        await auth_release.wait()
        return _auth(False)

    monkeypatch.setattr(coder_login, "isolated_auth_probe", blocked_auth)
    started = await manager.start(
        "codex", expected_reference=_REFERENCE, replace_existing=False
    )
    assert started["state"] == "starting"
    assert reservations.reserve_coder(location) is False

    cancelled = await manager.cancel(
        "codex", started["session_id"], expected_reference=_REFERENCE
    )
    assert cancelled["state"] == "cancelled"
    assert reservations.reserve_coder(location) is True
    reservations.release_coder(location)


async def _wait_for_state(
    manager: CoderLoginSessionManager,
    session_id: str,
    *states: str,
    plugin: str = "codex",
    reference: str = _REFERENCE,
) -> dict[str, Any]:
    for _ in range(200):
        payload = await manager.inspect(
            plugin, session_id, expected_reference=reference
        )
        if payload["state"] in states:
            return payload
        await asyncio.sleep(0.01)
    raise AssertionError(f"session did not reach {states}: {payload}")


@pytest.mark.asyncio
async def test_device_login_fake_process_reports_code_and_refreshes_auth(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    fake_cli = (
        "import sys,time;"
        "sys.stdout.write(" + repr(_PROMPT[:90]) + ");sys.stdout.flush();"
        "time.sleep(0.02);"
        "sys.stdout.write(" + repr(_PROMPT[90:]) + ");sys.stdout.flush();"
        "sys.stderr.write('raw-sensitive-diagnostic');sys.stderr.flush()"
    )
    adapter = replace(
        _adapter(location=str(tmp_path / ".codex")),
        command=(sys.executable, "-c", fake_cli),
        working_directory=str(tmp_path),
    )
    manager, _ = _manager(adapter)
    probes = iter((_auth(False), _auth(True)))

    async def auth_probe(*_args: object, **_kwargs: object) -> dict[str, Any]:
        return next(probes)

    monkeypatch.setattr(coder_login, "isolated_auth_probe", auth_probe)

    started = await manager.start(
        "codex", expected_reference=_REFERENCE, replace_existing=False
    )
    duplicate = await manager.start(
        "codex", expected_reference=_REFERENCE, replace_existing=False
    )

    assert started["state"] == "starting"
    assert duplicate["session_id"] == started["session_id"]
    assert duplicate["reused_session"] is True
    assert len(started["session_id"]) >= 32

    waiting = await _wait_for_state(
        manager,
        started["session_id"],
        "waiting_for_user",
        "succeeded",
    )
    if waiting["state"] == "waiting_for_user":
        assert waiting["verification_url"] == ("https://auth.openai.com/codex/device")
        assert waiting["user_code"] == "ABCD-EFGH"
        assert waiting["expires_at"] == 1_900.0

    completed = await _wait_for_state(manager, started["session_id"], "succeeded")
    assert completed["cleanup_confirmed"] is True
    assert completed["verification_url"] is None
    assert completed["user_code"] is None
    assert completed["auth_status"]["saved_credentials_present"] is True
    assert completed["auth_status"]["service_access_verified"] is None
    assert "raw-sensitive-diagnostic" not in json.dumps(completed)
    await manager.shutdown()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("saved", "replace_existing", "active", "reason"),
    [
        (True, False, False, "replacement_required"),
        (None, False, False, "auth_status_unavailable"),
        (False, True, True, "credential_in_use"),
    ],
)
async def test_device_login_preflight_rejects_unsafe_start(
    monkeypatch: pytest.MonkeyPatch,
    saved: bool | None,
    replace_existing: bool,
    active: bool,
    reason: str,
) -> None:
    manager, _ = _manager(active=lambda _location: active)
    launches: list[object] = []

    async def auth_probe(*_args: object, **_kwargs: object) -> dict[str, Any]:
        return _auth(saved)

    async def launch(*args: object, **kwargs: object) -> _Managed:
        launches.append((args, kwargs))
        return _Managed()

    monkeypatch.setattr(coder_login, "isolated_auth_probe", auth_probe)
    monkeypatch.setattr(coder_login, "launch_process", launch)

    started = await manager.start(
        "codex",
        expected_reference=_REFERENCE,
        replace_existing=replace_existing,
    )
    result = await _wait_for_state(manager, started["session_id"], "failed")

    assert result["failure_reason"] == reason
    assert launches == []
    assert "clears existing saved authentication" in result["replacement_warning"]


@pytest.mark.asyncio
async def test_device_login_fails_closed_when_activity_check_raises(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def broken_activity(_location: str) -> bool:
        raise RuntimeError("must-not-leak")

    manager, _ = _manager(active=broken_activity)

    async def auth_probe(*_args: object, **_kwargs: object) -> dict[str, Any]:
        return _auth(False)

    monkeypatch.setattr(coder_login, "isolated_auth_probe", auth_probe)
    started = await manager.start(
        "codex", expected_reference=_REFERENCE, replace_existing=False
    )
    result = await _wait_for_state(manager, started["session_id"], "failed")

    assert result["failure_reason"] == "credential_in_use"
    assert "must-not-leak" not in json.dumps(result)


@pytest.mark.asyncio
async def test_device_login_reports_missing_cli_before_launch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manager, _ = _manager()

    async def auth_probe(*_args: object, **_kwargs: object) -> dict[str, Any]:
        return _auth(False, cli_available=False)

    monkeypatch.setattr(coder_login, "isolated_auth_probe", auth_probe)
    started = await manager.start(
        "codex", expected_reference=_REFERENCE, replace_existing=False
    )
    result = await _wait_for_state(manager, started["session_id"], "failed")

    assert result["failure_reason"] == "cli_missing"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("error", "reason"),
    [
        (FileNotFoundError("fake-codex"), "cli_missing"),
        (OSError("launch-secret"), "startup_failed"),
    ],
)
async def test_device_login_sanitizes_startup_failure(
    monkeypatch: pytest.MonkeyPatch, error: Exception, reason: str
) -> None:
    manager, _ = _manager()

    async def auth_probe(*_args: object, **_kwargs: object) -> dict[str, Any]:
        return _auth(False)

    async def launch(*_args: object, **_kwargs: object) -> object:
        raise error

    monkeypatch.setattr(coder_login, "isolated_auth_probe", auth_probe)
    monkeypatch.setattr(coder_login, "launch_process", launch)
    started = await manager.start(
        "codex", expected_reference=_REFERENCE, replace_existing=False
    )
    result = await _wait_for_state(manager, started["session_id"], "failed")

    assert result["failure_reason"] == reason
    assert "launch-secret" not in json.dumps(result)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("result", "stderr", "expected_state", "expected_reason"),
    [
        (
            ProcessRunResult(1, b"", b"device auth timed out after 15 minutes"),
            b"device auth timed out after 15 minutes",
            "expired",
            "provider_expired",
        ),
        (
            ProcessRunResult(1, b"", b"device code login is not enabled"),
            b"device code login is not enabled",
            "failed",
            "device_login_disabled",
        ),
        (
            ProcessRunResult(1, b"", b"raw-secret-provider-error"),
            b"raw-secret-provider-error",
            "failed",
            "process_failed",
        ),
        (
            ProcessRunResult(1, b"", b"", timed_out=True),
            b"",
            "timed_out",
            "application_timeout",
        ),
        (
            ProcessRunResult(0, b"", b""),
            b"",
            "failed",
            "malformed_output",
        ),
    ],
)
async def test_device_login_classifies_process_outcomes(
    monkeypatch: pytest.MonkeyPatch,
    result: ProcessRunResult,
    stderr: bytes,
    expected_state: str,
    expected_reason: str,
) -> None:
    manager, _ = _manager()
    managed = _Managed()

    async def auth_probe(*_args: object, **_kwargs: object) -> dict[str, Any]:
        return _auth(False)

    async def launch(*_args: object, **_kwargs: object) -> _Managed:
        return managed

    async def run(*_args: object, **kwargs: object) -> ProcessRunResult:
        if stderr:
            kwargs["stderr_chunk_callback"](stderr)
        return result

    monkeypatch.setattr(coder_login, "isolated_auth_probe", auth_probe)
    monkeypatch.setattr(coder_login, "launch_process", launch)
    monkeypatch.setattr(coder_login, "run_supervised_process", run)

    started = await manager.start(
        "codex", expected_reference=_REFERENCE, replace_existing=False
    )
    final = await _wait_for_state(manager, started["session_id"], expected_state)

    assert final["failure_reason"] == expected_reason
    assert "raw-secret-provider-error" not in json.dumps(final)


@pytest.mark.asyncio
async def test_device_login_malformed_prompt_requests_owned_cleanup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manager, _ = _manager()
    managed = _Managed()
    invalid_prompt = _PROMPT.replace("https://", "http://")

    async def auth_probe(*_args: object, **_kwargs: object) -> dict[str, Any]:
        return _auth(False)

    async def launch(*_args: object, **_kwargs: object) -> _Managed:
        return managed

    async def run(*_args: object, **kwargs: object) -> ProcessRunResult:
        kwargs["stdout_chunk_callback"](invalid_prompt.encode())
        await asyncio.sleep(0)
        return ProcessRunResult(1, b"", b"")

    monkeypatch.setattr(coder_login, "isolated_auth_probe", auth_probe)
    monkeypatch.setattr(coder_login, "launch_process", launch)
    monkeypatch.setattr(coder_login, "run_supervised_process", run)
    started = await manager.start(
        "codex", expected_reference=_REFERENCE, replace_existing=False
    )
    final = await _wait_for_state(manager, started["session_id"], "failed")

    assert final["failure_reason"] == "malformed_output"
    assert managed.cleanup_calls >= 1


@pytest.mark.asyncio
async def test_device_login_rejects_prompt_outside_wire_contract(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manager, _ = _manager()
    managed = _Managed()

    async def auth_probe(*_args: object, **_kwargs: object) -> dict[str, Any]:
        return _auth(False)

    async def launch(*_args: object, **_kwargs: object) -> _Managed:
        return managed

    async def run(*_args: object, **kwargs: object) -> ProcessRunResult:
        kwargs["stdout_chunk_callback"](b"provider prompt\n")
        await asyncio.sleep(0)
        return ProcessRunResult(1, b"", b"")

    monkeypatch.setattr(coder_login, "isolated_auth_probe", auth_probe)
    monkeypatch.setattr(coder_login, "launch_process", launch)
    monkeypatch.setattr(coder_login, "run_supervised_process", run)
    monkeypatch.setattr(
        CodexDeviceLoginAdapter,
        "parse_progress",
        lambda *_args: CoderDeviceLoginPrompt(
            "https://example.com/device",
            "lowercase",
            60,
        ),
    )

    started = await manager.start(
        "codex", expected_reference=_REFERENCE, replace_existing=False
    )
    final = await _wait_for_state(manager, started["session_id"], "failed")

    assert final["failure_reason"] == "malformed_output"
    assert managed.cleanup_calls >= 1


@pytest.mark.asyncio
async def test_device_login_cancel_confirms_or_retains_process_ownership(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def auth_probe(*_args: object, **_kwargs: object) -> dict[str, Any]:
        return _auth(False)

    monkeypatch.setattr(coder_login, "isolated_auth_probe", auth_probe)

    for quiescent, expected in ((True, "cancelled"), (False, "cleanup_failed")):
        manager, _ = _manager()
        managed = _Managed(quiescent=quiescent)
        released = asyncio.Event()

        async def launch(*_args: object, **_kwargs: object) -> _Managed:
            return managed

        async def run(*_args: object, **kwargs: object) -> ProcessRunResult:
            kwargs["stdout_chunk_callback"](_PROMPT.encode())
            await released.wait()
            return ProcessRunResult(1, b"", b"")

        monkeypatch.setattr(coder_login, "launch_process", launch)
        monkeypatch.setattr(coder_login, "run_supervised_process", run)
        started = await manager.start(
            "codex", expected_reference=_REFERENCE, replace_existing=False
        )
        await _wait_for_state(manager, started["session_id"], "waiting_for_user")
        if quiescent:
            released.set()
        cancelled = await manager.cancel(
            "codex", started["session_id"], expected_reference=_REFERENCE
        )

        assert cancelled["state"] == expected
        assert cancelled["cleanup_confirmed"] is quiescent
        if not quiescent:
            assert manager._sessions[started["session_id"]].managed is managed
            manager._sessions[started["session_id"]].task.cancel()
            released.set()
            await asyncio.gather(
                manager._sessions[started["session_id"]].task,
                return_exceptions=True,
            )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("reconcile_quiescent", "expected_state"),
    ((True, "cancelled"), (False, "cleanup_failed")),
)
async def test_device_login_cancel_reconciles_prior_cleanup_failure(
    reconcile_quiescent: bool,
    expected_state: str,
) -> None:
    reservations = CoderCredentialReservations()
    manager, _ = _manager(reservations=reservations)
    managed = _Managed(
        quiescent=False,
        reconcile_quiescent=reconcile_quiescent,
    )
    session = coder_login._LoginSession(
        "R" * 43,
        "codex",
        _REFERENCE,
        _adapter(),
        False,
        1,
        state="cleanup_failed",
        cleanup_confirmed=False,
        managed=managed,  # type: ignore[arg-type]
        reservation_held=True,
    )
    manager._sessions[session.session_id] = session
    assert reservations.reserve_login(session.adapter.credential_location)

    result = await manager.cancel(
        "codex", session.session_id, expected_reference=_REFERENCE
    )

    assert result["state"] == expected_state
    assert result["cleanup_confirmed"] is (
        True if reconcile_quiescent else False
    )
    assert managed.cleanup_calls == 0
    assert managed.reconcile_calls == 1
    assert reservations.reserve_coder(
        session.adapter.credential_location
    ) is reconcile_quiescent
    if reconcile_quiescent:
        reservations.release_coder(session.adapter.credential_location)


@pytest.mark.asyncio
async def test_device_login_shutdown_reconciles_prior_cleanup_failure() -> None:
    reservations = CoderCredentialReservations()
    manager, _ = _manager(reservations=reservations)
    managed = _Managed(quiescent=False, reconcile_quiescent=True)
    session = coder_login._LoginSession(
        "W" * 43,
        "codex",
        _REFERENCE,
        _adapter(),
        False,
        1,
        state="cleanup_failed",
        cleanup_confirmed=False,
        managed=managed,  # type: ignore[arg-type]
        reservation_held=True,
    )
    manager._sessions[session.session_id] = session
    assert reservations.reserve_login(session.adapter.credential_location)

    await manager.shutdown()

    assert session.state == "cancelled"
    assert session.cleanup_confirmed is True
    assert managed.reconcile_calls == 1
    assert reservations.reserve_coder(session.adapter.credential_location)
    reservations.release_coder(session.adapter.credential_location)


@pytest.mark.asyncio
async def test_device_login_cancel_during_start_and_terminal_cancel(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manager, _ = _manager()
    entered = asyncio.Event()

    async def auth_probe(*_args: object, **_kwargs: object) -> dict[str, Any]:
        entered.set()
        await asyncio.Event().wait()
        raise AssertionError("unreachable")

    monkeypatch.setattr(coder_login, "isolated_auth_probe", auth_probe)
    started = await manager.start(
        "codex", expected_reference=_REFERENCE, replace_existing=False
    )
    await entered.wait()
    cancelled = await manager.cancel(
        "codex", started["session_id"], expected_reference=_REFERENCE
    )
    repeated = await manager.cancel(
        "codex", started["session_id"], expected_reference=_REFERENCE
    )

    assert cancelled["state"] == "cancelled"
    assert repeated["state"] == "cancelled"


@pytest.mark.asyncio
async def test_device_login_session_identity_and_retention(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = [10.0]
    manager, registry = _manager(monotonic=lambda: clock[0])

    async def auth_probe(*_args: object, **_kwargs: object) -> dict[str, Any]:
        return _auth(False, cli_available=False)

    monkeypatch.setattr(coder_login, "isolated_auth_probe", auth_probe)
    started = await manager.start(
        "codex", expected_reference=_REFERENCE, replace_existing=False
    )
    await _wait_for_state(manager, started["session_id"], "failed")

    assert (
        await manager.inspect(
            "other", started["session_id"], expected_reference=_REFERENCE
        )
    )["failure_reason"] == "session_plugin_mismatch"
    assert (
        await manager.cancel(
            "codex", started["session_id"], expected_reference="changed:factory"
        )
    )["failure_reason"] == "session_plugin_mismatch"
    assert (await manager.inspect("codex", "A" * 43, expected_reference=_REFERENCE))[
        "state"
    ] == "not_found"

    clock[0] += coder_login._TERMINAL_RETENTION_SECONDS + 1
    expired = await manager.inspect(
        "codex", started["session_id"], expected_reference=_REFERENCE
    )
    assert expired["state"] == "not_found"
    assert manager._sessions == {}
    assert registry.get("codex").display_name == "Codex CLI"


@pytest.mark.asyncio
async def test_device_login_optional_plugin_and_identity_failures(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    registry = CoderRegistry()
    registry.register(_UnsupportedPlugin(), reference="claude:factory")
    registry.register(
        _FactoryFailurePlugin(_adapter()),
        reference=_REFERENCE,
    )
    registry.register(
        _MissingLocatorPlugin(_adapter()),  # type: ignore[arg-type]
        reference="missing:locator",
    )
    manager = CoderLoginSessionManager(registry, config_path="/cfg/config.yml")

    assert (
        await manager.start(
            "missing", expected_reference="missing:factory", replace_existing=False
        )
    )["state"] == "unsupported"
    assert (
        await manager.start(
            "claude", expected_reference="claude:factory", replace_existing=False
        )
    )["state"] == "unsupported"
    factory_failure = await manager.start(
        "codex", expected_reference=_REFERENCE, replace_existing=False
    )
    assert factory_failure["failure_reason"] == "startup_failed"
    assert "must-not-leak" not in json.dumps(factory_failure)
    missing_locator = await manager.start(
        "missing-locator",
        expected_reference="missing:locator",
        replace_existing=False,
    )
    assert missing_locator["failure_reason"] == "startup_failed"
    assert (
        await manager.start(
            "claude", expected_reference="changed:factory", replace_existing=False
        )
    )["failure_reason"] == "session_plugin_mismatch"

    invalid_manager, _ = _manager(adapter=object())
    assert (
        await invalid_manager.start(
            "codex", expected_reference=_REFERENCE, replace_existing=False
        )
    )["state"] == "unsupported"

    mismatch_manager, mismatch_registry = _manager()
    monkeypatch.setattr(
        mismatch_registry.get("codex"),
        "device_login_credential_location",
        lambda **_kwargs: "/tmp/different-credential-home",
    )
    assert (
        await mismatch_manager.start(
            "codex", expected_reference=_REFERENCE, replace_existing=False
        )
    )["state"] == "unsupported"

    manager._shutting_down = True
    assert (
        await manager.start(
            "claude", expected_reference="claude:factory", replace_existing=True
        )
    )["failure_reason"] == "daemon_shutdown"

    monkeypatch.setattr(coder_login, "_MAX_SESSIONS", 0)
    capacity_manager, _ = _manager()
    assert (
        await capacity_manager.start(
            "codex", expected_reference=_REFERENCE, replace_existing=False
        )
    )["failure_reason"] == "session_capacity"


@pytest.mark.asyncio
async def test_device_login_shared_location_conflicts_across_plugins(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    registry = CoderRegistry()
    first = _Plugin(_adapter())
    second = _Plugin(_adapter())
    second.name = "other"
    registry.register(first, reference=_REFERENCE)
    registry.register(second, reference="other:factory")
    manager = CoderLoginSessionManager(registry, config_path="/cfg/config.yml")
    block = asyncio.Event()

    async def auth_probe(*_args: object, **_kwargs: object) -> dict[str, Any]:
        await block.wait()
        return _auth(False)

    monkeypatch.setattr(coder_login, "isolated_auth_probe", auth_probe)
    await manager.start("codex", expected_reference=_REFERENCE, replace_existing=False)
    conflict = await manager.start(
        "other", expected_reference="other:factory", replace_existing=False
    )

    assert conflict["failure_reason"] == "credential_in_use"
    await manager.shutdown()


@pytest.mark.asyncio
async def test_device_login_shutdown_cleans_starting_and_owned_sessions(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manager, _ = _manager()
    auth_release = asyncio.Event()

    async def blocked_auth(*_args: object, **_kwargs: object) -> dict[str, Any]:
        await auth_release.wait()
        return _auth(False)

    monkeypatch.setattr(coder_login, "isolated_auth_probe", blocked_auth)
    starting = await manager.start(
        "codex", expected_reference=_REFERENCE, replace_existing=False
    )
    await asyncio.sleep(0)
    await manager.shutdown()
    assert (
        await manager.inspect(
            "codex", starting["session_id"], expected_reference=_REFERENCE
        )
    )["state"] == "cancelled"

    owned_manager, _ = _manager()
    managed = _Managed()
    run_release = asyncio.Event()

    async def auth_probe(*_args: object, **_kwargs: object) -> dict[str, Any]:
        return _auth(False)

    async def launch(*_args: object, **_kwargs: object) -> _Managed:
        return managed

    async def run(*_args: object, **kwargs: object) -> ProcessRunResult:
        kwargs["stdout_chunk_callback"](_PROMPT.encode())
        await run_release.wait()
        return ProcessRunResult(1, b"", b"")

    monkeypatch.setattr(coder_login, "isolated_auth_probe", auth_probe)
    monkeypatch.setattr(coder_login, "launch_process", launch)
    monkeypatch.setattr(coder_login, "run_supervised_process", run)
    owned = await owned_manager.start(
        "codex", expected_reference=_REFERENCE, replace_existing=False
    )
    await _wait_for_state(owned_manager, owned["session_id"], "waiting_for_user")
    shutdown = asyncio.create_task(owned_manager.shutdown())
    run_release.set()
    await shutdown
    final = await owned_manager.inspect(
        "codex", owned["session_id"], expected_reference=_REFERENCE
    )
    assert final["state"] == "cancelled"
    assert managed.cleanup_calls >= 1


@pytest.mark.asyncio
async def test_device_login_process_supervision_failures_keep_cleanup_truthful(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def auth_probe(*_args: object, **_kwargs: object) -> dict[str, Any]:
        return _auth(False)

    monkeypatch.setattr(coder_login, "isolated_auth_probe", auth_probe)
    for quiescent, expected in ((True, "failed"), (False, "cleanup_failed")):
        reservations = CoderCredentialReservations()
        manager, _ = _manager(reservations=reservations)
        managed = _Managed(quiescent=quiescent)

        async def launch(*_args: object, **_kwargs: object) -> _Managed:
            return managed

        async def run(*_args: object, **_kwargs: object) -> ProcessRunResult:
            raise ProcessSupervisionError(
                "secret-supervision-detail",
                stdout=b"secret-stdout",
                stderr=b"secret-stderr",
            )

        monkeypatch.setattr(coder_login, "launch_process", launch)
        monkeypatch.setattr(coder_login, "run_supervised_process", run)
        started = await manager.start(
            "codex", expected_reference=_REFERENCE, replace_existing=False
        )
        final = await _wait_for_state(manager, started["session_id"], expected)
        assert "secret" not in json.dumps(final)
        assert final["cleanup_confirmed"] is quiescent
        assert reservations.reserve_coder("/tmp/fake-codex-home") is quiescent
        if quiescent:
            reservations.release_coder("/tmp/fake-codex-home")


def test_device_login_validates_adapter_and_prompt_contracts() -> None:
    good = _adapter()
    assert CoderLoginSessionManager._valid_adapter(good) is True
    assert CoderLoginSessionManager._valid_adapter(object()) is False
    assert CoderLoginSessionManager._valid_adapter(replace(good, command=())) is False
    assert (
        CoderLoginSessionManager._valid_adapter(replace(good, environment={"BAD": 3}))
        is False
    )
    assert (
        CoderLoginSessionManager._valid_adapter(replace(good, working_directory=""))
        is False
    )
    assert (
        CoderLoginSessionManager._valid_adapter(replace(good, credential_location=""))
        is False
    )
    assert (
        CoderLoginSessionManager._valid_adapter(
            replace(good, application_timeout_seconds=float("inf"))
        )
        is False
    )
    assert (
        CoderLoginSessionManager._valid_adapter(replace(good, replacement_warning=""))
        is False
    )

    prompt = CoderDeviceLoginPrompt("https://example.com/device", "ABCD", 60)
    assert CoderLoginSessionManager._valid_prompt(prompt) is True
    assert CoderLoginSessionManager._valid_prompt(object()) is False
    assert (
        CoderLoginSessionManager._valid_prompt(
            replace(prompt, verification_url="http://example.com")
        )
        is False
    )
    assert (
        CoderLoginSessionManager._valid_prompt(
            replace(prompt, verification_url="https://" + "x" * 249)
        )
        is False
    )
    assert (
        CoderLoginSessionManager._valid_prompt(replace(prompt, user_code="")) is False
    )
    assert (
        CoderLoginSessionManager._valid_prompt(
            replace(prompt, user_code="lowercase")
        )
        is False
    )
    assert (
        CoderLoginSessionManager._valid_prompt(replace(prompt, expires_in_seconds=True))
        is False
    )
    assert (
        CoderLoginSessionManager._valid_prompt(replace(prompt, expires_in_seconds=3601))
        is False
    )

    class ExplodingAdapter:
        @property
        def command(self) -> tuple[str, ...]:
            raise RuntimeError("broken property")

        environment = {}
        working_directory = "/tmp"
        credential_location = "/tmp/.codex"
        application_timeout_seconds = 1
        replacement_warning = "warning"

        def parse_progress(self, *_args: object) -> None:
            return None

        def classify_failure(self, *_args: object) -> CoderDeviceLoginFailure:
            return CoderDeviceLoginFailure("process_failed", "failed")

    assert CoderLoginSessionManager._valid_adapter(ExplodingAdapter()) is False


@pytest.mark.asyncio
async def test_device_login_defensive_session_paths(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manager, registry = _manager()
    plugin = registry.get("codex")
    pending = asyncio.create_task(asyncio.Event().wait())
    terminal = coder_login._LoginSession(
        "T" * 43,
        "codex",
        _REFERENCE,
        _adapter(location="/tmp/terminal"),
        False,
        1,
        state="failed",
        terminal_monotonic=10,
    )
    unrelated = coder_login._LoginSession(
        "U" * 43,
        "codex",
        _REFERENCE,
        _adapter(location="/tmp/unrelated"),
        False,
        1,
        task=pending,
    )
    manager._sessions = {
        terminal.session_id: terminal,
        unrelated.session_id: unrelated,
    }
    plugin.adapter = _adapter(location="/tmp/new")

    async def blocked_auth(*_args: object, **_kwargs: object) -> dict[str, Any]:
        await asyncio.Event().wait()
        raise AssertionError("unreachable")

    monkeypatch.setattr(coder_login, "isolated_auth_probe", blocked_auth)
    created = await manager.start(
        "codex", expected_reference=_REFERENCE, replace_existing=False
    )
    assert created["state"] == "starting"
    assert (await manager.cancel("codex", "missing", expected_reference=_REFERENCE))[
        "state"
    ] == "not_found"

    no_task = coder_login._LoginSession(
        "N" * 43,
        "codex",
        _REFERENCE,
        _adapter(),
        False,
        1,
    )
    manager._sessions[no_task.session_id] = no_task
    assert (
        await manager.cancel("codex", no_task.session_id, expected_reference=_REFERENCE)
    )["state"] == "cancelled"

    completed_task = asyncio.create_task(asyncio.sleep(0))
    await completed_task
    done_without_transition = coder_login._LoginSession(
        "D" * 43,
        "codex",
        _REFERENCE,
        _adapter(),
        False,
        1,
        managed=_Managed(),  # type: ignore[arg-type]
        task=completed_task,  # type: ignore[arg-type]
    )
    manager._sessions[done_without_transition.session_id] = done_without_transition
    assert (
        await manager.cancel(
            "codex",
            done_without_transition.session_id,
            expected_reference=_REFERENCE,
        )
    )["state"] == "cancelled"

    for task in (pending, manager._sessions[created["session_id"]].task):
        assert task is not None
        task.cancel()
    await asyncio.gather(
        pending,
        manager._sessions[created["session_id"]].task,
        return_exceptions=True,
    )


@pytest.mark.asyncio
async def test_device_login_start_races_and_unexpected_failures(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manager, _ = _manager()

    async def auth_probe(*_args: object, **_kwargs: object) -> dict[str, Any]:
        return _auth(False)

    monkeypatch.setattr(coder_login, "isolated_auth_probe", auth_probe)
    session = coder_login._LoginSession(
        "R" * 43,
        "codex",
        _REFERENCE,
        _adapter(),
        False,
        1,
        cancel_requested=True,
    )
    await manager._start_session(session)
    assert session.state == "cancelled"

    for quiescent, expected in ((True, "cancelled"), (False, "cleanup_failed")):
        managed = _Managed(quiescent=quiescent)
        raced = coder_login._LoginSession(
            ("Q" if quiescent else "F") * 43,
            "codex",
            _REFERENCE,
            _adapter(),
            False,
            1,
        )

        async def launch(*_args: object, **_kwargs: object) -> _Managed:
            raced.cancel_requested = True
            return managed

        monkeypatch.setattr(coder_login, "launch_process", launch)
        await manager._start_session(raced)
        assert raced.state == expected

    async def broken_auth(*_args: object, **_kwargs: object) -> dict[str, Any]:
        raise RuntimeError("auth-secret")

    monkeypatch.setattr(coder_login, "isolated_auth_probe", broken_auth)
    failed = coder_login._LoginSession(
        "E" * 43,
        "codex",
        _REFERENCE,
        _adapter(),
        False,
        1,
    )
    await manager._start_session(failed)
    assert failed.failure_reason == "auth_status_unavailable"
    assert "auth-secret" not in failed.detail


@pytest.mark.asyncio
async def test_device_login_cancelled_task_with_owned_process(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manager, _ = _manager()
    managed = _Managed()
    entered = asyncio.Event()

    async def auth_probe(*_args: object, **_kwargs: object) -> dict[str, Any]:
        return _auth(False)

    async def launch(*_args: object, **_kwargs: object) -> _Managed:
        return managed

    async def run(*_args: object, **_kwargs: object) -> ProcessRunResult:
        entered.set()
        await asyncio.Event().wait()
        raise AssertionError("unreachable")

    monkeypatch.setattr(coder_login, "isolated_auth_probe", auth_probe)
    monkeypatch.setattr(coder_login, "launch_process", launch)
    monkeypatch.setattr(coder_login, "run_supervised_process", run)
    session = coder_login._LoginSession(
        "C" * 43,
        "codex",
        _REFERENCE,
        _adapter(),
        False,
        1,
    )
    task = asyncio.create_task(manager._start_session(session))
    await entered.wait()
    task.cancel()
    await task

    assert session.state == "cancelled"
    assert managed.cleanup_calls == 1


@pytest.mark.asyncio
async def test_device_login_supervision_cancel_and_unexpected_cleanup_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manager, _ = _manager()

    async def auth_probe(*_args: object, **_kwargs: object) -> dict[str, Any]:
        return _auth(False)

    monkeypatch.setattr(coder_login, "isolated_auth_probe", auth_probe)

    for cleanup in (_Managed(), _Managed(quiescent=False)):
        session = coder_login._LoginSession(
            ("S" if cleanup.quiescent else "X") * 43,
            "codex",
            _REFERENCE,
            _adapter(),
            False,
            1,
            managed=cleanup,  # type: ignore[arg-type]
        )

        async def supervision_failure(
            *_args: object, **_kwargs: object
        ) -> ProcessRunResult:
            session.cancel_requested = True
            raise ProcessSupervisionError("x", stdout=b"", stderr=b"")

        monkeypatch.setattr(coder_login, "run_supervised_process", supervision_failure)
        await manager._run_owned_session(session)
        assert session.state == ("cancelled" if cleanup.quiescent else "cleanup_failed")

    cleanup_error = _Managed(cleanup_error=RuntimeError("cleanup-secret"))
    session = coder_login._LoginSession(
        "Y" * 43,
        "codex",
        _REFERENCE,
        _adapter(),
        False,
        1,
        managed=cleanup_error,  # type: ignore[arg-type]
    )

    async def unexpected(*_args: object, **_kwargs: object) -> ProcessRunResult:
        raise ValueError("unexpected-secret")

    async def launch_cleanup_error(*_args: object, **_kwargs: object) -> _Managed:
        return cleanup_error

    monkeypatch.setattr(coder_login, "run_supervised_process", unexpected)
    monkeypatch.setattr(coder_login, "launch_process", launch_cleanup_error)
    await manager._start_session(session)
    assert session.state == "cleanup_failed"
    assert (
        await manager._cleanup(
            coder_login._LoginSession(
                "Z" * 43,
                "codex",
                _REFERENCE,
                _adapter(),
                False,
                1,
            )
        )
        is True
    )


@pytest.mark.asyncio
async def test_device_login_parser_defenses_and_abort_failure() -> None:
    class ParserAdapter:
        command = ("fake",)
        environment: dict[str, str] = {}
        working_directory = "/tmp"
        credential_location = "/tmp/parser"
        application_timeout_seconds = 1
        replacement_warning = "warning"

        def __init__(self) -> None:
            self.prompt: object = None

        def parse_progress(self, *_args: object) -> object:
            return self.prompt

        def classify_failure(self, *_args: object) -> CoderDeviceLoginFailure:
            raise RuntimeError("classifier-secret")

    adapter = ParserAdapter()
    manager, _ = _manager(adapter)
    session = coder_login._LoginSession(
        "P" * 43,
        "codex",
        _REFERENCE,
        adapter,  # type: ignore[arg-type]
        False,
        1,
        managed=_Managed(quiescent=False),  # type: ignore[arg-type]
    )
    adapter.prompt = CoderDeviceLoginPrompt("https://example.com/device", "FIRST", 60)
    manager._observe_chunk(
        session, session.stdout, b"x" * (coder_login._OUTPUT_BUFFER_BYTES + 1)
    )
    assert len(session.stdout) == coder_login._OUTPUT_BUFFER_BYTES
    assert session.state == "waiting_for_user"

    adapter.prompt = CoderDeviceLoginPrompt("https://example.com/device", "SECOND", 60)
    manager._observe_chunk(session, session.stdout, b"more")
    assert session.parse_failed is True
    assert session.abort_task is not None
    await session.abort_task
    assert session.state == "cleanup_failed"

    terminal = replace(session, state="failed", parse_failed=False)
    manager._observe_chunk(terminal, terminal.stdout, b"ignored")
    assert terminal.stdout == session.stdout

    bad_prompt = replace(session, parse_failed=False, state="starting")
    bad_prompt.abort_task = None
    bad_prompt.managed = None
    adapter.prompt = object()
    manager._observe_chunk(bad_prompt, bad_prompt.stdout, b"bad")
    assert bad_prompt.parse_failed is True

    classifier_session = coder_login._LoginSession(
        "K" * 43,
        "codex",
        _REFERENCE,
        adapter,  # type: ignore[arg-type]
        False,
        1,
        managed=_Managed(),  # type: ignore[arg-type]
        verification_url="https://example.com/device",
    )

    async def failed_run(*_args: object, **_kwargs: object) -> ProcessRunResult:
        return ProcessRunResult(7, b"", b"")

    monkeypatch = pytest.MonkeyPatch()
    monkeypatch.setattr(coder_login, "run_supervised_process", failed_run)
    try:
        await manager._run_owned_session(classifier_session)
    finally:
        monkeypatch.undo()
    assert classifier_session.failure_reason == "process_failed"


def test_device_login_repeated_prompt_preserves_original_expiration() -> None:
    now = [1_000.0]
    adapter = type(
        "RepeatedPromptAdapter",
        (),
        {
            "parse_progress": lambda *_args: CoderDeviceLoginPrompt(
                "https://example.com/device",
                "ABCD-EFGH",
                900,
            )
        },
    )()
    manager, _ = _manager(adapter, wall_time=lambda: now[0])
    session = coder_login._LoginSession(
        "E" * 43,
        "codex",
        _REFERENCE,
        adapter,  # type: ignore[arg-type]
        False,
        1,
    )

    manager._observe_chunk(session, session.stdout, b"prompt")
    assert session.expires_at == 1_900.0

    now[0] = 1_600.0
    manager._observe_chunk(session, session.stderr, b"polling")

    assert session.state == "waiting_for_user"
    assert session.expires_at == 1_900.0


@pytest.mark.asyncio
async def test_device_login_invalid_classifier_result_is_normalized(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class InvalidClassifier(CodexDeviceLoginAdapter):
        def classify_failure(  # type: ignore[override]
            self, stdout: str, stderr: str, returncode: int
        ) -> object:
            return object()

    adapter = InvalidClassifier(
        command=("fake",),
        environment={},
        working_directory="/tmp",
        credential_location="/tmp/invalid-classifier",
    )
    manager, _ = _manager(adapter)
    session = coder_login._LoginSession(
        "I" * 43,
        "codex",
        _REFERENCE,
        adapter,
        False,
        1,
        managed=_Managed(),  # type: ignore[arg-type]
        verification_url="https://auth.openai.com/codex/device",
    )

    async def failed_run(*_args: object, **_kwargs: object) -> ProcessRunResult:
        return ProcessRunResult(3, b"", b"")

    monkeypatch.setattr(coder_login, "run_supervised_process", failed_run)
    await manager._run_owned_session(session)
    assert session.failure_reason == "process_failed"


@pytest.mark.asyncio
async def test_device_login_shutdown_retains_unconfirmed_cleanup() -> None:
    manager, _ = _manager()
    session = coder_login._LoginSession(
        "H" * 43,
        "codex",
        _REFERENCE,
        _adapter(),
        False,
        1,
        state="waiting_for_user",
        managed=_Managed(quiescent=False),  # type: ignore[arg-type]
    )
    manager._sessions[session.session_id] = session

    await manager.shutdown()

    assert session.state == "cleanup_failed"
    assert session.managed is not None


@pytest.mark.asyncio
async def test_device_login_cancellation_wins_during_success_auth_probe(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manager, _ = _manager()
    session = coder_login._LoginSession(
        "P" * 43,
        "codex",
        _REFERENCE,
        _adapter(),
        False,
        1,
        state="waiting_for_user",
        verification_url="https://auth.openai.com/codex/device",
        user_code="ABCD-EFGH",
        managed=_Managed(),  # type: ignore[arg-type]
    )

    async def successful_run(*_args: object, **_kwargs: object) -> ProcessRunResult:
        return ProcessRunResult(0, b"", b"")

    async def auth_probe(*_args: object, **_kwargs: object) -> dict[str, Any]:
        session.cancel_requested = True
        return _auth(True)

    monkeypatch.setattr(coder_login, "run_supervised_process", successful_run)
    monkeypatch.setattr(coder_login, "isolated_auth_probe", auth_probe)

    await manager._run_owned_session(session)

    assert session.state == "cancelled"
    assert session.auth_status is None


@pytest.mark.asyncio
async def test_device_login_cancel_bounds_task_settlement(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    reservations = CoderCredentialReservations()
    manager, _ = _manager(reservations=reservations)
    pending = asyncio.create_task(asyncio.Event().wait())
    session = coder_login._LoginSession(
        "B" * 43,
        "codex",
        _REFERENCE,
        _adapter(),
        False,
        1,
        managed=_Managed(),  # type: ignore[arg-type]
        task=pending,  # type: ignore[arg-type]
        reservation_held=True,
    )
    manager._sessions[session.session_id] = session
    assert reservations.reserve_login(session.adapter.credential_location)

    async def timeout(
        tasks: set[asyncio.Task[None]], *, timeout: float
    ) -> tuple[set[asyncio.Task[None]], set[asyncio.Task[None]]]:
        assert tasks == {pending}
        assert timeout == 3
        return set(), tasks

    monkeypatch.setattr(coder_login.asyncio, "wait", timeout)
    result = await manager.cancel(
        "codex", session.session_id, expected_reference=_REFERENCE
    )

    assert result["state"] == "cleanup_failed"
    assert result["cleanup_confirmed"] is False
    assert "session task did not stop" in result["detail"]
    assert reservations.reserve_coder(session.adapter.credential_location) is False
    await asyncio.gather(pending, return_exceptions=True)
