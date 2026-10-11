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
    CoderBrowserLoginFailure,
    CoderBrowserLoginProgress,
    CoderDeviceLoginFailure,
    CoderDeviceLoginPrompt,
    CoderRegistry,
    coder_auth_payload,
)
from src.coders.claude import ClaudePlugin
from src.coders.claude_login import ClaudeBrowserLoginAdapter
from src.coders.codex import CodexDeviceLoginAdapter
from src.config import AppConfig
from src.process_supervisor import (
    CleanupResult,
    CleanupStatus,
    ProcessLaunchCleanupError,
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

    def build_credential_environment(
        self,
        *,
        config: AppConfig,
        credential_location: str,
    ) -> dict[str, str]:
        del config
        environment = dict(getattr(self.adapter, "environment", {}))
        environment["CODEX_HOME"] = credential_location
        return environment


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
        environment={
            "HOME": str(tmp_path / "captured-home"),
            "CODEX_HOME": str(tmp_path / ".codex"),
        },
        working_directory=str(tmp_path),
    )
    manager, _ = _manager(adapter)
    probes = iter((_auth(False), _auth(True)))
    probe_environments: list[dict[str, str]] = []

    async def auth_probe(*_args: object, **kwargs: object) -> dict[str, Any]:
        probe_environments.append(kwargs["env"])  # type: ignore[arg-type]
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
    assert probe_environments == [adapter.environment, adapter.environment]
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
    reservations = CoderCredentialReservations()
    manager, _ = _manager(reservations=reservations)

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
    assert result["cleanup_confirmed"] is True
    assert "launch-secret" not in json.dumps(result)
    assert reservations.reserve_coder("/tmp/fake-codex-home") is True
    reservations.release_coder("/tmp/fake-codex-home")


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["cancel", "shutdown"])
async def test_device_login_failed_launch_retains_until_reconciled(
    monkeypatch: pytest.MonkeyPatch,
    operation: str,
) -> None:
    reservations = CoderCredentialReservations()
    manager, _ = _manager(reservations=reservations)
    managed = _Managed(quiescent=False, reconcile_quiescent=False)

    async def auth_probe(*_args: object, **_kwargs: object) -> dict[str, Any]:
        return _auth(False)

    async def launch(*_args: object, **_kwargs: object) -> object:
        raise ProcessLaunchCleanupError(
            "launch-cleanup-secret",
            managed=managed,  # type: ignore[arg-type]
            cleanup_result=CleanupResult(
                CleanupStatus.FAILED,
                None,
                True,
                True,
                "ownership-secret",
            ),
        )

    monkeypatch.setattr(coder_login, "isolated_auth_probe", auth_probe)
    monkeypatch.setattr(coder_login, "launch_process", launch)
    started = await manager.start(
        "codex", expected_reference=_REFERENCE, replace_existing=False
    )
    failed = await _wait_for_state(
        manager, started["session_id"], "cleanup_failed"
    )

    assert failed["cleanup_confirmed"] is False
    assert "secret" not in json.dumps(failed)
    assert reservations.reserve_coder("/tmp/fake-codex-home") is False
    inspected = await manager.inspect(
        "codex", started["session_id"], expected_reference=_REFERENCE
    )
    assert inspected["cleanup_confirmed"] is False

    if operation == "cancel":
        still_unconfirmed = await manager.cancel(
            "codex", started["session_id"], expected_reference=_REFERENCE
        )
    else:
        await manager.shutdown()
        still_unconfirmed = await manager.inspect(
            "codex", started["session_id"], expected_reference=_REFERENCE
        )
    assert still_unconfirmed["cleanup_confirmed"] is False
    assert reservations.reserve_coder("/tmp/fake-codex-home") is False

    managed.reconcile_quiescent = True
    if operation == "cancel":
        reconciled = await manager.cancel(
            "codex", started["session_id"], expected_reference=_REFERENCE
        )
    else:
        await manager.shutdown()
        reconciled = await manager.inspect(
            "codex", started["session_id"], expected_reference=_REFERENCE
        )
    assert reconciled["state"] == "cancelled"
    assert reconciled["cleanup_confirmed"] is True
    assert managed.cleanup_calls == 0
    assert managed.reconcile_calls == 2
    assert reservations.reserve_coder("/tmp/fake-codex-home") is True
    reservations.release_coder("/tmp/fake-codex-home")


@pytest.mark.asyncio
async def test_device_login_failed_launch_without_handle_stays_unconfirmed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    reservations = CoderCredentialReservations()
    manager, _ = _manager(reservations=reservations)

    async def auth_probe(*_args: object, **_kwargs: object) -> dict[str, Any]:
        return _auth(False)

    async def launch(*_args: object, **_kwargs: object) -> object:
        raise ProcessLaunchCleanupError(
            "launch-cleanup-secret",
            managed=None,
            cleanup_result=None,
        )

    monkeypatch.setattr(coder_login, "isolated_auth_probe", auth_probe)
    monkeypatch.setattr(coder_login, "launch_process", launch)
    started = await manager.start(
        "codex", expected_reference=_REFERENCE, replace_existing=False
    )
    failed = await _wait_for_state(
        manager, started["session_id"], "cleanup_failed"
    )

    assert failed["cleanup_confirmed"] is False
    assert reservations.reserve_coder("/tmp/fake-codex-home") is False
    cancelled = await manager.cancel(
        "codex", started["session_id"], expected_reference=_REFERENCE
    )
    assert cancelled["cleanup_confirmed"] is False
    await manager.shutdown()
    after_shutdown = await manager.inspect(
        "codex", started["session_id"], expected_reference=_REFERENCE
    )
    assert after_shutdown["cleanup_confirmed"] is False
    assert reservations.reserve_coder("/tmp/fake-codex-home") is False


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

    invalid_warning_manager, _ = _manager(
        adapter=replace(_adapter(), replacement_warning="unsafe\nwarning")
    )
    assert (
        await invalid_warning_manager.start(
            "codex", expected_reference=_REFERENCE, replace_existing=False
        )
    )["state"] == "unsupported"
    assert invalid_warning_manager._sessions == {}

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
    assert (
        CoderLoginSessionManager._valid_adapter(
            replace(good, replacement_warning="unsafe\nwarning")
        )
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
@pytest.mark.parametrize("saved_credentials", [False, None])
async def test_device_login_requires_confirmed_saved_credentials_after_success(
    monkeypatch: pytest.MonkeyPatch,
    saved_credentials: bool | None,
) -> None:
    manager, _ = _manager()
    session = coder_login._LoginSession(
        "V" * 43,
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
        return _auth(saved_credentials)

    monkeypatch.setattr(coder_login, "run_supervised_process", successful_run)
    monkeypatch.setattr(coder_login, "isolated_auth_probe", auth_probe)

    await manager._run_owned_session(session)

    assert session.state == "failed"
    assert session.failure_reason == "auth_status_unavailable"
    assert session.cleanup_confirmed is True
    assert session.auth_status is not None
    assert session.auth_status["saved_credentials_present"] is saved_credentials
    assert "Saved credentials found" not in session.detail


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


@pytest.fixture
def browser_preparation():
    plugin = ClaudePlugin()
    registry = CoderRegistry()
    registry.register(plugin, reference="src.coders.claude:ClaudePlugin")
    reservations = CoderCredentialReservations()
    location = "/tmp/captured-claude"
    assert reservations.reserve_login(location)
    manager = CoderLoginSessionManager(
        registry, config_path="/cfg/config.yml", credential_reservations=reservations,
    )
    kwargs = dict(
        plugin=plugin,
        expected_reference=registry.reference_for("claude"),
        credential_location=location,
        environment={"CLAUDE_CONFIG_DIR": location, "CONTEXT": "private"},
        reservation_held=True,
        replace_existing=False,
    )
    auth = plugin._auth_status(
        status="error", detail="No saved credentials", cli_available=True,
        cli_version="2.1.126", saved_credentials_present=False,
        failure_reason="credentials_missing",
    )
    return manager, registry, reservations, kwargs, auth


@pytest.mark.parametrize("saved,consent", [(False, False), (True, True), (None, True)])
async def test_browser_preparation_captures_context_and_retains_reservation(
    monkeypatch, tmp_path, browser_preparation, saved, consent,
):
    manager, registry, reservations, kwargs, auth = browser_preparation
    auth.update(status="ok", failure_reason=None, saved_credentials_present=saved)
    kwargs["replace_existing"] = consent
    config = tmp_path / "config.yml"
    config.write_text("auth: {claude_config_dir: /tmp/before}")
    manager._config_path = str(config)
    original_environment = dict(kwargs["environment"])
    location = kwargs["credential_location"]
    entered, release = asyncio.Event(), asyncio.Event()

    async def probe(*args, **probe_kwargs):
        assert args == ("claude", kwargs["expected_reference"], "Claude Code")
        assert probe_kwargs["env"] == original_environment
        assert probe_kwargs["config_path"] == str(config)
        assert probe_kwargs["timeout"] == 5
        entered.set()
        await release.wait()
        return auth

    monkeypatch.setattr(coder_login, "isolated_auth_probe", probe)
    task = asyncio.create_task(manager._prepare_browser_login("claude", **kwargs))
    await entered.wait()
    assert not reservations.reserve_coder(location)
    assert not reservations.reserve_login(location)
    config.write_text("auth: {claude_config_dir: /tmp/after}")
    kwargs["environment"]["CONTEXT"] = "changed"
    release.set()
    adapter = await task
    assert isinstance(adapter, ClaudeBrowserLoginAdapter)
    assert dict(adapter.environment) == original_environment
    assert adapter.credential_location == location
    assert adapter.working_directory == str(tmp_path)
    assert adapter.observed_cli_version == "2.1.126"
    assert "failed or cancelled replacement" in adapter.replacement_warning
    assert reservations.login_active(location)
    assert reservations.credential_version(location) == 0
    assert manager._sessions == {}
    assert "private" not in repr(adapter)
    assert auth["service_access_verified"] is None


@pytest.mark.parametrize("updates,consent,reason", [
    ({"saved_credentials_present": True}, False, "replacement_required"),
    ({"saved_credentials_present": None}, False, "replacement_required"),
    ({"cli_available": False}, True, "cli_missing"),
    ({"cli_available": None}, True, "auth_status_unavailable"),
    ({"failure_reason": "probe_failed"}, True, "auth_status_unavailable"),
    ({"failure_reason": "probe_timeout"}, True, "auth_status_unavailable"),
    ({"failure_reason": "unrecognized_output"}, True, "auth_status_unavailable"),
    ({"failure_reason": None}, True, "auth_status_unavailable"),
    ({"cli_version": None}, True, "unsupported_cli_version"),
    ({"cli_version": ""}, True, "unsupported_cli_version"),
    ({"cli_version": "secret"}, True, "unsupported_cli_version"),
    ({"cli_version": "2.1.126-beta"}, True, "unsupported_cli_version"),
    ({"cli_version": "2.1.125"}, True, "startup_failed"),
    ({"capabilities": {}}, True, "unsupported_cli_version"),
    ({"cli_version": 123}, True, "auth_status_unavailable"),
    ({"status": "invalid"}, True, "auth_status_unavailable"),
])
async def test_browser_preparation_preflight_failures(
    monkeypatch, browser_preparation, updates, consent, reason,
):
    manager, _, reservations, kwargs, auth = browser_preparation
    auth.update(updates)
    kwargs["replace_existing"] = consent

    async def probe(*_args, **_kwargs):
        return auth

    monkeypatch.setattr(coder_login, "isolated_auth_probe", probe)
    result = await manager._prepare_browser_login("claude", **kwargs)
    assert isinstance(result, CoderBrowserLoginFailure)
    assert result.reason == reason
    assert "secret" not in result.detail
    assert reservations.login_active(kwargs["credential_location"])
    assert reservations.credential_version(kwargs["credential_location"]) == 0


@pytest.mark.parametrize("change", ["reference", "plugin", "removed", "factory", "unowned", "inactive"])
async def test_browser_preparation_rejects_invalid_entry(
    monkeypatch, browser_preparation, change,
):
    manager, registry, reservations, kwargs, _ = browser_preparation
    if change == "reference":
        registry.register(kwargs["plugin"], reference="changed")
    elif change == "plugin":
        registry.register(ClaudePlugin(), reference=kwargs["expected_reference"])
    elif change == "removed":
        registry._plugins.clear()
    elif change == "factory":
        monkeypatch.setattr(kwargs["plugin"], "create_browser_login", None)
    elif change == "unowned":
        kwargs["reservation_held"] = False  # Another operation still owns it.
    else:
        reservations.release_login(kwargs["credential_location"])
    before = reservations.credential_version(kwargs["credential_location"])
    result = await manager._prepare_browser_login("claude", **kwargs)
    assert isinstance(result, CoderBrowserLoginFailure)
    assert result.reason == (
        "unsupported" if change == "factory" else
        "credential_in_use" if change in {"unowned", "inactive"} else
        "session_plugin_mismatch"
    )
    assert reservations.credential_version(kwargs["credential_location"]) == before
    assert reservations.login_active(kwargs["credential_location"]) == (change != "inactive")


@pytest.mark.parametrize("stage", ["probe", "factory"])
async def test_browser_preparation_rechecks_identity(monkeypatch, browser_preparation, stage):
    manager, registry, reservations, kwargs, auth = browser_preparation
    plugin = kwargs["plugin"]
    original_factory = plugin.create_browser_login

    async def probe(*_args, **_kwargs):
        if stage == "probe":
            registry.register(plugin, reference="changed")
        return auth

    def factory(**factory_kwargs):
        registry.register(ClaudePlugin(), reference=kwargs["expected_reference"])
        return original_factory(**factory_kwargs)

    monkeypatch.setattr(coder_login, "isolated_auth_probe", probe)
    if stage == "factory":
        monkeypatch.setattr(plugin, "create_browser_login", factory)
    result = await manager._prepare_browser_login("claude", **kwargs)
    assert result.reason == "session_plugin_mismatch"
    assert reservations.login_active(kwargs["credential_location"])


@pytest.mark.parametrize("mutation", [
    "environment", "location", "version", "pty", "command", "warning", "parser", "object", "raise",
])
async def test_browser_preparation_validates_adapter(monkeypatch, browser_preparation, mutation):
    manager, _, reservations, kwargs, auth = browser_preparation
    original_factory = kwargs["plugin"].create_browser_login

    async def probe(*_args, **_kwargs):
        return auth

    def factory(**factory_kwargs):
        if mutation == "raise":
            raise RuntimeError("private")
        if mutation == "object":
            return object()
        adapter = original_factory(**factory_kwargs)
        key, value = {
            "environment": ("environment", {}),
            "location": ("credential_location", "/tmp/other"),
            "version": ("observed_cli_version", "2.1.127"),
            "pty": ("requires_pty", False),
            "command": ("command", ()),
            "warning": ("replacement_warning", ""),
            "parser": ("parse_progress", None),
        }[mutation]
        object.__setattr__(adapter, key, value)
        return adapter

    monkeypatch.setattr(coder_login, "isolated_auth_probe", probe)
    monkeypatch.setattr(kwargs["plugin"], "create_browser_login", factory)
    result = await manager._prepare_browser_login("claude", **kwargs)
    assert isinstance(result, CoderBrowserLoginFailure)
    assert result.reason == ("startup_failed" if mutation == "raise" else "unsupported")
    assert "private" not in result.detail
    assert reservations.login_active(kwargs["credential_location"])


async def test_browser_preparation_probe_exception_and_cancellation(monkeypatch, browser_preparation):
    manager, _, reservations, kwargs, _ = browser_preparation

    async def broken_probe(*_args, **_kwargs):
        raise RuntimeError("private")

    monkeypatch.setattr(coder_login, "isolated_auth_probe", broken_probe)
    result = await manager._prepare_browser_login("claude", **kwargs)
    assert result.reason == "auth_status_unavailable"
    assert "private" not in result.detail
    entered, cleanup, stopped = asyncio.Event(), asyncio.Event(), asyncio.Event()

    async def cancelling_probe(*_args, **_kwargs):
        entered.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            await cleanup.wait()
            stopped.set()
            raise

    monkeypatch.setattr(coder_login, "isolated_auth_probe", cancelling_probe)
    task = asyncio.create_task(manager._prepare_browser_login("claude", **kwargs))
    await entered.wait()
    task.cancel()
    await asyncio.sleep(0)
    assert not task.done()
    assert not reservations.reserve_coder(kwargs["credential_location"])
    cleanup.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert stopped.is_set()
    assert reservations.login_active(kwargs["credential_location"])
    assert reservations.credential_version(kwargs["credential_location"]) == 0


_BROWSER_URL = (
    "https://claude.com/cai/oauth/authorize?client_id=test&code=true&"
    "code_challenge=challenge&code_challenge_method=S256&redirect_uri=https%3A%2F%2Fexample.com&"
    "response_type=code&scope=user&state=private-state"
)


async def _prepared_browser(monkeypatch, browser_preparation, *, timeout=5):
    manager, _, reservations, kwargs, auth = browser_preparation

    async def probe(*_args, **_kwargs):
        return auth

    monkeypatch.setattr(coder_login, "isolated_auth_probe", probe)
    adapter = await manager._prepare_browser_login("claude", **kwargs)
    assert isinstance(adapter, ClaudeBrowserLoginAdapter)
    object.__setattr__(adapter, "application_timeout_seconds", timeout)
    session = coder_login._LoginSession(
        "B" * 43, "claude", kwargs["expected_reference"], adapter, False, manager._monotonic(),
        reservation_held=True,
    )
    manager._sessions[session.session_id] = session
    return manager, reservations, session


async def _browser_snapshot(manager, session):
    return await manager.inspect("claude", session.session_id, expected_reference=session.reference)


@pytest.mark.parametrize("action", ["success", "timeout", "cancel", "shutdown"])
async def test_browser_real_pty_fragmented_progress(
    monkeypatch, tmp_path, browser_preparation, action,
):
    manager, reservations, session = await _prepared_browser(
        monkeypatch, browser_preparation, timeout=0.5 if action == "timeout" else 5,
    )
    user_seen, code_seen = asyncio.Event(), asyncio.Event()
    original_observe = manager._observe_browser_output

    def observe(current):
        original_observe(current)
        if current.state == "waiting_for_user":
            user_seen.set()
        if current.state == "waiting_for_code":
            code_seen.set()

    monkeypatch.setattr(manager, "_observe_browser_output", observe)
    script = (
        "import os,time,pathlib\n"
        f"url={_BROWSER_URL!r}\n"
        "os.write(1,url[:50].encode())\n"
        "os.write(2,(url[50:]+'\\n').encode())\n"
        f"gate=pathlib.Path({str(tmp_path / 'gate')!r})\n"
        "while not gate.exists(): time.sleep(.005)\n"
        "os.write(2,b'Paste code here if ')\n"
        "os.write(1,b'prompted >')\n"
        "received=b''\n"
        "while not received.endswith(b'\\n'): received+=os.read(0,1)\n"
        "assert received == '  opaque é + / =  \\n'.encode()\n"
        f"done=pathlib.Path({str(tmp_path / 'done')!r})\n"
        "while not done.exists(): time.sleep(.005)\n"
    )
    object.__setattr__(session.adapter, "command", (sys.executable, "-c", script))
    object.__setattr__(session.adapter, "working_directory", str(tmp_path))
    probe_calls = []

    async def fresh_probe(*_args, **kwargs):
        probe_calls.append(kwargs)
        assert reservations.login_active(session.adapter.credential_location)
        return coder_auth_payload(CoderAuthStatus(
            status="ok", detail="Saved authentication found", cli_available=True,
            saved_credentials_present=True,
        ))

    monkeypatch.setattr(coder_login, "isolated_auth_probe", fresh_probe)
    manager._execute_browser_session(session)
    await asyncio.wait_for(user_seen.wait(), 3)
    initial = await _browser_snapshot(manager, session)
    assert initial["authorization_url"] == _BROWSER_URL
    assert initial["state"] == "waiting_for_user"
    managed, channel = session.managed, session.pty_channel
    (tmp_path / "gate").touch()
    await asyncio.wait_for(code_seen.wait(), 3)
    ready = await _browser_snapshot(manager, session)
    assert ready["state"] == "waiting_for_code"
    assert ready["application_deadline"] == initial["application_deadline"]
    if action == "success":
        await manager._submit_browser_code(
            "claude", session.session_id, expected_reference=session.reference,
            authorization_code="  opaque é + / =  ",
        )
        (tmp_path / "done").touch()
    elif action == "cancel":
        await manager.cancel("claude", session.session_id, expected_reference=session.reference)
    elif action == "shutdown":
        await manager.shutdown()
    await asyncio.wait_for(session.task, 5)
    final = await _browser_snapshot(manager, session)
    assert final["state"] == {"success": "succeeded", "timeout": "timed_out"}.get(action, "cancelled")
    assert final["cleanup_confirmed"] is True
    assert final["authorization_url"] is None
    assert final["application_deadline"] == initial["application_deadline"]
    assert managed.process.returncode is not None
    assert channel.closed
    assert session.managed is None and session.pty_channel is None
    assert not reservations.login_active(session.adapter.credential_location)
    assert len(probe_calls) == (1 if action == "success" else 0)
    if probe_calls:
        assert probe_calls[0]["env"] == dict(session.adapter.environment)
        assert final["auth_status"]["service_access_verified"] is None


class _Channel:
    closed = False

    def close(self):
        self.closed = True


@pytest.mark.parametrize("case,reason", [
    ("malformed", "malformed_output"), ("conflicting", "malformed_output"),
    ("empty", "malformed_output"), ("nonzero", "process_failed"),
    ("classifier_exception", "process_failed"), ("bad_progress", "malformed_output"),
    ("bad_url", "malformed_output"), ("regression", "malformed_output"),
    ("invalid_code", "invalid_code"), ("probe_error", "auth_status_unavailable"),
    ("saved_missing", "auth_status_unavailable"), ("probe_exception", "auth_status_unavailable"),
    ("saved_but_error", "auth_status_unavailable"),
])
async def test_browser_execution_sanitized_failures(monkeypatch, browser_preparation, case, reason):
    manager, reservations, session = await _prepared_browser(monkeypatch, browser_preparation)
    managed, channel = _Managed(), _Channel()

    async def launch(*args, **kwargs):
        assert args == session.adapter.command
        assert kwargs == dict(cwd=session.adapter.working_directory, env=dict(session.adapter.environment))
        return managed, channel

    async def run(_managed, **kwargs):
        assert kwargs["pty_channel"] is channel
        assert kwargs["stderr_chunk_callback"] is None
        callback = kwargs["stdout_chunk_callback"]
        callback(b"Welcome\n")
        if case not in {"empty", "bad_url"}:
            callback((_BROWSER_URL + "\nPaste code here if prompted >").encode())
        if case == "malformed":
            callback(b"https://evil.example/private-secret\n")
        if case == "conflicting":
            callback((_BROWSER_URL.replace("private-state", "different") + "\n").encode())
        if case == "bad_progress":
            object.__setattr__(session.adapter, "parse_progress", lambda _output: object())
            callback(b"bad")
        if case == "bad_url":
            object.__setattr__(session.adapter, "parse_progress", lambda _output:
                               CoderBrowserLoginProgress("http://private-secret", True))
            callback(b"bad")
        if case == "regression":
            callback(b"Paste code here if prompted >")
            object.__setattr__(session.adapter, "parse_progress", lambda _output:
                               CoderBrowserLoginProgress(_BROWSER_URL, False))
            callback(b"bad")
        if case == "classifier_exception":
            def classify(*_args):
                raise RuntimeError("private-secret")
            object.__setattr__(session.adapter, "classify_failure", classify)
        if case == "invalid_code":
            callback(b"Invalid code. Please make sure the full code was copied.")
        if case in {"probe_error", "saved_missing", "probe_exception", "saved_but_error"}:
            session.code_delivered = True
        return ProcessRunResult(
            1 if case in {"nonzero", "invalid_code", "classifier_exception"} else 0, b"", b"", False,
        )

    async def probe(*_args, **_kwargs):
        if case == "probe_exception":
            raise RuntimeError("private-secret")
        if case == "saved_but_error":
            return coder_auth_payload(CoderAuthStatus(
                status="error", detail="Probe failed", saved_credentials_present=True,
                failure_reason="probe_failed",
            ))
        return _auth(False if case == "saved_missing" else True) if case != "probe_error" else _auth(None)

    monkeypatch.setattr(coder_login, "launch_pty_process", launch)
    monkeypatch.setattr(coder_login, "run_supervised_process", run)
    monkeypatch.setattr(coder_login, "isolated_auth_probe", probe)
    manager._execute_browser_session(session)
    await session.task
    final = await _browser_snapshot(manager, session)
    assert final["state"] == "failed" and final["failure_reason"] == reason
    assert final["authorization_url"] is None
    assert "private-secret" not in json.dumps(final)
    assert "device" not in final["detail"].lower()
    assert channel.closed and not reservations.login_active(session.adapter.credential_location)


@pytest.mark.parametrize("failure", ["missing", "exception", "timeout", "cancel_cleanup", "unknown_cleanup"])
async def test_browser_launch_failure_and_cancellation(monkeypatch, browser_preparation, failure):
    manager, reservations, session = await _prepared_browser(
        monkeypatch, browser_preparation, timeout=.02 if failure == "timeout" else 5,
    )
    entered = asyncio.Event()
    managed = _Managed(quiescent=False, reconcile_quiescent=True)

    async def launch(*_args, **_kwargs):
        entered.set()
        if failure == "missing":
            raise FileNotFoundError("private-secret")
        if failure == "exception":
            raise RuntimeError("private-secret")
        try:
            await asyncio.Future()
        except asyncio.CancelledError:
            if failure == "timeout":
                raise
            raise ProcessLaunchCleanupError(
                "private-secret", managed=managed if failure == "cancel_cleanup" else None,
                cleanup_result=None,
            ) from None

    monkeypatch.setattr(coder_login, "launch_pty_process", launch)
    manager._execute_browser_session(session)
    await entered.wait()
    if failure in {"cancel_cleanup", "unknown_cleanup"}:
        await manager.cancel("claude", session.session_id, expected_reference=session.reference)
    await session.task
    final = await _browser_snapshot(manager, session)
    assert "private-secret" not in json.dumps(final)
    if failure in {"cancel_cleanup", "unknown_cleanup"}:
        assert final["state"] == "cleanup_failed"
        assert reservations.login_active(session.adapter.credential_location)
        assert not reservations.reserve_coder(session.adapter.credential_location)
        await manager.cancel("claude", session.session_id, expected_reference=session.reference)
        assert session.state == ("cancelled" if failure == "cancel_cleanup" else "cleanup_failed")
    else:
        assert final["state"] == ("timed_out" if failure == "timeout" else "failed")
        assert not reservations.login_active(session.adapter.credential_location)


@pytest.mark.parametrize("quiescent", [True, False])
async def test_browser_settles_execution_before_releasing(monkeypatch, browser_preparation, quiescent):
    manager, reservations, session = await _prepared_browser(monkeypatch, browser_preparation)
    managed, channel = _Managed(quiescent=quiescent, reconcile_quiescent=True), _Channel()
    entered, settle = asyncio.Event(), asyncio.Event()

    async def launch(*_args, **_kwargs):
        return managed, channel

    async def run(*_args, **_kwargs):
        entered.set()
        try:
            await asyncio.Future()
        except asyncio.CancelledError:
            assert reservations.login_active(session.adapter.credential_location)
            assert not channel.closed
            await settle.wait()
            raise

    monkeypatch.setattr(coder_login, "launch_pty_process", launch)
    monkeypatch.setattr(coder_login, "run_supervised_process", run)
    manager._execute_browser_session(session)
    await entered.wait()
    cancel = asyncio.create_task(manager.cancel("claude", session.session_id, expected_reference=session.reference))
    if quiescent:
        await asyncio.sleep(0)
        assert reservations.login_active(session.adapter.credential_location)
        assert not channel.closed
        settle.set()
    await cancel
    if not quiescent:
        assert session.state == "cleanup_failed"
        assert session.managed is managed and session.pty_channel is channel
        assert reservations.login_active(session.adapter.credential_location)
        settle.set()
        await manager.cancel("claude", session.session_id, expected_reference=session.reference)
    await session.task
    assert session.state == "cancelled"
    assert channel.closed and not reservations.login_active(session.adapter.credential_location)


@pytest.mark.parametrize("invalid", ["untracked", "reservation", "active", "adapter", "shutdown"])
async def test_browser_execution_requires_prepared_ownership(monkeypatch, browser_preparation, invalid):
    manager, reservations, session = await _prepared_browser(monkeypatch, browser_preparation)
    if invalid == "untracked":
        manager._sessions.clear()
    elif invalid == "reservation":
        session.reservation_held = False
    elif invalid == "active":
        session.state = "waiting_for_user"
    elif invalid == "adapter":
        session.adapter = _adapter()
    else:
        manager._shutting_down = True
    with pytest.raises(ValueError, match="tracked prepared"):
        manager._execute_browser_session(session)
    assert session.task is None
    assert reservations.login_active("/tmp/captured-claude")


@pytest.mark.parametrize("outcome", ["success", "cancel", "supervision", "timeout_cleanup"])
async def test_browser_launch_elapsed_deadline_and_cleanup(monkeypatch, browser_preparation, outcome):
    manager, reservations, session = await _prepared_browser(monkeypatch, browser_preparation)
    now = [10.0]
    manager._monotonic = lambda: now[0]
    manager._wall_time = lambda: 1000.0
    managed = _Managed(quiescent=outcome != "timeout_cleanup", reconcile_quiescent=True)
    channel = _Channel()

    async def launch(*_args, **_kwargs):
        now[0] += 3
        if outcome == "cancel":
            session.cancel_requested = True
        return managed, channel

    async def run(*_args, **kwargs):
        assert kwargs["timeout"] == 2
        assert session.application_deadline == 1005
        if outcome == "supervision":
            raise ProcessSupervisionError("private-secret", stdout=b"", stderr=b"")
        kwargs["stdout_chunk_callback"]((_BROWSER_URL + "\nPaste code here if prompted >").encode())
        session.code_delivered = True
        now[0] += 1
        return ProcessRunResult(0, b"", b"", False)

    async def probe(*_args, **_kwargs):
        assert session.state == "authorizing" and session.verification_url is None
        assert reservations.login_active(session.adapter.credential_location)
        if outcome == "timeout_cleanup":
            raise asyncio.TimeoutError
        return _auth(True)

    monkeypatch.setattr(coder_login, "launch_pty_process", launch)
    monkeypatch.setattr(coder_login, "run_supervised_process", run)
    monkeypatch.setattr(coder_login, "isolated_auth_probe", probe)
    manager._execute_browser_session(session)
    await session.task
    final = await _browser_snapshot(manager, session)
    assert final["state"] == {
        "success": "succeeded", "cancel": "cancelled", "supervision": "failed",
        "timeout_cleanup": "cleanup_failed",
    }[outcome]
    assert final["application_deadline"] == 1005
    if outcome == "timeout_cleanup":
        assert not channel.closed and session.managed is managed
        assert reservations.login_active(session.adapter.credential_location)
        await manager.cancel("claude", session.session_id, expected_reference=session.reference)
    assert channel.closed and not reservations.login_active(session.adapter.credential_location)


async def test_browser_shutdown_preserves_failed_launch(monkeypatch, browser_preparation):
    manager, reservations, session = await _prepared_browser(monkeypatch, browser_preparation)
    entered = asyncio.Event()

    async def launch(*_args, **_kwargs):
        entered.set()
        try:
            await asyncio.Future()
        except asyncio.CancelledError:
            raise ProcessLaunchCleanupError("private-secret", managed=None, cleanup_result=None) from None

    monkeypatch.setattr(coder_login, "launch_pty_process", launch)
    manager._execute_browser_session(session)
    await entered.wait()
    await manager.shutdown()
    assert session.state == "cleanup_failed"
    assert reservations.login_active(session.adapter.credential_location)
    assert (await _browser_snapshot(manager, session))["cleanup_confirmed"] is False


async def test_browser_retains_channel_until_stubborn_reader_settles(
    monkeypatch, tmp_path, browser_preparation,
):
    from src import process_supervisor

    manager, reservations, session = await _prepared_browser(monkeypatch, browser_preparation)
    object.__setattr__(session.adapter, "command", (sys.executable, "-c", "print('done', flush=True)"))
    object.__setattr__(session.adapter, "working_directory", str(tmp_path))
    real_launch = coder_login.launch_pty_process
    release, stalled = asyncio.Event(), asyncio.Event()
    handles = []

    async def launch(*args, **kwargs):
        managed, channel = await real_launch(*args, **kwargs)
        handles.append((managed, channel))
        real_read = channel.read

        async def read(size):
            chunk = await real_read(size)
            if not chunk:
                stalled.set()
                try:
                    await asyncio.Future()
                except asyncio.CancelledError:
                    await release.wait()
            return chunk

        monkeypatch.setattr(channel, "read", read)
        return managed, channel

    monkeypatch.setattr(coder_login, "launch_pty_process", launch)
    monkeypatch.setattr(process_supervisor, "_OUTPUT_DRAIN_TIMEOUT_SECONDS", .01)
    monkeypatch.setattr(process_supervisor, "_OUTPUT_CANCEL_TIMEOUT_SECONDS", .01)
    manager._execute_browser_session(session)
    try:
        await asyncio.wait_for(stalled.wait(), 3)
        await asyncio.wait_for(session.task, 3)
        managed, channel = handles[0]
        assert session.state == "cleanup_failed"
        assert not managed.execution_settled
        assert reservations.login_active(session.adapter.credential_location) and not channel.closed
        await manager.cancel("claude", session.session_id, expected_reference=session.reference)
        assert session.state == "cleanup_failed" and not channel.closed
        release.set()
        await asyncio.gather(*managed._execution_tasks, return_exceptions=True)
        await manager.cancel("claude", session.session_id, expected_reference=session.reference)
        assert session.state == "cancelled" and channel.closed
        assert not reservations.login_active(session.adapter.credential_location)
    finally:
        release.set()
        await manager.shutdown()


async def test_browser_replacement_requires_observed_code_prompt(monkeypatch, browser_preparation):
    manager, _, reservations, kwargs, auth = browser_preparation
    kwargs["replace_existing"] = True
    auth.update(status="ok", saved_credentials_present=True, failure_reason=None)
    manager, reservations, session = await _prepared_browser(monkeypatch, browser_preparation)
    session.replacement_requested = True
    managed, channel = _Managed(), _Channel()
    probes = []

    async def launch(*_args, **_kwargs):
        return managed, channel

    async def run(*_args, **kwargs):
        kwargs["stdout_chunk_callback"]((_BROWSER_URL + "\n").encode())
        assert session.state == "waiting_for_user"
        return ProcessRunResult(0, b"", b"", False)

    async def probe(*_args, **_kwargs):
        probes.append(True)
        return _auth(True)

    monkeypatch.setattr(coder_login, "launch_pty_process", launch)
    monkeypatch.setattr(coder_login, "run_supervised_process", run)
    monkeypatch.setattr(coder_login, "isolated_auth_probe", probe)
    manager._execute_browser_session(session)
    await session.task
    final = await _browser_snapshot(manager, session)
    assert final["state"] == "failed" and final["failure_reason"] == "malformed_output"
    assert probes == [] and channel.closed
    assert not reservations.login_active(session.adapter.credential_location)


async def _submittable_browser(monkeypatch, browser_preparation):
    from types import SimpleNamespace

    manager, reservations, session = await _prepared_browser(monkeypatch, browser_preparation)
    managed, channel = _Managed(), _Channel()
    managed.process = SimpleNamespace(returncode=None)
    channel.at_eof = False
    writes = []
    ready, completed = asyncio.Event(), asyncio.Event()

    async def write(data):
        writes.append(data)

    channel.write = write

    async def launch(*_args, **_kwargs):
        return managed, channel

    async def run(*_args, **kwargs):
        kwargs["stdout_chunk_callback"]((_BROWSER_URL + "\nPaste code here if prompted >").encode())
        ready.set()
        await completed.wait()
        return ProcessRunResult(0, b"", b"", False)

    async def probe(*_args, **_kwargs):
        assert session.code_delivered
        assert reservations.login_active(session.adapter.credential_location)
        return _auth(True)

    monkeypatch.setattr(coder_login, "launch_pty_process", launch)
    monkeypatch.setattr(coder_login, "run_supervised_process", run)
    monkeypatch.setattr(coder_login, "isolated_auth_probe", probe)
    manager._execute_browser_session(session)
    await ready.wait()
    return manager, reservations, session, managed, channel, writes, completed


async def _submit(manager, session, code="synthetic-code", **kwargs):
    return await manager._submit_browser_code(
        kwargs.get("plugin", "claude"), kwargs.get("session_id", session.session_id),
        expected_reference=kwargs.get("reference", session.reference), authorization_code=code,
    )


@pytest.mark.parametrize("code", [None, 1, "", " \t", "a\n", "a\r", "a\x00", "a\x7f", "a\x85",
                                  "a\u2028", "a\u2029", "\ud800", "é" * 2049])
async def test_browser_submission_invalid_code_does_not_claim(monkeypatch, browser_preparation, code):
    manager, _, session, _, _, writes, completed = await _submittable_browser(monkeypatch, browser_preparation)
    before = await _browser_snapshot(manager, session)
    with pytest.raises(ValueError, match="^Invalid browser authorization code$"):
        await _submit(manager, session, code)
    assert await _browser_snapshot(manager, session) == before
    assert not session.submission_claimed and not writes
    await _submit(manager, session)
    completed.set()
    await session.task


@pytest.mark.parametrize("invalid", ["plugin", "id", "reference", "identity", "registry_reference", "device",
                                     "phase", "cancel", "parse", "cleanup", "reservation", "lock",
                                     "process", "exited", "channel", "closed", "eof", "task", "deadline", "shutdown"])
async def test_browser_submission_rejects_unowned_session(monkeypatch, browser_preparation, invalid):
    manager, reservations, session, managed, channel, writes, completed = await _submittable_browser(
        monkeypatch, browser_preparation,
    )
    kwargs = {}
    with monkeypatch.context() as patch:
        if invalid in {"plugin", "id", "reference"}:
            kwargs[{"plugin": "plugin", "id": "session_id", "reference": "reference"}[invalid]] = "wrong"
        elif invalid == "identity":
            patch.setattr(manager._registry, "get_optional", lambda _name: ClaudePlugin())
        elif invalid == "registry_reference":
            patch.setattr(manager._registry, "reference_for", lambda _name: "other")
        elif invalid == "device":
            patch.setattr(session, "application_deadline", None)
        elif invalid == "phase":
            patch.setattr(session, "state", "waiting_for_user")
        elif invalid in {"cancel", "parse", "cleanup", "reservation", "deadline"}:
            field, value = {"cancel": ("cancel_requested", True), "parse": ("parse_failed", True),
                            "cleanup": ("cleanup_confirmed", False), "reservation": ("reservation_held", False),
                            "deadline": ("deadline_monotonic", 0)}[invalid]
            patch.setattr(session, field, value)
        elif invalid == "lock":
            patch.setattr(reservations, "login_active", lambda _location: False)
        elif invalid == "process":
            patch.setattr(session, "managed", None)
        elif invalid == "exited":
            patch.setattr(managed.process, "returncode", 0)
        elif invalid == "channel":
            patch.setattr(session, "pty_channel", None)
        elif invalid in {"closed", "eof"}:
            patch.setattr(channel, "closed" if invalid == "closed" else "at_eof", True)
        elif invalid == "task":
            patch.setattr(session, "task", None)
        else:
            patch.setattr(manager, "_shutting_down", True)
        with pytest.raises(ValueError, match="^Browser login session cannot accept a code$"):
            await _submit(manager, session, **kwargs)
        assert not session.submission_claimed and not writes
    await _submit(manager, session)
    completed.set()
    await session.task


@pytest.mark.parametrize("code", ["  opaque é + / =  ", "é" * 2048])
async def test_browser_submission_is_atomic_and_opaque(monkeypatch, browser_preparation, code):
    manager, _, session, _, channel, writes, completed = await _submittable_browser(monkeypatch, browser_preparation)
    entered, release = asyncio.Event(), asyncio.Event()

    async def write(data):
        writes.append(data)
        entered.set()
        await release.wait()

    channel.write = write
    first = asyncio.create_task(_submit(manager, session, code))
    await entered.wait()
    assert session.state == "authorizing"
    assert session.verification_url is None and session.code_prompt_observed
    assert session.stdout == b""
    for chunk in [b"Paste code", b" here if prompted >", code.encode()]:
        manager._observe_chunk(session, session.stdout, chunk)
    manager._observe_browser_output(session)
    assert session.state == "authorizing" and session.stdout == b""
    with pytest.raises(ValueError):
        await _submit(manager, session, code)
    assert writes == [code.encode() + b"\n"]
    release.set()
    snapshot = await first
    assert snapshot["state"] == "authorizing"
    assert code not in json.dumps(snapshot)
    completed.set()
    await session.task
    assert (await _browser_snapshot(manager, session))["state"] == "succeeded"
    assert session.write_task.done() and session.code_delivered


@pytest.mark.parametrize("action", ["failure", "timeout", "cancel", "shutdown", "caller_cancel"])
async def test_browser_submission_retains_stubborn_writer_until_reconciled(
    monkeypatch, browser_preparation, action,
):
    manager, reservations, session, managed, channel, writes, completed = await _submittable_browser(
        monkeypatch, browser_preparation,
    )
    monkeypatch.setattr(coder_login, "_CLEANUP_GRACE_SECONDS", .01)
    entered, interrupted, release = asyncio.Event(), asyncio.Event(), asyncio.Event()

    async def write(data):
        writes.append(data)
        entered.set()
        try:
            await release.wait()
        except asyncio.CancelledError:
            interrupted.set()
            await release.wait()
        if action == "failure":
            raise RuntimeError("synthetic-secret")

    channel.write = write
    if action == "timeout":
        session.deadline_monotonic = manager._monotonic() + .03
    submission = asyncio.create_task(_submit(manager, session))
    await entered.wait()
    if action == "failure":
        # Fail cleanup as well, so even a settled failed writer retains ownership.
        managed.quiescent = False
        release.set()
        await submission
    elif action == "timeout":
        await submission
    elif action == "caller_cancel":
        submission.cancel()
        await submission
    elif action == "cancel":
        result = await manager.cancel("claude", session.session_id, expected_reference=session.reference)
        assert result["state"] == "cleanup_failed"
    else:
        completed.set()
        await manager.shutdown()
    assert session.state == "cleanup_failed" and session.cleanup_confirmed is False
    assert session.managed is managed and session.pty_channel is channel
    assert not channel.closed and reservations.login_active(session.adapter.credential_location)
    assert len(writes) == 1
    if action != "failure":
        assert interrupted.is_set() and not session.write_task.done()
        release.set()
    await session.write_task
    completed.set()
    await session.task
    if not submission.done():
        await submission
    managed.reconcile_quiescent = True
    result = await manager.cancel("claude", session.session_id, expected_reference=session.reference)
    assert result["state"] == "cancelled" and result["cleanup_confirmed"] is True
    assert channel.closed and session.managed is None
    assert not reservations.login_active(session.adapter.credential_location)
    assert "synthetic-secret" not in json.dumps(result)


async def test_browser_submission_deadline_cancels_writer(monkeypatch, browser_preparation):
    manager, reservations, session, _, channel, writes, completed = await _submittable_browser(
        monkeypatch, browser_preparation,
    )
    cancelled = asyncio.Event()

    async def write(data):
        writes.append(data)
        try:
            await asyncio.Event().wait()
        finally:
            cancelled.set()

    channel.write = write
    session.deadline_monotonic = manager._monotonic() + .02
    result = await _submit(manager, session)
    assert result["state"] == "timed_out" and result["cleanup_confirmed"] is True
    assert cancelled.is_set() and session.write_task.done() and not session.code_delivered
    assert reservations.login_active(session.adapter.credential_location)
    completed.set()
    await session.task
    assert channel.closed and not reservations.login_active(session.adapter.credential_location)
    assert len(writes) == 1


async def test_browser_old_credentials_without_submission_cannot_succeed(monkeypatch, browser_preparation):
    manager, reservations, session, _, channel, writes, completed = await _submittable_browser(
        monkeypatch, browser_preparation,
    )
    calls = []

    async def old_credentials(*_args, **_kwargs):
        calls.append(True)
        return _auth(True)

    monkeypatch.setattr(coder_login, "isolated_auth_probe", old_credentials)
    completed.set()
    await session.task
    assert (await _browser_snapshot(manager, session))["state"] == "failed"
    assert not calls and not writes
    assert channel.closed and not reservations.login_active(session.adapter.credential_location)


@pytest.mark.parametrize("field", ["plugin", "session_id", "reference"])
async def test_browser_submission_bad_identity_types(monkeypatch, browser_preparation, field):
    manager, _, session, _, _, writes, completed = await _submittable_browser(monkeypatch, browser_preparation)
    with pytest.raises(ValueError, match="^Browser login session cannot accept a code$"):
        await _submit(manager, session, **{field: []})
    assert not writes and not session.submission_claimed
    completed.set()
    await session.task


async def test_browser_exit_before_writer_settlement_times_out(monkeypatch, browser_preparation):
    manager, _, session, _, channel, _, completed = await _submittable_browser(monkeypatch, browser_preparation)
    entered = asyncio.Event()

    async def write(_data):
        entered.set()
        await asyncio.Event().wait()

    channel.write = write
    original_cleanup = manager._cleanup

    async def process_cleanup_first(current):
        if asyncio.current_task() is not session.task:
            await session.task
        return await original_cleanup(current)

    monkeypatch.setattr(manager, "_cleanup", process_cleanup_first)
    submission = asyncio.create_task(_submit(manager, session))
    await entered.wait()
    # The process finishes first; its remaining deadline bounds writer settlement.
    session.deadline_monotonic = manager._monotonic() + .02
    completed.set()
    await session.task
    await submission
    assert (await _browser_snapshot(manager, session))["state"] == "timed_out"
    assert session.write_task.done() and not session.code_delivered and channel.closed
