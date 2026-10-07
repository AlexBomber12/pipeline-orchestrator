"""Daemon-owned lifecycle for optional coder device-code login sessions."""

from __future__ import annotations

import asyncio
import math
import re
import secrets
import time
from dataclasses import dataclass, field
from typing import Any, Callable

from src.coder_auth import isolated_auth_probe
from src.coder_registry import (
    CoderDeviceLoginAdapter,
    CoderDeviceLoginFailure,
    CoderDeviceLoginPrompt,
    CoderRegistry,
    parse_coder_auth_payload,
    parse_coder_device_login_payload,
    resolve_device_login_credential_location,
)
from src.config import load_config
from src.process_supervisor import (
    ProcessLaunchCleanupError,
    ProcessSupervisionError,
    SupervisedProcess,
    launch_process,
    run_supervised_process,
)

_OUTPUT_BUFFER_BYTES = 16 * 1024
_TERMINAL_RETENTION_SECONDS = 5 * 60
_MAX_SESSIONS = 64
_AUTH_PROBE_TIMEOUT_SECONDS = 5.0
_CLEANUP_GRACE_SECONDS = 1.0
_DEVICE_CODE_PATTERN = re.compile(r"[A-Z0-9-]{4,64}")
_TERMINAL_STATES = frozenset(
    {"unsupported", "succeeded", "failed", "cancelled", "expired", "timed_out"}
)


class CoderCredentialReservations:
    """Coordinate credential mutation with concurrent read-only coder use.

    Coders may share one credential location concurrently, but a device-login
    process requires exclusive ownership because the pinned Codex CLI clears
    saved authentication before polling. All methods are synchronous so a
    check and reservation are atomic within the daemon event loop.
    """

    def __init__(self) -> None:
        self._login_locations: set[str] = set()
        self._coder_counts: dict[str, int] = {}
        self._credential_versions: dict[str, int] = {}

    def reserve_login(self, credential_location: str) -> bool:
        if (
            credential_location in self._login_locations
            or self._coder_counts.get(credential_location, 0) > 0
        ):
            return False
        self._login_locations.add(credential_location)
        return True

    def release_login(self, credential_location: str) -> None:
        if credential_location not in self._login_locations:
            return
        self._credential_versions[credential_location] = (
            self._credential_versions.get(credential_location, 0) + 1
        )
        self._login_locations.discard(credential_location)

    def login_active(self, credential_location: str) -> bool:
        return credential_location in self._login_locations

    def credential_version(self, credential_location: str) -> int:
        return self._credential_versions.get(credential_location, 0)

    def reserve_coder(self, credential_location: str) -> bool:
        if credential_location in self._login_locations:
            return False
        self._coder_counts[credential_location] = (
            self._coder_counts.get(credential_location, 0) + 1
        )
        return True

    def release_coder(self, credential_location: str) -> None:
        count = self._coder_counts.get(credential_location, 0)
        if count <= 1:
            self._coder_counts.pop(credential_location, None)
        else:
            self._coder_counts[credential_location] = count - 1


@dataclass
class _LoginSession:
    session_id: str
    plugin: str
    reference: str
    adapter: CoderDeviceLoginAdapter
    replacement_requested: bool
    created_monotonic: float
    state: str = "starting"
    detail: str = "Preparing device-code login"
    failure_reason: str | None = None
    verification_url: str | None = None
    user_code: str | None = None
    expires_at: float | None = None
    cleanup_confirmed: bool | None = None
    auth_status: dict[str, Any] | None = None
    terminal_monotonic: float | None = None
    managed: SupervisedProcess | None = None
    task: asyncio.Task[None] | None = None
    abort_task: asyncio.Task[None] | None = None
    cancel_requested: bool = False
    parse_failed: bool = False
    reservation_held: bool = False
    stdout: bytearray = field(default_factory=bytearray)
    stderr: bytearray = field(default_factory=bytearray)


class CoderLoginSessionManager:
    """Own bounded, in-memory login sessions for the daemon lifetime."""

    def __init__(
        self,
        registry: CoderRegistry,
        *,
        config_path: str,
        credential_location_in_use: Callable[[str], bool] | None = None,
        credential_reservations: CoderCredentialReservations | None = None,
        wall_time: Callable[[], float] = time.time,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        self._registry = registry
        self._config_path = config_path
        self._credential_location_in_use = credential_location_in_use or (
            lambda _location: False
        )
        self._credential_reservations = (
            credential_reservations or CoderCredentialReservations()
        )
        self._wall_time = wall_time
        self._monotonic = monotonic
        self._sessions: dict[str, _LoginSession] = {}
        self._shutting_down = False

    async def start(
        self,
        plugin_name: str,
        *,
        expected_reference: str,
        replace_existing: bool,
    ) -> dict[str, Any]:
        """Create a session and schedule all blocking work in the background."""
        self._purge_expired_sessions()
        if self._shutting_down:
            return self._error_payload(
                plugin_name,
                "failed",
                "Device-code login is unavailable while the daemon is shutting down",
                "daemon_shutdown",
                replacement_requested=replace_existing,
            )
        try:
            plugin = self._registry.get(plugin_name)
            reference = self._registry.reference_for(plugin_name)
        except KeyError:
            return self._unsupported_payload(plugin_name, replace_existing)
        if reference != expected_reference:
            return self._error_payload(
                plugin_name,
                "failed",
                "Coder plugin identity changed; refresh and start again",
                "session_plugin_mismatch",
                replacement_requested=replace_existing,
            )
        factory = getattr(plugin, "create_device_login", None)
        if not callable(factory):
            return self._unsupported_payload(plugin_name, replace_existing)
        try:
            credential_location = resolve_device_login_credential_location(
                plugin,
                config=load_config(self._config_path),
            )
            adapter = factory(config_path=self._config_path)
        except Exception:
            return self._error_payload(
                plugin_name,
                "failed",
                "Device-code login could not be prepared",
                "startup_failed",
                replacement_requested=replace_existing,
            )
        if (
            not self._valid_adapter(adapter)
            or adapter.credential_location != credential_location
        ):
            return self._unsupported_payload(plugin_name, replace_existing)

        for existing in self._sessions.values():
            if not self._retains_credential_ownership(existing):
                continue
            if existing.adapter.credential_location != adapter.credential_location:
                continue
            if (
                existing.plugin == plugin_name
                and existing.reference == expected_reference
            ):
                return self._session_payload(existing, reused_session=True)
            return self._error_payload(
                plugin_name,
                "failed",
                "Another login already owns this credential location",
                "credential_in_use",
                replacement_requested=replace_existing,
                replacement_warning=adapter.replacement_warning,
            )

        if len(self._sessions) >= _MAX_SESSIONS:
            return self._error_payload(
                plugin_name,
                "failed",
                "Too many retained device-login sessions",
                "session_capacity",
                replacement_requested=replace_existing,
                replacement_warning=adapter.replacement_warning,
            )
        if not self._credential_reservations.reserve_login(
            adapter.credential_location
        ):
            return self._error_payload(
                plugin_name,
                "failed",
                "A coder invocation or login is using this credential location",
                "credential_in_use",
                replacement_requested=replace_existing,
                replacement_warning=adapter.replacement_warning,
            )
        session_id = self._new_session_id()
        session = _LoginSession(
            session_id=session_id,
            plugin=plugin_name,
            reference=expected_reference,
            adapter=adapter,
            replacement_requested=replace_existing,
            created_monotonic=self._monotonic(),
            reservation_held=True,
        )
        self._sessions[session_id] = session
        session.task = asyncio.create_task(self._start_session(session))
        return self._session_payload(session)

    async def inspect(
        self,
        plugin_name: str,
        session_id: str,
        *,
        expected_reference: str,
    ) -> dict[str, Any]:
        """Return one allowlisted session snapshot."""
        self._purge_expired_sessions()
        session = self._sessions.get(session_id)
        if session is None:
            return self._not_found_payload(plugin_name, session_id)
        if session.plugin != plugin_name or session.reference != expected_reference:
            return self._mismatch_payload(plugin_name, session_id)
        return self._session_payload(session)

    async def cancel(
        self,
        plugin_name: str,
        session_id: str,
        *,
        expected_reference: str,
    ) -> dict[str, Any]:
        """Cancel a session without claiming success before cleanup is proven."""
        self._purge_expired_sessions()
        session = self._sessions.get(session_id)
        if session is None:
            return self._not_found_payload(plugin_name, session_id)
        if session.plugin != plugin_name or session.reference != expected_reference:
            return self._mismatch_payload(plugin_name, session_id)
        if session.state in _TERMINAL_STATES:
            return self._session_payload(session)

        session.cancel_requested = True
        session.state = "canceling"
        session.detail = "Canceling device-code login"
        session.verification_url = None
        session.user_code = None
        session.expires_at = None
        if session.managed is None:
            if session.cleanup_confirmed is False:
                self._cleanup_failed(session)
                return self._session_payload(session)
            if session.task is not None:
                session.task.cancel()
                await asyncio.gather(session.task, return_exceptions=True)
            if session.state not in _TERMINAL_STATES:
                self._finish(
                    session,
                    state="cancelled",
                    detail="Device-code login was cancelled",
                    cleanup_confirmed=True,
                )
            return self._session_payload(session)

        if not await self._cleanup(session):
            self._cleanup_failed(session)
            return self._session_payload(session)
        if session.task is not None and not session.task.done():
            session.task.cancel()
            done, _pending = await asyncio.wait({session.task}, timeout=3)
            if not done:
                self._cleanup_failed(
                    session,
                    detail=(
                        "Device-code login session task did not stop after "
                        "cancellation"
                    ),
                )
                return self._session_payload(session)
        if session.state not in _TERMINAL_STATES:
            self._finish(
                session,
                state="cancelled",
                detail="Device-code login was cancelled",
                cleanup_confirmed=True,
            )
        return self._session_payload(session)

    async def shutdown(self) -> None:
        """Cancel every owned login process during daemon shutdown."""
        self._shutting_down = True
        active = [
            session
            for session in self._sessions.values()
            if self._retains_credential_ownership(session)
        ]
        cleanup_confirmed_session_ids: set[str] = set()
        for session in active:
            session.cancel_requested = True
            session.state = "canceling"
            session.detail = "Stopping device-code login during daemon shutdown"
            session.verification_url = None
            session.user_code = None
            session.expires_at = None
            if session.managed is None and session.task is not None:
                session.task.cancel()
        for session in active:
            if session.managed is None:
                if session.cleanup_confirmed is False:
                    self._cleanup_failed(session)
                    continue
                cleanup_confirmed_session_ids.add(session.session_id)
                continue
            if not await self._cleanup(session):
                self._cleanup_failed(session)
                continue
            cleanup_confirmed_session_ids.add(session.session_id)
        tasks = [
            session.task
            for session in active
            if session.task is not None and not session.task.done()
        ]
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        for session in active:
            if (
                session.state in _TERMINAL_STATES
                or session.session_id not in cleanup_confirmed_session_ids
            ):
                continue
            self._finish(
                session,
                state="cancelled",
                detail="Device-code login stopped during daemon shutdown",
                failure_reason="daemon_shutdown",
                cleanup_confirmed=True,
            )

    async def _start_session(self, session: _LoginSession) -> None:
        try:
            auth = await isolated_auth_probe(
                session.plugin,
                session.reference,
                self._registry.get(session.plugin).display_name,
                config_path=self._config_path,
                timeout=_AUTH_PROBE_TIMEOUT_SECONDS,
                env=dict(session.adapter.environment),
            )
            auth = parse_coder_auth_payload(auth)
            if session.cancel_requested:
                self._finish(
                    session,
                    state="cancelled",
                    detail="Device-code login was cancelled",
                    cleanup_confirmed=True,
                )
                return
            if auth["cli_available"] is False:
                self._finish(
                    session,
                    state="failed",
                    detail="Coder CLI is unavailable",
                    failure_reason="cli_missing",
                    cleanup_confirmed=True,
                )
                return
            if (
                auth["saved_credentials_present"] is True
                and not session.replacement_requested
            ):
                self._finish(
                    session,
                    state="failed",
                    detail="Existing saved authentication requires explicit replacement",
                    failure_reason="replacement_required",
                    cleanup_confirmed=True,
                )
                return
            if auth["saved_credentials_present"] is None:
                self._finish(
                    session,
                    state="failed",
                    detail="Saved credential status is unavailable; login was not started",
                    failure_reason="auth_status_unavailable",
                    cleanup_confirmed=True,
                )
                return
            try:
                credential_busy = self._credential_location_in_use(
                    session.adapter.credential_location
                )
            except Exception:
                credential_busy = True
            if credential_busy:
                self._finish(
                    session,
                    state="failed",
                    detail="A coder invocation is using this credential location",
                    failure_reason="credential_in_use",
                    cleanup_confirmed=True,
                )
                return

            try:
                session.managed = await launch_process(
                    *session.adapter.command,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                    stdin=asyncio.subprocess.DEVNULL,
                    cwd=session.adapter.working_directory,
                    env=dict(session.adapter.environment),
                )
            except ProcessLaunchCleanupError as exc:
                session.managed = exc.managed
                self._cleanup_failed(
                    session,
                    detail=(
                        "Device-code login startup cleanup could not confirm "
                        "process termination"
                    ),
                )
                return
            except FileNotFoundError:
                self._finish(
                    session,
                    state="failed",
                    detail="Coder CLI is unavailable",
                    failure_reason="cli_missing",
                    cleanup_confirmed=True,
                )
                return
            except Exception:
                self._finish(
                    session,
                    state="failed",
                    detail="Device-code login process could not be started",
                    failure_reason="startup_failed",
                    cleanup_confirmed=True,
                )
                return
            if session.cancel_requested:
                if await self._cleanup(session):
                    self._finish(
                        session,
                        state="cancelled",
                        detail="Device-code login was cancelled",
                        cleanup_confirmed=True,
                    )
                else:
                    self._cleanup_failed(session)
                return
            await self._run_owned_session(session)
        except asyncio.CancelledError:
            if session.managed is None:
                self._finish(
                    session,
                    state="cancelled",
                    detail=(
                        "Device-code login stopped during daemon shutdown"
                        if self._shutting_down
                        else "Device-code login was cancelled"
                    ),
                    failure_reason=("daemon_shutdown" if self._shutting_down else None),
                    cleanup_confirmed=True,
                )
                return
            if await self._cleanup(session):
                self._finish(
                    session,
                    state="cancelled",
                    detail="Device-code login was cancelled",
                    cleanup_confirmed=True,
                )
            else:
                self._cleanup_failed(session)
        except Exception:
            if session.managed is not None:
                if not await self._cleanup(session):
                    self._cleanup_failed(session)
                    return
            self._finish(
                session,
                state="failed",
                detail="Device-code login could not complete safely",
                failure_reason="auth_status_unavailable",
                cleanup_confirmed=True,
            )

    async def _run_owned_session(self, session: _LoginSession) -> None:
        assert session.managed is not None
        try:
            result = await run_supervised_process(
                session.managed,
                timeout=session.adapter.application_timeout_seconds,
                stdout_chunk_callback=lambda chunk: self._observe_chunk(
                    session, session.stdout, chunk
                ),
                stderr_chunk_callback=lambda chunk: self._observe_chunk(
                    session, session.stderr, chunk
                ),
                max_output_bytes=_OUTPUT_BUFFER_BYTES,
            )
        except ProcessSupervisionError:
            if not await self._cleanup(session):
                self._cleanup_failed(session)
                return
            if session.cancel_requested:
                self._finish(
                    session,
                    state="cancelled",
                    detail="Device-code login was cancelled",
                    cleanup_confirmed=True,
                )
                return
            self._finish(
                session,
                state="failed",
                detail="Device-code login process supervision failed",
                failure_reason=(
                    "malformed_output" if session.parse_failed else "process_failed"
                ),
                cleanup_confirmed=True,
            )
            return
        if session.cancel_requested:
            self._finish(
                session,
                state="cancelled",
                detail="Device-code login was cancelled",
                cleanup_confirmed=True,
            )
            return
        if result.timed_out:
            self._finish(
                session,
                state="timed_out",
                detail="Device-code login exceeded the application deadline",
                failure_reason="application_timeout",
                cleanup_confirmed=True,
            )
            return
        if session.parse_failed or (
            result.returncode == 0 and session.verification_url is None
        ):
            self._finish(
                session,
                state="failed",
                detail="Codex returned malformed device-login output",
                failure_reason="malformed_output",
                cleanup_confirmed=True,
            )
            return
        if result.returncode == 0:
            auth = await isolated_auth_probe(
                session.plugin,
                session.reference,
                self._registry.get(session.plugin).display_name,
                config_path=self._config_path,
                timeout=_AUTH_PROBE_TIMEOUT_SECONDS,
                env=dict(session.adapter.environment),
            )
            if session.cancel_requested:
                self._finish(
                    session,
                    state="cancelled",
                    detail="Device-code login was cancelled",
                    cleanup_confirmed=True,
                )
                return
            session.auth_status = parse_coder_auth_payload(auth)
            self._finish(
                session,
                state="succeeded",
                detail="Device-code login completed; authentication status refreshed",
                cleanup_confirmed=True,
            )
            return
        try:
            failure = session.adapter.classify_failure(
                session.stdout.decode("utf-8", errors="replace"),
                session.stderr.decode("utf-8", errors="replace"),
                result.returncode,
            )
        except Exception:
            failure = CoderDeviceLoginFailure(
                "process_failed", "Device-code login failed"
            )
        if not isinstance(failure, CoderDeviceLoginFailure) or (
            failure.reason
            not in {"provider_expired", "device_login_disabled", "process_failed"}
        ):
            failure = CoderDeviceLoginFailure(
                "process_failed", "Device-code login failed"
            )
        failure_detail = {
            "provider_expired": (
                "The provider device code expired before authorization completed"
            ),
            "device_login_disabled": (
                "Device-code login is disabled for this account or configuration"
            ),
            "process_failed": "Device-code login failed",
        }[failure.reason]
        self._finish(
            session,
            state="expired" if failure.reason == "provider_expired" else "failed",
            detail=failure_detail,
            failure_reason=failure.reason,
            cleanup_confirmed=True,
        )

    def _observe_chunk(
        self,
        session: _LoginSession,
        target: bytearray,
        chunk: bytes,
    ) -> None:
        if session.state in _TERMINAL_STATES or session.parse_failed:
            return
        target.extend(chunk)
        if len(target) > _OUTPUT_BUFFER_BYTES:
            del target[: len(target) - _OUTPUT_BUFFER_BYTES]
        try:
            prompt = session.adapter.parse_progress(
                session.stdout.decode("utf-8", errors="replace"),
                session.stderr.decode("utf-8", errors="replace"),
            )
            if prompt is None:
                return
            if not self._valid_prompt(prompt):
                raise ValueError("invalid device-login prompt")
            first_prompt = session.verification_url is None
            if not first_prompt and (
                session.verification_url != prompt.verification_url
                or session.user_code != prompt.user_code
            ):
                raise ValueError("device-login prompt changed")
        except Exception:
            session.parse_failed = True
            session.detail = "Codex returned malformed device-login output"
            session.failure_reason = "malformed_output"
            if session.managed is not None and session.abort_task is None:
                session.abort_task = asyncio.create_task(
                    self._abort_malformed_session(session)
                )
            return
        session.state = "waiting_for_user"
        session.detail = "Open the verification URL and enter the one-time code"
        session.verification_url = prompt.verification_url
        session.user_code = prompt.user_code
        if first_prompt:
            session.expires_at = self._wall_time() + prompt.expires_in_seconds

    async def _abort_malformed_session(self, session: _LoginSession) -> None:
        assert session.managed is not None
        if not await self._cleanup(session):
            self._cleanup_failed(session)

    async def _cleanup(self, session: _LoginSession) -> bool:
        managed = session.managed
        if managed is None:
            return session.cleanup_confirmed is not False
        try:
            if session.cleanup_confirmed is False:
                result = await managed.reconcile_cleanup(
                    observation_grace=_CLEANUP_GRACE_SECONDS,
                )
            else:
                result = await managed.cleanup(
                    term_grace=_CLEANUP_GRACE_SECONDS,
                    kill_grace=_CLEANUP_GRACE_SECONDS,
                )
        except Exception:
            return False
        return result.quiescent

    def _finish(
        self,
        session: _LoginSession,
        *,
        state: str,
        detail: str,
        failure_reason: str | None = None,
        cleanup_confirmed: bool,
    ) -> None:
        session.state = state
        session.detail = detail
        session.failure_reason = failure_reason
        session.cleanup_confirmed = cleanup_confirmed
        session.verification_url = None
        session.user_code = None
        session.expires_at = None
        session.stdout.clear()
        session.stderr.clear()
        session.terminal_monotonic = self._monotonic()
        if cleanup_confirmed:
            self._release_reservation(session)
            session.managed = None

    def _cleanup_failed(
        self,
        session: _LoginSession,
        *,
        detail: str = (
            "Device-code login cleanup could not confirm process termination"
        ),
    ) -> None:
        session.state = "cleanup_failed"
        session.detail = detail
        session.failure_reason = "cancellation_failed"
        session.cleanup_confirmed = False
        session.verification_url = None
        session.user_code = None
        session.expires_at = None
        session.stdout.clear()
        session.stderr.clear()

    def _release_reservation(self, session: _LoginSession) -> None:
        if not session.reservation_held:
            return
        self._credential_reservations.release_login(
            session.adapter.credential_location
        )
        session.reservation_held = False

    def _session_payload(
        self, session: _LoginSession, *, reused_session: bool = False
    ) -> dict[str, Any]:
        return parse_coder_device_login_payload(
            {
                "plugin": session.plugin,
                "session_id": session.session_id,
                "state": session.state,
                "detail": session.detail,
                "failure_reason": session.failure_reason,
                "verification_url": session.verification_url,
                "user_code": session.user_code,
                "expires_at": session.expires_at,
                "cleanup_confirmed": session.cleanup_confirmed,
                "replacement_requested": session.replacement_requested,
                "reused_session": reused_session,
                "replacement_warning": session.adapter.replacement_warning,
                "auth_status": session.auth_status,
            },
            expected_plugin=session.plugin,
        )

    def _unsupported_payload(
        self, plugin_name: str, replacement_requested: bool
    ) -> dict[str, Any]:
        return self._error_payload(
            plugin_name,
            "unsupported",
            "This coder plugin does not support daemon-owned device-code login",
            "unsupported",
            replacement_requested=replacement_requested,
        )

    def _not_found_payload(self, plugin_name: str, session_id: str) -> dict[str, Any]:
        del session_id
        return self._error_payload(
            plugin_name,
            "not_found",
            "Device-login session was not found or is no longer retained",
            "session_not_found",
        )

    def _mismatch_payload(self, plugin_name: str, session_id: str) -> dict[str, Any]:
        return self._error_payload(
            plugin_name,
            "failed",
            "Device-login session does not belong to this coder plugin",
            "session_plugin_mismatch",
            session_id=session_id,
        )

    @staticmethod
    def _error_payload(
        plugin_name: str,
        state: str,
        detail: str,
        failure_reason: str,
        *,
        session_id: str | None = None,
        replacement_requested: bool = False,
        replacement_warning: str | None = None,
    ) -> dict[str, Any]:
        return parse_coder_device_login_payload(
            {
                "plugin": plugin_name,
                "session_id": session_id,
                "state": state,
                "detail": detail,
                "failure_reason": failure_reason,
                "verification_url": None,
                "user_code": None,
                "expires_at": None,
                "cleanup_confirmed": None,
                "replacement_requested": replacement_requested,
                "reused_session": False,
                "replacement_warning": replacement_warning,
                "auth_status": None,
            },
            expected_plugin=plugin_name,
        )

    def _purge_expired_sessions(self) -> None:
        cutoff = self._monotonic() - _TERMINAL_RETENTION_SECONDS
        stale = [
            session_id
            for session_id, session in self._sessions.items()
            if session.terminal_monotonic is not None
            and session.terminal_monotonic <= cutoff
        ]
        for session_id in stale:
            del self._sessions[session_id]

    def _new_session_id(self) -> str:
        while True:
            session_id = secrets.token_urlsafe(32)
            if session_id not in self._sessions:
                return session_id

    @staticmethod
    def _retains_credential_ownership(session: _LoginSession) -> bool:
        return session.terminal_monotonic is None

    @staticmethod
    def _valid_prompt(prompt: object) -> bool:
        return (
            isinstance(prompt, CoderDeviceLoginPrompt)
            and isinstance(prompt.verification_url, str)
            and len(prompt.verification_url) <= 256
            and prompt.verification_url.startswith("https://")
            and isinstance(prompt.user_code, str)
            and _DEVICE_CODE_PATTERN.fullmatch(prompt.user_code) is not None
            and isinstance(prompt.expires_in_seconds, int)
            and not isinstance(prompt.expires_in_seconds, bool)
            and 0 < prompt.expires_in_seconds <= 60 * 60
        )

    @staticmethod
    def _valid_adapter(adapter: object) -> bool:
        try:
            if not isinstance(adapter, CoderDeviceLoginAdapter):
                return False
            return (
                isinstance(adapter.command, tuple)
                and 1 <= len(adapter.command) <= 16
                and all(
                    isinstance(part, str) and 0 < len(part) <= 1024
                    for part in adapter.command
                )
                and len(adapter.environment) <= 512
                and all(
                    isinstance(key, str)
                    and isinstance(value, str)
                    and "\x00" not in key
                    and "\x00" not in value
                    for key, value in adapter.environment.items()
                )
                and isinstance(adapter.working_directory, str)
                and bool(adapter.working_directory)
                and isinstance(adapter.credential_location, str)
                and bool(adapter.credential_location)
                and isinstance(adapter.application_timeout_seconds, (int, float))
                and not isinstance(adapter.application_timeout_seconds, bool)
                and math.isfinite(adapter.application_timeout_seconds)
                and 0 < adapter.application_timeout_seconds <= 60 * 60
                and isinstance(adapter.replacement_warning, str)
                and 0 < len(adapter.replacement_warning) <= 512
                and not any(
                    ord(character) < 32
                    for character in adapter.replacement_warning
                )
            )
        except Exception:
            return False
