"""Coder plugin protocol and registry."""

from __future__ import annotations

import asyncio
import math
import re
from dataclasses import dataclass
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Hashable,
    Mapping,
    Protocol,
    runtime_checkable,
)

from src.process_supervisor import SupervisedProcess
from src.usage import UsageProvider

if TYPE_CHECKING:
    from src.config import AppConfig, DaemonConfig


AUTH_FAILURE_REASONS = frozenset(
    {
        "cli_missing",
        "credentials_missing",
        "daemon_unavailable",
        "probe_failed",
        "probe_timeout",
        "probe_unavailable",
        "unrecognized_output",
    }
)

DEVICE_LOGIN_STATES = frozenset(
    {
        "unsupported",
        "starting",
        "waiting_for_user",
        "succeeded",
        "failed",
        "canceling",
        "cancelled",
        "expired",
        "timed_out",
        "cleanup_failed",
        "not_found",
    }
)
DEVICE_LOGIN_FAILURE_REASONS = frozenset(
    {
        "unsupported",
        "session_not_found",
        "session_plugin_mismatch",
        "session_capacity",
        "replacement_required",
        "credential_in_use",
        "auth_status_unavailable",
        "cli_missing",
        "device_login_disabled",
        "startup_failed",
        "malformed_output",
        "provider_expired",
        "application_timeout",
        "process_failed",
        "cancellation_failed",
        "daemon_shutdown",
        "daemon_unavailable",
    }
)
_SESSION_ID_PATTERN = re.compile(r"[A-Za-z0-9_-]{32,128}")
_DEVICE_CODE_PATTERN = re.compile(r"[A-Z0-9-]{4,64}")


@dataclass(frozen=True)
class CoderAuthCapabilities:
    """Optional plugin-owned authentication capabilities.

    ``None`` means a legacy plugin did not advertise whether the capability is
    supported. Login method identifiers describe provider-neutral workflows;
    they are metadata only and never execute an authentication action.
    """

    can_check_cli: bool | None = None
    can_check_saved_credentials: bool | None = None
    can_report_authentication_mode: bool | None = None
    can_verify_service_access: bool | None = None
    interactive_login_methods: tuple[str, ...] | None = None


@dataclass(frozen=True)
class CoderAuthStatus:
    """Provider-neutral result of one read-only authentication probe.

    Every evidence field is tri-state. ``None`` means the probe did not learn
    that fact; in particular, saved credentials do not imply verified service
    access.
    """

    status: str
    detail: str
    cli_available: bool | None = None
    cli_version: str | None = None
    saved_credentials_present: bool | None = None
    authentication_mode: str | None = None
    service_access_verified: bool | None = None
    failure_reason: str | None = None


@dataclass(frozen=True)
class CoderDeviceLoginPrompt:
    """Allowlisted operator instructions parsed from provider output."""

    verification_url: str
    user_code: str
    expires_in_seconds: int


@dataclass(frozen=True)
class CoderDeviceLoginFailure:
    """Sanitized provider-specific failure classification."""

    reason: str
    detail: str


@runtime_checkable
class CoderDeviceLoginAdapter(Protocol):
    """Optional provider-owned device-login adapter.

    This protocol is deliberately separate from :class:`CoderPlugin`: legacy,
    Claude, and custom plugins remain valid without implementing login.
    Environment and output values stay inside the daemon and are never wire
    payloads.
    """

    @property
    def command(self) -> tuple[str, ...]: ...

    @property
    def environment(self) -> Mapping[str, str]: ...

    @property
    def working_directory(self) -> str: ...

    @property
    def credential_location(self) -> str: ...

    @property
    def application_timeout_seconds(self) -> float: ...

    @property
    def replacement_warning(self) -> str: ...

    def parse_progress(
        self, stdout: str, stderr: str
    ) -> CoderDeviceLoginPrompt | None: ...

    def classify_failure(
        self, stdout: str, stderr: str, returncode: int
    ) -> CoderDeviceLoginFailure: ...


@runtime_checkable
class CoderDeviceLoginPlugin(Protocol):
    """Complete optional capability for daemon-owned device login.

    A login-capable plugin must expose both the adapter factory and a
    side-effect-free credential locator. Daemon readers use the locator before
    launching any provider work, so accepting only one half of the capability
    would make safe coordination impossible.
    """

    def create_device_login(
        self, *, config_path: str
    ) -> CoderDeviceLoginAdapter: ...

    def device_login_credential_location(
        self, *, config: "AppConfig"
    ) -> str: ...


def resolve_device_login_credential_location(
    plugin: object,
    *,
    config: "AppConfig",
) -> str | None:
    """Return a login-capable plugin's validated coordination location.

    Plugins without a device-login factory do not participate. A plugin that
    advertises login but omits or returns an invalid locator fails closed so
    login and credential readers can never run without shared coordination.
    """
    factory = getattr(plugin, "create_device_login", None)
    if not callable(factory):
        return None
    resolver = getattr(plugin, "device_login_credential_location", None)
    if not callable(resolver):
        raise ValueError("login-capable coder is missing a credential locator")
    location = resolver(config=config)
    if not isinstance(location, str) or not location:
        raise ValueError("invalid coder credential location")
    return location


def _optional_bool(value: object, field: str) -> bool | None:
    if value is None or isinstance(value, bool):
        return value
    raise TypeError(f"{field} must be a boolean or null")


def _optional_identifier(value: object, field: str) -> str | None:
    if value is None:
        return None
    if (
        isinstance(value, str)
        and re.fullmatch(r"[a-z][a-z0-9_]{0,63}", value) is not None
    ):
        return value
    raise TypeError(f"{field} must be a bounded identifier or null")


def _auth_status_from_result(result: object) -> CoderAuthStatus:
    """Select and validate contract fields from a plugin result.

    Existing plugins may continue returning only ``status`` and ``detail``.
    Unknown keys are deliberately not forwarded across process boundaries.
    """
    if isinstance(result, CoderAuthStatus):
        raw: dict[str, object] = {
            "status": result.status,
            "detail": result.detail,
            "cli_available": result.cli_available,
            "cli_version": result.cli_version,
            "saved_credentials_present": result.saved_credentials_present,
            "authentication_mode": result.authentication_mode,
            "service_access_verified": result.service_access_verified,
            "failure_reason": result.failure_reason,
        }
    elif isinstance(result, dict):
        raw = result
    else:
        raise TypeError("invalid auth status")
    legacy_status = raw.get("status")
    detail = raw.get("detail")
    if legacy_status not in {"ok", "error"} or not isinstance(detail, str):
        raise TypeError("invalid auth status")
    cli_version = raw.get("cli_version")
    if cli_version is not None and (
        not isinstance(cli_version, str) or len(cli_version) > 64
    ):
        raise TypeError("cli_version must be a bounded string or null")
    failure_reason = raw.get("failure_reason")
    if failure_reason is not None and failure_reason not in AUTH_FAILURE_REASONS:
        raise TypeError("invalid auth failure reason")
    return CoderAuthStatus(
        status=legacy_status,
        detail=detail,
        cli_available=_optional_bool(
            raw.get("cli_available"), "cli_available"
        ),
        cli_version=cli_version,
        saved_credentials_present=_optional_bool(
            raw.get("saved_credentials_present"),
            "saved_credentials_present",
        ),
        authentication_mode=_optional_identifier(
            raw.get("authentication_mode"), "authentication_mode"
        ),
        service_access_verified=_optional_bool(
            raw.get("service_access_verified"),
            "service_access_verified",
        ),
        failure_reason=failure_reason,
    )


def _auth_capabilities_payload(
    capabilities: CoderAuthCapabilities | None,
) -> dict[str, Any]:
    selected = capabilities or CoderAuthCapabilities()
    methods = selected.interactive_login_methods
    if methods is not None:
        if not isinstance(methods, tuple):
            raise TypeError("interactive_login_methods must be a tuple or null")
        methods = tuple(
            _optional_identifier(method, "interactive_login_methods")
            for method in methods
        )
        if any(method is None for method in methods):
            raise TypeError("interactive_login_methods cannot contain null")
    return {
        "can_check_cli": _optional_bool(
            selected.can_check_cli, "can_check_cli"
        ),
        "can_check_saved_credentials": _optional_bool(
            selected.can_check_saved_credentials,
            "can_check_saved_credentials",
        ),
        "can_report_authentication_mode": _optional_bool(
            selected.can_report_authentication_mode,
            "can_report_authentication_mode",
        ),
        "can_verify_service_access": _optional_bool(
            selected.can_verify_service_access,
            "can_verify_service_access",
        ),
        "interactive_login_methods": (
            list(methods) if methods is not None else None
        ),
    }


def coder_auth_payload(
    result: object,
    *,
    capabilities: CoderAuthCapabilities | None = None,
) -> dict[str, Any]:
    """Return the explicit wire contract for a plugin auth result."""
    status = _auth_status_from_result(result)
    return {
        "status": status.status,
        "detail": status.detail,
        "cli_available": status.cli_available,
        "cli_version": status.cli_version,
        "saved_credentials_present": status.saved_credentials_present,
        "authentication_mode": status.authentication_mode,
        "service_access_verified": status.service_access_verified,
        "failure_reason": status.failure_reason,
        "capabilities": _auth_capabilities_payload(capabilities),
    }


def parse_coder_auth_payload(payload: object) -> dict[str, Any]:
    """Validate an auth result received from an isolated process."""
    if not isinstance(payload, dict):
        raise TypeError("invalid auth payload")
    raw_capabilities = payload.get("capabilities")
    if raw_capabilities is None:
        raw_capabilities = {}
    elif not isinstance(raw_capabilities, dict):
        raise TypeError("invalid auth capabilities")
    raw_methods = raw_capabilities.get("interactive_login_methods")
    if raw_methods is not None and (
        not isinstance(raw_methods, list)
        or not all(isinstance(method, str) for method in raw_methods)
    ):
        raise TypeError("invalid interactive login methods")
    capabilities = CoderAuthCapabilities(
        can_check_cli=_optional_bool(
            raw_capabilities.get("can_check_cli"), "can_check_cli"
        ),
        can_check_saved_credentials=_optional_bool(
            raw_capabilities.get("can_check_saved_credentials"),
            "can_check_saved_credentials",
        ),
        can_report_authentication_mode=_optional_bool(
            raw_capabilities.get("can_report_authentication_mode"),
            "can_report_authentication_mode",
        ),
        can_verify_service_access=_optional_bool(
            raw_capabilities.get("can_verify_service_access"),
            "can_verify_service_access",
        ),
        interactive_login_methods=(
            tuple(raw_methods) if raw_methods is not None else None
        ),
    )
    return coder_auth_payload(payload, capabilities=capabilities)


def parse_coder_device_login_payload(
    payload: object,
    *,
    expected_plugin: str | None = None,
) -> dict[str, Any]:
    """Validate and select the device-login fields allowed across the bridge."""
    if not isinstance(payload, dict):
        raise TypeError("invalid device login payload")
    plugin = payload.get("plugin")
    session_id = payload.get("session_id")
    state = payload.get("state")
    detail = payload.get("detail")
    failure_reason = payload.get("failure_reason")
    verification_url = payload.get("verification_url")
    user_code = payload.get("user_code")
    expires_at = payload.get("expires_at")
    cleanup_confirmed = payload.get("cleanup_confirmed")
    replacement_requested = payload.get("replacement_requested")
    reused_session = payload.get("reused_session")
    replacement_warning = payload.get("replacement_warning")
    raw_auth = payload.get("auth_status")
    if (
        not isinstance(plugin, str)
        or re.fullmatch(r"[a-z0-9][a-z0-9_-]{0,63}", plugin) is None
        or (expected_plugin is not None and plugin != expected_plugin)
    ):
        raise TypeError("invalid device login plugin")
    if session_id is not None and (
        not isinstance(session_id, str)
        or _SESSION_ID_PATTERN.fullmatch(session_id) is None
    ):
        raise TypeError("invalid device login session ID")
    if state not in DEVICE_LOGIN_STATES:
        raise TypeError("invalid device login state")
    if (
        not isinstance(detail, str)
        or not detail
        or len(detail) > 512
        or any(ord(character) < 32 for character in detail)
    ):
        raise TypeError("invalid device login detail")
    if (
        failure_reason is not None
        and failure_reason not in DEVICE_LOGIN_FAILURE_REASONS
    ):
        raise TypeError("invalid device login failure reason")
    if verification_url is not None and (
        not isinstance(verification_url, str)
        or len(verification_url) > 256
        or not verification_url.startswith("https://")
    ):
        raise TypeError("invalid device login verification URL")
    if user_code is not None and (
        not isinstance(user_code, str)
        or _DEVICE_CODE_PATTERN.fullmatch(user_code) is None
    ):
        raise TypeError("invalid device login user code")
    if expires_at is not None and (
        isinstance(expires_at, bool)
        or not isinstance(expires_at, (int, float))
        or not math.isfinite(expires_at)
        or expires_at <= 0
    ):
        raise TypeError("invalid device login expiry")
    if cleanup_confirmed is not None and not isinstance(cleanup_confirmed, bool):
        raise TypeError("invalid device login cleanup status")
    if not isinstance(replacement_requested, bool) or not isinstance(
        reused_session, bool
    ):
        raise TypeError("invalid device login flags")
    if replacement_warning is not None and (
        not isinstance(replacement_warning, str)
        or not replacement_warning
        or len(replacement_warning) > 512
        or any(ord(character) < 32 for character in replacement_warning)
    ):
        raise TypeError("invalid device login replacement warning")
    auth_status = (
        parse_coder_auth_payload(raw_auth) if raw_auth is not None else None
    )
    if state != "waiting_for_user" and (
        verification_url is not None or user_code is not None
    ):
        raise TypeError("device login instructions outlived waiting state")
    if (verification_url is None) != (user_code is None):
        raise TypeError("incomplete device login instructions")
    return {
        "plugin": plugin,
        "session_id": session_id,
        "state": state,
        "detail": detail,
        "failure_reason": failure_reason,
        "verification_url": verification_url,
        "user_code": user_code,
        "expires_at": float(expires_at) if expires_at is not None else None,
        "cleanup_confirmed": cleanup_confirmed,
        "replacement_requested": replacement_requested,
        "reused_session": reused_session,
        "replacement_warning": replacement_warning,
        "auth_status": auth_status,
    }


@dataclass(frozen=True)
class ModelReasoningEffort:
    """Reasoning-effort metadata advertised for one model."""

    name: str
    description: str | None = None


@dataclass(frozen=True)
class ModelMetadata:
    """Provider-neutral metadata for one invokable model."""

    invocation_id: str
    display_name: str
    is_default: bool = False
    default_reasoning_effort: str | None = None
    reasoning_efforts: tuple[ModelReasoningEffort, ...] = ()


@dataclass(frozen=True)
class ModelCatalog:
    """A plugin-owned model catalog normalized for shared consumers."""

    models: tuple[ModelMetadata, ...]
    source: str
    description: str


@dataclass(frozen=True)
class ModelSetting:
    """Plugin-owned binding for a model setting.

    New values live under ``daemon.coder_settings.<plugin_id>.<setting_key>``.
    ``config_field`` is an optional legacy fallback/input name; keeping that
    mapping in plugin metadata lets shared consumers stay provider-neutral.
    """

    config_field: str | None
    default_value: str
    default_label: str
    setting_key: str = "model"

    def control_name(self, plugin_id: str) -> str:
        """Return the generic Settings form field for ``plugin_id``."""
        return f"coder_settings.{plugin_id}.{self.setting_key}"

    def resolve(self, plugin_id: str, daemon_config: "DaemonConfig") -> str:
        """Resolve generic value, legacy fallback, then plugin default."""
        plugin_settings = daemon_config.coder_settings.get(plugin_id)
        if plugin_settings is not None and self.setting_key in plugin_settings:
            value = plugin_settings[self.setting_key]
            if not isinstance(value, str):
                raise ValueError(
                    f"daemon.coder_settings.{plugin_id}.{self.setting_key} "
                    "must be a string"
                )
            return value
        if self.config_field is not None:
            return str(getattr(daemon_config, self.config_field))
        return self.default_value


@dataclass(frozen=True)
class CoderMetadataView:
    """Non-executable plugin metadata used by the web control plane."""

    name: str
    display_name: str
    models: list[str]
    model_setting: ModelSetting
    model_catalog_refreshable: bool
    metadata_available: bool = True

    def resolve_model(self, daemon_config: "DaemonConfig") -> str:
        """Resolve the configured model without invoking plugin code."""
        return self.model_setting.resolve(self.name, daemon_config)

    def build_run_kwargs(
        self,
        *,
        daemon_config: "DaemonConfig",
        **_kwargs: Any,
    ) -> dict[str, Any]:
        """Expose model kwargs for provider-neutral settings consumers."""
        return {"model": self.resolve_model(daemon_config)}


class ModelCatalogUnavailable(RuntimeError):
    """A plugin could not provide a usable model catalog."""


@runtime_checkable
class CoderPlugin(Protocol):
    @property
    def name(self) -> str: ...

    @property
    def display_name(self) -> str: ...

    @property
    def models(self) -> list[str]: ...

    @property
    def model_setting(self) -> ModelSetting: ...

    def resolve_model(self, daemon_config: "DaemonConfig") -> str:
        """Return the effective model invocation ID for this plugin."""
        ...

    @property
    def model_catalog_refreshable(self) -> bool: ...

    def model_catalog_cache_key(
        self, *, config: "AppConfig", config_path: str
    ) -> Hashable:
        """Return the plugin/authentication context used to scope caching."""
        ...

    async def get_model_catalog(
        self, *, config: "AppConfig", config_path: str
    ) -> ModelCatalog:
        """Return normalized model metadata without starting inference."""
        ...

    async def run_planned_pr(
        self,
        repo_path: str,
        model: str | None,
        timeout: int,
        **kwargs: Any,
    ) -> tuple[int, str, str]: ...

    async def run_auto_pr(
        self,
        repo_path: str,
        *,
        pr_id: str,
        task_file: str,
        task_body: str,
        model: str | None,
        timeout: int,
        **kwargs: Any,
    ) -> tuple[int, str, str]: ...

    async def fix_review(
        self,
        repo_path: str,
        model: str | None,
        timeout: int | None = None,
        **kwargs: Any,
    ) -> tuple[int, str, str]: ...

    async def run_prompt(
        self,
        prompt: str,
        repo_path: str,
        model: str | None,
        timeout: int | None,
        on_process_start: Callable[[asyncio.subprocess.Process], None] | None = None,
        on_supervised_process_start: Callable[[SupervisedProcess], None]
        | None = None,
        **kwargs: Any,
    ) -> tuple[int, str, str]:
        """Run provider-neutral auxiliary work under process supervision."""
        ...

    def check_auth(self) -> dict[str, str]: ...

    def create_usage_provider(self, **kwargs: Any) -> UsageProvider | None: ...

    def rate_limit_patterns(self) -> list[re.Pattern[str]]: ...

    @property
    def supports_breach_lifecycle(self) -> bool:
        """True if the plugin honors breach detection.

        Anthropic CLI emits breach signals on stderr when usage hits
        configured thresholds. Other coders may not have this concept
        and return False here. Handlers check this property before
        wiring breach monitors.
        """
        ...

    @property
    def default_session_pause_percent(self) -> int:
        """Session-tier rate-limit pause threshold for this plugin."""
        ...

    @property
    def default_weekly_pause_percent(self) -> int:
        """Weekly-tier rate-limit pause threshold for this plugin."""
        ...

    async def diagnose_error(
        self,
        repo_path: str,
        context: str,
        model: str | None,
        on_process_start: Callable[[asyncio.subprocess.Process], None] | None = None,
        on_supervised_process_start: Callable[[SupervisedProcess], None]
        | None = None,
        **kwargs: Any,
    ) -> tuple[int, str, str]: ...

    def build_run_kwargs(
        self,
        *,
        daemon_config: "DaemonConfig",
        breach_dir: str | None = None,
        breach_run_id: str | None = None,
    ) -> dict[str, Any]:
        """Construct plugin-specific kwargs for primary and auxiliary runs.

        Returns the model selection plus any plugin-specific extras
        (e.g. breach monitoring inputs for plugins that support the
        breach lifecycle). Handlers compose handler-specific keys
        (timeout, on_process_start, extra_context) on top of the
        returned dict and pass the merged mapping via ``**kwargs`` to
        primary or auxiliary plugin method. Plugins that ignore the breach
        inputs (``supports_breach_lifecycle`` False) silently drop them so
        callers can pass them unconditionally.
        """
        ...


class CoderRegistry:
    def __init__(self) -> None:
        self._plugins: dict[str, CoderPlugin] = {}
        self._references: dict[str, str] = {}
        self._usage_providers: dict[str, UsageProvider | None] = {}

    def register(
        self,
        plugin: CoderPlugin,
        *,
        reference: str | None = None,
    ) -> None:
        self._plugins[plugin.name] = plugin
        if reference is None:
            self._references.pop(plugin.name, None)
        else:
            self._references[plugin.name] = reference

    def get(self, name: str) -> CoderPlugin:
        if name not in self._plugins:
            raise KeyError(f"Unknown coder: {name}")
        return self._plugins[name]

    def get_optional(self, name: str) -> CoderPlugin | None:
        """Return a loaded plugin, or ``None`` for an unavailable ID."""
        return self._plugins.get(name)

    def list_coders(self) -> list[CoderPlugin]:
        return list(self._plugins.values())

    def coder_names(self) -> list[str]:
        return list(self._plugins.keys())

    def reference_for(self, name: str) -> str | None:
        """Return the startup factory reference for a configured plugin."""
        self.get(name)
        return self._references.get(name)

    def set_usage_providers(
        self,
        providers: dict[str, UsageProvider | None],
    ) -> None:
        """Replace the provider snapshot created for the active config."""
        self._usage_providers = dict(providers)

    def usage_providers(self) -> dict[str, UsageProvider | None]:
        """Return a copy of the shared provider snapshot."""
        return dict(self._usage_providers)
