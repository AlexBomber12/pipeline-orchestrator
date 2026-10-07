"""Codex coder plugin."""

from __future__ import annotations

import asyncio
import os
import re
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable
from urllib.parse import urlsplit

from src import codex_cli
from src.coder_registry import (
    CoderAuthCapabilities,
    CoderAuthStatus,
    CoderDeviceLoginFailure,
    CoderDeviceLoginPrompt,
    ModelCatalog,
    ModelCatalogUnavailable,
    ModelMetadata,
    ModelReasoningEffort,
    ModelSetting,
    coder_auth_payload,
)
from src.coders.codex_models import (
    CodexModelDiscoveryInvalid,
    CodexModelDiscoveryUnavailable,
    discover_codex_models,
)
from src.config import AppConfig, load_config
from src.process_supervisor import SupervisedProcess
from src.usage import OpenAIUsageProvider, UsageProvider

if TYPE_CHECKING:
    from src.config import DaemonConfig

CONFIG_PATH = os.environ.get("PO_CONFIG_PATH", "config.yml")
_AUTH_CHECK_TIMEOUT_SEC = 5
_DEVICE_LOGIN_TIMEOUT_SECONDS = 16 * 60
_DEVICE_CODE_EXPIRES_IN_SECONDS = 15 * 60
_DEVICE_VERIFICATION_URL = "https://auth.openai.com/codex/device"
_DEVICE_LOGIN_REPLACEMENT_WARNING = (
    "Codex clears existing saved authentication before device login; an "
    "unsuccessful or cancelled replacement can leave authentication unavailable."
)
_ANSI_ESCAPE_PATTERN = re.compile(r"\x1b\[[0-?]*[ -/]*[@-~]")
_DEVICE_CODE_PATTERN = re.compile(r"[A-Z0-9-]{4,64}")
_REASONING_EFFORT_SETTING = "reasoning_effort"
_CODEX_RETRY_PATTERN = re.compile(
    r"try again in\s+"
    r"(?:(\d+)\s*days?)?\s*"
    r"(?:(\d+)\s*hours?)?\s*"
    r"(?:(\d+)\s*minutes?)?\s*"
    r"(?:(\d+(?:\.\d+)?)\s*(?:seconds?|secs?|s))?",
    re.IGNORECASE,
)
_CODEX_USAGE_LIMIT_PATTERN = re.compile(
    r"(you've hit your usage limit|usage limit|rate limit exceeded|try again later|retry later)",
    re.IGNORECASE,
)
_CODEX_VERSION_PATTERN = re.compile(
    r"^(?:codex|codex-cli)\s+"
    r"(?P<version>[0-9]+\.[0-9]+\.[0-9]+(?:[-+][0-9A-Za-z.-]+)?)$"
)
_AUTH_MODE_PATTERNS = (
    (re.compile(r"^Logged in (?:using|with) ChatGPT$"), "chatgpt"),
    (re.compile(r"^Logged in using an API key(?:\s+-\s+.*)?$"), "api_key"),
    (re.compile(r"^Logged in using (?:personal )?access token$"), "access_token"),
    (
        re.compile(r"^Logged in using Amazon Bedrock API key$"),
        "bedrock_api_key",
    ),
    (
        re.compile(r"^Logged in using Amazon Bedrock AWS access keys$"),
        "bedrock_access_keys",
    ),
    (re.compile(r"^Logged in using workload identity$"), "workload_identity"),
)


def _run_auth_command(
    cmd: list[str], *, env: dict[str, str] | None = None
) -> tuple[int, str, str]:
    try:
        completed = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=_AUTH_CHECK_TIMEOUT_SEC,
            stdin=subprocess.DEVNULL,
            env=env,
        )
    except FileNotFoundError:
        return 127, "", f"{cmd[0]} not found"
    except PermissionError as exc:
        return 126, "", str(exc)
    except subprocess.TimeoutExpired:
        return 124, "", f"{cmd[0]} timed out after {_AUTH_CHECK_TIMEOUT_SEC}s"
    return completed.returncode, completed.stdout or "", completed.stderr or ""


def _first_probe_line(text: str) -> str:
    for line in text.splitlines():
        stripped = line.strip()
        if stripped and not stripped.lower().startswith("warning:"):
            return stripped
    return ""


def _codex_version(text: str) -> str | None:
    line = _first_probe_line(text)
    match = _CODEX_VERSION_PATTERN.fullmatch(line)
    return match.group("version") if match is not None else None


def _authentication_mode(text: str) -> str | None:
    line = _first_probe_line(text)
    for pattern, mode in _AUTH_MODE_PATTERNS:
        if pattern.fullmatch(line) is not None:
            return mode
    return None


def _reports_missing_cli(text: str) -> bool:
    line = _first_probe_line(text).lower()
    return line in {"codex: not found", "codex not found"} or line.endswith(
        ": no such file or directory"
    )


def _strip_ansi(text: str) -> str:
    return _ANSI_ESCAPE_PATTERN.sub("", text).replace("\r", "")


def _line_after_marker(text: str, marker: str) -> str | None:
    _, separator, remainder = text.partition(marker)
    if not separator:
        return None
    for line in remainder.splitlines(keepends=True)[1:]:
        candidate = line.strip()
        if candidate:
            if not line.endswith("\n"):
                return None
            return candidate
    return None


@dataclass(frozen=True)
class CodexDeviceLoginAdapter:
    """Pinned Codex CLI 0.160.0 device-login command and parser."""

    command: tuple[str, ...]
    environment: dict[str, str]
    working_directory: str
    credential_location: str
    application_timeout_seconds: float = _DEVICE_LOGIN_TIMEOUT_SECONDS
    replacement_warning: str = _DEVICE_LOGIN_REPLACEMENT_WARNING

    def parse_progress(
        self, stdout: str, stderr: str
    ) -> CoderDeviceLoginPrompt | None:
        del stderr
        text = _strip_ansi(stdout)
        url = _line_after_marker(
            text,
            "1. Open this link in your browser and sign in to your account",
        )
        code = _line_after_marker(
            text,
            "2. Enter this one-time code (expires in 15 minutes)",
        )
        if url is None or code is None:
            return None
        parsed = urlsplit(url)
        if (
            url != _DEVICE_VERIFICATION_URL
            or parsed.scheme != "https"
            or parsed.hostname != "auth.openai.com"
            or parsed.port is not None
            or parsed.username is not None
            or parsed.password is not None
            or parsed.path != "/codex/device"
            or parsed.query
            or parsed.fragment
        ):
            raise ValueError("invalid Codex device verification URL")
        if _DEVICE_CODE_PATTERN.fullmatch(code) is None:
            raise ValueError("invalid Codex device code")
        return CoderDeviceLoginPrompt(
            verification_url=url,
            user_code=code,
            expires_in_seconds=_DEVICE_CODE_EXPIRES_IN_SECONDS,
        )

    def classify_failure(
        self, stdout: str, stderr: str, returncode: int
    ) -> CoderDeviceLoginFailure:
        del returncode
        output = _strip_ansi(f"{stdout}\n{stderr}").lower()
        if "device auth timed out after 15 minutes" in output:
            return CoderDeviceLoginFailure(
                "provider_expired",
                "The Codex device code expired before authorization completed",
            )
        if (
            "device code login is not enabled" in output
            or "chatgpt login is disabled" in output
        ):
            return CoderDeviceLoginFailure(
                "device_login_disabled",
                "Codex device-code login is disabled for this account or configuration",
            )
        return CoderDeviceLoginFailure(
            "process_failed",
            "Codex device-code login failed",
        )


class CodexPlugin:
    name = "codex"
    display_name = "Codex CLI"
    # Compatibility metadata for existing /api/coders consumers. Discovery,
    # not this legacy list, is authoritative for new model selections.
    models = [
        "",
        "gpt-5.4",
        "gpt-5.3-codex",
        "gpt-5.3-codex-spark",
        "gpt-5.2-codex",
        "gpt-5.4-mini",
        "gpt-5.1-codex-max",
        "gpt-5.1-codex-mini",
        "gpt-5.2",
    ]
    model_setting = ModelSetting(
        config_field="codex_model",
        default_value="",
        default_label="CLI default",
    )
    model_catalog_refreshable = True
    auth_capabilities = CoderAuthCapabilities(
        can_check_cli=True,
        can_check_saved_credentials=True,
        can_report_authentication_mode=True,
        can_verify_service_access=False,
        interactive_login_methods=("browser_oauth", "device_code"),
    )

    def __init__(self, *, discover: Any | None = None) -> None:
        self._discover = discover or discover_codex_models

    def device_login_credential_location(self, *, config: AppConfig) -> str:
        """Return the effective Codex auth directory for conflict checks."""
        env = codex_cli.build_codex_environment(
            codex_home_dir=config.auth.codex_home_dir
        )
        configured = env.get("CODEX_HOME")
        location = (
            Path(configured)
            if configured
            else Path(config.auth.codex_home_dir) / ".codex"
        )
        return str(location.resolve(strict=False))

    def create_device_login(
        self, *, config_path: str = CONFIG_PATH
    ) -> CodexDeviceLoginAdapter:
        """Resolve one immutable CLI device-login context."""
        config = load_config(config_path)
        environment = codex_cli.build_codex_environment(
            codex_home_dir=config.auth.codex_home_dir
        )
        return CodexDeviceLoginAdapter(
            command=("codex", "login", "--device-auth"),
            environment=environment,
            working_directory=str(Path(config_path).absolute().parent),
            credential_location=self.device_login_credential_location(
                config=config
            ),
        )

    def resolve_model(self, daemon_config: "DaemonConfig") -> str:
        """Prefer the plugin-ID setting, including an explicit empty value."""
        return self.model_setting.resolve(self.name, daemon_config)

    def _resolve_reasoning_effort(
        self, daemon_config: "DaemonConfig"
    ) -> str | None:
        """Return the configured override without validating provider choices."""
        plugin_settings = daemon_config.coder_settings.get(self.name)
        if (
            plugin_settings is None
            or _REASONING_EFFORT_SETTING not in plugin_settings
        ):
            return None
        value = plugin_settings[_REASONING_EFFORT_SETTING]
        if not isinstance(value, str):
            raise ValueError(
                "daemon.coder_settings.codex.reasoning_effort must be a string"
            )
        return value or None

    def model_catalog_cache_key(
        self, *, config: AppConfig, config_path: str
    ) -> tuple[str, str]:
        return (
            self.device_login_credential_location(config=config),
            str(Path(config_path).absolute().parent),
        )

    async def get_model_catalog(
        self, *, config: AppConfig, config_path: str
    ) -> ModelCatalog:
        """Discover Codex metadata in the configured CLI auth context."""
        _credential_location, working_directory = self.model_catalog_cache_key(
            config=config,
            config_path=config_path,
        )
        env = codex_cli.build_codex_environment(
            codex_home_dir=config.auth.codex_home_dir
        )
        # The configured CLI session is the supported discovery context. Do
        # not accidentally switch the metadata probe to API billing.
        env.pop("OPENAI_API_KEY", None)
        try:
            discovered = await self._discover(env=env, cwd=working_directory)
        except (CodexModelDiscoveryInvalid, CodexModelDiscoveryUnavailable) as exc:
            raise ModelCatalogUnavailable(
                "Codex CLI model discovery is unavailable"
            ) from exc
        models = tuple(
            ModelMetadata(
                invocation_id=model.identifier,
                display_name=model.display_name,
                is_default=model.is_default,
                default_reasoning_effort=model.default_reasoning_effort,
                reasoning_efforts=tuple(
                    ModelReasoningEffort(effort.name, effort.description)
                    for effort in model.reasoning_efforts
                ),
            )
            for model in discovered
        )
        return ModelCatalog(
            models=models,
            source="discovered",
            description=(
                f"{len(models)} model{'s' if len(models) != 1 else ''} "
                "advertised by Codex CLI."
                if models
                else "Codex CLI advertised no usable models."
            ),
        )

    async def run_planned_pr(
        self,
        repo_path: str,
        model: str | None,
        timeout: int,
        **kwargs: Any,
    ) -> tuple[int, str, str]:
        return await codex_cli.run_planned_pr_async(
            repo_path,
            model=model or None,
            timeout=timeout,
            **kwargs,
        )

    async def run_auto_pr(
        self,
        repo_path: str,
        *,
        pr_id: str,
        task_file: str,
        task_body: str,
        model: str | None = None,
        timeout: int = 900,
        **kwargs: Any,
    ) -> tuple[int, str, str]:
        return await codex_cli.run_auto_pr_async(
            repo_path,
            pr_id,
            task_file,
            task_body,
            model=model or None,
            timeout=timeout,
            **kwargs,
        )

    async def fix_review(
        self,
        repo_path: str,
        model: str | None,
        timeout: int | None = None,
        *,
        pr_id: str | None = None,
        task_file: str | None = None,
        **kwargs: Any,
    ) -> tuple[int, str, str]:
        return await codex_cli.fix_review_async(
            repo_path,
            model=model or None,
            timeout=timeout,
            pr_id=pr_id,
            task_file=task_file,
            **kwargs,
        )

    async def run_prompt(
        self,
        prompt: str,
        repo_path: str,
        model: str | None,
        timeout: int | None,
        on_process_start: Callable[[asyncio.subprocess.Process], None] | None = None,
        on_supervised_process_start: Callable[[SupervisedProcess], None]
        | None = None,
        reasoning_effort: str | None = None,
        environment: dict[str, str] | None = None,
        **_kwargs: Any,
    ) -> tuple[int, str, str]:
        return await codex_cli.run_codex_async(
            prompt,
            repo_path,
            model=model or None,
            reasoning_effort=reasoning_effort,
            environment=environment,
            timeout=timeout,
            on_process_start=on_process_start,
            on_supervised_process_start=on_supervised_process_start,
        )

    def _auth_status(self, **kwargs: Any) -> dict[str, Any]:
        return coder_auth_payload(
            CoderAuthStatus(**kwargs),
            capabilities=self.auth_capabilities,
        )

    def check_auth(
        self,
        *,
        config_path: str = CONFIG_PATH,
        environment: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        """Report saved Codex credentials without verifying service access."""
        env = (
            dict(environment)
            if environment is not None
            else codex_cli.build_codex_environment(
                codex_home_dir=load_config(config_path).auth.codex_home_dir
            )
        )
        version_rc, version_stdout, version_stderr = _run_auth_command(
            ["codex", "--version"], env=env
        )
        version_combined = f"{version_stdout}\n{version_stderr}".strip()
        version = _codex_version(version_combined)
        if version_rc != 0:
            if version_rc == 127 or _reports_missing_cli(version_combined):
                return self._auth_status(
                    status="error",
                    detail="Codex CLI is not installed",
                    cli_available=False,
                    failure_reason="cli_missing",
                )
            if version_rc == 124:
                return self._auth_status(
                    status="error",
                    detail="Codex CLI version check timed out",
                    failure_reason="probe_timeout",
                )
            return self._auth_status(
                status="error",
                detail="Codex CLI version check failed",
                failure_reason="probe_failed",
            )
        if version is None:
            return self._auth_status(
                status="error",
                detail="Codex CLI returned an unrecognized version",
                cli_available=True,
                failure_reason="unrecognized_output",
            )

        installed_detail = f"Codex CLI {version}"
        rc, stdout, stderr = _run_auth_command(
            ["codex", "login", "status"], env=env
        )
        combined = f"{stdout}\n{stderr}".strip()
        if rc == 0:
            mode = _authentication_mode(combined)
            if mode == "workload_identity":
                return self._auth_status(
                    status="ok",
                    detail=(
                        f"{installed_detail}; workload identity selected; "
                        "service access not verified"
                    ),
                    cli_available=True,
                    cli_version=version,
                    authentication_mode=mode,
                )
            if mode is not None:
                mode_label = {
                    "chatgpt": "ChatGPT",
                    "api_key": "API-key",
                    "access_token": "access-token",
                    "bedrock_api_key": "Amazon Bedrock API-key",
                    "bedrock_access_keys": "Amazon Bedrock AWS access-key",
                }[mode]
                return self._auth_status(
                    status="ok",
                    detail=(
                        f"{installed_detail}; saved {mode_label} credentials "
                        "found; service access not verified"
                    ),
                    cli_available=True,
                    cli_version=version,
                    saved_credentials_present=True,
                    authentication_mode=mode,
                )
            return self._auth_status(
                status="error",
                detail=(
                    f"{installed_detail}; saved credential status could not "
                    "be recognized"
                ),
                cli_available=True,
                cli_version=version,
                failure_reason="unrecognized_output",
            )
        if rc == 124:
            return self._auth_status(
                status="error",
                detail=f"{installed_detail}; saved credential check timed out",
                cli_available=True,
                cli_version=version,
                failure_reason="probe_timeout",
            )
        if rc == 127 or _reports_missing_cli(combined):
            return self._auth_status(
                status="error",
                detail="Codex CLI is not installed",
                cli_available=False,
                cli_version=version,
                failure_reason="cli_missing",
            )
        if _first_probe_line(combined).lower() == "not logged in":
            return self._auth_status(
                status="error",
                detail=(
                    f"{installed_detail}; no saved credentials found; "
                    "service access not verified"
                ),
                cli_available=True,
                cli_version=version,
                saved_credentials_present=False,
                failure_reason="credentials_missing",
            )
        return self._auth_status(
            status="error",
            detail=f"{installed_detail}; saved credential check failed",
            cli_available=True,
            cli_version=version,
            failure_reason="probe_failed",
        )

    def create_usage_provider(self, **kwargs: Any) -> UsageProvider:
        cfg = kwargs.pop("config", None)
        if cfg is None:
            cfg = load_config(kwargs.pop("config_path", CONFIG_PATH))
        assert isinstance(cfg, AppConfig)
        credentials_path = kwargs.pop(
            "credentials_path",
            str(
                Path(self.device_login_credential_location(config=cfg))
                / "auth.json"
            ),
        )
        cache_ttl_sec = kwargs.pop(
            "cache_ttl_sec", cfg.daemon.usage_api_cache_ttl_sec
        )
        return OpenAIUsageProvider(
            credentials_path=credentials_path,
            cache_ttl_sec=cache_ttl_sec,
            **kwargs,
        )

    def rate_limit_patterns(self) -> list[re.Pattern[str]]:
        return [_CODEX_RETRY_PATTERN, _CODEX_USAGE_LIMIT_PATTERN]

    @property
    def supports_breach_lifecycle(self) -> bool:
        return False

    @property
    def default_session_pause_percent(self) -> int:
        return 100

    @property
    def default_weekly_pause_percent(self) -> int:
        return 100

    async def diagnose_error(
        self,
        repo_path: str,
        context: str,
        model: str | None,
        on_process_start: Callable[[asyncio.subprocess.Process], None] | None = None,
        on_supervised_process_start: Callable[[SupervisedProcess], None]
        | None = None,
        reasoning_effort: str | None = None,
        environment: dict[str, str] | None = None,
        **_kwargs: Any,
    ) -> tuple[int, str, str]:
        return await codex_cli.diagnose_error_async(
            repo_path,
            context,
            model=model or None,
            reasoning_effort=reasoning_effort,
            environment=environment,
            on_process_start=on_process_start,
            on_supervised_process_start=on_supervised_process_start,
        )

    def build_run_kwargs(
        self,
        *,
        daemon_config: "DaemonConfig",
        breach_dir: str | None = None,
        breach_run_id: str | None = None,
    ) -> dict[str, Any]:
        # breach inputs are accepted for Protocol uniformity but ignored —
        # supports_breach_lifecycle is False, so the plugin emits no
        # breach kwargs even when callers pass them unconditionally.
        kwargs: dict[str, Any] = {"model": self.resolve_model(daemon_config)}
        reasoning_effort = self._resolve_reasoning_effort(daemon_config)
        if reasoning_effort is not None:
            kwargs[_REASONING_EFFORT_SETTING] = reasoning_effort
        return kwargs

    def build_credential_environment(
        self,
        *,
        config: AppConfig,
        credential_location: str,
    ) -> dict[str, str]:
        """Return the environment bound to one reserved credential location."""
        environment = codex_cli.build_codex_environment(
            codex_home_dir=config.auth.codex_home_dir
        )
        environment["CODEX_HOME"] = credential_location
        return environment
