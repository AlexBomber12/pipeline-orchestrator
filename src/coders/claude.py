"""Claude coder plugin."""

from __future__ import annotations

import asyncio
import json
import os
import re
import subprocess
from dataclasses import replace
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable, Mapping

from src import claude_cli
from src.coder_registry import (
    CoderAuthCapabilities,
    CoderAuthStatus,
    ModelCatalog,
    ModelCatalogUnavailable,
    ModelSetting,
    coder_auth_payload,
)
from src.coders.claude_login import (
    ClaudeBrowserLoginAdapter,
    supports_browser_login_version,
)
from src.coders.claude_models import (
    ClaudeModelDiscoveryInvalid,
    ClaudeModelDiscoveryUnavailable,
    discover_claude_models,
)
from src.config import AppConfig, load_config
from src.process_supervisor import SupervisedProcess
from src.usage import OAuthUsageProvider, UsageProvider

if TYPE_CHECKING:
    from src.config import DaemonConfig

CONFIG_PATH = os.environ.get("PO_CONFIG_PATH", "config.yml")
_AUTH_CHECK_TIMEOUT_SEC = 5
_REASONING_EFFORT_SETTING = "reasoning_effort"
_CLAUDE_VERSION_PATTERN = re.compile(
    r"^(?:(?:claude|claude-code)\s+)?"
    r"(?P<version>[0-9]+\.[0-9]+\.[0-9]+(?:[-+][0-9A-Za-z.-]+)?)"
    r"(?:\s+\(Claude Code\))?$",
    re.IGNORECASE,
)
_AUTH_METHODS = {
    "claude.ai": (
        "claude_ai",
        "saved Claude account credentials found",
        True,
    ),
    "oauth_token": (
        "oauth_token",
        "configured OAuth-token authentication found",
        None,
    ),
    "api_key": (
        "api_key",
        "configured API-key authentication found",
        None,
    ),
    "api_key_helper": (
        "api_key_helper",
        "configured API-key helper authentication found",
        None,
    ),
    "third_party": (
        "third_party",
        "configured third-party authentication found",
        None,
    ),
}
_UNSUPPORTED_AUTH_STATUS_PATTERNS = (
    re.compile(
        r"(?:unknown|unrecognized|invalid) (?:command|subcommand)(?::)?\s*"
        r"[\"']?(?:auth|status)\b",
        re.IGNORECASE,
    ),
    re.compile(
        r"\b(?:auth|status)\b is not (?:a )?(?:known|valid|supported) "
        r"(?:command|subcommand)\b",
        re.IGNORECASE,
    ),
)
_ANTHROPIC_RATE_LIMIT_PATTERN = re.compile(
    r"(\d{1,3})%\s*(?:of\s+)?(?:your\s+)?(?:(weekly|week|session|5-hour)\s+)?rate\s*limit"
    r"|(?:(weekly|week|session|5-hour)\s+)?rate\s*limit\s+(?:at\s+)?(\d{1,3})%",
    re.IGNORECASE,
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


def _claude_version(text: str) -> str | None:
    match = _CLAUDE_VERSION_PATTERN.fullmatch(_first_probe_line(text))
    return match.group("version") if match is not None else None


def _reports_missing_cli(text: str) -> bool:
    line = _first_probe_line(text).lower()
    return line in {"claude: not found", "claude not found"} or line.endswith(
        ": no such file or directory"
    )


def _auth_status_unsupported(text: str) -> bool:
    return any(
        pattern.search(text) is not None
        for pattern in _UNSUPPORTED_AUTH_STATUS_PATTERNS
    )


def _parse_auth_status(text: str) -> tuple[bool, str] | None:
    try:
        value = json.loads(text)
    except json.JSONDecodeError:
        return None
    if not isinstance(value, dict):
        return None
    logged_in = value.get("loggedIn")
    auth_method = value.get("authMethod")
    if not isinstance(logged_in, bool) or not isinstance(auth_method, str):
        return None
    return logged_in, auth_method


class ClaudePlugin:
    name = "claude"
    display_name = "Claude Code"
    models = ["opus", "sonnet"]
    model_setting = ModelSetting(
        config_field="claude_model",
        default_value="opus",
        default_label="Application default (opus)",
    )
    model_catalog_refreshable = True
    auth_capabilities = CoderAuthCapabilities(
        can_check_cli=True,
        can_check_saved_credentials=True,
        can_report_authentication_mode=True,
        can_verify_service_access=False,
        interactive_login_methods=(),
    )

    def __init__(self, *, discover: Any | None = None) -> None:
        self._discover = discover or discover_claude_models

    def resolve_model(self, daemon_config: "DaemonConfig") -> str:
        """Prefer the plugin-ID setting, then legacy ``claude_model``."""
        return (
            self.model_setting.resolve(self.name, daemon_config)
            or self.model_setting.default_value
        )

    def browser_login_credential_location(self, *, config: AppConfig) -> str:
        """Return the normalized Claude auth directory without side effects."""
        return str(
            Path(config.auth.claude_config_dir)
            .expanduser()
            .resolve(strict=False)
        )

    def build_credential_environment(
        self,
        *,
        config: AppConfig,
        credential_location: str,
    ) -> dict[str, str]:
        """Return a fresh environment bound to one credential location."""
        del config
        return claude_cli.build_claude_environment(
            claude_config_dir=credential_location
        )

    def create_browser_login(
        self,
        *,
        credential_location: str,
        environment: Mapping[str, str],
        config_path: str,
        observed_cli_version: str,
    ) -> ClaudeBrowserLoginAdapter:
        """Describe login using only the daemon's captured probe context."""
        return ClaudeBrowserLoginAdapter(
            environment=environment,
            working_directory=str(Path(config_path).absolute().parent),
            credential_location=credential_location,
            observed_cli_version=observed_cli_version,
        )

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
                "daemon.coder_settings.claude.reasoning_effort must be a string"
            )
        return value or None

    def model_catalog_cache_key(
        self, *, config: AppConfig, config_path: str
    ) -> tuple[str, str]:
        return (
            self.browser_login_credential_location(config=config),
            str(Path(config_path).absolute().parent),
        )

    async def get_model_catalog(
        self,
        *,
        config: AppConfig,
        config_path: str,
        environment: dict[str, str] | None = None,
    ) -> ModelCatalog:
        """Discover Claude metadata in the configured CLI auth context."""
        _credential_directory, working_directory = self.model_catalog_cache_key(
            config=config,
            config_path=config_path,
        )
        env = (
            dict(environment)
            if environment is not None
            else self.build_credential_environment(
                config=config,
                credential_location=self.browser_login_credential_location(
                    config=config
                ),
            )
        )
        try:
            models = await self._discover(env=env, cwd=working_directory)
        except ClaudeModelDiscoveryUnavailable as exc:
            raise ModelCatalogUnavailable(
                "Claude CLI model discovery is unavailable",
                managed=exc.managed,
                cleanup_result=exc.cleanup_result,
            ) from exc
        except ClaudeModelDiscoveryInvalid as exc:
            raise ModelCatalogUnavailable(
                "Claude CLI model discovery is unavailable"
            ) from exc
        return ModelCatalog(
            models=models,
            source="discovered",
            description=(
                f"{len(models)} model{'s' if len(models) != 1 else ''} "
                "advertised by Claude CLI; service access and account "
                "entitlement are not verified."
                if models
                else "Claude CLI advertised no usable models."
            ),
        )

    async def run_planned_pr(
        self,
        repo_path: str,
        model: str | None,
        timeout: int,
        **kwargs: Any,
    ) -> tuple[int, str, str]:
        return await claude_cli.run_planned_pr_async(
            repo_path,
            model=model,
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
        return await claude_cli.run_auto_pr_async(
            repo_path,
            pr_id,
            task_file,
            task_body,
            model=model,
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
        return await claude_cli.fix_review_async(
            repo_path,
            model=model,
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
        kwargs: dict[str, Any] = {
            "model": model,
            "timeout": timeout,
            "on_process_start": on_process_start,
            "on_supervised_process_start": on_supervised_process_start,
            "system_prompt_file": None,
        }
        if reasoning_effort:
            kwargs[_REASONING_EFFORT_SETTING] = reasoning_effort
        if environment is not None:
            kwargs["environment"] = environment
        return await claude_cli.run_claude_async(
            prompt,
            repo_path,
            **kwargs,
        )

    def _auth_status(self, **kwargs: Any) -> dict[str, Any]:
        status = CoderAuthStatus(**kwargs)
        capabilities = self.auth_capabilities
        if status.cli_available is True and supports_browser_login_version(status.cli_version):
            capabilities = replace(capabilities, interactive_login_methods=("browser_code",))
        return coder_auth_payload(status, capabilities=capabilities)

    def check_auth(
        self,
        *,
        config_path: str = CONFIG_PATH,
        environment: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        """Report local Claude authentication without testing service access."""
        if environment is not None:
            env = dict(environment)
        else:
            cfg = load_config(config_path)
            env = self.build_credential_environment(
                config=cfg,
                credential_location=self.browser_login_credential_location(
                    config=cfg
                ),
            )
        version_rc, version_stdout, version_stderr = _run_auth_command(
            ["claude", "--version"], env=env
        )
        version_output = f"{version_stdout}\n{version_stderr}".strip()
        version = _claude_version(version_output)
        if version_rc != 0:
            if version_rc == 127 or _reports_missing_cli(version_output):
                return self._auth_status(
                    status="error",
                    detail="Claude Code CLI was not found",
                    cli_available=False,
                    failure_reason="cli_missing",
                )
            if version_rc == 124:
                return self._auth_status(
                    status="error",
                    detail="Claude Code CLI version check timed out",
                    failure_reason="probe_timeout",
                )
            return self._auth_status(
                status="error",
                detail="Claude Code CLI version check failed",
                failure_reason="probe_failed",
            )
        if version is None:
            return self._auth_status(
                status="error",
                detail="Claude Code CLI returned an unrecognized version",
                cli_available=True,
                failure_reason="unrecognized_output",
            )

        installed_detail = f"Claude Code CLI {version}"
        status_rc, status_stdout, status_stderr = _run_auth_command(
            ["claude", "auth", "status"], env=env
        )
        status_output = f"{status_stdout}\n{status_stderr}".strip()
        if status_rc == 127 or _reports_missing_cli(status_output):
            return self._auth_status(
                status="error",
                detail="Claude Code CLI was not found",
                cli_available=False,
                cli_version=version,
                failure_reason="cli_missing",
            )
        if status_rc == 124:
            return self._auth_status(
                status="error",
                detail=f"{installed_detail}; authentication status check timed out",
                cli_available=True,
                cli_version=version,
                failure_reason="probe_timeout",
            )
        if _auth_status_unsupported(status_output):
            return self._auth_status(
                status="error",
                detail=f"{installed_detail}; authentication status is unsupported",
                cli_available=True,
                cli_version=version,
                failure_reason="probe_unavailable",
            )
        if status_rc not in {0, 1}:
            return self._auth_status(
                status="error",
                detail=f"{installed_detail}; authentication status check failed",
                cli_available=True,
                cli_version=version,
                failure_reason="probe_failed",
            )

        parsed_status = _parse_auth_status(status_stdout.strip())
        if parsed_status is None:
            return self._auth_status(
                status="error",
                detail=f"{installed_detail}; authentication status was not recognized",
                cli_available=True,
                cli_version=version,
                failure_reason="unrecognized_output",
            )
        logged_in, provider_method = parsed_status
        if (
            logged_in
            and (status_rc != 0 or provider_method == "none")
        ) or (
            not logged_in
            and (status_rc != 1 or provider_method != "none")
        ):
            return self._auth_status(
                status="error",
                detail=f"{installed_detail}; authentication status was inconsistent",
                cli_available=True,
                cli_version=version,
                failure_reason="probe_failed",
            )
        if not logged_in:
            return self._auth_status(
                status="error",
                detail=(
                    f"{installed_detail}; no active authentication reported; "
                    "service access not verified"
                ),
                cli_available=True,
                cli_version=version,
                failure_reason="credentials_missing",
            )

        method = _AUTH_METHODS.get(provider_method)
        if method is None:
            return self._auth_status(
                status="error",
                detail=f"{installed_detail}; authentication method was not recognized",
                cli_available=True,
                cli_version=version,
                failure_reason="unrecognized_output",
            )
        authentication_mode, evidence_detail, saved_credentials = method
        return self._auth_status(
            status="ok",
            detail=(
                f"{installed_detail}; {evidence_detail}; "
                "service access not verified"
            ),
            cli_available=True,
            cli_version=version,
            saved_credentials_present=saved_credentials,
            authentication_mode=authentication_mode,
        )

    def create_usage_provider(self, **kwargs: Any) -> UsageProvider:
        cfg = kwargs.pop("config", None)
        if cfg is None:
            cfg = load_config(kwargs.pop("config_path", CONFIG_PATH))
        assert isinstance(cfg, AppConfig)
        credentials_path = kwargs.pop(
            "credentials_path",
            str(
                Path(self.browser_login_credential_location(config=cfg))
                / ".credentials.json"
            ),
        )
        user_agent = kwargs.pop("user_agent", cfg.daemon.usage_api_user_agent)
        beta_header = kwargs.pop("beta_header", cfg.daemon.usage_api_beta_header)
        cache_ttl_sec = kwargs.pop(
            "cache_ttl_sec", cfg.daemon.usage_api_cache_ttl_sec
        )
        return OAuthUsageProvider(
            credentials_path=credentials_path,
            user_agent=user_agent,
            beta_header=beta_header,
            cache_ttl_sec=cache_ttl_sec,
            **kwargs,
        )

    def rate_limit_patterns(self) -> list[re.Pattern[str]]:
        return [_ANTHROPIC_RATE_LIMIT_PATTERN]

    @property
    def supports_breach_lifecycle(self) -> bool:
        return True

    @property
    def default_session_pause_percent(self) -> int:
        return 95

    @property
    def default_weekly_pause_percent(self) -> int:
        return 80

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
        kwargs: dict[str, Any] = {
            "model": model,
            "on_process_start": on_process_start,
            "on_supervised_process_start": on_supervised_process_start,
        }
        if reasoning_effort:
            kwargs[_REASONING_EFFORT_SETTING] = reasoning_effort
        if environment is not None:
            kwargs["environment"] = environment
        return await claude_cli.diagnose_error_async(
            repo_path,
            context,
            **kwargs,
        )

    def build_run_kwargs(
        self,
        *,
        daemon_config: "DaemonConfig",
        breach_dir: str | None = None,
        breach_run_id: str | None = None,
    ) -> dict[str, Any]:
        kwargs: dict[str, Any] = {"model": self.resolve_model(daemon_config)}
        reasoning_effort = self._resolve_reasoning_effort(daemon_config)
        if reasoning_effort is not None:
            kwargs[_REASONING_EFFORT_SETTING] = reasoning_effort
        if breach_dir is not None and breach_run_id is not None:
            kwargs["breach_dir"] = breach_dir
            kwargs["breach_run_id"] = breach_run_id
            kwargs["session_threshold"] = (
                daemon_config.rate_limit_session_pause_percent
            )
            kwargs["weekly_threshold"] = (
                daemon_config.rate_limit_weekly_pause_percent
            )
        return kwargs
