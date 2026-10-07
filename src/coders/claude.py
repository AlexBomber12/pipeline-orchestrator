"""Claude coder plugin."""

from __future__ import annotations

import asyncio
import json
import os
import re
import subprocess
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable

from src import claude_cli
from src.coder_registry import (
    CoderAuthCapabilities,
    CoderAuthStatus,
    ModelCatalog,
    ModelMetadata,
    ModelSetting,
    coder_auth_payload,
)
from src.config import AppConfig, load_config
from src.process_supervisor import SupervisedProcess
from src.usage import OAuthUsageProvider, UsageProvider

if TYPE_CHECKING:
    from src.config import DaemonConfig

CONFIG_PATH = os.environ.get("PO_CONFIG_PATH", "config.yml")
_AUTH_CHECK_TIMEOUT_SEC = 5
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
        default_label="(default)",
    )
    model_catalog_refreshable = False
    auth_capabilities = CoderAuthCapabilities(
        can_check_cli=True,
        can_check_saved_credentials=True,
        can_report_authentication_mode=True,
        can_verify_service_access=False,
        interactive_login_methods=(),
    )

    def resolve_model(self, daemon_config: "DaemonConfig") -> str:
        """Prefer the plugin-ID setting, then legacy ``claude_model``."""
        return (
            self.model_setting.resolve(self.name, daemon_config)
            or self.model_setting.default_value
        )

    def model_catalog_cache_key(
        self, *, config: AppConfig, config_path: str
    ) -> tuple[str, tuple[str, ...]]:
        del config, config_path
        return ("static-compatibility", tuple(self.models))

    async def get_model_catalog(
        self, *, config: AppConfig, config_path: str
    ) -> ModelCatalog:
        """Expose legacy Claude choices without implying live discovery."""
        del config, config_path
        return ModelCatalog(
            models=tuple(
                ModelMetadata(model, model, is_default=model == "opus")
                for model in self.models
            ),
            source="static_compatibility",
            description=(
                "Static compatibility choices; not live or account-verified."
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
        **_kwargs: Any,
    ) -> tuple[int, str, str]:
        return await claude_cli.run_claude_async(
            prompt,
            repo_path,
            model=model,
            timeout=timeout,
            on_process_start=on_process_start,
            on_supervised_process_start=on_supervised_process_start,
            system_prompt_file=None,
        )

    def _auth_status(self, **kwargs: Any) -> dict[str, Any]:
        return coder_auth_payload(
            CoderAuthStatus(**kwargs),
            capabilities=self.auth_capabilities,
        )

    def check_auth(self, *, config_path: str = CONFIG_PATH) -> dict[str, Any]:
        """Report local Claude authentication without testing service access."""
        cfg = load_config(config_path)
        env = {
            **os.environ,
            "CLAUDE_CONFIG_DIR": cfg.auth.claude_config_dir,
        }
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
            str(Path(cfg.auth.claude_config_dir) / ".credentials.json"),
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
        **_kwargs: Any,
    ) -> tuple[int, str, str]:
        return await claude_cli.diagnose_error_async(
            repo_path,
            context,
            model=model,
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
        kwargs: dict[str, Any] = {"model": self.resolve_model(daemon_config)}
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
