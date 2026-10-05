"""Codex coder plugin."""

from __future__ import annotations

import asyncio
import os
import re
import subprocess
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable

from src import codex_cli
from src.coder_registry import (
    ModelCatalog,
    ModelCatalogUnavailable,
    ModelMetadata,
    ModelReasoningEffort,
    ModelSetting,
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


def _auth_probe_env(**overrides: str) -> dict[str, str]:
    env = dict(os.environ)
    env.update(overrides)
    return env


def _first_probe_line(text: str) -> str:
    for line in text.splitlines():
        stripped = line.strip()
        if stripped and not stripped.lower().startswith("warning:"):
            return stripped
    return ""


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

    def __init__(self, *, discover: Any | None = None) -> None:
        self._discover = discover or discover_codex_models

    def resolve_model(self, daemon_config: "DaemonConfig") -> str:
        """Prefer the plugin-ID setting, including an explicit empty value."""
        return self.model_setting.resolve(self.name, daemon_config)

    def model_catalog_cache_key(
        self, *, config: AppConfig, config_path: str
    ) -> tuple[str, str]:
        return (
            config.auth.codex_home_dir,
            str(Path(config_path).absolute().parent),
        )

    async def get_model_catalog(
        self, *, config: AppConfig, config_path: str
    ) -> ModelCatalog:
        """Discover Codex metadata in the configured CLI auth context."""
        home_dir, working_directory = self.model_catalog_cache_key(
            config=config,
            config_path=config_path,
        )
        env = dict(os.environ)
        env["HOME"] = home_dir
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
    ) -> tuple[int, str, str]:
        return await codex_cli.run_codex_async(
            prompt,
            repo_path,
            model=model or None,
            timeout=timeout,
            on_process_start=on_process_start,
            on_supervised_process_start=on_supervised_process_start,
        )

    def check_auth(self, *, config_path: str = CONFIG_PATH) -> dict[str, str]:
        cfg = load_config(config_path)
        env = _auth_probe_env(HOME=cfg.auth.codex_home_dir)
        version_rc, version_stdout, version_stderr = _run_auth_command(
            ["codex", "--version"], env=env
        )
        version_combined = f"{version_stdout}\n{version_stderr}".strip()
        version_line = _first_probe_line(version_combined)
        if version_rc != 0:
            if (
                "not found" in version_combined.lower()
                or "no such file" in version_combined.lower()
            ):
                return {"status": "error", "detail": "codex CLI not installed"}
            detail = version_line or "codex CLI not installed"
            return {"status": "error", "detail": detail}

        installed_detail = (
            f"{version_line} (installed)"
            if version_line
            else "codex CLI installed"
        )
        rc, stdout, stderr = _run_auth_command(
            ["codex", "login", "status"], env=env
        )
        combined = f"{stdout}\n{stderr}".strip()
        if rc == 0:
            detail = _first_probe_line(combined) or "codex authenticated"
            return {"status": "ok", "detail": f"{installed_detail}; {detail}"}
        if "not found" in combined.lower() or "no such file" in combined.lower():
            return {"status": "error", "detail": "codex CLI not installed"}
        api_key = env.get("OPENAI_API_KEY", "")
        base_detail = _first_probe_line(combined) or "codex not authenticated"
        if api_key:
            base_detail = f"{base_detail} (OPENAI_API_KEY set but unverified)"
        return {"status": "error", "detail": f"{installed_detail}; {base_detail}"}

    def create_usage_provider(self, **kwargs: Any) -> UsageProvider:
        cfg = kwargs.pop("config", None)
        if cfg is None:
            cfg = load_config(kwargs.pop("config_path", CONFIG_PATH))
        assert isinstance(cfg, AppConfig)
        credentials_path = kwargs.pop(
            "credentials_path",
            str(Path(cfg.auth.codex_home_dir) / ".codex" / "auth.json"),
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
    ) -> tuple[int, str, str]:
        return await codex_cli.diagnose_error_async(
            repo_path,
            context,
            model=model or None,
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
        return {"model": self.resolve_model(daemon_config)}
