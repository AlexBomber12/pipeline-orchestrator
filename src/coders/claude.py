"""Claude coder plugin."""

from __future__ import annotations

import asyncio
import os
import re
import subprocess
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable

from src import claude_cli
from src.coder_registry import ModelCatalog, ModelMetadata, ModelSetting
from src.config import AppConfig, load_config
from src.process_supervisor import SupervisedProcess
from src.usage import OAuthUsageProvider, UsageProvider

if TYPE_CHECKING:
    from src.config import DaemonConfig

CONFIG_PATH = os.environ.get("PO_CONFIG_PATH", "config.yml")
_AUTH_CHECK_TIMEOUT_SEC = 5
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

    def check_auth(self, *, config_path: str = CONFIG_PATH) -> dict[str, str]:
        cfg = load_config(config_path)
        env = {
            **os.environ,
            "CLAUDE_CONFIG_DIR": cfg.auth.claude_config_dir,
        }
        rc, stdout, stderr = _run_auth_command(["claude", "--version"], env=env)
        if rc == 0:
            output = (stdout or stderr).strip()
            detail = output.splitlines()[0] if output else "claude CLI available"
            return {"status": "ok", "detail": detail}
        detail = (stderr or stdout).strip() or "claude CLI not available"
        return {"status": "error", "detail": detail}

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
