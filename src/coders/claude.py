"""Claude coder plugin."""

from __future__ import annotations

import os
import re
import subprocess
from pathlib import Path
from typing import Any

from src import claude_cli
from src.config import AppConfig, load_config
from src.usage import OAuthUsageProvider, UsageProvider

CONFIG_PATH = "config.yml"
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

    async def fix_review(
        self,
        repo_path: str,
        model: str | None,
        timeout: int | None,
        **kwargs: Any,
    ) -> tuple[int, str, str]:
        return await claude_cli.fix_review_async(
            repo_path,
            model=model,
            timeout=timeout,
            **kwargs,
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
