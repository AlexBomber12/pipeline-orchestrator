"""Codex coder plugin."""

from __future__ import annotations

import os
import re
import subprocess
from pathlib import Path
from typing import Any

from src import codex_cli
from src.config import AppConfig, load_config
from src.usage import OpenAIUsageProvider, UsageProvider

CONFIG_PATH = "config.yml"
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

    async def fix_review(
        self,
        repo_path: str,
        model: str | None,
        timeout: int | None,
        **kwargs: Any,
    ) -> tuple[int, str, str]:
        return await codex_cli.fix_review_async(
            repo_path,
            model=model or None,
            timeout=timeout,
            **kwargs,
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
