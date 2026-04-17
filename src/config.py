"""Configuration loader and Pydantic models for the pipeline orchestrator."""

from __future__ import annotations

import logging
import os
import tempfile
from enum import Enum
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, Field, field_validator


class CoderType(str, Enum):
    CLAUDE = "claude"
    CODEX = "codex"

logger = logging.getLogger(__name__)

_REPO_FIELDS = {
    "url",
    "branch",
    "auto_merge",
    "review_timeout_min",
    "active",
    "poll_interval_sec",
    "allow_merge_without_checks",
    "coder",
}

_DAEMON_FIELDS = {
    "poll_interval_sec",
    "review_timeout_min",
    "hung_fallback_codex_review",
    "error_handler_use_ai",
    "claude_model",
    "fix_idle_timeout_sec",
    "planned_pr_timeout_sec",
    "rate_limit_session_pause_percent",
    "rate_limit_weekly_pause_percent",
    "strict_queue_validation",
    "upload_staging_max_age_hours",
    "usage_api_user_agent",
    "usage_api_beta_header",
    "usage_api_cache_ttl_sec",
    "install_statusline_hook",
    "coder",
    "codex_model",
}


class RepoConfig(BaseModel):
    url: str
    branch: str = "main"
    auto_merge: bool = True
    # Optional per-repo override. ``None`` means "inherit
    # ``daemon.review_timeout_min``": the runner's hung-detection logic
    # falls back to the daemon-level setting whenever the repo itself
    # does not pin a timeout, so PR-016's "Default review timeout" UI
    # control actually steers every repo that has not opted into a
    # custom value.
    review_timeout_min: int | None = None
    active: bool = True
    poll_interval_sec: int = 60
    allow_merge_without_checks: bool = False
    coder: CoderType | None = None

    @field_validator("poll_interval_sec", mode="before")
    @classmethod
    def _poll_interval_at_least_one(cls, v: Any) -> int:
        if v is None:
            return 60
        if not isinstance(v, int) or isinstance(v, bool):
            raise ValueError("poll_interval_sec must be an integer")
        if v < 1:
            raise ValueError("poll_interval_sec must be at least 1")
        return v


class DaemonConfig(BaseModel):
    poll_interval_sec: int = 60
    review_timeout_min: int = 60
    hung_fallback_codex_review: bool = True
    error_handler_use_ai: bool = True
    claude_model: str = "opus"
    fix_idle_timeout_sec: int = Field(default=1800, ge=1)
    planned_pr_timeout_sec: int = 900
    rate_limit_session_pause_percent: int = 95
    rate_limit_weekly_pause_percent: int = 100
    strict_queue_validation: bool = True
    upload_staging_max_age_hours: int = 24
    usage_api_user_agent: str = "claude-code/2.1.104"
    usage_api_beta_header: str = "oauth-2025-04-20"
    usage_api_cache_ttl_sec: int = Field(default=60, ge=5, le=3600)
    install_statusline_hook: bool = True
    coder: CoderType = CoderType.CLAUDE
    codex_model: str = "o3"


class WebConfig(BaseModel):
    host: str = "0.0.0.0"
    port: int = 8000


class AuthConfig(BaseModel):
    claude_config_dir: str = "/data/auth/claude"
    gh_config_dir: str = "/data/auth/gh"


class AppConfig(BaseModel):
    repositories: list[RepoConfig] = Field(default_factory=list)
    daemon: DaemonConfig = Field(default_factory=DaemonConfig)
    web: WebConfig = Field(default_factory=WebConfig)
    auth: AuthConfig = Field(default_factory=AuthConfig)


def load_config(path: str = "config.yml") -> AppConfig:
    """Read a YAML config file and return an AppConfig.

    If the file is missing, return an AppConfig populated with defaults.
    """
    config_path = Path(path)
    if not config_path.is_file():
        return AppConfig()

    with config_path.open("r", encoding="utf-8") as fh:
        raw = yaml.safe_load(fh) or {}

    daemon = raw.get("daemon")
    if isinstance(daemon, dict):
        legacy = daemon.pop("fix_review_timeout_sec", None)
        if legacy is not None and "fix_idle_timeout_sec" not in daemon:
            daemon["fix_idle_timeout_sec"] = legacy

        legacy_rate = daemon.pop("rate_limit_pause_percent", None)
        if legacy_rate is not None:
            logger.warning(
                "Deprecated config field 'rate_limit_pause_percent' — "
                "use 'rate_limit_session_pause_percent' and "
                "'rate_limit_weekly_pause_percent' instead"
            )
            if "rate_limit_session_pause_percent" not in daemon:
                daemon["rate_limit_session_pause_percent"] = legacy_rate

    return AppConfig.model_validate(raw)


def normalize_repo_url(url: str) -> str:
    """Return a canonical form of ``url`` for equality comparisons.

    Strips trailing slashes and a ``.git`` suffix so that
    ``https://github.com/o/r``, ``https://github.com/o/r/`` and
    ``https://github.com/o/r.git`` all compare equal.
    """
    cleaned = url.strip().rstrip("/")
    if cleaned.endswith(".git"):
        cleaned = cleaned[: -len(".git")]
    return cleaned


def save_config(config: AppConfig, path: str = "config.yml") -> None:
    """Serialize ``config`` to YAML at ``path`` atomically.

    Writes to a temporary file in the target directory first and then
    renames it into place via ``os.replace`` so a crash mid-write cannot
    leave a half-written ``config.yml`` behind.
    """
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)

    # ``exclude_none=True`` keeps optional fields (``RepoConfig.review_timeout_min``)
    # out of the on-disk YAML when they are unset. Otherwise they would be
    # serialized as ``null``, which is both ugly and ambiguous on re-read.
    payload = config.model_dump(mode="json", exclude_none=True)
    yaml_text = yaml.dump(payload, default_flow_style=False, sort_keys=False)

    fd, tmp_path = tempfile.mkstemp(
        prefix=target.name + ".", suffix=".tmp", dir=str(target.parent)
    )
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            fh.write(yaml_text)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp_path, target)
    except Exception:
        # Best-effort cleanup of the tmp file if the replace never happened.
        try:
            os.unlink(tmp_path)
        except FileNotFoundError:
            pass
        raise


def _find_repo_index(config: AppConfig, url: str) -> int:
    """Return the index of ``url`` in ``config.repositories`` or ``-1``."""
    needle = normalize_repo_url(url)
    for idx, repo in enumerate(config.repositories):
        if normalize_repo_url(repo.url) == needle:
            return idx
    return -1


def add_repository(
    url: str, path: str = "config.yml", **overrides: Any
) -> AppConfig:
    """Append a repository to ``config.yml`` and return the updated config.

    Raises ``ValueError`` if ``url`` (normalized) is already configured or
    if ``overrides`` contains an unknown field.
    """
    unknown = set(overrides) - (_REPO_FIELDS - {"url"})
    if unknown:
        raise ValueError(f"Unknown repository fields: {sorted(unknown)}")

    config = load_config(path)
    if _find_repo_index(config, url) >= 0:
        raise ValueError(f"Repository already configured: {url}")

    repo = RepoConfig(url=url, **overrides)
    config.repositories.append(repo)
    save_config(config, path)
    return config


def remove_repository(url: str, path: str = "config.yml") -> AppConfig:
    """Remove a repository from ``config.yml`` and return the updated config.

    Raises ``ValueError`` if ``url`` (normalized) is not configured.
    """
    config = load_config(path)
    idx = _find_repo_index(config, url)
    if idx < 0:
        raise ValueError(f"Repository not found: {url}")

    config.repositories.pop(idx)
    save_config(config, path)
    return config


def update_repository(
    url: str, path: str = "config.yml", **updates: Any
) -> AppConfig:
    """Update fields on an existing repository and return the updated config.

    Only known fields on :class:`RepoConfig` may be updated; ``url`` itself
    is immutable here. Raises ``ValueError`` if the repo does not exist or
    an unknown field is supplied.
    """
    unknown = set(updates) - (_REPO_FIELDS - {"url"})
    if unknown:
        raise ValueError(f"Unknown repository fields: {sorted(unknown)}")

    config = load_config(path)
    idx = _find_repo_index(config, url)
    if idx < 0:
        raise ValueError(f"Repository not found: {url}")

    existing = config.repositories[idx]
    # model_copy(update=...) does NOT re-run validators in Pydantic v2, so
    # rebuild via model_validate to reject malformed patches before writing
    # anything to disk.
    merged = RepoConfig.model_validate({**existing.model_dump(), **updates})
    config.repositories[idx] = merged
    save_config(config, path)
    return config


def update_daemon_config(
    path: str = "config.yml", **updates: Any
) -> AppConfig:
    """Update fields on ``daemon:`` and return the updated config.

    Raises ``ValueError`` if ``updates`` contains an unknown field.
    """
    unknown = set(updates) - _DAEMON_FIELDS
    if unknown:
        raise ValueError(f"Unknown daemon fields: {sorted(unknown)}")

    config = load_config(path)
    # Same reasoning as update_repository: go through model_validate so a
    # malformed patch raises instead of corrupting the on-disk config.
    config.daemon = DaemonConfig.model_validate(
        {**config.daemon.model_dump(), **updates}
    )
    save_config(config, path)
    return config
