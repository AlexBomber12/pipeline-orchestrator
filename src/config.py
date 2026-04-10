"""Configuration loader and Pydantic models for the pipeline orchestrator."""

from __future__ import annotations

from pathlib import Path

import yaml
from pydantic import BaseModel, Field


class RepoConfig(BaseModel):
    url: str
    branch: str = "main"
    auto_merge: bool = True
    review_timeout_min: int = 60
    poll_interval_sec: int = 60


class DaemonConfig(BaseModel):
    poll_interval_sec: int = 60
    review_timeout_min: int = 60
    hung_fallback_codex_review: bool = True
    error_handler_use_ai: bool = True


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

    return AppConfig.model_validate(raw)
