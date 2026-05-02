"""Coder selection helpers shared across web route modules.

Lives in ``services`` so route modules can depend on it without importing
each other: ``dashboard`` and ``repo_control`` both need to know which
coder a repo currently resolves to, and routing the helper through
``services`` keeps those router modules independent.
"""

from __future__ import annotations

from src.config import AppConfig, RepoConfig


def _effective_coder_name(
    repo_config: RepoConfig | None, config: AppConfig
) -> str:
    """Return the effective coder name for a repo."""
    if repo_config is not None and repo_config.coder is not None:
        return repo_config.coder.value
    return config.daemon.coder.value
