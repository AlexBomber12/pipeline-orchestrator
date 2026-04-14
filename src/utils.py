"""Shared utility helpers used across daemon and web packages."""

from __future__ import annotations


def repo_slug_from_url(url: str) -> str:
    """Return canonical ``owner__repo`` slug for a GitHub URL."""
    cleaned = url.strip().rstrip("/")
    if cleaned.endswith(".git"):
        cleaned = cleaned[:-4]
    # Handle SSH URLs: git@github.com:owner/repo
    if ":" in cleaned and not cleaned.startswith("http"):
        cleaned = cleaned.rsplit(":", 1)[-1]
    parts = cleaned.rsplit("/", 2)
    if len(parts) >= 2:
        owner = parts[-2].rsplit("/", 1)[-1]
        repo = parts[-1]
        return f"{owner}__{repo}"
    return parts[-1]


def repo_name_from_url(url: str) -> str:
    """Deprecated: use ``repo_slug_from_url`` instead."""
    return repo_slug_from_url(url)
