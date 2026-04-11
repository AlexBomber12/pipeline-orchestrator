"""Shared utility helpers used across daemon and web packages."""

from __future__ import annotations


def repo_name_from_url(url: str) -> str:
    """Return the repo name (last URL segment without ``.git``)."""
    cleaned = url.rstrip("/")
    last = cleaned.rsplit("/", 1)[-1]
    if last.endswith(".git"):
        last = last[: -len(".git")]
    return last
