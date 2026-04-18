"""Coder plugin implementations."""

from __future__ import annotations

from src.coder_registry import CoderRegistry
from src.coders.claude import ClaudePlugin
from src.coders.codex import CodexPlugin


def build_coder_registry() -> CoderRegistry:
    """Return the default registry used by the daemon."""
    registry = CoderRegistry()
    registry.register(ClaudePlugin())
    registry.register(CodexPlugin())
    return registry
