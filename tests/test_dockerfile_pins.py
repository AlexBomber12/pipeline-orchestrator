"""Verify that npm-installed CLIs in the Dockerfile use pinned versions.

PR-168 introduced version pinning for ``@anthropic-ai/claude-code`` and
``@openai/codex`` to protect against silent breakage from upstream CLI
changes. This test guards against accidental regression — a future edit
that drops the version pin should fail here.
"""

from __future__ import annotations

import re
from pathlib import Path

DOCKERFILE = Path(__file__).resolve().parents[1] / "Dockerfile"


def _read_dockerfile() -> str:
    return DOCKERFILE.read_text(encoding="utf-8")


def test_claude_code_install_is_version_pinned() -> None:
    content = _read_dockerfile()
    assert re.search(
        r"npm install -g @anthropic-ai/claude-code@\$\{CLAUDE_CODE_VERSION\}",
        content,
    ), "claude-code npm install must use the CLAUDE_CODE_VERSION arg"


def test_codex_install_is_version_pinned() -> None:
    content = _read_dockerfile()
    assert re.search(
        r"npm i -g --prefix /home/runner/\.npm-global @openai/codex@\$\{CODEX_VERSION\}",
        content,
    ), "codex npm install must use the CODEX_VERSION arg"


def test_version_args_have_concrete_defaults() -> None:
    content = _read_dockerfile()
    claude_match = re.search(r"^ARG CLAUDE_CODE_VERSION=(\S+)$", content, re.MULTILINE)
    codex_match = re.search(r"^ARG CODEX_VERSION=(\S+)$", content, re.MULTILINE)
    assert claude_match, "CLAUDE_CODE_VERSION ARG with default must be declared"
    assert codex_match, "CODEX_VERSION ARG with default must be declared"
    for name, match in (("CLAUDE_CODE_VERSION", claude_match), ("CODEX_VERSION", codex_match)):
        value = match.group(1)
        assert value not in {"latest", ""}, f"{name} default must be a concrete version, not '{value}'"
        assert re.fullmatch(r"\d+\.\d+\.\d+", value), (
            f"{name} default must be a semver triple, got '{value}'"
        )


