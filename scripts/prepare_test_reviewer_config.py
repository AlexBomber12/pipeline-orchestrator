#!/usr/bin/env python3
"""Resolve the integration reviewer App bot and trust it in test config."""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import quote

import yaml


class ReviewerConfigError(RuntimeError):
    """Raised when the reviewer identity or test config cannot be prepared."""


@dataclass(frozen=True)
class ReviewerIdentity:
    user_id: int
    login: str


_APP_SLUG_PATTERN = re.compile(r"[A-Za-z0-9](?:[A-Za-z0-9-]{0,98}[A-Za-z0-9])?")


def reviewer_bot_login(app_slug: str) -> str:
    """Return the GitHub bot login belonging to a validated App slug."""
    if not _APP_SLUG_PATTERN.fullmatch(app_slug):
        raise ReviewerConfigError("reviewer App slug is missing or malformed")
    return f"{app_slug}[bot]"


def resolve_reviewer_identity(app_slug: str) -> ReviewerIdentity:
    """Resolve and validate the bot user for ``app_slug`` through ``gh api``."""
    expected_login = reviewer_bot_login(app_slug)
    api_path = f"users/{quote(expected_login, safe='')}"
    completed = subprocess.run(
        ["gh", "api", api_path],
        capture_output=True,
        text=True,
        check=False,
    )
    if completed.returncode != 0:
        raise ReviewerConfigError(
            "GitHub bot-user lookup failed for the reviewer App "
            f"(exit {completed.returncode})"
        )
    try:
        payload = json.loads(completed.stdout)
    except json.JSONDecodeError as exc:
        raise ReviewerConfigError(
            "GitHub bot-user lookup returned malformed JSON"
        ) from exc
    if not isinstance(payload, dict):
        raise ReviewerConfigError("GitHub bot-user lookup returned a non-object")

    user_id = payload.get("id")
    login = payload.get("login")
    user_type = payload.get("type")
    if isinstance(user_id, bool) or not isinstance(user_id, int) or user_id <= 0:
        raise ReviewerConfigError(
            "GitHub bot-user lookup returned an invalid numeric user ID"
        )
    if not isinstance(login, str) or login.casefold() != expected_login.casefold():
        raise ReviewerConfigError(
            "GitHub bot-user lookup returned a login that does not match the reviewer App"
        )
    if not isinstance(user_type, str) or user_type.casefold() != "bot":
        raise ReviewerConfigError(
            "GitHub bot-user lookup did not resolve to a bot account"
        )
    return ReviewerIdentity(user_id=user_id, login=login)


def add_reviewer_to_test_config(
    config_path: Path,
    identity: ReviewerIdentity,
) -> None:
    """Append ``identity`` without replacing the test config's existing reviewers."""
    try:
        document = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    except (OSError, yaml.YAMLError) as exc:
        raise ReviewerConfigError(f"could not read test config: {config_path}") from exc
    if not isinstance(document, dict):
        raise ReviewerConfigError("test config root must be a mapping")
    daemon = document.get("daemon")
    if not isinstance(daemon, dict):
        raise ReviewerConfigError("test config daemon section must be a mapping")
    identities = daemon.get("trusted_reviewer_identities")
    if not isinstance(identities, list) or not identities:
        raise ReviewerConfigError(
            "test config must define at least one baseline trusted reviewer identity"
        )
    if any(not isinstance(item, dict) for item in identities):
        raise ReviewerConfigError("test config reviewer identities must be mappings")

    exact_match = any(
        item.get("user_id") == identity.user_id
        and isinstance(item.get("login"), str)
        and item["login"].casefold() == identity.login.casefold()
        for item in identities
    )
    conflicting_match = any(
        item.get("user_id") == identity.user_id
        or (
            isinstance(item.get("login"), str)
            and item["login"].casefold() == identity.login.casefold()
        )
        for item in identities
    )
    if conflicting_match and not exact_match:
        raise ReviewerConfigError(
            "resolved reviewer bot conflicts with an existing test identity"
        )
    if not exact_match:
        identities.append(
            {"user_id": identity.user_id, "login": identity.login}
        )

    config_path.write_text(
        yaml.safe_dump(document, sort_keys=False),
        encoding="utf-8",
    )


def _write_github_output(path: Path, identity: ReviewerIdentity) -> None:
    with path.open("a", encoding="utf-8") as output:
        output.write(f"user-id={identity.user_id}\n")
        output.write(f"login={identity.login}\n")


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Trust the secret-selected reviewer App bot in config.test.yml."
    )
    parser.add_argument("--app-slug", required=True)
    parser.add_argument(
        "--config",
        type=Path,
        default=Path("config.test.yml"),
    )
    parser.add_argument("--github-output", type=Path)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    try:
        identity = resolve_reviewer_identity(args.app_slug)
        add_reviewer_to_test_config(args.config, identity)
        if args.github_output is not None:
            _write_github_output(args.github_output, identity)
    except ReviewerConfigError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    print(
        f"Prepared {args.config} for reviewer bot {identity.login} "
        f"(GitHub user ID {identity.user_id})"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
