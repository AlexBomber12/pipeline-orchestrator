#!/usr/bin/env python3
"""Install the prepared integration config in the stack's shared /data mount."""

from __future__ import annotations

import argparse
import os
import re
import tempfile
from pathlib import Path

import yaml


def prepare_test_config(
    source: Path, destination: Path, *, status_app_slug: str | None = None,
    status_app_id: str | None = None,
) -> None:
    """Copy the reviewer-prepared source without editing tracked configuration."""
    if source.resolve() == destination.resolve():
        raise ValueError("source and destination must differ")
    content = source.read_bytes()
    document = yaml.safe_load(content)
    daemon = document.get("daemon") if isinstance(document, dict) else None
    identities = (
        daemon.get("trusted_reviewer_identities")
        if isinstance(daemon, dict)
        else None
    )
    if not isinstance(identities, list) or not identities:
        raise ValueError("prepared config has no trusted reviewer identities")
    if status_app_slug is not None:
        if not re.fullmatch(r"[A-Za-z0-9](?:[A-Za-z0-9-]{0,98}[A-Za-z0-9])?", status_app_slug):
            raise ValueError("testbed status App slug is missing or malformed")
        if status_app_id is None or not status_app_id.isdecimal() or int(status_app_id) <= 0:
            raise ValueError("testbed status App ID is missing or malformed")
        repositories = document.get("repositories")
        if not isinstance(repositories, list) or len(repositories) != 1 or not isinstance(repositories[0], dict):
            raise ValueError("test config must define one testbed repository")
        repositories[0]["ci_fix_trusted_statuses"] = {
            "e2e/watch-merge-gate": [
                f"app:{int(status_app_id)}", f"user:{status_app_slug}[bot]",
            ],
        }
        content = yaml.safe_dump(document, sort_keys=False).encode("utf-8")
    destination.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=destination.name + ".", dir=destination.parent)
    try:
        with os.fdopen(fd, "wb") as output:
            output.write(content)
            output.flush()
            os.fsync(output.fileno())
        os.chmod(temporary, 0o644)
        os.replace(temporary, destination)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("source", type=Path)
    parser.add_argument("destination", type=Path)
    parser.add_argument("--status-app-slug")
    parser.add_argument("--status-app-id")
    args = parser.parse_args()
    prepare_test_config(
        args.source, args.destination,
        status_app_slug=args.status_app_slug, status_app_id=args.status_app_id,
    )


if __name__ == "__main__":
    main()
