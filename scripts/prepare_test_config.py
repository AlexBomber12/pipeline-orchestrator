#!/usr/bin/env python3
"""Install the prepared integration config in the stack's shared /data mount."""

from __future__ import annotations

import argparse
import os
import tempfile
from pathlib import Path

import yaml


def prepare_test_config(source: Path, destination: Path) -> None:
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
    args = parser.parse_args()
    prepare_test_config(args.source, args.destination)


if __name__ == "__main__":
    main()
