"""Smoke test: dev/test tooling must not leak into production requirements.

PR-235 split test/dev tooling out of ``requirements.txt`` (used by the
production Dockerfile) into ``requirements-test.txt``. This test guards
against accidental re-addition of those packages to production requirements.
"""

from __future__ import annotations

import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
PROD_REQUIREMENTS = REPO_ROOT / "requirements.txt"
TEST_REQUIREMENTS = REPO_ROOT / "requirements-test.txt"

DEV_PACKAGES = ("ruff", "pytest", "pytest-cov", "pytest-asyncio")


def _packages(path: Path) -> set[str]:
    names: set[str] = set()
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        match = re.match(r"^([A-Za-z0-9._-]+)", line)
        if match:
            names.add(match.group(1).lower())
    return names


def test_prod_requirements_excludes_dev_tooling() -> None:
    prod = _packages(PROD_REQUIREMENTS)
    leaked = sorted(pkg for pkg in DEV_PACKAGES if pkg in prod)
    assert not leaked, (
        f"requirements.txt must not contain dev/test tooling; found: {leaked}"
    )


def test_test_requirements_includes_dev_tooling() -> None:
    test_pkgs = _packages(TEST_REQUIREMENTS)
    missing = sorted(pkg for pkg in DEV_PACKAGES if pkg not in test_pkgs)
    assert not missing, (
        f"requirements-test.txt must contain dev/test tooling; missing: {missing}"
    )
