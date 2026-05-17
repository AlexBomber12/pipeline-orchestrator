"""AST + text scan validation that every subsource literal is registered.

PR-325 safety net: scans an explicit allow-list of source files for
subsource-shaped string literals and asserts each is present in
``src.subsource_registry``. Catches both directions of drift between
the registry and actual write/render sites.
"""

from __future__ import annotations

import ast
import re
from pathlib import Path

import pytest

from src import subsource_registry
from src.cancellation import SUBSOURCE_VOCABULARY
from src.subsource_registry import all_subsources, canonical_subsources

REPO_ROOT = Path(__file__).resolve().parents[2]

SUBSOURCE_SOURCE_FILES = [
    "src/cancellation/storage.py",
    "src/cancellation/__init__.py",
    "src/daemon/runner.py",
    "src/daemon/handlers/watch.py",
    "src/daemon/handlers/fix.py",
    "src/daemon/migrations/escalate_to_error.py",
    "src/daemon/migrations/hung_to_idle.py",
    "src/web/routes/repo_control.py",
    "src/web/routes/dashboard.py",
    "src/subsource_registry.py",
]

SUBSOURCE_TEMPLATE_FILES = [
    "src/web/templates/components/cancellation_card.html",
]

_TEMPLATE_BRANCH_RE = re.compile(r"subsource\s*==\s*['\"]([a-z_]+)['\"]")

# Frozen snapshot of the registry at module load. Used as the candidate
# basis so that a monkeypatched registry (plant test) can still produce
# a candidate for the assertion to check against the live registry.
_VOCABULARY_SNAPSHOT: frozenset[str] = all_subsources()


def _collect_python_string_literals(path: Path) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    found: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            found.add(node.value)
    return found


def _collect_template_subsource_branches(path: Path) -> set[str]:
    text = path.read_text(encoding="utf-8")
    return set(_TEMPLATE_BRANCH_RE.findall(text))


def test_canonical_subsources_match_vocabulary_frozenset():
    assert canonical_subsources() == SUBSOURCE_VOCABULARY


def test_every_python_subsource_literal_is_registered():
    registered = subsource_registry.all_subsources()
    for relpath in SUBSOURCE_SOURCE_FILES:
        path = REPO_ROOT / relpath
        if not path.is_file():
            continue
        literals = _collect_python_string_literals(path)
        candidates = literals & _VOCABULARY_SNAPSHOT
        if not candidates:
            continue
        for name in candidates:
            assert name in registered, (
                f"Subsource literal {name!r} found in {relpath} but missing "
                "from src.subsource_registry. Add it to the registry."
            )


def test_every_template_subsource_branch_is_registered():
    registered = subsource_registry.all_subsources()
    for relpath in SUBSOURCE_TEMPLATE_FILES:
        path = REPO_ROOT / relpath
        if not path.is_file():
            continue
        names = _collect_template_subsource_branches(path)
        unknown = names - registered
        assert not unknown, (
            f"Template subsource branches in {relpath} reference unregistered "
            f"subsource names: {sorted(unknown)}. Add them to the registry."
        )


def test_every_registered_subsource_appears_in_source():
    registered = subsource_registry.all_subsources()
    seen_in_python: set[str] = set()
    for relpath in SUBSOURCE_SOURCE_FILES:
        path = REPO_ROOT / relpath
        if not path.is_file():
            continue
        literals = _collect_python_string_literals(path)
        seen_in_python |= literals & registered
    seen_in_templates: set[str] = set()
    for relpath in SUBSOURCE_TEMPLATE_FILES:
        path = REPO_ROOT / relpath
        if not path.is_file():
            continue
        seen_in_templates |= _collect_template_subsource_branches(path)
    seen = seen_in_python | seen_in_templates
    unused = registered - seen
    assert not unused, (
        f"Registered subsources with no references in scanned files: "
        f"{sorted(unused)}. Either remove from registry or add the "
        "containing source file to SUBSOURCE_SOURCE_FILES or "
        "SUBSOURCE_TEMPLATE_FILES."
    )


def test_plant_failing_case_fires_assertion(monkeypatch):
    real_lookup = subsource_registry.all_subsources

    def fake_lookup() -> frozenset[str]:
        return frozenset(s for s in real_lookup() if s != "crash")

    monkeypatch.setattr(subsource_registry, "all_subsources", fake_lookup)
    with pytest.raises(AssertionError, match="crash"):
        test_every_python_subsource_literal_is_registered()
