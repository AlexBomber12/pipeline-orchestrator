"""AST + text scan validation that every subsource literal is registered.

PR-325 safety net: walks every Python file under ``src/`` and every HTML
template under ``src/web/templates/`` looking for string literals that
*syntactically denote* a subsource name, and asserts each is present in
``src.subsource_registry``. Catches both directions of drift between the
registry and actual write/render sites.

The scan is intentionally exhaustive rather than driven by a static
allow-list: a partial list would silently skip future write sites added
elsewhere in ``src/``, defeating the guardrail. Context-aware AST
detection (see ``_SubsourceLiteralCollector``) keeps false positives off
unrelated string literals.
"""

from __future__ import annotations

import ast
import re
from pathlib import Path

import pytest
from src import subsource_registry
from src.cancellation import SUBSOURCE_VOCABULARY
from src.subsource_registry import canonical_subsources

REPO_ROOT = Path(__file__).resolve().parents[2]
SRC_ROOT = REPO_ROOT / "src"
TEMPLATES_ROOT = SRC_ROOT / "web" / "templates"

# The registry module is the definition site: scanning its ``_REGISTRY``
# keys would by definition include every registered name and make the
# dead-entry drift test pass vacuously even when a name has no write or
# render site backing it. Excluded from the use-site scan only.
REGISTRY_MODULE = SRC_ROOT / "subsource_registry.py"

_TEMPLATE_BRANCH_RE = re.compile(r"subsource\s*==\s*['\"]([a-z_]+)['\"]")
_FOUNDATION_ONLY_SUBSOURCES = frozenset(
    {"diagnose_exhausted", "operator_stopped", "rate_limit"}
)


def _all_python_source_files() -> list[Path]:
    paths = sorted(SRC_ROOT.rglob("*.py"))
    assert paths, (
        f"No Python source files discovered under {SRC_ROOT}. Either the "
        "source tree moved or the glob is broken; either way the scan is "
        "no longer covering write sites."
    )
    return paths


def _python_use_site_files() -> list[Path]:
    return [p for p in _all_python_source_files() if p != REGISTRY_MODULE]


def _all_template_files() -> list[Path]:
    paths = sorted(TEMPLATES_ROOT.rglob("*.html"))
    assert paths, (
        f"No HTML templates discovered under {TEMPLATES_ROOT}. Either the "
        "template tree moved or the glob is broken; either way the scan "
        "is no longer covering render sites."
    )
    return paths


class _SubsourceLiteralCollector(ast.NodeVisitor):
    """Collect string literals that syntactically denote a subsource name.

    Detection is by syntactic context so the scanner picks up *new*
    literals a developer might add at a canonical write site even when
    the name has not been registered yet (which is the drift this test
    exists to catch). Filtering by a registry-derived snapshot would
    silently exclude exactly those names.

    Recognized contexts:

    * dict value where the corresponding key is the string ``"subsource"``
      (canonical payload write pattern, e.g. ``{"subsource": "X"}``)
    * keyword argument literally named ``subsource`` in any call
      (e.g. ``_register_cancellation_cause(subsource="X")``)
    * keyword argument ``name=`` to a ``SubsourceMetadata(...)`` call
      (registry definition site)
    * comparison where one operand is the ``subsource`` name/attribute
      or ``...get("subsource")`` (read-side dispatch, e.g.
      ``payload.get("subsource") == "X"`` or membership form
      ``payload.get("subsource") in ("X", "Y")``)
    * dict literal whose assignment target name marks its keys or values
      as subsources (``_SUBSOURCE_TO_*`` keys, ``*_TO_SUBSOURCE`` values,
      ``_REGISTRY`` keys)
    * set literal assigned to ``SUBSOURCE_VOCABULARY`` (opt-in via
      ``include_vocabulary_set``; disabled for the dead-entry scan so
      vocabulary membership alone never qualifies as a use site)
    """

    _SUBSOURCE_NAMES = {"subsource", "_subsource"}

    def __init__(self, *, include_vocabulary_set: bool = True) -> None:
        self.found: set[str] = set()
        self._assign_target: str | None = None
        self._include_vocabulary_set = include_vocabulary_set

    def _record(self, node: ast.AST | None) -> None:
        if isinstance(node, (ast.Tuple, ast.List, ast.Set)):
            for elt in node.elts:
                self._record(elt)
            return
        if (
            isinstance(node, ast.Constant)
            and isinstance(node.value, str)
            and node.value
        ):
            self.found.add(node.value)

    def visit_Assign(self, node: ast.Assign) -> None:
        target = (
            node.targets[0].id
            if len(node.targets) == 1 and isinstance(node.targets[0], ast.Name)
            else None
        )
        prev, self._assign_target = self._assign_target, target
        self.generic_visit(node)
        self._assign_target = prev

    def visit_AnnAssign(self, node: ast.AnnAssign) -> None:
        target = node.target.id if isinstance(node.target, ast.Name) else None
        prev, self._assign_target = self._assign_target, target
        self.generic_visit(node)
        self._assign_target = prev

    def visit_Dict(self, node: ast.Dict) -> None:
        target = (self._assign_target or "").upper()
        keys_are_subsources = (
            target.startswith("_SUBSOURCE_TO_") or target == "_REGISTRY"
        )
        values_are_subsources = target.endswith("_TO_SUBSOURCE")
        for key, val in zip(node.keys, node.values):
            if keys_are_subsources:
                self._record(key)
            if values_are_subsources:
                self._record(val)
            if isinstance(key, ast.Constant) and key.value == "subsource":
                self._record(val)
        self.generic_visit(node)

    def visit_Set(self, node: ast.Set) -> None:
        if (
            self._include_vocabulary_set
            and (self._assign_target or "").upper() == "SUBSOURCE_VOCABULARY"
        ):
            for elt in node.elts:
                self._record(elt)
        self.generic_visit(node)

    def visit_Call(self, node: ast.Call) -> None:
        is_metadata_call = (
            isinstance(node.func, ast.Name)
            and node.func.id == "SubsourceMetadata"
        )
        for kw in node.keywords:
            if kw.arg == "subsource":
                self._record(kw.value)
            elif kw.arg == "name" and is_metadata_call:
                self._record(kw.value)
        self.generic_visit(node)

    def visit_Compare(self, node: ast.Compare) -> None:
        if self._has_subsource_operand(node):
            for op in (node.left, *node.comparators):
                self._record(op)
        self.generic_visit(node)

    @classmethod
    def _has_subsource_operand(cls, node: ast.Compare) -> bool:
        for expr in (node.left, *node.comparators):
            if isinstance(expr, ast.Name) and expr.id in cls._SUBSOURCE_NAMES:
                return True
            if isinstance(expr, ast.Attribute) and expr.attr == "subsource":
                return True
            if (
                isinstance(expr, ast.Call)
                and isinstance(expr.func, ast.Attribute)
                and expr.func.attr == "get"
                and expr.args
                and isinstance(expr.args[0], ast.Constant)
                and expr.args[0].value == "subsource"
            ):
                return True
        return False


def _collect_python_subsource_literals(
    path: Path, *, include_vocabulary_set: bool = True
) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    collector = _SubsourceLiteralCollector(
        include_vocabulary_set=include_vocabulary_set
    )
    collector.visit(tree)
    return collector.found


def _collect_template_subsource_branches(path: Path) -> set[str]:
    text = path.read_text(encoding="utf-8")
    return set(_TEMPLATE_BRANCH_RE.findall(text))


def test_canonical_subsources_match_vocabulary_frozenset():
    assert canonical_subsources() == SUBSOURCE_VOCABULARY


def test_every_python_subsource_literal_is_registered():
    registered = subsource_registry.all_subsources()
    for path in _all_python_source_files():
        relpath = path.relative_to(REPO_ROOT)
        for name in _collect_python_subsource_literals(path):
            assert name in registered, (
                f"Subsource literal {name!r} found in {relpath} but missing "
                "from src.subsource_registry. Add it to the registry."
            )


def test_every_template_subsource_branch_is_registered():
    registered = subsource_registry.all_subsources()
    for path in _all_template_files():
        relpath = path.relative_to(REPO_ROOT)
        unknown = _collect_template_subsource_branches(path) - registered
        assert not unknown, (
            f"Template subsource branches in {relpath} reference unregistered "
            f"subsource names: {sorted(unknown)}. Add them to the registry."
        )


def test_every_registered_subsource_appears_in_source():
    registered = subsource_registry.all_subsources()
    seen: set[str] = set()
    for path in _python_use_site_files():
        seen |= _collect_python_subsource_literals(
            path, include_vocabulary_set=False
        )
    for path in _all_template_files():
        seen |= _collect_template_subsource_branches(path)
    unused = registered - seen - _FOUNDATION_ONLY_SUBSOURCES
    assert not unused, (
        f"Registered subsources with no references under src/: "
        f"{sorted(unused)}. Either remove from src.subsource_registry or "
        "add a write site / template branch that references the name."
    )


def test_plant_failing_case_fires_assertion(monkeypatch):
    real_lookup = subsource_registry.all_subsources

    def fake_lookup() -> frozenset[str]:
        return frozenset(s for s in real_lookup() if s != "crash")

    monkeypatch.setattr(subsource_registry, "all_subsources", fake_lookup)
    with pytest.raises(AssertionError, match="crash"):
        test_every_python_subsource_literal_is_registered()


def test_collector_detects_membership_container_literals():
    """Membership comparisons must expose their tuple/list/set elements.

    ``payload.get("subsource") in ("coder_escalate", "guardrail")`` in
    ``src/daemon/cascade_monitor.py`` would otherwise hide both names from
    the safety-net scan, leaving any new unregistered subsource added in
    the same ``in (...)`` form uncaught.
    """
    src = (
        "def f(payload):\n"
        "    if payload.get('subsource') in ('coder_escalate', 'guardrail'):\n"
        "        return True\n"
        "    if payload.get('subsource') in ['crash', 'review_timeout']:\n"
        "        return True\n"
        "    if payload.get('subsource') in {'fix_idle_timeout'}:\n"
        "        return True\n"
        "    return False\n"
    )
    tree = ast.parse(src)
    collector = _SubsourceLiteralCollector()
    collector.visit(tree)
    assert {
        "coder_escalate",
        "guardrail",
        "crash",
        "review_timeout",
        "fix_idle_timeout",
    } <= collector.found

    cascade_path = SRC_ROOT / "daemon" / "cascade_monitor.py"
    if cascade_path.is_file():
        found = _collect_python_subsource_literals(cascade_path)
        assert {"coder_escalate", "guardrail"} <= found


def test_dead_entry_scan_ignores_vocabulary_set_definition():
    """SUBSOURCE_VOCABULARY membership alone must not count as a use site.

    Otherwise the dead-entry assertion is self-fulfilling for canonical
    names: removing every real write/render site for ``"guardrail"`` would
    still leave it counted as "seen" purely because it remains in the
    vocabulary frozenset literal.
    """
    src = (
        "SUBSOURCE_VOCABULARY = frozenset({'guardrail', 'crash'})\n"
        "OTHER = {'guardrail'}\n"
    )
    tree = ast.parse(src)
    with_vocab = _SubsourceLiteralCollector(include_vocabulary_set=True)
    with_vocab.visit(tree)
    without_vocab = _SubsourceLiteralCollector(include_vocabulary_set=False)
    without_vocab.visit(tree)
    assert {"guardrail", "crash"} <= with_vocab.found
    assert "guardrail" not in without_vocab.found
    assert "crash" not in without_vocab.found
