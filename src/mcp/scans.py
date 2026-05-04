"""AGENTS.md anti-pattern scanner for MCP validate_task_spec.

Regex-based detection of phrases that indicate the task spec
contains instructions conflicting with AGENTS.md rules. Each
pattern carries a violation_type and human-readable message.
PR-259.
"""

from __future__ import annotations

import re
from dataclasses import dataclass


@dataclass
class ConflictViolation:
    violation_type: str
    line_excerpt: str
    rule: str  # AGENTS.md rule reference


_ANTI_PATTERNS: list[tuple[str, re.Pattern, str]] = [
    (
        "draft_pr_flag",
        re.compile(r"\bgh pr create[^\n]*--draft\b", re.IGNORECASE),
        "AGENTS.md prohibits opening PRs in draft state. PR-196.",
    ),
    (
        "draft_pr_text",
        # Match ``create [a|the] draft PR`` only when NOT immediately
        # preceded by a negation. Phrases like ``do not create a draft
        # PR`` restate the AGENTS.md rule and must not be flagged as a
        # violation. Each lookbehind is fixed-width (Python's ``re``
        # requires it), so common negations are listed individually.
        # Both straight (``'``) and typographic (``’``) apostrophes
        # are accepted.
        re.compile(
            r"(?<!do not )"
            r"(?<!don't )"
            r"(?<!don’t )"
            r"(?<!cannot )"
            r"(?<!can not )"
            r"(?<!can't )"
            r"(?<!can’t )"
            r"(?<!never )"
            r"(?<!must not )"
            r"(?<!mustn't )"
            r"(?<!mustn’t )"
            r"(?<!will not )"
            r"(?<!won't )"
            r"(?<!won’t )"
            r"(?<!should not )"
            r"(?<!shouldn't )"
            r"(?<!shouldn’t )"
            r"(?<!avoid )"
            r"\bcreate (a |the )?draft PR\b",
            re.IGNORECASE,
        ),
        "AGENTS.md prohibits opening PRs in draft state. PR-196.",
    ),
    (
        "force_push_main",
        # Detect ``git push`` lines that force-push to ``main``. Both
        # arms walk only the tokens that belong to the ``git push``
        # command itself, treating the args as whitespace-separated
        # tokens that contain no command-terminator chars (``,;|&#``).
        # ``#`` starts a shell comment, so anything after it is prose,
        # not a refspec. The walks are unbounded so that flag-heavy
        # invocations -- e.g. ``git push --force --set-upstream
        # --atomic --follow-tags --verbose origin main`` -- cannot
        # bypass detection by stuffing more flags between ``git push``
        # and the ``main`` refspec. The trade-off is that a
        # separator-less prose continuation such as
        # ``git push --force-with-lease origin feature/foo then open
        # PR to main.`` will be flagged as a false positive; per the
        # task spec's v1 trade-off, false positives on don't-do-this
        # commentary are acceptable -- the operator dismisses them --
        # while a force-push to main slipping past the scanner is
        # not. Sentence punctuation is therefore expected to be
        # written as ``,``, ``;``, ``|``, ``&``, or ``#`` (a shell
        # comment) when prose follows the command on the same line,
        # which the existing comma/comment cases continue to honor.
        # The standard arm requires both a ``--force``/``-f`` flag
        # token AND a refspec token whose destination resolves to
        # ``main`` (whitespace-bounded ``main`` or ``refs/heads/main``,
        # optionally prefixed by ``<src>:``). The ``main`` token
        # boundary rejects ``[\w/:-]`` AND a ``.`` followed by a word
        # char, so dot-suffixed branches like ``main.old`` or
        # ``main.fix`` are treated as distinct branches and not
        # flagged. ``main.`` at sentence end (``.`` followed by space
        # or EOS) still matches, preserving the v1 trade-off where
        # don't-do-this commentary may be flagged. The ``--force``
        # token excludes ``--force-if-includes`` (alone a no-op) but
        # still matches ``--force`` and ``--force-with-lease[=ref]``.
        # The plus-prefix arm catches ``+<refspec>`` targeting
        # ``main``, which ``git push -h`` documents as equivalent
        # force behavior.
        re.compile(
            r"\bgit push\b"
            r"(?:"
            # Standard force-flag form: --force/-f token AND main
            # refspec token, in any order, both within the command's
            # arg list.
            r"(?="
            r"(?:[ \t]+[^\s,;|&#]+)*?"
            r"[ \t]+"
            r"(?:--force(?:-with-lease(?:=[^\s,;|&#]*)?)?|-f)"
            r"(?![\w-])"
            r")"
            r"(?="
            r"(?:[ \t]+[^\s,;|&#]+)*?"
            r"[ \t]+"
            r"(?:[^\s:,;|&#]+:)?(?:refs/heads/)?main"
            r"(?![\w/:-]|\.\w)"
            r")"
            r"|"
            # Plus-prefix force form: +<refspec> with dst=main.
            r"(?="
            r"(?:[ \t]+[^\s,;|&#]+)*?"
            r"[ \t]+"
            r"\+(?:[^\s:,;|&#]+:)?(?:refs/heads/)?main"
            r"(?![\w/:-]|\.\w)"
            r")"
            r")",
            re.IGNORECASE,
        ),
        "AGENTS.md forbids force-push to main.",
    ),
    (
        "force_push_main_alt",
        re.compile(r"\bforce.push[^\n]*\bmain\b", re.IGNORECASE),
        "AGENTS.md forbids force-push to main.",
    ),
    (
        "no_verify_commit",
        re.compile(r"\bgit commit[^\n]*--no-verify\b", re.IGNORECASE),
        "AGENTS.md forbids bypassing pre-commit hooks.",
    ),
    (
        "skip_ci",
        # Match ``skip CI`` / ``bypass CI`` / ``ignore CI`` only when
        # they are NOT immediately preceded by a negation. Negations
        # such as ``do not skip CI`` or ``never skip CI`` restate the
        # AGENTS.md rule and must not be flagged as a violation. Each
        # alternative below is a fixed-width negative lookbehind --
        # Python's stdlib ``re`` requires fixed widths, so the common
        # negation phrases are listed individually instead of being
        # collapsed into a variable-width alternation. Both straight
        # (``'``) and typographic (``’``) apostrophes are
        # accepted.
        re.compile(
            r"(?<!do not )"
            r"(?<!don't )"
            r"(?<!don’t )"
            r"(?<!cannot )"
            r"(?<!can not )"
            r"(?<!can't )"
            r"(?<!can’t )"
            r"(?<!never )"
            r"(?<!must not )"
            r"(?<!mustn't )"
            r"(?<!mustn’t )"
            r"(?<!will not )"
            r"(?<!won't )"
            r"(?<!won’t )"
            r"(?<!should not )"
            r"(?<!shouldn't )"
            r"(?<!shouldn’t )"
            r"(?<!avoid )"
            r"\b(?:skip|bypass|ignore) CI\b",
            re.IGNORECASE,
        ),
        "AGENTS.md requires green CI before merge.",
    ),
    (
        "skip_ci_commit_msg",
        # ``\b`` does not anchor before ``\[`` since ``[`` is non-word;
        # match the marker anywhere on the line instead.
        re.compile(r"\[skip ci\]|\[ci skip\]", re.IGNORECASE),
        "AGENTS.md requires green CI; [skip ci] markers are prohibited.",
    ),
    (
        "auto_merge_dirty",
        re.compile(r"\bauto.merge[^\n]*(dirty|red|failing)\b", re.IGNORECASE),
        "AGENTS.md prohibits merging with failing checks.",
    ),
]


def scan_for_conflicts(task_spec_body: str) -> list[ConflictViolation]:
    """Return list of detected AGENTS.md anti-pattern violations.

    The body is scanned as a whole rather than per physical line, so
    forbidden commands cannot evade detection by inserting a shell
    line continuation (``\\<newline>``) between flag and refspec.
    Continuations are collapsed before matching, then each pattern
    runs against the full normalized body via ``finditer``.

    Empty list when no violations found. Each match captures the
    violation type and an 80-char excerpt of the offending line for
    operator context; for a continued command, the excerpt is the
    joined logical line that the shell would actually execute.
    """
    # Collapse shell line continuations so a multi-line command such
    # as ``git push --force \<newline>    origin main`` matches the
    # same patterns as its single-line equivalent.
    normalized = re.sub(r"\\\n[ \t]*", " ", task_spec_body)
    violations: list[ConflictViolation] = []
    for vtype, pattern, rule in _ANTI_PATTERNS:
        for match in pattern.finditer(normalized):
            line_start = normalized.rfind("\n", 0, match.start()) + 1
            line_end = normalized.find("\n", match.start())
            if line_end == -1:
                line_end = len(normalized)
            excerpt = normalized[line_start:line_end].strip()[:80]
            violations.append(
                ConflictViolation(
                    violation_type=vtype,
                    line_excerpt=excerpt,
                    rule=rule,
                )
            )
    return violations
