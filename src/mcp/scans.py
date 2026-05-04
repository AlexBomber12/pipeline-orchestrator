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


# Negation phrases that, when present in the same clause as a match,
# turn the match from a violation into policy-affirming prose. Lines
# such as ``Do not run gh pr create --draft`` or ``Never use git
# commit --no-verify`` restate the AGENTS.md rule and must not be
# flagged. Both straight (``'``) and typographic (``’``) apostrophes
# are accepted. Each phrase is anchored on a word boundary so the
# regex matches whole words, not substrings (e.g. ``don't`` matches
# but ``redon't`` would not).
_NEGATION_CONTEXT = re.compile(
    r"\b(?:"
    r"do not|don't|don’t|"
    r"cannot|can not|can't|can’t|"
    r"never|"
    r"must not|mustn't|mustn’t|"
    r"will not|won't|won’t|"
    r"should not|shouldn't|shouldn’t|"
    r"avoid"
    r")\b",
    re.IGNORECASE,
)

# Clause boundaries for the negation-window check. ``.;!?:`` are
# sentence/clause punctuation; ``\n`` is the physical line break.
# A negation must appear in the clause that contains the match -- not
# in a previous clause -- so that a sentence like ``Step 1: do not
# skip CI. Step 2: skip CI when forced.`` flags the second occurrence
# while suppressing the first.
_CLAUSE_BOUNDARIES = ".;!?:\n"


def _is_negated(text: str, match_start: int) -> bool:
    """Return True if a negation phrase precedes ``match_start`` in
    the same clause, indicating the matched command/phrase is being
    prohibited rather than instructed.

    The clause is the slice of ``text`` between the closest preceding
    clause boundary (``.;!?:\\n``) and ``match_start``. When no
    boundary is found, the clause starts at the beginning of the
    text. Examples that suppress detection:

    - ``Do not run gh pr create --draft`` (verb between negation and
      command)
    - ``Never use git commit --no-verify``
    - ``Coders must not skip CI``

    Without this check, command-based regex patterns flag the literal
    command even when the surrounding prose explicitly forbids it,
    rejecting compliant specs that document the AGENTS.md rule.
    """
    clause_start = max(text.rfind(c, 0, match_start) for c in _CLAUSE_BOUNDARIES)
    clause_start = clause_start + 1 if clause_start >= 0 else 0
    return _NEGATION_CONTEXT.search(text, clause_start, match_start) is not None


_ANTI_PATTERNS: list[tuple[str, re.Pattern, str]] = [
    (
        "draft_pr_flag",
        re.compile(r"\bgh pr create[^\n]*--draft\b", re.IGNORECASE),
        "AGENTS.md prohibits opening PRs in draft state. PR-196.",
    ),
    (
        "draft_pr_text",
        # Match ``create [a|the] draft PR``. Negated forms such as
        # ``do not create a draft PR`` are suppressed by the unified
        # ``_is_negated`` check in ``scan_for_conflicts``, which scans
        # the enclosing clause for a negation phrase rather than only
        # the token immediately before the match. Centralising the
        # negation handling there means a verb between negation and
        # match (``Coders should never create a draft PR``) is
        # recognised, which fixed-width lookbehinds cannot do.
        re.compile(r"\bcreate (a |the )?draft PR\b", re.IGNORECASE),
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
        # optionally prefixed by ``<src>:``). ``git push`` syntax is
        # ``git push [<repository> [<refspec>...]]``, so the first
        # non-flag positional token is the remote name and only
        # subsequent positionals are refspecs. The standard arm
        # therefore requires a non-flag positional token (the remote)
        # to appear before the ``main`` refspec; this prevents
        # ``git push --force main feature/foo`` -- which targets
        # ``feature/foo`` on a remote named ``main`` -- from being
        # flagged. The plus-prefix arm does not need this guard
        # because remote names cannot start with ``+``, so any
        # ``+<refspec>`` token is unambiguously a refspec. The
        # ``main`` token boundary rejects ``[\w/:-]`` AND a ``.``
        # followed by a word char, so dot-suffixed branches like
        # ``main.old`` or ``main.fix`` are treated as distinct
        # branches and not flagged. ``main.`` at sentence end (``.``
        # followed by space or EOS) still matches, preserving the v1
        # trade-off where don't-do-this commentary may be flagged.
        # The ``--force`` token excludes ``--force-if-includes``
        # (alone a no-op) but still matches ``--force`` and
        # ``--force-with-lease[=ref]``. The plus-prefix arm catches
        # ``+<refspec>`` targeting ``main``, which ``git push -h``
        # documents as equivalent force behavior.
        re.compile(
            r"\bgit push\b"
            r"(?:"
            # Standard force-flag form: --force/-f token AND main
            # refspec token, in any order, both within the command's
            # arg list. The main refspec token must be preceded by
            # at least one non-flag, non-plus-prefix token (the
            # remote name) so that a remote literally named ``main``
            # cannot be misread as the protected branch.
            r"(?="
            r"(?:[ \t]+[^\s,;|&#]+)*?"
            r"[ \t]+"
            r"(?:--force(?:-with-lease(?:=[^\s,;|&#]*)?)?|-f)"
            r"(?![\w-])"
            r")"
            r"(?="
            r"(?:[ \t]+[^\s,;|&#]+)*?"
            r"[ \t]+(?![-+])[^\s,;|&#]+"
            r"(?:[ \t]+[^\s,;|&#]+)*?"
            r"[ \t]+"
            r"(?:[^\s:,;|&#]+:)?(?:refs/heads/)?main"
            r"(?![\w/:-]|\.\w)"
            r")"
            r"|"
            # Plus-prefix force form: +<refspec> with dst=main. The
            # ``+`` prefix is unambiguous since remote names cannot
            # start with ``+``, so no remote-name guard is needed.
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
        # Match ``skip CI`` / ``bypass CI`` / ``ignore CI``. Negated
        # prose such as ``do not skip CI`` or ``never bypass CI`` is
        # suppressed by the unified ``_is_negated`` check in
        # ``scan_for_conflicts``. The ``(?<!\[)`` lookbehind here is
        # NOT about negation -- it excludes the bracketed marker
        # forms ``[skip ci]`` / ``[ci skip]`` so that the literal
        # marker only fires the ``skip_ci_commit_msg`` rule, never a
        # duplicate ``skip_ci`` finding for the same offending
        # substring (e.g. ``Title: Refactor stub [skip ci]``).
        re.compile(
            r"(?<!\[)\b(?:skip|bypass|ignore) CI\b",
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

    Matches whose enclosing clause begins with a negation phrase
    (``Do not run gh pr create --draft``, ``Never use git commit
    --no-verify``) are suppressed by ``_is_negated`` so prose that
    restates an AGENTS.md prohibition is not itself flagged as a
    violation. The check applies uniformly to every pattern, since
    every anti-pattern can appear in policy-affirming form.

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
            if _is_negated(normalized, match.start()):
                continue
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
