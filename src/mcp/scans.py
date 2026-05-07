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
#
# ``avoid`` is intentionally NOT in this list. It does not reliably
# negate the matched command/phrase: ``Avoid merge conflicts by
# running gh pr create --draft`` and ``To avoid delays, skip CI``
# both contain ``avoid`` but instruct the operator to perform the
# violating action. Treating ``avoid`` as a negation token would
# produce silent false negatives in the core safety scan.
_NEGATION_CONTEXT = re.compile(
    r"\b(?:"
    r"do not|don't|don’t|"
    r"cannot|can not|can't|can’t|"
    r"never|"
    r"must not|mustn't|mustn’t|"
    r"will not|won't|won’t|"
    r"should not|shouldn't|shouldn’t"
    r")\b",
    re.IGNORECASE,
)

# Clause boundaries for the negation-window check. ``.;!?:`` are
# sentence/clause punctuation; ``,`` separates a sub-clause (e.g. an
# introductory ``If X,`` from the main clause); ``\n`` is the physical
# line break. A negation must appear in the sub-clause that contains
# the match -- not in a previous (sub-)clause -- so that a sentence
# like ``If tests can not pass quickly, skip CI for this change.``
# flags ``skip CI`` (the ``can not`` negates ``pass quickly``, not the
# matched instruction), and ``Step 1: do not skip CI. Step 2: skip CI
# when forced.`` flags the second occurrence while suppressing the
# first.
_CLAUSE_BOUNDARIES = ".,;!?:\n"

# Double-negative inverters that flip a lexical negation back into a
# positive instruction. ``Don't forget to skip CI`` and ``Never fail
# to run gh pr create --draft`` contain a negation token but the
# verb-of-omission ("forget", "fail", "neglect", "hesitate", "refuse"
# followed by ``to``) cancels the prohibition: the surrounding prose
# is *requiring* the matched action, not forbidding it. When such an
# inverter sits between the negation phrase and the match, the
# negation must NOT suppress the violation -- otherwise lexical
# negation is silently equated with semantic prohibition and a
# conflicting task spec passes ``validate_task_spec``.
_DOUBLE_NEGATIVE_INVERTER = re.compile(
    r"\b(?:forget|fail|neglect|hesitate|refuse)\s+to\b",
    re.IGNORECASE,
)


def _is_negated(text: str, match_start: int) -> bool:
    """Return True if a negation phrase semantically prohibits the
    matched command/phrase at ``match_start``.

    The sub-clause is the slice of ``text`` between the closest
    preceding clause boundary (``.,;!?:\\n``) and ``match_start``.
    When no boundary is found, the sub-clause starts at the beginning
    of the text. Including ``,`` in the boundary set scopes the lookup
    to the comma-delimited segment that actually governs the match;
    without it, an unrelated negation in a separate sub-clause (e.g.
    ``can not`` in ``If tests can not pass quickly, skip CI ...``)
    would suppress a real violation.

    The check uses the NEAREST negation to the match (the rightmost
    negation in the sub-clause), not the first. ``Don't forget to run
    tests and do not skip CI`` contains two negations; the leading
    ``Don't ... forget to`` is a double-negative that re-asserts ``run
    tests``, while the trailing ``do not`` directly prohibits ``skip
    CI``. Examining only the first negation would let the leading
    inverter cancel a later, real prohibition and produce a false
    positive on a compliant spec.

    A bare lexical negation is not enough: a verb-of-omission inverter
    (``forget to``, ``fail to``, ``neglect to``, ``hesitate to``,
    ``refuse to``) between the nearest negation and the match flips
    the semantics back into an instruction. ``Don't forget to skip
    CI`` contains ``Don't`` but is NOT a prohibition of ``skip CI``;
    the inverter forces the spec to actively require the violating
    action. The check therefore returns True only when a negation is
    present AND no inverter sits between the nearest negation and the
    match.

    Examples that suppress detection (real prohibitions):

    - ``Do not run gh pr create --draft`` (verb between negation and
      command, no comma between them)
    - ``Never use git commit --no-verify``
    - ``Coders must not skip CI``
    - ``Don't forget to run tests and do not skip CI`` (nearest
      negation governs the match)

    Examples that do NOT suppress (double-negative instructions):

    - ``Don't forget to skip CI``
    - ``Never fail to run gh pr create --draft``

    Without this check, command-based regex patterns flag the literal
    command even when the surrounding prose explicitly forbids it,
    rejecting compliant specs that document the AGENTS.md rule; with
    only the lexical-negation half of the check, double-negative
    instructions silently pass validation despite restating a
    violation.
    """
    clause_start = max(text.rfind(c, 0, match_start) for c in _CLAUSE_BOUNDARIES)
    clause_start = clause_start + 1 if clause_start >= 0 else 0
    negations = list(_NEGATION_CONTEXT.finditer(text, clause_start, match_start))
    if not negations:
        return False
    nearest = negations[-1]
    if _DOUBLE_NEGATIVE_INVERTER.search(text, nearest.end(), match_start):
        return False
    return True


def _is_negated_cross_repo_shipping(text: str, match_start: int) -> bool:
    """Return True for scoped ``does not <shipping verb>`` negation."""
    clause_start = max(text.rfind(c, 0, match_start) for c in _CLAUSE_BOUNDARIES)
    clause_start = clause_start + 1 if clause_start >= 0 else 0
    negations = list(
        re.finditer(
            r"\b(?:does\s+not|doesn't|doesn’t)\b",
            text[clause_start:match_start],
            re.IGNORECASE,
        )
    )
    if not negations:
        return False
    nearest = negations[-1]
    negation_end = clause_start + nearest.end()
    between = text[negation_end:match_start]
    if _DOUBLE_NEGATIVE_INVERTER.search(between):
        return False
    return bool(re.fullmatch(r"(?:\s+\w+){0,3}\s+", between))


def _has_conditional_can_negation_before_cross_repo_command(
    text: str,
    match_start: int,
) -> bool:
    """Return True when ``can't`` negates a condition, not the command."""
    clause_start = max(text.rfind(c, 0, match_start) for c in _CLAUSE_BOUNDARIES)
    clause_start = clause_start + 1 if clause_start >= 0 else 0
    clause_prefix = text[clause_start:match_start]
    negations = list(_NEGATION_CONTEXT.finditer(clause_prefix))
    if not negations:
        return False
    nearest = negations[-1]
    if not re.fullmatch(
        r"(?:cannot|can not|can't|can’t)",
        nearest.group(0),
        re.IGNORECASE,
    ):
        return False
    prefix = text[clause_start : clause_start + nearest.start()]
    if not re.search(r"\b(?:if|when|unless)\b", prefix, re.IGNORECASE):
        return False
    negation_end = clause_start + nearest.end()
    between = text[negation_end:match_start]
    return len(re.findall(r"\b\w+\b", between)) >= 2


_INLINE_CODE_SUPPRESSION_CATEGORIES = {
    "cross_repo_repo_create",
    "cross_repo_repo_delete",
    "cross_repo_ships_in",
}

_FENCED_CODE_SUPPRESSION_CATEGORIES = {
    "cross_repo_ships_in",
}


def _is_inside_inline_code(text: str, match_start: int) -> bool:
    """Return True when ``match_start`` is in inline code.

    This intentionally checks only the current physical line. The
    cross-repo patterns need to ignore specs that describe the detection
    rule itself, without changing long-standing behavior for the older
    anti-pattern categories.
    """
    line_start = text.rfind("\n", 0, match_start) + 1
    line_end = text.find("\n", match_start)
    if line_end == -1:
        line_end = len(text)
    line = text[line_start:line_end]
    relative_match_start = match_start - line_start
    cursor = 0
    opener_pattern = re.compile(r"(?<!\\)`+")
    while cursor < len(line):
        opener = opener_pattern.search(line, cursor)
        if opener is None:
            return False
        opening_start = opener.start()
        opening_end = opener.end()
        delimiter_len = opening_end - opening_start
        closing_pattern = re.compile(rf"(?<!`)`{{{delimiter_len}}}(?!`)")
        closer = closing_pattern.search(line, opening_end)
        if closer is None:
            return False
        closing_start = closer.start()
        closing_end = closer.end()
        if opening_end <= relative_match_start < closing_start:
            return True
        cursor = closing_end
    return False


def _is_inside_double_quoted_span(text: str, match_start: int) -> bool:
    """Return True when ``match_start`` is inside a closed double-quoted span."""
    line_start = text.rfind("\n", 0, match_start) + 1
    line_end = text.find("\n", match_start)
    if line_end == -1:
        line_end = len(text)
    line = text[line_start:line_end]
    relative_match_start = match_start - line_start
    opener: int | None = None
    escaped = False
    for index, char in enumerate(line):
        if escaped:
            escaped = False
            continue
        if char == "\\":
            escaped = True
            continue
        if char != '"':
            continue
        if opener is None:
            opener = index
            continue
        if opener < relative_match_start < index:
            return True
        opener = None
    return False


def _is_inside_fenced_code(text: str, match_start: int) -> bool:
    """Return True when ``match_start`` is inside a Markdown code fence."""
    in_fence = False
    fence_char = ""
    fence_len = 0
    line_start = 0
    fence_pattern = re.compile(r"^[ \t]{0,3}(`{3,}|~{3,})(.*)$")
    while line_start < match_start:
        line_end = text.find("\n", line_start)
        if line_end == -1 or line_end > match_start:
            break
        line = text[line_start:line_end]
        fence = fence_pattern.match(line)
        if fence:
            marker = fence.group(1)
            suffix = fence.group(2)
            if not in_fence:
                in_fence = True
                fence_char = marker[0]
                fence_len = len(marker)
            elif (
                marker[0] == fence_char
                and len(marker) >= fence_len
                and suffix.strip() == ""
            ):
                in_fence = False
                fence_char = ""
                fence_len = 0
        line_start = line_end + 1
    return in_fence


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
    (
        "cross_repo_repo_create",
        # gh CLI command that creates a new GitHub repository on the
        # operator's account or organization. ``gh repo new`` is a
        # documented alias of ``gh repo create``; negation suppression
        # is handled by the shared _is_negated check.
        re.compile(r"\bgh repo (?:create|new)\b", re.IGNORECASE),
        "AGENTS.md prohibits creating GitHub repositories from coder dispatches. OBS-BT.",
    ),
    (
        "cross_repo_repo_delete",
        # gh CLI command that deletes a GitHub repository. Same form
        # and rationale as cross_repo_repo_create.
        re.compile(r"\bgh repo delete\b", re.IGNORECASE),
        "AGENTS.md prohibits deleting GitHub repositories from coder dispatches. OBS-BT.",
    ),
    (
        "cross_repo_ships_in",
        # Phrase form indicating the spec author believes this task ships
        # in a different repository. Captures phrasings observed in
        # OBS-BT (verb forms: `ships`, `belongs`, `lives`, `deploys`,
        # `targets`; identifier shape: slug-style chars). The verb arm
        # intentionally requires repository context so ordinary task prose
        # such as "service deploys to production" or "This PR targets
        # Python 3.12" is not treated as cross-repo routing intent.
        # Negation suppression by the shared `_is_negated` check covers
        # prose that explicitly forbids the shipping verb.
        re.compile(
            r"\b(?:"
            r"(?:ships? in|belongs to|lives in|deploys to|targets) "
            r"(?:[\w.-]+/[\w.-]+(?!/)|"
            r"(?!the repo(?:sitory)?\b)(?:the (?!repo(?:sitory)?\b)\w+|\w+) repo(?:sitory)?|"
            r"(?:the )?[\w.-]*[.-][\w.-]+ repo(?:sitory)?)|"
            r"in (?:(?:the )?[\w.-]*[.-][\w.-]+ repo(?:sitory)?|"
            r"(?:the )?[\w.-]+/[\w.-]+ repo(?:sitory)?|"
            r"(?!the repository\b)(?:the (?!repository\b)\w+|\w+) repository)"
            r")\b",
            re.IGNORECASE,
        ),
        "Cross-repo target phrasing detected. Verify the task is uploaded to the correct repo. OBS-BT.",
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
                if vtype in {
                    "cross_repo_repo_create",
                    "cross_repo_repo_delete",
                } and _has_conditional_can_negation_before_cross_repo_command(
                    normalized,
                    match.start(),
                ):
                    pass
                else:
                    continue
            if vtype == "cross_repo_ships_in" and _is_negated_cross_repo_shipping(
                normalized,
                match.start(),
            ):
                continue
            if (
                vtype in _INLINE_CODE_SUPPRESSION_CATEGORIES
                and _is_inside_inline_code(normalized, match.start())
            ):
                continue
            if (
                vtype in _INLINE_CODE_SUPPRESSION_CATEGORIES
                and _is_inside_double_quoted_span(normalized, match.start())
            ):
                continue
            if (
                vtype in _FENCED_CODE_SUPPRESSION_CATEGORIES
                and _is_inside_fenced_code(normalized, match.start())
            ):
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
