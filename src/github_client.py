"""Partial shim during PR-226a→PR-226b migration.

Foundation, PR-list, CI-checks, and review-status code now lives in
``src.github.*`` (PR-226a). The remaining four domains — reactions
(``_get_codex_issue_reactions``), comments (``post_comment``,
``get_latest_codex_feedback``, ``has_recent_codex_review_request``),
rate-limit (``fetch_rate_limit_*``), and ETag cache primitives — still
live here and will move in PR-226b. Existing ``from src.github_client``
imports remain valid for the entire migration window.
"""

from __future__ import annotations

import collections
import itertools
import json
import logging
import re
import subprocess
import time  # noqa: F401 — submodules look up time.monotonic via this binding so test patches on src.github_client.time.monotonic flow through.
from datetime import datetime, timezone

from src.daemon.github_rate_limit import RateLimitBudget, read_budget

# ===== Foundation re-exports (src.github.gh_runner) =====
from src.github.gh_runner import (  # noqa: F401
    _REPO_URL_RE,
    _extract_commit_date,
    _extract_head_sha,
    _is_http_404_error,
    _parse_iso,
    get_repo_full_name,
    run_gh,
)
from src.models import CIStatus, PRInfo, ReviewStatus  # noqa: F401 — re-exported for callers
from src.retry import (  # noqa: F401 — re-exported so submodules and tests reach them through src.github_client
    is_transient_error,
    retry_transient,
)

logger = logging.getLogger(__name__)

_REVIEW_FEEDBACK_TRUNCATE_CHARS = 5000

# ===== ETag cache primitives (still here, move in PR-226b) =====
#:
#: In-memory ETag cache for single-resource REST GET helpers. Keyed by the
#: ``gh api`` path (the same string passed to ``_etag_get``); the value is
#: ``(etag, parsed_payload)``. Sending ``If-None-Match`` on a cached path
#: lets GitHub respond with HTTP 304 and an empty body — and crucially that
#: 304 does not consume rate-limit budget. Daemon polling re-queries the
#: same handful of endpoints repeatedly with low data turnover, so most
#: cycles hit 304 and become free.
#:
#: The cache is in-memory only (lost on daemon restart); a persistent
#: cache could extend the cold-start grace but is not needed for the
#: primary diet effect.
_etag_cache: "collections.OrderedDict[str, tuple[str, object]]" = (
    collections.OrderedDict()
)
_ETAG_CACHE_MAX_ENTRIES = 500


def clear_etag_cache() -> None:
    """Clear the ETag conditional-request cache (used in tests)."""
    _etag_cache.clear()


def _etag_cache_put(path: str, etag: str, payload: object) -> None:
    """Insert into the ETag cache with simple LRU eviction."""
    _etag_cache[path] = (etag, payload)
    _etag_cache.move_to_end(path)
    while len(_etag_cache) > _ETAG_CACHE_MAX_ENTRIES:
        _etag_cache.popitem(last=False)


def _invalidate_etag_cache(prefix: str) -> None:
    """Drop ETag cache entries whose path begins with ``prefix``.

    Called at known list-mutation points (PR create, merge, close) so the
    next REST list fetch returns a fresh 200 instead of a cached 304 that
    would mask the new state for one polling cycle. False invalidation
    only costs one extra API call; false non-invalidation hides a real
    state change, so the call sites are deliberately conservative — when
    in doubt, invalidate.
    """
    stale = [key for key in _etag_cache if key.startswith(prefix)]
    for key in stale:
        _etag_cache.pop(key, None)


# List endpoints whose pages are walked one-at-a-time so each page can
# return 304 independently. Currently scoped to the top-level
# ``repos/{owner}/{name}/pulls`` list (open and closed states) — the
# dominant REST consumer when the GraphQL ``gh pr list`` rollup falls
# back. Sub-resources like ``pulls/{n}/comments`` stay on the legacy
# slurp path because they change too often for ETag caching to help.
_ETAG_PAGINATED_PATH_RE = re.compile(
    r"^repos/[^/]+/[^/]+/pulls(?:\?[^#]*)?$"
)
_ETAG_PAGINATED_DEFAULT_PER_PAGE = 30


_HTTP_STATUS_RE = re.compile(r"^HTTP/\S+\s+(\d{3})", re.MULTILINE)


def _split_include_response(raw: str) -> tuple[int | None, dict[str, str], str]:
    """Parse the output of ``gh api --include`` into ``(status, headers, body)``.

    The ``--include`` flag prepends the HTTP status line and headers to the
    response body, separated by a blank line. Splitting on the FIRST blank
    line yields the head and body; the LAST ``HTTP/`` line in the head is
    taken as the final status (in case ``gh`` surfaces an intermediate
    redirect, though by default it follows redirects internally).
    """
    sep = re.search(r"\r?\n\r?\n", raw)
    head = raw[: sep.start()] if sep else raw
    body = raw[sep.end() :] if sep else ""

    status: int | None = None
    matches = list(_HTTP_STATUS_RE.finditer(head))
    if matches:
        status = int(matches[-1].group(1))

    headers: dict[str, str] = {}
    for line in head.splitlines():
        if line.startswith("HTTP/") or ":" not in line:
            continue
        name, _, value = line.partition(":")
        headers[name.strip().lower()] = value.strip()

    return status, headers, body


def _etag_get(path: str) -> object:
    """Fetch a single-resource REST endpoint with ETag conditional caching.

    Issues ``gh api --include`` so the response status and ``ETag`` header
    are captured on stdout. When a prior ETag is cached for ``path`` an
    ``If-None-Match`` header is sent: a HTTP 304 response (free against
    the rate-limit budget) returns the cached payload; a 2xx response
    parses and re-caches the body. Returns ``None`` on non-2xx/304
    responses or unparseable bodies. Raises whatever ``run_gh`` raises
    on hard ``gh`` failures so callers can apply their own degradation.

    Paginated list endpoints are out of scope (handled in PR-191b); this
    helper expects a JSON object/array body for a single resource.
    """
    args: list[str] = ["api", path, "--include"]
    cached = _etag_cache.get(path)
    if cached is not None:
        args.extend(["-H", f"If-None-Match: {cached[0]}"])

    raw = run_gh(args)

    # When stubbed (tests mock ``run_gh`` to return a pre-parsed object or a
    # bare ``--jq`` scalar), bypass ``--include`` parsing and surface the
    # value directly so existing call-site tests keep their semantics.
    if not isinstance(raw, str) or not raw.lstrip().startswith("HTTP/"):
        return raw

    status, headers, body = _split_include_response(raw)
    if status == 304:
        if cached is None:
            # Server returned 304 but we have no cached body. This happens
            # after daemon restart (in-memory cache empty) or LRU eviction
            # while the GitHub edge cache still holds our prior ETag.
            # Retry without If-None-Match to force a fresh 200 + body.
            return _etag_get_no_cache(path)
        _etag_cache.move_to_end(path)
        return cached[1]
    if status is None or not (200 <= status < 300):
        return None
    body = body.strip()
    if not body:
        return None
    try:
        payload = json.loads(body)
    except json.JSONDecodeError:
        return None

    etag = headers.get("etag")
    if etag:
        _etag_cache_put(path, etag, payload)
    return payload


def _etag_get_no_cache(path: str) -> object:
    """Fetch ``path`` without ``If-None-Match`` and re-populate the cache."""
    raw = run_gh(["api", path, "--include"])
    if not isinstance(raw, str) or not raw.lstrip().startswith("HTTP/"):
        return raw
    status, headers, body = _split_include_response(raw)
    if status is None or not (200 <= status < 300):
        return None
    body = body.strip()
    if not body:
        return None
    try:
        payload = json.loads(body)
    except json.JSONDecodeError:
        return None
    etag = headers.get("etag")
    if etag:
        _etag_cache_put(path, etag, payload)
    return payload


def _gh_api_paginated(path: str) -> list[dict] | None:
    """Fetch every page of a GitHub REST endpoint that returns a JSON array.

    For list endpoints that benefit from ETag conditional caching (the
    top-level ``repos/{owner}/{name}/pulls`` list) the call is routed to
    ``_etag_get_paginated`` so each page is fetched individually with its
    own ``If-None-Match`` and 304s do not consume rate-limit budget.
    Other paths (sub-resource lists like ``pulls/{n}/comments`` or
    ``issues/{n}/reactions``, where ETag re-validation would rarely hit)
    keep the legacy ``gh api --paginate --slurp`` flow.

    The slurp path uses ``--slurp`` because ``--paginate`` alone writes
    one JSON document per page back-to-back, which is not parseable as a
    single document. Pages are flattened into a single item list.
    Returns ``None`` if the response is not a list (which would indicate
    an unexpected ``gh`` output).

    Wraps the underlying ``run_gh`` call with ``retry_transient`` so
    transient network errors (connection reset, 502/503/504, timeout) are
    retried up to 3 times with exponential backoff before propagating.
    """
    if _ETAG_PAGINATED_PATH_RE.match(path):
        return _etag_get_paginated(path)

    raw = retry_transient(
        lambda: run_gh(["api", "--paginate", "--slurp", path]),
        operation_name=f"gh api {path}",
    )
    if not isinstance(raw, list):
        return None

    items: list[dict] = []
    for page in raw:
        if isinstance(page, list):
            items.extend(item for item in page if isinstance(item, dict))
        elif isinstance(page, dict):
            items.append(page)
    return items


def _etag_get_paginated(path: str) -> list[dict] | None:
    """Walk a JSON-array REST list endpoint one page at a time, ETag-cached.

    Each page is keyed in ``_etag_cache`` by its full URL including
    ``page=N`` (and any pre-existing ``per_page=M``) so a 304 from any
    individual page returns its cached payload — and crucially that 304
    is free against the rate-limit budget. Pages with real changes
    round-trip a fresh 200 and refresh the cache.

    Stops when a page returns fewer items than the per-page size declared
    in the URL (the GitHub convention for "last page"); when the URL does
    not declare ``per_page=`` the helper assumes the GitHub default (30).
    No hard page cap: ``repos/{owner}/{name}/pulls?state=closed`` for a
    large repo can exceed 10,000 PRs, and the legacy ``gh api --paginate``
    walked until exhausted — capping at 100 pages would silently truncate
    merged history on those repos and let ``get_merged_prs`` derive
    queue/task status from an incomplete view.

    Returns ``None`` only when the very first page cannot be fetched or
    parsed; partial results from later pages are surfaced as-is so a
    transient mid-walk error degrades gracefully rather than dropping the
    whole list.

    Wraps each per-page ``_etag_get`` in ``retry_transient`` to match the
    transient-retry semantics callers had under the legacy slurp path.
    """
    sep = "&" if "?" in path else "?"
    per_page_match = re.search(r"(?:^|[?&])per_page=(\d+)", path)
    per_page = (
        int(per_page_match.group(1))
        if per_page_match
        else _ETAG_PAGINATED_DEFAULT_PER_PAGE
    )
    items: list[dict] = []
    for page_num in itertools.count(1):
        url = f"{path}{sep}page={page_num}"
        try:
            payload = retry_transient(
                lambda u=url: _etag_get(u),
                operation_name=f"gh api {url}",
            )
        except RuntimeError:
            if page_num == 1:
                raise
            break
        if payload is None:
            if page_num == 1:
                return None
            break
        if not isinstance(payload, list):
            if page_num == 1:
                return None
            break
        items.extend(item for item in payload if isinstance(item, dict))
        if len(payload) < per_page:
            break
    return items


# ===== CI-status re-exports (src.github.checks) =====
from src.github.checks import (  # noqa: E402, F401
    _CI_STATUS_CACHE_TTL_SECONDS,
    _REST_CI_FAILURE_STATES,
    _REST_CI_SUCCESS_STATES,
    _ci_status_cache,
    _evict_expired_ci_status_cache,
    _fetch_ci_status_rest,
    _map_rest_ci_status_to_enum,
    clear_ci_status_cache,
)

# ===== PR re-exports (src.github.prs) =====
from src.github.prs import (  # noqa: E402, F401
    _MERGED_PRS_CACHE_TTL_SECONDS,
    GitHubPollError,
    _extract_author_and_head_sha,
    _get_open_prs_rest,
    _last_known_sha,
    _merged_prs_cache,
    clear_last_known_sha,
    clear_merged_prs_cache,
    extract_queue_pr_id,
    get_branch_last_push_time,
    get_last_push_age_seconds,
    get_merged_prs,
    get_open_prs,
    get_pr_author,
    get_pr_head_commit_iso,
    get_pr_last_push_time,
    get_pr_metadata,
    is_pr_merged,
    merge_pr,
    pr_state,
)

# ===== Review-status re-exports (src.github.reviews) =====
from src.github.reviews import (  # noqa: E402, F401
    _CODEX_ONBOARDING_TEXT,
    CODEX_BOT_LOGIN_PATTERN,
    _begin_review_cache_cycle,
    _cache_key,
    _compute_review_status,
    _find_codex_plus_one_reaction,
    _get_codex_issue_reactions,
    _get_codex_review_signals,
    _get_commit_time,
    _get_latest_codex_review_info,
    _is_codex_onboarding_comment,
    _is_codex_user,
    _is_plus_one,
    _is_reaction_content,
    _review_status_cache,
    _should_degrade_reactions_error,
    clear_review_status_cache,
    get_pr_review_status,
)


def __getattr__(name: str) -> object:
    """Forward live reads of mutable module-level state held in src.github.*.

    Module-level integers reassigned via ``global`` (notably
    ``_review_status_cache_cycle``) lose their connection to the shim's
    ``from src.github.reviews import ...`` snapshot when the canonical
    module rebinds them, so we look those names up live on each access
    and return the current value.
    """
    if name == "_review_status_cache_cycle":
        from src.github import reviews

        return reviews._review_status_cache_cycle
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def get_latest_codex_feedback(repo: str, pr_number: int) -> str | None:
    """Return concatenated Codex feedback comments after the latest review anchor.

    Pulls from the same sources as ``_compute_review_status`` — Codex-authored
    issue and pull review comments posted after the most recent
    ``@codex review`` trigger by the PR author, with onboarding messages
    filtered out — so the FIX prompt sees exactly the feedback that drove
    ``ReviewStatus.CHANGES_REQUESTED``. Returns ``None`` when no qualifying
    feedback exists or both comment endpoints are unreachable; the FIX
    prompt then omits the section instead of blocking on observability.
    """
    pr_author = get_pr_author(repo, pr_number)
    try:
        issue_comments = (
            _gh_api_paginated(f"repos/{repo}/issues/{pr_number}/comments") or []
        )
    except (RuntimeError, subprocess.TimeoutExpired, OSError):
        issue_comments = []
    try:
        review_comments = (
            _gh_api_paginated(f"repos/{repo}/pulls/{pr_number}/comments") or []
        )
    except (RuntimeError, subprocess.TimeoutExpired, OSError):
        review_comments = []

    anchor_ts = ""
    for c in reversed(issue_comments):
        author = (c.get("user") or {}).get("login", "")
        if pr_author and author != pr_author:
            continue
        if "@codex review" in (c.get("body") or "").lower():
            anchor_ts = c.get("created_at") or ""
            break

    sections: list[str] = []
    for comment in issue_comments + review_comments:
        if not _is_codex_user(comment.get("user")):
            continue
        if _is_codex_onboarding_comment(comment):
            continue
        if anchor_ts and (comment.get("created_at") or "") <= anchor_ts:
            continue
        body = (comment.get("body") or "").strip()
        if body:
            sections.append(body)

    if not sections:
        return None
    joined = "\n\n".join(sections)
    if len(joined) > _REVIEW_FEEDBACK_TRUNCATE_CHARS:
        return f"[truncated]\n{joined[-_REVIEW_FEEDBACK_TRUNCATE_CHARS:]}"
    return joined


def post_comment(repo: str, pr_number: int, body: str) -> None:
    """Post a comment on a PR via ``gh pr comment``."""
    run_gh(["pr", "comment", str(pr_number), "--body", body], repo=repo)


def has_recent_codex_review_request(
    repo: str,
    pr_number: int,
    pr_author: str,
    within_minutes: int = 5,
    after_iso: str | None = None,
) -> bool:
    """Return ``True`` iff ``pr_author`` recently posted ``@codex review``.

    The daemon posts ``@codex review`` after every coding/fix cycle, but
    Claude may also post one itself from the AGENTS.md runbook. Without
    this guard both trigger comments land back-to-back and Codex starts
    two redundant reviews. The caller checks this before posting and
    skips when a qualifying trigger already exists within
    ``within_minutes``.

    ``after_iso`` optionally restricts matches to comments created
    strictly after the given ISO-8601 timestamp. Callers pass the
    PR's current head-commit time so a trigger posted for an earlier
    commit does not suppress the fresh anchor the new commit needs —
    this is what keeps the dedup safe when the daemon and PR author
    share a gh identity.
    """
    return (
        get_recent_codex_review_request_time(
            repo,
            pr_number,
            pr_author,
            within_minutes=within_minutes,
            after_iso=after_iso,
        )
        is not None
    )


def get_recent_codex_review_request_time(
    repo: str,
    pr_number: int,
    pr_author: str,
    within_minutes: int = 5,
    after_iso: str | None = None,
) -> datetime | None:
    """Return the latest qualifying PR-author ``@codex review`` timestamp."""
    try:
        comments = _gh_api_paginated(
            f"repos/{repo}/issues/{pr_number}/comments"
        ) or []
    except RuntimeError as exc:
        if _is_http_404_error(exc):
            return None
        raise
    now = datetime.now(timezone.utc)
    cutoff = within_minutes * 60
    for c in reversed(comments):
        author = (c.get("user") or {}).get("login", "")
        if author != pr_author:
            continue
        if "@codex review" not in (c.get("body") or "").lower():
            continue
        created_raw = c.get("created_at") or ""
        if after_iso and (not created_raw or created_raw <= after_iso):
            continue
        created = _parse_iso(created_raw)
        if created is None:
            continue
        if created.tzinfo is None:
            created = created.replace(tzinfo=timezone.utc)
        if (now - created).total_seconds() < cutoff:
            return created
    return None


async def get_current_rate_limit_budget(
    redis_client: object,
) -> RateLimitBudget | None:
    """Return the most recent GitHub API rate-limit budget from Redis.

    Returns ``None`` when no observation has been persisted yet (daemon
    just started, the rate_limit fetch failed, or Redis is unavailable).
    Callers treat ``None`` as "no data, proceed normally".
    """
    return await read_budget(redis_client)


def fetch_rate_limit_buckets() -> tuple[RateLimitBudget | None, RateLimitBudget | None]:
    """Fetch ``gh api rate_limit`` and return ``(rest_core, graphql)`` buckets.

    Either bucket may be ``None`` when the bucket is missing or its payload
    is malformed. Returns ``(None, None)`` if the gh CLI call fails or the
    response itself is unparseable so callers can treat the result as
    "no data" without distinguishing failure modes.
    """
    try:
        raw = run_gh(
            [
                "api",
                "rate_limit",
                "--jq",
                "{core: .resources.core, graphql: .resources.graphql}",
            ]
        )
    except (RuntimeError, subprocess.TimeoutExpired, OSError):
        return None, None
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except json.JSONDecodeError:
            return None, None
    if not isinstance(raw, dict):
        return None, None
    core = _parse_rate_limit_bucket(raw.get("core"))
    graphql = _parse_rate_limit_bucket(raw.get("graphql"))
    return core, graphql


def fetch_rate_limit_budget() -> RateLimitBudget | None:
    """Fetch ``gh api rate_limit`` and return a parsed :class:`RateLimitBudget`.

    Returns the more constrained of the REST/core and GraphQL buckets so the
    daemon throttles before either is exhausted. Hot-path polling here uses
    GraphQL-heavy ``gh`` commands (``gh pr list --json …``), which consume
    GraphQL points independently of the REST/core bucket; tracking only
    ``rate.*`` (== ``resources.core``) would let GraphQL exhaustion slip
    through. Returns ``None`` if the gh CLI call fails or returns an
    unparseable payload — callers treat that as "no data".
    """
    core, graphql = fetch_rate_limit_buckets()
    candidates = [b for b in (core, graphql) if b is not None]
    if not candidates:
        return None
    return min(candidates, key=lambda b: b.remaining_percent)


def _parse_rate_limit_bucket(raw: object) -> RateLimitBudget | None:
    """Parse one ``resources.<bucket>`` entry from ``gh api rate_limit``."""
    if not isinstance(raw, dict):
        return None
    try:
        remaining = int(raw["remaining"])
        limit = int(raw["limit"])
        reset_ts = int(raw["reset"])
    except (KeyError, TypeError, ValueError):
        return None
    return RateLimitBudget(
        installation_id=None,
        remaining=remaining,
        limit=limit,
        reset_at=datetime.fromtimestamp(reset_ts, tz=timezone.utc),
    )
