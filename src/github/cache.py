"""ETag conditional-request cache and paginated REST helpers.

In-memory ETag cache for single-resource REST GET helpers plus the
single-page-at-a-time paginated walker that keeps GitHub list endpoints
on the same conditional-request diet. ``_etag_get`` and
``_gh_api_paginated`` are the entry points the rest of the
``src.github`` package uses for REST reads; the cache stays purely
process-local (lost on daemon restart).
"""

from __future__ import annotations

import collections
import itertools
import json
import re

from src.github import gh_runner
from src.retry import retry_transient

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
_etag_cache: "collections.OrderedDict[str, tuple[str, object]]" = collections.OrderedDict()
_ETAG_CACHE_MAX_ENTRIES = 500


# List endpoints whose pages are walked one-at-a-time so each page can
# return 304 independently. Currently scoped to the top-level
# ``repos/{owner}/{name}/pulls`` list (open and closed states) — the
# dominant REST consumer when the GraphQL ``gh pr list`` rollup falls
# back. Sub-resources like ``pulls/{n}/comments`` stay on the legacy
# slurp path because they change too often for ETag caching to help.
_ETAG_PAGINATED_PATH_RE = re.compile(r"^repos/[^/]+/[^/]+/pulls(?:\?[^#]*)?$")
_ETAG_PAGINATED_DEFAULT_PER_PAGE = 30


_HTTP_STATUS_RE = re.compile(r"^HTTP/\S+\s+(\d{3})", re.MULTILINE)


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

    raw = gh_runner.run_gh(args)

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
    raw = gh_runner.run_gh(["api", path, "--include"])
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
        lambda: gh_runner.run_gh(["api", "--paginate", "--slurp", path]),
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
    per_page = int(per_page_match.group(1)) if per_page_match else _ETAG_PAGINATED_DEFAULT_PER_PAGE
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
