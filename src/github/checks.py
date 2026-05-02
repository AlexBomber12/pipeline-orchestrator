"""GitHub commit/check-run status helpers.

Owns the REST ``check-runs`` + commit ``status`` fetch path that powers the
WATCH gate's CI status read. Cache primitives (``_etag_get``,
``_gh_api_paginated``) still live in ``src.github_client`` until PR-226b;
this module accesses them via ``from src import github_client``.
"""

from __future__ import annotations

import json
import time

from src.models import CIStatus
from src.retry import retry_transient

_CI_STATUS_CACHE_TTL_SECONDS = 15.0

#: Per-(repo, sha) cache for the REST CI status fetch. The two REST calls
#: (``commits/{sha}/check-runs`` + ``commits/{sha}/status``) are the dominant
#: REST consumer in the daemon poll loop now that ``statusCheckRollup`` has
#: been removed. With ``poll_interval_sec`` as low as 2s in the e2e config,
#: refetching on every cycle exhausts the 5000/hour REST budget within
#: minutes — even one open PR at 2s polling burns 3600 calls/hour. The
#: cache key embeds ``head_sha`` so a new push immediately invalidates the
#: prior result; CI transitions on the same SHA are observed within
#: ``_CI_STATUS_CACHE_TTL_SECONDS`` of when GitHub publishes them, which is
#: well under the typical CI run length.
#:
#: Expired entries are swept on every write (i.e. on every cache miss).
#: Without that sweep the cache grows by one entry per push for every
#: watched repo — long-running daemons would retain full check-run
#: payloads for SHAs that will never be queried again. Sweeping on write
#: keeps the resident set ~O(unique SHAs queried within one TTL window).
_ci_status_cache: dict[
    tuple[str, str], tuple[float, list[dict], dict, bool]
] = {}


_REST_CI_FAILURE_STATES = {
    "FAILURE",
    "FAILED",
    "ERROR",
    "CANCELLED",
    "TIMED_OUT",
    "ACTION_REQUIRED",
}
_REST_CI_SUCCESS_STATES = {"SUCCESS", "COMPLETED", "NEUTRAL", "SKIPPED"}


def clear_ci_status_cache() -> None:
    """Clear the REST CI status cache (used in tests)."""
    _ci_status_cache.clear()


def _evict_expired_ci_status_cache(now: float) -> None:
    """Drop ``_ci_status_cache`` entries older than the TTL.

    Called from the cache-miss write path so the working set is bounded
    by the number of unique SHAs polled within a single TTL window
    rather than growing once per push for every watched repo.
    """
    expired = [
        key
        for key, entry in _ci_status_cache.items()
        if (now - entry[0]) >= _CI_STATUS_CACHE_TTL_SECONDS
    ]
    for key in expired:
        _ci_status_cache.pop(key, None)


def _fetch_ci_status_rest(repo: str, sha: str) -> tuple[list[dict], dict, bool]:
    """Fetch combined CI signals for ``sha`` via the REST API.

    Returns ``(check_runs, status_payload, fetch_ok)`` where ``check_runs`` is
    a flat list of all check_run dicts across pages from
    ``GET /repos/{repo}/commits/{sha}/check-runs`` and ``status_payload`` is
    the ``{"state": ..., "statuses": [...]}`` shape of
    ``GET /repos/{repo}/commits/{sha}/status``. ``fetch_ok`` is ``False`` only
    when *both* REST calls raised ``RuntimeError``; the caller currently
    folds that case back into ``empty_is_success`` so the WATCH gate does
    not stall on a transient REST-budget squeeze, matching the existing
    GraphQL-rate-limit fallback in ``_get_open_prs_rest``.
    """
    from src import github_client as _ghc

    check_runs: list[dict] = []
    status_payload: dict = {}
    if not sha:
        return check_runs, status_payload, True

    cache_key = (repo, sha)
    cached = _ci_status_cache.get(cache_key)
    now = time.monotonic()
    if cached is not None and (now - cached[0]) < _CI_STATUS_CACHE_TTL_SECONDS:
        return list(cached[1]), dict(cached[2]), cached[3]

    # check-runs is a paginated endpoint (per_page max 100). A commit can
    # carry more than 100 runs, and ``_map_rest_ci_status_to_enum`` reads
    # every entry — truncating to page 1 would let a failing or pending
    # run beyond the cap masquerade as SUCCESS and misclassify the PR as
    # mergeable, so we walk every page rather than relying on ETag-cached
    # single-page reads.
    check_runs_path = f"repos/{repo}/commits/{sha}/check-runs?per_page=100"
    check_runs_ok = False
    try:
        cr_pages = _ghc._gh_api_paginated(check_runs_path)
    except RuntimeError:
        cr_pages = None
    else:
        check_runs_ok = True
    if isinstance(cr_pages, list):
        for page in cr_pages:
            runs = page.get("check_runs")
            if isinstance(runs, list):
                check_runs.extend(r for r in runs if isinstance(r, dict))

    status_path = f"repos/{repo}/commits/{sha}/status"
    status_ok = False
    try:
        raw_status = retry_transient(
            lambda: _ghc._etag_get(status_path),
            operation_name=f"gh api {status_path}",
        )
    except RuntimeError:
        raw_status = None
    else:
        status_ok = True
    if isinstance(raw_status, dict):
        status_payload = raw_status
    elif isinstance(raw_status, str) and raw_status:
        try:
            parsed = json.loads(raw_status)
        except json.JSONDecodeError:
            parsed = None
        if isinstance(parsed, dict):
            status_payload = parsed

    fetch_ok = check_runs_ok or status_ok
    _evict_expired_ci_status_cache(now)
    _ci_status_cache[cache_key] = (now, list(check_runs), dict(status_payload), fetch_ok)
    return check_runs, status_payload, fetch_ok


def _map_rest_ci_status_to_enum(
    check_runs: list[dict],
    status_payload: dict,
    empty_is_success: bool = False,
    fetch_ok: bool = True,
) -> CIStatus:
    """Combine REST ``check-runs`` + commit ``status`` payloads into a ``CIStatus``.

    Mirrors the semantics of the previous rollup mapping: any failure-like
    state wins; SUCCESS only when every observed state is success-like;
    otherwise PENDING. When neither check-runs nor commit statuses are
    present the result follows ``empty_is_success`` so repos without
    required checks can still merge.

    When ``fetch_ok`` is ``False`` and there is no observable check data,
    the result still follows ``empty_is_success``: this matches the
    GraphQL-rate-limit fallback in ``_get_open_prs_rest``, which already
    returns SUCCESS for ``allow_merge_without_checks=True`` whenever the
    primary fetch is unavailable. Diverging here would mean a transient
    REST-budget squeeze (recurring in the e2e suite, where ``poll_interval_sec``
    is 2s and per-token quota is shared across runs) leaves the daemon
    permanently in WATCH on a testbed PR that has no checks at all,
    burning more REST on each retry without ever converging.

    The combined commit-status endpoint embeds at most the first page of
    ``statuses`` while ``status_payload["state"]`` reflects the aggregate
    across every context. In repos with many legacy status contexts the
    embedded list can omit a failing context entirely; honoring the
    aggregate ``state`` whenever any status context is reported keeps the
    pagination-capped failure from being silently classified SUCCESS.

    The ``statuses`` list itself is reverse-chronological history across
    contexts — the same context can appear multiple times with older
    states first overwritten by newer ones. ``state`` already reduces
    those entries to the latest per context, so the per-entry list is
    deliberately not consulted for the FAILURE/SUCCESS rollup; otherwise
    a stale ``failure`` from an earlier retry would force ``FAILURE``
    even after the latest status for that context turned green.
    """
    del fetch_ok  # retained for caller signature compatibility
    statuses_raw = (
        status_payload.get("statuses") if isinstance(status_payload, dict) else None
    )
    statuses = statuses_raw if isinstance(statuses_raw, list) else []
    combined_state = (
        status_payload.get("state") if isinstance(status_payload, dict) else None
    )

    if not check_runs and not statuses:
        return CIStatus.SUCCESS if empty_is_success else CIStatus.PENDING

    states: list[str] = []
    for run in check_runs:
        if not isinstance(run, dict):
            continue
        value = run.get("conclusion") or run.get("status")
        if value:
            states.append(str(value).upper())

    if statuses and isinstance(combined_state, str) and combined_state:
        states.append(combined_state.upper())

    if not states:
        return CIStatus.PENDING
    if any(s in _REST_CI_FAILURE_STATES for s in states):
        return CIStatus.FAILURE
    if all(s in _REST_CI_SUCCESS_STATES for s in states):
        return CIStatus.SUCCESS
    return CIStatus.PENDING
