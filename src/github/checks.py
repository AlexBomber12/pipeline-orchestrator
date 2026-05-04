"""GitHub commit/check-run status helpers.

Owns the REST ``check-runs`` + commit ``status`` fetch path that powers the
WATCH gate's CI status read. Reuses ``cache._etag_get`` and
``cache._gh_api_paginated`` for ETag-conditional REST reads.
"""

from __future__ import annotations

import json
import time
from typing import Any

from src.github import cache
from src.models import CIStatus
from src.retry import retry_transient


def _pending_tracker_key(repo: str, pr_number: int, head_sha: str) -> str:
    """Return the Redis key for the per-(repo, pr, sha) PENDING tracker."""
    return f"ci_pending_start:{repo}:{pr_number}:{head_sha}"

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
        cr_pages = cache._gh_api_paginated(check_runs_path)
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
            lambda: cache._etag_get(status_path),
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


async def _clear_pending_tracker(
    redis_client: Any, repo: str, pr_number: int, head_sha: str
) -> None:
    """Drop the stuck-PENDING tracker key for ``(repo, pr_number, head_sha)``.

    Called whenever the raw CI status leaves PENDING so a later regression
    back into PENDING (rare but possible when GitHub republishes a stale
    check) restarts the age clock from zero rather than re-using the
    original first-seen timestamp.
    """
    if redis_client is None or not head_sha:
        return
    await redis_client.delete(_pending_tracker_key(repo, pr_number, head_sha))


async def _get_or_set_pending_first_seen(
    redis_client: Any,
    repo: str,
    pr_number: int,
    head_sha: str,
    pending_max_seconds: int,
    now_seconds: float | None = None,
) -> float:
    """Return the first-seen-PENDING timestamp for ``head_sha``, writing it on first call.

    Uses ``SET NX`` so concurrent WATCH cycles across runners agree on a
    single anchor. The TTL is ``pending_max_seconds * 2`` so an abandoned
    tracker (PR closed before the threshold fires) self-expires from
    Redis without manual cleanup. The first-seen value uses Redis
    server time when available so daemon clock skew between restarts
    does not invalidate an in-flight window.

    ``now_seconds`` lets the caller share a single clock reading between
    the first-seen write and a subsequent age comparison; when omitted
    the helper resolves it via :func:`_resolve_now_seconds` (Redis
    server time with local fallback).
    """
    key = _pending_tracker_key(repo, pr_number, head_sha)
    raw = await redis_client.get(key)
    if raw is not None:
        try:
            return float(raw)
        except (TypeError, ValueError):
            pass

    if now_seconds is None:
        now_seconds = await _resolve_now_seconds(redis_client)

    ttl = max(1, pending_max_seconds * 2)
    await redis_client.set(key, str(now_seconds), nx=True, ex=ttl)

    # Re-read to honor a racing writer that won the SET NX.
    raw_after = await redis_client.get(key)
    if raw_after is not None:
        try:
            return float(raw_after)
        except (TypeError, ValueError):
            return now_seconds
    return now_seconds


async def _resolve_now_seconds(redis_client: Any) -> float:
    """Return current wall-clock seconds from Redis when available, else local.

    Centralizes the "Redis server time with local fallback" choice so the
    first-seen write and the later age comparison both come from the same
    source within a single classification call. Without this, daemon
    clock skew (NTP step or drift relative to Redis) would make
    ``age_seconds = local_now - redis_first_seen`` non-deterministic
    across hosts and restarts: too large would trigger ``stuck_pending``
    early, negative would prevent reclassification entirely.
    """
    redis_now = await _redis_server_time(redis_client)
    if redis_now is not None:
        return redis_now
    return time.time()


async def _redis_server_time(redis_client: Any) -> float | None:
    """Return Redis server-side wall-clock seconds, or ``None`` if unsupported.

    Production ``redis.asyncio`` clients expose ``time()`` returning
    ``(seconds, microseconds)``; the in-test ``_FakeRedis`` does not, in
    which case we fall back to ``time.time()`` at the call site.
    """
    time_fn = getattr(redis_client, "time", None)
    if time_fn is None:
        return None
    result = time_fn()
    if hasattr(result, "__await__"):
        result = await result
    if not isinstance(result, (tuple, list)) or len(result) < 2:
        return None
    seconds = float(result[0])
    microseconds = float(result[1])
    return seconds + microseconds / 1_000_000


async def classify_ci_status_with_age(
    repo: str,
    pr_number: int,
    head_sha: str,
    redis_client: Any,
    pending_max_seconds: int,
    runs_payload: list[dict],
    statuses_payload: dict,
    *,
    empty_is_success: bool = False,
    fetch_ok: bool = True,
) -> tuple[CIStatus, str | None]:
    """Augment :func:`_map_rest_ci_status_to_enum` with stuck-PENDING reclassification.

    Returns ``(status, reclassification_reason)``. When the raw status is
    PENDING for longer than ``pending_max_seconds`` on the same
    ``head_sha``, returns ``(CIStatus.FAILURE, "stuck_pending")``;
    otherwise returns the raw status with reason ``None``. The
    first-seen-PENDING timestamp is tracked per ``head_sha`` in Redis so
    a fresh push naturally resets the clock, and the tracker is cleared
    whenever the raw status leaves PENDING so a transient regression
    back into PENDING starts a new window.

    PR-250.
    """
    raw_status = _map_rest_ci_status_to_enum(
        runs_payload,
        statuses_payload,
        empty_is_success=empty_is_success,
        fetch_ok=fetch_ok,
    )
    if raw_status != CIStatus.PENDING:
        await _clear_pending_tracker(redis_client, repo, pr_number, head_sha)
        return raw_status, None
    if redis_client is None or not head_sha or pending_max_seconds <= 0:
        return raw_status, None

    # Resolve "now" once and pass it down so the first-seen write and
    # the age comparison share a single clock source. Mixing Redis time
    # for first_seen with local time.time() here makes age_seconds
    # unstable under NTP steps or daemon-vs-Redis clock skew.
    now_seconds = await _resolve_now_seconds(redis_client)
    first_seen = await _get_or_set_pending_first_seen(
        redis_client,
        repo,
        pr_number,
        head_sha,
        pending_max_seconds,
        now_seconds=now_seconds,
    )
    age_seconds = now_seconds - first_seen
    if age_seconds >= pending_max_seconds:
        return CIStatus.FAILURE, "stuck_pending"
    return raw_status, None
