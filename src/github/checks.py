"""GitHub commit/check-run status helpers.

Owns the REST ``check-runs`` + commit ``status`` fetch path that powers the
WATCH gate's CI status read. Reuses ``cache._etag_get`` and
``cache._gh_api_paginated`` for ETag-conditional REST reads.
"""

from __future__ import annotations

import time
from datetime import datetime
from typing import Any, NamedTuple

from src.github import cache, gh_runner
from src.github.ci_evidence import CIEvidence, evaluate_ci_evidence
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
class _CiSourceResult(NamedTuple):
    complete: bool
    empty: bool
    error: str | None = None


class _CiRetrieval(NamedTuple):
    evidence: CIEvidence
    fetched_at: float
    check_runs: list[dict]
    status_payload: dict
    check_runs_source: _CiSourceResult
    status_source: _CiSourceResult


_ci_status_cache: dict[tuple[str, str], _CiRetrieval] = {}


_REST_CI_FAILURE_STATES = {
    "FAILURE",
    "FAILED",
    "ERROR",
    "CANCELLED",
    "TIMED_OUT",
    "ACTION_REQUIRED",
    "STARTUP_FAILURE",
    # PR-251: ``stale`` is a GitHub Actions check-run conclusion meaning
    # the run is no longer relevant (workflow re-dispatched after a
    # newer push, or GitHub itself dropped the result). It must count
    # toward the failure rollup so ``_is_infra_failure`` can route it
    # through the INFRA_FAILURE retry path; otherwise a stale-only set
    # of check-runs would be silently classified ``PENDING``.
    "STALE",
}
_REST_CI_SUCCESS_STATES = {"SUCCESS", "COMPLETED", "NEUTRAL", "SKIPPED"}
_REST_COMMIT_STATUS_STATES = {"ERROR", "FAILURE", "PENDING", "SUCCESS"}
_REST_CHECK_RUN_STATUSES = {
    "COMPLETED",
    "IN_PROGRESS",
    "PENDING",
    "QUEUED",
    "REQUESTED",
    "WAITING",
}
_REST_CHECK_RUN_CONCLUSIONS = {
    "ACTION_REQUIRED",
    "CANCELLED",
    "FAILURE",
    "NEUTRAL",
    "SKIPPED",
    "STALE",
    "STARTUP_FAILURE",
    "SUCCESS",
    "TIMED_OUT",
}

# PR-251 (OBS-BC): conclusions that indicate an infrastructure-class
# failure rather than a logic failure. ``cancelled`` is unusual without
# operator intervention (workflow runner crash, abort), ``action_required``
# means the workflow boot pre-flight failed (GitHub App not installed,
# permissions missing), and ``stale`` means GitHub itself decided the
# run is no longer relevant.
_INFRA_CONCLUSION_STATES = {"CANCELLED", "ACTION_REQUIRED", "STALE"}

# PR-251 (OBS-BC): annotation message substrings that flag an
# infrastructure-class failure even when the conclusion itself looks
# logic-class (``failure``). The list is empirical and conservative —
# under-classifying (treating ambiguous as logic FAILURE and routing to
# FIX) is far safer than over-classifying (skipping a real bug as
# infra). Match is case-insensitive against the annotation ``message``
# field.
_INFRA_ANNOTATION_KEYWORDS = (
    "runner offline",
    "could not pull image",
    "no space left on device",
    "operation timed out",
    "infrastructure error",
    "we had a problem communicating with the server",
)

# PR-251 follow-up: hard cap on annotations fetched per failing
# check-run when hydrating for the infra-keyword scan. The classifier
# only needs *any* matching message, not the full set, and the
# annotations endpoint is paginated (``per_page`` max 100). Pulling
# every page on a large lint/test run can issue many REST calls per
# WATCH cycle for every failing run on every open PR, which quickly
# exhausts the GitHub REST budget. A single page of 50 messages
# captures any realistic infra annotation while keeping the worst case
# at one extra REST call per failing non-infra-conclusion check-run.
_ANNOTATION_HYDRATION_PER_PAGE = 50


def _is_infra_failure(check_run: dict) -> bool:
    """Return ``True`` iff a failing check-run's signals look infra-class.

    PR-251 (OBS-BC). Caller already filtered ``check_run`` down to runs
    whose conclusion/status maps to ``_REST_CI_FAILURE_STATES``; this
    helper decides whether the failure is an infrastructure flake
    (worth retrying once) versus a real logic failure (route to FIX).

    Two signals are checked, both case-insensitive:
    - ``conclusion`` in ``_INFRA_CONCLUSION_STATES``
    - any annotation ``message`` containing an
      ``_INFRA_ANNOTATION_KEYWORDS`` substring
    """
    conclusion = (check_run.get("conclusion") or "").upper()
    if conclusion in _INFRA_CONCLUSION_STATES:
        return True
    annotations = check_run.get("annotations") or []
    if not isinstance(annotations, list):
        return False
    for ann in annotations:
        if not isinstance(ann, dict):
            continue
        msg = (ann.get("message") or "").lower()
        if any(kw in msg for kw in _INFRA_ANNOTATION_KEYWORDS):
            return True
    return False


def _maybe_hydrate_annotations(repo: str, check_run: dict) -> None:
    """Populate ``check_run['annotations']`` from ``annotations_url``.

    PR-251 (OBS-BC). The ``GET /repos/{repo}/commits/{sha}/check-runs``
    response carries ``annotations_count`` and ``annotations_url`` but
    not the annotation messages themselves; ``_is_infra_failure``
    matches keywords against ``annotation['message']``, so without this
    hydration the keyword path never fires on real REST payloads.

    Hydration is gated to keep the extra REST calls bounded:

    - skip when ``annotations`` is already present (test fixtures
      pre-populate the field; double-fetching would mask their intent),
    - skip when the run already concludes with an infra-class state
      (``cancelled`` / ``action_required`` / ``stale`` — those classify
      as infra without consulting the message text),
    - skip when the run is not in a failure-like state (annotations on
      passing runs are not consulted by the classifier),
    - skip when ``annotations_count`` is missing or zero.

    Only the first ``_ANNOTATION_HYDRATION_PER_PAGE`` annotations are
    fetched (a single non-paginated REST call) instead of walking every
    page. ``_is_infra_failure`` only needs to detect that *any*
    annotation matches an infra keyword; for a large lint/test run
    with hundreds of annotations, paginating every cycle would
    multiply REST traffic by the number of failing runs and exhaust
    the budget for unrelated WATCH cycles.

    Failures of the annotation fetch are swallowed: leaving the field
    empty causes ``_is_infra_failure`` to return ``False``, which
    surfaces the run as logic FAILURE — the safe default.
    """
    if "annotations" in check_run:
        return
    conclusion = (check_run.get("conclusion") or "").upper()
    if conclusion in _INFRA_CONCLUSION_STATES:
        return
    if conclusion not in _REST_CI_FAILURE_STATES:
        return
    count = check_run.get("annotations_count")
    if not isinstance(count, int) or count <= 0:
        return
    check_run_id = check_run.get("id")
    if not isinstance(check_run_id, int):
        return
    path = (
        f"repos/{repo}/check-runs/{check_run_id}/annotations"
        f"?per_page={_ANNOTATION_HYDRATION_PER_PAGE}"
    )
    try:
        raw = retry_transient(
            lambda: gh_runner.run_gh(["api", path]),
            operation_name=f"gh api {path}",
        )
    except RuntimeError:
        return
    if isinstance(raw, list):
        check_run["annotations"] = [a for a in raw if isinstance(a, dict)]


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
        if (now - entry.fetched_at) >= _CI_STATUS_CACHE_TTL_SECONDS
    ]
    for key in expired:
        _ci_status_cache.pop(key, None)


def _source_error(exc: RuntimeError) -> str:
    msg = str(exc)
    lower = msg.lower()
    if "timeout" in lower or "timed out" in lower:
        return "timeout"
    if "403" in msg or "forbidden" in lower:
        return "forbidden"
    return msg


def _commit_status_state(value: object) -> str | None:
    if not isinstance(value, str) or not value:
        return None
    upper = value.upper()
    return upper if upper in _REST_COMMIT_STATUS_STATES else None


def _check_run_state(run: dict) -> str | None:
    conclusion_present = "conclusion" in run
    conclusion = run.get("conclusion")
    conclusion_state: str | None = None
    if conclusion_present and conclusion is not None:
        if not isinstance(conclusion, str) or not conclusion:
            return None
        conclusion_state = conclusion.upper()
        if conclusion_state not in _REST_CHECK_RUN_CONCLUSIONS:
            return None

    status_present = "status" in run
    status = run.get("status")
    status_state: str | None = None
    if status_present:
        if not isinstance(status, str) or not status:
            return None
        status_state = status.upper()
        if status_state not in _REST_CHECK_RUN_STATUSES:
            return None

    if status_state is None:
        return conclusion_state
    if status_state == "COMPLETED":
        if not conclusion_present or conclusion_state is None:
            return None
        return conclusion_state
    if conclusion_state is not None:
        return None
    return status_state


def _parse_status_payload(parsed: dict) -> tuple[dict, _CiSourceResult]:
    combined_state = parsed.get("state")
    combined_state_upper = _commit_status_state(combined_state)
    if combined_state_upper is None:
        return {}, _CiSourceResult(False, False, "malformed")
    statuses_raw = parsed.get("statuses", [])
    if not statuses_raw and combined_state_upper in _REST_CI_FAILURE_STATES:
        return parsed, _CiSourceResult(False, True, "malformed")
    statuses_malformed = False
    for status in statuses_raw:
        if not isinstance(status, dict):
            statuses_malformed = True
            continue
        if _commit_status_state(status.get("state")) is None:
            statuses_malformed = True
    if statuses_malformed:
        if combined_state_upper in _REST_CI_FAILURE_STATES:
            return parsed, _CiSourceResult(False, False, "malformed")
        return {}, _CiSourceResult(False, False, "malformed")
    return parsed, _CiSourceResult(True, len(statuses_raw) == 0)


def _parse_status_pages(
    pages: cache.PaginatedEvidence,
    requested_sha: str,
) -> tuple[dict, _CiSourceResult]:
    if not pages.items:
        error = _source_error(RuntimeError(pages.error)) if pages.error else "malformed"
        return {}, _CiSourceResult(False, pages.empty, error)

    status_payload = dict(pages.items[0])
    statuses: list[object] = []
    page_states: list[str] = []
    sha_mismatch = False
    for page in pages.items:
        page_statuses = page.get("statuses")
        if isinstance(page_statuses, list):
            statuses.extend(page_statuses)
        state = _commit_status_state(page.get("state"))
        if state is not None:
            page_states.append(state)
        page_sha = page.get("sha")
        if not isinstance(page_sha, str) or page_sha.lower() != requested_sha.lower():
            sha_mismatch = True
    if any(state in _REST_CI_FAILURE_STATES for state in page_states):
        status_payload["state"] = "failure"
    status_payload["statuses"] = statuses

    status_payload, parsed_source = _parse_status_payload(status_payload)
    inconsistent_states = len(page_states) != len(pages.items) or len(set(page_states)) != 1
    malformed = (
        inconsistent_states
        or sha_mismatch
        or parsed_source.error == "malformed"
        or pages.error == "malformed"
    )
    complete = pages.complete and parsed_source.complete and not malformed
    error = (
        "malformed"
        if malformed
        else _source_error(RuntimeError(pages.error))
        if pages.error
        else None
    )
    return status_payload, _CiSourceResult(complete, parsed_source.empty, error)


def _make_ci_retrieval(
    repo: str,
    sha: str,
    fetched_at: float,
    check_runs: list[dict],
    status_payload: dict,
    check_runs_source: _CiSourceResult,
    status_source: _CiSourceResult,
    observed_at: datetime | None = None,
) -> _CiRetrieval:
    statuses = status_payload.get("statuses", [])
    status_records = (
        [status for status in statuses if isinstance(status, dict)]
        if isinstance(statuses, list)
        else []
    )
    evidence = evaluate_ci_evidence(
        repo=repo,
        pr_number=None,
        sha=sha,
        check_runs=check_runs,
        statuses=status_records,
        check_runs_complete=check_runs_source.complete,
        statuses_complete=status_source.complete,
        observed_at=observed_at,
    )
    return _CiRetrieval(
        evidence,
        fetched_at,
        list(check_runs),
        dict(status_payload),
        check_runs_source,
        status_source,
    )


def _retrieve_ci_status_evidence(repo: str, sha: str) -> _CiRetrieval:
    now = time.monotonic()
    if not sha:
        empty = _CiSourceResult(True, True)
        return _make_ci_retrieval(repo, sha, now, [], {}, empty, empty)

    cache_key = (repo, sha)
    cached = _ci_status_cache.get(cache_key)
    if (
        cached is not None
        and cached.evidence.repo == repo
        and cached.evidence.sha == sha.lower()
        and (now - cached.fetched_at) < _CI_STATUS_CACHE_TTL_SECONDS
    ):
        return cached

    check_runs: list[dict] = []
    status_payload: dict = {}

    # check-runs is a paginated endpoint (per_page max 100). A commit can
    # carry more than 100 runs, and ``_map_rest_ci_status_to_enum`` reads
    # every entry — truncating to page 1 would let a failing or pending
    # run beyond the cap masquerade as SUCCESS and misclassify the PR as
    # mergeable, so we walk every page rather than relying on ETag-cached
    # single-page reads.
    check_runs_path = f"repos/{repo}/commits/{sha}/check-runs?per_page=100"
    cr_evidence = cache._gh_api_paginated_evidence(check_runs_path)
    check_runs_malformed = False
    for page in cr_evidence.items:
        runs = page.get("check_runs")
        if not isinstance(runs, list):
            check_runs_malformed = True
            continue
        for run in runs:
            run_sha = run.get("head_sha") if isinstance(run, dict) else None
            if (
                isinstance(run, dict)
                and isinstance(run_sha, str)
                and run_sha.lower() == sha.lower()
                and _check_run_state(run)
            ):
                check_runs.append(run)
            else:
                check_runs_malformed = True
    check_runs_source = _CiSourceResult(
        cr_evidence.complete and not check_runs_malformed,
        cr_evidence.empty or (cr_evidence.complete and not check_runs),
        "malformed"
        if check_runs_malformed
        else _source_error(RuntimeError(cr_evidence.error))
        if cr_evidence.error
        else None,
    )

    # PR-251 (OBS-BC): GitHub's check-runs REST payload exposes only
    # ``annotations_count`` + ``annotations_url`` — not the annotation
    # ``message`` strings the infra classifier inspects. Hydrate the
    # ``annotations`` field on each failing check-run that has at least
    # one annotation and a non-infra conclusion (infra-class conclusions
    # like ``cancelled`` already classify without needing the message
    # text). Without this step ``_is_infra_failure`` would never see
    # annotation messages on real REST payloads and would mis-route
    # failures whose only infra signal is an annotation keyword.
    for run in check_runs:
        _maybe_hydrate_annotations(repo, run)

    status_path = f"repos/{repo}/commits/{sha}/status?per_page=100"
    status_pages = cache._etag_get_object_pages_evidence(status_path, "statuses")
    status_payload, status_source = _parse_status_pages(status_pages, sha)

    retrieval = _make_ci_retrieval(
        repo,
        sha,
        now,
        list(check_runs),
        dict(status_payload),
        check_runs_source,
        status_source,
        status_pages.observed_at,
    )
    _evict_expired_ci_status_cache(now)
    _ci_status_cache[cache_key] = retrieval
    return retrieval


def _fetch_ci_status_rest(repo: str, sha: str) -> tuple[list[dict], dict, bool]:
    retrieval = _retrieve_ci_status_evidence(repo, sha)
    return (
        list(retrieval.check_runs),
        dict(retrieval.status_payload),
        retrieval.evidence.sources_complete,
    )


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

    ``fetch_ok=False`` always blocks SUCCESS, including when both payloads
    are empty and ``empty_is_success`` is enabled. Missing evidence must
    not be treated as evidence that the repository has no checks.

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
    statuses_raw = (
        status_payload.get("statuses") if isinstance(status_payload, dict) else None
    )
    statuses = statuses_raw if isinstance(statuses_raw, list) else []
    combined_state = (
        status_payload.get("state") if isinstance(status_payload, dict) else None
    )
    malformed_statuses = any(
        not isinstance(status, dict)
        or _commit_status_state(status.get("state")) is None
        for status in statuses
    )

    combined_state_upper = _commit_status_state(combined_state) or ""
    if not check_runs and not statuses:
        if combined_state_upper in _REST_CI_FAILURE_STATES:
            return CIStatus.FAILURE
        return CIStatus.SUCCESS if empty_is_success and fetch_ok else CIStatus.PENDING

    states: list[str] = []
    failing_runs: list[dict] = []
    malformed_check_runs = False
    for run in check_runs:
        if not isinstance(run, dict):
            malformed_check_runs = True
            continue
        upper = _check_run_state(run)
        if not upper:
            malformed_check_runs = True
            continue
        states.append(upper)
        if upper in _REST_CI_FAILURE_STATES:
            failing_runs.append(run)

    if combined_state_upper in _REST_CI_FAILURE_STATES:
        states.append(combined_state_upper)
    elif statuses and malformed_statuses:
        fetch_ok = False
    elif statuses and combined_state_upper:
        states.append(combined_state_upper)
    if malformed_check_runs:
        fetch_ok = False

    if not states:
        return CIStatus.PENDING
    if any(s in _REST_CI_FAILURE_STATES for s in states):
        # PR-251 (OBS-BC): when every failing check-run is infra-class,
        # surface ``INFRA_FAILURE`` so WATCH can rerun the workflow
        # once before consuming a coder FIX iteration. A combined
        # commit-status failure (legacy GitHub status API) cannot
        # carry annotations, so it dominates as logic FAILURE — better
        # to under-classify infra than to skip a real bug.
        combined_status_failed = combined_state_upper in _REST_CI_FAILURE_STATES
        if (
            failing_runs
            and not combined_status_failed
            and all(_is_infra_failure(run) for run in failing_runs)
        ):
            return CIStatus.INFRA_FAILURE
        return CIStatus.FAILURE
    if all(s in _REST_CI_SUCCESS_STATES for s in states) and fetch_ok:
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

    Errors from ``time()`` itself (ACL denial, transient command failure,
    connection drop) are also treated as "unsupported" so the caller's
    local-time fallback in :func:`_resolve_now_seconds` engages instead
    of the exception propagating up through WATCH and aborting the cycle.
    """
    time_fn = getattr(redis_client, "time", None)
    if time_fn is None:
        return None
    try:
        result = time_fn()
        if hasattr(result, "__await__"):
            result = await result
    except Exception:
        return None
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
    if not fetch_ok:
        # Both REST calls failed: the PENDING above is the
        # ``empty_is_success=False`` default, not an observation that CI
        # is actually pending. Counting outage/rate-limit windows toward
        # ``stuck_pending`` would route WATCH into ``handle_fix`` on a
        # PR whose CI state we genuinely cannot read. Drop any prior
        # anchor so the window restarts fresh once visibility resumes.
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
