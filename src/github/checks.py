"""GitHub commit/check-run status helpers.

Owns the REST ``check-runs`` + commit ``status`` fetch path that powers the
WATCH gate's CI status read. Reuses ``cache._etag_get`` and
``cache._gh_api_paginated`` for ETag-conditional REST reads.
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from src.github import cache, gh_runner
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
_ci_status_cache: dict[tuple[str, str], tuple[float, "CIEvidence"]] = {}


_REST_CI_FAILURE_STATES = {
    "FAILURE",
    "FAILED",
    "ERROR",
    "CANCELLED",
    "TIMED_OUT",
    "ACTION_REQUIRED",
    # PR-251: ``stale`` is a GitHub Actions check-run conclusion meaning
    # the run is no longer relevant (workflow re-dispatched after a
    # newer push, or GitHub itself dropped the result). It must count
    # toward the failure rollup so ``_is_infra_failure`` can route it
    # through the INFRA_FAILURE retry path; otherwise a stale-only set
    # of check-runs would be silently classified ``PENDING``.
    "STALE",
}
_REST_CI_SUCCESS_STATES = {"SUCCESS", "COMPLETED", "NEUTRAL", "SKIPPED"}

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


@dataclass(frozen=True)
class CISourceCompleteness:
    """Retrieval state for one authoritative CI source."""

    ok: bool
    complete: bool
    reason: str | None = None


@dataclass(frozen=True)
class CIContext:
    """Latest observed state for a normalized CI context identity."""

    display_name: str
    identity: str
    source: str
    state: str
    success: bool
    failure: bool
    pending: bool
    app: str | None = None
    attempt_id: str | None = None


@dataclass(frozen=True)
class CIEvidence:
    """Complete CI evidence for a single commit SHA."""

    repo: str
    head_sha: str
    pr_number: int | None
    fetched_at: datetime
    check_runs: list[dict] = field(default_factory=list)
    status_payload: dict = field(default_factory=dict)
    check_runs_source: CISourceCompleteness = field(
        default_factory=lambda: CISourceCompleteness(False, False, "not_fetched")
    )
    statuses_source: CISourceCompleteness = field(
        default_factory=lambda: CISourceCompleteness(False, False, "not_fetched")
    )
    contexts: tuple[CIContext, ...] = ()
    ci_status: CIStatus = CIStatus.PENDING
    pending_reason: str | None = None
    required_checks: tuple[str, ...] = ()

    @property
    def fetch_ok(self) -> bool:
        """Legacy adapter: at least one source was fetched successfully."""
        return self.check_runs_source.ok or self.statuses_source.ok

    @property
    def complete(self) -> bool:
        """Both authoritative sources were retrieved completely for this SHA."""
        return self.check_runs_source.complete and self.statuses_source.complete


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
        if (now - entry[0]) >= _CI_STATUS_CACHE_TTL_SECONDS
    ]
    for key in expired:
        _ci_status_cache.pop(key, None)


def _source_ok() -> CISourceCompleteness:
    return CISourceCompleteness(ok=True, complete=True)


def _source_failed(reason: str) -> CISourceCompleteness:
    return CISourceCompleteness(ok=False, complete=False, reason=reason)


def _fetch_ci_evidence_rest(
    repo: str,
    sha: str,
    *,
    pr_number: int | None = None,
    required_checks: list[str] | tuple[str, ...] | None = None,
    allow_merge_without_checks: bool = False,
) -> CIEvidence:
    """Fetch and evaluate complete REST CI evidence for ``sha``.

    Both check-runs and combined commit statuses must be fetched
    successfully before evidence can authorize a merge. Partial data can
    still surface observed failures, but never success.
    """
    required = tuple(required_checks or ())
    fetched_at = datetime.now(timezone.utc)
    if not sha:
        return _build_ci_evidence(
            repo=repo,
            sha=sha,
            pr_number=pr_number,
            fetched_at=fetched_at,
            check_runs=[],
            status_payload={},
            check_runs_source=_source_failed("missing_head_sha"),
            statuses_source=_source_failed("missing_head_sha"),
            required_checks=required,
            allow_merge_without_checks=allow_merge_without_checks,
        )

    cache_key = (repo, sha)
    cached = _ci_status_cache.get(cache_key)
    now = time.monotonic()
    if cached is not None and (now - cached[0]) < _CI_STATUS_CACHE_TTL_SECONDS:
        cached_evidence = cached[1]
        return _build_ci_evidence(
            repo=cached_evidence.repo,
            sha=cached_evidence.head_sha,
            pr_number=pr_number if pr_number is not None else cached_evidence.pr_number,
            fetched_at=cached_evidence.fetched_at,
            check_runs=list(cached_evidence.check_runs),
            status_payload=dict(cached_evidence.status_payload),
            check_runs_source=cached_evidence.check_runs_source,
            statuses_source=cached_evidence.statuses_source,
            required_checks=required,
            allow_merge_without_checks=allow_merge_without_checks,
        )

    check_runs: list[dict] = []
    status_payload: dict = {}

    # check-runs is a paginated endpoint (per_page max 100). A commit can
    # carry more than 100 runs, and ``_map_rest_ci_status_to_enum`` reads
    # every entry — truncating to page 1 would let a failing or pending
    # run beyond the cap masquerade as SUCCESS and misclassify the PR as
    # mergeable, so we walk every page rather than relying on ETag-cached
    # single-page reads.
    check_runs_path = f"repos/{repo}/commits/{sha}/check-runs?per_page=100"
    check_runs_source = _source_failed("check_runs_fetch_failed")
    try:
        cr_pages = cache._gh_api_paginated(check_runs_path)
    except RuntimeError:
        cr_pages = None
    if isinstance(cr_pages, list):
        check_runs_source = _source_ok()
        for page in cr_pages:
            if not isinstance(page, dict):
                check_runs_source = _source_failed("check_runs_unexpected_payload")
                continue
            runs = page.get("check_runs")
            if isinstance(runs, list):
                check_runs.extend(r for r in runs if isinstance(r, dict))
    elif cr_pages is not None:
        check_runs_source = _source_failed("check_runs_unexpected_payload")

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

    status_path = f"repos/{repo}/commits/{sha}/status"
    statuses_source = _source_failed("statuses_fetch_failed")
    try:
        raw_status = retry_transient(
            lambda: cache._etag_get(status_path),
            operation_name=f"gh api {status_path}",
        )
    except RuntimeError:
        raw_status = None
    if isinstance(raw_status, dict):
        status_payload = raw_status
        statuses_source = _source_ok()
    elif isinstance(raw_status, str) and raw_status:
        try:
            parsed = json.loads(raw_status)
        except json.JSONDecodeError:
            parsed = None
        if isinstance(parsed, dict):
            status_payload = parsed
            statuses_source = _source_ok()
        else:
            statuses_source = _source_failed("statuses_unexpected_payload")
    elif raw_status is not None:
        statuses_source = _source_failed("statuses_unexpected_payload")

    evidence = _build_ci_evidence(
        repo=repo,
        sha=sha,
        pr_number=pr_number,
        fetched_at=fetched_at,
        check_runs=check_runs,
        status_payload=status_payload,
        check_runs_source=check_runs_source,
        statuses_source=statuses_source,
        required_checks=required,
        allow_merge_without_checks=allow_merge_without_checks,
    )
    _evict_expired_ci_status_cache(now)
    _ci_status_cache[cache_key] = (now, evidence)
    return evidence


def _fetch_ci_status_rest(repo: str, sha: str) -> tuple[list[dict], dict, bool]:
    """Compatibility wrapper returning legacy REST CI payloads."""
    evidence = _fetch_ci_evidence_rest(repo, sha)
    return list(evidence.check_runs), dict(evidence.status_payload), evidence.fetch_ok


def _check_run_app_identity(run: dict) -> str:
    app = run.get("app")
    if not isinstance(app, dict):
        return "app:unknown"
    for key in ("slug", "id", "name"):
        value = app.get(key)
        if value not in (None, ""):
            return f"app:{key}:{value}"
    return "app:unknown"


def _attempt_sort_key(item: dict) -> tuple[str, int]:
    timestamp = ""
    for key in ("started_at", "completed_at", "updated_at", "created_at"):
        value = item.get(key)
        if isinstance(value, str) and value:
            timestamp = value
            break
    item_id = item.get("id")
    return timestamp, item_id if isinstance(item_id, int) else 0


def _collapse_check_run_contexts(check_runs: list[dict]) -> list[CIContext]:
    latest: dict[str, dict] = {}
    for run in check_runs:
        if not isinstance(run, dict):
            continue
        name = run.get("name")
        if not isinstance(name, str) or not name.strip():
            continue
        app_identity = _check_run_app_identity(run)
        identity = f"check-run:{name.strip()}:{app_identity}"
        current = latest.get(identity)
        if current is None or _attempt_sort_key(run) >= _attempt_sort_key(current):
            latest[identity] = run

    contexts: list[CIContext] = []
    for identity, run in latest.items():
        value = run.get("conclusion") or run.get("status")
        if not value:
            continue
        state = str(value).upper()
        contexts.append(
            CIContext(
                display_name=str(run.get("name", "")).strip(),
                identity=identity,
                source="check_runs",
                state=state,
                success=state in _REST_CI_SUCCESS_STATES,
                failure=state in _REST_CI_FAILURE_STATES,
                pending=state not in _REST_CI_SUCCESS_STATES
                and state not in _REST_CI_FAILURE_STATES,
                app=app_identity,
                attempt_id=str(run.get("id")) if run.get("id") is not None else None,
            )
        )
    return contexts


def _collapse_status_contexts(status_payload: dict) -> list[CIContext]:
    statuses_raw = (
        status_payload.get("statuses") if isinstance(status_payload, dict) else None
    )
    if not isinstance(statuses_raw, list):
        return []
    latest: dict[str, dict] = {}
    for status in statuses_raw:
        if not isinstance(status, dict):
            continue
        context = status.get("context")
        if not isinstance(context, str) or not context.strip():
            continue
        identity = f"status:{context.strip()}"
        # The combined-status endpoint returns newest statuses first.
        latest.setdefault(identity, status)

    contexts: list[CIContext] = []
    for identity, status in latest.items():
        value = status.get("state")
        if not isinstance(value, str) or not value:
            continue
        state = value.upper()
        contexts.append(
            CIContext(
                display_name=str(status.get("context", "")).strip(),
                identity=identity,
                source="statuses",
                state=state,
                success=state in _REST_CI_SUCCESS_STATES,
                failure=state in _REST_CI_FAILURE_STATES,
                pending=state not in _REST_CI_SUCCESS_STATES
                and state not in _REST_CI_FAILURE_STATES,
            )
        )
    return contexts


def _build_ci_evidence(
    *,
    repo: str,
    sha: str,
    pr_number: int | None,
    fetched_at: datetime,
    check_runs: list[dict],
    status_payload: dict,
    check_runs_source: CISourceCompleteness,
    statuses_source: CISourceCompleteness,
    required_checks: tuple[str, ...],
    allow_merge_without_checks: bool,
) -> CIEvidence:
    contexts = tuple(
        _collapse_check_run_contexts(check_runs) + _collapse_status_contexts(status_payload)
    )
    ci_status, pending_reason = _evaluate_ci_evidence(
        check_runs=check_runs,
        status_payload=status_payload,
        contexts=contexts,
        check_runs_source=check_runs_source,
        statuses_source=statuses_source,
        required_checks=required_checks,
        allow_merge_without_checks=allow_merge_without_checks,
    )
    return CIEvidence(
        repo=repo,
        head_sha=sha,
        pr_number=pr_number,
        fetched_at=fetched_at,
        check_runs=list(check_runs),
        status_payload=dict(status_payload),
        check_runs_source=check_runs_source,
        statuses_source=statuses_source,
        contexts=contexts,
        ci_status=ci_status,
        pending_reason=pending_reason,
        required_checks=required_checks,
    )


def _evaluate_ci_evidence(
    *,
    check_runs: list[dict],
    status_payload: dict,
    contexts: tuple[CIContext, ...],
    check_runs_source: CISourceCompleteness,
    statuses_source: CISourceCompleteness,
    required_checks: tuple[str, ...],
    allow_merge_without_checks: bool,
) -> tuple[CIStatus, str | None]:
    complete = check_runs_source.complete and statuses_source.complete
    visible_failure = _map_observed_contexts_to_enum(check_runs, status_payload, contexts)
    if visible_failure in {CIStatus.FAILURE, CIStatus.INFRA_FAILURE}:
        return visible_failure, None

    if not complete:
        reasons = [
            source.reason
            for source in (check_runs_source, statuses_source)
            if not source.complete and source.reason
        ]
        return CIStatus.PENDING, "+".join(reasons) or "ci_evidence_incomplete"

    if required_checks:
        for required in required_checks:
            matches = [ctx for ctx in contexts if ctx.display_name == required]
            if not matches:
                return CIStatus.PENDING, f"required_check_missing:{required}"
            if any(not ctx.success for ctx in matches):
                return CIStatus.PENDING, f"required_check_pending:{required}"
        return CIStatus.SUCCESS, None

    if not contexts:
        if allow_merge_without_checks:
            return CIStatus.SUCCESS, None
        return CIStatus.PENDING, "no_ci_contexts"
    if all(ctx.success for ctx in contexts):
        return CIStatus.SUCCESS, None
    return CIStatus.PENDING, "ci_pending"


def _map_observed_contexts_to_enum(
    check_runs: list[dict],
    status_payload: dict,
    contexts: tuple[CIContext, ...],
) -> CIStatus:
    if any(ctx.failure and ctx.source == "statuses" for ctx in contexts):
        return CIStatus.FAILURE
    latest_runs: dict[str, dict] = {}
    for run in check_runs:
        if not isinstance(run, dict):
            continue
        name = run.get("name")
        if not isinstance(name, str) or not name.strip():
            continue
        identity = f"check-run:{name.strip()}:{_check_run_app_identity(run)}"
        current = latest_runs.get(identity)
        if current is None or _attempt_sort_key(run) >= _attempt_sort_key(current):
            latest_runs[identity] = run
    failing_runs = []
    for ctx in contexts:
        if not ctx.failure or ctx.source != "check_runs":
            continue
        run = latest_runs.get(ctx.identity)
        if run is not None:
            failing_runs.append(run)
    if failing_runs:
        if all(_is_infra_failure(run) for run in failing_runs):
            return CIStatus.INFRA_FAILURE
        return CIStatus.FAILURE

    combined_state = (
        status_payload.get("state") if isinstance(status_payload, dict) else None
    )
    combined_state_upper = (
        combined_state.upper() if isinstance(combined_state, str) and combined_state else ""
    )
    if combined_state_upper in _REST_CI_FAILURE_STATES:
        return CIStatus.FAILURE
    return CIStatus.PENDING


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
    statuses_raw = (
        status_payload.get("statuses") if isinstance(status_payload, dict) else None
    )
    statuses = statuses_raw if isinstance(statuses_raw, list) else []
    combined_state = (
        status_payload.get("state") if isinstance(status_payload, dict) else None
    )

    if not fetch_ok and not check_runs and not statuses:
        return CIStatus.PENDING

    if not check_runs and not statuses:
        return CIStatus.SUCCESS if empty_is_success else CIStatus.PENDING

    states: list[str] = []
    failing_runs: list[dict] = []
    for run in check_runs:
        if not isinstance(run, dict):
            continue
        value = run.get("conclusion") or run.get("status")
        if not value:
            continue
        upper = str(value).upper()
        states.append(upper)
        if upper in _REST_CI_FAILURE_STATES:
            failing_runs.append(run)

    combined_state_upper = (
        combined_state.upper() if isinstance(combined_state, str) and combined_state else ""
    )
    if statuses and combined_state_upper:
        states.append(combined_state_upper)

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
