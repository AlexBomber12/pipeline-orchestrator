from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Iterable

from src.models import CIStatus

SUCCESS, PENDING, FAILURE, UNKNOWN = "success", "pending", "failure", "unknown"
_SUCCESS, _PENDING = {"SUCCESS", "COMPLETED", "NEUTRAL", "SKIPPED"}, {
    "PENDING", "QUEUED", "IN_PROGRESS", "REQUESTED", "WAITING", "EXPECTED"
}
_FAILURE = {"FAILURE", "FAILED", "ERROR", "CANCELLED", "TIMED_OUT", "ACTION_REQUIRED", "STALE"}


@dataclass(frozen=True)
class CIContextEvidence:
    name: str
    state: str
    sha: str
    producer: str | None = None
    attempt: int | None = None
    observed_at: datetime | None = None


@dataclass(frozen=True)
class CIEvidence:
    sha: str
    observed_at: datetime
    contexts: tuple[CIContextEvidence, ...]
    sources_complete: bool
    policy_result: CIStatus
    pending_reason: str | None = None
    repo: str | None = None
    pr_number: int | None = None


def evaluate_ci_evidence(
    *, repo: str | None, pr_number: int | None, sha: str,
    check_runs: Iterable[dict[str, Any]] = (), statuses: Iterable[dict[str, Any]] = (),
    check_runs_complete: bool = True, statuses_complete: bool = True,
    required_contexts: Iterable[str] | None = None, observed_at: datetime | None = None,
    empty_is_success: bool = False,
) -> CIEvidence:
    observed = _dt(observed_at) or datetime.now(timezone.utc)
    required = None if required_contexts is None else tuple(n for raw in required_contexts if (n := _text(raw)))
    contexts = tuple(_contexts(sha, check_runs, statuses, observed))
    complete = check_runs_complete and statuses_complete
    result, reason = _policy(contexts, complete, required, empty_is_success)
    return CIEvidence(
        repo=repo, pr_number=pr_number, sha=sha, observed_at=observed, contexts=contexts,
        sources_complete=complete, policy_result=result, pending_reason=reason
    )


def _contexts(sha: str, check_runs: Iterable[dict[str, Any]], statuses: Iterable[dict[str, Any]],
              observed_at: datetime) -> list[CIContextEvidence]:
    out: list[CIContextEvidence] = []
    for run in check_runs:
        name = _text(run.get("name"))
        run_sha = _text(run.get("head_sha") or run.get("sha") or run.get("commit_sha"))
        if name and run_sha == sha:
            out.append(CIContextEvidence(name, _state(run.get("conclusion") or run.get("status")), run_sha,
                                         _run_producer(run), _attempt(run), _time(run) or observed_at))
    for status in statuses:
        name = _text(status.get("context") or status.get("name"))
        status_sha = _text(status.get("sha") or status.get("commit_sha"))
        if name and status_sha == sha:
            out.append(CIContextEvidence(name, _state(status.get("state") or status.get("status")), status_sha,
                                         _status_producer(status), _attempt(status),
                                         _time(status) or observed_at))
    return out


def _policy(contexts: tuple[CIContextEvidence, ...], complete: bool, required: tuple[str, ...] | None,
            empty_is_success: bool) -> tuple[CIStatus, str | None]:
    latest = _latest(contexts)
    if any(ctx.state == FAILURE for producers in latest.values() for ctx in producers.values()):
        return CIStatus.FAILURE, None
    if not complete:
        return CIStatus.PENDING, "sources_incomplete"
    if required is None:
        if not latest:
            return (CIStatus.SUCCESS, None) if empty_is_success else (CIStatus.PENDING, "no_contexts")
        if any(ctx.state != SUCCESS for producers in latest.values() for ctx in producers.values()):
            return CIStatus.PENDING, "context_pending"
        return CIStatus.SUCCESS, None
    for name in required:
        producers = latest.get(name)
        if not producers:
            return CIStatus.PENDING, f"missing_required:{name}"
        if len(producers) != 1:
            return CIStatus.PENDING, f"ambiguous_required:{name}"
        [ctx] = producers.values()
        if ctx.producer is None:
            return CIStatus.PENDING, f"missing_identity:{name}"
        if ctx.state != SUCCESS:
            return CIStatus.PENDING, f"required_not_success:{name}"
    return CIStatus.SUCCESS, None


def _latest(contexts: Iterable[CIContextEvidence]) -> dict[str, dict[str | None, CIContextEvidence]]:
    latest: dict[str, dict[str | None, CIContextEvidence]] = {}
    for ctx in contexts:
        previous = latest.setdefault(ctx.name, {}).get(ctx.producer)
        if previous is None or _newer(ctx, previous):
            latest[ctx.name][ctx.producer] = ctx
    return latest


def _newer(left: CIContextEvidence, right: CIContextEvidence) -> bool:
    if left.attempt is not None and right.attempt is not None:
        return left.attempt >= right.attempt
    if left.attempt is not None:
        return True
    if right.attempt is not None:
        return False
    if left.observed_at is not None and right.observed_at is not None:
        return left.observed_at >= right.observed_at
    return True


def _state(value: object) -> str:
    upper = str(value or "").upper()
    if upper in _SUCCESS:
        return SUCCESS
    if upper in _FAILURE:
        return FAILURE
    if upper in _PENDING:
        return PENDING
    return UNKNOWN


def _run_producer(run: dict[str, Any]) -> str | None:
    app = run.get("app")
    if isinstance(app, dict):
        if app.get("id") is not None:
            return f"app:{app['id']}"
        if app.get("slug"):
            return f"app:{app['slug']}"
    return _status_producer(run)


def _status_producer(status: dict[str, Any]) -> str | None:
    for key in ("app_id", "node_id", "target_url"):
        if status.get(key):
            return f"{key}:{status[key]}"
    creator = status.get("creator")
    return f"user:{creator['login']}" if isinstance(creator, dict) and creator.get("login") else None


def _attempt(payload: dict[str, Any]) -> int | None:
    for key in ("run_attempt", "attempt"):
        value = payload.get(key)
        if isinstance(value, int):
            return value
        if isinstance(value, str) and value.isdigit():
            return int(value)
    return None


def _time(payload: dict[str, Any]) -> datetime | None:
    for key in ("completed_at", "updated_at", "started_at", "created_at"):
        if parsed := _dt(payload.get(key)):
            return parsed
    return None


def _dt(value: object) -> datetime | None:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if isinstance(value, str) and value:
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
    return None


def _text(value: object) -> str:
    return str(value or "").strip()
