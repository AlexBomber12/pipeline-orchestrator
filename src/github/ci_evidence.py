from collections import namedtuple
from datetime import datetime, timezone
from typing import Any, Iterable

from src.models import CIStatus

_SUCCESS = {"SUCCESS", "NEUTRAL", "SKIPPED"}
_PENDING = {"PENDING", "QUEUED", "IN_PROGRESS", "REQUESTED", "WAITING", "EXPECTED"}
_FAILURE = {"FAILURE", "FAILED", "ERROR", "CANCELLED", "TIMED_OUT", "ACTION_REQUIRED", "STALE"}

CIContextEvidence = namedtuple(
    "CIContextEvidence", "name state sha producer attempt observed_at run_id", defaults=[None, None, None, None]
)
CIEvidence = namedtuple(
    "CIEvidence", "sha observed_at contexts sources_complete policy_result pending_reason repo pr_number",
    defaults=[None, None, None],
)


def evaluate_ci_evidence(
    *, repo: str | None, pr_number: int | None, sha: str,
    check_runs: Iterable[dict[str, Any]] = (), statuses: Iterable[dict[str, Any]] = (),
    check_runs_complete: bool = True, statuses_complete: bool = True,
    required_contexts: Iterable[str] | None = None, observed_at: datetime | None = None,
    empty_is_success: bool = False,
) -> CIEvidence:
    sha = sha.lower()
    observed = _dt(observed_at) or datetime.now(timezone.utc)
    required = None if required_contexts is None else tuple(n for raw in required_contexts if (n := _text(raw)))
    check_runs = tuple(check_runs)
    statuses = tuple(statuses)
    contexts = tuple(_contexts(sha, check_runs, statuses, observed))
    complete = check_runs_complete and statuses_complete
    full_sha = len(sha) == 40 and all(char in "0123456789abcdefABCDEF" for char in sha)
    conflict = not full_sha or len(contexts) != len(check_runs) + len(statuses)
    result, reason = _policy(contexts, complete, required, empty_is_success, conflict)
    return CIEvidence(
        repo=repo, pr_number=pr_number, sha=sha, observed_at=observed, contexts=contexts,
        sources_complete=complete, policy_result=result, pending_reason=reason
    )


def _contexts(sha: str, check_runs: Iterable[dict[str, Any]], statuses: Iterable[dict[str, Any]],
              observed_at: datetime) -> list[CIContextEvidence]:
    out: list[CIContextEvidence] = []
    for run in check_runs:
        name = _text(run.get("name"))
        run_sha = _text(run.get("head_sha") or run.get("sha") or run.get("commit_sha")).lower()
        if name and run_sha == sha:
            out.append(CIContextEvidence(name, _state(run.get("conclusion") or run.get("status")), run_sha,
                                         _run_producer(run), _attempt(run), _time(run) or observed_at, run.get("id")))
    for status in statuses:
        name = _text(status.get("context") or status.get("name"))
        status_sha = _text(status.get("sha", status.get("commit_sha", sha))).lower()
        if name and status_sha == sha:
            out.append(CIContextEvidence(name, _state(status.get("state") or status.get("status")), status_sha,
                                         _status_producer(status), _attempt(status),
                                         _time(status) or observed_at, status.get("id")))
    return out


def _policy(contexts: tuple[CIContextEvidence, ...], complete: bool, required: tuple[str, ...] | None,
            empty_is_success: bool, conflict: bool) -> tuple[CIStatus, str | None]:
    latest = _latest(contexts)
    if any(ctx.state == "failure" for producers in latest.values() for ctx in producers.values()):
        return CIStatus.FAILURE, None
    if conflict:
        return CIStatus.PENDING, "conflicting_sha"
    if not complete:
        return CIStatus.PENDING, "sources_incomplete"
    if not required:
        if not latest:
            return (CIStatus.SUCCESS, None) if empty_is_success else (CIStatus.PENDING, "no_contexts")
        for name, producers in latest.items():
            if None in producers:
                return CIStatus.PENDING, f"missing_identity:{name}"
            if len(producers) != 1:
                return CIStatus.PENDING, f"ambiguous_context:{name}"
        if any(ctx.state != "success" for producers in latest.values() for ctx in producers.values()):
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
        if ctx.state != "success":
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
    if left.observed_at is not None and right.observed_at is not None and left.observed_at != right.observed_at:
        return left.observed_at > right.observed_at
    if left.run_id != right.run_id:
        if left.run_id is not None and right.run_id is not None:
            return left.run_id > right.run_id
        return left.run_id is not None
    if left.attempt != right.attempt:
        if left.attempt is not None and right.attempt is not None:
            return left.attempt > right.attempt
        return left.attempt is not None
    return True


def _state(value: object) -> str:
    upper = str(value or "").upper()
    if upper in _SUCCESS:
        return "success"
    if upper in _FAILURE:
        return "failure"
    if upper in _PENDING:
        return "pending"
    return "unknown"


def _run_producer(run: dict[str, Any]) -> str | None:
    app = run.get("app")
    if isinstance(app, dict):
        app_id = app.get("id")
        if (type(app_id) is int and app_id > 0) or (isinstance(app_id, str) and app_id.isdigit()):
            return f"app:{app_id}"
        if app.get("slug"):
            return f"app:{app['slug']}"
    return _status_producer(run)


def _status_producer(status: dict[str, Any]) -> str | None:
    if status.get("app_id"):
        return f"app:{status['app_id']}"
    creator = status.get("creator")
    if isinstance(creator, dict) and creator.get("login"):
        return f"user:{creator['login']}"
    return None


def _attempt(payload: dict[str, Any]) -> int | None:
    for key in ("run_attempt", "attempt"):
        value = payload.get(key)
        if isinstance(value, int):
            return value
        if isinstance(value, str) and value.isdigit():
            return int(value)
    return None


def _time(payload: dict[str, Any]) -> datetime | None:
    for key in ("started_at", "created_at", "completed_at", "updated_at"):
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
