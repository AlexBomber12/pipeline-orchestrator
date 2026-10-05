"""Read-only runtime diagnostics for the orchestrator MCP service.

The tools in this module deliberately read the producer-owned Redis keys and
files directly.  In particular, they do not use helpers that clean stale
indexes, refresh TTLs, synthesize healthy state, or otherwise mutate runtime
data while answering a diagnostic query.
"""

from __future__ import annotations

import json
import os
import re
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import redis.asyncio as aioredis

from src.config import AppConfig, RepoConfig, load_config
from src.events.publisher import EVENT_HISTORY_LIMIT
from src.keyspace import (
    cli_log_history,
    cli_log_latest,
    pipeline_state,
    repo_events_history,
    retry_command,
    retry_command_pending,
)
from src.mcp.server import mcp
from src.metrics import MetricsStore, RunRecord
from src.models import RepoState
from src.retry_commands import RetryCommand
from src.utils import repo_slug_from_url

_DEFAULT_REDIS_URL = "redis://localhost:6379/0"
_REPOS_ROOT = Path("/data/repos")
_EVENTS_ROOT = Path("/data/events")
_REPO_SLUG_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]*__[A-Za-z0-9][A-Za-z0-9_.-]*$")
_DISK_EVENT_SOURCE = re.compile(r"^events:disk/(\d{4}-\d{2}-\d{2})$")
_CLI_HISTORY_SOURCE_PREFIX = "cli:history/"
_MAX_STATUS_EVENTS = 25
_MAX_STATUS_RUNS = 20
_MAX_PENDING_RETRIES = 20
_MAX_LOG_SOURCES = 100
_MAX_READ_CHARS = 20_000
_MAX_EVENT_RECORD_CHARS = 4_000
_CLI_LATEST_TTL_SECONDS = 3600
_CLI_HISTORY_TTL_SECONDS = 86400

_SENSITIVE_NAMES = (
    "authorization",
    "proxy-authorization",
    "cookie",
    "set-cookie",
    "token",
    "secret",
    "credential",
    "access_token",
    "access-token",
    "refresh_token",
    "refresh-token",
    "auth_token",
    "auth-token",
    "api_key",
    "api-key",
    "client_secret",
    "client-secret",
    "password",
    "passwd",
    "private_key",
    "private-key",
    "aws_secret_access_key",
)
_SENSITIVE_NAME_PATTERN = "|".join(re.escape(name) for name in _SENSITIVE_NAMES)
# Environment variables commonly prefix the credential role (for example,
# ``DATABASE_PASSWORD`` and ``MY_API_KEY``). Match complete underscore/hyphen
# separated prefixes while requiring the sensitive name to end the key, so
# ordinary fields such as ``tokens_in`` are not mistaken for credentials.
_SENSITIVE_KEY_PATTERN = rf"(?:[A-Za-z0-9]+[_-])*(?:{_SENSITIVE_NAME_PATTERN})"
_REDACTION_RULES = (
    (
        re.compile(
            rf"(?i)([\"'](?:{_SENSITIVE_KEY_PATTERN})[\"']\s*:\s*[\"'])"
            r"[^\"'\r\n]*([\"'])"
        ),
        r"\1[REDACTED]\2",
    ),
    (
        re.compile(
            rf"(?im)((?<![A-Za-z0-9_-])(?:{_SENSITIVE_KEY_PATTERN})"
            r"(?![A-Za-z0-9_-])\s*[:=]\s*)"
            r"[\"'][^\"'\r\n]*[\"']"
        ),
        r"\1[REDACTED]",
    ),
    (
        re.compile(
            rf"(?im)((?<![A-Za-z0-9_-])(?:{_SENSITIVE_KEY_PATTERN})"
            r"(?![A-Za-z0-9_-])\s*[:=]\s*)"
            r"(?!\[REDACTED\])(?:bearer\s+|basic\s+)?[^\s,;]+"
        ),
        r"\1[REDACTED]",
    ),
    (re.compile(r"(?i)\b(?:bearer|basic)\s+[A-Za-z0-9._~+/=-]+"), "[REDACTED]"),
    (
        re.compile(r"\b(?:gh[pousr]_[A-Za-z0-9]{20,}|github_pat_[A-Za-z0-9_]{20,})\b"),
        "[REDACTED]",
    ),
    (re.compile(r"\b(?:AKIA|ASIA|AGPA|AIDA|AROA|AIPA|ANPA|ANVA)[A-Z0-9]{16}\b"), "[REDACTED]"),
    (re.compile(r"\bsk-ant-[A-Za-z0-9_-]{30,}\b"), "[REDACTED]"),
    (re.compile(r"\bsk-[A-Za-z0-9_-]{48,}\b"), "[REDACTED]"),
    (re.compile(r"\bxox[baprs]-[A-Za-z0-9-]{20,}\b"), "[REDACTED]"),
    (
        re.compile(
            r"\bhttps://hooks\.slack(?:-gov)?\.com/(?:services/)?"
            r"T[A-Z0-9]+/B[A-Z0-9]+/[A-Za-z0-9]{24}\b"
        ),
        "[REDACTED]",
    ),
    (re.compile(r"\bsk_(?:test|live)_[A-Za-z0-9]{20,}\b"), "[REDACTED]"),
    (re.compile(r"\brk_(?:test|live)_[A-Za-z0-9]{20,}\b"), "[REDACTED]"),
    (re.compile(r"\bAIza[A-Za-z0-9_-]{35}\b"), "[REDACTED]"),
    (
        re.compile(r"\beyJ[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}\b"),
        "[REDACTED]",
    ),
    (
        re.compile(
            r"(?is)-----BEGIN [^-\r\n]*PRIVATE KEY-----.*?"
            r"-----END [^-\r\n]*PRIVATE KEY-----"
        ),
        "[REDACTED PRIVATE KEY]",
    ),
    (
        re.compile(r"(?i)([a-z][a-z0-9+.-]*://[^\s/:@]*:)[^\s/@]+(@)"),
        r"\1[REDACTED]\2",
    ),
)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _iso_z(value: datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _decode(raw: object) -> str:
    if isinstance(raw, bytes):
        return raw.decode("utf-8", errors="replace")
    return str(raw)


def _validate_limit(value: int, *, maximum: int, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not 1 <= value <= maximum:
        raise ValueError(f"{name} must be between 1 and {maximum}")
    return value


def _validate_cursor(value: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError("cursor must be a non-negative integer")
    return value


def _new_redis_client() -> Any:
    return aioredis.from_url(
        os.environ.get("REDIS_URL", _DEFAULT_REDIS_URL),
        decode_responses=True,
    )


async def _close_redis(client: Any | None) -> None:
    if client is not None:
        await client.aclose()


def _configured_repositories() -> tuple[AppConfig, dict[str, RepoConfig]]:
    config = load_config()
    return config, {repo_slug_from_url(repo.url): repo for repo in config.repositories}


def _validate_configured_repo(repo_slug: str) -> tuple[AppConfig, RepoConfig]:
    if not _REPO_SLUG_PATTERN.fullmatch(repo_slug):
        raise ValueError(f"Invalid repo_slug: {repo_slug!r}")
    config, repositories = _configured_repositories()
    repo = repositories.get(repo_slug)
    if repo is None:
        raise ValueError(f"Repository is not configured: {repo_slug!r}")
    return config, repo


def _redact_text(text: str) -> tuple[str, int]:
    redacted = text
    count = 0
    for pattern, replacement in _REDACTION_RULES:
        redacted, replacements = pattern.subn(replacement, redacted)
        count += replacements
    return redacted, count


def _redact_structure(value: Any) -> tuple[Any, int]:
    if isinstance(value, str):
        return _redact_text(value)
    if isinstance(value, list):
        result: list[Any] = []
        count = 0
        for item in value:
            safe, replacements = _redact_structure(item)
            result.append(safe)
            count += replacements
        return result, count
    if isinstance(value, dict):
        result_dict: dict[Any, Any] = {}
        count = 0
        for key, item in value.items():
            safe, replacements = _redact_structure(item)
            result_dict[key] = safe
            count += replacements
        return result_dict, count
    return value, 0


def _error_text(exc: Exception) -> str:
    message = str(exc).strip()
    return message or type(exc).__name__


def _parse_timestamp(value: object) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _snapshot_metadata(
    state: RepoState | None,
    *,
    status: str,
    observed_at: datetime,
    stale_after_seconds: int,
    error: str | None = None,
) -> dict[str, Any]:
    result: dict[str, Any] = {
        "status": status,
        "observed_at": _iso_z(observed_at),
        "source_timestamp": None,
        "age_seconds": None,
        "stale_after_seconds": stale_after_seconds,
        "clock_skew_detected": False,
        "error": error,
        "meaning": (
            "Snapshot freshness only; last_updated is not evidence that a coder process is running or making progress."
        ),
    }
    if state is None:
        return result
    updated = state.last_updated
    if updated.tzinfo is None:
        updated = updated.replace(tzinfo=timezone.utc)
    updated = updated.astimezone(timezone.utc)
    age = (observed_at - updated).total_seconds()
    result.update(
        {
            "source_timestamp": _iso_z(updated),
            "age_seconds": round(age, 3),
            "clock_skew_detected": age < -5,
            "status": ("clock_skew" if age < -5 else "fresh" if age <= stale_after_seconds else "stale"),
        }
    )
    return result


def _stale_after_seconds(config: AppConfig, repo: RepoConfig) -> int:
    return max(
        300,
        repo.poll_interval_sec * 3,
        config.daemon.idle_extended_poll_interval_sec * 2,
        config.daemon.watch_slow_poll_interval_sec * 2,
    )


def _state_overview(
    slug: str,
    repo: RepoConfig,
    config: AppConfig,
    raw: object | None,
    *,
    redis_status: str,
    redis_error: str | None,
    observed_at: datetime,
) -> tuple[dict[str, Any], RepoState | None]:
    state: RepoState | None = None
    snapshot_status = redis_status
    snapshot_error = redis_error
    if redis_status == "available":
        if raw is None:
            snapshot_status = "missing"
            snapshot_error = "No pipeline state snapshot is stored for this repository."
        else:
            try:
                state = RepoState.model_validate_json(raw)
            except Exception as exc:
                snapshot_status = "malformed"
                snapshot_error = f"Stored pipeline state could not be decoded: {_error_text(exc)}"
            else:
                snapshot_status = "observed"
    stale_after = _stale_after_seconds(config, repo)
    snapshot = _snapshot_metadata(
        state,
        status=snapshot_status,
        observed_at=observed_at,
        stale_after_seconds=stale_after,
        error=snapshot_error,
    )
    configured_coder = (repo.coder or config.daemon.coder).value
    overview = {
        "repo_slug": slug,
        "url": repo.url,
        "configured": {
            "active": repo.active,
            "branch": repo.branch,
            "coder": configured_coder,
        },
        "snapshot": snapshot,
        "observed": {
            "state": state.state.value if state is not None else None,
            "active": state.active if state is not None else None,
            "user_paused": state.user_paused if state is not None else None,
            "coder": state.coder if state is not None else None,
            "current_task": (
                state.current_task.pr_id if state is not None and state.current_task is not None else None
            ),
            "current_pr": (state.current_pr.number if state is not None and state.current_pr is not None else None),
            "ci_status": (
                state.current_pr.ci_status.value if state is not None and state.current_pr is not None else None
            ),
            "review_status": (
                state.current_pr.review_status.value if state is not None and state.current_pr is not None else None
            ),
            "error": state.error_message if state is not None else None,
        },
        "integrity_warnings": (
            [f"Snapshot name {state.name!r} does not match configured slug {slug!r}."]
            if state is not None and state.name != slug
            else []
        ),
    }
    return overview, state


def _queue_summary(state: RepoState | None, observed_at: datetime) -> dict[str, Any]:
    if state is None or state.current_queue is None:
        return {
            "status": "unavailable",
            "source_timestamp": None,
            "snapshot_age_seconds": None,
            "reported_done": state.queue_done if state is not None else None,
            "reported_total": state.queue_total if state is not None else None,
            "counts_by_status": {},
            "tasks": [],
            "truncated": False,
        }
    timestamp = state.current_queue_snapshot_at
    age = None
    if timestamp is not None:
        if timestamp.tzinfo is None:
            timestamp = timestamp.replace(tzinfo=timezone.utc)
        timestamp = timestamp.astimezone(timezone.utc)
        age = round((observed_at - timestamp).total_seconds(), 3)
    counts: dict[str, int] = {}
    for task in state.current_queue:
        counts[task.status.value] = counts.get(task.status.value, 0) + 1
    tasks = [task.model_dump(mode="json") for task in state.current_queue[:20]]
    return {
        "status": "available",
        "source_timestamp": _iso_z(timestamp) if timestamp is not None else None,
        "snapshot_age_seconds": age,
        "reported_done": state.queue_done,
        "reported_total": state.queue_total,
        "counts_by_status": counts,
        "tasks": tasks,
        "truncated": len(state.current_queue) > len(tasks),
    }


def _bounded_event(raw: object) -> dict[str, Any]:
    text = _decode(raw)
    try:
        parsed = json.loads(text)
    except (TypeError, ValueError) as exc:
        return {
            "status": "malformed",
            "error": _error_text(exc),
            "raw_excerpt": text[:500],
            "record_truncated": len(text) > 500,
        }
    if not isinstance(parsed, dict):
        return {
            "status": "malformed",
            "error": "Event record is not a JSON object.",
            "raw_excerpt": text[:500],
            "record_truncated": len(text) > 500,
        }
    serialized = json.dumps(parsed, ensure_ascii=False, sort_keys=True, default=str)
    if len(serialized) > _MAX_EVENT_RECORD_CHARS:
        return {
            "status": "valid",
            "timestamp": parsed.get("timestamp"),
            "type": parsed.get("type") or parsed.get("event_type"),
            "record_excerpt": serialized[:_MAX_EVENT_RECORD_CHARS],
            "record_truncated": True,
        }
    return {"status": "valid", "record": parsed, "record_truncated": False}


async def _recent_events(redis_client: Any, repo_slug: str, limit: int) -> dict[str, Any]:
    try:
        total = int(await redis_client.llen(repo_events_history(repo_slug)))
        raw_events = await redis_client.lrange(repo_events_history(repo_slug), 0, limit - 1)
    except Exception as exc:
        return {
            "status": "unavailable",
            "source": "redis_event_history",
            "events": [],
            "error": _error_text(exc),
        }
    events = [_bounded_event(raw) for raw in raw_events]
    return {
        "status": "available",
        "source": "redis_event_history",
        "newest_first": True,
        "history_cap": EVENT_HISTORY_LIMIT,
        "events": events,
        "malformed_records": sum(event["status"] == "malformed" for event in events),
        "possibly_truncated": total >= EVENT_HISTORY_LIMIT,
        "error": None,
    }


def _retry_payload(command: RetryCommand, ttl_seconds: int) -> dict[str, Any]:
    payload = command.model_dump(mode="json")
    history = payload.pop("history", [])
    payload["recent_history"] = history[-5:]
    payload["history_truncated"] = len(history) > 5
    payload["ttl_seconds_remaining"] = ttl_seconds
    return payload


async def _pending_retries(redis_client: Any, repo_slug: str) -> dict[str, Any]:
    pending_key = retry_command_pending(repo_slug)
    try:
        total = int(await redis_client.zcard(pending_key))
        indexed = await redis_client.zrange(pending_key, 0, _MAX_PENDING_RETRIES - 1, withscores=True)
    except Exception as exc:
        return {
            "status": "unavailable",
            "count": None,
            "commands": [],
            "error": _error_text(exc),
            "read_only_note": "No stale index members were pruned.",
        }
    commands: list[dict[str, Any]] = []
    for row in indexed:
        raw_id, score = row
        command_id = _decode(raw_id)
        command_key = retry_command(repo_slug, command_id)
        try:
            raw = await redis_client.get(command_key)
            ttl = int(await redis_client.ttl(command_key))
        except Exception as exc:
            commands.append(
                {
                    "status": "unavailable",
                    "command_id": command_id,
                    "index_score": score,
                    "error": _error_text(exc),
                }
            )
            continue
        if raw is None:
            commands.append(
                {
                    "status": "missing_payload",
                    "command_id": command_id,
                    "index_score": score,
                    "error": "Pending index member has no retained command payload.",
                }
            )
            continue
        try:
            command = RetryCommand.model_validate_json(raw)
        except Exception as exc:
            commands.append(
                {
                    "status": "malformed",
                    "command_id": command_id,
                    "index_score": score,
                    "raw_excerpt": _decode(raw)[:500],
                    "error": _error_text(exc),
                }
            )
            continue
        commands.append(
            {
                "status": "available",
                "index_score": score,
                "command": _retry_payload(command, ttl),
            }
        )
    return {
        "status": "available",
        "count": total,
        "commands": commands,
        "truncated": total > len(indexed),
        "continuation": ({"next_index": len(indexed)} if total > len(indexed) else None),
        "error": None,
        "read_only_note": (
            "Index and payloads were read directly; missing or malformed members "
            "were reported without pruning or TTL refresh."
        ),
    }


def _run_payload(raw: object, run_id: str) -> dict[str, Any]:
    text = _decode(raw)
    try:
        decoded = json.loads(text)
        if not isinstance(decoded, dict):
            raise ValueError("Run record is not a JSON object.")
        record = RunRecord(**decoded)
    except Exception as exc:
        return {
            "status": "malformed",
            "run_id": run_id,
            "raw_excerpt": text[:500],
            "error": _error_text(exc),
        }
    payload = asdict(record)
    # Checkpointed in-progress records intentionally carry empty outcome/cause.
    # RunRecord's legacy migration maps an empty outcome to failed/CRASH on
    # construction, so restore the producer values for truthful diagnostics.
    if decoded.get("ended_at") is None:
        payload["outcome"] = decoded.get("outcome", "")
        payload["cause"] = decoded.get("cause")
    selected = {
        key: payload[key]
        for key in (
            "run_id",
            "task_id",
            "repo_name",
            "profile_id",
            "started_at",
            "ended_at",
            "duration_ms",
            "run_phase",
            "attempt_index",
            "fix_iterations",
            "exit_reason",
            "outcome",
            "cause",
            "cause_subsource",
            "base_sha",
            "head_sha",
        )
    }
    return {"status": "available", "record": selected}


async def _relevant_runs(
    redis_client: Any,
    repo_slug: str,
    task_id: str | None,
    limit: int,
) -> dict[str, Any]:
    index_key = MetricsStore._recent_key(task_id or "PR", repo_slug)
    scan_limit = min(200, max(20, limit * 4))
    try:
        raw_ids = await redis_client.lrange(index_key, 0, scan_limit - 1)
    except Exception as exc:
        return {
            "status": "unavailable",
            "task_filter": task_id,
            "records": [],
            "error": _error_text(exc),
        }
    records: list[dict[str, Any]] = []
    missing = 0
    for raw_id in raw_ids:
        run_id = _decode(raw_id)
        try:
            raw = await redis_client.get(MetricsStore._record_key(run_id))
        except Exception as exc:
            records.append({"status": "unavailable", "run_id": run_id, "error": _error_text(exc)})
            continue
        if raw is None:
            missing += 1
            continue
        item = _run_payload(raw, run_id)
        if task_id is not None and item["status"] == "available" and item["record"]["task_id"] != task_id:
            continue
        records.append(item)
        if len(records) >= limit:
            break
    return {
        "status": "available",
        "task_filter": task_id,
        "records": records,
        "missing_indexed_records": missing,
        "scanned_index_entries": len(raw_ids),
        "scan_limit": scan_limit,
        "truncated": len(raw_ids) >= scan_limit or len(records) >= limit,
        "error": None,
    }


def _state_history(state: RepoState | None, limit: int) -> dict[str, Any]:
    if state is None:
        return {"status": "unavailable", "newest_first": True, "events": []}
    selected = list(reversed(state.history[-limit:]))
    events: list[dict[str, Any]] = []
    for entry in selected:
        item = dict(entry)
        event = str(item.get("event", ""))
        if len(event) > 2_000:
            item["event"] = event[:2_000]
            item["record_truncated"] = True
        events.append(item)
    return {
        "status": "available",
        "newest_first": True,
        "events": events,
        "truncated": len(state.history) > len(events),
    }


def _state_detail(state: RepoState | None) -> dict[str, Any] | None:
    if state is None:
        return None
    return {
        "state": state.state.value,
        "active": state.active,
        "user_paused": state.user_paused,
        "coder": state.coder,
        "current_task": (state.current_task.model_dump(mode="json") if state.current_task is not None else None),
        "current_pr": (state.current_pr.model_dump(mode="json") if state.current_pr is not None else None),
        "error": state.error_message,
        "last_updated": _iso_z(state.last_updated),
        "merge_phase": state.merge_phase,
        "pending_queue_sync_branch": state.pending_queue_sync_branch,
        "pending_queue_sync_started_at": (
            _iso_z(state.pending_queue_sync_started_at) if state.pending_queue_sync_started_at is not None else None
        ),
        "upload_pending_count": state.upload_pending_count,
    }


def _progress_evidence(
    state: RepoState | None,
    runs: dict[str, Any],
    recent_events: dict[str, Any],
) -> dict[str, Any]:
    unfinished = [
        item["record"]
        for item in runs.get("records", [])
        if item.get("status") == "available" and item["record"].get("ended_at") is None
    ]
    event_timestamps: list[str] = []
    for item in recent_events.get("events", []):
        record = item.get("record") if isinstance(item, dict) else None
        timestamp = record.get("timestamp") if isinstance(record, dict) else None
        if isinstance(timestamp, str):
            event_timestamps.append(timestamp)
    return {
        "process_activity": "unknown",
        "current_task": (
            state.current_task.model_dump(mode="json") if state is not None and state.current_task is not None else None
        ),
        "current_pr": (
            state.current_pr.model_dump(mode="json") if state is not None and state.current_pr is not None else None
        ),
        "unfinished_run_records": unfinished,
        "latest_retained_event_at": max(event_timestamps) if event_timestamps else None,
        "interpretation": (
            "Task, PR, event, and unfinished run records are retained progress evidence. "
            "They do not prove a coder process is currently alive; MCP health and "
            "RepoState.last_updated are deliberately not used for that inference."
        ),
    }


@mcp.tool()
async def get_orchestrator_status(
    repo_slug: str | None = None,
    event_limit: int = 10,
    run_limit: int = 5,
) -> dict[str, Any]:
    """Return runtime status without mutating orchestrator state.

    Omit ``repo_slug`` for a compact overview of every configured repository.
    Supply a configured ``owner__repo`` slug for queue, inhibitor, event,
    pending-Retry, and run-record detail. Snapshot freshness is reported
    separately from progress evidence and never treated as coder liveness.
    """
    event_limit = _validate_limit(event_limit, maximum=_MAX_STATUS_EVENTS, name="event_limit")
    run_limit = _validate_limit(run_limit, maximum=_MAX_STATUS_RUNS, name="run_limit")
    observed_at = _utc_now()
    try:
        config, repositories = _configured_repositories()
    except Exception as exc:
        payload = {
            "observed_at": _iso_z(observed_at),
            "configuration": {"status": "unavailable", "error": _error_text(exc)},
            "redis": {"status": "not_checked", "error": None},
            "repositories": [],
            "detail": None,
        }
        safe, replacements = _redact_structure(payload)
        safe["redaction"] = {"applied": replacements > 0, "replacements": replacements}
        return safe
    if repo_slug is not None:
        if not _REPO_SLUG_PATTERN.fullmatch(repo_slug):
            raise ValueError(f"Invalid repo_slug: {repo_slug!r}")
        if repo_slug not in repositories:
            raise ValueError(f"Repository is not configured: {repo_slug!r}")

    client: Any | None = None
    redis_status = "available"
    redis_error: str | None = None
    state_raw: list[object | None] = [None] * len(repositories)
    slugs = list(repositories)
    try:
        client = _new_redis_client()
        state_raw = await client.mget([pipeline_state(slug) for slug in slugs])
    except Exception as exc:
        redis_status = "unavailable"
        redis_error = _error_text(exc)

    overviews: list[dict[str, Any]] = []
    states: dict[str, RepoState | None] = {}
    for index, slug in enumerate(slugs):
        overview, state = _state_overview(
            slug,
            repositories[slug],
            config,
            state_raw[index],
            redis_status=redis_status,
            redis_error=redis_error,
            observed_at=observed_at,
        )
        overviews.append(overview)
        states[slug] = state

    detail: dict[str, Any] | None = None
    if repo_slug is not None:
        state = states[repo_slug]
        if redis_status == "available" and client is not None:
            events = await _recent_events(client, repo_slug, event_limit)
            retries = await _pending_retries(client, repo_slug)
            runs = await _relevant_runs(
                client,
                repo_slug,
                state.current_task.pr_id if state is not None and state.current_task is not None else None,
                run_limit,
            )
        else:
            unavailable = {"status": "unavailable", "error": redis_error}
            events = {**unavailable, "source": "redis_event_history", "events": []}
            retries = {**unavailable, "count": None, "commands": []}
            runs = {**unavailable, "task_filter": None, "records": []}
        detail = {
            "repo_slug": repo_slug,
            "state": _state_detail(state),
            "queue": _queue_summary(state, observed_at),
            "inhibitors": (
                [item.model_dump(mode="json") for item in state.active_inhibitors] if state is not None else []
            ),
            "state_history": _state_history(state, event_limit),
            "recent_events": events,
            "pending_retries": retries,
            "run_records": runs,
            "coder_progress": _progress_evidence(state, runs, events),
        }

    await _close_redis(client)
    payload = {
        "observed_at": _iso_z(observed_at),
        "configuration": {"status": "available", "repository_count": len(slugs)},
        "redis": {"status": redis_status, "error": redis_error},
        "repositories": overviews,
        "detail": detail,
    }
    safe, replacements = _redact_structure(payload)
    safe["redaction"] = {"applied": replacements > 0, "replacements": replacements}
    return safe


def _ttl_metadata(ttl: int, observed_at: datetime) -> dict[str, Any]:
    return {
        "ttl_seconds_remaining": ttl if ttl >= 0 else None,
        "expires_at": (_iso_z(observed_at + timedelta(seconds=ttl)) if ttl >= 0 else None),
        "expiry_status": "expires" if ttl >= 0 else "persistent" if ttl == -1 else "missing",
    }


def _association(*, recorded: bool = False) -> dict[str, Any]:
    return {
        "recorded": recorded,
        "task_id": None,
        "run_id": None,
        "sha": None,
        "note": ("No task, run, or SHA association is recorded by this source." if not recorded else None),
    }


def _safe_path(root: Path, *parts: str) -> Path:
    resolved_root = root.resolve()
    candidate = resolved_root.joinpath(*parts).resolve()
    if not candidate.is_relative_to(resolved_root):
        raise ValueError("Resolved diagnostic path escapes its allowed root.")
    return candidate


async def _redis_log_sources(
    client: Any, repo_slug: str, observed_at: datetime
) -> tuple[list[dict[str, Any]], list[str]]:
    sources: list[dict[str, Any]] = []
    warnings: list[str] = []
    latest_key = cli_log_latest(repo_slug)
    try:
        latest = await client.get(latest_key)
        latest_ttl = int(await client.ttl(latest_key))
        event_count = int(await client.llen(repo_events_history(repo_slug)))
        event_edges = await client.lrange(repo_events_history(repo_slug), 0, -1)
    except Exception as exc:
        message = _error_text(exc)
        warnings.append(f"Redis diagnostic sources unavailable: {message}")
        sources.extend(
            [
                {
                    "source_id": "cli:latest",
                    "kind": "retained_cli_log",
                    "storage": "redis",
                    "availability": "unavailable",
                    "error": message,
                    "association": _association(),
                },
                {
                    "source_id": "events:redis",
                    "kind": "repository_event_history",
                    "storage": "redis",
                    "availability": "unavailable",
                    "error": message,
                    "association": _association(),
                },
            ]
        )
        return sources, warnings

    latest_text = _decode(latest) if latest is not None else ""
    sources.append(
        {
            "source_id": "cli:latest",
            "kind": "retained_cli_log",
            "storage": "redis",
            "availability": "available" if latest is not None else "missing_or_expired",
            "timestamps": {
                "recorded_at": None,
                **_ttl_metadata(latest_ttl, observed_at),
            },
            "size_chars": len(latest_text) if latest is not None else 0,
            "retention": {
                "producer_ttl_seconds": _CLI_LATEST_TTL_SECONDS,
                "truncated": latest_text.startswith("[truncated]\n"),
                "truncation_marker_preserved": True,
            },
            "association": _association(),
            "mutable": True,
        }
    )

    event_timestamps: list[str] = []
    malformed_events = 0
    for raw in event_edges:
        item = _bounded_event(raw)
        if item["status"] == "malformed":
            malformed_events += 1
            continue
        record = item.get("record")
        timestamp = record.get("timestamp") if isinstance(record, dict) else item.get("timestamp")
        if isinstance(timestamp, str):
            event_timestamps.append(timestamp)
    sources.append(
        {
            "source_id": "events:redis",
            "kind": "repository_event_history",
            "storage": "redis",
            "availability": "available" if event_count else "empty",
            "timestamps": {
                "newest_at": max(event_timestamps) if event_timestamps else None,
                "oldest_at": min(event_timestamps) if event_timestamps else None,
                "expires_at": None,
            },
            "record_count": event_count,
            "malformed_records": malformed_events,
            "retention": {
                "entry_cap": EVENT_HISTORY_LIMIT,
                "possibly_truncated": event_count >= EVENT_HISTORY_LIMIT,
                "expiry": "none",
            },
            "association": _association(),
            "mutable": True,
            "ordering": "newest_first",
        }
    )

    prefix = cli_log_history(repo_slug, "")
    malformed_keys = 0
    try:
        async for raw_key in client.scan_iter(match=cli_log_history(repo_slug, "*")):
            key = _decode(raw_key)
            if key == latest_key or not key.startswith(prefix):
                continue
            timestamp = key[len(prefix) :]
            if _parse_timestamp(timestamp) is None:
                malformed_keys += 1
                continue
            value = await client.get(key)
            ttl = int(await client.ttl(key))
            if value is None:
                continue
            text = _decode(value)
            sources.append(
                {
                    "source_id": f"{_CLI_HISTORY_SOURCE_PREFIX}{timestamp}",
                    "kind": "retained_cli_log",
                    "storage": "redis",
                    "availability": "available",
                    "timestamps": {
                        "recorded_at": timestamp,
                        **_ttl_metadata(ttl, observed_at),
                    },
                    "size_chars": len(text),
                    "retention": {
                        "producer_ttl_seconds": _CLI_HISTORY_TTL_SECONDS,
                        "truncated": text.startswith("[truncated]\n"),
                        "truncation_marker_preserved": True,
                    },
                    "association": _association(),
                    "mutable": False,
                }
            )
    except Exception as exc:
        warnings.append(f"CLI history discovery incomplete: {_error_text(exc)}")
    if malformed_keys:
        warnings.append(
            f"Ignored {malformed_keys} malformed CLI history key(s) in the constrained repository namespace."
        )
    return sources, warnings


def _file_log_sources(repo_slug: str) -> tuple[list[dict[str, Any]], list[str]]:
    sources: list[dict[str, Any]] = []
    warnings: list[str] = []
    try:
        event_dir = _safe_path(_EVENTS_ROOT, repo_slug)
    except ValueError as exc:
        return sources, [_error_text(exc)]
    if event_dir.is_dir():
        for path in sorted(event_dir.glob("*.jsonl"), reverse=True):
            if not re.fullmatch(r"\d{4}-\d{2}-\d{2}\.jsonl", path.name):
                continue
            try:
                resolved = _safe_path(event_dir, path.name)
                stat = resolved.stat()
            except (OSError, ValueError) as exc:
                warnings.append(f"Could not inspect event partition {path.name!r}: {_error_text(exc)}")
                continue
            date = path.stem
            sources.append(
                {
                    "source_id": f"events:disk/{date}",
                    "kind": "disk_event_log",
                    "storage": "filesystem",
                    "availability": "available",
                    "timestamps": {
                        "partition_date": date,
                        "modified_at": _iso_z(datetime.fromtimestamp(stat.st_mtime, timezone.utc)),
                        "expires_at": None,
                    },
                    "size_bytes": stat.st_size,
                    "retention": {"policy": "retained until file removal", "truncated": False},
                    "association": _association(),
                    "mutable": date == _utc_now().date().isoformat(),
                    "ordering": "oldest_first",
                }
            )

    try:
        ci_path = _safe_path(_REPOS_ROOT, repo_slug, "artifacts", "ci.log")
        ci_stat = ci_path.stat() if ci_path.is_file() else None
    except (OSError, ValueError) as exc:
        ci_stat = None
        warnings.append(f"Could not inspect artifacts/ci.log: {_error_text(exc)}")
    sources.append(
        {
            "source_id": "ci:artifact",
            "kind": "current_checkout_ci_artifact",
            "storage": "filesystem",
            "availability": "available" if ci_stat is not None else "missing",
            "timestamps": {
                "modified_at": (
                    _iso_z(datetime.fromtimestamp(ci_stat.st_mtime, timezone.utc)) if ci_stat is not None else None
                ),
                "expires_at": None,
            },
            "size_bytes": ci_stat.st_size if ci_stat is not None else 0,
            "retention": {
                "policy": "mutable checkout artifact; may be replaced by the next gate run",
                "truncated": False,
            },
            "association": _association(),
            "mutable": True,
        }
    )
    return sources, warnings


def _unretained_sources() -> list[dict[str, Any]]:
    return [
        {
            "source_id": "daemon:stdout",
            "kind": "daemon_stdout",
            "storage": "not_retained",
            "availability": "unavailable",
            "reason": "Daemon stdout is not persistently captured by the current deployment.",
            "association": _association(),
        },
        {
            "source_id": "cli:live",
            "kind": "live_cli_output",
            "storage": "not_retained",
            "availability": "unavailable",
            "reason": ("Live CLI output is not exposed; only completed retained CLI snapshots are available."),
            "association": _association(),
        },
    ]


@mcp.tool()
async def list_orchestrator_logs(
    repo_slug: str,
    cursor: int = 0,
    limit: int = 50,
) -> dict[str, Any]:
    """Discover retained log sources for one configured repository.

    Results are bounded and paginated. Each source reports timestamps,
    expiry/truncation behavior, mutability, and whether task/run/SHA identity
    was actually recorded by its producer.
    """
    _validate_configured_repo(repo_slug)
    cursor = _validate_cursor(cursor)
    limit = _validate_limit(limit, maximum=_MAX_LOG_SOURCES, name="limit")
    observed_at = _utc_now()
    client: Any | None = None
    try:
        client = _new_redis_client()
        redis_sources, redis_warnings = await _redis_log_sources(client, repo_slug, observed_at)
    except Exception as exc:
        message = _error_text(exc)
        redis_sources = [
            {
                "source_id": "redis:diagnostics",
                "kind": "redis_diagnostic_sources",
                "storage": "redis",
                "availability": "unavailable",
                "error": message,
                "association": _association(),
            }
        ]
        redis_warnings = [f"Redis diagnostic sources unavailable: {message}"]
    finally:
        await _close_redis(client)
    file_sources, file_warnings = _file_log_sources(repo_slug)
    sources = redis_sources + file_sources + _unretained_sources()
    sources.sort(
        key=lambda item: (
            item["source_id"] not in {"cli:latest", "events:redis", "ci:artifact"},
            item["source_id"],
        )
    )
    page = sources[cursor : cursor + limit]
    next_cursor = cursor + len(page) if cursor + len(page) < len(sources) else None
    payload = {
        "observed_at": _iso_z(observed_at),
        "repo_slug": repo_slug,
        "sources": page,
        "warnings": redis_warnings + file_warnings,
        "pagination": {
            "cursor": cursor,
            "limit": limit,
            "returned": len(page),
            "total": len(sources),
            "next_cursor": next_cursor,
        },
        "gaps": [
            "Expired Redis CLI logs cannot be discovered after their keys disappear.",
            "Daemon stdout and live CLI streaming are not retained; persistent capture is follow-up work.",
        ],
    }
    safe, replacements = _redact_structure(payload)
    safe["redaction"] = {"applied": replacements > 0, "replacements": replacements}
    return safe


def _parse_cli_history_source(source_id: str) -> str | None:
    if not source_id.startswith(_CLI_HISTORY_SOURCE_PREFIX):
        return None
    timestamp = source_id[len(_CLI_HISTORY_SOURCE_PREFIX) :]
    if _parse_timestamp(timestamp) is None or "/" in timestamp:
        raise ValueError("Invalid CLI history source timestamp.")
    return timestamp


async def _read_redis_source(
    client: Any, repo_slug: str, source_id: str, observed_at: datetime
) -> tuple[str | None, dict[str, Any], list[str]]:
    warnings: list[str] = []
    if source_id == "cli:latest":
        key = cli_log_latest(repo_slug)
        raw = await client.get(key)
        ttl = int(await client.ttl(key))
        return (
            _decode(raw) if raw is not None else None,
            {
                "kind": "retained_cli_log",
                "storage": "redis",
                "availability": "available" if raw is not None else "missing_or_expired",
                "timestamps": {"recorded_at": None, **_ttl_metadata(ttl, observed_at)},
                "retention": {
                    "producer_ttl_seconds": _CLI_LATEST_TTL_SECONDS,
                    "truncation_marker_preserved": True,
                },
                "association": _association(),
                "mutable": True,
            },
            warnings,
        )
    timestamp = _parse_cli_history_source(source_id)
    if timestamp is not None:
        key = cli_log_history(repo_slug, timestamp)
        raw = await client.get(key)
        ttl = int(await client.ttl(key))
        return (
            _decode(raw) if raw is not None else None,
            {
                "kind": "retained_cli_log",
                "storage": "redis",
                "availability": "available" if raw is not None else "missing_or_expired",
                "timestamps": {"recorded_at": timestamp, **_ttl_metadata(ttl, observed_at)},
                "retention": {
                    "producer_ttl_seconds": _CLI_HISTORY_TTL_SECONDS,
                    "truncation_marker_preserved": True,
                },
                "association": _association(),
                "mutable": False,
            },
            warnings,
        )
    if source_id == "events:redis":
        raw_events = await client.lrange(repo_events_history(repo_slug), 0, -1)
        malformed = sum(_bounded_event(item)["status"] == "malformed" for item in raw_events)
        if malformed:
            warnings.append(f"Source contains {malformed} malformed event record(s).")
        content = "\n".join(_decode(item) for item in raw_events)
        return (
            content,
            {
                "kind": "repository_event_history",
                "storage": "redis",
                "availability": "available" if raw_events else "empty",
                "record_count": len(raw_events),
                "malformed_records": malformed,
                "retention": {
                    "entry_cap": EVENT_HISTORY_LIMIT,
                    "possibly_truncated": len(raw_events) >= EVENT_HISTORY_LIMIT,
                },
                "association": _association(),
                "mutable": True,
                "ordering": "newest_first",
            },
            warnings,
        )
    raise ValueError(f"Unknown Redis diagnostic source: {source_id!r}")


def _read_file_source(repo_slug: str, source_id: str) -> tuple[str | None, dict[str, Any], list[str]]:
    warnings: list[str] = []
    if source_id == "ci:artifact":
        path = _safe_path(_REPOS_ROOT, repo_slug, "artifacts", "ci.log")
        kind = "current_checkout_ci_artifact"
        mutable = True
        association = _association()
    else:
        match = _DISK_EVENT_SOURCE.fullmatch(source_id)
        if match is None:
            raise ValueError(f"Unknown filesystem diagnostic source: {source_id!r}")
        date = match.group(1)
        try:
            datetime.strptime(date, "%Y-%m-%d")
        except ValueError as exc:
            raise ValueError("Invalid disk event partition date.") from exc
        path = _safe_path(_EVENTS_ROOT, repo_slug, f"{date}.jsonl")
        kind = "disk_event_log"
        mutable = date == _utc_now().date().isoformat()
        association = _association()
    if not path.is_file():
        return (
            None,
            {
                "kind": kind,
                "storage": "filesystem",
                "availability": "missing",
                "association": association,
                "mutable": mutable,
            },
            warnings,
        )
    try:
        content = path.read_text(encoding="utf-8", errors="replace")
        stat = path.stat()
    except OSError as exc:
        return (
            None,
            {
                "kind": kind,
                "storage": "filesystem",
                "availability": "unavailable",
                "error": _error_text(exc),
                "association": association,
                "mutable": mutable,
            },
            warnings,
        )
    malformed = 0
    if kind == "disk_event_log":
        malformed = sum(_bounded_event(line)["status"] == "malformed" for line in content.splitlines() if line)
        if malformed:
            warnings.append(f"Source contains {malformed} malformed event record(s).")
    return (
        content,
        {
            "kind": kind,
            "storage": "filesystem",
            "availability": "available",
            "timestamps": {
                "modified_at": _iso_z(datetime.fromtimestamp(stat.st_mtime, timezone.utc)),
                "expires_at": None,
            },
            "size_bytes": stat.st_size,
            "malformed_records": malformed if kind == "disk_event_log" else None,
            "retention": {
                "policy": (
                    "mutable checkout artifact; may be replaced by the next gate run"
                    if kind == "current_checkout_ci_artifact"
                    else "retained until file removal"
                ),
                "truncated": False,
            },
            "association": association,
            "mutable": mutable,
            "ordering": "oldest_first" if kind == "disk_event_log" else None,
        },
        warnings,
    )


def _page_content(content: str, *, cursor: int, max_chars: int, tail: bool) -> tuple[str, dict[str, Any]]:
    total = len(content)
    start = max(0, total - max_chars) if tail else min(cursor, total)
    end = min(total, start + max_chars)
    page = content[start:end]
    return page, {
        "cursor": start,
        "requested_cursor": cursor,
        "max_chars": max_chars,
        "returned_chars": len(page),
        "total_chars_after_redaction": total,
        "previous_cursor": max(0, start - max_chars) if start > 0 else None,
        "next_cursor": end if end < total else None,
        "has_more": end < total,
        "tail": tail,
    }


@mcp.tool()
async def read_orchestrator_log(
    repo_slug: str,
    source_id: str,
    cursor: int = 0,
    max_chars: int = 8_000,
    tail: bool = False,
) -> dict[str, Any]:
    """Read one discovered diagnostic source with bounded pagination.

    ``cursor`` is a character offset in the redacted content. Set ``tail`` to
    read the final page (useful for failure excerpts); tail mode requires the
    default cursor. Credential-like and Authorization values are redacted
    before pagination, while existing producer truncation markers are kept.
    """
    _validate_configured_repo(repo_slug)
    cursor = _validate_cursor(cursor)
    max_chars = _validate_limit(max_chars, maximum=_MAX_READ_CHARS, name="max_chars")
    if tail and cursor != 0:
        raise ValueError("cursor must be 0 when tail is true")
    observed_at = _utc_now()
    if source_id in {"daemon:stdout", "cli:live"}:
        reason = (
            "Daemon stdout is not persistently captured by the current deployment."
            if source_id == "daemon:stdout"
            else "Live CLI output is unavailable; use a retained cli:* source after the run completes."
        )
        return {
            "observed_at": _iso_z(observed_at),
            "repo_slug": repo_slug,
            "source_id": source_id,
            "source": {"availability": "unavailable", "reason": reason},
            "content": "",
            "pagination": None,
            "warnings": [reason],
            "redaction": {"applied": False, "replacements": 0},
        }

    content: str | None
    source: dict[str, Any]
    warnings: list[str]
    if source_id in {"cli:latest", "events:redis"} or source_id.startswith(_CLI_HISTORY_SOURCE_PREFIX):
        client: Any | None = None
        try:
            client = _new_redis_client()
            content, source, warnings = await _read_redis_source(client, repo_slug, source_id, observed_at)
        except ValueError:
            raise
        except Exception as exc:
            content = None
            source = {
                "availability": "unavailable",
                "storage": "redis",
                "error": _error_text(exc),
                "association": _association(),
            }
            warnings = [f"Redis source unavailable: {_error_text(exc)}"]
        finally:
            await _close_redis(client)
    else:
        content, source, warnings = _read_file_source(repo_slug, source_id)

    if content is None:
        payload = {
            "observed_at": _iso_z(observed_at),
            "repo_slug": repo_slug,
            "source_id": source_id,
            "source": source,
            "content": "",
            "pagination": None,
            "warnings": warnings,
        }
        safe, replacements = _redact_structure(payload)
        safe["redaction"] = {"applied": replacements > 0, "replacements": replacements}
        return safe

    redacted, replacements = _redact_text(content)
    page, pagination = _page_content(redacted, cursor=cursor, max_chars=max_chars, tail=tail)
    source["source_truncated"] = content.startswith("[truncated]\n")
    return {
        "observed_at": _iso_z(observed_at),
        "repo_slug": repo_slug,
        "source_id": source_id,
        "source": source,
        "content": page,
        "pagination": pagination,
        "warnings": warnings,
        "redaction": {"applied": replacements > 0, "replacements": replacements},
    }
