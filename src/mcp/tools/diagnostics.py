"""Bounded, read-only structured runtime diagnostics for the MCP service.

This module reads producer-owned Redis records directly.  It intentionally
does not call helpers that refresh TTLs, prune indexes, or otherwise mutate
orchestrator state.  Every returned field is selected explicitly; free-form
producer text and arbitrary payload mappings are never returned.
"""

from __future__ import annotations

import json
import math
import os
import re
import uuid
from dataclasses import asdict
from datetime import datetime, timezone
from typing import Any

import redis.asyncio as aioredis

from src.cancellation import SUBSOURCE_VOCABULARY
from src.cancellation.storage import CATEGORIES, cause_key
from src.config import AppConfig, CoderType, RepoConfig, load_config
from src.keyspace import pipeline_state, retry_command, retry_command_pending
from src.mcp.server import mcp
from src.metrics import MetricsStore, RunRecord
from src.models import RepoState
from src.retry_commands import RetryCommand
from src.utils import repo_slug_from_url

_DEFAULT_REDIS_URL = "redis://localhost:6379/0"
_REPO_SLUG = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]*__[A-Za-z0-9][A-Za-z0-9_.-]*$")
_TASK_ID = re.compile(r"^PR-[1-9][0-9]*$")
_SHA = re.compile(r"^[0-9a-fA-F]{40}$")

_MAX_RETRIES = 20
_MAX_RUNS = 20
_MAX_INHIBITORS = 20
_MAX_RUN_INDEX_ENTRIES = 200
_MAX_STATE_BYTES = 256 * 1024
_MAX_RETRY_BYTES = 64 * 1024
_MAX_CANCELLATION_BYTES = 64 * 1024
_MAX_RUN_BYTES = 64 * 1024
_MAX_INDEX_MEMBER_BYTES = 512

_KNOWN_CODERS = frozenset(item.value for item in CoderType)
_KNOWN_CATEGORIES = frozenset(CATEGORIES)
_KNOWN_SUBSOURCES = frozenset(SUBSOURCE_VOCABULARY)

_BOUNDED_RETRY_INDEX_SCRIPT = """
local total = redis.call('ZCARD', KEYS[1])
local rows = redis.call('ZRANGE', KEYS[1], 0, tonumber(ARGV[1]) - 1, 'WITHSCORES')
local result = {}
for index = 1, #rows, 2 do
  local member = rows[index]
  local size = string.len(member)
  table.insert(result, size)
  table.insert(result, size <= tonumber(ARGV[2]) and member or '')
  table.insert(result, rows[index + 1])
end
return {total, result}
"""

_BOUNDED_RUN_INDEX_SCRIPT = """
local total = redis.call('LLEN', KEYS[1])
local result = {}
local stop = math.min(total, tonumber(ARGV[1])) - 1
for index = 0, stop do
  local member = redis.call('LINDEX', KEYS[1], index)
  local size = string.len(member)
  table.insert(result, size)
  table.insert(result, size <= tonumber(ARGV[2]) and member or '')
end
return {total, result}
"""


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _iso_z(value: datetime) -> str:
    normalized = value.astimezone(timezone.utc)
    return normalized.isoformat().replace("+00:00", "Z")


def _timestamp(value: object) -> datetime | None:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str) and value:
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
    else:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _timestamp_text(value: object) -> str | None:
    parsed = _timestamp(value)
    return _iso_z(parsed) if parsed is not None else None


def _positive_int(value: object) -> int | None:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        return None
    return value


def _positive_number(value: object) -> int | None:
    result = _positive_int(value)
    return result if result is not None and result > 0 else None


def _task_id(value: object) -> str | None:
    return value if isinstance(value, str) and _TASK_ID.fullmatch(value) else None


def _sha(value: object) -> str | None:
    return value.lower() if isinstance(value, str) and _SHA.fullmatch(value) else None


def _uuid(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    try:
        return str(uuid.UUID(value))
    except (ValueError, AttributeError):
        return None


def _coder(value: object) -> str | None:
    return value if isinstance(value, str) and value in _KNOWN_CODERS else None


def _subsource(value: object) -> str | None:
    return value if isinstance(value, str) and value in _KNOWN_SUBSOURCES else None


def _validate_limit(value: int, *, name: str, maximum: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not 1 <= value <= maximum:
        raise ValueError(f"{name} must be an integer between 1 and {maximum}")
    return value


def _decode(raw: object) -> str:
    if isinstance(raw, bytes):
        return raw.decode("utf-8")
    if isinstance(raw, str):
        return raw
    raise TypeError("Redis value is not text")


def _new_redis_client() -> Any:
    return aioredis.from_url(
        os.environ.get("REDIS_URL", _DEFAULT_REDIS_URL),
        decode_responses=False,
    )


async def _close_redis(client: Any | None) -> None:
    if client is not None:
        await client.aclose()


def _configured_repositories() -> tuple[AppConfig, dict[str, RepoConfig]]:
    config = load_config()
    repositories: dict[str, RepoConfig] = {}
    for repo in config.repositories:
        slug = repo_slug_from_url(repo.url)
        if not _REPO_SLUG.fullmatch(slug) or slug in repositories:
            raise ValueError("configured repository identity is invalid")
        repositories[slug] = repo
    return config, repositories


async def _read_bounded_string(
    client: Any,
    key: str,
    maximum: int,
) -> tuple[object | None, int | None, bool]:
    """Read one string without allowing a concurrent growth past the bound."""
    reported_size = int(await client.strlen(key))
    if reported_size > maximum:
        return None, reported_size, True
    raw = await client.getrange(key, 0, maximum)
    observed_size = len(raw if isinstance(raw, bytes) else str(raw).encode())
    if observed_size > maximum:
        return None, max(reported_size, observed_size), True
    if observed_size == 0 and not await client.exists(key):
        return None, None, False
    return raw, max(reported_size, observed_size), False


def _stale_after_seconds(config: AppConfig, repo: RepoConfig) -> int:
    return max(
        300,
        repo.poll_interval_sec * 3,
        config.daemon.idle_extended_poll_interval_sec * 2,
        config.daemon.watch_slow_poll_interval_sec * 2,
    )


def _snapshot_unavailable(status: str, code: str) -> dict[str, Any]:
    return {
        "status": status,
        "code": code,
        "source_timestamp": None,
        "age_seconds": None,
        "stale_after_seconds": None,
        "clock_skew_detected": False,
        "source_size_bytes": None,
        "read_bound_bytes": _MAX_STATE_BYTES,
        "freshness_is_coder_activity": False,
    }


def _snapshot_result(
    raw: object | None,
    *,
    size_bytes: int | None,
    oversized: bool,
    slug: str,
    repo: RepoConfig,
    config: AppConfig,
    observed_at: datetime,
) -> tuple[dict[str, Any], RepoState | None]:
    if oversized:
        result = _snapshot_unavailable("oversized", "snapshot_size_limit")
        result["source_size_bytes"] = size_bytes
        return result, None
    if raw is None:
        return _snapshot_unavailable("missing", "snapshot_missing"), None
    try:
        state = RepoState.model_validate_json(_decode(raw))
    except Exception:
        result = _snapshot_unavailable("malformed", "snapshot_invalid")
        result["source_size_bytes"] = size_bytes
        return result, None
    if state.name != slug or state.url != repo.url:
        result = _snapshot_unavailable("malformed", "snapshot_repository_mismatch")
        result["source_size_bytes"] = size_bytes
        return result, None

    source_timestamp = state.last_updated
    if source_timestamp.tzinfo is None:
        source_timestamp = source_timestamp.replace(tzinfo=timezone.utc)
    source_timestamp = source_timestamp.astimezone(timezone.utc)
    age = (observed_at - source_timestamp).total_seconds()
    stale_after = _stale_after_seconds(config, repo)
    status = "clock_skew" if age < -5 else "fresh" if age <= stale_after else "stale"
    return (
        {
            "status": status,
            "code": None,
            "source_timestamp": _iso_z(source_timestamp),
            "age_seconds": round(age, 3),
            "stale_after_seconds": stale_after,
            "clock_skew_detected": age < -5,
            "source_size_bytes": size_bytes,
            "read_bound_bytes": _MAX_STATE_BYTES,
            "freshness_is_coder_activity": False,
        },
        state,
    )


async def _read_snapshot(
    client: Any,
    slug: str,
    repo: RepoConfig,
    config: AppConfig,
    observed_at: datetime,
) -> tuple[dict[str, Any], RepoState | None]:
    try:
        raw, size_bytes, oversized = await _read_bounded_string(client, pipeline_state(slug), _MAX_STATE_BYTES)
    except Exception:
        return _snapshot_unavailable("unavailable", "snapshot_read_failed"), None
    return _snapshot_result(
        raw,
        size_bytes=size_bytes,
        oversized=oversized,
        slug=slug,
        repo=repo,
        config=config,
        observed_at=observed_at,
    )


def _pipeline_view(state: RepoState | None) -> dict[str, Any] | None:
    if state is None:
        return None
    integrity_codes: list[str] = []
    coder = _coder(state.coder)
    if state.coder is not None and coder is None:
        integrity_codes.append("invalid_coder")

    task = None
    if state.current_task is not None:
        task_id = _task_id(state.current_task.pr_id)
        if task_id is None:
            integrity_codes.append("invalid_current_task_id")
        else:
            task = {"id": task_id, "status": state.current_task.status.value}

    pull_request = None
    if state.current_pr is not None:
        number = _positive_number(state.current_pr.number)
        head_sha = _sha(state.current_pr.head_sha)
        if number is None:
            integrity_codes.append("invalid_current_pr_number")
        if state.current_pr.head_sha and head_sha is None:
            integrity_codes.append("invalid_current_pr_sha")
        if number is not None:
            pull_request = {
                "number": number,
                "head_sha": head_sha,
                "ci_status": state.current_pr.ci_status.value,
                "review_status": state.current_pr.review_status.value,
            }

    return {
        "state": state.state.value,
        "active": state.active,
        "paused": state.user_paused,
        "coder": coder,
        "current_task": task,
        "current_pr": pull_request,
        "integrity_codes": integrity_codes,
        "coder_activity": "unknown",
    }


def _overview(
    slug: str,
    repo: RepoConfig,
    config: AppConfig,
    snapshot: dict[str, Any],
    state: RepoState | None,
) -> dict[str, Any]:
    return {
        "repo_slug": slug,
        "configured": {
            "active": repo.active,
            "coder": (repo.coder or config.daemon.coder).value,
        },
        "snapshot": snapshot,
        "pipeline": _pipeline_view(state),
    }


def _inhibitors(state: RepoState | None, observed_at: datetime) -> dict[str, Any]:
    if state is None:
        return {
            "status": "unavailable",
            "records": [],
            "record_count": None,
            "truncated": False,
        }
    records: list[dict[str, Any]] = []
    for inhibitor in state.active_inhibitors[:_MAX_INHIBITORS]:
        affected = _coder(inhibitor.coder_affected)
        expires = inhibitor.expires_at
        records.append(
            {
                "type": inhibitor.inhibitor_type.value,
                "coder_scope": (
                    "all" if inhibitor.coder_affected is None else affected if affected is not None else "unknown"
                ),
                "expires_at": _timestamp_text(expires),
                "expired_at_observation": expires is not None and expires <= observed_at,
            }
        )
    return {
        "status": "available",
        "records": records,
        "record_count": len(state.active_inhibitors),
        "truncated": len(state.active_inhibitors) > len(records),
    }


def _ttl_fields(ttl: int) -> dict[str, Any]:
    return {
        "ttl_seconds_remaining": ttl if ttl >= 0 else None,
        "expiry_status": "expires" if ttl >= 0 else "persistent" if ttl == -1 else "missing",
    }


async def _current_cancellation(
    client: Any,
    slug: str,
    task_id: str | None,
) -> dict[str, Any]:
    if task_id is None:
        return {"status": "not_applicable", "classification": None}
    key = cause_key(slug, task_id)
    try:
        raw, size_bytes, oversized = await _read_bounded_string(client, key, _MAX_CANCELLATION_BYTES)
        ttl = int(await client.ttl(key))
    except Exception:
        return {"status": "unavailable", "code": "cancellation_read_failed", "classification": None}
    if oversized:
        return {
            "status": "oversized",
            "code": "cancellation_size_limit",
            "classification": None,
            "source_size_bytes": size_bytes,
            "read_bound_bytes": _MAX_CANCELLATION_BYTES,
            **_ttl_fields(ttl),
        }
    if raw is None:
        return {"status": "missing", "code": "cancellation_missing", "classification": None}
    try:
        decoded = json.loads(_decode(raw))
        if not isinstance(decoded, dict):
            raise ValueError
        stored_task = _task_id(decoded.get("task_id"))
        stored_repo = decoded.get("repo_slug")
        category = decoded.get("category")
        payload = decoded.get("payload")
        created_at = _timestamp_text(decoded.get("created_at"))
        if stored_task != task_id or stored_repo != slug or created_at is None:
            raise ValueError
        if not isinstance(payload, dict):
            payload = {}
    except Exception:
        return {
            "status": "malformed",
            "code": "cancellation_invalid",
            "classification": None,
            "source_size_bytes": size_bytes,
            **_ttl_fields(ttl),
        }
    return {
        "status": "available",
        "code": None,
        "classification": {
            "category": (category if isinstance(category, str) and category in _KNOWN_CATEGORIES else "unclassified"),
            "subsource": _subsource(payload.get("subsource")) or "unclassified",
            "task_id": stored_task,
            "created_at": created_at,
        },
        "source_size_bytes": size_bytes,
        "read_bound_bytes": _MAX_CANCELLATION_BYTES,
        **_ttl_fields(ttl),
    }


def _retry_metadata(command: RetryCommand, ttl: int) -> dict[str, Any] | None:
    command_id = _uuid(command.command_id)
    task_id = _task_id(command.task_id)
    if command_id is None or task_id is None:
        return None
    return {
        "command_id": command_id,
        "task_id": task_id,
        "status": command.status.value,
        "requested_at": _timestamp_text(command.requested_at),
        "updated_at": _timestamp_text(command.updated_at),
        "failure_subsource": _subsource(command.failure_subsource)
        or ("unclassified" if command.failure_subsource is not None else None),
        "bound_pr_number": _positive_number(command.bound_pr_number),
        "bound_pr_head_sha": _sha(command.bound_pr_head_sha),
        "retry_count": _positive_int(command.retry_count),
        "retry_cap": _positive_number(command.retry_cap),
        "processing_attempts": _positive_int(command.processing_attempts),
        "effect_stage": command.effect_stage.value,
        "execution_state": command.execution_state.value,
        **_ttl_fields(ttl),
    }


async def _pending_retries(client: Any, slug: str, limit: int) -> dict[str, Any]:
    index_key = retry_command_pending(slug)
    try:
        page = await client.eval_ro(
            _BOUNDED_RETRY_INDEX_SCRIPT,
            1,
            index_key,
            limit,
            _MAX_INDEX_MEMBER_BYTES,
        )
        if not isinstance(page, (list, tuple)) or len(page) != 2:
            raise ValueError
        total = int(page[0])
        flat_rows = list(page[1])
        if total < 0 or len(flat_rows) % 3:
            raise ValueError
    except Exception:
        return {
            "status": "unavailable",
            "code": "retry_index_read_failed",
            "records": [],
            "record_count": None,
            "scanned_index_entries": 0,
            "truncated": False,
        }

    records: list[dict[str, Any]] = []
    invalid_records = 0
    for offset in range(0, len(flat_rows), 3):
        try:
            member_size = int(flat_rows[offset])
            member = flat_rows[offset + 1]
            score = float(flat_rows[offset + 2])
            if member_size < 0 or not math.isfinite(score):
                raise ValueError
        except (TypeError, ValueError):
            invalid_records += 1
            continue
        if member_size > _MAX_INDEX_MEMBER_BYTES:
            records.append(
                {
                    "status": "oversized_index_member",
                    "source_size_bytes": member_size,
                    "read_bound_bytes": _MAX_INDEX_MEMBER_BYTES,
                }
            )
            continue
        try:
            command_id = _uuid(_decode(member))
        except (TypeError, UnicodeDecodeError):
            command_id = None
        if command_id is None:
            invalid_records += 1
            continue
        key = retry_command(slug, command_id)
        try:
            raw, size_bytes, oversized = await _read_bounded_string(client, key, _MAX_RETRY_BYTES)
            ttl = int(await client.ttl(key))
        except Exception:
            records.append({"status": "unavailable", "code": "retry_record_read_failed", "command_id": command_id})
            continue
        if oversized:
            records.append(
                {
                    "status": "oversized",
                    "code": "retry_record_size_limit",
                    "command_id": command_id,
                    "source_size_bytes": size_bytes,
                    "read_bound_bytes": _MAX_RETRY_BYTES,
                    **_ttl_fields(ttl),
                }
            )
            continue
        if raw is None:
            records.append({"status": "missing", "code": "retry_record_missing", "command_id": command_id})
            continue
        try:
            command = RetryCommand.model_validate_json(_decode(raw))
        except Exception:
            records.append(
                {
                    "status": "malformed",
                    "code": "retry_record_invalid",
                    "command_id": command_id,
                    "source_size_bytes": size_bytes,
                    **_ttl_fields(ttl),
                }
            )
            continue
        metadata = _retry_metadata(command, ttl)
        if metadata is None or metadata["command_id"] != command_id or command.repo_slug != slug:
            records.append(
                {
                    "status": "malformed",
                    "code": "retry_record_identity_mismatch",
                    "command_id": command_id,
                    "source_size_bytes": size_bytes,
                    **_ttl_fields(ttl),
                }
            )
            continue
        records.append({"status": "available", "metadata": metadata, "source_size_bytes": size_bytes})
    return {
        "status": "available",
        "code": None,
        "records": records,
        "record_count": total,
        "scanned_index_entries": len(flat_rows) // 3,
        "invalid_records_omitted": invalid_records,
        "limit": limit,
        "truncated": total > len(flat_rows) // 3,
        "read_only": True,
    }


def _run_metadata(raw: object, expected_id: str, slug: str) -> dict[str, Any] | None:
    try:
        decoded = json.loads(_decode(raw))
        if not isinstance(decoded, dict):
            raise ValueError
        record = RunRecord(**decoded)
    except Exception:
        return None
    run_id = _uuid(record.run_id)
    task_id = _task_id(record.task_id)
    if run_id != expected_id or task_id is None or record.repo_name != slug:
        return None
    started_at = _timestamp_text(record.started_at)
    ended_at = _timestamp_text(record.ended_at) if record.ended_at is not None else None
    if started_at is None or (record.ended_at is not None and ended_at is None):
        return None
    payload = asdict(record)
    if decoded.get("ended_at") is None:
        payload["outcome"] = decoded.get("outcome", "")
        payload["cause"] = decoded.get("cause")
    unfinished = ended_at is None
    cause_subsource = _subsource(payload["cause_subsource"])
    return {
        "run_id": run_id,
        "task_id": task_id,
        "started_at": started_at,
        "ended_at": ended_at,
        "duration_ms": _positive_int(payload["duration_ms"]),
        "phase": payload["run_phase"],
        "attempt_index": payload["attempt_index"],
        "fix_iterations": _positive_int(payload["fix_iterations"]),
        "outcome": "in_progress" if unfinished else payload["outcome"],
        "cause": None if unfinished else payload["cause"],
        "cause_subsource": (
            None
            if unfinished
            else cause_subsource or ("unclassified" if payload["cause_subsource"] is not None else None)
        ),
        "base_sha": _sha(payload["base_sha"]),
        "head_sha": _sha(payload["head_sha"]),
    }


async def _run_records(
    client: Any,
    slug: str,
    task_filter: str | None,
    limit: int,
) -> dict[str, Any]:
    key = MetricsStore._recent_key(task_filter or "PR", slug)
    try:
        page = await client.eval_ro(
            _BOUNDED_RUN_INDEX_SCRIPT,
            1,
            key,
            _MAX_RUN_INDEX_ENTRIES,
            _MAX_INDEX_MEMBER_BYTES,
        )
        if not isinstance(page, (list, tuple)) or len(page) != 2:
            raise ValueError
        total = int(page[0])
        flat_rows = list(page[1])
        if total < 0 or len(flat_rows) % 2:
            raise ValueError
    except Exception:
        return {
            "status": "unavailable",
            "code": "run_index_read_failed",
            "task_filter": task_filter,
            "records": [],
            "record_count": None,
            "scanned_index_entries": 0,
            "truncated": False,
        }

    records: list[dict[str, Any]] = []
    invalid_records = 0
    scanned = 0
    for offset in range(0, len(flat_rows), 2):
        if len(records) >= limit:
            break
        scanned += 1
        try:
            member_size = int(flat_rows[offset])
            member = flat_rows[offset + 1]
            if member_size < 0:
                raise ValueError
        except (TypeError, ValueError):
            invalid_records += 1
            continue
        if member_size > _MAX_INDEX_MEMBER_BYTES:
            records.append(
                {
                    "status": "oversized_index_member",
                    "source_size_bytes": member_size,
                    "read_bound_bytes": _MAX_INDEX_MEMBER_BYTES,
                }
            )
            continue
        try:
            run_id = _uuid(_decode(member))
        except (TypeError, UnicodeDecodeError):
            run_id = None
        if run_id is None:
            invalid_records += 1
            continue
        try:
            raw, size_bytes, oversized = await _read_bounded_string(
                client, MetricsStore._record_key(run_id), _MAX_RUN_BYTES
            )
        except Exception:
            records.append({"status": "unavailable", "code": "run_record_read_failed", "run_id": run_id})
            continue
        if oversized:
            records.append(
                {
                    "status": "oversized",
                    "code": "run_record_size_limit",
                    "run_id": run_id,
                    "source_size_bytes": size_bytes,
                    "read_bound_bytes": _MAX_RUN_BYTES,
                }
            )
            continue
        if raw is None:
            records.append({"status": "missing", "code": "run_record_missing", "run_id": run_id})
            continue
        metadata = _run_metadata(raw, run_id, slug)
        if metadata is None:
            records.append(
                {
                    "status": "malformed",
                    "code": "run_record_invalid",
                    "run_id": run_id,
                    "source_size_bytes": size_bytes,
                }
            )
            continue
        if task_filter is not None and metadata["task_id"] != task_filter:
            continue
        records.append({"status": "available", "metadata": metadata, "source_size_bytes": size_bytes})
    return {
        "status": "available",
        "code": None,
        "task_filter": task_filter,
        "records": records,
        "record_count": total,
        "scanned_index_entries": scanned,
        "scan_limit": _MAX_RUN_INDEX_ENTRIES,
        "invalid_records_omitted": invalid_records,
        "limit": limit,
        "truncated": total > scanned,
        "read_only": True,
    }


def _source_summary(statuses: list[str]) -> str:
    failed = sum(status == "unavailable" for status in statuses)
    if failed == 0:
        return "available"
    if failed == len(statuses):
        return "unavailable"
    return "partially_available"


@mcp.tool()
async def get_orchestrator_status(
    repo_slug: str | None = None,
    retry_limit: int = 5,
    run_limit: int = 5,
) -> dict[str, Any]:
    """Return allowlisted runtime status without mutating orchestrator state.

    Omit ``repo_slug`` for a compact configured-repository overview.  Supply a
    configured ``owner__repo`` slug for bounded Retry, cancellation, inhibitor,
    and run-record metadata.  Snapshot freshness is never coder liveness.
    """
    retry_limit = _validate_limit(retry_limit, name="retry_limit", maximum=_MAX_RETRIES)
    run_limit = _validate_limit(run_limit, name="run_limit", maximum=_MAX_RUNS)
    observed_at = _utc_now()
    try:
        config, repositories = _configured_repositories()
    except Exception:
        return {
            "schema_version": 1,
            "observed_at": _iso_z(observed_at),
            "configuration": {"status": "unavailable", "code": "configuration_invalid"},
            "redis": {"status": "not_checked", "code": None},
            "repositories": [],
            "detail": None,
        }
    if repo_slug is not None:
        if not isinstance(repo_slug, str) or not _REPO_SLUG.fullmatch(repo_slug):
            raise ValueError("repo_slug must be a canonical owner__repo slug")
        if repo_slug not in repositories:
            raise ValueError("repo_slug is not configured")

    client: Any | None = None
    try:
        try:
            client = _new_redis_client()
        except Exception:
            snapshots = {
                slug: (_snapshot_unavailable("unavailable", "redis_connection_failed"), None) for slug in repositories
            }
            redis_status = "unavailable"
            redis_code = "redis_connection_failed"
        else:
            snapshots = {}
            for slug, repo in repositories.items():
                snapshots[slug] = await _read_snapshot(client, slug, repo, config, observed_at)
            snapshot_statuses = [snapshot[0]["status"] for snapshot in snapshots.values()]
            redis_status = _source_summary(snapshot_statuses) if snapshot_statuses else "available"
            redis_code = "redis_read_failed" if redis_status != "available" else None

        overviews = [_overview(slug, repo, config, *snapshots[slug]) for slug, repo in repositories.items()]
        detail = None
        if repo_slug is not None:
            snapshot, state = snapshots[repo_slug]
            current_task_id = (
                _task_id(state.current_task.pr_id) if state is not None and state.current_task is not None else None
            )
            if client is None:
                retries = {
                    "status": "unavailable",
                    "code": "redis_connection_failed",
                    "records": [],
                    "record_count": None,
                    "scanned_index_entries": 0,
                    "truncated": False,
                }
                runs = {
                    "status": "unavailable",
                    "code": "redis_connection_failed",
                    "task_filter": current_task_id,
                    "records": [],
                    "record_count": None,
                    "scanned_index_entries": 0,
                    "truncated": False,
                }
                cancellation = {
                    "status": "unavailable",
                    "code": "redis_connection_failed",
                    "classification": None,
                }
            else:
                retries = await _pending_retries(client, repo_slug, retry_limit)
                runs = await _run_records(client, repo_slug, current_task_id, run_limit)
                cancellation = await _current_cancellation(client, repo_slug, current_task_id)
                source_statuses = [
                    *(item[0]["status"] for item in snapshots.values()),
                    retries["status"],
                    runs["status"],
                    cancellation["status"],
                ]
                redis_status = _source_summary(source_statuses)
                redis_code = "redis_read_failed" if redis_status != "available" else None
            detail = {
                "repo_slug": repo_slug,
                "pipeline": _pipeline_view(state),
                "snapshot": snapshot,
                "inhibitors": _inhibitors(state, observed_at),
                "current_cancellation": cancellation,
                "pending_retries": retries,
                "run_records": runs,
                "activity_interpretation": {
                    "coder_activity": "unknown",
                    "snapshot_freshness_is_progress": False,
                    "note_code": "no_process_liveness_source",
                },
            }

        return {
            "schema_version": 1,
            "observed_at": _iso_z(observed_at),
            "configuration": {
                "status": "available",
                "code": None,
                "repository_count": len(repositories),
            },
            "redis": {"status": redis_status, "code": redis_code},
            "repositories": overviews,
            "detail": detail,
        }
    finally:
        await _close_redis(client)
