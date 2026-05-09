"""Backfill legacy run metrics with outcome/cause fields.

PR-286 added normalized run-record fields for new writes. This startup
migration fills the fields that can be derived from legacy ``exit_reason``
values and rebuilds the per-task run index for existing Redis records.
"""

from __future__ import annotations

import inspect
import json
from collections.abc import AsyncIterator
from typing import Any

RUN_RECORD_TTL_SECONDS = 365 * 86400
NULL_CAUSE_VALUE = ""
_INT_FIELDS = {
    "attempt_index",
    "diff_lines_added",
    "diff_lines_deleted",
    "files_touched_count",
    "fix_iterations",
    "tokens_in",
    "tokens_out",
}
_OPTIONAL_INT_FIELDS = {"duration_ms"}
_BOOL_FIELDS = {"had_merge_conflict", "operator_intervention"}
_FLOAT_FIELDS = {"test_file_ratio"}
_LIST_FIELDS = {"languages_touched"}

_EXIT_REASON_TO_OUTCOME_CAUSE: dict[str, tuple[str, str]] = {
    "success_merged": ("merged", NULL_CAUSE_VALUE),
    "coding_complete": ("superseded", NULL_CAUSE_VALUE),
    "closed_unmerged": ("superseded", NULL_CAUSE_VALUE),
    "rate_limit": ("paused", NULL_CAUSE_VALUE),
    "crash": ("failed", "CRASH"),
    "timeout": ("failed", "TIMEOUT"),
    "error": ("failed", "CRASH"),
    "escalated": ("failed", "ESCALATE"),
    "paused": ("paused", NULL_CAUSE_VALUE),
    "stopped": ("paused", NULL_CAUSE_VALUE),
    "cancelled": ("paused", NULL_CAUSE_VALUE),
}


async def _maybe_await(value: Any) -> Any:
    if inspect.isawaitable(value):
        return await value
    return value


async def _iter_run_record_keys(redis_client: Any) -> AsyncIterator[str | bytes]:
    keys = redis_client.scan_iter(match="metrics:run:*")
    if hasattr(keys, "__aiter__"):
        async for key in keys:
            yield key
        return
    for key in keys:
        yield key


def _decode(value: Any) -> Any:
    if isinstance(value, bytes):
        return value.decode("utf-8")
    return value


def _normalize_hash(raw: Any) -> dict[str, Any]:
    if not isinstance(raw, dict):
        return {}
    record = {str(_decode(key)): _decode(value) for key, value in raw.items()}
    _coerce_legacy_hash_types(record)
    return record


def _coerce_legacy_hash_types(record: dict[str, Any]) -> None:
    for field in _INT_FIELDS:
        if field in record:
            record[field] = _coerce_int(record[field], optional=False)
    for field in _OPTIONAL_INT_FIELDS:
        if field in record:
            record[field] = _coerce_int(record[field], optional=True)
    for field in _BOOL_FIELDS:
        if field in record:
            record[field] = _coerce_bool(record[field])
    for field in _FLOAT_FIELDS:
        if field in record:
            record[field] = _coerce_float(record[field])
    for field in _LIST_FIELDS:
        if field in record:
            record[field] = _coerce_list(record[field])


def _coerce_int(value: Any, *, optional: bool) -> Any:
    if optional and (value is None or value == ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return value


def _coerce_float(value: Any) -> Any:
    try:
        return float(value)
    except (TypeError, ValueError):
        return value


def _coerce_bool(value: Any) -> Any:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"1", "true", "yes"}:
            return True
        if normalized in {"", "0", "false", "no"}:
            return False
    return value


def _coerce_list(value: Any) -> Any:
    if isinstance(value, list):
        return value
    if not isinstance(value, str):
        return value
    if not value:
        return []
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError:
        return [item.strip() for item in value.split(",") if item.strip()]
    return parsed if isinstance(parsed, list) else value


def _normalize_json_record(raw: Any) -> dict[str, Any]:
    decoded = _decode(raw)
    if not isinstance(decoded, str):
        return {}
    try:
        record = json.loads(decoded)
    except json.JSONDecodeError:
        return {}
    if not isinstance(record, dict):
        return {}
    return {str(key): value for key, value in record.items()}


def _apply_outcome_cause(record: dict[str, Any], outcome: str, cause: str) -> None:
    record["outcome"] = outcome
    record["cause"] = cause if outcome == "failed" else None


async def _key_type(redis_client: Any, key: str | bytes) -> str | None:
    type_method = getattr(redis_client, "type", None)
    if type_method is None:
        return None
    raw_type = await _maybe_await(type_method(key))
    decoded = _decode(raw_type)
    if decoded is None:
        return None
    return str(decoded)


async def _load_record(
    redis_client: Any,
    key: str | bytes,
    redis_type: str | None,
) -> tuple[dict[str, Any], str]:
    if redis_type == "string":
        raw_record = await _maybe_await(redis_client.get(key))
        return _normalize_json_record(raw_record), "string"

    raw_record = await _maybe_await(redis_client.hgetall(key))
    return _normalize_hash(raw_record), "hash"


async def _write_outcome_cause(
    redis_client: Any,
    key: str | bytes,
    record: dict[str, Any],
    storage: str,
    outcome: str,
    cause: str,
) -> None:
    _apply_outcome_cause(record, outcome, cause)
    if storage == "string":
        await _maybe_await(
            redis_client.set(
                key,
                json.dumps(record, sort_keys=True),
                ex=RUN_RECORD_TTL_SECONDS,
            )
        )
        return

    await _maybe_await(
        redis_client.hset(key, mapping={"outcome": outcome, "cause": cause})
    )


async def _write_canonical_record_key(
    redis_client: Any,
    record_id: str,
    record: dict[str, Any],
    repo_scope: str,
) -> None:
    if not record.get("repo_name"):
        record["repo_name"] = repo_scope
    canonical_key = f"metrics:run:{record_id}"
    await _maybe_await(
        redis_client.set(
            canonical_key,
            json.dumps(record, sort_keys=True),
            ex=RUN_RECORD_TTL_SECONDS,
        )
    )


async def _canonical_string_exists(redis_client: Any, record_id: str) -> bool:
    return await _key_type(redis_client, f"metrics:run:{record_id}") == "string"


def _extract_repo_and_record_id(key: str | bytes) -> tuple[str | None, str] | None:
    normalized = _decode(key)
    if not isinstance(normalized, str):
        return None
    parts = normalized.split(":", 3)
    if len(parts) not in {3, 4} or parts[0] != "metrics" or parts[1] != "run":
        return None
    if len(parts) == 3:
        record_id = parts[2]
        if not record_id:
            return None
        return None, record_id

    repo, record_id = parts[2], parts[3]
    if not repo or not record_id:
        return None
    return repo, record_id


def _warn(log: Any, message: str) -> None:
    if hasattr(log, "warning"):
        log.warning(message)
    else:
        log(message)


async def migrate_run_records_to_outcome_cause(
    redis_client: Any,
    log: Any,
) -> dict[str, int]:
    """Populate derivable PR-286 fields on legacy ``metrics:run:*`` records."""
    counts = {
        "records_scanned": 0,
        "records_migrated": 0,
        "records_skipped_already_migrated": 0,
        "records_skipped_non_hash": 0,
        "records_skipped_malformed": 0,
    }

    async for key in _iter_run_record_keys(redis_client):
        counts["records_scanned"] += 1
        parsed = _extract_repo_and_record_id(key)
        if parsed is None:
            counts["records_skipped_malformed"] += 1
            _warn(log, f"[MIGRATION] Skipping malformed run-record key {key}")
            continue
        repo, record_id = parsed

        redis_type = await _key_type(redis_client, key)
        if redis_type not in {None, "hash", "string"}:
            counts["records_skipped_non_hash"] += 1
            continue

        record, storage = await _load_record(redis_client, key, redis_type)
        if not record:
            counts["records_skipped_malformed"] += 1
            _warn(log, f"[MIGRATION] Skipping malformed run-record {key}")
            continue
        if "outcome" in record and storage == "string":
            counts["records_skipped_already_migrated"] += 1
            continue

        task_id = record.get("task_id")
        if not task_id:
            counts["records_skipped_malformed"] += 1
            _warn(log, f"[MIGRATION] Skipping run record {key}: missing task_id")
            continue
        repo_scope = repo or str(record.get("repo_name") or "global")

        if "outcome" in record:
            counts["records_skipped_already_migrated"] += 1
            if await _canonical_string_exists(redis_client, record_id):
                continue
        else:
            exit_reason = str(record.get("exit_reason") or "")
            mapped = _EXIT_REASON_TO_OUTCOME_CAUSE.get(exit_reason)
            if mapped is None:
                mapped = ("failed", "CRASH")
                _warn(
                    log,
                    "[MIGRATION] Unknown run-record exit_reason "
                    f"{exit_reason!r} for {key}; defaulting to failed",
                )
            outcome, cause = mapped
            await _write_outcome_cause(
                redis_client,
                key,
                record,
                storage,
                outcome,
                cause,
            )
            counts["records_migrated"] += 1

        await _write_canonical_record_key(
            redis_client,
            record_id,
            record,
            repo_scope,
        )
        task_runs_key = f"metrics:task_runs:{repo_scope}:{task_id}"
        await _maybe_await(redis_client.sadd(task_runs_key, record_id))
        await _maybe_await(
            redis_client.expire(task_runs_key, RUN_RECORD_TTL_SECONDS)
        )
        await _maybe_await(redis_client.expire(key, RUN_RECORD_TTL_SECONDS))

    return counts
