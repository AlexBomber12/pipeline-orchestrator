"""Tests for the merged-PR outcome logger and schema."""

from __future__ import annotations

import json
import multiprocessing
import os
import threading
from pathlib import Path
from typing import Any

import pytest
from src.analytics import (
    OUTCOME_FIELDS,
    OUTCOME_SCHEMA_VERSION,
    log_merged_pr,
    validate_outcome_record,
)
from src.analytics.coder_version import detect_coder_extension_version
from src.analytics.outcome_logger import (
    _partition_path,
    _resolve_analytics_dir,
    _resolve_partition_when,
    compute_task_id_hash,
)
from src.analytics.schema import (
    OUTCOME_FIELD_TYPES,
    OutcomeValidationError,
)


def _full_record(**overrides: Any) -> dict:
    base: dict[str, Any] = {
        "pr_id": "PR-204",
        "task_id_hash": "",
        "repo_slug": "owner__repo",
        "merged_at": "2026-04-29T14:25:23+00:00",
        "coder": "claude",
        "coder_model_string": "claude-opus-4-7",
        "coder_extension_version": "1.2.3",
        "task_type": "feature",
        "task_complexity": "medium",
        "fix_iterations": 4,
        "ci_runs_total": 10,
        "ci_runs_failed": 8,
        "wall_clock_seconds": 14045,
        "files_changed": 12,
        "lines_added": 287,
        "lines_removed": 142,
        "review_blocker_count": 0,
        "review_nit_count": 2,
        "codex_review_iterations": 1,
        "tokens_estimate": 145000,
        "outcome": "merged",
    }
    base.update(overrides)
    return base


def test_schema_lists_all_eighteen_fields() -> None:
    # Schema is fixed at 21 keys (the 18 documented plus task_id_hash,
    # repo_slug, outcome that are also part of the record). Lock the
    # name set so a typo in a future PR can't silently rename a field.
    assert OUTCOME_SCHEMA_VERSION == 1
    assert set(OUTCOME_FIELDS) == set(OUTCOME_FIELD_TYPES)
    assert len(OUTCOME_FIELDS) == 21


def test_validate_accepts_canonical_record() -> None:
    validate_outcome_record(_full_record())


def test_validate_accepts_null_for_missing_data() -> None:
    record = _full_record(
        ci_runs_total=None,
        ci_runs_failed=None,
        review_blocker_count=None,
        review_nit_count=None,
        tokens_estimate=None,
        coder_extension_version=None,
    )
    validate_outcome_record(record)


def test_validate_rejects_non_dict() -> None:
    with pytest.raises(OutcomeValidationError, match="must be a dict"):
        validate_outcome_record("not a dict")  # type: ignore[arg-type]


def test_validate_rejects_missing_field() -> None:
    record = _full_record()
    record.pop("outcome")
    with pytest.raises(OutcomeValidationError, match="missing required fields"):
        validate_outcome_record(record)


def test_validate_rejects_unknown_field() -> None:
    record = _full_record()
    record["surprise_field"] = "boom"
    with pytest.raises(OutcomeValidationError, match="unknown fields"):
        validate_outcome_record(record)


def test_validate_rejects_wrong_type() -> None:
    record = _full_record(fix_iterations="four")
    with pytest.raises(OutcomeValidationError, match="type errors"):
        validate_outcome_record(record)


def test_validate_rejects_bool_for_int_field() -> None:
    record = _full_record(fix_iterations=True)
    with pytest.raises(OutcomeValidationError, match="bool not allowed"):
        validate_outcome_record(record)


def test_compute_task_id_hash_is_deterministic() -> None:
    a = compute_task_id_hash("PR-204", "owner__repo")
    b = compute_task_id_hash("PR-204", "owner__repo")
    assert a == b
    # Different inputs must produce different hashes.
    assert a != compute_task_id_hash("PR-205", "owner__repo")
    assert a != compute_task_id_hash("PR-204", "owner__other")


def test_log_merged_pr_creates_file_with_one_record(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("PO_ANALYTICS_DIR", str(tmp_path / "analytics"))
    log_merged_pr(_full_record())
    target = tmp_path / "analytics" / "2026-04.jsonl"
    assert target.exists()
    lines = target.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 1
    parsed = json.loads(lines[0])
    # Recomputed hash must match the canonical formula.
    assert parsed["task_id_hash"] == compute_task_id_hash(
        "PR-204", "owner__repo"
    )
    # Schema row carries every documented field.
    assert set(parsed) == set(OUTCOME_FIELDS)


def test_log_merged_pr_appends_without_overwriting(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("PO_ANALYTICS_DIR", str(tmp_path / "analytics"))
    log_merged_pr(_full_record(pr_id="PR-204"))
    log_merged_pr(_full_record(pr_id="PR-205"))
    target = tmp_path / "analytics" / "2026-04.jsonl"
    lines = target.read_text(encoding="utf-8").splitlines()
    assert [json.loads(line)["pr_id"] for line in lines] == ["PR-204", "PR-205"]


def test_log_merged_pr_month_rollover_preserves_old_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("PO_ANALYTICS_DIR", str(tmp_path / "analytics"))
    log_merged_pr(_full_record(merged_at="2026-04-30T23:59:59+00:00"))
    log_merged_pr(_full_record(merged_at="2026-05-01T00:00:00+00:00"))
    april = tmp_path / "analytics" / "2026-04.jsonl"
    may = tmp_path / "analytics" / "2026-05.jsonl"
    assert april.exists() and may.exists()
    assert len(april.read_text().splitlines()) == 1
    assert len(may.read_text().splitlines()) == 1


def test_log_merged_pr_writes_null_not_omitted(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("PO_ANALYTICS_DIR", str(tmp_path / "analytics"))
    log_merged_pr(
        _full_record(
            ci_runs_total=None,
            ci_runs_failed=None,
            tokens_estimate=None,
            review_blocker_count=None,
            review_nit_count=None,
            coder_extension_version=None,
        )
    )
    target = tmp_path / "analytics" / "2026-04.jsonl"
    parsed = json.loads(target.read_text())
    # ``json`` writes None as null; the keys must still be present.
    for key in (
        "ci_runs_total",
        "ci_runs_failed",
        "tokens_estimate",
        "review_blocker_count",
        "review_nit_count",
        "coder_extension_version",
    ):
        assert key in parsed
        assert parsed[key] is None


def test_log_merged_pr_falls_back_to_now_for_missing_merged_at(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Defensive: a malformed merged_at falls back to ``now()`` partitioning."""
    monkeypatch.setenv("PO_ANALYTICS_DIR", str(tmp_path / "analytics"))
    record = _full_record()
    # Bypass validation by writing directly to ``_resolve_partition_when``;
    # the production path validates first so a missing ``merged_at`` would
    # raise. Here we just assert the helper's fallback shape.
    assert _resolve_partition_when({}).year >= 2026
    assert _resolve_partition_when({"merged_at": "garbage"}).year >= 2026
    # And the canonical path uses the field when present.
    when = _resolve_partition_when(record)
    assert (when.year, when.month) == (2026, 4)


def test_resolve_analytics_dir_uses_env_then_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("PO_ANALYTICS_DIR", "/tmp/custom-analytics")
    assert _resolve_analytics_dir() == Path("/tmp/custom-analytics")
    monkeypatch.delenv("PO_ANALYTICS_DIR", raising=False)
    assert _resolve_analytics_dir() == Path("/data/analytics")


def test_partition_path_zero_pads_month() -> None:
    from datetime import datetime, timezone
    when = datetime(2026, 3, 1, tzinfo=timezone.utc)
    assert _partition_path(when, Path("/x")) == Path("/x/2026-03.jsonl")


def test_log_merged_pr_validates_before_writing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("PO_ANALYTICS_DIR", str(tmp_path / "analytics"))
    bad = _full_record()
    bad.pop("outcome")
    with pytest.raises(OutcomeValidationError):
        log_merged_pr(bad)
    # File must not be created if validation failed.
    assert not (tmp_path / "analytics" / "2026-04.jsonl").exists()


def _append_in_thread(target_dir: str, pr_id: str) -> None:
    os.environ["PO_ANALYTICS_DIR"] = target_dir
    log_merged_pr(_full_record(pr_id=pr_id))


def test_concurrent_appends_do_not_interleave_bytes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """flock ensures every JSON line is well-formed under contention."""
    target_dir = tmp_path / "analytics"
    monkeypatch.setenv("PO_ANALYTICS_DIR", str(target_dir))

    threads = [
        threading.Thread(
            target=_append_in_thread,
            args=(str(target_dir), f"PR-{i:04d}"),
        )
        for i in range(40)
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    target = target_dir / "2026-04.jsonl"
    lines = target.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 40
    # Every line must parse; partial-line interleaving would break json.loads.
    parsed_ids = {json.loads(line)["pr_id"] for line in lines}
    assert parsed_ids == {f"PR-{i:04d}" for i in range(40)}


def _append_in_process(target_dir: str, pr_id: str) -> None:
    os.environ["PO_ANALYTICS_DIR"] = target_dir
    log_merged_pr(_full_record(pr_id=pr_id))


def test_concurrent_appends_across_processes_use_flock(
    tmp_path: Path,
) -> None:
    """Cross-process flock: matches the multi-daemon deployment shape."""
    target_dir = tmp_path / "analytics"
    ctx = multiprocessing.get_context("fork")
    procs = [
        ctx.Process(
            target=_append_in_process,
            args=(str(target_dir), f"PR-{i:04d}"),
        )
        for i in range(8)
    ]
    for p in procs:
        p.start()
    for p in procs:
        p.join()
    for p in procs:
        assert p.exitcode == 0

    target = target_dir / "2026-04.jsonl"
    lines = target.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 8
    parsed_ids = {json.loads(line)["pr_id"] for line in lines}
    assert parsed_ids == {f"PR-{i:04d}" for i in range(8)}


def test_detect_coder_extension_version_returns_none_for_unknown_coder() -> None:
    assert detect_coder_extension_version("nobody") is None
    assert detect_coder_extension_version("") is None


def test_detect_coder_extension_version_returns_none_when_npm_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def boom(*args: Any, **kwargs: Any) -> None:
        raise FileNotFoundError("no npm")

    monkeypatch.setattr(
        "src.analytics.coder_version.subprocess.run", boom
    )
    assert detect_coder_extension_version("claude") is None


def test_detect_coder_extension_version_handles_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import subprocess as sp

    def slow(*args: Any, **kwargs: Any) -> None:
        raise sp.TimeoutExpired(cmd=["npm"], timeout=5)

    monkeypatch.setattr(
        "src.analytics.coder_version.subprocess.run", slow
    )
    assert detect_coder_extension_version("codex") is None


def test_detect_coder_extension_version_handles_oserror(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fail(*args: Any, **kwargs: Any) -> None:
        raise OSError("boom")

    monkeypatch.setattr(
        "src.analytics.coder_version.subprocess.run", fail
    )
    assert detect_coder_extension_version("claude") is None


def _fake_completed(stdout: str, returncode: int = 0) -> Any:
    class _CP:
        pass

    cp = _CP()
    cp.stdout = stdout
    cp.returncode = returncode
    return cp


def test_detect_coder_extension_version_parses_npm_json(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payload = json.dumps(
        {
            "dependencies": {
                "@anthropic-ai/claude-code": {"version": "1.2.3"}
            }
        }
    )
    monkeypatch.setattr(
        "src.analytics.coder_version.subprocess.run",
        lambda *a, **kw: _fake_completed(payload),
    )
    assert detect_coder_extension_version("claude") == "1.2.3"


def test_detect_coder_extension_version_returns_none_for_empty_stdout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "src.analytics.coder_version.subprocess.run",
        lambda *a, **kw: _fake_completed(""),
    )
    assert detect_coder_extension_version("claude") is None


def test_detect_coder_extension_version_returns_none_for_invalid_json(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "src.analytics.coder_version.subprocess.run",
        lambda *a, **kw: _fake_completed("not json"),
    )
    assert detect_coder_extension_version("claude") is None


def test_detect_coder_extension_version_returns_none_for_non_dict_payload(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "src.analytics.coder_version.subprocess.run",
        lambda *a, **kw: _fake_completed("[]"),
    )
    assert detect_coder_extension_version("claude") is None


def test_detect_coder_extension_version_returns_none_when_dependencies_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "src.analytics.coder_version.subprocess.run",
        lambda *a, **kw: _fake_completed(json.dumps({"name": "root"})),
    )
    assert detect_coder_extension_version("claude") is None


def test_detect_coder_extension_version_returns_none_when_entry_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "src.analytics.coder_version.subprocess.run",
        lambda *a, **kw: _fake_completed(
            json.dumps({"dependencies": {"other": {"version": "9.9.9"}}})
        ),
    )
    assert detect_coder_extension_version("claude") is None


def test_detect_coder_extension_version_returns_none_when_entry_not_dict(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "src.analytics.coder_version.subprocess.run",
        lambda *a, **kw: _fake_completed(
            json.dumps(
                {
                    "dependencies": {
                        "@anthropic-ai/claude-code": "1.2.3",
                    }
                }
            )
        ),
    )
    assert detect_coder_extension_version("claude") is None


def test_detect_coder_extension_version_returns_none_when_version_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "src.analytics.coder_version.subprocess.run",
        lambda *a, **kw: _fake_completed(
            json.dumps(
                {
                    "dependencies": {
                        "@anthropic-ai/claude-code": {"name": "x"},
                    }
                }
            )
        ),
    )
    assert detect_coder_extension_version("claude") is None
