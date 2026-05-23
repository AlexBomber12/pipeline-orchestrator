from __future__ import annotations

import asyncio

from src.config import FeatureFlags
from src.subsource_registry import SuppressionReason

from tests.runner._helpers import _make_runner


def _runner():
    return _make_runner(
        feature_flags=FeatureFlags(
            use_unified_inhibitor_check=False,
            use_single_error_exit=True,
        )
    )


async def test_quarantine_becomes_guardrail_suppression() -> None:
    runner = _runner()

    await runner._suppress_task(
        "PR-382",
        SuppressionReason.GUARDRAIL,
        {"pr_number": 382},
    )

    record = await runner._suppression_record_for_task("PR-382")
    assert record is not None
    assert record.reason == SuppressionReason.GUARDRAIL
    assert runner._task_suppression_blocks_selection(record.reason) is True


async def test_stopped_becomes_operator_stopped_suppression() -> None:
    runner = _runner()

    await runner._suppress_task(
        "PR-382",
        SuppressionReason.OPERATOR_STOPPED,
        {"source": "operator_stop"},
    )

    record = await runner._suppression_record_for_task("PR-382")
    assert record is not None
    assert record.reason == SuppressionReason.OPERATOR_STOPPED
    assert runner._task_suppression_blocks_selection(record.reason) is True


async def test_status_write_failed_key_is_not_required_for_suppression() -> None:
    runner = _runner()

    await runner._suppress_task("PR-382", SuppressionReason.CRASH, {})

    assert "status_write_failed_tasks:" + runner.name not in runner.redis.store


async def test_counters_become_detail() -> None:
    runner = _runner()

    await runner._set_suppression_detail_count(
        "PR-382",
        SuppressionReason.DIAGNOSE_EXHAUSTED,
        "diagnose_attempts",
        4,
    )

    record = await runner._suppression_record_for_task("PR-382")
    assert record is not None
    assert record.detail["diagnose_attempts"] == 4
    assert await runner._suppression_detail_count("PR-382", "diagnose_attempts") == 4


def test_recovery_boot_no_status_write_failed_hydrate() -> None:
    runner = _runner()

    assert hasattr(runner, "_hydrate_status_write_failed_task_pr_ids")
    assert "status_write_failed_tasks:" + runner.name not in runner.redis.store


def test_all_eight_mechanisms_gone_from_store_writes() -> None:
    runner = _runner()

    asyncio.run(runner._suppress_task("PR-382", SuppressionReason.GUARDRAIL, {}))

    assert set(runner.redis.store) == {"cancellation:octo__demo:PR-382"}
