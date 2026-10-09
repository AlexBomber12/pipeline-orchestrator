from __future__ import annotations

import asyncio
import json
import sys
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from src.coder_registry import ModelMetadata, ModelReasoningEffort
from src.coders import claude_models
from src.coders.claude_models import (
    ClaudeModelDiscoveryInvalid,
    ClaudeModelDiscoveryUnavailable,
    _exchange,
    discover_claude_models,
)
from src.process_supervisor import ProcessLaunchCleanupError

_FAKE_CLI = r'''
import json, os, select, subprocess, sys, time
from pathlib import Path

record_path = Path(os.environ["RECORD_PATH"])
mode = os.environ.get("FAKE_MODE", "success")
line = sys.stdin.readline()
record = {
    "argv": sys.argv[1:],
    "auth_marker": os.environ.get("AUTH_MARKER"),
    "cwd": os.getcwd(),
    "pid": os.getpid(),
    "request": json.loads(line) if line else None,
}
def save():
    record_path.write_text(json.dumps(record), encoding="utf-8")

if mode == "premature-exit":
    save()
    raise SystemExit(17)
if mode == "hang":
    child = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"])
    record["child_pid"] = child.pid
    save()
    time.sleep(60)
if mode == "oversize":
    save()
    sys.stdout.write("x" * int(os.environ["OUTPUT_SIZE"]) + "\n")
    sys.stdout.flush()
    time.sleep(60)

ready, _, _ = select.select([sys.stdin], [], [], 0.05)
record["extra_before_response"] = sys.stdin.readline() if ready else None
save()
request_id = record["request"].get("request_id")
response = {
    "subtype": "success",
    "request_id": request_id,
    "response": {
        "models": json.loads(os.environ["CATALOG_JSON"]),
        "account": {"email": "raw-account-secret"},
        "future": True,
    },
    "future": True,
}
if mode == "malformed-json":
    output = "not-json"
elif mode == "non-object":
    output = "[]"
elif mode == "wrong-id":
    response["request_id"] = "some-other-request"
    output = json.dumps({"type": "control_response", "response": response})
elif mode == "protocol-error":
    response = {"subtype": "error", "request_id": request_id,
                "error": "account=account-secret credential=credential-secret"}
    output = json.dumps({"type": "control_response", "response": response})
elif mode == "bad-envelope":
    output = json.dumps({"type": "control_response",
                         "response": "raw-initialization-secret"})
elif mode in {"bad-subtype", "bad-payload"}:
    response["subtype"] = "future" if mode == "bad-subtype" else "success"
    response["response"] = "raw-initialization-secret"
    output = json.dumps({"type": "control_response", "response": response})
else:
    if mode == "notice-first":
        print(json.dumps({"type": "system", "account": "raw-account-secret"}))
    output = json.dumps({"type": "control_response", "response": response,
                         "future": True})
print(output, flush=True)
sys.stdin.read()
'''


def _fake_environment(
    tmp_path: Path,
    *,
    mode: str = "success",
    catalog: object = (),
) -> tuple[dict[str, str], Path, Path]:
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    executable = bin_dir / "claude"
    executable.write_text(
        f"#!{sys.executable}\n{_FAKE_CLI}", encoding="utf-8"
    )
    executable.chmod(0o755)
    record_path = tmp_path / "record.json"
    working_directory = tmp_path / "working"
    working_directory.mkdir()
    return (
        {
            "PATH": str(bin_dir),
            "FAKE_MODE": mode,
            "RECORD_PATH": str(record_path),
            "CATALOG_JSON": json.dumps(catalog),
            "AUTH_MARKER": "credential-secret",
            "OUTPUT_SIZE": "4096",
        },
        record_path,
        working_directory,
    )


async def _wait_for_record(record_path: Path) -> dict[str, Any]:
    for _ in range(200):
        if record_path.exists():
            return json.loads(record_path.read_text(encoding="utf-8"))
        await asyncio.sleep(0.01)
    raise AssertionError("fake CLI did not publish its process record")


async def _assert_processes_gone(record: dict[str, Any]) -> None:
    for _ in range(200):
        if all(
            not Path(f"/proc/{record[key]}").exists()
            for key in ("pid", "child_pid")
            if key in record
        ):
            return
        await asyncio.sleep(0.01)
    raise AssertionError("owned fake CLI process remained after discovery")


@pytest.mark.asyncio
async def test_discovers_normalized_models_without_writing_a_user_message(
    tmp_path: Path,
) -> None:
    catalog = [
        {
            "value": "sonnet",
            "resolvedModel": "claude-sonnet-versioned",
            "displayName": "Claude Sonnet (recommended)",
            "description": "Balanced",
            "supportsEffort": True,
            "supportedEffortLevels": ["low", "medium", "high"],
            "supportsAdaptiveThinking": True,
            "futureField": {"ignored": True},
        },
        {
            "value": "opusplan",
            "displayName": "Opus Plan",
            "description": "Plan with Opus",
        },
    ]
    env, record_path, cwd = _fake_environment(
        tmp_path, mode="notice-first", catalog=catalog
    )

    models = await discover_claude_models(env=env, cwd=cwd)

    assert models == (
        ModelMetadata(
            "sonnet",
            "Claude Sonnet (recommended)",
            reasoning_efforts=(
                ModelReasoningEffort("low"),
                ModelReasoningEffort("medium"),
                ModelReasoningEffort("high"),
            ),
        ),
        ModelMetadata("opusplan", "Opus Plan"),
    )
    record = await _wait_for_record(record_path)
    assert record["argv"] == [
        "--output-format",
        "stream-json",
        "--verbose",
        "--input-format",
        "stream-json",
    ]
    assert record["cwd"] == str(cwd)
    assert record["auth_marker"] == "credential-secret"
    assert record["extra_before_response"] is None
    assert record["request"] == {
        "type": "control_request",
        "request_id": "pipeline-orchestrator-model-discovery",
        "request": {"subtype": "initialize", "hooks": None},
    }
    await _assert_processes_gone(record)


@pytest.mark.asyncio
async def test_accepts_empty_catalog_and_missing_optional_effort_metadata(
    tmp_path: Path,
) -> None:
    env, record_path, cwd = _fake_environment(tmp_path, catalog=[])
    assert await discover_claude_models(env=env, cwd=cwd) == ()
    await _assert_processes_gone(await _wait_for_record(record_path))


@pytest.mark.parametrize(
    "catalog",
    [
        {},
        [42],
        [{"value": "sonnet"}],
        [
            {
                "value": "sonnet",
                "displayName": "Sonnet",
                "supportedEffortLevels": ["low", 42],
            }
        ],
        [
            {"value": "same", "displayName": "One"},
            {"value": "same", "displayName": "Two"},
        ],
    ],
)
@pytest.mark.asyncio
async def test_rejects_malformed_catalogs_through_real_transport(
    tmp_path: Path, catalog: object
) -> None:
    env, record_path, cwd = _fake_environment(tmp_path, catalog=catalog)
    with pytest.raises(ClaudeModelDiscoveryInvalid) as exc_info:
        await discover_claude_models(env=env, cwd=cwd)
    assert "credential-secret" not in str(exc_info.value)
    await _assert_processes_gone(await _wait_for_record(record_path))


@pytest.mark.parametrize(
    ("mode", "error_type", "match"),
    [
        ("malformed-json", ClaudeModelDiscoveryInvalid, "invalid"),
        ("non-object", ClaudeModelDiscoveryInvalid, "invalid"),
        ("wrong-id", ClaudeModelDiscoveryInvalid, "identifier"),
        ("bad-envelope", ClaudeModelDiscoveryInvalid, "invalid"),
        ("bad-subtype", ClaudeModelDiscoveryInvalid, "invalid"),
        ("bad-payload", ClaudeModelDiscoveryInvalid, "invalid"),
        ("protocol-error", ClaudeModelDiscoveryUnavailable, "support"),
        ("premature-exit", ClaudeModelDiscoveryUnavailable, "closed"),
    ],
)
@pytest.mark.asyncio
async def test_handles_protocol_and_early_exit_errors_without_raw_data(
    tmp_path: Path,
    mode: str,
    error_type: type[Exception],
    match: str,
) -> None:
    env, record_path, cwd = _fake_environment(tmp_path, mode=mode)
    with pytest.raises(error_type, match=match) as exc_info:
        await discover_claude_models(env=env, cwd=cwd)
    error = str(exc_info.value)
    assert "account-secret" not in error
    assert "credential-secret" not in error
    assert "raw-initialization-secret" not in error
    await _assert_processes_gone(await _wait_for_record(record_path))


@pytest.mark.asyncio
async def test_output_limit_is_explicit_and_cleans_up(tmp_path: Path) -> None:
    env, record_path, cwd = _fake_environment(tmp_path, mode="oversize")
    with pytest.raises(ClaudeModelDiscoveryInvalid, match="too large"):
        await discover_claude_models(
            env=env, cwd=cwd, max_output_bytes=128
        )
    await _assert_processes_gone(await _wait_for_record(record_path))


@pytest.mark.asyncio
async def test_timeout_cleans_up_the_owned_process_group(tmp_path: Path) -> None:
    env, record_path, cwd = _fake_environment(tmp_path, mode="hang")
    with pytest.raises(ClaudeModelDiscoveryUnavailable, match="timed out"):
        await discover_claude_models(env=env, cwd=cwd, timeout_seconds=1.0)
    record = await _wait_for_record(record_path)
    assert "child_pid" in record
    await _assert_processes_gone(record)


@pytest.mark.asyncio
async def test_cancellation_propagates_after_confirmed_cleanup(
    tmp_path: Path,
) -> None:
    env, record_path, cwd = _fake_environment(tmp_path, mode="hang")
    task = asyncio.create_task(
        discover_claude_models(env=env, cwd=cwd, timeout_seconds=60)
    )
    record = await _wait_for_record(record_path)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert "child_pid" in record
    await _assert_processes_gone(record)


@pytest.mark.asyncio
async def test_missing_cli_is_sanitized(tmp_path: Path) -> None:
    empty_path = tmp_path / "empty-bin"
    empty_path.mkdir()
    with pytest.raises(ClaudeModelDiscoveryUnavailable) as exc_info:
        await discover_claude_models(
            env={
                "PATH": str(empty_path),
                "ACCOUNT": "account-secret",
            },
            cwd=tmp_path,
        )
    assert str(exc_info.value) == "Claude model discovery is unavailable"


class _StubStdin:
    def __init__(self, *, broken: bool = False) -> None:
        self.broken = broken

    def write(self, _data: bytes) -> None:
        return None

    async def drain(self) -> None:
        if self.broken:
            raise BrokenPipeError("raw pipe detail")


class _StubStdout:
    def __init__(self, lines: list[bytes]) -> None:
        self.lines = lines

    async def readline(self) -> bytes:
        return self.lines.pop(0) if self.lines else b""


@pytest.mark.asyncio
async def test_exchange_handles_missing_pipes_and_broken_stdin() -> None:
    with pytest.raises(ClaudeModelDiscoveryUnavailable, match="unavailable"):
        await _exchange(SimpleNamespace(stdin=None, stdout=None), 100)
    process = SimpleNamespace(
        stdin=_StubStdin(broken=True), stdout=_StubStdout([])
    )
    with pytest.raises(ClaudeModelDiscoveryUnavailable, match="closed") as exc_info:
        await _exchange(process, 100)
    assert "raw pipe detail" not in str(exc_info.value)


@pytest.mark.asyncio
async def test_exchange_enforces_cumulative_output_limit() -> None:
    process = SimpleNamespace(
        stdin=_StubStdin(),
        stdout=_StubStdout([b'{"type":"system"}\n', b'{"type":"system"}\n']),
    )
    with pytest.raises(ClaudeModelDiscoveryInvalid, match="too large"):
        await _exchange(process, 30)


@pytest.mark.parametrize("cleanup_raises", [False, True])
@pytest.mark.asyncio
async def test_cleanup_failure_is_sanitized(
    monkeypatch: pytest.MonkeyPatch,
    cleanup_raises: bool,
) -> None:
    class _Closable:
        def close(self) -> None:
            return None

    class _Managed:
        process = SimpleNamespace(stdin=_Closable())

        async def cleanup(self, **_kwargs: object) -> SimpleNamespace:
            if cleanup_raises:
                raise RuntimeError("raw cleanup detail")
            return SimpleNamespace(quiescent=False)

    managed = _Managed()

    async def launch(*_args: object, **_kwargs: object) -> _Managed:
        return managed

    async def exchange(*_args: object) -> tuple[ModelMetadata, ...]:
        return ()

    monkeypatch.setattr(claude_models, "launch_process", launch)
    monkeypatch.setattr(claude_models, "_exchange", exchange)
    with pytest.raises(
        ClaudeModelDiscoveryUnavailable, match="cleanup could not be confirmed"
    ) as exc_info:
        await discover_claude_models()
    assert "raw cleanup detail" not in str(exc_info.value)
    assert exc_info.value.managed is managed
    assert (exc_info.value.cleanup_result is None) is cleanup_raises


@pytest.mark.asyncio
async def test_startup_cleanup_failure_retains_ownership(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    managed = SimpleNamespace(reconcile_cleanup=object())
    cleanup = SimpleNamespace(quiescent=False)

    async def launch(*_args: object, **_kwargs: object) -> None:
        raise ProcessLaunchCleanupError(
            "raw startup detail",
            managed=managed,  # type: ignore[arg-type]
            cleanup_result=cleanup,  # type: ignore[arg-type]
        )

    monkeypatch.setattr(claude_models, "launch_process", launch)
    with pytest.raises(
        ClaudeModelDiscoveryUnavailable, match="startup cleanup"
    ) as exc_info:
        await discover_claude_models()
    assert "raw startup detail" not in str(exc_info.value)
    assert exc_info.value.managed is managed
    assert exc_info.value.cleanup_result is cleanup


@pytest.mark.parametrize(
    "kwargs",
    [
        {"timeout_seconds": 0},
        {"timeout_seconds": float("inf")},
        {"max_output_bytes": 0},
        {"max_output_bytes": True},
    ],
)
@pytest.mark.asyncio
async def test_requires_finite_positive_bounds(kwargs: dict[str, object]) -> None:
    with pytest.raises(ValueError, match="finite and positive"):
        await discover_claude_models(**kwargs)  # type: ignore[arg-type]
