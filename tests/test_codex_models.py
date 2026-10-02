from __future__ import annotations

import asyncio
import json
from typing import Any

import pytest
from src.coders import codex_models
from src.coders.codex_models import (
    CodexModel,
    CodexModelDiscoveryInvalid,
    CodexModelDiscoveryUnavailable,
    CodexReasoningEffort,
    discover_codex_models,
)


def _message(request_id: int, result: object) -> dict[str, object]:
    return {"id": request_id, "result": result}


class _FakeStdin:
    def __init__(self) -> None:
        self.messages: list[dict[str, Any]] = []
        self.closed = False
    def write(self, data: bytes) -> None:
        self.messages.append(json.loads(data))
    async def drain(self) -> None:
        return None
    def close(self) -> None:
        self.closed = True


class _FakeStdout:
    def __init__(self, responses: list[object], *, block: bool = False) -> None:
        self.responses = responses
        self.block = block
        self.started = asyncio.Event()
    async def readline(self) -> bytes:
        self.started.set()
        if self.responses:
            response = self.responses.pop(0)
            if isinstance(response, Exception):
                raise response
            if isinstance(response, bytes):
                return response
            return json.dumps(response).encode() + b"\n"
        if self.block:
            await asyncio.Event().wait()
        return b""


class _FakeProcess:
    def __init__(self, responses: list[object], *, block: bool = False) -> None:
        self.stdin = _FakeStdin()
        self.stdout = _FakeStdout(responses, block=block)
        self.killed = False
        self.reaped = False
    def kill(self) -> None:
        self.killed = True
    async def wait(self) -> int:
        self.reaped = True
        return -9 if self.killed else 0


def _install_process(
    monkeypatch: pytest.MonkeyPatch,
    responses: list[object],
    *,
    block: bool = False,
) -> tuple[_FakeProcess, list[tuple[tuple[object, ...], dict[str, object]]]]:
    process = _FakeProcess(responses, block=block)
    calls: list[tuple[tuple[object, ...], dict[str, object]]] = []
    async def create(*args: object, **kwargs: object) -> _FakeProcess:
        calls.append((args, kwargs))
        return process
    monkeypatch.setattr(codex_models.asyncio, "create_subprocess_exec", create)
    return process, calls


@pytest.mark.asyncio
async def test_discovers_normalized_models_without_starting_work(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    responses = [
        {"method": "account/updated", "params": {}},
        _message(1, {"userAgent": "codex"}),
        _message(
            2,
            {
                "data": [
                    {
                        "model": "  gpt-one  ",
                        "displayName": "GPT One",
                        "isDefault": True,
                        "defaultReasoningEffort": "medium",
                        "supportedReasoningEfforts": [
                            {"reasoningEffort": " low ", "description": "Fast"},
                            {"reasoningEffort": "medium"},
                        ],
                    },
                    {"model": "gpt-two", "displayName": "GPT Two"},
                ],
                "nextCursor": None,
            },
        ),
    ]
    process, calls = _install_process(monkeypatch, responses)
    catalog = await discover_codex_models()
    assert catalog == (
        CodexModel(
            "gpt-one",
            "GPT One",
            True,
            "medium",
            (
                CodexReasoningEffort("low", "Fast"),
                CodexReasoningEffort("medium", None),
            ),
        ),
        CodexModel("gpt-two", "GPT Two", False, None, ()),
    )
    assert calls[0][0] == ("codex", "app-server")
    assert calls[0][1]["stderr"] is asyncio.subprocess.DEVNULL
    assert [message["method"] for message in process.stdin.messages] == [
        "initialize",
        "initialized",
        "model/list",
    ]
    assert process.stdin.messages[-1]["params"] == {
        "limit": 50,
        "includeHidden": True,
    }
    assert process.stdin.closed and process.killed and process.reaped


@pytest.mark.asyncio
async def test_paginates_filters_hidden_and_accepts_absent_optional_fields(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    process, _ = _install_process(
        monkeypatch,
        [
            _message(1, {}),
            _message(
                2,
                {
                    "data": [
                        {"model": "hidden", "hidden": True},
                        {"id": " visible-one ", "displayName": ""},
                    ],
                    "nextCursor": "page-2",
                },
            ),
            _message(3, {"data": [{"model": "visible-two"}]}),
        ],
    )
    catalog = await discover_codex_models(page_size=1)
    assert [model.identifier for model in catalog] == ["visible-one", "visible-two"]
    assert catalog[0] == CodexModel("visible-one", "visible-one", False, None, ())
    assert process.stdin.messages[-1]["params"]["cursor"] == "page-2"


@pytest.mark.parametrize(
    ("responses", "error_type", "kwargs"),
    [
        ([b"not json\n"], CodexModelDiscoveryInvalid, {}),
        ([["not", "an", "object"]], CodexModelDiscoveryInvalid, {}),
        ([{"id": 99, "result": {}}], CodexModelDiscoveryInvalid, {}),
        ([_message(1, [])], CodexModelDiscoveryInvalid, {}),
        ([], CodexModelDiscoveryUnavailable, {}),
        ([ValueError("line too long")], CodexModelDiscoveryInvalid, {}),
        ([_message(1, {}), {"id": 2, "error": {"message": "secret"}}], CodexModelDiscoveryUnavailable, {}),
        ([_message(1, {}), _message(2, {"data": {}})], CodexModelDiscoveryInvalid, {}),
        ([_message(1, {}), _message(2, {"data": [42]})], CodexModelDiscoveryInvalid, {}),
        ([_message(1, {}), _message(2, {"data": [{"id": ""}]})], CodexModelDiscoveryInvalid, {}),
        (
            [_message(1, {}), _message(2, {"data": [{"id": "x", "supportedReasoningEfforts": [42]}]})],
            CodexModelDiscoveryInvalid,
            {},
        ),
        ([_message(1, {}), _message(2, [])], CodexModelDiscoveryInvalid, {}),
        ([_message(1, {})], CodexModelDiscoveryInvalid, {"max_output_bytes": 1}),
    ],
)
@pytest.mark.asyncio
async def test_rejects_malformed_or_error_responses(
    monkeypatch: pytest.MonkeyPatch,
    responses: list[object],
    error_type: type[Exception],
    kwargs: dict[str, object],
) -> None:
    process, _ = _install_process(monkeypatch, responses)
    with pytest.raises(error_type) as exc_info:
        await discover_codex_models(**kwargs)
    assert "secret" not in str(exc_info.value)
    assert process.stdin.closed and process.killed and process.reaped


@pytest.mark.parametrize("case", ["cursor", "duplicate", "pages"])
@pytest.mark.asyncio
async def test_rejects_ambiguous_or_unbounded_pagination(
    monkeypatch: pytest.MonkeyPatch, case: str
) -> None:
    first = {"data": [{"model": "same"}], "nextCursor": "again"}
    second = {
        "data": [{"model": "same"}] if case == "duplicate" else [],
        "nextCursor": "again" if case == "cursor" else None,
    }
    process, _ = _install_process(
        monkeypatch, [_message(1, {}), _message(2, first), _message(3, second)]
    )
    with pytest.raises(CodexModelDiscoveryInvalid):
        await discover_codex_models(max_pages=1 if case == "pages" else 2)
    assert process.reaped


@pytest.mark.asyncio
async def test_timeout_is_explicit_and_reaps_process(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    process, _ = _install_process(monkeypatch, [], block=True)
    with pytest.raises(CodexModelDiscoveryUnavailable, match="timed out"):
        await discover_codex_models(timeout_seconds=0.001)
    assert process.stdin.closed and process.killed and process.reaped


@pytest.mark.asyncio
async def test_cancellation_propagates_and_reaps_process(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    process, _ = _install_process(monkeypatch, [], block=True)
    task = asyncio.create_task(discover_codex_models(timeout_seconds=60))
    await process.stdout.started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert process.stdin.closed and process.killed and process.reaped


@pytest.mark.asyncio
async def test_missing_cli_is_reported_without_raw_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def missing(*args: object, **kwargs: object) -> _FakeProcess:
        raise FileNotFoundError("sensitive path")
    monkeypatch.setattr(codex_models.asyncio, "create_subprocess_exec", missing)
    with pytest.raises(CodexModelDiscoveryUnavailable) as exc_info:
        await discover_codex_models()
    assert str(exc_info.value) == "Codex model discovery is unavailable"


@pytest.mark.asyncio
async def test_requires_positive_bounds() -> None:
    with pytest.raises(ValueError, match="positive"):
        await discover_codex_models(max_pages=0)
