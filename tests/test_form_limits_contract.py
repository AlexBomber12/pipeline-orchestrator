"""PR-435 investigation: real routes, disposable dependency matrix, no runtime fix.

The candidate guard exists only here. Run this module with both the pinned stack
and FastAPI 0.162.0 / Starlette 1.3.1; see docs/operations.md for interpretation.
"""

from importlib.metadata import version

import pytest
from fastapi.testclient import TestClient
from src.config import load_config
from src.web.app import app
from starlette.formparsers import FormParser
from starlette.responses import PlainTextResponse

from tests import test_upload as upload_fixtures

one_repo_config = upload_fixtures.one_repo_config
repo_dir = upload_fixtures.repo_dir
uploads_dir = upload_fixtures.uploads_dir
_task_file = upload_fixtures._task_file

URLENCODED = "application/x-www-form-urlencoded"
BYTE_LIMIT = 64 * 1024
FIELD_LIMIT = 1000
PATCHED = tuple(int(n) for n in version("starlette").split(".")[:3]) >= (1, 3, 1)
PATHS = [("POST", "/settings/webhook/test"), ("PUT", "/settings/daemon")]


class CandidateFormGuard:
    """Test-only bounded preflight; never installed on the production app.

    Buffer at most BYTE_LIMIT received bytes, then replay once. Counting raw '&'
    is conservative (empty segments count); percent-encoded '&' is field data.
    Multipart uploads retain the existing parser and application file limits.
    """

    def __init__(self, downstream):
        self.downstream = downstream

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            return await self.downstream(scope, receive, send)
        media_types = [
            value.partition(b";")[0].strip().lower()
            for name, value in scope["headers"]
            if name.lower() == b"content-type"
        ]
        if URLENCODED.encode() not in media_types:
            return await self.downstream(scope, receive, send)
        body = bytearray()
        while True:
            message = await receive()
            if message["type"] == "http.disconnect":
                return
            chunk = message.get("body", b"")
            if len(body) + len(chunk) > BYTE_LIMIT:
                return await PlainTextResponse("Form body limit", 413)(scope, receive, send)
            body.extend(chunk)
            if not message.get("more_body", False):
                break
        if body and body.count(b"&") + 1 > FIELD_LIMIT:
            return await PlainTextResponse("Form field limit", 413)(scope, receive, send)
        replayed = False

        async def replay():
            nonlocal replayed
            if not replayed:
                replayed = True
                return {"type": "http.request", "body": bytes(body), "more_body": False}
            return await receive()

        return await self.downstream(scope, replay, send)


@pytest.mark.parametrize("method,path", PATHS)
@pytest.mark.parametrize(
    "body", [b"x=" + b"a" * (1024 * 1024 + 1), b"&".join([b"x=1"] * 1001)], ids=["oversized-field", "excess-fields"]
)
def test_native_parser_limits_on_actual_routes(one_repo_config, method, path, body):
    """Characterize the unfixed pins and prove the candidate upgrade's limits."""
    with TestClient(app) as client:
        response = client.request(method, path, content=body, headers={"content-type": URLENCODED})
    assert response.status_code == (400 if PATCHED else 200)
    if PATCHED:
        assert "maximum" in response.text.lower() or "too many fields" in response.text.lower()


@pytest.mark.parametrize("method,path", PATHS)
@pytest.mark.parametrize(
    "body", [b"x=" + b"a" * BYTE_LIMIT, b"&".join([b"x=1"] * 1001)], ids=["oversized-body", "excess-fields"]
)
@pytest.mark.parametrize("declared_length", [None, "1"])
def test_guard_rejects_before_parser(one_repo_config, monkeypatch, method, path, body, declared_length):
    async def forbidden_parse(self):
        pytest.fail("Rejected form reached the URL-encoded parser")

    monkeypatch.setattr(FormParser, "parse", forbidden_parse)
    headers = {"content-type": URLENCODED + "; charset=UTF-8"}
    if declared_length is not None:
        headers["content-length"] = declared_length
    # Iterator content exercises ASGI streams without trusting Content-Length.
    with TestClient(CandidateFormGuard(app)) as client:
        response = client.request(method, path, content=iter([body[:100], body[100:]]), headers=headers)
    assert response.status_code == 413


@pytest.mark.parametrize("guarded", [False, True])
def test_settings_and_multipart_upload_remain_usable(one_repo_config, repo_dir, uploads_dir, guarded):
    target = CandidateFormGuard(app) if guarded else app
    with TestClient(target) as client:
        response = client.put("/settings/daemon", data={"poll_interval_sec": "45", "review_timeout_min": "90"})
        assert response.status_code == 200
        assert load_config(str(one_repo_config)).daemon.poll_interval_sec == 45
        response = client.post("/settings/webhook/test", data={"guardrail_notification_webhook_url": ""})
        assert response.status_code == 200
        assert "No URL configured" in response.text
        response = client.post("/repos/example__alpha/upload-tasks", files=[_task_file()], data={"subject": "Test"})
        assert response.status_code == 200
        assert "Accepted 1 task file (PR-001)." in response.text
    staged = list((uploads_dir / "example__alpha").glob("*/PR-001.md"))
    assert len(staged) == 1
    assert staged[0].read_bytes() == _task_file()[1][1]


@pytest.mark.parametrize(
    "body",
    [b"x=" + b"a" * (BYTE_LIMIT - 2), b"&".join([b"x=1"] * FIELD_LIMIT), b"x=%26%3D"],
    ids=["byte-limit", "field-limit", "escaped-delimiters"],
)
def test_guard_inclusive_boundaries_and_escaped_delimiters(one_repo_config, body):
    with TestClient(CandidateFormGuard(app)) as client:
        response = client.post("/settings/webhook/test", content=body, headers={"content-type": URLENCODED})
    assert response.status_code == 200


async def test_guard_disconnect_never_dispatches():
    async def downstream(*args):
        pytest.fail("Incomplete body dispatched")

    async def receive():
        return {"type": "http.disconnect"}

    async def send(message):
        pytest.fail("Disconnected client received a response")

    await CandidateFormGuard(downstream)(
        {"type": "http", "headers": [(b"content-type", URLENCODED.encode())]},
        receive,
        send,
    )


@pytest.mark.parametrize("field_heavy", [False, True])
async def test_guard_consumes_bounded_asgi_messages(field_heavy):
    """TestClient may coalesce chunks; prove receive behavior directly too."""
    chunks = [b"x=1&" * FIELD_LIMIT, b"x=1"] if field_heavy else [b"x=" + b"a" * (BYTE_LIMIT - 2), b"a"]
    reads = 0
    sent = []

    async def downstream(*args):
        pytest.fail("Rejected stream dispatched")

    async def receive():
        nonlocal reads
        if reads >= len(chunks):
            pytest.fail("Guard drained the rejected request")
        chunk = chunks[reads]
        reads += 1
        return {"type": "http.request", "body": chunk, "more_body": reads == 1}

    async def send(message):
        sent.append(message)

    await CandidateFormGuard(downstream)(
        {"type": "http", "headers": [(b"content-type", URLENCODED.encode())]},
        receive,
        send,
    )
    assert reads == 2
    assert sent[0]["status"] == 413
