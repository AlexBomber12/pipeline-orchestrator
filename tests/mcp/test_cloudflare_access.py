"""Cloudflare Access protection for the remote diagnostics listener."""

from __future__ import annotations

import asyncio
import time
from typing import Any

import httpx
import jwt
import pytest
from cryptography.hazmat.primitives.asymmetric import rsa
from jwt.algorithms import RSAAlgorithm
from src.mcp import cloudflare_access as access
from starlette.applications import Starlette
from starlette.responses import JSONResponse
from starlette.routing import Route

from mcp.server.fastmcp import FastMCP
from mcp.server.transport_security import TransportSecuritySettings

ISSUER = "https://operators.cloudflareaccess.com"
AUDIENCE = "a" * 64
PUBLIC_HOST = "mcp.example.com"


def _key_material(kid: str) -> tuple[Any, dict[str, Any]]:
    private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    jwk = RSAAlgorithm.to_jwk(private_key.public_key(), as_dict=True)
    jwk.update({"kid": kid, "alg": "RS256", "use": "sig"})
    return private_key, jwk


def _assertion(
    private_key: Any,
    kid: str,
    *,
    issuer: str = ISSUER,
    audience: str = AUDIENCE,
    expires: int | None = None,
    extra: dict[str, Any] | None = None,
) -> str:
    now = int(time.time())
    payload = {
        "iss": issuer,
        "aud": audience,
        "sub": "operator",
        "iat": now - 1,
        "nbf": now - 1,
        "exp": expires if expires is not None else now + 300,
        **(extra or {}),
    }
    return jwt.encode(payload, private_key, algorithm="RS256", headers={"kid": kid})


def _transport_for_keys(
    key_documents: list[dict[str, Any] | httpx.Response],
    calls: list[str] | None = None,
) -> httpx.MockTransport:
    documents = iter(key_documents)

    def handler(request: httpx.Request) -> httpx.Response:
        if calls is not None:
            calls.append(str(request.url))
        document = next(documents)
        if isinstance(document, httpx.Response):
            return document
        return httpx.Response(200, json=document)

    return httpx.MockTransport(handler)


class _OversizedStream(httpx.AsyncByteStream):
    async def __aiter__(self):
        yield b"x" * (access._MAX_JWKS_BYTES + 1)


def test_configuration_accepts_only_a_team_issuer_and_application_audience() -> None:
    config = access.CloudflareAccessConfig.from_values(ISSUER, AUDIENCE.upper())
    assert config.issuer == ISSUER
    assert config.audience == AUDIENCE.upper()
    assert config.signing_keys_url == f"{ISSUER}/cdn-cgi/access/certs"

    invalid_issuers = (
        None,
        "https://[invalid",
        "http://operators.cloudflareaccess.com",
        "https://user@operators.cloudflareaccess.com",
        "https://operators.cloudflareaccess.com:443",
        "https://operators.cloudflareaccess.com/path",
        "https://operators.cloudflareaccess.com?query=yes",
        "https://operators.cloudflareaccess.com#fragment",
        "https://operators.example.com",
        "https://OPERATORS.cloudflareaccess.com",
    )
    for issuer in invalid_issuers:
        with pytest.raises(access.CloudflareAccessConfigurationError):
            access.CloudflareAccessConfig.from_values(issuer, AUDIENCE)
    for audience in (None, "short", "g" * 64):
        with pytest.raises(access.CloudflareAccessConfigurationError):
            access.CloudflareAccessConfig.from_values(ISSUER, audience)


@pytest.mark.asyncio
async def test_signing_key_cache_supports_rotation_and_bounds_refreshes() -> None:
    first_private, first_jwk = _key_material("first")
    second_private, second_jwk = _key_material("second")
    calls: list[str] = []
    now = [100.0]
    config = access.CloudflareAccessConfig.from_values(ISSUER, AUDIENCE)
    cache = access.CloudflareSigningKeyCache(
        config,
        transport=_transport_for_keys([{"keys": [first_jwk]}, {"keys": [second_jwk]}], calls),
        clock=lambda: now[0],
    )

    assert await cache.get("first") == first_private.public_key()
    assert await cache.get("first") == first_private.public_key()
    assert calls == [config.signing_keys_url]

    with pytest.raises(access.CloudflareAccessAuthenticationError, match="temporarily unavailable"):
        await cache.get("second")
    assert len(calls) == 1

    now[0] += access._JWKS_REFRESH_COOLDOWN_SECONDS
    assert await cache.get("second") == second_private.public_key()
    assert len(calls) == 2


@pytest.mark.asyncio
async def test_signing_key_cache_coalesces_concurrent_refreshes() -> None:
    _private, jwk = _key_material("active")
    started = asyncio.Event()
    release = asyncio.Event()
    calls = 0

    async def handler(_request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        started.set()
        await release.wait()
        return httpx.Response(200, json={"keys": [jwk]})

    config = access.CloudflareAccessConfig.from_values(ISSUER, AUDIENCE)
    cache = access.CloudflareSigningKeyCache(
        config,
        transport=httpx.MockTransport(handler),
        clock=lambda: 100.0,
    )
    first = asyncio.create_task(cache.get("active"))
    await started.wait()
    second = asyncio.create_task(cache.get("active"))
    release.set()
    await asyncio.gather(first, second)
    assert calls == 1


@pytest.mark.asyncio
async def test_signing_key_cache_fails_closed_and_rate_limits_fetch_failures() -> None:
    calls: list[str] = []
    now = [100.0]
    config = access.CloudflareAccessConfig.from_values(ISSUER, AUDIENCE)
    cache = access.CloudflareSigningKeyCache(
        config,
        transport=_transport_for_keys([httpx.Response(503)], calls),
        clock=lambda: now[0],
    )

    with pytest.raises(access.CloudflareAccessAuthenticationError, match="unavailable"):
        await cache.get("missing")
    with pytest.raises(access.CloudflareAccessAuthenticationError, match="temporarily unavailable"):
        await cache.get("missing")
    assert calls == [config.signing_keys_url]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "response, message",
    [
        (httpx.Response(200, headers={"content-length": str(access._MAX_JWKS_BYTES + 1)}), "too large"),
        (httpx.Response(200, stream=_OversizedStream()), "too large"),
        (httpx.Response(200, content=b"not-json"), "invalid"),
        (httpx.Response(200, content=b"\xff"), "invalid"),
        (httpx.Response(200, json={}), "invalid"),
        (httpx.Response(200, json={"keys": []}), "invalid"),
        (httpx.Response(200, json={"keys": ["not-an-object"]}), "invalid"),
        (
            httpx.Response(
                200,
                json={"keys": [{"kid": "bad", "kty": "EC", "alg": "RS256", "use": "sig"}]},
            ),
            "invalid",
        ),
        (
            httpx.Response(
                200,
                json={"keys": [{"kid": "bad", "kty": "RSA", "alg": "RS256", "use": "sig"}]},
            ),
            "invalid",
        ),
    ],
)
async def test_signing_key_cache_rejects_unbounded_or_invalid_documents(
    response: httpx.Response,
    message: str,
) -> None:
    config = access.CloudflareAccessConfig.from_values(ISSUER, AUDIENCE)
    cache = access.CloudflareSigningKeyCache(
        config,
        transport=_transport_for_keys([response]),
        clock=lambda: 100.0,
    )
    with pytest.raises(access.CloudflareAccessAuthenticationError, match=message):
        await cache.get("bad")


@pytest.mark.asyncio
async def test_signing_key_cache_rejects_an_unknown_key_after_refresh() -> None:
    _private, jwk = _key_material("known")
    config = access.CloudflareAccessConfig.from_values(ISSUER, AUDIENCE)
    cache = access.CloudflareSigningKeyCache(
        config,
        transport=_transport_for_keys([{"keys": [jwk]}]),
        clock=lambda: 100.0,
    )
    with pytest.raises(access.CloudflareAccessAuthenticationError, match="unknown"):
        await cache.get("other")


@pytest.mark.asyncio
async def test_verifier_accepts_valid_assertions_and_rejects_invalid_variants() -> None:
    private_key, jwk = _key_material("active")
    forged_key, _forged_jwk = _key_material("active")
    config = access.CloudflareAccessConfig.from_values(ISSUER, AUDIENCE)
    cache = access.CloudflareSigningKeyCache(
        config,
        transport=_transport_for_keys([{"keys": [jwk]}]),
    )
    verifier = access.CloudflareAccessVerifier(config, keys=cache)

    await verifier.verify(_assertion(private_key, "active"))

    invalid_assertions = (
        _assertion(forged_key, "active"),
        _assertion(private_key, "active", expires=int(time.time()) - 60),
        _assertion(private_key, "active", issuer="https://other.cloudflareaccess.com"),
        _assertion(private_key, "active", audience="b" * 64),
        _assertion(private_key, "active", extra={"nbf": int(time.time()) + 300}),
        _assertion(private_key, "active", extra={"iat": int(time.time()) + 300}),
    )
    for assertion in invalid_assertions:
        with pytest.raises(access.CloudflareAccessAuthenticationError, match="assertion is invalid"):
            await verifier.verify(assertion)

    missing_exp = jwt.encode(
        {"iss": ISSUER, "aud": AUDIENCE},
        private_key,
        algorithm="RS256",
        headers={"kid": "active"},
    )
    with pytest.raises(access.CloudflareAccessAuthenticationError, match="assertion is invalid"):
        await verifier.verify(missing_exp)


@pytest.mark.asyncio
async def test_verifier_rejects_malformed_oversized_and_unsupported_headers() -> None:
    config = access.CloudflareAccessConfig.from_values(ISSUER, AUDIENCE)
    verifier = access.CloudflareAccessVerifier(config)
    for assertion, message in (
        ("", "missing or too large"),
        ("x" * (access._MAX_ASSERTION_BYTES + 1), "missing or too large"),
        ("not-a-jwt", "malformed"),
        (
            jwt.encode(
                {"exp": int(time.time()) + 60},
                "s" * 32,
                algorithm="HS256",
                headers={"kid": "active"},
            ),
            "header is invalid",
        ),
    ):
        with pytest.raises(access.CloudflareAccessAuthenticationError, match=message):
            await verifier.verify(assertion)


class _Verifier:
    def __init__(self, error: Exception | None = None) -> None:
        self.error = error
        self.assertions: list[str] = []

    async def verify(self, assertion: str) -> None:
        self.assertions.append(assertion)
        if self.error is not None:
            raise self.error


@pytest.mark.asyncio
async def test_middleware_uses_only_one_assertion_and_returns_fixed_errors(caplog: pytest.LogCaptureFixture) -> None:
    dispatched = 0

    async def endpoint(_request: Any) -> JSONResponse:
        nonlocal dispatched
        dispatched += 1
        return JSONResponse({"ok": True})

    inner = Starlette(routes=[Route("/mcp", endpoint, methods=["POST"])])
    verifier = _Verifier(RuntimeError("raw assertion verification secret"))
    app = access.CloudflareAccessMiddleware(inner, verifier)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="https://mcp.example.com") as client:
        for headers in (
            {},
            {
                "Authorization": "Bearer opaque-client-token",
                "Cookie": "CF_Authorization=untrusted-cookie",
                "Cf-Access-Authenticated-User-Email": "untrusted@example.com",
            },
            {"Cf-Access-Jwt-Assertion": "secret-token"},
            [("Cf-Access-Jwt-Assertion", "one"), ("Cf-Access-Jwt-Assertion", "two")],
        ):
            response = await client.post("/mcp", headers=headers)
            assert response.status_code == 401
            assert response.json() == {"error": "unauthorized"}
            assert response.headers["cache-control"] == "no-store"
            assert "secret-token" not in response.text
    assert dispatched == 0
    assert "raw assertion verification secret" not in caplog.text


@pytest.mark.asyncio
async def test_middleware_passes_authenticated_http_and_lifespan_scopes() -> None:
    verifier = _Verifier()

    async def endpoint(_request: Any) -> JSONResponse:
        return JSONResponse({"ok": True})

    inner = Starlette(routes=[Route("/mcp", endpoint, methods=["POST"])])
    app = access.CloudflareAccessMiddleware(inner, verifier)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="https://mcp.example.com") as client:
        response = await client.post("/mcp", headers={"Cf-Access-Jwt-Assertion": "assertion"})
    assert response.json() == {"ok": True}
    assert verifier.assertions == ["assertion"]

    received = iter(({"type": "lifespan.startup"}, {"type": "lifespan.shutdown"}))
    sent: list[dict[str, Any]] = []

    async def receive() -> dict[str, Any]:
        return next(received)

    async def send(message: dict[str, Any]) -> None:
        sent.append(message)

    await app({"type": "lifespan", "asgi": {"version": "3.0"}}, receive, send)
    assert sent == [{"type": "lifespan.startup.complete"}, {"type": "lifespan.shutdown.complete"}]


def _mcp_request(method: str, request_id: int | None, params: dict[str, Any] | None = None) -> dict[str, Any]:
    request: dict[str, Any] = {"jsonrpc": "2.0", "method": method}
    if request_id is not None:
        request["id"] = request_id
    if params is not None:
        request["params"] = params
    return request


@pytest.mark.asyncio
async def test_protected_streamable_http_covers_initialization_discovery_calls_and_sessions() -> None:
    private_key, jwk = _key_material("active")
    config = access.CloudflareAccessConfig.from_values(ISSUER, AUDIENCE)
    key_cache = access.CloudflareSigningKeyCache(
        config,
        transport=_transport_for_keys([{"keys": [jwk]}]),
    )
    verifier = access.CloudflareAccessVerifier(config, keys=key_cache)
    calls = 0
    test_mcp = FastMCP(
        "protected-diagnostics-test",
        json_response=True,
        transport_security=TransportSecuritySettings(
            enable_dns_rebinding_protection=True,
            allowed_hosts=[PUBLIC_HOST],
            allowed_origins=[],
        ),
    )

    @test_mcp.tool()
    def get_orchestrator_status() -> dict[str, Any]:
        nonlocal calls
        calls += 1
        return {"status": "available", "read_only": True}

    base_app = test_mcp.streamable_http_app()
    protected_app = access.CloudflareAccessMiddleware(base_app, verifier)
    token = _assertion(private_key, "active")
    common_headers = {
        "Accept": "application/json, text/event-stream",
        "Content-Type": "application/json",
        "Cf-Access-Jwt-Assertion": token,
    }
    transport = httpx.ASGITransport(app=protected_app)

    async with base_app.router.lifespan_context(base_app):
        async with httpx.AsyncClient(transport=transport, base_url=f"https://{PUBLIC_HOST}") as client:
            denied = await client.post(
                "/mcp",
                json=_mcp_request(
                    "initialize",
                    1,
                    {
                        "protocolVersion": "2025-06-18",
                        "capabilities": {},
                        "clientInfo": {"name": "test", "version": "1"},
                    },
                ),
            )
            assert denied.status_code == 401

            initialized = await client.post(
                "/mcp",
                headers=common_headers,
                json=_mcp_request(
                    "initialize",
                    2,
                    {
                        "protocolVersion": "2025-06-18",
                        "capabilities": {},
                        "clientInfo": {"name": "test", "version": "1"},
                    },
                ),
            )
            assert initialized.status_code == 200
            session_id = initialized.headers["mcp-session-id"]
            session_headers = {**common_headers, "Mcp-Session-Id": session_id}

            ready = await client.post(
                "/mcp",
                headers=session_headers,
                json=_mcp_request("notifications/initialized", None),
            )
            assert ready.status_code == 202

            tools = await client.post(
                "/mcp",
                headers=session_headers,
                json=_mcp_request("tools/list", 3),
            )
            assert tools.status_code == 200
            assert [tool["name"] for tool in tools.json()["result"]["tools"]] == ["get_orchestrator_status"]

            called = await client.post(
                "/mcp",
                headers=session_headers,
                json=_mcp_request("tools/call", 4, {"name": "get_orchestrator_status", "arguments": {}}),
            )
            assert called.status_code == 200
            assert called.json()["result"]["structuredContent"] == {
                "status": "available",
                "read_only": True,
            }
            assert calls == 1

            bypass_headers = {
                "Accept": common_headers["Accept"],
                "Content-Type": common_headers["Content-Type"],
                "Mcp-Session-Id": session_id,
            }
            bypass = await client.post(
                "/mcp",
                headers=bypass_headers,
                json=_mcp_request("tools/call", 5, {"name": "get_orchestrator_status", "arguments": {}}),
            )
            assert bypass.status_code == 401
            assert calls == 1
