"""Cloudflare Access assertion validation for the remote MCP listener."""

from __future__ import annotations

import asyncio
import json
import re
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Protocol
from urllib.parse import urlsplit

import httpx
import jwt
from jwt.algorithms import RSAAlgorithm
from starlette.responses import JSONResponse
from starlette.types import ASGIApp, Receive, Scope, Send

_ALGORITHM = "RS256"
_ASSERTION_HEADER = b"cf-access-jwt-assertion"
_AUDIENCE_PATTERN = re.compile(r"[0-9a-fA-F]{64}")
_TEAM_HOST_PATTERN = re.compile(
    r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.cloudflareaccess\.com"
)
_MAX_ASSERTION_BYTES = 16 * 1024
_MAX_JWKS_BYTES = 64 * 1024
_MAX_JWKS_KEYS = 8
_MAX_KID_LENGTH = 256
_JWKS_CACHE_SECONDS = 60 * 60
_JWKS_REFRESH_COOLDOWN_SECONDS = 30
_JWKS_TIMEOUT_SECONDS = 5.0


class CloudflareAccessConfigurationError(ValueError):
    """The protected listener cannot safely start with this configuration."""


class CloudflareAccessAuthenticationError(Exception):
    """An Access assertion could not be authenticated."""


@dataclass(frozen=True)
class CloudflareAccessConfig:
    """Validated inputs used to authenticate Cloudflare Access assertions."""

    issuer: str
    audience: str

    @classmethod
    def from_values(cls, issuer: str | None, audience: str | None) -> CloudflareAccessConfig:
        """Validate a Cloudflare team issuer and application AUD tag."""
        issuer_value = (issuer or "").strip()
        audience_value = (audience or "").strip()
        try:
            parsed = urlsplit(issuer_value)
            port = parsed.port
        except ValueError as exc:
            raise CloudflareAccessConfigurationError(
                "MCP_CLOUDFLARE_ACCESS_ISSUER must be a valid Cloudflare Access team issuer"
            ) from exc
        hostname = parsed.hostname or ""
        if (
            parsed.scheme != "https"
            or parsed.username is not None
            or parsed.password is not None
            or port is not None
            or parsed.path
            or parsed.query
            or parsed.fragment
            or not _TEAM_HOST_PATTERN.fullmatch(hostname)
            or issuer_value != f"https://{hostname}"
        ):
            raise CloudflareAccessConfigurationError(
                "MCP_CLOUDFLARE_ACCESS_ISSUER must be an https://<team>.cloudflareaccess.com issuer"
            )
        if not _AUDIENCE_PATTERN.fullmatch(audience_value):
            raise CloudflareAccessConfigurationError(
                "MCP_CLOUDFLARE_ACCESS_AUDIENCE must be a 64-character application AUD tag"
            )
        return cls(issuer=issuer_value, audience=audience_value)

    @property
    def signing_keys_url(self) -> str:
        """Return the only endpoint from which signing keys may be loaded."""
        return f"{self.issuer}/cdn-cgi/access/certs"


class _AssertionVerifier(Protocol):
    async def verify(self, assertion: str) -> None: ...


class CloudflareSigningKeyCache:
    """Bounded, rotation-aware cache for one team's Access signing keys."""

    def __init__(
        self,
        config: CloudflareAccessConfig,
        *,
        transport: httpx.AsyncBaseTransport | None = None,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._config = config
        self._transport = transport
        self._clock = clock
        self._keys: dict[str, Any] = {}
        self._expires_at = 0.0
        self._refresh_after = 0.0
        self._lock = asyncio.Lock()

    async def get(self, kid: str) -> Any:
        """Return a cached key, refreshing at most once per cooldown window."""
        now = self._clock()
        key = self._keys.get(kid)
        if key is not None and now < self._expires_at:
            return key

        async with self._lock:
            now = self._clock()
            key = self._keys.get(kid)
            if key is not None and now < self._expires_at:
                return key
            if now < self._refresh_after:
                raise CloudflareAccessAuthenticationError("signing keys are temporarily unavailable")
            self._refresh_after = now + _JWKS_REFRESH_COOLDOWN_SECONDS
            keys = await self._fetch()
            self._keys = keys
            self._expires_at = self._clock() + _JWKS_CACHE_SECONDS
            try:
                return keys[kid]
            except KeyError:
                raise CloudflareAccessAuthenticationError("assertion signing key is unknown") from None

    async def _fetch(self) -> dict[str, Any]:
        timeout = httpx.Timeout(_JWKS_TIMEOUT_SECONDS)
        try:
            async with httpx.AsyncClient(
                transport=self._transport,
                timeout=timeout,
                follow_redirects=False,
            ) as client:
                async with client.stream(
                    "GET",
                    self._config.signing_keys_url,
                    headers={"Accept": "application/json"},
                ) as response:
                    response.raise_for_status()
                    content_length = response.headers.get("content-length")
                    if content_length is not None and int(content_length) > _MAX_JWKS_BYTES:
                        raise CloudflareAccessAuthenticationError("signing-key response is too large")
                    body = bytearray()
                    async for chunk in response.aiter_bytes():
                        body.extend(chunk)
                        if len(body) > _MAX_JWKS_BYTES:
                            raise CloudflareAccessAuthenticationError("signing-key response is too large")
        except CloudflareAccessAuthenticationError:
            raise
        except (httpx.HTTPError, UnicodeError, ValueError) as exc:
            raise CloudflareAccessAuthenticationError("signing keys are unavailable") from exc

        try:
            document = json.loads(body)
            raw_keys = document["keys"]
            if not isinstance(raw_keys, list) or not 1 <= len(raw_keys) <= _MAX_JWKS_KEYS:
                raise ValueError("invalid signing-key count")
            keys: dict[str, Any] = {}
            for raw_key in raw_keys:
                if not isinstance(raw_key, dict):
                    raise ValueError("invalid signing key")
                kid = raw_key.get("kid")
                if (
                    not isinstance(kid, str)
                    or not kid
                    or len(kid) > _MAX_KID_LENGTH
                    or raw_key.get("kty") != "RSA"
                    or raw_key.get("alg") != _ALGORITHM
                    or raw_key.get("use") != "sig"
                    or kid in keys
                ):
                    raise ValueError("invalid signing key")
                keys[kid] = RSAAlgorithm.from_jwk(raw_key)
            return keys
        except (KeyError, TypeError, UnicodeError, ValueError, jwt.PyJWTError) as exc:
            raise CloudflareAccessAuthenticationError("signing-key response is invalid") from exc


class CloudflareAccessVerifier:
    """Verify only signed Cloudflare Access assertions for the configured app."""

    def __init__(
        self,
        config: CloudflareAccessConfig,
        *,
        keys: CloudflareSigningKeyCache | None = None,
    ) -> None:
        self._config = config
        self._keys = keys or CloudflareSigningKeyCache(config)

    async def verify(self, assertion: str) -> None:
        if not assertion or len(assertion.encode("utf-8")) > _MAX_ASSERTION_BYTES:
            raise CloudflareAccessAuthenticationError("assertion is missing or too large")
        try:
            header = jwt.get_unverified_header(assertion)
        except jwt.PyJWTError as exc:
            raise CloudflareAccessAuthenticationError("assertion is malformed") from exc
        kid = header.get("kid")
        if header.get("alg") != _ALGORITHM or not isinstance(kid, str) or not kid or len(kid) > _MAX_KID_LENGTH:
            raise CloudflareAccessAuthenticationError("assertion header is invalid")
        key = await self._keys.get(kid)
        try:
            jwt.decode(
                assertion,
                key=key,
                algorithms=[_ALGORITHM],
                issuer=self._config.issuer,
                audience=self._config.audience,
                options={"require": ["exp", "iss", "aud"]},
            )
        except jwt.PyJWTError as exc:
            raise CloudflareAccessAuthenticationError("assertion is invalid") from exc


class CloudflareAccessMiddleware:
    """Authenticate every HTTP request before it reaches the MCP transport."""

    def __init__(self, app: ASGIApp, verifier: _AssertionVerifier) -> None:
        self._app = app
        self._verifier = verifier

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self._app(scope, receive, send)
            return
        assertions = [value for name, value in scope.get("headers", []) if name.lower() == _ASSERTION_HEADER]
        if len(assertions) != 1:
            await self._reject(scope, receive, send)
            return
        try:
            assertion = assertions[0].decode("ascii")
            await self._verifier.verify(assertion)
        except Exception:
            await self._reject(scope, receive, send)
            return
        await self._app(scope, receive, send)

    @staticmethod
    async def _reject(scope: Scope, receive: Receive, send: Send) -> None:
        response = JSONResponse(
            {"error": "unauthorized"},
            status_code=401,
            headers={"Cache-Control": "no-store"},
        )
        await response(scope, receive, send)
