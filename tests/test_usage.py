"""Tests for src/usage.py."""

from __future__ import annotations

import json
import time
from pathlib import Path
from unittest.mock import patch

import httpx
import pytest

from src.usage import OAuthUsageProvider, UsageSnapshot


def _valid_response_json() -> dict:
    return {
        "five_hour": {"used_percentage": 87, "resets_at": 1744824000},
        "seven_day": {"used_percentage": 92, "resets_at": 1745430000},
    }


def _write_credentials(tmp_path: Path, data: dict) -> str:
    creds = tmp_path / ".credentials.json"
    creds.write_text(json.dumps(data), encoding="utf-8")
    return str(creds)


def _make_provider(
    tmp_path: Path,
    creds: dict | None = None,
    cache_ttl_sec: int = 60,
) -> OAuthUsageProvider:
    if creds is not None:
        path = _write_credentials(tmp_path, creds)
    else:
        path = str(tmp_path / "missing.json")
    return OAuthUsageProvider(
        credentials_path=path,
        user_agent="test-agent/1.0",
        beta_header="oauth-2025-04-20",
        cache_ttl_sec=cache_ttl_sec,
    )


def _mock_response(status_code: int = 200, json_data: dict | None = None) -> httpx.Response:
    resp = httpx.Response(
        status_code=status_code,
        json=json_data if json_data is not None else _valid_response_json(),
        request=httpx.Request("GET", "https://example.com"),
    )
    return resp


class TestOAuthProviderReturnsSnapshot:
    def test_on_success(self, tmp_path: Path) -> None:
        provider = _make_provider(tmp_path, creds={"accessToken": "tok123"})
        with patch.object(httpx, "get", return_value=_mock_response()):
            snap = provider.fetch()
        assert snap is not None
        assert snap.session_percent == 87
        assert snap.session_resets_at == 1744824000
        assert snap.weekly_percent == 92
        assert snap.weekly_resets_at == 1745430000
        assert snap.fetched_at > 0


class TestOAuthProviderReturnsNone:
    def test_on_missing_credentials_file(self, tmp_path: Path) -> None:
        provider = _make_provider(tmp_path, creds=None)
        assert provider.fetch() is None

    def test_on_401(self, tmp_path: Path) -> None:
        provider = _make_provider(tmp_path, creds={"accessToken": "expired"})
        with patch.object(httpx, "get", return_value=_mock_response(status_code=401)):
            assert provider.fetch() is None

    def test_on_network_error(self, tmp_path: Path) -> None:
        provider = _make_provider(tmp_path, creds={"accessToken": "tok"})
        with patch.object(httpx, "get", side_effect=httpx.ConnectError("fail")):
            assert provider.fetch() is None

    def test_on_malformed_json(self, tmp_path: Path) -> None:
        provider = _make_provider(tmp_path, creds={"accessToken": "tok"})
        bad_data = {"unexpected": "shape"}
        with patch.object(httpx, "get", return_value=_mock_response(json_data=bad_data)):
            assert provider.fetch() is None


class TestOAuthProviderCache:
    def test_caches_within_ttl(self, tmp_path: Path) -> None:
        provider = _make_provider(tmp_path, creds={"accessToken": "tok"}, cache_ttl_sec=300)
        call_count = 0
        original_get = httpx.get

        def counting_get(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            return _mock_response()

        with patch.object(httpx, "get", side_effect=counting_get):
            snap1 = provider.fetch()
            snap2 = provider.fetch()
        assert call_count == 1
        assert snap1 is snap2

    def test_fetches_again_after_ttl(self, tmp_path: Path) -> None:
        provider = _make_provider(tmp_path, creds={"accessToken": "tok"}, cache_ttl_sec=0)
        call_count = 0

        def counting_get(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            return _mock_response()

        with patch.object(httpx, "get", side_effect=counting_get):
            provider.fetch()
            provider.fetch()
        assert call_count == 2

    def test_invalidate_cache_forces_refetch(self, tmp_path: Path) -> None:
        provider = _make_provider(tmp_path, creds={"accessToken": "tok"}, cache_ttl_sec=300)
        call_count = 0

        def counting_get(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            return _mock_response()

        with patch.object(httpx, "get", side_effect=counting_get):
            provider.fetch()
            provider.invalidate_cache()
            provider.fetch()
        assert call_count == 2


class TestOAuthProviderConsecutiveFailures:
    def test_increment(self, tmp_path: Path) -> None:
        provider = _make_provider(tmp_path, creds=None)
        provider.fetch()
        provider.fetch()
        provider.fetch()
        assert provider.consecutive_failures == 3

    def test_reset_on_success(self, tmp_path: Path) -> None:
        provider = _make_provider(tmp_path, creds={"accessToken": "tok"})
        # Force some failures first.
        provider._credentials_path = Path("/nonexistent")
        provider.fetch()
        provider.fetch()
        assert provider.consecutive_failures == 2
        # Now restore and succeed.
        provider._credentials_path = Path(
            _write_credentials(tmp_path, {"accessToken": "tok"})
        )
        with patch.object(httpx, "get", return_value=_mock_response()):
            provider.fetch()
        assert provider.consecutive_failures == 0


class TestOAuthProviderTokenReading:
    def test_reads_flat_access_token(self, tmp_path: Path) -> None:
        provider = _make_provider(tmp_path, creds={"accessToken": "flat-tok"})
        with patch.object(httpx, "get", return_value=_mock_response()) as mock_get:
            provider.fetch()
        auth_header = mock_get.call_args.kwargs.get("headers", {}).get("Authorization")
        assert auth_header == "Bearer flat-tok"

    def test_reads_flat_access_token_snake_case(self, tmp_path: Path) -> None:
        provider = _make_provider(tmp_path, creds={"access_token": "snake-tok"})
        with patch.object(httpx, "get", return_value=_mock_response()) as mock_get:
            provider.fetch()
        auth_header = mock_get.call_args.kwargs.get("headers", {}).get("Authorization")
        assert auth_header == "Bearer snake-tok"

    def test_reads_nested_claudeAiOauth(self, tmp_path: Path) -> None:
        creds = {"claudeAiOauth": {"accessToken": "nested-tok"}}
        provider = _make_provider(tmp_path, creds=creds)
        with patch.object(httpx, "get", return_value=_mock_response()) as mock_get:
            provider.fetch()
        auth_header = mock_get.call_args.kwargs.get("headers", {}).get("Authorization")
        assert auth_header == "Bearer nested-tok"

    def test_reads_nested_claude_ai_oauth_snake_case(self, tmp_path: Path) -> None:
        creds = {"claude_ai_oauth": {"access_token": "nested-snake-tok"}}
        provider = _make_provider(tmp_path, creds=creds)
        with patch.object(httpx, "get", return_value=_mock_response()) as mock_get:
            provider.fetch()
        auth_header = mock_get.call_args.kwargs.get("headers", {}).get("Authorization")
        assert auth_header == "Bearer nested-snake-tok"
