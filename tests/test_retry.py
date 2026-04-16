"""Tests for src/retry.py."""

from __future__ import annotations

import subprocess
from unittest.mock import patch

import pytest

from src.retry import is_transient_error, retry_transient


class TestIsTransientError:
    def test_timeout_expired(self) -> None:
        exc = subprocess.TimeoutExpired(cmd=["git", "fetch"], timeout=30)
        assert is_transient_error(exc) is True

    def test_connection_reset_in_stderr(self) -> None:
        exc = subprocess.CalledProcessError(
            1, ["git", "fetch"], stderr="fatal: connection reset by peer"
        )
        assert is_transient_error(exc) is True

    def test_503_in_stderr(self) -> None:
        exc = subprocess.CalledProcessError(
            1, ["gh", "api"], stderr="HTTP 503 Service Unavailable"
        )
        assert is_transient_error(exc) is True

    def test_502_in_stderr(self) -> None:
        exc = subprocess.CalledProcessError(
            1, ["gh", "api"], stderr="502 Bad Gateway"
        )
        assert is_transient_error(exc) is True

    def test_504_in_stderr(self) -> None:
        exc = subprocess.CalledProcessError(
            1, ["gh", "api"], stderr="504 Gateway Timeout"
        )
        assert is_transient_error(exc) is True

    def test_remote_end_hung_up(self) -> None:
        exc = subprocess.CalledProcessError(
            128, ["git", "push"], stderr="fatal: the remote end hung up unexpectedly"
        )
        assert is_transient_error(exc) is True

    def test_could_not_resolve_host(self) -> None:
        exc = subprocess.CalledProcessError(
            128, ["git", "fetch"], stderr="fatal: could not resolve host: github.com"
        )
        assert is_transient_error(exc) is True

    def test_exit_code_128_alone_not_transient(self) -> None:
        exc = subprocess.CalledProcessError(
            128, ["git", "fetch"], stderr=""
        )
        assert is_transient_error(exc) is False

    def test_exit_code_128_with_transient_marker(self) -> None:
        exc = subprocess.CalledProcessError(
            128, ["git", "fetch"], stderr="fatal: remote end hung up unexpectedly"
        )
        assert is_transient_error(exc) is True

    def test_authentication_failed_not_transient(self) -> None:
        exc = subprocess.CalledProcessError(
            1, ["git", "push"], stderr="fatal: Authentication failed"
        )
        assert is_transient_error(exc) is False

    def test_permission_denied_not_transient(self) -> None:
        exc = subprocess.CalledProcessError(
            1, ["gh", "api"], stderr="permission denied"
        )
        assert is_transient_error(exc) is False

    def test_invalid_ref_not_transient(self) -> None:
        exc = subprocess.CalledProcessError(
            1, ["git", "checkout"], stderr="error: pathspec 'foo' did not match"
        )
        assert is_transient_error(exc) is False

    def test_runtime_error_502_transient(self) -> None:
        exc = RuntimeError(
            "gh api failed (exit 1): 502 bad gateway"
        )
        assert is_transient_error(exc) is True

    def test_runtime_error_503_transient(self) -> None:
        exc = RuntimeError(
            "gh api failed (exit 1): 503 service unavailable"
        )
        assert is_transient_error(exc) is True

    def test_runtime_error_504_transient(self) -> None:
        exc = RuntimeError(
            "gh api failed (exit 1): 504 gateway timeout"
        )
        assert is_transient_error(exc) is True

    def test_runtime_error_connection_reset_transient(self) -> None:
        exc = RuntimeError(
            "gh api failed (exit 1): connection reset"
        )
        assert is_transient_error(exc) is True

    def test_runtime_error_not_found_not_transient(self) -> None:
        exc = RuntimeError(
            "gh api failed (exit 1): HTTP 404 Not Found"
        )
        assert is_transient_error(exc) is False

    def test_generic_exception_not_transient(self) -> None:
        exc = ValueError("something went wrong")
        assert is_transient_error(exc) is False

    def test_none_stderr_handled(self) -> None:
        exc = subprocess.CalledProcessError(1, ["git"], stderr=None)
        assert is_transient_error(exc) is False


class TestRetryTransient:
    @patch("src.retry.time.sleep")
    def test_succeeds_first_attempt(self, mock_sleep) -> None:
        calls = []

        def op():
            calls.append(1)
            return "ok"

        result = retry_transient(op, operation_name="test")
        assert result == "ok"
        assert len(calls) == 1
        mock_sleep.assert_not_called()

    @patch("src.retry.time.sleep")
    def test_succeeds_second_attempt(self, mock_sleep) -> None:
        calls = []

        def op():
            calls.append(1)
            if len(calls) == 1:
                raise subprocess.TimeoutExpired(cmd=["git"], timeout=30)
            return "ok"

        result = retry_transient(op, operation_name="test")
        assert result == "ok"
        assert len(calls) == 2
        mock_sleep.assert_called_once_with(1.0)

    @patch("src.retry.time.sleep")
    def test_exhausts_and_raises(self, mock_sleep) -> None:
        def op():
            raise subprocess.TimeoutExpired(cmd=["git"], timeout=30)

        with pytest.raises(RuntimeError, match="failed after 3 attempts"):
            retry_transient(op, attempts=3, operation_name="git fetch")

        assert mock_sleep.call_count == 2
        mock_sleep.assert_any_call(1.0)
        mock_sleep.assert_any_call(2.0)

    @patch("src.retry.time.sleep")
    def test_non_transient_propagates_immediately(self, mock_sleep) -> None:
        def op():
            raise subprocess.CalledProcessError(
                1, ["git"], stderr="fatal: Authentication failed"
            )

        with pytest.raises(subprocess.CalledProcessError):
            retry_transient(op, operation_name="test")

        mock_sleep.assert_not_called()

    @patch("src.retry.time.sleep")
    def test_exponential_backoff(self, mock_sleep) -> None:
        calls = []

        def op():
            calls.append(1)
            if len(calls) < 4:
                raise subprocess.CalledProcessError(
                    128, ["git", "fetch"], stderr="fatal: the remote end hung up unexpectedly"
                )
            return "ok"

        result = retry_transient(
            op,
            attempts=4,
            initial_backoff_sec=0.5,
            operation_name="test",
        )
        assert result == "ok"
        assert len(calls) == 4
        assert mock_sleep.call_args_list[0][0] == (0.5,)
        assert mock_sleep.call_args_list[1][0] == (1.0,)
        assert mock_sleep.call_args_list[2][0] == (2.0,)

    @patch("src.retry.time.sleep")
    def test_retries_runtime_error_with_transient_marker(self, mock_sleep) -> None:
        calls = []

        def op():
            calls.append(1)
            if len(calls) == 1:
                raise RuntimeError("gh api failed (exit 1): 502 bad gateway")
            return "ok"

        result = retry_transient(op, operation_name="test")
        assert result == "ok"
        assert len(calls) == 2
        mock_sleep.assert_called_once_with(1.0)

    @patch("src.retry.time.sleep")
    def test_runtime_error_wraps_original(self, mock_sleep) -> None:
        original = subprocess.TimeoutExpired(cmd=["git"], timeout=30)

        def op():
            raise original

        with pytest.raises(RuntimeError) as exc_info:
            retry_transient(op, attempts=2, operation_name="test op")

        assert exc_info.value.__cause__ is original
        assert "test op" in str(exc_info.value)
        assert "2 attempts" in str(exc_info.value)
