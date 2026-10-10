from __future__ import annotations

import math
from dataclasses import FrozenInstanceError

import pytest
from src.coder_registry import (
    CoderBrowserLoginAdapter,
    CoderBrowserLoginFailure,
    CoderBrowserLoginProgress,
)
from src.coders.claude_login import ClaudeBrowserLoginAdapter

_QUERY_VALUES = {
    "client_id": "client%2Fopaque",
    "code": "code%2Bopaque",
    "code_challenge": "challenge_-opaque",
    "code_challenge_method": "S256",
    "redirect_uri": "http%3A%2F%2Flocalhost%3A54545%2Fcallback",
    "response_type": "code",
    "scope": "user%3Ainference+user%3Aprofile",
    "state": "state_-opaque",
}
_PROMPT = "Paste code here if prompted >"


def _url(**overrides: str) -> str:
    values = {**_QUERY_VALUES, **overrides}
    query = "&".join(f"{key}={value}" for key, value in values.items())
    return f"https://claude.com/cai/oauth/authorize?{query}"


def _adapter(
    *,
    version: object = "2.1.126",
    environment: object | None = None,
    credential_location: str = "/data/auth/claude",
) -> ClaudeBrowserLoginAdapter:
    if environment is None:
        environment = {
            "HOME": "/home/coder",
            "CLAUDE_CONFIG_DIR": credential_location,
        }
    return ClaudeBrowserLoginAdapter(
        environment=environment,  # type: ignore[arg-type]
        working_directory="/workspace",
        credential_location=credential_location,
        observed_cli_version=version,  # type: ignore[arg-type]
    )


@pytest.mark.parametrize("version", ["2.1.126", "2.1.127", "2.2.0", "2.10.0", "10.0.0"])
def test_descriptor_accepts_supported_stable_versions_numerically(
    version: str,
) -> None:
    adapter = _adapter(version=version)

    assert adapter.observed_cli_version == version
    assert isinstance(adapter, CoderBrowserLoginAdapter)


@pytest.mark.parametrize("version", ["2.1.125", "2.0.999", "1.99.999"])
def test_descriptor_rejects_old_versions_without_echoing_evidence(
    version: str,
) -> None:
    with pytest.raises(ValueError) as raised:
        _adapter(version=version)

    assert str(raised.value) == ("Claude Code CLI 2.1.126 or later is required for browser-code login")
    assert version not in str(raised.value)


@pytest.mark.parametrize("version", [None, ""])
def test_descriptor_rejects_missing_version(version: object) -> None:
    with pytest.raises(ValueError, match="^Claude Code CLI version evidence is required$"):
        _adapter(version=version)


@pytest.mark.parametrize(
    "version",
    [
        "2.1",
        "v2.1.126",
        "Claude Code 2.1.126",
        "2.1.126+local",
        "02.1.126",
        "1000000.1.126",
        2126,
    ],
)
def test_descriptor_rejects_malformed_version(version: object) -> None:
    with pytest.raises(ValueError, match="^Claude Code CLI version evidence is invalid$"):
        _adapter(version=version)


@pytest.mark.parametrize("version", ["2.1.126-beta.1", "3.0.0-rc.2"])
def test_descriptor_rejects_unverified_prerelease_version(version: str) -> None:
    with pytest.raises(ValueError) as raised:
        _adapter(version=version)

    assert str(raised.value) == ("Claude Code CLI prerelease versions are not verified for browser-code login")
    assert version not in str(raised.value)


def test_descriptor_is_fixed_pty_bound_and_copies_environment() -> None:
    environment = {
        "HOME": "/home/coder",
        "CLAUDE_CONFIG_DIR": "/data/auth/claude",
    }
    adapter = _adapter(environment=environment)
    environment["HOME"] = "mutated"

    assert adapter.command == ("claude", "auth", "login")
    assert adapter.requires_pty is True
    assert math.isfinite(adapter.application_timeout_seconds)
    assert 0 < adapter.application_timeout_seconds <= 60 * 60
    assert adapter.environment == {
        "HOME": "/home/coder",
        "CLAUDE_CONFIG_DIR": "/data/auth/claude",
    }
    with pytest.raises(TypeError):
        adapter.environment["HOME"] = "mutated"  # type: ignore[index]
    with pytest.raises(FrozenInstanceError):
        adapter.observed_cli_version = "9.9.9"  # type: ignore[misc]
    assert "failed or cancelled replacement" in adapter.replacement_warning
    assert "prior authentication unavailable" in adapter.replacement_warning


@pytest.mark.parametrize(
    ("environment", "location", "message"),
    [
        (
            {"CLAUDE_CONFIG_DIR": "/different"},
            "/data/auth/claude",
            "Claude browser login credential context is inconsistent",
        ),
        (
            {"HOME": "/home/coder"},
            "/data/auth/claude",
            "Claude browser login credential context is inconsistent",
        ),
        (
            {"CLAUDE_CONFIG_DIR": 3},
            "/data/auth/claude",
            "Claude browser login environment is invalid",
        ),
    ],
)
def test_descriptor_requires_bound_credential_environment(environment: object, location: str, message: str) -> None:
    with pytest.raises(ValueError) as raised:
        _adapter(environment=environment, credential_location=location)

    assert str(raised.value) == message


def test_descriptor_rejects_non_mapping_environment_and_empty_context() -> None:
    with pytest.raises(ValueError, match="^Claude browser login environment is invalid$"):
        _adapter(environment=object())
    with pytest.raises(ValueError, match="^Claude browser login context is invalid$"):
        ClaudeBrowserLoginAdapter(
            environment={"CLAUDE_CONFIG_DIR": "/data/auth/claude"},
            working_directory="",
            credential_location="/data/auth/claude",
            observed_cli_version="2.1.126",
        )


def test_progress_reports_url_before_prompt_without_rewriting_opaque_values() -> None:
    adapter = _adapter()
    authorization_url = _url()

    progress = adapter.parse_progress(f"Opening browser to sign in…\n{authorization_url}\n")

    assert progress == CoderBrowserLoginProgress(
        authorization_url=authorization_url,
        ready_for_code=False,
    )
    assert progress.authorization_url == authorization_url


def test_progress_waits_for_fragment_boundary_and_handles_prompt_first() -> None:
    adapter = _adapter()
    authorization_url = _url()
    fragment = f"{_PROMPT}\n{authorization_url}"

    assert adapter.parse_progress("Opening browser to sign in…\n") is None
    assert adapter.parse_progress(fragment) is None
    assert adapter.parse_progress(fragment + "\n") == CoderBrowserLoginProgress(
        authorization_url=authorization_url,
        ready_for_code=True,
    )


def test_progress_normalizes_ansi_and_carriage_return_redraws() -> None:
    adapter = _adapter()
    authorization_url = _url()
    decorated_url = authorization_url.replace("?", "\x1b[0m\x1b[36m?", 1)
    output = f"\x1b[?25l⠋ Opening browser\r\x1b[2K{decorated_url}\x1b[0m\r\x1b[32m{_PROMPT}\x1b[0m\n"

    assert adapter.parse_progress(output) == CoderBrowserLoginProgress(
        authorization_url=authorization_url,
        ready_for_code=True,
    )


@pytest.mark.parametrize(
    "candidate",
    [
        _url().replace("https://", "http://", 1),
        _url().replace("https://", "ftp://", 1),
        _url().replace("claude.com", "example.com", 1),
        _url().replace("claude.com", "user@claude.com", 1),
        _url().replace("claude.com", "claude.com:443", 1),
        _url().replace("claude.com", "claude.com:not-a-port", 1),
        _url().replace("/cai/oauth/authorize", "/oauth/authorize", 1),
        _url() + "#fragment",
        _url().replace("state=state_-opaque", "state=bad%0Avalue"),
        _url().replace("state=state_-opaque", "state=bad%ZZvalue"),
        _url().replace("state=state_-opaque", "state="),
        _url().replace("&state=state_-opaque", ""),
        _url().replace("state=state_-opaque", "extra=value"),
        _url() + "&state=duplicate",
    ],
)
def test_progress_rejects_malformed_or_ambiguous_authorization_url(
    candidate: str,
) -> None:
    secret = "fixture-secret-must-not-leak"
    candidate = candidate.replace("state_-opaque", secret)

    with pytest.raises(ValueError) as raised:
        _adapter().parse_progress(f"{candidate}\n{_PROMPT}\n")

    assert str(raised.value) == "Claude browser-code login output is malformed"
    assert raised.value.__cause__ is None
    assert secret not in str(raised.value)


def test_progress_rejects_conflicting_complete_urls_but_allows_redraw() -> None:
    adapter = _adapter()
    first = _url(state="first")
    second = _url(state="second")

    assert adapter.parse_progress(f"{first}\r{first}\n") == (
        CoderBrowserLoginProgress(
            authorization_url=first,
            ready_for_code=False,
        )
    )
    with pytest.raises(ValueError, match="^Claude browser-code login output is malformed$"):
        adapter.parse_progress(f"{first}\n{second}\n")


def test_progress_bounds_terminal_output_and_authorization_url() -> None:
    adapter = _adapter()

    with pytest.raises(ValueError, match="^Claude browser-code login output is malformed$"):
        adapter.parse_progress("x" * (16 * 1024 + 1))
    with pytest.raises(ValueError, match="^Claude browser-code login output is malformed$"):
        adapter.parse_progress(f"{_url(state='x' * 4096)}\n")
    with pytest.raises(ValueError, match="^Claude browser-code login output is malformed$"):
        adapter.parse_progress(_url(state="x" * 4096))


def test_failure_classification_is_evidence_based_and_sanitized() -> None:
    adapter = _adapter()
    secret = "submitted-code-fixture-secret"

    invalid = adapter.classify_failure(
        f"\x1b[31mInvalid code.\x1b[0m Please make sure the full code was copied.\r\n{secret}",
        1,
    )
    generic = adapter.classify_failure(
        f"Unexpected provider failure: {secret}",
        23,
    )
    bounded_generic = adapter.classify_failure("x" * (16 * 1024 + 1), 23)

    assert invalid == CoderBrowserLoginFailure(
        reason="invalid_code",
        detail="Claude Code rejected the authorization code",
    )
    assert generic == CoderBrowserLoginFailure(
        reason="process_failed",
        detail="Claude browser-code login failed",
    )
    assert bounded_generic == generic
    assert secret not in repr(invalid)
    assert secret not in repr(generic)


def test_sensitive_descriptor_and_progress_fields_are_excluded_from_repr() -> None:
    environment_secret = "environment-fixture-secret"
    credential_secret = "/credential-fixture-secret"
    workdir_secret = "/workdir-fixture-secret"
    authorization_url = _url(state="url-fixture-secret")
    adapter = ClaudeBrowserLoginAdapter(
        environment={
            "SECRET": environment_secret,
            "CLAUDE_CONFIG_DIR": credential_secret,
        },
        working_directory=workdir_secret,
        credential_location=credential_secret,
        observed_cli_version="2.1.126",
    )
    progress = adapter.parse_progress(f"{authorization_url}\n")

    assert progress is not None
    descriptor_repr = repr(adapter)
    progress_repr = repr(progress)
    assert environment_secret not in descriptor_repr
    assert credential_secret not in descriptor_repr
    assert workdir_secret not in descriptor_repr
    assert authorization_url not in progress_repr
    assert "url-fixture-secret" not in progress_repr
