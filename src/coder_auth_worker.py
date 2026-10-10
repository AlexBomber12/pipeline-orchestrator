"""Subprocess entry point for configured coder authentication probes."""

from __future__ import annotations

import inspect
import json
import os
import sys
from typing import Any

from src.coder_registry import (
    CoderAuthCapabilities,
    CoderAuthStatus,
    coder_auth_payload,
)
from src.coders import _load_plugin

RESULT_PREFIX = "PIPELINE_AUTH_RESULT:"
EXPLICIT_ENVIRONMENT_ARG = "--explicit-environment"


def _accepts_keyword(callable_: object, keyword: str) -> bool:
    """Return whether ``callable_`` accepts ``keyword`` by name."""
    parameters = inspect.signature(callable_).parameters
    parameter = parameters.get(keyword)
    if parameter is not None and parameter.kind in {
        inspect.Parameter.POSITIONAL_OR_KEYWORD,
        inspect.Parameter.KEYWORD_ONLY,
    }:
        return True
    return any(
        candidate.kind is inspect.Parameter.VAR_KEYWORD
        for candidate in parameters.values()
    )


def run_probe(
    plugin_id: str,
    reference: str,
    config_path: str,
    *,
    environment: dict[str, str] | None = None,
) -> dict[str, Any]:
    """Load one startup reference and return a redacted auth status."""
    display_name = plugin_id
    try:
        plugin = _load_plugin(plugin_id, reference)
        display_name = plugin.display_name
        check_auth = plugin.check_auth
        kwargs: dict[str, Any] = {}
        if "config_path" in inspect.signature(check_auth).parameters:
            kwargs["config_path"] = config_path
        if environment is not None:
            if not _accepts_keyword(check_auth, "environment"):
                raise TypeError("check_auth does not accept environment")
            kwargs["environment"] = dict(environment)
        result: Any = check_auth(**kwargs)
        capabilities = getattr(plugin, "auth_capabilities", None)
        if capabilities is not None and not isinstance(
            capabilities, CoderAuthCapabilities
        ):
            raise TypeError("invalid auth capabilities")
        return coder_auth_payload(result, capabilities=capabilities)
    except Exception as exc:
        return coder_auth_payload(
            CoderAuthStatus(
                status="error",
                detail=(
                    f"{display_name} auth check failed ({type(exc).__name__})"
                ),
                failure_reason="probe_failed",
            )
        )


def main() -> None:
    """Write one machine-readable result as the worker's final line."""
    explicit_environment = (
        len(sys.argv) == 5 and sys.argv[4] == EXPLICIT_ENVIRONMENT_ARG
    )
    if len(sys.argv) not in {4, 5} or (
        len(sys.argv) == 5 and not explicit_environment
    ):
        raise SystemExit(2)
    if explicit_environment:
        result = run_probe(
            sys.argv[1],
            sys.argv[2],
            sys.argv[3],
            environment=dict(os.environ),
        )
    else:
        result = run_probe(sys.argv[1], sys.argv[2], sys.argv[3])
    print(f"{RESULT_PREFIX}{json.dumps(result, separators=(',', ':'))}")


if __name__ == "__main__":  # pragma: no cover - exercised only in child process
    main()
