"""Subprocess entry point for configured coder authentication probes."""

from __future__ import annotations

import inspect
import json
import sys
from typing import Any

from src.coders import _load_plugin

RESULT_PREFIX = "PIPELINE_AUTH_RESULT:"


def run_probe(
    plugin_id: str,
    reference: str,
    config_path: str,
) -> dict[str, str]:
    """Load one startup reference and return a redacted auth status."""
    display_name = plugin_id
    try:
        plugin = _load_plugin(plugin_id, reference)
        display_name = plugin.display_name
        check_auth = plugin.check_auth
        kwargs = (
            {"config_path": config_path}
            if "config_path" in inspect.signature(check_auth).parameters
            else {}
        )
        result: Any = check_auth(**kwargs)
        if (
            not isinstance(result, dict)
            or result.get("status") not in {"ok", "error"}
            or not isinstance(result.get("detail"), str)
            or not all(
                isinstance(key, str) and isinstance(value, str)
                for key, value in result.items()
            )
        ):
            raise TypeError("invalid auth status")
        return result
    except Exception as exc:
        return {
            "status": "error",
            "detail": f"{display_name} auth check failed ({type(exc).__name__})",
        }


def main() -> None:
    """Write one machine-readable result as the worker's final line."""
    if len(sys.argv) != 4:
        raise SystemExit(2)
    result = run_probe(sys.argv[1], sys.argv[2], sys.argv[3])
    print(f"{RESULT_PREFIX}{json.dumps(result, separators=(',', ':'))}")


if __name__ == "__main__":  # pragma: no cover - exercised only in child process
    main()
