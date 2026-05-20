"""Sandbox-level helpers exposed outside :mod:`src.daemon.sandbox`.

PR-353 adds :mod:`src.sandbox.runtime_state` so the dashboard and the
daemon can both share a single source of truth for the three-state
``disabled``/``active``/``unavailable`` sandbox UI badge, without
coupling the badge logic to the coder-dispatch helper that lives in
:mod:`src.daemon.sandbox`.
"""
