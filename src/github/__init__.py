"""GitHub API helpers split out of the legacy ``github_client`` module.

Foundation submodule (``gh_runner``) plus thematic API-domain submodules
(``cache``, ``checks``, ``comments``, ``prs``, ``rate_limit``,
``reactions``, ``reviews``). PR-226b finished the migration: the legacy
``github_client`` shim is gone, callers must import from
``src.github.<module>`` explicitly.
"""
