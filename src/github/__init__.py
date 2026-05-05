"""GitHub API helpers split out of the legacy ``github_client`` module.

Foundation submodule (``gh_runner``) plus thematic API-domain submodules
(``cache``, ``checks``, ``comments``, ``prs``, ``rate_limit``,
``reactions``, ``reviews``). PR-226b finished the migration: the legacy
``github_client`` shim is gone, so most callers import from
``src.github.<module>`` explicitly. Selected stable helpers are re-exported
below.
"""

from src.github.prs import GhPrMergedBranchesUnavailable, gh_pr_get_merged_branches

__all__ = [
    "GhPrMergedBranchesUnavailable",
    "gh_pr_get_merged_branches",
]
