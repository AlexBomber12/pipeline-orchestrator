"""src.github package — split out from src.github_client during PR-226a/b.

Foundation submodule (gh_runner) plus three thematic API-domain submodules
(prs, checks, reviews). The remaining four domains (reactions, comments,
rate_limit, cache) still live in src.github_client until PR-226b finishes
the migration; this package re-exports the moved domains so callers can
opt into the new import surface incrementally.
"""

from src.github import checks, gh_runner, prs, reviews

__all__ = ["checks", "gh_runner", "prs", "reviews"]
