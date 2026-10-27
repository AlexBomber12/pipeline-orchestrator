"""Share daemon-verified GitHub evidence with credential-free MCP readers."""

from __future__ import annotations

import json
import logging
import os
from collections.abc import Iterable

import redis

from src.keyspace import verified_merged_branches
from src.queue_parser import TaskHeader
from src.task_status import _branch_name_matches_task_pr_id

logger = logging.getLogger(__name__)


def read_branch_completions(
    owner_repo: str, base_branch: str, headers: Iterable[TaskHeader],
) -> set[str]:
    """Read positive evidence only; outages and malformed snapshots fail closed.

    The daemon replaces this snapshot after a successful API observation.
    Readers never need GitHub credentials or trust task status frontmatter.
    """
    try:
        with redis.Redis.from_url(
            os.environ.get("REDIS_URL", "redis://localhost:6379/0"),
            socket_connect_timeout=2, socket_timeout=2,
        ) as client:
            raw = client.get(verified_merged_branches(owner_repo, base_branch))
        branches = json.loads(raw) if raw is not None else []
        if not isinstance(branches, list) or any(not isinstance(b, str) for b in branches):
            return set()
    except (redis.RedisError, OSError, ValueError):
        logger.warning("Daemon branch-completion evidence unavailable for %s", owner_repo)
        return set()
    return {
        header.pr_id for header in headers
        if header.branch in branches
        and _branch_name_matches_task_pr_id(header.pr_id, header.branch)
    }
