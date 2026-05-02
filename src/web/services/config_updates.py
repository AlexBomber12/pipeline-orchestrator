"""Shared helper for web config mutations.

Every endpoint that mutates ``config.yml`` must, after the on-disk write,
nudge the daemon so the change applies promptly instead of only after the
next ``config_watcher`` tick. The nudge has two parts per affected repo:
``SET control:{repo}:config_dirty`` so the runner picks the change up at
its next IDLE boundary, and ``PUBLISH`` on ``orchestrator:wake:{repo}``
so the daemon's main loop short-circuits its sleep.

For daemon-level fields (e.g. ``exploration_epsilon``, ``claude_model``)
the affected set is every active repo, since daemon-level config feeds
into every runner. The cost of waking N repos for a daemon-level write is
acceptable for the small fleets this orchestrator targets; if that ever
grows, callers can supply a smaller subset.
"""

from __future__ import annotations

import logging
from collections.abc import Iterable

from src import keyspace
from src.events import publish_wake

logger = logging.getLogger(__name__)


async def apply_config_mutation(
    *,
    redis_client,
    affected_repo_names: Iterable[str],
    event_type: str,
) -> None:
    """Set dirty flags and publish wake events for affected repos.

    Call this *after* ``config.yml`` has been written. Per repo, this
    performs ``SET control:{repo}:config_dirty`` followed by ``PUBLISH``
    on ``orchestrator:wake:{repo}``. The order matters: set the inbox
    flag before ringing the doorbell so the daemon never wakes up to
    find no dirty key and goes back to sleep.

    Failures are logged at WARNING and swallowed; the daemon's polling
    fallback (``config_watcher`` at ``CONFIG_WATCH_INTERVAL_SEC``) will
    pick the change up even if Redis is briefly unavailable. Endpoints
    must therefore still return success when this helper logs a warning.
    """
    for name in affected_repo_names:
        dirty_key = keyspace.control_config_dirty(name)
        try:
            await redis_client.set(dirty_key, "1")
        except Exception:
            logger.warning(
                "Failed to set %s; daemon polling fallback will pick up "
                "config change within CONFIG_WATCH_INTERVAL_SEC",
                dirty_key,
            )
        try:
            await publish_wake(redis_client, name, event_type)
        except Exception:
            logger.warning(
                "publish_wake failed for %s; daemon will pick up %s on "
                "next polling tick",
                name,
                event_type,
            )
