"""Data models and enums shared by the daemon and the dashboard."""

from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from typing import NotRequired, TypedDict

from pydantic import BaseModel, Field


class PipelineState(str, Enum):
    PREFLIGHT = "PREFLIGHT"
    IDLE = "IDLE"
    CODING = "CODING"
    WATCH = "WATCH"
    FIX = "FIX"
    MERGE = "MERGE"
    HUNG = "HUNG"
    ERROR = "ERROR"
    PAUSED = "PAUSED"


class TaskStatus(str, Enum):
    TODO = "TODO"
    DOING = "DOING"
    DONE = "DONE"
    CANCELED = "CANCELED"


class ReviewStatus(str, Enum):
    PENDING = "PENDING"
    EYES = "EYES"
    APPROVED = "APPROVED"
    CHANGES_REQUESTED = "CHANGES_REQUESTED"


class CIStatus(str, Enum):
    PENDING = "PENDING"
    SUCCESS = "SUCCESS"
    FAILURE = "FAILURE"


class FeedbackCheckResult(str, Enum):
    NEW = "new"  # Codex posted after last push
    NONE = "none"  # No Codex activity after last push
    UNKNOWN = "unknown"  # API call failed, cannot determine


class QueueTask(BaseModel):
    pr_id: str
    title: str
    status: TaskStatus
    task_file: str | None = None
    depends_on: list[str] = Field(default_factory=list)
    branch: str | None = None


class PRInfo(BaseModel):
    number: int
    branch: str
    title: str = ""
    pr_id: str | None = None
    ci_status: CIStatus = CIStatus.PENDING
    review_status: ReviewStatus = ReviewStatus.PENDING
    commits_count: int = 0
    push_count: int = 0
    # Distinct head SHAs the daemon has observed for this PR. ``push_count``
    # is derived from the cardinality of this set so a force-push that
    # rewrites remote history (and shrinks ``commits_count``) does not
    # silently shrink the dashboard "Pushes" reading too — each push that
    # the daemon actually witnessed stays counted.
    observed_head_shas: set[str] = Field(default_factory=set)
    fix_iteration_count: int = 0
    no_push_fix_count: int = 0
    url: str = ""
    last_activity: datetime | None = None
    is_escalated: bool = False
    # True when the PR head is on a forked repository instead of
    # ``origin``. The daemon's auto-commit safety net cannot push to a
    # fork (no credentials, different remote), so it must refuse
    # rather than silently publish to the wrong branch on origin.
    is_cross_repository: bool = False

    def record_observed_head(self, sha: str) -> None:
        """Add ``sha`` to ``observed_head_shas`` and refresh ``push_count``.

        Empty ``sha`` is a deliberate no-op: the post-push ``git rev-parse
        HEAD`` lookup failed (timeout, ``OSError``, or non-zero exit), so
        the daemon does not yet know which SHA the push landed at. The
        next poll cycle observes the real SHA and
        ``merge_observed_pushes`` increments ``push_count`` for it.
        Bumping ``push_count`` here would double-count the same real
        push because the polling merge would then count the freshly
        observed SHA as new again (Codex P2 follow-up: empty-SHA
        fallback + WATCH/IDLE refresh counting one push twice).

        On upgrade from pre-PR-195 state, persisted ``PRInfo`` entries
        can have ``push_count > 0`` while ``observed_head_shas`` is
        empty (the field default). Bumping ``push_count`` by 1 for each
        previously-unseen SHA preserves the legacy count *and* ensures
        every genuine post-upgrade push is registered, instead of
        being suppressed until the set cardinality catches up to the
        old counter (the ``max(len(set), push_count)`` pitfall).
        """
        if not sha:
            return
        if sha in self.observed_head_shas:
            return
        self.observed_head_shas.add(sha)
        self.push_count += 1

    def merge_observed_pushes(
        self, other: "PRInfo"
    ) -> tuple[set[str], int]:
        """Return ``(observed_head_shas, push_count)`` after merging ``other``.

        ``self`` is the daemon's persisted PR state; ``other`` is a
        fresh GitHub observation. The merged SHA set is the union;
        ``push_count`` is ``self.push_count`` plus one for every
        previously-unseen SHA observed in ``other``. The earlier
        ``max(len(merged), self.push_count, other.push_count)`` formula
        froze the counter on upgrade from pre-PR-195 state
        (``push_count > 0`` with an empty ``observed_head_shas``):
        a single newly observed SHA produced ``len(merged) == 1`` while
        the legacy count won the ``max`` and dropped the push. Counting
        newly observed SHAs against the legacy base keeps polling-only
        push detection accurate after an upgrade.
        """
        new_shas = other.observed_head_shas - self.observed_head_shas
        merged_shas = self.observed_head_shas | other.observed_head_shas
        return merged_shas, self.push_count + len(new_shas)


class EventEntry(TypedDict, total=False):
    time: str
    state: str
    event: str
    count: NotRequired[int]
    last_seen_at: NotRequired[str]


class RepoState(BaseModel):
    url: str
    name: str
    state: PipelineState = PipelineState.IDLE
    user_paused: bool = False
    current_task: QueueTask | None = None
    current_pr: PRInfo | None = None
    error_message: str | None = None
    last_updated: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc)
    )
    queue_done: int = 0
    queue_total: int = 0
    active: bool = True
    history: list[EventEntry] = Field(default_factory=list)
    pending_queue_sync_branch: str | None = None
    pending_queue_sync_started_at: datetime | None = None
    rate_limited_until: datetime | None = None
    rate_limit_reactive: bool = False
    rate_limit_reactive_coder: str | None = None
    rate_limited_coders: set[str] = Field(default_factory=set)
    rate_limited_coder_until: dict[str, datetime] = Field(default_factory=dict)
    usage_session_percent: int | None = None
    usage_session_resets_at: int | None = None
    usage_weekly_percent: int | None = None
    usage_weekly_resets_at: int | None = None
    usage_api_degraded: bool = False
    coder: str | None = None
    last_stale_retrigger_at: datetime | None = None
    last_codex_retrigger_at: datetime | None = None

    def __setattr__(self, name: str, value: object) -> None:
        """Couple related task/PR fields so a single write resets them together.

        ``current_pr`` change resets the per-PR retrigger timestamps so
        a new PR does not inherit "we already retriggered Codex" memory
        from the previous one.

        ``current_task = None`` is the canonical "drop the active work
        handle" signal: it implies no live PR and no operator-relevant
        error. Coupling those resets here lets every clear callsite
        write the single triggering line.

        Pydantic v2 ordering caveat: the side-effect writes
        (``current_pr = None``, ``error_message = None``) run BEFORE the
        triggering assignment goes through Pydantic's own validation.
        Both fields are ``Optional`` and accept ``None`` today, so the
        side-effect writes never fail validation. A future field
        validator on ``current_task`` that rejects the new value would
        leave the resets in place; contributors adding such a validator
        must consciously decide whether the resets should run before or
        after validation.
        """
        if name == "current_pr":
            current_pr = getattr(self, "current_pr", None)
            if self._is_new_pr_transition(current_pr, value):
                super().__setattr__("last_stale_retrigger_at", None)
                super().__setattr__("last_codex_retrigger_at", None)
        if name == "current_task" and value is None:
            super().__setattr__("current_pr", None)
            super().__setattr__("error_message", None)
        super().__setattr__(name, value)

    @staticmethod
    def _is_new_pr_transition(old_pr: object, new_pr: object) -> bool:
        if old_pr is None and new_pr is None:
            return False
        if old_pr is None or new_pr is None:
            return True
        if not isinstance(old_pr, PRInfo) or not isinstance(new_pr, PRInfo):
            return old_pr != new_pr
        return (
            old_pr.number != new_pr.number
            or old_pr.branch != new_pr.branch
        )
