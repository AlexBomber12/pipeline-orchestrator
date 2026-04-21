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
    fix_iteration_count: int = 0
    url: str = ""
    last_activity: datetime | None = None
    is_escalated: bool = False
    # True when the PR head is on a forked repository instead of
    # ``origin``. The daemon's auto-commit safety net cannot push to a
    # fork (no credentials, different remote), so it must refuse
    # rather than silently publish to the wrong branch on origin.
    is_cross_repository: bool = False


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

    def __setattr__(self, name: str, value: object) -> None:
        if name == "current_pr":
            current_pr = getattr(self, "current_pr", None)
            if self._is_new_pr_transition(current_pr, value):
                super().__setattr__("last_stale_retrigger_at", None)
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
