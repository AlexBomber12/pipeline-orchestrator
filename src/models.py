"""Data models and enums shared by the daemon and the dashboard."""

from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum

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
    ci_status: CIStatus = CIStatus.PENDING
    review_status: ReviewStatus = ReviewStatus.PENDING
    push_count: int = 0
    url: str = ""
    last_activity: datetime | None = None
    # True when the PR head is on a forked repository instead of
    # ``origin``. The daemon's auto-commit safety net cannot push to a
    # fork (no credentials, different remote), so it must refuse
    # rather than silently publish to the wrong branch on origin.
    is_cross_repository: bool = False


class RepoState(BaseModel):
    url: str
    name: str
    state: PipelineState = PipelineState.IDLE
    current_task: QueueTask | None = None
    current_pr: PRInfo | None = None
    error_message: str | None = None
    last_updated: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc)
    )
    queue_done: int = 0
    queue_total: int = 0
    active: bool = True
    history: list[dict] = Field(default_factory=list)
    pending_queue_sync_branch: str | None = None
    pending_queue_sync_started_at: datetime | None = None
    rate_limited_until: datetime | None = None
    rate_limit_reactive: bool = False
    usage_session_percent: int | None = None
    usage_session_resets_at: int | None = None
    usage_weekly_percent: int | None = None
    usage_weekly_resets_at: int | None = None
    usage_api_degraded: bool = False
