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
    history: list[dict] = Field(default_factory=list)
