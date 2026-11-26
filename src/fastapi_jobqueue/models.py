"""Data model of jobs, their steps and events, and the request bodies of the REST API."""

from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field, JsonValue

JobStatus = Literal["queued", "active", "interrupted", "done", "error", "cancelled", "expired"]
"""
- queued: waits for a concurrency slot (server jobs) or for a client to claim it (client jobs)
- active: running
- interrupted: the lease expired; resumable until `expires_at`
- done, error, cancelled, expired: finished; deleted after the retention time
"""

Driver = Literal["server", "client"]
Visibility = Literal["owner", "users"]

FINISHED_STATUSES: frozenset[str] = frozenset({"done", "error", "cancelled", "expired"})
HOLDING_STATUSES: frozenset[str] = frozenset({"queued", "active", "interrupted"})
"""Statuses in which a job holds its resources."""


class Progress(BaseModel):
    """Progress of a job. All fields are optional; `value` null means indeterminate."""

    value: int | None = Field(default=None, ge=0, le=100, description="Percentage, or null if indeterminate")
    done: int | None = Field(default=None, ge=0, description="Units done in the current phase")
    total: int | None = Field(default=None, ge=0, description="Units of the current phase")
    unit: str | None = Field(default=None, description="Name of the units, e.g. 'pages'")
    phase: str | None = Field(default=None, description="Name of the current phase")
    eta_seconds: int | None = Field(default=None, ge=0, description="Estimated seconds until the phase is done")


class JobError(BaseModel):
    message: str
    status: int = Field(default=500, description="HTTP-like status: below 500 for errors caused by the request, 500 and above for failures")


class Lease(BaseModel):
    holder: str = Field(..., description="Id of the driver: a worker process or a client tab")
    expires_at: datetime


class Step(BaseModel):
    result: JsonValue
    at: datetime


class Job(BaseModel):
    """Snapshot of a job."""

    id: str
    type: str
    owner: str
    visibility: Visibility
    driver: Driver
    status: JobStatus
    progress: Progress = Field(default_factory=Progress)
    message: str | None = None
    data: JsonValue = None
    params: JsonValue = None
    input_hash: str | None = None
    result: JsonValue = None
    error: JobError | None = None
    resources: list[str] = Field(default_factory=list)
    lease: Lease | None = None
    cancel_requested: bool = False
    attempts: int = Field(default=0, description="How often a server job was started; more than 1 after a resume")
    seq: int = Field(default=0, description="Sequence number of the job's latest event")
    created_at: datetime
    updated_at: datetime
    finished_at: datetime | None = None
    expires_at: datetime | None = Field(default=None, description="When an interrupted job is discarded, or a finished one deleted")
    steps: dict[str, Step] | None = Field(default=None, description="Checkpoints; only included where stated")

    @property
    def finished(self) -> bool:
        return self.status in FINISHED_STATUSES


class JobEvent(BaseModel):
    """A change of a job, as sent to subscribers."""

    id: int | None = Field(default=None, description="Global event id (for the stream of all jobs); null for snapshots")
    seq: int = Field(..., description="Per-job sequence number")
    job_id: str
    type: str
    status: JobStatus
    progress: Progress
    message: str | None = None
    data: JsonValue = None
    result: JsonValue = Field(default=None, description="Only for status 'done'")
    error: JobError | None = Field(default=None, description="Only for status 'error'")
    cancel_requested: bool = False
    at: datetime


# ── Request bodies ──────────────────────────────────────────────────────────


class SubmitRequest(BaseModel):
    type: str
    params: JsonValue = None
    visibility: Visibility = "owner"
    input_hash: str | None = None
    resources: list[str] = Field(default_factory=list)


class HolderRequest(BaseModel):
    holder: str = Field(..., min_length=1, max_length=128)


class StepRequest(HolderRequest):
    result: JsonValue = None


class ReportRequest(HolderRequest):
    progress: Progress | None = None
    message: str | None = None
    data: JsonValue = None


class FinishRequest(HolderRequest):
    result: JsonValue = None
    error: JobError | None = None
    cancelled: bool = False


class BulkFilter(BaseModel):
    type: str | None = None
    owner: str | None = None
    status: list[JobStatus] | None = None


class BulkResult(BaseModel):
    count: int
