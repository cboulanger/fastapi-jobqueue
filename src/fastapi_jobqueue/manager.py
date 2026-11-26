"""
The job manager: registers job types, submits, runs, cancels and resumes jobs, and keeps the
store tidy.

Each worker process of a host has one manager on the shared store. Background tasks of the
manager:

- dispatcher: starts queued server jobs of the types this worker has handlers for, as asyncio
  tasks, within each type's `max_concurrent` limit across all workers
- monitor (one per running job): renews the job's lease, picks up cancel requests, writes
  coalesced progress
- maintenance: handles expired leases (a resumable server job is queued again, a client job
  becomes interrupted), discards interrupted jobs after their resume window and deletes finished
  jobs after their retention time
- event bus: delivers events to this worker's subscribers (see events.py)
"""

from __future__ import annotations

import asyncio
import logging
import os
import secrets
import socket
import time
from collections.abc import AsyncIterator, Awaitable, Callable, Collection, Mapping
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Literal

from fastapi import APIRouter, FastAPI
from pydantic import BaseModel, JsonValue, ValidationError

from ._aio import Timeout, wait_or_timeout
from .auth import JobAuth, JobHooks, Principal, call_hook
from .errors import (
    InputChanged,
    InvalidParams,
    InvalidState,
    JobCancelled,
    JobNotFound,
    LeaseConflict,
    PermissionDenied,
    UnknownJobType,
)
from .events import EventBus
from .models import FINISHED_STATUSES, Driver, Job, JobError, Progress, Step, Visibility
from .store import SqliteJobStore

logger = logging.getLogger(__name__)

Handler = Callable[["JobContext", Any], Awaitable[Any]]
"""A server job handler: receives the context and the parameters, returns a JSON-serializable result."""

MAX_ATTEMPTS = 3
"""A server job that was interrupted this many times (e.g. it crashes its worker) fails."""


@dataclass
class JobType:
    """A registered job type."""

    name: str
    driver: Driver
    handler: Handler | None = None
    max_concurrent: int | None = None
    resumable: bool = False
    cancel_task: bool = False
    params_model: type[BaseModel] | None = None
    resume_window: float | None = None
    retention: float | None = None
    rest: bool = True


_NO_CHANGE: Any = object()


class JobContext:
    """
    What a handler of a server job gets: its parameters, its steps, and the means to report
    progress and to notice a cancel.
    """

    def __init__(self, manager: JobManager, job: Job, steps: dict[str, Step]):
        self._manager = manager
        self.job = job
        self._steps = steps
        self._progress = job.progress
        self._message = job.message
        self._data: JsonValue = job.data
        self._dirty = False
        self._last_write = 0.0
        self._cancelled = False
        self._loop = asyncio.get_running_loop()

    @property
    def job_id(self) -> str:
        return self.job.id

    @property
    def params(self) -> JsonValue:
        return self.job.params

    @property
    def attempt(self) -> int:
        """1 for the first run, higher when the job was resumed after an interruption."""
        return self.job.attempts

    @property
    def cancel_requested(self) -> bool:
        return self._cancelled

    def has_step(self, key: str) -> bool:
        return key in self._steps

    def step(self, key: str) -> JsonValue:
        """The result of a saved step; KeyError if it was not saved."""
        return self._steps[key].result

    @property
    def steps(self) -> dict[str, JsonValue]:
        return {key: step.result for key, step in self._steps.items()}

    async def save_step(self, key: str, result: JsonValue = None) -> None:
        """Save a checkpoint; a resumed run finds it with `has_step()` / `step()`."""
        self.check_cancelled()
        now = self._manager.clock()
        await asyncio.to_thread(self._manager.store.save_step, self.job.id, key, result, now)
        self._steps[key] = Step(result=result, at=datetime.fromtimestamp(now, timezone.utc))

    def check_cancelled(self) -> None:
        """Raise `JobCancelled` if a cancel was requested. Safe to call from other threads."""
        if self._cancelled:
            raise JobCancelled()

    async def report(
        self,
        value: int | None = _NO_CHANGE,
        *,
        done: int | None = _NO_CHANGE,
        total: int | None = _NO_CHANGE,
        unit: str | None = _NO_CHANGE,
        phase: str | None = _NO_CHANGE,
        eta_seconds: int | None = _NO_CHANGE,
        message: str | None = _NO_CHANGE,
        data: JsonValue = _NO_CHANGE,
    ) -> None:
        """
        Report progress; omitted fields keep their value. Without `value`, it is computed from
        `done` and `total` when both are known. Writes are coalesced to one per `report_interval`;
        a change of the phase or the message is written at once.

        Raises:
            JobCancelled: if a cancel was requested
        """
        self.check_cancelled()
        urgent = self._apply(value, done, total, unit, phase, eta_seconds, message, data)
        if urgent or self._manager.clock() - self._last_write >= self._manager.report_interval:
            await self.flush()

    def report_threadsafe(self, **fields: Any) -> None:
        """
        `report()` for code that runs in another thread (e.g. via `asyncio.to_thread`). Takes the
        same keyword arguments, with the percentage as `value`.

        Raises:
            JobCancelled: if a cancel was requested
        """
        self.check_cancelled()
        self._loop.call_soon_threadsafe(self._apply_from_thread, fields)

    def _apply_from_thread(self, fields: dict[str, Any]) -> None:
        self._apply(
            fields.get("value", _NO_CHANGE), fields.get("done", _NO_CHANGE), fields.get("total", _NO_CHANGE),
            fields.get("unit", _NO_CHANGE), fields.get("phase", _NO_CHANGE), fields.get("eta_seconds", _NO_CHANGE),
            fields.get("message", _NO_CHANGE), fields.get("data", _NO_CHANGE),
        )

    def _apply(self, value: Any, done: Any, total: Any, unit: Any, phase: Any, eta_seconds: Any, message: Any, data: Any) -> bool:
        """Apply a report to the pending state; returns whether it should be written at once."""
        changes = {
            name: field_value
            for name, field_value in (("value", value), ("done", done), ("total", total), ("unit", unit), ("phase", phase), ("eta_seconds", eta_seconds))
            if field_value is not _NO_CHANGE
        }
        urgent = False
        if changes:
            merged = {**self._progress.model_dump(), **changes}
            if "value" not in changes and merged["total"] and merged["done"] is not None:
                merged["value"] = min(100, round(merged["done"] * 100 / merged["total"]))
            if "phase" in changes and changes["phase"] != self._progress.phase:
                urgent = True
            self._progress = Progress.model_validate(merged)
        if message is not _NO_CHANGE and message != self._message:
            self._message = message
            urgent = True
        if data is not _NO_CHANGE:
            self._data = data
        self._dirty = self._dirty or bool(changes) or message is not _NO_CHANGE or data is not _NO_CHANGE
        return urgent

    async def flush(self) -> None:
        """Write pending progress now."""
        if not self._dirty:
            return
        self._dirty = False
        self._last_write = self._manager.clock()
        job = await asyncio.to_thread(
            self._manager.store.update, self.job.id,
            {"progress": self._progress, "message": self._message, "data": self._data},
            now=self._last_write, expect_status=("active",), expect_holder=self._manager.worker_id,
        )
        self._manager._poke()
        if job is not None:
            self.job = job

    def _flush_due(self) -> bool:
        return self._dirty and self._manager.clock() - self._last_write >= self._manager.report_interval


@dataclass
class _Running:
    job_id: str
    task: asyncio.Task[None]
    context: JobContext
    job_type: JobType
    lost: bool = False


class JobManager:
    """Registers job types and submits, runs, cancels and resumes jobs on a shared store."""

    def __init__(
        self,
        store: SqliteJobStore,
        *,
        auth: JobAuth | None = None,
        hooks: JobHooks | None = None,
        retention: float = 3600,
        resume_window: float = 900,
        lease_ttl: float = 30,
        poll_interval: float = 0.5,
        report_interval: float = 1.0,
        maintenance_interval: float = 5.0,
        worker_id: str | None = None,
        clock: Callable[[], float] = time.time,
    ):
        """
        Args:
            store: The store shared by all workers
            auth: Authentication of the REST API
            hooks: Resource and input hooks of the host
            retention: Seconds a finished job is kept
            resume_window: Seconds an interrupted job can be resumed after its lease expired
            lease_ttl: Seconds a lease lasts without a heartbeat
            poll_interval: Seconds between checks for cancel requests and new events
            report_interval: Minimum seconds between two progress writes of a job
            maintenance_interval: Seconds between runs of the maintenance task
            worker_id: Lease holder id of this worker; by default host, pid and a random part
            clock: Current time in epoch seconds (for tests)
        """
        self.store = store
        self.auth = auth or JobAuth()
        self.hooks = hooks or JobHooks()
        self.retention = retention
        self.resume_window = resume_window
        self.lease_ttl = lease_ttl
        self.poll_interval = poll_interval
        self.report_interval = report_interval
        self.maintenance_interval = maintenance_interval
        self.worker_id = worker_id or f"worker:{socket.gethostname()}:{os.getpid()}:{secrets.token_hex(3)}"
        self.clock = clock
        self.types: dict[str, JobType] = {}
        self.events = EventBus(store, poll_interval)
        self._running: dict[str, _Running] = {}
        self._tasks: list[asyncio.Task[None]] = []
        self._dispatch_wake: asyncio.Event | None = None
        self._started = False
        self._stopping = False

    # ── Registration ────────────────────────────────────────────────────────

    def handler(
        self,
        name: str,
        *,
        max_concurrent: int | None = None,
        resumable: bool = False,
        cancel_task: bool = False,
        params_model: type[BaseModel] | None = None,
        resume_window: float | None = None,
        retention: float | None = None,
        rest: bool = True,
    ) -> Callable[[Handler], Handler]:
        """
        Register the handler of a server job type (decorator).

        Args:
            name: Job type, namespaced by plugin (e.g. "ocr", "my-plugin.import")
            max_concurrent: Maximum number of active jobs of this type across all workers
            resumable: After an interruption (worker restart), run the handler again with the saved
                steps; otherwise the job fails
            cancel_task: On cancel, also cancel the handler's asyncio task, which interrupts an
                awaited call (e.g. an HTTP request) instead of waiting for the next report
            params_model: Validates the parameters; the handler gets an instance of it
            resume_window, retention: Override the manager's defaults
            rest: Whether the type can be submitted through the REST API
        """
        def register(handler: Handler) -> Handler:
            self._register(JobType(name, "server", handler, max_concurrent, resumable, cancel_task, params_model, resume_window, retention, rest))
            return handler
        return register

    def register_type(
        self,
        name: str,
        *,
        driver: Driver = "client",
        max_concurrent: int | None = None,
        params_model: type[BaseModel] | None = None,
        resume_window: float | None = None,
        retention: float | None = None,
        rest: bool = True,
    ) -> None:
        """
        Register a job type without a handler in this process.

        - driver "client": a client drives its jobs through the REST API
        - driver "server": the jobs are submitted here but run by other workers that register a
          handler for the type (e.g. an API-only process next to job workers)
        """
        self._register(JobType(name, driver, None, max_concurrent, driver == "client", False, params_model, resume_window, retention, rest))

    def _register(self, job_type: JobType) -> None:
        if job_type.name in self.types:
            raise ValueError(f"Job type already registered: {job_type.name}")
        self.types[job_type.name] = job_type

    def _type(self, name: str) -> JobType:
        job_type = self.types.get(name)
        if job_type is None:
            raise UnknownJobType(f"Unknown job type: {name}")
        return job_type

    def _retention(self, job_type: JobType | None) -> float:
        return job_type.retention if job_type and job_type.retention is not None else self.retention

    def _resume_window(self, job_type: JobType | None) -> float:
        return job_type.resume_window if job_type and job_type.resume_window is not None else self.resume_window

    # ── Lifecycle ───────────────────────────────────────────────────────────

    async def start(self) -> None:
        """Start the background tasks of this worker."""
        if self._started:
            return
        self._started = True
        self._stopping = False
        self._dispatch_wake = asyncio.Event()
        await self.events.start()
        self._tasks = [
            asyncio.create_task(self._dispatch_loop(), name="fastapi-jobqueue-dispatch"),
            asyncio.create_task(self._maintenance_loop(), name="fastapi-jobqueue-maintenance"),
        ]

    async def stop(self) -> None:
        """
        Stop the background tasks. Running server jobs are stopped and their leases released, so
        that another worker (or this one after a restart) resumes a resumable job at once.
        """
        if not self._started:
            return
        self._stopping = True
        for running in list(self._running.values()):
            running.task.cancel()
        await asyncio.gather(*(running.task for running in list(self._running.values())), return_exceptions=True)
        for task in self._tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks = []
        await self.events.stop()
        self._started = False

    @asynccontextmanager
    async def lifespan(self, _app: FastAPI | None = None) -> AsyncIterator[None]:
        """Lifespan for FastAPI (`FastAPI(lifespan=jobs.lifespan)`), or to wrap in the host's own."""
        await self.start()
        try:
            yield
        finally:
            await self.stop()

    def router(self, **kwargs: Any) -> APIRouter:
        """The REST API; keyword arguments go to `APIRouter` (e.g. `tags`). Mount it with a prefix."""
        from .router import create_router
        return create_router(self, **kwargs)

    def _poke(self) -> None:
        self.events.poke()

    def _kick(self) -> None:
        if self._dispatch_wake is not None:
            self._dispatch_wake.set()

    # ── Submitting and reading ─────────────────────────────────────────────

    async def submit(
        self,
        job_type: str,
        params: JsonValue | BaseModel = None,
        *,
        owner: str,
        visibility: Visibility = "owner",
        input_hash: str | None = None,
        resources: Collection[str] = (),
        idempotency_key: str | None = None,
    ) -> Job:
        """
        Submit a job. A server job is queued and started by a worker; a client job is queued
        until a client claims it.

        Args:
            idempotency_key: A second submit of the same owner with the same key returns the
                first job instead of creating another one

        Raises:
            UnknownJobType, InvalidParams, ResourceBusy (from the host's acquire hook)
        """
        registered = self._type(job_type)
        if isinstance(params, BaseModel):
            params = params.model_dump(mode="json")
        if registered.params_model is not None:
            try:
                params = registered.params_model.model_validate(params).model_dump(mode="json")
            except ValidationError as e:
                raise InvalidParams(str(e)) from e
        if idempotency_key is not None:
            existing = await asyncio.to_thread(self.store.find_by_idempotency_key, owner, idempotency_key)
            if existing is not None:
                return existing
        now = self.clock()
        moment = datetime.fromtimestamp(now, timezone.utc)
        job = Job(
            id=secrets.token_urlsafe(12),
            type=job_type,
            owner=owner,
            visibility=visibility,
            driver=registered.driver,
            status="queued",
            params=params,
            input_hash=input_hash,
            resources=list(resources),
            created_at=moment,
            updated_at=moment,
        )
        if job.resources:
            await call_hook(self.hooks.acquire(job))
        stored, created = await asyncio.to_thread(self.store.insert, job, idempotency_key)
        if not created and job.resources:
            await call_hook(self.hooks.release(job))
        self._poke()
        if registered.driver == "server":
            self._kick()
        logger.info(f"Submitted {job_type} job {stored.id} of {owner}")
        return stored

    async def get(self, job_id: str, *, steps: bool = False, viewer: Principal | None = None) -> Job:
        """
        Raises:
            JobNotFound: if the job does not exist or is not visible to the viewer
        """
        job = await asyncio.to_thread(self.store.get, job_id, steps=steps)
        if job is None or (viewer is not None and not self.can_view(viewer, job)):
            raise JobNotFound(f"Job not found: {job_id}")
        return job

    async def list(
        self,
        *,
        viewer: Principal | None = None,
        owner: str | None = None,
        statuses: Collection[str] | None = None,
        job_type: str | None = None,
        resumable: bool = False,
    ) -> list[Job]:
        """
        List jobs, oldest first.

        Args:
            resumable: Only active and interrupted client jobs, i.e. the jobs a client may claim to resume them
        """
        if resumable:
            statuses = [s for s in (statuses or ("active", "interrupted")) if s in ("active", "interrupted")]
        jobs = await asyncio.to_thread(self.store.list, viewer=viewer, owner=owner, statuses=statuses, job_type=job_type, driver="client" if resumable else None)
        return jobs

    @staticmethod
    def can_view(principal: Principal, job: Job) -> bool:
        return principal.is_admin or job.owner == principal.id or job.visibility == "users"

    @staticmethod
    def can_cancel(principal: Principal, job: Job) -> bool:
        return principal.is_admin or job.owner == principal.id

    # ── Cancelling and deleting ────────────────────────────────────────────

    async def cancel(self, job_id: str) -> Job:
        """
        Cancel a job. A queued or interrupted job is cancelled at once; an active one gets
        `cancel_requested` and becomes 'cancelled' when its driver stops. A finished job is
        returned as it is.
        """
        while True:
            job = await self.get(job_id)
            if job.finished:
                return job
            now = self.clock()
            if job.status in ("queued", "interrupted"):
                changed = await self._finish(job, "cancelled", now=now, expect_status=(job.status,))
            elif job.cancel_requested:
                return job
            else:
                changed = await asyncio.to_thread(self.store.update, job_id, {"cancel_requested": True}, now=now, expect_status=("active",))
                if changed is not None:
                    running = self._running.get(job_id)
                    if running is not None:
                        running.context._cancelled = True
                        if running.job_type.cancel_task:
                            running.task.cancel()
            self._poke()
            if changed is not None:
                logger.info(f"Cancel of job {job_id} requested")
                return changed

    async def delete(self, job_id: str) -> Job:
        """Delete a job, whatever its status; an active one is stopped. Releases its resources."""
        job = await asyncio.to_thread(self.store.delete, job_id)
        if job is None:
            raise JobNotFound(f"Job not found: {job_id}")
        running = self._running.get(job_id)
        if running is not None:
            running.lost = True
            running.task.cancel()
        if job.status not in FINISHED_STATUSES and job.resources:
            await self._release(job)
        logger.info(f"Deleted job {job_id}")
        return job

    async def cancel_all(self, *, job_type: str | None = None, owner: str | None = None, statuses: Collection[str] | None = None) -> int:
        """Cancel all unfinished jobs matching the filters; returns how many were cancelled or asked to stop."""
        wanted = [s for s in (statuses or ("queued", "active", "interrupted")) if s not in FINISHED_STATUSES]
        count = 0
        for job in await asyncio.to_thread(self.store.list, owner=owner, statuses=wanted, job_type=job_type):
            try:
                await self.cancel(job.id)
                count += 1
            except JobNotFound:
                pass
        return count

    async def delete_all(self, *, job_type: str | None = None, owner: str | None = None, statuses: Collection[str] | None = None) -> int:
        """Delete all jobs matching the filters; returns how many were deleted."""
        count = 0
        for job in await asyncio.to_thread(self.store.list, owner=owner, statuses=statuses, job_type=job_type):
            try:
                await self.delete(job.id)
                count += 1
            except JobNotFound:
                pass
        return count

    async def _finish(
        self,
        job: Job,
        status: Literal["done", "error", "cancelled", "expired"],
        *,
        now: float,
        result: JsonValue = None,
        error: JobError | None = None,
        expect_status: Collection[str] = ("active",),
        expect_holder: str | None = None,
        lease_expired_before: float | None = None,
        progress: Progress | None = None,
        message: str | None = _NO_CHANGE,
        data: JsonValue = _NO_CHANGE,
    ) -> Job | None:
        """Move a job to a finished status, if it still matches the expectations, and release its resources."""
        changes: dict[str, Any] = {
            "status": status,
            "result": result if status == "done" else None,
            "error": error if status == "error" else None,
            "lease_holder": None,
            "lease_expires": None,
            "finished_at": now,
            "expires_at": now + self._retention(self.types.get(job.type)),
        }
        if progress is not None:
            changes["progress"] = progress
        if message is not _NO_CHANGE:
            changes["message"] = message
        if data is not _NO_CHANGE:
            changes["data"] = data
        kwargs: dict[str, Any] = {"expect_status": expect_status}
        if expect_holder is not None:
            kwargs["expect_holder"] = expect_holder
        if lease_expired_before is not None:
            kwargs["lease_expired_before"] = lease_expired_before
        changed = await asyncio.to_thread(self.store.update, job.id, changes, now=now, **kwargs)
        if changed is not None:
            self._poke()
            if changed.resources:
                await self._release(changed)
            logger.info(f"Job {job.id} ({job.type}) {status}")
        return changed

    async def _release(self, job: Job) -> None:
        try:
            await call_hook(self.hooks.release(job))
        except Exception:
            logger.exception(f"Could not release the resources of job {job.id}")

    # ── Client driver ──────────────────────────────────────────────────────

    async def claim(self, job_id: str, holder: str, principal: Principal | None = None) -> Job:
        """
        Take or renew the lease of a client job, to start it, take it over or resume it. The
        returned job includes its steps.

        Raises:
            JobNotFound, PermissionDenied (not the owner), InvalidState (finished, or not a client
            job), LeaseConflict (another holder's lease is valid), InputChanged (the job was discarded)
        """
        job = await self._client_job(job_id, principal)
        if job.finished:
            raise InvalidState(f"The job is {job.status}")
        now = self.clock()
        if job.status == "interrupted":
            if job.expires_at is not None and job.expires_at.timestamp() < now:
                raise InvalidState("The job can no longer be resumed")
            if await call_hook(self.hooks.input_changed(job)):
                await self._finish(job, "expired", now=now, expect_status=("interrupted",), message="The input of the job changed, so it was not resumed.")
                raise InputChanged("The input of the job changed since it was submitted, so it cannot be resumed")
        claimed = await asyncio.to_thread(self.store.claim, job_id, holder, now + self.lease_ttl, now, from_statuses=("queued", "active", "interrupted"), resume_until=now)
        if claimed is None:
            raise LeaseConflict("Another driver holds the job's lease")
        self._poke()
        if claimed.resources:
            await call_hook(self.hooks.renew(claimed))
        claimed.steps = await asyncio.to_thread(self.store.steps, job_id)
        return claimed

    async def heartbeat(self, job_id: str, holder: str, principal: Principal | None = None) -> Job:
        """
        Renew the lease of a client job. The returned job says whether a cancel was requested.

        Raises:
            LeaseConflict: if the holder no longer holds the lease (e.g. it expired and another tab claimed the job)
        """
        await self._client_job(job_id, principal)
        now = self.clock()
        job = await asyncio.to_thread(self.store.update, job_id, {"lease_expires": now + self.lease_ttl}, now=now, expect_status=("active",), expect_holder=holder, event=False)
        if job is None:
            raise LeaseConflict("The lease is not held by this holder")
        if job.resources:
            await call_hook(self.hooks.renew(job))
        return job

    async def save_step(self, job_id: str, key: str, result: JsonValue = None, *, holder: str | None = None, principal: Principal | None = None) -> None:
        """
        Save a step of a job. Through the REST API, only the lease holder may; host code (e.g. a
        route that does the work of a client job's step) passes no holder.
        """
        if holder is not None:
            await self._require_holder(job_id, holder, principal)
        elif (await self.get(job_id)).finished:
            raise InvalidState("The job is finished")
        if not await asyncio.to_thread(self.store.save_step, job_id, key, result, self.clock()):
            raise JobNotFound(f"Job not found: {job_id}")

    async def report(
        self,
        job_id: str,
        holder: str,
        *,
        progress: Progress | None = None,
        message: str | None = None,
        data: JsonValue = None,
        principal: Principal | None = None,
    ) -> Job:
        """Report the progress of a client job; renews the lease. Fields that are None keep their value."""
        await self._require_holder(job_id, holder, principal)
        now = self.clock()
        changes: dict[str, Any] = {"lease_expires": now + self.lease_ttl}
        if progress is not None:
            changes["progress"] = progress
        if message is not None:
            changes["message"] = message
        if data is not None:
            changes["data"] = data
        job = await asyncio.to_thread(self.store.update, job_id, changes, now=now, expect_status=("active",), expect_holder=holder)
        if job is None:
            raise LeaseConflict("The lease is not held by this holder")
        self._poke()
        return job

    async def finish(
        self,
        job_id: str,
        holder: str,
        *,
        result: JsonValue = None,
        error: JobError | None = None,
        cancelled: bool = False,
        principal: Principal | None = None,
    ) -> Job:
        """Finish a client job: 'cancelled' if `cancelled`, else 'error' if `error`, else 'done'."""
        job = await self._require_holder(job_id, holder, principal)
        status: Literal["done", "error", "cancelled"] = "cancelled" if cancelled else "error" if error is not None else "done"
        finished = await self._finish(job, status, now=self.clock(), result=result, error=error, expect_holder=holder)
        if finished is None:
            raise LeaseConflict("The lease is not held by this holder")
        return finished

    async def _client_job(self, job_id: str, principal: Principal | None) -> Job:
        job = await self.get(job_id, viewer=principal)
        if principal is not None and job.owner != principal.id:
            raise PermissionDenied("Only the owner can drive a job")
        if job.driver != "client":
            raise InvalidState("Not a client job")
        return job

    async def _require_holder(self, job_id: str, holder: str, principal: Principal | None) -> Job:
        job = await self._client_job(job_id, principal)
        if job.status != "active" or job.lease is None or job.lease.holder != holder:
            raise LeaseConflict("The lease is not held by this holder")
        return job

    # ── Server driver ──────────────────────────────────────────────────────

    async def _dispatch_loop(self) -> None:
        assert self._dispatch_wake is not None
        while True:
            try:
                await wait_or_timeout(self._dispatch_wake.wait(), self.poll_interval * 4)
            except Timeout:
                pass
            self._dispatch_wake.clear()
            try:
                await self._dispatch()
            except Exception:
                logger.exception("Could not start queued jobs")

    async def _dispatch(self) -> None:
        limits = {name: t.max_concurrent for name, t in self.types.items() if t.driver == "server" and t.handler is not None}
        while not self._stopping:
            now = self.clock()
            job = await asyncio.to_thread(self.store.claim_queued, limits, self.worker_id, now + self.lease_ttl, now)
            if job is None:
                return
            self._poke()
            await self._start(job)

    async def _start(self, job: Job) -> None:
        job_type = self.types[job.type]
        steps = await asyncio.to_thread(self.store.steps, job.id)
        context = JobContext(self, job, steps)
        context._cancelled = job.cancel_requested
        task = asyncio.create_task(self._run(job, job_type, context), name=f"fastapi-jobqueue-job-{job.id}")
        self._running[job.id] = _Running(job.id, task, context, job_type)

    async def _run(self, job: Job, job_type: JobType, context: JobContext) -> None:
        assert job_type.handler is not None
        monitor = asyncio.create_task(self._monitor(job.id))
        try:
            params: Any = job.params
            if job_type.params_model is not None:
                params = job_type.params_model.model_validate(job.params)
            try:
                if context.cancel_requested:
                    raise JobCancelled()
                result = await job_type.handler(context, params)
            except JobCancelled:
                await self._finish_run(job, context, "cancelled")
            except asyncio.CancelledError:
                running = self._running.get(job.id)
                if running is not None and running.lost:
                    return
                if self._stopping:
                    await self._interrupt_run(job, job_type)
                    raise
                if context.cancel_requested:
                    await self._finish_run(job, context, "cancelled")
                else:
                    raise
            except Exception as e:
                logger.exception(f"Job {job.id} ({job.type}) failed")
                status = getattr(e, "status_code", 500)
                await self._finish_run(job, context, "error", error=JobError(message=str(e) or type(e).__name__, status=status if isinstance(status, int) else 500))
            else:
                await self._finish_run(job, context, "done", result=result)
        finally:
            monitor.cancel()
            self._running.pop(job.id, None)
            self._kick()

    async def _finish_run(self, job: Job, context: JobContext, status: Literal["done", "error", "cancelled"], *, result: JsonValue = None, error: JobError | None = None) -> None:
        await asyncio.sleep(0)  # apply reports that threads scheduled with report_threadsafe()
        progress = context._progress
        if status == "done" and progress.total:
            progress = progress.model_copy(update={"value": 100, "done": progress.total})
        elif status == "done" and progress.value is not None:
            progress = progress.model_copy(update={"value": 100})
        await self._finish(job, status, now=self.clock(), result=result, error=error, expect_holder=self.worker_id, progress=progress, message=context._message, data=context._data)

    async def _interrupt_run(self, job: Job, job_type: JobType) -> None:
        """The worker stops: release the lease, so that the job is resumed or fails at once."""
        now = self.clock()
        await asyncio.shield(asyncio.to_thread(self.store.update, job.id, {"lease_expires": now - 1}, now=now, expect_status=("active",), expect_holder=self.worker_id, event=False))
        logger.info(f"Job {job.id} ({job_type.name}) interrupted, since the worker stops")

    async def _monitor(self, job_id: str) -> None:
        """Renew the lease of a running job, pick up cancel requests and write coalesced progress."""
        last_renew = self.clock()
        while True:
            await asyncio.sleep(self.poll_interval)
            running = self._running.get(job_id)
            if running is None:
                return
            try:
                job = await asyncio.to_thread(self.store.get, job_id)
                if job is None or job.status != "active" or job.lease is None or job.lease.holder != self.worker_id:
                    # deleted, or the lease was lost (e.g. this worker hung) and the job taken over
                    logger.warning(f"Job {job_id} is no longer run by this worker; stopping it")
                    running.lost = True
                    running.task.cancel()
                    return
                if job.cancel_requested and not running.context._cancelled:
                    running.context._cancelled = True
                    if running.job_type.cancel_task:
                        running.task.cancel()
                now = self.clock()
                if now - last_renew >= self.lease_ttl / 3:
                    renewed = await asyncio.to_thread(self.store.update, job_id, {"lease_expires": now + self.lease_ttl}, now=now, expect_status=("active",), expect_holder=self.worker_id, event=False)
                    last_renew = now
                    if renewed is not None and renewed.resources:
                        await call_hook(self.hooks.renew(renewed))
                if running.context._flush_due():
                    await running.context.flush()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception(f"Monitoring job {job_id} failed")

    # ── Maintenance ────────────────────────────────────────────────────────

    async def _maintenance_loop(self) -> None:
        while True:
            try:
                await self.maintain()
            except Exception:
                logger.exception("Job maintenance failed")
            await asyncio.sleep(self.maintenance_interval)

    async def maintain(self) -> None:
        """
        Handle expired leases and expired jobs. Runs periodically in every worker; the
        conditional updates of the store make sure that each change happens once.
        """
        now = self.clock()
        for job in await asyncio.to_thread(self.store.due_leases, now):
            await self._lease_expired(job, now)
        for job in await asyncio.to_thread(self.store.due_expiry, now):
            if job.status == "interrupted":
                await self._finish(job, "expired", now=now, expect_status=("interrupted",), message="The job was not resumed in time.")
            elif job.finished:
                deleted = await asyncio.to_thread(self.store.delete, job.id)
                if deleted is not None:
                    logger.info(f"Deleted finished job {job.id}")

    async def _lease_expired(self, job: Job, now: float) -> None:
        job_type = self.types.get(job.type)
        expired_lease = {"expect_status": ("active",), "lease_expired_before": now}
        if job.cancel_requested:
            await self._finish(job, "cancelled", now=now, **expired_lease)
        elif job.driver == "client":
            changed = await asyncio.to_thread(
                self.store.update, job.id,
                {"status": "interrupted", "lease_holder": None, "lease_expires": None, "expires_at": now + self._resume_window(job_type)},
                now=now, **expired_lease,
            )
            if changed is not None:
                self._poke()
                logger.info(f"Client job {job.id} interrupted")
        elif (job_type is None or job_type.handler is None) and job.lease is not None and job.lease.expires_at.timestamp() > now - self._resume_window(job_type):
            # a worker with a handler for the type knows whether it is resumable; this one waits
            # for such a worker and fails the job only when none took it up within the resume window
            return
        elif job_type is not None and job_type.resumable and job.attempts < MAX_ATTEMPTS:
            changed = await asyncio.to_thread(self.store.update, job.id, {"status": "queued", "lease_holder": None, "lease_expires": None}, now=now, **expired_lease)
            if changed is not None:
                self._poke()
                self._kick()
                logger.info(f"Server job {job.id} queued again after an interruption")
        else:
            if job_type is None or job_type.handler is None:
                reason = "The job was interrupted, and no worker can run it."
            elif job_type.resumable:
                reason = "The job was interrupted too often."
            else:
                reason = "The job was interrupted by a server restart."
            await self._finish(job, "error", now=now, error=JobError(message=reason, status=503), **expired_lease)
