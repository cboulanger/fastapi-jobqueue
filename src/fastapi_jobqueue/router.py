"""
The REST API of a job manager. See README.md for the endpoints.

Streams (`/events`) send SSE by default and NDJSON with `Accept: application/x-ndjson`. A stream
starts with a snapshot of the job (or of all visible unfinished jobs), then sends every event
after it. With `Last-Event-ID` (SSE header, or `last_event_id` query parameter for NDJSON), a
reconnecting client gets the events it missed instead of a snapshot, if they are still kept. A
job stream ends after the job's final event. Keep-alives are SSE comments, or empty lines in
NDJSON.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from typing import TYPE_CHECKING, Any

from fastapi import APIRouter, Body, Depends, Header, HTTPException, Query, Request, Response
from fastapi.responses import StreamingResponse
from pydantic import JsonValue

from ._aio import Timeout, wait_or_timeout
from .auth import Principal
from .errors import JobQueueError, PermissionDenied, UnknownJobType
from .models import (
    BulkFilter,
    BulkResult,
    FinishRequest,
    HolderRequest,
    Job,
    JobEvent,
    JobStatus,
    ReportRequest,
    StepRequest,
    SubmitRequest,
)

if TYPE_CHECKING:
    from .manager import JobManager

KEEPALIVE_SECONDS = 15.0
NDJSON = "application/x-ndjson"


def _http(error: JobQueueError) -> HTTPException:
    return HTTPException(status_code=error.status_code, detail=error.message)


def _snapshot_event(job: Job, global_id: int | None = None) -> JobEvent:
    return JobEvent(
        id=global_id, seq=job.seq, job_id=job.id, type=job.type, status=job.status, progress=job.progress,
        message=job.message, data=job.data, result=job.result if job.status == "done" else None,
        error=job.error, cancel_requested=job.cancel_requested, at=job.updated_at,
    )


def _wants_ndjson(request: Request) -> bool:
    return NDJSON in request.headers.get("accept", "")


def _format(event: JobEvent, ndjson: bool, event_id: int | None) -> str:
    payload = event.model_dump_json()
    if ndjson:
        return payload + "\n"
    head = f"id: {event_id}\n" if event_id is not None else ""
    return f"{head}event: job\ndata: {payload}\n\n"


def _keepalive(ndjson: bool) -> str:
    return "\n" if ndjson else ": keepalive\n\n"


def _parse_last_id(header: str | None, query: int | None) -> int | None:
    if query is not None:
        return query
    if header is None:
        return None
    try:
        return int(header)
    except ValueError:
        return None


def create_router(manager: JobManager, **kwargs: Any) -> APIRouter:
    router = APIRouter(**kwargs)
    current_principal = manager.auth.current_principal

    async def run(coro: Any) -> Any:
        try:
            return await coro
        except JobQueueError as e:
            raise _http(e) from e

    def require_admin(principal: Principal) -> None:
        if not principal.is_admin:
            raise _http(PermissionDenied("Only admins can do this"))

    @router.post("", response_model=Job, status_code=201, summary="Submit a job")
    async def submit_job(
        body: SubmitRequest,
        principal: Principal = Depends(current_principal),
        idempotency_key: str | None = Header(None, alias="Idempotency-Key", max_length=128),
    ) -> Job:
        job_type = manager.types.get(body.type)
        if job_type is None or not job_type.rest or not manager.auth.may_submit(principal, body.type):
            raise _http(UnknownJobType(f"Job type cannot be submitted: {body.type}"))
        return await run(manager.submit(
            body.type, body.params, owner=principal.id, visibility=body.visibility,
            input_hash=body.input_hash, resources=body.resources, idempotency_key=idempotency_key,
        ))

    @router.get("", response_model=list[Job], summary="List visible jobs")
    async def list_jobs(
        status: list[JobStatus] | None = Query(None),
        type: str | None = Query(None),
        mine: bool = Query(False, description="Only the caller's jobs"),
        resumable: bool = Query(False, description="Only active and interrupted client jobs"),
        principal: Principal = Depends(current_principal),
    ) -> list[Job]:
        return await manager.list(viewer=principal, owner=principal.id if mine else None, statuses=status, job_type=type, resumable=resumable)

    @router.post("/cancel-all", response_model=BulkResult, summary="Cancel all matching jobs (admins)")
    async def cancel_all(body: BulkFilter = Body(default_factory=BulkFilter), principal: Principal = Depends(current_principal)) -> BulkResult:
        require_admin(principal)
        return BulkResult(count=await manager.cancel_all(job_type=body.type, owner=body.owner, statuses=body.status))

    @router.delete("", response_model=BulkResult, summary="Delete all matching jobs (admins)")
    async def delete_all(
        status: list[JobStatus] | None = Query(None),
        type: str | None = Query(None),
        owner: str | None = Query(None),
        principal: Principal = Depends(current_principal),
    ) -> BulkResult:
        require_admin(principal)
        return BulkResult(count=await manager.delete_all(job_type=type, owner=owner, statuses=status))

    @router.get("/events", summary="Stream the events of all visible jobs", response_class=StreamingResponse)
    async def all_events(
        request: Request,
        last_event_id_query: int | None = Query(None, alias="last_event_id"),
        last_event_id: str | None = Header(None, alias="Last-Event-ID"),
        mine: bool = Query(False),
        principal: Principal = Depends(current_principal),
    ) -> StreamingResponse:
        ndjson = _wants_ndjson(request)
        after = _parse_last_id(last_event_id, last_event_id_query)

        def accept(_event: JobEvent, owner: str, visibility: str) -> bool:
            if mine:
                return owner == principal.id
            return principal.is_admin or owner == principal.id or visibility == "users"

        subscription = manager.events.subscribe(accept)

        async def stream() -> AsyncIterator[str]:
            sent: dict[str, int] = {}
            try:
                start_id = await asyncio.to_thread(manager.store.max_event_id)
                backlog: list[JobEvent] = []
                if after is not None:
                    backlog = [e for e, owner, vis in await asyncio.to_thread(manager.store.events_since, after, 5000) if accept(e, owner, vis)]
                else:
                    jobs = await manager.list(viewer=principal, owner=principal.id if mine else None, statuses=("queued", "active", "interrupted"))
                    backlog = [_snapshot_event(job, start_id) for job in jobs]
                for event in backlog:
                    sent[event.job_id] = max(sent.get(event.job_id, 0), event.seq)
                    yield _format(event, ndjson, event.id)
                while not subscription.overflowed:
                    try:
                        event = await wait_or_timeout(subscription.queue.get(), KEEPALIVE_SECONDS)
                    except Timeout:
                        yield _keepalive(ndjson)
                        continue
                    if event.seq <= sent.get(event.job_id, 0):
                        continue
                    sent[event.job_id] = event.seq
                    yield _format(event, ndjson, event.id)
            finally:
                manager.events.unsubscribe(subscription)

        return StreamingResponse(stream(), media_type=NDJSON if ndjson else "text/event-stream", headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})

    @router.get("/{job_id}", response_model=Job, summary="Get a job", responses={304: {"description": "Not modified (If-None-Match)"}})
    async def get_job(
        job_id: str,
        response: Response,
        steps: bool = Query(False, description="Include the saved steps"),
        if_none_match: str | None = Header(None, alias="If-None-Match"),
        principal: Principal = Depends(current_principal),
    ) -> Job | Response:
        job = await run(manager.get(job_id, steps=steps, viewer=principal))
        etag = f'"{job.id}-{job.seq}"'
        if if_none_match == etag:
            return Response(status_code=304, headers={"ETag": etag})
        response.headers["ETag"] = etag
        return job

    @router.get("/{job_id}/events", summary="Stream the events of a job", response_class=StreamingResponse)
    async def job_events(
        job_id: str,
        request: Request,
        last_event_id_query: int | None = Query(None, alias="last_event_id"),
        last_event_id: str | None = Header(None, alias="Last-Event-ID"),
        principal: Principal = Depends(current_principal),
    ) -> StreamingResponse:
        ndjson = _wants_ndjson(request)
        after = _parse_last_id(last_event_id, last_event_id_query)
        subscription = manager.events.subscribe(lambda event, _owner, _visibility: event.job_id == job_id)
        try:
            job = await run(manager.get(job_id, viewer=principal))
        except HTTPException:
            manager.events.unsubscribe(subscription)
            raise

        async def stream() -> AsyncIterator[str]:
            try:
                backlog: list[JobEvent] = []
                if after is not None and after < job.seq:
                    oldest = await asyncio.to_thread(manager.store.oldest_seq, job_id)
                    if oldest is not None and oldest <= after + 1:
                        backlog = await asyncio.to_thread(manager.store.events_after, job_id, after, 5000)
                if not backlog and (after is None or after < job.seq):
                    backlog = [_snapshot_event(job)]
                last = after or 0
                for event in backlog:
                    last = event.seq
                    yield _format(event, ndjson, event.seq)
                    if event.status in ("done", "error", "cancelled", "expired"):
                        return
                if job.finished:
                    return
                while not subscription.overflowed:
                    try:
                        event = await wait_or_timeout(subscription.queue.get(), KEEPALIVE_SECONDS)
                    except Timeout:
                        if await asyncio.to_thread(manager.store.get, job_id) is None:
                            return
                        yield _keepalive(ndjson)
                        continue
                    if event.seq <= last:
                        continue
                    last = event.seq
                    yield _format(event, ndjson, event.seq)
                    if event.status in ("done", "error", "cancelled", "expired"):
                        return
            finally:
                manager.events.unsubscribe(subscription)

        return StreamingResponse(stream(), media_type=NDJSON if ndjson else "text/event-stream", headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})

    @router.post("/{job_id}/cancel", response_model=Job, summary="Cancel a job")
    async def cancel_job(job_id: str, principal: Principal = Depends(current_principal)) -> Job:
        job = await run(manager.get(job_id, viewer=principal))
        if not manager.can_cancel(principal, job):
            raise _http(PermissionDenied("Only the owner or an admin can cancel the job"))
        return await run(manager.cancel(job_id))

    @router.delete("/{job_id}", response_model=Job, summary="Delete a job")
    async def delete_job(job_id: str, principal: Principal = Depends(current_principal)) -> Job:
        job = await run(manager.get(job_id, viewer=principal))
        if not principal.is_admin:
            if job.owner != principal.id:
                raise _http(PermissionDenied("Only the owner or an admin can delete the job"))
            if not job.finished:
                raise HTTPException(status_code=409, detail="Cancel the job first; only admins can delete unfinished jobs")
        return await run(manager.delete(job_id))

    @router.post("/{job_id}/claim", response_model=Job, summary="Take or renew the lease of a client job (includes the steps)")
    async def claim_job(job_id: str, body: HolderRequest, principal: Principal = Depends(current_principal)) -> Job:
        return await run(manager.claim(job_id, body.holder, principal))

    @router.post("/{job_id}/heartbeat", response_model=Job, summary="Renew the lease of a client job")
    async def heartbeat(job_id: str, body: HolderRequest, principal: Principal = Depends(current_principal)) -> Job:
        return await run(manager.heartbeat(job_id, body.holder, principal))

    @router.put("/{job_id}/steps/{key}", status_code=204, summary="Save a step of a client job")
    async def save_step(job_id: str, key: str, body: StepRequest, principal: Principal = Depends(current_principal)) -> Response:
        result: JsonValue = body.result
        await run(manager.save_step(job_id, key, result, holder=body.holder, principal=principal))
        return Response(status_code=204)

    @router.post("/{job_id}/report", response_model=Job, summary="Report the progress of a client job")
    async def report(job_id: str, body: ReportRequest, principal: Principal = Depends(current_principal)) -> Job:
        return await run(manager.report(job_id, body.holder, progress=body.progress, message=body.message, data=body.data, principal=principal))

    @router.post("/{job_id}/finish", response_model=Job, summary="Finish a client job")
    async def finish(job_id: str, body: FinishRequest, principal: Principal = Depends(current_principal)) -> Job:
        return await run(manager.finish(job_id, body.holder, result=body.result, error=body.error, cancelled=body.cancelled, principal=principal))

    return router
