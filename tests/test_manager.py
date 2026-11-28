from __future__ import annotations

import asyncio
from pathlib import Path

import pytest
from pydantic import BaseModel

from fastapi_jobqueue import (
    InputChanged,
    InvalidParams,
    JobCancelled,
    JobContext,
    JobManager,
    JobNotFound,
    LeaseConflict,
    Progress,
    ResourceBusy,
    SqliteJobStore,
    UnknownJobType,
)

from .conftest import Clock, RecordingHooks, make_manager, wait_for, wait_status

pytestmark = pytest.mark.anyio


class CountParams(BaseModel):
    count: int


async def test_server_job_runs_with_progress_steps_and_result(manager: JobManager) -> None:
    @manager.handler("count", params_model=CountParams)
    async def count(ctx: JobContext, params: CountParams) -> dict[str, int]:
        for i in range(params.count):
            await ctx.report(done=i + 1, total=params.count, unit="items", phase="Counting")
            await ctx.save_step(f"item-{i}", i * 2)
        return {"sum": sum(ctx.step(f"item-{i}") for i in range(params.count))}  # type: ignore[misc]

    await manager.start()
    job = await manager.submit("count", {"count": 4}, owner="alice")
    assert job.status == "queued" and job.driver == "server"
    done = await wait_status(manager, job.id, "done")
    assert done.result == {"sum": 12}
    assert done.progress.value == 100 and done.progress.done == 4 and done.progress.phase == "Counting"
    assert done.steps is not None and set(done.steps) == {"item-0", "item-1", "item-2", "item-3"}
    assert done.lease is None and done.finished_at is not None and done.expires_at is not None
    events = manager.store.events_after(job.id, 0)
    assert [e.status for e in events][0] == "queued" and events[-1].status == "done"
    assert events[-1].result == {"sum": 12}


async def test_params_are_validated(manager: JobManager) -> None:
    @manager.handler("count", params_model=CountParams)
    async def count(ctx: JobContext, params: CountParams) -> None:
        return None

    with pytest.raises(InvalidParams):
        await manager.submit("count", {"count": "many"}, owner="alice")
    with pytest.raises(UnknownJobType):
        await manager.submit("nope", None, owner="alice")


async def test_cancel_stops_a_job_at_its_next_report(manager: JobManager) -> None:
    started = asyncio.Event()

    @manager.handler("loop")
    async def loop(ctx: JobContext, _params: None) -> None:
        started.set()
        while True:
            await ctx.report(message="working")
            await asyncio.sleep(0.01)

    await manager.start()
    job = await manager.submit("loop", owner="alice")
    await asyncio.wait_for(started.wait(), 5)
    requested = await manager.cancel(job.id)
    assert requested.status == "active" and requested.cancel_requested
    cancelled = await wait_status(manager, job.id, "cancelled")
    assert cancelled.error is None


async def test_cancel_is_not_swallowed_by_except_exception(manager: JobManager) -> None:
    attempts: list[int] = []

    @manager.handler("stubborn")
    async def stubborn(ctx: JobContext, _params: None) -> str:
        while True:
            try:
                attempts.append(1)
                await ctx.report(message="again")
                await asyncio.sleep(0.01)
            except Exception:
                continue

    await manager.start()
    job = await manager.submit("stubborn", owner="alice")
    await wait_for(lambda: _async(bool(attempts)))
    await manager.cancel(job.id)
    await wait_status(manager, job.id, "cancelled")


async def _async(value: bool) -> bool:
    return value


async def test_cancel_task_interrupts_an_awaited_call(manager: JobManager) -> None:
    started = asyncio.Event()

    @manager.handler("sleepy", cancel_task=True)
    async def sleepy(ctx: JobContext, _params: None) -> None:
        started.set()
        await asyncio.sleep(60)

    await manager.start()
    job = await manager.submit("sleepy", owner="alice")
    await asyncio.wait_for(started.wait(), 5)
    await manager.cancel(job.id)
    await wait_status(manager, job.id, "cancelled", timeout=2)


async def test_a_queued_job_is_cancelled_at_once(manager: JobManager, hooks: RecordingHooks) -> None:
    @manager.handler("never")
    async def never(ctx: JobContext, _params: None) -> None:
        return None

    job = await manager.submit("never", owner="alice", resources=["document:1"])
    assert hooks.held == {"document:1": job.id}
    cancelled = await manager.cancel(job.id)
    assert cancelled.status == "cancelled"
    assert hooks.held == {} and hooks.released == [job.id]


async def test_handler_errors(manager: JobManager) -> None:
    class Rejected(Exception):
        status_code = 422

    @manager.handler("fails")
    async def fails(ctx: JobContext, _params: None) -> None:
        raise Rejected("bad input")

    await manager.start()
    job = await manager.submit("fails", owner="alice")
    failed = await wait_status(manager, job.id, "error")
    assert failed.error is not None and failed.error.message == "bad input" and failed.error.status == 422


async def test_max_concurrent(manager: JobManager) -> None:
    active = 0
    peak = 0

    @manager.handler("serial", max_concurrent=1)
    async def serial(ctx: JobContext, _params: None) -> None:
        nonlocal active, peak
        active += 1
        peak = max(peak, active)
        await asyncio.sleep(0.05)
        active -= 1

    await manager.start()
    jobs = [await manager.submit("serial", owner="alice") for _ in range(3)]
    for job in jobs:
        await wait_status(manager, job.id, "done")
    assert peak == 1


async def test_resources_and_idempotency(manager: JobManager, hooks: RecordingHooks) -> None:
    manager.register_type("client")
    first = await manager.submit("client", owner="alice", resources=["document:1"], idempotency_key="k")
    again = await manager.submit("client", owner="alice", resources=["document:1"], idempotency_key="k")
    assert again.id == first.id
    with pytest.raises(ResourceBusy):
        await manager.submit("client", owner="bob", resources=["document:1"])
    assert len(manager.store.list()) == 1


async def test_resumable_server_job_continues_after_a_worker_restart(db_path: Path) -> None:
    clock = Clock()
    progress_at_stop = asyncio.Event()
    calls: list[str] = []

    async def pages(ctx: JobContext, _params: None) -> list[str]:
        for page in range(4):
            key = f"page-{page}"
            if ctx.has_step(key):
                continue
            calls.append(key)
            if page == 2 and ctx.attempt == 1:
                progress_at_stop.set()
                await asyncio.sleep(60)
            await ctx.save_step(key, f"text {page}")
        return [str(ctx.step(f"page-{page}")) for page in range(4)]

    first = make_manager(SqliteJobStore(db_path), clock)
    first.handler("pages", resumable=True)(pages)
    await first.start()
    job = await first.submit("pages", owner="alice")
    await asyncio.wait_for(progress_at_stop.wait(), 5)
    await first.stop()
    assert first.store.get(job.id).status == "active"  # type: ignore[union-attr]

    second = make_manager(SqliteJobStore(db_path), clock)
    second.handler("pages", resumable=True)(pages)
    await second.start()
    try:
        done = await wait_status(second, job.id, "done")
    finally:
        await second.stop()
    assert done.result == ["text 0", "text 1", "text 2", "text 3"]
    assert done.attempts == 2
    assert calls == ["page-0", "page-1", "page-2", "page-2", "page-3"]


async def test_non_resumable_server_job_fails_after_a_restart(db_path: Path) -> None:
    started = asyncio.Event()

    async def once(ctx: JobContext, _params: None) -> None:
        started.set()
        await asyncio.sleep(60)

    first = make_manager(SqliteJobStore(db_path))
    first.handler("once")(once)
    await first.start()
    job = await first.submit("once", owner="alice")
    await asyncio.wait_for(started.wait(), 5)
    await first.stop()

    second = make_manager(SqliteJobStore(db_path))
    second.handler("once")(once)
    await second.start()
    try:
        failed = await wait_status(second, job.id, "error")
    finally:
        await second.stop()
    assert failed.error is not None and "restart" in failed.error.message


async def test_client_job_lifecycle(manager: JobManager, clock: Clock, hooks: RecordingHooks) -> None:
    manager.register_type("batches", resume_window=60)
    await manager.start()
    job = await manager.submit("batches", {"n": 3}, owner="alice", resources=["document:7"], input_hash="h1")

    claimed = await manager.claim(job.id, "tab-1")
    assert claimed.status == "active" and claimed.steps == {}
    with pytest.raises(LeaseConflict):
        await manager.claim(job.id, "tab-2")
    await manager.save_step(job.id, "batch-0", {"fixed": 2}, holder="tab-1")
    await manager.report(job.id, "tab-1", progress=Progress(done=1, total=3), message="batch 1 of 3")
    with pytest.raises(LeaseConflict):
        await manager.save_step(job.id, "batch-1", None, holder="tab-2")

    # the tab is reloaded and does not come back in time: the lease expires
    clock.advance(manager.lease_ttl + 1)
    interrupted = await wait_status(manager, job.id, "interrupted")
    assert interrupted.lease is None and interrupted.expires_at is not None
    assert hooks.held == {"document:7": job.id}

    # another tab resumes it with the saved steps
    resumed = await manager.claim(job.id, "tab-2")
    assert resumed.status == "active" and resumed.steps is not None and list(resumed.steps) == ["batch-0"]
    assert resumed.expires_at is None
    await manager.save_step(job.id, "batch-1", {"fixed": 0}, holder="tab-2")
    finished = await manager.finish(job.id, "tab-2", result={"changed": 2})
    assert finished.status == "done" and finished.result == {"changed": 2}
    assert hooks.held == {} and hooks.released == [job.id]
    with pytest.raises(LeaseConflict):
        await manager.heartbeat(job.id, "tab-2")


async def test_interrupted_client_job_expires_after_its_resume_window(manager: JobManager, clock: Clock, hooks: RecordingHooks) -> None:
    manager.register_type("batches", resume_window=60)
    await manager.start()
    job = await manager.submit("batches", owner="alice", resources=["document:7"])
    await manager.claim(job.id, "tab-1")
    clock.advance(manager.lease_ttl + 1)
    await wait_status(manager, job.id, "interrupted")
    clock.advance(61)
    expired = await wait_status(manager, job.id, "expired")
    assert expired.expires_at is not None
    assert hooks.released == [job.id]
    # finished jobs are deleted after the retention time
    clock.advance(manager.retention + 1)
    await wait_for(lambda: _missing(manager, job.id))


async def _missing(manager: JobManager, job_id: str) -> bool:
    return manager.store.get(job_id) is None


async def test_resume_refused_when_the_input_changed(manager: JobManager, clock: Clock, hooks: RecordingHooks) -> None:
    manager.register_type("review")
    await manager.start()
    job = await manager.submit("review", owner="alice", input_hash="text-v1")
    await manager.claim(job.id, "tab-1")
    clock.advance(manager.lease_ttl + 1)
    await wait_status(manager, job.id, "interrupted")
    hooks.changed_inputs.add("text-v1")
    with pytest.raises(InputChanged):
        await manager.claim(job.id, "tab-1")
    assert (await manager.get(job.id)).status == "expired"


async def test_cancelled_client_job_whose_driver_is_gone(manager: JobManager, clock: Clock) -> None:
    manager.register_type("batches")
    await manager.start()
    job = await manager.submit("batches", owner="alice")
    await manager.claim(job.id, "tab-1")
    await manager.cancel(job.id)
    beat = await manager.heartbeat(job.id, "tab-1")
    assert beat.cancel_requested and beat.status == "active"
    clock.advance(manager.lease_ttl + 1)
    await wait_status(manager, job.id, "cancelled")


async def test_cancelled_client_job_finished_by_its_driver(manager: JobManager) -> None:
    manager.register_type("batches")
    job = await manager.submit("batches", owner="alice")
    await manager.claim(job.id, "tab-1")
    await manager.cancel(job.id)
    finished = await manager.finish(job.id, "tab-1", cancelled=True)
    assert finished.status == "cancelled"


async def test_report_threadsafe_and_check_cancelled_from_a_thread(manager: JobManager) -> None:
    @manager.handler("threaded")
    async def threaded(ctx: JobContext, _params: None) -> int:
        def work() -> int:
            for i in range(5):
                ctx.report_threadsafe(done=i + 1, total=5)
                ctx.check_cancelled()
            return 5
        return await asyncio.to_thread(work)

    await manager.start()
    job = await manager.submit("threaded", owner="alice")
    done = await wait_status(manager, job.id, "done")
    assert done.result == 5
    assert done.progress.done == 5 and done.progress.value == 100


async def test_delete_stops_a_running_job(manager: JobManager, hooks: RecordingHooks) -> None:
    started = asyncio.Event()
    stopped = asyncio.Event()

    @manager.handler("long")
    async def long(ctx: JobContext, _params: None) -> None:
        started.set()
        try:
            await asyncio.sleep(60)
        finally:
            stopped.set()

    await manager.start()
    job = await manager.submit("long", owner="alice", resources=["document:1"])
    await asyncio.wait_for(started.wait(), 5)
    await manager.delete(job.id)
    await asyncio.wait_for(stopped.wait(), 5)
    assert manager.store.get(job.id) is None
    assert hooks.released == [job.id]


async def test_job_cancelled_raised_by_a_handler(manager: JobManager) -> None:
    @manager.handler("gives-up")
    async def gives_up(ctx: JobContext, _params: None) -> None:
        raise JobCancelled()

    await manager.start()
    job = await manager.submit("gives-up", owner="alice")
    await wait_status(manager, job.id, "cancelled")


async def test_deleted_hook_called_on_delete(manager: JobManager, hooks: RecordingHooks) -> None:
    manager.register_type("review")
    job = await manager.submit("review", owner="alice")
    await manager.delete(job.id)
    assert hooks.deleted_ids == [job.id]


async def test_deleted_hook_called_on_retention_cleanup(manager: JobManager, clock: Clock, hooks: RecordingHooks) -> None:
    manager.register_type("review")
    await manager.start()
    job = await manager.submit("review", owner="alice")
    await manager.claim(job.id, "tab-1")
    await manager.finish(job.id, "tab-1")
    assert hooks.deleted_ids == []
    clock.advance(manager.retention + 1)
    await wait_for(lambda: _missing(manager, job.id))
    assert hooks.deleted_ids == [job.id]


async def test_deleted_hook_not_called_for_missing_job(manager: JobManager, hooks: RecordingHooks) -> None:
    with pytest.raises(JobNotFound):
        await manager.delete("nope")
    assert hooks.deleted_ids == []
