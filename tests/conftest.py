from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Awaitable, Callable
from pathlib import Path

import pytest
from fastapi import FastAPI, Header, HTTPException

from fastapi_jobqueue import Job, JobAuth, JobHooks, JobManager, MemoryJobStore, Principal, ResourceBusy, SqliteJobStore


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


class Clock:
    """Real time plus an offset that tests advance to let leases and windows expire."""

    def __init__(self) -> None:
        import time
        self._time = time.time
        self.offset = 0.0

    def __call__(self) -> float:
        return self._time() + self.offset

    def advance(self, seconds: float) -> None:
        self.offset += seconds


class RecordingHooks(JobHooks):
    """Keeps a set of held resources like a host's lock table."""

    def __init__(self) -> None:
        self.held: dict[str, str] = {}
        self.released: list[str] = []
        self.renewed: list[str] = []
        self.deleted_ids: list[str] = []
        self.changed_inputs: set[str] = set()

    def acquire(self, job: Job) -> None:
        for resource in job.resources:
            holder = self.held.get(resource)
            if holder is not None and holder != job.id:
                raise ResourceBusy(f"{resource} is held by job {holder}")
        for resource in job.resources:
            self.held[resource] = job.id

    def renew(self, job: Job) -> None:
        self.renewed.append(job.id)

    def deleted(self, job: Job) -> None:
        self.deleted_ids.append(job.id)

    def release(self, job: Job) -> None:
        self.released.append(job.id)
        for resource in job.resources:
            if self.held.get(resource) == job.id:
                del self.held[resource]

    async def input_changed(self, job: Job) -> bool:
        return job.input_hash in self.changed_inputs


def header_principal(x_user: str | None = Header(None), x_admin: str | None = Header(None)) -> Principal:
    if not x_user:
        raise HTTPException(status_code=401, detail="Not authenticated")
    return Principal(id=x_user, is_admin=x_admin == "1")


def make_manager(store: SqliteJobStore, clock: Clock | None = None, hooks: JobHooks | None = None) -> JobManager:
    return JobManager(
        store, auth=JobAuth(current_principal=header_principal), hooks=hooks, clock=clock or Clock(),
        poll_interval=0.02, report_interval=0.0, maintenance_interval=0.05, lease_ttl=5.0,
    )


@pytest.fixture
def clock() -> Clock:
    return Clock()


@pytest.fixture
def hooks() -> RecordingHooks:
    return RecordingHooks()


@pytest.fixture
async def manager(clock: Clock, hooks: RecordingHooks) -> AsyncIterator[JobManager]:
    jobs = make_manager(MemoryJobStore(), clock, hooks)
    yield jobs
    await jobs.stop()


@pytest.fixture
def db_path(tmp_path: Path) -> Path:
    return tmp_path / "jobs.db"


def app_for(manager: JobManager) -> FastAPI:
    app = FastAPI()
    app.include_router(manager.router(), prefix="/jobs")
    return app


async def wait_for(condition: Callable[[], Awaitable[bool]], timeout: float = 5.0) -> None:
    """Wait until an async condition holds."""
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while not await condition():
        if loop.time() > deadline:
            raise AssertionError("Condition not met in time")
        await asyncio.sleep(0.02)


async def wait_status(manager: JobManager, job_id: str, *statuses: str, timeout: float = 5.0) -> Job:
    async def reached() -> bool:
        return (await manager.get(job_id)).status in statuses
    await wait_for(reached, timeout)
    return await manager.get(job_id, steps=True)
