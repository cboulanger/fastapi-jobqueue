"""
App for the tests with several worker processes (test_workers.py): `uvicorn tests.worker_app:app`.

Environment: JOBS_DB (store file), RUN_HANDLERS ("0": this worker serves the API but runs no
server jobs).
"""

from __future__ import annotations

import asyncio
import os
import time

from fastapi import FastAPI

from fastapi_jobqueue import JobAuth, JobContext, JobManager, SqliteJobStore

from .conftest import header_principal

jobs = JobManager(
    SqliteJobStore(os.environ["JOBS_DB"]),
    auth=JobAuth(current_principal=header_principal),
    poll_interval=0.05,
    report_interval=0.0,
    maintenance_interval=0.2,
    lease_ttl=3.0,
)

if os.environ.get("RUN_HANDLERS", "1") == "1":
    @jobs.handler("serial", max_concurrent=1)
    async def serial(ctx: JobContext, _params: None) -> dict[str, float | int]:
        start = time.time()
        await asyncio.sleep(0.1)
        return {"pid": os.getpid(), "start": start, "end": time.time()}

    @jobs.handler("loop")
    async def loop(ctx: JobContext, _params: None) -> None:
        i = 0
        while True:
            i += 1
            await ctx.report(done=i, data={"pid": os.getpid()})
            await asyncio.sleep(0.05)
    @jobs.handler("pages", resumable=True)
    async def pages(ctx: JobContext, _params: None) -> list[int]:
        for page in range(30):
            if ctx.has_step(f"page-{page}"):
                continue
            await asyncio.sleep(0.1)
            await ctx.save_step(f"page-{page}", os.getpid())
            await ctx.report(done=page + 1, total=30, data={"pid": os.getpid()})
        return [int(ctx.step(f"page-{page}")) for page in range(30)]  # type: ignore[arg-type]
else:
    # the types must be known to submit their jobs here; they run in the other workers
    jobs.register_type("serial", driver="server", max_concurrent=1)
    jobs.register_type("loop", driver="server")
    jobs.register_type("pages", driver="server")

app = FastAPI(lifespan=jobs.lifespan)
app.include_router(jobs.router(), prefix="/jobs")


@app.get("/pid")
def pid() -> int:
    return os.getpid()
