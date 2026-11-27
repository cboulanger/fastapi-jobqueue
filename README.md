# fastapi-jobqueue

Resumable long-running jobs for FastAPI apps. Core code and plugins of a host submit jobs, follow them by polling, SSE or an NDJSON stream, cancel them, and resume them after a page reload, a dropped connection or a worker restart. All worker processes of a host share one SQLite store, so it works with several uvicorn workers.

The library is the backend only: hosts mount its router and write their own API clients against the REST API below. Design and decisions: [issue #1](https://github.com/cboulanger/fastapi-jobqueue/issues/1).

## Concepts

- **Job**: a unit of work with an id, a type, an owner, a visibility (`owner` or `users`) and a status.
- **Driver**: who runs the job. A `server` job is run by a handler registered for its type, as an asyncio task in one of the host's workers. A `client` job is run by a client (a browser tab, a plugin), which claims it and reports to it step by step.
- **Lease**: the driver holds the job's lease and renews it. When the lease expires, a client job becomes `interrupted` and can be resumed within the resume window; a resumable server job is queued again and continues in any worker.
- **Steps**: named checkpoints with results. A resumed job skips the steps it already has. Saving a step twice overwrites it, so steps must be idempotent.
- **Resources**: keys a job holds while it is unfinished (e.g. `document:42`). The host maps them to its own locks through hooks.

```text
queued ──► active ──► done
   │         │  ├──► error
   │         │  └──► cancelled
   │         └──► interrupted ──► active (resumed)
   │                     └──────► expired (resume window over)
   └──► cancelled
```

Finished jobs (`done`, `error`, `cancelled`, `expired`) are deleted after the retention time.

## Mounting

```python
from fastapi import Depends, FastAPI, HTTPException
from fastapi_jobqueue import JobAuth, JobContext, JobManager, Principal, SqliteJobStore


def current_principal(user=Depends(get_current_user)) -> Principal:  # the host's own auth
    if user is None:
        raise HTTPException(status_code=401)
    return Principal(id=user.id, is_admin=user.is_admin)


jobs = JobManager(
    SqliteJobStore("data/jobs.db"),
    auth=JobAuth(current_principal=current_principal),
    retention=3600,        # finished jobs are kept 1 hour
    resume_window=900,     # interrupted jobs can be resumed for 15 minutes
)

app = FastAPI(lifespan=jobs.lifespan)  # or call jobs.start() / jobs.stop() in the host's lifespan
app.include_router(jobs.router(tags=["jobs"]), prefix="/api/jobs")
```

`JobAuth.may_submit(principal, job_type)` restricts which types a principal may submit through the REST API.

### Hooks

Subclass `JobHooks` to map resources to the host's locks and to check a job's input before it is resumed, and to clean up after a deleted job. Each method may be sync or async.

```python
class EditorHooks(JobHooks):
    def acquire(self, job):        # before the job is stored; raise ResourceBusy to refuse it
        for resource in job.resources:
            locks.acquire(resource, holder=f"job:{job.id}")

    def renew(self, job):          # the driver renewed the lease
        for resource in job.resources:
            locks.refresh(resource, holder=f"job:{job.id}")

    def release(self, job):        # finished, expired or deleted
        for resource in job.resources:
            locks.release(resource, holder=f"job:{job.id}")

    def input_changed(self, job):  # before an interrupted job is resumed
        return current_hash(job.params) != job.input_hash

    def deleted(self, job):        # after the job row was deleted (DELETE, delete_all, retention cleanup)
        remove_files(job.id)
```

## Server jobs

```python
from pydantic import BaseModel


class OcrParams(BaseModel):
    file_id: str


@jobs.handler("ocr", max_concurrent=1, resumable=True, params_model=OcrParams)
async def run_ocr(ctx: JobContext, params: OcrParams) -> dict:
    if not ctx.has_step("ocr"):
        for page in range(page_count):
            await ctx.report(done=page + 1, total=page_count, unit="pages", phase="Recognizing text")
            ...
        await ctx.save_step("ocr", {"hash": new_hash})
    for window in windows:
        if not ctx.has_step(f"llm-{window}"):
            await ctx.save_step(f"llm-{window}", await correct(window))
    return {"changed": True}


job = await jobs.submit("ocr", {"file_id": "abc"}, owner=user_id, resources=["document:abc"])
```

- `ctx.report(value=None, *, done, total, unit, phase, eta_seconds, message, data)` updates the progress; omitted fields keep their value, and `value` is computed from `done`/`total`. Writes are coalesced to one per `report_interval` (default 1 s); a new phase or message is written at once.
- `ctx.report()`, `ctx.save_step()` and `ctx.check_cancelled()` raise `JobCancelled` when the user cancelled the job. It is a `BaseException`, so `except Exception` handlers do not swallow it. With `cancel_task=True`, a cancel also cancels the handler's task, which interrupts an awaited call at once.
- For blocking work in a thread (`asyncio.to_thread`), use `ctx.report_threadsafe(...)` and `ctx.check_cancelled()`.
- `max_concurrent` limits the active jobs of the type across all workers.
- `resumable=True`: after an interruption (worker restart or crash), the handler runs again in any worker, with the saved steps (`ctx.attempt` is then above 1). After 3 attempts the job fails. Without it, an interrupted job fails.
- An exception fails the job with its message; an exception with an integer `status_code` attribute sets the error status.
- A process that only serves the API registers the type without a handler: `jobs.register_type("ocr", driver="server")`. The jobs then run in the workers that have the handler.

## Client jobs

```python
jobs.register_type("auto-annotation.correct", resume_window=600)
```

A client submits the job, claims it with a holder id that it keeps (e.g. per browser tab in `sessionStorage`), saves its steps and reports, and finishes it. When a reload or a network problem keeps it away longer than the lease (`lease_ttl`, default 30 s), the job becomes `interrupted`. The client (the same tab, or another one after asking the user) claims it again and gets the saved steps. Host routes that do the work of a step can save it themselves with `await jobs.save_step(job_id, key, result)`.

## REST API

Paths are relative to the prefix the router is mounted at. All endpoints require the principal from `JobAuth.current_principal`.

| Method and path | Who | Purpose |
| --- | --- | --- |
| `POST /` | user | Submit `{type, params?, visibility?, input_hash?, resources?}`. With an `Idempotency-Key` header, a retry returns the same job. 201 with the job |
| `GET /` | user | Visible jobs, oldest first. Query: `status` (repeatable), `type`, `mine`, `resumable` (active and interrupted client jobs) |
| `GET /{id}` | viewer | The job; `steps=true` includes the steps. `ETag`, and 304 for a matching `If-None-Match` |
| `GET /{id}/events` | viewer | Event stream of the job (see below); ends after its final event |
| `GET /events` | user | Event stream of all visible jobs; `mine=true` for the caller's own |
| `POST /{id}/cancel` | owner, admin | Cancel: a queued or interrupted job at once, an active one at its driver's next check (`cancel_requested`) |
| `DELETE /{id}` | owner (finished jobs), admin (any) | Delete |
| `POST /{id}/claim` | owner | `{holder}`: start, take over or resume a client job; returns it with its steps. 409 if another holder's lease is valid, or the input changed |
| `POST /{id}/heartbeat` | lease holder | `{holder}`: renew the lease; the job says whether a cancel was requested |
| `PUT /{id}/steps/{key}` | lease holder | `{holder, result}`: save a step. 204 |
| `POST /{id}/report` | lease holder | `{holder, progress?, message?, data?}`: report and renew the lease |
| `POST /{id}/finish` | lease holder | `{holder, result?}`, `{holder, error: {message, status}}` or `{holder, cancelled: true}` |
| `POST /cancel-all` | admin | `{type?, owner?, status?}`: cancel matching unfinished jobs; returns `{count}` |
| `DELETE /` | admin | Query `type`, `owner`, `status`: delete matching jobs; returns `{count}` |

Errors: 400 unknown type or type not submittable, 401 from the host's auth, 403 not allowed, 404 not found or not visible, 409 state, lease, resource or input conflicts, 422 invalid parameters.

### Events

Every change of a job is an event:

```json
{"id": 812, "seq": 7, "job_id": "Qx…", "type": "ocr", "status": "active",
 "progress": {"value": 40, "done": 4, "total": 10, "unit": "pages", "phase": "Recognizing text", "eta_seconds": null},
 "message": null, "data": null, "result": null, "error": null, "cancel_requested": false, "at": "2026-11-26T15:00:00Z"}
```

`seq` counts the events of a job; `id` is a global id (null in snapshots). `result` is set in the `done` event, `error` in the `error` event.

Streams send SSE (`text/event-stream`, event name `job`) by default, and NDJSON with `Accept: application/x-ndjson`. They start with a snapshot (of the job, or of all visible unfinished jobs), then send every later event. A client that reconnects with `Last-Event-ID` (header, or `last_event_id` query parameter) gets the events it missed instead, if they are still kept (the last 100 per job). In a job stream, the event id is `seq`; in the stream of all jobs, it is the global `id`. Keep-alives are sent every 15 s: SSE comments, or empty lines in NDJSON.

Polling `GET /{id}` with `If-None-Match` is the alternative for clients that cannot stream.

## Configuration

| `JobManager` argument | Default | Meaning |
| --- | --- | --- |
| `retention` | 3600 | Seconds a finished job is kept |
| `resume_window` | 900 | Seconds an interrupted job can be resumed; per type with `resume_window=` |
| `lease_ttl` | 30 | Seconds a lease lasts without renewal |
| `poll_interval` | 0.5 | Seconds between checks for cancel requests and new events |
| `report_interval` | 1.0 | Minimum seconds between progress writes of a server job |
| `maintenance_interval` | 5.0 | Seconds between checks for expired leases and jobs |

`SqliteJobStore(path, events_kept=100)` creates the database file and its directory. `MemoryJobStore()` is the same store in memory, for tests and single-process hosts.

## Releases and versions

Every push to `main` is tested and then released by [semantic-release](https://github.com/semantic-release/semantic-release) (`.github/workflows/release.yml`): the Conventional Commit messages since the last release decide the next version (`fix:` patch, `feat:` minor, a breaking change major; `chore:`, `docs:`, `ci:`, `test:` and `refactor:` release nothing). It creates the tag `vX.Y.Z` and a GitHub release with the release notes, the wheel and the sdist. The package version comes from the tag (hatch-vcs), so a release pushes no commits.

To depend on a release:

```bash
uv add "fastapi-jobqueue @ git+https://github.com/cboulanger/fastapi-jobqueue@v0.1.0"
```

or in `pyproject.toml`: `"fastapi-jobqueue @ git+https://github.com/cboulanger/fastapi-jobqueue@v0.1.0"`. The wheel attached to each release can be used instead of the Git URL.

## Development

```bash
uv sync
uv run pytest
uv run pyright
```

The tests include several uvicorn worker processes on one store (`tests/test_workers.py`): the concurrency limit, a cancel and events across workers, and a crashed worker whose job another worker resumes.

## License

MIT, see [LICENSE](LICENSE).
