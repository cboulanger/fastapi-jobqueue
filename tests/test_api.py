from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator

import httpx
import pytest

from fastapi_jobqueue import JobAuth, JobContext, JobManager, Principal

from .conftest import Clock, app_for, wait_status

pytestmark = pytest.mark.anyio

ALICE = {"X-User": "alice"}
BOB = {"X-User": "bob"}
ADMIN = {"X-User": "root", "X-Admin": "1"}


@pytest.fixture
async def client(manager: JobManager) -> AsyncIterator[httpx.AsyncClient]:
    @manager.handler("echo")
    async def echo(ctx: JobContext, params: dict[str, int]) -> dict[str, int]:
        for i in range(3):
            await ctx.report(done=i + 1, total=3, message=f"step {i + 1}")
            await asyncio.sleep(0.01)
        return params

    gate = asyncio.Event()

    @manager.handler("gated")
    async def gated(ctx: JobContext, _params: None) -> str:
        while not gate.is_set():
            await ctx.report(message="waiting")
            await asyncio.sleep(0.01)
        return "open"

    manager.register_type("client")
    manager.register_type("internal", rest=False)
    await manager.start()
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app_for(manager)), base_url="http://test") as c:
        c.gate = gate  # type: ignore[attr-defined]
        yield c


async def test_submit_get_and_etag(client: httpx.AsyncClient, manager: JobManager) -> None:
    response = await client.post("/jobs", json={"type": "echo", "params": {"a": 1}}, headers=ALICE)
    assert response.status_code == 201
    job_id = response.json()["id"]
    await wait_status(manager, job_id, "done")
    got = await client.get(f"/jobs/{job_id}", headers=ALICE)
    assert got.json()["result"] == {"a": 1}
    etag = got.headers["ETag"]
    not_modified = await client.get(f"/jobs/{job_id}", headers={**ALICE, "If-None-Match": etag})
    assert not_modified.status_code == 304
    with_steps = await client.get(f"/jobs/{job_id}?steps=true", headers=ALICE)
    assert with_steps.json()["steps"] == {}


async def test_submit_rules(client: httpx.AsyncClient, manager: JobManager) -> None:
    assert (await client.post("/jobs", json={"type": "echo"})).status_code == 401
    assert (await client.post("/jobs", json={"type": "nope"}, headers=ALICE)).status_code == 400
    assert (await client.post("/jobs", json={"type": "internal"}, headers=ALICE)).status_code == 400
    first = await client.post("/jobs", json={"type": "client"}, headers={**ALICE, "Idempotency-Key": "k1"})
    again = await client.post("/jobs", json={"type": "client"}, headers={**ALICE, "Idempotency-Key": "k1"})
    assert first.json()["id"] == again.json()["id"]


async def test_may_submit(manager: JobManager) -> None:
    manager.register_type("client")
    manager.auth = JobAuth(current_principal=manager.auth.current_principal, may_submit=lambda p, t: p.is_admin)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app_for(manager)), base_url="http://test") as c:
        assert (await c.post("/jobs", json={"type": "client"}, headers=ALICE)).status_code == 400
        assert (await c.post("/jobs", json={"type": "client"}, headers=ADMIN)).status_code == 201


async def test_visibility_and_permissions(client: httpx.AsyncClient) -> None:
    private = (await client.post("/jobs", json={"type": "client"}, headers=ALICE)).json()
    shared = (await client.post("/jobs", json={"type": "client", "visibility": "users"}, headers=ALICE)).json()
    assert [j["id"] for j in (await client.get("/jobs", headers=BOB)).json()] == [shared["id"]]
    assert len((await client.get("/jobs", headers=ADMIN)).json()) == 2
    assert len((await client.get("/jobs?mine=true", headers=ADMIN)).json()) == 0
    assert (await client.get(f"/jobs/{private['id']}", headers=BOB)).status_code == 404
    assert (await client.post(f"/jobs/{shared['id']}/cancel", headers=BOB)).status_code == 403
    assert (await client.post(f"/jobs/{shared['id']}/claim", json={"holder": "t"}, headers=BOB)).status_code == 403
    # the owner deletes only finished jobs
    assert (await client.delete(f"/jobs/{private['id']}", headers=ALICE)).status_code == 409
    assert (await client.post(f"/jobs/{private['id']}/cancel", headers=ALICE)).json()["status"] == "cancelled"
    assert (await client.delete(f"/jobs/{private['id']}", headers=ALICE)).status_code == 200
    assert (await client.delete(f"/jobs/{shared['id']}", headers=ADMIN)).status_code == 200


async def test_admin_bulk_operations(client: httpx.AsyncClient) -> None:
    for headers in (ALICE, BOB):
        await client.post("/jobs", json={"type": "client"}, headers=headers)
    assert (await client.post("/jobs/cancel-all", json={}, headers=ALICE)).status_code == 403
    cancelled = await client.post("/jobs/cancel-all", json={"owner": "bob"}, headers=ADMIN)
    assert cancelled.json() == {"count": 1}
    statuses = sorted(j["status"] for j in (await client.get("/jobs", headers=ADMIN)).json())
    assert statuses == ["cancelled", "queued"]
    deleted = await client.delete("/jobs", params={"status": ["cancelled"]}, headers=ADMIN)
    assert deleted.json() == {"count": 1}
    assert (await client.delete("/jobs", headers=ADMIN)).json() == {"count": 1}


async def test_client_driver_endpoints(client: httpx.AsyncClient) -> None:
    job = (await client.post("/jobs", json={"type": "client", "params": {"n": 2}}, headers=ALICE)).json()
    base = f"/jobs/{job['id']}"
    claimed = await client.post(f"{base}/claim", json={"holder": "tab-1"}, headers=ALICE)
    assert claimed.json()["status"] == "active" and claimed.json()["lease"]["holder"] == "tab-1"
    assert (await client.post(f"{base}/claim", json={"holder": "tab-2"}, headers=ALICE)).status_code == 409
    assert (await client.put(f"{base}/steps/batch-0", json={"holder": "tab-1", "result": [1, 2]}, headers=ALICE)).status_code == 204
    assert (await client.put(f"{base}/steps/batch-1", json={"holder": "tab-2", "result": None}, headers=ALICE)).status_code == 409
    reported = await client.post(f"{base}/report", json={"holder": "tab-1", "progress": {"done": 1, "total": 2}, "message": "half"}, headers=ALICE)
    assert reported.json()["message"] == "half" and reported.json()["cancel_requested"] is False
    await client.post(f"{base}/cancel", headers=ALICE)
    beat = await client.post(f"{base}/heartbeat", json={"holder": "tab-1"}, headers=ALICE)
    assert beat.json()["cancel_requested"] is True
    again = await client.post(f"{base}/claim", json={"holder": "tab-1"}, headers=ALICE)
    assert again.json()["steps"]["batch-0"]["result"] == [1, 2]
    finished = await client.post(f"{base}/finish", json={"holder": "tab-1", "cancelled": True}, headers=ALICE)
    assert finished.json()["status"] == "cancelled"
    assert (await client.post(f"{base}/claim", json={"holder": "tab-1"}, headers=ALICE)).status_code == 409


def parse_sse(text: str) -> list[tuple[int | None, dict[str, object]]]:
    events: list[tuple[int | None, dict[str, object]]] = []
    for block in text.strip().split("\n\n"):
        event_id: int | None = None
        data = None
        for line in block.splitlines():
            if line.startswith("id: "):
                event_id = int(line[4:])
            elif line.startswith("data: "):
                data = json.loads(line[6:])
        if data is not None:
            events.append((event_id, data))
    return events


async def test_sse_stream_of_a_job(client: httpx.AsyncClient, manager: JobManager) -> None:
    job = (await client.post("/jobs", json={"type": "gated"}, headers=ALICE)).json()
    stream = asyncio.create_task(client.get(f"/jobs/{job['id']}/events", headers=ALICE))
    await wait_status(manager, job["id"], "active")
    await asyncio.sleep(0.1)
    client.gate.set()  # type: ignore[attr-defined]
    response = await asyncio.wait_for(stream, 5)
    assert response.headers["content-type"].startswith("text/event-stream")
    events = parse_sse(response.text)
    seqs = [event_id for event_id, _ in events if event_id is not None]
    assert len(seqs) == len(events)
    assert seqs == sorted(seqs) and len(set(seqs)) == len(seqs)
    assert events[-1][1]["status"] == "done" and events[-1][1]["result"] == "open"

    # a reconnect with Last-Event-ID gets only the missed events
    middle = seqs[len(seqs) // 2]
    resumed = await client.get(f"/jobs/{job['id']}/events", headers={**ALICE, "Last-Event-ID": str(middle)})
    assert [event_id for event_id, _ in parse_sse(resumed.text)] == [s for s in seqs if s > middle]


async def test_ndjson_stream_of_a_finished_job(client: httpx.AsyncClient, manager: JobManager) -> None:
    job = (await client.post("/jobs", json={"type": "echo", "params": {"b": 2}}, headers=ALICE)).json()
    await wait_status(manager, job["id"], "done")
    response = await client.get(f"/jobs/{job['id']}/events", headers={**ALICE, "Accept": "application/x-ndjson"})
    assert response.headers["content-type"].startswith("application/x-ndjson")
    lines = [json.loads(line) for line in response.text.splitlines() if line]
    assert len(lines) == 1 and lines[0]["status"] == "done" and lines[0]["result"] == {"b": 2}
    assert (await client.get(f"/jobs/{job['id']}/events", headers=BOB)).status_code == 404


async def test_principal_dependency_can_be_async() -> None:
    async def principal() -> Principal:
        return Principal("alice")

    from fastapi_jobqueue import MemoryJobStore
    jobs = JobManager(MemoryJobStore(), auth=JobAuth(current_principal=principal), clock=Clock())
    jobs.register_type("client")
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app_for(jobs)), base_url="http://test") as c:
        assert (await c.post("/jobs", json={"type": "client"})).json()["owner"] == "alice"
