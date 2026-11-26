"""Several uvicorn processes on one store: concurrency limits, cancel and events across workers."""

from __future__ import annotations

import json
import os
import socket
import subprocess
import sys
import time
from collections.abc import Iterator
from pathlib import Path

import httpx
import pytest

ALICE = {"X-User": "alice"}
ROOT = Path(__file__).resolve().parent.parent


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class Worker:
    def __init__(self, db: Path, run_handlers: bool):
        self.port = free_port()
        self.url = f"http://127.0.0.1:{self.port}"
        env = {**os.environ, "JOBS_DB": str(db), "RUN_HANDLERS": "1" if run_handlers else "0"}
        self.process = subprocess.Popen(
            [sys.executable, "-m", "uvicorn", "tests.worker_app:app", "--port", str(self.port), "--log-level", "warning"],
            cwd=ROOT, env=env,
        )
        deadline = time.time() + 20
        while True:
            try:
                self.pid = httpx.get(f"{self.url}/pid").json()
                break
            except httpx.TransportError:
                if time.time() > deadline or self.process.poll() is not None:
                    raise RuntimeError("Worker did not start")
                time.sleep(0.1)

    def stop(self) -> None:
        self.process.terminate()
        self.process.wait(10)


@pytest.fixture
def workers(tmp_path: Path) -> Iterator[tuple[Worker, Worker, Worker]]:
    db = tmp_path / "jobs.db"
    started: list[Worker] = []
    try:
        for run_handlers in (False, True, True):
            started.append(Worker(db, run_handlers))
        yield started[0], started[1], started[2]
    finally:
        for worker in started:
            worker.stop()


def wait_job(url: str, job_id: str, statuses: set[str], timeout: float = 15) -> dict[str, object]:
    deadline = time.time() + timeout
    while True:
        job = httpx.get(f"{url}/jobs/{job_id}", headers=ALICE).json()
        if job["status"] in statuses:
            return job
        if time.time() > deadline:
            raise AssertionError(f"Job {job_id} is {job['status']}")
        time.sleep(0.05)


def test_max_concurrent_across_workers(workers: tuple[Worker, Worker, Worker]) -> None:
    api, runner_a, runner_b = workers
    ids = [httpx.post(f"{api.url}/jobs", json={"type": "serial"}, headers=ALICE).json()["id"] for _ in range(8)]
    results: list[dict[str, float]] = [wait_job(api.url, job_id, {"done"})["result"] for job_id in ids]  # type: ignore[misc]
    intervals = sorted((r["start"], r["end"]) for r in results)
    for (_, end), (start, _) in zip(intervals, intervals[1:]):
        assert start >= end - 0.01, "two jobs of a type with max_concurrent=1 overlapped"
    pids = {int(r["pid"]) for r in results}
    assert pids <= {runner_a.pid, runner_b.pid}


def test_cancel_and_events_reach_the_running_worker(workers: tuple[Worker, Worker, Worker]) -> None:
    api, runner_a, runner_b = workers
    job_id = httpx.post(f"{api.url}/jobs", json={"type": "loop"}, headers=ALICE).json()["id"]
    events: list[dict[str, object]] = []
    with httpx.stream("GET", f"{api.url}/jobs/{job_id}/events", headers={**ALICE, "Accept": "application/x-ndjson"}, timeout=20) as response:
        cancelled = False
        for line in response.iter_lines():
            if not line:
                continue
            event = json.loads(line)
            events.append(event)
            data = event.get("data")
            if not cancelled and isinstance(data, dict) and data.get("pid") and len(events) >= 3:
                # the job runs in another process than the one that serves the stream and the cancel
                assert data["pid"] in (runner_a.pid, runner_b.pid) and data["pid"] != api.pid
                assert httpx.post(f"{api.url}/jobs/{job_id}/cancel", headers=ALICE).json()["cancel_requested"]
                cancelled = True
    assert events[-1]["status"] == "cancelled"
    seqs = [int(str(e["seq"])) for e in events]
    assert seqs == sorted(seqs)


def test_the_stream_of_all_jobs_sees_jobs_of_other_workers(workers: tuple[Worker, Worker, Worker]) -> None:
    api, runner_a, _ = workers
    job_id = httpx.post(f"{runner_a.url}/jobs", json={"type": "loop"}, headers=ALICE).json()["id"]
    with httpx.stream("GET", f"{api.url}/jobs/events", headers={**ALICE, "Accept": "application/x-ndjson"}, timeout=20) as response:
        for line in response.iter_lines():
            if line and json.loads(line)["job_id"] == job_id:
                break
    httpx.post(f"{api.url}/jobs/{job_id}/cancel", headers=ALICE)
    assert wait_job(api.url, job_id, {"cancelled"})["status"] == "cancelled"


def test_a_resumable_job_continues_in_another_worker_after_a_crash(workers: tuple[Worker, Worker, Worker]) -> None:
    api, runner_a, runner_b = workers
    job_id = httpx.post(f"{api.url}/jobs", json={"type": "pages"}, headers=ALICE).json()["id"]
    deadline = time.time() + 15
    while True:
        job = httpx.get(f"{api.url}/jobs/{job_id}?steps=true", headers=ALICE).json()
        if job["steps"] and len(job["steps"]) >= 3 and isinstance(job["data"], dict):
            break
        assert time.time() < deadline
        time.sleep(0.05)
    crashed = runner_a if job["data"]["pid"] == runner_a.pid else runner_b
    survivor = runner_b if crashed is runner_a else runner_a
    crashed.process.kill()
    crashed.process.wait(10)
    done = wait_job(api.url, job_id, {"done", "error"}, timeout=30)
    assert done["status"] == "done" and done["attempts"] == 2
    pids: list[int] = done["result"]  # type: ignore[assignment]
    first_survivor_page = pids.index(survivor.pid)
    assert first_survivor_page >= 3
    assert set(pids[:first_survivor_page]) == {crashed.pid} and set(pids[first_survivor_page:]) == {survivor.pid}
