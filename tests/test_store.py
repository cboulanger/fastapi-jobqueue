from __future__ import annotations

import time
from datetime import datetime, timezone

from fastapi_jobqueue import Job, MemoryJobStore, Principal, Progress


def new_job(job_id: str, *, owner: str = "alice", job_type: str = "t", driver: str = "server", visibility: str = "owner") -> Job:
    now = datetime.now(timezone.utc)
    return Job.model_validate({
        "id": job_id, "type": job_type, "owner": owner, "visibility": visibility, "driver": driver,
        "status": "queued", "created_at": now, "updated_at": now,
    })


def test_insert_get_and_idempotency() -> None:
    store = MemoryJobStore()
    job, created = store.insert(new_job("a"), "key-1")
    assert created and job.seq == 1
    again, created = store.insert(new_job("b"), "key-1")
    assert not created and again.id == "a"
    other_owner, created = store.insert(new_job("c", owner="bob"), "key-1")
    assert created and other_owner.id == "c"
    assert store.get("b") is None


def test_conditional_update() -> None:
    store = MemoryJobStore()
    store.insert(new_job("a"))
    now = time.time()
    assert store.update("a", {"status": "active"}, now=now, expect_status=("active",)) is None
    changed = store.update("a", {"status": "active", "progress": Progress(value=5)}, now=now, expect_status=("queued",))
    assert changed is not None and changed.status == "active" and changed.progress.value == 5 and changed.seq == 2
    assert store.update("a", {"message": "x"}, now=now, expect_holder="someone") is None
    assert store.update("a", {"message": "x"}, now=now, expect_holder=None) is not None


def test_claim_queued_respects_the_limit_across_types() -> None:
    store = MemoryJobStore()
    for job_id in ("a", "b"):
        store.insert(new_job(job_id, job_type="ocr"))
    store.insert(new_job("c", job_type="other"))
    now = time.time()
    first = store.claim_queued({"ocr": 1, "other": None}, "w", now + 10, now)
    assert first is not None and first.id == "a" and first.attempts == 1
    second = store.claim_queued({"ocr": 1, "other": None}, "w", now + 10, now)
    assert second is not None and second.id == "c"
    assert store.claim_queued({"ocr": 1, "other": None}, "w", now + 10, now) is None


def test_claim_takes_over_expired_leases_only() -> None:
    store = MemoryJobStore()
    store.insert(new_job("a", driver="client"))
    now = time.time()
    assert store.claim("a", "tab-1", now + 10, now, from_statuses=("queued", "active")) is not None
    assert store.claim("a", "tab-2", now + 10, now, from_statuses=("queued", "active")) is None
    assert store.claim("a", "tab-1", now + 10, now, from_statuses=("queued", "active")) is not None
    later = now + 20
    taken = store.claim("a", "tab-2", later + 10, later, from_statuses=("queued", "active"))
    assert taken is not None and taken.lease is not None and taken.lease.holder == "tab-2"


def test_events_are_trimmed() -> None:
    store = MemoryJobStore(events_kept=5)
    store.insert(new_job("a"))
    for i in range(30):
        store.update("a", {"message": str(i)}, now=time.time())
    events = store.events_after("a", 0)
    assert len(events) <= 15
    assert events[-1].seq == 31 and events[-1].message == "29"
    assert store.oldest_seq("a") == events[0].seq


def test_list_visibility() -> None:
    store = MemoryJobStore()
    store.insert(new_job("a", owner="alice"))
    store.insert(new_job("b", owner="bob"))
    store.insert(new_job("c", owner="bob", visibility="users"))
    assert [j.id for j in store.list(viewer=Principal("alice"))] == ["a", "c"]
    assert [j.id for j in store.list(viewer=Principal("carol", is_admin=True))] == ["a", "b", "c"]
    assert store.list(statuses=[]) == []


def test_steps_overwrite_and_cascade() -> None:
    store = MemoryJobStore()
    store.insert(new_job("a"))
    assert store.save_step("a", "s1", {"n": 1}, time.time())
    assert store.save_step("a", "s1", {"n": 2}, time.time())
    assert store.steps("a")["s1"].result == {"n": 2}
    assert not store.save_step("missing", "s1", None, time.time())
    store.delete("a")
    assert store.steps("a") == {}
    assert store.events_after("a", 0) == []
