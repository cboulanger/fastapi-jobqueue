"""
Storage of jobs, their steps and events in SQLite.

`SqliteJobStore` keeps them in one file in WAL mode, which all worker processes of a host share.
Every write that a concurrent writer could interfere with runs in an immediate transaction, and
conditional updates (`expect_*`) make state transitions safe when several workers try the same
one: exactly one of them succeeds. Writes update single columns, so a progress report never
overwrites a cancel request.

`MemoryJobStore` is the same store on an in-memory database, for tests and single-process hosts.

The methods are synchronous; the manager calls them via `asyncio.to_thread`.
"""

from __future__ import annotations

import json
import sqlite3
import threading
from collections.abc import Collection, Iterator, Mapping
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pydantic import JsonValue

from .auth import Principal
from .models import Job, JobError, JobEvent, Lease, Progress, Step

SCHEMA = """
CREATE TABLE IF NOT EXISTS jobs (
    id TEXT PRIMARY KEY,
    type TEXT NOT NULL,
    owner TEXT NOT NULL,
    visibility TEXT NOT NULL,
    driver TEXT NOT NULL,
    status TEXT NOT NULL,
    progress TEXT NOT NULL,
    message TEXT,
    data TEXT,
    params TEXT,
    input_hash TEXT,
    result TEXT,
    error TEXT,
    resources TEXT NOT NULL,
    lease_holder TEXT,
    lease_expires REAL,
    cancel_requested INTEGER NOT NULL DEFAULT 0,
    attempts INTEGER NOT NULL DEFAULT 0,
    seq INTEGER NOT NULL DEFAULT 0,
    idempotency_key TEXT,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL,
    finished_at REAL,
    expires_at REAL
);
CREATE UNIQUE INDEX IF NOT EXISTS jobs_idempotency ON jobs (owner, idempotency_key) WHERE idempotency_key IS NOT NULL;
CREATE INDEX IF NOT EXISTS jobs_status ON jobs (status, type);
CREATE TABLE IF NOT EXISTS job_steps (
    job_id TEXT NOT NULL REFERENCES jobs (id) ON DELETE CASCADE,
    key TEXT NOT NULL,
    result TEXT,
    at REAL NOT NULL,
    PRIMARY KEY (job_id, key)
);
CREATE TABLE IF NOT EXISTS job_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id TEXT NOT NULL REFERENCES jobs (id) ON DELETE CASCADE,
    seq INTEGER NOT NULL,
    at REAL NOT NULL,
    status TEXT NOT NULL,
    progress TEXT NOT NULL,
    message TEXT,
    data TEXT,
    result TEXT,
    error TEXT,
    cancel_requested INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS job_events_job ON job_events (job_id, seq);
"""

UPDATABLE_COLUMNS = frozenset({
    "status", "progress", "message", "data", "result", "error", "lease_holder", "lease_expires",
    "cancel_requested", "attempts", "finished_at", "expires_at",
})
JSON_COLUMNS = frozenset({"data", "params", "result"})

_UNSET: Any = object()


def _ts(value: float | None) -> datetime | None:
    return None if value is None else datetime.fromtimestamp(value, timezone.utc)


def _dump(value: JsonValue) -> str | None:
    return None if value is None else json.dumps(value)


def _load(value: str | None) -> JsonValue:
    return None if value is None else json.loads(value)


def _column_value(column: str, value: Any) -> Any:
    """Convert a value given to `update()` to what is stored in the column."""
    if column == "progress":
        return (value if isinstance(value, Progress) else Progress.model_validate(value)).model_dump_json()
    if column == "error":
        if value is None:
            return None
        return (value if isinstance(value, JobError) else JobError.model_validate(value)).model_dump_json()
    if column in JSON_COLUMNS:
        return _dump(value)
    if column == "cancel_requested":
        return int(bool(value))
    return value


class SqliteJobStore:
    """Jobs, steps and events in a SQLite database shared by all worker processes of a host."""

    def __init__(self, path: str | Path, *, events_kept: int = 100):
        """
        Args:
            path: Database file; created with its directory if missing. ":memory:" for an
                in-memory database (see `MemoryJobStore`).
            events_kept: Events kept per job; older ones are deleted, since the snapshot is authoritative
        """
        self.path = str(path)
        self.events_kept = events_kept
        if self.path != ":memory:":
            Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(self.path, isolation_level=None, check_same_thread=False, timeout=30)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA busy_timeout = 30000")
        self._conn.execute("PRAGMA foreign_keys = ON")
        if self.path != ":memory:":
            self._conn.execute("PRAGMA journal_mode = WAL")
            self._conn.execute("PRAGMA synchronous = NORMAL")
        with self._lock:
            self._conn.executescript(SCHEMA)

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    @contextmanager
    def _transaction(self) -> Iterator[sqlite3.Connection]:
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                yield self._conn
            except BaseException:
                self._conn.execute("ROLLBACK")
                raise
            self._conn.execute("COMMIT")

    def _query(self, sql: str, params: Collection[Any] = ()) -> list[sqlite3.Row]:
        with self._lock:
            return self._conn.execute(sql, tuple(params)).fetchall()

    # ── Conversion ──────────────────────────────────────────────────────────

    def _job(self, row: sqlite3.Row, steps: dict[str, Step] | None = None) -> Job:
        lease = None
        if row["lease_holder"] is not None and row["lease_expires"] is not None:
            lease = Lease(holder=row["lease_holder"], expires_at=datetime.fromtimestamp(row["lease_expires"], timezone.utc))
        return Job(
            id=row["id"],
            type=row["type"],
            owner=row["owner"],
            visibility=row["visibility"],
            driver=row["driver"],
            status=row["status"],
            progress=Progress.model_validate_json(row["progress"]),
            message=row["message"],
            data=_load(row["data"]),
            params=_load(row["params"]),
            input_hash=row["input_hash"],
            result=_load(row["result"]),
            error=JobError.model_validate_json(row["error"]) if row["error"] else None,
            resources=json.loads(row["resources"]),
            lease=lease,
            cancel_requested=bool(row["cancel_requested"]),
            attempts=row["attempts"],
            seq=row["seq"],
            created_at=datetime.fromtimestamp(row["created_at"], timezone.utc),
            updated_at=datetime.fromtimestamp(row["updated_at"], timezone.utc),
            finished_at=_ts(row["finished_at"]),
            expires_at=_ts(row["expires_at"]),
            steps=steps,
        )

    @staticmethod
    def _event(row: sqlite3.Row, job_type: str, with_id: bool = True) -> JobEvent:
        return JobEvent(
            id=row["id"] if with_id else None,
            seq=row["seq"],
            job_id=row["job_id"],
            type=job_type,
            status=row["status"],
            progress=Progress.model_validate_json(row["progress"]),
            message=row["message"],
            data=_load(row["data"]),
            result=_load(row["result"]),
            error=JobError.model_validate_json(row["error"]) if row["error"] else None,
            cancel_requested=bool(row["cancel_requested"]),
            at=datetime.fromtimestamp(row["at"], timezone.utc),
        )

    def _append_event(self, conn: sqlite3.Connection, job_id: str, now: float) -> None:
        """Append an event with the job's current state and the next sequence number."""
        conn.execute("UPDATE jobs SET seq = seq + 1 WHERE id = ?", (job_id,))
        row = conn.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
        conn.execute(
            "INSERT INTO job_events (job_id, seq, at, status, progress, message, data, result, error, cancel_requested) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (job_id, row["seq"], now, row["status"], row["progress"], row["message"], row["data"],
             row["result"] if row["status"] == "done" else None, row["error"], row["cancel_requested"]),
        )
        if row["seq"] % 10 == 0:
            conn.execute("DELETE FROM job_events WHERE job_id = ? AND seq <= ?", (job_id, row["seq"] - self.events_kept))

    # ── Jobs ────────────────────────────────────────────────────────────────

    def insert(self, job: Job, idempotency_key: str | None = None) -> tuple[Job, bool]:
        """
        Store a new job and its first event.

        Returns:
            The stored job, and whether it was created; False if a job of the same owner with the
            same idempotency key exists, which is returned instead
        """
        now = job.created_at.timestamp()
        with self._transaction() as conn:
            if idempotency_key is not None:
                existing = conn.execute("SELECT * FROM jobs WHERE owner = ? AND idempotency_key = ?", (job.owner, idempotency_key)).fetchone()
                if existing is not None:
                    return self._job(existing), False
            conn.execute(
                "INSERT INTO jobs (id, type, owner, visibility, driver, status, progress, message, data, params, input_hash, resources, idempotency_key, created_at, updated_at, expires_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (job.id, job.type, job.owner, job.visibility, job.driver, job.status, job.progress.model_dump_json(),
                 job.message, _dump(job.data), _dump(job.params), job.input_hash, json.dumps(job.resources),
                 idempotency_key, now, now, job.expires_at.timestamp() if job.expires_at else None),
            )
            self._append_event(conn, job.id, now)
            row = conn.execute("SELECT * FROM jobs WHERE id = ?", (job.id,)).fetchone()
        return self._job(row), True

    def find_by_idempotency_key(self, owner: str, key: str) -> Job | None:
        rows = self._query("SELECT * FROM jobs WHERE owner = ? AND idempotency_key = ?", (owner, key))
        return self._job(rows[0]) if rows else None

    def get(self, job_id: str, *, steps: bool = False) -> Job | None:
        rows = self._query("SELECT * FROM jobs WHERE id = ?", (job_id,))
        if not rows:
            return None
        return self._job(rows[0], self.steps(job_id) if steps else None)

    def list(
        self,
        *,
        viewer: Principal | None = None,
        owner: str | None = None,
        statuses: Collection[str] | None = None,
        job_type: str | None = None,
        driver: str | None = None,
    ) -> list[Job]:
        """
        List jobs, oldest first.

        Args:
            viewer: Only jobs visible to this principal (all for admins); None for all jobs
            owner: Only jobs of this owner
            statuses: Only jobs with one of these statuses
            job_type: Only jobs of this type
            driver: Only jobs with this driver
        """
        where: list[str] = []
        params: list[Any] = []
        if viewer is not None and not viewer.is_admin:
            where.append("(owner = ? OR visibility = 'users')")
            params.append(viewer.id)
        if owner is not None:
            where.append("owner = ?")
            params.append(owner)
        if statuses is not None:
            statuses = list(statuses)
            if not statuses:
                return []
            where.append(f"status IN ({', '.join('?' * len(statuses))})")
            params.extend(statuses)
        if job_type is not None:
            where.append("type = ?")
            params.append(job_type)
        if driver is not None:
            where.append("driver = ?")
            params.append(driver)
        sql = "SELECT * FROM jobs" + (f" WHERE {' AND '.join(where)}" if where else "") + " ORDER BY created_at, id"
        return [self._job(row) for row in self._query(sql, params)]

    def update(
        self,
        job_id: str,
        changes: Mapping[str, Any],
        *,
        now: float,
        expect_status: Collection[str] | None = None,
        expect_holder: str | None = _UNSET,
        lease_expired_before: float | None = None,
        event: bool = True,
    ) -> Job | None:
        """
        Change columns of a job if it matches the expectations, and append an event.

        Args:
            changes: Column values (see UPDATABLE_COLUMNS); `progress` and `error` as models or dicts,
                times as epoch seconds
            now: Current time (epoch seconds)
            expect_status: Only if the job has one of these statuses
            expect_holder: Only if the lease holder is this one (None: no holder)
            lease_expired_before: Only if the job's lease expired before this time
            event: Append an event for subscribers

        Returns:
            The changed job, or None if it does not exist or does not match the expectations
        """
        unknown = set(changes) - UPDATABLE_COLUMNS
        if unknown:
            raise ValueError(f"Cannot update columns: {', '.join(sorted(unknown))}")
        assignments = [f"{column} = ?" for column in changes] + ["updated_at = ?"]
        params: list[Any] = [_column_value(column, value) for column, value in changes.items()] + [now]
        where = ["id = ?"]
        params.append(job_id)
        if expect_status is not None:
            expect_status = list(expect_status)
            where.append(f"status IN ({', '.join('?' * len(expect_status))})")
            params.extend(expect_status)
        if expect_holder is not _UNSET:
            if expect_holder is None:
                where.append("lease_holder IS NULL")
            else:
                where.append("lease_holder = ?")
                params.append(expect_holder)
        if lease_expired_before is not None:
            where.append("lease_expires IS NOT NULL AND lease_expires < ?")
            params.append(lease_expired_before)
        with self._transaction() as conn:
            cursor = conn.execute(f"UPDATE jobs SET {', '.join(assignments)} WHERE {' AND '.join(where)}", params)
            if cursor.rowcount == 0:
                return None
            if event:
                self._append_event(conn, job_id, now)
            row = conn.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
        return self._job(row)

    def claim(self, job_id: str, holder: str, lease_until: float, now: float, *, from_statuses: Collection[str], resume_until: float | None = None) -> Job | None:
        """
        Make a job active with the given lease holder, if its lease is free, expired or held by the
        same holder.

        Args:
            from_statuses: Statuses the job may have
            resume_until: An interrupted job is only claimed if it does not expire before this time
                (usually `now`)
        """
        statuses = list(from_statuses)
        sql = (
            "UPDATE jobs SET status = 'active', lease_holder = ?, lease_expires = ?, expires_at = NULL, updated_at = ? "
            f"WHERE id = ? AND status IN ({', '.join('?' * len(statuses))}) "
            "AND (lease_holder IS NULL OR lease_holder = ? OR lease_expires < ?)"
        )
        params: list[Any] = [holder, lease_until, now, job_id, *statuses, holder, now]
        if resume_until is not None:
            sql += " AND (status != 'interrupted' OR expires_at IS NULL OR expires_at >= ?)"
            params.append(resume_until)
        with self._transaction() as conn:
            if conn.execute(sql, params).rowcount == 0:
                return None
            self._append_event(conn, job_id, now)
            row = conn.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
        return self._job(row)

    def claim_queued(self, limits: Mapping[str, int | None], holder: str, lease_until: float, now: float) -> Job | None:
        """
        Make the oldest queued server job of one of these types active for the holder, unless as
        many jobs of its type as its limit allows are already active (across all workers).

        Args:
            limits: Job types and their maximum number of active jobs (None: no limit)
        """
        if not limits:
            return None
        types = list(limits)
        with self._transaction() as conn:
            candidates = conn.execute(
                f"SELECT id, type FROM jobs WHERE status = 'queued' AND driver = 'server' AND type IN ({', '.join('?' * len(types))}) ORDER BY created_at, id",
                types,
            ).fetchall()
            active: dict[str, int] = {}
            for candidate in candidates:
                job_type = candidate["type"]
                limit = limits[job_type]
                if limit is not None:
                    if job_type not in active:
                        active[job_type] = conn.execute("SELECT COUNT(*) FROM jobs WHERE type = ? AND status = 'active'", (job_type,)).fetchone()[0]
                    if active[job_type] >= limit:
                        continue
                conn.execute(
                    "UPDATE jobs SET status = 'active', lease_holder = ?, lease_expires = ?, attempts = attempts + 1, updated_at = ? WHERE id = ?",
                    (holder, lease_until, now, candidate["id"]),
                )
                self._append_event(conn, candidate["id"], now)
                row = conn.execute("SELECT * FROM jobs WHERE id = ?", (candidate["id"],)).fetchone()
                return self._job(row)
        return None

    def delete(self, job_id: str) -> Job | None:
        """Delete a job with its steps and events; returns it as it was, or None if it did not exist."""
        with self._transaction() as conn:
            row = conn.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
            if row is None:
                return None
            conn.execute("DELETE FROM jobs WHERE id = ?", (job_id,))
        return self._job(row)

    def due_leases(self, now: float) -> list[Job]:
        """Active jobs whose lease expired."""
        return [self._job(row) for row in self._query("SELECT * FROM jobs WHERE status = 'active' AND lease_expires < ?", (now,))]

    def due_expiry(self, now: float) -> list[Job]:
        """Interrupted and finished jobs whose `expires_at` passed."""
        return [self._job(row) for row in self._query("SELECT * FROM jobs WHERE status != 'active' AND status != 'queued' AND expires_at < ?", (now,))]

    # ── Steps ───────────────────────────────────────────────────────────────

    def save_step(self, job_id: str, key: str, result: JsonValue, now: float) -> bool:
        """Save or overwrite a step; False if the job does not exist."""
        with self._transaction() as conn:
            if conn.execute("SELECT 1 FROM jobs WHERE id = ?", (job_id,)).fetchone() is None:
                return False
            conn.execute(
                "INSERT INTO job_steps (job_id, key, result, at) VALUES (?, ?, ?, ?) ON CONFLICT (job_id, key) DO UPDATE SET result = excluded.result, at = excluded.at",
                (job_id, key, _dump(result), now),
            )
            conn.execute("UPDATE jobs SET updated_at = ? WHERE id = ?", (now, job_id))
        return True

    def steps(self, job_id: str) -> dict[str, Step]:
        rows = self._query("SELECT key, result, at FROM job_steps WHERE job_id = ? ORDER BY at, key", (job_id,))
        return {row["key"]: Step(result=_load(row["result"]), at=datetime.fromtimestamp(row["at"], timezone.utc)) for row in rows}

    # ── Events ──────────────────────────────────────────────────────────────

    def events_after(self, job_id: str, after_seq: int, limit: int = 500) -> list[JobEvent]:
        """Events of a job with a sequence number above `after_seq`, oldest first."""
        rows = self._query(
            "SELECT e.*, j.type AS job_type FROM job_events e JOIN jobs j ON j.id = e.job_id WHERE e.job_id = ? AND e.seq > ? ORDER BY e.seq LIMIT ?",
            (job_id, after_seq, limit),
        )
        return [self._event(row, row["job_type"]) for row in rows]

    def oldest_seq(self, job_id: str) -> int | None:
        """Sequence number of the oldest event kept for a job."""
        rows = self._query("SELECT MIN(seq) FROM job_events WHERE job_id = ?", (job_id,))
        return rows[0][0] if rows else None

    def events_since(self, after_id: int, limit: int = 500) -> list[tuple[JobEvent, str, str]]:
        """Events of all jobs with a global id above `after_id`, with the owner and visibility of their job."""
        rows = self._query(
            "SELECT e.*, j.type AS job_type, j.owner AS job_owner, j.visibility AS job_visibility FROM job_events e JOIN jobs j ON j.id = e.job_id WHERE e.id > ? ORDER BY e.id LIMIT ?",
            (after_id, limit),
        )
        return [(self._event(row, row["job_type"]), row["job_owner"], row["job_visibility"]) for row in rows]

    def max_event_id(self) -> int:
        rows = self._query("SELECT MAX(id) FROM job_events")
        return rows[0][0] or 0


class MemoryJobStore(SqliteJobStore):
    """The SQLite store on an in-memory database: for tests and single-process hosts."""

    def __init__(self, *, events_kept: int = 100):
        super().__init__(":memory:", events_kept=events_kept)
