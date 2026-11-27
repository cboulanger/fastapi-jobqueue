"""What the host supplies: who the caller is, what they may submit, and how resources map to its locks."""

from __future__ import annotations

import inspect
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .models import Job


@dataclass(frozen=True)
class Principal:
    """The caller of the REST API."""

    id: str
    is_admin: bool = False


def _anonymous() -> Principal:
    raise RuntimeError("JobAuth.current_principal is not configured")


@dataclass
class JobAuth:
    """
    Authentication of the REST API.

    `current_principal` is a FastAPI dependency (it may itself declare dependencies, e.g. on the
    request or the host's session) that returns the caller, or raises an HTTPException (401).
    `may_submit` decides whether a principal may submit a job type through the REST API.
    """

    current_principal: Callable[..., Principal | Awaitable[Principal]] = _anonymous
    may_submit: Callable[[Principal, str], bool] = field(default=lambda _principal, _type: True)


class JobHooks:
    """
    Hooks through which the host maps a job's resources to its own locks and checks its input.
    Override the methods that apply; each may be sync or async.
    """

    def acquire(self, job: Job) -> Any:
        """
        Called before a job is created, with the job as it will be stored. Acquire the job's
        resources (e.g. lock `document:<id>` for the holder `job:<job.id>`), or raise
        `ResourceBusy`.
        """
        return None

    def renew(self, job: Job) -> Any:
        """Called when the driver renews the job's lease, e.g. to refresh the host's locks."""
        return None

    def release(self, job: Job) -> Any:
        """Called once when the job no longer holds its resources: it finished, expired or was deleted."""
        return None

    def deleted(self, job: Job) -> Any:
        """Called once after a job row was deleted (by `delete`, `delete_all` or the retention cleanup), e.g. to remove files the job produced."""
        return None

    def input_changed(self, job: Job) -> bool | Awaitable[bool]:
        """Called before an interrupted job is resumed; return True if its input changed (compare `job.input_hash`)."""
        return False


async def call_hook(result: Any) -> Any:
    """Await the result of a hook if it is awaitable."""
    if inspect.isawaitable(result):
        return await result
    return result
