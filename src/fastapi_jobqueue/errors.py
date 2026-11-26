"""Exceptions of the job queue."""

from __future__ import annotations


class JobCancelled(BaseException):
    """
    Raised in a job handler when the user cancelled the job.

    A BaseException like `asyncio.CancelledError`, so that `except Exception` handlers that keep a
    job going despite errors do not swallow it.
    """


class JobQueueError(Exception):
    """Base class of the errors that the REST API maps to HTTP responses."""

    status_code = 400

    def __init__(self, message: str):
        super().__init__(message)
        self.message = message


class UnknownJobType(JobQueueError):
    """No job type with this name is registered, or it cannot be submitted this way."""

    status_code = 400


class InvalidParams(JobQueueError):
    """The parameters do not match the job type's parameter model."""

    status_code = 422


class JobNotFound(JobQueueError):
    """The job does not exist or is not visible to the caller."""

    status_code = 404


class PermissionDenied(JobQueueError):
    """The caller may see the job but not do this with it."""

    status_code = 403


class InvalidState(JobQueueError):
    """The job's status does not allow the operation."""

    status_code = 409


class LeaseConflict(JobQueueError):
    """Another driver holds the job's lease, or the caller's lease is no longer valid."""

    status_code = 409


class ResourceBusy(JobQueueError):
    """A resource the job needs is held by another job or user (raised by the host's hooks)."""

    status_code = 409


class InputChanged(JobQueueError):
    """The job's input changed since it was submitted, so it cannot be resumed."""

    status_code = 409
