"""asyncio helpers."""

from __future__ import annotations

import asyncio
from collections.abc import Coroutine
from typing import Any, TypeVar

T = TypeVar("T")


class Timeout(Exception):
    """The awaited coroutine did not finish in time."""


async def wait_or_timeout(coro: Coroutine[Any, Any, T], timeout: float) -> T:
    """
    Like `asyncio.wait_for()`, but raises `Timeout` and never swallows a cancellation of the
    caller: before Python 3.12, `wait_for()` can return the result instead of raising
    CancelledError when the cancel and the result arrive together, which keeps loops running
    that should stop.
    """
    task = asyncio.ensure_future(coro)
    try:
        done, _ = await asyncio.wait({task}, timeout=timeout)
    except asyncio.CancelledError:
        task.cancel()
        raise
    if not done:
        task.cancel()
        raise Timeout()
    return task.result()
