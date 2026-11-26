"""
Fan-out of job events to the subscribers of one worker process.

One background task per worker tails the store's event table by global id and puts each event
into the queues of the subscribers whose filter accepts it. Events of jobs that run in other
workers arrive the same way, since all workers share the store.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from dataclasses import dataclass, field

from .models import JobEvent
from .store import SqliteJobStore

logger = logging.getLogger(__name__)

EventFilter = Callable[[JobEvent, str, str], bool]
"""Receives an event and the owner and visibility of its job; returns whether to deliver it."""


@dataclass(eq=False)
class Subscription:
    accept: EventFilter
    queue: asyncio.Queue[JobEvent] = field(default_factory=lambda: asyncio.Queue(maxsize=1000))
    overflowed: bool = False


class EventBus:
    def __init__(self, store: SqliteJobStore, poll_interval: float):
        self._store = store
        self._poll_interval = poll_interval
        self._subscriptions: set[Subscription] = set()
        self._last_id = 0
        self._task: asyncio.Task[None] | None = None
        self._wake = asyncio.Event()

    async def start(self) -> None:
        self._last_id = await asyncio.to_thread(self._store.max_event_id)
        self._task = asyncio.create_task(self._run(), name="fastapi-jobqueue-events")

    async def stop(self) -> None:
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None

    def subscribe(self, accept: EventFilter) -> Subscription:
        subscription = Subscription(accept)
        self._subscriptions.add(subscription)
        return subscription

    def unsubscribe(self, subscription: Subscription) -> None:
        self._subscriptions.discard(subscription)

    def poke(self) -> None:
        """Check for new events now, e.g. after this worker wrote one."""
        self._wake.set()

    async def _run(self) -> None:
        while True:
            try:
                await asyncio.wait_for(self._wake.wait(), self._poll_interval)
            except TimeoutError:
                pass
            self._wake.clear()
            try:
                await self._deliver()
            except Exception:
                logger.exception("Could not read job events")

    async def _deliver(self) -> None:
        while True:
            rows = await asyncio.to_thread(self._store.events_since, self._last_id)
            if not rows:
                return
            for event, owner, visibility in rows:
                self._last_id = max(self._last_id, event.id or 0)
                for subscription in list(self._subscriptions):
                    if subscription.overflowed or not subscription.accept(event, owner, visibility):
                        continue
                    try:
                        subscription.queue.put_nowait(event)
                    except asyncio.QueueFull:
                        # a subscriber that does not read is dropped; its stream ends
                        subscription.overflowed = True
            if len(rows) < 500:
                return
