"""asyncio adapter: ``async for chunk in batch_within(source, K, D)``.

All state lives in the pure ``BatchWithin`` machine. The adapter only
translates events: an upstream item -> ``offer``; ``__anext__`` ->
``request``; the single timer -> ``advance``. Every transition runs inside one
synchronous callback on the loop thread, so no lock is needed and no await
can interleave in the middle of a transition. There is exactly one timer
handle, re-armed from ``machine.next_deadline`` after every transition; a stale
handle is cancelled, and even if one fired the machine would treat it as a
no-op because it only seals when ``now >= deadline`` of the *current* window.

Backpressure: the pump task holds at most one item that was refused
(machine CLOSED) and waits on an Event until a pull reopens admission, so the
upstream iterator is not advanced while downstream is stalled.
Buffered items <= (max_ready + 1) * K + 1.
"""
from __future__ import annotations

import asyncio
from typing import AsyncIterable, AsyncIterator, Optional

from .core import BatchWithin, Emission

_END = object()


class BatchWithinStream:
    def __init__(self, source: AsyncIterable, k: int, d: float, max_ready: int = 1,
                 *, with_meta: bool = False):
        self.machine = BatchWithin(k, d, max_ready)
        self._src = source.__aiter__()
        self._with_meta = with_meta
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._timer: Optional[asyncio.TimerHandle] = None
        self._timer_at = None
        self._pump: Optional[asyncio.Task] = None
        self._waiter: Optional[asyncio.Future] = None
        self._outbox: list = []
        self._reopen = asyncio.Event()
        self._error: Optional[BaseException] = None
        self.completed_at = None
        self._src_closed = False
        self.timer_arms = 0
        self.stale_fires = 0

    def __aiter__(self) -> AsyncIterator:
        return self

    def _now(self):
        return self._loop.time()

    def start(self):
        """Subscribe eagerly (default is lazy: on the first pull)."""
        self._start()

    def _start(self):
        if self._loop is None:
            self._loop = asyncio.get_running_loop()
            self._pump = self._loop.create_task(self._run_pump())

    # -- one place that applies the effects of any transition
    def _after(self, new):
        self._outbox.extend(new)
        dl = self.machine.next_deadline
        if dl != self._timer_at:
            if self._timer is not None:
                self._timer.cancel()
            self._timer, self._timer_at = None, dl
            if dl is not None:
                self._timer = self._loop.call_at(dl, self._on_timer, dl)
                self.timer_arms += 1
        if self.machine.can_admit:
            self._reopen.set()
        else:
            self._reopen.clear()
        self._wake()

    def _wake(self):
        w = self._waiter
        if w is not None and not w.done() and (self._outbox or self.machine.done or self._error):
            w.set_result(None)

    def _on_timer(self, armed_for):
        if armed_for != self._timer_at:
            self.stale_fires += 1  # cannot happen (handles are cancelled); counted for tests
            return
        self._timer, self._timer_at = None, None
        self._after(self.machine.advance(self._now()))

    async def _run_pump(self):
        try:
            async for item in self._src:
                while True:
                    n0 = len(self.machine.emissions)
                    ok = self.machine.offer(self._now(), item)
                    self._after(self.machine.emissions[n0:])
                    if ok:
                        break
                    await self._reopen.wait()
            self.completed_at = self._now()
            self._after(self.machine.complete(self.completed_at))
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # upstream failure: surface downstream
            self._error = exc
            self._wake()

    async def __anext__(self):
        self._start()
        if not self._outbox and self._error is None and not self.machine.done:
            self._after(self.machine.request(self._now()))
        while not self._outbox:
            if self._error is not None:
                err, self._error = self._error, None
                await self.aclose()
                raise err
            if self.machine.done:
                await self.aclose()
                raise StopAsyncIteration
            self._waiter = self._loop.create_future()
            try:
                await self._waiter
            finally:
                self._waiter = None
        e: Emission = self._outbox.pop(0)
        return e if self._with_meta else list(e.chunk)

    async def aclose(self):
        if self._timer is not None:
            self._timer.cancel()
            self._timer = None
        if self._pump is not None and not self._pump.done():
            self._pump.cancel()
            try:
                await self._pump
            except asyncio.CancelledError:
                pass
        close = getattr(self._src, "aclose", None)
        if close is not None and not self._src_closed:
            self._src_closed = True
            await close()


def batch_within(source: AsyncIterable, k: int, d: float, max_ready: int = 1,
                 *, with_meta: bool = False) -> BatchWithinStream:
    return BatchWithinStream(source, k, d, max_ready, with_meta=with_meta)
