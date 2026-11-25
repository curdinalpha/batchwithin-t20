"""A real asyncio event loop whose clock is virtual.

Nothing sleeps: when the loop would block waiting for the next timer, the
clock jumps exactly to that timer's deadline. Ordering, cancellation and
call_at semantics are asyncio's own, so the adapter under test runs on the
genuine scheduler, just deterministically and in microseconds.
"""
from __future__ import annotations

import asyncio
import selectors


class _VirtualSelector(selectors.BaseSelector):
    def __init__(self):
        self._real = selectors.DefaultSelector()
        self.loop = None

    def register(self, fileobj, events, data=None):
        return self._real.register(fileobj, events, data)

    def unregister(self, fileobj):
        return self._real.unregister(fileobj)

    def modify(self, fileobj, events, data=None):
        return self._real.modify(fileobj, events, data)

    def get_map(self):
        return self._real.get_map()

    def close(self):
        self._real.close()

    def select(self, timeout=None):
        events = self._real.select(0)
        if events or timeout == 0:
            return events
        loop = self.loop
        if loop._scheduled:
            loop._vtime = max(loop._vtime, loop._scheduled[0].when())
            return []
        if timeout is None:
            raise RuntimeError("virtual-time deadlock: nothing scheduled and nothing ready")
        loop._vtime += timeout
        return []


class VirtualTimeLoop(asyncio.SelectorEventLoop):
    def __init__(self, start: float = 0.0):
        sel = _VirtualSelector()
        super().__init__(selector=sel)
        sel.loop = self
        self._vtime = float(start)

    def time(self) -> float:
        return self._vtime


def run_virtual(coro, start: float = 0.0):
    loop = VirtualTimeLoop(start)
    try:
        return loop.run_until_complete(coro)
    finally:
        try:
            loop.run_until_complete(loop.shutdown_asyncgens())
        finally:
            loop.close()
