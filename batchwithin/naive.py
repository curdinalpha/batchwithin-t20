"""A deliberately buggy batchWithin, written the way it usually goes wrong.

It has the same driver interface as ``BatchWithin`` so the same harness,
invariant checker and reproducer can run against both. Each bug is a flag:

reset_on_arrival   every arrival re-arms the timer to now + D (debounce, not
                   a window). An upstream faster than D never lets a partial
                   chunk out: the classic timer starvation.
stale_timers       timers are fire-and-forget handles that are never
                   cancelled when a size-K flush happens; the old timer then
                   fires inside the *next* window and flushes it early (or
                   emits an empty chunk).
drop_tick_on_backpressure
                   if the timer fires while downstream has no demand the tick
                   is dropped and the window is re-armed at pull time
                   (pull + D). Under slow pulls partial chunks overstay their
                   window and keep growing; with a fast upstream they only
                   ever leave as size-K chunks.
"""
from __future__ import annotations

import heapq
from typing import Any, List

from .core import Emission


class NaiveBatchWithin:
    def __init__(self, k, d, reset_on_arrival=True, stale_timers=True,
                 drop_tick_on_backpressure=True):
        self.k, self.d = k, d
        self.reset_on_arrival = reset_on_arrival
        self.stale_timers = stale_timers
        self.drop_tick = drop_tick_on_backpressure
        self.buf: List[Any] = []
        self.t0 = None
        self.timers: list = []          # heap of fire times (may contain stale ones)
        self.ready: list = []           # unbounded list of (items, t0, seal, reason)
        self.demand = 0
        self.completed = False
        self.emissions: List[Emission] = []
        self.now = None
        self.state = None

    @property
    def next_deadline(self):
        return self.timers[0] if self.timers else None

    @property
    def can_admit(self):
        return not self.completed

    @property
    def done(self):
        return self.completed and not self.buf and not self.ready

    def _arm(self, t):
        if not self.stale_timers:
            self.timers = []
        heapq.heappush(self.timers, t + self.d)

    def _fire_due(self, t):
        while self.timers and self.timers[0] <= t:
            ft = heapq.heappop(self.timers)
            if self.demand > 0 or not self.drop_tick:
                # bug: emits whatever is there, even [] after a size flush
                self.ready.append((tuple(self.buf), self.t0, ft, "time"))
                self.buf, self.t0 = [], None
                self._deliver(ft)
            else:
                self.pending_rearm = True  # tick lost; re-armed on next pull

    def advance(self, t):
        n = len(self.emissions)
        self.now = t
        self._fire_due(t)
        return self.emissions[n:]

    def offer(self, t, item):
        self.advance(t)
        if not self.buf:
            self.t0 = t
            self._arm(t)
        elif self.reset_on_arrival:
            self._arm(t)
        self.buf.append(item)
        if len(self.buf) >= self.k:
            self.ready.append((tuple(self.buf), self.t0, t, "size"))
            self.buf, self.t0 = [], None
            self._deliver(t)
        return True

    def request(self, t, n=1):
        self.advance(t)
        k0 = len(self.emissions)
        self.demand += n
        if getattr(self, "pending_rearm", False) and self.buf:
            self.pending_rearm = False
            self._arm(t)  # bug: window restarts at pull time
        self._deliver(t)
        return self.emissions[k0:]

    def complete(self, t):
        self.advance(t)
        k0 = len(self.emissions)
        self.completed = True
        if self.buf:
            self.ready.append((tuple(self.buf), self.t0, t, "complete"))
            self.buf = []
        self._deliver(t)
        return self.emissions[k0:]

    def _deliver(self, t):
        while self.demand > 0 and self.ready:
            items, t0, st, why = self.ready.pop(0)
            self.demand -= 1
            self.emissions.append(Emission(items, t0, st, max(st, t), why))

    @property
    def buffered(self):
        return len(self.buf) + sum(len(r[0]) for r in self.ready)
