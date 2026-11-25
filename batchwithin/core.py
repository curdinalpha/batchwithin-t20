"""Pure, clock-injected state machine for batchWithin(K, D).

Contract (what "exact K-or-D emission" means here)
--------------------------------------------------
* A window opens when an item is admitted into an empty active buffer at
  time t0. Its deadline is t0 + D and is fixed: later arrivals, pulls,
  backpressure or a full ready slot never move it.
* The active chunk is sealed at the first of:
    - the K-th admission (reason "size", seal time = that admission time), or
    - t0 + D with 1..K-1 items (reason "time", seal time = exactly t0 + D), or
    - upstream completion with 1..K-1 items (reason "complete").
  Tie rule: a deadline due at time t is processed before anything else at t,
  so an item arriving exactly at t0 + D opens the next window.
* Sealed chunks go to a bounded ready queue (capacity R, default 1). If it is
  full the sealed chunk stays in the active slot ("CLOSED") and admission
  pauses: that is the backpressure signal to upstream. Nothing is ever
  dropped, merged, split or emitted empty.
* A chunk is delivered at max(seal time, time of the matching request).
  Requests (downstream pulls) accumulate as demand.

Every public method takes the current time and first runs ``_advance(t)``;
time must be monotonic. Because the deadline is a single value guarded by the
state, a stale timer callback is a no-op by construction (there is no timer
object to forget to cancel), which is what makes this race-free when driven
from asyncio callbacks, threads behind a lock, or a simulated clock.
"""
from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from enum import Enum
from typing import Any, Deque, List, Optional


class ProtocolError(RuntimeError):
    """Driver misuse: time went backwards, offer while closed, use after done."""


class State(Enum):
    IDLE = "IDLE"          # active buffer empty, no deadline armed
    FILLING = "FILLING"    # 1..K-1 items, deadline armed
    CLOSED = "CLOSED"      # active chunk sealed but ready queue full: admission paused
    DRAINING = "DRAINING"  # upstream completed, ready chunks left to deliver
    DONE = "DONE"


@dataclass(frozen=True)
class Emission:
    chunk: tuple
    window_start: Any
    seal_time: Any
    deliver_time: Any
    reason: str  # "size" | "time" | "complete"


class BatchWithin:
    def __init__(self, k: int, d, max_ready: int = 1):
        if not isinstance(k, int) or k < 1:
            raise ValueError("K must be an int >= 1")
        if not d > 0:
            raise ValueError("D must be > 0")
        if not isinstance(max_ready, int) or max_ready < 1:
            raise ValueError("max_ready must be an int >= 1")
        self.k, self.d, self.max_ready = k, d, max_ready
        self.state = State.IDLE
        self._active: List[Any] = []
        self._t0 = None
        self._deadline = None
        self._sealed_active: Optional[tuple] = None  # (items, t0, seal_t, reason) while CLOSED
        self._ready: Deque[tuple] = deque()
        self._demand = 0
        self._now = None
        self._completed = False
        self.emissions: List[Emission] = []
        self.trace: List[tuple] = []  # (time, event, state) for debugging/replay
        self.max_buffered = 0

    # ----------------------------------------------------------------- queries
    @property
    def next_deadline(self):
        """The single timer the driver must arm (None = no timer)."""
        return self._deadline if self.state is State.FILLING else None

    @property
    def can_admit(self) -> bool:
        return self.state in (State.IDLE, State.FILLING) and not self._completed

    @property
    def buffered(self) -> int:
        n = len(self._active) + sum(len(c[0]) for c in self._ready)
        if self._sealed_active is not None:
            n += len(self._sealed_active[0])
        return n

    @property
    def demand(self) -> int:
        return self._demand

    @property
    def done(self) -> bool:
        return self.state is State.DONE

    # ------------------------------------------------------------------ inputs
    def advance(self, t) -> List[Emission]:
        n = len(self.emissions)
        self._advance(t)
        return self.emissions[n:]

    def offer(self, t, item) -> bool:
        """Upstream presents one item at time t. Returns False (not admitted)
        while CLOSED; the caller must hold the item and retry (pull-based)."""
        self._advance(t)
        if self._completed or self.state is State.DONE:
            raise ProtocolError("offer after complete")
        if self.state is State.CLOSED:
            return False
        if self.state is State.IDLE:
            self._t0, self._deadline = t, t + self.d
            self.state = State.FILLING
        self._active.append(item)
        self.trace.append((t, "admit", self.state.value))
        if len(self._active) == self.k:
            self._seal(t, "size")
        self.max_buffered = max(self.max_buffered, self.buffered)
        return True

    def request(self, t, n: int = 1) -> List[Emission]:
        """Downstream asks for n more chunks at time t."""
        if n < 1:
            raise ValueError("n must be >= 1")
        self._advance(t)
        if self.state is State.DONE:
            return []  # demand after termination is a no-op (reactive-streams rule)
        k0 = len(self.emissions)
        self._demand += n
        self.trace.append((t, "request", self.state.value))
        self._deliver(t)
        return self.emissions[k0:]

    def complete(self, t) -> List[Emission]:
        """Upstream finished: flush the partial chunk (never an empty one)."""
        self._advance(t)
        k0 = len(self.emissions)
        if self._completed:
            return []
        self._completed = True
        self.trace.append((t, "complete", self.state.value))
        if self.state is State.FILLING:
            self._seal(t, "complete")
        self._maybe_finish()
        return self.emissions[k0:]

    # ---------------------------------------------------------------- internals
    def _advance(self, t) -> None:
        if self._now is not None and t < self._now:
            raise ProtocolError(f"time went backwards: {t} < {self._now}")
        self._now = t
        if self.state is State.FILLING and t >= self._deadline:
            # catch-up is exact: the chunk is sealed *at* the deadline, not at t
            self._seal(self._deadline, "time")

    def _seal(self, at, reason) -> None:
        chunk = (tuple(self._active), self._t0, at, reason)
        assert 1 <= len(chunk[0]) <= self.k
        self._active = []
        self._deadline = None
        self._t0 = None
        self.trace.append((at, "seal:" + reason, self.state.value))
        if len(self._ready) < self.max_ready:
            self._ready.append(chunk)
            self.state = State.IDLE
            self._deliver(at)
        else:
            self._sealed_active = chunk
            self.state = State.CLOSED

    def _deliver(self, t) -> None:
        while self._demand > 0 and self._ready:
            items, t0, seal_t, reason = self._ready.popleft()
            self._demand -= 1
            self.emissions.append(Emission(items, t0, seal_t, max(seal_t, t), reason))
            self.trace.append((t, "emit", self.state.value))
            if self._sealed_active is not None:  # a slot freed: reopen admission
                self._ready.append(self._sealed_active)
                self._sealed_active = None
                if self.state is State.CLOSED:
                    self.state = State.IDLE
        self._maybe_finish()

    def _maybe_finish(self) -> None:
        if self._completed and self.state is not State.DONE:
            if not self._ready and self._sealed_active is None and not self._active:
                self.state = State.DONE
            else:
                self.state = State.DRAINING
