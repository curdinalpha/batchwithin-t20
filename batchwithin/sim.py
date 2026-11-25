"""Deterministic discrete-event driver (fake clock) for any machine with the
BatchWithin driver interface.

Schedule model
--------------
arrivals : non-decreasing times a_i at which upstream *could* produce item i.
           The source is pull-based: item i is offered at max(a_i, time item
           i-1 was admitted) and, if refused (CLOSED), held and re-offered as
           soon as admission reopens. Gap 0 = an infinitely fast upstream.
requests : non-decreasing times of downstream pulls (1 chunk each).
complete : optional time upstream completes (after its last item).
Same-instant order: due deadline -> requests -> offers.
"""
from __future__ import annotations

import random
from dataclasses import dataclass, field
from typing import List, Optional, Sequence


@dataclass
class RunResult:
    k: int
    d: object
    max_ready: int
    admissions: List[tuple] = field(default_factory=list)  # (t, item)
    requests: List[object] = field(default_factory=list)
    complete_at: Optional[object] = None
    emissions: list = field(default_factory=list)
    max_buffered: int = 0
    held_offers: int = 0
    end_time: object = None


def run_schedule(machine, arrivals: Sequence, requests: Sequence,
                 complete_at=None, horizon=None) -> RunResult:
    arrivals, requests = list(arrivals), list(requests)
    assert arrivals == sorted(arrivals) and requests == sorted(requests)
    res = RunResult(machine.k, machine.d, getattr(machine, "max_ready", 0),
                    requests=requests, complete_at=None)
    i = j = 0
    held = False
    completed = False
    last_adm = None
    while True:
        cands = []
        if machine.next_deadline is not None:
            cands.append(machine.next_deadline)
        if j < len(requests):
            cands.append(requests[j])
        if i < len(arrivals) and not held:
            cands.append(arrivals[i] if last_adm is None else max(arrivals[i], last_adm))
        if complete_at is not None and not completed and i == len(arrivals):
            cands.append(max(complete_at, last_adm) if last_adm is not None else complete_at)
        if not cands:
            break
        t = min(cands)
        if horizon is not None and t > horizon:
            break
        machine.advance(t)
        while j < len(requests) and requests[j] <= t:
            machine.request(t)
            j += 1
        while i < len(arrivals) and (arrivals[i] <= t) and (last_adm is None or last_adm <= t):
            if machine.offer(t, i):
                res.admissions.append((t, i))
                last_adm = t
                held = False
                i += 1
            else:
                if not held:
                    res.held_offers += 1
                held = True
                break
        if (complete_at is not None and not completed and i == len(arrivals)
                and t >= complete_at):
            machine.complete(t)
            res.complete_at = t  # actual call time
            completed = True
        if held and j >= len(requests) and machine.next_deadline is None:
            break  # blocked forever: no more demand
    res.emissions = list(machine.emissions)
    res.max_buffered = getattr(machine, "max_buffered", 0)
    res.end_time = getattr(machine, "_now", None)
    return res


def random_schedule(rng: random.Random, n_items=None, d=10):
    """Mixed-regime integer schedule: bursts (gap 0), sub-D trickles, gaps >= D,
    and downstream stalls far longer than D."""
    n = n_items if n_items is not None else rng.randint(0, 60)
    t, arrivals = 0, []
    regime = rng.choice(["burst", "trickle", "sparse", "mixed"])
    for _ in range(n):
        r = regime if regime != "mixed" else rng.choice(["burst", "trickle", "sparse"])
        if r == "burst":
            t += rng.choice([0, 0, 0, 1])
        elif r == "trickle":
            t += rng.randint(0, max(1, d - 1))
        else:
            t += rng.randint(d - 1 if d > 1 else 0, 3 * d)
        arrivals.append(t)
    m = rng.randint(0, n + 3)
    t2, reqs = 0, []
    style = rng.choice(["eager", "slow", "stall", "bursty"])
    for _ in range(m):
        if style == "eager":
            t2 += rng.randint(0, 2)
        elif style == "slow":
            t2 += rng.randint(d // 2, 2 * d + 1)
        elif style == "stall":
            t2 += rng.choice([0, 1, 5 * d, 9 * d])
        else:
            t2 += rng.choice([0, 0, 0, 3 * d])
        reqs.append(t2)
    complete_at = (arrivals[-1] if arrivals else 0) + rng.randint(0, 2 * d) if rng.random() < 0.7 else None
    return arrivals, reqs, complete_at
