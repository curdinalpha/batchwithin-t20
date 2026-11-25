"""Trace-only invariant checker. It needs no knowledge of the implementation,
so it judges the fixed machine and the naive one with the same rules."""
from __future__ import annotations

from typing import List

from .sim import RunResult


def check(res: RunResult, require_drained: bool = False) -> List[str]:
    v: List[str] = []
    K, D = res.k, res.d
    adm_t = {item: t for t, item in res.admissions}
    order = [item for _, item in res.admissions]
    flat = [x for e in res.emissions for x in e.chunk]
    # I1 non-empty, bounded size
    for n, e in enumerate(res.emissions):
        if not 1 <= len(e.chunk) <= K:
            v.append(f"I1 chunk#{n} size {len(e.chunk)} not in 1..{K}")
    # I2 conservation + FIFO: emitted items are exactly a prefix of admitted ones
    if flat != order[:len(flat)]:
        v.append("I2 emitted items are not an in-order prefix of admitted items")
    if require_drained and flat != order:
        v.append(f"I2 {len(order) - len(flat)} admitted items never emitted")
    for n, e in enumerate(res.emissions):
        if not e.chunk or any(x not in adm_t for x in e.chunk):
            continue
        first, last = adm_t[e.chunk[0]], adm_t[e.chunk[-1]]
        # I3 fixed deadline: window opens at first admission, never extended
        if e.window_start != first:
            v.append(f"I3 chunk#{n} window_start {e.window_start} != first admission {first}")
        if e.seal_time > first + D:  # compare absolute deadlines (float-safe)
            v.append(f"I3 chunk#{n} lived {e.seal_time - first} > D={D} (deadline reset/starved)")
        if last > e.seal_time:
            v.append(f"I3 chunk#{n} holds an item admitted after its seal")
        # I4 exact K-or-D reason semantics
        if e.reason == "size" and (len(e.chunk) != K or e.seal_time != last):
            v.append(f"I4 chunk#{n} 'size' but len={len(e.chunk)} seal={e.seal_time} last={last}")
        if e.reason == "time" and (len(e.chunk) >= K or e.seal_time != first + D):
            v.append(f"I4 chunk#{n} 'time' but len={len(e.chunk)} seal={e.seal_time} != {first}+D")
        if e.reason == "complete" and (len(e.chunk) >= K or e.seal_time != res.complete_at):
            v.append(f"I4 chunk#{n} 'complete' len={len(e.chunk)} seal={e.seal_time} "
                     f"!= completion {res.complete_at}")
        # I5 maximal window: a 'time' chunk is not cut early -> next item came at >= t0+D
        nxt = n + 1
        if e.reason == "time" and nxt < len(res.emissions) and res.emissions[nxt].chunk:
            nf = adm_t.get(res.emissions[nxt].chunk[0])
            if nf is not None and nf < first + D:
                v.append(f"I5 chunk#{n} cut early: next item admitted at {nf} < {first}+D")
    # I6 exact delivery: j-th chunk delivered at max(seal_j, request_j)
    for n, e in enumerate(res.emissions):
        if n >= len(res.requests):
            v.append(f"I6 chunk#{n} delivered without a request")
            continue
        want = max(e.seal_time, res.requests[n])
        if e.deliver_time != want:
            v.append(f"I6 chunk#{n} delivered at {e.deliver_time}, expected {want} (stall/early)")
    # I7 bounded buffering
    if res.max_ready and res.max_buffered > (res.max_ready + 1) * K:
        v.append(f"I7 buffered {res.max_buffered} > (R+1)K")
    return v
