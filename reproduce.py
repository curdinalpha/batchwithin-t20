#!/usr/bin/env python3
"""Deterministic reproducer: the same schedules on a naive batchWithin (one
bug enabled at a time) and on the fixed state machine, judged by the same
trace-only invariant checker. No sleeps, no randomness, fake integer clock.

Exit code 0 iff every bug reproduces on the naive version AND the fixed
machine has zero violations on every scenario.
"""
import hashlib
import json
import sys

from batchwithin import BatchWithin, NaiveBatchWithin
from batchwithin.invariants import check
from batchwithin.sim import run_schedule

ONLY = dict(reset_on_arrival=False, stale_timers=False, drop_tick_on_backpressure=False)


def naive(k, d, bug):
    flags = dict(ONLY)
    flags[bug] = True
    return NaiveBatchWithin(k, d, **flags)


SCENARIOS = [
    # name, bug flag, K, D, arrivals, requests, complete_at, note
    ("starvation", "reset_on_arrival", 1000, 10, list(range(200)), [0] * 200, None,
     "1 item/tick forever, D=10, eager downstream"),
    ("backpressure", "drop_tick_on_backpressure", 50, 10, list(range(0, 200, 2)),
     list(range(35, 400, 35)), None, "item every 2 ticks, pull every 35 ticks"),
    ("stale-timer", "stale_timers", 3, 10, [0, 1, 2, 5, 6], [0] * 10, None,
     "size-K flush at t=2, next window opens at t=5"),
]


def summarize(res, live_until):
    ages = [e.seal_time - e.window_start for e in res.emissions if e.window_start is not None]
    return {
        "chunks": len(res.emissions),
        "live": sum(1 for e in res.emissions if e.deliver_time <= live_until),
        "partial_chunks": sum(1 for e in res.emissions if len(e.chunk) < res.k),
        "empty_chunks": sum(1 for e in res.emissions if not e.chunk),
        "max_window_age": max(ages) if ages else None,
        "items_out": sum(len(e.chunk) for e in res.emissions),
    }


def main():
    ok = True
    digest = hashlib.sha256()
    print(f"{'scenario':13} {'impl':28} {'chunks':>6} {'live':>4} {'partial':>7} {'empty':>5} "
          f"{'maxAge':>6} {'violations':>10}  first violation")
    for name, bug, k, d, arr, req, comp, note in SCENARIOS:
        print(f"# {name}: K={k} D={d}; {note}")
        for label, m in ((f"naive[{bug}]", naive(k, d, bug)), ("fixed BatchWithin", BatchWithin(k, d))):
            res = run_schedule(m, arr, req, comp, horizon=max(arr + req) + 4 * d)
            v = check(res)
            s = summarize(res, arr[-1])
            print(f"{name:13} {label:28} {s['chunks']:>6} {s['live']:>4} {s['partial_chunks']:>7} "
                  f"{s['empty_chunks']:>5} {str(s['max_window_age']):>6} {len(v):>10}  "
                  f"{v[0] if v else '-'}")
            if label.startswith("naive") and not v:
                ok = False
                print("  !! bug did not reproduce")
            if label.startswith("fixed"):
                if v:
                    ok = False
                digest.update(json.dumps([(e.chunk, e.window_start, e.seal_time, e.deliver_time,
                                           e.reason) for e in res.emissions]).encode())
    print("fixed trace sha256:", digest.hexdigest())
    print("RESULT:", "PASS (all bugs reproduced, fixed machine clean)" if ok else "FAIL")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
