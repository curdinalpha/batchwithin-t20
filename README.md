# batchwithin-t20

`batchWithin(K, D)` without timer starvation, for Stellarator Yard **t=20**
(DISPATCH-03, incidere/solana-agent-bounties#3).

Python 3.12 (also 3.13), standard library only, no dependencies, no network.
Tests use `unittest` and a fake clock, so nothing sleeps and every run is
identical.

## The bug

Three ways `batchWithin` usually breaks, each reproduced on its own
(`batchwithin/naive.py`, one flag per bug):

| bug | what happens | reproducer result |
|---|---|---|
| `reset_on_arrival` | every item re-arms the timer (debounce) | 1 item/tick, D=10, K=1000: **0 chunks in 200 ticks**; the first chunk finally leaves at age 209 |
| `drop_tick_on_backpressure` | timer fires with no demand, tick is dropped, window restarts at pull | windows live up to 45 ticks with D=10 |
| `stale_timers` | size-K flush does not cancel the old timer | next window cut at age 5 (D=10) plus an **empty chunk** |

The fixed machine runs the same schedules with 0 violations (20 chunks, each
sealed exactly at window start + 10).

## The fix: a pure, clock-injected state machine

`batchwithin/core.py`, states `IDLE -> FILLING -> (IDLE | CLOSED) ... DRAINING -> DONE`.

* The deadline is set once, when the first item enters an empty buffer:
  `t0 + D`. Arrivals, pulls, a full ready slot and backpressure never move it.
* Seal at the K-th item (`size`), at exactly `t0 + D` (`time`), or on upstream
  completion (`complete`). Never empty, never more than K.
* Tie rule: a deadline due at `t` is processed before anything else at `t`, so
  an item arriving exactly at the deadline opens the next window.
* Late driver? `advance(t)` seals at the deadline, not at `t`.
* Bounded: a ready queue of `R` chunks (default 1). If it is full when the
  active chunk seals, the machine goes `CLOSED` and refuses `offer()` until a
  pull frees a slot. At most `(R+1)*K` items are buffered. Nothing is dropped.
* Delivery time of chunk j is exactly `max(seal_j, request_j)`.
* There is no timer object inside the machine, just one deadline value guarded
  by state, so a stale timer can't do anything.

`batchwithin/stream.py` is the asyncio adapter (`async for chunk in
batch_within(src, K, D)`). Every transition happens in one synchronous
callback, with exactly one `call_at` handle that is re-armed from
`machine.next_deadline`. The pump holds at most one refused item and does not
advance upstream while downstream is stalled.

## Invariants (checked from the output trace alone)

`batchwithin/invariants.py` judges any implementation, naive or fixed:

* I1: `1 <= len(chunk) <= K`
* I2: output is an in-order prefix of the admitted items (all of them once drained)
* I3: the window starts at the first item's admission and never outlives `t0 + D`
* I4: exact reasons: `size` means len K sealed at the K-th admission, `time` means sealed at exactly `t0 + D`
* I5: a `time` chunk is never cut early (the next item came at `>= t0 + D`)
* I6: delivered at exactly `max(seal, request)`, no stall and no early emit
* I7: buffered `<= (R+1)K`

## Tests: 57 unittest cases, all passing

| file | what |
|---|---|
| `test_core.py` (29) | every transition, tie rules, catch-up, CLOSED/reopen, completion, protocol errors |
| `test_properties.py` (9) | **4,000 seeded random schedules** + **559,872 exhaustive small schedules** compared exactly against an independent tick-stepped oracle (`tests/oracle.py`), plus invariants; float and `Fraction` time; drained liveness; time translation, time scaling and pull independence metamorphic tests; determinism hash |
| `test_reproducer.py` (8) | each naive bug fails on its own; random search finds every bug; fixed never fails |
| `test_stream.py` (11) | asyncio adapter on a **real asyncio loop with a virtual clock**: 2,000 random schedules whose recorded yield/pull times are replayed into the oracle (exact match); infinite upstream; slow consumer; one timer per window; errors; aclose; a real wall-clock smoke test |

Exhaustive grid: 0..2 items per tick over 5 ticks, every pull subset over 6
ticks, K and D in 1..3, R in 1..2, with and without completion.

## Run

```bash
python3.12 -m unittest discover -s tests -t . -v   # 57 tests, ~17 s
python3.12 reproduce.py                           # exit 0 = bugs reproduced, fix clean
BW_SEED=7 BW_N=20000 python3.12 -m unittest tests.test_properties   # other seeds / more runs
```

Captured output is in `evidence/`: py3.12 and py3.13 runs, a run with no
network (`unshare -rn`), and 3 identical reproducer hashes.

## Use

```python
from batchwithin import batch_within

async for chunk in batch_within(source, 64, 0.020):   # K=64, D=20 ms
    await sink(chunk)
```

Or drive `BatchWithin(K, D, max_ready)` yourself: `offer(t, x)`,
`request(t)`, `advance(t)` at `next_deadline`, `complete(t)`.

MIT licensed.
