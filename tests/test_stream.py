"""asyncio adapter on a real asyncio loop with a virtual clock (fake time,
real scheduler). Each run's actual source-yield and pull times are recorded
and replayed into the independent oracle: the async output must match it
exactly and pass the invariant checker."""
import asyncio
import random
import time
import unittest

from batchwithin import VirtualTimeLoop, batch_within, run_virtual
from batchwithin.invariants import check
from batchwithin.sim import RunResult
from tests.oracle import oracle


async def timed_source(arrivals, log, fail_at=None):
    loop = asyncio.get_running_loop()
    for i, a in enumerate(arrivals):
        delay = a - loop.time()
        if delay > 0:
            await asyncio.sleep(delay)
        if fail_at is not None and i == fail_at:
            raise ValueError("upstream boom")
        log.append((loop.time(), i))
        yield i


async def infinite_source(gap, log):
    loop = asyncio.get_running_loop()
    i = 0
    while True:
        await asyncio.sleep(gap)
        log.append((loop.time(), i))
        yield i
        i += 1


async def consume(stream, pulls, req_log, out, until=None):
    loop = asyncio.get_running_loop()
    for p in pulls:
        if p > loop.time():
            await asyncio.sleep(p - loop.time())
        if until is not None and loop.time() > until:
            break
        req_log.append(loop.time())
        try:
            out.append(await stream.__anext__())
        except StopAsyncIteration:
            break


def run_async_schedule(k, d, r, arrivals, pulls):
    yields, reqs, out = [], [], []

    async def main():
        s = batch_within(timed_source(arrivals, yields), k, d, r, with_meta=True)
        s.start()
        await consume(s, pulls, reqs, out)
        await s.aclose()
        return s
    s = run_virtual(main())
    return s, yields, reqs, out


class TestVirtualLoop(unittest.TestCase):
    def test_clock_jumps_exactly(self):
        async def main():
            loop = asyncio.get_running_loop()
            t0 = loop.time()
            await asyncio.sleep(3600)
            return loop.time() - t0
        w = time.perf_counter()
        self.assertEqual(run_virtual(main()), 3600)
        self.assertLess(time.perf_counter() - w, 1.0)

    def test_deadlock_detected(self):
        async def main():
            await asyncio.get_running_loop().create_future()
        with self.assertRaises(RuntimeError):
            run_virtual(main())


class TestStreamEquivalence(unittest.TestCase):
    def test_random_schedules_match_oracle(self):
        rng = random.Random(4242)
        for i in range(2000):
            k, d, r = rng.randint(1, 6), rng.randint(1, 8), rng.randint(1, 3)
            t, arr = 0, []
            for _ in range(rng.randint(1, 40)):
                t += rng.choice([0, 0, 1, rng.randint(0, d), rng.randint(d, 3 * d)])
                arr.append(t)
            t, pulls = 0, []
            for _ in range(rng.randint(1, 30)):
                t += rng.choice([0, 1, rng.randint(0, 2 * d), 6 * d])
                pulls.append(t)
            s, yields, reqs, out = run_async_schedule(k, d, r, arr, pulls)
            ys = [int(y) for y, _ in yields]
            self.assertEqual(ys, [y for y, _ in yields])  # integral virtual times stay exact
            reqs = [int(x) for x in reqs]
            comp = None if s.completed_at is None else int(s.completed_at)
            exp, adm, ca = oracle(k, d, r, ys, reqs, comp)
            self.assertEqual(ca, comp)
            got = [(e.chunk, e.window_start, e.seal_time, e.deliver_time, e.reason) for e in out]
            self.assertEqual(got, exp[:len(got)], f"i={i} K={k} D={d} R={r} arr={arr} pulls={pulls}")
            # consumer blocks on each pull, so every delivered chunk is accounted for
            self.assertEqual(len(got), min(len(exp), len(reqs)))
            res = RunResult(k, d, r, admissions=adm, requests=reqs, emissions=out, complete_at=ca,
                            max_buffered=s.machine.max_buffered)
            self.assertEqual(check(res), [], f"i={i}")
            self.assertEqual(s.stale_fires, 0)


class TestStreamBehaviour(unittest.TestCase):
    def test_infinite_fast_upstream_no_starvation(self):
        """1 item per ms forever, D = 10 ms, K huge: a chunk every 10 ms."""
        out = []

        async def main():
            s = batch_within(infinite_source(0.001 * 1, []), 10**6, 0.010, with_meta=True)
            for _ in range(50):
                out.append(await s.__anext__())
            await s.aclose()
        run_virtual(main())
        self.assertEqual(len(out), 50)
        for e in out:
            self.assertEqual(e.reason, "time")
            self.assertLessEqual(e.seal_time, e.window_start + 0.010)
            self.assertGreaterEqual(len(e.chunk), 9)

    def test_slow_consumer_does_not_advance_upstream(self):
        log = []

        async def main():
            s = batch_within(infinite_source(1, log), 3, 10)
            first = await s.__anext__()
            await asyncio.sleep(1000)  # downstream stalls for 100 windows
            r = first, len(log), s.machine.buffered
            await s.aclose()
            return r
        first, produced, buffered = run_virtual(main())
        self.assertEqual(first, [0, 1, 2])
        self.assertLessEqual(buffered, 2 * 3)
        self.assertLessEqual(produced, 3 + 2 * 3 + 1)  # (R+1)K + 1 read-ahead

    def test_completion_flush_and_stop(self):
        async def main():
            got = []
            async for c in batch_within(timed_source([0, 1, 2, 3, 4], []), 2, 100):
                got.append(c)
            return got
        self.assertEqual(run_virtual(main()), [[0, 1], [2, 3], [4]])

    def test_empty_source(self):
        async def main():
            return [c async for c in batch_within(timed_source([], []), 3, 10)]
        self.assertEqual(run_virtual(main()), [])

    def test_upstream_error_propagates_after_aclose(self):
        async def main():
            s = batch_within(timed_source([0, 1, 2, 3], [], fail_at=3), 2, 100)
            got = [await s.__anext__()]
            with self.assertRaises(ValueError):
                await s.__anext__()
            return got
        self.assertEqual(run_virtual(main()), [[0, 1]])

    def test_single_timer_handle(self):
        """One armed timer per window: arms == number of windows opened."""
        async def main():
            s = batch_within(timed_source(list(range(0, 100)), []), 1000, 10, with_meta=True)
            got = [e async for e in s]
            return s, got
        s, got = run_virtual(main())
        self.assertEqual(s.timer_arms, len(got))
        self.assertEqual(s.stale_fires, 0)

    def test_aclose_cancels_pump(self):
        async def main():
            s = batch_within(infinite_source(1, []), 2, 10)
            await s.__anext__()
            await s.aclose()
            return s._pump.cancelled() or s._pump.done()
        self.assertTrue(run_virtual(main()))

    def test_real_wall_clock_smoke(self):
        """Same adapter on the default loop with real time (small numbers)."""
        async def main():
            got = []
            s = batch_within(infinite_source(0.001, []), 10**6, 0.02, with_meta=True)
            async for e in s:
                got.append(e)
                if len(got) == 3:
                    break
            await s.aclose()
            return got
        got = asyncio.run(main())
        self.assertEqual(len(got), 3)
        for e in got:
            self.assertEqual(e.reason, "time")
            self.assertGreater(len(e.chunk), 0)


if __name__ == "__main__":
    unittest.main()
