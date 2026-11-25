"""The naive implementation must fail, one bug at a time, on deterministic
schedules; the fixed machine must pass the same schedules."""
import random
import subprocess
import sys
import unittest
from pathlib import Path

from batchwithin import BatchWithin, NaiveBatchWithin
from batchwithin.invariants import check
from batchwithin.sim import random_schedule, run_schedule

OFF = dict(reset_on_arrival=False, stale_timers=False, drop_tick_on_backpressure=False)
ROOT = Path(__file__).resolve().parents[1]


def naive(k, d, **bug):
    f = dict(OFF)
    f.update(bug)
    return NaiveBatchWithin(k, d, **f)


class TestStarvation(unittest.TestCase):
    ARR = list(range(200))   # 1 item per tick, D = 10: upstream faster than D
    REQ = [0] * 200

    def test_naive_reset_on_arrival_starves(self):
        res = run_schedule(naive(1000, 10, reset_on_arrival=True), self.ARR, self.REQ, horizon=199)
        self.assertEqual(res.emissions, [])  # 0 chunks in 200 ticks
        later = run_schedule(naive(1000, 10, reset_on_arrival=True), self.ARR, self.REQ)
        self.assertTrue(any(v.startswith("I3") for v in check(later)))

    def test_fixed_emits_every_d(self):
        res = run_schedule(BatchWithin(1000, 10), self.ARR, self.REQ, horizon=199)
        self.assertEqual(len(res.emissions), 19)
        self.assertEqual([e.seal_time for e in res.emissions], list(range(10, 200, 10)))
        self.assertTrue(all(len(e.chunk) == 10 for e in res.emissions))
        self.assertEqual(check(res), [])


class TestBackpressureTickLoss(unittest.TestCase):
    ARR = list(range(0, 200, 2))
    REQ = list(range(35, 400, 35))

    def test_naive_drop_tick_overstays_window(self):
        res = run_schedule(naive(50, 10, drop_tick_on_backpressure=True), self.ARR, self.REQ)
        ages = [e.seal_time - e.window_start for e in res.emissions]
        self.assertGreater(max(ages), 10)
        self.assertTrue(any(v.startswith("I3") for v in check(res)))

    def test_fixed_keeps_window_under_backpressure(self):
        res = run_schedule(BatchWithin(50, 10), self.ARR, self.REQ)
        self.assertEqual(check(res), [])
        self.assertTrue(all(e.seal_time - e.window_start == 10 for e in res.emissions))
        self.assertGreater(res.held_offers, 0)  # backpressure really engaged


class TestStaleTimer(unittest.TestCase):
    def test_naive_stale_timer_cuts_next_window_and_emits_empty(self):
        res = run_schedule(naive(3, 10, stale_timers=True), [0, 1, 2, 5, 6], [0] * 10)
        sizes = [len(e.chunk) for e in res.emissions]
        self.assertIn(0, sizes)
        self.assertEqual(res.emissions[1].seal_time, 10)  # window opened at 5, cut at 10
        v = check(res)
        self.assertTrue(any(x.startswith("I1") for x in v))
        self.assertTrue(any(x.startswith("I4") for x in v))

    def test_fixed_boundary(self):
        res = run_schedule(BatchWithin(3, 10), [0, 1, 2, 5, 6], [0] * 10)
        self.assertEqual([(e.chunk, e.seal_time) for e in res.emissions],
                         [((0, 1, 2), 2), ((3, 4), 15)])
        self.assertEqual(check(res), [])


class TestFuzzFindsNaiveBugs(unittest.TestCase):
    def test_each_bug_found_by_random_search_fixed_never(self):
        rng = random.Random(3)
        found = {b: 0 for b in OFF}
        for _ in range(1500):
            k, d = rng.randint(1, 6), rng.randint(2, 8)
            arr, req, comp = random_schedule(rng, d=d)
            self.assertEqual(check(run_schedule(BatchWithin(k, d), arr, req, comp)), [])
            for b in OFF:
                if check(run_schedule(naive(k, d, **{b: True}), arr, req, comp)):
                    found[b] += 1
        for b, n in found.items():
            self.assertGreater(n, 20, f"{b}: {found}")

    def test_reproducer_script_exit_zero(self):
        p = subprocess.run([sys.executable, str(ROOT / "reproduce.py")], cwd=ROOT,
                           capture_output=True, text=True, timeout=60)
        self.assertEqual(p.returncode, 0, p.stdout + p.stderr)
        self.assertIn("RESULT: PASS", p.stdout)


if __name__ == "__main__":
    unittest.main()
