"""Property tests: seeded random schedules, exhaustive small schedules,
metamorphic relations. Every run is checked twice: against the independent
tick-stepped oracle (exact equality of chunks, window starts, seal and
delivery times, reasons, admission times) and by the trace-only invariant
checker (I1..I7)."""
import hashlib
import itertools
import json
import os
import random
import unittest
from fractions import Fraction

from batchwithin import BatchWithin
from batchwithin.invariants import check
from batchwithin.sim import random_schedule, run_schedule
from tests.oracle import oracle

SEED = int(os.environ.get("BW_SEED", "20261126"))
N_RANDOM = int(os.environ.get("BW_N", "4000"))


def as_tuples(es):
    return [(e.chunk, e.window_start, e.seal_time, e.deliver_time, e.reason) for e in es]


def run_both(k, d, r, arr, req, comp):
    res = run_schedule(BatchWithin(k, d, r), arr, req, comp)
    exp, adm, ca = oracle(k, d, r, arr, req, comp)
    return res, exp, adm, ca


class TestRandomSchedules(unittest.TestCase):
    def test_seeded_random_vs_oracle_and_invariants(self):
        rng = random.Random(SEED)
        stats = {"runs": 0, "chunks": 0, "time": 0, "size": 0, "complete": 0, "held": 0}
        for i in range(N_RANDOM):
            k, d, r = rng.randint(1, 7), rng.randint(1, 9), rng.randint(1, 3)
            arr, req, comp = random_schedule(rng, d=d)
            res, exp, adm, ca = run_both(k, d, r, arr, req, comp)
            ctx = f"seed={SEED} i={i} K={k} D={d} R={r} arr={arr} req={req} comp={comp}"
            self.assertEqual(as_tuples(res.emissions), exp, ctx)
            self.assertEqual(res.admissions, adm, ctx)
            self.assertEqual(res.complete_at, ca, ctx)
            self.assertEqual(check(res), [], ctx)
            stats["runs"] += 1
            stats["chunks"] += len(res.emissions)
            stats["held"] += res.held_offers
            for e in res.emissions:
                stats[e.reason] += 1
        # the generator must actually exercise every branch
        for key in ("time", "size", "complete", "held"):
            self.assertGreater(stats[key], 100, stats)
        print(f"\n  random: {stats}", end=" ")

    def test_drained_liveness(self):
        """With completion and unlimited demand every admitted item comes out."""
        rng = random.Random(SEED + 1)
        for i in range(1500):
            k, d, r = rng.randint(1, 6), rng.randint(1, 8), rng.randint(1, 3)
            arr, _, _ = random_schedule(rng, d=d)
            comp = (arr[-1] if arr else 0) + rng.randint(0, d)
            req = [0] * (len(arr) + 1)
            res = run_schedule(BatchWithin(k, d, r), arr, req, comp)
            self.assertEqual(check(res, require_drained=True), [], f"i={i}")
            self.assertEqual(len(res.admissions), len(arr))

    def test_float_times(self):
        rng = random.Random(SEED + 2)
        for i in range(1500):
            k, d, r = rng.randint(1, 6), rng.uniform(0.001, 0.05), rng.randint(1, 3)
            t, arr = 0.0, []
            for _ in range(rng.randint(0, 50)):
                t += rng.choice([0.0, rng.uniform(0, d), rng.uniform(d, 3 * d)])
                arr.append(t)
            t, req = 0.0, []
            for _ in range(rng.randint(0, 40)):
                t += rng.choice([0.0, rng.uniform(0, 4 * d)])
                req.append(t)
            res = run_schedule(BatchWithin(k, d, r), arr, req, t + 1)
            self.assertEqual(check(res), [], f"i={i}")

    def test_exact_rationals(self):
        """Fractions: deadline arithmetic is exact, ties are real ties."""
        rng = random.Random(SEED + 3)
        for i in range(800):
            k, r = rng.randint(1, 5), rng.randint(1, 2)
            d = Fraction(rng.randint(1, 7), rng.randint(1, 3))
            t, arr = Fraction(0), []
            for _ in range(rng.randint(0, 30)):
                t += rng.choice([Fraction(0), d, d / 2, d / 3])
                arr.append(t)
            req = sorted(Fraction(rng.randint(0, 60), 3) for _ in range(rng.randint(0, 30)))
            res = run_schedule(BatchWithin(k, d, r), arr, req, t + d)
            self.assertEqual(check(res), [], f"i={i}")


class TestExhaustive(unittest.TestCase):
    def test_all_small_schedules(self):
        """Every arrival pattern with 0..2 items per tick over 5 ticks x every
        pull subset over 6 ticks x K,D in 1..3 x R in 1..2 x with/without
        completion = 559,872 runs, each equal to the oracle and invariant-clean."""
        H, P = 5, 6
        n = 0
        for k, d, r in itertools.product((1, 2, 3), (1, 2, 3), (1, 2)):
            for counts in itertools.product((0, 1, 2), repeat=H):
                arr = [t for t, c in enumerate(counts) for _ in range(c)]
                for mask in range(1 << P):
                    req = [i for i in range(P) if mask >> i & 1]
                    for comp in (None, H):
                        res, exp, adm, ca = run_both(k, d, r, arr, req, comp)
                        if as_tuples(res.emissions) != exp or res.admissions != adm or check(res):
                            self.fail(f"K={k} D={d} R={r} arr={arr} req={req} comp={comp}: "
                                      f"{check(res)}")
                        n += 1
        self.assertEqual(n, 559872)


class TestMetamorphic(unittest.TestCase):
    def _cases(self, seed, n=600):
        rng = random.Random(seed)
        for _ in range(n):
            k, d, r = rng.randint(1, 6), rng.randint(1, 8), rng.randint(1, 3)
            yield (k, d, r) + random_schedule(rng, d=d)

    def test_time_translation(self):
        for k, d, r, arr, req, comp in self._cases(SEED + 10):
            c = 1000
            a = run_schedule(BatchWithin(k, d, r), arr, req, comp)
            b = run_schedule(BatchWithin(k, d, r), [x + c for x in arr], [x + c for x in req],
                             None if comp is None else comp + c)
            self.assertEqual([(e.chunk, e.seal_time + c, e.deliver_time + c) for e in a.emissions],
                             [(e.chunk, e.seal_time, e.deliver_time) for e in b.emissions])

    def test_time_scaling(self):
        for k, d, r, arr, req, comp in self._cases(SEED + 11):
            s = 7
            a = run_schedule(BatchWithin(k, d, r), arr, req, comp)
            b = run_schedule(BatchWithin(k, d * s, r), [x * s for x in arr], [x * s for x in req],
                             None if comp is None else comp * s)
            self.assertEqual([(e.chunk, e.seal_time * s, e.reason) for e in a.emissions],
                             [(e.chunk, e.seal_time, e.reason) for e in b.emissions])

    def test_chunk_boundaries_independent_of_pulls_without_backpressure(self):
        """With a ready queue big enough to never close, pull timing changes
        delivery times only, never chunk boundaries (deadline is not tied to demand)."""
        rng = random.Random(SEED + 12)
        for _ in range(600):
            k, d = rng.randint(1, 6), rng.randint(1, 8)
            arr, req, comp = random_schedule(rng, d=d)
            big = len(arr) + 2
            eager = run_schedule(BatchWithin(k, d, big), arr, [0] * big, comp)
            lazy = run_schedule(BatchWithin(k, d, big), arr, sorted(req), comp)
            n = len(lazy.emissions)
            self.assertEqual([(e.chunk, e.seal_time) for e in eager.emissions[:n]],
                             [(e.chunk, e.seal_time) for e in lazy.emissions])

    def test_determinism_hash(self):
        def digest():
            h = hashlib.sha256()
            for k, d, r, arr, req, comp in self._cases(SEED + 13, 300):
                res = run_schedule(BatchWithin(k, d, r), arr, req, comp)
                h.update(json.dumps(as_tuples(res.emissions)).encode())
            return h.hexdigest()
        self.assertEqual(digest(), digest())


if __name__ == "__main__":
    unittest.main()
