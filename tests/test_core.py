"""Unit tests for the pure state machine: every transition, tie rule and edge."""
import unittest

from batchwithin import BatchWithin, ProtocolError, State


def chunks(es):
    return [list(e.chunk) for e in es]


class TestTransitions(unittest.TestCase):
    def test_idle_to_filling_arms_fixed_deadline(self):
        m = BatchWithin(5, 10)
        self.assertIsNone(m.next_deadline)
        m.offer(3, "a")
        self.assertEqual(m.state, State.FILLING)
        self.assertEqual(m.next_deadline, 13)

    def test_arrivals_never_move_deadline(self):
        m = BatchWithin(100, 10)
        for t in range(0, 10):
            m.offer(t, t)
            self.assertEqual(m.next_deadline, 10)

    def test_pulls_never_move_deadline(self):
        m = BatchWithin(100, 10)
        m.offer(0, "a")
        for t in range(1, 10):
            m.request(t)
            self.assertEqual(m.next_deadline, 10)

    def test_size_seal_at_kth_admission(self):
        m = BatchWithin(3, 10)
        m.request(0)
        for t, x in ((1, "a"), (2, "b"), (4, "c")):
            m.offer(t, x)
        e = m.emissions[0]
        self.assertEqual((e.chunk, e.seal_time, e.reason), (("a", "b", "c"), 4, "size"))
        self.assertEqual(m.state, State.IDLE)
        self.assertIsNone(m.next_deadline)

    def test_time_seal_exactly_at_deadline(self):
        m = BatchWithin(3, 10)
        m.request(0)
        m.offer(2, "a")
        m.advance(12)
        e = m.emissions[0]
        self.assertEqual((e.chunk, e.window_start, e.seal_time, e.reason), (("a",), 2, 12, "time"))

    def test_catch_up_seals_at_deadline_not_at_now(self):
        m = BatchWithin(3, 10)
        m.request(0)
        m.offer(2, "a")
        m.advance(500)  # driver was late by 488
        e = m.emissions[0]
        self.assertEqual((e.seal_time, e.deliver_time), (12, 12))

    def test_tie_deadline_before_arrival(self):
        m = BatchWithin(3, 10)
        m.request(0, 2)
        m.offer(0, "a")
        m.offer(10, "b")  # exactly at deadline -> opens next window
        self.assertEqual(chunks(m.emissions), [["a"]])
        self.assertEqual(m.next_deadline, 20)

    def test_tie_deadline_before_request(self):
        m = BatchWithin(3, 10)
        m.offer(0, "a")
        out = m.request(10)
        self.assertEqual(out[0].seal_time, 10)
        self.assertEqual(out[0].deliver_time, 10)

    def test_arrival_just_before_deadline_joins_window(self):
        m = BatchWithin(3, 10.0)
        m.request(0.0)
        m.offer(0.0, "a")
        m.offer(9.999, "b")
        m.advance(10.0)
        self.assertEqual(chunks(m.emissions), [["a", "b"]])

    def test_delivery_waits_for_request(self):
        m = BatchWithin(2, 10)
        m.offer(0, "a")
        m.offer(1, "b")
        self.assertEqual(m.emissions, [])
        out = m.request(7)
        self.assertEqual((out[0].seal_time, out[0].deliver_time), (1, 7))

    def test_request_waits_for_chunk(self):
        m = BatchWithin(2, 10)
        self.assertEqual(m.request(0), [])
        self.assertEqual(m.demand, 1)
        m.offer(4, "a")
        m.offer(5, "b")
        self.assertEqual(m.emissions[0].deliver_time, 5)
        self.assertEqual(m.demand, 0)

    def test_closed_when_ready_full_and_active_sealed(self):
        m = BatchWithin(2, 10)
        for t in range(4):
            self.assertTrue(m.offer(t, t))
        self.assertEqual(m.state, State.CLOSED)
        self.assertFalse(m.can_admit)
        self.assertFalse(m.offer(4, 99))
        self.assertIsNone(m.next_deadline)  # no timer while closed

    def test_request_reopens_closed(self):
        m = BatchWithin(2, 10)
        for t in range(4):
            m.offer(t, t)
        out = m.request(20)
        self.assertEqual(chunks(out), [[0, 1]])
        self.assertEqual(m.state, State.IDLE)
        self.assertTrue(m.offer(20, 4))
        self.assertEqual(m.next_deadline, 30)

    def test_deadline_sealed_while_ready_full_keeps_exact_time(self):
        m = BatchWithin(5, 10)
        for t in range(5):
            m.offer(t, t)          # size seal at 4 -> ready
        m.offer(6, "x")            # new window, deadline 16
        m.advance(40)              # ready still full -> sealed into CLOSED at 16
        self.assertEqual(m.state, State.CLOSED)
        out = m.request(50, 2)
        self.assertEqual([(e.chunk, e.seal_time, e.deliver_time) for e in out],
                         [((0, 1, 2, 3, 4), 4, 50), (("x",), 16, 50)])

    def test_max_ready_larger_queue(self):
        m = BatchWithin(1, 10, max_ready=3)
        for t in range(4):
            self.assertTrue(m.offer(t, t))
        self.assertEqual(m.state, State.CLOSED)
        self.assertEqual(m.buffered, 4)
        self.assertEqual(len(m.request(9, 4)), 4)

    def test_buffer_bound(self):
        m = BatchWithin(4, 10, max_ready=2)
        t = 0
        while m.offer(t, t):
            t += 1
        self.assertEqual(m.buffered, 12)
        self.assertEqual(m.max_buffered, (2 + 1) * 4)

    def test_complete_flushes_partial(self):
        m = BatchWithin(5, 10)
        m.request(0, 5)
        m.offer(1, "a")
        m.offer(2, "b")
        out = m.complete(3)
        self.assertEqual([(e.chunk, e.seal_time, e.reason) for e in out], [(("a", "b"), 3, "complete")])
        self.assertTrue(m.done)

    def test_complete_on_empty_emits_nothing(self):
        m = BatchWithin(5, 10)
        m.request(0)
        self.assertEqual(m.complete(1), [])
        self.assertTrue(m.done)
        self.assertEqual(m.emissions, [])

    def test_complete_after_size_flush_no_empty_chunk(self):
        m = BatchWithin(2, 10)
        m.request(0, 3)
        m.offer(0, 1)
        m.offer(0, 2)
        m.complete(0)
        self.assertEqual(chunks(m.emissions), [[1, 2]])

    def test_complete_while_closed_drains(self):
        m = BatchWithin(1, 10)
        m.offer(0, "a")
        m.offer(0, "b")
        m.complete(1)
        self.assertEqual(m.state, State.DRAINING)
        m.request(2, 2)
        self.assertEqual(chunks(m.emissions), [["a"], ["b"]])
        self.assertTrue(m.done)

    def test_deadline_due_at_complete_wins(self):
        m = BatchWithin(5, 10)
        m.request(0, 2)
        m.offer(0, "a")
        m.complete(10)
        self.assertEqual(m.emissions[0].reason, "time")

    def test_k_equals_one(self):
        m = BatchWithin(1, 10)
        m.request(0, 3)
        for t in range(3):
            m.offer(t, t)
        self.assertEqual(chunks(m.emissions), [[0], [1], [2]])
        self.assertTrue(all(e.reason == "size" for e in m.emissions))

    def test_burst_same_instant(self):
        m = BatchWithin(3, 10, max_ready=5)
        m.request(0, 5)
        for x in range(7):
            m.offer(0, x)
        m.advance(10)
        self.assertEqual(chunks(m.emissions), [[0, 1, 2], [3, 4, 5], [6]])
        self.assertEqual([e.seal_time for e in m.emissions], [0, 0, 10])

    def test_request_after_done_is_noop(self):
        m = BatchWithin(2, 10)
        m.complete(0)
        self.assertEqual(m.request(1), [])


class TestProtocol(unittest.TestCase):
    def test_time_backwards_rejected(self):
        m = BatchWithin(2, 10)
        m.offer(5, "a")
        with self.assertRaises(ProtocolError):
            m.offer(4, "b")

    def test_offer_after_complete_rejected(self):
        m = BatchWithin(2, 10)
        m.complete(0)
        with self.assertRaises(ProtocolError):
            m.offer(1, "a")

    def test_bad_parameters(self):
        for args in ((0, 1), (1, 0), (1, -1), (2.5, 1), (1, 1, 0)):
            with self.assertRaises(ValueError):
                BatchWithin(*args)
        with self.assertRaises(ValueError):
            BatchWithin(1, 1).request(0, 0)

    def test_double_complete_idempotent(self):
        m = BatchWithin(2, 10)
        m.offer(0, 1)
        m.complete(1)
        self.assertEqual(m.complete(2), [])

    def test_trace_records_transitions(self):
        m = BatchWithin(2, 10)
        m.request(0)
        m.offer(0, 1)
        m.advance(10)
        kinds = [ev for _, ev, _ in m.trace]
        self.assertEqual(kinds, ["request", "admit", "seal:time", "emit"])


if __name__ == "__main__":
    unittest.main()
